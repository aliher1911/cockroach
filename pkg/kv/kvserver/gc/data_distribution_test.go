// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gc

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// dataDistribution is an abstraction for testing that represents a stream of
// MVCCKeyValues. The stream may indicate that a value is an intent by returning
// a non-nil transaction. If an intent is returned it must have a higher
// timestamp than any other version written for the key.
type dataDistribution func() (storage.MVCCKeyValue, *roachpb.Transaction, bool)

// setupTest writes the data from this distribution into eng. All data should
// be a part of the range represented by desc.
func (ds dataDistribution) setupTest(
	t testing.TB, eng storage.Engine, desc roachpb.RangeDescriptor,
) enginepb.MVCCStats {
	ctx := context.Background()
	var maxTs hlc.Timestamp
	var ms enginepb.MVCCStats
	for {
		kv, txn, ok := ds()
		if !ok {
			break
		}
		if txn == nil {
			if kv.Key.Timestamp.IsEmpty() {
				require.NoError(t, eng.PutUnversioned(kv.Key.Key, kv.Value))
			} else {
				require.NoError(t, eng.PutMVCC(kv.Key, kv.Value))
			}
		} else {
			// TODO(ajwerner): Decide if using MVCCPut is worth it.
			ts := kv.Key.Timestamp
			if txn.ReadTimestamp.IsEmpty() {
				txn.ReadTimestamp = ts
			}
			if txn.WriteTimestamp.IsEmpty() {
				txn.WriteTimestamp = ts
			}
			err := storage.MVCCPut(ctx, eng, &ms, kv.Key.Key, ts,
				roachpb.Value{RawBytes: kv.Value}, txn)
			require.NoError(t, err)
		}
		if !kv.Key.Timestamp.Less(maxTs) {
			maxTs = kv.Key.Timestamp
		}
	}
	require.NoError(t, eng.Flush())
	snap := eng.NewSnapshot()
	defer snap.Close()
	ms, err := rditer.ComputeStatsForRange(&desc, snap, maxTs.WallTime)
	require.NoError(t, err)
	return ms
}

// newDataDistribution constructs a dataDistribution from various underlying
// distributions.
func newDataDistribution(
	tsDist func() hlc.Timestamp,
	keyDist func() roachpb.Key,
	valueDist func() []byte,
	versionsPerKey func() int,
	intentFrac float64,
	totalKeys int,
	rng *rand.Rand,
) dataDistribution {
	// TODO(ajwerner): provide a mechanism to control the rate of expired intents
	// or the intent age. Such a knob would likely require decoupling intents from
	// other keys.
	var (
		// Remaining values (all versions of all keys together with intents).
		remaining = totalKeys
		// Key for the objects currently emitted (if versions are not empty).
		key roachpb.Key
		// Set of key.String() to avoid generating data for the same key multiple
		// times.
		seen = map[string]struct{}{}
		// Pending timestamps for current key sorted in ascending order.
		timestamps []hlc.Timestamp
		// If we should have an intent at the start of history.
		haveIntent bool
	)
	return func() (storage.MVCCKeyValue, *roachpb.Transaction, bool) {
		if remaining == 0 {
			return storage.MVCCKeyValue{}, nil, false
		}
		defer func() { remaining-- }()
		// Loop because we can have duplicate keys or unacceptable values, in that
		// case we retry key from scratch.
		for len(timestamps) == 0 {
			versions := versionsPerKey()
			if versions == 0 {
				continue
			}
			if versions > remaining {
				versions = remaining
			}
			timestamps = make([]hlc.Timestamp, 0, versions)
			for i := 0; i < versions; i++ {
				// Looks like we could have duplicate timestamps here.
				timestamps = append(timestamps, tsDist())
			}
			sort.Slice(timestamps, func(i, j int) bool {
				return timestamps[i].Less(timestamps[j])
			})
			for {
				key = keyDist()
				sk := string(key)
				if _, ok := seen[sk]; ok {
					continue
				}
				seen[sk] = struct{}{}
				break
			}
			haveIntent = rng.Float64() < intentFrac
		}
		ts := timestamps[0]
		timestamps = timestamps[1:]
		var txn *roachpb.Transaction
		// On the last version, we generate a transaction as needed.
		if len(timestamps) == 0 && haveIntent {
			txn = &roachpb.Transaction{
				Status:                 roachpb.PENDING,
				ReadTimestamp:          ts,
				GlobalUncertaintyLimit: ts.Next().Next(),
			}
			txn.ID = uuid.MakeV4()
			txn.WriteTimestamp = ts
			txn.Key = keyDist()
		}
		return storage.MVCCKeyValue{
			Key:   storage.MVCCKey{Key: key, Timestamp: ts},
			Value: valueDist(),
		}, txn, true
	}
}

// distSpec abstractly represents a distribution.
type distSpec interface {
	dist(maxRows int, rng *rand.Rand) dataDistribution
	desc() *roachpb.RangeDescriptor
	String() string
}

// uniformDistSpec is a distSpec which represents uniform distributions over its
// various dimensions.
type uniformDistSpec struct {
	tsSecFrom, tsSecTo               int64 // seconds
	keySuffixMin, keySuffixMax       int
	valueLenMin, valueLenMax         int
	deleteFrac                       float64
	keysPerValueMin, keysPerValueMax int
	intentFrac                       float64
}

var _ distSpec = uniformDistSpec{}

func (ds uniformDistSpec) dist(maxRows int, rng *rand.Rand) dataDistribution {
	return newDataDistribution(
		uniformTimestampDistribution(ds.tsSecFrom*time.Second.Nanoseconds(), ds.tsSecTo*time.Second.Nanoseconds(), rng),
		uniformTableStringKeyDistribution(ds.desc().StartKey.AsRawKey(), ds.keySuffixMin, ds.keySuffixMax, rng),
		uniformValueStringDistribution(ds.valueLenMin, ds.valueLenMax, ds.deleteFrac, rng),
		uniformValuesPerKey(ds.keysPerValueMin, ds.keysPerValueMax, rng),
		ds.intentFrac,
		maxRows,
		rng,
	)
}

func (ds uniformDistSpec) desc() *roachpb.RangeDescriptor {
	tablePrefix := keys.SystemSQLCodec.TablePrefix(42)
	return &roachpb.RangeDescriptor{
		StartKey: roachpb.RKey(tablePrefix),
		EndKey:   roachpb.RKey(tablePrefix.PrefixEnd()),
	}
}

func (ds uniformDistSpec) String() string {
	return fmt.Sprintf(
		"ts=[%d,%d],"+
			"keySuffix=[%d,%d],"+
			"valueLen=[%d,%d],"+
			"keysPerValue=[%d,%d],"+
			"deleteFrac=%f,intentFrac=%f",
		ds.tsSecFrom, ds.tsSecTo,
		ds.keySuffixMin, ds.keySuffixMax,
		ds.valueLenMin, ds.valueLenMax,
		ds.keysPerValueMin, ds.keysPerValueMax,
		ds.deleteFrac, ds.intentFrac)
}

// uniformTimestamp returns an hlc timestamp distribution with a wall time
// uniform over [from, to] and a zero logical timestamp.
func uniformTimestampDistribution(from, to int64, rng *rand.Rand) func() hlc.Timestamp {
	if from >= to {
		panic(fmt.Errorf("from (%d) >= to (%d)", from, to))
	}
	n := int(to-from) + 1
	return func() hlc.Timestamp {
		return hlc.Timestamp{WallTime: from + int64(rng.Intn(n))}
	}
}

// returns a uniform length random value distribution.
func uniformValueDistribution(min, max int, deleteFrac float64, rng *rand.Rand) func() []byte {
	if min > max {
		panic(fmt.Errorf("min (%d) > max (%d)", min, max))
	}
	n := (max - min) + 1
	return func() []byte {
		if rng.Float64() < deleteFrac {
			return nil
		}
		value := make([]byte, min+rng.Intn(n))
		if _, err := rng.Read(value); err != nil {
			panic(err)
		}
		return value
	}
}

const allowedPrintableAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// returns a uniform length random value distribution.
func uniformValueStringDistribution(
	min, max int, deleteFrac float64, rng *rand.Rand,
) func() []byte {
	if min > max {
		panic(fmt.Errorf("min (%d) > max (%d)", min, max))
	}
	n := (max - min) + 1
	return func() []byte {
		if rng.Float64() < deleteFrac {
			return nil
		}
		return []byte(randString(rng, min+rng.Intn(n), allowedPrintableAlphabet))
	}
}

func uniformValuesPerKey(valuesPerKeyMin, valuesPerKeyMax int, rng *rand.Rand) func() int {
	if valuesPerKeyMin > valuesPerKeyMax {
		panic(fmt.Errorf("min (%d) > max (%d)", valuesPerKeyMin, valuesPerKeyMax))
	}
	n := (valuesPerKeyMax - valuesPerKeyMin) + 1
	return func() int { return valuesPerKeyMin + rng.Intn(n) }
}

func uniformTableKeyDistribution(
	prefix roachpb.Key, suffixMin, suffixMax int, rng *rand.Rand,
) func() roachpb.Key {
	if suffixMin > suffixMax {
		panic(fmt.Errorf("suffixMin (%d) > suffixMax (%d)", suffixMin, suffixMax))
	}
	n := (suffixMax - suffixMin) + 1
	return func() roachpb.Key {
		randData := make([]byte, suffixMin+rng.Intn(n))
		_, _ = rng.Read(randData)
		return encoding.EncodeBytesAscending(prefix[0:len(prefix):len(prefix)], randData)
	}
}

func uniformTableStringKeyDistribution(
	prefix roachpb.Key, suffixMin, suffixMax int, rng *rand.Rand,
) func() roachpb.Key {
	if suffixMin > suffixMax {
		panic(fmt.Errorf("suffixMin (%d) > suffixMax (%d)", suffixMin, suffixMax))
	}
	n := (suffixMax - suffixMin) + 1
	return func() roachpb.Key {
		lenSuffix := suffixMin + rng.Intn(n)
		var mvccKey roachpb.Key = make([]byte, len(prefix)+lenSuffix)
		key := randString(rng, lenSuffix, allowedPrintableAlphabet)
		mvccKey = append(mvccKey[:0], prefix...)
		mvccKey = append(mvccKey, key...)
		return mvccKey
	}
}

// RandString generates a random string of the desired length from the
// input alphabet.
func randString(rng *rand.Rand, length int, alphabet string) string {
	buf := make([]byte, length)
	for i := range buf {
		buf[i] = alphabet[rng.Intn(len(alphabet))]
	}
	return string(buf)
}
