// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/gc"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

type TestGCer struct {
	gcKeys map[string]roachpb.GCRequest_GCKey
	// fake GCer stores range key batches as it since we need to be able to
	// feed them into MVCCGarbageCollectRangeKeys and ranges argument should be
	// non-overlapping.
	gcRangeKeyBatches [][]roachpb.GCRequest_GCRangeKey
	gcClearRangeKeys  []roachpb.GCRequest_GCClearRangeKey
	threshold         gc.Threshold
}

func MakeTestGCer() TestGCer {
	return TestGCer{
		gcKeys: make(map[string]roachpb.GCRequest_GCKey),
	}
}

var _ gc.GCer = (*TestGCer)(nil)

func (f *TestGCer) SetGCThreshold(ctx context.Context, t gc.Threshold) error {
	f.threshold = t
	return nil
}

func (f *TestGCer) GC(
	ctx context.Context,
	keys []roachpb.GCRequest_GCKey,
	rangeKeys []roachpb.GCRequest_GCRangeKey,
	clearRangeKey *roachpb.GCRequest_GCClearRangeKey,
) error {
	for _, k := range keys {
		f.gcKeys[k.Key.String()] = k
	}
	f.gcRangeKeyBatches = append(f.gcRangeKeyBatches, rangeKeys)
	if clearRangeKey != nil {
		f.gcClearRangeKeys = append(f.gcClearRangeKeys, *clearRangeKey)
	}
	return nil
}

func (f *TestGCer) resolveIntentsAsync(_ context.Context, txn *roachpb.Transaction) error {
	return nil
}

func (f *TestGCer) resolveIntents(_ context.Context, intents []roachpb.Intent) error {
	return nil
}

func (f *TestGCer) pointKeys() []roachpb.GCRequest_GCKey {
	var reqs []roachpb.GCRequest_GCKey
	for _, r := range f.gcKeys {
		reqs = append(reqs, r)
	}
	return reqs
}

func (f *TestGCer) rangeKeyBatches() [][]roachpb.GCRequest_GCRangeKey {
	return f.gcRangeKeyBatches
}

func (f *TestGCer) rangeKeys() []roachpb.GCRequest_GCRangeKey {
	var reqs []roachpb.GCRequest_GCRangeKey
	for _, r := range f.gcRangeKeyBatches {
		reqs = append(reqs, r...)
	}
	return reqs
}

func (f *TestGCer) ExecuteGC(ctx context.Context, t *testing.T, desc roachpb.RangeDescriptor,
	rw storage.ReadWriter, rangeStats enginepb.MVCCStats,
) enginepb.MVCCStats {
	var statsUpdate enginepb.MVCCStats
	if crks := len(f.gcClearRangeKeys); crks > 0 {
		require.Equal(t, 1, crks, "can only have one clear range in one GC run")
		require.NoError(t,
			storage.MVCCGarbageCollectWholeRange(ctx, rw, &statsUpdate, f.gcClearRangeKeys[0].StartKey,
				f.gcClearRangeKeys[0].EndKey, f.threshold.Key, rangeStats))
	}
	if len(f.gcKeys) > 0 {
		require.NoError(t,
			storage.MVCCGarbageCollect(ctx, rw, &statsUpdate, f.pointKeys(), f.threshold.Key))
	}

	for _, batch := range f.gcRangeKeyBatches {
		rangeKeys := makeCollectableGCRangesFromGCRequests(desc.StartKey.AsRawKey(),
			desc.EndKey.AsRawKey(), batch)
		require.NoError(t,
			storage.MVCCGarbageCollectRangeKeys(ctx, rw, &statsUpdate, rangeKeys))
	}
	return statsUpdate
}

// makeCollectableGCRangesFromGCRequests mirrors
// MakeCollectableGCRangesFromGCRequests to break cyclic dependencies.
func makeCollectableGCRangesFromGCRequests(
	rangeStart, rangeEnd roachpb.Key, rangeKeys []roachpb.GCRequest_GCRangeKey,
) []storage.CollectableGCRangeKey {
	collectableKeys := make([]storage.CollectableGCRangeKey, len(rangeKeys))
	for i, rk := range rangeKeys {
		leftPeekBound := rk.StartKey.Prevish(roachpb.PrevishKeyLength)
		if len(rangeStart) > 0 && leftPeekBound.Compare(rangeStart) <= 0 {
			leftPeekBound = rangeStart
		}
		rightPeekBound := rk.EndKey.Next()
		if len(rangeEnd) > 0 && rightPeekBound.Compare(rangeEnd) >= 0 {
			rightPeekBound = rangeEnd
		}
		collectableKeys[i] = storage.CollectableGCRangeKey{
			MVCCRangeKey: storage.MVCCRangeKey{
				StartKey:  rk.StartKey,
				EndKey:    rk.EndKey,
				Timestamp: rk.Timestamp,
			},
			LatchSpan: roachpb.Span{
				Key:    leftPeekBound,
				EndKey: rightPeekBound,
			},
		}
	}
	return collectableKeys
}

func uniformDist(rng *rand.Rand, minKey, maxKey int) func() int {
 return func() int {
	 return rng.Intn(maxKey-minKey) + minKey
 }
}

func normalDist(rng *rand.Rand, minKey, maxKey int) func() int {
	stdDev := float64(maxKey - minKey) / 6
	mean := float64(maxKey + minKey) / 2
	return func() int {
		for {
			sample := int(rng.NormFloat64()*stdDev + mean)
			if sample >= minKey && sample < maxKey {
				return sample
			}
		}
	}
}

func TestGarbageAmount(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	type dataSpec struct {
		name           string
		minKey, maxKey int
		keyDist        func(rng *rand.Rand, minKey, maxKey int) func() int
	}

	type iterationParams struct {
		write              bool
		wipe               bool
		useRangeTombstones bool
		rowTTL             time.Duration
		gcTTL              time.Duration
		protectedTS        hlc.Timestamp
	}

	// Change behaviours when certain points reached
	type event struct {
		// Name will trigger creation of column
		name      string
		count     int
		// Next invocation
		triggerAt hlc.Timestamp
		// Bump trigger time every time it is triggered until count is exhausted
		interval  time.Duration
		action    func(*iterationParams, hlc.Timestamp)
	}

	type testSpec struct {
		name string

		// GC Params.
		ttl         time.Duration
		rowLevelTTL time.Duration

		// Generation params.
		runTime         time.Duration
		checkInterval   time.Duration
		keysPerInterval int
		delRate         float64

		// Events
		events          []event
	}

	stopWriteAt := func(after time.Duration) event {
		return event{
			name:      "WriteCutoff",
			count:     1,
			interval:  after,
			action:    func(p *iterationParams, now hlc.Timestamp) {
				p.write = false
			},
		}
	}

	wipeData := func(count int, interval time.Duration, useRangeTombstones bool) event {
		return event{
			name:      "Delete",
			count:     count,
			interval:  interval,
			action: func(params *iterationParams, now hlc.Timestamp) {
				params.wipe = true
				params.useRangeTombstones = useRangeTombstones
			},
		}
	}

	setRowTTL := func(value time.Duration, at time.Duration) event {
		return event{
			name:      "RowTTL",
			count:     1,
			interval:  at,
			action: func(params *iterationParams, now hlc.Timestamp) {
				params.rowTTL = value
			},
		}
	}

	setPTS := func(at time.Duration) event {
		return event{
			name:      "SetPTS",
			count:     1,
			interval:  at,
			action: func(params *iterationParams, now hlc.Timestamp) {
				params.protectedTS = now
			},
		}
	}

	clearPTS := func(at time.Duration) event {
		return event{
			name:      "ClearPTS",
			count:     1,
			interval:  at,
			action: func(params *iterationParams, now hlc.Timestamp) {
				params.protectedTS = hlc.Timestamp{}
			},
		}
	}

	setGCTTL := func(value time.Duration, at time.Duration) event {
		return event{
			name:      "SetGCTTL",
			count:     1,
			interval:  at,
			action: func(params *iterationParams, now hlc.Timestamp) {
				params.gcTTL = value
			},
		}
	}

	namePrefix := fmt.Sprintf("threshold-%d", mvccGCKeyScoreThreshold)

	dataSpecs := []dataSpec{
		{
			name:    "extreme-rewrite",
			minKey:  0,
			maxKey:  100,
			keyDist: uniformDist,
		},
		{
			name:    "high-rewrite",
			minKey:  0,
			maxKey:  1000,
			keyDist: uniformDist,
		},
		{
			name:    "low-rewrite",
			minKey:  0,
			maxKey:  100000,
			keyDist: uniformDist,
		},
		{
			name:    "med-rewrite",
			minKey:  0,
			maxKey:  10000,
			keyDist: normalDist,
		},
	}

	for _, spec := range []testSpec{
		{
			name:            "steady-write",
			ttl:             24 * time.Hour,
			runTime:         288 * time.Hour,
			checkInterval:   10 * time.Minute,
			keysPerInterval: 1000,
			delRate:         0.5,
		},
		{
			name:            "stop-writing",
			ttl:             24 * time.Hour,
			runTime:         144 * time.Hour,
			checkInterval:   10 * time.Minute,
			keysPerInterval: 1000,
			delRate:         0.5,
			events: []event{
				stopWriteAt(72 * time.Hour),
			},
		},
		{
			name:               "delete-range",
			ttl:                24 * time.Hour,
			runTime:            144 * time.Hour,
			checkInterval:      10 * time.Minute,
			keysPerInterval:    1000,
			delRate:            0.5,
			events: []event{
				stopWriteAt(72 * time.Hour),
				wipeData(1, 72 * time.Hour, true /* useRangeTombstones */),
			},
		},
		{
			name:               "delete-range-points",
			ttl:                24 * time.Hour,
			runTime:            144 * time.Hour,
			checkInterval:      10 * time.Minute,
			keysPerInterval:    1000,
			delRate:            0.5,
			events: []event{
				stopWriteAt(72 * time.Hour),
				wipeData(1, 72 * time.Hour, false /* useRangeTombstones */),
			},
		},
		{
			name:            "periodic-delete",
			ttl:             24 * time.Hour,
			runTime:         288 * time.Hour,
			checkInterval:   10 * time.Minute,
			keysPerInterval: 1000,
			delRate:         0.5,
			events: []event{
				wipeData(99999, 48 * time.Hour, true /* useRangeTombstones */),
			},
		},
		{
			name:             "row-level-ttl",
			ttl:              24 * time.Hour,
			runTime:          288 * time.Hour,
			checkInterval:    10 * time.Minute,
			keysPerInterval:  1000,
			delRate:          0,
			events: []event{
				setRowTTL(10 * time.Minute, 164 * time.Hour),
			},
		},
		{
			name:             "gc-ttl-drop",
			ttl:              24 * time.Hour,
			runTime:          144 * time.Hour,
			checkInterval:    10 * time.Minute,
			keysPerInterval:  1000,
			delRate:          0.5,
			events: []event{
				setGCTTL(4 * time.Hour, 72 * time.Hour),
			},
		},
		{
			name:             "remove-protected-timestamp",
			ttl:              24 * time.Hour,
			runTime:          288 * time.Hour,
			checkInterval:    10 * time.Minute,
			keysPerInterval:  1000,
			delRate:          0.5,
			events: []event{
				setPTS(0 * time.Hour),
				clearPTS(200 * time.Hour),
			},
		},
	} {
		for _, d := range dataSpecs {
			fullName := spec.name + "-" + d.name
			t.Run(fullName, func(t *testing.T) {
				eng := storage.NewDefaultInMemForTesting()
				defer eng.Close()
				rng, _ := randutil.NewTestRand()

				writeStopwatch := timeutil.NewStopWatch()
				deleteStopwatch := timeutil.NewStopWatch()
				rowTTLStopwatch := timeutil.NewStopWatch()
				gcScanStopwatch := timeutil.NewStopWatch()
				gcEvecStopwatch := timeutil.NewStopWatch()
				compactStopwatch := timeutil.NewStopWatch()

				keyDist := d.keyDist(rng, d.minKey, d.maxKey)

				prefix := keys.SystemSQLCodec.TablePrefix(42)
				desc := roachpb.RangeDescriptor{
					RangeID:  42,
					StartKey: roachpb.RKey(prefix),
					EndKey:   roachpb.RKey(prefix.PrefixEnd()),
				}

				keyLen := len(fmt.Sprintf("%d", d.maxKey))
				keyFormat := fmt.Sprintf("%%0%dd", keyLen)

				intKey := func(key int) roachpb.Key {
					return encoding.EncodeStringAscending(prefix[:len(prefix):len(prefix)],
						fmt.Sprintf(keyFormat, key))
				}
				var ms enginepb.MVCCStats
				addData := func(keyDist func() int, deleteRate float64, base hlc.Timestamp, count int,
					interval time.Duration,
				) {
					writeStopwatch.Start()
					defer writeStopwatch.Stop()

					td := float64(interval-1) / float64(count)
					for i := 0; i < count; i++ {
						ts := base.Add(int64(td*float64(i)+1), 0)
						key := intKey(keyDist())
						v, _, err := storage.MVCCGet(ctx, eng, key, ts,
							storage.MVCCGetOptions{FailOnMoreRecent: true})
						require.NoError(t, err)
						var newValue roachpb.Value
						if v == nil || v.RawBytes == nil || rng.Float64() >= deleteRate {
							newValue.SetString(randutil.RandString(rng, 100, randutil.PrintableKeyAlphabet))
						}
						require.NoError(t,
							storage.MVCCPut(ctx, eng, &ms, key, ts, hlc.ClockTimestamp{}, newValue, nil))
					}
				}
				wipeData := func(base hlc.Timestamp, useRangeTombstone bool) {
					deleteStopwatch.Start()
					defer deleteStopwatch.Stop()

					msCovered := ms
					if useRangeTombstone {
						require.NoError(t,
							storage.MVCCDeleteRangeUsingTombstone(ctx, eng, &ms, desc.StartKey.AsRawKey(),
								desc.EndKey.AsRawKey(), base, hlc.ClockTimestamp{}, desc.StartKey.AsRawKey(),
								desc.EndKey.AsRawKey(), false, 0, &msCovered))
					} else {
						_, _, _, err := storage.MVCCDeleteRange(ctx, eng, &ms, desc.StartKey.AsRawKey(), desc.EndKey.AsRawKey(), 0, base, hlc.ClockTimestamp{}, nil, false)
						require.NoError(t, err)
					}
				}
				rowLevelTTL := func(base hlc.Timestamp, ttl time.Duration) {
					rowTTLStopwatch.Start()
					defer rowTTLStopwatch.Stop()

					// Find all rows older than TTL and put point tombstones.
					// Slightly sketchy as it skips range keys here for now.
					ttlThreshold := base.Add(-ttl.Nanoseconds(), 0)
					it := storage.NewMVCCIncrementalIterator(eng, storage.MVCCIncrementalIterOptions{
						StartKey: desc.StartKey.AsRawKey(),
						EndKey: desc.EndKey.AsRawKey(),
						IntentPolicy: storage.MVCCIncrementalIterIntentPolicyEmit,
						KeyTypes: storage.IterKeyTypePointsOnly,
					})
					defer it.Close()
					it.SeekGE(storage.MVCCKey{Key: desc.StartKey.AsRawKey()})
					for {
						ok, err := it.Valid()
						require.NoError(t, err)
						if !ok {
							break
						}
						if it.UnsafeKey().Timestamp.Less(ttlThreshold) {
							_, tombs, err := it.MVCCValueLenAndIsTombstone()
							require.NoError(t, err, "failed to get iterator value")
							if !tombs {
								require.NoError(t,
									storage.MVCCPut(ctx, eng, &ms, it.UnsafeKey().Key, base.Add(0, 1),
										hlc.ClockTimestamp{}, roachpb.Value{}, nil))
							}
						}
						it.NextKey()
					}
				}

				lastGC := hlc.Timestamp{}

				bv := map[bool]int{
					false: 0,
					true:  1,
				}

				now := hlc.Timestamp{WallTime: 1}

				// Prime all event triggers
				for i := range spec.events {
					spec.events[i].triggerAt = now.Add(spec.events[i].interval.Nanoseconds(), 0)
				}

				out, err := os.OpenFile(namePrefix+"-"+fullName+".tsv", os.O_WRONLY|os.O_CREATE|os.O_TRUNC,
					0640)
				require.NoError(t, err, "can't open result file for writing")

				// Extra aggregate metrics not coming directly from stats but used for
				// measuring GC efficiency.
				var totalScanned int64
				var totalCollected int64

				params := iterationParams{
					// Write op is permanent state, others are reset every iteration.
					write:       true,
					// Initial values from spec
					rowTTL:      spec.rowLevelTTL,
					gcTTL:       spec.ttl,
				}

				eventHeaders := make([]string, len(spec.events))
				for i, e := range spec.events {
					eventHeaders[i] = e.name
				}
				const compactEach = 10
				compact := compactEach
				_, _ = fmt.Fprintf(out, "Time\tLive\tDead\tGCAge\tCollectable\tQueue\tPrio\tScanned\tCollected\t%s\n", strings.Join(eventHeaders, "\t"))
				for ; now.WallTime <= spec.runTime.Nanoseconds(); now = now.Add(spec.checkInterval.Nanoseconds(),
					0) {
					changes := false
					params.wipe = false
					ets := make([]int, len(spec.events))
					for i := range spec.events {
						if spec.events[i].count > 0 && spec.events[i].triggerAt.LessEq(now) {
							ets[i] = 1
							spec.events[i].action(&params, now)
							spec.events[i].triggerAt = spec.events[i].triggerAt.Add(spec.events[i].interval.Nanoseconds(), 0)
							spec.events[i].count--
						}
					}
					if params.wipe {
						wipeData(now, params.useRangeTombstones)
						changes = true
					}
					if params.rowTTL > 0 {
						rowLevelTTL(now, params.rowTTL)
						changes = true
					}
					if params.write {
						addData(keyDist, spec.delRate, now, spec.keysPerInterval, spec.checkInterval)
						changes = true
					}

					// GC data using batch.
					gcer := MakeTestGCer()
					gcThreshold := now.Add(-params.gcTTL.Nanoseconds(), 0)
					snap := eng.NewSnapshot()
					gcScanStopwatch.Start()
					_, err := gc.Run(ctx, &desc, snap, now, gcThreshold, gc.RunOptions{}, params.gcTTL, &gcer,
						gcer.resolveIntents, gcer.resolveIntentsAsync)
					gcScanStopwatch.Stop()
					snap.Close()
					require.NoError(t, err)

					// Cleanup in batch to evaluate garbage changes.
					b := eng.NewBatch()
					gcEvecStopwatch.Start()
					updMs := gcer.ExecuteGC(ctx, t, desc, b, ms)

					// Protected timestamp check
					canAdvanceGCThreshold := params.protectedTS.IsEmpty() || now.Add(-params.gcTTL.Nanoseconds(),
						0).LessEq(params.protectedTS)

					// Check score.
					s := makeMVCCGCQueueScoreImpl(ctx, 1, now, ms, params.gcTTL, lastGC, canAdvanceGCThreshold, roachpb.GCHint{},
						2*time.Hour)

					if s.ShouldQueue {
						totalScanned += ms.Total()
						totalCollected -= updMs.Total()
					}

					eVals := make([]string, len(spec.events))
					for i, et := range ets {
						eVals[i] = strconv.Itoa(et)
					}
					_, _ = fmt.Fprintf(out, "%d\t%d\t%d\t%d\t%d\t%d\t%f\t%d\t%d\t%s\n",
						now.WallTime/1e9, ms.LiveBytes, ms.GCBytes(), ms.GCBytesAge, -updMs.GCBytes(),
						bv[s.ShouldQueue], s.FinalScore, totalScanned, totalCollected, strings.Join(eVals, "\t"))

					// If instructed, commit pending GC changes.
					if s.ShouldQueue {
						require.NoError(t, b.Commit(true))
						ms.Add(updMs)
						changes = true
					}
					b.Close()

					// Its late here because we want to include commit time.
					gcEvecStopwatch.Stop()

					if changes {
						compact--
						if compact == 0 {
							compact = compactEach
							compactStopwatch.Start()
							_ = eng.Compact()
							compactStopwatch.Stop()
						}
					}
				}
				_ = out.Close()
				s, err := rditer.ComputeStatsForRange(&desc, eng, ms.LastUpdateNanos)
				require.NoError(t, err)
				require.EqualValues(t, s, ms, "computed vs maintained stats")

				fmt.Printf("%s: write=%s, delete=%s, row-ttl=%s, gc-scan=%s, gc-exec=%s, compact=%s\n",
					fullName, writeStopwatch.Elapsed(), deleteStopwatch.Elapsed(), rowTTLStopwatch.Elapsed(),
					gcScanStopwatch.Elapsed(), gcEvecStopwatch.Elapsed(), compactStopwatch.Elapsed())
			})
		}
	}
}

func TestDist(t *testing.T) {
	const maxKey = 100
	rng, _ := randutil.NewTestRand()
	keyDist := uniformDist(rng, 0, maxKey)
	var buckets [maxKey]int
	for i:=0; i<100000; i++ {
		buckets[keyDist()]++
	}
	for i, v := range buckets {
		fmt.Printf("%d : %d\n", i, v)
	}
}
