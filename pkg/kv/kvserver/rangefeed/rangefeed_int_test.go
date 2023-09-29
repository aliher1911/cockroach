// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	crangefeed "github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/container/list"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	securityassets.SetLoader(securitytest.EmbeddedAssets)
	serverutils.InitTestServerFactory(server.TestServerFactory)
	serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)
	os.Exit(m.Run())
}

func TestRangeFeeds(t *testing.T) {
	skip.UnderStress(t, "test uses test cluster and flakes under stress because of timeout checks")
	defer log.Scope(t).Close(t)

	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		e := newEnv(t)
		defer e.close()
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) (output string) {
			var out []string
			switch d.Cmd {
			case "put-keys":
				out = handlePutKeys(t, e, d)
			case "create-feed":
				out = handleCreateFeed(t, e, d)
			case "wait-feed":
				out = handleWaitFeed(t, e, d)
			case "dump-feed":
				out = handleDumpFeed(t, e, d)
			default:
				t.Fatalf("unknown command on %s: %s", d.Pos, d.Cmd)
			}
			var builder strings.Builder
			for _, s := range out {
				builder.WriteString(s)
				builder.WriteRune('\n')
			}
			return builder.String()
		})
	})
}

func handlePutKeys(t *testing.T, e *env, d *datadriven.TestData) []string {
	db := e.tc.SystemLayer(0).DB()
	readKvs(e, d.Input, func(key roachpb.Key, val string, tsName, txnName string) {
		require.NoError(t, db.Put(context.Background(), key, val), "failed to put value")
		// Might not work with txn
		if tsName != "" {
			kv, err := db.Get(context.Background(), key)
			require.NoError(t, err, "failed to read written value")
			e.tts.addTs(tsName, kv.Value.Timestamp)
		}
	}, func(key roachpb.Key, endKey roachpb.Key, tsName string) {
		require.Empty(t, tsName, "can't set/save timestamp on put")
		require.NoError(t, db.DelRangeUsingTombstone(context.Background(), key, endKey))
	})
	return []string{"ok"}
}

var keyRe = regexp.MustCompile(`^(\s*)key=(\w+?),\s*val=(\w+)(,\s*ts=(\w+?))?(,\s*txn=(\w+?))?$`)
var rangeRe = regexp.MustCompile(`^(\s*)key=(\w+?),\s*endKey=(\w+?)(,\s*ts=(\w+))?$`)

func readKvs(
	e *env,
	input string,
	kv func(key roachpb.Key, val string, tsName, txnName string),
	dr func(key roachpb.Key, endKey roachpb.Key, tsName string),
) {
	ls := strings.Split(input, "\n")
	var submatch []string
	match := func(l string, r *regexp.Regexp) bool {
		submatch = r.FindStringSubmatch(l)
		return submatch != nil
	}
	for _, l := range ls {
		switch {
		case match(l, keyRe):
			kk := e.startKey.key(submatch[2])
			val := submatch[3]
			tsName := submatch[5]
			txn := submatch[7]
			kv(kk, val, tsName, txn)
		case match(l, rangeRe):
			kk := e.startKey.key(submatch[2])
			ekk := e.startKey.key(submatch[3])
			tsName := submatch[5]
			dr(kk, ekk, tsName)
		default:
			panic(fmt.Sprintf("bad line: %s", l))
		}
	}
}

func handleCreateFeed(t *testing.T, e *env, d *datadriven.TestData) []string {
	var (
		server               int
		key, endKey, startTs string
		// If we don't set initial ts, then feed will start after request completes
		// and we will skip first events. We can always explicitly set time to 0
		// from the test itself if we need to test that.
		fo = feedOpts{initialTs: hlc.Timestamp{WallTime: 1}}
	)
	d.MaybeScanArgs(t, "server", &server)
	d.MaybeScanArgs(t, "startKey", &key)
	d.MaybeScanArgs(t, "endKey", &endKey)
	d.MaybeScanArgs(t, "startTs", &startTs)
	d.MaybeScanArgs(t, "withDiff", &fo.withDiff)
	k := e.startKey.key(key)
	ek := roachpb.Key(e.startKey).PrefixEnd()
	if len(endKey) > 0 {
		ek = e.startKey.key(endKey)
	}
	if len(startTs) > 0 {
		var ok bool
		fo.initialTs, ok = e.tts.getTimestamp(startTs)
		if !ok {
			if wall, err := strconv.Atoi(startTs); err == nil {
				fo.initialTs = hlc.Timestamp{WallTime: int64(wall)}
			} else {
				t.Fatalf("not found named timestamp and can't parse as wall time '%s'", startTs)
			}
		}
	}
	rf, cd := createTestFeed(t, context.Background(), e.tc.SystemLayer(server),
		e.tc.Server(0).Stopper(),	roachpb.Span{Key: k, EndKey: ek}, fo)
	e.feeds = append(e.feeds, rf)
	e.captures = append(e.captures, cd)
	return []string{"ok"}
}

func createTestFeed(
	t *testing.T,
	ctx context.Context,
	s serverutils.ApplicationLayerInterface,
	stopper *stop.Stopper,
	span roachpb.Span,
	o feedOpts,
) (*crangefeed.RangeFeed, *feedData) {

	fd := &feedData{
		t:           t,
		span:        span,
		firstCP:     make(chan interface{}),
		failedC:     make(chan interface{}),
	}

	rff, err := crangefeed.NewFactory(stopper, s.DB(), s.ClusterSettings(), nil)
	require.NoError(t, err, "failed to create client rangefeed factory")

	opts := []crangefeed.Option{
		crangefeed.WithOnCheckpoint(fd.onCheckpoint),
		crangefeed.WithDiff(o.withDiff),
		crangefeed.WithOnInternalError(fd.onInternalError),
	}
	rf, err := rff.RangeFeed(ctx, "nice", []roachpb.Span{span}, o.initialTs, fd.onValue, opts...)
	require.NoError(t, err, "failed to start rangefeed")
	return rf, fd
}

type feedOpts struct {
	withDiff        bool
	initialTs       hlc.Timestamp
}

func handleWaitFeed(t *testing.T, e *env, d *datadriven.TestData) []string {
	var (
		feedIndex                    int
		startKey, endKey, timeoutStr string
	)
	d.MaybeScanArgs(t, "id", &feedIndex)
	d.MaybeScanArgs(t, "startKey", &startKey)
	d.MaybeScanArgs(t, "endKey", &endKey)
	d.MaybeScanArgs(t, "timeout", &timeoutStr)
	timeout := 30 * time.Second
	if timeoutStr != "" {
		var err error
		timeout, err = time.ParseDuration(timeoutStr)
		require.NoError(t, err, "invalid duration value '%s'", timeoutStr)
	}
	now := e.tc.SystemLayer(0).Clock().Now()
	var startSpan, endSpan roachpb.Key
	if len(startKey) > 0 {
		startSpan = e.startKey.key(startKey)
	}
	if len(endKey) > 0 {
		endSpan = e.startKey.key(endKey)
	}
	c := e.captures[feedIndex].waitCheckpoint(now, roachpb.Span{Key: startSpan, EndKey: endSpan})
	select {
	case <-c:
	case <-time.After(timeout):
		return []string{"timeout"}
	}
	return []string{"ok"}
}

func handleDumpFeed(t *testing.T, e *env, d *datadriven.TestData) []string {
	var feedIndex int
	var byTimestamp bool
	d.MaybeScanArgs(t, "id", &feedIndex)
	d.MaybeScanArgs(t, "byTs", &byTimestamp)
	e.feeds[feedIndex].Close()
	stream := e.captures[feedIndex].values().asSortedData(byTimestamp)
	return dumpKVS(stream, "", e.tts, e.startKey)
}

type env struct {
	// test cluster
	tc *testcluster.TestCluster

	// range feeds
	feeds    []*crangefeed.RangeFeed
	captures []*feedData

	// named timestamps
	tts timestamps

	// work keys
	startKey testKey
}

func newEnv(t *testing.T) *env {
	tc := testcluster.NewTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					// Disable rangefeed intent pushing as it may interfere with tests
					// asserting intents.
					RangeFeedPushTxnsAge:      60 * time.Hour,
					RangeFeedPushTxnsInterval: 60 * time.Hour,
				},
			},
		},
	})
	tc.Start(t)

	// Lower the closed timestamp target duration to speed up the test.
	db := tc.SystemLayer(0).SQLConn(t, "")
	_, err := db.Exec("SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'")
	require.NoError(t, err)

	sr := append(tc.SystemLayer(0).Codec().TenantPrefix(), keys.ScratchRangeMin...)
			tc.ScratchRange(t)
	return &env{
		tc: tc,
		tts: timestamps{
			nameToTs: make(map[string]hlc.Timestamp),
			tsToName: make(map[hlc.Timestamp]string),
		},
		startKey: testKey(sr),
	}
}

func (e *env) close() {
	for _, f := range e.feeds {
		f.Close()
	}
	e.tc.Stopper().Stop(context.Background())
}

type timestamps struct {
	nameToTs map[string]hlc.Timestamp
	tsToName map[hlc.Timestamp]string
}

func (t timestamps) addTs(name string, ts hlc.Timestamp) (hlc.Timestamp, bool) {
	// If name is used, return previous ts value to use instead.
	if cts, ok := t.nameToTs[name]; ok {
		return cts, cts.Equal(ts)
	}
	// If same timestamp is registered with different name, increment logical.
	unchanged := true
	if _, ok := t.tsToName[ts]; ok {
		ts.Logical += 1
		unchanged = false
	}

	t.nameToTs[name] = ts
	t.tsToName[ts] = name
	return ts, unchanged
}

func (t timestamps) getTsName(ts hlc.Timestamp) (string, bool) {
	if n, ok := t.tsToName[ts]; ok {
		return n, true
	}
	return "?", false
}

func (t timestamps) getTimestamp(name string) (hlc.Timestamp, bool) {
	ts, ok := t.nameToTs[name]
	return ts, ok
}

type testKey roachpb.Key

func (p testKey) print(key roachpb.Key) string {
	if len(key) > len(p) && key[:len(p)].Equal(roachpb.Key(p)) {
		if key[len(key)-1] == 0 {
			// This is generated end interval from some key, turn it into key+ to
			// hint that this is past real key.
			return string(key[len(p):len(key)-1]) + "+"
		}
		return string(key[len(p):])
	}
	return key.String()
}

func (p testKey) key(k string) roachpb.Key {
	return roachpb.Key(append(p[:len(k):len(k)], k...))
}

type feedData struct {
	t      *testing.T
	span   roachpb.Span
	dataMu struct {
		syncutil.Mutex
		values     eventStream
		failure    error
		failureSet bool
	}
	failedC chan interface{}

	signalCP sync.Once
	firstCP  chan interface{}

	cpMu struct {
		syncutil.Mutex
		waiters []waitCP
	}
}

func (d *feedData) onValue(_ context.Context, v *kvpb.RangeFeedValue) {
	d.dataMu.Lock()
	defer d.dataMu.Unlock()
	d.dataMu.values = append(d.dataMu.values, testFeedEvent{v: v})
	d.t.Logf("on Value: %s/%s", v.Key.String(), v.Value.String())
}

func (d *feedData) onCheckpoint(_ context.Context, cp *kvpb.RangeFeedCheckpoint) {
	d.dataMu.Lock()
	d.dataMu.values = append(d.dataMu.values, testFeedEvent{cp: cp})
	defer d.dataMu.Unlock()
	d.signalCP.Do(func() {
		close(d.firstCP)
	})
	d.t.Logf("on Checkpoint: [%s, %s) %s", cp.Span.Key.String(), cp.Span.EndKey.String(),
		cp.ResolvedTS.String())
	d.cpMu.Lock()
	defer d.cpMu.Unlock()
	for i, w := range d.cpMu.waiters {
		if !w.targetTS.IsEmpty() {
			if w.addCP(cp.ResolvedTS, cp.Span) {
				close(w.timeReached)
				d.cpMu.waiters[i] = waitCP{}
			}
		}
	}
}

func (d *feedData) onInternalError(_ context.Context, err error) {
	d.dataMu.Lock()
	defer d.dataMu.Unlock()
	d.dataMu.failure = err
	close(d.failedC)
	d.t.Logf("on internal error: %s", err)
}

// NB: frontier update happens after checkpoint update so there's no guarantee
// that frontier is up to date at the time returned channel is closed.
func (d *feedData) waitCheckpoint(ts hlc.Timestamp, span roachpb.Span) <-chan interface{} {
	d.cpMu.Lock()
	defer d.cpMu.Unlock()
	if span.Key == nil {
		span.Key = d.span.Key
	}
	if span.EndKey == nil {
		span.EndKey = d.span.EndKey
	}
	w := waitCP{
		timeReached: make(chan interface{}),
		targetTS:    ts,
		span:        span,
	}
	for i, oldW := range d.cpMu.waiters {
		if oldW.targetTS.IsEmpty() {
			d.cpMu.waiters[i] = w
			return w.timeReached
		}
	}
	d.cpMu.waiters = append(d.cpMu.waiters, w)
	return w.timeReached
}

func (d *feedData) waitError() <-chan interface{} {
	return d.failedC
}

func (d *feedData) err() error {
	d.dataMu.Lock()
	defer d.dataMu.Unlock()
	return d.dataMu.failure
}

func (d *feedData) values() eventStream {
	d.dataMu.Lock()
	defer d.dataMu.Unlock()
	return d.dataMu.values
}

type testFeedEvent struct {
	v        *kvpb.RangeFeedValue
	cp       *kvpb.RangeFeedCheckpoint
}

type eventStream []testFeedEvent

// asSortedData produces sequence of values ordered by key and timestamp.
// Consecutive equal entries are removed. For entries to be equal they must have
// the same type and both mvcc keys and values equal. For sst's metadata and
// byte contents must be equal.
// Produced slice could be used by dumpKVS function to create a human-readable
// representation.
func (s eventStream) asSortedData(byTimestamp bool) kvs {
	var data []sorted
	for _, e := range s {
		switch {
		case e.v != nil && !e.v.PrevValue.IsPresent():
			data = append(data, sorted{
				MVCCRangeKey: storage.MVCCRangeKey{
					StartKey:  e.v.Key,
					Timestamp: e.v.Timestamp(),
				},
				val: storage.MVCCKeyValue{
					Key: storage.MVCCKey{
						Key:       e.v.Key,
						Timestamp: e.v.Timestamp(),
					},
					Value: e.v.Value.RawBytes,
				},
			})
		case e.v != nil && e.v.PrevValue.IsPresent():
			data = append(data, sorted{
				MVCCRangeKey: storage.MVCCRangeKey{
					StartKey:  e.v.Key,
					Timestamp: e.v.Timestamp(),
				},
				val: *e.v,
			})
		}
	}
	if byTimestamp {
		sort.Sort(sortedByTime(data))
	} else {
		sort.Sort(sortedByKey(data))
	}
	result := make(kvs, 0, len(data))
	var prev sorted
	for _, s := range data {
		if s.equals(prev) {
			continue
		}
		result = append(result, s.val)
		prev = s
	}
	return result
}

// sorted is a helper type that allows test to present data in different order.
type sorted struct {
	storage.MVCCRangeKey
	val interface{}
}

func (s sorted) equals(o sorted) bool {
	switch v := s.val.(type) {
	case storage.MVCCKeyValue:
		if ov, ok := o.val.(storage.MVCCKeyValue); ok {
			return v.Key.Equal(ov.Key) && bytes.Equal(v.Value, ov.Value)
		}
		return false
	case storage.MVCCRangeKeyValue:
		if ov, ok := o.val.(storage.MVCCRangeKeyValue); ok {
			return v.RangeKey.Compare(ov.RangeKey) == 0 && bytes.Equal(v.Value, ov.Value)
		}
		return false
	case kvpb.RangeFeedValue:
		if ov, ok := o.val.(kvpb.RangeFeedEvent); ok {
			return v.Key.Equal(ov.Val.Key) && v.Value.EqualTagAndData(ov.Val.Value) && v.PrevValue.EqualTagAndData(ov.Val.PrevValue)
		}
		return false
	default:
		panic(fmt.Sprintf("unknown data event type %s", s))
	}
}

type sortedByKey []sorted
var _ sort.Interface = (sortedByKey)(nil)
func (s sortedByKey) Len() int { return len(s) }
func (s sortedByKey) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s sortedByKey) Less(i, j int) bool {
	o1, o2 := s[i], s[j]
	if c := o1.StartKey.Compare(o2.StartKey); c != 0 {
		return c < 0
	}
	if o1.Timestamp.IsEmpty() && !o2.Timestamp.IsEmpty() {
		return true
	} else if !o1.Timestamp.IsEmpty() && o2.Timestamp.IsEmpty() {
		return false
	} else if c := o1.Timestamp.Compare(o2.Timestamp); c != 0 {
		return c < 0
	}
	return o1.EndKey.Compare(o2.EndKey) < 0
}

type sortedByTime []sorted
var _ sort.Interface = (sortedByTime)(nil)
func (s sortedByTime) Len() int { return len(s) }
func (s sortedByTime) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s sortedByTime) Less(i, j int) bool {
	o1, o2 := s[i], s[j]
	if o1.Timestamp.IsEmpty() && !o2.Timestamp.IsEmpty() {
		return true
	} else if !o1.Timestamp.IsEmpty() && o2.Timestamp.IsEmpty() {
		return false
	} else if c := o1.Timestamp.Compare(o2.Timestamp); c != 0 {
		return c < 0
	}
	if c := o1.StartKey.Compare(o2.StartKey); c != 0 {
		return c < 0
	}
	return o1.EndKey.Compare(o2.EndKey) < 0
}

// kvs is any data that could be written into test output for diffing.
type kvs = []interface{}

// dumpKVS produced human-readable dump of provided slice of data items.
func dumpKVS(data kvs, indent string, tts timestamps, kk testKey) []string {
	var ss []string
	for _, v := range data {
		switch kv := v.(type) {
		case storage.MVCCKeyValue:
			if tsn, exists := tts.getTsName(kv.Key.Timestamp); exists {
				ss = append(ss, fmt.Sprintf("%skey=%s, val=%s, ts=%s", indent, kk.print(kv.Key.Key),
					stringValue(kv.Value), tsn))
			} else {
				ss = append(ss,
					fmt.Sprintf("%skey=%s, val=%s", indent, kk.print(kv.Key.Key), stringValue(kv.Value)))
			}
		case kvpb.RangeFeedValue:
			if tsn, exists := tts.getTsName(kv.Value.Timestamp); exists {
				ss = append(ss, fmt.Sprintf("%skey=%s, val=%s, ts=%s, prev=%s", indent, kk.print(kv.Key),
					stringValue(kv.Value.RawBytes), tsn, stringValue(kv.PrevValue.RawBytes)))
			} else {
				ss = append(ss, fmt.Sprintf("%skey=%s, val=%s, prev=%s", indent, kk.print(kv.Key),
					stringValue(kv.Value.RawBytes), stringValue(kv.PrevValue.RawBytes)))
			}
		case storage.MVCCRangeKeyValue:
			ss = append(ss, fmt.Sprintf("%skey=%s, endKey=%s", indent, kk.print(kv.RangeKey.StartKey),
				kk.print(kv.RangeKey.EndKey)))
		default:
			panic(fmt.Sprintf("unknown data element in dump: %+q", v))
		}
	}
	return ss
}

func stringValue(data []byte) string {
	val, err := storage.DecodeMVCCValue(data)
	if err != nil {
		return fmt.Sprintf("%q", err)
	}
	b, err := val.Value.GetBytes()
	if err != nil {
		return fmt.Sprintf("%q", err)
	}
	return string(b)
}

type waitCP struct {
	timeReached chan interface{}
	targetTS    hlc.Timestamp
	span        roachpb.Span
	received    list.List[roachpb.Span]
}

// add span to cp. if all desired span is covered close channel and return true
func (w *waitCP) addCP(ts hlc.Timestamp, newSpan roachpb.Span) bool {
	// Disregard timestamps that are too early.
	if ts.Less(w.targetTS) {
		return false
	}
	// Trim span to interesting part only.
	newSpan = newSpan.Intersect(w.span)
	if !newSpan.Valid() {
		return false
	}

	e := w.received.Front()
	for e != nil {
		span := e.Value
		if span.EqualValue(newSpan) {
			// We already saw this span after desired timestamp.
			return false
		}
		// Overlapping or adjacent spans merge into larger span superseding new one and removing
		// previous.
		if span.Overlaps(newSpan) || span.Key.Equal(newSpan.EndKey) || span.EndKey.Equal(newSpan.Key) {
			// Merge spans
			newSpan = span.Combine(newSpan)
			pe := e
			e = e.Next()
			w.received.Remove(pe)
			continue
		}
		// If new span is before existing, insert it and finish.
		if newSpan.Key.Compare(span.EndKey) < 0 {
			w.received.InsertBefore(newSpan, e)
			break
		}
		e = e.Next()
	}
	// If no more elements after, then push back to the list after all other
	// ranges. This includes the empty list case.
	if e == nil {
		w.received.PushBack(newSpan)
	}

	// Simple check that keys are chained and that end key is equal to span end.
	prev := w.span.Key
	for e := w.received.Front(); e != nil; e = e.Next() {
		span := e.Value
		if !prev.Equal(span.Key) {
			return false
		}
		if span.EndKey.Equal(w.span.EndKey) {
			return true
		}
	}
	return false
}
