// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

import (
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"math/rand"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/stretchr/testify/require"
)

// TestRangefeedWorksOnSystemRangesUnconditionally ensures that a rangefeed will
// not return an error when operating on a system span even if the setting is
// disabled. The test also ensures that an error is received if a rangefeed is
// run on a user table.
func TestRangefeedWorksOnSystemRangesUnconditionally(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	// Make sure the rangefeed setting really is disabled.
	_, err := tc.ServerConn(0).Exec("SET CLUSTER SETTING kv.rangefeed.enabled = false")
	require.NoError(t, err)

	db := tc.Server(0).DB()
	ds := tc.Server(0).DistSenderI().(*kvcoord.DistSender)

	t.Run("works on system ranges", func(t *testing.T) {
		startTS := db.Clock().Now()
		descTableKey := keys.SystemSQLCodec.TablePrefix(keys.DescriptorTableID)
		descTableSpan := roachpb.Span{
			Key:    descTableKey,
			EndKey: descTableKey.PrefixEnd(),
		}

		evChan := make(chan *roachpb.RangeFeedEvent)
		rangefeedErrChan := make(chan error, 1)
		ctxToCancel, cancel := context.WithCancel(ctx)
		go func() {
			rangefeedErrChan <- ds.RangeFeed(ctxToCancel, descTableSpan, startTS, false /* withDiff */, evChan)
		}()

		// Note: 42 is a system descriptor.
		const junkDescriptorID = 42
		require.GreaterOrEqual(t, keys.MaxReservedDescID, junkDescriptorID)
		junkDescriptorKey := catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, junkDescriptorID)
		junkDescriptor := dbdesc.NewInitial(
			junkDescriptorID, "junk", security.AdminRoleName())
		require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			if err := txn.SetSystemConfigTrigger(true /* forSystemTenant */); err != nil {
				return err
			}
			return txn.Put(ctx, junkDescriptorKey, junkDescriptor.DescriptorProto())
		}))
		after := db.Clock().Now()
		for {
			ev := <-evChan
			if ev.Checkpoint != nil && after.Less(ev.Checkpoint.ResolvedTS) {
				t.Fatal("expected to see write which occurred before the checkpoint")
			}

			if ev.Val != nil && ev.Val.Key.Equal(junkDescriptorKey) {
				var gotProto descpb.Descriptor
				require.NoError(t, ev.Val.Value.GetProto(&gotProto))
				require.EqualValues(t, junkDescriptor.DescriptorProto(), &gotProto)
				break
			}
		}
		cancel()
		// There are several cases that seems like they can happen due
		// to closed connections. Instead we just expect an error.
		// The main point is we get an error in a timely manner.
		require.Error(t, <-rangefeedErrChan)
	})
	t.Run("does not work on user ranges", func(t *testing.T) {
		k := tc.ScratchRange(t)
		require.NoError(t, tc.WaitForSplitAndInitialization(k))
		startTS := db.Clock().Now()
		scratchSpan := roachpb.Span{Key: k, EndKey: k.PrefixEnd()}
		evChan := make(chan *roachpb.RangeFeedEvent)
		require.Regexp(t, `rangefeeds require the kv\.rangefeed.enabled setting`,
			ds.RangeFeed(ctx, scratchSpan, startTS, false /* withDiff */, evChan))
	})
}

// TestMergeOfRangeEventTableWhileRunningRangefeed ensures that it is safe
// for a range merge transaction which has laid down intents on the RHS of the
// merge commit while a rangefeed is running on the RHS. At the time of writing,
// the only such range that this can happen to is the RangeEventTable.
func TestMergeOfRangeEventTableWhileRunningRangefeed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		// Using ReplicationManual will disable the merge queue.
		ReplicationMode: base.ReplicationManual,
	})
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)

	// Set a short closed timestamp interval so that we don't need to wait long
	// for resolved events off of the rangefeed later.
	_, err := tc.ServerConn(0).Exec(
		"SET CLUSTER SETTING kv.closed_timestamp.target_duration = '50ms'")
	require.NoError(t, err)

	// Find the range containing the range event table and then find the range
	// to its left.
	rangeEventTableStart := keys.SystemSQLCodec.TablePrefix(keys.RangeEventTableID)
	require.NoError(t, tc.WaitForSplitAndInitialization(rangeEventTableStart))
	store, _ := getFirstStoreReplica(t, tc.Server(0), rangeEventTableStart)
	var lhsRepl *kvserver.Replica
	store.VisitReplicas(func(repl *kvserver.Replica) (wantMore bool) {
		if repl.Desc().EndKey.AsRawKey().Equal(rangeEventTableStart) {
			lhsRepl = repl
			return false
		}
		return true
	})
	require.NotNil(t, lhsRepl)

	// Set up a rangefeed for the lhs.
	db := tc.Server(0).DB()
	ds := tc.Server(0).DistSenderI().(*kvcoord.DistSender)

	rangefeedCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	rangefeedErrChan := make(chan error, 1)
	// Make the buffer large so we don't risk blocking.
	eventCh := make(chan *roachpb.RangeFeedEvent, 1000)
	start := db.Clock().Now()
	go func() {
		rangefeedErrChan <- ds.RangeFeed(rangefeedCtx,
			lhsRepl.Desc().RSpan().AsRawSpanWithNoLocals(),
			start,
			false, /* withDiff */
			eventCh)
	}()

	// Wait for an event on the rangefeed to let us know that we're connected.
	<-eventCh

	// Merge the range event table range with its lhs neighbor.
	require.NoError(t, db.AdminMerge(ctx, lhsRepl.Desc().StartKey.AsRawKey()))

	// Ensure that we get a checkpoint after the merge.
	afterMerge := db.Clock().Now()
	for ev := range eventCh {
		if ev.Checkpoint == nil {
			continue
		}
		if afterMerge.Less(ev.Checkpoint.ResolvedTS) {
			break
		}
	}

	// Cancel the rangefeed and ensure we get the right error.
	cancel()
	require.Regexp(t, context.Canceled.Error(), <-rangefeedErrChan)
}

func scratchKey(key string) roachpb.Key {
	return testutils.MakeKey(keys.TableDataMax, []byte(key))
}

func TestRangefeedIsRoutedToNonVoter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	clusterArgs := aggressiveResolvedTimestampClusterArgs
	// We want to manually add a non-voter to a range in this test, so disable
	// the replicateQueue to prevent it from disrupting the test.
	clusterArgs.ReplicationMode = base.ReplicationManual
	// NB: setupClusterForClosedTSTesting sets a low closed timestamp target
	// duration.
	tc, _, desc := setupClusterForClosedTSTesting(ctx, t, testingTargetDuration,
		testingCloseFraction, clusterArgs, "cttest", "kv")
	defer tc.Stopper().Stop(ctx)
	tc.AddNonVotersOrFatal(t, desc.StartKey.AsRawKey(), tc.Target(1))

	db := tc.Server(1).DB()
	ds := tc.Server(1).DistSenderI().(*kvcoord.DistSender)
	_, err := tc.ServerConn(1).Exec("SET CLUSTER SETTING kv.rangefeed.enabled = true")
	require.NoError(t, err)

	startTS := db.Clock().Now()
	rangefeedCtx, rangefeedCancel := context.WithCancel(ctx)
	rangefeedCtx, getRec, cancel := tracing.ContextWithRecordingSpan(rangefeedCtx,
		tracing.NewTracer(),
		"rangefeed over non-voter")
	defer cancel()

	// Do a read on the range to make sure that the dist sender learns about the
	// latest state of the range (with the new non-voter).
	_, err = db.Get(ctx, desc.StartKey.AsRawKey())
	require.NoError(t, err)

	rangefeedErrChan := make(chan error, 1)
	eventCh := make(chan *roachpb.RangeFeedEvent, 1000)
	go func() {
		rangefeedErrChan <- ds.RangeFeed(
			rangefeedCtx,
			desc.RSpan().AsRawSpanWithNoLocals(),
			startTS,
			false, /* withDiff */
			eventCh,
		)
	}()

	// Wait for an event to ensure that the rangefeed is set up.
	select {
	case <-eventCh:
	case err := <-rangefeedErrChan:
		t.Fatalf("rangefeed failed with %s", err)
	case <-time.After(60 * time.Second):
		t.Fatalf("rangefeed initialization took too long")
	}
	rangefeedCancel()
	require.Regexp(t, "context canceled", <-rangefeedErrChan)
	require.Regexp(t, "attempting to create a RangeFeed over replica.*2NON_VOTER", getRec().String())
}

func TestRangefeedStuck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	rng := rand.New(rand.NewSource(823748192734))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc := testcluster.StartTestCluster(t, 5, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	db := tc.Server(0).DB()
	ds := tc.Server(0).DistSenderI().(*kvcoord.DistSender)
	_, err := tc.ServerConn(0).Exec("SET CLUSTER SETTING kv.rangefeed.enabled = true")
	require.NoError(t, err)

	tc.ScratchRange(t)

	// Start rangefeed goroutine.
	errCh := make(chan error, 2)
	eventCh := make(chan *roachpb.RangeFeedEvent, 1000)
	go func() {
		errCh <- ds.RangeFeed(
			ctx,
			roachpb.Span{Key: scratchKey("a"), EndKey: scratchKey("z")},
			db.Clock().Now(),
			false, /* withDiff */
			eventCh,
		)
	}()

	var receivedValues int32 = 0
	var sentValues int32 = 0

	// Start consumer goroutine, which filters checkpoint events and passes
	// resolved timestamps through resolvedCh.
	resolvedCh := make(chan hlc.Timestamp, 100)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case event := <-eventCh:
				switch {
				case event.Checkpoint != nil:
					resolvedCh <- event.Checkpoint.ResolvedTS
				case event.Error != nil:
					errCh <- event.Error.Error.GoError()
				case event.Val != nil:
					currentReceived := atomic.AddInt32(&receivedValues, 1)
					if currentReceived%5000 == 0 {
						currentSent := atomic.LoadInt32(&sentValues)
						t.Logf("received values %d, send values %d, current value: %v", currentReceived, currentSent, event.Val)
					}
				}
			}
		}
	}()

	randRange := func() []byte {
		first := byte(randutil.RandIntInRange(rng, 'a', 'z'))
		return testutils.MakeKey(keys.TableDataMax, []byte{first})
	}

	randKey := func() []byte {
		return testutils.MakeKey(randRange(), randutil.RandBytes(rng, 5))
	}

	// Start writer go routine that would feed data into change feed
	go func() {
		throttle := time.NewTicker(time.Millisecond * 10)
		defer throttle.Stop()
		var lastSent int32 = 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-throttle.C:
				// send data
				if err := db.Put(ctx, randKey(), lastSent); err != nil {
					t.Logf("Failed to write value %v", err)
					continue
				}
				lastSent = atomic.AddInt32(&sentValues, 1)
			}
		}
	}()

	doSplit := func(rangeKey roachpb.Key) {
		if err := db.AdminSplit(ctx, rangeKey, hlc.Timestamp{}); err != nil {
			t.Logf("Failed to split range at %v", err)
		}
		t.Logf("Split on %v", rangeKey)
	}

	doTransfer := func(rangeKey roachpb.Key) bool {
		// transfer lease and remove original leaseholder
		rd, err := tc.LookupRange(rangeKey)
		if err != nil {
			t.Logf("Failed to lookup range %v for replica changes: %v", rangeKey, err)
			return false
		}
		lease, _, err := tc.FindRangeLeaseEx(ctx, rd, nil)
		if err != nil {
			t.Logf("Failed to find lease on range %v for replica changes: %v", rangeKey, err)
			return false
		}
		replicaSubset := rd.Replicas().FilterToDescriptors(func(rDesc roachpb.ReplicaDescriptor) bool {
			return rDesc.NodeID != lease.Current().Replica.NodeID
		})
		if len(replicaSubset) == 0 {
			t.Logf("Range %v has only single replica.", rangeKey)
			return false
		}
		newLeasholderDescriptor := replicaSubset[rng.Intn(len(replicaSubset))]
		err = tc.TransferRangeLease(rd,
			roachpb.ReplicationTarget{NodeID: newLeasholderDescriptor.NodeID, StoreID: newLeasholderDescriptor.StoreID})
		if err != nil {
			t.Logf("Failed to transfer lease for %v: %v", rangeKey, err)
			return false
		}
		r := lease.Current().Replica
		_, err = tc.RemoveVoters(rangeKey, roachpb.ReplicationTarget{NodeID: r.NodeID, StoreID: r.StoreID})
		if err != nil {
			t.Logf("Failed to remove ex leaseholder replica on n%v from range %v: %v", r.NodeID, rd, err)
			return false
		}
		t.Logf("Transferred lease on %v from n%v to n%v", rd, r.NodeID, newLeasholderDescriptor.NodeID)
		return true
	}

	doMagic := func() {
		// Pick a range by key
		initial := randKey()
		// create several splits
		rd, err := tc.LookupRange(initial)
		if err != nil {
			t.Logf("Failed to get range descriptor for %v", initial)
			return
		}
		var rangeToMove roachpb.Key
		for i := 0; i < 3; i++ {
			middle := midKey(roachpb.Key(rd.StartKey), roachpb.Key(rd.EndKey))
			_, rd, err = tc.SplitRange(middle)
			if err != nil {
				t.Logf("Failed to get range descriptor for %v on iteration %d at %v", rd, i, middle)
				return
			}
			if i == 1 {
				rangeToMove = roachpb.Key(rd.StartKey)
			}
			t.Logf("Split range %v at %v", rd, middle)
		}
		// move leaseholder for intermediate node? is it even possible at this point
		if doTransfer(rangeToMove) {
			t.Logf("Magic complete")
		}
	}

	// Start split-merge-move routine.
	go func() {
		throttle := time.NewTicker(time.Millisecond * 500)
		defer throttle.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Millisecond * time.Duration(randutil.RandIntInRange(rng, 10, 1000))):
				rangeKey := randKey()
				switch rng.Intn(3) {
				case 0:
					doSplit(rangeKey)
				case 1:
					doMagic()

					//doSplit(rangeKey)
					//doTransfer(rangeKey)
					//t.Logf("Split + transfer complete")
					// this is potentially useless because ranges are not co-located
					//t.Logf("Trying to merge on %v", rangeKey)
					//if err := db.AdminMerge(ctx, rangeKey); err != nil {
					//  t.Logf("Failed to merge range %v", err)
					//}
				case 2:
					doTransfer(rangeKey)
				}
			}
		}
	}()

	// Start timestamp monitoring
	testLimit := time.After(10 * time.Minute)
	var ts hlc.Timestamp
	for {
		select {
		case ts = <-resolvedCh:
			//t.Logf("Frontier is behind by %d", hlc.UnixNano() - ts.WallTime)
		case err := <-errCh:
			require.Fail(t, "rangefeed failed", "err: %v", err)
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Second):
			t.Logf("timed out waiting for resolved timestamp, last: %v", ts)
			fmt.Printf("%s\n", stacks())
			return
		case <-testLimit:
			return
		}
	}

}

// stacks is a wrapper for runtime.Stack that attempts to recover the data for all goroutines.
func stacks() []byte {
	// We don't know how big the traces are, so grow a few times if they don't fit. Start large, though.
	var trace []byte
	for n := 1 << 20; /* 1mb */ n <= (1 << 29); /* 512mb */ n *= 2 {
		trace = make([]byte, n)
		nbytes := runtime.Stack(trace, true /* all */)
		if nbytes < len(trace) {
			return trace[:nbytes]
		}
	}
	return trace
}

func midKey(start, end roachpb.Key) roachpb.Key {
	longestKey := len(start)
	if longestKey < len(end) {
		longestKey = len(end)
	}
	newkey := make([]byte, longestKey)
	overflow := 0
	for i := 0; i < longestKey; i++ {
		left := 0
		right := 0
		if i < len(start) {
			left = int(start[i])
		}
		if i < len(end) {
			right = int(end[i])
		}
		mid := (left + right) / 2  + overflow
		odd := (left + right) % 2
		newkey[i] = byte(mid)
		if odd != 0 {
			overflow = 127
		} else {
			overflow = 0
		}
	}
	if overflow != 0 {
		newkey = append(newkey, byte(overflow))
	}
	return newkey
}
