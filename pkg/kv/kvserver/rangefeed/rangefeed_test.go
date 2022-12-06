// Copyright 2022 The Cockroach Authors.
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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/errors"
	atomic2 "go.uber.org/atomic"

	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func init() {
	randutil.SeedForTests()
	//security.SetAssetLoader(securitytest.EmbeddedAssets)
	securityassets.SetLoader(securitytest.EmbeddedAssets)
	serverutils.InitTestServerFactory(server.TestServerFactory)
	serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)
}

type testStream struct {
	ctx       context.Context
	cancel    func()
	count     atomic2.Int64
	t         *testing.T
	delay     time.Duration
	failAfter int
}

func newTestStream(t *testing.T, delay time.Duration, failAfter int) *testStream {
	ctx, cancel := context.WithCancel(context.Background())
	return &testStream{ctx: ctx, cancel: cancel, t: t, delay: delay, failAfter: failAfter}
}

func (t *testStream) Send(event *roachpb.RangeFeedEvent) error {
	if cnt := t.count.Add(1); cnt % 100 == 0 {
		t.t.Logf("sent events %d", cnt)
	}
	if event.Error != nil {
		t.t.Logf("rangefeed failed after %d events with: %s", t.count.Load(),
			event.Error.Error.GoError().Error())
	} else {
		<-time.After(t.delay)
	}
	if t.failAfter == 1 {
		t.failAfter = 0
		return errors.New("failing upon request")
	}
	if t.failAfter > 0 {
		t.failAfter--
	}
	return nil
}

func (t *testStream) SetHeader(md metadata.MD) error {
	return nil
}

func (t *testStream) SendHeader(md metadata.MD) error {
	return nil
}

func (t *testStream) SetTrailer(md metadata.MD) {
}

func (t *testStream) Context() context.Context {
	return t.ctx
}

func (t *testStream) SendMsg(m interface{}) error {
	return nil
}

func (t *testStream) RecvMsg(m interface{}) error {
	return nil
}

func TestBudgetOverflow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	args := base.TestServerArgs{}
	ts, err := serverutils.NewServer(args)
	require.NoError(t, err)
	require.NoError(t, ts.Start(ctx))
	srv := ts.(*server.TestServer)
	defer srv.Stopper().Stop(context.Background())
	n := ts.Node().(*server.Node)
	sr, _ := ts.ScratchRange()
	lr, err := n.RangeLookup(ctx, &roachpb.RangeLookupRequest{
		Key:             roachpb.RKey(sr),
		ReadConsistency: 0,
		PrefetchNum:     0,
		PrefetchReverse: false,
	})
	_ = lr
	r := lr.Descriptors[0]
	req := roachpb.RangeFeedRequest{
		Header: roachpb.Header{
			RangeID: r.RangeID,
			Replica: r.InternalReplicas[0],
		},
		Span: roachpb.Span{
			Key:    r.StartKey.AsRawKey(),
			EndKey: r.EndKey.AsRawKey(),
		},
		WithDiff:        false,
		AdmissionHeader: roachpb.AdmissionHeader{},
		//StreamID:        1000,
	}

	//d := 1*time.Microsecond
	d := 10*time.Millisecond

	doneC := make(chan struct{})
	go func() {
		str := newTestStream(t, d, 20)
		err := n.RangeFeed(&req, str)
		if err != nil {
			t.Log("Range feed terminated with error: ", err)
		}
		t.Log("Range feed terminated")
		close(doneC)
	}()

	const batchSize = 500
	kvdb := ts.DB()
	closeC := make(chan struct{})
	put := func(id byte) {
		var count int
		k := append(sr[0:len(sr):len(sr)], id, 0, 0)
		kpos := len(k) - 2
		for {
			if err := kvdb.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				for i:=0;i<batchSize;i++{
					k[kpos] = byte(count >> 8 & 0xff)
					k[kpos+1] = byte(count & 0xff)
					if err := txn.Put(ctx, k, fmt.Sprintf("%d,%d", id, count+i)); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Logf("%d: put failed after %d successful writes", id, count)
				return
			}
			if count % 1000 == 0 {
				t.Logf("%d: wrote events %d", id, count)
			}
			count += batchSize
			select {
			case <-closeC:
				return
			default:
			}
		}
	}

	go put(1)
	go put(2)
	go put(3)
	go put(4)
	go put(5)
	go put(6)
	go put(7)
	go put(8)
	go put(9)
	go put(10)

	select {
	case <- time.After(2*time.Second):
	//case <-doneC:
	//	t.Fatal("stream failed")
	}

	t.Log("trying split")
	_, _, err = ts.SplitRange(append(sr, "aaa"...))
	require.NoError(t, err, "failed to split range")
	<- time.After(1*time.Second)
	close(closeC)
	<- time.After(1*time.Second)
}
/*
func TestBudgetOverflowCluster(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	//rng, _ := randutil.NewTestRand()

	tc := testcluster.NewTestCluster(t, 3, base.TestClusterArgs{})
	tc.Start(t)
	defer tc.Stopper().Stop(context.Background())

	n := tc.ServerTyped(0).Node().(*server.Node)
	sr := tc.ScratchRange(t)
	r, err := tc.LookupRange(sr)
	require.NoError(t, err, "failed lookup")
	req := roachpb.RangeFeedRequest{
		Header: roachpb.Header{
			RangeID: r.RangeID,
			Replica: r.InternalReplicas[0],
		},
		Span: roachpb.Span{
			Key:    r.StartKey.AsRawKey(),
			EndKey: r.EndKey.AsRawKey(),
		},
		WithDiff:        false,
		AdmissionHeader: roachpb.AdmissionHeader{},
		//StreamID:        1000,
	}

	//d := 1*time.Microsecond
	d := 5*time.Second

	doneC := make(chan struct{})
	go func() {
		str := newTestStream(t, d)

		err := n.RangeFeed(&req, str)
		if err != nil {
			t.Log("Range feed terminated with error: ", err)
		}
		t.Log("Range feed terminated")
		close(doneC)
	}()

	const basePayloadLen = 3
	kv := tc.Server(0).DB()
	closeC := make(chan struct{})
	put := func(id byte) {
		//content := append(randutil.RandBytes(rng, basePayloadLen), 0, 0, 0, 0)
		var count int
		k := append(sr, "key"...)
		for {
			//content[basePayloadLen] = byte(count & 0xff)
			//content[basePayloadLen+1] = byte((count>>8) & 0xff)
			//content[basePayloadLen+2] = byte((count>>16) & 0xff)
			//content[basePayloadLen+3] = byte((count>>24) & 0xff)
			if kv.Put(ctx, k, fmt.Sprintf("%d,%d", id, count)) != nil {
				return
			}
			if count % 1000 == 0 {
				t.Logf("%d wrote events %d", id, count)
			}
			count += 1
			select {
			case <-closeC:
				return
			default:
			}
		}
	}

	go put(1)
	go put(2)
	go put(3)
	go put(4)
	go put(5)
	go put(6)
	go put(7)
	go put(8)
	go put(9)
	go put(10)

	<- time.After(3*time.Second)
	select {
	case <-doneC:
		t.Fatal("stream failed")
	default:
	}
	t.Log("trying split")
	_, _, err = tc.SplitRange(append(sr, "aaa"...))
	require.NoError(t, err, "failed to split range")
	<- time.After(3*time.Second)
	close(closeC)
	<- time.After(1*time.Second)
*/
