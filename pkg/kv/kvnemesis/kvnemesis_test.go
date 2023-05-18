// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvnemesis

import (
	"context"
	gosql "database/sql"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

var defaultNumSteps = envutil.EnvOrDefaultInt("COCKROACH_KVNEMESIS_STEPS", 10)

func testClusterArgs(tr *SeqTracker) (base.TestClusterArgs, server.StickyInMemEnginesRegistry) {
	storeKnobs := &kvserver.StoreTestingKnobs{
		// Drop the clock MaxOffset to reduce commit-wait time for
		// transactions that write to global_read ranges.
		MaxOffset: 10 * time.Millisecond,
		// Make sure we know the seq for each of our writes when they come out of
		// the rangefeed. We do this via an interceptor to avoid having to change
		// RangeFeed's APIs.
		RangefeedValueHeaderFilter: func(key, endKey roachpb.Key, ts hlc.Timestamp, vh enginepb.MVCCValueHeader) {
			if seq := vh.KVNemesisSeq.Get(); seq > 0 {
				tr.Add(key, endKey, ts, seq)
			}
		},
	}

	stickyRegistry := server.NewStickyInMemEnginesRegistry()
	return base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: storeKnobs,
				KVClient: &kvcoord.ClientTestingKnobs{
					// Don't let DistSender split DeleteRangeUsingTombstone across range boundaries.
					// This does happen in real CRDB, but leads to separate atomic subunits, which
					// would add complexity to kvnemesis that isn't worth it. Instead, the operation
					// generator for the most part tries to avoid range-spanning requests, and the
					// ones that do end up happening get a hard error.
					OnRangeSpanningNonTxnalBatch: func(ba *kvpb.BatchRequest) *kvpb.Error {
						for _, req := range ba.Requests {
							if req.GetInner().Method() != kvpb.DeleteRange {
								continue
							}
							if req.GetDeleteRange().UseRangeTombstone == true {
								return kvpb.NewError(errDelRangeUsingTombstoneStraddlesRangeBoundary)
							}
						}
						return nil
					},
				},
				Server: &server.TestingKnobs{
					StickyEngineRegistry: stickyRegistry,
				},
			},
		},
	}, stickyRegistry
}

func randWithSeed(
	t interface {
		Logf(string, ...interface{})
		Helper()
	}, seedOrZero int64,
) *rand.Rand {
	t.Helper()
	var rng *rand.Rand
	if seedOrZero > 0 {
		rng = rand.New(rand.NewSource(seedOrZero))
	} else {
		rng, seedOrZero = randutil.NewTestRand()
	}
	t.Logf("seed: %d", seedOrZero)
	return rng
}

type ti interface {
	Helper()
	Logf(string, ...interface{})
}

type tBridge struct {
	ti
	ll logLogger
}

func newTBridge(t *testing.T) *tBridge {
	// NB: we're not using t.TempDir() because we want these to survive
	// on failure.
	td, err := os.MkdirTemp("", "kvnemesis")
	if err != nil {
		td = os.TempDir()
	}
	t.Cleanup(func() {
		if t.Failed() {
			return
		}
		_ = os.RemoveAll(td)
	})
	t.Logf("kvnemesis logging to %s", td)
	return &tBridge{
		ti: t,
		ll: logLogger{
			dir: td,
		},
	}
}

func (t *tBridge) WriteFile(basename string, contents string) string {
	return t.ll.WriteFile(basename, contents)
}

type kvnemesisTestCfg struct {
	numNodes     int
	numSteps     int
	concurrency  int
	seedOverride int64
}

func TestKVNemesisSingleNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testKVNemesisImpl(t, kvnemesisTestCfg{
		numNodes:     1,
		numSteps:     defaultNumSteps,
		concurrency:  5,
		seedOverride: 0,
	})
}

func TestKVNemesisMultiNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testKVNemesisImpl(t, kvnemesisTestCfg{
		numNodes:     4,
		numSteps:     defaultNumSteps,
		concurrency:  5,
		seedOverride: 0,
	})
}

func testKVNemesisImpl(t *testing.T, cfg kvnemesisTestCfg) {
	skip.UnderRace(t)

	if !buildutil.CrdbTestBuild {
		// `kvpb.RequestHeader` and `MVCCValueHeader` have a KVNemesisSeq field
		// that is zero-sized outside test builds. We could revisit that should
		// a need arise to run kvnemesis against production binaries.
		skip.IgnoreLint(t, "kvnemesis must be run with the crdb_test build tag")
	}

	// Can set a seed here for determinism. This works best when the seed was
	// obtained with cfg.concurrency=1.
	rng := randWithSeed(t, cfg.seedOverride)

	// 4 nodes so we have somewhere to move 3x replicated ranges to.
	ctx := context.Background()
	tr := &SeqTracker{}
	tcCfg, registry := testClusterArgs(tr)
	defer registry.CloseAllStickyInMemEngines()
	tc := testcluster.StartTestCluster(t, cfg.numNodes, tcCfg)
	defer tc.Stopper().Stop(ctx)
	dbs, sqlDBs := make([]*kv.DB, cfg.numNodes), make([]*gosql.DB, cfg.numNodes)
	for i := 0; i < cfg.numNodes; i++ {
		dbs[i] = tc.Server(i).DB()
		sqlDBs[i] = tc.ServerConn(i)
	}
	sqlutils.MakeSQLRunner(sqlDBs[0]).Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	// Turn net/trace on, which results in real trace spans created throughout.
	// This gives kvnemesis a chance to hit NPEs related to tracing.
	sqlutils.MakeSQLRunner(sqlDBs[0]).Exec(t, `SET CLUSTER SETTING trace.debug.enable = true`)

	config := NewDefaultConfig()
	config.NumNodes = cfg.numNodes
	config.NumReplicas = 3
	if config.NumReplicas > cfg.numNodes {
		config.NumReplicas = cfg.numNodes
	}
	// Massage operations (only needed to introduce delays between interesting ops)
	tOps := ClientOperationConfig{
		PutMissing:                1,
		PutExisting:               1,
		DeleteExisting:            1,
		DeleteRange:               1,
	}
	config.Ops = OperationConfig{
		Batch: BatchOperationConfig{},
		ClosureTxn: ClosureTxnConfig{
			TxnClientOps: tOps,
			TxnBatchOps: BatchOperationConfig{
				Batch: 4,
				Ops:   tOps,
			},
			Commit:         5,
			Rollback:       5,
			CommitInBatch:  5,
			CommitBatchOps: tOps,
		},
		Split: SplitConfig{
			SplitNew:   1,
		},
		Merge: MergeConfig{
			MergeNotSplit: 1,
			MergeIsSplit:  1,
		},
		ChangeLease: ChangeLeaseConfig{
			TransferLease: 1,
		},
	}
	// config.Ops.DB = ntOps
	// config.Ops.ClosureTxn.TxnClientOps = tOps
	// config.Ops.ClosureTxn.TxnBatchOps.Ops = tOps
	// config.Ops.ClosureTxn.CommitBatchOps = tOps

	logger := newTBridge(t)
	env := &Env{SQLDBs: sqlDBs, Tracker: tr, L: logger}
	failures, err := RunNemesis(ctx, rng, env, config, cfg.concurrency, cfg.numSteps, dbs...)
	require.NoError(t, err, `%+v`, err)
	if len(failures) > 0 {
		tc.Stopper().Stop(ctx)
		eng, err := registry.GetOrCreateStickyInMemEngine(ctx, &server.Config{}, base.StoreSpec{InMemory: true, StickyInMemoryEngineID: "auto-node1-store1"})
		require.NoError(t, err, "failed to find underlying store")
		defer eng.Close()
		logrr := func(value storage.MVCCKeyValue, stack storage.MVCCRangeKeyStack) error {
						t.Logf("key %s (%d), val len %d, total %d", value.Key.String(), len(value.Key.Key)+1, len(value.Value), len(value.Key.Key)+1+len(value.Value))
						return nil
					}
		_ = eng.MVCCIterate(keys.MinKey, keys.LocalMax, storage.MVCCKeyAndIntentsIterKind, storage.IterKeyTypePointsAndRanges, logrr)
		_ = eng.MVCCIterate(keys.LocalMax, keys.MaxKey, storage.MVCCKeyAndIntentsIterKind, storage.IterKeyTypePointsAndRanges, logrr)
	}
	require.Zero(t, len(failures), "kvnemesis detected failures") // they've been logged already
}

// TestRunReproductionSteps is a helper that allows quickly running a kvnemesis
// history.
func TestRunReproductionSteps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	//skip.IgnoreLint(t, "test unskipped only on demand")
	ctx := context.Background()

	const n = 1 // number of nodes

	tc := testcluster.StartTestCluster(t, n, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	db0 := tc.Server(0).DB()
	_, _ = db0, ctx

	// Paste a repro as printed by kvnemesis here.

	// Reproduction steps:
	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
	  db0.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
	    {
	      b := &kv.Batch{}
	      b.DelRange(tk(15437304249821172471), tk(17896817519063886085), true /* @s1 */) // <nil>
	      b.DelRange(tk(2783063523861483279), tk(13588992340785195907), true /* @s2 */) // <nil>
	      b.Put(tk(8202726363549829476), sv(3)) // <nil>
	      txn.Run(ctx, b) // @1684325347.447464615,0 <nil>
	    }
	    b := &kv.Batch{}
	    b.DelRange(tk(85332698134672705), tk(8855008832984309147), true /* @s4 */) // (/Table/100/"71d5f02af11e8964", <nil>)
	    b.Put(tk(11201170644642938550), sv(5)) // <nil>
	    txn.CommitInBatch(ctx, b) // @1684325347.447464615,0 <nil>
	    return nil
	  }) // @1684325347.447464615,0 <nil>
	  // ^-- txnpb:("unnamed" meta={id=757acc4c key=/Table/100/"d63c526cc43812f7" iso=Serializable pri=0.02960023 epo=0 ts=1684325347.447464615,0 min=1684325347.447464615,0 seq=6} lock=true stat=COMMITTED rts=1684325347.447464615,0 wto=false gul=1684325347.457464615,0)
	  // ^-- trace in: /tmp/cockroach-unitetest-logs/_tmp/1f42cf5be2fc021646bf9b2daf5eaef3/kvnemesis2851964199/w0_step0_trace


	  db0.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
	    txn.Put(ctx, tk(8111906539006066932), sv(12)) // @1684325347.526138622,1 <nil>
	    txn.Put(ctx, tk(16250148871782865394), sv(13)) // @1684325347.526138622,1 <nil>
	    b := &kv.Batch{}
	    b.DelRange(tk(6131020332872553677), tk(7998082027689730789), true /* @s14 */) // <nil>
	    txn.CommitInBatch(ctx, b) // @1684325347.526138622,1 <nil>
	    return nil
	  }) // @1684325347.526138622,1 <nil>
	  // ^-- txnpb:("unnamed" meta={id=973f5834 key=/Table/100/"70934804bca000f4" iso=Serializable pri=0.03197533 epo=0 ts=1684325347.526138622,1 min=1684325347.526138622,1 seq=4} lock=true stat=COMMITTED rts=1684325347.526138622,1 wto=false gul=1684325347.536138622,1)
	  // ^-- trace in: /tmp/cockroach-unitetest-logs/_tmp/1f42cf5be2fc021646bf9b2daf5eaef3/kvnemesis2851964199/w0_step1_trace


	  return nil
	})

	g.GoCtx(func(ctx context.Context) error {
	  db0.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
	    return errors.New("rollback")
	  }) // rollback
	  // ^-- trace in: /tmp/cockroach-unitetest-logs/_tmp/1f42cf5be2fc021646bf9b2daf5eaef3/kvnemesis2851964199/w1_step0_trace


	  db0.AdminSplit(ctx, tk(5922492017994197895), hlc.MaxTimestamp) // <nil>
	  // ^-- trace in: /tmp/cockroach-unitetest-logs/_tmp/1f42cf5be2fc021646bf9b2daf5eaef3/kvnemesis2851964199/w1_step1_trace


	  return nil
	})

	g.GoCtx(func(ctx context.Context) error {
	  db0.AdminMerge(ctx, tk(7188130618827856142)) // kv/kvserver/replica_command.go:808: merge failed: cannot merge final range
	  // ^-- trace in: /tmp/cockroach-unitetest-logs/_tmp/1f42cf5be2fc021646bf9b2daf5eaef3/kvnemesis2851964199/w2_step0_trace


	  db0.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
	    txn.DelRange(ctx, tk(10526490575933819230), tk(11972116358037793888), true /* @s8 */) // @1684325347.520578268,0 <nil>
	    {
	      b := &kv.Batch{}
	      txn.Run(ctx, b) // <nil>
	    }
	    {
	      b := &kv.Batch{}
	      b.Put(tk(14561064167864067443), sv(9)) // <nil>
	      b.Put(tk(13269365661547804993), sv(10)) // <nil>
	      b.DelRange(tk(9632692263582819), tk(15265603566517686815), true /* @s11 */) // (/Table/100/"70934804bca000f4", /Table/100/"b82641c3db52b941", /Table/100/"ca134ab67d34e573", <nil>)
	      txn.Run(ctx, b) // @1684325347.529282404,1 <nil>
	    }
	    return errors.New("rollback")
	  }) // rollback
	  // ^-- trace in: /tmp/cockroach-unitetest-logs/_tmp/1f42cf5be2fc021646bf9b2daf5eaef3/kvnemesis2851964199/w2_step1_trace


	  return nil
	})

	g.GoCtx(func(ctx context.Context) error {
	  db0.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
	    return nil
	  }) // @1684325347.456937710,0 <nil>
	  // ^-- txnpb:("unnamed" meta={id=1aff7014 key=/Min iso=Serializable pri=0.00688927 epo=0 ts=1684325347.456937710,0 min=1684325347.456937710,0 seq=0} lock=false stat=COMMITTED rts=1684325347.456937710,0 wto=false gul=1684325347.466937710,0)
	  // ^-- trace in: /tmp/cockroach-unitetest-logs/_tmp/1f42cf5be2fc021646bf9b2daf5eaef3/kvnemesis2851964199/w3_step0_trace


	  db0.AdminTransferLease(ctx, tk(12572279573916004186), 1) // <nil>
	  // ^-- trace in: /tmp/cockroach-unitetest-logs/_tmp/1f42cf5be2fc021646bf9b2daf5eaef3/kvnemesis2851964199/w3_step1_trace


	  db0.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
	    txn.DelRange(ctx, tk(8939902705414936401), tk(15303120399225987600), true /* @s15 */) // @1684325347.529282404,0 <nil>
	    txn.Put(ctx, tk(17448477547287752915), sv(16)) // @1684325347.529282404,0 <nil>
	    return errors.New("rollback")
	  }) // rollback
	  // ^-- trace in: /tmp/cockroach-unitetest-logs/_tmp/1f42cf5be2fc021646bf9b2daf5eaef3/kvnemesis2851964199/w3_step2_trace


	  return nil
	})

	g.GoCtx(func(ctx context.Context) error {
	  db0.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
	    txn.Put(ctx, tk(9685521916777433396), sv(6)) // @1684325347.479120115,0 <nil>
	    txn.DelRange(ctx, tk(2935440784285765645), tk(12675982142521309599), true /* @s7 */) // @1684325347.479120115,0 (/Table/100/"8669e2de01c78934", /Table/100/"9b728d5ade79beb6", <nil>)
	    b := &kv.Batch{}
	    txn.CommitInBatch(ctx, b) // @1684325347.479120115,0 <nil>
	    return nil
	  }) // @1684325347.479120115,0 <nil>
	  // ^-- txnpb:("unnamed" meta={id=880fa404 key=/Table/100/"8669e2de01c78934" iso=Serializable pri=0.03411248 epo=0 ts=1684325347.479120115,0 min=1684325347.479120115,0 seq=3} lock=true stat=COMMITTED rts=1684325347.479120115,0 wto=false gul=1684325347.489120115,0)
	  // ^-- trace in: /tmp/cockroach-unitetest-logs/_tmp/1f42cf5be2fc021646bf9b2daf5eaef3/kvnemesis2851964199/w4_step0_trace


	  return nil
	})

	require.NoError(t, g.Wait())
}
