// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"bytes"
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// loqTestSpec defines workload to run on cluster as well as range size which
// affects how frequently ranges are split. Splitting behavior introduces
// uncertainty that triggers situations where plan can't be created or resulting
// cluster state is not workable.
type loqTestSpec struct {
	wl workload
	// We don't use one "small" limit for different workloads as it depends on the
	// data size and needs to be tweaked per workload to have a non zero failure
	// rate.
	rangeSize int64
}

func (s loqTestSpec) String() string {
	sizeName := "default"
	if s.rangeSize > 0 {
		sizeName = strconv.Itoa(int(s.rangeSize))
	}
	return fmt.Sprintf("loqrecovery/workload=%s/rangeSize=%s", s.wl, sizeName)
}

func registerLOQRecovery(r registry.Registry) {
	spec := r.MakeClusterSpec(6)
	for _, s := range []loqTestSpec{
		{wl: movrLoqWorkload{concurrency: 32}, rangeSize: 1024 * 1024 * 2},
		{wl: movrLoqWorkload{concurrency: 32}},
		{wl: tpccLoqWorkload{warehouses: 100, concurrency: 32}, rangeSize: 1024 * 1024 * 16},
		{wl: tpccLoqWorkload{warehouses: 100, concurrency: 32}},
	} {
		testSpec := s
		r.Add(registry.TestSpec{
			Name:              s.String(),
			Owner:             registry.OwnerKV,
			Tags:              []string{`default`},
			Cluster:           spec,
			NonReleaseBlocker: true,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runRecoverLossOfQuorum(ctx, t, c, testSpec)
			},
		})
	}
}

type testOutcomeMetric int

const (
	// Plan can not be created
	planCantBeCreated testOutcomeMetric = 0
	// Nodes fail to start after recovery was performed.
	restartFailed testOutcomeMetric = 10
	// Nodes restarted but decommission didn't work.
	decommissionFailed testOutcomeMetric = 20
	// Nodes restarted but replica count didn't go up within timeout.
	upreplicationFailed testOutcomeMetric = 30
	// Nodes started, but workload failed to succeed on any writes after restart.
	workloadFailed testOutcomeMetric = 40
	// Nodes restarted and workload produced some writes.
	success testOutcomeMetric = 100
)

var outcomeNames = map[testOutcomeMetric]string{
	planCantBeCreated:   "planCantBeCreated",
	restartFailed:       "restartFailed",
	decommissionFailed:  "decommissionFailed",
	upreplicationFailed: "upreplicationFailed",
	workloadFailed:      "workloadFailed",
	success:             "success",
}

// RecoveryImpossibleError is an error indicating that we have encountered
// recovery failure that is detected by recovery procedures and is not a test
// failure.
type RecoveryImpossibleError struct {
	testOutcome testOutcomeMetric
}

func (r RecoveryImpossibleError) Error() string {
	return fmt.Sprintf("recovery failed with code %d", r.testOutcome)
}

/*
The purpose of this test is to exercise loss of quorum recovery on a real
cluster and ensure that recovery doesn't cause it to crash or fail in unexpected
ways.
This test does not expect recovery to always succeed functionally e.g. failure
to create a recovery plan is a success.
Test will fail for infrastructure failure reasons like failure to setup a
cluster, run workload, collect replica info, distribute recovery plan etc.
But it would succeed if plan can't be created or workload won't be able to write
data after recovery.
Level of success is currently written as a "fake" perf value so that it could
be analyzed retrospectively. That gives as an indication if recovery ever
succeed without a need to break the build every time we stop the cluster in the
state where recovery is impossible.
While not ideal, it could be changed later when we have an appropriate facility
to store such results.
*/
func runRecoverLossOfQuorum(ctx context.Context, t test.Test, c cluster.Cluster, s loqTestSpec) {
	// To debug or analyze recovery failures, enable this option. The test will
	// start failing every time recovery is not successful. That would let you
	// get the logs and plans and check if there was a node panic happening as a
	// result of recovery.
	debugFailures := true

	// Number of cockroach cluster nodes.
	maxNode := c.Spec().NodeCount - 1
	nodes := c.Range(1, maxNode)
	// Controller node runs offline procedures and workload.
	controller := c.Spec().NodeCount
	// Nodes that we plan to keep after simulated failure.
	remaining := []int{1, 4, 5}
	planName := "recover-plan.json"
	pgURL := fmt.Sprintf("{pgurl:1-%d}", c.Spec().NodeCount-1)
	dbName := "test_db"
	workloadHistogramFile := "restored.json"

	c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
	startOpts := option.DefaultStartOpts()
	settings := install.MakeClusterSettings()
	c.Start(ctx, t.L(), startOpts, settings, nodes)

	// Cleanup stale files that could prevent normal cli operation.
	args := []string{"rm", "-f", planName}
	for _, node := range remaining {
		args = append(args, fmt.Sprintf("replica-info-%d.json", node))
	}
	c.Run(ctx, c.All(), args...)

	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()

	t.L().Printf("started cluster")

	// Lower the dead node detection threshold to make decommissioning nodes
	// faster. 1:30 is currently the smallest allowed value, otherwise we could
	// go as low as possible to improve test time.
	_, err := db.Exec("SET CLUSTER SETTING server.time_until_store_dead = '1m30s'")
	require.NoError(t, err, "failed to set dead store timeout")
	// Lower default statement timeout so that we can detect if workload gets stuck.
	_, err = db.Exec("SET CLUSTER SETTING sql.defaults.statement_timeout = '60s'")
	require.NoError(t, err, "failed to set default statement timeout")
	if debugFailures {
		// For debug runs enable statement tracing to have a better visibility of which
		// queries failed.
		_, err = db.Exec("SET CLUSTER SETTING sql.trace.stmt.enable_threshold = '30s'")
	}

	m := c.NewMonitor(ctx, nodes)
	m.Go(func(ctx context.Context) error {
		t.L().Printf("initializing workload")

		c.Run(ctx, c.Node(controller), s.wl.initCmd(pgURL, dbName))

		if s.rangeSize > 0 {
			err = setDbRangeLimits(db, dbName, s.rangeSize)
			require.NoError(t, err, "failed to set range limits configuration")
		}

		t.L().Printf("running workload")
		c.Run(ctx, c.Node(controller), s.wl.runCmd(pgURL, dbName, ifLocal(c, "10s", "30s"), ""))
		t.L().Printf("workload finished")
		return nil
	})
	m.Wait()

	recordOutcome, buffer := initPerfCapture()

	// Perform recovery
	testOutcome := success

	stopOpts := option.DefaultStopOpts()
	c.Stop(ctx, t.L(), stopOpts, nodes)

	planArguments := ""
	for _, node := range remaining {
		t.L().Printf("collecting replica info from %d", node)
		name := fmt.Sprintf("replica-info-%d.json", node)
		collectCmd := "./cockroach debug recover collect-info --store={store-dir} " + name
		c.Run(ctx, c.Nodes(node), collectCmd)
		if err := c.Get(ctx, t.L(), name, path.Join(t.ArtifactsDir(), name), c.Node(node)); err != nil {
			t.Errorf("failed to collect node replica info %s from node %d: %s", name, node, err)
		}
		c.Put(ctx, path.Join(t.ArtifactsDir(), name), name, c.Nodes(controller))
		planArguments += " " + name
	}
	t.L().Printf("running plan creation")
	planCmd := "./cockroach debug recover make-plan --confirm y -o " + planName + planArguments
	if err = c.RunE(ctx, c.Node(controller), planCmd); err != nil {
		t.L().Printf("failed to create plan, test can't proceed assuming unrecoverable cluster: %s",
			err)
		testOutcome = planCantBeCreated
	} else {
		if err := c.Get(ctx, t.L(), planName, path.Join(t.ArtifactsDir(), planName),
			c.Node(controller)); err != nil {
			t.Errorf("failed to collect plan %s from controller node %d: %s", planName, controller, err)
		}

		t.L().Printf("distributing and applying recovery plan")
		c.Put(ctx, path.Join(t.ArtifactsDir(), planName), planName, c.Nodes(remaining...))
		applyCommand := "./cockroach debug recover apply-plan --store={store-dir} --confirm y " + planName
		c.Run(ctx, c.Nodes(remaining...), applyCommand)

		// Restart cluster and continue workload
		startOpts.RoachprodOpts.SkipInit = true
		c.Start(ctx, t.L(), startOpts, settings, c.Nodes(remaining...))

		m = c.NewMonitor(ctx, c.Nodes(remaining...))
		m.Go(func(ctx context.Context) error {
			t.L().Printf("waiting for nodes to restart")
			if err = contextutil.RunWithTimeout(ctx, "wait-for-restart", time.Minute,
				func(ctx context.Context) error {
					var err error
					for {
						if ctx.Err() != nil {
							return &RecoveryImpossibleError{testOutcome: restartFailed}
						}
						db, err = c.ConnE(ctx, t.L(), 1)
						if err == nil {
							break
						}
						time.Sleep(5 * time.Second)
					}
					// Restoring range limits if they were changed to improve recovery
					// times.
					for {
						if ctx.Err() != nil {
							return &RecoveryImpossibleError{testOutcome: restartFailed}
						}
						if s.rangeSize > 0 {
							err = setDbRangeLimits(db, dbName, 512<<20)
						} else {
							_, err = db.ExecContext(ctx, "SELECT 1")
						}
						if err == nil {
							break
						}
						time.Sleep(5 * time.Second)
					}
					return nil
				}); err != nil {
				return err
			}

			t.L().Printf("decommissioning dead nodes")
			if err := contextutil.RunWithTimeout(ctx, "decommission-removed-nodes", 5*time.Minute,
				func(ctx context.Context) error {
					decommissionCmd := fmt.Sprintf(
						"./cockroach node decommission --wait none --insecure --url={pgurl:%d} 2 3", 1)
					return c.RunE(ctx, c.Node(controller), decommissionCmd)
				}); err != nil {
				// Timeout means we failed to recover ranges especially system ones
				// correctly.
				return &RecoveryImpossibleError{testOutcome: decommissionFailed}
			}

			t.L().Printf("force ranges through replication queue")
			if err := contextutil.RunWithTimeout(ctx, "run-repl-queue", 5*time.Minute,
				func(ctx context.Context) error {
					rows, err := db.QueryContext(ctx,
						"select range_id from crdb_internal.ranges where 2=any(replicas) or 3=any(replicas) or array_length(replicas, 1) < 3")
					if err != nil {
						return errors.Wrap(err, "failed to query suspicious ranges")
					}
					var ranges []int64
					for rows.Next() {
						var rangeId int64
						if err = rows.Scan(&rangeId); err != nil {
							return errors.Wrap(err, "failed to query suspicious ranges")
						}
						ranges = append(ranges, rangeId)
					}
					if rows.Err() != nil {
						return errors.Wrap(rows.Err(), "failed to query suspicious ranges")
					}

					dbs := make(map[int]*gosql.DB)
					for _, node := range remaining {
						dbs[node], err = c.ConnE(ctx, t.L(), node)
						if err != nil {
							return errors.Wrapf(err, "failed connect to dburl of node %d during up-replication",
								node)
						}
					}
					for _, rangeId := range ranges {
						var enqueued bool
						var enqErrs []string
						for _, db := range dbs {
							if err := db.QueryRowContext(
								ctx,
								"select crdb_internal.kv_enqueue_replica($1, 'replicate', true)",
								rangeId,
							).Scan(&enqueued); err != nil || !enqueued {
								enqErrs = append(enqErrs, err.Error())
								continue
							}
							break
						}
						if !enqueued {
							t.L().Printf("range r%d failed to enqueue into replication: \n%s",
								rangeId, strings.Join(enqErrs, "\n"))
						}
					}
					return nil
				}); err != nil {
				t.L().Printf("up-replication failed: %s", err)
				return &RecoveryImpossibleError{testOutcome: upreplicationFailed}
			}

			t.L().Printf("waiting for up-replication")
			if err = retryWithTimeout(ctx, 25*time.Minute, 5*time.Second, func(ctx context.Context) bool {
				var replicasStillOnDeadNodes int
				if err := db.QueryRowContext(ctx,
					// Wait till all ranges move from dead nodes.
					"select count(*) from crdb_internal.ranges where 2=any(replicas) or 3=any(replicas)",
				).Scan(&replicasStillOnDeadNodes); err != nil {
					return false
				}
				t.L().Printf("replicas on dead nodes %d", replicasStillOnDeadNodes)
				return replicasStillOnDeadNodes == 0
			}, func(ctx context.Context) bool {
				var fullyReplicated bool
				if err := db.QueryRowContext(ctx,
					// Check if all ranges are replicated fully.
					"SELECT min(array_length(replicas, 1)) >= 3 FROM crdb_internal.ranges",
				).Scan(&fullyReplicated); err != nil {
					return false
				}
				t.L().Printf("fully up-replicated %t", fullyReplicated)
				return fullyReplicated
			}); err != nil {
				return &RecoveryImpossibleError{testOutcome: upreplicationFailed}
			}

			t.L().Printf("resuming workload")
			if err = c.RunE(ctx, c.Node(controller),
				s.wl.runCmd(
					fmt.Sprintf("{pgurl:1,4-%d}", maxNode), dbName, ifLocal(c, "10s", "120s"),
					workloadHistogramFile)); err != nil {
				t.L().Printf("workload failed: %s", err)
				return &RecoveryImpossibleError{testOutcome: workloadFailed}
			}
			t.L().Printf("workload finished")

			return nil
		})

		if err = m.WaitE(); err != nil {
			testOutcome = restartFailed
			if recErr := (*RecoveryImpossibleError)(nil); errors.As(err, &recErr) {
				testOutcome = recErr.testOutcome
			}
		} else {
			testOutcome = analyzeWorkloadOutput(ctx, t, c, controller, workloadHistogramFile)
		}
	}

	recordOutcome(testOutcome)
	buffer.upload(ctx, t, c)
	if testOutcome == success {
		t.L().Printf("recovery succeeded. Pop champagne! 🍾 \U0001F973")
	} else {
		t.L().Printf("recovery failed with error %s(%d)", outcomeNames[testOutcome], testOutcome)
		if debugFailures && testOutcome > 0 {
			t.Fatalf("Test failed with error %s(%d)", outcomeNames[testOutcome], testOutcome)
		}
	}
}

func setDbRangeLimits(db *gosql.DB, dbName string, size int64) error {
	_, err := db.Exec(
		fmt.Sprintf("ALTER DATABASE %s CONFIGURE ZONE USING range_max_bytes=%d, range_min_bytes=1024",
			dbName, size))
	return err
}

func retryWithTimeout(
	ctx context.Context,
	duration, retryDelay time.Duration,
	funcs ...func(context.Context) bool,
) error {
	return contextutil.RunWithTimeout(ctx, "wait-for-checks", duration,
		func(ctx context.Context) error {
			for _, fun := range funcs {
				ok := false
				for !ok {
					if ok = fun(ctx); ok {
						// Next function.
						continue
					}
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(retryDelay):
					}
				}
			}
			return nil
		})
}

func analyzeWorkloadOutput(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	workloadNode int,
	workloadHistogramFile string,
) testOutcomeMetric {
	// Check if any workload histogram contains a non-zero value. That means
	// some writes succeed after recovery and we treat is as a proxy for
	// some success.
	if err := c.Get(ctx, t.L(), workloadHistogramFile, workloadHistogramFile,
		c.Node(workloadNode)); err != nil {
		t.L().Printf("failed to retrieve workload histogram file %s from node %d",
			workloadHistogramFile,
			workloadNode)
		return workloadFailed
	}
	sMap, err := histogram.DecodeSnapshots(workloadHistogramFile)
	if err != nil {
		t.L().Printf("failed to parse histograms from workload")
		return workloadFailed
	}
	for _, ticks := range sMap {
		for _, tick := range ticks {
			for _, count := range tick.Hist.Counts {
				if count > 0 {
					return success
				}
			}
		}
	}
	// We didn't find any non-zero counter across all workload operations,
	// that means cluster is in a broken state.
	return workloadFailed
}

type perfArtifact bytes.Buffer

func (p *perfArtifact) upload(ctx context.Context, t test.Test, c cluster.Cluster) {
	// Upload the perf artifacts to any one of the nodes so that the test
	// runner copies it into an appropriate directory path.
	dest := filepath.Join(t.PerfArtifactsDir(), "stats.json")
	if err := c.RunE(ctx, c.Node(1), "mkdir -p "+filepath.Dir(dest)); err != nil {
		t.L().Errorf("failed to create perf dir: %+v", err)
	}
	if err := c.PutString(ctx, (*bytes.Buffer)(p).String(), dest, 0755, c.Node(1)); err != nil {
		t.L().Errorf("failed to upload perf artifacts to node: %s", err.Error())
	}
}

// Register histogram and create a function that would record test outcome value.
// Returned buffer contains all recorded ticks for the test and is updated
// every time metric function is called.
func initPerfCapture() (func(testOutcomeMetric), *perfArtifact) {
	reg := histogram.NewRegistry(time.Second*time.Duration(success), histogram.MockWorkloadName)
	bytesBuf := bytes.NewBuffer([]byte{})
	jsonEnc := json.NewEncoder(bytesBuf)

	writeSnapshot := func() {
		reg.Tick(func(tick histogram.Tick) {
			_ = jsonEnc.Encode(tick.Snapshot())
		})
	}

	recordOutcome := func(metric testOutcomeMetric) {
		reg.GetHandle().Get("recovery_result").Record(time.Duration(metric) * time.Second)
		writeSnapshot()
	}

	// Capture start time for the test.
	writeSnapshot()

	return recordOutcome, (*perfArtifact)(bytesBuf)
}

type workload interface {
	initCmd(pgURL string, db string) string
	runCmd(pgURL string, db string, duration string, histFile string) string
}

type movrLoqWorkload struct {
	concurrency int
}

func (movrLoqWorkload) initCmd(pgURL string, db string) string {
	return fmt.Sprintf(`./cockroach workload init movr %s --db %s`, pgURL, db)
}

func (w movrLoqWorkload) runCmd(pgURL string, db string, duration string, histFile string) string {
	if len(histFile) == 0 {
		return fmt.Sprintf(
			`./cockroach workload run movr %s --db %s --duration %s`,
			pgURL, db, duration)
	}
	return fmt.Sprintf(
		`./cockroach workload run movr %s --db %s --histograms %s --duration %s --concurrency %d`,
		pgURL, db, histFile, duration, w.concurrency)
}

func (w movrLoqWorkload) String() string {
	return "movr"
}

type tpccLoqWorkload struct {
	warehouses  int
	concurrency int
}

func (w tpccLoqWorkload) initCmd(pgURL string, db string) string {
	return fmt.Sprintf(`./cockroach workload init tpcc %s --db %s --warehouses %d`, pgURL, db,
		w.warehouses)
}

func (w tpccLoqWorkload) runCmd(pgURL string, db string, duration string, histFile string) string {
	if len(histFile) == 0 {
		return fmt.Sprintf(
			`./cockroach workload run tpcc %s --db %s --duration %s --warehouses %d --concurrency %d`,
			pgURL, db, duration, w.warehouses, w.concurrency)
	}
	return fmt.Sprintf(
		`./cockroach workload run tpcc %s --db %s --histograms %s --duration %s --warehouses %d --concurrency %d`,
		pgURL, db, histFile, duration, w.warehouses, w.concurrency)
}

func (w tpccLoqWorkload) String() string {
	return "tpcc"
}
