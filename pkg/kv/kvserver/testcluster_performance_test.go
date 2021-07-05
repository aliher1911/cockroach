package kvserver

import (
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/fileutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func TestHowFast(t *testing.T) {
	dir, _ := ioutil.TempDir("/tmp/rdisk", fileutil.EscapeFilename(t.Name()))
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	}()

	numNodes := 3

	serverArgs := make(map[int]base.TestServerArgs)
	for i := 0; i < numNodes; i++ {
		serverArgs[i] = base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &StoreTestingKnobs{
					DisableRaftLogQueue: true,
				},
			},
			ScanInterval: time.Second,
			StoreSpecs: []base.StoreSpec{
				{
					Path:          filepath.Join(dir, fmt.Sprintf("store-%d", i)),
					InMemory:      false,
					//PebbleOptions: "[Options]\ndisable_wal=true\n",
				},
			},
		}
	}

	start := timeutil.Now()
	ctx := context.Background()
	s := serverutils.StartNewTestCluster(t, numNodes, base.TestClusterArgs{
		ServerArgsPerNode: serverArgs,
	})

	fmt.Printf("Started within %v\n", timeutil.Now().Sub(start))
	s.Stopper().Stop(ctx)
	fmt.Println("Stopped")
	t.Fail()
}

func TestTestClusterRemoteCalls(t *testing.T) {
	s := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{})
	ctx := context.Background()

	key := s.ScratchRange(t)

	// Prep batch
	ba := roachpb.BatchRequest{
		Header: roachpb.Header{},
	}
	var v roachpb.Value
	v.SetString("hullo")
	ba.Add(roachpb.NewPut(key, v))

	ds0 := s.Server(2).DistSenderI().(*kvcoord.DistSender)
	_, err := ds0.Send(ctx, ba)
	require.NoError(t, err.GoError())

	// Shutdown everything
	s.Stopper().Stop(ctx)
}
