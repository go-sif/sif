package integration

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/go-sif/sif"
	"github.com/go-sif/sif/cluster"
	"github.com/stretchr/testify/require"
)

// runs a test dataframe on a test cluster
func runTestFrame(ctx context.Context, t *testing.T, frame sif.DataFrame, opts *cluster.NodeOptions, numWorkers int) (map[string]sif.CollectedPartition, error) {
	// configure and start coordinator
	opts.Host = "127.0.0.1"
	opts.Port = 8080
	opts.CoordinatorPort = opts.Port
	opts.CoordinatorHost = "127.0.0.1"
	opts.NumWorkers = numWorkers
	opts.WorkerJoinTimeout = time.Duration(5) * time.Second
	opts.RPCTimeout = time.Duration(5) * time.Second
	if opts.NumInMemoryPartitions == 0 {
		opts.NumInMemoryPartitions = 10
	}

	coordinator, err := cluster.CreateNodeInRole(cluster.Coordinator, opts)
	require.Nil(t, err)
	go func() {
		err := coordinator.Start(frame)
		require.Nil(t, err)
	}()
	defer coordinator.GracefulStop()
	time.Sleep(50 * time.Millisecond) // TODO worker should retry a few times

	baseWorkerPort := 8081
	// start workers
	for port := baseWorkerPort; port < baseWorkerPort+numWorkers; port++ {
		cwd, err := os.Getwd()
		if err != nil {
			log.Fatal(err)
		}
		wopts := cluster.CloneNodeOptions(opts)
		wopts.Port = port
		wopts.TempDir = cwd
		worker, err := cluster.CreateNodeInRole(cluster.Worker, wopts)
		require.Nil(t, err)
		go func() {
			err := worker.Start(frame)
			require.Nil(t, err)
		}()
		// test running worker as well, to make sure blocking is functional
		go func() {
			_, err := worker.Run(ctx)
			require.Nil(t, err)
		}()
		defer worker.GracefulStop()
	}
	return coordinator.Run(ctx)
}
