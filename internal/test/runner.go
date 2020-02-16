package integration

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	"github.com/go-sif/sif"
	"github.com/go-sif/sif/cluster"
	"github.com/stretchr/testify/require"
)

// runs a test dataframe on a test cluster
func runTestFrame(ctx context.Context, t *testing.T, frame sif.DataFrame, copts *cluster.NodeOptions, wopts *cluster.NodeOptions, numWorkers int) (map[string]sif.CollectedPartition, error) {
	// configure and start coordinator
	copts.Host = "localhost"
	copts.Port = 8080
	copts.NumWorkers = numWorkers
	copts.WorkerJoinTimeout = time.Duration(5) * time.Second
	copts.RPCTimeout = time.Duration(5) * time.Second
	coordinator, err := cluster.CreateNodeInRole(cluster.Coordinator, copts)
	require.Nil(t, err)
	go func() {
		err := coordinator.Start(frame)
		require.Nil(t, err)
	}()
	defer coordinator.GracefulStop()
	time.Sleep(50 * time.Millisecond) // TODO worker should retry a few times

	baseWorkerPort := 8081
	wopts.Host = "localhost"
	wopts.CoordinatorHost = "localhost"
	wopts.CoordinatorPort = copts.Port
	if wopts.RPCTimeout == 0 {
		wopts.RPCTimeout = time.Duration(5) * time.Second
	}
	if wopts.NumInMemoryPartitions == 0 {
		wopts.NumInMemoryPartitions = 10
	}
	// start workers
	for port := baseWorkerPort; port < baseWorkerPort+numWorkers; port++ {
		cwd, err := os.Getwd()
		if err != nil {
			log.Fatal(err)
		}
		tmpDir, err := ioutil.TempDir(cwd, fmt.Sprintf("sif-worker-%d", port))
		if err != nil {
			log.Fatal(err)
		}
		defer os.RemoveAll(tmpDir)
		wopts := &cluster.NodeOptions{
			Port:                  port,
			Host:                  wopts.Host,
			CoordinatorPort:       wopts.CoordinatorPort,
			CoordinatorHost:       wopts.CoordinatorHost,
			RPCTimeout:            wopts.RPCTimeout,
			TempDir:               tmpDir,
			NumInMemoryPartitions: wopts.NumInMemoryPartitions,
			IgnoreRowErrors:       wopts.IgnoreRowErrors,
		}
		worker, err := cluster.CreateNodeInRole(cluster.Worker, wopts)
		require.Nil(t, err)
		go func() {
			err := worker.Start(frame)
			require.Nil(t, err)
		}()
		defer worker.GracefulStop()
	}
	return coordinator.Run(ctx)
}
