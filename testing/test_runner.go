package testing

import (
	"context"
	"os"
	"time"

	"github.com/go-sif/sif"
	"github.com/go-sif/sif/cluster"
)

// LocalRunFrame runs a Dataframe on a localhost test cluster with a certain number of workers
func LocalRunFrame(ctx context.Context, frame sif.DataFrame, opts *cluster.NodeOptions, numWorkers int) (result *cluster.Result, err error) {
	// handle panics
	defer func() {
		if r := recover(); r != nil {
			if anErr, ok := r.(error); ok {
				err = anErr
			} else {
				panic(r)
			}
		}
	}()

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
	if err != nil {
		return nil, err
	}
	go func() {
		err = coordinator.Start(frame)
		if err != nil {
			panic(err)
		}
	}()
	defer coordinator.GracefulStop()
	time.Sleep(50 * time.Millisecond)

	baseWorkerPort := 8081
	// start workers
	for port := baseWorkerPort; port < baseWorkerPort+numWorkers; port++ {
		cwd, err := os.Getwd()
		if err != nil {
			return nil, err
		}
		wopts := cluster.CloneNodeOptions(opts)
		wopts.Port = port
		wopts.TempDir = cwd
		worker, err := cluster.CreateNodeInRole(cluster.Worker, wopts)
		if err != nil {
			return nil, err
		}
		go func() {
			err := worker.Start(frame)
			if err != nil {
				panic(err)
			}
		}()
		// test running worker as well, to make sure blocking is functional
		go func() {
			_, err := worker.Run(ctx)
			if err != nil {
				panic(err)
			}
		}()
		defer worker.GracefulStop()
	}
	return coordinator.Run(ctx)
}
