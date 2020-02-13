package core

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	pb "github.com/go-sif/sif/core/internal/rpc"
	uuid "github.com/gofrs/uuid"
	"google.golang.org/grpc"
)

type worker struct {
	id            string
	opts          *NodeOptions
	server        *grpc.Server
	lifecycleLock sync.Mutex
	clusterClient pb.ClusterServiceClient
	logClient     pb.LogServiceClient
}

// CreateWorker is a factory for Workers
func createWorker(opts *NodeOptions) (*worker, error) {
	// default certain options if not supplied
	ensureDefaultNodeOptionsValues(opts)
	// generate worker ID
	id, err := uuid.NewV4()
	if err != nil {
		log.Fatalf("failed to generate UUID: %v", err)
	}
	return &worker{id: id.String(), opts: opts}, nil
}

func (w *worker) mconnect() (*grpc.ClientConn, error) {
	// start client
	conn, err := grpc.Dial(w.opts.coordinatorConnectionString(), grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("fail to dial: %v", err)
	}
	w.logClient = pb.NewLogServiceClient(conn)
	w.clusterClient = pb.NewClusterServiceClient(conn)
	return conn, nil
}

func (w *worker) register() error {
	ctx, cancel := context.WithTimeout(context.Background(), w.opts.RPCTimeout)
	defer cancel()
	req := pb.MRegisterRequest{
		Id:   w.id,
		Host: w.opts.Host,
		Port: int32(w.opts.Port),
	}
	if w.logClient == nil {
		log.Fatalf("Cannot register before dialing master with mconnect()")
	}
	_, err := w.clusterClient.RegisterWorker(ctx, &req)
	return err
}

// ID returns the ID of this worker
func (w *worker) ID() string {
	return w.id
}

// Start the worker - will block the current thread
func (w *worker) Start(frame DataFrame) error {
	if frame == nil {
		return fmt.Errorf("DataFrame cannot be nil")
	}
	// connect to master
	conn, err := w.mconnect()
	if err != nil {
		return err
	}
	defer conn.Close()
	// start worker server
	lis, err := net.Listen("tcp", w.opts.connectionString())
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}
	w.lifecycleLock.Lock()
	w.server = grpc.NewServer()
	w.lifecycleLock.Unlock()
	// optimize dataframe to create plan
	eframe, ok := frame.(executableDataFrame)
	if !ok {
		return fmt.Errorf("DataFrame must be executable")
	}
	planExecutor := eframe.optimize().execute(&PlanExecutorConfig{
		tempFilePath:       w.opts.TempDir,
		inMemoryPartitions: w.opts.NumInMemoryPartitions,
		streaming:          eframe.getParent().GetDataSource().IsStreaming(),
		ignoreRowErrors:    w.opts.IgnoreRowErrors,
	})
	// register rpc handlers for frame execution
	pb.RegisterLifecycleServiceServer(w.server, &lifecycleServer{node: w})
	pb.RegisterExecutionServiceServer(w.server, &executionServer{logClient: w.logClient, planExecutor: planExecutor})
	pb.RegisterPartitionsServiceServer(w.server, &partitionServer{planExecutor: planExecutor})
	// register with master after we are serving
	ctx, cancel := context.WithTimeout(context.Background(), w.opts.RPCTimeout)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	asyncErrors := createAsyncErrorChannel()
	go w.asyncRegisterWithCoordinator(ctx, &wg, asyncErrors)
	if err = waitAndFetchError(&wg, asyncErrors); err != nil {
		return err
	}
	// start server
	err = w.server.Serve(lis)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}
	// finished
	return nil
}

// GracefulStop the worker, waiting for RPCs to finish
func (w *worker) GracefulStop() error {
	w.lifecycleLock.Lock()
	defer w.lifecycleLock.Unlock()
	if w.server != nil {
		w.server.GracefulStop()
		w.server = nil
	}
	return nil
}

// Stop the worker immediately
func (w *worker) Stop() error {
	w.lifecycleLock.Lock()
	defer w.lifecycleLock.Unlock()
	if w.server != nil {
		w.server.Stop()
		w.server = nil
	}
	return nil
}

// Run is a no-op for workers
func (w *worker) Run(ctx context.Context) (map[string]CollectedPTition, error) {
	return nil, nil
}

// registerWithCoordinator is a very dirty approach to asynchronously registering workers with coordinators
func (w *worker) asyncRegisterWithCoordinator(ctx context.Context, wg *sync.WaitGroup, errors chan<- error) {
	defer wg.Done()
	for retries := 0; retries < 5; retries++ {
		// Wait for server to register
		err := w.register()
		if err != nil && retries >= 4 {
			errors <- err
		} else if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			errors <- ctx.Err()
		case <-time.After(time.Second):
			// Wait 1 second and check again (iterate)
		}
	}
}
