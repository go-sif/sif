package core

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	pb "github.com/go-sif/sif/v0.0.1/core/rpc"
	uuid "github.com/gofrs/uuid"
	"google.golang.org/grpc"
)

// WorkerOptions describes configuration for a Master
type WorkerOptions struct {
	Port                  int           // Port for this Worker
	Host                  string        // Hostname for this Worker
	CoordinatorPort       int           // Port for Coordinator
	CoordinatorHost       string        // Hostname of Coordinator
	RPCTimeout            time.Duration // timeout for all RPC calls
	TempDir               string        // location for storing temporary files (primarily persisted partitions)
	NumInMemoryPartitions int           // the number of partitions to retain in memory before swapping to disk
	IgnoreRowErrors       bool          // iff true, log row transformation errors instead of crashing immediately
}

type worker struct {
	id            string
	opts          *WorkerOptions
	server        *grpc.Server
	lifecycleLock sync.Mutex
	clusterClient pb.ClusterServiceClient
	logClient     pb.LogServiceClient
}

// CreateWorker is a factory for Workers
func createWorker(opts nodeOptions) (*worker, error) {
	wOpts, ok := opts.(*WorkerOptions)
	if !ok {
		return nil, fmt.Errorf("Options for a Worker must be WorkerOptions")
	}
	// default certain options if not supplied
	if wOpts.NumInMemoryPartitions == 0 {
		wOpts.NumInMemoryPartitions = 100 // TODO should this just be a memory limit, and we compute NumInMemoryPartitions ourselves?
	}
	if wOpts.RPCTimeout == 0 {
		wOpts.RPCTimeout = time.Duration(5) * time.Second // TODO sensible default?
	}
	// generate worker ID
	id, err := uuid.NewV4()
	if err != nil {
		log.Fatalf("failed to generate UUID: %v", err)
	}
	return &worker{id: id.String(), opts: wOpts}, nil
}

// connectionString returns the connection string for the  worker
func (o *WorkerOptions) connectionString() string {
	return fmt.Sprintf("%s:%d", o.Host, o.Port)
}

// coordinatorConnectionString returns the connection string for the coordinator
func (o *WorkerOptions) coordinatorConnectionString() string {
	return fmt.Sprintf("%s:%d", o.CoordinatorHost, o.CoordinatorPort)
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
func (w *worker) Start(frame *DataFrame) error {
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
	planExecutor := frame.optimize().execute(&PlanExecutorConfig{
		tempFilePath:       w.opts.TempDir,
		inMemoryPartitions: w.opts.NumInMemoryPartitions,
		streaming:          frame.GetParent().GetDataSource().IsStreaming(),
		ignoreRowErrors:    w.opts.IgnoreRowErrors,
	})
	// register rpc handlers for frame execution
	pb.RegisterLifecycleServiceServer(w.server, &lifecycleServer{node: w})
	pb.RegisterExecutionServiceServer(w.server, &executionServer{logClient: w.logClient, planExecutor: planExecutor})
	pb.RegisterPartitionsServiceServer(w.server, &partitionServer{planExecutor: planExecutor, cache: make(map[string]*Partition)})
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
func (w *worker) Run(ctx context.Context) (map[string]*Partition, error) {
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
