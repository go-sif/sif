package cluster

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/go-sif/sif"
	pb "github.com/go-sif/sif/internal/rpc"
	itypes "github.com/go-sif/sif/internal/types"
	iutil "github.com/go-sif/sif/internal/util"
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
	jobFinishedWg sync.Mutex
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
	res := &worker{id: id.String(), opts: opts}
	res.jobFinishedWg.Lock()
	return res, nil
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
	log.Printf("[%s] attempting to register with coordinator at %s:%d", w.ID(), w.opts.CoordinatorHost, w.opts.CoordinatorPort)
	ctx, cancel := context.WithTimeout(context.Background(), w.opts.RPCTimeout)
	defer cancel()
	req := pb.MRegisterRequest{
		Id:   w.id,
		Port: int32(w.opts.Port),
	}
	if w.logClient == nil {
		log.Fatalf("[%s] Cannot register before dialing coordinator with mconnect()", w.ID())
	}
	_, err := w.clusterClient.RegisterWorker(ctx, &req)
	return err
}

// ID returns the ID of this worker
func (w *worker) ID() string {
	return w.id
}

// IsCoordinator returns true for coordinators
func (w *worker) IsCoordinator() bool {
	return false
}

// Start the worker - will block the current thread
func (w *worker) Start(frame sif.DataFrame) error {
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
	eframe, ok := frame.(itypes.ExecutableDataFrame)
	if !ok {
		return fmt.Errorf("DataFrame must be executable")
	}
	tmpDir, err := ioutil.TempDir(w.opts.TempDir, fmt.Sprintf("sif-worker-%d", w.opts.Port))
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)
	statsTracker := &itypes.RunStatistics{}
	planExecutor := eframe.Optimize().Execute(&itypes.PlanExecutorConfig{
		TempFilePath:       tmpDir,
		InMemoryPartitions: w.opts.NumInMemoryPartitions,
		Streaming:          eframe.GetParent().GetDataSource().IsStreaming(),
		IgnoreRowErrors:    w.opts.IgnoreRowErrors,
	}, statsTracker)
	// register rpc handlers for frame execution
	pb.RegisterLifecycleServiceServer(w.server, createLifecycleServer(w))
	pb.RegisterExecutionServiceServer(w.server, createExecutionServer(w.logClient, planExecutor, statsTracker))
	pb.RegisterPartitionsServiceServer(w.server, createPartitionServer(planExecutor))
	// register with master after we are serving
	ctx, cancel := context.WithTimeout(context.Background(), w.opts.RPCTimeout)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	asyncErrors := iutil.CreateAsyncErrorChannel()
	go w.asyncRegisterWithCoordinator(ctx, &wg, asyncErrors)
	if err = iutil.WaitAndFetchError(&wg, asyncErrors); err != nil {
		panic(err) // if we can't reigster with the coordinator, panic now
	}
	// start server
	log.Printf("[%s] ready to run job at %s", w.ID(), w.opts.connectionString())
	err = w.server.Serve(lis)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}
	return nil
}

// GracefulStop the worker, waiting for RPCs to finish
func (w *worker) GracefulStop() error {
	log.Printf("[%s] Gracefully stopping worker", w.ID())
	w.lifecycleLock.Lock()
	defer w.lifecycleLock.Unlock()
	if w.server != nil {
		w.server.GracefulStop()
		w.server = nil
	}
	w.jobFinishedWg.Unlock()
	return nil
}

// Stop the worker immediately
func (w *worker) Stop() error {
	log.Printf("[%s] Stopping worker", w.ID())
	w.lifecycleLock.Lock()
	defer w.lifecycleLock.Unlock()
	if w.server != nil {
		w.server.Stop()
		w.server = nil
	}
	w.jobFinishedWg.Unlock()
	return nil
}

// Run is a no-op for workers, blocking until worker is shut down
func (w *worker) Run(ctx context.Context) (map[string]sif.CollectedPartition, error) {
	// Run should block until execution is complete
	w.jobFinishedWg.Lock()
	log.Printf("[%s] Finished worker Run()", w.ID())
	return nil, nil
}

// asyncRegisterWithCoordinator is a very dirty approach to asynchronously registering workers with coordinators
func (w *worker) asyncRegisterWithCoordinator(ctx context.Context, wg *sync.WaitGroup, errors chan<- error) {
	defer wg.Done()
	for retries := 0; retries < w.opts.WorkerJoinRetries; retries++ {
		// Wait for server to register
		err := w.register()
		if err != nil && retries >= (w.opts.WorkerJoinRetries-1) {
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
