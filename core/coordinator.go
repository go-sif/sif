package core

import (
	"context"
	"fmt"
	"log"
	"math"
	"net"
	"strings"
	"sync"

	"github.com/go-sif/sif"
	"github.com/go-sif/sif/internal/partition"
	pb "github.com/go-sif/sif/internal/rpc"
	itypes "github.com/go-sif/sif/internal/types"
	iutil "github.com/go-sif/sif/internal/util"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
)

// coordinator is a Coordinator node which has lifecycle methods
type coordinator struct {
	opts          *NodeOptions
	server        *grpc.Server
	clusterServer *clusterServer
	frame         sif.DataFrame
}

func createCoordinator(opts *NodeOptions) (*coordinator, error) {
	// default certain options if not supplied
	ensureDefaultNodeOptionsValues(opts)
	return &coordinator{opts: opts}, nil
}

// Start the Coordinator - blocking unless run in a goroutine
func (c *coordinator) Start(frame sif.DataFrame) error {
	if frame == nil {
		return fmt.Errorf("DataFrame cannot be nil")
	}
	c.frame = frame
	// start server
	lis, err := net.Listen("tcp", c.opts.connectionString())
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}
	c.server = grpc.NewServer()
	// register rpc handlers
	c.clusterServer = createClusterServer()
	pb.RegisterClusterServiceServer(c.server, c.clusterServer)
	pb.RegisterLogServiceServer(c.server, createLogServer())
	// start server
	err = c.server.Serve(lis)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}
	return nil
}

// GracefulStop the Coordinator, waiting for RPCs to finish
func (c *coordinator) GracefulStop() error {
	if c.server != nil {
		c.server.GracefulStop()
	}
	return nil
}

// Stop the Coordinator immediately
func (c *coordinator) Stop() error {
	if c.server != nil {
		c.server.Stop()
	}
	return nil
}

// Run a DataFrame Plan within this cluster
func (c *coordinator) Run(ctx context.Context) (map[string]sif.CollectedPartition, error) {
	var wg sync.WaitGroup
	waitCtx, cancel := context.WithTimeout(ctx, c.opts.WorkerJoinTimeout)
	defer cancel()
	if err := c.clusterServer.waitForWorkers(waitCtx, c.opts.NumWorkers); err != nil {
		return nil, err
	}
	workers := c.clusterServer.Workers()
	workerConns, err := dialWorkers(workers)
	if err != nil {
		return nil, err
	}
	defer closeGRPCConnections(workerConns)
	// optimize dataframe to create plan
	eframe, ok := c.frame.(itypes.ExecutableDataFrame)
	if !ok {
		return nil, fmt.Errorf("DataFrame must be executable")
	}
	planExecutor := eframe.Optimize().Execute(&itypes.PlanExecutorConfig{
		TempFilePath:       "",
		InMemoryPartitions: 0,
		Streaming:          c.frame.GetDataSource().IsStreaming(),
	})
	// analyze and assign partitions
	partitionMap, err := eframe.AnalyzeSource()
	if err != nil {
		return nil, err
	}
	var numPartitions int64
	asyncErrors := iutil.CreateAsyncErrorChannel()
	for i := int64(0); partitionMap.HasNext(); i = (i + 1) % int64(len(workers)) {
		wg.Add(1)
		numPartitions++
		part := partitionMap.Next()
		// log.Printf("Assigning partition loader \"%s\" to worker %d\n", part.ToString(), i)
		go asyncAssignPartition(ctx, part, workers[i], workerConns[i], &wg, asyncErrors)
	}
	if err = iutil.WaitAndFetchError(&wg, asyncErrors); err != nil {
		return nil, err
	}
	// moderate execution of stages, blocking on completion of each
	for planExecutor.HasNextStage() {
		asyncErrors = iutil.CreateAsyncErrorChannel()
		select {
		// check for shutdown signal
		case <-ctx.Done():
			// shutdown workers
			if err = stopWorkers(workers, workerConns); err != nil {
				return nil, err
			}
			return nil, ctx.Err()
		default:
			// run stage on each worker, blocking until stage is complete across the cluster
			stage := planExecutor.GetNextStage()
			runShuffle := stage.EndsInShuffle()
			prepCollect := stage.EndsInCollect()
			shuffleBuckets := computeShuffleBuckets(workers)
			wg.Add(len(workers))
			for i := range workers {
				go asyncRunStage(ctx, stage, workers[i], workerConns[i], runShuffle, prepCollect, shuffleBuckets[i], shuffleBuckets, workers, &wg, asyncErrors)
			}
			// wait for all the workers to finish the stage
			if err = iutil.WaitAndFetchError(&wg, asyncErrors); err != nil {
				return nil, err
			}
			// If we need to run a collect, then trigger that
			if prepCollect {
				asyncErrors = iutil.CreateAsyncErrorChannel()
				// run collect
				wg.Add(len(workers))
				collected := make(map[string]sif.CollectedPartition)
				collectionLimit := semaphore.NewWeighted(stage.GetCollectionLimit())
				var collectedLock sync.Mutex
				for i := range workers {
					go asyncRunCollect(ctx, workers[i], workerConns[i], shuffleBuckets[i], shuffleBuckets, workers, stage.FinalSchema(), stage.OutgoingSchema(), collected, &collectedLock, collectionLimit, &wg, asyncErrors)
				}
				if err = iutil.WaitAndFetchError(&wg, asyncErrors); err != nil {
					return nil, err
				}
				return collected, nil
			}
		}
	}
	// shutdown workers, since the job is done
	if err = stopWorkers(workers, workerConns); err != nil {
		return nil, err
	}
	return nil, nil
}

// Assigns a maximum key to each worker (in ascending order). The worker will handle keys less than their maximum and greater than or equal to the previous workers' maximum.
func computeShuffleBuckets(workers []*pb.MWorkerDescriptor) []uint64 {
	buckets := make([]uint64, len(workers), len(workers))
	interval := uint64(math.MaxUint64) / uint64(len(workers))
	for i := range workers {
		bucket := uint64(i+1) * interval
		buckets[i] = bucket
	}
	// this compensates for rounding errors, but makes
	// the last bucket a bit bigger than the others
	buckets[len(buckets)-1] = uint64(math.MaxUint64)
	return buckets
}

func stopWorkers(workers []*pb.MWorkerDescriptor, workerConns []*grpc.ClientConn) error {
	var wg sync.WaitGroup
	asyncErrors := iutil.CreateAsyncErrorChannel()
	// shutdown workers
	wg.Add(len(workers))
	for i := range workers {
		go asyncStopWorker(workers[i], workerConns[i], &wg, asyncErrors)
	}
	// if something went wrong, other than the worker perhaps shutting itself down, return error
	if err := iutil.WaitAndFetchError(&wg, asyncErrors); err != nil && !strings.Contains(err.Error(), "transport is closing") {
		return err
	}
	return nil
}

func asyncStopWorker(w *pb.MWorkerDescriptor, conn *grpc.ClientConn, wg *sync.WaitGroup, errors chan<- error) {
	defer wg.Done()
	lifecycleClient := pb.NewLifecycleServiceClient(conn)
	_, err := lifecycleClient.Stop(context.Background(), w)
	if err != nil {
		errors <- fmt.Errorf("Unable to stop worker %v\n%s", w.Id, err.Error())
	}
	fmt.Printf("Stopped worker %s\n", w.Id)
}

func asyncAssignPartition(ctx context.Context, part sif.PartitionLoader, w *pb.MWorkerDescriptor, conn *grpc.ClientConn, wg *sync.WaitGroup, errors chan<- error) {
	defer wg.Done()

	// Assign partition loader to worker
	partitionClient := pb.NewPartitionsServiceClient(conn)
	// serialize partition loader
	buff, err := part.GobEncode()
	if err != nil {
		errors <- fmt.Errorf("Could not serialize PartitionLoader")
		return
	}
	req := &pb.MAssignPartitionRequest{Loader: buff}
	_, err = partitionClient.AssignPartition(ctx, req)
	if err != nil {
		errors <- fmt.Errorf("Something went wrong while assigning partition %s to worker: %v\n%s", part.ToString(), w.Id, err.Error())
		log.Printf("Something went wrong while assigning partition %s to worker: %v\n%s\n", part.ToString(), w.Id, err.Error())
		return
	}
	// TODO do something with response
}

func asyncRunStage(ctx context.Context, s itypes.Stage, w *pb.MWorkerDescriptor, conn *grpc.ClientConn, runShuffle bool, prepCollect bool, assignedBucket uint64, shuffleBuckets []uint64, workers []*pb.MWorkerDescriptor, wg *sync.WaitGroup, errors chan<- error) {
	defer wg.Done()

	// Trigger remote stage execution
	log.Printf("Asking worker %s to run stage %s", w.Id, s.ID())
	executionClient := pb.NewExecutionServiceClient(conn)
	req := &pb.MRunStageRequest{StageId: s.ID(), RunShuffle: runShuffle, PrepCollect: prepCollect, AssignedBucket: assignedBucket, Buckets: shuffleBuckets, Workers: workers}
	_, err := executionClient.RunStage(ctx, req)
	if err != nil {
		errors <- fmt.Errorf("Something went wrong while running stage %s on worker %s: %v", s.ID(), w.Id, err)
		return
	}
	// TODO do something with response
}

func asyncRunCollect(ctx context.Context, w *pb.MWorkerDescriptor, conn *grpc.ClientConn, assignedBucket uint64, shuffleBuckets []uint64, workers []*pb.MWorkerDescriptor, currentSchema sif.Schema, incomingSchema sif.Schema, collected map[string]sif.CollectedPartition, collectedLock *sync.Mutex, collectionLimit *semaphore.Weighted, wg *sync.WaitGroup, errors chan<- error) {
	defer wg.Done()

	// Collect from worker
	log.Printf("Asking worker %s to supply prepared partitions to coordinator", w.Id)
	partitionClient := pb.NewPartitionsServiceClient(conn)
	for {
		req := &pb.MShufflePartitionRequest{Bucket: assignedBucket}
		res, err := partitionClient.ShufflePartition(ctx, req)
		if err != nil {
			errors <- fmt.Errorf("Something went wrong while running shuffling partition from worker %s: %v", w.Id, err)
			return
		} else if res.Part != nil {
			if collectionLimit.TryAcquire(1) {
				part := partition.FromMetaMessage(res.Part, incomingSchema, currentSchema)
				transferReq := &pb.MTransferPartitionDataRequest{Id: res.Part.Id}
				stream, err := partitionClient.TransferPartitionData(ctx, transferReq)
				if err != nil {
					errors <- err
					return
				}
				err = part.ReceiveStreamedData(stream, incomingSchema)
				if err != nil {
					errors <- err
					return
				}
				collectedLock.Lock()
				collected[res.Part.Id] = part.(sif.CollectedPartition)
				collectedLock.Unlock()
			} else {
				break
			}
		}
		if !res.HasNext {
			break
		}
	}
}
