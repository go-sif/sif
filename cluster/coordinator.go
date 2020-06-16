package cluster

import (
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"sync"

	"github.com/go-sif/sif"
	"github.com/go-sif/sif/internal/partition"
	pb "github.com/go-sif/sif/internal/rpc"
	"github.com/go-sif/sif/internal/stats"
	itypes "github.com/go-sif/sif/internal/types"
	iutil "github.com/go-sif/sif/internal/util"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
)

// coordinator is a Coordinator node which has lifecycle methods
type coordinator struct {
	opts              *NodeOptions
	server            *grpc.Server
	clusterServer     *clusterServer
	frame             sif.DataFrame
	bootstrappingLock sync.Mutex
}

func createCoordinator(opts *NodeOptions) (*coordinator, error) {
	// default certain options if not supplied
	ensureDefaultNodeOptionsValues(opts)
	res := &coordinator{opts: opts, clusterServer: createClusterServer(opts)}
	res.bootstrappingLock.Lock() // lock node as bootstrapping immediately
	return res, nil
}

// IsCoordinator returns true for coordinators
func (c *coordinator) IsCoordinator() bool {
	return true
}

// Start the Coordinator - blocking unless run in a goroutine
func (c *coordinator) Start(frame sif.DataFrame) error {
	// start node
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
	pb.RegisterClusterServiceServer(c.server, c.clusterServer)
	pb.RegisterLogServiceServer(c.server, createLogServer())
	// we're done bootstrapping
	c.bootstrappingLock.Unlock()
	// start server
	log.Printf("Starting Sif Coordinator at %s", c.opts.coordinatorConnectionString())
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
func (c *coordinator) Run(ctx context.Context) (*Result, error) {
	c.bootstrappingLock.Lock()
	defer c.bootstrappingLock.Unlock()

	var wg sync.WaitGroup
	waitCtx, cancel := context.WithTimeout(ctx, c.opts.WorkerJoinTimeout)
	defer cancel()
	log.Printf("Waiting for %d workers to connect...", c.opts.NumWorkers)
	if err := c.clusterServer.waitForWorkers(waitCtx); err != nil {
		return nil, err
	}
	workers := c.clusterServer.Workers()
	workerConns, err := dialWorkers(workers)
	if err != nil {
		return nil, err
	}
	// now that worker connections are open, defer shutting them down
	defer func() {
		// shutdown workers, since the job is done
		waitCtx := context.Background()
		if err = stopWorkers(waitCtx, workers, workerConns); err != nil {
			log.Fatal(err)
		}
		closeGRPCConnections(workerConns)
	}()
	// optimize dataframe to create plan
	eframe, ok := c.frame.(itypes.ExecutableDataFrame)
	if !ok {
		return nil, fmt.Errorf("DataFrame must be executable")
	}
	log.Printf("Running job...")
	statsTracker := &stats.RunStatistics{}
	planExecutor := eframe.Optimize().Execute(ctx, &itypes.PlanExecutorConfig{
		TempFilePath:             "",
		CacheMemoryHighWatermark: c.opts.CacheMemoryHighWatermark,
		Streaming:                c.frame.GetDataSource().IsStreaming(),
	}, statsTracker, true)
	defer planExecutor.Stop()
	statsTracker.Start(planExecutor.GetNumStages())
	defer statsTracker.Finish()
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
		log.Printf("Assigning partition loader \"%s\" to worker %d\n", part.ToString(), i)
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
			return nil, ctx.Err()
		default:
			// run stage on each worker, blocking until stage is complete across the cluster
			stage := planExecutor.GetNextStage()
			log.Println("------------------------------")
			log.Printf("Starting stage %d...", stage.ID())
			runShuffle := stage.EndsInShuffle()
			prepCollect := stage.EndsInCollect()
			prepAccumulate := stage.EndsInAccumulate()
			shuffleBuckets := computeShuffleBuckets(workers)
			wg.Add(len(workers))
			for i := range workers {
				go asyncRunStage(ctx, stage, workers[i], workerConns[i], runShuffle, prepAccumulate, prepCollect, shuffleBuckets[i], shuffleBuckets, workers, &wg, asyncErrors)
			}
			// wait for all the workers to finish the stage
			if err = iutil.WaitAndFetchError(&wg, asyncErrors); err != nil {
				return nil, err
			}
			defer func() {
				log.Printf("Finished stage %d", stage.ID())
				log.Println("------------------------------")
			}()
			if prepAccumulate { // If we need to run an accumulate
				asyncErrors = iutil.CreateAsyncErrorChannel()
				// run accumulate
				log.Printf("Starting accumulation for stage %d...", stage.ID())
				wg.Add(len(workers))
				accumulated := stage.Accumulator()
				var accumulatedLock sync.Mutex
				for i := range workers {
					go asyncRunAccumulate(ctx, workers[i], workerConns[i], accumulated, &accumulatedLock, &wg, asyncErrors)
				}
				if err = iutil.WaitAndFetchError(&wg, asyncErrors); err != nil {
					return nil, err
				}
				return &Result{Accumulated: accumulated}, nil
			} else if prepCollect { // If we need to run a collect, then trigger that
				asyncErrors = iutil.CreateAsyncErrorChannel()
				// run collect
				log.Printf("Starting collect for stage %d...", stage.ID())
				wg.Add(len(workers))
				collected := make(map[string]sif.CollectedPartition)
				collectionLimit := semaphore.NewWeighted(stage.GetCollectionLimit())
				var collectedLock sync.Mutex
				for i := range workers {
					go asyncRunCollect(ctx, workers[i], workerConns[i], shuffleBuckets[i], shuffleBuckets, stage.OutgoingSchema(), collected, &collectedLock, collectionLimit, &wg, asyncErrors)
				}
				if err = iutil.WaitAndFetchError(&wg, asyncErrors); err != nil {
					return nil, err
				}
				return &Result{Collected: collected}, nil
			}
		}
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

func stopWorkers(ctx context.Context, workers []*pb.MWorkerDescriptor, workerConns []*grpc.ClientConn) error {
	var wg sync.WaitGroup
	asyncErrors := iutil.CreateAsyncErrorChannel()
	// shutdown workers
	wg.Add(len(workers))
	for i := range workers {
		log.Printf("Stopping worker %s...", workers[i].Id)
		go asyncStopWorker(ctx, workers[i], workerConns[i], &wg, asyncErrors)
	}
	// // if something went wrong, other than the worker perhaps shutting itself down, return error
	// if err := iutil.WaitAndFetchError(&wg, asyncErrors); err != nil && !strings.Contains(err.Error(), "transport is closing") {
	// 	return err
	// }
	return nil
}

func asyncStopWorker(ctx context.Context, w *pb.MWorkerDescriptor, conn *grpc.ClientConn, wg *sync.WaitGroup, errors chan<- error) {
	defer wg.Done()
	lifecycleClient := pb.NewLifecycleServiceClient(conn)
	_, err := lifecycleClient.Stop(ctx, w)
	if err != nil {
		errors <- fmt.Errorf("Unable to stop worker %v\n%e", w.Id, err)
	}
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
		errors <- err
		log.Printf("Something went wrong while assigning partition %s to worker: %v\n%e\n", part.ToString(), w.Id, err)
		return
	}
	// TODO do something with response
}

func asyncRunStage(ctx context.Context, s itypes.Stage, w *pb.MWorkerDescriptor, conn *grpc.ClientConn, runShuffle bool, prepAccumulate bool, prepCollect bool, assignedBucket uint64, shuffleBuckets []uint64, workers []*pb.MWorkerDescriptor, wg *sync.WaitGroup, errors chan<- error) {
	defer wg.Done()

	// Trigger remote stage execution
	log.Printf("Asking worker %s to run stage %d", w.Id, s.ID())
	executionClient := pb.NewExecutionServiceClient(conn)
	req := &pb.MRunStageRequest{
		StageId:        int32(s.ID()),
		RunShuffle:     runShuffle,
		PrepCollect:    prepCollect,
		PrepAccumulate: prepAccumulate,
		AssignedBucket: assignedBucket,
		Buckets:        shuffleBuckets,
		Workers:        workers,
	}
	_, err := executionClient.RunStage(ctx, req)
	if err != nil {
		log.Printf("Something went wrong while running stage %d on worker %s: %e", s.ID(), w.Id, err)
		errors <- err
		return
	}
	log.Printf("Worker %s finished running stage %d", w.Id, s.ID())
	// TODO do something with response
}

func asyncRunAccumulate(ctx context.Context, w *pb.MWorkerDescriptor, conn *grpc.ClientConn, accumulator sif.Accumulator, accumulatedLock *sync.Mutex, wg *sync.WaitGroup, errors chan<- error) {
	defer wg.Done()
	// Grab accumulators from workers
	log.Printf("Asking worker %s to supply prepared accumulator", w.Id)
	partitionClient := pb.NewPartitionsServiceClient(conn)
	for {
		req := &pb.MShuffleAccumulatorRequest{}
		res, err := partitionClient.ShuffleAccumulator(ctx, req)
		if err != nil {
			log.Printf("Something went wrong while running shuffling accumulator from worker %s: %e", w.Id, err)
			errors <- err
			return
		} else if res.GetReady() {
			transferReq := &pb.MTransferAccumulatorDataRequest{}
			buf := make([]byte, 0, res.GetTotalSizeBytes())
			stream, err := partitionClient.TransferAccumulatorData(ctx, transferReq)
			if err != nil {
				errors <- err
				return
			}
			for chunk, err := stream.Recv(); err != io.EOF; chunk, err = stream.Recv() {
				if err != nil {
					errors <- err
					return
				}
				buf = append(buf, chunk.Data...)
			}
			inAcc, err := accumulator.FromBytes(buf)
			accumulatedLock.Lock()
			defer accumulatedLock.Unlock()
			err = accumulator.Merge(inAcc)
			if err != nil {
				errors <- err
				return
			}
			return
		}
		if ctx.Err() != nil {
			break
		}
	}
}

func asyncRunCollect(ctx context.Context, w *pb.MWorkerDescriptor, conn *grpc.ClientConn, assignedBucket uint64, shuffleBuckets []uint64, incomingSchema sif.Schema, collected map[string]sif.CollectedPartition, collectedLock *sync.Mutex, collectionLimit *semaphore.Weighted, wg *sync.WaitGroup, errors chan<- error) {
	defer wg.Done()

	// Collect from worker
	log.Printf("Asking worker %s to supply prepared partitions to coordinator", w.Id)
	partitionClient := pb.NewPartitionsServiceClient(conn)
	for {
		req := &pb.MShufflePartitionRequest{Bucket: assignedBucket}
		res, err := partitionClient.ShufflePartition(ctx, req)
		if err != nil {
			log.Printf("Something went wrong while running shuffling partition from worker %s: %e", w.Id, err)
			errors <- err
			return
		} else if res.Part != nil {
			if collectionLimit.TryAcquire(1) {
				part := partition.FromMetaMessage(res.Part, incomingSchema)
				transferReq := &pb.MTransferPartitionDataRequest{Id: res.Part.Id}
				stream, err := partitionClient.TransferPartitionData(ctx, transferReq)
				if err != nil {
					errors <- err
					return
				}
				err = part.ReceiveStreamedData(stream, res.Part)
				if err != nil {
					errors <- err
					return
				}
				if part.GetNumRows() > 0 {
					collectedLock.Lock()
					collected[res.Part.Id] = part.(sif.CollectedPartition)
					collectedLock.Unlock()
				}
			} else {
				break
			}
		}
		if !res.HasNext {
			break
		}
	}
}
