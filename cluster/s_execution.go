package cluster

import (
	"context"
	"fmt"
	"log"
	"sync"

	pb "github.com/go-sif/sif/internal/rpc"
	"github.com/go-sif/sif/internal/stats"
	itypes "github.com/go-sif/sif/internal/types"
	iutil "github.com/go-sif/sif/internal/util"
	logging "github.com/go-sif/sif/logging"
	"github.com/hashicorp/go-multierror"
)

type executionServer struct {
	planExecutor itypes.PlanExecutor
	logClient    pb.LogServiceClient
	statsTracker *stats.RunStatistics
}

// createExecutionServer creates a new execution server
func createExecutionServer(logClient pb.LogServiceClient, planExecutor itypes.PlanExecutor, statsTracker *stats.RunStatistics) *executionServer {
	return &executionServer{logClient: logClient, planExecutor: planExecutor, statsTracker: statsTracker}
}

// RunStage executes a stage on a Worker
func (s *executionServer) RunStage(ctx context.Context, req *pb.MRunStageRequest) (*pb.MRunStageResponse, error) {
	if !s.planExecutor.HasNextStage() {
		return nil, fmt.Errorf("Plan Executor %s does not have a next stage to run (stage %d expected)", s.planExecutor.ID(), req.StageId)
	}
	onRowErrorWithContext := func(err error) error {
		return s.onRowError(ctx, err)
	}
	stage := s.planExecutor.GetNextStage()
	if stage.ID() != int(req.StageId) {
		return nil, fmt.Errorf("Next stage on worker (%d) does not match expected (%d)", stage.ID(), req.StageId)
	}
	log.Println("------------------------------")
	log.Printf("Running stage %d...", req.StageId)
	defer func() {
		log.Printf("Finished running stage %d", req.StageId)
		log.Println("------------------------------")
	}()
	s.statsTracker.StartStage()
	s.statsTracker.StartTransform()
	log.Printf("Mapping partitions in stage %d...", req.StageId)
	err := s.planExecutor.FlatMapPartitions(stage.WorkerExecute, req, onRowErrorWithContext)
	if err != nil {
		if _, ok := err.(*multierror.Error); !s.planExecutor.GetConf().IgnoreRowErrors || !ok {
			// either this isn't a multierr or we're supposed to fail immediately
			s.statsTracker.EndTransform(stage.ID())
			log.Printf("Failed to map in stage %d: %e", req.StageId, err)
			return nil, err
		}
	}
	log.Printf("Finished mapping partitions in stage %d", req.StageId)
	s.statsTracker.EndTransform(stage.ID())
	if req.RunShuffle {
		log.Printf("Shuffling partitions in stage %d...", req.StageId)
		s.statsTracker.StartShuffle()
		err = s.runShuffle(ctx, req)
		if err != nil {
			s.statsTracker.EndShuffle(stage.ID())
			log.Printf("Failed to shuffle in stage %d: %e", req.StageId, err)
			return nil, err
		}
		log.Printf("Finished shuffling partitions in stage %d", req.StageId)
		s.statsTracker.EndShuffle(stage.ID())
	}
	s.statsTracker.EndStage(stage.ID())
	return &pb.MRunStageResponse{}, nil
}

func (s *executionServer) onRowError(ctx context.Context, err error) (outgoingErr error) {
	defer func() {
		if r := recover(); r != nil {
			if anErr, ok := r.(error); ok {
				outgoingErr = anErr
			} else {
				outgoingErr = fmt.Errorf("Panic was not an error")
			}
		}
	}()
	// if this is a multierror, it's from a row transformation, which we might want to ignore
	if multierr, ok := err.(*multierror.Error); s.planExecutor.GetConf().IgnoreRowErrors && ok {
		multierr.ErrorFormat = iutil.FormatMultiError
		// log errors and carry on
		logger, err := s.logClient.Log(ctx)
		if err != nil {
			return err
		}
		err = logger.Send(&pb.MLogMsg{
			Level:   logging.ErrorLevel,
			Source:  s.planExecutor.ID(),
			Message: fmt.Sprintf("Map error in stage %d:\n%s", s.planExecutor.GetCurrentStage().ID(), multierr.Error()),
		})
		if err != nil {
			return err
		}
		_, err = logger.CloseAndRecv()
		if err != nil {
			return err
		}
	} else {
		// otherwise, crash immediately
		return err
	}
	return nil
}

// runShuffle executes a prepared shuffle on a Worker
func (s *executionServer) runShuffle(ctx context.Context, req *pb.MRunStageRequest) (err error) {
	// build list of workers to communicate with
	buckets := make([]uint64, 0)
	targets := make([]pb.PartitionsServiceClient, 0)
	for i := 0; i < len(req.Buckets); i++ {
		if req.Buckets[i] != req.AssignedBucket {
			conn, err := dialWorker(req.Workers[i])
			partitionClient := pb.NewPartitionsServiceClient(conn)
			if err != nil {
				return err
			}
			targets = append(targets, partitionClient)
			buckets = append(buckets, req.Buckets[i])
			defer conn.Close()
		}
	}
	// assign bucket to self
	s.planExecutor.AssignShuffleBucket(req.AssignedBucket)
	// start partition merger, which merges partitions into our tree sequentially
	var fetchWg, mergeWg sync.WaitGroup
	asyncFetchErrors := make(chan error, len(buckets)) // each fetch goroutine can send one error before terminating
	asyncMergeErrors := make(chan error, 1)            // the merge goroutine can only send one error before terminating
	partChan := make(chan itypes.ReduceablePartition, len(buckets))
	// setup our cleanup in the correct order (read in reverse)
	defer close(asyncMergeErrors)
	// check for remaining merge errors
	defer func() {
		select {
		case mergeErr := <-asyncMergeErrors:
			if err := s.onRowError(ctx, mergeErr); err != nil {
				err = mergeErr
			}
		default:
		}
	}()
	defer close(asyncFetchErrors) // finally, close error channels
	// check for remaining fetch errors
	defer func() {
		select {
		case fetchErr := <-asyncFetchErrors:
			if err := s.onRowError(ctx, fetchErr); err != nil {
				err = fetchErr
			}
		default:
		}
	}()
	defer mergeWg.Wait()  // then, wait for any outstanding merges to finish
	defer close(partChan) // then, close the partition channel
	defer fetchWg.Wait()  // first, wait for all fetches to finish
	mergeWg.Add(1)
	go s.planExecutor.MergeShuffledPartitions(&mergeWg, partChan, asyncMergeErrors)
	// round-robin request partitions
	t := 0
	concurrentFetchLimit := 4
	activeFetches := 0
	for {
		if len(targets) == 0 {
			break
		}
		shuffleReq := &pb.MShufflePartitionRequest{Bucket: req.AssignedBucket}
		res, err := targets[t].ShufflePartition(ctx, shuffleReq)
		if err != nil {
			return err
		} else if !res.Ready {
			continue // TODO maybe skip worker for a while?
		} else if res.Part != nil {
			transferReq := &pb.MTransferPartitionDataRequest{Id: res.Part.Id}
			// initiate request to shuffle partition data
			stream, err := targets[t].TransferPartitionData(ctx, transferReq)
			if err != nil {
				return err
			}
			// split off goroutine to actually transfer the data
			fetchWg.Add(1)
			activeFetches++
			go s.planExecutor.ShufflePartitionData(&fetchWg, partChan, asyncFetchErrors, res.Part, stream)
		}
		// if the target worker has no more partitions, take it out of the rotation
		if !res.HasNext {
			// remove target from rotation
			copy(buckets[t:], buckets[t+1:])
			buckets = buckets[:len(buckets)-1]
			copy(targets[t:], targets[t+1:])
			targets[len(targets)-1] = nil // for garbage collection
			targets = targets[:len(targets)-1]
		}
		// block every time we've fetched one partition from each target,
		// so we don't create too many goroutines for ShufflePartitionData
		if activeFetches >= concurrentFetchLimit {
			fetchWg.Wait()
			// reset counter
			activeFetches = 0
			// check fetch errors
			select {
			case fetchErr := <-asyncFetchErrors:
				if err := s.onRowError(ctx, fetchErr); err != nil {
					return err
				}
			default:
			}
			// check merge errors
			select {
			case mergeErr := <-asyncMergeErrors:
				if mergeErr != nil {
					return mergeErr
				}
			default:
			}
		}
		if len(targets) > 0 {
			t = (t + 1) % len(targets)
		}
	}
	return nil
}
