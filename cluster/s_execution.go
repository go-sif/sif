package cluster

import (
	"context"
	"fmt"

	pb "github.com/go-sif/sif/internal/rpc"
	itypes "github.com/go-sif/sif/internal/types"
	iutil "github.com/go-sif/sif/internal/util"
	logging "github.com/go-sif/sif/logging"
	"github.com/hashicorp/go-multierror"
)

type executionServer struct {
	planExecutor itypes.PlanExecutor
	logClient    pb.LogServiceClient
}

// createExecutionServer creatse a new execution server
func createExecutionServer(logClient pb.LogServiceClient, planExecutor itypes.PlanExecutor) *executionServer {
	return &executionServer{logClient: logClient, planExecutor: planExecutor}
}

// RunStage executes a stage on a Worker
func (s *executionServer) RunStage(ctx context.Context, req *pb.MRunStageRequest) (*pb.MRunStageResponse, error) {
	if !s.planExecutor.HasNextStage() {
		return nil, fmt.Errorf("Plan Executor %s does not have a next stage to run (stage %s expected)", s.planExecutor.ID(), req.StageId)
	}
	stage := s.planExecutor.GetNextStage()
	if stage.ID() != req.StageId {
		return nil, fmt.Errorf("Next stage on worker (%s) does not match expected (%s)", stage.ID(), req.StageId)
	}
	err := s.planExecutor.FlatMapPartitions(ctx, stage.WorkerExecute, s.logClient, req)
	if err != nil {
		if _, ok := err.(*multierror.Error); !s.planExecutor.GetConf().IgnoreRowErrors || !ok {
			// either this isn't a multierr or we're supposed to fail immediately
			return nil, err
		}
	}
	if req.RunShuffle {
		err = s.runShuffle(ctx, req)
		if err != nil {
			return nil, err
		}
	}
	return &pb.MRunStageResponse{}, nil
}

// runShuffle executes a prepared shuffle on a Worker
func (s *executionServer) runShuffle(ctx context.Context, req *pb.MRunStageRequest) error {
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
	// round-robin request partitions
	t := 0
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
			// shuffle partition into my local tree
			stream, err := targets[t].TransferPartitionData(ctx, transferReq)
			if err != nil {
				return err
			}
			err = s.planExecutor.AcceptShuffledPartition(res.Part, stream)
			if err != nil {
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
						Message: fmt.Sprintf("Shuffle error in stage %s:\n%s", s.planExecutor.GetCurrentStage().ID(), multierr.Error()),
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
			}
		}
		if !res.HasNext {
			// remove target from rotation
			copy(buckets[t:], buckets[t+1:])
			buckets = buckets[:len(buckets)-1]
			copy(targets[t:], targets[t+1:])
			targets[len(targets)-1] = nil // for garbage collection
			targets = targets[:len(targets)-1]
		}
		if len(targets) > 0 {
			t = (t + 1) % len(targets)
		}
	}
	return nil
}
