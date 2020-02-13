package core

import (
	"context"
	"fmt"

	pb "github.com/go-sif/sif/core/internal/rpc"
	logging "github.com/go-sif/sif/logging"
	"github.com/hashicorp/go-multierror"
)

//go:generate protoc --proto_path=./rpc_proto --go_out=plugins=grpc:./internal/rpc s_execution.proto

type executionServer struct {
	planExecutor *planExecutor
	logClient    pb.LogServiceClient
}

// RunStage executes a stage on a Worker
func (s *executionServer) RunStage(ctx context.Context, req *pb.MRunStageRequest) (*pb.MRunStageResponse, error) {
	if !s.planExecutor.hasNextStage() {
		return nil, fmt.Errorf("Plan Executor %s does not have a next stage to run (stage %s expected)", s.planExecutor.id, req.StageId)
	}
	stage := s.planExecutor.getNextStage()
	if stage.id != req.StageId {
		return nil, fmt.Errorf("Next stage on worker (%s) does not match expected (%s)", stage.id, req.StageId)
	}
	err := s.planExecutor.flatMapPartitions(ctx, stage.workerExecute, s.logClient, req.RunShuffle, req.PrepCollect, req.Buckets, req.Workers)
	if err != nil {
		if _, ok := err.(*multierror.Error); !s.planExecutor.conf.ignoreRowErrors || !ok {
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
	s.planExecutor.assignShuffleBucket(req.AssignedBucket)
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
			err = s.planExecutor.acceptShuffledPartition(res.Part, stream)
			if err != nil {
				// if this is a multierror, it's from a row transformation, which we might want to ignore
				if multierr, ok := err.(*multierror.Error); s.planExecutor.conf.ignoreRowErrors && ok {
					multierr.ErrorFormat = formatMultiError
					// log errors and carry on
					logger, err := s.logClient.Log(ctx)
					if err != nil {
						return err
					}
					err = logger.Send(&pb.MLogMsg{
						Level:   logging.ErrorLevel,
						Source:  s.planExecutor.id,
						Message: fmt.Sprintf("Shuffle error in stage %s:\n%s", s.planExecutor.getCurrentStage().id, multierr.Error()),
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
