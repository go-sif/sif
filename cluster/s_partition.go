package cluster

import (
	"context"
	"fmt"
	"sync"

	pb "github.com/go-sif/sif/internal/rpc"
	itypes "github.com/go-sif/sif/internal/types"
	iutil "github.com/go-sif/sif/internal/util"
)

type partitionServer struct {
	planExecutor          itypes.PlanExecutor
	cache                 map[string]*shuffleablePartition
	cacheLock             sync.Mutex
	serializedAccumulator []byte
}

type shuffleablePartition struct {
	buff []byte
	done func()
}

// createPartitionServer creates a new partitionServer
func createPartitionServer(planExecutor itypes.PlanExecutor) *partitionServer {
	return &partitionServer{planExecutor: planExecutor, cache: make(map[string]*shuffleablePartition)}
}

// AssignPartition assigns a partition to a Worker
func (s *partitionServer) AssignPartition(ctx context.Context, req *pb.MAssignPartitionRequest) (*pb.MAssignPartitionResponse, error) {
	s.planExecutor.AssignPartitionLoader(req.Loader)
	return &pb.MAssignPartitionResponse{}, nil
}

// ShufflePartition receives a request from another partitionServer, asking for a
// partition which belongs on the requester but exists on this server. The response
// contains metadata for a single partition if one is available to shuffle, along with a bool
// indicating whether or not another one is available.
func (s *partitionServer) ShufflePartition(ctx context.Context, req *pb.MShufflePartitionRequest) (*pb.MShufflePartitionResponse, error) {
	if !s.planExecutor.IsShuffleReady() {
		return &pb.MShufflePartitionResponse{Ready: false}, nil
	}
	pi, err := s.planExecutor.GetShufflePartitionIterator(req.Bucket)
	if err != nil {
		return nil, err
	}
	if !pi.HasNextSerializedPartition() {
		return &pb.MShufflePartitionResponse{Ready: true, HasNext: false, Part: nil}, nil
	}
	partID, spart, done, err := pi.NextSerializedPartition() // we don't need unlockPartition, because ShufflePartitionIterators remove the partition from the internal lru cache
	if err != nil {
		return nil, err
	}
	s.cacheLock.Lock()
	s.cache[partID] = &shuffleablePartition{
		buff: spart,
		done: done,
	}
	s.cacheLock.Unlock()
	return &pb.MShufflePartitionResponse{
		Ready:   true,
		HasNext: pi.HasNextSerializedPartition(),
		Part: &pb.MPartitionMeta{
			Id:    partID,
			Bytes: uint32(len(spart)),
		},
	}, nil
}

// ShuffleAccumulator shuffles an Accumulator
func (s *partitionServer) ShuffleAccumulator(ctx context.Context, req *pb.MShuffleAccumulatorRequest) (*pb.MShuffleAccumulatorResponse, error) {
	if !s.planExecutor.IsAccumulatorReady() {
		return &pb.MShuffleAccumulatorResponse{Ready: false}, nil
	}
	acc := s.planExecutor.GetAccumulator()
	if acc == nil {
		return nil, fmt.Errorf("Accumulator marked as ready, but is not available")
	}
	serializedAccumulator, err := acc.ToBytes()
	s.serializedAccumulator = serializedAccumulator
	if err != nil {
		return nil, fmt.Errorf("Unable to serialize Accumulator for transfer to coordinator")
	}
	return &pb.MShuffleAccumulatorResponse{
		Ready:          true,
		TotalSizeBytes: int32(len(s.serializedAccumulator)),
	}, nil
}

// TransferPartitionData streams Partition data from the cache to the requester
func (s *partitionServer) TransferPartitionData(req *pb.MTransferPartitionDataRequest, stream pb.PartitionsService_TransferPartitionDataServer) error {
	s.cacheLock.Lock()
	spart, ok := s.cache[req.Id]
	if !ok {
		s.cacheLock.Unlock()
		return fmt.Errorf("Partition id %s was not in the transfer cache", req.Id)
	}
	delete(s.cache, req.Id)
	defer spart.done()
	s.cacheLock.Unlock()
	// 16-64kb is the ideal stream chunk size according to https://jbrandhorst.com/post/grpc-binary-blob-stream/
	maxChunkBytes := 63 * 1024 // leave room for 1kb of other things
	// transfer row data
	sPartitionBytes := len(spart.buff)
	for i := 0; i < sPartitionBytes; i += maxChunkBytes {
		end := i + maxChunkBytes
		if end > sPartitionBytes {
			end = sPartitionBytes
		}
		rowData := spart.buff[i:end]
		if len(rowData) == 0 {
			break
		}
		stream.Send(&pb.MPartitionChunk{
			Data:     rowData,
			DataType: iutil.SerializedPartitionDataType,
		})
	}
	return nil
}

// TransferPartitionData streams Accumulator data from the planExecutor to the requester
func (s *partitionServer) TransferAccumulatorData(req *pb.MTransferAccumulatorDataRequest, stream pb.PartitionsService_TransferAccumulatorDataServer) error {
	// 16-64kb is the ideal stream chunk size according to https://jbrandhorst.com/post/grpc-binary-blob-stream/
	maxChunkBytes := 63 * 1024 // leave room for 1kb of other things
	// transfer accumulator data
	if s.serializedAccumulator == nil {
		return fmt.Errorf("Unable to transfer accumulator data because accumulator is not ready for transfer")
	}
	for i := 0; i < len(s.serializedAccumulator); i += maxChunkBytes {
		end := i + maxChunkBytes
		if end > len(s.serializedAccumulator) {
			end = len(s.serializedAccumulator)
		}
		chunk := s.serializedAccumulator[i:end]
		if len(chunk) == 0 {
			break
		}
		stream.Send(&pb.MAccumulatorChunk{
			Data: chunk,
		})
	}
	return nil
}
