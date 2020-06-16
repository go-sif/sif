package cluster

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/go-sif/sif"
	pb "github.com/go-sif/sif/internal/rpc"
	itypes "github.com/go-sif/sif/internal/types"
	iutil "github.com/go-sif/sif/internal/util"
)

type partitionServer struct {
	planExecutor          itypes.PlanExecutor
	cache                 map[string]itypes.TransferrablePartition
	cacheLock             sync.Mutex
	serializedAccumulator []byte
}

// createPartitionServer creates a new partitionServer
func createPartitionServer(planExecutor itypes.PlanExecutor) *partitionServer {
	return &partitionServer{planExecutor: planExecutor, cache: make(map[string]itypes.TransferrablePartition)}
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
	if !pi.HasNextPartition() {
		return &pb.MShufflePartitionResponse{Ready: true, HasNext: false, Part: nil}, nil
	}
	part, _, err := pi.NextPartition() // we don't need unlockPartition, because ShufflePartitionIterators remove the partition from the internal lru cache
	if err != nil {
		return nil, err
	}
	tpart := part.(itypes.TransferrablePartition)
	s.cacheLock.Lock()
	s.cache[part.ID()] = tpart
	s.cacheLock.Unlock()
	return &pb.MShufflePartitionResponse{
		Ready:   true,
		HasNext: pi.HasNextPartition(),
		Part:    tpart.ToMetaMessage(),
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
	part, ok := s.cache[req.Id]
	if !ok {
		s.cacheLock.Unlock()
		return fmt.Errorf("Partition id %s was not in the transfer cache", req.Id)
	}
	delete(s.cache, req.Id)
	s.cacheLock.Unlock()
	// 16-64kb is the ideal stream chunk size according to https://jbrandhorst.com/post/grpc-binary-blob-stream/
	maxChunkBytes := 63 * 1024 // leave room for 1kb of other things
	// transfer row data
	partitionBytes := part.GetSchema().Size() * part.GetNumRows()
	for i := 0; i < partitionBytes; i += maxChunkBytes {
		rowData := part.GetRowDataRange(i, i+maxChunkBytes)
		if len(rowData) == 0 {
			break
		}
		stream.Send(&pb.MPartitionChunk{
			Data:     rowData,
			DataType: iutil.RowDataType,
		})
	}
	// transfer meta data
	partitionMetaBytes := part.GetSchema().NumFixedLengthColumns() * part.GetNumRows()
	for i := 0; i < partitionMetaBytes; i += maxChunkBytes {
		metaData := part.GetRowMetaRange(i, i+maxChunkBytes)
		if len(metaData) == 0 {
			break
		}
		stream.Send(&pb.MPartitionChunk{
			Data:     metaData,
			DataType: iutil.MetaDataType,
		})
	}
	// transfer variable-length data
	for i := 0; i < part.GetNumRows(); i++ {
		varData := part.GetVarRowData(i)
		for k, v := range varData {
			// don't transfer nil values
			if v == nil {
				continue
			}
			// no need to serialize values for columns we've dropped
			if col, err := part.GetSchema().GetOffset(k); err == nil {
				if vcol, ok := col.Type().(sif.VarColumnType); ok {
					sdata, err := vcol.Serialize(v)
					if err != nil {
						return err
					}
					totalSize := len(sdata)
					for j := 0; j < totalSize; j += maxChunkBytes {
						end := j + maxChunkBytes
						if end > totalSize {
							end = totalSize
						}
						stream.Send(&pb.MPartitionChunk{
							Data:               sdata[j:end],
							DataType:           iutil.RowVarDataType,
							VarDataRowNum:      int32(i),
							VarDataColName:     k,
							TotalSizeBytes:     int32(totalSize),
							RemainingSizeBytes: int32(totalSize - end),
							Append:             int32(j),
						})
					}
				} else {
					log.Panicf("Column %s is not a variable-length type", k)
				}
			}
		}
		// transfer un-deserialized variable-length data (possible if never accessed after a reduction)
		svarData := part.GetSerializedVarRowData(i)
		for k, v := range svarData {
			// don't transfer nil values
			if v == nil {
				continue
			}
			if len(v) == 0 {
				return fmt.Errorf("Serialized column data for column %s in partition %s should not be zero-length", k, part.ID())
			}
			totalSize := len(v)
			for j := 0; j < totalSize; j += maxChunkBytes {
				end := j + maxChunkBytes
				if end > totalSize {
					end = totalSize
				}
				stream.Send(&pb.MPartitionChunk{
					Data:               v[j:end],
					DataType:           iutil.RowVarDataType,
					VarDataRowNum:      int32(i),
					VarDataColName:     k,
					TotalSizeBytes:     int32(totalSize),
					RemainingSizeBytes: int32(totalSize - end),
					Append:             int32(j),
				})
			}
		}
	}
	// transfer key data
	if part.GetIsKeyed() {
		maxChunkInts := maxChunkBytes / 8
		for i := 0; i < part.GetNumRows(); i += maxChunkInts {
			keyRange := part.GetKeyRange(i, i+maxChunkInts)
			if len(keyRange) == 0 {
				break
			}
			stream.Send(&pb.MPartitionChunk{
				KeyData:  keyRange,
				DataType: iutil.KeyDataType,
			})
		}
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
