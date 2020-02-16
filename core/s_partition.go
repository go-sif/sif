package core

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
	planExecutor *planExecutor
	cache        map[string]itypes.TransferrablePartition
	cacheLock    sync.Mutex
}

// createPartitionServer creates a new partitionServer
func createPartitionServer(planExecutor *planExecutor) *partitionServer {
	return &partitionServer{planExecutor: planExecutor, cache: make(map[string]itypes.TransferrablePartition)}
}

// AssignPartition assigns a partition to a Worker
func (s *partitionServer) AssignPartition(ctx context.Context, req *pb.MAssignPartitionRequest) (*pb.MAssignPartitionResponse, error) {
	s.planExecutor.assignPartitionLoader(req.Loader)
	return &pb.MAssignPartitionResponse{}, nil
}

// ShufflePartition receives a request from another partitionServer, asking for a
// partition which belongs on the requester but exists on this server. The response
// contains a single partition if one is available to shuffle, along with a bool
// indicating whether or not another one is available.
func (s *partitionServer) ShufflePartition(ctx context.Context, req *pb.MShufflePartitionRequest) (*pb.MShufflePartitionResponse, error) {
	if !s.planExecutor.isShuffleReady() {
		return &pb.MShufflePartitionResponse{Ready: false}, nil
	}
	pi, err := s.planExecutor.getShufflePartitionIterator(req.Bucket)
	if err != nil {
		return nil, err
	}
	if !pi.HasNextPartition() {
		return &pb.MShufflePartitionResponse{Ready: true, HasNext: false, Part: nil}, nil
	}
	part, err := pi.NextPartition()
	if err != nil {
		return nil, err
	}
	tpart := part.(itypes.TransferrablePartition)
	s.cacheLock.Lock()
	s.cache[part.ID()] = tpart
	s.cacheLock.Unlock()
	return &pb.MShufflePartitionResponse{Ready: true, HasNext: pi.HasNextPartition(), Part: tpart.ToMetaMessage()}, nil
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
	partitionBytes := part.GetWidestSchema().Size() * part.GetNumRows()
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
	partitionMetaBytes := part.GetWidestSchema().NumFixedLengthColumns() * part.GetNumRows()
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
			if col, err := part.GetCurrentSchema().GetOffset(k); err == nil {
				if vcol, ok := col.Type().(sif.VarColumnType); ok {
					sdata, err := vcol.Serialize(v)
					if err != nil {
						return err
					}
					for j := 0; j < len(sdata); j += maxChunkBytes {
						end := j + maxChunkBytes
						if end > len(sdata) {
							end = len(sdata)
						}
						stream.Send(&pb.MPartitionChunk{
							Data:           sdata[j:end],
							DataType:       iutil.RowVarDataType,
							VarDataRowNum:  int32(i),
							VarDataColName: k,
							TotalSizeBytes: int32(len(sdata)),
							Append:         int32(j),
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
			stream.Send(&pb.MPartitionChunk{
				Data:           v,
				DataType:       iutil.RowVarDataType,
				VarDataRowNum:  int32(i),
				VarDataColName: k,
			})
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
