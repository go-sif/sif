package partition

import (
	"fmt"
	"io"
	"log"

	"github.com/go-sif/sif"
	pb "github.com/go-sif/sif/internal/rpc"
	iutil "github.com/go-sif/sif/internal/util"
	uuid "github.com/gofrs/uuid"
)

const defaultCapacity = 2

// partitionImpl is Sif's internal implementation of Partition
type partitionImpl struct {
	id                   string
	maxRows              int
	numRows              int
	capacity             int
	rows                 []byte
	varRowData           []map[string]interface{}
	serializedVarRowData []map[string][]byte // for receiving serialized data from a shuffle (temporary)
	rowMeta              []byte
	schema               sif.Schema
	keys                 []uint64
	isKeyed              bool
}

// createPartitionImpl creates a new Partition containing an empty byte array and a schema
func createPartitionImpl(maxRows int, initialCapacity int, schema sif.Schema) *partitionImpl {
	id, err := uuid.NewV4()
	if err != nil {
		log.Fatalf("failed to generate UUID for Partition: %v", err)
	}
	if initialCapacity > maxRows {
		initialCapacity = maxRows
	}
	return &partitionImpl{
		id:                   id.String(),
		maxRows:              maxRows,
		numRows:              0,
		capacity:             initialCapacity,
		rows:                 make([]byte, initialCapacity*schema.Size(), initialCapacity*schema.Size()),
		varRowData:           make([]map[string]interface{}, initialCapacity),
		serializedVarRowData: make([]map[string][]byte, initialCapacity),
		rowMeta:              make([]byte, initialCapacity*schema.NumColumns()),
		schema:               schema,
		keys:                 make([]uint64, 0),
		isKeyed:              false,
	}
}

// CreatePartition creates a new Partition containing an empty byte array and a schema
func CreatePartition(maxRows int, initialCapacity int, schema sif.Schema) sif.Partition {
	return createPartitionImpl(maxRows, initialCapacity, schema)
}

// ID retrieves the ID of this Partition
func (p *partitionImpl) ID() string {
	return p.id
}

// GetMaxRows retrieves the maximum number of rows in this Partition
func (p *partitionImpl) GetMaxRows() int {
	return p.maxRows
}

// GetNumRows retrieves the number of rows in this Partition
func (p *partitionImpl) GetNumRows() int {
	return p.numRows
}

// getRowInternal retrieves a specific row from this Partition, without allocation
func (p *partitionImpl) getRow(row *rowImpl, rowNum int) sif.Row {
	row.partID = p.id
	row.meta = p.GetRowMeta(rowNum)
	row.data = p.GetRowData(rowNum)
	row.varData = p.GetVarRowData(rowNum)
	row.serializedVarData = p.GetSerializedVarRowData(rowNum)
	row.schema = p.schema
	return row
}

// GetRow retrieves a specific row from this Partition
func (p *partitionImpl) GetRow(rowNum int) sif.Row {
	return &rowImpl{
		partID:            p.id,
		meta:              p.GetRowMeta(rowNum),
		data:              p.GetRowData(rowNum),
		varData:           p.GetVarRowData(rowNum),
		serializedVarData: p.GetSerializedVarRowData(rowNum),
		schema:            p.schema,
	}
}

// ReceiveStreamedData loads data from a protobuf stream into this Partition
func ReceiveStreamedData(stream pb.PartitionsService_TransferPartitionDataClient, partitionMeta *pb.MPartitionMeta) (sif.Partition, error) {
	// stream data for Partition
	dataOffset := 0
	buff := make([]byte, partitionMeta.GetBytes())
	for chunk, err := stream.Recv(); err != io.EOF; chunk, err = stream.Recv() {
		if err != nil {
			// not an EOF, but something else
			return nil, err
		}
		switch chunk.DataType {
		case iutil.SerializedPartitionDataType:
			copy(buff[dataOffset:dataOffset+len(chunk.Data)], chunk.Data)
			dataOffset += len(chunk.Data)
		default:
			return nil, fmt.Errorf("Unknown chunk data type encountered: %d", chunk.DataType)
		}
	}
	// confirm we received the correct amount of data
	if uint32(dataOffset) != partitionMeta.GetBytes() {
		return nil, fmt.Errorf("Streamed %d bytes for SerializedPartition %s. Expected %d", dataOffset, partitionMeta.GetId(), partitionMeta.GetBytes())
	}
	// TODO deserialize
	return part, nil
}
