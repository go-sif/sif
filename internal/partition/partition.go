package partition

import (
	"fmt"
	"io"
	"log"

	"github.com/go-sif/sif"
	pb "github.com/go-sif/sif/internal/rpc"
	itypes "github.com/go-sif/sif/internal/types"
	iutil "github.com/go-sif/sif/internal/util"
	uuid "github.com/gofrs/uuid"
)

// partitionImpl is Sif's internal implementation of Partition
type partitionImpl struct {
	id                   string
	maxRows              int
	numRows              int
	rows                 []byte
	varRowData           []map[string]interface{}
	serializedVarRowData []map[string][]byte // for receiving serialized data from a shuffle (temporary)
	rowMeta              []byte
	widestSchema         sif.Schema
	currentSchema        sif.Schema
	keys                 []uint64
	isKeyed              bool
}

// createPartitionImpl creates a new Partition containing an empty byte array and a schema
func createPartitionImpl(maxRows int, widestSchema sif.Schema, currentSchema sif.Schema) *partitionImpl {
	id, err := uuid.NewV4()
	if err != nil {
		log.Fatalf("failed to generate UUID for Partition: %v", err)
	}
	return &partitionImpl{
		id:                   id.String(),
		maxRows:              maxRows,
		numRows:              0,
		rows:                 make([]byte, maxRows*widestSchema.Size(), maxRows*widestSchema.Size()),
		varRowData:           make([]map[string]interface{}, maxRows),
		serializedVarRowData: make([]map[string][]byte, maxRows),
		rowMeta:              make([]byte, maxRows*widestSchema.NumColumns()),
		widestSchema:         widestSchema,
		currentSchema:        currentSchema,
		keys:                 make([]uint64, 0),
		isKeyed:              false,
	}
}

// CreatePartition creates a new Partition containing an empty byte array and a schema
func CreatePartition(maxRows int, widestSchema sif.Schema, currentSchema sif.Schema) sif.Partition {
	return createPartitionImpl(maxRows, widestSchema, currentSchema)
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
	row.meta = p.GetRowMeta(rowNum)
	row.data = p.GetRowData(rowNum)
	row.varData = p.GetVarRowData(rowNum)
	row.serializedVarData = p.GetSerializedVarRowData(rowNum)
	row.schema = p.currentSchema
	return row
}

// GetRow retrieves a specific row from this Partition
func (p *partitionImpl) GetRow(rowNum int) sif.Row {
	return &rowImpl{
		meta:              p.GetRowMeta(rowNum),
		data:              p.GetRowData(rowNum),
		varData:           p.GetVarRowData(rowNum),
		serializedVarData: p.GetSerializedVarRowData(rowNum),
		schema:            p.currentSchema,
	}
}

// ToMetaMessage serializes metadata about this Partition to a protobuf message
func (p *partitionImpl) ToMetaMessage() *pb.MPartitionMeta {
	return &pb.MPartitionMeta{
		Id:        p.id,
		NumRows:   uint32(p.numRows),
		MaxRows:   uint32(p.maxRows),
		IsKeyed:   p.isKeyed,
		RowBytes:  uint32(len(p.rows)),
		MetaBytes: uint32(len(p.rowMeta)),
	}
}

// ReceiveStreamedData loads data from a protobuf stream into this Partition
func (p *partitionImpl) ReceiveStreamedData(stream pb.PartitionsService_TransferPartitionDataClient, incomingSchema sif.Schema) error {
	// stream data for Partition
	rowOffset := 0
	metaOffset := 0
	keyOffset := 0
	for chunk, err := stream.Recv(); err != io.EOF; chunk, err = stream.Recv() {
		if err != nil {
			// not an EOF, but something else
			return err
		}
		switch chunk.DataType {
		case iutil.RowDataType:
			copy(p.rows[rowOffset:rowOffset+len(chunk.Data)], chunk.Data)
			rowOffset += len(chunk.Data)
		case iutil.RowVarDataType:
			// Stream one key at a time, basically. Not efficient if people are
			// using var data to store small data, but better if they're storing
			// large data there. Data is streamed in chunks, especially if it's
			// bigger than the grpc max message size
			// When Partition is transferred over a network, all variable-length data is Gob-encoded.
			// We deserialize later, the first time they ask for a value from a Row, since that's when
			// we know the type they're looking for
			m := p.GetSerializedVarRowData(int(chunk.VarDataRowNum))
			if chunk.Append > 0 {
				copy(m[chunk.VarDataColName][chunk.Append:], chunk.Data)
			} else {
				m[chunk.VarDataColName] = make([]byte, chunk.TotalSizeBytes)
				copy(m[chunk.VarDataColName], chunk.Data)
			}
		case iutil.MetaDataType:
			copy(p.rowMeta[metaOffset:metaOffset+len(chunk.Data)], chunk.Data)
			metaOffset += len(chunk.Data)
		case iutil.KeyDataType:
			copy(p.keys[keyOffset:keyOffset+len(chunk.KeyData)], chunk.KeyData)
			keyOffset += len(chunk.KeyData)
		}
	}
	// confirm we received the correct number of rows
	if p.numRows != rowOffset/incomingSchema.Size() {
		return fmt.Errorf("Streamed %d rows for Partition %s. Expected %d", rowOffset/incomingSchema.Size(), p.id, p.numRows)
	} else if p.isKeyed && p.numRows != keyOffset {
		return fmt.Errorf("Streamed %d keys for Partition %s. Expected %d", keyOffset, p.id, p.numRows)
	}
	return nil
}

// FromMetaMessage deserializes a Partition from a protobuf message
func FromMetaMessage(m *pb.MPartitionMeta, currentSchema sif.Schema) itypes.TransferrablePartition {
	part := &partitionImpl{
		m.Id,
		int(m.MaxRows),
		int(m.NumRows),
		make([]byte, m.GetRowBytes()),
		make([]map[string]interface{}, int(m.MaxRows)),
		make([]map[string][]byte, int(m.MaxRows)),
		make([]byte, m.GetMetaBytes()),
		currentSchema,
		currentSchema,
		nil,
		m.IsKeyed,
	}
	if m.IsKeyed {
		part.keys = make([]uint64, int(m.MaxRows))
	}
	return part
}
