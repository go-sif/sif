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

// ToMetaMessage serializes metadata about this Partition to a protobuf message
func (p *partitionImpl) ToMetaMessage() *pb.MPartitionMeta {
	return &pb.MPartitionMeta{
		Id:        p.id,
		NumRows:   uint32(p.numRows),
		MaxRows:   uint32(p.maxRows),
		Capacity:  uint32(p.capacity),
		IsKeyed:   p.isKeyed,
		RowBytes:  uint32(len(p.rows)),
		MetaBytes: uint32(len(p.rowMeta)),
	}
}

// ReceiveStreamedData loads data from a protobuf stream into this Partition
func (p *partitionImpl) ReceiveStreamedData(stream pb.PartitionsService_TransferPartitionDataClient, partitionMeta *pb.MPartitionMeta) error {
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
			if chunk.Append <= 0 {
				m[chunk.VarDataColName] = make([]byte, 0, chunk.TotalSizeBytes)
			} else if chunk.Append > 0 && (m[chunk.VarDataColName] == nil || len(m[chunk.VarDataColName]) == 0) {
				return fmt.Errorf("Received chunk for column %s to be appended at %d, but there is no data to append to", chunk.VarDataColName, chunk.Append)
			}
			if len(chunk.Data) == 0 {
				return fmt.Errorf("Streamed 0-length chunk for column %s. Remaining bytes: %d/%d", chunk.VarDataColName, chunk.RemainingSizeBytes, chunk.TotalSizeBytes)
			}
			m[chunk.VarDataColName] = append(m[chunk.VarDataColName], chunk.Data...)
			// validate length when we've finished streaming
			if chunk.RemainingSizeBytes == 0 && int32(len(m[chunk.VarDataColName])) != chunk.TotalSizeBytes {
				return fmt.Errorf("Streamed %d total bytes for column %s. Expected %d. Append was: %d", int32(len(m[chunk.VarDataColName])), chunk.VarDataColName, chunk.TotalSizeBytes, chunk.Append)
			}
		case iutil.MetaDataType:
			copy(p.rowMeta[metaOffset:metaOffset+len(chunk.Data)], chunk.Data)
			metaOffset += len(chunk.Data)
		case iutil.KeyDataType:
			copy(p.keys[keyOffset:keyOffset+len(chunk.KeyData)], chunk.KeyData)
			keyOffset += len(chunk.KeyData)
		}
	}
	// confirm we received the correct amount of data
	if uint32(len(p.rows)) != partitionMeta.GetRowBytes() {
		return fmt.Errorf("Streamed %d bytes for fixed-width data in Partition %s. Expected %d", rowOffset, p.id, partitionMeta.GetRowBytes())
	} else if uint32(len(p.rowMeta)) != partitionMeta.GetMetaBytes() {
		return fmt.Errorf("Streamed %d bytes for metadata in Partition %s. Expected %d", metaOffset, p.id, partitionMeta.GetMetaBytes())
	}
	return nil
}

// FromMetaMessage deserializes a Partition from a protobuf message
func FromMetaMessage(m *pb.MPartitionMeta, currentSchema sif.Schema) itypes.TransferrablePartition {
	part := &partitionImpl{
		id:                   m.Id,
		maxRows:              int(m.MaxRows),
		numRows:              int(m.NumRows),
		capacity:             int(m.Capacity),
		rows:                 make([]byte, m.GetRowBytes()),
		varRowData:           make([]map[string]interface{}, int(m.Capacity)),
		serializedVarRowData: make([]map[string][]byte, int(m.Capacity)),
		rowMeta:              make([]byte, m.GetMetaBytes()),
		schema:               currentSchema,
		keys:                 nil,
		isKeyed:              m.IsKeyed,
	}
	if m.IsKeyed {
		part.keys = make([]uint64, int(m.Capacity))
	}
	return part
}
