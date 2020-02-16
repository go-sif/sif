package partition

import (
	"fmt"

	"github.com/go-sif/sif"
	itypes "github.com/go-sif/sif/internal/types"
)

// createCloneablePartition creates a new Partition containing an empty byte array and a schema
func createCloneablePartition(maxRows int, widestSchema sif.Schema, currentSchema sif.Schema) itypes.CloneablePartition {
	return createPartitionImpl(maxRows, widestSchema, currentSchema)
}

// GetRowMeta retrieves specific row metadata from this Partition
func (p *partitionImpl) GetRowMeta(rowNum int) []byte {
	start := rowNum * p.widestSchema.NumColumns()
	end := start + p.widestSchema.NumColumns()
	return p.rowMeta[start:end]
}

// GetRowMetaRange retrieves an arbitrary range of bytes from the row meta
func (p *partitionImpl) GetRowMetaRange(start int, end int) []byte {
	maxByte := p.numRows * p.widestSchema.NumColumns()
	if end > maxByte {
		end = maxByte
	}
	return p.rowMeta[start:end]
}

// GetRowData retrieves a specific row from this Partition
func (p *partitionImpl) GetRowData(rowNum int) []byte {
	start := rowNum * p.widestSchema.Size()
	end := start + p.widestSchema.Size()
	return p.rows[start:end]
}

// GetRowDataRange retrieves an arbitrary range of bytes from the row data
func (p *partitionImpl) GetRowDataRange(start int, end int) []byte {
	maxByte := p.numRows * p.widestSchema.Size()
	if end > maxByte {
		end = maxByte
	}
	return p.rows[start:end]
}

// GetVarRowData retrieves the variable-length data for a given row from this Partition
func (p *partitionImpl) GetVarRowData(rowNum int) map[string]interface{} {
	if p.varRowData[rowNum] == nil {
		p.varRowData[rowNum] = make(map[string]interface{})
	}
	return p.varRowData[rowNum]
}

// GetSerializedVarRowData retrieves the serialized variable-length data for a given row from this Partition
func (p *partitionImpl) GetSerializedVarRowData(rowNum int) map[string][]byte {
	if p.serializedVarRowData[rowNum] == nil {
		p.serializedVarRowData[rowNum] = make(map[string][]byte)
	}
	return p.serializedVarRowData[rowNum]
}

// GetCurrentSchema retrieves the Schema from the most recent task that manipulated this Partition
func (p *partitionImpl) GetCurrentSchema() sif.Schema {
	return p.currentSchema
}

// GetWidestSchema retrieves the widest Schema from the stage that produced this Partition, which is equal to the size of a row
func (p *partitionImpl) GetWidestSchema() sif.Schema {
	return p.widestSchema
}

// GetIsKeyed returns true iff this Partition has been keyed with KeyRows
func (p *partitionImpl) GetIsKeyed() bool {
	return p.isKeyed
}

// GetKey returns the shuffle key for a row, as generated by KeyRows
func (p *partitionImpl) GetKey(rowNum int) (uint64, error) {
	if !p.isKeyed {
		return 0, fmt.Errorf("Partition is not keyed")
	}
	return p.keys[rowNum], nil
}

// GetKeyRange returns a range of shuffle keys for a row, as generated by KeyRows, starting at rowNum
func (p *partitionImpl) GetKeyRange(rowNum int, numRows int) []uint64 {
	end := rowNum + numRows
	if end > p.numRows {
		end = p.numRows
	}
	return p.keys[rowNum:end]
}
