package partition

import (
	"fmt"

	errors "github.com/go-sif/sif/errors"
	"github.com/go-sif/sif/types"
)

// CreateBuildablePartition creates a new Partition containing an empty byte array and a schema
func CreateBuildablePartition(maxRows int, widestSchema types.Schema, currentSchema types.Schema) types.BuildablePartition {
	return createPartitionImpl(maxRows, widestSchema, currentSchema)
}

// CanInsertRowData checks if a Row can be inserted into this Partition
func (p *partitionImpl) CanInsertRowData(row []byte) error {
	// TODO accept and check variable length map for unknown keys
	if len(row) > p.widestSchema.Size() {
		return errors.IncompatibleRowError{}
	} else if p.numRows >= p.maxRows {
		return errors.PartitionFullError{}
	} else {
		return nil
	}
}

// AppendEmptyRowData is a convenient way to add an empty Row to the end of this Partition, returning the Row so that Row methods can be used to populate it
func (p *partitionImpl) AppendEmptyRowData() (types.Row, error) {
	newRowNum := p.numRows
	err := p.AppendRowData([]byte{0}, []byte{0}, make(map[string]interface{}), make(map[string][]byte))
	if err != nil {
		return nil, err
	}
	return p.GetRow(newRowNum), nil
}

// AppendRowData adds a Row to the end of this Partition, if it isn't full and if the Row fits within the schema
func (p *partitionImpl) AppendRowData(row []byte, meta []byte, varData map[string]interface{}, serializedVarRowData map[string][]byte) error {
	if err := p.CanInsertRowData(row); err != nil {
		return err
	}
	copy(p.rows[p.numRows*p.widestSchema.Size():(p.numRows+1)*p.widestSchema.Size()], row)
	copy(p.rowMeta[p.numRows*p.widestSchema.NumColumns():(p.numRows+1)*p.widestSchema.NumColumns()], meta)
	p.varRowData[p.numRows] = varData
	p.serializedVarRowData[p.numRows] = serializedVarRowData
	p.numRows++
	return nil
}

// AppendKeyedRowData appends a keyed Row to the end of this Partition
func (p *partitionImpl) AppendKeyedRowData(row []byte, meta []byte, varData map[string]interface{}, serializedVarRowData map[string][]byte, key uint64) error {
	if !p.isKeyed {
		return fmt.Errorf("Partition is not keyed")
	}
	err := p.AppendRowData(row, meta, varData, serializedVarRowData)
	if err != nil {
		return err
	}
	if p.isKeyed {
		p.keys[p.numRows-1] = key
	}
	return nil
}

// InsertRowData inserts a Row at a specific position within this Partition, if it isn't full and if the Row fits within the schema. Other Rows are shifted as necessary.
func (p *partitionImpl) InsertRowData(row []byte, meta []byte, varRowData map[string]interface{}, serializedVarRowData map[string][]byte, pos int) error {
	if err := p.CanInsertRowData(row); err != nil {
		return err
	}
	rowWidth := p.widestSchema.Size()
	numCols := p.widestSchema.NumColumns()
	// shift row data
	copy(p.rows[(pos+1)*rowWidth:], p.rows[pos*rowWidth:p.numRows*rowWidth])
	// insert row data
	copy(p.rows[pos*rowWidth:(pos+1)*rowWidth], row)
	// shift meta data
	copy(p.rowMeta[(pos+1)*numCols:], p.rowMeta[pos*numCols:p.numRows*numCols])
	// insert meta data
	copy(p.rowMeta[pos*numCols:(pos+1)*numCols], meta)
	// shift variable length row data
	copy(p.varRowData[pos+1:], p.varRowData[pos:p.numRows])
	// insert variable length row data
	p.varRowData[pos] = varRowData
	// shift serialized variable length row data
	copy(p.serializedVarRowData[pos+1:], p.serializedVarRowData[pos:p.numRows])
	// insert serialized variable length row data
	p.serializedVarRowData[pos] = serializedVarRowData
	p.numRows++
	return nil
}

// InsertKeyedRowData inserts a keyed Row into this Partition
func (p *partitionImpl) InsertKeyedRowData(row []byte, meta []byte, varData map[string]interface{}, serializedVarRowData map[string][]byte, key uint64, pos int) error {
	if !p.isKeyed {
		return fmt.Errorf("Partition is not keyed")
	}
	err := p.InsertRowData(row, meta, varData, serializedVarRowData, pos)
	if err != nil {
		return err
	}
	// shift
	copy(p.keys[pos+1:], p.keys[pos:p.numRows-1]) // we updated numRows already, so subtract
	// insert
	p.keys[pos] = key
	return nil
}
