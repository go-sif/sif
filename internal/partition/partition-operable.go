package partition

import (
	itypes "github.com/go-sif/sif/internal/types"
	types "github.com/go-sif/sif/types"
	"github.com/hashicorp/go-multierror"
)

// createOperablePartition creates a new Partition containing an empty byte array and a schema
func createOperablePartition(maxRows int, widestSchema types.Schema, currentSchema types.Schema) types.OperablePartition {
	return createPartitionImpl(maxRows, widestSchema, currentSchema)
}

// UpdateCurrentSchema updates the Schema of this Partition
func (p *partitionImpl) UpdateCurrentSchema(currentSchema types.Schema) {
	p.currentSchema = currentSchema
}

// MapRows runs a MapOperation on each row in this Partition, manipulating them in-place. Will fall back to creating a fresh partition if PartitionRowErrors occur.
func (p *partitionImpl) MapRows(fn types.MapOperation) (types.OperablePartition, error) {
	inPlace := true // start by attempting to manipulate rows in-place
	result := p
	var multierr *multierror.Error
	for i := 0; i < p.GetNumRows(); i++ {
		row := p.GetRow(i)
		err := fn(row)
		if err != nil {
			multierr = multierror.Append(multierr, err)
			// create a new partition and switch to non-in-place mode
			if inPlace {
				inPlace = false
				// immediately switch into creating a new Partition if we haven't already
				result := createPartitionImpl(p.maxRows, p.widestSchema, p.currentSchema)
				// append all rows we've successfully processed so far (up to this one)
				for j := 0; j < i; j++ {
					err := result.AppendRowData(p.GetRowData(j), p.GetRowMeta(j), p.GetVarRowData(j), p.GetSerializedVarRowData(j))
					if err != nil {
						return nil, err
					}
				}
			}
		} else if !inPlace { // if we're not in in-place mode, append successful rows to new Partition
			result.AppendRowData(p.GetRowData(i), p.GetRowMeta(i), p.GetVarRowData(i), p.GetSerializedVarRowData(i))
		}
	}
	return result, multierr.ErrorOrNil()
}

// FlatMapRows runs a FlatMapOperation on each row in this Partition, creating new Partitions
func (p *partitionImpl) FlatMapRows(fn types.FlatMapOperation) ([]types.OperablePartition, error) {
	var multierr *multierror.Error
	// factory for producing new rows compatible with this Partition
	factory := func() types.Row {
		return &rowImpl{
			meta:              make([]byte, p.widestSchema.NumColumns()),
			data:              make([]byte, p.widestSchema.Size()),
			varData:           make(map[string]interface{}),
			serializedVarData: make(map[string][]byte),
			schema:            p.currentSchema,
		}
	}
	parts := make([]types.OperablePartition, 1)
	parts = append(parts, createPartitionImpl(p.maxRows, p.widestSchema, p.currentSchema))
	for i := 0; i < p.GetNumRows(); i++ {
		newRows, err := fn(p.GetRow(i), factory)
		if err != nil {
			multierr = multierror.Append(multierr, err)
		} else {
			for _, row := range newRows {
				appendTarget := parts[len(parts)-1]
				if appendTarget.GetNumRows() >= appendTarget.GetMaxRows() {
					parts = append(parts, createPartitionImpl(p.maxRows, p.widestSchema, p.currentSchema))
					appendTarget = parts[len(parts)-1]
				}
				irow := row.(itypes.AccessibleRow)
				appendTarget.(types.BuildablePartition).AppendRowData(irow.GetData(), irow.GetMeta(), irow.GetVarData(), irow.GetSerializedVarData())
			}
		}
	}
	return parts, multierr.ErrorOrNil()
}

// FilterRows filters the Rows in the current Partition, creating a new one
func (p *partitionImpl) FilterRows(fn types.FilterOperation) (types.OperablePartition, error) {
	var multierr *multierror.Error
	result := createPartitionImpl(p.maxRows, p.widestSchema, p.currentSchema)
	for i := 0; i < p.GetNumRows(); i++ {
		shouldKeep, err := fn(p.GetRow(i))
		if err != nil {
			multierr = multierror.Append(multierr, err)
		}
		if shouldKeep {
			err := result.AppendRowData(p.GetRowData(i), p.GetRowMeta(i), p.GetVarRowData(i), p.GetSerializedVarRowData(i))
			// there's no way we can fill up this Partition, since we have to have fewer rows that
			// the current one, so this error should never happen
			if err != nil {
				return nil, err
			}
		}
	}
	return result, multierr.ErrorOrNil()
}

// Repack repacks a Partition according to a new Schema
func (p *partitionImpl) Repack(newSchema types.Schema) (types.OperablePartition, error) {
	// create a new Partition
	part := createPartitionImpl(p.maxRows, newSchema, newSchema)
	for i := 0; i < p.GetNumRows(); i++ {
		row := p.GetRow(i)
		newRow, err := row.Repack(newSchema)
		if err != nil {
			return nil, err
		}
		iNewRow := newRow.(itypes.AccessibleRow)
		err = part.AppendRowData(iNewRow.GetData(), iNewRow.GetMeta(), iNewRow.GetVarData(), iNewRow.GetSerializedVarData())
		if err != nil {
			return nil, err
		}
	}
	return part, nil
}
