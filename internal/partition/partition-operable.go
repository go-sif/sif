package partition

import (
	"sync"

	"github.com/go-sif/sif"
	itypes "github.com/go-sif/sif/internal/types"
	"github.com/hashicorp/go-multierror"
)

// createOperablePartition creates a new Partition containing an empty byte array and a schema
func createOperablePartition(maxRows int, schema sif.Schema) sif.OperablePartition {
	return createPartitionImpl(maxRows, schema)
}

// UpdateSchema updates the Schema of this Partition
func (p *partitionImpl) UpdateSchema(schema sif.Schema) {
	p.schema = schema
}

// MapRows runs a MapOperation on each row in this Partition, manipulating them in-place. Will fall back to creating a fresh partition if PartitionRowErrors occur.
func (p *partitionImpl) MapRows(fn sif.MapOperation) (sif.OperablePartition, error) {
	inPlace := true // start by attempting to manipulate rows in-place
	result := p
	var multierr *multierror.Error
	row := &rowImpl{}
	for i := 0; i < p.GetNumRows(); i++ {
		row := p.getRow(row, i)
		err := fn(row)
		if err != nil {
			multierr = multierror.Append(multierr, err)
			// create a new partition and switch to non-in-place mode
			if inPlace {
				inPlace = false
				// immediately switch into creating a new Partition if we haven't already
				result = createPartitionImpl(p.maxRows, p.schema)
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
func (p *partitionImpl) FlatMapRows(fn sif.FlatMapOperation) ([]sif.OperablePartition, error) {
	var multierr *multierror.Error
	parts := make([]sif.OperablePartition, 0, 1)
	parts = append(parts, createPartitionImpl(p.maxRows, p.schema))
	// some temp Row structs we can re-use
	row := &rowImpl{}
	factoryRow := &rowImpl{}
	rowsProduced := 0
	// factory for producing new Partitions in advance of when they are needed
	tempParts := make([]sif.OperablePartition, 0, 8)
	var tempPartsLock sync.Mutex
	partFactory := func(num int) {
		tempPartsLock.Lock()
		defer tempPartsLock.Unlock()
		for i := 0; i < num; i++ {
			tempParts = append(tempParts, createPartitionImpl(p.maxRows, p.schema))
		}
	}
	go partFactory(1)
	// factory for producing new rows compatible with this Partition
	rowFactory := func() sif.Row {
		appendTarget := parts[len(parts)-1]
		// allocate a new partition if this one is full
		if appendTarget.GetNumRows() >= appendTarget.GetMaxRows() {
			var makeNewPart bool
			tempPartsLock.Lock()
			tempPart := tempParts[0]
			tempParts = tempParts[1:]
			makeNewPart = len(tempParts) == 0
			tempPartsLock.Unlock()
			parts = append(parts, tempPart)
			if makeNewPart {
				go partFactory(1) // make a new partition in the background
			}
			appendTarget = parts[len(parts)-1]
		}
		// we have room, so allocate a new row
		res, err := appendTarget.(sif.BuildablePartition).AppendEmptyRowData(factoryRow)
		// this should never happen, since we checked if the partition had room
		if err != nil {
			panic(err)
		}
		rowsProduced++
		return res
	}
	for i := 0; i < p.GetNumRows(); i++ {
		rowsProduced = 0
		err := fn(p.getRow(row, i), rowFactory)
		// try to guess how many partitions we'll need to make, based on
		// the first few rows, under the assumption that each row will be
		// mapped to a similar number of rows
		tempPartsLock.Lock()
		if rowsProduced > len(tempParts) && i < 5 {
			go partFactory(rowsProduced - len(tempParts))
		}
		tempPartsLock.Unlock()
		if err != nil {
			multierr = multierror.Append(multierr, err)
			// wind back all rows added by factory function
			// start by checking if we can just throw away the tail "new partition"
			appendTarget := parts[len(parts)-1]
			if rowsProduced > appendTarget.GetNumRows() {
				rowsProduced -= appendTarget.GetNumRows()
				parts = parts[0 : len(parts)-2]
				appendTarget = parts[len(parts)-1]
			}
			// now zero out rows in the tail "new partition"
			appendTarget.(sif.BuildablePartition).TruncateRowData(rowsProduced)
		}
	}
	return parts, multierr.ErrorOrNil()
}

// FilterRows filters the Rows in the current Partition, creating a new one
func (p *partitionImpl) FilterRows(fn sif.FilterOperation) (sif.OperablePartition, error) {
	var multierr *multierror.Error
	result := createPartitionImpl(p.maxRows, p.schema)
	row := &rowImpl{}
	for i := 0; i < p.GetNumRows(); i++ {
		shouldKeep, err := fn(p.getRow(row, i))
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
func (p *partitionImpl) Repack(newSchema sif.Schema) (sif.OperablePartition, error) {
	// create a new Partition
	part := createPartitionImpl(p.maxRows, newSchema)
	row := &rowImpl{}
	for i := 0; i < p.GetNumRows(); i++ {
		row := p.getRow(row, i).(itypes.AccessibleRow)
		newRow, err := row.Repack(newSchema)
		if err != nil {
			return nil, err
		}
		aNewRow := newRow.(itypes.AccessibleRow)
		err = part.AppendRowData(aNewRow.GetData(), aNewRow.GetMeta(), aNewRow.GetVarData(), aNewRow.GetSerializedVarData())
		if err != nil {
			return nil, err
		}
	}
	if p.isKeyed {
		part.isKeyed = true
		part.keys = make([]uint64, len(p.keys))
		copy(part.keys, p.keys)
	}
	return part, nil
}
