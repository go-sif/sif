package dsv

import (
	"encoding/csv"
	"io"
	"sync"

	core "github.com/go-sif/sif/core"
)

type dsvFilePartitionIterator struct {
	parser              *Parser
	reader              *csv.Reader
	hasNext             bool
	source              core.DataSource
	schema              *core.Schema
	widestInitialSchema *core.Schema
	lock                sync.Mutex
	endListeners        []func()
}

// OnEnd registers a listener which fires when this iterator runs out of Partitions
func (dsvi *dsvFilePartitionIterator) OnEnd(onEnd func()) {
	dsvi.lock.Lock()
	defer dsvi.lock.Unlock()
	dsvi.endListeners = append(dsvi.endListeners, onEnd)
}

// HasNextPartition returns true iff this PartitionIterator can produce another Partition
func (dsvi *dsvFilePartitionIterator) HasNextPartition() bool {
	dsvi.lock.Lock()
	defer dsvi.lock.Unlock()
	return dsvi.hasNext
}

// NextPartition returns the next Partition if one is available, or an error
func (dsvi *dsvFilePartitionIterator) NextPartition() (core.PTition, error) {
	dsvi.lock.Lock()
	defer dsvi.lock.Unlock()
	colNames := dsvi.schema.ColumnNames()
	colTypes := dsvi.schema.ColumnTypes()
	part := core.CreateBuildablePartition(dsvi.parser.PartitionSize(), dsvi.widestInitialSchema, dsvi.schema)
	// parse lines
	for {
		// If the partition is full, we're done
		if part.GetNumRows() == part.GetMaxRows() {
			return part, nil
		}
		// Otherwise, grab another line from the file
		rowStrings, err := dsvi.reader.Read()
		if err != nil && err == io.EOF {
			dsvi.hasNext = false
			for _, l := range dsvi.endListeners {
				l()
			}
			dsvi.endListeners = []func(){}
			// TODO have the other side discard empty partitions
			return part, nil
		} else if err != nil {
			return nil, err
		}
		// create a new row to place values into
		row, err := part.AppendEmptyRowData()
		if err != nil {
			return nil, err
		}
		err = scanRow(dsvi.parser.conf, colNames, colTypes, rowStrings, row)
		if err != nil {
			return nil, err
		}
	}
}
