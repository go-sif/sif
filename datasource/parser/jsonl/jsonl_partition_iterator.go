package jsonl

import (
	"bufio"
	"log"
	"sync"

	"github.com/go-sif/sif"
	"github.com/go-sif/sif/datasource"
	"github.com/tidwall/gjson"
)

type jsonlFilePartitionIterator struct {
	parser              *Parser
	scanner             *bufio.Scanner
	hasNext             bool
	source              sif.DataSource
	schema              sif.Schema
	widestInitialSchema sif.Schema
	lock                sync.Mutex
	endListeners        []func()
}

// OnEnd registers a listener which fires when this iterator runs out of Partitions
func (jsonli *jsonlFilePartitionIterator) OnEnd(onEnd func()) {
	jsonli.lock.Lock()
	defer jsonli.lock.Unlock()
	jsonli.endListeners = append(jsonli.endListeners, onEnd)
}

// HasNextPartition returns true iff this PartitionIterator can produce another Partition
func (jsonli *jsonlFilePartitionIterator) HasNextPartition() bool {
	jsonli.lock.Lock()
	defer jsonli.lock.Unlock()
	return jsonli.hasNext
}

// NextPartition returns the next Partition if one is available, or an error
func (jsonli *jsonlFilePartitionIterator) NextPartition() (sif.Partition, error) {
	jsonli.lock.Lock()
	defer jsonli.lock.Unlock()
	colNames := jsonli.schema.ColumnNames()
	colTypes := jsonli.schema.ColumnTypes()
	part := datasource.CreateBuildablePartition(jsonli.parser.PartitionSize(), jsonli.widestInitialSchema)
	// parse lines
	tempRow := datasource.CreateTempRow()
	for {
		// If the partition is full, we're done
		if part.GetNumRows() == part.GetMaxRows() {
			return part, nil
		}
		// Otherwise, grab another line from the file
		hasNext := jsonli.scanner.Scan()
		if !hasNext {
			jsonli.hasNext = false
			for _, l := range jsonli.endListeners {
				l()
			}
			jsonli.endListeners = []func(){}
			// TODO have the other side discard empty partitions
			return part, nil
		}
		if err := jsonli.scanner.Err(); err != nil {
			return nil, err
		}
		rowString := jsonli.scanner.Text()
		// create a new row to place values into
		row, err := part.AppendEmptyRowData(tempRow)
		if err != nil {
			return nil, err
		}
		err = ParseJSONRow(colNames, colTypes, gjson.Parse(rowString), row)
		if err != nil {
			log.Printf("Unable to parse line:\n\t%s", rowString)
			return nil, err
		}
	}
}
