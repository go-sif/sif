package transform

import (
	core "github.com/go-sif/sif/core"
)

// addColumnTask is a task that does nothing
type addColumnTask struct{}

// RunWorker for addColumnTask does nothing
func (s *addColumnTask) RunWorker(previous *core.Partition) ([]*core.Partition, error) {
	return []*core.Partition{previous}, nil
}

// AddColumn declares that a new (empty) column with a
// specific type and name should be available to the
// next Task of the DataFrame pipeline
func AddColumn(colName string, colType core.ColumnType) core.DataFrameOperation {
	return func(d core.DataFrame) (core.Task, string, *core.Schema, error) {
		newSchema, err := d.GetSchema().Clone().CreateColumn(colName, colType)
		if err != nil {
			return nil, "", nil, err
		}
		nextTask := &addColumnTask{}
		return nextTask, "add_column", newSchema, nil
	}
}
