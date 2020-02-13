package transform

import (
	core "github.com/go-sif/sif/core"
)

// withColumnTask is a task that does nothing
type withColumnTask struct{}

// RunWorker for withColumnTask does nothing
func (s *withColumnTask) RunWorker(previous *core.Partition) ([]*core.Partition, error) {
	return []*core.Partition{previous}, nil
}

// WithColumn declares that a new (empty) column with a
// specific type and name should be available to the
// next Task of the DataFrame pipeline
func WithColumn(colName string, colType core.ColumnType) core.DataFrameOperation {
	return func(d *core.DataFrame) (core.Task, string, *core.Schema, error) {
		newSchema, err := d.GetSchema().Clone().CreateColumn(colName, colType)
		if err != nil {
			return nil, "", nil, err
		}
		nextTask := &withColumnTask{}
		return nextTask, "with_column", newSchema, nil
	}
}
