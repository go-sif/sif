package transform

import (
	"github.com/go-sif/sif"
)

// addColumnTask is a task that does nothing
type addColumnTask struct{}

// RunWorker for addColumnTask does nothing
func (s *addColumnTask) RunWorker(previous sif.OperablePartition) ([]sif.OperablePartition, error) {
	return []sif.OperablePartition{previous}, nil
}

// AddColumn declares that a new (empty) column with a
// specific type and name should be available to the
// next Task of the DataFrame pipeline
func AddColumn(colName string, colType sif.ColumnType) sif.DataFrameOperation {
	return func(d sif.DataFrame) (sif.Task, sif.TaskType, sif.Schema, error) {
		newSchema, err := d.GetSchema().Clone().CreateColumn(colName, colType)
		if err != nil {
			return nil, "", nil, err
		}
		nextTask := &addColumnTask{}
		return nextTask, sif.NoOpTaskType, newSchema, nil
	}
}
