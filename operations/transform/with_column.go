package transform

import types "github.com/go-sif/sif/types"

// addColumnTask is a task that does nothing
type addColumnTask struct{}

// RunWorker for addColumnTask does nothing
func (s *addColumnTask) RunWorker(previous types.OperablePartition) ([]types.OperablePartition, error) {
	return []types.OperablePartition{previous}, nil
}

// AddColumn declares that a new (empty) column with a
// specific type and name should be available to the
// next Task of the DataFrame pipeline
func AddColumn(colName string, colType types.ColumnType) types.DataFrameOperation {
	return func(d types.DataFrame) (types.Task, string, types.Schema, error) {
		newSchema, err := d.GetSchema().Clone().CreateColumn(colName, colType)
		if err != nil {
			return nil, "", nil, err
		}
		nextTask := &addColumnTask{}
		return nextTask, "add_column", newSchema, nil
	}
}
