package transform

import types "github.com/go-sif/sif/types"

// renameColumnTask is a task that does nothing
type renameColumnTask struct{}

// RunWorker for renameColumnTask does nothing
func (s *renameColumnTask) RunWorker(previous types.OperablePartition) ([]types.OperablePartition, error) {
	return []types.OperablePartition{previous}, nil
}

// RenameColumn renames an existing column
func RenameColumn(oldName string, newName string) types.DataFrameOperation {
	return func(d types.DataFrame) (types.Task, string, types.Schema, error) {
		newSchema, err := d.GetSchema().Clone().RenameColumn(oldName, newName)
		if err != nil {
			return nil, "", nil, err
		}
		nextTask := &renameColumnTask{}
		return nextTask, "no_op", newSchema, nil
	}
}
