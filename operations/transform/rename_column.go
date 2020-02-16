package transform

import "github.com/go-sif/sif"

// renameColumnTask is a task that does nothing
type renameColumnTask struct{}

// RunWorker for renameColumnTask does nothing
func (s *renameColumnTask) RunWorker(previous sif.OperablePartition) ([]sif.OperablePartition, error) {
	return []sif.OperablePartition{previous}, nil
}

// RenameColumn renames an existing column
func RenameColumn(oldName string, newName string) sif.DataFrameOperation {
	return func(d sif.DataFrame) (sif.Task, string, sif.Schema, error) {
		newSchema, err := d.GetSchema().Clone().RenameColumn(oldName, newName)
		if err != nil {
			return nil, "", nil, err
		}
		nextTask := &renameColumnTask{}
		return nextTask, "no_op", newSchema, nil
	}
}
