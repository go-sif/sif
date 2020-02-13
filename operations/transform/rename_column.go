package transform

import (
	core "github.com/go-sif/sif/core"
)

// renameColumnTask is a task that does nothing
type renameColumnTask struct{}

// RunWorker for renameColumnTask does nothing
func (s *renameColumnTask) RunWorker(previous core.OperablePTition) ([]core.OperablePTition, error) {
	return []core.OperablePTition{previous}, nil
}

// RenameColumn renames an existing column
func RenameColumn(oldName string, newName string) core.DataFrameOperation {
	return func(d core.DataFrame) (core.Task, string, *core.Schema, error) {
		newSchema, err := d.GetSchema().Clone().RenameColumn(oldName, newName)
		if err != nil {
			return nil, "", nil, err
		}
		nextTask := &renameColumnTask{}
		return nextTask, "no_op", newSchema, nil
	}
}
