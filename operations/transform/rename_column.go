package transform

import (
	"github.com/go-sif/sif"
)

// renameColumnTask is a task that does nothing
type renameColumnTask struct{}

// RunWorker for renameColumnTask does nothing
func (s *renameColumnTask) RunWorker(previous sif.OperablePartition) ([]sif.OperablePartition, error) {
	return []sif.OperablePartition{previous}, nil
}

// RenameColumn renames an existing column
func RenameColumn(oldName string, newName string) *sif.DataFrameOperation {
	return &sif.DataFrameOperation{
		TaskType: sif.RenameColumnTaskType,
		Do: func(d sif.DataFrame) (*sif.DataFrameOperationResult, error) {
			newPublicSchema, err := d.GetPublicSchema().Clone().RenameColumn(oldName, newName)
			if err != nil {
				return nil, err
			}
			newPrivateSchema, err := d.GetPrivateSchema().Clone().RenameColumn(oldName, newName)
			if err != nil {
				return nil, err
			}
			return &sif.DataFrameOperationResult{
				Task:          &renameColumnTask{},
				PublicSchema:  newPublicSchema,
				PrivateSchema: newPrivateSchema,
			}, nil
		},
	}
}
