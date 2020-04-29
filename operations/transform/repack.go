package transform

import (
	"github.com/go-sif/sif"
)

type repackTask struct {
	newSchema sif.Schema
}

func (s *repackTask) RunWorker(previous sif.OperablePartition) ([]sif.OperablePartition, error) {
	part, err := previous.Repack(s.newSchema)
	if err != nil {
		return nil, err
	}
	return []sif.OperablePartition{part}, nil
}

// Repack rearranges memory layout of rows to respect a new schema
func Repack() sif.DataFrameOperation {
	return func(d sif.DataFrame) (*sif.DataFrameOperationResult, error) {
		newSchema := d.GetPublicSchema().Repack()
		return &sif.DataFrameOperationResult{
			Task:          &repackTask{newSchema},
			TaskType:      sif.RepackTaskType,
			PublicSchema:  newSchema,
			PrivateSchema: newSchema,
		}, nil
	}
}
