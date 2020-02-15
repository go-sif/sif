package transform

import types "github.com/go-sif/sif/types"

type repackTask struct {
	newSchema types.Schema
}

func (s *repackTask) RunWorker(previous types.OperablePartition) ([]types.OperablePartition, error) {
	part, err := previous.Repack(s.newSchema)
	if err != nil {
		return nil, err
	}
	return []types.OperablePartition{part}, nil
}

// Repack rearranges memory layout of rows to respect a new schema
func Repack() types.DataFrameOperation {
	return func(d types.DataFrame) (types.Task, string, types.Schema, error) {
		nextTask := repackTask{d.GetSchema().Repack()}
		return &nextTask, "repack", nextTask.newSchema, nil
	}
}
