package transform

import (
	iutil "github.com/go-sif/sif/internal/util"
	types "github.com/go-sif/sif/types"
)

type mapTask struct {
	fn types.MapOperation
}

func (s *mapTask) RunWorker(previous types.OperablePartition) ([]types.OperablePartition, error) {
	next, err := previous.MapRows(s.fn)
	if err != nil {
		return nil, err
	}
	return []types.OperablePartition{next}, nil
}

// Map transforms a Row in-place
func Map(fn types.MapOperation) types.DataFrameOperation {
	return func(d types.DataFrame) (types.Task, string, types.Schema, error) {
		nextTask := mapTask{fn: iutil.SafeMapOperation(fn)}
		return &nextTask, "map", d.GetSchema().Clone(), nil
	}
}
