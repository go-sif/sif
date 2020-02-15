package transform

import (
	iutil "github.com/go-sif/sif/internal/util"
	types "github.com/go-sif/sif/types"
)

type flatMapTask struct {
	fn types.FlatMapOperation
}

func (s *flatMapTask) RunWorker(previous types.OperablePartition) ([]types.OperablePartition, error) {
	results, err := previous.FlatMapRows(s.fn)
	if err != nil {
		return nil, err
	}
	return results, nil
}

// FlatMap transforms a Row, potentially producing new rows
func FlatMap(fn types.FlatMapOperation) types.DataFrameOperation {
	return func(d types.DataFrame) (types.Task, string, types.Schema, error) {
		nextTask := flatMapTask{fn: iutil.SafeFlatMapOperation(fn)}
		return &nextTask, "flatmap", d.GetSchema().Clone(), nil
	}
}
