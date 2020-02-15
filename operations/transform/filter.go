package transform

import (
	types "github.com/go-sif/sif/types"
	iutil "github.com/go-sif/sif/internal/util"
)

type filterTask struct {
	fn types.FilterOperation
}

func (s *filterTask) RunWorker(previous types.OperablePartition) ([]types.OperablePartition, error) {
	result, err := previous.FilterRows(s.fn)
	if err != nil {
		return nil, err
	}
	return []types.OperablePartition{result}, nil
}

// Filter filters Rows out of a Partition, creating a new one
func Filter(fn types.FilterOperation) types.DataFrameOperation {
	return func(d types.DataFrame) (types.Task, string, types.Schema, error) {
		nextTask := filterTask{fn: iutil.SafeFilterOperation(fn)}
		return &nextTask, "filter", d.GetSchema().Clone(), nil
	}
}
