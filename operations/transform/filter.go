package transform

import (
	"github.com/go-sif/sif"
	iutil "github.com/go-sif/sif/internal/util"
)

type filterTask struct {
	fn sif.FilterOperation
}

func (s *filterTask) RunWorker(previous sif.OperablePartition) ([]sif.OperablePartition, error) {
	result, err := previous.FilterRows(s.fn)
	if err != nil {
		return nil, err
	}
	return []sif.OperablePartition{result}, nil
}

// Filter filters Rows out of a Partition, creating a new one
func Filter(fn sif.FilterOperation) sif.DataFrameOperation {
	return func(d sif.DataFrame) (sif.Task, string, sif.Schema, error) {
		nextTask := filterTask{fn: iutil.SafeFilterOperation(fn)}
		return &nextTask, "filter", d.GetSchema().Clone(), nil
	}
}
