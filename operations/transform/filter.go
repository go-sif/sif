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
func Filter(fn sif.FilterOperation) *sif.DataFrameOperation {
	return &sif.DataFrameOperation{
		TaskType: sif.FilterTaskType,
		Do: func(d sif.DataFrame) (*sif.DataFrameOperationResult, error) {
			return &sif.DataFrameOperationResult{
				Task:       &filterTask{fn: iutil.SafeFilterOperation(fn)},
				DataSchema: d.GetSchema().Clone(),
			}, nil
		},
	}
}
