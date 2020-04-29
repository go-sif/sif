package transform

import (
	"github.com/go-sif/sif"
	iutil "github.com/go-sif/sif/internal/util"
)

type mapTask struct {
	fn sif.MapOperation
}

func (s *mapTask) RunWorker(previous sif.OperablePartition) ([]sif.OperablePartition, error) {
	next, err := previous.MapRows(s.fn)
	if err != nil {
		return nil, err
	}
	return []sif.OperablePartition{next}, nil
}

// Map transforms a Row in-place
func Map(fn sif.MapOperation) sif.DataFrameOperation {
	return func(d sif.DataFrame) (*sif.DataFrameOperationResult, error) {
		return &sif.DataFrameOperationResult{
			Task:          &mapTask{fn: iutil.SafeMapOperation(fn)},
			TaskType:      sif.MapTaskType,
			PublicSchema:  d.GetPublicSchema().Clone(),
			PrivateSchema: d.GetPrivateSchema().Clone(),
		}, nil
	}
}
