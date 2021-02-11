package transform

import (
	"github.com/go-sif/sif"
	iutil "github.com/go-sif/sif/internal/util"
)

type mapTask struct {
	fn sif.MapOperation
}

func (s *mapTask) RunInitialize(sctx sif.StageContext) error {
	return nil
}

func (s *mapTask) RunWorker(sctx sif.StageContext, previous sif.OperablePartition) ([]sif.OperablePartition, error) {
	next, err := previous.MapRows(s.fn)
	if err != nil {
		return nil, err
	}
	return []sif.OperablePartition{next}, nil
}

// Map transforms a Row in-place
func Map(fn sif.MapOperation) *sif.DataFrameOperation {
	return &sif.DataFrameOperation{
		TaskType: sif.MapTaskType,
		Do: func(d sif.DataFrame) (*sif.DataFrameOperationResult, error) {
			return &sif.DataFrameOperationResult{
				Task:       &mapTask{fn: iutil.SafeMapOperation(fn)},
				DataSchema: d.GetSchema().Clone(),
			}, nil
		},
	}
}
