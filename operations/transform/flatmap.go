package transform

import (
	"github.com/go-sif/sif"
	iutil "github.com/go-sif/sif/internal/util"
)

type flatMapTask struct {
	fn sif.FlatMapOperation
}

func (s *flatMapTask) RunInitialize(sctx sif.StageContext) error {
	return nil
}

func (s *flatMapTask) RunWorker(sctx sif.StageContext, previous sif.OperablePartition) ([]sif.OperablePartition, error) {
	results, err := previous.FlatMapRows(s.fn)
	if err != nil {
		return nil, err
	}
	return results, nil
}

// FlatMap transforms a Row, potentially producing new rows
func FlatMap(fn sif.FlatMapOperation) *sif.DataFrameOperation {
	return &sif.DataFrameOperation{
		TaskType: sif.FlatMapTaskType,
		Do: func(d sif.DataFrame) (*sif.DataFrameOperationResult, error) {
			return &sif.DataFrameOperationResult{
				Task:       &flatMapTask{fn: iutil.SafeFlatMapOperation(fn)},
				DataSchema: d.GetSchema().Clone(),
			}, nil
		},
	}
}
