package util

import (
	"github.com/go-sif/sif"
)

type accumulateTask struct {
	facc sif.AccumulatorFactory
}

func (s *accumulateTask) RunWorker(previous sif.OperablePartition) ([]sif.OperablePartition, error) {
	return []sif.OperablePartition{previous}, nil
}

func (s *accumulateTask) GetAccumulatorFactory() sif.AccumulatorFactory {
	return s.facc
}

// Accumulate combines rows across workers, using a user-provided data structure
func Accumulate(facc sif.AccumulatorFactory) *sif.DataFrameOperation {
	return &sif.DataFrameOperation{
		TaskType: sif.AccumulateTaskType,
		Do: func(d sif.DataFrame) (*sif.DataFrameOperationResult, error) {
			return &sif.DataFrameOperationResult{
				Task: &accumulateTask{
					facc: facc,
				},
				PublicSchema:  d.GetPublicSchema().Clone(),
				PrivateSchema: d.GetPrivateSchema().Clone(),
			}, nil
		},
	}
}
