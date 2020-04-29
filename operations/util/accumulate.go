package util

import (
	"github.com/go-sif/sif"
)

type accumulateTask struct {
	acc sif.Accumulator
}

func (s *accumulateTask) RunWorker(previous sif.OperablePartition) ([]sif.OperablePartition, error) {
	_, err := previous.MapRows(func(row sif.Row) error {
		return s.acc.Accumulate(row)
	})
	if err != nil {
		return nil, err
	}
	return []sif.OperablePartition{previous}, nil
}

func (s *accumulateTask) GetAccumulator() sif.Accumulator {
	return s.acc
}

// Accumulate combines rows across workers, using a user-provided data structure
func Accumulate(facc sif.AccumulatorFactory) *sif.DataFrameOperation {
	return &sif.DataFrameOperation{
		TaskType: sif.AccumulateTaskType,
		Do: func(d sif.DataFrame) (*sif.DataFrameOperationResult, error) {
			return &sif.DataFrameOperationResult{
				Task: &accumulateTask{
					acc: facc(),
				},
				PublicSchema:  d.GetPublicSchema().Clone(),
				PrivateSchema: d.GetPrivateSchema().Clone(),
			}, nil
		},
	}
}
