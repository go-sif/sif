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
func Accumulate(facc sif.AccumulatorFactory) sif.DataFrameOperation {
	return func(d sif.DataFrame) (sif.Task, sif.TaskType, sif.Schema, error) {
		nextTask := accumulateTask{
			facc: facc,
		}
		return &nextTask, sif.AccumulateTaskType, d.GetSchema().Clone(), nil
	}
}
