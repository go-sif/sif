package transform

import (
	"github.com/go-sif/sif"
	iutil "github.com/go-sif/sif/internal/util"
)

type repartitionTask struct {
	kfn sif.KeyingOperation
}

func (s *repartitionTask) RunWorker(previous sif.OperablePartition) ([]sif.OperablePartition, error) {
	// Start by keying the rows in the partition
	part, err := previous.KeyRows(s.kfn)
	if err != nil {
		return nil, err
	}
	return []sif.OperablePartition{part}, nil
}

func (s *repartitionTask) GetKeyingOperation() sif.KeyingOperation {
	return s.kfn
}

func (s *repartitionTask) GetReductionOperation() sif.ReductionOperation {
	return nil
}

// Repartition shuffles rows across workers, using a key - useful for grouping buckets of data together on single workers
func Repartition(kfn sif.KeyingOperation) sif.DataFrameOperation {
	return func(d sif.DataFrame) (sif.Task, sif.TaskType, sif.Schema, error) {
		nextTask := repartitionTask{
			kfn: iutil.SafeKeyingOperation(kfn),
		}
		return &nextTask, sif.ShuffleTaskType, d.GetSchema().Clone(), nil
	}
}
