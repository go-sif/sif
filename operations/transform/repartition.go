package transform

import (
	"github.com/go-sif/sif"
	iutil "github.com/go-sif/sif/internal/util"
)

type repartitionTask struct {
	kfn                 sif.KeyingOperation
	targetPartitionSize int
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

func (s *repartitionTask) GetTargetPartitionSize() int {
	return s.targetPartitionSize
}

// Repartition is identical to Group, with the added ability to change the
// number of rows per partition during the shuffle
func Repartition(targetPartitionSize int, kfn sif.KeyingOperation) *sif.DataFrameOperation {
	return &sif.DataFrameOperation{
		TaskType: sif.ShuffleTaskType,
		Do: func(d sif.DataFrame) (*sif.DataFrameOperationResult, error) {
			return &sif.DataFrameOperationResult{
				Task: &repartitionTask{
					kfn:                 iutil.SafeKeyingOperation(kfn),
					targetPartitionSize: targetPartitionSize,
				},
				DataSchema: d.GetSchema().Clone(),
			}, nil
		},
	}
}
