package transform

import (
	"github.com/go-sif/sif"
	iutil "github.com/go-sif/sif/internal/util"
)

// RepartitionReduce is identical to Reduce, with the added ability to change the
// number of rows per partition during the reduction
func RepartitionReduce(targetPartitionSize int, kfn sif.KeyingOperation, fn sif.ReductionOperation) *sif.DataFrameOperation {
	return &sif.DataFrameOperation{
		TaskType: sif.ShuffleTaskType,
		Do: func(d sif.DataFrame) (*sif.DataFrameOperationResult, error) {
			return &sif.DataFrameOperationResult{
				Task: &reduceTask{
					kfn:                 iutil.SafeKeyingOperation(kfn),
					fn:                  iutil.SafeReductionOperation(fn),
					targetPartitionSize: targetPartitionSize,
				},
				PublicSchema:  d.GetPublicSchema().Clone(),
				PrivateSchema: d.GetPrivateSchema().Clone(),
			}, nil
		},
	}
}
