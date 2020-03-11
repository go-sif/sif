package transform

import (
	"github.com/go-sif/sif"
	iutil "github.com/go-sif/sif/internal/util"
)

// RepartitionReduce is identical to Reduce, with the added ability to change the
// number of rows per partition during the reduction
func RepartitionReduce(targetPartitionSize int, kfn sif.KeyingOperation, fn sif.ReductionOperation) sif.DataFrameOperation {
	return func(d sif.DataFrame) (sif.Task, sif.TaskType, sif.Schema, error) {
		nextTask := reduceTask{
			kfn:                 iutil.SafeKeyingOperation(kfn),
			fn:                  iutil.SafeReductionOperation(fn),
			targetPartitionSize: targetPartitionSize,
		}
		return &nextTask, sif.ShuffleTaskType, d.GetSchema().Clone(), nil
	}
}
