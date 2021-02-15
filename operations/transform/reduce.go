package transform

import (
	"fmt"

	"github.com/cespare/xxhash/v2"
	"github.com/go-sif/sif"
	"github.com/go-sif/sif/internal/pindex/bucketed"
	"github.com/go-sif/sif/internal/pindex/tree"
	itypes "github.com/go-sif/sif/internal/types"
	iutil "github.com/go-sif/sif/internal/util"
)

// With inspiration from:
// https://blog.cloudera.com/blog/2015/01/improving-sort-performance-in-apache-spark-its-a-double/
// https://github.com/cespare/xxhash/v2

type reduceTask struct {
	kfn                 sif.KeyingOperation
	fn                  sif.ReductionOperation
	targetPartitionSize int
}

// RunInitialize populates the StateContext for reduction,
// with a reduction-oriented BucketedPartitionIndex and
// relevant configuration parameters
func (s *reduceTask) RunInitialize(sctx sif.StageContext) error {
	// set up operations in StageContext
	if err := sctx.SetKeyingOperation(s.kfn); err != nil {
		return err
	}
	if err := sctx.SetReductionOperation(s.fn); err != nil {
		return err
	}
	if err := sctx.SetTargetPartitionSize(s.targetPartitionSize); err != nil {
		return err
	}
	// fetch the PartitionCache from the StageContext so we can initialize a PartitionIndex
	cache := sctx.PartitionCache()
	if cache == nil {
		return fmt.Errorf("Cannot initialize reduceTask: StageContext does not contain a PartitionCache")
	}
	sctx.SetTargetPartitionSize(s.targetPartitionSize)
	// create shuffle bucket-based PartitionIndex and set it in the StageContext
	bucketFactory := func() sif.PartitionIndex {
		return tree.CreateTreePartitionIndex(cache, s.targetPartitionSize, sctx.NextStageWidestInitialSchema())
	}
	err := sctx.SetPartitionIndex(bucketed.CreateBucketedPartitionIndex(sctx.ShuffleBuckets(), bucketFactory, sctx.NextStageWidestInitialSchema()))
	if err != nil {
		return err
	}
	return nil
}

// RunWorker reduces OperablePartitions into the StageContext's BucketedPartitionIndex
func (s *reduceTask) RunWorker(sctx sif.StageContext, previous sif.OperablePartition) ([]sif.OperablePartition, error) {
	if previous.GetNumRows() == 0 {
		return nil, nil
	}
	// key the rows in the partition
	part, err := previous.KeyRows(s.kfn)
	if err != nil {
		return nil, err
	}
	rpart, ok := part.(sif.ReduceablePartition)
	if !ok {
		return nil, fmt.Errorf("Partition is not reduceable")
	}
	// If no s.targetPartitionSize was set (-1), then we should default to the existing size of the incoming partitions
	if s.targetPartitionSize < 0 {
		s.targetPartitionSize = rpart.GetMaxRows()
		sctx.PartitionIndex().SetMaxRows(s.targetPartitionSize)
	}
	// we might need to repack the partition before shuffling,
	// if this stage removed columns and/or the next stage adds any
	nextStageWidestInitialSchema := sctx.NextStageWidestInitialSchema()
	if rpart.GetSchema().NumRemovedColumns() > 0 || rpart.GetSchema().Equals(nextStageWidestInitialSchema) != nil {
		repackedPart, err := part.(sif.OperablePartition).Repack(nextStageWidestInitialSchema)
		if err != nil {
			return nil, err
		}
		rpart = repackedPart.(sif.ReduceablePartition)
	}
	// merge partition into the index
	pi := sctx.PartitionIndex()
	err = pi.MergePartition(rpart, sctx.KeyingOperation(), sctx.ReductionOperation())
	if err != nil {
		return nil, err
	}
	return []sif.OperablePartition{part}, nil
}

// Reduce combines rows across workers, using a key
func Reduce(kfn sif.KeyingOperation, fn sif.ReductionOperation) *sif.DataFrameOperation {
	return &sif.DataFrameOperation{
		TaskType: sif.ShuffleTaskType,
		Do: func(d sif.DataFrame) (*sif.DataFrameOperationResult, error) {
			return &sif.DataFrameOperationResult{
				Task: &reduceTask{
					kfn:                 iutil.SafeKeyingOperation(kfn),
					fn:                  iutil.SafeReductionOperation(fn),
					targetPartitionSize: -1,
				},
				DataSchema: d.GetSchema().Clone(),
			}, nil
		},
	}
}

// KeyColumns is a shortcut for defining a KeyingOperation which uses multiple source
// column values to produce a compound key.
func KeyColumns(colNames ...string) sif.KeyingOperation {
	return func(row sif.Row) ([]byte, error) {
		irow := row.(itypes.AccessibleRow) // access row internals
		hasher := xxhash.New()
		for _, cname := range colNames {
			data, err := irow.GetColData(cname)
			if err != nil {
				return nil, err
			}
			hasher.Write(data)
		}
		return hasher.Sum(nil), nil
	}
}
