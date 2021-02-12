package util

import (
	"fmt"

	"github.com/go-sif/sif"
	"github.com/go-sif/sif/internal/pindex/hashmap"
)

type collectTask struct {
	collectionLimit int
}

func (s *collectTask) Name() string {
	return "collect"
}

func (s *collectTask) RunInitialize(sctx sif.StageContext) error {
	cache := sctx.PartitionCache()
	if cache == nil {
		return fmt.Errorf("Cannot initialize collectTask: StageContext does not contain a PartitionCache")
	}
	// populate StageContext with important configuration params
	err := sctx.SetCollectionLimit(s.collectionLimit)
	if err != nil {
		return err
	}
	// initialize Map-based PartitionIndex for collection
	err = sctx.SetPartitionIndex(hashmap.CreateMapPartitionIndex(cache, sctx.NextStageWidestInitialSchema()))
	if err != nil {
		return err
	}
	return nil
}

func (s *collectTask) RunWorker(sctx sif.StageContext, previous sif.OperablePartition) ([]sif.OperablePartition, error) {
	idx := sctx.PartitionIndex()
	rpart, ok := previous.(sif.ReduceablePartition)
	if !ok {
		return nil, fmt.Errorf("Partition is not reduceable")
	}
	if rpart.GetNumRows() > 0 && idx.CacheSize() < int(s.collectionLimit) {
		// repack if any columns have been removed
		if rpart.GetSchema().NumRemovedColumns() > 0 {
			repackedSchema := rpart.GetSchema().Repack()
			repackedPart, err := rpart.(sif.OperablePartition).Repack(repackedSchema)
			if err != nil {
				return nil, err
			}
			rpart = repackedPart.(sif.ReduceablePartition)
		}
		idx.MergePartition(rpart, nil, nil)
	}
	return nil, nil
}

// Collect declares that data should be shuffled to the Coordinator
// upon completion of the previous stage. This also signals
// the end of a Dataframe's tasks.
func Collect(collectionLimit int) *sif.DataFrameOperation {
	return &sif.DataFrameOperation{
		TaskType: sif.CollectTaskType,
		Do: func(d sif.DataFrame) (*sif.DataFrameOperationResult, error) {
			if d.GetDataSource().IsStreaming() {
				return nil, fmt.Errorf("Cannot collect() from a streaming DataSource")
			}
			return &sif.DataFrameOperationResult{
				Task:       &collectTask{collectionLimit},
				DataSchema: d.GetSchema().Clone(),
			}, nil
		},
	}
}
