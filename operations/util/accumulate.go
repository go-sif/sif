package util

import (
	"github.com/go-sif/sif"
)

type accumulateTask struct {
	acc sif.Accumulator
}

// RunInitialize populates the StateContext with an Accumulator
func (s *accumulateTask) RunInitialize(sctx sif.StageContext) error {
	// populate StageContext with accumulator
	if err := sctx.SetAccumulator(s.acc); err != nil {
		return err
	}
	return nil
}

// RunWorker performs accumulation of incoming OperablePartitions
func (s *accumulateTask) RunWorker(sctx sif.StageContext, previous sif.OperablePartition) ([]sif.OperablePartition, error) {
	acc := sctx.Accumulator()
	_, err := previous.MapRows(func(row sif.Row) error {
		return acc.Accumulate(row)
	})
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// Accumulate is an alternative reduction technique, which siphons data from
// Partitions into a custom data structure. The result is itself an Accumulator,
// rather than a series of Partitions, thus ending the job (no more operations may)
// be performed against the data. The advantage, however, is full control over the
// reduction technique, which can yield substantial performance benefits.
// As reduction is performed locally on all workers, then worker results are
// all reduced on the Coordinator, Accumulators are best utilized for smaller
// results. Distributed reductions via Reduce() are more efficient when
// there is a large reduction result (e.g. a large number of buckets).
func Accumulate(facc sif.AccumulatorFactory) *sif.DataFrameOperation {
	return &sif.DataFrameOperation{
		TaskType: sif.AccumulateTaskType,
		Do: func(d sif.DataFrame) (*sif.DataFrameOperationResult, error) {
			return &sif.DataFrameOperationResult{
				Task: &accumulateTask{
					acc: facc(),
				},
				DataSchema: d.GetSchema().Clone(),
			}, nil
		},
	}
}
