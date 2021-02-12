package cluster

import (
	"context"
	"fmt"
	"time"

	"github.com/go-sif/sif"
	itypes "github.com/go-sif/sif/internal/types"
)

type stageContextKey string

const stageContextStateKey = "sif.cluster.stageContextState"

type stageContextState struct {
	stage                        itypes.Stage
	nextStageWidestInitialSchema sif.Schema
	shuffleBuckets               []uint64
	pCache                       sif.PartitionCache
	pIndex                       sif.PartitionIndex
	pIncoming                    sif.PartitionIterator
	keyFn                        sif.KeyingOperation
	reduceFn                     sif.ReductionOperation
	accumulator                  sif.Accumulator
	collectionLimit              int
	targetPartitionSize          int
}

type stageContextImpl struct {
	ctx   context.Context
	state *stageContextState
}

// TODO put all vals into a struct and just store/lookup the struct once when rehydrating a stagecontext from a context. saves on casts.
func createStageContext(ctx context.Context, stage itypes.Stage) sif.StageContext {
	// if ctx already contains a StageContext, rehydrate it
	var state *stageContextState
	if i := ctx.Value(stageContextStateKey); i != nil {
		state = i.(*stageContextState)
	} else {
		// othwerwise, create a fresh one
		state = &stageContextState{
			stage:               stage,
			collectionLimit:     -1,
			targetPartitionSize: -1,
		}
	}
	return &stageContextImpl{
		ctx:   ctx,
		state: state,
	}
}

func (s *stageContextImpl) Deadline() (deadline time.Time, ok bool) {
	return s.ctx.Deadline()
}

func (s *stageContextImpl) Done() <-chan struct{} {
	return s.ctx.Done()
}

func (s *stageContextImpl) Err() error {
	return s.ctx.Err()
}

func (s *stageContextImpl) Value(key interface{}) interface{} {
	return s.ctx.Value(key)
}

func (s *stageContextImpl) NextStageWidestInitialSchema() sif.Schema {
	return s.state.nextStageWidestInitialSchema
}

func (s *stageContextImpl) SetNextStageWidestInitialSchema(schema sif.Schema) error {
	if s.NextStageWidestInitialSchema() != nil {
		return fmt.Errorf("Cannot overwrite widest initial Schema for next Stage (already set)")
	}
	s.state.nextStageWidestInitialSchema = schema
	return nil
}

func (s *stageContextImpl) ShuffleBuckets() []uint64 {
	return s.state.shuffleBuckets
}

func (s *stageContextImpl) SetShuffleBuckets(buckets []uint64) error {
	if s.ShuffleBuckets() != nil {
		return fmt.Errorf("Cannot overwrite shuffle buckets for Stage (already set)")
	}
	s.state.shuffleBuckets = buckets
	return nil
}

func (s *stageContextImpl) PartitionCache() sif.PartitionCache {
	return s.state.pCache
}

func (s *stageContextImpl) SetPartitionCache(cache sif.PartitionCache) error {
	if s.PartitionCache() != nil {
		return fmt.Errorf("Cannot overwrite PartitionCache for Stage (already set)")
	}
	s.state.pCache = cache
	return nil
}

func (s *stageContextImpl) PartitionIndex() sif.PartitionIndex {
	return s.state.pIndex
}

func (s *stageContextImpl) SetPartitionIndex(idx sif.PartitionIndex) error {
	if s.PartitionIndex() != nil {
		return fmt.Errorf("Cannot overwrite PartitionIndex for Stage (already set)")
	}
	s.state.pIndex = idx
	return nil
}

func (s *stageContextImpl) IncomingPartitionIterator() sif.PartitionIterator {
	return s.state.pIncoming
}

func (s *stageContextImpl) SetIncomingPartitionIterator(i sif.PartitionIterator) error {
	if s.IncomingPartitionIterator() != nil {
		return fmt.Errorf("Cannot overwrite IncomingPartitionIterator for Stage (already set)")
	}
	s.state.pIncoming = i
	return nil
}

// KeyingOperation retrieves the KeyingOperation for this Stage (if it exists)
func (s *stageContextImpl) KeyingOperation() sif.KeyingOperation {
	return s.state.keyFn
}

// Configure the keying operation for the end of this stage
func (s *stageContextImpl) SetKeyingOperation(val sif.KeyingOperation) error {
	if s.KeyingOperation() != nil {
		return fmt.Errorf("Cannot overwrite KeyingOperation for Stage (already set)")
	}
	s.state.keyFn = val
	return nil
}

// ReductionOperation retrieves the ReductionOperation for this Stage (if it exists)
func (s *stageContextImpl) ReductionOperation() sif.ReductionOperation {
	return s.state.reduceFn
}

// Configure the reduction operation for the end of this stage
func (s *stageContextImpl) SetReductionOperation(val sif.ReductionOperation) error {
	if s.ReductionOperation() != nil {
		return fmt.Errorf("Cannot overwrite ReductionOperation for Stage (already set)")
	}
	s.state.reduceFn = val
	return nil
}

// Accumulator retrieves the Accumulator for this Stage (if it exists)
func (s *stageContextImpl) Accumulator() sif.Accumulator {
	return s.state.accumulator
}

// Configure the accumulator for the end of this stage
func (s *stageContextImpl) SetAccumulator(val sif.Accumulator) error {
	if s.Accumulator() != nil {
		return fmt.Errorf("Cannot overwrite Accumulator for Stage (already set)")
	}
	s.state.accumulator = val
	return nil
}

// CollectionLimit retrieves the CollectionLimit for this Stage (or -1, if unset)
func (s *stageContextImpl) CollectionLimit() int {
	return s.state.collectionLimit
}

// SetCollectionLimit configures the CollectionLimit for the end of this stage
func (s *stageContextImpl) SetCollectionLimit(limit int) error {
	if s.CollectionLimit() > 0 {
		return fmt.Errorf("Cannot overwrite CollectionLimit for Stage (already set)")
	}
	s.state.collectionLimit = limit
	return nil
}

// TargetPartitionSize retrieves the TargetPartitionSize for this Stage (if it exists)
func (s *stageContextImpl) TargetPartitionSize() int {
	return s.state.targetPartitionSize
}

// Configure the reduction operation for the end of this stage
func (s *stageContextImpl) SetTargetPartitionSize(val int) error {
	if s.TargetPartitionSize() > 0 {
		return fmt.Errorf("Cannot overwrite TargetPartitionSize for Stage (already set)")
	}
	s.state.targetPartitionSize = val
	return nil
}

func (s *stageContextImpl) Destroy() error {
	if s.PartitionIndex() != nil {
		s.PartitionIndex().Destroy()
	}
	if s.PartitionCache() != nil {
		s.PartitionCache().Destroy()
	}
	return nil
}
