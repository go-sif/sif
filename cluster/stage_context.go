package cluster

import (
	"context"
	"fmt"
	"time"

	"github.com/go-sif/sif"
	itypes "github.com/go-sif/sif/internal/types"
)

type stageContextKey string

const nextStageWidestInitialSchema stageContextKey = "sif.cluster.stageContextImpl.nextStageWidestInitialSchema"
const shuffleBuckets stageContextKey = "sif.cluster.stageContextImpl.shuffleBuckets"
const pCache stageContextKey = "sif.cluster.stageContextImpl.pCache"
const pIndex stageContextKey = "sif.cluster.stageContextImpl.pIndex"
const pIncoming stageContextKey = "sif.cluster.stageContextImpl.pIncoming"
const keyFn stageContextKey = "sif.cluster.stageContextImpl.keyingFn"
const reduceFn stageContextKey = "sif.cluster.stageContextImpl.reduceFn"
const accumulator stageContextKey = "sif.cluster.stageContextImpl.accumulator"
const collectionLimit stageContextKey = "sif.cluster.stageContextImpl.collectionLimit"
const targetPartitionSize stageContextKey = "sif.cluster.stageContextImpl.targetPartitionSize"

type stageContextImpl struct {
	ctx   context.Context
	stage itypes.Stage
}

// TODO put all vals into a struct and just store/lookup the struct once when rehydrating a stagecontext from a context. saves on casts.
func createStageContext(ctx context.Context, stage itypes.Stage) sif.StageContext {
	return &stageContextImpl{
		ctx:   ctx,
		stage: stage,
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
	if i := s.ctx.Value(nextStageWidestInitialSchema); i != nil {
		return i.(sif.Schema)
	}
	return nil
}

func (s *stageContextImpl) SetNextStageWidestInitialSchema(schema sif.Schema) error {
	if s.NextStageWidestInitialSchema() != nil {
		return fmt.Errorf("Cannot overwrite widest initial Schema for next Stage (already set)")
	}
	s.ctx = context.WithValue(s.ctx, nextStageWidestInitialSchema, schema)
	return nil
}

func (s *stageContextImpl) ShuffleBuckets() []uint64 {
	if b := s.ctx.Value(shuffleBuckets); b != nil {
		return b.([]uint64)
	}
	return nil
}

func (s *stageContextImpl) SetShuffleBuckets(buckets []uint64) error {
	if s.ShuffleBuckets() != nil {
		return fmt.Errorf("Cannot overwrite shuffle buckets for Stage (already set)")
	}
	s.ctx = context.WithValue(s.ctx, shuffleBuckets, buckets)
	return nil
}

func (s *stageContextImpl) PartitionCache() sif.PartitionCache {
	if c := s.ctx.Value(pCache); c != nil {
		return c.(sif.PartitionCache)
	}
	return nil
}

func (s *stageContextImpl) SetPartitionCache(cache sif.PartitionCache) error {
	if s.PartitionCache() != nil {
		return fmt.Errorf("Cannot overwrite PartitionCache for Stage (already set)")
	}
	s.ctx = context.WithValue(s.ctx, pCache, cache)
	return nil
}

func (s *stageContextImpl) PartitionIndex() sif.PartitionIndex {
	if i := s.ctx.Value(pIndex); i != nil {
		return i.(sif.PartitionIndex)
	}
	return nil
}

func (s *stageContextImpl) SetPartitionIndex(idx sif.PartitionIndex) error {
	if s.PartitionIndex() != nil {
		return fmt.Errorf("Cannot overwrite PartitionIndex for Stage (already set)")
	}
	s.ctx = context.WithValue(s.ctx, pIndex, idx)
	return nil
}

func (s *stageContextImpl) IncomingPartitionIterator() sif.PartitionIterator {
	if i := s.ctx.Value(pIncoming); i != nil {
		return i.(sif.PartitionIterator)
	}
	return nil
}

func (s *stageContextImpl) SetIncomingPartitionIterator(i sif.PartitionIterator) error {
	if s.IncomingPartitionIterator() != nil {
		return fmt.Errorf("Cannot overwrite IncomingPartitionIterator for Stage (already set)")
	}
	s.ctx = context.WithValue(s.ctx, pIncoming, i)
	return nil
}

// KeyingOperation retrieves the KeyingOperation for this Stage (if it exists)
func (s *stageContextImpl) KeyingOperation() sif.KeyingOperation {
	if k := s.ctx.Value(keyFn); k != nil {
		return k.(sif.KeyingOperation)
	}
	return nil
}

// Configure the keying operation for the end of this stage
func (s *stageContextImpl) SetKeyingOperation(val sif.KeyingOperation) error {
	if s.KeyingOperation() != nil {
		return fmt.Errorf("Cannot overwrite KeyingOperation for Stage (already set)")
	}
	s.ctx = context.WithValue(s.ctx, keyFn, val)
	return nil
}

// ReductionOperation retrieves the ReductionOperation for this Stage (if it exists)
func (s *stageContextImpl) ReductionOperation() sif.ReductionOperation {
	if r := s.ctx.Value(reduceFn); r != nil {
		return r.(sif.ReductionOperation)
	}
	return nil
}

// Configure the reduction operation for the end of this stage
func (s *stageContextImpl) SetReductionOperation(val sif.ReductionOperation) error {
	if s.ReductionOperation() != nil {
		return fmt.Errorf("Cannot overwrite ReductionOperation for Stage (already set)")
	}
	s.ctx = context.WithValue(s.ctx, reduceFn, val)
	return nil
}

// Accumulator retrieves the Accumulator for this Stage (if it exists)
func (s *stageContextImpl) Accumulator() sif.Accumulator {
	if a := s.ctx.Value(accumulator); a != nil {
		return a.(sif.Accumulator)
	}
	return nil
}

// Configure the accumulator for the end of this stage
func (s *stageContextImpl) SetAccumulator(val sif.Accumulator) error {
	if s.Accumulator() != nil {
		return fmt.Errorf("Cannot overwrite Accumulator for Stage (already set)")
	}
	s.ctx = context.WithValue(s.ctx, accumulator, val)
	return nil
}

// CollectionLimit retrieves the CollectionLimit for this Stage (or -1, if unset)
func (s *stageContextImpl) CollectionLimit() int {
	if a := s.ctx.Value(collectionLimit); a != nil {
		return a.(int)
	}
	return -1
}

// SetCollectionLimit configures the CollectionLimit for the end of this stage
func (s *stageContextImpl) SetCollectionLimit(limit int) error {
	if s.CollectionLimit() > 0 {
		return fmt.Errorf("Cannot overwrite CollectionLimit for Stage (already set)")
	}
	s.ctx = context.WithValue(s.ctx, collectionLimit, limit)
	return nil
}

// TargetPartitionSize retrieves the TargetPartitionSize for this Stage (if it exists)
func (s *stageContextImpl) TargetPartitionSize() int {
	if t := s.ctx.Value(targetPartitionSize); t != nil {
		return t.(int)
	}
	return -1
}

// Configure the reduction operation for the end of this stage
func (s *stageContextImpl) SetTargetPartitionSize(val int) error {
	if s.TargetPartitionSize() > 0 {
		return fmt.Errorf("Cannot overwrite TargetPartitionSize for Stage (already set)")
	}
	s.ctx = context.WithValue(s.ctx, targetPartitionSize, val)
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
