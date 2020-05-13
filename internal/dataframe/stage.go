package dataframe

import (
	"log"

	"github.com/go-sif/sif"
)

// Stage is a group of tasks which share a common schema.
// stages block the execution of further stages until they
// are complete.
type stageImpl struct {
	id                  int
	incomingSchema      sif.Schema
	outgoingSchema      sif.Schema
	frames              []*dataFrameImpl
	keyFn               sif.KeyingOperation
	reduceFn            sif.ReductionOperation
	accumulator         sif.Accumulator
	targetPartitionSize int
}

// createStage is a factory for Stages, safely assigning deterministic IDs
func createStage(nextID int) *stageImpl {
	s := &stageImpl{
		id:                  nextID,
		incomingSchema:      nil,
		outgoingSchema:      nil,
		frames:              []*dataFrameImpl{},
		keyFn:               nil,
		reduceFn:            nil,
		accumulator:         nil,
		targetPartitionSize: -1,
	}
	nextID++
	return s
}

// ID returns the ID for this Stage
func (s *stageImpl) ID() int {
	return s.id
}

// IncomingSchema is the Schema for data entering this Stage
func (s *stageImpl) IncomingSchema() sif.Schema {
	return s.incomingSchema
}

// OutgoingSchema is the Schema for data leaving this Stage
func (s *stageImpl) OutgoingSchema() sif.Schema {
	return s.outgoingSchema
}

// WidestInitialSchema returns the number of bytes required to fit data for the whole stage
func (s *stageImpl) WidestInitialSchema() sif.Schema {
	var widest sif.Schema
	for _, f := range s.frames {
		if widest == nil || f.GetSchema().Size() > widest.Size() || (f.GetSchema().Size() == widest.Size() && f.GetSchema().NumVariableLengthColumns() > widest.NumVariableLengthColumns()) {
			widest = f.schema
		}
	}
	return widest
}

// workerExecute runs a stage against a Partition of data, returning
// the modified Partition (which may have been modified in-place, filtered,
// Or turned into multiple Partitions)
func (s *stageImpl) WorkerExecute(part sif.OperablePartition) ([]sif.OperablePartition, error) {
	var prev = []sif.OperablePartition{part}
	for _, frame := range s.frames {
		next := make([]sif.OperablePartition, 0, len(prev))
		for _, p := range prev {
			out, err := frame.workerExecuteTask(p)
			if err != nil {
				// TODO wrapping this error breaks multierror type checking
				// return nil, fmt.Errorf("Error in task %s of stage %s:\n%w", frame.taskType, s.id, err)
				return nil, err
			}
			next = append(next, out...)
		}
		prev = next
	}
	return prev, nil
}

// EndsInAccumulate returns true iff this Stage ends with an accumulation task
func (s *stageImpl) EndsInAccumulate() bool {
	return len(s.frames) > 0 && s.frames[len(s.frames)-1].taskType == sif.AccumulateTaskType
}

// EndsInShuffle returns true iff this Stage ends with a reduction task
func (s *stageImpl) EndsInShuffle() bool {
	return len(s.frames) > 0 && s.frames[len(s.frames)-1].taskType == sif.ShuffleTaskType
}

// EndsInCollect returns true iff this Stage represents a collect task
func (s *stageImpl) EndsInCollect() bool {
	return len(s.frames) > 0 && s.frames[len(s.frames)-1].taskType == sif.CollectTaskType
}

// GetCollectionLimit returns the maximum number of Partitions to collect
func (s *stageImpl) GetCollectionLimit() int64 {
	if !s.EndsInCollect() {
		return 0
	}
	cTask, ok := s.frames[len(s.frames)-1].task.(collectionTask)
	if !ok {
		log.Panicf("taskType is collect but Task is not a collectionTask")
	}
	return cTask.GetCollectionLimit()
}

// KeyingOperation retrieves the KeyingOperation for this Stage (if it exists)
func (s *stageImpl) KeyingOperation() sif.KeyingOperation {
	return s.keyFn
}

// Configure the keying operation for the end of this stage
func (s *stageImpl) SetKeyingOperation(keyFn sif.KeyingOperation) {
	s.keyFn = keyFn
}

// ReductionOperation retrieves the ReductionOperation for this Stage (if it exists)
func (s *stageImpl) ReductionOperation() sif.ReductionOperation {
	return s.reduceFn
}

// Configure the reduction operation for the end of this stage
func (s *stageImpl) SetReductionOperation(reduceFn sif.ReductionOperation) {
	s.reduceFn = reduceFn
}

// Accumulator retrieves the Accumulator for this Stage (if it exists)
func (s *stageImpl) Accumulator() sif.Accumulator {
	return s.accumulator
}

// Configure the accumulator for the end of this stage
func (s *stageImpl) SetAccumulator(acc sif.Accumulator) {
	s.accumulator = acc
}

// TargetPartitionSize retrieves the TargetPartitionSize for this Stage (if it exists)
func (s *stageImpl) TargetPartitionSize() int {
	return s.targetPartitionSize
}

// Configure the reduction operation for the end of this stage
func (s *stageImpl) SetTargetPartitionSize(targetPartitionSize int) {
	s.targetPartitionSize = targetPartitionSize
}
