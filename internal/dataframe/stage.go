package dataframe

import (
	"log"

	"github.com/go-sif/sif"
)

// Stage is a group of tasks which share a common schema.
// stages block the execution of further stages until they
// are complete.
type stageImpl struct {
	id                    int
	incomingPublicSchema  sif.Schema
	outgoingPublicSchema  sif.Schema
	incomingPrivateSchema sif.Schema
	outgoingPrivateSchema sif.Schema
	frames                []*dataFrameImpl
	keyFn                 sif.KeyingOperation
	reduceFn              sif.ReductionOperation
	accumulator           sif.Accumulator
	targetPartitionSize   int
}

// createStage is a factory for Stages, safely assigning deterministic IDs
func createStage(nextID int) *stageImpl {
	s := &stageImpl{
		id:                    nextID,
		incomingPublicSchema:  nil,
		outgoingPublicSchema:  nil,
		incomingPrivateSchema: nil,
		outgoingPrivateSchema: nil,
		frames:                []*dataFrameImpl{},
		keyFn:                 nil,
		reduceFn:              nil,
		accumulator:           nil,
		targetPartitionSize:   -1,
	}
	nextID++
	return s
}

// ID returns the ID for this Stage
func (s *stageImpl) ID() int {
	return s.id
}

// IncomingPublicSchema is the public Schema for data entering this Stage
func (s *stageImpl) IncomingPublicSchema() sif.Schema {
	return s.incomingPublicSchema
}

// OutgoingPublicSchema is the public Schema for data leaving this Stage
func (s *stageImpl) OutgoingPublicSchema() sif.Schema {
	return s.outgoingPublicSchema
}

// IncomingPrivateSchema is the private Schema for data entering this Stage
func (s *stageImpl) IncomingPrivateSchema() sif.Schema {
	return s.incomingPrivateSchema
}

// OutgoingPrivateSchema is the private Schema for data leaving this Stage
func (s *stageImpl) OutgoingPrivateSchema() sif.Schema {
	return s.outgoingPrivateSchema
}

// WidestInitialPrivateSchema returns the number of bytes
func (s *stageImpl) WidestInitialPrivateSchema() sif.Schema {
	var widest sif.Schema
	for _, f := range s.frames {
		if f.taskType == sif.RepackTaskType {
			return widest
		}
		widest = f.privateSchema
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
