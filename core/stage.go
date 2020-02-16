package core

import (
	"fmt"
	"log"

	"github.com/go-sif/sif"
)

// Stage is a group of tasks which share a common schema.
// stages block the execution of further stages until they
// are complete.
type stage struct {
	id             string
	incomingSchema sif.Schema // the true, final schema for partitions which come from a previous stage (received during a shuffle, for example). This may include columns which have been removed, but not repacked
	outgoingSchema sif.Schema // the true, final schema for partitions which exit this stage (dispatched during a shuffle, for example). This may include columns which have been removed, but not repacked
	frames         []*dataFrameImpl
	keyFn          sif.KeyingOperation
	reduceFn       sif.ReductionOperation
}

// createStage is a factory for Stages, safely assigning deterministic IDs
func createStage(nextID int) *stage {
	s := &stage{fmt.Sprintf("stage-%d", nextID), nil, nil, []*dataFrameImpl{}, nil, nil}
	nextID++
	return s
}

// finalSchema returns the schema from the final task of the stage, or the nil if there are no tasks
func (s *stage) finalSchema() sif.Schema {
	if len(s.frames) > 0 {
		return s.frames[len(s.frames)-1].schema
	}
	return nil
}

// initialSchemaSize returns the number of bytes
func (s *stage) widestInitialSchema() sif.Schema {
	var widest sif.Schema
	for _, f := range s.frames {
		if f.taskType == "repack" {
			return widest
		}
		if widest == nil || f.schema.Size() > widest.Size() {
			widest = f.schema
		}
	}
	return widest
}

// workerExecute runs a stage against a Partition of data, returning
// the modified Partition (which may have been modified in-place, filtered,
// or turned into multiple Partitions)
func (s *stage) workerExecute(part sif.OperablePartition) ([]sif.OperablePartition, error) {
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

// endsInShuffle returns true iff this Stage ends with a reduction task
func (s *stage) endsInShuffle() bool {
	return s.reduceFn != nil && s.keyFn != nil
}

// endsInCollect returns true iff this Stage represents a collect task
func (s *stage) endsInCollect() bool {
	return len(s.frames) > 0 && s.frames[len(s.frames)-1].taskType == "collect"
}

// GetCollectionLimit returns the maximum number of Partitions to collect
func (s *stage) GetCollectionLimit() int64 {
	if !s.endsInCollect() {
		return 0
	}
	cTask, ok := s.frames[len(s.frames)-1].task.(collectionTask)
	if !ok {
		log.Panicf("taskType is collect but Task is not a collectionTask")
	}
	return cTask.GetCollectionLimit()
}

// Configure the keying operation for the end of this stage
func (s *stage) setKeyingOperation(keyFn sif.KeyingOperation) {
	s.keyFn = keyFn
}

// Configure the reduction operation for the end of this stage
func (s *stage) setReductionOperation(reduceFn sif.ReductionOperation) {
	s.reduceFn = reduceFn
}
