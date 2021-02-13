package dataframe

import (
	"github.com/go-sif/sif"
)

// Stage is a group of tasks which share a common schema.
// stages block the execution of further stages until they
// are complete.
type stageImpl struct {
	id                  int
	incomingSchema      sif.Schema
	outgoingSchema      sif.Schema
	widestInitialSchema sif.Schema
	frames              []*dataFrameImpl
}

// createStage is a factory for Stages, safely assigning deterministic IDs
func createStage(nextID int) *stageImpl {
	s := &stageImpl{
		id:                  nextID,
		incomingSchema:      nil,
		outgoingSchema:      nil,
		widestInitialSchema: nil,
		frames:              []*dataFrameImpl{},
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
	return s.widestInitialSchema
}

func (s *stageImpl) WorkerInitialize(sctx sif.StageContext) error {
	for _, frame := range s.frames {
		if err := frame.workerInitialize(sctx); err != nil {
			return err
		}
	}
	return nil
}

// workerExecute runs a stage against a Partition of data, returning
// the modified Partition (which may have been modified in-place, filtered,
// Or turned into multiple Partitions)
func (s *stageImpl) WorkerExecute(sctx sif.StageContext, part sif.OperablePartition) ([]sif.OperablePartition, error) {
	var prev = []sif.OperablePartition{part}
	for _, frame := range s.frames {
		next := make([]sif.OperablePartition, 0, len(prev))
		for _, p := range prev {
			out, err := frame.workerExecuteTask(sctx, p)
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
