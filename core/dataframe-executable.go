package core

import (
	"log"
)

// getParent returns the parent DataFrame of a DataFrame
func (df *dataFrameImpl) getParent() DataFrame {
	return df.parent
}

// optimize splits the DataFrame chain into stages which each share a schema.
// Each stage's execution will be blocked until the completion of the previous stage
func (df *dataFrameImpl) optimize() *plan {
	// create a slice of frames, in order of execution, by following parent links
	frames := []*dataFrameImpl{}
	for next := df; next != nil; next = next.parent {
		frames = append([]*dataFrameImpl{next}, frames...)
	}
	// split into stages at reductions and repacks, discovering incoming and outgoing schemas for the stage
	nextID := 0
	stages := []*stage{createStage(nextID)}
	nextID++
	for _, f := range frames {
		currentStage := stages[len(stages)-1]
		currentStage.frames = append(currentStage.frames, f)
		if len(stages) > 1 {
			currentStage.incomingSchema = stages[len(stages)-2].outgoingSchema
		}
		// the outgoing schema is always the last schema
		currentStage.outgoingSchema = f.schema
		// if this is a reduce, this is the end of the Stage
		if f.taskType == "reduce" {
			rTask, ok := f.task.(reductionTask)
			if !ok {
				log.Panicf("taskType is reduce but Task is not a reductionTask")
			}
			currentStage.setKeyingOperation(rTask.GetKeyingOperation())
			currentStage.setReductionOperation(rTask.GetReductionOperation())
			stages = append(stages, createStage(nextID))
			nextID++
		} else if f.taskType == "repack" {
			// repack should never be the first frame. Throw error if that is the case
			if len(currentStage.frames) == 0 {
				log.Panicf("Repack cannot be the first Task in a DataFrame")
			}
		} else if f.taskType == "collect" {
			break // no tasks can come after a collect
		}
	}
	return &plan{stages, df.parser, df.source}
}

// analyzeSource returns a PartitionMap for the source data for this DataFrame
func (df *dataFrameImpl) analyzeSource() (PartitionMap, error) {
	return df.source.Analyze()
}

func test(p OperablePTition) OperablePTition {
	return p
}

// workerExecuteTask runs this DataFrame's task against the previous Partition,
// returning the modified Partition (or a new one(s) if necessary).
// The previous Partition may be nil.
func (df *dataFrameImpl) workerExecuteTask(previous OperablePTition) ([]OperablePTition, error) {
	res, err := df.task.RunWorker(test(previous))
	if err != nil {
		return nil, err
	}
	previous.UpdateCurrentSchema(df.schema) // update current schema
	return res, nil
}
