package dataframe

import (
	"log"

	"github.com/go-sif/sif"
	itypes "github.com/go-sif/sif/internal/types"
)

// An executableDataFrame adds methods specific to cluster execution of DataFrames
type executableDataFrame interface {
	sif.DataFrame
	getParent() sif.DataFrame                                                          // getParent returns the parent DataFrame of a DataFrame
	optimize() itypes.Plan                                                             // optimize splits the DataFrame chain into stages which each share a schema. Each stage's execution will be blocked until the completion of the previous stage
	analyzeSource() (sif.PartitionMap, error)                                          // analyzeSource returns a PartitionMap for the source data for this DataFrame
	workerExecuteTask(previous sif.OperablePartition) ([]sif.OperablePartition, error) // workerExecuteTask runs this DataFrame's task against the previous Partition, returning the modified Partition (or a new one(s) if necessary). The previous Partition may be nil.
}

// getParent returns the parent DataFrame of a DataFrame
func (df *dataFrameImpl) GetParent() sif.DataFrame {
	return df.parent
}

// optimize splits the DataFrame chain into stages which each share a schema.
// Each stage's execution will be blocked until the completion of the previous stage
func (df *dataFrameImpl) Optimize() itypes.Plan {
	// create a slice of frames, in order of execution, by following parent links
	frames := []*dataFrameImpl{}
	for next := df; next != nil; next = next.parent {
		frames = append([]*dataFrameImpl{next}, frames...)
	}
	// split into stages at reductions and repacks, discovering incoming and outgoing schemas for the stage
	nextID := 0
	stages := []*stageImpl{createStage(nextID)}
	nextID++
	for i, f := range frames {
		currentStage := stages[len(stages)-1]
		currentStage.frames = append(currentStage.frames, f)
		if len(stages) > 1 {
			currentStage.incomingSchema = stages[len(stages)-2].outgoingSchema
		}
		// the outgoing schema is always the last schema
		currentStage.outgoingSchema = f.schema
		// if this is a reduce, this is the end of the Stage
		if f.taskType == sif.ShuffleTaskType {
			sTask, ok := f.task.(shuffleTask)
			if !ok {
				log.Panicf("taskType is ShuffleTaskType but Task is not a shuffleTask. Task is misdefined.")
			}
			currentStage.SetKeyingOperation(sTask.GetKeyingOperation())
			currentStage.SetReductionOperation(sTask.GetReductionOperation())
			currentStage.SetTargetPartitionSize(sTask.GetTargetPartitionSize())
			stages = append(stages, createStage(nextID))
			nextID++
		} else if f.taskType == sif.RepackTaskType {
			// repack should never be the first frame. Throw error if that is the case
			if len(currentStage.frames) == 0 {
				log.Panicf("Repack cannot be the first Task in a DataFrame")
			}
		} else if f.taskType == sif.AccumulateTaskType {
			aTask, ok := f.task.(accumulationTask)
			if !ok {
				log.Panicf("taskType is AccumulateTaskType but Task is not an accumulationTask. Task is misdefined.")
			}
			currentStage.SetAccumulator(aTask.GetAccumulatorFactory()())
			if i+1 < len(frames) {
				log.Panicf("No tasks can follow an Accumulate()")
			}
			break // no tasks can come after an accumulation
		} else if f.taskType == sif.CollectTaskType {
			if i+1 < len(frames) {
				log.Panicf("No tasks can follow a Collect()")
			}
			break // no tasks can come after a collect
		}
	}
	return &planImpl{stages, df.parser, df.source}
}

// analyzeSource returns a PartitionMap for the source data for this DataFrame
func (df *dataFrameImpl) AnalyzeSource() (sif.PartitionMap, error) {
	return df.source.Analyze()
}

// workerExecuteTask runs this DataFrame's task against the previous Partition,
// returning the modified Partition (or a new one(s) if necessary).
// The previous Partition may be nil.
func (df *dataFrameImpl) workerExecuteTask(previous sif.OperablePartition) ([]sif.OperablePartition, error) {
	res, err := df.task.RunWorker(previous)
	if err != nil {
		return nil, err
	}
	previous.UpdateCurrentSchema(df.schema) // update current schema
	return res, nil
}
