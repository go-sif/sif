package dataframe

import (
	"log"
	"sort"

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
	for next := df.Clone(); next != nil; next = next.parent {
		frames = append([]*dataFrameImpl{next}, frames...)
	}
	// split into stages by taskType
	nextID := 0
	stages := []*stageImpl{}
	endStage := func() {
		currentStage := stages[len(stages)-1]
		var previousStage *stageImpl
		if len(stages) > 1 {
			previousStage = stages[len(stages)-2]
		}
		// repack should never be the first frame in a stage. Throw error if that is the case
		if len(currentStage.frames) > 0 && currentStage.frames[0].taskType == sif.RepackTaskType {
			log.Panicf("Repack cannot be the first Task in a DataFrame")
		}
		// sort CreateColumns to top of stage
		sort.SliceStable(currentStage.frames, func(i, j int) bool {
			if currentStage.frames[i].taskType == sif.ExtractTaskType {
				return true
			} else if currentStage.frames[j].taskType == sif.ExtractTaskType {
				return false
			}
			if currentStage.frames[i].taskType == sif.WithColumnTaskType && currentStage.frames[j].taskType != sif.WithColumnTaskType {
				return true
			} else if currentStage.frames[i].taskType != sif.WithColumnTaskType && currentStage.frames[j].taskType == sif.WithColumnTaskType {
				return false
			}
			return false
		})
		// Fix links and call apply() to update frame schemas etc.
		for i, f := range currentStage.frames {
			// fix links since we sorted
			if i == 0 && previousStage != nil {
				f.parent = previousStage.frames[len(previousStage.frames)-1]
			} else if i == 0 {
				f.parent = nil
			} else {
				f.parent = currentStage.frames[i-1]
			}
			dfor, err := f.apply(f.parent)
			if err != nil {
				panic(err)
			}
			f.task = dfor.Task
			f.privateSchema = dfor.PrivateSchema
			f.publicSchema = dfor.PublicSchema
		}
		// set outgoing schemas for stage
		if len(currentStage.frames) > 0 {
			currentStage.outgoingPrivateSchema = currentStage.frames[len(currentStage.frames)-1].privateSchema
			currentStage.outgoingPublicSchema = currentStage.frames[len(currentStage.frames)-1].publicSchema
		}
	}
	newStage := func() {
		// TODO panic if no removes precede the repack, as it's pointless
		stages = append(stages, createStage(nextID))
		nextID++
	}
	newStage()
	for i, f := range frames {
		currentStage := stages[len(stages)-1]
		currentStage.frames = append(currentStage.frames, f)
		if len(stages) > 1 {
			currentStage.incomingPublicSchema = stages[len(stages)-2].outgoingPublicSchema
			currentStage.incomingPrivateSchema = stages[len(stages)-2].outgoingPrivateSchema
		}
		// if this is a reduce, this is the end of the Stage
		if f.taskType == sif.ShuffleTaskType {
			endStage()
			sTask, ok := f.task.(shuffleTask)
			if !ok {
				log.Panicf("taskType is ShuffleTaskType but Task is not a shuffleTask. Task is misdefined.")
			}
			currentStage.SetKeyingOperation(sTask.GetKeyingOperation())
			currentStage.SetReductionOperation(sTask.GetReductionOperation())
			currentStage.SetTargetPartitionSize(sTask.GetTargetPartitionSize())
			// if there are still frames left, make a new stage
			if i+1 < len(frames) {
				newStage()
			}
		} else if f.taskType == sif.AccumulateTaskType {
			endStage()
			aTask, ok := f.task.(accumulationTask)
			if !ok {
				log.Panicf("taskType is AccumulateTaskType but Task is not an accumulationTask. Task is misdefined.")
			}
			currentStage.SetAccumulator(aTask.GetAccumulator())
			if i+1 < len(frames) {
				log.Panicf("No tasks can follow an Accumulate()")
			}
			break // no tasks can come after an accumulation
		} else if f.taskType == sif.CollectTaskType {
			endStage()
			if i+1 < len(frames) {
				log.Panicf("No tasks can follow a Collect()")
			}
			break // no tasks can come after a collect
		}
	}
	// hack for checking if we never called endStage() on the last stage, which can
	// happen if it's just a set of map()s which don't end in a collect, accumulate or shuffle
	if len(stages) > 0 && stages[len(stages)-1].OutgoingPrivateSchema() == nil {
		endStage()
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
	// update current schemas
	previous.UpdatePublicSchema(df.publicSchema)
	previous.UpdatePrivateSchema(df.privateSchema)
	return res, nil
}
