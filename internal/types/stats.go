package types

import "time"

const StatisticRollingWindows = 5

// RunStatistics contains statistics about a running Sif pipeline
type RunStatistics struct {
	startTime                   time.Time
	totalRuntime                time.Duration
	rowsProcessed               []int64
	partitionsProcessed         []int64
	recentPartitionRuntimes     []time.Duration // for rolling average of recent partition processing times
	recentPartitionRuntimesHead int
	stageRuntimes               []time.Duration // most recent runtime for a stage in streaming mode
	transformPhaseRuntimes      []time.Duration // most recent runtime for a stage in streaming mode
	shufflePhaseRuntimes        []time.Duration // most recent runtime for a stage in streaming mode

	// temp vars
	finished                  bool
	currentStageStartTime     time.Time
	currentTransformStartTime time.Time
	currentShuffleStartTime   time.Time
	currentPartitionStartTime time.Time
}

func (rs *RunStatistics) start(numStages int) {
	rs.startTime = time.Now()
	rs.rowsProcessed = make([]int64, numStages)
	rs.partitionsProcessed = make([]int64, numStages)
	rs.recentPartitionRuntimes = make([]time.Duration, StatisticRollingWindows)
	rs.stageRuntimes = make([]time.Duration, StatisticRollingWindows)
	rs.transformPhaseRuntimes = make([]time.Duration, StatisticRollingWindows)
	rs.shufflePhaseRuntimes = make([]time.Duration, StatisticRollingWindows)
}

func (rs *RunStatistics) finish() {
	rs.totalRuntime = time.Since(rs.startTime)
}

func (rs *RunStatistics) startStage() {
	rs.currentStageStartTime = time.Now()
}

func (rs *RunStatistics) endStage(sidx int) {
	rs.stageRuntimes[sidx] = time.Since(rs.currentStageStartTime)
	rs.recentPartitionRuntimes = make([]time.Duration, StatisticRollingWindows)
	rs.recentPartitionRuntimesHead = 0
}

func (rs *RunStatistics) startTransform() {
	rs.currentTransformStartTime = time.Now()
}

func (rs *RunStatistics) endTransform(sidx int) {
	rs.transformPhaseRuntimes[sidx] = time.Since(rs.currentTransformStartTime)
}

func (rs *RunStatistics) startShuffle() {
	rs.currentShuffleStartTime = time.Now()
}

func (rs *RunStatistics) endShuffle(sidx int) {
	rs.shufflePhaseRuntimes[sidx] = time.Since(rs.currentShuffleStartTime)
}

func (rs *RunStatistics) startPartition() {
	rs.currentPartitionStartTime = time.Now()
}

func (rs *RunStatistics) endPartition(sidx int, numRows int) {
	rs.recentPartitionRuntimes[rs.recentPartitionRuntimesHead] = time.Since(rs.currentPartitionStartTime)
	rs.recentPartitionRuntimesHead = (rs.recentPartitionRuntimesHead + 1) % len(rs.recentPartitionRuntimes)
	rs.rowsProcessed[sidx] += int64(numRows)
	rs.partitionsProcessed[sidx]++
}

// GetStartTime returns the start time of the Sif pipeline
func (rs *RunStatistics) GetStartTime() time.Time {
	return rs.startTime
}

// GetRuntime returns the running time of the Sif pipeline
func (rs *RunStatistics) GetRuntime() time.Duration {
	if rs.finished {
		return rs.totalRuntime
	}
	return time.Since(rs.startTime)
}

// GetNumRowsProcessed returns the number of Rows which have been processed so far, counted by stage
func (rs *RunStatistics) GetNumRowsProcessed() []int64 {
	return rs.rowsProcessed
}

// GetNumPartitionsProcessed returns the number of Partitions which have been processed so far, counted by stage
func (rs *RunStatistics) GetNumPartitionsProcessed() []int64 {
	return rs.partitionsProcessed
}

// GetCurrentPartitionProcessingTime returns a rolling average of partition processing time
func (rs *RunStatistics) GetCurrentPartitionProcessingTime() time.Duration {
	var total time.Duration
	for _, d := range rs.recentPartitionRuntimes {
		total += d
	}
	return total / StatisticRollingWindows
}

// GetStageRuntimes returns all recorded stage runtimes, from the most recent run of each Stage
func (rs *RunStatistics) GetStageRuntimes() []time.Duration {
	return rs.stageRuntimes
}

// GetStageTransformRuntimes returns all recorded stage transform-phase runtimes, from the most recent run of each Stage
func (rs *RunStatistics) GetStageTransformRuntimes() []time.Duration {
	return rs.transformPhaseRuntimes
}

// GetStageShuffleRuntimes returns all recorded stage shuffle-phase runtimes, from the most recent run of each Stage
func (rs *RunStatistics) GetStageShuffleRuntimes() []time.Duration {
	return rs.shufflePhaseRuntimes
}
