package stats

import (
	"time"

	"github.com/go-sif/sif/stats"
)

const statisticRollingWindows = 5

// RunStatistics contains statistics about a running Sif pipeline
type RunStatistics struct {
	started                     bool
	startTime                   time.Time
	totalRuntime                int64
	rowsProcessed               []int64
	partitionsProcessed         []int64
	recentPartitionRuntimes     []int64 // for rolling average of recent partition processing times
	recentPartitionRuntimesHead int
	stageRuntimes               []int64 // most recent runtime for a stage in streaming mode
	transformPhaseRuntimes      []int64 // most recent runtime for a stage in streaming mode
	shufflePhaseRuntimes        []int64 // most recent runtime for a stage in streaming mode

	// temp vars
	finished                  bool
	currentStageStartTime     time.Time
	currentTransformStartTime time.Time
	currentShuffleStartTime   time.Time
	currentPartitionStartTime time.Time
}

// Start triggers statistics tracking, if it hasn't been started already
func (rs *RunStatistics) Start(numStages int) {
	if !rs.started {
		rs.startTime = time.Now()
		rs.rowsProcessed = make([]int64, numStages)
		rs.partitionsProcessed = make([]int64, numStages)
		rs.recentPartitionRuntimes = make([]int64, statisticRollingWindows)
		rs.stageRuntimes = make([]int64, statisticRollingWindows)
		rs.transformPhaseRuntimes = make([]int64, statisticRollingWindows)
		rs.shufflePhaseRuntimes = make([]int64, statisticRollingWindows)
	}
}

// Finish completes statistics tracking
func (rs *RunStatistics) Finish() {
	rs.totalRuntime = time.Since(rs.startTime).Nanoseconds()
}

// StartStage tracks the beginning of a new Stage
func (rs *RunStatistics) StartStage() {
	rs.currentStageStartTime = time.Now()
}

// EndStage tracks the end of a Stage
func (rs *RunStatistics) EndStage(sidx int) {
	rs.stageRuntimes[sidx] = time.Since(rs.currentStageStartTime).Nanoseconds()
	rs.recentPartitionRuntimes = make([]int64, statisticRollingWindows)
	rs.recentPartitionRuntimesHead = 0
}

// StartTransform tracks the beginning of the transformation portion of a Stage
func (rs *RunStatistics) StartTransform() {
	rs.currentTransformStartTime = time.Now()
}

// EndTransform tracks the end of the transformation portion of a Stage
func (rs *RunStatistics) EndTransform(sidx int) {
	rs.transformPhaseRuntimes[sidx] = time.Since(rs.currentTransformStartTime).Nanoseconds()
}

// StartShuffle tracks the beginning of the shuffle portion of a Stage
func (rs *RunStatistics) StartShuffle() {
	rs.currentShuffleStartTime = time.Now()
}

// EndShuffle tracks the end of the shuffle portion of a Stage
func (rs *RunStatistics) EndShuffle(sidx int) {
	rs.shufflePhaseRuntimes[sidx] = time.Since(rs.currentShuffleStartTime).Nanoseconds()
}

// StartPartition tracks the beginning of the processing of a partition
func (rs *RunStatistics) StartPartition() {
	rs.currentPartitionStartTime = time.Now()
}

// EndPartition tracks the end of the processing of a partition
func (rs *RunStatistics) EndPartition(sidx int, numRows int) {
	rs.recentPartitionRuntimes[rs.recentPartitionRuntimesHead] = time.Since(rs.currentPartitionStartTime).Nanoseconds()
	rs.recentPartitionRuntimesHead = (rs.recentPartitionRuntimesHead + 1) % len(rs.recentPartitionRuntimes)
	rs.rowsProcessed[sidx] += int64(numRows)
	rs.partitionsProcessed[sidx]++
}

// GetStartTime returns the start time of the Sif pipeline
func (rs *RunStatistics) GetStartTime() time.Time {
	return rs.startTime
}

// GetRuntime returns the running time of the Sif pipeline
func (rs *RunStatistics) GetRuntime() int64 {
	if rs.finished {
		return rs.totalRuntime
	}
	return time.Since(rs.startTime).Nanoseconds()
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
func (rs *RunStatistics) GetCurrentPartitionProcessingTime() int64 {
	var total int64
	for _, d := range rs.recentPartitionRuntimes {
		total += d
	}
	return total / statisticRollingWindows
}

// GetStageRuntimes returns all recorded stage runtimes, from the most recent run of each Stage
func (rs *RunStatistics) GetStageRuntimes() []int64 {
	return rs.stageRuntimes
}

// GetStageTransformRuntimes returns all recorded stage transform-phase runtimes, from the most recent run of each Stage
func (rs *RunStatistics) GetStageTransformRuntimes() []int64 {
	return rs.transformPhaseRuntimes
}

// GetStageShuffleRuntimes returns all recorded stage shuffle-phase runtimes, from the most recent run of each Stage
func (rs *RunStatistics) GetStageShuffleRuntimes() []int64 {
	return rs.shufflePhaseRuntimes
}

// ToMessage converts this struct into a protobuf message
func (rs *RunStatistics) ToMessage() *stats.MStatisticsResponse {
	return &stats.MStatisticsResponse{
		Started:                     rs.started,
		Finished:                    rs.finished,
		StartTime:                   rs.startTime.UnixNano(),
		TotalRuntime:                rs.totalRuntime,
		RowsProcessed:               rs.rowsProcessed,
		PartitionsProcessed:         rs.partitionsProcessed,
		RecentPartitionRuntimes:     rs.recentPartitionRuntimes,
		RecentPartitionRuntimesHead: int32(rs.recentPartitionRuntimesHead),
		StageRuntimes:               rs.stageRuntimes,
		ShufflePhaseRuntimes:        rs.shufflePhaseRuntimes,
		TransformPhaseRuntimes:      rs.transformPhaseRuntimes,
	}
}
