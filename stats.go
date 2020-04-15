package sif

import "time"

// RuntimeStatistics facilitates the retrieval of statistics about a running Sif pipeline
type RuntimeStatistics interface {
	// GetStartTime returns the start time of the Sif pipeline
	GetStartTime() time.Time
	// GetRuntime returns the running time of the Sif pipeline
	GetRuntime() time.Duration
	// GetNumRowsProcessed returns the number of Rows which have been processed so far, counted by stage
	GetNumRowsProcessed() []int64
	// GetNumPartitionsProcessed returns the number of Partitions which have been processed so far, counted by stage
	GetNumPartitionsProcessed() []int64
	// GetCurrentPartitionProcessingTime returns a rolling average of partition processing time
	GetCurrentPartitionProcessingTime() time.Duration
	// GetStageRuntimes returns all recorded stage runtimes, from the most recent run of each Stage
	GetStageRuntimes() []time.Duration
	// GetStageTransformRuntimes returns all recorded stage transform-phase runtimes, from the most recent run of each Stage
	GetStageTransformRuntimes() []time.Duration
	// GetStageShuffleRuntimes returns all recorded stage shuffle-phase runtimes, from the most recent run of each Stage
	GetStageShuffleRuntimes() []time.Duration
}
