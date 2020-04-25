package sif

// TaskType describes the type of a Task, used internally to control behaviour
type TaskType string

const (
	// NoOpTaskType indicates that this task does not manipulate data
	NoOpTaskType TaskType = "no_op"
	// ExtractTaskType indicates that this task sources data from a DataSource
	ExtractTaskType TaskType = "extract"
	// RepackTaskType indicates that this task triggers a Repack
	RepackTaskType TaskType = "repack"
	// ShuffleTaskType indicates that this task triggers a Shuffle
	ShuffleTaskType TaskType = "shuffle"
	// AccumulateTaskType indicates that this task triggers an Accumulation
	AccumulateTaskType TaskType = "accumulate"
	// FlatMapTaskType indicates that this task triggers a FlatMap
	FlatMapTaskType TaskType = "flatmap"
	// MapTaskType indicates that this task triggers a Map
	MapTaskType TaskType = "map"
	// FilterTaskType indicates that this task triggers a Filter
	FilterTaskType TaskType = "filter"
	// CollectTaskType indicates that this task triggers a Collect
	CollectTaskType TaskType = "collect"
)
