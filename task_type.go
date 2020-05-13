package sif

// TaskType describes the type of a Task, used internally to control behaviour
type TaskType string

const (
	// WithColumnTaskType indicates that this task adds a column
	WithColumnTaskType TaskType = "add_column"
	// RemoveColumnTaskType indicates that this task removes a column
	RemoveColumnTaskType TaskType = "remove_column"
	// RenameColumnTaskType indicates that this task renames a column
	RenameColumnTaskType TaskType = "rename_column"
	// ExtractTaskType indicates that this task sources data from a DataSource
	ExtractTaskType TaskType = "extract"
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
