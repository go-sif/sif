package types

// PlanExecutorConfig configures the execution of a plan
type PlanExecutorConfig struct {
	TempFilePath       string // the directory to use as on-disk swap space for partitions
	InMemoryPartitions int    // the number of partitions to retain in memory before swapping to disk
	Streaming          bool   // whether or not this executor is operating on streaming data
	IgnoreRowErrors    bool   // iff true, log row transformation errors instead of crashing immediately
}
