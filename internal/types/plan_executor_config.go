package types

// PlanExecutorConfig configures the execution of a plan
type PlanExecutorConfig struct {
	NumWorkers               int
	TempFilePath             string // the directory to use as on-disk swap space for partitions
	CacheMemoryInitialSize   int    // The initial size of the partition cache, measured in partitions
	CacheMemoryHighWatermark uint64 // soft memory limit for in-memory partition caches, in bytes
	Streaming                bool   // whether or not this executor is operating on streaming data
	IgnoreRowErrors          bool   // iff true, log row transformation errors instead of crashing immediately
}
