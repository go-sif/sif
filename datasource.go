package sif

// PartitionLoader is a description of how to load specific Partitions of data from a particular DataSource.
// DataSources implement this interface to implement data-loading logic. PartitionLoaders are assigned round-robin
// to workers, so an assumption is made that each PartitionLoader will produce a roughly equal number of Partitions
type PartitionLoader interface {
	ToString() string                                                                    // for logging
	Load(parser DataSourceParser, widestInitialSchema Schema) (PartitionIterator, error) // how to actually load data -  how much room to leave for the largest upcoming schema (before the first repack or the next stage, if any)
	GobEncode() ([]byte, error)                                                          // how to serialize this PartitionLoader
	GobDecode([]byte) error                                                              // how to deserialize this PartitionLoader
}

// PartitionMap is an interface describing an iterator for PartitionLoaders.
// Returned by DataSource.Analyze(), a Coordinator will iterate through
// PartitionLoaders and assign them to Workers.
type PartitionMap interface {
	HasNext() bool
	Next() PartitionLoader
}

// DataSource is a source of data which will be manipulating according to transformations and actions defined in a DataFrame.
// It represents information about how to load data from the source as Partitions.
type DataSource interface {
	Analyze() (PartitionMap, error)
	DeserializeLoader([]byte) (PartitionLoader, error)
	IsStreaming() bool
}
