# Implementing Custom DataSources

`DataSource` is `Sif`'s abstraction for a source of `Partition`s. `DataSource`s adhere to the following interface:

```go
type DataSource interface {
	// See below for details
	Analyze() (PartitionMap, error)
	// For deserializing a PartitionLoader, commonly
	// constructing a fresh one and calling GobDecode
	DeserializeLoader([]byte) (PartitionLoader, error)
	// returns true iff this is a streaming DataSource
	IsStreaming() bool
}
```

## Implementing Analyze()

The first task of any `DataSource` is to produce a `PartitionMap`. A `PartitionMap` represents a sequence of "units of work" (`PartitionLoader`s) which can be assigned to individual `Worker`s. Each "unit of work" is a task which will produce one or more `Partition`s.

For example, when loading a directory of files, `datasource.file` produces a `PartitionMap` where each `PartitionLoader` represents an individual file, and each `Worker` receiving one of these `PartitionLoader`s uses it to read the file and produce `Partition`s.

## Factory

It is commonplace for a `DataSource` package to also provide a `CreateDataFrame` factory function, which accepts configuration, a `Schema` and a `DataSourceParser`, instantiates the `DataSource`, and passes it to `datasource.CreateDataFrame()`. For example, `datasource.file`'s factory function looks like this:

```go
func CreateDataFrame(glob string, parser sif.DataSourceParser, schema sif.Schema) sif.DataFrame {
	source := &DataSource{glob, schema}
	df := datasource.CreateDataFrame(source, parser, schema)
	return df
}
```

**Explore the included `DataSource`s (particularly `datasource.file`) for concrete implementation examples.**
