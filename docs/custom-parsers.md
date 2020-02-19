# Implementing Custom Parsers

A `DataSourceParser` is intended to accept a stream of `byte`s represented as an `io.Reader`, and convert it into one or more `Partition`s (containing `Row`s). This stream of `byte`s is generally provided by a `DataSource`, which itself may be reading from a file, a queue, a database, or any other conceivable source of data.

A custom `DataSourceParser` should adhere to the following interface:

```go
type DataSourceParser interface {
	// returns the maximum size of Partitions
	// produced by this DataSourceParser, in rows
	PartitionSize() int
	// lazily converts bytes from a Reader into Partitions
	Parse(
		r io.Reader,
		source sif.DataSource,
		schema sif.Schema,
		widestInitialSchema sif.Schema,
		onIteratorEnd func(),
	) (sif.PartitionIterator, error)
}
```

It is common to also provide a `CreateParser` factory function, along with a configuration struct:

```go
type ParserConf struct {
	PartitionSize int
}

func CreateParser(conf *ParserConf) *Parser {
	if conf.PartitionSize == 0 {
		conf.PartitionSize = 128
	}
	return &Parser{conf: conf}
}
```

## Implementing Parse()

The architecture of a `DataSourceParser` beyond what has already been outlined is largely up to the individual developer, though a few important details should be kept in mind:

1. `DataSourceParser`s should leverage `BuildablePartition` to construct `Partition`s, the construction of which can be accomplished via `datasource.CreateBuildablePartition`.
1. `DataSourceParser`s should be as lazy as is feasible for performance purposes - consume only as much of the `io.Reader` as is necessary, and rely on the `PartitionIterator` abstraction to lazily produce `Partition`s.

**Explore the included `DataSourceParser`s (`datasource.parser.dsv` and `datasource.parser.jsonl`) for concrete implementation examples.**
