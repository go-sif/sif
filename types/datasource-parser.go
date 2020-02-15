package types

import (
	"io"
)

// A DataSourceParser is capable of parsing raw data from a DataSource.Load to produce Partitions
type DataSourceParser interface {
	PartitionSize() int // returns the maximum size of Partitions produced by this DataSourceParser, in rows
	Parse(r io.Reader, source DataSource, schema Schema, widestInitialSchema Schema, onIteratorEnd func()) (PartitionIterator, error)
}
