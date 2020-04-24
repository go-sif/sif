package sif

// A Partition is a portion of a columnar dataset, consisting of multiple Rows.
// Partitions are not generally interacted with directly, instead being
// manipulated in parallel by DataFrame Tasks.
type Partition interface {
	ID() string            // ID retrieves the ID of this Partition
	GetMaxRows() int       // GetMaxRows retrieves the maximum number of rows in this Partition
	GetNumRows() int       // GetNumRows retrieves the number of rows in this Partition
	GetRow(rowNum int) Row // GetRow retrieves a specific row from this Partition
}

// A BuildablePartition can be built. Used in the implementation of DataSources and Parsers
type BuildablePartition interface {
	Partition
	ForEachRow(fn MapOperation) error                                                                                                // ForEachRow iterates over Rows in a Partition
	AppendEmptyRowData(tempRow Row) (Row, error)                                                                                     // AppendEmptyRowData is a convenient way to add an empty Row to the end of this Partition, returning the Row so that Row methods can be used to populate it
	AppendRowData(row []byte, meta []byte, varData map[string]interface{}, serializedVarRowData map[string][]byte) error             // AppendRowData adds a Row to the end of this Partition, if it isn't full and if the Row fits within the schema
	InsertRowData(row []byte, meta []byte, varRowData map[string]interface{}, serializedVarRowData map[string][]byte, pos int) error // InsertRowData inserts a Row at a specific position within this Partition, if it isn't full and if the Row fits within the schema. Other Rows are shifted as necessary.
}

// A KeyablePartition can be keyed. Used in the implementation of Partition shuffling and reduction
type KeyablePartition interface {
	KeyRows(kfn KeyingOperation) (OperablePartition, error) // KeyRows generates hash keys for a row from a key column. Attempts to manipulate partition in-place, falling back to creating a fresh partition if there are row errors
}

// An OperablePartition can be operated on
type OperablePartition interface {
	Partition
	KeyablePartition
	UpdateCurrentSchema(currentSchema Schema)                     // Sets the current schema of a Partition
	MapRows(fn MapOperation) (OperablePartition, error)           // MapRows runs a MapOperation on each row in this Partition, manipulating them in-place. Will fall back to creating a fresh partition if PartitionRowErrors occur.
	FlatMapRows(fn FlatMapOperation) ([]OperablePartition, error) // FlatMapRows runs a FlatMapOperation on each row in this Partition, creating new Partitions
	FilterRows(fn FilterOperation) (OperablePartition, error)     // FilterRows filters the Rows in the current Partition, creating a new one
	Repack(newSchema Schema) (OperablePartition, error)           // Repack repacks a Partition according to a new Schema
}

// A CollectedPartition has been collected
type CollectedPartition interface {
	Partition
	ForEachRow(fn MapOperation) error // ForEachRow iterates over Rows in a Partition
}
