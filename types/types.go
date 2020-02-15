package types

import "time"

// Column describes the byte offsets of the start
// and end of a field in a Row.
type Column interface {
	Clone() Column         // Clone returns a copy of this Column
	Index() int            // Index returns the index of this Column within a Schema
	SetIndex(newIndex int) // Modifies the Index of this Column within a Schema
	Start() int            // Start returns the Start position of this Column within a Row
	Type() ColumnType      // Type returns the ColumnType of this Column
}

// Schema is a mapping from column names to byte offsets
// within a Row. It allows one to obtain offsets by name,
// define new columns, remove columns, etc.
type Schema interface {
	Clone() Schema
	Size() int
	NumColumns() int
	NumFixedLengthColumns() int
	NumVariableLengthColumns() int
	Repack() (newSchema Schema)
	GetOffset(colName string) (offset Column, err error)
	HasColumn(colName string) bool
	CreateColumn(colName string, columnType ColumnType) (newSchema Schema, err error)
	RenameColumn(oldName string, newName string) (newSchema Schema, err error)
	RemoveColumn(colName string) (newSchema Schema, wasRemoved bool)
	ColumnNames() []string
	ColumnTypes() []ColumnType
	ForEachColumn(fn func(name string, col Column) error) error
}

// A DataFrame is a tool for constructing a chain of
// transformations and actions applied to columnar data
type DataFrame interface {
	GetSchema() Schema                           // GetSchema returns the Schema of a DataFrame
	GetDataSource() DataSource                   // GetDataSource returns the DataSource of a DataFrame
	GetParser() DataSourceParser                 // GetParser returns the DataSourceParser of a DataFrame
	To(...DataFrameOperation) (DataFrame, error) // To is a "functional operations" factory method for DataFrames, chaining operations onto the current one(s).
}

// A Task is an action or transformation applied
// to Partitions of columnar data.
type Task interface {
	RunWorker(previous OperablePartition) ([]OperablePartition, error)
}

// Row is a representation of a single row of columnar data,
// (a slice of a Partition), along with a reference to the
// Schema for that row (a mapping of column names to byte
// offsets). In practice, users of Row will call its
// getter and setter methods to retrieve, manipulate and store data
type Row interface {
	Schema() Schema              // Schema returns a read-only copy of the schema for a row
	ToString() string            // ToString returns a string representation of this row
	IsNil(colName string) bool   // IsNil returns true iff the given column value is nil in this row. If an error occurs, this function will return false.
	SetNil(colName string) error // SetNil sets the given column value to nil within this row
	// TODO make private
	CheckIsNil(colName string, offset Column) error // checkIsNil is for internal use only
	SetNotNil(offset Column)                        // setNotNil is for internal use only
	// end TODO
	Get(colName string) (col interface{}, err error)                // Get returns the value of any column as an interface{}, if it exists
	GetByte(colName string) (col byte, err error)                   // GetByte retrieves a single byte from the column with the given name.
	GetBytes(colName string) (col []byte, err error)                // GetBytes retrieves a multiple byte from the column with the given name.
	GetBool(colName string) (col bool, err error)                   // GetBool retrieves a single bool from the column with the given name.
	GetUint8(colName string) (col uint8, err error)                 // GetUint8 retrieves a single uint8 from the column with the given name.
	GetUint16(colName string) (col uint16, err error)               // GetUint16 retrieves a single uint16 from the column with the given name
	GetUint32(colName string) (col uint32, err error)               // GetUint32 retrieves a single uint32 from the column with the given name
	GetUint64(colName string) (col uint64, err error)               // GetUint64 retrieves a single uint64 from the column with the given name
	GetInt8(colName string) (col int8, err error)                   // GetInt8 retrieves a single int8 from the column with the given name
	GetInt16(colName string) (col int16, err error)                 // GetInt16 retrieves a single int16 from the column with the given name
	GetInt32(colName string) (col int32, err error)                 // GetInt32 retrieves a single int32 from the column with the given name
	GetInt64(colName string) (col int64, err error)                 // GetInt64 retrieves a single int64 from the column with the given name
	GetFloat32(colName string) (col float32, err error)             // GetFloat32 retrieves a single float32 from the column with the given name
	GetFloat64(colName string) (col float64, err error)             // GetFloat64 retrieves a single float64 from the column with the given name
	GetTime(colName string) (col time.Time, err error)              // GetTime retrieves a single Time from the column with the given name
	GetString(colName string) (string, error)                       // GetString returns a single, fixed-length string value from the column with the given name
	GetVarCustomData(colName string) (interface{}, error)           // GetVarCustomData retrieves variable-length data of a custom type from the column with the given name
	GetVarBytes(colName string) (col []byte, err error)             // GetVarBytes retrieves a variable-length byte array from the column with the given name
	GetVarString(colName string) (col string, err error)            // GetVarString retrieves a single string from the column with the given name
	SetByte(colName string, value byte) (err error)                 // SetByte modifies a single byte from the column with the given name.
	SetBytes(colName string, value []byte) (err error)              // SetBytes overwrites multiple byte from the column with the given name.
	SetBool(colName string, value bool) (err error)                 // SetBool modifies a single bool from the column with the given name.
	SetUint8(colName string, value uint8) (err error)               // SetUint8 modifies a single uint8 from the column with the given name.
	SetUint16(colName string, value uint16) (err error)             // SetUint16 modifies a single uint16 from the column with the given name.
	SetUint32(colName string, value uint32) (err error)             // SetUint32 modifies a single uint32 from the column with the given name.
	SetUint64(colName string, value uint64) (err error)             // SetUint64 modifies a single uint64 from the column with the given name.
	SetInt8(colName string, value int8) (err error)                 // SetInt8 modifies a single int8 from the column with the given name.
	SetInt16(colName string, value int16) (err error)               // SetInt16 modifies a single int16 from the column with the given name.
	SetInt32(colName string, value int32) (err error)               // SetInt32 modifies a single int32 from the column with the given name.
	SetInt64(colName string, value int64) (err error)               // SetInt64 modifies a single int64 from the column with the given name.
	SetFloat32(colName string, value float32) (err error)           // SetFloat32 modifies a single float32 from the column with the given name.
	SetFloat64(colName string, value float64) (err error)           // SetFloat64 modifies a single float64 from the column with the given name.
	SetTime(colName string, value time.Time) (err error)            // SetTime modifies a single Time from the column with the given name.
	SetString(colName string, value string) (err error)             // SetString modifies a single fixed-length string from the column with the given name.
	SetVarCustomData(colName string, value interface{}) (err error) // SetVarCustomData stores variable-length data of a custom type in this Row
	SetVarBytes(colName string, value []byte) (err error)           // SetVarBytes modifies a single variable-length byte array from the column with the given name.
	SetVarString(colName string, value string) (err error)          // SetVarString modifies a single string from the column with the given name.
	// TODO make private by splitting into new interface like partition
	Repack(newSchema Schema) (Row, error) // Repack resizes a row to a new Schema
}

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
	// TODO make private by splitting into new interface
	CanInsertRowData(row []byte) error                                                                                   // CanInsertRowData checks if a Row can be inserted into this Partition
	AppendEmptyRowData() (Row, error)                                                                                    // AppendEmptyRowData is a convenient way to add an empty Row to the end of this Partition, returning the Row so that Row methods can be used to populate it
	AppendRowData(row []byte, meta []byte, varData map[string]interface{}, serializedVarRowData map[string][]byte) error // AppendRowData adds a Row to the end of this Partition, if it isn't full and if the Row fits within the schema
	// TODO make private by splitting into new interface
	AppendKeyedRowData(row []byte, meta []byte, varData map[string]interface{}, serializedVarRowData map[string][]byte, key uint64) error // appendKeyedRowData appends a keyed Row to the end of this Partition
	InsertRowData(row []byte, meta []byte, varRowData map[string]interface{}, serializedVarRowData map[string][]byte, pos int) error      // InsertRowData inserts a Row at a specific position within this Partition, if it isn't full and if the Row fits within the schema. Other Rows are shifted as necessary.
	// TODO make private by splitting into new interface
	InsertKeyedRowData(row []byte, meta []byte, varData map[string]interface{}, serializedVarRowData map[string][]byte, key uint64, pos int) error // insertKeyedRowData inserts a keyed Row into this Partition
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
