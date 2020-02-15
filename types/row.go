package types

import "time"

// Row is a representation of a single row of columnar data,
// (a slice of a Partition), along with a reference to the
// Schema for that row (a mapping of column names to byte
// offsets). In practice, users of Row will call its
// getter and setter methods to retrieve, manipulate and store data
type Row interface {
	Schema() Schema                                                 // Schema returns a read-only copy of the schema for a row
	ToString() string                                               // ToString returns a string representation of this row
	IsNil(colName string) bool                                      // IsNil returns true iff the given column value is nil in this row. If an error occurs, this function will return false.
	SetNil(colName string) error                                    // SetNil sets the given column value to nil within this row
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
}
