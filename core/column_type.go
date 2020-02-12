package core

func isVariableLength(colType ColumnType) (isVariableLength bool) {
	_, isVariableLength = colType.(VarColumnType)
	return
}

// ColumnType is an interface which is implemented to define a supported fixed-width column types.
// Sif provides a variety of built-in types in the columntype package.
type ColumnType interface {
	Size() int                     // returns size in bytes of a column type
	ToString(v interface{}) string // produces a string representation of a value of this type
}

// VarColumnType is an interface which is implemented to define supported variable-length column types. Size() for VarColumnTypes should always return 0.
// Sif provides a variety of built-in types in the columntype package.
type VarColumnType interface {
	ColumnType
	Serialize(v interface{}) ([]byte, error) // Defines how this type is serialized
	Deserialize([]byte) (interface{}, error) // Defines how this type is deserialized
}
