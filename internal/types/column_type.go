package types

import "github.com/go-sif/sif"

// IsVariableLength returns true iff colType is a VarColumnType
func IsVariableLength(colType sif.ColumnType) (isVariableLength bool) {
	_, isVariableLength = colType.(VarColumnType)
	return
}

// VarColumnType is an interface which is implemented to define supported variable-length column types. Size() for VarColumnTypes should always return 0.
// Sif provides a variety of built-in types in the columntype package.
type VarColumnType interface {
	sif.ColumnType
	Serialize(v interface{}) ([]byte, error) // Defines how this type is serialized
	Deserialize([]byte) (interface{}, error) // Defines how this type is deserialized
}
