package types

import (
	"github.com/go-sif/sif/types"
)

// AccessibleRow allows access to Row internals
type AccessibleRow interface {
	types.Row
	GetMeta() []byte
	GetData() []byte
	GetVarData() map[string]interface{}
	GetSerializedVarData() map[string][]byte
	GetSchema() types.Schema
	CheckIsNil(colName string, offset types.Column) error
	SetNotNil(offset types.Column)
	Repack(newSchema types.Schema) (AccessibleRow, error)
}
