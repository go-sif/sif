package types

import "github.com/go-sif/sif"

// AccessibleRow allows access to Row internals
type AccessibleRow interface {
	sif.Row
	GetMeta() []byte
	GetData() []byte
	GetColData(colName string) ([]byte, error)
	GetVarData() map[string]interface{}
	GetSerializedVarData() map[string][]byte
	GetSchema() sif.Schema
	CheckIsNil(colName string, offset sif.Column) error
	SetNotNil(offset sif.Column)
	Repack(newSchema sif.Schema) (sif.Row, error)
}
