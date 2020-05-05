package sif

// Schema is a mapping from column names to byte offsets
// within a Row. It allows one to obtain offsets by name,
// define new columns, remove columns, etc.
type Schema interface {
	Equals(otherSchema Schema) bool
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
