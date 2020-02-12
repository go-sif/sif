package core

import (
	"fmt"
)

// column describes the byte offsets of the start
// and end of a field in a Row.
type column struct {
	idx     int
	start   int
	colType ColumnType
}

// Return a copy of this Column
func (c *column) clone() *column {
	return &column{c.idx, c.start, c.colType} // TODO careful with not cloning column type
}

// Schema is a mapping from column names to byte offsets
// within a Row. It allows one to obtain offsets by name,
// define new columns, remove columns, etc.
type Schema struct {
	schema map[string]*column
	size   int
}

// CreateSchema is a factory for Schemas
func CreateSchema() *Schema {
	return &Schema{
		schema: make(map[string]*column),
		size:   0,
	}
}

// Clone returns a copy of this Schema
func (s *Schema) Clone() *Schema {
	newSchema := make(map[string]*column)
	for k, v := range s.schema {
		newSchema[k] = v.clone()
	}
	return &Schema{schema: newSchema, size: s.size}
}

// Size returns the current byte size of a Row respecting this Schema
func (s *Schema) Size() int {
	return s.size
}

// NumColumns returns the number of columns (fixed-length and variable-length) in this Schema
func (s *Schema) NumColumns() int {
	return len(s.schema)
}

// NumFixedLengthColumns returns the number of fixed-length columns in this Schema
func (s *Schema) NumFixedLengthColumns() int {
	i := 0
	for _, col := range s.schema {
		if !isVariableLength(col.colType) {
			i++
		}
	}
	return i
}

// NumVariableLengthColumns returns the number of variable-length columns in this Schema
func (s *Schema) NumVariableLengthColumns() int {
	i := 0
	for _, col := range s.schema {
		if isVariableLength(col.colType) {
			i++
		}
	}
	return i
}

// Repack optimizes the memory layout of the Schema, removing any gaps in fixed-length data.
func (s *Schema) Repack() (newSchema *Schema) {
	newSchema = &Schema{
		schema: make(map[string]*column),
	}
	for k, v := range s.schema {
		newSchema, _ = newSchema.CreateColumn(k, v.colType)
	}
	return
}

// getOffset returns the byte offset of a particular column within a row.
func (s *Schema) getOffset(colName string) (offset *column, err error) {
	offset, ok := s.schema[colName]
	if !ok {
		err = fmt.Errorf("Schema does not contain column with name %s", colName)
	}
	return
}

// CreateColumn defines a new column within the Schema
func (s *Schema) CreateColumn(colName string, columnType ColumnType) (newSchema *Schema, err error) {
	_, containsOffset := s.schema[colName]
	if containsOffset {
		err = fmt.Errorf("Schema already contains column with name %s", colName)
	} else {
		if !isVariableLength(columnType) {
			s.schema[colName] = &column{len(s.schema), s.size, columnType}
			s.size += columnType.Size()
		} else {
			s.schema[colName] = &column{len(s.schema), 0, columnType}
		}
		newSchema = s
	}
	return
}

// RenameColumn renames a column within the Schema
func (s *Schema) RenameColumn(oldName string, newName string) (newSchema *Schema, err error) {
	_, err = s.getOffset(oldName)
	if err == nil {
		s.schema[newName] = s.schema[oldName]
		delete(s.schema, oldName)
		newSchema = s
	}
	return
}

// RemoveColumn removes a column from the Schema
// This does not adjust the size of the Schema, as data is not moved
// when a column is deleted.
func (s *Schema) RemoveColumn(colName string) (newSchema *Schema, wasRemoved bool) {
	newSchema = s
	removed, wasRemoved := s.schema[colName]
	if wasRemoved {
		delete(s.schema, colName)
	}
	// update indices
	for _, v := range s.schema {
		if v.idx > removed.idx {
			v.idx--
		}
	}
	return
}

// ColumnNames returns the names in the schema, in index order
func (s *Schema) ColumnNames() []string {
	names := make([]string, len(s.schema))
	for k, v := range s.schema {
		names[v.idx] = k
	}
	return names
}

// ColumnTypes returns the types in the schema, in index order
func (s *Schema) ColumnTypes() []ColumnType {
	types := make([]ColumnType, len(s.schema))
	for _, v := range s.schema {
		types[v.idx] = v.colType
	}
	return types
}

// ForEachColumn iterates over the columns in this Schema. Does not necessarily iterate in order of column index.
func (s *Schema) ForEachColumn(fn func(name string, col *column) error) error {
	for k, v := range s.schema {
		err := fn(k, v)
		if err != nil {
			return err
		}
	}
	return nil
}
