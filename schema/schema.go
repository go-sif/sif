package schema

import (
	"fmt"

	"github.com/go-sif/sif"
)

// Column describes the byte offsets of the start
// and end of a field in a Row.
type column struct {
	idx     int
	start   int
	colType sif.ColumnType
}

// Clone returns a copy of this Column
func (c *column) Clone() sif.Column {
	return &column{c.idx, c.start, c.colType} // TODO careful with not cloning column type
}

// Index returns the index of this Column within a Schema
func (c *column) Index() int {
	return c.idx
}

// SetIndex modifies the index of this Column within a Schema
func (c *column) SetIndex(newIndex int) {
	c.idx = newIndex
}

// Start returns the Start position of this Column within a Row
func (c *column) Start() int {
	return c.start
}

// Type returns the ColumnType of this Column
func (c *column) Type() sif.ColumnType {
	return c.colType
}

// Schema is a mapping from column names to byte offsets
// within a Row. It allows one to obtain offsets by name,
// define new columns, remove columns, etc.
type schema struct {
	schema map[string]sif.Column
	size   int
}

// CreateSchema is a factory for Schemas
func CreateSchema() sif.Schema {
	return &schema{
		schema: make(map[string]sif.Column),
		size:   0,
	}
}

// Equals returns true iff this and another Schema are equivalent
func (s *schema) Equals(otherSchema sif.Schema) bool {
	if s.Size() != otherSchema.Size() {
		return false
	}
	if s.NumFixedLengthColumns() != otherSchema.NumFixedLengthColumns() {
		return false
	}
	if s.NumVariableLengthColumns() != otherSchema.NumVariableLengthColumns() {
		return false
	}
	return s.ForEachColumn(func(name string, offset sif.Column) error {
		otherOffset, err := otherSchema.GetOffset(name)
		if err != nil {
			return err
		}
		if offset.Start() != otherOffset.Start() {
			return fmt.Errorf("Column %s does not match", name)
		}
		if offset.Type() != otherOffset.Type() {
			return fmt.Errorf("Column %s does not match", name)
		}
		return nil
	}) != nil
}

// Clone returns a copy of this Schema
func (s *schema) Clone() sif.Schema {
	newSchema := make(map[string]sif.Column)
	for k, v := range s.schema {
		newSchema[k] = v.Clone()
	}
	return &schema{schema: newSchema, size: s.size}
}

// Size returns the current byte size of a Row respecting this Schema
func (s *schema) Size() int {
	return s.size
}

// NumColumns returns the number of columns (fixed-length and variable-length) in this Schema
func (s *schema) NumColumns() int {
	return len(s.schema)
}

// NumFixedLengthColumns returns the number of fixed-length columns in this Schema
func (s *schema) NumFixedLengthColumns() int {
	i := 0
	for _, col := range s.schema {
		if !sif.IsVariableLength(col.Type()) {
			i++
		}
	}
	return i
}

// NumVariableLengthColumns returns the number of variable-length columns in this Schema
func (s *schema) NumVariableLengthColumns() int {
	i := 0
	for _, col := range s.schema {
		if sif.IsVariableLength(col.Type()) {
			i++
		}
	}
	return i
}

// Repack optimizes the memory layout of the Schema, removing any gaps in fixed-length data.
func (s *schema) Repack() (newSchema sif.Schema) {
	newSchema = &schema{
		schema: make(map[string]sif.Column),
	}
	for k, v := range s.schema {
		newSchema, _ = newSchema.CreateColumn(k, v.Type())
	}
	return
}

// GetOffset returns the byte offset of a particular column within a row.
func (s *schema) GetOffset(colName string) (offset sif.Column, err error) {
	offset, ok := s.schema[colName]
	if !ok {
		err = fmt.Errorf("Schema does not contain column with name %s", colName)
	}
	return
}

// HasColumn returns true iff this schema contains a column with the given name
func (s *schema) HasColumn(colName string) bool {
	_, err := s.GetOffset(colName)
	return err == nil
}

// CreateColumn defines a new column within the Schema
func (s *schema) CreateColumn(colName string, columnType sif.ColumnType) (newSchema sif.Schema, err error) {
	_, containsOffset := s.schema[colName]
	if containsOffset {
		err = fmt.Errorf("Schema already contains column with name %s", colName)
	} else {
		if !sif.IsVariableLength(columnType) {
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
func (s *schema) RenameColumn(oldName string, newName string) (newSchema sif.Schema, err error) {
	_, err = s.GetOffset(oldName)
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
func (s *schema) RemoveColumn(colName string) (sif.Schema, bool) {
	removed, canRemoved := s.schema[colName]
	if !canRemoved {
		panic(fmt.Errorf("Cannot remove column %s because it does not exist", colName))
	}
	delete(s.schema, colName)
	// update indices
	for _, v := range s.schema {
		if v.Index() > removed.Index() {
			v.SetIndex(v.Index() - 1)
		}
	}
	return s, true
}

// ColumnNames returns the names in the schema, in index order
func (s *schema) ColumnNames() []string {
	names := make([]string, len(s.schema))
	for k, v := range s.schema {
		names[v.Index()] = k
	}
	return names
}

// ColumnTypes returns the types in the schema, in index order
func (s *schema) ColumnTypes() []sif.ColumnType {
	types := make([]sif.ColumnType, len(s.schema))
	for _, v := range s.schema {
		types[v.Index()] = v.Type()
	}
	return types
}

// ForEachColumn iterates over the columns in this Schema. Does not necessarily iterate in order of column index.
func (s *schema) ForEachColumn(fn func(name string, col sif.Column) error) error {
	for k, v := range s.schema {
		err := fn(k, v)
		if err != nil {
			return err
		}
	}
	return nil
}
