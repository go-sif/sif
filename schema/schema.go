package schema

import (
	"fmt"
	"reflect"
	"sort"

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
	schema   map[string]sif.Column
	toRemove map[string]bool
	size     int
}

// CreateSchema is a factory for Schemas
func CreateSchema() sif.Schema {
	return &schema{
		schema:   make(map[string]sif.Column),
		toRemove: make(map[string]bool),
		size:     0,
	}
}

// Equals returns true iff this and another Schema are equivalent
func (s *schema) Equals(otherSchema sif.Schema) error {
	if s.Size() != otherSchema.Size() {
		return fmt.Errorf("Schemas have unequal sizes")
	}
	if s.NumFixedLengthColumns() != otherSchema.NumFixedLengthColumns() {
		return fmt.Errorf("Schemas have unequal numbers of fixed-length columns")
	}
	if s.NumVariableLengthColumns() != otherSchema.NumVariableLengthColumns() {
		return fmt.Errorf("Schemas have unequal numbers of variable-length columns")
	}
	return s.ForEachColumn(func(name string, offset sif.Column) error {
		otherOffset, err := otherSchema.GetOffset(name)
		if err != nil {
			return err
		}
		if offset.Start() != otherOffset.Start() {
			return fmt.Errorf("Column %s offsets do not match", name)
		}
		if offset.Index() != otherOffset.Index() {
			return fmt.Errorf("Column %s indices do not match", name)
		}
		if reflect.TypeOf(offset.Type()) != reflect.TypeOf(otherOffset.Type()) {
			return fmt.Errorf("Column %s types do not match", name)
		}
		if offset.Type().Size() != otherOffset.Type().Size() {
			return fmt.Errorf("Column %s type fields do not match", name)
		}
		return nil
	})
}

// Clone returns a copy of this Schema
func (s *schema) Clone() sif.Schema {
	newSchema := make(map[string]sif.Column)
	for k, v := range s.schema {
		newSchema[k] = v.Clone()
	}
	newRemoved := make(map[string]bool)
	for k, v := range s.toRemove {
		newRemoved[k] = v
	}
	return &schema{schema: newSchema, size: s.size, toRemove: newRemoved}
}

// RowWidth returns the current byte size of a Row respecting this Schema, without padding
func (s *schema) RowWidth() int {
	return s.size
}

// Size returns the current byte size of a Row respecting this Schema, padded so rows fit neatly into 64 bit chunks
func (s *schema) Size() int {
	if s.size < 16 {
		return 16
	} else if s.size < 32 {
		return 32
	} else if s.size < 64 {
		return 64
	} else if s.size%64 != 0 {
		return ((s.size / 64) + 1) * 64
	} else {
		return (s.size / 64) * 64
	}
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

// NumRemovedColumns returns the number of removed columns in this Schema
func (s *schema) NumRemovedColumns() int {
	return len(s.toRemove)
}

// Repack optimizes the memory layout of the Schema, removing any gaps in fixed-length data.
func (s *schema) Repack() (newSchema sif.Schema) {
	newSchema = &schema{
		schema: make(map[string]sif.Column),
	}
	// we need the column names in index order
	cols := make([]string, 0, len(s.ColumnNames())-s.NumRemovedColumns())
	for k := range s.schema {
		if !s.toRemove[k] {
			cols = append(cols, k)
		}
	}
	sort.Slice(cols, func(i, j int) bool {
		return s.schema[cols[i]].Index() < s.schema[cols[j]].Index()
	})
	// re-insert into fresh schema in original index order
	for _, name := range cols {
		newSchema, _ = newSchema.CreateColumn(name, s.schema[name].Type())
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
	if s.IsMarkedForRemoval(oldName) {
		return nil, fmt.Errorf("Cannot rename removed column %s", oldName)
	}
	_, err = s.GetOffset(oldName)
	if err == nil {
		s.schema[newName] = s.schema[oldName]
		delete(s.schema, oldName)
		newSchema = s
	}
	return
}

// RemoveColumn marks a column for removal from the Schema, at a convenient time
// This does not alter the schema, other than to mark the column for later removal
func (s *schema) RemoveColumn(colName string) (sif.Schema, bool) {
	_, canRemoved := s.schema[colName]
	if !canRemoved {
		panic(fmt.Errorf("Cannot remove column %s because it does not exist", colName))
	}
	s.toRemove[colName] = true
	return s, true
}

// IsMarkedForRemoval returns true iff the given column has been marked for removal
func (s *schema) IsMarkedForRemoval(colName string) bool {
	_, marked := s.toRemove[colName]
	return marked
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
