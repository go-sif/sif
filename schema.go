package sif

// Schema is a mapping from column names to byte offsets
// within a Row. It allows one to obtain offsets by name,
// define new columns, remove columns, etc.
type Schema interface {
	Equals(otherSchema Schema) error                                                  // Equals return true iff this Schema is identical to otherSchema
	Clone() Schema                                                                    // Clone() returns a deep copy of this Schema
	RowWidth() int                                                                    // RowWidth returns the total size of the data required to store a Row respecting this Schema (not including padding). Includes removed columns.
	Size() int                                                                        // Size returns the total size of the data required to store a Row respecting this Schema (including padding). Includes removed columns.
	NumColumns() int                                                                  // NumColumns returns the number of columns in this Schema. Includes removed columns.
	NumFixedLengthColumns() int                                                       // NumFixedLengthColumns returns the number of fixed-length columns in this Schema. Includes removed columns.
	NumVariableLengthColumns() int                                                    // NumVariableLengthColumns returns the number of variable-length columns in this Schema. Includes removed columns.
	NumRemovedColumns() int                                                           // NumRemovedColumns returns the number of removed columns in this Schema
	Repack() (newSchema Schema)                                                       // Repack optimizes the Schema, truly removing removed Columns
	GetColumn(colName string) (offset Column, err error)                              // GetColumn returns the Column associated with colName, or an error if none exists
	HasColumn(colName string) bool                                                    // HasColumn returns true iff colName corresponds to a Column in this Schema
	CreateColumn(colName string, columnType ColumnType) (newSchema Schema, err error) // CreateColumn defines a new Column in this Schema
	RenameColumn(oldName string, newName string) (newSchema Schema, err error)        // RenameColumn renames a Column in this Schema, returning a reference to this Schema
	RemoveColumn(colName string) (newSchema Schema, wasRemoved bool)                  // RemoveColumn marks a Column for removal during the next Repack(), but does not actually remove it
	IsMarkedForRemoval(colName string) bool                                           // IsMarkedForRemoval returns true iff the Column corresponding to colName exists and has been Remove()d
	ColumnNames() []string                                                            // ColumnNames returns a list of Column names in this Schema, in the order they were created
	ColumnTypes() []ColumnType                                                        // ColumnTypes returns a list of Column types in this Schema, in the order they were created
	ForEachColumn(fn func(name string, col Column) error) error                       // ForEachColumn runs a function for each Column in this Schema
}
