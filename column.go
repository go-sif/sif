package sif

// Column describes the byte offsets of the start
// and end of a field in a Row.
type Column interface {
	Clone() Column         // Clone returns a copy of this Column
	Index() int            // Index returns the index of this Column within a Schema
	SetIndex(newIndex int) // Modifies the Index of this Column within a Schema
	Start() int            // Start returns the Start (byte) position of this Column within a Row. Not relevant for variable-length columns.
	Type() ColumnType      // Type returns the ColumnType of this Column
}
