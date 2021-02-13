package types

// An InternalBuildablePartition is a sif-internal version of a BuildablePartition
type InternalBuildablePartition interface {
	CanInsertRowData(row []byte) error                                                                                                             // CanInsertRowData checks if a Row can be inserted into this Partition
	AppendKeyedRowData(row []byte, meta []byte, varData map[string]interface{}, serializedVarRowData map[string][]byte, key uint64) error          // appendKeyedRowData appends a keyed Row to the end of this Partition
	InsertKeyedRowData(row []byte, meta []byte, varData map[string]interface{}, serializedVarRowData map[string][]byte, key uint64, pos int) error // insertKeyedRowData inserts a keyed Row into this Partition
}
