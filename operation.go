package sif

// RowFactory is a function that produces a fresh Row. Used specifically within a FlatMapOperation, a RowFactory gives the client a mechanism to return more Rows than were originally within a Partition.
type RowFactory func() Row

// AccumulatorFactory is a function that produces a fresh Accumulator
type AccumulatorFactory func() Accumulator

// DataFrameOperation - A generic DataFrame transform, returning a Task that performs the "work", a string representation of the Task, and a (potentially) altered Schema.
type DataFrameOperation func(df DataFrame) (Task, TaskType, Schema, error)

// MapOperation - A generic function for manipulating Rows in-place
type MapOperation func(row Row) error

// FilterOperation - A generic function for determining whether or not a Row should be retained
type FilterOperation func(row Row) (bool, error)

// FlatMapOperation - A generic function for turning a Row into multiple Rows. newRow() is used to produce new rows, each of which must be used before calling newRow() again.
type FlatMapOperation func(row Row, newRow RowFactory) error

// KeyingOperation - A generic function for generating a key from a Row
type KeyingOperation func(row Row) ([]byte, error)

// ReductionOperation - A generic function for reducing Rows across workers. rrow is merged into lrow, and rrow is discarded.
type ReductionOperation func(lrow Row, rrow Row) error
