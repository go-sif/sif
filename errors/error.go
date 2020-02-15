package errors

import (
	"fmt"
)

// NilValueError occurs when a value in a Row is null
type NilValueError struct{ Name string }

// Error returns a textual representation of this NilValueError
func (e NilValueError) Error() string {
	return fmt.Sprintf("Value for column %s is nil", e.Name)
}

// MissingKeyError occurs when FindFirstKey cannot find a given key in this Partition
type MissingKeyError struct{}

// Error returns a textual representation of this MissingKeyError
func (e MissingKeyError) Error() string {
	return "Key does not exist in partition"
}

// IncompatibleRowError occurs when a Row's width does not match an expected Schema
type IncompatibleRowError struct{}

// Error returns a textual representation of this IncompatibleRowError
func (e IncompatibleRowError) Error() string {
	return "Row width is not compatible with Schema"
}

// PartitionFullError occurs when a Partition has reached its max size an a new Row insertion is attempted
type PartitionFullError struct{}

// Error returns a textual representation of this PartitionFullError
func (e PartitionFullError) Error() string {
	return "Partition is full"
}

// NoMorePartitionsError occurs when there are no more partitions in a PartitionIterator
type NoMorePartitionsError struct{}

// Error returns a textual representation of this NoMorePartitionsError
func (e NoMorePartitionsError) Error() string {
	return "No more partitions"
}
