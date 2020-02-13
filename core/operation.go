package core

import (
	"fmt"
)

// RowFactory is a function that produces a fresh Row. Used specifically within a FlatMapOperation, a RowFactory gives the client a mechanism to return more Rows than were originally within a Partition.
type RowFactory func() *Row

// DataFrameOperation - A generic DataFrame transform, returning a Task that performs the "work", a string representation of the Task, and a (potentially) altered Schema.
type DataFrameOperation func(df DataFrame) (Task, string, *Schema, error)

// MapOperation - A generic function for manipulating Rows in-place
type MapOperation func(row *Row) error

// FilterOperation - A generic function for determining whether or not a Row should be retained
type FilterOperation func(row *Row) (bool, error)

// FlatMapOperation - A generic function for turning a Row into multiple Rows
type FlatMapOperation func(row *Row, newRow RowFactory) ([]*Row, error)

// KeyingOperation - A generic function for generating a key from a Row
type KeyingOperation func(row *Row) ([]byte, error)

// ReductionOperation - A generic function for reducing Rows across workers. rrow is merged into lrow, and rrow is discarded.
type ReductionOperation func(lrow *Row, rrow *Row) error

// SafeMapOperation wraps a MapOperation such that panics are recovered and nice error messages are constructed
func SafeMapOperation(mapOp MapOperation) (safeMapOp MapOperation) {
	return func(row *Row) (err error) {
		defer func() {
			if r := recover(); r != nil {
				if anErr, ok := r.(error); ok {
					err = fmt.Errorf("Map Panic: %w\nRow: %s\n%s", anErr, row.ToString(), getTrace())
				} else {
					err = fmt.Errorf("Map Panic: %v\nRow: %s\n%s", r, row.ToString(), getTrace())
				}
			} else if err != nil {
				err = fmt.Errorf("Map Error: %w\nRow: %s", err, row.ToString())
			}
		}()
		err = mapOp(row)
		return
	}
}

// SafeFilterOperation wraps a FilterOperation such that panics are recovered and nice error messages are constructed
func SafeFilterOperation(filterOp FilterOperation) (safeFilterOp FilterOperation) {
	return func(row *Row) (shouldFilter bool, err error) {
		defer func() {
			if r := recover(); r != nil {
				if anErr, ok := r.(error); ok {
					err = fmt.Errorf("Filter Panic: %w\nRow: %s\n%s", anErr, row.ToString(), getTrace())
				} else {
					err = fmt.Errorf("Filter Panic: %v\nRow: %s\n%s", r, row.ToString(), getTrace())
				}
			} else if err != nil {
				err = fmt.Errorf("Filter Error: %w\nRow: %s", err, row.ToString())
			}
		}()
		shouldFilter, err = filterOp(row)
		return
	}
}

// SafeFlatMapOperation wraps a FlatMapOperation such that panics are recovered and nice error messages are constructed
func SafeFlatMapOperation(flatMapOp FlatMapOperation) (safeFlatMapOp FlatMapOperation) {
	return func(row *Row, newRow RowFactory) (result []*Row, err error) {
		defer func() {
			if r := recover(); r != nil {
				if anErr, ok := r.(error); ok {
					err = fmt.Errorf("FlatMap Panic: %w\nRow: %s\n%s", anErr, row.ToString(), getTrace())
				} else {
					err = fmt.Errorf("FlatMap Panic: %v\nRow: %s\n%s", r, row.ToString(), getTrace())
				}
			} else if err != nil {
				err = fmt.Errorf("FlatMap Error: %w\nRow: %s", err, row.ToString())
			}
		}()
		result, err = flatMapOp(row, newRow)
		return
	}
}

// SafeKeyingOperation wraps a KeyingOperation such that panics are recovered and nice error messages are constructed
func SafeKeyingOperation(keyingOp KeyingOperation) (safeKeyingOp KeyingOperation) {
	return func(row *Row) (key []byte, err error) {
		defer func() {
			if r := recover(); r != nil {
				if anErr, ok := r.(error); ok {
					err = fmt.Errorf("Keying Panic: %w\nRow: %s\n%s", anErr, row.ToString(), getTrace())
				} else {
					err = fmt.Errorf("Keying Panic: %v\nRow: %s\n%s", r, row.ToString(), getTrace())
				}
			} else if err != nil {
				err = fmt.Errorf("Keying Error: %w\nRow: %s", err, row.ToString())
			}
		}()
		key, err = keyingOp(row)
		return
	}
}

// SafeReductionOperation wraps a ReductionOperation such that panics are recovered and nice error messages are constructed
func SafeReductionOperation(reductionOp ReductionOperation) (safeReductionOp ReductionOperation) {
	return func(lrow, rrow *Row) (err error) {
		defer func() {
			if r := recover(); r != nil {
				if anErr, ok := r.(error); ok {
					err = fmt.Errorf("Reduction Panic: %w\nLRow: %s\nRRow: %s\n%s", anErr, lrow.ToString(), rrow.ToString(), getTrace())
				} else {
					err = fmt.Errorf("Reduction Panic: %v\nLRow: %s\nRRow: %s\n%s", r, lrow.ToString(), rrow.ToString(), getTrace())
				}
			} else if err != nil {
				err = fmt.Errorf("Reduction Error: %w\nLRow: %s\nRRow: %s", err, lrow.ToString(), rrow.ToString())
			}
		}()
		err = reductionOp(lrow, rrow)
		return
	}
}
