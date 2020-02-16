package util

import (
	"fmt"

	"github.com/go-sif/sif"
)

// SafeMapOperation wraps a MapOperation such that panics are recovered and nice error messages are constructed
func SafeMapOperation(mapOp sif.MapOperation) (safeMapOp sif.MapOperation) {
	return func(row sif.Row) (err error) {
		defer func() {
			if r := recover(); r != nil {
				if anErr, ok := r.(error); ok {
					err = fmt.Errorf("Map Panic: %w\nRow: %s\n%s", anErr, row.ToString(), GetTrace())
				} else {
					err = fmt.Errorf("Map Panic: %v\nRow: %s\n%s", r, row.ToString(), GetTrace())
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
func SafeFilterOperation(filterOp sif.FilterOperation) (safeFilterOp sif.FilterOperation) {
	return func(row sif.Row) (shouldFilter bool, err error) {
		defer func() {
			if r := recover(); r != nil {
				if anErr, ok := r.(error); ok {
					err = fmt.Errorf("Filter Panic: %w\nRow: %s\n%s", anErr, row.ToString(), GetTrace())
				} else {
					err = fmt.Errorf("Filter Panic: %v\nRow: %s\n%s", r, row.ToString(), GetTrace())
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
func SafeFlatMapOperation(flatMapOp sif.FlatMapOperation) (safeFlatMapOp sif.FlatMapOperation) {
	return func(row sif.Row, newRow sif.RowFactory) (result []sif.Row, err error) {
		defer func() {
			if r := recover(); r != nil {
				if anErr, ok := r.(error); ok {
					err = fmt.Errorf("FlatMap Panic: %w\nRow: %s\n%s", anErr, row.ToString(), GetTrace())
				} else {
					err = fmt.Errorf("FlatMap Panic: %v\nRow: %s\n%s", r, row.ToString(), GetTrace())
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
func SafeKeyingOperation(keyingOp sif.KeyingOperation) (safeKeyingOp sif.KeyingOperation) {
	return func(row sif.Row) (key []byte, err error) {
		defer func() {
			if r := recover(); r != nil {
				if anErr, ok := r.(error); ok {
					err = fmt.Errorf("Keying Panic: %w\nRow: %s\n%s", anErr, row.ToString(), GetTrace())
				} else {
					err = fmt.Errorf("Keying Panic: %v\nRow: %s\n%s", r, row.ToString(), GetTrace())
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
func SafeReductionOperation(reductionOp sif.ReductionOperation) (safeReductionOp sif.ReductionOperation) {
	return func(lrow, rrow sif.Row) (err error) {
		defer func() {
			if r := recover(); r != nil {
				if anErr, ok := r.(error); ok {
					err = fmt.Errorf("Reduction Panic: %w\nLRow: %s\nRRow: %s\n%s", anErr, lrow.ToString(), rrow.ToString(), GetTrace())
				} else {
					err = fmt.Errorf("Reduction Panic: %v\nLRow: %s\nRRow: %s\n%s", r, lrow.ToString(), rrow.ToString(), GetTrace())
				}
			} else if err != nil {
				err = fmt.Errorf("Reduction Error: %w\nLRow: %s\nRRow: %s", err, lrow.ToString(), rrow.ToString())
			}
		}()
		err = reductionOp(lrow, rrow)
		return
	}
}
