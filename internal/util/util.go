package util

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/cespare/xxhash"
	"github.com/go-sif/sif"
	itypes "github.com/go-sif/sif/internal/types"
)

// CreateAsyncErrorChannel produces a channel for errors
func CreateAsyncErrorChannel() chan error {
	return make(chan error)
}

// WaitAndFetchError attempts to fetch an error from an async goroutine
func WaitAndFetchError(wg *sync.WaitGroup, errors chan error) error {
	// use reading from the errors channel to block, rather than
	// the WaitGroup directly. when the wg is done, we know there
	// aren't any more errors. Closing the channel breaks us out
	// of the infinite loop.
	go func() {
		wg.Wait()
		close(errors)
	}()
	for {
		select {
		case err := <-errors:
			if err != nil {
				return err
			}
			return nil
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// GetTrace produces the string representation of a stack trace
func GetTrace() string {
	var name, file string
	var line int
	var pc [16]uintptr
	var res strings.Builder
	n := runtime.Callers(3, pc[:])
	for _, pc := range pc[:n] {
		fn := runtime.FuncForPC(pc)
		if fn == nil {
			continue
		}
		file, line = fn.FileLine(pc)
		name = fn.Name()
		if !strings.HasPrefix(name, "runtime.") {
			fmt.Fprintf(&res, "%s\n\t%s:%d\n", name, file, line)
		}
	}
	return res.String()
}

// FormatMultiError formats multierrors for logging
func FormatMultiError(merrs []error) string {
	var msg = ""
	for i := 0; i < len(merrs); i++ {
		msg += fmt.Sprintf("%+v\n", merrs[i])
	}
	return msg
}

// KeyColumns is a shortcut for defining a KeyingOperation which uses multiple source
// column values to produce a compound key.
func KeyColumns(colNames ...string) sif.KeyingOperation {
	return func(row sif.Row) ([]byte, error) {
		irow := row.(itypes.AccessibleRow) // access row internals
		hasher := xxhash.New()
		for _, cname := range colNames {
			data, err := irow.GetColData(cname)
			if err != nil {
				return nil, err
			}
			hasher.Write(data)
		}
		return hasher.Sum(nil), nil
	}
}
