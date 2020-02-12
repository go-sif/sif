package core

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	pb "github.com/go-sif/sif/v0.0.1/core/rpc"
	"google.golang.org/grpc"
)

// dialWorker connects to a Worker
func dialWorker(w *pb.MWorkerDescriptor) (*grpc.ClientConn, error) {
	// start client
	conn, err := grpc.Dial(
		fmt.Sprintf("%s:%d", w.Host, w.Port),
		// grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(128*1024*1024))
		grpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("fail to dial: %v", err)
	}
	return conn, nil
}

// dialWorkers returns collections to multiple workers
func dialWorkers(ws []*pb.MWorkerDescriptor) ([]*grpc.ClientConn, error) {
	conns := make([]*grpc.ClientConn, len(ws))
	for i, w := range ws {
		conn, err := dialWorker(w)
		if err != nil {
			// TODO close already-opened connections
			return nil, err
		}
		conns[i] = conn
	}
	return conns, nil
}

// closeGRPCConnections closes multiple grpc clients
func closeGRPCConnections(conns []*grpc.ClientConn) {
	for _, conn := range conns {
		conn.Close()
	}
}

// createAsyncErrorChannel produces a channel for errors
func createAsyncErrorChannel() chan error {
	return make(chan error)
}

// waitAndFetchError attempts to fetch an error from an asyc goroutine
func waitAndFetchError(wg *sync.WaitGroup, errors chan error) error {
	// use reading from the errors channel to block, rather than
	// the WaitGroup directly.
	go func() {
		defer close(errors)
		wg.Wait()
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

func getTrace() string {
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

// formatMultiError formats multierrors for logging
func formatMultiError(merrs []error) string {
	var msg = ""
	for i := 0; i < len(merrs); i++ {
		msg += fmt.Sprintf("%+v\n", merrs[i])
	}
	return msg
}
