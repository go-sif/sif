package core

import (
	"fmt"

	pb "github.com/go-sif/sif/internal/rpc"
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
