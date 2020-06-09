package cluster

import (
	"context"
	"time"

	pb "github.com/go-sif/sif/internal/rpc"
)

type lifecycleServer struct {
	node Node
}

// createLifecycleServer creates a new lifecycleServer
func createLifecycleServer(node Node) *lifecycleServer {
	return &lifecycleServer{node: node}
}

func (s *lifecycleServer) GracefulStop(ctx context.Context, req *pb.MWorkerDescriptor) (*pb.MStopResponse, error) {
	// we can't wait for the error to respond, because this counts as an open RPC, which blocks GracefulStop
	defer s.node.GracefulStop()
	return &pb.MStopResponse{Time: time.Now().Unix()}, nil
}

func (s *lifecycleServer) Stop(ctx context.Context, req *pb.MWorkerDescriptor) (*pb.MStopResponse, error) {
	err := s.node.Stop()
	if err != nil {
		return nil, err
	}
	return &pb.MStopResponse{Time: time.Now().Unix()}, nil
}
