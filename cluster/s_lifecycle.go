package cluster

import (
	"context"
	"log"
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
	log.Println("Received request to stop gracefully...")
	// we can't wait for the error to respond, because this counts as an open RPC, which blocks GracefulStop
	go s.node.GracefulStop()
	return &pb.MStopResponse{Time: time.Now().Unix()}, nil
}

func (s *lifecycleServer) Stop(ctx context.Context, req *pb.MWorkerDescriptor) (*pb.MStopResponse, error) {
	log.Println("Received request to stop...")
	go s.node.Stop()
	return &pb.MStopResponse{Time: time.Now().Unix()}, nil
}
