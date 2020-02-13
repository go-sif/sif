package core

import (
	"context"
	"time"

	pb "github.com/go-sif/sif/core/internal/rpc"
)

//go:generate protoc --proto_path=./rpc_proto --go_out=plugins=grpc:./internal/rpc s_lifecycle.proto

type lifecycleServer struct {
	node Node
}

func (s *lifecycleServer) GracefulStop(ctx context.Context, req *pb.MWorkerDescriptor) (*pb.MStopResponse, error) {
	err := s.node.GracefulStop()
	if err != nil {
		return nil, err
	}
	return &pb.MStopResponse{Time: time.Now().Unix()}, nil
}

func (s *lifecycleServer) Stop(ctx context.Context, req *pb.MWorkerDescriptor) (*pb.MStopResponse, error) {
	err := s.node.Stop()
	if err != nil {
		return nil, err
	}
	return &pb.MStopResponse{Time: time.Now().Unix()}, nil
}
