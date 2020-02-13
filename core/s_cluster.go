package core

//go:generate protoc --proto_path=./rpc_proto --go_out=plugins=grpc:./internal/rpc s_cluster.proto

import (
	"fmt"
	"log"
	"sync"
	"time"

	pb "github.com/go-sif/sif/core/internal/rpc"
	"golang.org/x/net/context"
)

type clusterServer struct {
	workers sync.Map
}

// RegisterWorker registers new workers with the cluster
func (s *clusterServer) RegisterWorker(ctx context.Context, req *pb.MRegisterRequest) (*pb.MRegisterResponse, error) {
	if _, exists := s.workers.Load(req.Id); exists {
		return nil, fmt.Errorf("Worker %s is already registered", req.Id)
	}
	s.workers.Store(req.Id, pb.MWorkerDescriptor{
		Id:   req.Id,
		Host: req.Host,
		Port: int32(req.Port),
	})
	for _, w := range s.Workers() {
		conn, err := dialWorker(w)
		if err != nil {
			log.Fatalf("Unable to connect to worker %s", w.Id)
		}
		defer conn.Close()
	}
	return &pb.MRegisterResponse{Time: time.Now().Unix()}, nil
}

// NumberOfWorkers returns the current worker count
func (s *clusterServer) NumberOfWorkers() int {
	i := 0
	s.workers.Range(func(_, _ interface{}) bool {
		i++
		return true
	})
	return i
}

// workers retrieves a slice of connected workers
func (s *clusterServer) Workers() []*pb.MWorkerDescriptor {
	result := make([]*pb.MWorkerDescriptor, 0)
	s.workers.Range(func(_, v interface{}) bool {
		w := v.(pb.MWorkerDescriptor)
		result = append(result, &w)
		return true
	})
	return result
}

func (s *clusterServer) waitForWorkers(ctx context.Context, numWorkers int) error {
	for {
		if s.NumberOfWorkers() == numWorkers {
			break
		}
		select {
		case <-ctx.Done():
			// Did we time out?
			return ctx.Err()
		case <-time.After(time.Second):
			// Wait 1 second and check again (iterate)
		}
	}
	return nil
}
