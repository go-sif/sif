package cluster

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	pb "github.com/go-sif/sif/internal/rpc"
	"golang.org/x/net/context"
	"google.golang.org/grpc/peer"
)

type clusterServer struct {
	workers    sync.Map
	numWorkers int
}

// createClusterServer creates a new cluster server
func createClusterServer() *clusterServer {
	return &clusterServer{workers: sync.Map{}}
}

// RegisterWorker registers new workers with the cluster
func (s *clusterServer) RegisterWorker(ctx context.Context, req *pb.MRegisterRequest) (*pb.MRegisterResponse, error) {
	if _, exists := s.workers.Load(req.Id); exists {
		return nil, fmt.Errorf("Worker %s is already registered", req.Id)
	}
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("Unable to fetch peer data for connecting worker %s", req.Id)
	}
	tcpAddr, ok := peer.Addr.(*net.TCPAddr)
	if !ok {
		return nil, fmt.Errorf("Connecting worker %s is not using TCP", req.Id)
	}
	wDescriptor := pb.MWorkerDescriptor{
		Id:   req.Id,
		Host: tcpAddr.IP.String(),
		Port: int32(req.Port),
	}
	s.workers.Store(req.Id, wDescriptor)
	s.numWorkers++

	// test connection
	conn, err := dialWorker(&wDescriptor)
	if err != nil {
		log.Fatalf("Unable to connect to worker %s", wDescriptor.Id)
	}
	defer conn.Close()
	log.Printf("Registered worker %s at %s:%d", wDescriptor.Id, wDescriptor.Host, wDescriptor.Port)
	return &pb.MRegisterResponse{Time: time.Now().Unix()}, nil
}

// NumberOfWorkers returns the current worker count
func (s *clusterServer) NumberOfWorkers() int {
	return s.numWorkers
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
