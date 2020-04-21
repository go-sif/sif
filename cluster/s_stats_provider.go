package cluster

import (
	"context"

	pb "github.com/go-sif/sif/internal/rpc"
	itypes "github.com/go-sif/sif/internal/types"
)

type statsSourceServer struct {
	statsTracker *itypes.RunStatistics
}

// createStatsSource creates a new stats Source server
func createStatsSource(statsTracker *itypes.RunStatistics) *statsSourceServer {
	return &statsSourceServer{statsTracker: statsTracker}
}

func (s *statsSourceServer) ProvideStatistics(context.Context, *pb.MStatisticsRequest) (*pb.MStatisticsResponse, error) {
	return s.statsTracker.ToMessage(), nil
}
