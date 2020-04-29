package cluster

import (
	"context"

	pb "github.com/go-sif/sif/internal/rpc"
	"github.com/go-sif/sif/internal/stats"
)

type statsSourceServer struct {
	statsTracker *stats.RunStatistics
}

// createStatsSource creates a new stats Source server
func createStatsSource(statsTracker *stats.RunStatistics) *statsSourceServer {
	return &statsSourceServer{statsTracker: statsTracker}
}

func (s *statsSourceServer) ProvideStatistics(context.Context, *pb.MStatisticsRequest) (*pb.MStatisticsResponse, error) {
	return s.statsTracker.ToMessage(), nil
}
