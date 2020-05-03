package cluster

import (
	"context"

	istats "github.com/go-sif/sif/internal/stats"
	"github.com/go-sif/sif/stats"
)

type statsSourceServer struct {
	statsTracker *istats.RunStatistics
}

// createStatsSource creates a new stats Source server
func createStatsSource(statsTracker *istats.RunStatistics) *statsSourceServer {
	return &statsSourceServer{statsTracker: statsTracker}
}

func (s *statsSourceServer) ProvideStatistics(context.Context, *stats.MStatisticsRequest) (*stats.MStatisticsResponse, error) {
	return s.statsTracker.ToMessage(), nil
}
