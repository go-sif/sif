package types

import (
	"context"

	"github.com/go-sif/sif"
	"github.com/go-sif/sif/internal/stats"
)

// A Plan is an execution plan for a DataFrame
type Plan interface {
	Size() int                                                                                             // returns the number of stages
	GetStage(idx int) Stage                                                                                // GetStage returns a particular Stage in this Plan
	Parser() sif.DataSourceParser                                                                          // Parser returns this Plan's DataSourceParser
	Source() sif.DataSource                                                                                // Source returns this Plan's DataSource
	Execute(ctx context.Context, conf *PlanExecutorConfig, statsTracker *stats.RunStatistics) PlanExecutor // Creates a PlanExecutor given a particular configuration
}
