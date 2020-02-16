package core

import "github.com/go-sif/sif/types"

// plan is an optimized execution plan for a DataFrame
type plan struct {
	stages []*stage
	parser types.DataSourceParser
	source types.DataSource
}

// size returns the number of stages in this Plan
func (p *plan) size() int {
	return len(p.stages)
}

// execute creates a planExecutor for this Plan
func (p *plan) execute(conf *planExecutorConfig) *planExecutor {
	return createplanExecutor(p, conf)
}
