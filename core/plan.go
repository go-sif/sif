package core

// plan is an optimized execution plan for a DataFrame
type plan struct {
	stages []*stage
	parser DataSourceParser
	source DataSource
}

// size returns the number of stages in this Plan
func (p *plan) size() int {
	return len(p.stages)
}

// execute creates a planExecutor for this Plan
func (p *plan) execute(conf *PlanExecutorConfig) *planExecutor {
	return createplanExecutor(p, conf)
}
