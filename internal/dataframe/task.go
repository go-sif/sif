package dataframe

import (
	"github.com/go-sif/sif"
)

// noOpTask is a task that does nothing
type noOpTask struct{}

// RunInitialize for noOpTask does nothing
func (s *noOpTask) RunInitialize(sctx sif.StageContext) error {
	return nil
}

// RunWorker for noOpTask does nothing
func (s *noOpTask) RunWorker(sctx sif.StageContext, previous sif.OperablePartition) ([]sif.OperablePartition, error) {
	return []sif.OperablePartition{previous}, nil
}
