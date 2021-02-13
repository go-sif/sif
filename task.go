package sif

// A Task is an action or transformation applied
// to Partitions of columnar data.
type Task interface {
	RunInitialize(sctx StageContext) error
	RunWorker(sctx StageContext, previous OperablePartition) ([]OperablePartition, error)
}
