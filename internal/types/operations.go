package types

import "github.com/go-sif/sif"

// PartitionTransform transforms an OperablePartition into 0 or more OperablePartitions
type PartitionTransform func(sif.StageContext, sif.OperablePartition) ([]sif.OperablePartition, error)
