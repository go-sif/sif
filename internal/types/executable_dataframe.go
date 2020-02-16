package types

import (
	"github.com/go-sif/sif"
)

// An ExecutableDataFrame is a DataFrame that can be executed by a Sif cluster
type ExecutableDataFrame interface {
	GetParent() sif.DataFrame
	Optimize() Plan                           // Optimize splits the DataFrame chain into stages which each share a schema. Each stage's execution will be blocked until the completion of the previous stage
	AnalyzeSource() (sif.PartitionMap, error) // AnalyzeSource returns a PartitionMap for the source data for this DataFrame
}
