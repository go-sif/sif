package core

import (
	"context"
	"fmt"
)

// NodeRole describes the intended role of a Node
type NodeRole string

const (
	// Coordinator indicates that a node should coordinate work
	//   e.g. CreateNode(Coordinator, &CoordinatorOptions{...})
	Coordinator NodeRole = "coordinator"
	// Worker indicates that a node should coordinate work
	//   e.g. CreateNode(Worker, &WorkerOptions{...})
	Worker NodeRole = "worker"
)

// Node is a member of a Sif cluster, either coordinating or performing work.
// Nodes present several methods to control their lifecycle.
type Node interface {
	Start(*DataFrame) error
	GracefulStop() error
	Stop() error
	Run(ctx context.Context) (map[string]*Partition, error)
}

// nodeOptions are options for a Node.
type nodeOptions interface {
	connectionString() string
}

// CreateNode creates a Sif node in a specific role (Coordinator or Worker)
func CreateNode(role NodeRole, opts nodeOptions) (Node, error) {
	switch role {
	case Coordinator:
		return createCoordinator(opts)
	case Worker:
		return createWorker(opts)
	default:
		return nil, fmt.Errorf("%s is an unknown NodeRole", role)
	}
}
