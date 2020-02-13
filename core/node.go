package core

import (
	"context"
	"fmt"
	"os"
	"time"
)

// NodeRole describes the intended role of a Node
type NodeRole = string

const (
	// Coordinator indicates that a node should coordinate work
	//   e.g. CreateNode(Coordinator, &NodeOptions{...})
	Coordinator NodeRole = "coordinator"
	// Worker indicates that a node should coordinate work
	//   e.g. CreateNode(Worker, &NodeOptions{...})
	Worker NodeRole = "worker"
)

// Node is a member of a Sif cluster, either coordinating or performing work.
// Nodes present several methods to control their lifecycle.
type Node interface {
	Start(DataFrame) error
	GracefulStop() error
	Stop() error
	Run(ctx context.Context) (map[string]CollectedPTition, error)
}

// NodeOptions are options for a Node, configuring elements of a Sif cluster
type NodeOptions struct {
	Port                  int           // Port for this Node
	Host                  string        // Hostname for this Node
	CoordinatorPort       int           // Port for the Coordinator Node (potentially identical to Port if this is the Coordinator)
	CoordinatorHost       string        // Hostname of the Coordinator Node (potentially identical to Host if this is the Coordinator)
	NumWorkers            int           // the number of workers to wait for before running the job
	WorkerJoinTimeout     time.Duration // how long to wait for workers
	RPCTimeout            time.Duration // timeout for all RPC calls
	TempDir               string        // location for storing temporary files (primarily persisted partitions)
	NumInMemoryPartitions int           // the number of partitions to retain in memory before swapping to disk
	IgnoreRowErrors       bool          // iff true, log row transformation errors instead of crashing immediately
}

func ensureDefaultNodeOptionsValues(opts *NodeOptions) {
	// default certain options if not supplied
	if opts.NumInMemoryPartitions == 0 {
		opts.NumInMemoryPartitions = 100 // TODO should this just be a memory limit, and we compute NumInMemoryPartitions ourselves?
	}
	if opts.RPCTimeout == 0 {
		opts.RPCTimeout = time.Duration(5) * time.Second // TODO sensible default?
	}
}

// connectionString returns the connection string for this node
func (o *NodeOptions) connectionString() string {
	return fmt.Sprintf("%s:%d", o.Host, o.Port)
}

// coordinatorConnectionString returns the connection string for the coordinator
func (o *NodeOptions) coordinatorConnectionString() string {
	return fmt.Sprintf("%s:%d", o.CoordinatorHost, o.CoordinatorPort)
}

// CreateNodeInRole creates a Sif node in a specific role (Coordinator or Worker)
func CreateNodeInRole(role NodeRole, opts *NodeOptions) (Node, error) {
	switch role {
	case Coordinator:
		return createCoordinator(opts)
	case Worker:
		return createWorker(opts)
	default:
		return nil, fmt.Errorf("%s is an unknown NodeRole", role)
	}
}

// CreateNode creates a Sif node, deriving role from environment variables
func CreateNode(opts *NodeOptions) (Node, error) {
	if len(os.Getenv("SIF_NODE_TYPE")) == 0 {
		return nil, fmt.Errorf("$SIF_NODE_TYPE is not set - must be \"%s\" or \"%s\"", Coordinator, Worker)
	}
	switch os.Getenv("SIF_NODE_TYPE") {
	case Coordinator:
		return createCoordinator(opts)
	case Worker:
		return createWorker(opts)
	default:
		return nil, fmt.Errorf("$SIF_NODE_TYPE=\"%s\" is an unknown NodeRole", os.Getenv("SIF_NODE_TYPE"))
	}
}
