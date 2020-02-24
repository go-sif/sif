package cluster

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/go-sif/sif"
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
	IsCoordinator() bool
	Start(sif.DataFrame) error
	GracefulStop() error
	Stop() error
	Run(ctx context.Context) (map[string]sif.CollectedPartition, error)
}

// NodeOptions are options for a Node, configuring elements of a Sif cluster
type NodeOptions struct {
	Port                  int           // port for this Node to bind to
	Host                  string        // hostname for this Node to bind to
	CoordinatorPort       int           // port for the Coordinator Node (potentially identical to Port if this is the Coordinator)
	CoordinatorHost       string        // [REQUIRED] hostname of the Coordinator Node (potentially identical to Host if this is the Coordinator)
	NumWorkers            int           // [REQUIRED] the number of Workers to wait for before running the job
	WorkerJoinTimeout     time.Duration // how long the Coordinator should wait for Workers to join
	WorkerJoinRetries     int           // how many times a Worker should retry connecting to the Coordinator (at one second intervals)
	RPCTimeout            time.Duration // timeout for all RPC calls
	TempDir               string        // location for storing temporary files (primarily persisted partitions)
	NumInMemoryPartitions int           // the number of partitions to retain in memory before swapping to disk
	IgnoreRowErrors       bool          // iff true, log row transformation errors instead of crashing immediately
}

// CloneNodeOptions makes a copy of a NodeOptions
func CloneNodeOptions(opts *NodeOptions) *NodeOptions {
	return &NodeOptions{
		Port:                  opts.Port,
		Host:                  opts.Host,
		CoordinatorPort:       opts.CoordinatorPort,
		CoordinatorHost:       opts.CoordinatorHost,
		NumWorkers:            opts.NumWorkers,
		WorkerJoinTimeout:     opts.WorkerJoinTimeout,
		WorkerJoinRetries:     opts.WorkerJoinRetries,
		RPCTimeout:            opts.RPCTimeout,
		TempDir:               opts.TempDir,
		NumInMemoryPartitions: opts.NumInMemoryPartitions,
		IgnoreRowErrors:       opts.IgnoreRowErrors,
	}
}

func ensureDefaultNodeOptionsValues(opts *NodeOptions) {
	// crash if certain required options are not supplied
	if opts.NumWorkers == 0 {
		log.Fatal("NodeOptions.NumWorkers must be greater than 0")
	}
	if len(opts.CoordinatorHost) == 0 {
		log.Fatal("NodeOptions.CoordinatorHost must be the IP address of the Sif Coordinator")
	}
	// default certain options if not supplied
	if opts.Port == 0 {
		opts.Port = 1643
	}
	if len(opts.Host) == 0 {
		opts.Host = "0.0.0.0"
	}
	if opts.CoordinatorPort == 0 {
		opts.CoordinatorPort = 1643
	}
	if opts.RPCTimeout == 0 {
		opts.RPCTimeout = time.Duration(5) * time.Second // TODO sensible default?
	}
	if opts.WorkerJoinTimeout == 0 {
		opts.WorkerJoinTimeout = time.Duration(5) * time.Second // TODO sensible default?
	}
	if opts.WorkerJoinRetries == 0 {
		opts.WorkerJoinRetries = 5 // TODO sensible default?
	}
	if len(opts.TempDir) == 0 {
		opts.TempDir = os.TempDir()
	}
	if opts.NumInMemoryPartitions == 0 {
		opts.NumInMemoryPartitions = 100 // TODO should this just be a memory limit, and we compute NumInMemoryPartitions ourselves?
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
