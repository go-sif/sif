# Sif

[![GoDoc](https://godoc.org/github.com/go-sif/sif?status.svg)](https://pkg.go.dev/github.com/go-sif/sif/v8) [![Go Report Card](https://goreportcard.com/badge/github.com/go-sif/sif)](https://goreportcard.com/report/github.com/go-sif/sif) ![Tests](https://github.com/go-sif/sif/workflows/Tests/badge.svg) [![codecov.io](https://codecov.io/github/go-sif/sif/coverage.svg?branch=master)](https://codecov.io/gh/go-sif/sif?branch=master)

![Logo](https://raw.githubusercontent.com/go-sif/sif/master/media/logo-128.png)

`Sif` is a framework for fast, predictable, general-purpose distributed computing in the map/reduce paradigm.

`Sif` is new, and currently under heavy development. It should be considered **alpha software** prior to a 1.0.0 release, with the API and behaviours subject to change.

## Table of Contents

- [Why Sif?](#why-sif)
- [Installation](#installation)
- [Getting Started](#getting-started)
	- [Schemas](#schemas)
	- [DataSources and Parsers](#datasources-and-parsers)
	- [DataFrames](#dataframes)
	- [Execution (Bringing it all Together)](#execution-bringing-it-all-together)
- [Advanced Usage](#advanced-usage)
	- [Operations](#operations)
		- [Reduction](#reduction)
		- [Collection](#collection)
- [Extending Sif](#extending-sif)
	- [Custom ColumnTypes](#custom-columntypes)
	- [Custom DataSources](#custom-datasources)
	- [Custom Parsers](#custom-parsers)
- [License](#license)


## Why Sif?

`Sif` is offered primarily as a simpler alternative to [Apache Spark](https://spark.apache.org/), with the following goals in mind:

**Predictability:** An emphasis on fixed-width data and in-place manipulation makes it easier to reason over the compute and memory requirements of a particular job. `Sif`'s API is designed around a "no magic" philosophy, and attempts to make obvious the runtime consequences of any particular line of code.

**Scalability:** While scaling up to massive datasets is the primary focus of any distributed computing framework, `Sif` is also designed to scale down to small datasets as well with minimal startup time and other cluster-oriented overhead.

**Ease of Integration:** Rather than deploying `Sif` as a complex, persistent cluster, `Sif` is a *library* designed to ease the integration of cluster computing functionality within a larger application or architectural context. Write your own REST API which manages the initiation of a `Sif` pipeline!

**Ease of Development:** `Sif` applications are traceable and debuggable, and the core `Sif` codebase is designed to be *as small as possible*.

**API Minimalism:** A single representation of distributed data, with a single set of tools for manipulating it.

**Architectural Minimalism:** Throw away YARN and Mesos. Compile your `Sif` pipeline, package it as a [Docker](https://www.docker.com/) image, then deploy it to distributed container infrastructure such as [Docker Swarm](https://docs.docker.com/engine/swarm/) or [Kubernetes](https://kubernetes.io/).

## Installation

**Note:** `Sif` is developed and tested against Go 1.15, leveraging module support:

```go
module example.com/sif_test

require(
	github.com/go-sif/sif v0.1.0
)

go 1.15
```

## Getting Started

`Sif` facilitates the definition and execution of a distributed compute pipeline through the use of a few basic components. For the sake of this example, we will assume that we have JSON Lines-type data with the following format:

```jsonl
{"id": 1234, "meta": {"uuid": "27366d2d-502c-4c03-84c3-55dc5ecedd3f", "name": "John Smith"}}
{"id": 5678, "meta": {"uuid": "f21dec0a-37f3-4b0d-9d92-d26b11c62ed8", "name": "Jane Doe"}}
...
```

### Schemas

A `Schema` outlines the structure of the data which will be manipulated. In `Sif`, data is represented as a sequence of `Row`s, each of which consist of `Column`s with particular `ColumnType`s.

```go
package main

import (
	"github.com/go-sif/sif"
	"github.com/go-sif/sif/schema"
)

func main() {
	schema := schema.CreateSchema()
	// Schemas should employ fixed-length ColumnTypes whenever possible
	schema.CreateColumn("id", &sif.Int32ColumnType{})
	schema.CreateColumn("meta.uuid", &sif.StringColumnType{Length: 36})
	// or variable-length ColumnTypes, if the size of a field is not known
	schema.CreateColumn("meta.name", &sif.VarStringColumnType{})
}
```

### DataSources and Parsers

A `DataSource` represents a source of raw data, which will be partitioned and parsed into `Row`s via a `Parser` in parallel across workers in the cluster. `Sif` contains several example `DataSource`s, primarily for testing purposes, but it is intended that `DataSource`s for common sources such as databases, Kafka, etc. will be provided as separate packages.

Ultimately, a `DataSource` provides a `DataFrame` which can be manipulated by `Sif` operations.

```go
package main

import (
	"path"

	"github.com/go-sif/sif"
	"github.com/go-sif/sif/schema"
	"github.com/go-sif/sif/datasource/file"
	"github.com/go-sif/sif/datasource/parser/jsonl"
)

func main() {
	schema := schema.CreateSchema()
	schema.CreateColumn("id", &sif.Int32ColumnType{})
	// In this case, since our Schema featured column names with dots,
	// the jsonl parser is smart enough to search within each JSON object
	// for a nested field matching these paths.
	schema.CreateColumn("meta.uuid", &sif.StringColumnType{Length: 36})
	schema.CreateColumn("meta.name", &sif.VarStringColumnType{})

	parser := jsonl.CreateParser(&jsonl.ParserConf{
		PartitionSize: 128,
	})
	frame := file.CreateDataFrame("path/to/*.jsonl", parser, schema)
}
```

### DataFrames

A `DataFrame` facilitates the definition of an execution plan. Multiple `Operation`s are chained together, and then passed to `Sif` for evaluation:

```go
package main

import (
	"path"
	"log"
	"strings"

	"github.com/go-sif/sif"
	"github.com/go-sif/sif/schema"
	"github.com/go-sif/sif/datasource/file"
	"github.com/go-sif/sif/datasource/parser/jsonl"
	ops "github.com/go-sif/sif/operations/transform"
)

func main() {
	schema := schema.CreateSchema()
	schema.CreateColumn("id", &sif.Int32ColumnType{})
	schema.CreateColumn("meta.uuid", &sif.StringColumnType{Length: 36})
	schema.CreateColumn("meta.name", &sif.VarStringColumnType{})

	parser := jsonl.CreateParser(&jsonl.ParserConf{
		PartitionSize: 128,
	})
	frame := file.CreateDataFrame("path/to/*.jsonl", parser, schema)

	frame, err := frame.To(
		ops.AddColumn("lowercase_name", &sif.VarStringColumnType{}),
		ops.Map(func(row sif.Row) error {
			if row.IsNil("meta.name") {
				return nil
			}
			name, err := row.GetVarString("meta.name")
			if err != nil {
				return err
			}
			return row.SetVarString("lowercase_name", strings.ToLower(name))
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
}
```

### Execution (Bringing it all Together)

Execution of a `DataFrame` involves starting and passing it to a `Sif` cluster. `Sif` clusters, at the moment, consist of a single `Coordinator` and multiple `Worker`s. Each is an identical binary, with the difference in role determined by the `SIF_NODE_TYPE` environment variable (set to `"coordinator"` or `"worker"`). This makes it easy to compile a single executable which can then be deployed and scaled up or down as one sees fit.

```go
package main

import (
	"path"
	"log"
	"strings"
	"context"

	"github.com/go-sif/sif"
	"github.com/go-sif/sif/schema"
	"github.com/go-sif/sif/datasource/file"
	"github.com/go-sif/sif/datasource/parser/jsonl"
	ops "github.com/go-sif/sif/operations/transform"
	"github.com/go-sif/sif/cluster"
)

func main() {
	schema := schema.CreateSchema()
	schema.CreateColumn("id", &sif.Int32ColumnType{})
	schema.CreateColumn("meta.uuid", &sif.StringColumnType{Length: 36})
	schema.CreateColumn("meta.name", &sif.VarStringColumnType{})

	parser := jsonl.CreateParser(&jsonl.ParserConf{
		PartitionSize: 128,
	})
	frame := file.CreateDataFrame("path/to/*.jsonl", parser, schema)

	frame, err := frame.To(
		ops.AddColumn("lowercase_name", &sif.VarStringColumnType{}),
		ops.Map(func(row sif.Row) error {
			if row.IsNil("meta.name") {
				return nil
			}
			name, err := row.GetVarString("meta.name")
			if err != nil {
				return err
			}
			return row.SetVarString("lowercase_name", strings.ToLower(name))
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Define a node
	// Sif will read the SIF_NODE_TYPE environment variable to
	// determine whether this copy of the binary
	// is a "coordinator" or "worker".
	opts := &cluster.NodeOptions{
		NumWorkers: 2,
		CoordinatorHost: "insert.coordinator.hostname",
	}
	node, err := cluster.CreateNode(opts)
	if err != nil {
		log.Fatal(err)
	}
	// start this node in the background and run the DataFrame
	defer node.GracefulStop()
	go func() {
		err := node.Start(frame)
		if err != nil {
			log.Fatal(err)
		}
	}()
	// result will be nil in this case, as only certain
	// operations produce a result.
	result, err := node.Run(context.Background())
	if err != nil {
		log.Fatal(err)
	}
}
```

## Advanced Usage

### Operations

`Sif` includes multiple operations suitable for manipulating `DataFrame`s, which can be found under the `github.com/go-sif/sif/operations/transform` package.

Additional utility operations are included in the `github.com/go-sif/sif/operations/util` package, which at this time only includes `Collect()`, which allows for the collection of results to the `Coordinator` for further, local processing.

A couple of complex `Operation`s are covered in additional detail here:

#### Reduction

Reduction in `Sif` is a two step process:

1. A `KeyingOperation` labels `Row`s, with the intention that two `Row`s with the same key are reduced together
1. A `ReductionOperation` defines the mechanism by which two `Row`s are combined (the "right" `Row` into the "left" `Row`)

For example, if we wanted to bucket the JSON Lines data from [Getting Started](#getting-started) by name, and then produce counts for names beginning with the same letter:

```go
// ...
frame, err := frame.To(
	// Add a column to store the total for each first-letter bucket
	ops.AddColumn("total", &sif.UInt32ColumnType{}),
	ops.Reduce(func(row sif.Row) ([]byte, error) {
		// Our KeyingOperation comes first, using the first letter as the key
		name, err := row.GetVarString("meta.name")
		if err != nil {
			return nil, err
		}
		if len(name) == 0 {
			return []byte{0}, nil
		}
		return []byte{name[0]}, nil
	}, func(lrow sif.Row, rrow sif.Row) error {
		// Our ReductionOperation comes second
		// Since our keys ensure two Rows are only reduced together if they
		// have a matching key, we can just add the totals together.
		lval, err := lrow.GetUInt32("total")
		if err != nil {
			return err
		}
		rval, err := rrow.GetUInt32("total")
		if err != nil {
			return err
		}
		return lrow.SetInt32("total", lval+rval)
	}),
)
// ...
```

**Tip:** `ops.KeyColumns(colNames ...string)` can be used with `ops.Reduce` to quickly produce a key (or compound key) from a set of column values.

#### Accumulation

Sif `Accumulator`s are an alternative mechanism for reduction, which offers full customization of reduciton technique, in exchange for accumulation ending a `sif` pipeline. In exchange for losing the ability to further transform and reduce data, `Accumulator`s offer the potential for significant performance benefits.

Sif offers built-in `Accumulators` in the `accumulators` package.

For example, we can use `accumulators.Counter` to efficiently count records:

```go
// ...
frame, err := frame.To(
	util.Accumulate(accumulators.Counter)
)
// ...
// In this case, node.Run returns an Accumulator, which can be
// manipulated on the Coordinator node.
result, err := node.Run(context.Background())
if node.IsCoordinator() {
	res, _ = result.Accumulator.(accumulators.Count)
	// Do something with res.GetCount()
}
```

#### Collection

Collection is the process of pulling results from distributed data back to the `Coordinator` for local processing. This is not generally encouraged - rather, it is best if `Worker`s write their results directly to an output destination. But, it is occasionally useful, such as in the writing of tests:

```go
// ...
frame, err := frame.To(
	ops.AddColumn("lowercase_name", &sif.VarStringColumnType{}),
	ops.Map(func(row sif.Row) error {
		if row.IsNil("meta.name") {
			return nil
		}
		name, err := row.GetVarString("meta.name")
		if err != nil {
			return err
		}
		return row.SetVarString("lowercase_name", strings.ToLower(name))
	}),
	// To discourage use and unpredictability, you must specify exactly
	// how many Partitions of data you wish to Collect:
	util.Collect(1)
)
// ...
// In this case, node.Run returns a CollectedPartition, which can be
// manipulated on the Coordinator node.
result, err := node.Run(context.Background())
if node.IsCoordinator() {
	err = result.Collected.ForEachRow(func(row sif.Row) error {
		// Do something with results
	})
}
// ...
```

## Extending Sif

### Custom ColumnTypes

See [Implementing Custom ColumnTypes](docs/custom-columntypes.md) for details.

### Custom DataSources

See [Implementing Custom DataSources](docs/custom-datasources.md) for details.

### Custom Parsers

See [Implementing Custom Parsers](docs/custom-parsers.md) for details.

## License

`Sif` is licensed under the Apache 2.0 License, found in the LICENSE file.
