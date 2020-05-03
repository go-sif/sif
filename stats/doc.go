// Package stats exposes the grpc API for accessing runtime statistics from a Sif coordinator
package stats

//go:generate protoc --proto_path=./rpc_proto --go_out=plugins=grpc:./ ./rpc_proto/s_stats.proto
