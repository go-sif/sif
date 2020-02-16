package rpc

//go:generate protoc --proto_path=../rpc_proto --go_out=plugins=grpc:./ s_cluster.proto

//go:generate protoc --proto_path=../rpc_proto --go_out=plugins=grpc:./ s_execution.proto

//go:generate protoc --proto_path=../rpc_proto --go_out=plugins=grpc:./ s_partition.proto

//go:generate protoc --proto_path=../rpc_proto --go_out=plugins=grpc:./ s_log.proto

//go:generate protoc --proto_path=../rpc_proto --go_out=plugins=grpc:./ s_lifecycle.proto
