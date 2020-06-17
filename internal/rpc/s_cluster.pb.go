// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.24.0
// 	protoc        v3.12.3
// source: s_cluster.proto

package rpc

import (
	context "context"
	proto "github.com/golang/protobuf/proto"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// This is a compile-time assertion that a sufficiently up-to-date version
// of the legacy proto package is being used.
const _ = proto.ProtoPackageIsVersion4

type MRegisterRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id   string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Port int32  `protobuf:"varint,2,opt,name=port,proto3" json:"port,omitempty"`
}

func (x *MRegisterRequest) Reset() {
	*x = MRegisterRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_s_cluster_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MRegisterRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MRegisterRequest) ProtoMessage() {}

func (x *MRegisterRequest) ProtoReflect() protoreflect.Message {
	mi := &file_s_cluster_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MRegisterRequest.ProtoReflect.Descriptor instead.
func (*MRegisterRequest) Descriptor() ([]byte, []int) {
	return file_s_cluster_proto_rawDescGZIP(), []int{0}
}

func (x *MRegisterRequest) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *MRegisterRequest) GetPort() int32 {
	if x != nil {
		return x.Port
	}
	return 0
}

type MRegisterResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Time int64 `protobuf:"varint,1,opt,name=time,proto3" json:"time,omitempty"`
}

func (x *MRegisterResponse) Reset() {
	*x = MRegisterResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_s_cluster_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MRegisterResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MRegisterResponse) ProtoMessage() {}

func (x *MRegisterResponse) ProtoReflect() protoreflect.Message {
	mi := &file_s_cluster_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MRegisterResponse.ProtoReflect.Descriptor instead.
func (*MRegisterResponse) Descriptor() ([]byte, []int) {
	return file_s_cluster_proto_rawDescGZIP(), []int{1}
}

func (x *MRegisterResponse) GetTime() int64 {
	if x != nil {
		return x.Time
	}
	return 0
}

// For the moment, this is identical to MRegisterRequest, but may diverge in the future.
type MWorkerDescriptor struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id   string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Host string `protobuf:"bytes,2,opt,name=host,proto3" json:"host,omitempty"`
	Port int32  `protobuf:"varint,3,opt,name=port,proto3" json:"port,omitempty"`
}

func (x *MWorkerDescriptor) Reset() {
	*x = MWorkerDescriptor{}
	if protoimpl.UnsafeEnabled {
		mi := &file_s_cluster_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MWorkerDescriptor) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MWorkerDescriptor) ProtoMessage() {}

func (x *MWorkerDescriptor) ProtoReflect() protoreflect.Message {
	mi := &file_s_cluster_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MWorkerDescriptor.ProtoReflect.Descriptor instead.
func (*MWorkerDescriptor) Descriptor() ([]byte, []int) {
	return file_s_cluster_proto_rawDescGZIP(), []int{2}
}

func (x *MWorkerDescriptor) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *MWorkerDescriptor) GetHost() string {
	if x != nil {
		return x.Host
	}
	return ""
}

func (x *MWorkerDescriptor) GetPort() int32 {
	if x != nil {
		return x.Port
	}
	return 0
}

var File_s_cluster_proto protoreflect.FileDescriptor

var file_s_cluster_proto_rawDesc = []byte{
	0x0a, 0x0f, 0x73, 0x5f, 0x63, 0x6c, 0x75, 0x73, 0x74, 0x65, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x12, 0x03, 0x72, 0x70, 0x63, 0x22, 0x36, 0x0a, 0x10, 0x4d, 0x52, 0x65, 0x67, 0x69, 0x73,
	0x74, 0x65, 0x72, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x70, 0x6f,
	0x72, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05, 0x52, 0x04, 0x70, 0x6f, 0x72, 0x74, 0x22, 0x27,
	0x0a, 0x11, 0x4d, 0x52, 0x65, 0x67, 0x69, 0x73, 0x74, 0x65, 0x72, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x74, 0x69, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x03, 0x52, 0x04, 0x74, 0x69, 0x6d, 0x65, 0x22, 0x4b, 0x0a, 0x11, 0x4d, 0x57, 0x6f, 0x72, 0x6b,
	0x65, 0x72, 0x44, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x6f, 0x72, 0x12, 0x0e, 0x0a, 0x02,
	0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x12, 0x12, 0x0a, 0x04,
	0x68, 0x6f, 0x73, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x68, 0x6f, 0x73, 0x74,
	0x12, 0x12, 0x0a, 0x04, 0x70, 0x6f, 0x72, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x05, 0x52, 0x04,
	0x70, 0x6f, 0x72, 0x74, 0x32, 0x53, 0x0a, 0x0e, 0x43, 0x6c, 0x75, 0x73, 0x74, 0x65, 0x72, 0x53,
	0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0x41, 0x0a, 0x0e, 0x52, 0x65, 0x67, 0x69, 0x73, 0x74,
	0x65, 0x72, 0x57, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x12, 0x15, 0x2e, 0x72, 0x70, 0x63, 0x2e, 0x4d,
	0x52, 0x65, 0x67, 0x69, 0x73, 0x74, 0x65, 0x72, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a,
	0x16, 0x2e, 0x72, 0x70, 0x63, 0x2e, 0x4d, 0x52, 0x65, 0x67, 0x69, 0x73, 0x74, 0x65, 0x72, 0x52,
	0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x42, 0x24, 0x5a, 0x22, 0x67, 0x69, 0x74,
	0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x67, 0x6f, 0x2d, 0x73, 0x69, 0x66, 0x2f, 0x73,
	0x69, 0x66, 0x2f, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2f, 0x72, 0x70, 0x63, 0x62,
	0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_s_cluster_proto_rawDescOnce sync.Once
	file_s_cluster_proto_rawDescData = file_s_cluster_proto_rawDesc
)

func file_s_cluster_proto_rawDescGZIP() []byte {
	file_s_cluster_proto_rawDescOnce.Do(func() {
		file_s_cluster_proto_rawDescData = protoimpl.X.CompressGZIP(file_s_cluster_proto_rawDescData)
	})
	return file_s_cluster_proto_rawDescData
}

var file_s_cluster_proto_msgTypes = make([]protoimpl.MessageInfo, 3)
var file_s_cluster_proto_goTypes = []interface{}{
	(*MRegisterRequest)(nil),  // 0: rpc.MRegisterRequest
	(*MRegisterResponse)(nil), // 1: rpc.MRegisterResponse
	(*MWorkerDescriptor)(nil), // 2: rpc.MWorkerDescriptor
}
var file_s_cluster_proto_depIdxs = []int32{
	0, // 0: rpc.ClusterService.RegisterWorker:input_type -> rpc.MRegisterRequest
	1, // 1: rpc.ClusterService.RegisterWorker:output_type -> rpc.MRegisterResponse
	1, // [1:2] is the sub-list for method output_type
	0, // [0:1] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_s_cluster_proto_init() }
func file_s_cluster_proto_init() {
	if File_s_cluster_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_s_cluster_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*MRegisterRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_s_cluster_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*MRegisterResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_s_cluster_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*MWorkerDescriptor); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_s_cluster_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   3,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_s_cluster_proto_goTypes,
		DependencyIndexes: file_s_cluster_proto_depIdxs,
		MessageInfos:      file_s_cluster_proto_msgTypes,
	}.Build()
	File_s_cluster_proto = out.File
	file_s_cluster_proto_rawDesc = nil
	file_s_cluster_proto_goTypes = nil
	file_s_cluster_proto_depIdxs = nil
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConnInterface

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion6

// ClusterServiceClient is the client API for ClusterService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type ClusterServiceClient interface {
	RegisterWorker(ctx context.Context, in *MRegisterRequest, opts ...grpc.CallOption) (*MRegisterResponse, error)
}

type clusterServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewClusterServiceClient(cc grpc.ClientConnInterface) ClusterServiceClient {
	return &clusterServiceClient{cc}
}

func (c *clusterServiceClient) RegisterWorker(ctx context.Context, in *MRegisterRequest, opts ...grpc.CallOption) (*MRegisterResponse, error) {
	out := new(MRegisterResponse)
	err := c.cc.Invoke(ctx, "/rpc.ClusterService/RegisterWorker", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ClusterServiceServer is the server API for ClusterService service.
type ClusterServiceServer interface {
	RegisterWorker(context.Context, *MRegisterRequest) (*MRegisterResponse, error)
}

// UnimplementedClusterServiceServer can be embedded to have forward compatible implementations.
type UnimplementedClusterServiceServer struct {
}

func (*UnimplementedClusterServiceServer) RegisterWorker(context.Context, *MRegisterRequest) (*MRegisterResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RegisterWorker not implemented")
}

func RegisterClusterServiceServer(s *grpc.Server, srv ClusterServiceServer) {
	s.RegisterService(&_ClusterService_serviceDesc, srv)
}

func _ClusterService_RegisterWorker_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(MRegisterRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ClusterServiceServer).RegisterWorker(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/rpc.ClusterService/RegisterWorker",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ClusterServiceServer).RegisterWorker(ctx, req.(*MRegisterRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _ClusterService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "rpc.ClusterService",
	HandlerType: (*ClusterServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "RegisterWorker",
			Handler:    _ClusterService_RegisterWorker_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "s_cluster.proto",
}
