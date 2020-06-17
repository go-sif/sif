// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.24.0
// 	protoc        v3.12.3
// source: s_lifecycle.proto

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

type MStopResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Time int64 `protobuf:"varint,1,opt,name=time,proto3" json:"time,omitempty"`
}

func (x *MStopResponse) Reset() {
	*x = MStopResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_s_lifecycle_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MStopResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MStopResponse) ProtoMessage() {}

func (x *MStopResponse) ProtoReflect() protoreflect.Message {
	mi := &file_s_lifecycle_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MStopResponse.ProtoReflect.Descriptor instead.
func (*MStopResponse) Descriptor() ([]byte, []int) {
	return file_s_lifecycle_proto_rawDescGZIP(), []int{0}
}

func (x *MStopResponse) GetTime() int64 {
	if x != nil {
		return x.Time
	}
	return 0
}

var File_s_lifecycle_proto protoreflect.FileDescriptor

var file_s_lifecycle_proto_rawDesc = []byte{
	0x0a, 0x11, 0x73, 0x5f, 0x6c, 0x69, 0x66, 0x65, 0x63, 0x79, 0x63, 0x6c, 0x65, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x12, 0x03, 0x72, 0x70, 0x63, 0x1a, 0x0f, 0x73, 0x5f, 0x63, 0x6c, 0x75, 0x73,
	0x74, 0x65, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x23, 0x0a, 0x0d, 0x4d, 0x53, 0x74,
	0x6f, 0x70, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x74, 0x69,
	0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x04, 0x74, 0x69, 0x6d, 0x65, 0x32, 0x86,
	0x01, 0x0a, 0x10, 0x4c, 0x69, 0x66, 0x65, 0x63, 0x79, 0x63, 0x6c, 0x65, 0x53, 0x65, 0x72, 0x76,
	0x69, 0x63, 0x65, 0x12, 0x3c, 0x0a, 0x0c, 0x47, 0x72, 0x61, 0x63, 0x65, 0x66, 0x75, 0x6c, 0x53,
	0x74, 0x6f, 0x70, 0x12, 0x16, 0x2e, 0x72, 0x70, 0x63, 0x2e, 0x4d, 0x57, 0x6f, 0x72, 0x6b, 0x65,
	0x72, 0x44, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x6f, 0x72, 0x1a, 0x12, 0x2e, 0x72, 0x70,
	0x63, 0x2e, 0x4d, 0x53, 0x74, 0x6f, 0x70, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22,
	0x00, 0x12, 0x34, 0x0a, 0x04, 0x53, 0x74, 0x6f, 0x70, 0x12, 0x16, 0x2e, 0x72, 0x70, 0x63, 0x2e,
	0x4d, 0x57, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x44, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x6f,
	0x72, 0x1a, 0x12, 0x2e, 0x72, 0x70, 0x63, 0x2e, 0x4d, 0x53, 0x74, 0x6f, 0x70, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x42, 0x24, 0x5a, 0x22, 0x67, 0x69, 0x74, 0x68, 0x75,
	0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x67, 0x6f, 0x2d, 0x73, 0x69, 0x66, 0x2f, 0x73, 0x69, 0x66,
	0x2f, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2f, 0x72, 0x70, 0x63, 0x62, 0x06, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_s_lifecycle_proto_rawDescOnce sync.Once
	file_s_lifecycle_proto_rawDescData = file_s_lifecycle_proto_rawDesc
)

func file_s_lifecycle_proto_rawDescGZIP() []byte {
	file_s_lifecycle_proto_rawDescOnce.Do(func() {
		file_s_lifecycle_proto_rawDescData = protoimpl.X.CompressGZIP(file_s_lifecycle_proto_rawDescData)
	})
	return file_s_lifecycle_proto_rawDescData
}

var file_s_lifecycle_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_s_lifecycle_proto_goTypes = []interface{}{
	(*MStopResponse)(nil),     // 0: rpc.MStopResponse
	(*MWorkerDescriptor)(nil), // 1: rpc.MWorkerDescriptor
}
var file_s_lifecycle_proto_depIdxs = []int32{
	1, // 0: rpc.LifecycleService.GracefulStop:input_type -> rpc.MWorkerDescriptor
	1, // 1: rpc.LifecycleService.Stop:input_type -> rpc.MWorkerDescriptor
	0, // 2: rpc.LifecycleService.GracefulStop:output_type -> rpc.MStopResponse
	0, // 3: rpc.LifecycleService.Stop:output_type -> rpc.MStopResponse
	2, // [2:4] is the sub-list for method output_type
	0, // [0:2] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_s_lifecycle_proto_init() }
func file_s_lifecycle_proto_init() {
	if File_s_lifecycle_proto != nil {
		return
	}
	file_s_cluster_proto_init()
	if !protoimpl.UnsafeEnabled {
		file_s_lifecycle_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*MStopResponse); i {
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
			RawDescriptor: file_s_lifecycle_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_s_lifecycle_proto_goTypes,
		DependencyIndexes: file_s_lifecycle_proto_depIdxs,
		MessageInfos:      file_s_lifecycle_proto_msgTypes,
	}.Build()
	File_s_lifecycle_proto = out.File
	file_s_lifecycle_proto_rawDesc = nil
	file_s_lifecycle_proto_goTypes = nil
	file_s_lifecycle_proto_depIdxs = nil
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConnInterface

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion6

// LifecycleServiceClient is the client API for LifecycleService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type LifecycleServiceClient interface {
	GracefulStop(ctx context.Context, in *MWorkerDescriptor, opts ...grpc.CallOption) (*MStopResponse, error)
	Stop(ctx context.Context, in *MWorkerDescriptor, opts ...grpc.CallOption) (*MStopResponse, error)
}

type lifecycleServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewLifecycleServiceClient(cc grpc.ClientConnInterface) LifecycleServiceClient {
	return &lifecycleServiceClient{cc}
}

func (c *lifecycleServiceClient) GracefulStop(ctx context.Context, in *MWorkerDescriptor, opts ...grpc.CallOption) (*MStopResponse, error) {
	out := new(MStopResponse)
	err := c.cc.Invoke(ctx, "/rpc.LifecycleService/GracefulStop", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *lifecycleServiceClient) Stop(ctx context.Context, in *MWorkerDescriptor, opts ...grpc.CallOption) (*MStopResponse, error) {
	out := new(MStopResponse)
	err := c.cc.Invoke(ctx, "/rpc.LifecycleService/Stop", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// LifecycleServiceServer is the server API for LifecycleService service.
type LifecycleServiceServer interface {
	GracefulStop(context.Context, *MWorkerDescriptor) (*MStopResponse, error)
	Stop(context.Context, *MWorkerDescriptor) (*MStopResponse, error)
}

// UnimplementedLifecycleServiceServer can be embedded to have forward compatible implementations.
type UnimplementedLifecycleServiceServer struct {
}

func (*UnimplementedLifecycleServiceServer) GracefulStop(context.Context, *MWorkerDescriptor) (*MStopResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GracefulStop not implemented")
}
func (*UnimplementedLifecycleServiceServer) Stop(context.Context, *MWorkerDescriptor) (*MStopResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Stop not implemented")
}

func RegisterLifecycleServiceServer(s *grpc.Server, srv LifecycleServiceServer) {
	s.RegisterService(&_LifecycleService_serviceDesc, srv)
}

func _LifecycleService_GracefulStop_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(MWorkerDescriptor)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LifecycleServiceServer).GracefulStop(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/rpc.LifecycleService/GracefulStop",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LifecycleServiceServer).GracefulStop(ctx, req.(*MWorkerDescriptor))
	}
	return interceptor(ctx, in, info, handler)
}

func _LifecycleService_Stop_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(MWorkerDescriptor)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LifecycleServiceServer).Stop(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/rpc.LifecycleService/Stop",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LifecycleServiceServer).Stop(ctx, req.(*MWorkerDescriptor))
	}
	return interceptor(ctx, in, info, handler)
}

var _LifecycleService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "rpc.LifecycleService",
	HandlerType: (*LifecycleServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GracefulStop",
			Handler:    _LifecycleService_GracefulStop_Handler,
		},
		{
			MethodName: "Stop",
			Handler:    _LifecycleService_Stop_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "s_lifecycle.proto",
}
