// Code generated by protoc-gen-go. DO NOT EDIT.
// source: s_cluster.proto

package rpc

import (
	context "context"
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type MRegisterRequest struct {
	Id                   string   `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Port                 int32    `protobuf:"varint,2,opt,name=port,proto3" json:"port,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *MRegisterRequest) Reset()         { *m = MRegisterRequest{} }
func (m *MRegisterRequest) String() string { return proto.CompactTextString(m) }
func (*MRegisterRequest) ProtoMessage()    {}
func (*MRegisterRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_ea1288847038ea77, []int{0}
}

func (m *MRegisterRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_MRegisterRequest.Unmarshal(m, b)
}
func (m *MRegisterRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_MRegisterRequest.Marshal(b, m, deterministic)
}
func (m *MRegisterRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_MRegisterRequest.Merge(m, src)
}
func (m *MRegisterRequest) XXX_Size() int {
	return xxx_messageInfo_MRegisterRequest.Size(m)
}
func (m *MRegisterRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_MRegisterRequest.DiscardUnknown(m)
}

var xxx_messageInfo_MRegisterRequest proto.InternalMessageInfo

func (m *MRegisterRequest) GetId() string {
	if m != nil {
		return m.Id
	}
	return ""
}

func (m *MRegisterRequest) GetPort() int32 {
	if m != nil {
		return m.Port
	}
	return 0
}

type MRegisterResponse struct {
	Time                 int64    `protobuf:"varint,1,opt,name=time,proto3" json:"time,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *MRegisterResponse) Reset()         { *m = MRegisterResponse{} }
func (m *MRegisterResponse) String() string { return proto.CompactTextString(m) }
func (*MRegisterResponse) ProtoMessage()    {}
func (*MRegisterResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_ea1288847038ea77, []int{1}
}

func (m *MRegisterResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_MRegisterResponse.Unmarshal(m, b)
}
func (m *MRegisterResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_MRegisterResponse.Marshal(b, m, deterministic)
}
func (m *MRegisterResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_MRegisterResponse.Merge(m, src)
}
func (m *MRegisterResponse) XXX_Size() int {
	return xxx_messageInfo_MRegisterResponse.Size(m)
}
func (m *MRegisterResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_MRegisterResponse.DiscardUnknown(m)
}

var xxx_messageInfo_MRegisterResponse proto.InternalMessageInfo

func (m *MRegisterResponse) GetTime() int64 {
	if m != nil {
		return m.Time
	}
	return 0
}

// For the moment, this is identical to MRegisterRequest, but may diverge in the future.
type MWorkerDescriptor struct {
	Id                   string   `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Host                 string   `protobuf:"bytes,2,opt,name=host,proto3" json:"host,omitempty"`
	Port                 int32    `protobuf:"varint,3,opt,name=port,proto3" json:"port,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *MWorkerDescriptor) Reset()         { *m = MWorkerDescriptor{} }
func (m *MWorkerDescriptor) String() string { return proto.CompactTextString(m) }
func (*MWorkerDescriptor) ProtoMessage()    {}
func (*MWorkerDescriptor) Descriptor() ([]byte, []int) {
	return fileDescriptor_ea1288847038ea77, []int{2}
}

func (m *MWorkerDescriptor) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_MWorkerDescriptor.Unmarshal(m, b)
}
func (m *MWorkerDescriptor) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_MWorkerDescriptor.Marshal(b, m, deterministic)
}
func (m *MWorkerDescriptor) XXX_Merge(src proto.Message) {
	xxx_messageInfo_MWorkerDescriptor.Merge(m, src)
}
func (m *MWorkerDescriptor) XXX_Size() int {
	return xxx_messageInfo_MWorkerDescriptor.Size(m)
}
func (m *MWorkerDescriptor) XXX_DiscardUnknown() {
	xxx_messageInfo_MWorkerDescriptor.DiscardUnknown(m)
}

var xxx_messageInfo_MWorkerDescriptor proto.InternalMessageInfo

func (m *MWorkerDescriptor) GetId() string {
	if m != nil {
		return m.Id
	}
	return ""
}

func (m *MWorkerDescriptor) GetHost() string {
	if m != nil {
		return m.Host
	}
	return ""
}

func (m *MWorkerDescriptor) GetPort() int32 {
	if m != nil {
		return m.Port
	}
	return 0
}

func init() {
	proto.RegisterType((*MRegisterRequest)(nil), "rpc.MRegisterRequest")
	proto.RegisterType((*MRegisterResponse)(nil), "rpc.MRegisterResponse")
	proto.RegisterType((*MWorkerDescriptor)(nil), "rpc.MWorkerDescriptor")
}

func init() {
	proto.RegisterFile("s_cluster.proto", fileDescriptor_ea1288847038ea77)
}

var fileDescriptor_ea1288847038ea77 = []byte{
	// 209 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xe2, 0x2f, 0x8e, 0x4f, 0xce,
	0x29, 0x2d, 0x2e, 0x49, 0x2d, 0xd2, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0x62, 0x2e, 0x2a, 0x48,
	0x56, 0x32, 0xe3, 0x12, 0xf0, 0x0d, 0x4a, 0x4d, 0xcf, 0x04, 0x89, 0x07, 0xa5, 0x16, 0x96, 0xa6,
	0x16, 0x97, 0x08, 0xf1, 0x71, 0x31, 0x65, 0xa6, 0x48, 0x30, 0x2a, 0x30, 0x6a, 0x70, 0x06, 0x31,
	0x65, 0xa6, 0x08, 0x09, 0x71, 0xb1, 0x14, 0xe4, 0x17, 0x95, 0x48, 0x30, 0x29, 0x30, 0x6a, 0xb0,
	0x06, 0x81, 0xd9, 0x4a, 0xea, 0x5c, 0x82, 0x48, 0xfa, 0x8a, 0x0b, 0xf2, 0xf3, 0x8a, 0x53, 0x41,
	0x0a, 0x4b, 0x32, 0x73, 0x53, 0xc1, 0x5a, 0x99, 0x83, 0xc0, 0x6c, 0x25, 0x6f, 0x2e, 0x41, 0xdf,
	0xf0, 0xfc, 0xa2, 0xec, 0xd4, 0x22, 0x97, 0xd4, 0xe2, 0xe4, 0xa2, 0xcc, 0x82, 0x92, 0xfc, 0x22,
	0x6c, 0x36, 0x64, 0xe4, 0x17, 0x43, 0x6c, 0xe0, 0x0c, 0x02, 0xb3, 0xe1, 0xb6, 0x32, 0x23, 0x6c,
	0x35, 0x0a, 0xe6, 0xe2, 0x73, 0x86, 0xf8, 0x21, 0x38, 0xb5, 0xa8, 0x2c, 0x33, 0x39, 0x55, 0xc8,
	0x91, 0x8b, 0x0f, 0xe6, 0x0c, 0x88, 0x2d, 0x42, 0xa2, 0x7a, 0x45, 0x05, 0xc9, 0x7a, 0xe8, 0x9e,
	0x92, 0x12, 0x43, 0x17, 0x86, 0xb8, 0x59, 0x89, 0xc1, 0x89, 0x35, 0x0a, 0x14, 0x12, 0x49, 0x6c,
	0xe0, 0x50, 0x31, 0x06, 0x04, 0x00, 0x00, 0xff, 0xff, 0x2b, 0x3e, 0xe9, 0x1b, 0x28, 0x01, 0x00,
	0x00,
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

func (*UnimplementedClusterServiceServer) RegisterWorker(ctx context.Context, req *MRegisterRequest) (*MRegisterResponse, error) {
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
