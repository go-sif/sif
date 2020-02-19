// Code generated by protoc-gen-go. DO NOT EDIT.
// source: s_execution.proto

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

type MRunStageRequest struct {
	StageId              string               `protobuf:"bytes,1,opt,name=stageId,proto3" json:"stageId,omitempty"`
	RunShuffle           bool                 `protobuf:"varint,2,opt,name=runShuffle,proto3" json:"runShuffle,omitempty"`
	PrepCollect          bool                 `protobuf:"varint,3,opt,name=prepCollect,proto3" json:"prepCollect,omitempty"`
	AssignedBucket       uint64               `protobuf:"varint,4,opt,name=assignedBucket,proto3" json:"assignedBucket,omitempty"`
	Buckets              []uint64             `protobuf:"varint,5,rep,packed,name=buckets,proto3" json:"buckets,omitempty"`
	Workers              []*MWorkerDescriptor `protobuf:"bytes,6,rep,name=workers,proto3" json:"workers,omitempty"`
	XXX_NoUnkeyedLiteral struct{}             `json:"-"`
	XXX_unrecognized     []byte               `json:"-"`
	XXX_sizecache        int32                `json:"-"`
}

func (m *MRunStageRequest) Reset()         { *m = MRunStageRequest{} }
func (m *MRunStageRequest) String() string { return proto.CompactTextString(m) }
func (*MRunStageRequest) ProtoMessage()    {}
func (*MRunStageRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_a1270cbd13ab8b72, []int{0}
}

func (m *MRunStageRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_MRunStageRequest.Unmarshal(m, b)
}
func (m *MRunStageRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_MRunStageRequest.Marshal(b, m, deterministic)
}
func (m *MRunStageRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_MRunStageRequest.Merge(m, src)
}
func (m *MRunStageRequest) XXX_Size() int {
	return xxx_messageInfo_MRunStageRequest.Size(m)
}
func (m *MRunStageRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_MRunStageRequest.DiscardUnknown(m)
}

var xxx_messageInfo_MRunStageRequest proto.InternalMessageInfo

func (m *MRunStageRequest) GetStageId() string {
	if m != nil {
		return m.StageId
	}
	return ""
}

func (m *MRunStageRequest) GetRunShuffle() bool {
	if m != nil {
		return m.RunShuffle
	}
	return false
}

func (m *MRunStageRequest) GetPrepCollect() bool {
	if m != nil {
		return m.PrepCollect
	}
	return false
}

func (m *MRunStageRequest) GetAssignedBucket() uint64 {
	if m != nil {
		return m.AssignedBucket
	}
	return 0
}

func (m *MRunStageRequest) GetBuckets() []uint64 {
	if m != nil {
		return m.Buckets
	}
	return nil
}

func (m *MRunStageRequest) GetWorkers() []*MWorkerDescriptor {
	if m != nil {
		return m.Workers
	}
	return nil
}

type MRunStageResponse struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *MRunStageResponse) Reset()         { *m = MRunStageResponse{} }
func (m *MRunStageResponse) String() string { return proto.CompactTextString(m) }
func (*MRunStageResponse) ProtoMessage()    {}
func (*MRunStageResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_a1270cbd13ab8b72, []int{1}
}

func (m *MRunStageResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_MRunStageResponse.Unmarshal(m, b)
}
func (m *MRunStageResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_MRunStageResponse.Marshal(b, m, deterministic)
}
func (m *MRunStageResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_MRunStageResponse.Merge(m, src)
}
func (m *MRunStageResponse) XXX_Size() int {
	return xxx_messageInfo_MRunStageResponse.Size(m)
}
func (m *MRunStageResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_MRunStageResponse.DiscardUnknown(m)
}

var xxx_messageInfo_MRunStageResponse proto.InternalMessageInfo

func init() {
	proto.RegisterType((*MRunStageRequest)(nil), "rpc.MRunStageRequest")
	proto.RegisterType((*MRunStageResponse)(nil), "rpc.MRunStageResponse")
}

func init() { proto.RegisterFile("s_execution.proto", fileDescriptor_a1270cbd13ab8b72) }

var fileDescriptor_a1270cbd13ab8b72 = []byte{
	// 270 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x5c, 0xd0, 0x41, 0x4b, 0xfb, 0x30,
	0x18, 0x06, 0xf0, 0x7f, 0xd6, 0xfd, 0xb7, 0xf9, 0x0e, 0xb4, 0x8b, 0x38, 0xc2, 0x10, 0x09, 0x3d,
	0x48, 0x4f, 0x45, 0xe6, 0xd1, 0xdb, 0xd4, 0x83, 0x87, 0x21, 0xa4, 0x07, 0x8f, 0x63, 0xcb, 0xde,
	0xcd, 0xb2, 0xd2, 0xc4, 0xbc, 0x89, 0xfa, 0x89, 0xfd, 0x1c, 0x62, 0x4b, 0xa1, 0xf4, 0x96, 0xf7,
	0xc7, 0xc3, 0x03, 0x79, 0x60, 0x46, 0x1b, 0xfc, 0x46, 0x1d, 0x7c, 0x61, 0xaa, 0xcc, 0x3a, 0xe3,
	0x0d, 0x8f, 0x9c, 0xd5, 0x8b, 0x0b, 0xda, 0xe8, 0x32, 0x90, 0x47, 0xd7, 0x68, 0xf2, 0xc3, 0x20,
	0x5e, 0xab, 0x50, 0xe5, 0x7e, 0x7b, 0x44, 0x85, 0x1f, 0x01, 0xc9, 0x73, 0x01, 0x63, 0xfa, 0xbb,
	0x5f, 0xf6, 0x82, 0x49, 0x96, 0x9e, 0xa9, 0xf6, 0xe4, 0x37, 0x00, 0x2e, 0x54, 0xf9, 0x7b, 0x38,
	0x1c, 0x4a, 0x14, 0x03, 0xc9, 0xd2, 0x89, 0xea, 0x08, 0x97, 0x30, 0xb5, 0x0e, 0xed, 0xa3, 0x29,
	0x4b, 0xd4, 0x5e, 0x44, 0x75, 0xa0, 0x4b, 0xfc, 0x16, 0xce, 0xb7, 0x44, 0xc5, 0xb1, 0xc2, 0xfd,
	0x2a, 0xe8, 0x13, 0x7a, 0x31, 0x94, 0x2c, 0x1d, 0xaa, 0x9e, 0xf2, 0x6b, 0x18, 0xef, 0xea, 0x17,
	0x89, 0xff, 0x32, 0x4a, 0x87, 0xab, 0x41, 0xcc, 0x54, 0x4b, 0xfc, 0x0e, 0xc6, 0x5f, 0xc6, 0x9d,
	0xd0, 0x91, 0x18, 0xc9, 0x28, 0x9d, 0x2e, 0xe7, 0x99, 0xb3, 0x3a, 0x5b, 0xbf, 0xd5, 0xf8, 0x84,
	0xa4, 0x5d, 0x61, 0xbd, 0x71, 0xaa, 0x8d, 0x25, 0x97, 0x30, 0xeb, 0xfc, 0x93, 0xac, 0xa9, 0x08,
	0x97, 0xaf, 0x10, 0x3f, 0xb7, 0x33, 0xe5, 0xe8, 0x3e, 0x0b, 0x8d, 0xfc, 0x01, 0x26, 0x6d, 0x8e,
	0x5f, 0x35, 0xad, 0xbd, 0x7d, 0x16, 0xf3, 0x3e, 0x37, 0x75, 0xc9, 0xbf, 0xdd, 0xa8, 0x5e, 0xf5,
	0xfe, 0x37, 0x00, 0x00, 0xff, 0xff, 0xec, 0x20, 0xe6, 0xde, 0x80, 0x01, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConnInterface

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion6

// ExecutionServiceClient is the client API for ExecutionService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type ExecutionServiceClient interface {
	RunStage(ctx context.Context, in *MRunStageRequest, opts ...grpc.CallOption) (*MRunStageResponse, error)
}

type executionServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewExecutionServiceClient(cc grpc.ClientConnInterface) ExecutionServiceClient {
	return &executionServiceClient{cc}
}

func (c *executionServiceClient) RunStage(ctx context.Context, in *MRunStageRequest, opts ...grpc.CallOption) (*MRunStageResponse, error) {
	out := new(MRunStageResponse)
	err := c.cc.Invoke(ctx, "/rpc.ExecutionService/RunStage", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ExecutionServiceServer is the server API for ExecutionService service.
type ExecutionServiceServer interface {
	RunStage(context.Context, *MRunStageRequest) (*MRunStageResponse, error)
}

// UnimplementedExecutionServiceServer can be embedded to have forward compatible implementations.
type UnimplementedExecutionServiceServer struct {
}

func (*UnimplementedExecutionServiceServer) RunStage(ctx context.Context, req *MRunStageRequest) (*MRunStageResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RunStage not implemented")
}

func RegisterExecutionServiceServer(s *grpc.Server, srv ExecutionServiceServer) {
	s.RegisterService(&_ExecutionService_serviceDesc, srv)
}

func _ExecutionService_RunStage_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(MRunStageRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExecutionServiceServer).RunStage(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/rpc.ExecutionService/RunStage",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExecutionServiceServer).RunStage(ctx, req.(*MRunStageRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _ExecutionService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "rpc.ExecutionService",
	HandlerType: (*ExecutionServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "RunStage",
			Handler:    _ExecutionService_RunStage_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "s_execution.proto",
}