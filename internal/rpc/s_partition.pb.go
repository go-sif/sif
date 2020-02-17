// Code generated by protoc-gen-go. DO NOT EDIT.
// source: s_partition.proto

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

type MAssignPartitionRequest struct {
	Loader               []byte   `protobuf:"bytes,1,opt,name=loader,proto3" json:"loader,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *MAssignPartitionRequest) Reset()         { *m = MAssignPartitionRequest{} }
func (m *MAssignPartitionRequest) String() string { return proto.CompactTextString(m) }
func (*MAssignPartitionRequest) ProtoMessage()    {}
func (*MAssignPartitionRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_8dfd1f9f5f8b7abf, []int{0}
}

func (m *MAssignPartitionRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_MAssignPartitionRequest.Unmarshal(m, b)
}
func (m *MAssignPartitionRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_MAssignPartitionRequest.Marshal(b, m, deterministic)
}
func (m *MAssignPartitionRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_MAssignPartitionRequest.Merge(m, src)
}
func (m *MAssignPartitionRequest) XXX_Size() int {
	return xxx_messageInfo_MAssignPartitionRequest.Size(m)
}
func (m *MAssignPartitionRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_MAssignPartitionRequest.DiscardUnknown(m)
}

var xxx_messageInfo_MAssignPartitionRequest proto.InternalMessageInfo

func (m *MAssignPartitionRequest) GetLoader() []byte {
	if m != nil {
		return m.Loader
	}
	return nil
}

type MAssignPartitionResponse struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *MAssignPartitionResponse) Reset()         { *m = MAssignPartitionResponse{} }
func (m *MAssignPartitionResponse) String() string { return proto.CompactTextString(m) }
func (*MAssignPartitionResponse) ProtoMessage()    {}
func (*MAssignPartitionResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_8dfd1f9f5f8b7abf, []int{1}
}

func (m *MAssignPartitionResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_MAssignPartitionResponse.Unmarshal(m, b)
}
func (m *MAssignPartitionResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_MAssignPartitionResponse.Marshal(b, m, deterministic)
}
func (m *MAssignPartitionResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_MAssignPartitionResponse.Merge(m, src)
}
func (m *MAssignPartitionResponse) XXX_Size() int {
	return xxx_messageInfo_MAssignPartitionResponse.Size(m)
}
func (m *MAssignPartitionResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_MAssignPartitionResponse.DiscardUnknown(m)
}

var xxx_messageInfo_MAssignPartitionResponse proto.InternalMessageInfo

type MPartitionMeta struct {
	Id                   string   `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	NumRows              uint32   `protobuf:"varint,2,opt,name=numRows,proto3" json:"numRows,omitempty"`
	MaxRows              uint32   `protobuf:"varint,3,opt,name=maxRows,proto3" json:"maxRows,omitempty"`
	IsKeyed              bool     `protobuf:"varint,4,opt,name=isKeyed,proto3" json:"isKeyed,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *MPartitionMeta) Reset()         { *m = MPartitionMeta{} }
func (m *MPartitionMeta) String() string { return proto.CompactTextString(m) }
func (*MPartitionMeta) ProtoMessage()    {}
func (*MPartitionMeta) Descriptor() ([]byte, []int) {
	return fileDescriptor_8dfd1f9f5f8b7abf, []int{2}
}

func (m *MPartitionMeta) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_MPartitionMeta.Unmarshal(m, b)
}
func (m *MPartitionMeta) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_MPartitionMeta.Marshal(b, m, deterministic)
}
func (m *MPartitionMeta) XXX_Merge(src proto.Message) {
	xxx_messageInfo_MPartitionMeta.Merge(m, src)
}
func (m *MPartitionMeta) XXX_Size() int {
	return xxx_messageInfo_MPartitionMeta.Size(m)
}
func (m *MPartitionMeta) XXX_DiscardUnknown() {
	xxx_messageInfo_MPartitionMeta.DiscardUnknown(m)
}

var xxx_messageInfo_MPartitionMeta proto.InternalMessageInfo

func (m *MPartitionMeta) GetId() string {
	if m != nil {
		return m.Id
	}
	return ""
}

func (m *MPartitionMeta) GetNumRows() uint32 {
	if m != nil {
		return m.NumRows
	}
	return 0
}

func (m *MPartitionMeta) GetMaxRows() uint32 {
	if m != nil {
		return m.MaxRows
	}
	return 0
}

func (m *MPartitionMeta) GetIsKeyed() bool {
	if m != nil {
		return m.IsKeyed
	}
	return false
}

type MShufflePartitionRequest struct {
	Bucket               uint64   `protobuf:"varint,1,opt,name=bucket,proto3" json:"bucket,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *MShufflePartitionRequest) Reset()         { *m = MShufflePartitionRequest{} }
func (m *MShufflePartitionRequest) String() string { return proto.CompactTextString(m) }
func (*MShufflePartitionRequest) ProtoMessage()    {}
func (*MShufflePartitionRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_8dfd1f9f5f8b7abf, []int{3}
}

func (m *MShufflePartitionRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_MShufflePartitionRequest.Unmarshal(m, b)
}
func (m *MShufflePartitionRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_MShufflePartitionRequest.Marshal(b, m, deterministic)
}
func (m *MShufflePartitionRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_MShufflePartitionRequest.Merge(m, src)
}
func (m *MShufflePartitionRequest) XXX_Size() int {
	return xxx_messageInfo_MShufflePartitionRequest.Size(m)
}
func (m *MShufflePartitionRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_MShufflePartitionRequest.DiscardUnknown(m)
}

var xxx_messageInfo_MShufflePartitionRequest proto.InternalMessageInfo

func (m *MShufflePartitionRequest) GetBucket() uint64 {
	if m != nil {
		return m.Bucket
	}
	return 0
}

type MShufflePartitionResponse struct {
	Ready                bool            `protobuf:"varint,1,opt,name=ready,proto3" json:"ready,omitempty"`
	HasNext              bool            `protobuf:"varint,2,opt,name=hasNext,proto3" json:"hasNext,omitempty"`
	Part                 *MPartitionMeta `protobuf:"bytes,3,opt,name=part,proto3" json:"part,omitempty"`
	XXX_NoUnkeyedLiteral struct{}        `json:"-"`
	XXX_unrecognized     []byte          `json:"-"`
	XXX_sizecache        int32           `json:"-"`
}

func (m *MShufflePartitionResponse) Reset()         { *m = MShufflePartitionResponse{} }
func (m *MShufflePartitionResponse) String() string { return proto.CompactTextString(m) }
func (*MShufflePartitionResponse) ProtoMessage()    {}
func (*MShufflePartitionResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_8dfd1f9f5f8b7abf, []int{4}
}

func (m *MShufflePartitionResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_MShufflePartitionResponse.Unmarshal(m, b)
}
func (m *MShufflePartitionResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_MShufflePartitionResponse.Marshal(b, m, deterministic)
}
func (m *MShufflePartitionResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_MShufflePartitionResponse.Merge(m, src)
}
func (m *MShufflePartitionResponse) XXX_Size() int {
	return xxx_messageInfo_MShufflePartitionResponse.Size(m)
}
func (m *MShufflePartitionResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_MShufflePartitionResponse.DiscardUnknown(m)
}

var xxx_messageInfo_MShufflePartitionResponse proto.InternalMessageInfo

func (m *MShufflePartitionResponse) GetReady() bool {
	if m != nil {
		return m.Ready
	}
	return false
}

func (m *MShufflePartitionResponse) GetHasNext() bool {
	if m != nil {
		return m.HasNext
	}
	return false
}

func (m *MShufflePartitionResponse) GetPart() *MPartitionMeta {
	if m != nil {
		return m.Part
	}
	return nil
}

type MTransferPartitionDataRequest struct {
	Id                   string   `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *MTransferPartitionDataRequest) Reset()         { *m = MTransferPartitionDataRequest{} }
func (m *MTransferPartitionDataRequest) String() string { return proto.CompactTextString(m) }
func (*MTransferPartitionDataRequest) ProtoMessage()    {}
func (*MTransferPartitionDataRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_8dfd1f9f5f8b7abf, []int{5}
}

func (m *MTransferPartitionDataRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_MTransferPartitionDataRequest.Unmarshal(m, b)
}
func (m *MTransferPartitionDataRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_MTransferPartitionDataRequest.Marshal(b, m, deterministic)
}
func (m *MTransferPartitionDataRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_MTransferPartitionDataRequest.Merge(m, src)
}
func (m *MTransferPartitionDataRequest) XXX_Size() int {
	return xxx_messageInfo_MTransferPartitionDataRequest.Size(m)
}
func (m *MTransferPartitionDataRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_MTransferPartitionDataRequest.DiscardUnknown(m)
}

var xxx_messageInfo_MTransferPartitionDataRequest proto.InternalMessageInfo

func (m *MTransferPartitionDataRequest) GetId() string {
	if m != nil {
		return m.Id
	}
	return ""
}

type MPartitionChunk struct {
	Data                 []byte   `protobuf:"bytes,1,opt,name=data,proto3" json:"data,omitempty"`
	DataType             int32    `protobuf:"varint,2,opt,name=dataType,proto3" json:"dataType,omitempty"`
	KeyData              []uint64 `protobuf:"varint,3,rep,packed,name=keyData,proto3" json:"keyData,omitempty"`
	VarDataRowNum        int32    `protobuf:"varint,4,opt,name=varDataRowNum,proto3" json:"varDataRowNum,omitempty"`
	VarDataColName       string   `protobuf:"bytes,5,opt,name=varDataColName,proto3" json:"varDataColName,omitempty"`
	TotalSizeBytes       int32    `protobuf:"varint,6,opt,name=totalSizeBytes,proto3" json:"totalSizeBytes,omitempty"`
	Append               int32    `protobuf:"varint,7,opt,name=append,proto3" json:"append,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *MPartitionChunk) Reset()         { *m = MPartitionChunk{} }
func (m *MPartitionChunk) String() string { return proto.CompactTextString(m) }
func (*MPartitionChunk) ProtoMessage()    {}
func (*MPartitionChunk) Descriptor() ([]byte, []int) {
	return fileDescriptor_8dfd1f9f5f8b7abf, []int{6}
}

func (m *MPartitionChunk) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_MPartitionChunk.Unmarshal(m, b)
}
func (m *MPartitionChunk) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_MPartitionChunk.Marshal(b, m, deterministic)
}
func (m *MPartitionChunk) XXX_Merge(src proto.Message) {
	xxx_messageInfo_MPartitionChunk.Merge(m, src)
}
func (m *MPartitionChunk) XXX_Size() int {
	return xxx_messageInfo_MPartitionChunk.Size(m)
}
func (m *MPartitionChunk) XXX_DiscardUnknown() {
	xxx_messageInfo_MPartitionChunk.DiscardUnknown(m)
}

var xxx_messageInfo_MPartitionChunk proto.InternalMessageInfo

func (m *MPartitionChunk) GetData() []byte {
	if m != nil {
		return m.Data
	}
	return nil
}

func (m *MPartitionChunk) GetDataType() int32 {
	if m != nil {
		return m.DataType
	}
	return 0
}

func (m *MPartitionChunk) GetKeyData() []uint64 {
	if m != nil {
		return m.KeyData
	}
	return nil
}

func (m *MPartitionChunk) GetVarDataRowNum() int32 {
	if m != nil {
		return m.VarDataRowNum
	}
	return 0
}

func (m *MPartitionChunk) GetVarDataColName() string {
	if m != nil {
		return m.VarDataColName
	}
	return ""
}

func (m *MPartitionChunk) GetTotalSizeBytes() int32 {
	if m != nil {
		return m.TotalSizeBytes
	}
	return 0
}

func (m *MPartitionChunk) GetAppend() int32 {
	if m != nil {
		return m.Append
	}
	return 0
}

// Intended for disk serialization, not transmission
type DPartition struct {
	Id                   string                `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	NumRows              uint32                `protobuf:"varint,2,opt,name=numRows,proto3" json:"numRows,omitempty"`
	MaxRows              uint32                `protobuf:"varint,3,opt,name=maxRows,proto3" json:"maxRows,omitempty"`
	IsKeyed              bool                  `protobuf:"varint,4,opt,name=isKeyed,proto3" json:"isKeyed,omitempty"`
	RowData              []byte                `protobuf:"bytes,5,opt,name=rowData,proto3" json:"rowData,omitempty"`
	RowMeta              []byte                `protobuf:"bytes,6,opt,name=rowMeta,proto3" json:"rowMeta,omitempty"`
	Keys                 []uint64              `protobuf:"varint,7,rep,packed,name=keys,proto3" json:"keys,omitempty"`
	SerializedVarRowData []*DPartition_DVarRow `protobuf:"bytes,8,rep,name=serializedVarRowData,proto3" json:"serializedVarRowData,omitempty"`
	XXX_NoUnkeyedLiteral struct{}              `json:"-"`
	XXX_unrecognized     []byte                `json:"-"`
	XXX_sizecache        int32                 `json:"-"`
}

func (m *DPartition) Reset()         { *m = DPartition{} }
func (m *DPartition) String() string { return proto.CompactTextString(m) }
func (*DPartition) ProtoMessage()    {}
func (*DPartition) Descriptor() ([]byte, []int) {
	return fileDescriptor_8dfd1f9f5f8b7abf, []int{7}
}

func (m *DPartition) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_DPartition.Unmarshal(m, b)
}
func (m *DPartition) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_DPartition.Marshal(b, m, deterministic)
}
func (m *DPartition) XXX_Merge(src proto.Message) {
	xxx_messageInfo_DPartition.Merge(m, src)
}
func (m *DPartition) XXX_Size() int {
	return xxx_messageInfo_DPartition.Size(m)
}
func (m *DPartition) XXX_DiscardUnknown() {
	xxx_messageInfo_DPartition.DiscardUnknown(m)
}

var xxx_messageInfo_DPartition proto.InternalMessageInfo

func (m *DPartition) GetId() string {
	if m != nil {
		return m.Id
	}
	return ""
}

func (m *DPartition) GetNumRows() uint32 {
	if m != nil {
		return m.NumRows
	}
	return 0
}

func (m *DPartition) GetMaxRows() uint32 {
	if m != nil {
		return m.MaxRows
	}
	return 0
}

func (m *DPartition) GetIsKeyed() bool {
	if m != nil {
		return m.IsKeyed
	}
	return false
}

func (m *DPartition) GetRowData() []byte {
	if m != nil {
		return m.RowData
	}
	return nil
}

func (m *DPartition) GetRowMeta() []byte {
	if m != nil {
		return m.RowMeta
	}
	return nil
}

func (m *DPartition) GetKeys() []uint64 {
	if m != nil {
		return m.Keys
	}
	return nil
}

func (m *DPartition) GetSerializedVarRowData() []*DPartition_DVarRow {
	if m != nil {
		return m.SerializedVarRowData
	}
	return nil
}

type DPartition_DVarRow struct {
	RowData              map[string][]byte `protobuf:"bytes,1,rep,name=rowData,proto3" json:"rowData,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	XXX_NoUnkeyedLiteral struct{}          `json:"-"`
	XXX_unrecognized     []byte            `json:"-"`
	XXX_sizecache        int32             `json:"-"`
}

func (m *DPartition_DVarRow) Reset()         { *m = DPartition_DVarRow{} }
func (m *DPartition_DVarRow) String() string { return proto.CompactTextString(m) }
func (*DPartition_DVarRow) ProtoMessage()    {}
func (*DPartition_DVarRow) Descriptor() ([]byte, []int) {
	return fileDescriptor_8dfd1f9f5f8b7abf, []int{7, 0}
}

func (m *DPartition_DVarRow) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_DPartition_DVarRow.Unmarshal(m, b)
}
func (m *DPartition_DVarRow) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_DPartition_DVarRow.Marshal(b, m, deterministic)
}
func (m *DPartition_DVarRow) XXX_Merge(src proto.Message) {
	xxx_messageInfo_DPartition_DVarRow.Merge(m, src)
}
func (m *DPartition_DVarRow) XXX_Size() int {
	return xxx_messageInfo_DPartition_DVarRow.Size(m)
}
func (m *DPartition_DVarRow) XXX_DiscardUnknown() {
	xxx_messageInfo_DPartition_DVarRow.DiscardUnknown(m)
}

var xxx_messageInfo_DPartition_DVarRow proto.InternalMessageInfo

func (m *DPartition_DVarRow) GetRowData() map[string][]byte {
	if m != nil {
		return m.RowData
	}
	return nil
}

func init() {
	proto.RegisterType((*MAssignPartitionRequest)(nil), "rpc.MAssignPartitionRequest")
	proto.RegisterType((*MAssignPartitionResponse)(nil), "rpc.MAssignPartitionResponse")
	proto.RegisterType((*MPartitionMeta)(nil), "rpc.MPartitionMeta")
	proto.RegisterType((*MShufflePartitionRequest)(nil), "rpc.MShufflePartitionRequest")
	proto.RegisterType((*MShufflePartitionResponse)(nil), "rpc.MShufflePartitionResponse")
	proto.RegisterType((*MTransferPartitionDataRequest)(nil), "rpc.MTransferPartitionDataRequest")
	proto.RegisterType((*MPartitionChunk)(nil), "rpc.MPartitionChunk")
	proto.RegisterType((*DPartition)(nil), "rpc.DPartition")
	proto.RegisterType((*DPartition_DVarRow)(nil), "rpc.DPartition.DVarRow")
	proto.RegisterMapType((map[string][]byte)(nil), "rpc.DPartition.DVarRow.RowDataEntry")
}

func init() { proto.RegisterFile("s_partition.proto", fileDescriptor_8dfd1f9f5f8b7abf) }

var fileDescriptor_8dfd1f9f5f8b7abf = []byte{
	// 598 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xb4, 0x54, 0x4f, 0x73, 0xd3, 0x3e,
	0x10, 0xad, 0xff, 0xa4, 0xc9, 0x6f, 0xfb, 0x5f, 0xbf, 0xd2, 0x1a, 0x4f, 0xcb, 0x64, 0x3c, 0x1d,
	0xc8, 0x29, 0x40, 0xb8, 0x30, 0x3d, 0x30, 0x43, 0x5b, 0x4e, 0x9d, 0x74, 0x3a, 0x4a, 0xe1, 0xca,
	0xa8, 0xf1, 0x96, 0x7a, 0xe2, 0xd8, 0x46, 0x92, 0x9b, 0xba, 0x77, 0x4e, 0x9c, 0xf8, 0xa0, 0x7c,
	0x07, 0x46, 0xb2, 0xec, 0x50, 0x93, 0x70, 0xe3, 0x64, 0xbf, 0x7d, 0xda, 0xd5, 0xd3, 0xd3, 0xae,
	0x60, 0x47, 0x7c, 0xce, 0x18, 0x97, 0x91, 0x8c, 0xd2, 0xa4, 0x9f, 0xf1, 0x54, 0xa6, 0xc4, 0xe1,
	0xd9, 0x38, 0x78, 0x0d, 0xfb, 0xc3, 0xf7, 0x42, 0x44, 0x5f, 0x92, 0xcb, 0x8a, 0xa6, 0xf8, 0x35,
	0x47, 0x21, 0xc9, 0x1e, 0xac, 0xc6, 0x29, 0x0b, 0x91, 0x7b, 0x56, 0xd7, 0xea, 0xad, 0x53, 0x83,
	0x02, 0x1f, 0xbc, 0x3f, 0x53, 0x44, 0x96, 0x26, 0x02, 0x83, 0x04, 0x36, 0x87, 0x75, 0x74, 0x88,
	0x92, 0x91, 0x4d, 0xb0, 0xa3, 0x50, 0x57, 0xf8, 0x8f, 0xda, 0x51, 0x48, 0x3c, 0x68, 0x27, 0xf9,
	0x94, 0xa6, 0x33, 0xe1, 0xd9, 0x5d, 0xab, 0xb7, 0x41, 0x2b, 0xa8, 0x98, 0x29, 0xbb, 0xd7, 0x8c,
	0x53, 0x32, 0x06, 0x2a, 0x26, 0x12, 0xe7, 0x58, 0x60, 0xe8, 0xb9, 0x5d, 0xab, 0xd7, 0xa1, 0x15,
	0x0c, 0x06, 0xe0, 0x0d, 0x47, 0xb7, 0xf9, 0xcd, 0x4d, 0x8c, 0x8b, 0xf4, 0x5f, 0xe7, 0xe3, 0x09,
	0x4a, 0xbd, 0xbb, 0x4b, 0x0d, 0x0a, 0x24, 0x3c, 0x5d, 0x90, 0x53, 0x1e, 0x80, 0xec, 0x42, 0x8b,
	0x23, 0x0b, 0x0b, 0x9d, 0xd3, 0xa1, 0x25, 0x50, 0x02, 0x6e, 0x99, 0xb8, 0xc0, 0x7b, 0xa9, 0x45,
	0x77, 0x68, 0x05, 0xc9, 0x0b, 0x70, 0x95, 0xaf, 0x5a, 0xf1, 0xda, 0xe0, 0xff, 0x3e, 0xcf, 0xc6,
	0xfd, 0xc7, 0x0e, 0x50, 0xbd, 0x20, 0x78, 0x09, 0x87, 0xc3, 0x2b, 0xce, 0x12, 0x71, 0x83, 0xbc,
	0xe6, 0xcf, 0x98, 0x64, 0x95, 0xdc, 0x86, 0x51, 0xc1, 0x4f, 0x0b, 0xb6, 0xe6, 0x95, 0x4e, 0x6f,
	0xf3, 0x64, 0x42, 0x08, 0xb8, 0x21, 0x93, 0xcc, 0x5c, 0x88, 0xfe, 0x27, 0x3e, 0x74, 0xd4, 0xf7,
	0xaa, 0xc8, 0x50, 0x8b, 0x6b, 0xd1, 0x1a, 0x93, 0x03, 0x68, 0x4f, 0xb0, 0x50, 0xbb, 0x78, 0x4e,
	0xd7, 0xe9, 0xb9, 0x27, 0xf6, 0xb6, 0x45, 0xab, 0x10, 0x39, 0x82, 0x8d, 0x3b, 0xc6, 0xb5, 0x86,
	0x74, 0x76, 0x91, 0x4f, 0xb5, 0xb9, 0x2d, 0xfa, 0x38, 0x48, 0x9e, 0xc3, 0xa6, 0x09, 0x9c, 0xa6,
	0xf1, 0x05, 0x9b, 0xa2, 0xd7, 0xd2, 0x1a, 0x1b, 0x51, 0xb5, 0x4e, 0xa6, 0x92, 0xc5, 0xa3, 0xe8,
	0x01, 0x4f, 0x0a, 0x89, 0xc2, 0x5b, 0xd5, 0xe5, 0x1a, 0x51, 0x75, 0x2d, 0x2c, 0xcb, 0x30, 0x09,
	0xbd, 0xb6, 0xe6, 0x0d, 0x0a, 0xbe, 0x3b, 0x00, 0x67, 0xf5, 0x79, 0xff, 0x6d, 0xdf, 0x28, 0x86,
	0xa7, 0x33, 0x6d, 0x4c, 0x4b, 0x7b, 0x59, 0x41, 0xc3, 0xa8, 0x8b, 0xd3, 0xfa, 0x4b, 0x46, 0x77,
	0xf2, 0x1e, 0xb8, 0x13, 0x2c, 0x84, 0xd7, 0xae, 0x9d, 0xd4, 0x98, 0x9c, 0xc3, 0xae, 0x40, 0x1e,
	0xb1, 0x38, 0x7a, 0xc0, 0xf0, 0x13, 0xe3, 0xd4, 0x14, 0xee, 0x74, 0x9d, 0xde, 0xda, 0x60, 0x5f,
	0xb7, 0xc4, 0xfc, 0x60, 0xfd, 0xb3, 0x72, 0x09, 0x5d, 0x98, 0xe4, 0x7f, 0xb3, 0xa0, 0x6d, 0x56,
	0x90, 0x77, 0x73, 0x91, 0x96, 0xae, 0x75, 0xb4, 0xa4, 0x56, 0xdf, 0x64, 0x7f, 0x48, 0x24, 0x2f,
	0xea, 0xa3, 0xf8, 0xc7, 0xb0, 0xfe, 0x3b, 0x41, 0xb6, 0xc1, 0x99, 0x60, 0x61, 0x3c, 0x55, 0xbf,
	0xaa, 0xdb, 0xef, 0x58, 0x9c, 0x97, 0x8d, 0xb3, 0x4e, 0x4b, 0x70, 0x6c, 0xbf, 0xb5, 0x06, 0x3f,
	0x6c, 0xd8, 0xa9, 0xf7, 0x11, 0x23, 0xe4, 0x77, 0xd1, 0x18, 0xc9, 0x25, 0x6c, 0x35, 0x26, 0x9f,
	0x1c, 0x94, 0x2d, 0xbf, 0xf8, 0x0d, 0xf1, 0x0f, 0x97, 0xb0, 0xe6, 0xb9, 0x58, 0x21, 0x23, 0xd8,
	0x6e, 0xce, 0x22, 0x31, 0x49, 0x4b, 0xe6, 0xda, 0x7f, 0xb6, 0x8c, 0xae, 0x8b, 0x7e, 0x84, 0x27,
	0x0b, 0x47, 0x8d, 0x04, 0x65, 0xea, 0xdf, 0xe6, 0xd0, 0xdf, 0x6d, 0xcc, 0xb0, 0x9e, 0xbc, 0x60,
	0xe5, 0x95, 0x75, 0xbd, 0xaa, 0xdf, 0xcd, 0x37, 0xbf, 0x02, 0x00, 0x00, 0xff, 0xff, 0xb3, 0x3f,
	0xe6, 0x53, 0x4c, 0x05, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConnInterface

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion6

// PartitionsServiceClient is the client API for PartitionsService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type PartitionsServiceClient interface {
	AssignPartition(ctx context.Context, in *MAssignPartitionRequest, opts ...grpc.CallOption) (*MAssignPartitionResponse, error)
	ShufflePartition(ctx context.Context, in *MShufflePartitionRequest, opts ...grpc.CallOption) (*MShufflePartitionResponse, error)
	TransferPartitionData(ctx context.Context, in *MTransferPartitionDataRequest, opts ...grpc.CallOption) (PartitionsService_TransferPartitionDataClient, error)
}

type partitionsServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewPartitionsServiceClient(cc grpc.ClientConnInterface) PartitionsServiceClient {
	return &partitionsServiceClient{cc}
}

func (c *partitionsServiceClient) AssignPartition(ctx context.Context, in *MAssignPartitionRequest, opts ...grpc.CallOption) (*MAssignPartitionResponse, error) {
	out := new(MAssignPartitionResponse)
	err := c.cc.Invoke(ctx, "/rpc.PartitionsService/AssignPartition", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *partitionsServiceClient) ShufflePartition(ctx context.Context, in *MShufflePartitionRequest, opts ...grpc.CallOption) (*MShufflePartitionResponse, error) {
	out := new(MShufflePartitionResponse)
	err := c.cc.Invoke(ctx, "/rpc.PartitionsService/ShufflePartition", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *partitionsServiceClient) TransferPartitionData(ctx context.Context, in *MTransferPartitionDataRequest, opts ...grpc.CallOption) (PartitionsService_TransferPartitionDataClient, error) {
	stream, err := c.cc.NewStream(ctx, &_PartitionsService_serviceDesc.Streams[0], "/rpc.PartitionsService/TransferPartitionData", opts...)
	if err != nil {
		return nil, err
	}
	x := &partitionsServiceTransferPartitionDataClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type PartitionsService_TransferPartitionDataClient interface {
	Recv() (*MPartitionChunk, error)
	grpc.ClientStream
}

type partitionsServiceTransferPartitionDataClient struct {
	grpc.ClientStream
}

func (x *partitionsServiceTransferPartitionDataClient) Recv() (*MPartitionChunk, error) {
	m := new(MPartitionChunk)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// PartitionsServiceServer is the server API for PartitionsService service.
type PartitionsServiceServer interface {
	AssignPartition(context.Context, *MAssignPartitionRequest) (*MAssignPartitionResponse, error)
	ShufflePartition(context.Context, *MShufflePartitionRequest) (*MShufflePartitionResponse, error)
	TransferPartitionData(*MTransferPartitionDataRequest, PartitionsService_TransferPartitionDataServer) error
}

// UnimplementedPartitionsServiceServer can be embedded to have forward compatible implementations.
type UnimplementedPartitionsServiceServer struct {
}

func (*UnimplementedPartitionsServiceServer) AssignPartition(ctx context.Context, req *MAssignPartitionRequest) (*MAssignPartitionResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AssignPartition not implemented")
}
func (*UnimplementedPartitionsServiceServer) ShufflePartition(ctx context.Context, req *MShufflePartitionRequest) (*MShufflePartitionResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ShufflePartition not implemented")
}
func (*UnimplementedPartitionsServiceServer) TransferPartitionData(req *MTransferPartitionDataRequest, srv PartitionsService_TransferPartitionDataServer) error {
	return status.Errorf(codes.Unimplemented, "method TransferPartitionData not implemented")
}

func RegisterPartitionsServiceServer(s *grpc.Server, srv PartitionsServiceServer) {
	s.RegisterService(&_PartitionsService_serviceDesc, srv)
}

func _PartitionsService_AssignPartition_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(MAssignPartitionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PartitionsServiceServer).AssignPartition(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/rpc.PartitionsService/AssignPartition",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PartitionsServiceServer).AssignPartition(ctx, req.(*MAssignPartitionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _PartitionsService_ShufflePartition_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(MShufflePartitionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PartitionsServiceServer).ShufflePartition(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/rpc.PartitionsService/ShufflePartition",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PartitionsServiceServer).ShufflePartition(ctx, req.(*MShufflePartitionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _PartitionsService_TransferPartitionData_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(MTransferPartitionDataRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(PartitionsServiceServer).TransferPartitionData(m, &partitionsServiceTransferPartitionDataServer{stream})
}

type PartitionsService_TransferPartitionDataServer interface {
	Send(*MPartitionChunk) error
	grpc.ServerStream
}

type partitionsServiceTransferPartitionDataServer struct {
	grpc.ServerStream
}

func (x *partitionsServiceTransferPartitionDataServer) Send(m *MPartitionChunk) error {
	return x.ServerStream.SendMsg(m)
}

var _PartitionsService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "rpc.PartitionsService",
	HandlerType: (*PartitionsServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "AssignPartition",
			Handler:    _PartitionsService_AssignPartition_Handler,
		},
		{
			MethodName: "ShufflePartition",
			Handler:    _PartitionsService_ShufflePartition_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "TransferPartitionData",
			Handler:       _PartitionsService_TransferPartitionData_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "s_partition.proto",
}
