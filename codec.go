package hurricache

import (
	"bytes"
	"compress/gzip"
	"fmt"
	pb "github.com/hurricache/hurricache-go-client/proto/cachepb"
	"github.com/pierrec/lz4/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
)

func copyPtr[T any](p *T) *T {
	if p == nil {
		return nil
	}
	return Ptr(*p)
}
func hintPB(h *KeyHint) *pb.KeyHint {
	if h == nil {
		return nil
	}
	return &pb.KeyHint{WeakHash: copyPtr(h.WeakHash), StrongHash: copyPtr(h.StrongHash)}
}
func hintGo(h *pb.KeyHint) *KeyHint {
	if h == nil {
		return nil
	}
	return &KeyHint{WeakHash: copyPtr(h.WeakHash), StrongHash: copyPtr(h.StrongHash)}
}
func compressionGo(c *pb.CompressedInfo) *CompressionInfo {
	if c == nil {
		return nil
	}
	return &CompressionInfo{c.Enabled, copyPtr(c.RawSize)}
}
func lockGo(l *pb.LockInfo) *LockInfo {
	if l == nil {
		return nil
	}
	return &LockInfo{l.Type, copyPtr(l.LockedBy), copyPtr(l.LockedTill)}
}
func lockPB(l *LockInfo) *pb.LockInfo {
	if l == nil {
		return nil
	}
	return &pb.LockInfo{Type: l.Type, LockedBy: copyPtr(l.LockedBy), LockedTill: copyPtr(l.LockedTillMillis)}
}

func encode(data []byte, compress bool, c Config) ([]byte, *pb.CompressedInfo, error) {
	if len(data) > c.MaxDecodedBytes {
		return nil, nil, status.Error(codes.ResourceExhausted, "payload exceeds MaxDecodedBytes")
	}
	threshold := 1024
	if c.CompressionThreshold != nil {
		threshold = *c.CompressionThreshold
	}
	if !compress || len(data) <= threshold {
		return bytes.Clone(data), nil, nil
	}
	info := &pb.CompressedInfo{Enabled: true, RawSize: Ptr(uint32(len(data)))}
	if c.CompressionCodec == CompressionLZ4 {
		// A full block bound makes incompressible input a valid literal block,
		// rather than the zero-length result used with undersized destinations.
		dst := make([]byte, lz4.CompressBlockBound(len(data)))
		var compressor lz4.Compressor
		n, err := compressor.CompressBlock(data, dst)
		if err != nil {
			return nil, nil, err
		}
		if n == 0 {
			return nil, nil, status.Error(codes.Internal, "LZ4 block compressor returned no data")
		}
		return dst[:n], info, nil
	}
	var dst bytes.Buffer
	w := gzip.NewWriter(&dst)
	if _, err := w.Write(data); err != nil {
		return nil, nil, err
	}
	if err := w.Close(); err != nil {
		return nil, nil, err
	}
	return dst.Bytes(), info, nil
}
func decode(data []byte, size uint32, info *pb.CompressedInfo, c Config) ([]byte, error) {
	if uint64(len(data)) != uint64(size) {
		return nil, status.Error(codes.DataLoss, "payload size metadata does not match bytes")
	}
	if info == nil || !info.Enabled {
		if len(data) > c.MaxDecodedBytes {
			return nil, status.Error(codes.ResourceExhausted, "payload exceeds MaxDecodedBytes")
		}
		return append([]byte{}, data...), nil
	}
	if info.RawSize == nil || *info.RawSize == 0 {
		return nil, status.Error(codes.DataLoss, "compressed payload lacks a positive raw size")
	}
	if uint64(*info.RawSize) > uint64(c.MaxDecodedBytes) {
		return nil, status.Error(codes.ResourceExhausted, "decoded payload exceeds MaxDecodedBytes")
	}
	if c.CompressionCodec == CompressionLZ4 {
		dst := make([]byte, int(*info.RawSize))
		n, err := lz4.UncompressBlock(data, dst)
		if err != nil || n != len(dst) {
			return nil, status.Error(codes.DataLoss, fmt.Sprintf("invalid LZ4 payload: decoded %d of %d bytes (%v)", n, len(dst), err))
		}
		return dst, nil
	}
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, status.Error(codes.DataLoss, fmt.Sprintf("invalid GZIP header: %v", err))
	}
	defer r.Close()
	dst, err := io.ReadAll(io.LimitReader(r, int64(*info.RawSize)+1))
	if err != nil || uint64(len(dst)) != uint64(*info.RawSize) {
		return nil, status.Error(codes.DataLoss, fmt.Sprintf("invalid GZIP payload: decoded %d of %d bytes (%v)", len(dst), *info.RawSize, err))
	}
	return dst, nil
}
func encodeKey(data []byte, h *KeyHint, id *uint32, c Config) (*pb.Key, error) {
	return encodeKeyPolicy(data, h, id, c, true)
}
func encodeKeyPolicy(data []byte, h *KeyHint, id *uint32, c Config, compress bool) (*pb.Key, error) {
	b, ci, e := encode(data, compress, c)
	if e != nil {
		return nil, e
	}
	return &pb.Key{Payload: &pb.KeyBinaryPayload{Payload: b, Size: uint32(len(b))}, KeyHint: hintPB(h), ClientId: copyPtr(id), CompressionInfo: ci}, nil
}

func encodeValue(p Variant, ttl *uint64, c Config) (*pb.Value, error) {
	return encodeValuePolicy(p, ttl, c, true)
}
func encodeValuePolicy(p Variant, ttl *uint64, c Config, compress bool) (*pb.Value, error) {
	v := &pb.Value{Ttl: copyPtr(ttl)}
	switch p.Type() {
	case NoneType:
		v.ValueType = Ptr(pb.ValueType_AUTO)
	case BytesType:
		b, ci, e := encode([]byte(p.data), compress, c)
		if e != nil {
			return nil, e
		}
		v.ValueType = Ptr(pb.ValueType_BYTES)
		v.Value = &pb.BinaryPayload{Data: &pb.BinaryPayload_Payload{Payload: b}, Size: uint32(len(b))}
		v.CompressionInfo = ci
	case IntType:
		v.ValueType = Ptr(pb.ValueType_INT)
		v.Value = &pb.BinaryPayload{Data: &pb.BinaryPayload_IntPayload{IntPayload: int64(p.bits)}}
	case DoubleType:
		value, _ := p.Float64()
		v.ValueType = Ptr(pb.ValueType_DOUBLE)
		v.Value = &pb.BinaryPayload{Data: &pb.BinaryPayload_DoublePayload{DoublePayload: value}}
	default:
		return nil, status.Error(codes.InvalidArgument, "unknown Variant type")
	}
	return v, nil
}
func encodeOrdered(p OrderedVariant, ttl *uint64, c Config) (*pb.OrderedValue, error) {
	v, e := encodeValue(p.Value(), ttl, c)
	if e != nil {
		return nil, e
	}
	return &pb.OrderedValue{Value: v.Value, ValueType: v.ValueType, Order: Ptr(p.order), CompressionInfo: v.CompressionInfo, Ttl: v.Ttl}, nil
}
func decodeValue(v *pb.Value, c Config) (*Variant, error) {
	if v == nil {
		return nil, nil
	}
	typ := v.GetValueType()
	var data any
	if v.Value != nil {
		data = v.Value.Data
	}
	if typ < pb.ValueType_AUTO || typ > pb.ValueType_DOUBLE {
		return nil, status.Error(codes.DataLoss, "unknown value_type")
	}
	if v.CompressionInfo.GetEnabled() {
		switch data.(type) {
		case *pb.BinaryPayload_Payload, *pb.BinaryPayload_NonCordPayload:
		default:
			return nil, status.Error(codes.DataLoss, "compression on a non-byte value")
		}
	}
	var result Variant
	switch p := data.(type) {
	case nil:
		if typ != pb.ValueType_AUTO {
			return nil, status.Error(codes.DataLoss, "typed value lacks oneof payload")
		}
		result = None()
	case *pb.BinaryPayload_Payload:
		if typ != pb.ValueType_AUTO && typ != pb.ValueType_BYTES {
			return nil, status.Error(codes.DataLoss, "value_type conflicts with byte payload")
		}
		b, e := decode(p.Payload, v.Value.Size, v.CompressionInfo, c)
		if e != nil {
			return nil, e
		}
		result = Bytes(b)
	case *pb.BinaryPayload_NonCordPayload:
		if typ != pb.ValueType_AUTO && typ != pb.ValueType_BYTES {
			return nil, status.Error(codes.DataLoss, "value_type conflicts with byte payload")
		}
		b, e := decode(p.NonCordPayload, v.Value.Size, v.CompressionInfo, c)
		if e != nil {
			return nil, e
		}
		result = Bytes(b)
	case *pb.BinaryPayload_IntPayload:
		if typ != pb.ValueType_AUTO && typ != pb.ValueType_INT {
			return nil, status.Error(codes.DataLoss, "value_type conflicts with integer payload")
		}
		result = Int(p.IntPayload)
	case *pb.BinaryPayload_DoublePayload:
		if typ != pb.ValueType_AUTO && typ != pb.ValueType_DOUBLE {
			return nil, status.Error(codes.DataLoss, "value_type conflicts with double payload")
		}
		result = Double(p.DoublePayload)
	default:
		return nil, status.Error(codes.DataLoss, "unknown value payload")
	}
	return &result, nil
}
func decodeOrdered(v *pb.OrderedValue, c Config) (*OrderedVariant, error) {
	if v == nil {
		return nil, nil
	}
	p, e := decodeValue(&pb.Value{Value: v.Value, ValueType: v.ValueType, CompressionInfo: v.CompressionInfo, Ttl: v.Ttl, LockInfo: v.LockInfo}, c)
	if e != nil {
		return nil, e
	}
	result := Ordered(*p, v.Order)
	return &result, nil
}
func decodeResult(v *pb.ValueResponse, c Config) (ValueResult, error) {
	if v == nil {
		return ValueResult{}, status.Error(codes.DataLoss, "nil value response")
	}
	r := ValueResult{Hint: hintGo(v.KeyHint)}
	if v.ValueUnordered != nil && v.ValueOrdered != nil {
		return r, status.Error(codes.DataLoss, "mixed scalar response representations")
	}
	var e error
	r.Value, e = decodeValue(v.ValueUnordered, c)
	if e != nil {
		return r, e
	}
	r.Ordered, e = decodeOrdered(v.ValueOrdered, c)
	if p := v.ValueUnordered; p != nil {
		r.ExpiresAtMillis = copyPtr(p.Ttl)
		r.Lock = lockGo(p.LockInfo)
		r.Compression = compressionGo(p.CompressionInfo)
	}
	return r, e
}
func variantResult(v *pb.ValueResponse, c Config) (Variant, error) {
	r, e := decodeResult(v, c)
	if e != nil {
		return None(), e
	}
	if r.Value != nil {
		return *r.Value, nil
	}
	if r.Ordered != nil {
		return r.Ordered.Value(), nil
	}
	return None(), nil
}
func hintResponse(v *pb.KeyHintResponse) *KeyHint {
	h := hintGo(v.KeyHint)
	if h == nil {
		h = &KeyHint{}
	}
	h.WasRelayed = v.GetRelayed()
	return h
}
