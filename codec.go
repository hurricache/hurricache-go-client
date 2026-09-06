package hurricache

import (
	"bytes"
	"fmt"
	pb "github.com/hurricache/hurricache-go-client/proto/cachepb"
	"github.com/pierrec/lz4/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	return &pb.KeyHint{WeekHash: copyPtr(h.WeakHash), StrongHash: copyPtr(h.StrongHash)}
}
func hintGo(h *pb.KeyHint) *KeyHint {
	if h == nil {
		return nil
	}
	return &KeyHint{copyPtr(h.WeekHash), copyPtr(h.StrongHash)}
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
	if !compress || len(data) <= 1024 {
		return bytes.Clone(data), nil, nil
	}
	dst := make([]byte, lz4.CompressBlockBound(len(data)))
	n, err := lz4.CompressBlock(data, dst, nil)
	if err != nil {
		return nil, nil, err
	}
	if n == 0 {
		// A literal-only raw block is required when the compressor reports incompressible
		// input. This keeps Java's enabled compression metadata above the threshold.
		dst = dst[:0]
		length := len(data)
		dst = append(dst, 0xf0)
		length -= 15
		for length >= 255 {
			dst = append(dst, 255)
			length -= 255
		}
		dst = append(dst, byte(length))
		dst = append(dst, data...)
		n = len(dst)
	}
	return dst[:n], &pb.CompressedInfo{Enabled: true, RawSize: Ptr(uint32(len(data)))}, nil
}
func decode(data []byte, size uint32, info *pb.CompressedInfo, c Config) ([]byte, error) {
	if uint64(len(data)) != uint64(size) {
		return nil, status.Error(codes.DataLoss, "payload size metadata does not match bytes")
	}
	if info == nil || !info.Enabled {
		return append([]byte{}, data...), nil
	}
	if info.RawSize == nil || *info.RawSize == 0 {
		return nil, status.Error(codes.DataLoss, "compressed payload lacks a positive raw size")
	}
	if uint64(*info.RawSize) > uint64(c.MaxDecodedBytes) {
		return nil, status.Error(codes.ResourceExhausted, "decoded payload exceeds MaxDecodedBytes")
	}
	dst := make([]byte, int(*info.RawSize))
	n, err := lz4.UncompressBlock(data, dst)
	if err != nil || n != len(dst) {
		return nil, status.Error(codes.DataLoss, fmt.Sprintf("invalid LZ4 block: decoded %d of %d bytes (%v)", n, len(dst), err))
	}
	return dst, nil
}
func encodeKey(data []byte, h *KeyHint, id *uint32, c Config) (*pb.Key, error) {
	b, ci, e := encode(data, true, c)
	if e != nil {
		return nil, e
	}
	return &pb.Key{Payload: &pb.KeyBinaryPayload{Payload: b, Size: uint32(len(b))}, KeyHint: hintPB(h), ClientId: copyPtr(id), CompressionInfo: ci}, nil
}
func encodeValue(p Payload, ttl *uint64, c Config) (*pb.Value, error) {
	b, ci, e := encode(p.Data, true, c)
	if e != nil {
		return nil, e
	}
	if p.ExpiresAtMillis != nil {
		ttl = p.ExpiresAtMillis
	}
	return &pb.Value{Value: &pb.BinaryPayload{Payload: b, Size: uint32(len(b))}, CompressionInfo: ci, Ttl: copyPtr(ttl), LockInfo: lockPB(p.Lock)}, nil
}
func encodeOrdered(p OrderedPayload, ttl *uint64, c Config) (*pb.OrderedValue, error) {
	b, _, e := encode(p.Data, false, c)
	if e != nil {
		return nil, e
	}
	if p.ExpiresAtMillis != nil {
		ttl = p.ExpiresAtMillis
	}
	order := p.Order
	if order == nil {
		order = Ptr(uint64(0))
	}
	return &pb.OrderedValue{Value: &pb.BinaryPayload{Payload: b, Size: uint32(len(b))}, Order: copyPtr(order), Ttl: copyPtr(ttl), LockInfo: lockPB(p.Lock)}, nil
}
func decodeValue(v *pb.Value, c Config) (*Payload, error) {
	if v == nil {
		return nil, nil
	}
	if v.Value == nil {
		return nil, status.Error(codes.DataLoss, "present value lacks binary payload")
	}
	b, e := decode(v.Value.Payload, v.Value.Size, v.CompressionInfo, c)
	if e != nil {
		return nil, e
	}
	return &Payload{Data: b, ExpiresAtMillis: copyPtr(v.Ttl), Lock: lockGo(v.LockInfo), Compression: compressionGo(v.CompressionInfo)}, nil
}
func decodeOrdered(v *pb.OrderedValue, c Config) (*OrderedPayload, error) {
	if v == nil {
		return nil, nil
	}
	p, e := decodeValue(&pb.Value{Value: v.Value, CompressionInfo: v.CompressionInfo, Ttl: v.Ttl, LockInfo: v.LockInfo}, c)
	if e != nil {
		return nil, e
	}
	return &OrderedPayload{*p, copyPtr(v.Order)}, nil
}
func decodeResult(v *pb.ValueResponse, c Config) (ValueResult, error) {
	r := ValueResult{Hint: hintGo(v.KeyHint)}
	var e error
	r.Value, e = decodeValue(v.ValueUnordered, c)
	if e != nil {
		return r, e
	}
	r.Ordered, e = decodeOrdered(v.ValueOrdered, c)
	return r, e
}
func decodeUpdate(v *pb.UpdateValueResponse, c Config) (UpdateResult, error) {
	p, e := decodeValue(v.Value, c)
	return UpdateResult{v.Result, p}, e
}
func atomicGo(v *pb.AtomicValue) AtomicResult { return AtomicResult{v.Val, hintGo(v.KeyHint)} }
