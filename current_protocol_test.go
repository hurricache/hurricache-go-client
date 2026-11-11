package hurricache

import (
	"context"
	pb "github.com/hurricache/hurricache-go-client/proto/cachepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestContainerSelectorOneofAndNumericTTL(t *testing.T) {
	for _, codec := range []CompressionCodec{CompressionGZIP, CompressionLZ4} {
		config, _ := normalizeConfig(Config{ClientID: 17, CompressionCodec: codec})
		var selector any
		conn := transport(t, func(_ context.Context, _ string, req proto.Message) (any, error) {
			r := req.(*pb.CreateRequest)
			if r.Key.ClientId == nil || r.Key.GetClientId() != 0 || r.ContainerKey == nil || r.Value.Ttl == nil {
				t.Error("selector/CID/TTL presence")
			}
			switch want := selector.(type) {
			case uint64:
				p, ok := r.ContainerKey.Key.(*pb.ContainerKey_Pos)
				if !ok || p.Pos != want {
					t.Error("position oneof, including explicit zero")
				}
			case string:
				k, ok := r.ContainerKey.Key.(*pb.ContainerKey_ContainerKey)
				if !ok {
					t.Error("key oneof")
					break
				}
				decoded, e := decodeKey(k.ContainerKey, config)
				if e != nil || string(decoded.Data) != want || decoded.ClientID == nil || *decoded.ClientID != 0 {
					t.Error("compressed byte selector / owner", e)
				}
				if len(want) > 1024 && k.ContainerKey.CompressionInfo == nil {
					t.Error("selector key compression")
				}
			}
			return &pb.ValueResponse{ValueUnordered: r.Value}, nil
		}, nil)
		c, e := NewClientWithConnection(conn, config)
		if e != nil {
			t.Fatal(e)
		}
		for _, want := range []any{uint64(0), uint64(math.MaxUint64), "", strings.Repeat("key", 1024)} {
			selector = want
			n, e := c.ExchangeInContainer(context.Background(), []byte("k"), want, 1.25, Options{ClientID: Ptr(uint32(0)), TTL: Ptr(time.Minute)})
			if e != nil || n != float64(1.25) {
				t.Fatal(n, e)
			}
		}
		_ = c.Close()
	}
}

func TestCASOptionalObservedPresenceAndType(t *testing.T) {
	config, _ := normalizeConfig(Config{})
	observedCases := []*pb.ValueResponse{nil, {}, {ValueUnordered: &pb.Value{ValueType: Ptr(pb.ValueType_AUTO)}}, {ValueUnordered: &pb.Value{ValueType: Ptr(pb.ValueType_INT), Value: &pb.BinaryPayload{Data: &pb.BinaryPayload_IntPayload{IntPayload: 0}}}, KeyHint: &pb.KeyHint{WeakHash: Ptr(uint32(0))}}, {ValueOrdered: &pb.OrderedValue{ValueType: Ptr(pb.ValueType_DOUBLE), Value: &pb.BinaryPayload{Data: &pb.BinaryPayload_DoublePayload{DoublePayload: math.Copysign(0, -1)}}}}}
	for i, observed := range observedCases {
		t.Run(string(rune('A'+i)), func(t *testing.T) {
			conn := transport(t, func(_ context.Context, _ string, _ proto.Message) (any, error) {
				return &pb.AtomicCasRes{Expected: observed}, nil
			}, nil)
			c, _ := NewClientWithConnection(conn, config)
			defer c.Close()
			got, e := c.CompareAndSet(context.Background(), []byte("k"), int64(0), int64(1), Options{})
			if e != nil {
				t.Fatal(e)
			}
			var want *Variant
			switch i {
			case 2:
				want = Ptr(None())
			case 3:
				want = Ptr(Int(0))
			case 4:
				want = Ptr(Double(math.Copysign(0, -1)))
			}
			if !reflect.DeepEqual(got.Expected, want) {
				t.Fatalf("optional observed value %v want %v", got.Expected, want)
			}
			if i == 3 && (got.Hint == nil || got.Hint.WeakHash == nil || *got.Hint.WeakHash != 0) {
				t.Fatal("nested optional hint")
			}
		})
	}
}

func TestCurrentTTLAndPositionPresence(t *testing.T) {
	var request proto.Message
	conn := transport(t, func(_ context.Context, method string, r proto.Message) (any, error) {
		request = r
		if strings.HasSuffix(method, "getTtl") {
			return &pb.TtlResponse{}, nil
		}
		return response(method), nil
	}, nil)
	c, _ := NewClientWithConnection(conn, Config{})
	defer c.Close()
	ctx := context.Background()
	for _, ttl := range []time.Duration{-1, 0, time.Minute} {
		if _, e := c.SetTTL(ctx, []byte("k"), ttl, Options{}); e != nil {
			t.Fatal(e)
		}
		r := request.(*pb.TtlRequest)
		if r.Ttl == nil {
			t.Fatal("TTL field presence")
		}
		if ttl < 0 {
			if *r.Ttl != math.MaxUint64 {
				t.Fatal("clear expiration sentinel")
			}
		} else if *r.Ttl < uint64(time.Now().Add(ttl-time.Second).UnixMilli()) || *r.Ttl > uint64(time.Now().Add(ttl+time.Second).UnixMilli()) {
			t.Fatal("absolute milliseconds")
		}
	}
	n, e := c.GetTTL(ctx, []byte("k"), Options{})
	if e != nil || n != -1 {
		t.Fatal("absent TTL sentinel", n, e)
	}
	for _, end := range []uint64{0, 1, 2} {
		if _, e = c.RemoveAt(ctx, []byte("k"), Position{Start: 1, End: &end}, Options{}); e != nil {
			t.Fatal(e)
		}
		r := request.(*pb.KeyPositionRequest)
		if r.Pos == nil || *r.Pos != 1 || (r.End != nil) != (end > 1) {
			t.Fatal("Java removal end presence")
		}
	}
	if _, e = c.Get(ctx, []byte("k"), Options{TTL: Ptr(-time.Second)}); status.Code(e) != codes.InvalidArgument {
		t.Fatal("invalid creation options")
	}
}
