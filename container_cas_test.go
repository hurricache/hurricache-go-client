package hurricache

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	pb "github.com/hurricache/hurricache-go-client/proto/cachepb"
	"google.golang.org/protobuf/proto"
)

func TestContainerCASAcrossClients(t *testing.T) {
	for codecName, codec := range map[string]CompressionCodec{"gzip": CompressionGZIP, "lz4": CompressionLZ4} {
		t.Run(codecName, func(t *testing.T) {
			config, err := normalizeConfig(Config{CompressionCodec: codec, CompressionThreshold: Ptr(8), DefaultTTL: time.Minute})
			if err != nil {
				t.Fatal(err)
			}
			var wantSelector any
			var wantDouble bool
			handler := func(_ context.Context, method string, message proto.Message) (any, error) {
				if !strings.HasSuffix(method, "/atomicCompareAndSet") {
					t.Fatal(method)
				}
				r := message.(*pb.AtomicCas)
				if r.Key.GetClientId() != 7 || r.Expected.Ttl != nil || r.ToSet.Ttl != nil {
					t.Fatal("owner or TTL", r)
				}
				var key *pb.Key
				switch k := r.Keys.(type) {
				case *pb.AtomicCas_ElementKey:
					key = k.ElementKey
				case *pb.AtomicCas_OrderedElementKey:
					if k.OrderedElementKey.Order == nil {
						t.Fatal("order presence lost")
					}
					key = &pb.Key{Payload: k.OrderedElementKey.Payload, CompressionInfo: k.OrderedElementKey.CompressionInfo, ClientId: k.OrderedElementKey.ClientId}
				default:
					t.Fatal("missing selector")
				}
				var want []byte
				switch v := wantSelector.(type) {
				case []byte:
					want = v
				case string:
					want = []byte(v)
				case OrderedPayload:
					want = v.Data
					if r.GetOrderedElementKey() == nil || r.GetOrderedElementKey().GetOrder() != *v.Order {
						t.Fatal("exact order lost", r)
					}
				}
				decoded, e := decode(key.Payload.Payload, key.Payload.Size, key.CompressionInfo, config)
				if e != nil || string(decoded) != string(want) {
					t.Fatal("selector data", decoded, e)
				}
				if key.GetClientId() != 7 || key.CompressionInfo.GetEnabled() != (len(want) > 8) {
					t.Fatal("selector owner/compression", key)
				}
				if (r.Expected.GetValueType() == pb.ValueType_DOUBLE) != wantDouble {
					t.Fatal("numeric promotion", r)
				}
				if r.Expected.GetValueType() != r.ToSet.GetValueType() || r.Expected.CompressionInfo != nil || r.ToSet.CompressionInfo != nil {
					t.Fatal("numeric types/compression", r)
				}
				return &pb.AtomicCasRes{Result: false, Expected: &pb.ValueResponse{ValueUnordered: &pb.Value{ValueType: Ptr(pb.ValueType_DOUBLE), Value: &pb.BinaryPayload{Data: &pb.BinaryPayload_DoublePayload{DoublePayload: 1.25}}}, KeyHint: &pb.KeyHint{WeakHash: Ptr(uint32(0)), StrongHash: Ptr(uint32(2))}}}, nil
			}
			for form, c := range currentClients(t, config, handler, nil) {
				t.Run(form, func(t *testing.T) {
					for _, selector := range []any{[]byte{}, "field", []byte(strings.Repeat("field", 100)), OrderedPayload{Payload: Payload{Data: []byte(strings.Repeat("field", 100))}, Order: Ptr(uint64(0))}, OrderedPayload{Payload: Payload{Data: []byte("field")}, Order: Ptr(uint64(math.MaxUint64))}} {
						for _, numbers := range [][2]any{{int64(math.MinInt64), int64(math.MaxInt64)}, {int64(1), 2.5}} {
							wantSelector = selector
							_, wantDouble = numbers[1].(float64)
							got, e := c.CompareAndSetInContainer(context.Background(), []byte("map"), selector, numbers[0], numbers[1], Options{ClientID: Ptr(uint32(7))})
							if e != nil || got.Swapped || got.Expected == nil || *got.Expected != Double(1.25) || got.Hint == nil || got.Hint.WeakHash == nil || *got.Hint.WeakHash != 0 {
								t.Fatal(got, e)
							}
						}
					}
				})
			}
		})
	}
}

func TestContainerCASRejectsInvalidArguments(t *testing.T) {
	calls := 0
	handler := func(_ context.Context, _ string, _ proto.Message) (any, error) {
		calls++
		return &pb.AtomicCasRes{}, nil
	}
	for form, c := range currentClients(t, Config{}, handler, nil) {
		t.Run(form, func(t *testing.T) {
			for _, selector := range []any{nil, 0, true, OrderedPayload{Payload: Payload{Data: []byte("k")}}} {
				if _, e := c.CompareAndSetInContainer(context.Background(), []byte("map"), selector, int64(1), int64(2), Options{}); e == nil {
					t.Fatal("accepted invalid selector", selector)
				}
			}
			for _, number := range []any{true, uint64(math.MaxUint64), "1"} {
				if _, e := c.CompareAndSetInContainer(context.Background(), []byte("map"), "k", number, int64(2), Options{}); e == nil {
					t.Fatal("accepted invalid numeric", number)
				}
			}
		})
	}
	if calls != 0 {
		t.Fatal("invalid inputs executed RPC", calls)
	}
}
