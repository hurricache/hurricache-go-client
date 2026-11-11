package hurricache

import (
	"bytes"
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/hurricache/hurricache-go-client/proto/cachepb"
	cpb "github.com/hurricache/hurricache-go-client/proto/coordinatorpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestVariantSemantics(t *testing.T) {
	key := Payload{Data: []byte("key"), ClientID: Ptr(uint32(7))}
	if !key.Equal(Payload{Data: []byte("key")}) || (Payload{}).Equal(Payload{Data: []byte{}}) {
		t.Fatal("byte key equality / metadata")
	}
	if (OrderedPayload{Payload: key}).Equal(OrderedPayload{Payload: key, Order: Ptr(uint64(0))}) {
		t.Fatal("ordered key optional order equality")
	}
	input := []byte("abc")
	v := Bytes(input)
	input[0] = 'x'
	out, e := v.Bytes()
	if e != nil {
		t.Fatal(e)
	}
	out[1] = 'x'
	if v != String("abc") {
		t.Fatal("bytes alias caller")
	}
	values := []Variant{None(), Bytes(nil), Int(0), Double(0), Double(math.Copysign(0, -1)), Int(1), Double(1)}
	set := map[Variant]bool{}
	for _, value := range values {
		set[value] = true
	}
	if len(set) != len(values) {
		t.Fatal("type or signed-zero equality lost")
	}
	if Double(math.NaN()) != Double(math.Float64frombits(0xfff0000000000001)) {
		t.Fatal("Java NaN equality")
	}
	if _, e := None().Bytes(); e == nil {
		t.Fatal("unchecked bytes accessor")
	}
	if _, e := Double(1).Int64(); e == nil {
		t.Fatal("unchecked integer accessor")
	}
	if _, e := Int(1).Float64(); e == nil {
		t.Fatal("unchecked double accessor")
	}
	if _, e := None().Number(); e == nil {
		t.Fatal("unchecked number accessor")
	}
	order := uint64(7)
	ov := Ordered(v, &order)
	order = 9
	*ov.Order() = 11
	if *ov.Order() != 7 || ov.Value() != v {
		t.Fatal("order aliases caller")
	}
	if Ordered(v, nil) == Ordered(v, Ptr(uint64(0))) {
		t.Fatal("absent order equals zero")
	}
	m := map[OrderedVariant]int{ov: 1}
	if m[Ordered(v, Ptr(uint64(7)))] != 1 {
		t.Fatal("ordered map key")
	}
	h := &KeyHint{WeakHash: Ptr(uint32(0))}
	if !h.Equal(&KeyHint{WeakHash: Ptr(uint32(0)), WasRelayed: true}) || h.Equal(&KeyHint{}) {
		t.Fatal("hint equality must use only present hashes")
	}
}

func TestTypedCodecPresence(t *testing.T) {
	config, _ := normalizeConfig(Config{})
	if _, e := decodeBatch(&pb.BatchValueResponse{ValueUnordered: []*pb.Value{{}}, ValueOrdered: []*pb.OrderedValue{{}}}, config); status.Code(e) != codes.DataLoss {
		t.Fatal("mixed collection representations accepted")
	}
	for name, codec := range map[string]CompressionCodec{"gzip": CompressionGZIP, "lz4": CompressionLZ4} {
		t.Run(name, func(t *testing.T) {
			c, _ := normalizeConfig(Config{CompressionThreshold: Ptr(0), CompressionCodec: codec})
			for _, v := range mixedFixture() {
				wire, e := encodeValue(v, nil, c)
				if e != nil {
					t.Fatal(e)
				}
				got, e := decodeValue(wire, c)
				if e != nil || got == nil || *got != v {
					t.Fatalf("typed roundtrip %v %v", v, e)
				}
				if v.IsNumber() && (wire.CompressionInfo != nil || wire.Value.Size != 0) {
					t.Fatal("numeric value entered byte compression/size path")
				}
				if v.IsNone() && (wire.Value != nil || wire.GetValueType() != pb.ValueType_AUTO) {
					t.Fatal("NONE presence")
				}
				if v.IsBytes() {
					if _, ok := wire.Value.Data.(*pb.BinaryPayload_Payload); !ok {
						t.Fatal("empty byte oneof absent")
					}
					wire.Value.Data = &pb.BinaryPayload_NonCordPayload{NonCordPayload: wire.Value.GetPayload()}
					got, e = decodeValue(wire, c)
					if e != nil || *got != v {
						t.Fatal("non-CORD bytes")
					}
				}
				ordered, e := encodeOrdered(Ordered(v, nil), nil, c)
				if e != nil || ordered.Order == nil || *ordered.Order != 0 {
					t.Fatal("write missing order default")
				}
				ordered.Order = nil
				decoded, e := decodeOrdered(ordered, c)
				if e != nil || decoded.Order() != nil || decoded.Value() != v {
					t.Fatal("optional read order")
				}
			}
			for _, v := range []*pb.Value{
				{ValueType: Ptr(pb.ValueType_INT)},
				{ValueType: Ptr(pb.ValueType_DOUBLE), Value: &pb.BinaryPayload{Data: &pb.BinaryPayload_IntPayload{IntPayload: 0}}},
				{ValueType: Ptr(pb.ValueType_BYTES), Value: &pb.BinaryPayload{Data: &pb.BinaryPayload_DoublePayload{DoublePayload: 0}}},
				{ValueType: Ptr(pb.ValueType(999))},
				{Value: &pb.BinaryPayload{Data: &pb.BinaryPayload_IntPayload{IntPayload: 0}}, CompressionInfo: &pb.CompressedInfo{Enabled: true, RawSize: Ptr(uint32(8))}},
			} {
				if _, e := decodeValue(v, c); status.Code(e) != codes.DataLoss {
					t.Fatalf("malformed typed value accepted: %v %v", v, e)
				}
			}
		})
	}
}

func currentClients(t *testing.T, config Config, handler func(context.Context, string, proto.Message) (any, error), stream func(proto.Message, grpc.ServerStreamingServer[pb.BatchValueResponse]) error) map[string]Operations {
	t.Helper()
	conn := transport(t, handler, stream)
	direct, e := NewClientWithConnection(conn, config)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = direct.Close() })
	coord := coordinatorTransport(t, func(g grpc.ServerStreamingServer[cpb.RoutingInfoData]) error { return g.Send(topologyData("m", "b")) })
	smart, e := NewSmartClient([]string{"c"}, SmartConfig{Client: config, Mode: Backup, ConnectionFactory: func(_ context.Context, target string) (Connection, error) {
		if target == "c" {
			return Connection{Conn: coord}, nil
		}
		return Connection{Conn: conn}, nil
	}})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = smart.Close() })
	waitReady(t, smart)
	return map[string]Operations{"direct": direct, "smart": smart}
}

func TestTypedScalarsAndCollectionsAcrossClients(t *testing.T) {
	for codecName, codec := range map[string]CompressionCodec{"gzip": CompressionGZIP, "lz4": CompressionLZ4} {
		t.Run(codecName, func(t *testing.T) {
			config, _ := normalizeConfig(Config{CompressionCodec: codec})
			var mu sync.Mutex
			stored := map[string]*pb.Value{}
			handler := func(_ context.Context, method string, req proto.Message) (any, error) {
				mu.Lock()
				defer mu.Unlock()
				switch r := req.(type) {
				case *pb.CreateRequest:
					if r.ContainerKey != nil || r.Value.LockInfo != nil {
						t.Error("scalar create field presence differs from Java")
					}
					stored[string(r.Key.Payload.Payload)] = proto.Clone(r.Value).(*pb.Value)
					return &pb.KeyHintResponse{Relayed: Ptr(true)}, nil
				case *pb.UpdateRequest:
					k := string(r.Key.Payload.Payload)
					previous := stored[k]
					stored[k] = r.Value
					return &pb.UpdateValueResponse{Result: true, Value: previous}, nil
				case *pb.GetRequest:
					k := string(r.Key.Payload.Payload)
					v := stored[k]
					if strings.HasSuffix(method, "getAndDeleteValue") {
						delete(stored, k)
					}
					return &pb.ValueResponse{ValueUnordered: v}, nil
				default:
					return response(method), nil
				}
			}
			stream := func(req proto.Message, g grpc.ServerStreamingServer[pb.BatchValueResponse]) error {
				for _, v := range mixedFixture() {
					wire, e := encodeValue(v, nil, config)
					if e != nil {
						return e
					}
					if e = g.Send(&pb.BatchValueResponse{ValueUnordered: []*pb.Value{wire}}); e != nil {
						return e
					}
				}
				return nil
			}
			for name, c := range currentClients(t, config, handler, stream) {
				t.Run(name, func(t *testing.T) {
					for i, v := range mixedFixture() {
						key := []byte(name + string(rune(i)))
						hint, e := c.Create(context.Background(), key, v, Options{})
						if e != nil || hint == nil || !hint.WasRelayed {
							t.Fatal("relay metadata", e)
						}
						got, e := c.Get(context.Background(), key, Options{Hint: hint})
						if e != nil || got != v {
							t.Fatal("get", got, e)
						}
						got, e = c.Replace(context.Background(), key, Int(42), Options{})
						if e != nil || got != v {
							t.Fatal("replace", got, e)
						}
						got, e = c.Pop(context.Background(), key, Options{})
						if e != nil || got != Int(42) {
							t.Fatal("pop", got, e)
						}
					}
					got, e := c.GetList(context.Background(), []byte("mixed"), Options{})
					if e != nil || !reflect.DeepEqual(got, mixedFixture()) {
						t.Fatal("mixed stream", got, e)
					}
				})
			}
		})
	}
}

func TestNumericAtomicsSelectorsAndCAS(t *testing.T) {
	var calls atomic.Int32
	for _, codec := range []CompressionCodec{CompressionGZIP, CompressionLZ4} {
		handler := func(_ context.Context, method string, req proto.Message) (any, error) {
			calls.Add(1)
			switch r := req.(type) {
			case *pb.CreateRequest:
				if r.Value.CompressionInfo != nil || r.Value.Value.Size != 0 {
					t.Error("numeric compression/size")
				}
				if r.Value.Ttl == nil {
					t.Error("atomic TTL")
				}
				return &pb.ValueResponse{ValueUnordered: r.Value}, nil
			case *pb.AtomicCas:
				if r.Expected.GetValueType() != pb.ValueType_DOUBLE || r.ToSet.GetValueType() != pb.ValueType_DOUBLE || r.Expected.Ttl != nil || r.ToSet.Ttl == nil {
					t.Error("CAS promotion/TTL")
				}
				return &pb.AtomicCasRes{Expected: &pb.ValueResponse{ValueUnordered: r.Expected, KeyHint: &pb.KeyHint{WeakHash: Ptr(uint32(0))}}}, nil
			}
			return response(method), nil
		}
		for name, c := range currentClients(t, Config{CompressionCodec: codec, CompressionThreshold: Ptr(0)}, handler, nil) {
			t.Run(fmtName(codec, name), func(t *testing.T) {
				o := Options{TTL: Ptr(time.Minute)}
				for _, operand := range []any{int64(math.MinInt64), int64(math.MaxInt64), float64(0), math.Copysign(0, -1), 1.25} {
					for _, method := range []string{"Exchange", "Add", "Sub"} {
						result := reflect.ValueOf(c).MethodByName(method).Call([]reflect.Value{reflect.ValueOf(context.Background()), reflect.ValueOf([]byte("k")), reflect.ValueOf(operand), reflect.ValueOf(o)})
						if !result[1].IsNil() {
							t.Fatal(result[1].Interface())
						}
						v, e := numeric(result[0].Interface())
						want, _ := numeric(operand)
						if e != nil || v != want {
							t.Fatal("numeric type/value", v, e)
						}
					}
					for _, selector := range []any{[]byte{}, []byte(strings.Repeat("key", 1024)), "entry", int(0), int64(1), uint64(math.MaxUint64)} {
						n, e := c.AddInContainer(context.Background(), []byte("k"), selector, operand, o)
						if e != nil {
							t.Fatal(e)
						}
						v, _ := numeric(n)
						want, _ := numeric(operand)
						if v != want {
							t.Fatal("container numeric type")
						}
					}
				}
				result, e := c.CompareAndSet(context.Background(), []byte("k"), int64(1), 1.5, o)
				if e != nil || result.Expected == nil || *result.Expected != Double(1) || result.Hint == nil || result.Hint.WeakHash == nil {
					t.Fatal("nested CAS", result, e)
				}
				before := calls.Load()
				for _, v := range []any{true, false, uint64(math.MaxUint64), "1", nil, float32(1)} {
					if _, e := c.Add(context.Background(), []byte("k"), v, o); status.Code(e) != codes.InvalidArgument {
						t.Fatal("invalid operand", v, e)
					}
				}
				for _, selector := range []any{true, false, -1, int64(-1), nil, 1.5} {
					if _, e := c.AddInContainer(context.Background(), []byte("k"), selector, int64(1), o); status.Code(e) != codes.InvalidArgument {
						t.Fatal("invalid selector", selector, e)
					}
				}
				if calls.Load() != before {
					t.Fatal("invalid input reached data RPC")
				}
			})
		}
	}
}

func fmtName(codec CompressionCodec, name string) string {
	if codec == CompressionGZIP {
		return "gzip/" + name
	}
	return "lz4/" + name
}

func TestOrderUpdateRequestsAndBounds(t *testing.T) {
	for _, codec := range []CompressionCodec{CompressionGZIP, CompressionLZ4} {
		config, _ := normalizeConfig(Config{CompressionCodec: codec, CompressionThreshold: Ptr(1)})
		for _, typ := range []ContainerType{OrderedMap, OrderedSet} {
			var mu sync.Mutex
			type member struct {
				data  string
				order uint64
			}
			entries := map[member]Variant{}
			keyBytes := bytes.Repeat([]byte("key"), 1024)
			handler := func(_ context.Context, _ string, req proto.Message) (any, error) {
				mu.Lock()
				defer mu.Unlock()
				r := req.(*pb.UpdateContainerRequest)
				k := r.GetOrderedElementKey()
				if k == nil || k.Order == nil || k.GetClientId() != 0 || k.CompressionInfo == nil || r.GetElementKey() != nil || r.GetValue() != nil {
					t.Error("update oneofs/compression/zero CID")
				}
				raw, e := decode(k.Payload.Payload, k.Payload.Size, k.CompressionInfo, config)
				if e != nil {
					return nil, e
				}
				old := member{string(raw), *k.Order}
				value, ok := entries[old]
				if !ok {
					return nil, status.Error(codes.NotFound, "exact member/current order missing")
				}
				next := r.GetNewOrder()
				if delta, ok := r.Values.(*pb.UpdateContainerRequest_DeltaOrder); ok {
					if delta.DeltaOrder < 0 {
						magnitude := uint64(-(delta.DeltaOrder + 1)) + 1
						if old.order < magnitude {
							return nil, status.Error(codes.OutOfRange, "order underflow")
						}
						next = old.order - magnitude
					} else {
						if old.order > math.MaxUint64-uint64(delta.DeltaOrder) {
							return nil, status.Error(codes.OutOfRange, "order overflow")
						}
						next = old.order + uint64(delta.DeltaOrder)
					}
				}
				delete(entries, old)
				entries[member{old.data, next}] = value
				return &pb.UpdateValueResponse{Result: true}, nil
			}
			for name, c := range currentClients(t, config, handler, nil) {
				t.Run(fmtName(codec, name)+"/"+typ.String(), func(t *testing.T) {
					mu.Lock()
					entries = map[member]Variant{{string(keyBytes), 1}: Double(1.25), {string(keyBytes), 2}: Int(2)}
					mu.Unlock()
					key := OrderedPayload{Payload: Payload{Data: keyBytes}, Order: Ptr(uint64(1))}
					o := Options{ClientID: Ptr(uint32(0))}
					ok, e := c.IncrementOrder(context.Background(), []byte("container"), key, 4, o)
					if e != nil || !ok {
						t.Fatal(e)
					}
					key.Order = Ptr(uint64(2))
					ok, e = c.UpdateOrder(context.Background(), []byte("container"), key, 0, o)
					if e != nil || !ok {
						t.Fatal(e)
					}
					key.Order = Ptr(uint64(0))
					if _, e = c.IncrementOrder(context.Background(), []byte("container"), key, -1, o); status.Code(e) != codes.OutOfRange {
						t.Fatal("underflow", e)
					}
					ok, e = c.UpdateOrder(context.Background(), []byte("container"), key, math.MaxUint64, o)
					if e != nil || !ok {
						t.Fatal(e)
					}
					key.Order = Ptr(uint64(math.MaxUint64))
					if _, e = c.IncrementOrder(context.Background(), []byte("container"), key, 1, o); status.Code(e) != codes.OutOfRange {
						t.Fatal("overflow", e)
					}
					key.Order = Ptr(uint64(2))
					if _, e = c.UpdateOrder(context.Background(), []byte("container"), key, 3, o); status.Code(e) != codes.NotFound {
						t.Fatal("current order required", e)
					}
					key.Order = nil
					if _, e = c.UpdateOrder(context.Background(), []byte("container"), key, 0, o); status.Code(e) != codes.InvalidArgument {
						t.Fatal("missing order", e)
					}
					mu.Lock()
					if len(entries) != 2 || entries[member{string(keyBytes), 5}] != Double(1.25) || entries[member{string(keyBytes), math.MaxUint64}] != Int(2) {
						t.Error("order failure mutated entries or lost payload")
					}
					mu.Unlock()
				})
			}
		}
	}
}

func TestDestructiveRangePresenceAndNoReplay(t *testing.T) {
	for _, typ := range []ContainerType{List, Vector, OrderedMap, OrderedSet} {
		t.Run(typ.String(), func(t *testing.T) {
			var backups atomic.Int32
			master := transport(t, nil, func(req proto.Message, g grpc.ServerStreamingServer[pb.BatchValueResponse]) error {
				r := req.(*pb.KeyPositionRequest)
				if r.Pos != nil || r.End != nil || r.Type == nil || *r.Type != typ || !r.Reverse {
					t.Error("destructive zero/open bound presence")
				}
				return g.Send(&pb.BatchValueResponse{KeyUnordered: []*pb.Key{{}}})
			})
			backup := transport(t, nil, func(_ proto.Message, _ grpc.ServerStreamingServer[pb.BatchValueResponse]) error {
				backups.Add(1)
				return nil
			})
			coord := coordinatorTransport(t, func(g grpc.ServerStreamingServer[cpb.RoutingInfoData]) error { return g.Send(topologyData("m", "b")) })
			c, e := NewSmartClient([]string{"c"}, SmartConfig{Mode: Backup, ConnectionFactory: func(_ context.Context, target string) (Connection, error) {
				if target == "c" {
					return Connection{Conn: coord}, nil
				}
				if strings.HasPrefix(target, "m:") {
					return Connection{Conn: master}, nil
				}
				return Connection{Conn: backup}, nil
			}})
			if e != nil {
				t.Fatal(e)
			}
			defer c.Close()
			waitReady(t, c)
			_, e = c.PopRange(context.Background(), []byte("k"), Position{Start: 0, End: Ptr(uint64(0)), Type: Ptr(typ), Reverse: true}, Options{})
			var partial *PartialError
			if !errors.As(e, &partial) || partial.CompletedChunks != 1 || backups.Load() != 0 {
				t.Fatalf("destructive acknowledged batch replayed: %v backups=%d", e, backups.Load())
			}
		})
	}
}

func TestEndpointTargets(t *testing.T) {
	for _, tc := range []struct{ host, want string }{{"127.0.0.1", "127.0.0.1:50000"}, {"::1", "[::1]:50000"}, {"[2001:db8::1]", "[2001:db8::1]:50000"}, {"fe80::1%eth0", "[fe80::1%eth0]:50000"}, {"node.local", "node.local:50000"}} {
		got, e := endpointTarget(&cpb.Endpoint{Ip: tc.host, Port: 50000})
		if e != nil || got != tc.want {
			t.Fatal(got, e)
		}
	}
	for _, ep := range []*cpb.Endpoint{nil, {}, {Ip: "::1"}, {Ip: "x", Port: 65536}} {
		if _, e := endpointTarget(ep); status.Code(e) != codes.DataLoss {
			t.Fatal("invalid endpoint", ep, e)
		}
	}
}

func TestTransportCompressionIndependentOfPayloadCodec(t *testing.T) {
	conn := transport(t, func(_ context.Context, _ string, req proto.Message) (any, error) {
		v := req.(*pb.CreateRequest).Value
		config, _ := normalizeConfig(Config{CompressionCodec: CompressionLZ4})
		decoded, e := decodeValue(v, config)
		if e != nil || decoded == nil || *decoded != String(strings.Repeat("x", 4096)) {
			t.Fatal("transport altered raw LZ4", e)
		}
		return &pb.KeyHintResponse{}, nil
	}, nil)
	c, e := NewClientWithConnection(conn, Config{CompressionCodec: CompressionLZ4})
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	// The transport setting is independent of payload Config.CompressionCodec.
	stub := c.backend.(*directBackend)
	stub.stub = pb.NewHurriCacheGrpcServiceClient(&compressedConnection{ClientConnInterface: conn})
	_, e = c.Create(context.Background(), []byte("k"), String(strings.Repeat("x", 4096)), Options{})
	if e != nil {
		t.Fatal(e)
	}
}

type compressedConnection struct{ grpc.ClientConnInterface }

func (c *compressedConnection) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	return c.ClientConnInterface.Invoke(ctx, method, args, reply, append(opts, grpc.UseCompressor(gzip.Name))...)
}
