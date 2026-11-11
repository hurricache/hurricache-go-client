package hurricache

import (
	"bytes"
	"context"
	"errors"
	pb "github.com/hurricache/hurricache-go-client/proto/cachepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"math"
	"math/rand/v2"
	"net"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type testService struct {
	pb.UnimplementedHurriCacheGrpcServiceServer
	stream func(proto.Message, grpc.ServerStreamingServer[pb.BatchValueResponse]) error
}

func (s *testService) GetContainer(r *pb.GetRequest, g grpc.ServerStreamingServer[pb.BatchValueResponse]) error {
	return s.stream(r, g)
}
func (s *testService) GetElementsInRange(r *pb.KeyPositionRequest, g grpc.ServerStreamingServer[pb.BatchValueResponse]) error {
	return s.stream(r, g)
}
func (s *testService) GetAndDeleteElementsInRange(r *pb.KeyPositionRequest, g grpc.ServerStreamingServer[pb.BatchValueResponse]) error {
	return s.stream(r, g)
}
func transport(t *testing.T, handler func(context.Context, string, proto.Message) (any, error), stream func(proto.Message, grpc.ServerStreamingServer[pb.BatchValueResponse]) error) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(8 * 1024 * 1024)
	server := grpc.NewServer(grpc.MaxRecvMsgSize(16*1024*1024), grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, _ grpc.UnaryHandler) (any, error) {
		return handler(ctx, info.FullMethod, req.(proto.Message))
	}))
	pb.RegisterHurriCacheGrpcServiceServer(server, &testService{stream: stream})
	go func() { _ = server.Serve(listener) }()
	conn, e := grpc.NewClient("passthrough:///test", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = conn.Close(); server.Stop(); _ = listener.Close() })
	return conn
}
func plain(data string) *pb.Value {
	return &pb.Value{Value: &pb.BinaryPayload{Data: &pb.BinaryPayload_Payload{Payload: []byte(data)}, Size: uint32(len(data))}}
}
func response(method string) any {
	switch method[strings.LastIndex(method, "/")+1:] {
	case "createKeyValue", "createContainer":
		return &pb.KeyHintResponse{KeyHint: &pb.KeyHint{WeakHash: Ptr(uint32(0))}, Relayed: Ptr(true)}
	case "getValue", "getAndDeleteValue", "getHead", "getTail", "getAndDeleteHead", "getAndDeleteTail", "getElementAtPosition", "getAndDeleteElementAtPosition", "getValueInContainerByKey", "getAndDeleteValueInContainer":
		return &pb.ValueResponse{ValueUnordered: plain("value"), KeyHint: &pb.KeyHint{StrongHash: Ptr(uint32(math.MaxUint32))}}
	case "updateValue", "updateValueInContainer":
		return &pb.UpdateValueResponse{Result: true, Value: plain("previous")}
	case "lockObject":
		return &pb.LockResponse{Result: CantLock, Message: Ptr("held")}
	case "unlockObject":
		return &pb.UnlockResponse{Result: CantUnlock, Message: Ptr("held")}
	case "getTtl":
		return &pb.TtlResponse{Ttl: Ptr(uint64(time.Now().Add(time.Minute).UnixMilli()))}
	case "getSize", "addElementToHead", "addElementToTail", "addElementToPositionByValue", "addElement", "deleteFromContainerByKeyValue", "deleteFromContainerByKey", "deleteHead", "deleteTail", "deleteElementAtPosition":
		return &pb.IntResponse{Size: 7}
	case "atomicCompareAndSet":
		return &pb.AtomicCasRes{Result: false, Expected: &pb.ValueResponse{ValueUnordered: &pb.Value{ValueType: Ptr(pb.ValueType_INT), Value: &pb.BinaryPayload{Data: &pb.BinaryPayload_IntPayload{IntPayload: math.MinInt64}}}, KeyHint: &pb.KeyHint{WeakHash: Ptr(uint32(math.MaxUint32))}}}
	default:
		if strings.Contains(method, "/atomic") {
			return &pb.ValueResponse{ValueUnordered: &pb.Value{ValueType: Ptr(pb.ValueType_INT), Value: &pb.BinaryPayload{Data: &pb.BinaryPayload_IntPayload{IntPayload: math.MinInt64}}}}
		}
		return &pb.BoolResponse{Value: true}
	}
}

type operationCase struct {
	name, rpc string
	args      []any
	shape     string
}

func operationCases() []operationCase {
	values := []Variant{String("input")}
	ordered := []OrderedVariant{Ordered(values[0], Ptr(uint64(math.MaxUint64)))}
	entries := []Entry{{Key: Payload{Data: []byte("k")}, Value: values[0]}}
	oe := []OrderedEntry{{Key: OrderedPayload{Payload: entries[0].Key, Order: Ptr(uint64(math.MaxUint64))}, Value: values[0]}}
	return []operationCase{
		{"Get", "getValue", []any{}, "values"},
		{"CompareAndSetInContainer", "atomicCompareAndSet", []any{[]byte("entry"), int64(math.MinInt64), int64(math.MaxInt64)}, "values"},
		{"Pop", "getAndDeleteValue", []any{}, "values"},
		{"PeekHead", "getHead", []any{}, "values"},
		{"PeekTail", "getTail", []any{}, "values"},
		{"PopHead", "getAndDeleteHead", []any{}, "values"},
		{"PopTail", "getAndDeleteTail", []any{}, "values"},
		{"ContainsKey", "existKey", []any{}, "values"},
		{"Remove", "remove", []any{}, "values"},
		{"GetSize", "getSize", []any{}, "values"},
		{"DeleteHead", "deleteHead", []any{}, "values"},
		{"DeleteTail", "deleteTail", []any{}, "values"},
		{"Create", "createKeyValue", []any{String("input")}, "values"},
		{"Replace", "updateValue", []any{String("input")}, "values"},
		{"GetAt", "getElementAtPosition", []any{uint64(math.MaxUint64)}, "values"},
		{"PopAt", "getAndDeleteElementAtPosition", []any{uint64(math.MaxUint64)}, "values"},
		{"RemoveAt", "deleteElementAtPosition", []any{Position{Start: math.MaxUint64, End: Ptr(uint64(math.MaxUint64))}}, "values"},
		{"GetElementWithWeight", "getElementAtPosition", []any{uint64(math.MaxUint64)}, "values"},
		{"GetAndRemoveElementWithWeight", "getAndDeleteElementAtPosition", []any{uint64(math.MaxUint64)}, "values"},
		{"GetInContainer", "getValueInContainerByKey", []any{[]byte("element")}, "values"},
		{"PopInContainer", "getAndDeleteValueInContainer", []any{[]byte("element")}, "values"},
		{"ContainsInContainer", "existInContainerByKey", []any{[]byte("element")}, "values"},
		{"RemoveInContainer", "deleteFromContainerByKey", []any{[]byte("element")}, "values"},
		{"Update", "updateValueInContainer", []any{[]byte("element"), String("input")}, "values"},
		{"UpdateOrder", "updateValueInContainer", []any{OrderedPayload{Payload: Payload{Data: []byte("element")}, Order: Ptr(uint64(0))}, uint64(math.MaxUint64)}, "values"},
		{"IncrementOrder", "updateValueInContainer", []any{OrderedPayload{Payload: Payload{Data: []byte("element")}, Order: Ptr(uint64(0))}, int64(math.MinInt64)}, "values"},
		{"Exchange", "atomicExchange", []any{int64(math.MinInt64)}, "values"},
		{"ExchangeInContainer", "atomicExchange", []any{[]byte("element"), int64(math.MinInt64)}, "values"},
		{"Add", "atomicAdd", []any{int64(math.MinInt64)}, "values"},
		{"AddInContainer", "atomicAdd", []any{[]byte("element"), int64(math.MinInt64)}, "values"},
		{"Sub", "atomicSub", []any{int64(math.MinInt64)}, "values"},
		{"SubInContainer", "atomicSub", []any{[]byte("element"), int64(math.MinInt64)}, "values"},
		{"And", "atomicAnd", []any{int64(math.MinInt64)}, "values"},
		{"AndInContainer", "atomicAnd", []any{[]byte("element"), int64(math.MinInt64)}, "values"},
		{"Or", "atomicOr", []any{int64(math.MinInt64)}, "values"},
		{"OrInContainer", "atomicOr", []any{[]byte("element"), int64(math.MinInt64)}, "values"},
		{"Xor", "atomicXor", []any{int64(math.MinInt64)}, "values"},
		{"XorInContainer", "atomicXor", []any{[]byte("element"), int64(math.MinInt64)}, "values"},
		{"CompareAndSet", "atomicCompareAndSet", []any{int64(math.MinInt64), int64(math.MaxInt64)}, "values"},
		{"SetTTL", "setTtl", []any{time.Minute}, "values"},
		{"GetTTL", "getTtl", []any{}, "values"},
		{"LockObject", "lockObject", []any{WriteLock, 2500 * time.Millisecond}, "values"},
		{"UnlockObject", "unlockObject", []any{}, "values"},
		{"GetValue", "getValue", []any{}, "values"},
		{"GetAndDeleteValue", "getAndDeleteValue", []any{}, "values"},
		{"ExistKey", "existKey", []any{}, "values"},
		{"GetFront", "getHead", []any{}, "values"},
		{"GetHead", "getHead", []any{}, "values"},
		{"GetTail", "getTail", []any{}, "values"},
		{"GetAndRemoveFront", "getAndDeleteHead", []any{}, "values"},
		{"GetAndRemoveTail", "getAndDeleteTail", []any{}, "values"},
		{"RemoveHead", "deleteHead", []any{}, "values"},
		{"RemoveTail", "deleteTail", []any{}, "values"},
		{"CreateKeyValue", "createKeyValue", []any{[]byte("input")}, "values"},
		{"UpdateKeyValue", "updateValue", []any{[]byte("input")}, "values"},
		{"GetContainerValue", "getValueInContainerByKey", []any{[]byte("element")}, "values"},
		{"GetAndRemoveContainerValue", "getAndDeleteValueInContainer", []any{[]byte("element")}, "values"},
		{"ContainsContainerKey", "existInContainerByKey", []any{[]byte("element")}, "values"},
		{"RemoveContainerKey", "deleteFromContainerByKey", []any{[]byte("element")}, "values"},
		{"UpdateContainerValue", "updateValueInContainer", []any{[]byte("element"), []byte("input")}, "values"},
		{"Lock", "lockObject", []any{WriteLock, 2500 * time.Millisecond}, "values"},
		{"Unlock", "unlockObject", []any{}, "values"},
		{"CreateContainer", "createContainer", []any{Map, ContainerData{Entries: entries}}, "Container"},
		{"RemoveFromContainer", "deleteFromContainerByKeyValue", []any{Removal{Type: Map, Keys: []Payload{{Data: []byte("element")}}, Values: values}}, "values"},
		{"GetContainer", "getContainer", []any{}, "Container"},
		{"GetElementInRange", "getElementsInRange", []any{Position{Start: 0, End: Ptr(uint64(math.MaxUint64)), Type: Ptr(OrderedSet), Reverse: true}}, "OrderedSet"},
		{"CreateQueue", "createContainer", []any{values}, "Queue"},
		{"GetQueue", "getContainer", []any{}, "Queue"},
		{"StreamQueue", "getContainer", []any{}, "Queue"},
		{"CreateList", "createContainer", []any{values}, "List"},
		{"GetList", "getContainer", []any{}, "List"},
		{"StreamList", "getContainer", []any{}, "List"},
		{"CreateVector", "createContainer", []any{values}, "Vector"},
		{"GetVector", "getContainer", []any{}, "Vector"},
		{"StreamVector", "getContainer", []any{}, "Vector"},
		{"CreateSet", "createContainer", []any{values}, "Set"},
		{"GetSet", "getContainer", []any{}, "Set"},
		{"StreamSet", "getContainer", []any{}, "Set"},
		{"CreateOrderedSet", "createContainer", []any{ordered}, "OrderedSet"},
		{"GetOrderedSet", "getContainer", []any{}, "OrderedSet"},
		{"StreamOrderedSet", "getContainer", []any{}, "OrderedSet"},
		{"CreateMap", "createContainer", []any{entries}, "Map"},
		{"GetMap", "getContainer", []any{}, "Map"},
		{"StreamMap", "getContainer", []any{}, "Map"},
		{"CreateOrderedMap", "createContainer", []any{oe}, "OrderedMap"},
		{"GetOrderedMap", "getContainer", []any{}, "OrderedMap"},
		{"StreamOrderedMap", "getContainer", []any{}, "OrderedMap"},
		{"PushHead", "addElementToHead", []any{values}, "values"},
		{"PushTail", "addElementToTail", []any{values}, "values"},
		{"AddElementUnordered", "addElement", []any{values}, "values"},
		{"AddElementWithWeight", "addElement", []any{ordered}, "values"},
		{"InsertAt", "addElement", []any{values, uint32(math.MaxUint32 - 1)}, "values"},
		{"InsertBefore", "addElementToPositionByValue", []any{values, String("pivot")}, "values"},
		{"InsertAfter", "addElementToPositionByValue", []any{values, String("pivot")}, "values"},
		{"PutAll", "addElement", []any{entries}, "values"},
		{"AddElementOrderedMap", "addElement", []any{oe}, "values"},
		{"GetRange", "getElementsInRange", []any{Position{Start: 0, End: Ptr(uint64(math.MaxUint64)), Type: Ptr(List)}}, "values"},
		{"PopRange", "getAndDeleteElementsInRange", []any{Position{Start: 0, End: Ptr(uint64(math.MaxUint64)), Type: Ptr(OrderedSet), Reverse: true}}, "OrderedSet"},
		{"GetListElementsInRange", "getElementsInRange", []any{Ptr(uint64(1)), Ptr(uint64(math.MaxUint64)), true}, "values"},
		{"GetAndDeleteListElementsInRange", "getAndDeleteElementsInRange", []any{Ptr(uint64(1)), Ptr(uint64(math.MaxUint64)), true}, "values"},
		{"GetVectorElementsInRange", "getElementsInRange", []any{Ptr(uint64(1)), Ptr(uint64(math.MaxUint64)), true}, "values"},
		{"GetAndDeleteVectorElementsInRange", "getAndDeleteElementsInRange", []any{Ptr(uint64(1)), Ptr(uint64(math.MaxUint64)), true}, "values"},
		{"GetElementInRangeOrderedSet", "getElementsInRange", []any{Ptr(uint64(1)), Ptr(uint64(math.MaxUint64)), true}, "OrderedSet"},
		{"GetAndDeleteElementInRangeOrderedSet", "getAndDeleteElementsInRange", []any{Ptr(uint64(1)), Ptr(uint64(math.MaxUint64)), true}, "OrderedSet"},
		{"GetElementInRangeOrderedMap", "getElementsInRange", []any{Ptr(uint64(1)), Ptr(uint64(math.MaxUint64)), true}, "OrderedMap"},
		{"GetAndDeleteElementInRangeOrderedMap", "getAndDeleteElementsInRange", []any{Ptr(uint64(1)), Ptr(uint64(math.MaxUint64)), true}, "OrderedMap"},
		{"RemoveFromContainerByKey", "deleteFromContainerByKey", []any{Payload{Data: []byte("element")}}, "values"},
		{"RemoveFromContainerByKeyAndValue", "deleteFromContainerByKeyValue", []any{[]Payload{{Data: []byte("element")}}, values}, "values"},
		{"AddElement", "addElement", []any{values}, "values"},
		{"AddElementOrdered", "addElement", []any{ordered}, "values"},
		{"AddElementOrderedSet", "addElement", []any{ordered}, "values"},
		{"AddElementToHead", "addElementToHead", []any{values}, "values"},
		{"AddElementToHeadCount", "addElementToHead", []any{values}, "values"},
		{"AddElementToTail", "addElementToTail", []any{values}, "values"},
		{"AddElementToTailCount", "addElementToTail", []any{values}, "values"},
		{"AddElementToPosition", "addElement", []any{values, uint32(math.MaxUint32 - 1)}, "values"},
		{"AddElementToPositionBefore", "addElementToPositionByValue", []any{values, String("pivot")}, "values"},
		{"AddElementToPositionBeforeCount", "addElementToPositionByValue", []any{values, String("pivot")}, "values"},
		{"AddElementToPositionAfter", "addElementToPositionByValue", []any{values, String("pivot")}, "values"},
		{"AddElementToPositionAfterCount", "addElementToPositionByValue", []any{values, String("pivot")}, "values"},
		{"AddElementHashMap", "addElement", []any{entries}, "values"},
		{"StreamElementInRangeUnordered", "getElementsInRange", []any{List, uint32(1), uint32(math.MaxUint32)}, "values"},
		{"StreamElementInRangeOrderedSet", "getElementsInRange", []any{uint64(0), uint64(math.MaxUint64), true}, "OrderedSet"},
		{"StreamElementInRangeOrdered", "getElementsInRange", []any{uint64(0), uint64(math.MaxUint64)}, "OrderedSet"},
		{"StreamElementInRangeOrderedMap", "getElementsInRange", []any{uint64(0), uint64(math.MaxUint64), true}, "OrderedMap"},
		{"AtomicExchange", "atomicExchange", []any{int64(math.MinInt64)}, "values"},
		{"AtomicAdd", "atomicAdd", []any{int64(math.MinInt64)}, "values"},
		{"AtomicSub", "atomicSub", []any{int64(math.MinInt64)}, "values"},
		{"AtomicAnd", "atomicAnd", []any{int64(math.MinInt64)}, "values"},
		{"AtomicOr", "atomicOr", []any{int64(math.MinInt64)}, "values"},
		{"AtomicXor", "atomicXor", []any{int64(math.MinInt64)}, "values"},
		{"AtomicCompareAndSet", "atomicCompareAndSet", []any{int64(math.MinInt64), int64(math.MaxInt64)}, "values"},
		{"GetElementAtPosition", "getElementAtPosition", []any{Position{Start: 1, End: Ptr(uint64(2))}}, "values"},
		{"GetAndRemoveElementAtPosition", "getAndDeleteElementAtPosition", []any{Position{Start: 1, End: Ptr(uint64(2))}}, "values"},
		{"RemoveElementAtPosition", "deleteElementAtPosition", []any{Position{Start: 1, End: Ptr(uint64(2))}}, "values"},
	}
}
func testBatch(shape string) *pb.BatchValueResponse {
	b := &pb.BatchValueResponse{ValueUnordered: []*pb.Value{plain("value")}}
	switch shape {
	case "Map":
		b.KeyUnordered = []*pb.Key{{Payload: &pb.KeyBinaryPayload{Payload: []byte{0, 255}, Size: 2}, ClientId: Ptr(uint32(math.MaxUint32))}}
	case "OrderedMap":
		b.KeyOrdered = []*pb.OrderedKey{{Payload: &pb.KeyBinaryPayload{Payload: []byte{0, 255}, Size: 2}, Order: Ptr(uint64(math.MaxUint64))}}
	case "OrderedSet":
		b.ValueUnordered = nil
		b.ValueOrdered = []*pb.OrderedValue{{Value: plain("value").Value, Order: Ptr(uint64(math.MaxUint64))}}
	}
	return b
}
func checkRequest(t *testing.T, tc operationCase, req proto.Message) {
	t.Helper()
	m := req.ProtoReflect()
	k := m.Get(m.Descriptor().Fields().ByName("key")).Message().Interface().(*pb.Key)
	if !bytes.Equal(k.GetPayload().GetPayload(), []byte{0, 1, 255}) || k.ClientId == nil || *k.ClientId != math.MaxUint32 || k.KeyHint == nil || k.KeyHint.WeakHash == nil || *k.KeyHint.WeakHash != 0 || k.KeyHint.StrongHash != nil {
		t.Errorf("key presence/IDs/hints lost: %v", k)
	}
	checkValue := func(v *pb.Value) {
		if v == nil {
			t.Fatal("missing value")
		}
		d, e := decodeValue(v, Config{MaxDecodedBytes: DefaultMaxDecodedBytes})
		if e != nil || d == nil {
			t.Fatalf("invalid value %v: %v", v, e)
		}
		if d.IsBytes() && string(dataOf(d)) != "input" {
			t.Errorf("wrong value %v", d)
		}
	}
	switch r := req.(type) {
	case *pb.CreateRequest:
		checkValue(r.Value)
		if strings.Contains(tc.name, "InContainer") && r.ContainerKey == nil {
			t.Error("missing selector")
		}
		if r.Value.Ttl == nil {
			t.Error("TTL lost")
		}
	case *pb.UpdateRequest:
		checkValue(r.Value)
	case *pb.AtomicCas:
		if r.Expected.Value.GetIntPayload() != math.MinInt64 || r.ToSet.Value.GetIntPayload() != math.MaxInt64 {
			t.Error("CAS fields lost")
		}
		if tc.name == "CompareAndSetInContainer" {
			if r.GetElementKey() == nil || r.ToSet.Ttl != nil {
				t.Error("container CAS selector or absent TTL lost")
			}
		} else if r.ToSet.Ttl == nil {
			t.Error("scalar CAS TTL lost")
		}
	case *pb.UpdateContainerRequest:
		if r.GetOrderedElementKey() != nil {
			if r.GetOrderedElementKey().Order == nil {
				t.Error("current order lost")
			}
		} else {
			checkValue(r.GetValue())
			if r.GetElementKey() == nil {
				t.Error("entry key lost")
			}
		}
	case *pb.LockRequest:
		if r.LockDuration == nil || int64(*r.LockDuration) < time.Now().Unix()+1 || int64(*r.LockDuration) > time.Now().Unix()+3 || r.GetClientId() != math.MaxUint32 {
			t.Error("absolute lock seconds/owner lost")
		}
	case *pb.CreateContainerRequest:
		if r.Ttl == nil {
			t.Error("creation expiration absent")
		}
		if len(r.KeyOrdered)+len(r.KeyUnordered) > 0 && len(r.KeyOrdered)+len(r.KeyUnordered) != len(r.ValueUnordered) {
			t.Error("unpaired map")
		}
	case *pb.AddToValRequest:
		before := strings.Contains(tc.name, "Before")
		if r.IsBefore != before || string(r.Pos.Value.GetPayload()) != "pivot" {
			t.Error("pivot fields")
		}
	case *pb.RemoveFromContainerRequest:
		if len(r.Keys) != 1 || len(r.Values) != 1 {
			t.Error("removal keys/values lost")
		}
	}
}
func TestOperationContract(t *testing.T) {
	for _, tc := range operationCases() {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			conn := transport(t, func(ctx context.Context, method string, req proto.Message) (any, error) {
				calls.Add(1)
				if !strings.HasSuffix(method, "/"+tc.rpc) {
					t.Errorf("RPC %s want %s", method, tc.rpc)
				}
				checkRequest(t, tc, req)
				return response(method), nil
			}, func(req proto.Message, g grpc.ServerStreamingServer[pb.BatchValueResponse]) error {
				calls.Add(1)
				checkRequest(t, tc, req)
				return g.Send(testBatch(tc.shape))
			})
			c, e := NewClientWithConnection(conn, Config{})
			if e != nil {
				t.Fatal(e)
			}
			defer c.Close()
			args := []reflect.Value{reflect.ValueOf(context.Background()), reflect.ValueOf([]byte{0, 1, 255})}
			for _, v := range tc.args {
				args = append(args, reflect.ValueOf(v))
			}
			args = append(args, reflect.ValueOf(Options{ClientID: Ptr(uint32(math.MaxUint32)), Hint: &KeyHint{WeakHash: Ptr(uint32(0))}, TTL: Ptr(time.Minute)}))
			results := reflect.ValueOf(c).MethodByName(tc.name).Call(args)
			if !results[1].IsNil() {
				t.Fatal(results[1].Interface())
			}
			if calls.Load() != 1 {
				t.Errorf("calls %d want 1", calls.Load())
			}
			r := results[0].Interface()
			switch v := r.(type) {
			case Variant:
				want := "value"
				if strings.Contains(tc.rpc, "updateValue") {
					want = "previous"
				}
				if v != String(want) {
					t.Errorf("typed result %v", v)
				}
			case CASResult:
				if v.Swapped || v.Expected == nil || *v.Expected != Int(math.MinInt64) || v.Hint == nil {
					t.Errorf("CAS %v", v)
				}
			case *KeyHint:
				if v == nil || v.WeakHash == nil || *v.WeakHash != 0 || !v.WasRelayed {
					t.Errorf("hint/relay %v", v)
				}
			case uint32:
				if v != 7 {
					t.Errorf("count %d", v)
				}
			case int64:
				if tc.rpc == "getTtl" {
					if v < 50000 || v > 60000 {
						t.Errorf("TTL %d", v)
					}
				} else if v != math.MinInt64 {
					t.Errorf("numeric %d", v)
				}
			case []Variant:
				if len(v) != 1 || v[0] != String("value") {
					t.Errorf("collection %v", v)
				}
			case []OrderedVariant:
				if len(v) != 1 || v[0] != Ordered(String("value"), Ptr(uint64(math.MaxUint64))) {
					t.Errorf("ordered %v", v)
				}
			case []Entry:
				if len(v) != 1 || v[0].Value != String("value") {
					t.Errorf("map %v", v)
				}
			case []OrderedEntry:
				if len(v) != 1 || v[0].Value != String("value") {
					t.Errorf("ordered map %v", v)
				}
			case bool:
				if !v {
					t.Error("false result")
				}
			}

		})
	}
}

func TestCompressionPresenceAndMalformed(t *testing.T) {
	c, _ := normalizeConfig(Config{})
	for _, n := range []int{0, 1023, 1024, 1025, 100000} {
		for _, random := range []bool{false, true} {
			data := bytes.Repeat([]byte{42}, n)
			if random {
				r := rand.New(rand.NewPCG(1, 2))
				for i := range data {
					data[i] = byte(r.Uint32())
				}
			}
			p, e := encodeValue(Bytes(data), nil, c)
			if e != nil {
				t.Fatal(e)
			}
			if (p.CompressionInfo != nil) != (n > 1024) {
				t.Errorf("threshold at %d", n)
			}
			d, e := decodeValue(p, c)
			if e != nil || !bytes.Equal(dataOf(d), data) {
				t.Fatalf("roundtrip %d random %v: %v", n, random, e)
			}
			k, e := encodeKey(data, &KeyHint{StrongHash: Ptr(uint32(0))}, Ptr(uint32(0)), c)
			if e != nil {
				t.Fatal(e)
			}
			kd, e := decodeKey(k, c)
			if e != nil || !bytes.Equal(dataOf(kd), data) || kd.Hint.WeakHash != nil || kd.ClientID == nil {
				t.Fatalf("key roundtrip %d: %v", n, e)
			}
		}
	}
	absent, e := decodeResult(&pb.ValueResponse{}, c)
	if e != nil || absent.Value != nil {
		t.Fatal("absence lost")
	}
	empty, e := decodeResult(&pb.ValueResponse{ValueUnordered: plain("")}, c)
	if e != nil || empty.Value == nil || dataOf(empty.Value) == nil {
		t.Fatal("empty present value lost")
	}
	for _, v := range []*pb.Value{
		{Value: &pb.BinaryPayload{Data: &pb.BinaryPayload_Payload{Payload: []byte{1}}, Size: 2}},
		{Value: &pb.BinaryPayload{Data: &pb.BinaryPayload_Payload{Payload: []byte{1}}, Size: 1}, CompressionInfo: &pb.CompressedInfo{Enabled: true}},
		{Value: &pb.BinaryPayload{Data: &pb.BinaryPayload_Payload{Payload: []byte{1}}, Size: 1}, CompressionInfo: &pb.CompressedInfo{Enabled: true, RawSize: Ptr(uint32(50))}},
	} {
		if _, e := decodeValue(v, c); status.Code(e) != codes.DataLoss {
			t.Errorf("malformed accepted: %v", e)
		}
	}
	_, e = decodeValue(&pb.Value{Value: &pb.BinaryPayload{Data: &pb.BinaryPayload_Payload{Payload: []byte{}}}, CompressionInfo: &pb.CompressedInfo{Enabled: true, RawSize: Ptr(uint32(math.MaxUint32))}}, c)
	if status.Code(e) != codes.ResourceExhausted {
		t.Fatal(e)
	}
	b := testBatch("Map")
	b.ValueUnordered = nil
	if _, e := decodeBatch(b, c); status.Code(e) != codes.DataLoss {
		t.Fatal("mismatched map accepted")
	}
}

func TestErrorsCancellationAndBorrowedClose(t *testing.T) {
	started := make(chan struct{}, 1)
	conn := transport(t, func(ctx context.Context, m string, r proto.Message) (any, error) {
		key := r.(*pb.GetRequest).Key.GetPayload().GetPayload()
		if string(key) == "fail" {
			grpc.SetTrailer(ctx, metadata.Pairs("x-fastcache-route", "elsewhere", "detail", "retained"))
			return nil, status.Error(codes.FailedPrecondition, "route")
		}
		started <- struct{}{}
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}, nil)
	c, _ := NewClientWithConnection(conn, Config{})
	_, e := c.GetValue(context.Background(), []byte("fail"), Options{})
	var rpc *RPCError
	if status.Code(e) != codes.FailedPrecondition || !errors.As(e, &rpc) || rpc.Trailers.Get("detail")[0] != "retained" {
		t.Fatal(e)
	}
	for _, deadline := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		if deadline {
			ctx, cancel = context.WithTimeout(context.Background(), 30*time.Millisecond)
		}
		done := make(chan error, 1)
		go func() { _, e := c.GetValue(ctx, []byte("wait"), Options{}); done <- e }()
		<-started
		if !deadline {
			cancel()
		}
		e := <-done
		cancel()
		want := codes.Canceled
		if deadline {
			want = codes.DeadlineExceeded
		}
		if status.Code(e) != want {
			t.Errorf("%v want %v", e, want)
		}
	}
	done := make(chan error, 1)
	go func() { _, e := c.GetValue(context.Background(), []byte("wait"), Options{}); done <- e }()
	<-started
	if e := c.Close(); e != nil {
		t.Fatal(e)
	}
	_ = c.Close()
	if e := <-done; status.Code(e) != codes.Canceled {
		t.Fatal(e)
	}
	_, e = pb.NewHurriCacheGrpcServiceClient(conn).GetValue(context.Background(), &pb.GetRequest{Key: &pb.Key{Payload: &pb.KeyBinaryPayload{Payload: []byte("fail")}}})
	if status.Code(e) != codes.FailedPrecondition {
		t.Fatalf("borrowed channel closed: %v", e)
	}
}
