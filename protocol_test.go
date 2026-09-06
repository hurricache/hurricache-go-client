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
func (s *testService) GetElementInRange(r *pb.KeyPositionRequest, g grpc.ServerStreamingServer[pb.BatchValueResponse]) error {
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
	return &pb.Value{Value: &pb.BinaryPayload{Payload: []byte(data), Size: uint32(len(data))}}
}
func response(method string) any {
	switch method[strings.LastIndex(method, "/")+1:] {
	case "createKeyValue", "createContainer", "atomicCreate", "atomicStore":
		return &pb.KeyHintResponse{KeyHint: &pb.KeyHint{WeekHash: Ptr(uint32(0))}}
	case "getValue", "getAndDeleteValue", "getHead", "getTail", "getAndRemoveFront", "getAndRemoveTail", "getElementAtPosition", "getAndRemoveElementAtPosition", "getValueInContainer", "getAndDeleteValueInContainer":
		return &pb.ValueResponse{ValueUnordered: plain("value"), KeyHint: &pb.KeyHint{StrongHash: Ptr(uint32(math.MaxUint32))}}
	case "updateValue", "updateValueInContainer":
		return &pb.UpdateValueResponse{Result: true, Value: plain("previous")}
	case "lockObject":
		return &pb.LockResponse{Result: CantLock, Message: Ptr("held")}
	case "unlockObject":
		return &pb.UnlockResponse{Result: CantUnlock, Message: Ptr("held")}
	case "getTtl":
		return &pb.TtlResponse{Ttl: Ptr(uint64(time.Now().Add(time.Minute).UnixMilli()))}
	case "getSize", "addElement", "removeFromContainerByKeyValue", "removeInContainer":
		return &pb.IntResponse{Size: 7}
	case "atomicCompareAndSet":
		return &pb.AtomicCasRes{Result: false, Expected: &pb.AtomicValue{Val: math.MinInt64}, KeyHint: &pb.KeyHint{WeekHash: Ptr(uint32(math.MaxUint32))}}
	default:
		if strings.Contains(method, "/atomic") {
			return &pb.AtomicValue{Val: math.MinInt64, KeyHint: &pb.KeyHint{WeekHash: Ptr(uint32(0))}}
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
	var cases []operationCase
	add := func(name, rpc string, args ...any) { cases = append(cases, operationCase{name, rpc, args, "values"}) }
	for _, name := range []string{"GetValue", "GetAndDeleteValue", "ExistKey", "Remove", "GetSize", "GetHead", "GetTail", "GetAndRemoveFront", "GetAndRemoveTail", "RemoveHead", "RemoveTail", "AtomicLoad", "AtomicLoadAndDelete", "UnlockObject"} {
		add(name, strings.ToLower(name[:1])+name[1:])
	}
	add("GetFront", "getHead")
	add("SetTTL", "setTtl", time.Minute)
	add("GetTTL", "getTtl")
	add("CreateKeyValue", "createKeyValue", []byte("input"))
	add("UpdateKeyValue", "updateValue", []byte("input"))
	add("LockObject", "lockObject", WriteLock, 2500*time.Millisecond)
	for _, name := range []string{"AtomicCreate", "AtomicStore", "AtomicExchange", "AtomicAdd", "AtomicSub", "AtomicOr", "AtomicAnd", "AtomicXor"} {
		add(name, strings.ToLower(name[:1])+name[1:], int64(math.MinInt64))
	}
	add("AtomicCompareAndSet", "atomicCompareAndSet", int64(math.MinInt64), int64(math.MaxInt64))
	for _, name := range []string{"GetElementAtPosition", "GetAndRemoveElementAtPosition", "RemoveElementAtPosition"} {
		add(name, strings.ToLower(name[:1])+name[1:], Position{Start: math.MaxUint64, End: Ptr(uint64(math.MaxUint64)), Reverse: true})
	}
	for _, p := range [][2]string{{"GetContainerValue", "getValueInContainer"}, {"GetAndRemoveContainerValue", "getAndDeleteValueInContainer"}, {"ContainsContainerKey", "existKeyInContainer"}, {"RemoveContainerKey", "removeInContainer"}} {
		add(p[0], p[1], []byte("element"))
	}
	add("UpdateContainerValue", "updateValueInContainer", []byte("element"), []byte("input"))
	vals := []Payload{{Data: []byte("input")}}
	ordered := []OrderedPayload{{Payload: vals[0], Order: Ptr(uint64(math.MaxUint64))}}
	entries := []Entry{{Key: Payload{Data: []byte("k")}, Value: vals[0]}}
	oe := []OrderedEntry{{Key: OrderedPayload{Payload: Payload{Data: []byte("k")}, Order: Ptr(uint64(math.MaxUint64))}, Value: vals[0]}}
	for _, name := range []string{"Queue", "List", "Vector", "Set"} {
		add("Create"+name, "createContainer", vals)
	}
	add("CreateMap", "createContainer", entries)
	add("CreateOrderedMap", "createContainer", oe)
	add("CreateOrderedSet", "createContainer", ordered)
	add("CreateContainer", "createContainer", Map, ContainerData{Entries: entries})
	for _, name := range []string{"AddElement", "AddElementToTail", "AddElementToHead"} {
		rpc := "addElement"
		if name != "AddElement" {
			rpc = strings.ToLower(name[:1]) + name[1:]
		}
		add(name, rpc, vals)
	}
	add("AddElementToPosition", "addElement", vals, uint32(math.MaxUint32-1))
	for _, name := range []string{"AddElementOrdered", "AddElementOrderedSet", "AddElementWithWeight"} {
		add(name, "addElement", ordered)
	}
	add("AddElementHashMap", "addElement", entries)
	add("AddElementOrderedMap", "addElement", oe)
	add("AddElementToPositionBefore", "addElementToPositionByValue", vals, Payload{Data: []byte("pivot")})
	add("AddElementToPositionAfter", "addElementToPositionByValue", vals, Payload{Data: []byte("pivot")})
	add("RemoveFromContainer", "removeFromContainerByKeyValue", Removal{Type: Map, Keys: []Payload{{Data: []byte("element")}}, Values: vals})
	for _, name := range []string{"Queue", "List", "Vector", "Set", "Map", "OrderedMap", "OrderedSet"} {
		add("Stream"+name, "getContainer")
		cases[len(cases)-1].shape = name
	}
	add("GetContainer", "getContainer")
	add("GetElementInRange", "getElementInRange", Position{Start: 0, End: Ptr(uint64(math.MaxUint64)), Type: Ptr(OrderedSet), Reverse: true})
	cases[len(cases)-1].shape = "OrderedSet"
	add("StreamElementInRangeUnordered", "getElementInRange", List, uint32(1), uint32(math.MaxUint32))
	add("StreamElementInRangeOrderedSet", "getElementInRange", uint64(0), uint64(math.MaxUint64), true)
	cases[len(cases)-1].shape = "OrderedSet"
	add("StreamElementInRangeOrdered", "getElementInRange", uint64(0), uint64(math.MaxUint64))
	cases[len(cases)-1].shape = "OrderedSet"
	return cases
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
	f := m.Descriptor().Fields().ByName("key")
	k := m.Get(f).Message().Interface().(*pb.Key)
	if !bytes.Equal(k.Payload.Payload, []byte{0, 1, 255}) || k.Payload.Size != 3 || k.ClientId == nil || *k.ClientId != math.MaxUint32 || k.KeyHint == nil || k.KeyHint.WeekHash == nil || *k.KeyHint.WeekHash != 0 || k.KeyHint.StrongHash != nil {
		t.Errorf("key presence/IDs/hints lost: %v", k)
	}
	ttl := func(v *uint64) {
		if v == nil || *v < uint64(time.Now().Add(50*time.Second).UnixMilli()) || *v > uint64(time.Now().Add(65*time.Second).UnixMilli()) {
			t.Errorf("TTL not absolute milliseconds: %v", v)
		}
	}
	value := func(v *pb.Value) {
		if v == nil || v.Value == nil || !bytes.Equal(v.Value.Payload, []byte("input")) || v.Value.Size != 5 {
			t.Errorf("incorrect binary value: %v", v)
		}
	}
	switch r := req.(type) {
	case *pb.TtlRequest:
		ttl(r.Ttl)
	case *pb.CreateRequest:
		value(r.Value)
		ttl(r.Value.Ttl)
		if r.Value.LockInfo == nil || r.Value.LockInfo.Type != NoLock || r.Value.LockInfo.LockedBy == nil || *r.Value.LockInfo.LockedBy != math.MaxUint32 {
			t.Error("creation lock owner lost")
		}
	case *pb.UpdateRequest:
		value(r.Value)
		ttl(r.Value.Ttl)
	case *pb.LockRequest:
		if r.LockType != WriteLock || r.ClientId == nil || *r.ClientId != math.MaxUint32 || r.LockDuration == nil || *r.LockDuration != 2 {
			t.Errorf("lock request %v", r)
		}
	case *pb.UnLockRequest:
		if r.ClientId == nil || *r.ClientId != math.MaxUint32 {
			t.Error("unlock ID lost")
		}
	case *pb.AtomicCreate:
		if r.Val == nil || r.Val.Val != math.MinInt64 {
			t.Error("atomic integer truncated")
		}
		ttl(r.Ttl)
	case *pb.AtomicCas:
		if r.Expected.Val != math.MinInt64 || r.ToSet.Val != math.MaxInt64 {
			t.Error("CAS integer truncated")
		}
		ttl(r.Ttl)
	case *pb.KeyPositionRequest:
		if strings.Contains(tc.name, "Ordered") || tc.name == "GetElementInRange" {
			if r.Type == nil || *r.Type != OrderedSet || r.End == nil || *r.End != math.MaxUint64 {
				t.Error("ordered range fields lost")
			}
			if tc.name != "StreamElementInRangeOrdered" && !r.Reverse {
				t.Error("reverse lost")
			}
		}
		if strings.Contains(tc.name, "AtPosition") && (r.Pos != math.MaxUint64 || r.End == nil || *r.End != math.MaxUint64 || !r.Reverse) {
			t.Error("position integer/presence lost")
		}
	case *pb.ContainerGetRequest:
		if string(r.ElementKey.Payload.Payload) != "element" || r.ElementKey.ClientId == nil {
			t.Error("element key lost")
		}
	case *pb.UpdateContainerRequest:
		value(r.Value)
		if r.Value.Ttl != nil {
			t.Error("map update must omit TTL")
		}
		if string(r.ElementKey.Payload.Payload) != "element" {
			t.Error("element key lost")
		}
	case *pb.CreateContainerRequest:
		ttl(r.Ttl)
		want := map[string]ContainerType{"CreateQueue": Queue, "CreateList": List, "CreateVector": Vector, "CreateSet": Set, "CreateMap": Map, "CreateOrderedMap": OrderedMap, "CreateOrderedSet": OrderedSet, "CreateContainer": Map}[tc.name]
		if r.Type != want {
			t.Errorf("container type %v, want %v", r.Type, want)
		}
		if want == OrderedSet {
			if len(r.ValueOrdered) != 1 || r.ValueOrdered[0].GetOrder() != math.MaxUint64 {
				t.Error("ordered value lost")
			}
		} else {
			if len(r.ValueUnordered) != 1 {
				t.Fatal("missing initial value")
			}
			value(r.ValueUnordered[0])
		}
		if want == Map && (len(r.KeyUnordered) != 1 || string(r.KeyUnordered[0].Payload.Payload) != "k") {
			t.Error("map pair lost")
		}
		if want == OrderedMap && (len(r.KeyOrdered) != 1 || r.KeyOrdered[0].GetOrder() != math.MaxUint64) {
			t.Error("ordered map pair lost")
		}
	case *pb.AddToRequest:
		if tc.name == "AddElementToPosition" && (r.Pos == nil || *r.Pos != math.MaxUint32-1) {
			t.Error("position lost")
		}
		if tc.name == "AddElement" && (r.Pos == nil || *r.Pos != math.MaxUint32) {
			t.Error("Java sentinel lost")
		}
		if strings.Contains(tc.name, "Map") {
			if r.Type == nil || len(r.ValueUnordered) != 1 {
				t.Error("map representation lost")
			}
			value(r.ValueUnordered[0])
			if r.ValueUnordered[0].Value.Size == 1 {
				t.Error("Java key/value length defect reproduced")
			}
		}
		if strings.Contains(tc.name, "Ordered") || tc.name == "AddElementWithWeight" {
			if strings.Contains(tc.name, "Map") {
				if r.KeyOrdered[0].GetOrder() != math.MaxUint64 {
					t.Error("order truncated")
				}
			} else if r.ValueOrdered[0].GetOrder() != math.MaxUint64 {
				t.Error("order truncated")
			}
		}
	case *pb.AddToValRequest:
		if r.IsBefore != (tc.name == "AddElementToPositionBefore") || string(r.Pos.Value.Payload) != "pivot" || len(r.Value) != 1 {
			t.Error("relative insertion incorrect")
		}
	case *pb.RemoveFromContainerRequest:
		if r.GetType() != Map || len(r.Keys) != 1 || len(r.Values) != 1 || string(r.Keys[0].Payload.Payload) != "element" || string(r.Values[0].Value.Payload) != "input" {
			t.Error("removal keys/values swapped")
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
			case ValueResult:
				if v.Value == nil || string(v.Value.Data) != "value" || v.Hint.StrongHash == nil || *v.Hint.StrongHash != math.MaxUint32 {
					t.Errorf("decoded value %v", v)
				}
			case UpdateResult:
				if !v.Updated || v.Previous == nil || string(v.Previous.Data) != "previous" {
					t.Errorf("update %v", v)
				}
			case AtomicResult:
				if v.Value != math.MinInt64 || v.Hint.WeakHash == nil {
					t.Errorf("atomic %v", v)
				}
			case CASResult:
				if v.Swapped || v.Expected == nil || v.Expected.Value != math.MinInt64 || v.Hint.WeakHash == nil || *v.Hint.WeakHash != math.MaxUint32 {
					t.Errorf("CAS %v", v)
				}
			case *KeyHint:
				if v == nil || v.WeakHash == nil || *v.WeakHash != 0 || v.StrongHash != nil {
					t.Errorf("hint %v", v)
				}
			case bool:
				if !v {
					t.Error("false result")
				}
			case uint32:
				if v != 7 {
					t.Errorf("count %d", v)
				}
			case []Payload:
				if len(v) != 1 || string(v[0].Data) != "value" {
					t.Errorf("collection %v", v)
				}
			case []OrderedPayload:
				if len(v) != 1 || v[0].Order == nil || *v[0].Order != math.MaxUint64 || string(v[0].Data) != "value" {
					t.Errorf("ordered values %v", v)
				}
			case []Entry:
				if len(v) != 1 || !bytes.Equal(v[0].Key.Data, []byte{0, 255}) || string(v[0].Value.Data) != "value" {
					t.Errorf("map %v", v)
				}
			case []OrderedEntry:
				if len(v) != 1 || !bytes.Equal(v[0].Key.Data, []byte{0, 255}) || v[0].Key.Order == nil || *v[0].Key.Order != math.MaxUint64 {
					t.Errorf("ordered map %v", v)
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
			p, e := encodeValue(Payload{Data: data}, nil, c)
			if e != nil {
				t.Fatal(e)
			}
			if (p.CompressionInfo != nil) != (n > 1024) {
				t.Errorf("threshold at %d", n)
			}
			d, e := decodeValue(p, c)
			if e != nil || !bytes.Equal(d.Data, data) {
				t.Fatalf("roundtrip %d random %v: %v", n, random, e)
			}
			k, e := encodeKey(data, &KeyHint{StrongHash: Ptr(uint32(0))}, Ptr(uint32(0)), c)
			if e != nil {
				t.Fatal(e)
			}
			kd, e := decodeKey(k, c)
			if e != nil || !bytes.Equal(kd.Data, data) || kd.Hint.WeakHash != nil || kd.ClientID == nil {
				t.Fatalf("key roundtrip %d: %v", n, e)
			}
		}
	}
	absent, e := decodeResult(&pb.ValueResponse{}, c)
	if e != nil || absent.Value != nil {
		t.Fatal("absence lost")
	}
	empty, e := decodeResult(&pb.ValueResponse{ValueUnordered: plain("")}, c)
	if e != nil || empty.Value == nil || empty.Value.Data == nil {
		t.Fatal("empty present value lost")
	}
	for _, v := range []*pb.Value{
		{Value: &pb.BinaryPayload{Payload: []byte{1}, Size: 2}},
		{Value: &pb.BinaryPayload{Payload: []byte{1}, Size: 1}, CompressionInfo: &pb.CompressedInfo{Enabled: true}},
		{Value: &pb.BinaryPayload{Payload: []byte{1}, Size: 1}, CompressionInfo: &pb.CompressedInfo{Enabled: true, RawSize: Ptr(uint32(50))}},
	} {
		if _, e := decodeValue(v, c); status.Code(e) != codes.DataLoss {
			t.Errorf("malformed accepted: %v", e)
		}
	}
	_, e = decodeValue(&pb.Value{Value: &pb.BinaryPayload{}, CompressionInfo: &pb.CompressedInfo{Enabled: true, RawSize: Ptr(uint32(math.MaxUint32))}}, c)
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
		key := r.(*pb.GetRequest).Key.Payload.Payload
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
