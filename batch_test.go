package hurricache

import (
	"context"
	"errors"
	"fmt"
	pb "github.com/hurricache/hurricache-go-client/proto/cachepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func batchValues(n int) []Payload {
	v := make([]Payload, n)
	for i := range v {
		v[i].Data = []byte(fmt.Sprintf("%03d%s", i, strings.Repeat("x", 90)))
	}
	return v
}
func TestBatchOrderingPairsAndCounts(t *testing.T) {
	for _, op := range []string{"createMap", "createOrderedMap", "createList", "createSet", "createOrderedSet", "head", "tail", "position", "before", "after", "remove", "addMap", "addOrderedMap", "weighted", "orderedBool", "setBool"} {
		t.Run(op, func(t *testing.T) {
			input := batchValues(9)
			var got []string
			var calls int
			var mu sync.Mutex
			handler := func(ctx context.Context, method string, m proto.Message) (any, error) {
				mu.Lock()
				defer mu.Unlock()
				calls++
				if proto.Size(m) > 350 {
					t.Errorf("serialized size %d exceeds limit", proto.Size(m))
				}
				var vals []*pb.Value
				var ordered []*pb.OrderedValue
				var keys []*pb.Key
				var oks []*pb.OrderedKey
				var prefix bool
				switch r := m.(type) {
				case *pb.CreateContainerRequest:
					vals, ordered, keys, oks = r.ValueUnordered, r.ValueOrdered, r.KeyUnordered, r.KeyOrdered
				case *pb.AddToRequest:
					vals, ordered, keys, oks = r.ValueUnordered, r.ValueOrdered, r.KeyUnordered, r.KeyOrdered
					prefix = op == "head"
					if op == "position" && (r.Pos == nil || int(*r.Pos) != 5+len(got)) {
						t.Errorf("position %v want %d", r.Pos, 5+len(got))
					}
					if op == "setBool" && r.GetPos() != ^uint32(0) {
						t.Error("sentinel advanced across chunks")
					}
				case *pb.AddToValRequest:
					vals = r.Value
					prefix = op == "after"
				case *pb.RemoveFromContainerRequest:
					vals = r.Values
				default:
					t.Fatalf("unexpected request %T", m)
				}
				if strings.Contains(strings.ToLower(op), "map") {
					if len(keys)+len(oks) != len(vals) {
						t.Error("map pair split")
					}
					for i, v := range vals {
						var k []byte
						if len(keys) > 0 {
							k = keys[i].Payload.Payload
						} else {
							k = oks[i].Payload.Payload
						}
						if string(k) != string(v.Value.Payload[:3]) {
							t.Error("map pair reordered")
						}
					}
				}
				var chunk []string
				for _, v := range vals {
					chunk = append(chunk, string(v.Value.Payload))
				}
				for _, v := range ordered {
					chunk = append(chunk, string(v.Value.Payload))
				}
				if op == "head" {
					slices.Reverse(chunk)
				}
				if prefix {
					got = append(chunk, got...)
				} else {
					got = append(got, chunk...)
				}
				if _, ok := m.(*pb.CreateContainerRequest); ok {
					return &pb.KeyHintResponse{KeyHint: &pb.KeyHint{WeekHash: Ptr(uint32(19)), StrongHash: Ptr(uint32(20))}}, nil
				}
				if strings.HasSuffix(method, "/addElement") || strings.HasSuffix(method, "/removeFromContainerByKeyValue") {
					return &pb.IntResponse{Size: uint32(len(chunk))}, nil
				}
				return &pb.BoolResponse{Value: true}, nil
			}
			conn := transport(t, handler, nil)
			c, _ := NewClientWithConnection(conn, Config{MaxRequestBytes: 350})
			defer c.Close()
			ctx := context.Background()
			o := Options{}
			key := []byte("batch")
			var e error
			var count uint32
			var countResult bool
			entries := make([]Entry, len(input))
			oe := make([]OrderedEntry, len(input))
			ov := make([]OrderedPayload, len(input))
			for i, v := range input {
				entries[i] = Entry{Key: Payload{Data: v.Data[:3]}, Value: v}
				oe[i] = OrderedEntry{Key: OrderedPayload{Payload: entries[i].Key, Order: Ptr(uint64(i))}, Value: v}
				ov[i] = OrderedPayload{v, Ptr(uint64(i))}
			}
			switch op {
			case "createMap":
				_, e = c.CreateMap(ctx, key, entries, o)
			case "createOrderedMap":
				_, e = c.CreateOrderedMap(ctx, key, oe, o)
			case "createList":
				_, e = c.CreateList(ctx, key, input, o)
			case "createSet":
				_, e = c.CreateSet(ctx, key, input, o)
			case "createOrderedSet":
				_, e = c.CreateOrderedSet(ctx, key, ov, o)
			case "head":
				_, e = c.AddElementToHead(ctx, key, input, o)
			case "tail":
				_, e = c.AddElementToTail(ctx, key, input, o)
			case "position":
				count, e = c.AddElementToPosition(ctx, key, input, 5, o)
				countResult = true
			case "before":
				_, e = c.AddElementToPositionBefore(ctx, key, input, Payload{Data: []byte("pivot")}, o)
			case "after":
				_, e = c.AddElementToPositionAfter(ctx, key, input, Payload{Data: []byte("pivot")}, o)
			case "remove":
				count, e = c.RemoveFromContainer(ctx, key, Removal{Type: Set, Values: input}, o)
				countResult = true
			case "addMap":
				count, e = c.AddElementHashMap(ctx, key, entries, o)
				countResult = true
			case "addOrderedMap":
				count, e = c.AddElementOrderedMap(ctx, key, oe, o)
				countResult = true
			case "weighted":
				count, e = c.AddElementWithWeight(ctx, key, ov, o)
				countResult = true
			case "orderedBool":
				_, e = c.AddElementOrdered(ctx, key, ov, o)
			case "setBool":
				_, e = c.AddElement(ctx, key, input, o)
			}
			if e != nil {
				t.Fatal(e)
			}
			if calls < 2 {
				t.Fatal("test did not chunk")
			}
			if countResult && count != 9 {
				t.Errorf("count %d want 9", count)
			}
			want := make([]string, len(input))
			for i, v := range input {
				want[i] = string(v.Data)
			}
			if op == "head" {
				slices.Reverse(want)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("chunk ordering/data loss: got %v want %v", got, want)
			}
		})
	}
}
func TestOversizedPreflightAndPartialFailure(t *testing.T) {
	var calls atomic.Int32
	conn := transport(t, func(ctx context.Context, m string, r proto.Message) (any, error) {
		n := calls.Add(1)
		if n == 2 {
			grpc.SetTrailer(ctx, metadata.Pairs("detail", "second chunk"))
			return nil, status.Error(codes.Unavailable, "node lost")
		}
		return response(m), nil
	}, nil)
	c, _ := NewClientWithConnection(conn, Config{MaxRequestBytes: 350})
	defer c.Close()
	values := batchValues(3)
	values = append(values, Payload{Data: []byte(strings.Repeat("large", 150))})
	_, e := c.CreateList(context.Background(), []byte("batch"), values, Options{})
	if status.Code(e) != codes.ResourceExhausted || calls.Load() != 0 {
		t.Fatalf("oversized preflight %v calls %d", e, calls.Load())
	}
	_, e = c.AddElementToTail(context.Background(), []byte("batch"), batchValues(9), Options{})
	var partialErr *PartialError
	var rpc *RPCError
	if !errors.As(e, &partialErr) || partialErr.CompletedChunks != 1 || partialErr.CompletedItems == 0 || !errors.As(e, &rpc) || status.Code(e) != codes.Unavailable || rpc.Trailers.Get("detail")[0] != "second chunk" {
		t.Fatalf("partial failure %v", e)
	}
	if calls.Load() != 2 {
		t.Fatalf("retried completed chunks: %d", calls.Load())
	}
}
func TestBatchDeadlineBoundsAllChunks(t *testing.T) {
	var calls atomic.Int32
	conn := transport(t, func(ctx context.Context, m string, r proto.Message) (any, error) {
		calls.Add(1)
		select {
		case <-time.After(25 * time.Millisecond):
			return response(m), nil
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}, nil)
	c, _ := NewClientWithConnection(conn, Config{MaxRequestBytes: 200})
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 65*time.Millisecond)
	defer cancel()
	begin := time.Now()
	_, e := c.AddElementToTail(ctx, []byte("batch"), batchValues(9), Options{})
	if status.Code(e) != codes.DeadlineExceeded || time.Since(begin) > 300*time.Millisecond || calls.Load() > 3 {
		t.Fatalf("batch deadline %v calls %d elapsed %v", e, calls.Load(), time.Since(begin))
	}
}
func TestStreamPrefixFailureAndCompression(t *testing.T) {
	config, _ := normalizeConfig(Config{})
	compressed, e := encodeValue(Payload{Data: []byte(strings.Repeat("compress", 1000))}, nil, config)
	if e != nil {
		t.Fatal(e)
	}
	conn := transport(t, nil, func(r proto.Message, g grpc.ServerStreamingServer[pb.BatchValueResponse]) error {
		k, e := encodeKey([]byte(strings.Repeat("key", 1000)), nil, Ptr(uint32(7)), config)
		if e != nil {
			return e
		}
		if e := g.Send(&pb.BatchValueResponse{KeyUnordered: []*pb.Key{k}, ValueUnordered: []*pb.Value{compressed}}); e != nil {
			return e
		}
		g.SetTrailer(metadata.Pairs("detail", "stream failed"))
		return status.Error(codes.Internal, "after prefix")
	})
	c, _ := NewClientWithConnection(conn, Config{})
	defer c.Close()
	r, e := c.StreamMap(context.Background(), []byte("map"), Options{})
	var p *PartialError
	var rpc *RPCError
	if len(r) != 1 || string(r[0].Key.Data) != strings.Repeat("key", 1000) || string(r[0].Value.Data) != strings.Repeat("compress", 1000) || !errors.As(e, &p) || !errors.As(e, &rpc) || status.Code(e) != codes.Internal || rpc.Trailers.Get("detail")[0] != "stream failed" {
		t.Fatalf("stream prefix/decode error: len=%d err=%v", len(r), e)
	}
}
func TestExactSerializedLimit(t *testing.T) {
	var calls atomic.Int32
	conn := transport(t, func(ctx context.Context, m string, r proto.Message) (any, error) {
		calls.Add(1)
		return response(m), nil
	}, nil)
	config, _ := normalizeConfig(Config{})
	key, _ := encodeKey([]byte("k"), nil, Ptr(uint32(0)), config)
	v, _ := encodeValue(Payload{Data: []byte("123")}, nil, config)
	req := &pb.AddToRequest{Key: key, ValueUnordered: []*pb.Value{v}}
	exact := proto.Size(req)
	c, _ := NewClientWithConnection(conn, Config{MaxRequestBytes: exact})
	defer c.Close()
	if _, e := c.AddElementToTail(context.Background(), []byte("k"), []Payload{{Data: []byte("123")}}, Options{}); e != nil {
		t.Fatal(e)
	}
	smaller, _ := NewClientWithConnection(conn, Config{MaxRequestBytes: exact - 1})
	defer smaller.Close()
	if _, e := smaller.AddElementToTail(context.Background(), []byte("k"), []Payload{{Data: []byte("123")}}, Options{}); status.Code(e) != codes.ResourceExhausted {
		t.Fatal(e)
	}
	if calls.Load() != 1 {
		t.Fatal("oversize RPC was sent")
	}
}
