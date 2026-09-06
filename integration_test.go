package hurricache

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	pb "github.com/hurricache/hurricache-go-client/proto/cachepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func liveClient(t *testing.T) *Client {
	t.Helper()
	target := os.Getenv("HURRICACHE_TEST_TARGET")
	if target == "" {
		t.Skip("set HURRICACHE_TEST_TARGET to the pinned standalone server")
	}
	c, e := NewClient(target, Config{Timeout: 5 * time.Second})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}
func liveKey(t *testing.T, c *Client) []byte {
	t.Helper()
	nonce := make([]byte, 12)
	if _, e := rand.Read(nonce); e != nil {
		t.Fatal(e)
	}
	key := []byte(fmt.Sprintf("go-parity/%x/%s", nonce, t.Name()))
	t.Cleanup(func() {
		if _, e := c.Remove(context.Background(), key, Options{ClientID: Ptr(uint32(77))}); e != nil && status.Code(e) != codes.NotFound {
			t.Errorf("test-key cleanup: %v", e)
		}
	})
	return key
}

func TestLiveServerObservations(t *testing.T) {
	c := liveClient(t)
	key := liveKey(t, c)
	ctx := context.Background()
	hint, e := c.CreateOrderedSet(ctx, key, []OrderedPayload{{Payload{Data: []byte("one")}, Ptr(uint64(1))}, {Payload{Data: []byte("two")}, Ptr(uint64(2))}, {Payload{Data: []byte("three")}, Ptr(uint64(3))}}, Options{})
	if e != nil {
		t.Fatal(e)
	}
	for _, bounds := range [][2]uint64{{1, 2}, {2, 1}, {0, 4}, {4, 0}} {
		data, e := perform(c, ctx, key, Options{Hint: hint}, false, func(s *session) ([]uint64, error) {
			stream, e := s.stub.GetElementInRange(ctx, &pb.KeyPositionRequest{Key: s.key, Type: Ptr(OrderedSet), Pos: bounds[0], End: Ptr(bounds[1]), Reverse: true})
			if e != nil {
				return nil, e
			}
			var result []uint64
			for {
				b, e := stream.Recv()
				if e != nil {
					if e == io.EOF {
						return result, nil
					}
					return result, e
				}
				for _, v := range b.ValueOrdered {
					result = append(result, v.GetOrder())
				}
			}
		})
		t.Logf("reverse wire bounds %v: %v (%v)", bounds, data, e)
	}
	mapKey := liveKey(t, c)
	_, e = c.CreateMap(ctx, mapKey, []Entry{{Payload{Data: []byte("a")}, Payload{Data: []byte("one")}}}, Options{})
	if e != nil {
		t.Fatal(e)
	}
	u, e := c.UpdateContainerValue(ctx, mapKey, []byte("a"), []byte("replacement"), Options{})
	if e != nil {
		t.Fatal(e)
	}
	v, e := c.GetContainerValue(ctx, mapKey, []byte("a"), Options{})
	if e != nil {
		t.Fatal(e)
	}
	t.Logf("map update Updated=%v Previous=%q readback=%q", u.Updated, u.Previous.Data, v.Value.Data)
}
func TestLiveOperationMatrix(t *testing.T) {
	c := liveClient(t)
	ctx := context.Background()
	values := []Payload{{Data: []byte("one")}, {Data: []byte("two")}, {Data: []byte("three")}}
	ordered := []OrderedPayload{{values[0], Ptr(uint64(1))}, {values[1], Ptr(uint64(2))}, {values[2], Ptr(uint64(3))}}
	entries := []Entry{{Payload{Data: []byte("a")}, values[0]}, {Payload{Data: []byte("b")}, values[1]}, {Payload{Data: []byte("c")}, values[2]}}
	oe := []OrderedEntry{{OrderedPayload{Payload{Data: []byte("a")}, Ptr(uint64(1))}, values[0]}, {OrderedPayload{Payload{Data: []byte("b")}, Ptr(uint64(2))}, values[1]}, {OrderedPayload{Payload{Data: []byte("c")}, Ptr(uint64(3))}, values[2]}}
	for _, tc := range operationCases() {
		t.Run(tc.name, func(t *testing.T) {
			key := liveKey(t, c)
			o := Options{ClientID: Ptr(uint32(77))}
			var hint *KeyHint
			var e error
			name := tc.name
			isCreate := strings.HasPrefix(name, "Create") || name == "AtomicCreate"
			kind := "scalar"
			switch {
			case strings.HasPrefix(name, "Atomic"):
				kind = "atomic"
			case strings.Contains(name, "OrderedMap"):
				kind = "orderedMap"
			case strings.Contains(name, "Map") || strings.Contains(name, "ContainerValue") || name == "ContainsContainerKey" || name == "RemoveContainerKey" || name == "RemoveFromContainer":
				kind = "map"
			case strings.Contains(name, "Ordered") || name == "AddElementWithWeight":
				kind = "orderedSet"
			case name == "AddElement" || name == "StreamSet":
				kind = "set"
			case name == "StreamQueue":
				kind = "queue"
			case name == "StreamVector":
				kind = "vector"
			case strings.Contains(name, "Element") || strings.Contains(name, "Head") || strings.Contains(name, "Tail") || strings.Contains(name, "Front") || name == "StreamList" || name == "GetContainer":
				kind = "list"
			}
			if !isCreate {
				switch kind {
				case "atomic":
					hint, e = c.AtomicCreate(ctx, key, 10, o)
				case "map":
					hint, e = c.CreateMap(ctx, key, entries, o)
				case "orderedMap":
					hint, e = c.CreateOrderedMap(ctx, key, oe, o)
				case "orderedSet":
					hint, e = c.CreateOrderedSet(ctx, key, ordered, o)
				case "set":
					hint, e = c.CreateSet(ctx, key, values, o)
				case "queue":
					hint, e = c.CreateQueue(ctx, key, values, o)
				case "vector":
					hint, e = c.CreateVector(ctx, key, values, o)
				case "list":
					hint, e = c.CreateList(ctx, key, values, o)
				default:
					hint, e = c.CreateKeyValue(ctx, key, []byte("initial"), o)
				}
				if e != nil {
					t.Fatalf("setup %s: %v", kind, e)
				}
				o.Hint = hint
			}
			if name == "GetTTL" {
				if _, e := c.SetTTL(ctx, key, time.Minute, o); e != nil {
					t.Fatal(e)
				}
			}
			if name == "UnlockObject" {
				r, e := c.LockObject(ctx, key, WriteLock, 10*time.Second, o)
				if e != nil || r.Status != LockOK {
					t.Fatalf("lock setup %v %v", r, e)
				}
			}
			args := append([]any(nil), tc.args...)
			for i, a := range args {
				switch a.(type) {
				case Position:
					args[i] = Position{Start: 1, End: Ptr(uint64(1))}
				case uint32:
					args[i] = uint32(1)
				case uint64:
					args[i] = uint64(i + 1)
				case int64:
					args[i] = int64(2)
				case []byte:
					args[i] = []byte("a")
					if name == "CreateKeyValue" || name == "UpdateKeyValue" || (name == "UpdateContainerValue" && i == 1) {
						args[i] = []byte("replacement")
					}
				case Payload:
					args[i] = values[1]
				case Removal:
					args[i] = Removal{Type: Map, Keys: []Payload{{Data: []byte("a")}}}
				}
			}
			if name == "GetElementInRange" {
				args[0] = Position{Start: 0, End: Ptr(uint64(2)), Type: Ptr(List)}
			}
			if name == "StreamElementInRangeUnordered" {
				args = []any{List, uint32(0), uint32(2)}
			}
			if name == "AtomicCompareAndSet" {
				args = []any{int64(10), int64(12)}
			}
			if name == "LockObject" {
				args = []any{WriteLock, 2 * time.Second}
			}
			call := []reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(key)}
			for _, a := range args {
				call = append(call, reflect.ValueOf(a))
			}
			call = append(call, reflect.ValueOf(o))
			result := reflect.ValueOf(c).MethodByName(name).Call(call)
			if name == "StreamQueue" {
				if result[1].IsNil() || status.Code(result[1].Interface().(error)) != codes.Internal {
					t.Fatal("pinned server queue-stream behavior changed")
				}
				t.Log("server 26.35 limitation: getContainer rejects QUEUE; use queue front/tail operations")
				return
			}
			if !result[1].IsNil() {
				t.Fatal(result[1].Interface())
			}
			v := result[0].Interface()
			switch r := v.(type) {
			case *KeyHint:
				if r == nil || r.WeakHash == nil || r.StrongHash == nil {
					t.Errorf("server omitted key hints: %v", r)
				}
			case ValueResult:
				if r.Value == nil && r.Ordered == nil {
					t.Error("existing value absent")
				}
			case UpdateResult:
				if name == "UpdateContainerValue" {
					readback, e := c.GetContainerValue(ctx, key, []byte("a"), o)
					if e != nil || readback.Value == nil || string(readback.Value.Data) != "replacement" || r.Updated || r.Previous == nil || string(r.Previous.Data) != "one" {
						t.Fatalf("pinned map update regression: %+v readback=%+v err=%v", r, readback, e)
					}
					t.Log("server 26.35 defect: mutation succeeds but result=false; previous payload is preserved")
					break
				}
				if !r.Updated || r.Previous == nil {
					t.Errorf("update failed %v", r)
				}
			case bool:
				if !r {
					t.Error("operation returned false")
				}
			case LockResult:
				if r.Status != LockOK {
					t.Errorf("lock status %v", r)
				}
				if name == "LockObject" {
					_, _ = c.UnlockObject(ctx, key, o)
				}
			case CASResult:
				if !r.Swapped {
					t.Errorf("CAS failed: %v", r)
				}
			case TTLResult:
				if r.Remaining == nil || *r.Remaining < 50*time.Second || *r.Remaining > time.Minute {
					t.Errorf("TTL invalid: %v", r)
				}
			case []Payload:
				if len(r) != 3 {
					t.Errorf("stream length %d", len(r))
				}
			case []Entry:
				if len(r) != 3 {
					t.Errorf("map length %d", len(r))
				}
			case []OrderedEntry:
				if len(r) != 3 {
					t.Errorf("ordered map length %d", len(r))
				}
			case []OrderedPayload:
				if name == "StreamElementInRangeOrderedSet" {
					if len(r) != 0 {
						t.Fatal("pinned reverse-range behavior changed; revisit server finding")
					}
					t.Log("server 26.35 defect: reverse weighted range 1..2 returns empty; see raw regression")
					break
				}
				want := 3
				if strings.Contains(name, "Range") {
					want = 2
				}
				if len(r) != want {
					t.Errorf("ordered length %d want %d", len(r), want)
				}
				if name == "StreamElementInRangeOrderedSet" && len(r) == 2 && *r[0].Order < *r[1].Order {
					t.Error("reverse ignored")
				}
			}
		})
	}
}
func TestLiveCompressionAndBatchRoundTrip(t *testing.T) {
	c := liveClient(t)
	ctx := context.Background()
	for _, n := range []int{0, 1023, 1024, 1025, 100000} {
		for _, random := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/random=%v", n, random), func(t *testing.T) {
				key := liveKey(t, c)
				data := bytes.Repeat([]byte("x"), n)
				if random {
					if _, e := rand.Read(data); e != nil {
						t.Fatal(e)
					}
				}
				hint, e := c.CreateKeyValue(ctx, key, data, Options{})
				if e != nil {
					t.Fatal(e)
				}
				result, e := c.GetValue(ctx, key, Options{Hint: hint})
				if e != nil || result.Value == nil || !bytes.Equal(result.Value.Data, data) {
					t.Fatalf("roundtrip %d: %v", n, e)
				}
			})
		}
	}
	t.Run("map_chunks", func(t *testing.T) {
		key := liveKey(t, c)
		small, e := NewClient(c.Target(), Config{Timeout: 10 * time.Second, MaxRequestBytes: 1000})
		if e != nil {
			t.Fatal(e)
		}
		defer small.Close()
		var entries []Entry
		for i, v := range batchValues(100) {
			entries = append(entries, Entry{Payload{Data: []byte(fmt.Sprintf("k%d", i))}, v})
		}
		hint, e := small.CreateMap(ctx, key, entries, Options{})
		if e != nil {
			t.Fatal(e)
		}
		got, e := small.StreamMap(ctx, key, Options{Hint: hint})
		if e != nil {
			t.Fatal(e)
		}
		if len(got) != len(entries) {
			t.Fatalf("silent truncation: %d want %d", len(got), len(entries))
		}
		want := map[string]string{}
		for _, v := range entries {
			want[string(v.Key.Data)] = string(v.Value.Data)
		}
		for _, v := range got {
			if want[string(v.Key.Data)] != string(v.Value.Data) {
				t.Error("map pair corrupted")
			}
		}
	})
}
