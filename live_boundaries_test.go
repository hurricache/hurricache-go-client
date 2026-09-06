package hurricache

import (
	"bytes"
	"context"
	"crypto/rand"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"reflect"
	"testing"
	"time"
)

func TestLiveDefaultBatchLimit(t *testing.T) {
	c := liveClient(t)
	key := liveKey(t, c)
	values := make([]Payload, 5)
	for i := range values {
		values[i].Data = make([]byte, 1024*1024)
		if _, e := rand.Read(values[i].Data); e != nil {
			t.Fatal(e)
		}
	}
	hint, e := c.CreateVector(context.Background(), key, values, Options{})
	if e != nil {
		t.Fatal(e)
	}
	got, e := c.StreamVector(context.Background(), key, Options{Hint: hint})
	if e != nil {
		t.Fatal(e)
	}
	if len(got) != len(values) {
		t.Fatalf("large batch count %d", len(got))
	}
	for i := range got {
		if !bytes.Equal(got[i].Data, values[i].Data) {
			t.Fatalf("large batch corrupted at %d", i)
		}
	}
}

func TestLiveChunkEquivalence(t *testing.T) {
	c := liveClient(t)
	small, e := NewClient(c.Target(), Config{Timeout: 10 * time.Second, MaxRequestBytes: 350})
	if e != nil {
		t.Fatal(e)
	}
	defer small.Close()
	for _, op := range []string{"head", "tail", "position", "before", "after", "queue", "set", "orderedSet", "orderedMap"} {
		t.Run(op, func(t *testing.T) {
			var results []ContainerData
			input := batchValues(9)
			for _, client := range []*Client{c, small} {
				key := liveKey(t, c)
				ctx := context.Background()
				o := Options{}
				initial := []Payload{{Data: []byte("pivot")}}
				var h *KeyHint
				var e error
				switch op {
				case "queue":
					h, e = client.CreateQueue(ctx, key, input, o)
				case "set":
					h, e = client.CreateSet(ctx, key, input, o)
				case "orderedSet":
					var v []OrderedPayload
					for i, p := range input {
						v = append(v, OrderedPayload{p, Ptr(uint64(i))})
					}
					h, e = client.CreateOrderedSet(ctx, key, v, o)
				case "orderedMap":
					var v []OrderedEntry
					for i, p := range input {
						v = append(v, OrderedEntry{OrderedPayload{Payload{Data: p.Data[:3]}, Ptr(uint64(i))}, p})
					}
					h, e = client.CreateOrderedMap(ctx, key, v, o)
				default:
					h, e = client.CreateList(ctx, key, initial, o)
				}
				if e != nil {
					t.Fatal(e)
				}
				o.Hint = h
				switch op {
				case "head":
					_, e = client.AddElementToHead(ctx, key, input, o)
				case "tail":
					_, e = client.AddElementToTail(ctx, key, input, o)
				case "position":
					_, e = client.AddElementToPosition(ctx, key, input, 0, o)
				case "before":
					_, e = client.AddElementToPositionBefore(ctx, key, input, initial[0], o)
				case "after":
					_, e = client.AddElementToPositionAfter(ctx, key, input, initial[0], o)
				}
				if e != nil {
					t.Fatal(e)
				}
				var d ContainerData
				if op == "queue" {
					for range input {
						r, err := client.GetAndRemoveFront(ctx, key, o)
						if err != nil {
							t.Fatal(err)
						}
						d.Values = append(d.Values, *r.Value)
					}
				} else {
					d, e = client.GetContainer(ctx, key, o)
					if e != nil {
						t.Fatal(e)
					}
				}
				// Compare semantic bytes/weights; server client IDs/hints may differ by key.
				results = append(results, d)
			}
			if !reflect.DeepEqual(results[0], results[1]) {
				t.Errorf("split %s differs from one request: %+v vs %+v", op, results[0], results[1])
			}
		})
	}
}
func TestLiveLocksTTLAndAtomicResults(t *testing.T) {
	c := liveClient(t)
	ctx := context.Background()
	for _, lock := range []LockType{NoLock, WriteLock, ReadLock, GlobalLock} {
		t.Run(lock.String(), func(t *testing.T) {
			key := liveKey(t, c)
			o := Options{ClientID: Ptr(uint32(77))}
			_, e := c.CreateKeyValue(ctx, key, []byte("locked"), o)
			if e != nil {
				t.Fatal(e)
			}
			r, e := c.LockObject(ctx, key, lock, 2*time.Second, o)
			if e != nil || r.Status != LockOK {
				t.Fatalf("lock %v %v", r, e)
			}
			u, e := c.UnlockObject(ctx, key, o)
			if e != nil || u.Status != LockOK {
				t.Fatalf("unlock %v %v", u, e)
			}
		})
	}
	t.Run("expiration", func(t *testing.T) {
		key := liveKey(t, c)
		_, e := c.CreateKeyValue(ctx, key, []byte("expires"), Options{TTL: Ptr(40 * time.Millisecond)})
		if e != nil {
			t.Fatal(e)
		}
		time.Sleep(80 * time.Millisecond)
		_, e = c.GetValue(ctx, key, Options{})
		if status.Code(e) != codes.NotFound {
			t.Fatalf("TTL did not expire: %v", e)
		}
	})
	t.Run("atomics", func(t *testing.T) {
		key := liveKey(t, c)
		h, e := c.AtomicCreate(ctx, key, 10, Options{})
		if e != nil {
			t.Fatal(e)
		}
		o := Options{Hint: h}
		for _, op := range []struct {
			name            string
			operand, stored int64
		}{{"AtomicAdd", 3, 13}, {"AtomicSub", 2, 11}, {"AtomicOr", 16, 27}, {"AtomicAnd", 15, 11}, {"AtomicXor", 3, 8}, {"AtomicExchange", -10, -10}} {
			before, e := c.AtomicLoad(ctx, key, o)
			if e != nil {
				t.Fatal(e)
			}
			res := reflect.ValueOf(c).MethodByName(op.name).Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(key), reflect.ValueOf(op.operand), reflect.ValueOf(o)})
			if !res[1].IsNil() {
				t.Fatal(res[1].Interface())
			}
			returned := res[0].Interface().(AtomicResult)
			stored, e := c.AtomicLoad(ctx, key, o)
			if e != nil || stored.Value != op.stored {
				t.Fatalf("%s stored %d want %d err %v", op.name, stored.Value, op.stored, e)
			}
			if returned.Value != before.Value {
				t.Errorf("%s returned %d want previous %d", op.name, returned.Value, before.Value)
			}
		}
		fail, e := c.AtomicCompareAndSet(ctx, key, 1, 12, o)
		if e != nil || fail.Swapped || fail.Expected == nil || fail.Expected.Value != -10 {
			t.Fatalf("failed CAS metadata %+v %v", fail, e)
		}
		ok, e := c.AtomicCompareAndSet(ctx, key, -10, 12, o)
		if e != nil || !ok.Swapped {
			t.Fatalf("CAS %+v %v", ok, e)
		}
	})
}
