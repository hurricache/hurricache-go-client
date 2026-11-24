package hurricache

import (
	"bytes"
	"context"
	"crypto/rand"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestLiveDefaultBatchLimit(t *testing.T) {
	c := liveClient(t)
	key := liveKey(t, c)
	values := make([]Variant, 5)
	for i := range values {
		body := make([]byte, 1024*1024)
		if _, e := rand.Read(body); e != nil {
			t.Fatal(e)
		}
		values[i] = Bytes(body)
	}
	hint, e := c.CreateVector(context.Background(), key, values, Options{})
	if e != nil {
		t.Fatal(e)
	}
	waitVisible(t, c, key, hint)
	got, e := c.StreamVector(context.Background(), key, Options{Hint: hint})
	if e != nil {
		t.Fatal(e)
	}
	if len(got) != len(values) {
		t.Fatalf("large batch count %d", len(got))
	}
	for i := range got {
		if !bytes.Equal(dataOf(got[i]), dataOf(values[i])) {
			t.Fatalf("large batch corrupted at %d", i)
		}
	}
}

func TestLiveChunkEquivalence(t *testing.T) {
	c := liveClient(t)
	small := newLiveClient(t, os.Getenv("HURRICACHE_TEST_TARGET"), Config{Timeout: 10 * time.Second,
		MaxRequestBytes: 350, CompressionCodec: c.config.CompressionCodec})
	for _, op := range []string{"head", "tail", "position", "before", "after", "queue", "set", "orderedSet", "orderedMap"} {
		t.Run(op, func(t *testing.T) {
			var results []ContainerData
			input := batchValues(9)
			for _, client := range []*Client{c, small} {
				key := liveKey(t, c)
				ctx := context.Background()
				o := Options{}
				initial := []Variant{Bytes([]byte("pivot"))}
				var h *KeyHint
				var e error
				switch op {
				case "queue":
					h, e = client.CreateQueue(ctx, key, input, o)
				case "set":
					h, e = client.CreateSet(ctx, key, input, o)
				case "orderedSet":
					var v []OrderedVariant
					for i, p := range input {
						v = append(v, Ordered(p, Ptr(uint64(i))))
					}
					h, e = client.CreateOrderedSet(ctx, key, v, o)
				case "orderedMap":
					var v []OrderedEntry
					for i, p := range input {
						v = append(v, OrderedEntry{OrderedPayload{Payload{Data: dataOf(p)[:3]}, Ptr(uint64(i))}, p})
					}
					h, e = client.CreateOrderedMap(ctx, key, v, o)
				default:
					h, e = client.CreateList(ctx, key, initial, o)
				}
				if e != nil {
					t.Fatal(e)
				}
				waitVisible(t, client, key, h)
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
						d.Values = append(d.Values, r)
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
			if op == "set" {
				// set does not guarantee iteration order — compare as sets
				a, b := map[Variant]bool{}, map[Variant]bool{}
				for _, v := range results[0].Values {
					a[v] = true
				}
				for _, v := range results[1].Values {
					b[v] = true
				}
				if len(results[0].Values) != len(results[1].Values) || !reflect.DeepEqual(a, b) {
					t.Errorf("split set differs from one request: %+v vs %+v", results[0], results[1])
				}
			} else if !reflect.DeepEqual(results[0], results[1]) {
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
			h, e := c.CreateKeyValue(ctx, key, []byte("locked"), o)
			if e != nil {
				t.Fatal(e)
			}
			waitVisible(t, c, key, h)
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
	t.Run("lock-expiration", func(t *testing.T) {
		key := liveKey(t, c)
		o := Options{ClientID: Ptr(uint32(77))}
		h, e := c.Create(ctx, key, Int(1), o)
		if e != nil {
			t.Fatal(e)
		}
		waitVisible(t, c, key, h)
		if r, e := c.LockObject(ctx, key, WriteLock, 2*time.Second, o); e != nil || r.Status != LockOK {
			t.Fatal("initial lock", r, e)
		}
		other := Options{ClientID: Ptr(uint32(78))}
		defer c.UnlockObject(ctx, key, o)
		defer c.UnlockObject(ctx, key, other)
		if r, e := c.LockObject(ctx, key, WriteLock, 2*time.Second, other); e != nil || r.Status == LockOK {
			t.Fatal("concurrent owner acquired lock", r, e)
		}
		// Allow the server's coarse lock timer to advance after expiration.
		time.Sleep(2100 * time.Millisecond)
		deadline := time.Now().Add(2 * time.Second)
		for {
			r, e := c.LockObject(ctx, key, WriteLock, 2*time.Second, other)
			if e != nil {
				t.Fatal(e)
			}
			if r.Status == LockOK {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("lock did not expire", r)
			}
			time.Sleep(50 * time.Millisecond)
		}
	})
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
		h, e := c.Create(ctx, key, Int(10), Options{})
		if e != nil {
			t.Fatal(e)
		}
		waitVisible(t, c, key, h)
		o := Options{Hint: h}
		for _, op := range []struct {
			name            string
			operand, stored int64
		}{{"Add", 3, 13}, {"Sub", 2, 11}, {"Or", 16, 27}, {"And", 15, 11}, {"Xor", 3, 8}, {"Exchange", -10, -10}} {
			before, e := c.Get(ctx, key, o)
			if e != nil {
				t.Fatal(e)
			}
			res := reflect.ValueOf(c).MethodByName(op.name).Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(key), reflect.ValueOf(op.operand), reflect.ValueOf(o)})
			if !res[1].IsNil() {
				t.Fatal(res[1].Interface())
			}
			returned := res[0].Interface()
			stored, e := c.Get(ctx, key, o)
			if e != nil || stored != Int(op.stored) {
				t.Fatalf("%s stored %v want %d err %v", op.name, stored, op.stored, e)
			}
			if returned != int64(before.bits) {
				t.Errorf("%s returned %d want previous %d", op.name, returned, int64(before.bits))
			}
		}
		fail, e := c.CompareAndSet(ctx, key, 1, 12, o)
		if e != nil || fail.Swapped || fail.Expected == nil || *fail.Expected != Int(-10) {
			t.Fatalf("failed CAS metadata %+v %v", fail, e)
		}
		ok, e := c.CompareAndSet(ctx, key, -10, 12, o)
		if e != nil || !ok.Swapped {
			t.Fatalf("CAS %+v %v", ok, e)
		}
	})
}
