package hurricache

import (
	"context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"math"
	"slices"
	"strings"
	"testing"
)

func TestLiveOrderUpdatesAndDestructiveRanges(t *testing.T) {
	c := liveClient(t)
	ctx := context.Background()
	for name, codec := range liveCodecs(t) {
		t.Run(name, func(t *testing.T) {
			c.config.CompressionCodec = codec // exclusive test client
			for _, kind := range []ContainerType{OrderedMap, OrderedSet} {
				t.Run("order/"+kind.String(), func(t *testing.T) {
					key := liveKey(t, c)
					member := []byte(strings.Repeat("member", 1024))
					d := ContainerData{}
					if kind == OrderedMap {
						d.OrderedEntries = []OrderedEntry{{Key: OrderedPayload{Payload: Payload{Data: member}, Order: Ptr(uint64(1))}, Value: Double(1.25)}, {Key: OrderedPayload{Payload: Payload{Data: member}, Order: Ptr(uint64(2))}, Value: Int(2)}}
					} else {
						d.OrderedValues = []OrderedVariant{Ordered(Bytes(member), Ptr(uint64(1))), Ordered(Bytes(member), Ptr(uint64(2)))}
					}
					h, e := c.CreateContainer(ctx, key, kind, d, Options{})
					if e != nil {
						t.Fatal(e)
					}
					waitVisible(t, c, key, h)
					o := Options{Hint: h}
					entry := OrderedPayload{Payload: Payload{Data: member}, Order: Ptr(uint64(1))}
					ok, e := c.IncrementOrder(ctx, key, entry, 4, o)
					if e != nil || !ok {
						t.Fatal(ok, e)
					}
					entry.Order = Ptr(uint64(2))
					ok, e = c.UpdateOrder(ctx, key, entry, 0, o)
					if e != nil || !ok {
						t.Fatal(ok, e)
					}
					entry.Order = Ptr(uint64(2))
					if _, e = c.UpdateOrder(ctx, key, entry, 3, o); status.Code(e) != codes.NotFound {
						t.Fatal("exact current order", e)
					}
					entry.Order = Ptr(uint64(0))
					if _, e = c.IncrementOrder(ctx, key, entry, -1, o); status.Code(e) != codes.OutOfRange {
						t.Fatal("order underflow", e)
					}
					ok, e = c.UpdateOrder(ctx, key, entry, math.MaxUint64, o)
					if e != nil || !ok {
						t.Fatal(ok, e)
					}
					entry.Order = Ptr(uint64(math.MaxUint64))
					if _, e = c.IncrementOrder(ctx, key, entry, 1, o); status.Code(e) != codes.OutOfRange {
						t.Fatal("order overflow", e)
					}
					got, e := c.GetContainer(ctx, key, o)
					if e != nil {
						t.Fatal(e)
					}
					if kind == OrderedMap {
						if len(got.OrderedEntries) != 2 || *got.OrderedEntries[0].Key.Order != 5 || *got.OrderedEntries[1].Key.Order != math.MaxUint64 || got.OrderedEntries[0].Value != Double(1.25) || got.OrderedEntries[1].Value != Int(2) {
							t.Fatal("order update changed map payload", got)
						}
					} else {
						if len(got.OrderedValues) != 2 || got.OrderedValues[0] != Ordered(Bytes(member), Ptr(uint64(5))) || got.OrderedValues[1] != Ordered(Bytes(member), Ptr(uint64(math.MaxUint64))) {
							t.Fatal("order update changed set member", got)
						}
					}
					// Traversal alone misses stale lookup metadata after an increment.
					entry.Order = Ptr(uint64(5))
					if ok, e := c.UpdateOrder(ctx, key, entry, 6, o); e != nil || !ok {
						t.Fatal("lookup after increment", ok, e)
					}
					entry.Order = Ptr(uint64(6))
					if ok, e := c.UpdateOrder(ctx, key, entry, 5, o); e != nil || !ok {
						t.Fatal("restore incremented order", ok, e)
					}
				})
			}
			for _, kind := range []ContainerType{List, Vector, OrderedMap, OrderedSet} {
				t.Run("pop-range/"+kind.String(), func(t *testing.T) {
					key := liveKey(t, c)
					d := ContainerData{}
					for i := 0; i < 4; i++ {
						v := Int(int64(i))
						switch kind {
						case OrderedMap:
							d.OrderedEntries = append(d.OrderedEntries, OrderedEntry{Key: OrderedPayload{Payload: Payload{Data: []byte{byte(i)}}, Order: Ptr(uint64(i + 1))}, Value: v})
						case OrderedSet:
							d.OrderedValues = append(d.OrderedValues, Ordered(v, Ptr(uint64(i+1))))
						default:
							d.Values = append(d.Values, v)
						}
					}
					h, e := c.CreateContainer(ctx, key, kind, d, Options{})
					if e != nil {
						t.Fatal(e)
					}
					waitVisible(t, c, key, h)
					o := Options{Hint: h}
					start, end := uint64(1), uint64(2)
					if kind == OrderedMap || kind == OrderedSet {
						start, end = 2, 3
					}
					got, e := c.PopRange(ctx, key, Position{Start: start, End: &end, Type: &kind}, o)
					if e != nil {
						t.Fatal(e)
					}
					values := func(d ContainerData) []Variant {
						v := append([]Variant(nil), d.Values...)
						for _, e := range d.OrderedEntries {
							v = append(v, e.Value)
						}
						for _, e := range d.OrderedValues {
							v = append(v, e.Value())
						}
						return v
					}
					if !slices.Equal(values(got), []Variant{Int(1), Int(2)}) {
						t.Fatal("destructive result", got)
					}
					remaining, e := c.GetContainer(ctx, key, o)
					if e != nil || !slices.Equal(values(remaining), []Variant{Int(0), Int(3)}) {
						t.Fatal("destructive remaining collection", remaining, e)
					}
				})
			}
		})
	}
}
