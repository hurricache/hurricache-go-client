package hurricache

import (
	"context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strings"
	"testing"
)

func TestLiveContainerCAS(t *testing.T) {
	c := liveClient(t)
	ctx := context.Background()
	for _, kind := range []ContainerType{Map, OrderedMap} {
		t.Run(kind.String(), func(t *testing.T) {
			for _, numbers := range [][2]any{{int64(100), int64(200)}, {1.5, 2.5}} {
				key := liveKey(t, c)
				field := []byte(strings.Repeat("field", 1000))
				initial, _ := numeric(numbers[0])
				expected, _ := numeric(numbers[1])
				data := ContainerData{Entries: []Entry{{Key: Payload{Data: field}, Value: initial}}}
				var selector any = field
				if kind == OrderedMap {
					selector = OrderedPayload{Payload: Payload{Data: field}, Order: Ptr(uint64(10))}
					data = ContainerData{OrderedEntries: []OrderedEntry{{Key: selector.(OrderedPayload), Value: initial}}}
				}
				hint, e := c.CreateContainer(ctx, key, kind, data, Options{})
				if e != nil {
					t.Fatal(e)
				}
				o := Options{Hint: hint}
				got, e := c.CompareAndSetInContainer(ctx, key, selector, numbers[0], numbers[1], o)
				if e != nil || !got.Swapped || got.Expected != nil {
					t.Fatal("successful CAS", got, e)
				}
				value, e := c.GetInContainer(ctx, key, field, o)
				if e != nil || value != expected {
					t.Fatal("CAS storage", value, e)
				}
				got, e = c.CompareAndSetInContainer(ctx, key, selector, numbers[0], numbers[0], o)
				if e != nil || got.Swapped || got.Expected == nil || *got.Expected != expected {
					t.Fatal("failed CAS observed value", got, e)
				}
				if kind == OrderedMap {
					wrong := OrderedPayload{Payload: Payload{Data: field}, Order: Ptr(uint64(999))}
					if _, e = c.CompareAndSetInContainer(ctx, key, wrong, numbers[1], numbers[0], o); status.Code(e) != codes.NotFound {
						t.Fatal("wrong exact order", e)
					}
				}
				if _, e = c.CompareAndSetInContainer(ctx, key, "missing", numbers[0], numbers[1], o); e == nil {
					t.Fatal("missing field accepted")
				}
			}
		})
	}
}
