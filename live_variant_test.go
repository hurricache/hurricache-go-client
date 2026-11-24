package hurricache

import (
	"context"
	"math"
	"slices"
	"strings"
	"testing"
)

func TestLiveVariantSetEquality(t *testing.T) {
	c := liveClient(t)
	ctx := context.Background()
	key := liveKey(t, c)
	values := []Variant{None(), Bytes(nil), Int(0), Double(0), Double(math.Copysign(0, -1)),
		Double(math.NaN()), Double(math.Float64frombits(0x7ff8000000000001)), Int(1), Int(0x100000001)}
	hint, err := c.CreateSet(ctx, key, values, Options{})
	if err != nil {
		t.Fatal(err)
	}
	waitVisible(t, c, key, hint)
	got, err := c.GetSet(ctx, key, Options{Hint: hint})
	if err != nil {
		t.Fatal(err)
	}
	want := map[Variant]bool{}
	for _, value := range values {
		if value.IsNone() { // None is dropped by the server on insert
			continue
		}
		want[value] = true
	}
	if len(got) != len(want) {
		t.Fatalf("set cardinality %d, want %d", len(got), len(want))
	}
	for _, value := range got {
		if !want[value] {
			t.Fatalf("unexpected typed set value %v", value)
		}
		delete(want, value)
	}
	if len(want) != 0 {
		t.Fatalf("missing typed set values %v", want)
	}
}

func TestLiveWeightedScalarContract(t *testing.T) {
	c := liveClient(t)
	ctx := context.Background()
	for _, value := range []Variant{Bytes(nil), Double(math.Copysign(0, -1)), String(strings.Repeat("weighted", 1024))} {
		for _, weight := range []uint64{2, math.MaxUint64} {
			other := uint64(3)
			if weight == math.MaxUint64 {
				other = 1
			}
			key := liveKey(t, c)
			hint, err := c.CreateOrderedSet(ctx, key, []OrderedVariant{Ordered(value, Ptr(weight)), Ordered(Int(9), Ptr(other))}, Options{})
			if err != nil {
				t.Fatal(err)
			}
			waitVisible(t, c, key, hint)
			o := Options{Hint: hint}
			got, err := c.GetElementWithWeight(ctx, key, weight, o)
			if err != nil || got != value {
				t.Fatal("weighted scalar read", got, err)
			}
			got, err = c.GetAndRemoveElementWithWeight(ctx, key, weight, o)
			if err != nil || got != value {
				t.Fatal("weighted scalar pop", got, err)
			}
			remaining, err := c.GetOrderedSet(ctx, key, o)
			if err != nil || !slices.Equal(remaining, []OrderedVariant{Ordered(Int(9), Ptr(other))}) {
				t.Fatal("weighted pop remaining entries", remaining, err)
			}
		}
	}
}

func TestLivePositionalReadsAndPops(t *testing.T) {
	c := liveClient(t)
	ctx := context.Background()
	for _, kind := range []ContainerType{List, Vector} {
		t.Run(kind.String(), func(t *testing.T) {
			key := liveKey(t, c)
			// None is dropped by the server on create (partial insert): 4 elements
			// remain and positions after None shift by 1
			values := []Variant{None(), Bytes(nil), Int(0), Double(math.Copysign(0, -1)), String(strings.Repeat("position", 1024))}
			hint, err := c.CreateContainer(ctx, key, kind, ContainerData{Values: values}, Options{})
			if err != nil {
				t.Fatal(err)
			}
			waitVisible(t, c, key, hint)
			stored := []Variant{values[1], values[2], values[3], values[4]}
			o := Options{Hint: hint}
			got, err := c.GetAt(ctx, key, 2, o)
			if err != nil || got != stored[2] {
				t.Fatal("positional read", got, err)
			}
			got, err = c.PopAt(ctx, key, 0, o)
			if err != nil || got != stored[0] {
				t.Fatal("positional pop", got, err)
			}
			count, err := c.RemoveAt(ctx, key, Position{Start: 0}, o)
			if err != nil || count != 1 {
				t.Fatal("positional delete", count, err)
			}
			remaining, err := c.GetContainer(ctx, key, o)
			if err != nil || !slices.Equal(remaining.Values, []Variant{stored[2], stored[3]}) {
				t.Fatal("positional remaining entries", remaining, err)
			}
		})
	}
}
