package hurricache

import (
	"bytes"
	"context"
	"testing"

	pb "github.com/hurricache/hurricache-go-client/proto/cachepb"
)

func TestCompressionConfigurationAndFieldPolicies(t *testing.T) {
	for _, threshold := range []int{0, 8, 1024} {
		supplied := threshold
		config, err := normalizeConfig(Config{CompressionThreshold: &supplied})
		if err != nil {
			t.Fatal(err)
		}
		supplied++
		if *config.CompressionThreshold != threshold {
			t.Fatal("configuration aliases caller")
		}
		for _, n := range []int{0, threshold, threshold + 1, 4096} {
			raw := make([]byte, n)
			for i := range raw {
				raw[i] = byte(i * 31)
			}
			body, info, err := encode(raw, true, config)
			if err != nil {
				t.Fatal(err)
			}
			if (info != nil) != (n > threshold) {
				t.Fatalf("threshold %d length %d", threshold, n)
			}
			decoded, err := decode(body, uint32(len(body)), info, config)
			if err != nil || !bytes.Equal(raw, decoded) {
				t.Fatalf("roundtrip: %v", err)
			}
		}
		raw := bytes.Repeat([]byte("x"), threshold+1)
		s := &session{ctx: context.Background(), config: config, key: &pb.Key{ClientId: Ptr(uint32(7))}, ttl: Ptr(uint64(9999))}
		for _, creation := range []bool{false, true} {
			for _, typ := range []ContainerType{List, Vector, Queue, Set, Map, OrderedMap, OrderedSet} {
				p := Payload{Data: raw}
				data := ContainerData{Values: []Payload{p}}
				switch typ {
				case Map:
					data = ContainerData{Entries: []Entry{{p, p}}}
				case OrderedMap:
					data = ContainerData{OrderedEntries: []OrderedEntry{{OrderedPayload{Payload: p}, p}}}
				case OrderedSet:
					data = ContainerData{OrderedValues: []OrderedPayload{{Payload: p}}}
				}
				items, err := encodeItems(s, typ, data, creation)
				if err != nil {
					t.Fatal(err)
				}
				it := items[0]
				if it.key != nil && it.key.CompressionInfo != nil {
					t.Fatal("map key compressed")
				}
				if it.orderedKey != nil && it.orderedKey.CompressionInfo == nil {
					t.Fatal("ordered key not compressed")
				}
				if it.orderedValue != nil && it.orderedValue.CompressionInfo == nil {
					t.Fatal("ordered value not compressed")
				}
				if it.value != nil && (it.value.CompressionInfo != nil) != (typ == OrderedMap) {
					t.Fatalf("policy %v creation=%v", typ, creation)
				}
				if creation && (typ == Map || typ == OrderedMap) && it.value.GetTtl() != 9999 {
					t.Fatal("map TTL")
				}
				if creation && typ == OrderedSet && it.orderedValue.GetTtl() != 9999 {
					t.Fatal("ordered TTL")
				}
			}
		}
	}
	if _, err := normalizeConfig(Config{CompressionThreshold: Ptr(-1)}); err == nil {
		t.Fatal("negative threshold accepted")
	}
	c, err := NewClient("localhost:1", Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.DefaultCompressionThreshold() != 1024 {
		t.Fatal("default threshold")
	}
}
