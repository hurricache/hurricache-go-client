package hurricache

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"math"
	"testing"

	pb "github.com/hurricache/hurricache-go-client/proto/cachepb"
	"google.golang.org/protobuf/proto"
)

func TestGZIPIndependentWireAndCorruption(t *testing.T) {
	config, _ := normalizeConfig(Config{})
	raw := bytes.Repeat([]byte("abc"), 1000)
	body, info, err := encode(raw, true, config)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	reader.Close()
	if err != nil || !bytes.Equal(raw, got) {
		t.Fatalf("GZIP wire: %v", err)
	}
	for _, kind := range []string{"header", "checksum", "truncated", "trailing", "declared", "limit", "legacy"} {
		t.Run(kind, func(t *testing.T) {
			data := bytes.Clone(body)
			ci := proto.Clone(info).(*pb.CompressedInfo)
			c := config
			switch kind {
			case "header":
				data[0] = 0
			case "checksum":
				data[len(data)-8] ^= 1
			case "truncated":
				data = data[:len(data)-2]
			case "trailing":
				data = append(data, []byte("garbage")...)
			case "declared":
				ci.RawSize = Ptr(*ci.RawSize - 1)
			case "limit":
				c.MaxDecodedBytes = len(raw) - 1
			case "legacy":
				data = []byte{0x30, 'a', 'b', 'c'}
			}
			if _, err := decode(data, uint32(len(data)), ci, c); err == nil {
				t.Fatal("invalid payload accepted")
			}
		})
	}
}

func TestCountAndBooleanChunkSemantics(t *testing.T) {
	for _, kind := range []addKind{addHead, addTail, addBefore, addAfter, addBool, addOrderedBool} {
		t.Run(string(rune('A'+kind)), func(t *testing.T) {
			calls := 0
			zeroAt := 0
			overflow := false
			conn := transport(t, func(_ context.Context, _ string, _ proto.Message) (any, error) {
				calls++
				count := uint32(2)
				if calls == zeroAt {
					count = 0
				}
				if overflow {
					count = math.MaxUint32
				}
				return &pb.IntResponse{Size: count}, nil
			}, nil)
			c, _ := NewClientWithConnection(conn, Config{MaxRequestBytes: 160, CompressionThreshold: Ptr(1000)})
			defer c.Close()
			data := ContainerData{Values: []Payload{{Data: bytes.Repeat([]byte("a"), 80)}, {Data: bytes.Repeat([]byte("b"), 80)}, {Data: bytes.Repeat([]byte("c"), 80)}}}
			typ := List
			if kind == addOrderedBool {
				typ = OrderedSet
				for _, p := range data.Values {
					data.OrderedValues = append(data.OrderedValues, OrderedPayload{Payload: p})
				}
				data.Values = nil
			}
			run := func(count bool) (addOutcome, error) {
				if count {
					return c.add(context.Background(), []byte("k"), typ, data, kind, 0, Payload{}, Options{}, true)
				}
				return c.add(context.Background(), []byte("k"), typ, data, kind, 0, Payload{}, Options{})
			}
			r, err := run(true)
			if err != nil || r.count != 6 || calls != 3 {
				t.Fatalf("count: %+v %v calls=%d", r, err, calls)
			}
			calls, zeroAt = 0, 2
			r, err = run(false)
			var partial *PartialError
			if !errors.As(err, &partial) || r.ok || calls != 2 || partial.CompletedChunks != 1 {
				t.Fatalf("boolean: %+v %v calls=%d", r, err, calls)
			}
			calls = 0
			r, err = run(true)
			if err != nil || r.count != 4 || calls != 3 {
				t.Fatalf("zero count: %+v %v calls=%d", r, err, calls)
			}
			calls, zeroAt, overflow = 0, 0, true
			_, err = run(true)
			if !errors.As(err, &partial) || calls != 2 {
				t.Fatalf("overflow: %v calls=%d", err, calls)
			}
		})
	}
}
