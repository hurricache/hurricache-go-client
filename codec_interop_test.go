package hurricache

import (
	"bytes"
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"

	pb "github.com/hurricache/hurricache-go-client/proto/cachepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func codecFixtureData(index int) []byte {
	sizes := []int{0, 1, 1023, 1024, 1025, 4096, 16384}
	b := make([]byte, sizes[index/2])
	x := uint32(1234567)
	for i := range b {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		b[i] = 42
		if index%2 == 1 {
			b[i] = byte(x)
		}
	}
	return b
}

func TestJavaCodecFixtures(t *testing.T) {
	for name, codec := range map[string]CompressionCodec{"gzip": CompressionGZIP, "lz4": CompressionLZ4} {
		t.Run(name, func(t *testing.T) {
			config, err := normalizeConfig(Config{CompressionCodec: codec})
			if err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(filepath.Join("testdata", "java-codecs", name+".pb"))
			if err != nil {
				t.Fatal(err)
			}
			batch := &pb.BatchValueResponse{}
			if err := proto.Unmarshal(raw, batch); err != nil {
				t.Fatal(err)
			}
			if len(batch.KeyOrdered) != 14 {
				t.Fatal("fixture count")
			}
			encoded := &pb.BatchValueResponse{}
			for i, k := range batch.KeyOrdered {
				data := codecFixtureData(i)
				got, err := decode(k.Payload.Payload, k.Payload.Size, k.CompressionInfo, config)
				if err != nil || !bytes.Equal(data, got) {
					t.Fatalf("key %d: %v", i, err)
				}
				v, err := decodeValue(batch.ValueUnordered[i], config)
				if err != nil || !bytes.Equal(data, v.Data) {
					t.Fatalf("value %d: %v", i, err)
				}
				if k.ClientId == nil || *k.ClientId != 0 || k.GetOrder() != uint64(i+1) {
					t.Fatal("metadata")
				}
				if k.CompressionInfo.GetEnabled() != (len(data) > 1024) {
					t.Fatal("threshold")
				}
				body, ci, err := encode(data, true, config)
				if err != nil {
					t.Fatal(err)
				}
				encoded.KeyOrdered = append(encoded.KeyOrdered, &pb.OrderedKey{Payload: &pb.KeyBinaryPayload{Payload: body, Size: uint32(len(body))}, CompressionInfo: ci, Order: Ptr(uint64(i + 1)), ClientId: Ptr(uint32(0))})
				value, err := encodeValue(Payload{Data: data}, nil, config)
				if err != nil {
					t.Fatal(err)
				}
				encoded.ValueUnordered = append(encoded.ValueUnordered, value)
				ordered, err := encodeOrdered(OrderedPayload{Payload: Payload{Data: data}, Order: Ptr(uint64(i + 1))}, nil, config)
				if err != nil {
					t.Fatal(err)
				}
				decoded, err := decodeOrdered(ordered, config)
				if err != nil || !bytes.Equal(decoded.Data, data) {
					t.Fatalf("ordered: %v", err)
				}
			}
			// Go-produced bytes are exported only by the explicit offline harness.
			if out := os.Getenv("HURRICACHE_CODEC_OUTPUT"); out != "" {
				body, err := proto.Marshal(encoded)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(out, name+".pb"), body, 0600); err != nil {
					t.Fatal(err)
				}
			}
			for _, reverse := range []bool{false, true} {
				conn := transport(t, nil, func(req proto.Message, stream grpc.ServerStreamingServer[pb.BatchValueResponse]) error {
					p := req.(*pb.KeyPositionRequest)
					if p.Reverse != reverse {
						t.Error("range direction lost")
					}
					indices := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}
					if reverse {
						slices.Reverse(indices)
					}
					chunk := &pb.BatchValueResponse{}
					for n, i := range indices {
						chunk.KeyOrdered = append(chunk.KeyOrdered, batch.KeyOrdered[i])
						chunk.ValueUnordered = append(chunk.ValueUnordered, batch.ValueUnordered[i])
						if len(chunk.KeyOrdered) == 3 || n == 13 {
							if err := stream.Send(chunk); err != nil {
								return err
							}
							chunk = &pb.BatchValueResponse{}
						}
					}
					return nil
				})
				c, err := NewClientWithConnection(conn, config)
				if err != nil {
					t.Fatal(err)
				}
				entries, err := c.StreamElementInRangeOrderedMap(context.Background(), []byte("map"), 1, 14, reverse, Options{})
				c.Close()
				if err != nil || len(entries) != 14 {
					t.Fatalf("stream: %v", err)
				}
				for n, entry := range entries {
					i := n
					if reverse {
						i = 13 - n
					}
					if entry.Key.Order == nil || *entry.Key.Order != uint64(i+1) || !bytes.Equal(entry.Key.Data, codecFixtureData(i)) || !bytes.Equal(entry.Value.Data, codecFixtureData(i)) {
						t.Fatal("stream bytes/order")
					}
				}
			}
			conn := transport(t, nil, func(_ proto.Message, stream grpc.ServerStreamingServer[pb.BatchValueResponse]) error {
				if err := stream.Send(&pb.BatchValueResponse{KeyOrdered: batch.KeyOrdered[:3], ValueUnordered: batch.ValueUnordered[:3]}); err != nil {
					return err
				}
				bad := proto.Clone(batch.ValueUnordered[10]).(*pb.Value)
				bad.CompressionInfo.RawSize = Ptr(uint32(math.MaxUint32))
				return stream.Send(&pb.BatchValueResponse{KeyOrdered: batch.KeyOrdered[10:11], ValueUnordered: []*pb.Value{bad}})
			})
			c, err := NewClientWithConnection(conn, config)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			entries, err := c.StreamOrderedMap(context.Background(), []byte("map"), Options{})
			var partial *PartialError
			if len(entries) != 3 || !errors.As(err, &partial) || partial.CompletedChunks != 1 || partial.CompletedItems != 3 || status.Code(err) != codes.ResourceExhausted {
				t.Fatalf("decoded prefix=%d error=%v", len(entries), err)
			}
		})
	}
}
