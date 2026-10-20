package hurricache

import (
	"bytes"
	"context"
	"errors"
	"math"
	"testing"
	"time"

	pb "github.com/hurricache/hurricache-go-client/proto/cachepb"
	cpb "github.com/hurricache/hurricache-go-client/proto/coordinatorpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestCodecValidation(t *testing.T) {
	invalid := Config{CompressionCodec: CompressionCodec(255)}
	if _, err := NewClient("localhost:1", invalid); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	if _, err := NewClientWithConnection(transport(t, nil, nil), invalid); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	if _, err := NewSmartClient([]string{"localhost:1"}, SmartConfig{Client: invalid}); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
}

func TestLZ4BoundariesAndMalformed(t *testing.T) {
	for _, threshold := range []int{0, 8, 1024} {
		config, err := normalizeConfig(Config{CompressionCodec: CompressionLZ4, CompressionThreshold: Ptr(threshold)})
		if err != nil {
			t.Fatal(err)
		}
		for _, size := range []int{0, threshold, threshold + 1, 65536} {
			for _, random := range []bool{false, true} {
				raw := make([]byte, size)
				x := uint32(1234567)
				for i := range raw {
					x ^= x << 13
					x ^= x >> 17
					x ^= x << 5
					raw[i] = 42
					if random {
						raw[i] = byte(x)
					}
				}
				body, info, err := encode(raw, true, config)
				if err != nil || (info != nil) != (size > threshold) {
					t.Fatalf("encode %d: %v", size, err)
				}
				got, err := decode(body, uint32(len(body)), info, config)
				if err != nil || !bytes.Equal(raw, got) {
					t.Fatalf("roundtrip %d random=%v: %v", size, random, err)
				}
				if random && size == 65536 && len(body) <= len(raw) {
					t.Fatal("incompressible fixture unexpectedly shrank")
				}
				plain, ci, err := encode(raw, false, config)
				if err != nil || ci != nil || !bytes.Equal(plain, raw) {
					t.Fatal("field policy")
				}
			}
		}
	}
	c, _ := normalizeConfig(Config{CompressionCodec: CompressionLZ4})
	raw := bytes.Repeat([]byte("abc"), 2048)
	body, info, err := encode(raw, true, c)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"truncated", "offset", "size", "raw-small", "raw-large", "missing", "zero", "limit", "allocation", "wrong-codec"} {
		t.Run(kind, func(t *testing.T) {
			data := bytes.Clone(body)
			ci := proto.Clone(info).(*pb.CompressedInfo)
			config := c
			encodedSize := uint32(len(data))
			want := codes.DataLoss
			switch kind {
			case "truncated":
				data = data[:len(data)-2]
				encodedSize = uint32(len(data))
			case "offset":
				data = []byte{0, 0, 0}
				encodedSize = uint32(len(data))
			case "size":
				encodedSize++
			case "raw-small":
				ci.RawSize = Ptr(uint32(len(raw) - 1))
			case "raw-large":
				ci.RawSize = Ptr(uint32(len(raw) + 1))
			case "missing":
				ci.RawSize = nil
			case "zero":
				ci.RawSize = Ptr(uint32(0))
			case "limit":
				config.MaxDecodedBytes = len(raw) - 1
				want = codes.ResourceExhausted
			case "allocation":
				ci.RawSize = Ptr(uint32(math.MaxUint32))
				want = codes.ResourceExhausted
			case "wrong-codec":
				config.CompressionCodec = CompressionGZIP
			}
			if _, err := decode(data, encodedSize, ci, config); status.Code(err) != want {
				t.Fatalf("got %v want %v", err, want)
			}
		})
	}
}

func TestCodecsChunkingAndPartialProgress(t *testing.T) {
	for name, codec := range map[string]CompressionCodec{"gzip": CompressionGZIP, "lz4": CompressionLZ4} {
		for _, smart := range []bool{false, true} {
			t.Run(name+map[bool]string{false: "/direct", true: "/smart"}[smart], func(t *testing.T) {
				config, _ := normalizeConfig(Config{CompressionCodec: codec, MaxRequestBytes: 9000, ClientID: 77})
				calls, backups := 0, 0
				fail := false
				var cancelOnAdd context.CancelFunc
				var expiration uint64
				keyData := bytes.Repeat([]byte("k"), 2048)
				conn := transport(t, func(rpcCtx context.Context, _ string, req proto.Message) (any, error) {
					calls++
					if proto.Size(req) > config.MaxRequestBytes {
						t.Error("request limit")
					}
					var key *pb.Key
					var keys []*pb.OrderedKey
					var vals []*pb.Value
					switch r := req.(type) {
					case *pb.CreateContainerRequest:
						key, keys, vals = r.Key, r.KeyOrdered, r.ValueUnordered
						expiration = r.GetTtl()
					case *pb.AddToRequest:
						key, keys, vals = r.Key, r.KeyOrdered, r.ValueUnordered
						if r.Key.KeyHint.GetWeekHash() != 9 {
							t.Error("continuation hint")
						}
					default:
						t.Fatalf("unexpected %T", req)
					}
					if key.ClientId == nil || *key.ClientId != 0 {
						t.Error("explicit client ID zero")
					}
					decodedKey, err := decode(key.Payload.Payload, key.Payload.Size, key.CompressionInfo, config)
					if err != nil || !key.CompressionInfo.GetEnabled() || !bytes.Equal(decodedKey, keyData) {
						t.Fatalf("compressed top-level key: %v", err)
					}
					if len(keys) != 1 || len(vals) != 1 {
						t.Fatal("chunk pair count")
					}
					if keys[0].CompressionInfo == nil || vals[0].CompressionInfo == nil {
						t.Fatal("ordered map compression")
					}
					b, err := decode(keys[0].Payload.Payload, keys[0].Payload.Size, keys[0].CompressionInfo, config)
					if err != nil || !bytes.Equal(b, codecFixtureData(11)) {
						t.Fatalf("key: %v", err)
					}
					v, err := decodeValue(vals[0], config)
					if err != nil || !bytes.Equal(v.Data, b) || v.ExpiresAtMillis == nil || *v.ExpiresAtMillis != expiration {
						t.Fatalf("value/TTL: %v", err)
					}
					if _, ok := req.(*pb.CreateContainerRequest); ok {
						return &pb.KeyHintResponse{KeyHint: &pb.KeyHint{WeekHash: Ptr(uint32(9))}}, nil
					}
					if cancelOnAdd != nil {
						cancelOnAdd()
						<-rpcCtx.Done()
						return nil, status.FromContextError(rpcCtx.Err()).Err()
					}
					if fail {
						return nil, status.Error(codes.Unavailable, "after progress")
					}
					return &pb.IntResponse{Size: 1}, nil
				}, nil)
				var c *Client
				if smart {
					backup := transport(t, func(_ context.Context, _ string, _ proto.Message) (any, error) {
						backups++
						return nil, status.Error(codes.Internal, "unexpected replay")
					}, nil)
					coord := coordinatorTransport(t, func(g grpc.ServerStreamingServer[cpb.RoutingInfoData]) error { return g.Send(topologyData("m", "b")) })
					s, err := NewSmartClient([]string{"c"}, SmartConfig{Client: config, Mode: Backup, ConnectionFactory: func(_ context.Context, target string) (Connection, error) {
						return Connection{Conn: map[string]*grpc.ClientConn{"c": coord, "m": conn, "b": backup}[target]}, nil
					}})
					if err != nil {
						t.Fatal(err)
					}
					waitReady(t, s)
					c = s.Client
				} else {
					var err error
					c, err = NewClientWithConnection(conn, config)
					if err != nil {
						t.Fatal(err)
					}
				}
				defer c.Close()
				entries := []OrderedEntry{}
				for i := 0; i < 3; i++ {
					entries = append(entries, OrderedEntry{Key: OrderedPayload{Payload: Payload{Data: codecFixtureData(11)}, Order: Ptr(uint64(i + 1))}, Value: Payload{Data: codecFixtureData(11)}})
				}
				options := Options{ClientID: Ptr(uint32(0)), TTL: Ptr(time.Minute)}
				if _, err := c.CreateOrderedMap(context.Background(), keyData, entries, options); err != nil || calls != 3 {
					t.Fatalf("chunks=%d err=%v", calls, err)
				}
				calls, fail = 0, true
				_, err := c.CreateOrderedMap(context.Background(), keyData, entries, options)
				var partial *PartialError
				if !errors.As(err, &partial) || partial.CompletedChunks != 1 || partial.CompletedItems != 1 || calls != 2 || backups != 0 {
					t.Fatalf("partial=%v calls=%d backups=%d", err, calls, backups)
				}
				ctx, cancel := context.WithCancel(context.Background())
				cancelOnAdd, calls = cancel, 0
				_, err = c.CreateOrderedMap(ctx, keyData, entries, options)
				cancel()
				if !errors.As(err, &partial) || partial.CompletedChunks != 1 || status.Code(err) != codes.Canceled || calls != 2 || backups != 0 {
					t.Fatalf("canceled prefix=%v calls=%d backups=%d", err, calls, backups)
				}
				ctx, cancel = context.WithCancel(context.Background())
				cancel()
				calls = 0
				if _, err := c.CreateOrderedMap(ctx, keyData, entries, options); status.Code(err) != codes.Canceled || calls != 0 {
					t.Fatalf("cancellation: %v", err)
				}
			})
		}
	}
}
