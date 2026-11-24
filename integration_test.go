package hurricache

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	cpb "github.com/hurricache/hurricache-go-client/proto/coordinatorpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"math"
	"net"
	"os"
	"reflect"
	"slices"
	"strconv"
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
	codec := CompressionGZIP
	switch os.Getenv("HURRICACHE_TEST_CODEC") {
	case "", "gzip":
	case "lz4":
		t.Skip("lz4 is not supported by the server")
	default:
		t.Fatal("HURRICACHE_TEST_CODEC must be gzip")
	}
	return newLiveClient(t, target, Config{Timeout: 5 * time.Second, CompressionCodec: codec})
}

// Discovery is deterministic; every data RPC still reaches the pinned standalone.
// This exercises smart clients without claiming live cluster acceptance.
func newLiveClient(t *testing.T, target string, config Config) *Client {
	t.Helper()
	var c *Client
	var e error
	config, e = normalizeConfig(config)
	if e != nil {
		t.Fatal(e)
	}
	smartEnv := os.Getenv("HURRICACHE_TEST_SMART")
	if smartEnv != "" && smartEnv != "1" {
		// Real cluster: a comma-separated coordinator list.
		smart, err := NewSmartClient(strings.Split(smartEnv, ","), SmartConfig{Client: config})
		if err != nil {
			t.Fatal(err)
		}
		c = smart.Client
		t.Cleanup(func() { _ = c.Close() })
		waitReady(t, smart)
		return c
	}
	if smartEnv == "1" {
		host, port, err := net.SplitHostPort(target)
		if err != nil {
			t.Fatal(err)
		}
		portNumber, err := strconv.ParseUint(port, 10, 16)
		if err != nil {
			t.Fatal(err)
		}
		coord := coordinatorTransport(t, func(g grpc.ServerStreamingServer[cpb.RoutingInfoData]) error {
			return g.Send(&cpb.RoutingInfoData{MaxShards: 1, PeerRouting: []*cpb.PeerRouting{{
				Target: &cpb.Endpoint{Ip: host, Port: uint32(portNumber)}, Role: cpb.NodeRole_MASTER, PartitionIds: []uint32{0},
			}}})
		})
		smart, err := NewSmartClient([]string{"live-coordinator"}, SmartConfig{Client: config,
			ConnectionFactory: func(ctx context.Context, address string) (Connection, error) {
				if address == "live-coordinator" {
					return Connection{Conn: coord}, nil
				}
				return dial(address, config)
			}})
		if err != nil {
			t.Fatal(err)
		}
		c = smart.Client
		t.Cleanup(func() { _ = c.Close() })
		waitReady(t, smart)
		return c
	}
	c, e = NewClient(target, config)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func liveCodecs(t *testing.T) map[string]CompressionCodec {
	t.Helper()
	// The server supports gzip only; lz4 is a local codec, not exercised live.
	switch os.Getenv("HURRICACHE_TEST_CODEC") {
	case "", "gzip":
		return map[string]CompressionCodec{"gzip": CompressionGZIP}
	case "lz4":
		t.Skip("lz4 is not supported by the server")
	default:
		t.Fatal("HURRICACHE_TEST_CODEC must be gzip")
	}
	return nil
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

// In a cluster a create may become visible asynchronously (relay, replication,
// stale snapshot) — poll key visibility by hint, up to 2s. Instant on standalone.
func waitVisible(t *testing.T, c *Client, key []byte, h *KeyHint) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, e := c.GetSize(context.Background(), key, Options{Hint: h})
		if status.Code(e) != codes.NotFound {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("key %s not visible after create: %v", key, e)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Live acceptance has no inherited expected failures from previous server builds.
func TestLiveOperationMatrix(t *testing.T) {
	for name, codec := range liveCodecs(t) {
		t.Run(name, func(t *testing.T) {
			target := os.Getenv("HURRICACHE_TEST_TARGET")
			if target == "" {
				t.Skip("standalone acceptance requires a successfully started pinned image")
			}
			c := newLiveClient(t, target, Config{ClientID: 77, Timeout: 10 * time.Second, CompressionCodec: codec})
			ctx := context.Background()
			values := []Variant{None(), Bytes(nil), Int(0), Double(0), Int(math.MinInt64), Int(math.MaxInt64), Double(-1.25),
				Double(math.Copysign(0, -1)), Double(math.NaN()), Double(math.Inf(1)), Double(math.Inf(-1)),
				Bytes(bytes.Repeat([]byte("x"), 8192))}
			t.Run("scalar-none-rejected", func(t *testing.T) {
				// None is not a storable type — the server rejects it.
				// In a cluster the create may be relayed (fire-and-forget): then OK without storage.
				h, e := c.Create(ctx, liveKey(t, c), None(), Options{})
				if e == nil && (h == nil || !h.WasRelayed) {
					t.Fatal("create with None value must be rejected")
				}
			})
			for i, value := range values[1:] {
				t.Run(fmt.Sprint("scalar", i+1), func(t *testing.T) {
					key := liveKey(t, c)
					h, e := c.Create(ctx, key, value, Options{})
					if e != nil {
						t.Fatal(e)
					}
					waitVisible(t, c, key, h)
					o := Options{Hint: h}
					got, e := c.Get(ctx, key, o)
					if e != nil || got != value {
						t.Fatalf("get %v %v", got, e)
					}
					previous, e := c.Replace(ctx, key, Int(42), o)
					if e != nil || previous != value {
						t.Fatalf("replace %v %v", previous, e)
					}
					got, e = c.Pop(ctx, key, o)
					if e != nil || got != Int(42) {
						t.Fatalf("pop %v %v", got, e)
					}
				})
			}
			for _, includeNone := range []bool{false, true} {
				values := values
				group := "including-none/"
				if !includeNone {
					values = values[1:]
					group = "stored-types/"
				}
				// None is dropped by the server on insert (partial insert)
				want := make([]Variant, 0, len(values))
				for _, v := range values {
					if v.IsNone() {
						continue
					}
					want = append(want, v)
				}
				for _, kind := range []ContainerType{List, Vector, Queue, Set, Map, OrderedMap, OrderedSet} {
					t.Run(group+kind.String(), func(t *testing.T) {
						key := liveKey(t, c)
						d := ContainerData{Values: values}
						switch kind {
						case Map:
							d = ContainerData{}
							for i, v := range values {
								d.Entries = append(d.Entries, Entry{Key: Payload{Data: []byte(fmt.Sprint(i))}, Value: v})
							}
						case OrderedMap:
							d = ContainerData{}
							for i, v := range values {
								d.OrderedEntries = append(d.OrderedEntries, OrderedEntry{Key: OrderedPayload{Payload: Payload{Data: []byte(fmt.Sprint(i))}, Order: Ptr(uint64(i + 1))}, Value: v})
							}
						case OrderedSet:
							d = ContainerData{}
							for i, v := range values {
								d.OrderedValues = append(d.OrderedValues, Ordered(v, Ptr(uint64(i+1))))
							}
						}
						h, e := c.CreateContainer(ctx, key, kind, d, Options{})
						if e != nil {
							t.Fatal(e)
						}
						waitVisible(t, c, key, h)
						got, e := c.GetContainer(ctx, key, Options{Hint: h})
						if e != nil && kind != Queue {
							t.Fatal(e)
						}
						if kind == Queue {
							if e == nil {
								t.Fatal("full queue-content reads must be rejected")
							}
							size, err := c.GetSize(ctx, key, Options{Hint: h})
							if err != nil || size != uint32(len(want)) {
								t.Fatal("queue read removed entries", size, err)
							}
							for _, expected := range want {
								actual, err := c.PopHead(ctx, key, Options{Hint: h})
								if err != nil || actual != expected {
									t.Fatal("queue pop after get", actual, err)
								}
							}
							return
						}
						if kind == Set {
							set := map[Variant]bool{}
							for _, v := range got.Values {
								set[v] = true
							}
							for _, v := range want {
								if !set[v] {
									t.Fatalf("missing typed set value %v", v)
								}
							}
						} else if kind == Map {
							m := map[string]Variant{}
							for _, v := range got.Entries {
								m[string(v.Key.Data)] = v.Value
							}
							// keys are built by the index in values; None is dropped by the server
							for i, v := range values {
								if v.IsNone() {
									continue
								}
								if m[fmt.Sprint(i)] != v {
									t.Fatal("map pair mismatch")
								}
							}
						} else if kind == OrderedSet {
							ov := make([]Variant, 0, len(got.OrderedValues))
							for _, v := range got.OrderedValues {
								ov = append(ov, v.Value())
							}
							if !slices.Equal(ov, want) {
								t.Fatalf("typed collection %v", got)
							}
						} else if !slices.Equal(got.Values, want) || len(got.OrderedValues) != 0 {
							// Map results carry server key metadata, so compare bytes/order/value explicitly.
							if kind != OrderedMap {
								t.Fatalf("typed collection %v", got)
							}
						}
						if kind == OrderedMap {
							if len(got.OrderedEntries) != len(want) {
								t.Fatal("map length")
							}
							// offset: None in d.OrderedEntries[0] was dropped by the server
							offset := len(d.OrderedEntries) - len(got.OrderedEntries)
							for i, v := range got.OrderedEntries {
								want := d.OrderedEntries[i+offset]
								if v.Value != want.Value || string(v.Key.Data) != string(want.Key.Data) || v.Key.Order == nil || *v.Key.Order != *want.Key.Order {
									t.Fatal("ordered map pairing/order")
								}
							}
						}
					})
				}
			}
			t.Run("atomics", func(t *testing.T) {
				key := liveKey(t, c)
				h, e := c.Create(ctx, key, Int(10), Options{})
				if e != nil {
					t.Fatal(e)
				}
				waitVisible(t, c, key, h)
				o := Options{Hint: h}
				n, e := c.Add(ctx, key, int64(2), o)
				if e != nil || n != int64(10) {
					t.Fatalf("fetch-add %v %v", n, e)
				}
				result, e := c.CompareAndSet(ctx, key, int64(12), int64(14), o)
				if e != nil || !result.Swapped {
					t.Fatalf("CAS %v %v", result, e)
				}
				result, e = c.CompareAndSet(ctx, key, int64(12), int64(15), o)
				if e != nil || result.Swapped || result.Expected == nil || *result.Expected != Int(14) {
					t.Fatalf("failed CAS %v %v", result, e)
				}
				_, e = c.Replace(ctx, key, Double(1.5), o)
				if e != nil {
					t.Fatal(e)
				}
				n, e = c.Sub(ctx, key, 0.25, o)
				if e != nil || n != float64(1.5) {
					t.Fatalf("double %v %v", n, e)
				}
			})
			t.Run("container-atomics", func(t *testing.T) {
				key := liveKey(t, c)
				h, e := c.CreateList(ctx, key, []Variant{Int(10), Double(1.5)}, Options{})
				if e != nil {
					t.Fatal(e)
				}
				waitVisible(t, c, key, h)
				n, e := c.AddInContainer(ctx, key, uint64(0), int64(2), Options{})
				if e != nil || n != int64(10) {
					t.Fatalf("position selector %v %v", n, e)
				}
				key = liveKey(t, c)
				h, e = c.CreateMap(ctx, key, []Entry{{Key: Payload{Data: []byte("entry")}, Value: Double(2.5)}}, Options{})
				if e != nil {
					t.Fatal(e)
				}
				waitVisible(t, c, key, h)
				n, e = c.ExchangeInContainer(ctx, key, []byte("entry"), 1.25, Options{})
				if e != nil || n != float64(2.5) {
					t.Fatalf("key selector %v %v", n, e)
				}
			})
		})
	}
}
func TestLiveCompressionAndBatchRoundTrip(t *testing.T) {
	c := liveClient(t)
	ctx := context.Background()
	for _, codec := range liveCodecs(t) {
		c.config.CompressionCodec = codec // exclusive local test client
		key := liveKey(t, c)
		input := make([]Variant, 1025)
		for i := range input {
			if i%2 == 0 {
				input[i] = Int(int64(i))
			} else {
				input[i] = Bytes(codecFixtureData(13))
			}
		}
		h, e := c.CreateList(ctx, key, input, Options{})
		if e != nil {
			t.Fatal(e)
		}
		waitVisible(t, c, key, h)
		got, e := c.GetList(ctx, key, Options{Hint: h})
		if e != nil || !reflect.DeepEqual(got, input) {
			t.Fatalf("continuation roundtrip %v", e)
		}
	}
}
