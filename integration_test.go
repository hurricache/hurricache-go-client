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
		codec = CompressionLZ4
	default:
		t.Fatal("HURRICACHE_TEST_CODEC must be gzip or lz4")
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
	if os.Getenv("HURRICACHE_TEST_SMART") == "1" {
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
	switch os.Getenv("HURRICACHE_TEST_CODEC") {
	case "":
		return map[string]CompressionCodec{"gzip": CompressionGZIP, "lz4": CompressionLZ4}
	case "gzip":
		return map[string]CompressionCodec{"gzip": CompressionGZIP}
	case "lz4":
		return map[string]CompressionCodec{"lz4": CompressionLZ4}
	default:
		t.Fatal("HURRICACHE_TEST_CODEC must be gzip or lz4")
		return nil
	}
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
			for i, value := range values {
				t.Run(fmt.Sprint("scalar", i), func(t *testing.T) {
					key := liveKey(t, c)
					h, e := c.Create(ctx, key, value, Options{})
					if e != nil {
						t.Fatal(e)
					}
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
						got, e := c.GetContainer(ctx, key, Options{Hint: h})
						if e != nil && kind != Queue {
							t.Fatal(e)
						}
						if kind == Queue {
							if e == nil {
								t.Fatal("full queue-content reads must be rejected")
							}
							size, err := c.GetSize(ctx, key, Options{Hint: h})
							if err != nil || size != uint32(len(values)) {
								t.Fatal("queue read removed entries", size, err)
							}
							for _, expected := range values {
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
							for _, v := range values {
								if !set[v] {
									t.Fatalf("missing typed set value %v", v)
								}
							}
						} else if kind == Map {
							m := map[string]Variant{}
							for _, v := range got.Entries {
								m[string(v.Key.Data)] = v.Value
							}
							for _, v := range d.Entries {
								if m[string(v.Key.Data)] != v.Value {
									t.Fatal("map pair mismatch")
								}
							}
						} else if !slices.Equal(got.Values, d.Values) || !slices.Equal(got.OrderedValues, d.OrderedValues) {
							// Map results carry server key metadata, so compare bytes/order/value explicitly.
							if kind != OrderedMap {
								t.Fatalf("typed collection %v", got)
							}
						}
						if kind == OrderedMap {
							if len(got.OrderedEntries) != len(d.OrderedEntries) {
								t.Fatal("map length")
							}
							for i, v := range got.OrderedEntries {
								want := d.OrderedEntries[i]
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
				_, e := c.CreateList(ctx, key, []Variant{Int(10), Double(1.5)}, Options{})
				if e != nil {
					t.Fatal(e)
				}
				n, e := c.AddInContainer(ctx, key, uint64(0), int64(2), Options{})
				if e != nil || n != int64(10) {
					t.Fatalf("position selector %v %v", n, e)
				}
				key = liveKey(t, c)
				_, e = c.CreateMap(ctx, key, []Entry{{Key: Payload{Data: []byte("entry")}, Value: Double(2.5)}}, Options{})
				if e != nil {
					t.Fatal(e)
				}
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
		got, e := c.GetList(ctx, key, Options{Hint: h})
		if e != nil || !reflect.DeepEqual(got, input) {
			t.Fatalf("continuation roundtrip %v", e)
		}
	}
}
