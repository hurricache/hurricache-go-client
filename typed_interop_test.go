package hurricache

import (
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	pb "github.com/hurricache/hurricache-go-client/proto/cachepb"
	"google.golang.org/protobuf/proto"
)

func mixedFixture() []Variant {
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 251)
	}
	return []Variant{None(), Bytes(nil), Int(0), Double(0), Int(math.MinInt64), Int(math.MaxInt64), Double(math.Copysign(0, -1)), Double(1.25), Bytes(data), Double(math.NaN())}
}

func TestJavaTypedFixtures(t *testing.T) {
	for name, codec := range map[string]CompressionCodec{"gzip": CompressionGZIP, "lz4": CompressionLZ4} {
		t.Run(name, func(t *testing.T) {
			config, _ := normalizeConfig(Config{CompressionCodec: codec})
			read := func(suffix string, message proto.Message) {
				t.Helper()
				raw, e := os.ReadFile(filepath.Join("testdata/java-codecs", name+".pb"+suffix))
				if e != nil {
					t.Fatal(e)
				}
				if e = proto.Unmarshal(raw, message); e != nil {
					t.Fatal(e)
				}
			}
			write := func(suffix string, message proto.Message) {
				t.Helper()
				if out := os.Getenv("HURRICACHE_CODEC_OUTPUT"); out != "" {
					raw, e := proto.Marshal(message)
					if e != nil {
						t.Fatal(e)
					}
					if e = os.WriteFile(filepath.Join(out, name+".pb"+suffix), raw, 0600); e != nil {
						t.Fatal(e)
					}
				}
			}
			batch := &pb.BatchValueResponse{}
			read(".typed", batch)
			got, e := decodeBatch(batch, config)
			if e != nil || !reflect.DeepEqual(got.Values, mixedFixture()) {
				t.Fatalf("Java typed fixtures %v %v", got, e)
			}
			encoded := &pb.BatchValueResponse{}
			for _, v := range mixedFixture() {
				wire, e := encodeValue(v, nil, config)
				if e != nil {
					t.Fatal(e)
				}
				encoded.ValueUnordered = append(encoded.ValueUnordered, wire)
			}
			write(".typed", encoded)
			cas := &pb.AtomicCasRes{}
			read(".cas", cas)
			if cas.Expected == nil {
				t.Fatal("CAS presence")
			}
			observed, e := variantResult(cas.Expected, config)
			if e != nil || observed != Double(math.Copysign(0, -1)) || cas.Expected.KeyHint.WeakHash == nil {
				t.Fatal("CAS numeric type/zero/hint")
			}
			v, _ := encodeValue(Double(math.Copysign(0, -1)), nil, config)
			write(".cas", &pb.AtomicCasRes{Expected: &pb.ValueResponse{ValueUnordered: v, KeyHint: &pb.KeyHint{WeakHash: Ptr(uint32(0))}}})
			updates := &pb.BatchMessage{}
			read(".updates", updates)
			if len(updates.BatchSingleMessages) != 3 {
				t.Fatal("update fixture count")
			}
			out := &pb.BatchMessage{}
			for i, row := range updates.BatchSingleMessages {
				u := row.BatchCommand.UpdateInContainerByKey
				if i < 2 {
					k := u.GetOrderedElementKey()
					if k == nil || k.Order == nil || *k.Order != 10 || k.GetClientId() != 17 {
						t.Fatal("ordered update key/CID")
					}
					raw, e := decode(k.Payload.Payload, k.Payload.Size, k.CompressionInfo, config)
					if e != nil || Bytes(raw) != mixedFixture()[8] {
						t.Fatal("compressed ordered selector")
					}
					if i == 0 {
						if _, ok := u.Values.(*pb.UpdateContainerRequest_NewOrder); !ok || u.GetNewOrder() != 0 {
							t.Fatal("absolute zero oneof")
						}
					} else if _, ok := u.Values.(*pb.UpdateContainerRequest_DeltaOrder); !ok || u.GetDeltaOrder() != -5 {
						t.Fatal("delta oneof")
					}
					encodedKey, e := encodeKey(raw, nil, Ptr(uint32(17)), config)
					if e != nil {
						t.Fatal(e)
					}
					u = &pb.UpdateContainerRequest{Key: u.Key, Keys: &pb.UpdateContainerRequest_OrderedElementKey{OrderedElementKey: &pb.OrderedKey{Payload: encodedKey.Payload, CompressionInfo: encodedKey.CompressionInfo, ClientId: encodedKey.ClientId, Order: Ptr(uint64(10))}}, Values: u.Values}
				} else {
					value, e := decodeValue(u.GetValue(), config)
					if e != nil || value == nil || *value != Int(0) || u.GetElementKey() == nil {
						t.Fatal("value tag four")
					}
					wire, _ := encodeValue(Int(0), nil, config)
					u = &pb.UpdateContainerRequest{Key: u.Key, Keys: u.Keys, Values: &pb.UpdateContainerRequest_Value{Value: wire}}
				}
				out.BatchSingleMessages = append(out.BatchSingleMessages, &pb.BatchSingleMessage{ReplicationCommandType: pb.ReplicationCommandType_UPDATE_IN_CONTAINER_BY_KEY, BatchCommand: &pb.BatchCommand{UpdateInContainerByKey: u}})
			}
			write(".updates", out)
		})
	}
}
