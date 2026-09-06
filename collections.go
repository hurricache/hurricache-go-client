package hurricache

import (
	"context"
	pb "github.com/hurricache/hurricache-go-client/proto/cachepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"io"
	"math"
	"slices"
)

type item struct {
	key          *pb.Key
	orderedKey   *pb.OrderedKey
	value        *pb.Value
	orderedValue *pb.OrderedValue
}

func encodeItems(s *session, t ContainerType, d ContainerData, creation bool) ([]item, error) {
	if t < Vector || t > OrderedSet {
		return nil, status.Error(codes.InvalidArgument, "unsupported container type")
	}
	if (t == Map && (len(d.Values)+len(d.OrderedValues)+len(d.OrderedEntries) > 0)) ||
		(t == OrderedMap && (len(d.Values)+len(d.OrderedValues)+len(d.Entries) > 0)) ||
		(t == OrderedSet && (len(d.Values)+len(d.Entries)+len(d.OrderedEntries) > 0)) ||
		(t != Map && t != OrderedMap && t != OrderedSet && (len(d.OrderedValues)+len(d.Entries)+len(d.OrderedEntries) > 0)) {
		return nil, status.Error(codes.InvalidArgument, "collection data does not match container type")
	}
	var result []item
	var ttl *uint64
	if creation && (t == Map || t == OrderedMap || t == OrderedSet) {
		ttl = s.ttl
	}
	for _, p := range d.Values {
		if e := s.ctx.Err(); e != nil {
			return nil, status.FromContextError(e).Err()
		}
		v, e := encodeValue(p, ttl, s.config)
		if e != nil {
			return nil, e
		}
		result = append(result, item{value: v})
	}
	for _, p := range d.OrderedValues {
		if e := s.ctx.Err(); e != nil {
			return nil, status.FromContextError(e).Err()
		}
		v, e := encodeOrdered(p, ttl, s.config)
		if e != nil {
			return nil, e
		}
		result = append(result, item{orderedValue: v})
	}
	for _, p := range d.Entries {
		if e := s.ctx.Err(); e != nil {
			return nil, status.FromContextError(e).Err()
		}
		id := s.key.ClientId
		if p.Key.ClientID != nil {
			id = p.Key.ClientID
		}
		k, e := encodeKey(p.Key.Data, p.Key.Hint, id, s.config)
		if e != nil {
			return nil, e
		}
		v, e := encodeValue(p.Value, ttl, s.config)
		if e != nil {
			return nil, e
		}
		result = append(result, item{key: k, value: v})
	}
	for _, p := range d.OrderedEntries {
		if e := s.ctx.Err(); e != nil {
			return nil, status.FromContextError(e).Err()
		}
		b, _, e := encode(p.Key.Data, false, s.config)
		if e != nil {
			return nil, e
		}
		order := p.Key.Order
		if order == nil {
			order = Ptr(uint64(0))
		}
		id := s.key.ClientId
		if p.Key.ClientID != nil {
			id = p.Key.ClientID
		}
		k := &pb.OrderedKey{Payload: &pb.KeyBinaryPayload{Payload: b, Size: uint32(len(b))}, Order: copyPtr(order), ClientId: copyPtr(id), KeyHint: hintPB(p.Key.Hint)}
		v, e := encodeValue(p.Value, ttl, s.config)
		if e != nil {
			return nil, e
		}
		result = append(result, item{orderedKey: k, value: v})
	}
	return result, nil
}
func addRequest(k *pb.Key, t *ContainerType, pos *uint32, items []item) *pb.AddToRequest {
	r := &pb.AddToRequest{Key: k, Type: copyPtr(t), Pos: copyPtr(pos)}
	for _, it := range items {
		if it.key != nil {
			r.KeyUnordered = append(r.KeyUnordered, it.key)
		}
		if it.orderedKey != nil {
			r.KeyOrdered = append(r.KeyOrdered, it.orderedKey)
		}
		if it.value != nil {
			r.ValueUnordered = append(r.ValueUnordered, it.value)
		}
		if it.orderedValue != nil {
			r.ValueOrdered = append(r.ValueOrdered, it.orderedValue)
		}
	}
	return r
}
func createRequest(k *pb.Key, t ContainerType, ttl *uint64, items []item) *pb.CreateContainerRequest {
	a := addRequest(k, nil, nil, items)
	return &pb.CreateContainerRequest{Key: k, Type: t, Ttl: copyPtr(ttl), KeyUnordered: a.KeyUnordered, KeyOrdered: a.KeyOrdered, ValueUnordered: a.ValueUnordered, ValueOrdered: a.ValueOrdered}
}

type span struct{ start, end int }

// split validates every individual item before any mutation, then packs by actual
// serialized request size. A map pair is indivisible. Binary search avoids quadratic
// repeated prefix sizing for large collections.
func split(s *session, n int, build func(int, int) proto.Message) ([]span, error) {
	if proto.Size(build(0, 0)) > s.config.MaxRequestBytes {
		return nil, status.Error(codes.ResourceExhausted, "request header exceeds MaxRequestBytes")
	}
	for i := 0; i < n; i++ {
		if e := s.ctx.Err(); e != nil {
			return nil, status.FromContextError(e).Err()
		}
		if proto.Size(build(i, i+1)) > s.config.MaxRequestBytes {
			return nil, status.Errorf(codes.ResourceExhausted, "item %d exceeds MaxRequestBytes", i)
		}
	}
	var parts []span
	for start := 0; start < n; {
		lo, hi := start+1, n
		for lo < hi {
			mid := lo + (hi-lo+1)/2
			if proto.Size(build(start, mid)) <= s.config.MaxRequestBytes {
				lo = mid
			} else {
				hi = mid - 1
			}
		}
		parts = append(parts, span{start, lo})
		start = lo
	}
	return parts, nil
}

// CreateContainer creates any supported container and inserts all initial items.
// Large inputs use one create followed by bounded additions, preserving map pairs.
func (c *Client) CreateContainer(ctx context.Context, key []byte, t ContainerType, data ContainerData, o Options) (*KeyHint, error) {
	return perform(c, ctx, key, o, false, func(s *session) (*KeyHint, error) {
		items, e := encodeItems(s, t, data, true)
		if e != nil {
			return nil, e
		}
		// Reserve the maximum hint width so the returned hint always fits later chunks.
		reserve := proto.Clone(s.key).(*pb.Key)
		reserve.KeyHint = &pb.KeyHint{WeekHash: Ptr(uint32(math.MaxUint32)), StrongHash: Ptr(uint32(math.MaxUint32))}
		parts, e := split(s, len(items), func(i, j int) proto.Message { return createRequest(reserve, t, s.ttl, items[i:j]) })
		if e != nil {
			return nil, e
		}
		if len(parts) == 0 {
			parts = []span{{0, 0}}
		}
		first := parts[0]
		r, e := unary(s, createRequest(s.key, t, s.ttl, items[first.start:first.end]), s.stub.CreateContainer)
		if e != nil {
			return nil, e
		}
		hint := hintGo(r.KeyHint)
		k := proto.Clone(s.key).(*pb.Key)
		if r.KeyHint != nil {
			k.KeyHint = r.KeyHint
		}
		done, chunks := first.end, 1
		for _, p := range parts[1:] {
			a := addRequest(k, Ptr(t), nil, items[p.start:p.end])
			if t == List || t == Vector || t == Queue {
				r, e := unary(s, a, s.stub.AddElementToTail)
				if e != nil {
					return hint, partial(chunks, done, e)
				}
				if !r.Value {
					return hint, partial(chunks, done, status.Error(codes.Aborted, "server rejected continuation chunk"))
				}
			} else {
				_, e := unary(s, a, s.stub.AddElement)
				if e != nil {
					return hint, partial(chunks, done, e)
				}
			}
			done += p.end - p.start
			chunks++
		}
		return hint, nil
	})
}

type addKind uint8

const (
	addGeneric addKind = iota
	addHead
	addTail
	addPosition
	addBefore
	addAfter
	addBool
	addOrderedBool
)

type addOutcome struct {
	count uint32
	ok    bool
}

func (c *Client) add(ctx context.Context, key []byte, t ContainerType, data ContainerData, kind addKind, pos uint32, pivot Payload, o Options) (addOutcome, error) {
	return perform(c, ctx, key, o, true, func(s *session) (addOutcome, error) {
		items, e := encodeItems(s, t, data, false)
		if e != nil {
			return addOutcome{}, e
		}
		if kind == addPosition && uint64(pos)+uint64(len(items)) > math.MaxUint32 {
			return addOutcome{}, status.Error(codes.OutOfRange, "insertion position overflows uint32")
		}
		var pv *pb.Value
		if kind == addBefore || kind == addAfter {
			pv, e = encodeValue(pivot, nil, s.config)
			if e != nil {
				return addOutcome{}, e
			}
		}
		build := func(i, j int) proto.Message {
			var position *uint32
			if kind == addPosition {
				position = Ptr(pos + uint32(i))
			}
			if kind == addBool {
				position = Ptr(uint32(math.MaxUint32))
			}
			if kind == addOrderedBool {
				position = Ptr(uint32(i))
			}
			var containerType *ContainerType
			if t == Map || t == OrderedMap {
				containerType = Ptr(t)
			}
			a := addRequest(s.key, containerType, position, items[i:j])
			if kind == addBefore || kind == addAfter {
				return &pb.AddToValRequest{Key: s.key, IsBefore: kind == addBefore, Pos: pv, Value: a.ValueUnordered}
			}
			return a
		}
		parts, e := split(s, len(items), build)
		if e != nil {
			return addOutcome{}, e
		}
		// Head insertion reverses items within each RPC, so forward chunks retain
		// the same overall reversal as one RPC. Relative-after preserves each
		// RPC's order and therefore needs reverse chunk traversal.
		if kind == addAfter {
			slices.Reverse(parts)
		}
		result := addOutcome{ok: true}
		done, chunks := 0, 0
		for _, p := range parts {
			req := build(p.start, p.end)
			var ok = true
			switch kind {
			case addHead, addTail:
				f := s.stub.AddElementToTail
				if kind == addHead {
					f = s.stub.AddElementToHead
				}
				r, err := unary(s, req.(*pb.AddToRequest), f)
				e = err
				if err == nil {
					ok = r.Value
				}
			case addBefore, addAfter:
				r, err := unary(s, req.(*pb.AddToValRequest), s.stub.AddElementToPositionByValue)
				e = err
				if err == nil {
					ok = r.Value
				}
			default:
				r, err := unary(s, req.(*pb.AddToRequest), s.stub.AddElement)
				e = err
				if err == nil {
					if uint64(result.count)+uint64(r.Size) > math.MaxUint32 {
						return result, partial(chunks+1, done+p.end-p.start, status.Error(codes.OutOfRange, "count overflows uint32"))
					}
					result.count += r.Size
					if kind == addBool || kind == addOrderedBool {
						ok = r.Size > 0
					}
				}
			}
			if e != nil {
				return result, partial(chunks, done, e)
			}
			if !ok {
				result.ok = false
				if len(parts) > 1 {
					return result, partial(chunks+1, done, status.Error(codes.Aborted, "server rejected chunk"))
				}
				return result, nil
			}
			done += p.end - p.start
			chunks++
		}
		return result, nil
	})
}

// RemoveFromContainer removes explicit value/key batches and returns the server count.
func (c *Client) RemoveFromContainer(ctx context.Context, key []byte, removal Removal, o Options) (uint32, error) {
	return perform(c, ctx, key, o, true, func(s *session) (uint32, error) {
		if removal.Type < Vector || removal.Type > OrderedSet {
			return 0, status.Error(codes.InvalidArgument, "unsupported container type")
		}
		var items []item
		for _, v := range removal.Values {
			if e := s.ctx.Err(); e != nil {
				return 0, status.FromContextError(e).Err()
			}
			p, e := encodeValue(v, nil, s.config)
			if e != nil {
				return 0, e
			}
			items = append(items, item{value: p})
		}
		for _, k := range removal.Keys {
			if e := s.ctx.Err(); e != nil {
				return 0, status.FromContextError(e).Err()
			}
			p, e := encodeKey(k.Data, k.Hint, s.key.ClientId, s.config)
			if e != nil {
				return 0, e
			}
			items = append(items, item{key: p})
		}
		build := func(i, j int) proto.Message {
			a := addRequest(s.key, nil, nil, items[i:j])
			return &pb.RemoveFromContainerRequest{Key: s.key, Type: Ptr(removal.Type), Values: a.ValueUnordered, Keys: a.KeyUnordered}
		}
		parts, e := split(s, len(items), build)
		if e != nil {
			return 0, e
		}
		var total uint32
		chunks, done := 0, 0
		for _, p := range parts {
			r, e := unary(s, build(p.start, p.end).(*pb.RemoveFromContainerRequest), s.stub.RemoveFromContainerByKeyValue)
			if e != nil {
				return total, partial(chunks, done, e)
			}
			if uint64(total)+uint64(r.Size) > math.MaxUint32 {
				return total, partial(chunks+1, done+p.end-p.start, status.Error(codes.OutOfRange, "count overflows uint32"))
			}
			total += r.Size
			done += p.end - p.start
			chunks++
		}
		return total, nil
	})
}

func decodeKey(k *pb.Key, c Config) (Payload, error) {
	if k == nil || k.Payload == nil {
		return Payload{}, status.Error(codes.DataLoss, "map key lacks payload")
	}
	b, e := decode(k.Payload.Payload, k.Payload.Size, k.CompressionInfo, c)
	return Payload{Data: b, Hint: hintGo(k.KeyHint), ClientID: copyPtr(k.ClientId), Compression: compressionGo(k.CompressionInfo)}, e
}
func decodeBatch(b *pb.BatchValueResponse, c Config) (ContainerData, error) {
	var d ContainerData
	if len(b.KeyUnordered) > 0 && len(b.KeyOrdered) > 0 {
		return d, status.Error(codes.DataLoss, "mixed map key representations")
	}
	nk := len(b.KeyUnordered) + len(b.KeyOrdered)
	if nk > 0 && (nk != len(b.ValueUnordered) || len(b.ValueOrdered) > 0) {
		return d, status.Error(codes.DataLoss, "map key/value counts differ")
	}
	for i, v := range b.ValueUnordered {
		p, e := decodeValue(v, c)
		if e != nil {
			return d, e
		}
		if p == nil {
			return d, status.Error(codes.DataLoss, "nil collection value")
		}
		if len(b.KeyUnordered) > 0 {
			k, e := decodeKey(b.KeyUnordered[i], c)
			if e != nil {
				return d, e
			}
			d.Entries = append(d.Entries, Entry{k, *p})
		} else if len(b.KeyOrdered) > 0 {
			k := b.KeyOrdered[i]
			if k == nil {
				return d, status.Error(codes.DataLoss, "nil ordered map key")
			}
			kp, e := decodeKey(&pb.Key{Payload: k.Payload, CompressionInfo: k.CompressionInfo, KeyHint: k.KeyHint, ClientId: k.ClientId}, c)
			if e != nil {
				return d, e
			}
			d.OrderedEntries = append(d.OrderedEntries, OrderedEntry{OrderedPayload{kp, copyPtr(k.Order)}, *p})
		} else {
			d.Values = append(d.Values, *p)
		}
	}
	for _, v := range b.ValueOrdered {
		p, e := decodeOrdered(v, c)
		if e != nil {
			return d, e
		}
		if p == nil {
			return d, status.Error(codes.DataLoss, "nil ordered value")
		}
		d.OrderedValues = append(d.OrderedValues, *p)
	}
	return d, nil
}
func collect(s *session, pos *Position) (ContainerData, error) {
	var stream grpc.ServerStreamingClient[pb.BatchValueResponse]
	var err error
	var md metadata.MD
	if pos == nil {
		req := &pb.GetRequest{Key: s.key}
		if proto.Size(req) > s.config.MaxRequestBytes {
			return ContainerData{}, status.Error(codes.ResourceExhausted, "request too large")
		}
		stream, err = s.stub.GetContainer(s.ctx, req, grpc.Trailer(&md), grpc.MaxCallRecvMsgSize(s.config.MaxDecodedBytes), grpc.MaxCallSendMsgSize(s.config.MaxRequestBytes))
	} else {
		if e := validatePosition(*pos); e != nil {
			return ContainerData{}, e
		}
		req := positionPB(s.key, *pos)
		if proto.Size(req) > s.config.MaxRequestBytes {
			return ContainerData{}, status.Error(codes.ResourceExhausted, "request too large")
		}
		stream, err = s.stub.GetElementInRange(s.ctx, req, grpc.Trailer(&md), grpc.MaxCallRecvMsgSize(s.config.MaxDecodedBytes), grpc.MaxCallSendMsgSize(s.config.MaxRequestBytes))
	}
	if err != nil {
		return ContainerData{}, &RPCError{err, md.Copy()}
	}
	result := ContainerData{Values: []Payload{}, OrderedValues: []OrderedPayload{}, Entries: []Entry{}, OrderedEntries: []OrderedEntry{}}
	chunks, items := 0, 0
	for {
		b, e := stream.Recv()
		if e == io.EOF {
			return result, nil
		}
		if e != nil {
			return result, partial(chunks, items, &RPCError{e, stream.Trailer().Copy()})
		}
		d, e := decodeBatch(b, s.config)
		if e != nil {
			return result, partial(chunks, items, e)
		}
		result.Values = append(result.Values, d.Values...)
		result.OrderedValues = append(result.OrderedValues, d.OrderedValues...)
		result.Entries = append(result.Entries, d.Entries...)
		result.OrderedEntries = append(result.OrderedEntries, d.OrderedEntries...)
		chunks++
		items += len(d.Values) + len(d.OrderedValues) + len(d.Entries) + len(d.OrderedEntries)
	}
}

// GetContainer collects and decodes every streamed batch, preserving entry ordering.
// On a stream failure it returns the completed prefix with a non-nil error.
func (c *Client) GetContainer(ctx context.Context, key []byte, o Options) (ContainerData, error) {
	return perform(c, ctx, key, o, false, func(s *session) (ContainerData, error) { return collect(s, nil) })
}

// GetElementInRange collects a positional or weighted range, including Reverse.
func (c *Client) GetElementInRange(ctx context.Context, key []byte, pos Position, o Options) (ContainerData, error) {
	return perform(c, ctx, key, o, false, func(s *session) (ContainerData, error) { return collect(s, &pos) })
}
