package hurricache

import (
	"context"
	"errors"
	pb "github.com/hurricache/hurricache-go-client/proto/cachepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"math"
	"time"
)

// Get returns a typed value; an absent response becomes NONE.
func (c *Client) Get(ctx context.Context, key []byte, o Options) (Variant, error) {
	return perform(c, ctx, key, o, false, func(s *session) (Variant, error) {
		r, e := unary(s, &pb.GetRequest{Key: s.key}, s.stub.GetValue)
		if e != nil {
			return None(), e
		}
		return variantResult(r, s.config)
	})
}

// Pop returns a typed value; an absent response becomes NONE.
func (c *Client) Pop(ctx context.Context, key []byte, o Options) (Variant, error) {
	return perform(c, ctx, key, o, true, func(s *session) (Variant, error) {
		r, e := unary(s, &pb.GetRequest{Key: s.key}, s.stub.GetAndDeleteValue)
		if e != nil {
			return None(), e
		}
		return variantResult(r, s.config)
	})
}

// PeekHead returns a typed value; an absent response becomes NONE.
func (c *Client) PeekHead(ctx context.Context, key []byte, o Options) (Variant, error) {
	return perform(c, ctx, key, o, false, func(s *session) (Variant, error) {
		r, e := unary(s, &pb.GetRequest{Key: s.key}, s.stub.GetHead)
		if e != nil {
			return None(), e
		}
		return variantResult(r, s.config)
	})
}

// PeekTail returns a typed value; an absent response becomes NONE.
func (c *Client) PeekTail(ctx context.Context, key []byte, o Options) (Variant, error) {
	return perform(c, ctx, key, o, false, func(s *session) (Variant, error) {
		r, e := unary(s, &pb.GetRequest{Key: s.key}, s.stub.GetTail)
		if e != nil {
			return None(), e
		}
		return variantResult(r, s.config)
	})
}

// PopHead returns a typed value; an absent response becomes NONE.
func (c *Client) PopHead(ctx context.Context, key []byte, o Options) (Variant, error) {
	return perform(c, ctx, key, o, true, func(s *session) (Variant, error) {
		r, e := unary(s, &pb.GetRequest{Key: s.key}, s.stub.GetAndDeleteHead)
		if e != nil {
			return None(), e
		}
		return variantResult(r, s.config)
	})
}

// PopTail returns a typed value; an absent response becomes NONE.
func (c *Client) PopTail(ctx context.Context, key []byte, o Options) (Variant, error) {
	return perform(c, ctx, key, o, true, func(s *session) (Variant, error) {
		r, e := unary(s, &pb.GetRequest{Key: s.key}, s.stub.GetAndDeleteTail)
		if e != nil {
			return None(), e
		}
		return variantResult(r, s.config)
	})
}

func (c *Client) ContainsKey(ctx context.Context, key []byte, o Options) (bool, error) {
	return perform(c, ctx, key, o, false, func(s *session) (bool, error) {
		r, e := unary(s, &pb.GetRequest{Key: s.key}, s.stub.ExistKey)
		if e != nil {
			var z bool
			return z, e
		}
		return r.Value, nil
	})
}

func (c *Client) Remove(ctx context.Context, key []byte, o Options) (bool, error) {
	return perform(c, ctx, key, o, false, func(s *session) (bool, error) {
		r, e := unary(s, &pb.GetRequest{Key: s.key}, s.stub.Remove)
		if e != nil {
			var z bool
			return z, e
		}
		return r.Value, nil
	})
}

func (c *Client) GetSize(ctx context.Context, key []byte, o Options) (uint32, error) {
	return perform(c, ctx, key, o, false, func(s *session) (uint32, error) {
		r, e := unary(s, &pb.GetRequest{Key: s.key}, s.stub.GetSize)
		if e != nil {
			var z uint32
			return z, e
		}
		return r.Size, nil
	})
}

func (c *Client) DeleteHead(ctx context.Context, key []byte, o Options) (uint32, error) {
	return perform(c, ctx, key, o, true, func(s *session) (uint32, error) {
		r, e := unary(s, &pb.GetRequest{Key: s.key}, s.stub.DeleteHead)
		if e != nil {
			var z uint32
			return z, e
		}
		return r.Size, nil
	})
}

func (c *Client) DeleteTail(ctx context.Context, key []byte, o Options) (uint32, error) {
	return perform(c, ctx, key, o, true, func(s *session) (uint32, error) {
		r, e := unary(s, &pb.GetRequest{Key: s.key}, s.stub.DeleteTail)
		if e != nil {
			var z uint32
			return z, e
		}
		return r.Size, nil
	})
}

// Create stores a typed scalar using Java's configured-mode dispatch.
func (c *Client) Create(ctx context.Context, key []byte, value Variant, o Options) (*KeyHint, error) {
	return perform(c, ctx, key, o, false, func(s *session) (*KeyHint, error) {
		v, e := encodeValue(value, s.ttl, s.config)
		if e != nil {
			return nil, e
		}
		r, e := unary(s, &pb.CreateRequest{Key: s.key, Value: v}, s.stub.CreateKeyValue)
		if e != nil {
			return nil, e
		}
		return hintResponse(r), nil
	})
}

// Replace returns the previous typed scalar, or NONE when no value is supplied.
func (c *Client) Replace(ctx context.Context, key []byte, value Variant, o Options) (Variant, error) {
	return perform(c, ctx, key, o, false, func(s *session) (Variant, error) {
		v, e := encodeValue(value, s.ttl, s.config)
		if e != nil {
			return None(), e
		}
		r, e := unary(s, &pb.UpdateRequest{Key: s.key, Value: v}, s.stub.UpdateValue)
		if e != nil {
			return None(), e
		}
		previous, e := decodeValue(r.Value, s.config)
		if previous == nil {
			return None(), e
		}
		return *previous, e
	})
}
func positionPB(key *pb.Key, p Position) *pb.KeyPositionRequest {
	r := &pb.KeyPositionRequest{Key: key, Type: copyPtr(p.Type), End: copyPtr(p.End), Reverse: p.Reverse}
	if !p.OmitStart {
		r.Pos = Ptr(p.Start)
	}
	return r
}
func validatePosition(p Position) error {
	if p.Type != nil && (*p.Type < Vector || *p.Type > OrderedSet) {
		return status.Error(codes.InvalidArgument, "unsupported range container type")
	}
	return nil // Java preserves bounds, including reverse traversal and open limits.
}

func (c *Client) GetAt(ctx context.Context, key []byte, pos uint64, o Options) (Variant, error) {
	return perform(c, ctx, key, o, false, func(s *session) (Variant, error) {
		r, e := unary(s, positionPB(s.key, Position{Start: pos}), s.stub.GetElementAtPosition)
		if e != nil {
			return None(), e
		}
		return variantResult(r, s.config)
	})
}

func (c *Client) PopAt(ctx context.Context, key []byte, pos uint64, o Options) (Variant, error) {
	return perform(c, ctx, key, o, true, func(s *session) (Variant, error) {
		r, e := unary(s, positionPB(s.key, Position{Start: pos}), s.stub.GetAndDeleteElementAtPosition)
		if e != nil {
			return None(), e
		}
		return variantResult(r, s.config)
	})
}

func (c *Client) RemoveAt(ctx context.Context, key []byte, pos Position, o Options) (uint32, error) {
	return perform(c, ctx, key, o, true, func(s *session) (uint32, error) {
		if pos.End != nil && *pos.End <= pos.Start {
			pos.End = nil
		}
		r, e := unary(s, positionPB(s.key, pos), s.stub.DeleteElementAtPosition)
		if e != nil {
			return 0, e
		}
		return r.Size, nil
	})
}

func (c *Client) GetElementWithWeight(ctx context.Context, key []byte, pos uint64, o Options) (Variant, error) {
	return perform(c, ctx, key, o, false, func(s *session) (Variant, error) {
		r, e := unary(s, positionPB(s.key, Position{Start: pos}), s.stub.GetElementAtPosition)
		if e != nil {
			return None(), e
		}
		return variantResult(r, s.config)
	})
}

func (c *Client) GetAndRemoveElementWithWeight(ctx context.Context, key []byte, pos uint64, o Options) (Variant, error) {
	return perform(c, ctx, key, o, false, func(s *session) (Variant, error) {
		r, e := unary(s, positionPB(s.key, Position{Start: pos}), s.stub.GetAndDeleteElementAtPosition)
		if e != nil {
			return None(), e
		}
		return variantResult(r, s.config)
	})
}

func (c *Client) GetInContainer(ctx context.Context, key, elementKey []byte, o Options) (Variant, error) {
	return perform(c, ctx, key, o, false, func(s *session) (Variant, error) {
		k, e := encodeKeyPolicy(elementKey, nil, s.key.ClientId, s.config, false)
		if e != nil {
			return None(), e
		}
		r, e := unary(s, &pb.ContainerGetRequest{Key: s.key, ElementKey: k}, s.stub.GetValueInContainerByKey)
		if e != nil {
			return None(), e
		}
		return variantResult(r, s.config)
	})
}

func (c *Client) PopInContainer(ctx context.Context, key, elementKey []byte, o Options) (Variant, error) {
	return perform(c, ctx, key, o, true, func(s *session) (Variant, error) {
		k, e := encodeKeyPolicy(elementKey, nil, s.key.ClientId, s.config, false)
		if e != nil {
			return None(), e
		}
		r, e := unary(s, &pb.ContainerGetRequest{Key: s.key, ElementKey: k}, s.stub.GetAndDeleteValueInContainer)
		if e != nil {
			return None(), e
		}
		return variantResult(r, s.config)
	})
}

func (c *Client) ContainsInContainer(ctx context.Context, key, elementKey []byte, o Options) (bool, error) {
	return perform(c, ctx, key, o, false, func(s *session) (bool, error) {
		k, e := encodeKeyPolicy(elementKey, nil, s.key.ClientId, s.config, false)
		if e != nil {
			return false, e
		}
		r, e := unary(s, &pb.ContainerGetRequest{Key: s.key, ElementKey: k}, s.stub.ExistInContainerByKey)
		if e != nil {
			return false, e
		}
		return r.Value, nil
	})
}

func (c *Client) RemoveInContainer(ctx context.Context, key, elementKey []byte, o Options) (uint32, error) {
	return perform(c, ctx, key, o, true, func(s *session) (uint32, error) {
		k, e := encodeKeyPolicy(elementKey, nil, s.key.ClientId, s.config, false)
		if e != nil {
			return 0, e
		}
		r, e := unary(s, &pb.ContainerGetRequest{Key: s.key, ElementKey: k}, s.stub.DeleteFromContainerByKey)
		if e != nil {
			return 0, e
		}
		return r.Size, nil
	})
}

// Update replaces a map entry and returns the previous typed value.
func (c *Client) Update(ctx context.Context, key, elementKey []byte, value Variant, o Options) (Variant, error) {
	return perform(c, ctx, key, o, true, func(s *session) (Variant, error) {
		k, e := encodeKeyPolicy(elementKey, nil, s.key.ClientId, s.config, false)
		if e != nil {
			return None(), e
		}
		v, e := encodeValue(value, nil, s.config)
		if e != nil {
			return None(), e
		}
		r, e := unary(s, &pb.UpdateContainerRequest{Key: s.key, Keys: &pb.UpdateContainerRequest_ElementKey{ElementKey: k}, Values: &pb.UpdateContainerRequest_Value{Value: v}}, s.stub.UpdateValueInContainer)
		if e != nil {
			return None(), e
		}
		p, e := decodeValue(r.Value, s.config)
		if p == nil {
			return None(), e
		}
		return *p, e
	})
}
func (c *Client) orderUpdate(ctx context.Context, key []byte, elementKey OrderedPayload, order uint64, delta int64, increment bool, o Options) (bool, error) {
	if elementKey.Order == nil {
		return false, status.Error(codes.InvalidArgument, "ordered key requires its current order")
	}
	return perform(c, ctx, key, o, true, func(s *session) (bool, error) {
		k, e := encodeKey(elementKey.Data, elementKey.Hint, s.key.ClientId, s.config)
		if e != nil {
			return false, e
		}
		ok := &pb.OrderedKey{Payload: k.Payload, CompressionInfo: k.CompressionInfo, KeyHint: k.KeyHint, ClientId: k.ClientId, Order: copyPtr(elementKey.Order)}
		r := &pb.UpdateContainerRequest{Key: s.key, Keys: &pb.UpdateContainerRequest_OrderedElementKey{OrderedElementKey: ok}}
		if increment {
			r.Values = &pb.UpdateContainerRequest_DeltaOrder{DeltaOrder: delta}
		} else {
			r.Values = &pb.UpdateContainerRequest_NewOrder{NewOrder: order}
		}
		res, e := unary(s, r, s.stub.UpdateValueInContainer)
		if e != nil {
			return false, e
		}
		return res.Result, nil
	})
}

// UpdateOrder moves exactly one member identified by bytes AND current order.
func (c *Client) UpdateOrder(ctx context.Context, key []byte, elementKey OrderedPayload, newOrder uint64, o Options) (bool, error) {
	return c.orderUpdate(ctx, key, elementKey, newOrder, 0, false, o)
}

// IncrementOrder preserves the member value and sends a signed delta.
func (c *Client) IncrementOrder(ctx context.Context, key []byte, elementKey OrderedPayload, delta int64, o Options) (bool, error) {
	return c.orderUpdate(ctx, key, elementKey, 0, delta, true, o)
}
func numeric(value any) (Variant, error) {
	switch n := value.(type) {
	case int:
		return Int(int64(n)), nil
	case int64:
		return Int(n), nil
	case float64:
		return Double(n), nil
	default:
		return None(), status.Error(codes.InvalidArgument, "atomic operand must be int, int64 or float64 (excluding bool)")
	}
}
func containerSelector(selector any, s *session) (*pb.ContainerKey, error) {
	var key []byte
	switch p := selector.(type) {
	case []byte:
		key = p
	case string:
		key = []byte(p)
	case uint64:
		return &pb.ContainerKey{Key: &pb.ContainerKey_Pos{Pos: p}}, nil
	case int:
		if p < 0 {
			return nil, status.Error(codes.InvalidArgument, "negative container position")
		}
		return &pb.ContainerKey{Key: &pb.ContainerKey_Pos{Pos: uint64(p)}}, nil
	case int64:
		if p < 0 {
			return nil, status.Error(codes.InvalidArgument, "negative container position")
		}
		return &pb.ContainerKey{Key: &pb.ContainerKey_Pos{Pos: uint64(p)}}, nil
	default:
		return nil, status.Error(codes.InvalidArgument, "selector must be bytes, string or integer position (excluding bool)")
	}
	k, e := encodeKey(key, nil, s.key.ClientId, s.config)
	if e != nil {
		return nil, e
	}
	return &pb.ContainerKey{Key: &pb.ContainerKey_ContainerKey{ContainerKey: k}}, nil
}

type atomicRPC func(context.Context, *pb.CreateRequest, ...grpc.CallOption) (*pb.ValueResponse, error)

func (c *Client) atomic(ctx context.Context, key []byte, value any, selector any, container bool, o Options, rpc func(*session) atomicRPC) (any, error) {
	v, e := numeric(value)
	if e != nil {
		return nil, e
	}
	return perform(c, ctx, key, o, true, func(s *session) (any, error) {
		pv, e := encodeValue(v, s.ttl, s.config)
		if e != nil {
			return nil, e
		}
		r := &pb.CreateRequest{Key: s.key, Value: pv}
		if container {
			r.ContainerKey, e = containerSelector(selector, s)
			if e != nil {
				return nil, e
			}
		}
		res, e := unary(s, r, rpc(s))
		if e != nil {
			return nil, e
		}
		observed, e := variantResult(res, s.config)
		if e != nil {
			return nil, e
		}
		n, e := observed.Number()
		if e != nil {
			return nil, status.Error(codes.DataLoss, e.Error())
		}
		return n, nil
	})
}

// Exchange returns the server's observed prior numeric value.
func (c *Client) Exchange(ctx context.Context, key []byte, value any, o Options) (any, error) {
	r, e := c.atomic(ctx, key, value, nil, false, o, func(s *session) atomicRPC { return s.stub.AtomicExchange })
	return r, e
}

// ExchangeInContainer returns the server's observed prior numeric value.
func (c *Client) ExchangeInContainer(ctx context.Context, key []byte, selector any, value any, o Options) (any, error) {
	r, e := c.atomic(ctx, key, value, selector, true, o, func(s *session) atomicRPC { return s.stub.AtomicExchange })
	return r, e
}

// Add returns the server's observed prior numeric value.
func (c *Client) Add(ctx context.Context, key []byte, value any, o Options) (any, error) {
	r, e := c.atomic(ctx, key, value, nil, false, o, func(s *session) atomicRPC { return s.stub.AtomicAdd })
	return r, e
}

// AddInContainer returns the server's observed prior numeric value.
func (c *Client) AddInContainer(ctx context.Context, key []byte, selector any, value any, o Options) (any, error) {
	r, e := c.atomic(ctx, key, value, selector, true, o, func(s *session) atomicRPC { return s.stub.AtomicAdd })
	return r, e
}

// Sub returns the server's observed prior numeric value.
func (c *Client) Sub(ctx context.Context, key []byte, value any, o Options) (any, error) {
	r, e := c.atomic(ctx, key, value, nil, false, o, func(s *session) atomicRPC { return s.stub.AtomicSub })
	return r, e
}

// SubInContainer returns the server's observed prior numeric value.
func (c *Client) SubInContainer(ctx context.Context, key []byte, selector any, value any, o Options) (any, error) {
	r, e := c.atomic(ctx, key, value, selector, true, o, func(s *session) atomicRPC { return s.stub.AtomicSub })
	return r, e
}

// And returns the server's observed prior numeric value.
func (c *Client) And(ctx context.Context, key []byte, value int64, o Options) (int64, error) {
	r, e := c.atomic(ctx, key, value, nil, false, o, func(s *session) atomicRPC { return s.stub.AtomicAnd })
	if e != nil {
		return 0, e
	}
	n, ok := r.(int64)
	if !ok {
		return 0, status.Error(codes.DataLoss, "bitwise response is not INT")
	}
	return n, nil
}

// AndInContainer returns the server's observed prior numeric value.
func (c *Client) AndInContainer(ctx context.Context, key []byte, selector any, value int64, o Options) (int64, error) {
	r, e := c.atomic(ctx, key, value, selector, true, o, func(s *session) atomicRPC { return s.stub.AtomicAnd })
	if e != nil {
		return 0, e
	}
	n, ok := r.(int64)
	if !ok {
		return 0, status.Error(codes.DataLoss, "bitwise response is not INT")
	}
	return n, nil
}

// Or returns the server's observed prior numeric value.
func (c *Client) Or(ctx context.Context, key []byte, value int64, o Options) (int64, error) {
	r, e := c.atomic(ctx, key, value, nil, false, o, func(s *session) atomicRPC { return s.stub.AtomicOr })
	if e != nil {
		return 0, e
	}
	n, ok := r.(int64)
	if !ok {
		return 0, status.Error(codes.DataLoss, "bitwise response is not INT")
	}
	return n, nil
}

// OrInContainer returns the server's observed prior numeric value.
func (c *Client) OrInContainer(ctx context.Context, key []byte, selector any, value int64, o Options) (int64, error) {
	r, e := c.atomic(ctx, key, value, selector, true, o, func(s *session) atomicRPC { return s.stub.AtomicOr })
	if e != nil {
		return 0, e
	}
	n, ok := r.(int64)
	if !ok {
		return 0, status.Error(codes.DataLoss, "bitwise response is not INT")
	}
	return n, nil
}

// Xor returns the server's observed prior numeric value.
func (c *Client) Xor(ctx context.Context, key []byte, value int64, o Options) (int64, error) {
	r, e := c.atomic(ctx, key, value, nil, false, o, func(s *session) atomicRPC { return s.stub.AtomicXor })
	if e != nil {
		return 0, e
	}
	n, ok := r.(int64)
	if !ok {
		return 0, status.Error(codes.DataLoss, "bitwise response is not INT")
	}
	return n, nil
}

// XorInContainer returns the server's observed prior numeric value.
func (c *Client) XorInContainer(ctx context.Context, key []byte, selector any, value int64, o Options) (int64, error) {
	r, e := c.atomic(ctx, key, value, selector, true, o, func(s *session) atomicRPC { return s.stub.AtomicXor })
	if e != nil {
		return 0, e
	}
	n, ok := r.(int64)
	if !ok {
		return 0, status.Error(codes.DataLoss, "bitwise response is not INT")
	}
	return n, nil
}

// CompareAndSet promotes both operands to DOUBLE if either operand is a double.
// Expected retains optional presence and the observed type from nested ValueResponse.
func (c *Client) CompareAndSet(ctx context.Context, key []byte, expected, newValue any, o Options) (CASResult, error) {
	return c.compareAndSet(ctx, key, nil, false, expected, newValue, o)
}

// CompareAndSetInContainer addresses a map key ([]byte/string), or an exact
// ordered-map key and weight (OrderedPayload with a non-nil Order).
// Like Java's container CAS, this operation leaves the element TTL absent.
func (c *Client) CompareAndSetInContainer(ctx context.Context, key []byte, selector any, expected, newValue any, o Options) (CASResult, error) {
	switch v := selector.(type) {
	case []byte, string:
	case OrderedPayload:
		if v.Order == nil {
			return CASResult{}, errors.New("ordered selector must carry an order")
		}
	default:
		return CASResult{}, errors.New("container CAS selector must be []byte, string, or OrderedPayload")
	}
	return c.compareAndSet(ctx, key, selector, true, expected, newValue, o)
}

func (c *Client) compareAndSet(ctx context.Context, key []byte, selector any, container bool, expected, newValue any, o Options) (CASResult, error) {
	a, e := numeric(expected)
	if e != nil {
		return CASResult{}, e
	}
	b, e := numeric(newValue)
	if e != nil {
		return CASResult{}, e
	}
	if a.IsDouble() || b.IsDouble() {
		if a.IsInt() {
			n, _ := a.Int64()
			a = Double(float64(n))
		}
		if b.IsInt() {
			n, _ := b.Int64()
			b = Double(float64(n))
		}
	}
	return perform(c, ctx, key, o, true, func(s *session) (CASResult, error) {
		av, e := encodeValue(a, nil, s.config)
		if e != nil {
			return CASResult{}, e
		}
		ttl := s.ttl
		if container {
			ttl = nil
		}
		bv, e := encodeValue(b, ttl, s.config)
		if e != nil {
			return CASResult{}, e
		}
		request := &pb.AtomicCas{Key: s.key, Expected: av, ToSet: bv}
		if container {
			switch v := selector.(type) {
			case OrderedPayload:
				k, err := encodeKeyPolicy(v.Data, nil, s.key.ClientId, s.config, true)
				if err != nil {
					return CASResult{}, err
				}
				request.Keys = &pb.AtomicCas_OrderedElementKey{OrderedElementKey: &pb.OrderedKey{
					Payload: k.Payload, Order: v.Order, CompressionInfo: k.CompressionInfo,
					KeyHint: k.KeyHint, ClientId: k.ClientId,
				}}
			default:
				var data []byte
				if text, ok := v.(string); ok {
					data = []byte(text)
				} else {
					data = v.([]byte)
				}
				k, err := encodeKeyPolicy(data, nil, s.key.ClientId, s.config, true)
				if err != nil {
					return CASResult{}, err
				}
				request.Keys = &pb.AtomicCas_ElementKey{ElementKey: k}
			}
		}
		r, e := unary(s, request, s.stub.AtomicCompareAndSet)
		if e != nil {
			return CASResult{}, e
		}
		result := CASResult{Swapped: r.Result}
		if r.Expected != nil {
			v, e := decodeResult(r.Expected, s.config)
			if e != nil {
				return result, e
			}
			result.Hint = v.Hint
			result.Expected = v.Value
			if v.Ordered != nil {
				result.Expected = Ptr(v.Ordered.Value())
			}
		}
		return result, nil
	})
}
func (c *Client) SetTTL(ctx context.Context, key []byte, ttl time.Duration, o Options) (bool, error) {
	return perform(c, ctx, key, o, true, func(s *session) (bool, error) {

		expiration := uint64(time.Now().Add(ttl).UnixMilli())
		if ttl < 0 {
			expiration = math.MaxUint64
		}
		r, e := unary(s, &pb.TtlRequest{Key: s.key, Ttl: Ptr(expiration)}, s.stub.SetTtl)
		if e != nil {
			return false, e
		}
		return r.Value, nil
	})
}

// GetTTL returns remaining milliseconds, or -1 for an absent expiration, as Java does.
func (c *Client) GetTTL(ctx context.Context, key []byte, o Options) (int64, error) {
	return perform(c, ctx, key, o, false, func(s *session) (int64, error) {
		r, e := unary(s, &pb.GetRequest{Key: s.key}, s.stub.GetTtl)
		if e != nil {
			return 0, e
		}
		if r.Ttl == nil {
			return -1, nil
		}
		if *r.Ttl > math.MaxInt64 {
			return 0, status.Error(codes.OutOfRange, "TTL timestamp exceeds int64")
		}
		return int64(*r.Ttl) - time.Now().UnixMilli(), nil
	})
}

// LockObject retains the status and optional diagnostic. Lock returns only status.
func (c *Client) LockObject(ctx context.Context, key []byte, kind LockType, duration time.Duration, o Options) (LockResult, error) {
	return perform(c, ctx, key, o, true, func(s *session) (LockResult, error) {

		if kind < NoLock || kind > GlobalLock || duration < 0 {
			return LockResult{}, status.Error(codes.InvalidArgument, "invalid lock type or duration")
		}
		var expiration *uint32
		if duration > 0 {
			seconds := (time.Now().UnixMilli() + duration.Milliseconds()) / 1000
			if seconds < 0 || seconds > math.MaxUint32 {
				return LockResult{}, status.Error(codes.InvalidArgument, "lock expiration exceeds uint32")
			}
			expiration = Ptr(uint32(seconds))
		}
		r, e := unary(s, &pb.LockRequest{Key: s.key, LockType: kind, ClientId: copyPtr(s.key.ClientId), LockDuration: expiration}, s.stub.LockObject)
		if e != nil {
			return LockResult{}, e
		}
		return LockResult{r.Result, copyPtr(r.Message)}, nil
	})
}

func (c *Client) UnlockObject(ctx context.Context, key []byte, o Options) (LockResult, error) {
	return perform(c, ctx, key, o, true, func(s *session) (LockResult, error) {

		r, e := unary(s, &pb.UnLockRequest{Key: s.key, ClientId: copyPtr(s.key.ClientId)}, s.stub.UnlockObject)
		if e != nil {
			return LockResult{}, e
		}
		return LockResult{r.Result, copyPtr(r.Message)}, nil
	})
}

// Deprecated: use Get.
func (c *Client) GetValue(ctx context.Context, key []byte, o Options) (Variant, error) {
	return c.Get(ctx, key, o)
}

// Deprecated: use Pop.
func (c *Client) GetAndDeleteValue(ctx context.Context, key []byte, o Options) (Variant, error) {
	return c.Pop(ctx, key, o)
}

// Deprecated: use ContainsKey.
func (c *Client) ExistKey(ctx context.Context, key []byte, o Options) (bool, error) {
	return c.ContainsKey(ctx, key, o)
}

// Deprecated: use PeekHead.
func (c *Client) GetFront(ctx context.Context, key []byte, o Options) (Variant, error) {
	return c.PeekHead(ctx, key, o)
}

// Deprecated: use PeekHead.
func (c *Client) GetHead(ctx context.Context, key []byte, o Options) (Variant, error) {
	return c.PeekHead(ctx, key, o)
}

// Deprecated: use PeekTail.
func (c *Client) GetTail(ctx context.Context, key []byte, o Options) (Variant, error) {
	return c.PeekTail(ctx, key, o)
}

// Deprecated: use PopHead.
func (c *Client) GetAndRemoveFront(ctx context.Context, key []byte, o Options) (Variant, error) {
	return c.PopHead(ctx, key, o)
}

// Deprecated: use PopTail.
func (c *Client) GetAndRemoveTail(ctx context.Context, key []byte, o Options) (Variant, error) {
	return c.PopTail(ctx, key, o)
}

// Deprecated: use DeleteHead.
func (c *Client) RemoveHead(ctx context.Context, key []byte, o Options) (uint32, error) {
	return c.DeleteHead(ctx, key, o)
}

// Deprecated: use DeleteTail.
func (c *Client) RemoveTail(ctx context.Context, key []byte, o Options) (uint32, error) {
	return c.DeleteTail(ctx, key, o)
}

// Deprecated: use Create with a Variant.
func (c *Client) CreateKeyValue(ctx context.Context, key, value []byte, o Options) (*KeyHint, error) {
	return c.Create(ctx, key, Bytes(value), o)
}

// Deprecated: use Replace with a Variant.
func (c *Client) UpdateKeyValue(ctx context.Context, key, value []byte, o Options) (Variant, error) {
	return c.Replace(ctx, key, Bytes(value), o)
}

// Deprecated: use GetInContainer.
func (c *Client) GetContainerValue(ctx context.Context, key, elementKey []byte, o Options) (Variant, error) {
	return c.GetInContainer(ctx, key, elementKey, o)
}

// Deprecated: use PopInContainer.
func (c *Client) GetAndRemoveContainerValue(ctx context.Context, key, elementKey []byte, o Options) (Variant, error) {
	return c.PopInContainer(ctx, key, elementKey, o)
}

// Deprecated: use ContainsInContainer.
func (c *Client) ContainsContainerKey(ctx context.Context, key, elementKey []byte, o Options) (bool, error) {
	return c.ContainsInContainer(ctx, key, elementKey, o)
}

// Deprecated: use RemoveInContainer.
func (c *Client) RemoveContainerKey(ctx context.Context, key, elementKey []byte, o Options) (uint32, error) {
	return c.RemoveInContainer(ctx, key, elementKey, o)
}

// Deprecated: use Update with a Variant.
func (c *Client) UpdateContainerValue(ctx context.Context, key, elementKey, value []byte, o Options) (Variant, error) {
	return c.Update(ctx, key, elementKey, Bytes(value), o)
}

// Lock returns the Java lock status. LockObject additionally retains diagnostics.
func (c *Client) Lock(ctx context.Context, key []byte, kind LockType, duration time.Duration, o Options) (LockStatus, error) {
	r, e := c.LockObject(ctx, key, kind, duration, o)
	return r.Status, e
}
func (c *Client) Unlock(ctx context.Context, key []byte, o Options) (LockStatus, error) {
	r, e := c.UnlockObject(ctx, key, o)
	return r.Status, e
}
