package hurricache

import (
	"context"
	pb "github.com/hurricache/hurricache-go-client/proto/cachepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"math"
	"time"
)

// GetValue retrieves a scalar with presence and metadata.
func (c *Client) GetValue(ctx context.Context, key []byte, o Options) (ValueResult, error) {
	return perform(c, ctx, key, o, false, func(s *session) (ValueResult, error) {
		r, e := unary(s, &pb.GetRequest{Key: s.key}, s.stub.GetValue)
		if e != nil {
			var z ValueResult
			return z, e
		}
		return decodeResult(r, s.config)
	})
}

// GetAndDeleteValue retrieves and deletes a scalar.
func (c *Client) GetAndDeleteValue(ctx context.Context, key []byte, o Options) (ValueResult, error) {
	return perform(c, ctx, key, o, true, func(s *session) (ValueResult, error) {
		r, e := unary(s, &pb.GetRequest{Key: s.key}, s.stub.GetAndDeleteValue)
		if e != nil {
			var z ValueResult
			return z, e
		}
		return decodeResult(r, s.config)
	})
}

// ExistKey reports whether a top-level key exists.
func (c *Client) ExistKey(ctx context.Context, key []byte, o Options) (bool, error) {
	return perform(c, ctx, key, o, false, func(s *session) (bool, error) {
		r, e := unary(s, &pb.GetRequest{Key: s.key}, s.stub.ExistKey)
		if e != nil {
			var z bool
			return z, e
		}
		return r.Value, nil
	})
}

// Remove removes a top-level key using Java configured-mode dispatch.
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

// GetSize returns the server size/count without signed truncation.
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

// GetFront retrieves the front element (alias of GetHead).
func (c *Client) GetFront(ctx context.Context, key []byte, o Options) (ValueResult, error) {
	return perform(c, ctx, key, o, false, func(s *session) (ValueResult, error) {
		r, e := unary(s, &pb.GetRequest{Key: s.key}, s.stub.GetHead)
		if e != nil {
			var z ValueResult
			return z, e
		}
		return decodeResult(r, s.config)
	})
}

// GetHead retrieves the head element.
func (c *Client) GetHead(ctx context.Context, key []byte, o Options) (ValueResult, error) {
	return perform(c, ctx, key, o, false, func(s *session) (ValueResult, error) {
		r, e := unary(s, &pb.GetRequest{Key: s.key}, s.stub.GetHead)
		if e != nil {
			var z ValueResult
			return z, e
		}
		return decodeResult(r, s.config)
	})
}

// GetTail retrieves the tail element.
func (c *Client) GetTail(ctx context.Context, key []byte, o Options) (ValueResult, error) {
	return perform(c, ctx, key, o, false, func(s *session) (ValueResult, error) {
		r, e := unary(s, &pb.GetRequest{Key: s.key}, s.stub.GetTail)
		if e != nil {
			var z ValueResult
			return z, e
		}
		return decodeResult(r, s.config)
	})
}

// GetAndRemoveFront pops the front element.
func (c *Client) GetAndRemoveFront(ctx context.Context, key []byte, o Options) (ValueResult, error) {
	return perform(c, ctx, key, o, true, func(s *session) (ValueResult, error) {
		r, e := unary(s, &pb.GetRequest{Key: s.key}, s.stub.GetAndRemoveFront)
		if e != nil {
			var z ValueResult
			return z, e
		}
		return decodeResult(r, s.config)
	})
}

// GetAndRemoveTail pops the tail element.
func (c *Client) GetAndRemoveTail(ctx context.Context, key []byte, o Options) (ValueResult, error) {
	return perform(c, ctx, key, o, true, func(s *session) (ValueResult, error) {
		r, e := unary(s, &pb.GetRequest{Key: s.key}, s.stub.GetAndRemoveTail)
		if e != nil {
			var z ValueResult
			return z, e
		}
		return decodeResult(r, s.config)
	})
}

// RemoveHead removes the head element.
func (c *Client) RemoveHead(ctx context.Context, key []byte, o Options) (bool, error) {
	return perform(c, ctx, key, o, true, func(s *session) (bool, error) {
		r, e := unary(s, &pb.GetRequest{Key: s.key}, s.stub.RemoveHead)
		if e != nil {
			var z bool
			return z, e
		}
		return r.Value, nil
	})
}

// RemoveTail removes the tail element.
func (c *Client) RemoveTail(ctx context.Context, key []byte, o Options) (bool, error) {
	return perform(c, ctx, key, o, true, func(s *session) (bool, error) {
		r, e := unary(s, &pb.GetRequest{Key: s.key}, s.stub.RemoveTail)
		if e != nil {
			var z bool
			return z, e
		}
		return r.Value, nil
	})
}

// AtomicLoad loads a signed 64-bit atomic value.
func (c *Client) AtomicLoad(ctx context.Context, key []byte, o Options) (AtomicResult, error) {
	return perform(c, ctx, key, o, false, func(s *session) (AtomicResult, error) {
		r, e := unary(s, &pb.GetRequest{Key: s.key}, s.stub.AtomicLoad)
		if e != nil {
			var z AtomicResult
			return z, e
		}
		return atomicGo(r), nil
	})
}

// AtomicLoadAndDelete loads and deletes an atomic value.
func (c *Client) AtomicLoadAndDelete(ctx context.Context, key []byte, o Options) (AtomicResult, error) {
	return perform(c, ctx, key, o, true, func(s *session) (AtomicResult, error) {
		r, e := unary(s, &pb.GetRequest{Key: s.key}, s.stub.AtomicLoadAndDelete)
		if e != nil {
			var z AtomicResult
			return z, e
		}
		return atomicGo(r), nil
	})
}

// SetTTL sets a relative TTL, encoded as an absolute Unix millisecond timestamp; zero expires now.
func (c *Client) SetTTL(ctx context.Context, key []byte, ttl time.Duration, o Options) (bool, error) {
	return perform(c, ctx, key, o, true, func(s *session) (bool, error) {

		if ttl < 0 {
			return false, status.Error(codes.InvalidArgument, "negative TTL")
		}
		r, e := unary(s, &pb.TtlRequest{Key: s.key, Ttl: Ptr(uint64(time.Now().Add(ttl).UnixMilli()))}, s.stub.SetTtl)
		if e != nil {
			return false, e
		}
		return r.Value, nil
	})
}

// GetTTL returns optional remaining duration and the original wire expiration.
func (c *Client) GetTTL(ctx context.Context, key []byte, o Options) (TTLResult, error) {
	return perform(c, ctx, key, o, false, func(s *session) (TTLResult, error) {

		r, e := unary(s, &pb.GetRequest{Key: s.key}, s.stub.GetTtl)
		if e != nil {
			return TTLResult{}, e
		}
		result := TTLResult{ExpiresAtMillis: copyPtr(r.Ttl)}
		if r.Ttl != nil {
			now := time.Now().UnixMilli()
			if *r.Ttl > math.MaxInt64 || int64(*r.Ttl)-now > math.MaxInt64/int64(time.Millisecond) || int64(*r.Ttl)-now < math.MinInt64/int64(time.Millisecond) {
				return result, status.Error(codes.OutOfRange, "TTL does not fit time.Duration")
			}
			result.Remaining = Ptr(time.Duration(int64(*r.Ttl)-now) * time.Millisecond)
		}
		return result, nil
	})
}

// CreateKeyValue creates a scalar and returns its optional hash hints.
func (c *Client) CreateKeyValue(ctx context.Context, key []byte, value []byte, o Options) (*KeyHint, error) {
	return perform(c, ctx, key, o, false, func(s *session) (*KeyHint, error) {

		v, e := encodeValue(Payload{Data: value}, s.ttl, s.config)
		if e != nil {
			return nil, e
		}
		v.LockInfo = &pb.LockInfo{Type: NoLock, LockedBy: copyPtr(s.key.ClientId)}
		r, e := unary(s, &pb.CreateRequest{Key: s.key, Value: v}, s.stub.CreateKeyValue)
		if e != nil {
			return nil, e
		}
		return hintGo(r.KeyHint), nil
	})
}

// UpdateKeyValue updates a scalar and preserves success separately from the previous value.
func (c *Client) UpdateKeyValue(ctx context.Context, key []byte, value []byte, o Options) (UpdateResult, error) {
	return perform(c, ctx, key, o, false, func(s *session) (UpdateResult, error) {

		v, e := encodeValue(Payload{Data: value}, s.ttl, s.config)
		if e != nil {
			return UpdateResult{}, e
		}
		r, e := unary(s, &pb.UpdateRequest{Key: s.key, Value: v}, s.stub.UpdateValue)
		if e != nil {
			return UpdateResult{}, e
		}
		return decodeUpdate(r, s.config)
	})
}

// GetElementAtPosition operates on a position or an explicitly supplied range.
func (c *Client) GetElementAtPosition(ctx context.Context, key []byte, pos Position, o Options) (ValueResult, error) {
	return perform(c, ctx, key, o, false, func(s *session) (ValueResult, error) {

		if e := validatePosition(pos); e != nil {
			var z ValueResult
			return z, e
		}
		r, e := unary(s, positionPB(s.key, pos), s.stub.GetElementAtPosition)
		if e != nil {
			var z ValueResult
			return z, e
		}
		return decodeResult(r, s.config)
	})
}

// GetAndRemoveElementAtPosition operates on a position or an explicitly supplied range.
func (c *Client) GetAndRemoveElementAtPosition(ctx context.Context, key []byte, pos Position, o Options) (ValueResult, error) {
	return perform(c, ctx, key, o, true, func(s *session) (ValueResult, error) {

		if e := validatePosition(pos); e != nil {
			var z ValueResult
			return z, e
		}
		r, e := unary(s, positionPB(s.key, pos), s.stub.GetAndRemoveElementAtPosition)
		if e != nil {
			var z ValueResult
			return z, e
		}
		return decodeResult(r, s.config)
	})
}

// RemoveElementAtPosition operates on a position or an explicitly supplied range.
func (c *Client) RemoveElementAtPosition(ctx context.Context, key []byte, pos Position, o Options) (bool, error) {
	return perform(c, ctx, key, o, true, func(s *session) (bool, error) {

		if e := validatePosition(pos); e != nil {
			var z bool
			return z, e
		}
		r, e := unary(s, positionPB(s.key, pos), s.stub.RemoveElementAtPosition)
		if e != nil {
			var z bool
			return z, e
		}
		return r.Value, nil
	})
}

// GetContainerValue operates on an individual binary map key.
func (c *Client) GetContainerValue(ctx context.Context, key []byte, elementKey []byte, o Options) (ValueResult, error) {
	return perform(c, ctx, key, o, false, func(s *session) (ValueResult, error) {

		k, e := encodeKey(elementKey, nil, s.key.ClientId, s.config)
		if e != nil {
			var z ValueResult
			return z, e
		}
		r, e := unary(s, &pb.ContainerGetRequest{Key: s.key, ElementKey: k}, s.stub.GetValueInContainer)
		if e != nil {
			var z ValueResult
			return z, e
		}
		return decodeResult(r, s.config)
	})
}

// GetAndRemoveContainerValue operates on an individual binary map key.
func (c *Client) GetAndRemoveContainerValue(ctx context.Context, key []byte, elementKey []byte, o Options) (ValueResult, error) {
	return perform(c, ctx, key, o, true, func(s *session) (ValueResult, error) {

		k, e := encodeKey(elementKey, nil, s.key.ClientId, s.config)
		if e != nil {
			var z ValueResult
			return z, e
		}
		r, e := unary(s, &pb.ContainerGetRequest{Key: s.key, ElementKey: k}, s.stub.GetAndDeleteValueInContainer)
		if e != nil {
			var z ValueResult
			return z, e
		}
		return decodeResult(r, s.config)
	})
}

// ContainsContainerKey operates on an individual binary map key.
func (c *Client) ContainsContainerKey(ctx context.Context, key []byte, elementKey []byte, o Options) (bool, error) {
	return perform(c, ctx, key, o, false, func(s *session) (bool, error) {

		k, e := encodeKey(elementKey, nil, s.key.ClientId, s.config)
		if e != nil {
			var z bool
			return z, e
		}
		r, e := unary(s, &pb.ContainerGetRequest{Key: s.key, ElementKey: k}, s.stub.ExistKeyInContainer)
		if e != nil {
			var z bool
			return z, e
		}
		return r.Value, nil
	})
}

// RemoveContainerKey operates on an individual binary map key.
func (c *Client) RemoveContainerKey(ctx context.Context, key []byte, elementKey []byte, o Options) (uint32, error) {
	return perform(c, ctx, key, o, true, func(s *session) (uint32, error) {

		k, e := encodeKey(elementKey, nil, s.key.ClientId, s.config)
		if e != nil {
			var z uint32
			return z, e
		}
		r, e := unary(s, &pb.ContainerGetRequest{Key: s.key, ElementKey: k}, s.stub.RemoveInContainer)
		if e != nil {
			var z uint32
			return z, e
		}
		return r.Size, nil
	})
}

// UpdateContainerValue updates a map entry and returns the previous value and success.
func (c *Client) UpdateContainerValue(ctx context.Context, key []byte, elementKey, value []byte, o Options) (UpdateResult, error) {
	return perform(c, ctx, key, o, true, func(s *session) (UpdateResult, error) {

		k, e := encodeKey(elementKey, nil, s.key.ClientId, s.config)
		if e != nil {
			return UpdateResult{}, e
		}
		v, e := encodeValue(Payload{Data: value}, nil, s.config)
		if e != nil {
			return UpdateResult{}, e
		}
		r, e := unary(s, &pb.UpdateContainerRequest{Key: s.key, ElementKey: k, Value: v}, s.stub.UpdateValueInContainer)
		if e != nil {
			return UpdateResult{}, e
		}
		return decodeUpdate(r, s.config)
	})
}

// LockObject requests a lock; duration is truncated to whole seconds as in Java.
func (c *Client) LockObject(ctx context.Context, key []byte, kind LockType, duration time.Duration, o Options) (LockResult, error) {
	return perform(c, ctx, key, o, true, func(s *session) (LockResult, error) {

		if kind < NoLock || kind > GlobalLock || duration < 0 || uint64(duration/time.Second) > math.MaxUint32 {
			return LockResult{}, status.Error(codes.InvalidArgument, "invalid lock type or duration")
		}
		r, e := unary(s, &pb.LockRequest{Key: s.key, LockType: kind, ClientId: copyPtr(s.key.ClientId), LockDuration: Ptr(uint32(duration / time.Second))}, s.stub.LockObject)
		if e != nil {
			return LockResult{}, e
		}
		return LockResult{r.Result, copyPtr(r.Message)}, nil
	})
}

// UnlockObject releases a lock for the effective client ID.
func (c *Client) UnlockObject(ctx context.Context, key []byte, o Options) (LockResult, error) {
	return perform(c, ctx, key, o, true, func(s *session) (LockResult, error) {

		r, e := unary(s, &pb.UnLockRequest{Key: s.key, ClientId: copyPtr(s.key.ClientId)}, s.stub.UnlockObject)
		if e != nil {
			return LockResult{}, e
		}
		return LockResult{r.Result, copyPtr(r.Message)}, nil
	})
}

// AtomicCreate performs the corresponding server atomic operation, preserving its return semantics.
func (c *Client) AtomicCreate(ctx context.Context, key []byte, value int64, o Options) (*KeyHint, error) {
	return perform(c, ctx, key, o, true, func(s *session) (*KeyHint, error) {

		r, e := unary(s, &pb.AtomicCreate{Key: s.key, Val: &pb.AtomicValue{Val: value}, Ttl: copyPtr(s.ttl)}, s.stub.AtomicCreate)
		if e != nil {
			var z *KeyHint
			return z, e
		}
		return hintGo(r.KeyHint), nil
	})
}

// AtomicStore performs the corresponding server atomic operation, preserving its return semantics.
func (c *Client) AtomicStore(ctx context.Context, key []byte, value int64, o Options) (*KeyHint, error) {
	return perform(c, ctx, key, o, true, func(s *session) (*KeyHint, error) {

		r, e := unary(s, &pb.AtomicCreate{Key: s.key, Val: &pb.AtomicValue{Val: value}, Ttl: copyPtr(s.ttl)}, s.stub.AtomicStore)
		if e != nil {
			var z *KeyHint
			return z, e
		}
		return hintGo(r.KeyHint), nil
	})
}

// AtomicExchange performs the corresponding server atomic operation, preserving its return semantics.
func (c *Client) AtomicExchange(ctx context.Context, key []byte, value int64, o Options) (AtomicResult, error) {
	return perform(c, ctx, key, o, true, func(s *session) (AtomicResult, error) {

		r, e := unary(s, &pb.AtomicCreate{Key: s.key, Val: &pb.AtomicValue{Val: value}, Ttl: copyPtr(s.ttl)}, s.stub.AtomicExchange)
		if e != nil {
			var z AtomicResult
			return z, e
		}
		return atomicGo(r), nil
	})
}

// AtomicAdd performs the corresponding server atomic operation, preserving its return semantics.
func (c *Client) AtomicAdd(ctx context.Context, key []byte, value int64, o Options) (AtomicResult, error) {
	return perform(c, ctx, key, o, true, func(s *session) (AtomicResult, error) {

		r, e := unary(s, &pb.AtomicCreate{Key: s.key, Val: &pb.AtomicValue{Val: value}, Ttl: copyPtr(s.ttl)}, s.stub.AtomicAdd)
		if e != nil {
			var z AtomicResult
			return z, e
		}
		return atomicGo(r), nil
	})
}

// AtomicSub performs the corresponding server atomic operation, preserving its return semantics.
func (c *Client) AtomicSub(ctx context.Context, key []byte, value int64, o Options) (AtomicResult, error) {
	return perform(c, ctx, key, o, true, func(s *session) (AtomicResult, error) {

		r, e := unary(s, &pb.AtomicCreate{Key: s.key, Val: &pb.AtomicValue{Val: value}, Ttl: copyPtr(s.ttl)}, s.stub.AtomicSub)
		if e != nil {
			var z AtomicResult
			return z, e
		}
		return atomicGo(r), nil
	})
}

// AtomicOr performs the corresponding server atomic operation, preserving its return semantics.
func (c *Client) AtomicOr(ctx context.Context, key []byte, value int64, o Options) (AtomicResult, error) {
	return perform(c, ctx, key, o, true, func(s *session) (AtomicResult, error) {

		r, e := unary(s, &pb.AtomicCreate{Key: s.key, Val: &pb.AtomicValue{Val: value}, Ttl: copyPtr(s.ttl)}, s.stub.AtomicOr)
		if e != nil {
			var z AtomicResult
			return z, e
		}
		return atomicGo(r), nil
	})
}

// AtomicAnd performs the corresponding server atomic operation, preserving its return semantics.
func (c *Client) AtomicAnd(ctx context.Context, key []byte, value int64, o Options) (AtomicResult, error) {
	return perform(c, ctx, key, o, true, func(s *session) (AtomicResult, error) {

		r, e := unary(s, &pb.AtomicCreate{Key: s.key, Val: &pb.AtomicValue{Val: value}, Ttl: copyPtr(s.ttl)}, s.stub.AtomicAnd)
		if e != nil {
			var z AtomicResult
			return z, e
		}
		return atomicGo(r), nil
	})
}

// AtomicXor performs the corresponding server atomic operation, preserving its return semantics.
func (c *Client) AtomicXor(ctx context.Context, key []byte, value int64, o Options) (AtomicResult, error) {
	return perform(c, ctx, key, o, true, func(s *session) (AtomicResult, error) {

		r, e := unary(s, &pb.AtomicCreate{Key: s.key, Val: &pb.AtomicValue{Val: value}, Ttl: copyPtr(s.ttl)}, s.stub.AtomicXor)
		if e != nil {
			var z AtomicResult
			return z, e
		}
		return atomicGo(r), nil
	})
}

// AtomicCompareAndSet compares and sets, preserving the optional observed value and hints.
func (c *Client) AtomicCompareAndSet(ctx context.Context, key []byte, expected, newValue int64, o Options) (CASResult, error) {
	return perform(c, ctx, key, o, true, func(s *session) (CASResult, error) {

		r, e := unary(s, &pb.AtomicCas{Key: s.key, Expected: &pb.AtomicValue{Val: expected}, ToSet: &pb.AtomicValue{Val: newValue}, Ttl: copyPtr(s.ttl)}, s.stub.AtomicCompareAndSet)
		if e != nil {
			return CASResult{}, e
		}
		result := CASResult{Swapped: r.Result, Hint: hintGo(r.KeyHint)}
		if r.Expected != nil {
			result.Expected = Ptr(atomicGo(r.Expected))
		}
		return result, nil
	})
}

func validatePosition(p Position) error {
	if p.End != nil && *p.End < p.Start {
		return status.Error(codes.InvalidArgument, "range end precedes start")
	}
	return nil
}
func positionPB(key *pb.Key, p Position) *pb.KeyPositionRequest {
	return &pb.KeyPositionRequest{Key: key, Type: copyPtr(p.Type), Pos: p.Start, End: copyPtr(p.End), Reverse: p.Reverse}
}
