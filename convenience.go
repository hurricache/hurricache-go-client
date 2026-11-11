package hurricache

import (
	"context"
	pb "github.com/hurricache/hurricache-go-client/proto/cachepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (c *Client) CreateQueue(ctx context.Context, key []byte, data []Variant, o Options) (*KeyHint, error) {
	return c.CreateContainer(ctx, key, Queue, ContainerData{Values: data}, o)
}

// Deprecated: full queue-content reads are intentionally unsupported by the
// server. Use GetSize, PeekHead/PeekTail, and PopHead/PopTail instead.
func (c *Client) GetQueue(ctx context.Context, key []byte, o Options) ([]Variant, error) {
	d, e := c.GetContainer(ctx, key, o)
	if e == nil && len(d.Values)+len(d.OrderedValues)+len(d.Entries)+len(d.OrderedEntries) != len(d.Values) {
		e = status.Error(codes.DataLoss, "unexpected collection representation")
	}
	return d.Values, e
}

// Deprecated: use GetQueue.
func (c *Client) StreamQueue(ctx context.Context, key []byte, o Options) ([]Variant, error) {
	return c.GetQueue(ctx, key, o)
}

func (c *Client) CreateList(ctx context.Context, key []byte, data []Variant, o Options) (*KeyHint, error) {
	return c.CreateContainer(ctx, key, List, ContainerData{Values: data}, o)
}

// GetList preserves the server's iteration order and typed values.
func (c *Client) GetList(ctx context.Context, key []byte, o Options) ([]Variant, error) {
	d, e := c.GetContainer(ctx, key, o)
	if e == nil && len(d.Values)+len(d.OrderedValues)+len(d.Entries)+len(d.OrderedEntries) != len(d.Values) {
		e = status.Error(codes.DataLoss, "unexpected collection representation")
	}
	return d.Values, e
}

// Deprecated: use GetList.
func (c *Client) StreamList(ctx context.Context, key []byte, o Options) ([]Variant, error) {
	return c.GetList(ctx, key, o)
}

func (c *Client) CreateVector(ctx context.Context, key []byte, data []Variant, o Options) (*KeyHint, error) {
	return c.CreateContainer(ctx, key, Vector, ContainerData{Values: data}, o)
}

// GetVector preserves the server's iteration order and typed values.
func (c *Client) GetVector(ctx context.Context, key []byte, o Options) ([]Variant, error) {
	d, e := c.GetContainer(ctx, key, o)
	if e == nil && len(d.Values)+len(d.OrderedValues)+len(d.Entries)+len(d.OrderedEntries) != len(d.Values) {
		e = status.Error(codes.DataLoss, "unexpected collection representation")
	}
	return d.Values, e
}

// Deprecated: use GetVector.
func (c *Client) StreamVector(ctx context.Context, key []byte, o Options) ([]Variant, error) {
	return c.GetVector(ctx, key, o)
}

func (c *Client) CreateSet(ctx context.Context, key []byte, data []Variant, o Options) (*KeyHint, error) {
	return c.CreateContainer(ctx, key, Set, ContainerData{Values: data}, o)
}

// GetSet preserves the server's iteration order and typed values.
func (c *Client) GetSet(ctx context.Context, key []byte, o Options) ([]Variant, error) {
	d, e := c.GetContainer(ctx, key, o)
	if e == nil && len(d.Values)+len(d.OrderedValues)+len(d.Entries)+len(d.OrderedEntries) != len(d.Values) {
		e = status.Error(codes.DataLoss, "unexpected collection representation")
	}
	return d.Values, e
}

// Deprecated: use GetSet.
func (c *Client) StreamSet(ctx context.Context, key []byte, o Options) ([]Variant, error) {
	return c.GetSet(ctx, key, o)
}

func (c *Client) CreateOrderedSet(ctx context.Context, key []byte, data []OrderedVariant, o Options) (*KeyHint, error) {
	return c.CreateContainer(ctx, key, OrderedSet, ContainerData{OrderedValues: data}, o)
}

// GetOrderedSet preserves the server's iteration order and typed values.
func (c *Client) GetOrderedSet(ctx context.Context, key []byte, o Options) ([]OrderedVariant, error) {
	d, e := c.GetContainer(ctx, key, o)
	if e == nil && len(d.Values)+len(d.OrderedValues)+len(d.Entries)+len(d.OrderedEntries) != len(d.OrderedValues) {
		e = status.Error(codes.DataLoss, "unexpected collection representation")
	}
	return d.OrderedValues, e
}

// Deprecated: use GetOrderedSet.
func (c *Client) StreamOrderedSet(ctx context.Context, key []byte, o Options) ([]OrderedVariant, error) {
	return c.GetOrderedSet(ctx, key, o)
}

func (c *Client) CreateMap(ctx context.Context, key []byte, data []Entry, o Options) (*KeyHint, error) {
	return c.CreateContainer(ctx, key, Map, ContainerData{Entries: data}, o)
}

// GetMap preserves the server's iteration order and typed values.
func (c *Client) GetMap(ctx context.Context, key []byte, o Options) ([]Entry, error) {
	d, e := c.GetContainer(ctx, key, o)
	if e == nil && len(d.Values)+len(d.OrderedValues)+len(d.Entries)+len(d.OrderedEntries) != len(d.Entries) {
		e = status.Error(codes.DataLoss, "unexpected collection representation")
	}
	return d.Entries, e
}

// Deprecated: use GetMap.
func (c *Client) StreamMap(ctx context.Context, key []byte, o Options) ([]Entry, error) {
	return c.GetMap(ctx, key, o)
}

func (c *Client) CreateOrderedMap(ctx context.Context, key []byte, data []OrderedEntry, o Options) (*KeyHint, error) {
	return c.CreateContainer(ctx, key, OrderedMap, ContainerData{OrderedEntries: data}, o)
}

// GetOrderedMap preserves the server's iteration order and typed values.
func (c *Client) GetOrderedMap(ctx context.Context, key []byte, o Options) ([]OrderedEntry, error) {
	d, e := c.GetContainer(ctx, key, o)
	if e == nil && len(d.Values)+len(d.OrderedValues)+len(d.Entries)+len(d.OrderedEntries) != len(d.OrderedEntries) {
		e = status.Error(codes.DataLoss, "unexpected collection representation")
	}
	return d.OrderedEntries, e
}

// Deprecated: use GetOrderedMap.
func (c *Client) StreamOrderedMap(ctx context.Context, key []byte, o Options) ([]OrderedEntry, error) {
	return c.GetOrderedMap(ctx, key, o)
}

func (c *Client) PushHead(ctx context.Context, key []byte, data []Variant, o Options) (uint32, error) {
	r, e := c.add(ctx, key, List, ContainerData{Values: data}, addHead, 0, None(), o)
	return r.count, e
}

func (c *Client) PushTail(ctx context.Context, key []byte, data []Variant, o Options) (uint32, error) {
	r, e := c.add(ctx, key, List, ContainerData{Values: data}, addTail, 0, None(), o)
	return r.count, e
}

func (c *Client) AddElementUnordered(ctx context.Context, key []byte, data []Variant, o Options) (uint32, error) {
	r, e := c.add(ctx, key, List, ContainerData{Values: data}, addBool, 0, None(), o)
	return r.count, e
}

func (c *Client) AddElementWithWeight(ctx context.Context, key []byte, data []OrderedVariant, o Options) (uint32, error) {
	r, e := c.add(ctx, key, OrderedSet, ContainerData{OrderedValues: data}, addGeneric, 0, None(), o)
	return r.count, e
}

func (c *Client) InsertAt(ctx context.Context, key []byte, data []Variant, pos uint32, o Options) (uint32, error) {
	r, e := c.add(ctx, key, List, ContainerData{Values: data}, addPosition, pos, None(), o)
	return r.count, e
}

func (c *Client) InsertBefore(ctx context.Context, key []byte, data []Variant, pivot Variant, o Options) (uint32, error) {
	r, e := c.add(ctx, key, List, ContainerData{Values: data}, addBefore, 0, pivot, o)
	return r.count, e
}

func (c *Client) InsertAfter(ctx context.Context, key []byte, data []Variant, pivot Variant, o Options) (uint32, error) {
	r, e := c.add(ctx, key, List, ContainerData{Values: data}, addAfter, 0, pivot, o)
	return r.count, e
}

func (c *Client) PutAll(ctx context.Context, key []byte, data []Entry, o Options) (uint32, error) {
	r, e := c.add(ctx, key, Map, ContainerData{Entries: data}, addGeneric, 0, None(), o)
	return r.count, e
}

func (c *Client) AddElementOrderedMap(ctx context.Context, key []byte, data []OrderedEntry, o Options) (uint32, error) {
	r, e := c.add(ctx, key, OrderedMap, ContainerData{OrderedEntries: data}, addGeneric, 0, None(), o)
	return r.count, e
}

// GetRange returns typed values from a LIST, VECTOR or SET positional range.
func (c *Client) GetRange(ctx context.Context, key []byte, pos Position, o Options) ([]Variant, error) {
	if pos.Type == nil {
		pos.Type = Ptr(List)
	}
	if *pos.Type != List && *pos.Type != Vector && *pos.Type != Set {
		return nil, status.Error(codes.InvalidArgument, "GetRange requires LIST, VECTOR or SET")
	}
	d, e := c.GetElementInRange(ctx, key, pos, o)
	if e == nil && len(d.OrderedValues)+len(d.Entries)+len(d.OrderedEntries) > 0 {
		e = status.Error(codes.DataLoss, "unexpected range representation")
	}
	return d.Values, e
}

// PopRange destructively collects a range. A failure returns its decoded prefix
// and never permits smart replay after receiving any batch.
func (c *Client) PopRange(ctx context.Context, key []byte, pos Position, o Options) (ContainerData, error) {
	return perform(c, ctx, key, o, true, func(s *session) (ContainerData, error) { return collect(s, &pos, true) })
}

func (c *Client) GetListElementsInRange(ctx context.Context, key []byte, start, end *uint64, reverse bool, o Options) ([]Variant, error) {
	p := Position{OmitStart: start == nil, End: copyPtr(end), Type: Ptr(List), Reverse: reverse}
	if start != nil {
		p.Start = *start
	}
	d, e := c.GetElementInRange(ctx, key, p, o)
	if e == nil && len(d.Values)+len(d.OrderedValues)+len(d.Entries)+len(d.OrderedEntries) != len(d.Values) {
		e = status.Error(codes.DataLoss, "unexpected range representation")
	}
	return d.Values, e
}

func (c *Client) GetAndDeleteListElementsInRange(ctx context.Context, key []byte, start, end *uint64, reverse bool, o Options) ([]Variant, error) {
	p := Position{OmitStart: start == nil, End: copyPtr(end), Type: Ptr(List), Reverse: reverse}
	if start != nil {
		p.Start = *start
	}
	d, e := c.PopRange(ctx, key, p, o)
	if e == nil && len(d.Values)+len(d.OrderedValues)+len(d.Entries)+len(d.OrderedEntries) != len(d.Values) {
		e = status.Error(codes.DataLoss, "unexpected range representation")
	}
	return d.Values, e
}

func (c *Client) GetVectorElementsInRange(ctx context.Context, key []byte, start, end *uint64, reverse bool, o Options) ([]Variant, error) {
	p := Position{OmitStart: start == nil, End: copyPtr(end), Type: Ptr(Vector), Reverse: reverse}
	if start != nil {
		p.Start = *start
	}
	d, e := c.GetElementInRange(ctx, key, p, o)
	if e == nil && len(d.Values)+len(d.OrderedValues)+len(d.Entries)+len(d.OrderedEntries) != len(d.Values) {
		e = status.Error(codes.DataLoss, "unexpected range representation")
	}
	return d.Values, e
}

func (c *Client) GetAndDeleteVectorElementsInRange(ctx context.Context, key []byte, start, end *uint64, reverse bool, o Options) ([]Variant, error) {
	p := Position{OmitStart: start == nil, End: copyPtr(end), Type: Ptr(Vector), Reverse: reverse}
	if start != nil {
		p.Start = *start
	}
	d, e := c.PopRange(ctx, key, p, o)
	if e == nil && len(d.Values)+len(d.OrderedValues)+len(d.Entries)+len(d.OrderedEntries) != len(d.Values) {
		e = status.Error(codes.DataLoss, "unexpected range representation")
	}
	return d.Values, e
}

func (c *Client) GetElementInRangeOrderedSet(ctx context.Context, key []byte, start, end *uint64, reverse bool, o Options) ([]OrderedVariant, error) {
	p := Position{OmitStart: start == nil, End: copyPtr(end), Type: Ptr(OrderedSet), Reverse: reverse}
	if start != nil {
		p.Start = *start
	}
	d, e := c.GetElementInRange(ctx, key, p, o)
	if e == nil && len(d.Values)+len(d.OrderedValues)+len(d.Entries)+len(d.OrderedEntries) != len(d.OrderedValues) {
		e = status.Error(codes.DataLoss, "unexpected range representation")
	}
	return d.OrderedValues, e
}

func (c *Client) GetAndDeleteElementInRangeOrderedSet(ctx context.Context, key []byte, start, end *uint64, reverse bool, o Options) ([]OrderedVariant, error) {
	p := Position{OmitStart: start == nil, End: copyPtr(end), Type: Ptr(OrderedSet), Reverse: reverse}
	if start != nil {
		p.Start = *start
	}
	d, e := c.PopRange(ctx, key, p, o)
	if e == nil && len(d.Values)+len(d.OrderedValues)+len(d.Entries)+len(d.OrderedEntries) != len(d.OrderedValues) {
		e = status.Error(codes.DataLoss, "unexpected range representation")
	}
	return d.OrderedValues, e
}

func (c *Client) GetElementInRangeOrderedMap(ctx context.Context, key []byte, start, end *uint64, reverse bool, o Options) ([]OrderedEntry, error) {
	p := Position{OmitStart: start == nil, End: copyPtr(end), Type: Ptr(OrderedMap), Reverse: reverse}
	if start != nil {
		p.Start = *start
	}
	d, e := c.GetElementInRange(ctx, key, p, o)
	if e == nil && len(d.Values)+len(d.OrderedValues)+len(d.Entries)+len(d.OrderedEntries) != len(d.OrderedEntries) {
		e = status.Error(codes.DataLoss, "unexpected range representation")
	}
	return d.OrderedEntries, e
}

func (c *Client) GetAndDeleteElementInRangeOrderedMap(ctx context.Context, key []byte, start, end *uint64, reverse bool, o Options) ([]OrderedEntry, error) {
	p := Position{OmitStart: start == nil, End: copyPtr(end), Type: Ptr(OrderedMap), Reverse: reverse}
	if start != nil {
		p.Start = *start
	}
	d, e := c.PopRange(ctx, key, p, o)
	if e == nil && len(d.Values)+len(d.OrderedValues)+len(d.Entries)+len(d.OrderedEntries) != len(d.OrderedEntries) {
		e = status.Error(codes.DataLoss, "unexpected range representation")
	}
	return d.OrderedEntries, e
}

func (c *Client) RemoveFromContainerByKey(ctx context.Context, key []byte, containerKey Payload, o Options) (uint32, error) {
	return perform(c, ctx, key, o, true, func(s *session) (uint32, error) {
		k, e := encodeKey(containerKey.Data, containerKey.Hint, s.key.ClientId, s.config)
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
func (c *Client) RemoveFromContainerByKeyAndValue(ctx context.Context, key []byte, keys []Payload, values []Variant, o Options) (uint32, error) {
	return c.RemoveFromContainer(ctx, key, Removal{Keys: keys, Values: values}, o)
}

// Deprecated: use AddElementUnordered.
func (c *Client) AddElement(ctx context.Context, key []byte, data []Variant, o Options) (uint32, error) {
	return c.AddElementUnordered(ctx, key, data, o)
}

// Deprecated: use AddElementWithWeight.
func (c *Client) AddElementOrdered(ctx context.Context, key []byte, data []OrderedVariant, o Options) (uint32, error) {
	return c.AddElementWithWeight(ctx, key, data, o)
}

// Deprecated: use AddElementWithWeight.
func (c *Client) AddElementOrderedSet(ctx context.Context, key []byte, data []OrderedVariant, o Options) (uint32, error) {
	return c.AddElementWithWeight(ctx, key, data, o)
}

// Deprecated: use PushHead.
func (c *Client) AddElementToHead(ctx context.Context, key []byte, data []Variant, o Options) (uint32, error) {
	return c.PushHead(ctx, key, data, o)
}

// Deprecated: use PushHead.
func (c *Client) AddElementToHeadCount(ctx context.Context, key []byte, data []Variant, o Options) (uint32, error) {
	return c.PushHead(ctx, key, data, o)
}

// Deprecated: use PushTail.
func (c *Client) AddElementToTail(ctx context.Context, key []byte, data []Variant, o Options) (uint32, error) {
	return c.PushTail(ctx, key, data, o)
}

// Deprecated: use PushTail.
func (c *Client) AddElementToTailCount(ctx context.Context, key []byte, data []Variant, o Options) (uint32, error) {
	return c.PushTail(ctx, key, data, o)
}

// Deprecated: use InsertAt.
func (c *Client) AddElementToPosition(ctx context.Context, key []byte, data []Variant, pos uint32, o Options) (uint32, error) {
	return c.InsertAt(ctx, key, data, pos, o)
}

// Deprecated: use InsertBefore.
func (c *Client) AddElementToPositionBefore(ctx context.Context, key []byte, data []Variant, pivot Variant, o Options) (uint32, error) {
	return c.InsertBefore(ctx, key, data, pivot, o)
}

// Deprecated: use InsertBefore.
func (c *Client) AddElementToPositionBeforeCount(ctx context.Context, key []byte, data []Variant, pivot Variant, o Options) (uint32, error) {
	return c.InsertBefore(ctx, key, data, pivot, o)
}

// Deprecated: use InsertAfter.
func (c *Client) AddElementToPositionAfter(ctx context.Context, key []byte, data []Variant, pivot Variant, o Options) (uint32, error) {
	return c.InsertAfter(ctx, key, data, pivot, o)
}

// Deprecated: use InsertAfter.
func (c *Client) AddElementToPositionAfterCount(ctx context.Context, key []byte, data []Variant, pivot Variant, o Options) (uint32, error) {
	return c.InsertAfter(ctx, key, data, pivot, o)
}

// Deprecated: use PutAll.
func (c *Client) AddElementHashMap(ctx context.Context, key []byte, data []Entry, o Options) (uint32, error) {
	return c.PutAll(ctx, key, data, o)
}

// Deprecated: use GetRange.
func (c *Client) StreamElementInRangeUnordered(ctx context.Context, key []byte, t ContainerType, start, end uint32, o Options) ([]Variant, error) {
	d, e := c.GetElementInRange(ctx, key, Position{Start: uint64(start), End: Ptr(uint64(end)), Type: Ptr(t)}, o)
	return d.Values, e
}

// Deprecated: use GetElementInRangeOrderedSet.
func (c *Client) StreamElementInRangeOrderedSet(ctx context.Context, key []byte, start, end uint64, reverse bool, o Options) ([]OrderedVariant, error) {
	return c.GetElementInRangeOrderedSet(ctx, key, Ptr(start), Ptr(end), reverse, o)
}

// Deprecated: use GetElementInRangeOrderedSet.
func (c *Client) StreamElementInRangeOrdered(ctx context.Context, key []byte, start, end uint64, o Options) ([]OrderedVariant, error) {
	return c.GetElementInRangeOrderedSet(ctx, key, Ptr(start), Ptr(end), false, o)
}

// Deprecated: use GetElementInRangeOrderedMap.
func (c *Client) StreamElementInRangeOrderedMap(ctx context.Context, key []byte, start, end uint64, reverse bool, o Options) ([]OrderedEntry, error) {
	return c.GetElementInRangeOrderedMap(ctx, key, Ptr(start), Ptr(end), reverse, o)
}
