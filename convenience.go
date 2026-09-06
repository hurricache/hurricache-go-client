package hurricache

import (
	"context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CreateQueue creates a Queue with all supplied initial data.
func (c *Client) CreateQueue(ctx context.Context, key []byte, data []Payload, o Options) (*KeyHint, error) {
	return c.CreateContainer(ctx, key, Queue, ContainerData{Values: data}, o)
}

// StreamQueue returns a decoded collection in server stream order.
func (c *Client) StreamQueue(ctx context.Context, key []byte, o Options) ([]Payload, error) {
	d, e := c.GetContainer(ctx, key, o)
	if e == nil && len(d.Values)+len(d.OrderedValues)+len(d.Entries)+len(d.OrderedEntries) != len(d.Values) {
		e = status.Error(codes.DataLoss, "unexpected collection representation")
	}
	return d.Values, e
}

// CreateList creates a List with all supplied initial data.
func (c *Client) CreateList(ctx context.Context, key []byte, data []Payload, o Options) (*KeyHint, error) {
	return c.CreateContainer(ctx, key, List, ContainerData{Values: data}, o)
}

// StreamList returns a decoded collection in server stream order.
func (c *Client) StreamList(ctx context.Context, key []byte, o Options) ([]Payload, error) {
	d, e := c.GetContainer(ctx, key, o)
	if e == nil && len(d.Values)+len(d.OrderedValues)+len(d.Entries)+len(d.OrderedEntries) != len(d.Values) {
		e = status.Error(codes.DataLoss, "unexpected collection representation")
	}
	return d.Values, e
}

// CreateVector creates a Vector with all supplied initial data.
func (c *Client) CreateVector(ctx context.Context, key []byte, data []Payload, o Options) (*KeyHint, error) {
	return c.CreateContainer(ctx, key, Vector, ContainerData{Values: data}, o)
}

// StreamVector returns a decoded collection in server stream order.
func (c *Client) StreamVector(ctx context.Context, key []byte, o Options) ([]Payload, error) {
	d, e := c.GetContainer(ctx, key, o)
	if e == nil && len(d.Values)+len(d.OrderedValues)+len(d.Entries)+len(d.OrderedEntries) != len(d.Values) {
		e = status.Error(codes.DataLoss, "unexpected collection representation")
	}
	return d.Values, e
}

// CreateSet creates a Set with all supplied initial data.
func (c *Client) CreateSet(ctx context.Context, key []byte, data []Payload, o Options) (*KeyHint, error) {
	return c.CreateContainer(ctx, key, Set, ContainerData{Values: data}, o)
}

// StreamSet returns a decoded collection in server stream order.
func (c *Client) StreamSet(ctx context.Context, key []byte, o Options) ([]Payload, error) {
	d, e := c.GetContainer(ctx, key, o)
	if e == nil && len(d.Values)+len(d.OrderedValues)+len(d.Entries)+len(d.OrderedEntries) != len(d.Values) {
		e = status.Error(codes.DataLoss, "unexpected collection representation")
	}
	return d.Values, e
}

// CreateOrderedSet creates a OrderedSet with all supplied initial data.
func (c *Client) CreateOrderedSet(ctx context.Context, key []byte, data []OrderedPayload, o Options) (*KeyHint, error) {
	return c.CreateContainer(ctx, key, OrderedSet, ContainerData{OrderedValues: data}, o)
}

// StreamOrderedSet returns a decoded collection in server stream order.
func (c *Client) StreamOrderedSet(ctx context.Context, key []byte, o Options) ([]OrderedPayload, error) {
	d, e := c.GetContainer(ctx, key, o)
	if e == nil && len(d.Values)+len(d.OrderedValues)+len(d.Entries)+len(d.OrderedEntries) != len(d.OrderedValues) {
		e = status.Error(codes.DataLoss, "unexpected collection representation")
	}
	return d.OrderedValues, e
}

// CreateMap creates a Map with all supplied initial data.
func (c *Client) CreateMap(ctx context.Context, key []byte, data []Entry, o Options) (*KeyHint, error) {
	return c.CreateContainer(ctx, key, Map, ContainerData{Entries: data}, o)
}

// StreamMap returns a decoded collection in server stream order.
func (c *Client) StreamMap(ctx context.Context, key []byte, o Options) ([]Entry, error) {
	d, e := c.GetContainer(ctx, key, o)
	if e == nil && len(d.Values)+len(d.OrderedValues)+len(d.Entries)+len(d.OrderedEntries) != len(d.Entries) {
		e = status.Error(codes.DataLoss, "unexpected collection representation")
	}
	return d.Entries, e
}

// CreateOrderedMap creates a OrderedMap with all supplied initial data.
func (c *Client) CreateOrderedMap(ctx context.Context, key []byte, data []OrderedEntry, o Options) (*KeyHint, error) {
	return c.CreateContainer(ctx, key, OrderedMap, ContainerData{OrderedEntries: data}, o)
}

// StreamOrderedMap returns a decoded collection in server stream order.
func (c *Client) StreamOrderedMap(ctx context.Context, key []byte, o Options) ([]OrderedEntry, error) {
	d, e := c.GetContainer(ctx, key, o)
	if e == nil && len(d.Values)+len(d.OrderedValues)+len(d.Entries)+len(d.OrderedEntries) != len(d.OrderedEntries) {
		e = status.Error(codes.DataLoss, "unexpected collection representation")
	}
	return d.OrderedEntries, e
}

// AddElement inserts every supplied item using bounded requests.
func (c *Client) AddElement(ctx context.Context, key []byte, data []Payload, o Options) (bool, error) {
	r, e := c.add(ctx, key, Set, ContainerData{Values: data}, addBool, 0, Payload{}, o)
	return r.ok, e
}

// AddElementOrdered inserts every supplied item using bounded requests.
func (c *Client) AddElementOrdered(ctx context.Context, key []byte, data []OrderedPayload, o Options) (bool, error) {
	r, e := c.add(ctx, key, OrderedSet, ContainerData{OrderedValues: data}, addOrderedBool, 0, Payload{}, o)
	return r.ok, e
}

// AddElementOrderedSet inserts every supplied item using bounded requests.
func (c *Client) AddElementOrderedSet(ctx context.Context, key []byte, data []OrderedPayload, o Options) (bool, error) {
	r, e := c.add(ctx, key, OrderedSet, ContainerData{OrderedValues: data}, addOrderedBool, 0, Payload{}, o)
	return r.ok, e
}

// AddElementToTail inserts every supplied item using bounded requests.
func (c *Client) AddElementToTail(ctx context.Context, key []byte, data []Payload, o Options) (bool, error) {
	r, e := c.add(ctx, key, List, ContainerData{Values: data}, addTail, 0, Payload{}, o)
	return r.ok, e
}

// AddElementToHead inserts every supplied item using bounded requests.
func (c *Client) AddElementToHead(ctx context.Context, key []byte, data []Payload, o Options) (bool, error) {
	r, e := c.add(ctx, key, List, ContainerData{Values: data}, addHead, 0, Payload{}, o)
	return r.ok, e
}

// AddElementToPosition inserts every supplied item using bounded requests.
func (c *Client) AddElementToPosition(ctx context.Context, key []byte, data []Payload, pos uint32, o Options) (uint32, error) {
	r, e := c.add(ctx, key, List, ContainerData{Values: data}, addPosition, pos, Payload{}, o)
	return r.count, e
}

// AddElementWithWeight inserts every supplied item using bounded requests.
func (c *Client) AddElementWithWeight(ctx context.Context, key []byte, data []OrderedPayload, o Options) (uint32, error) {
	r, e := c.add(ctx, key, OrderedSet, ContainerData{OrderedValues: data}, addGeneric, 0, Payload{}, o)
	return r.count, e
}

// AddElementToPositionBefore inserts every supplied item using bounded requests.
func (c *Client) AddElementToPositionBefore(ctx context.Context, key []byte, data []Payload, pivot Payload, o Options) (bool, error) {
	r, e := c.add(ctx, key, List, ContainerData{Values: data}, addBefore, 0, pivot, o)
	return r.ok, e
}

// AddElementToPositionAfter inserts every supplied item using bounded requests.
func (c *Client) AddElementToPositionAfter(ctx context.Context, key []byte, data []Payload, pivot Payload, o Options) (bool, error) {
	r, e := c.add(ctx, key, List, ContainerData{Values: data}, addAfter, 0, pivot, o)
	return r.ok, e
}

// AddElementHashMap inserts every supplied item using bounded requests.
func (c *Client) AddElementHashMap(ctx context.Context, key []byte, data []Entry, o Options) (uint32, error) {
	r, e := c.add(ctx, key, Map, ContainerData{Entries: data}, addGeneric, 0, Payload{}, o)
	return r.count, e
}

// AddElementOrderedMap inserts every supplied item using bounded requests.
func (c *Client) AddElementOrderedMap(ctx context.Context, key []byte, data []OrderedEntry, o Options) (uint32, error) {
	r, e := c.add(ctx, key, OrderedMap, ContainerData{OrderedEntries: data}, addGeneric, 0, Payload{}, o)
	return r.count, e
}

// StreamElementInRangeUnordered collects a LIST, VECTOR or SET positional range.
func (c *Client) StreamElementInRangeUnordered(ctx context.Context, key []byte, t ContainerType, start, end uint32, o Options) ([]Payload, error) {
	if t != List && t != Vector && t != Set {
		return nil, status.Error(codes.InvalidArgument, "unsupported unordered range type")
	}
	d, e := c.GetElementInRange(ctx, key, Position{Start: uint64(start), End: Ptr(uint64(end)), Type: Ptr(t)}, o)
	if e == nil && len(d.OrderedValues)+len(d.Entries)+len(d.OrderedEntries) > 0 {
		e = status.Error(codes.DataLoss, "unexpected range representation")
	}
	return d.Values, e
}

// StreamElementInRangeOrderedSet collects an unsigned weighted range, optionally reversed.
func (c *Client) StreamElementInRangeOrderedSet(ctx context.Context, key []byte, start, end uint64, reverse bool, o Options) ([]OrderedPayload, error) {
	d, e := c.GetElementInRange(ctx, key, Position{Start: start, End: Ptr(end), Type: Ptr(OrderedSet), Reverse: reverse}, o)
	if e == nil && len(d.Values)+len(d.Entries)+len(d.OrderedEntries) > 0 {
		e = status.Error(codes.DataLoss, "unexpected range representation")
	}
	return d.OrderedValues, e
}

// StreamElementInRangeOrdered is the convenience alias for an ascending ordered-set range.
func (c *Client) StreamElementInRangeOrdered(ctx context.Context, key []byte, start, end uint64, o Options) ([]OrderedPayload, error) {
	return c.StreamElementInRangeOrderedSet(ctx, key, start, end, false, o)
}
