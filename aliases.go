package hurricache

import (
	"context"
)

// Deprecated: use Exchange. Results are native int64 or float64.
func (c *Client) AtomicExchange(ctx context.Context, key []byte, value int64, o Options) (any, error) {
	return c.Exchange(ctx, key, value, o)
}

// Deprecated: use Add. Results are native int64 or float64.
func (c *Client) AtomicAdd(ctx context.Context, key []byte, value int64, o Options) (any, error) {
	return c.Add(ctx, key, value, o)
}

// Deprecated: use Sub. Results are native int64 or float64.
func (c *Client) AtomicSub(ctx context.Context, key []byte, value int64, o Options) (any, error) {
	return c.Sub(ctx, key, value, o)
}

// Deprecated: use And.
func (c *Client) AtomicAnd(ctx context.Context, key []byte, value int64, o Options) (int64, error) {
	return c.And(ctx, key, value, o)
}

// Deprecated: use Or.
func (c *Client) AtomicOr(ctx context.Context, key []byte, value int64, o Options) (int64, error) {
	return c.Or(ctx, key, value, o)
}

// Deprecated: use Xor.
func (c *Client) AtomicXor(ctx context.Context, key []byte, value int64, o Options) (int64, error) {
	return c.Xor(ctx, key, value, o)
}

// Deprecated: use CompareAndSet.
func (c *Client) AtomicCompareAndSet(ctx context.Context, key []byte, expected, newValue int64, o Options) (CASResult, error) {
	return c.CompareAndSet(ctx, key, expected, newValue, o)
}

// Deprecated: use GetAt for a point read.
func (c *Client) GetElementAtPosition(ctx context.Context, key []byte, pos Position, o Options) (Variant, error) {
	return perform(c, ctx, key, o, false, func(s *session) (Variant, error) {
		r, e := unary(s, positionPB(s.key, pos), s.stub.GetElementAtPosition)
		if e != nil {
			return None(), e
		}
		return variantResult(r, s.config)
	})
}

// Deprecated: use PopAt for a point pop.
func (c *Client) GetAndRemoveElementAtPosition(ctx context.Context, key []byte, pos Position, o Options) (Variant, error) {
	return perform(c, ctx, key, o, true, func(s *session) (Variant, error) {
		r, e := unary(s, positionPB(s.key, pos), s.stub.GetAndDeleteElementAtPosition)
		if e != nil {
			return None(), e
		}
		return variantResult(r, s.config)
	})
}

// Deprecated: use RemoveAt. Results are integer mutation counts.
func (c *Client) RemoveElementAtPosition(ctx context.Context, key []byte, pos Position, o Options) (uint32, error) {
	return c.RemoveAt(ctx, key, pos, o)
}
