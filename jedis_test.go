package hurricache

import (
	"context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
)

func TestJedisSharedLifecycle(t *testing.T) {
	if _, e := NewJedis(nil); e == nil {
		t.Fatal("nil client accepted")
	}
	if _, e := NewJedisPool(nil); e == nil {
		t.Fatal("nil pool client accepted")
	}
	for name, c := range currentClients(t, Config{}, nil, nil) {
		t.Run(name, func(t *testing.T) {
			pool, e := NewJedisPool(c)
			if e != nil {
				t.Fatal(e)
			}
			a, e := pool.GetResource()
			if e != nil {
				t.Fatal(e)
			}
			b, e := pool.GetResource()
			if e != nil {
				t.Fatal(e)
			}
			if a.Ping() != "PONG" || b.Ping() != "PONG" {
				t.Fatal("local ping")
			}
			if e = a.Close(); e != nil {
				t.Fatal(e)
			}
			if _, e = c.Get(context.Background(), []byte("k"), Options{}); status.Code(e) != codes.Canceled {
				t.Fatal("resource did not close shared client", e)
			}
			if e = b.Close(); e != nil {
				t.Fatal(e)
			}
			if e = pool.Close(); e != nil {
				t.Fatal(e)
			}
		})
	}
}
