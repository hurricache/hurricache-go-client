package hurricache

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Jedis implements only the active Java adapter's lifecycle and local Ping.
// Commented Redis commands in the Java source are deliberately not implemented.
type Jedis struct{ client Operations }

func NewJedis(client Operations) (*Jedis, error) {
	if client == nil {
		return nil, status.Error(codes.InvalidArgument, "nil cache client")
	}
	return &Jedis{client: client}, nil
}
func (j *Jedis) Ping() string { return "PONG" }
func (j *Jedis) Close() error { return j.client.Close() }

// JedisPool creates lightweight wrappers sharing one client's lifecycle.
// Closing any resource or the pool closes that shared client, as in Java.
type JedisPool struct{ client Operations }

func NewJedisPool(client Operations) (*JedisPool, error) {
	if client == nil {
		return nil, status.Error(codes.InvalidArgument, "nil cache client")
	}
	return &JedisPool{client: client}, nil
}
func (j *JedisPool) GetResource() (*Jedis, error) { return NewJedis(j.client) }
func (j *JedisPool) Close() error                 { return j.client.Close() }
