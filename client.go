package hurricache

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	pb "github.com/hurricache/hurricache-go-client/proto/cachepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"sync"
	"time"
)

const DefaultMaxRequestBytes = 7 * 1024 * 1024 / 2
const DefaultMaxDecodedBytes = 64 * 1024 * 1024

// Config is copied at construction. Do not mutate slices or TLS settings concurrently.
type Config struct {
	ClientID        uint32
	Timeout         time.Duration
	DefaultTTL      time.Duration
	TLS             *tls.Config
	DialOptions     []grpc.DialOption
	MaxRequestBytes int
	MaxDecodedBytes int
}

// Connection declares ownership of a gRPC channel.
type Connection struct {
	Conn  grpc.ClientConnInterface
	Close func() error
}

// RPCError retains the gRPC status (including details) and trailers.
type RPCError struct {
	Err      error
	Trailers metadata.MD
}

func (e *RPCError) Error() string              { return e.Err.Error() }
func (e *RPCError) Unwrap() error              { return e.Err }
func (e *RPCError) GRPCStatus() *status.Status { return status.Convert(e.Err) }

// PartialError reports acknowledged work before a later failure. The failed RPC may
// also have executed on the server. CompletedItems counts input items, not changes.
// CompletedChunks > 0 forbids replaying the whole operation through smart routing.
type PartialError struct {
	CompletedChunks, CompletedItems int
	Err                             error
}

func (e *PartialError) Error() string {
	return fmt.Sprintf("hurricache: %d chunks (%d items) completed: %v", e.CompletedChunks, e.CompletedItems, e.Err)
}
func (e *PartialError) Unwrap() error              { return e.Err }
func (e *PartialError) GRPCStatus() *status.Status { return status.Convert(e.Err) }

type backend interface {
	run(context.Context, Options, bool, func(pb.HurriCacheGrpcServiceClient) error) error
	close() error
	target() string
}
type directBackend struct {
	stub       pb.HurriCacheGrpcServiceClient
	connection Connection
	address    string
}

func (d *directBackend) run(ctx context.Context, _ Options, _ bool, f func(pb.HurriCacheGrpcServiceClient) error) error {
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	return f(d.stub)
}
func (d *directBackend) close() error {
	if d.connection.Close != nil {
		return d.connection.Close()
	}
	return nil
}
func (d *directBackend) target() string { return d.address }

// Client is safe for concurrent operations. Close cancels this client's pending work.
type Client struct {
	config   Config
	backend  backend
	life     context.Context
	cancel   context.CancelFunc
	once     sync.Once
	closeErr error
}

func normalizeConfig(c Config) (Config, error) {
	if c.Timeout < 0 || c.DefaultTTL < 0 || c.MaxRequestBytes < 0 || c.MaxDecodedBytes < 0 {
		return c, status.Error(codes.InvalidArgument, "negative client configuration")
	}
	if c.Timeout == 0 {
		c.Timeout = time.Second
	}
	if c.MaxRequestBytes == 0 {
		c.MaxRequestBytes = DefaultMaxRequestBytes
	}
	if c.MaxDecodedBytes == 0 {
		c.MaxDecodedBytes = DefaultMaxDecodedBytes
	}
	c.DialOptions = append([]grpc.DialOption(nil), c.DialOptions...)
	if c.TLS != nil {
		c.TLS = c.TLS.Clone()
	}
	return c, nil
}
func dial(target string, c Config) (Connection, error) {
	var cred credentials.TransportCredentials = insecure.NewCredentials()
	if c.TLS != nil {
		cred = credentials.NewTLS(c.TLS.Clone())
	}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(cred), grpc.WithDisableRetry(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(c.MaxDecodedBytes), grpc.MaxCallSendMsgSize(c.MaxRequestBytes))}
	opts = append(opts, c.DialOptions...)
	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return Connection{}, err
	}
	return Connection{Conn: conn, Close: conn.Close}, nil
}
func newClient(c Config, b backend) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{config: c, backend: b, life: ctx, cancel: cancel}
}

// NewClient creates an owned connection. Dialing is lazy; the first RPC connects.
func NewClient(target string, config Config) (*Client, error) {
	c, err := normalizeConfig(config)
	if err != nil {
		return nil, err
	}
	if target == "" {
		return nil, status.Error(codes.InvalidArgument, "empty target")
	}
	conn, err := dial(target, c)
	if err != nil {
		return nil, err
	}
	return newClient(c, &directBackend{pb.NewHurriCacheGrpcServiceClient(conn.Conn), conn, target}), nil
}

// NewClientWithConnection borrows conn. Close never closes the borrowed channel.
func NewClientWithConnection(conn grpc.ClientConnInterface, config Config) (*Client, error) {
	c, err := normalizeConfig(config)
	if err != nil {
		return nil, err
	}
	if conn == nil {
		return nil, status.Error(codes.InvalidArgument, "nil connection")
	}
	return newClient(c, &directBackend{pb.NewHurriCacheGrpcServiceClient(conn), Connection{Conn: conn}, "borrowed"}), nil
}

// Close is idempotent and cancels pending calls before releasing owned connections.
func (c *Client) Close() error {
	c.once.Do(func() { c.cancel(); c.closeErr = c.backend.close() })
	return c.closeErr
}

// Target returns the direct address or active coordinator address.
func (c *Client) Target() string { return c.backend.target() }

// DefaultClientID returns the configured client ID.
func (c *Client) DefaultClientID() uint32 { return c.config.ClientID }

// DefaultTimeout returns the configured operation timeout.
func (c *Client) DefaultTimeout() time.Duration { return c.config.Timeout }

// DefaultTTL returns the configured creation/update TTL.
func (c *Client) DefaultTTL() time.Duration { return c.config.DefaultTTL }

type session struct {
	ctx    context.Context
	stub   pb.HurriCacheGrpcServiceClient
	key    *pb.Key
	ttl    *uint64
	config Config
}

func perform[T any](c *Client, ctx context.Context, key []byte, o Options, write bool, f func(*session) (T, error)) (out T, err error) {
	timeout := c.config.Timeout
	if o.Timeout != nil {
		timeout = *o.Timeout
	}
	ttl := c.config.DefaultTTL
	if o.TTL != nil {
		ttl = *o.TTL
	}
	if timeout < 0 || ttl < 0 || (o.Mode != nil && *o.Mode > LBSmart) {
		return out, status.Error(codes.InvalidArgument, "invalid options")
	}
	if ctx.Err() != nil {
		return out, status.FromContextError(ctx.Err()).Err()
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(c.life, cancel)
	defer stop()
	if c.life.Err() != nil {
		return out, status.Error(codes.Canceled, "client closed")
	}
	if timeout > 0 {
		var end context.CancelFunc
		ctx, end = context.WithTimeout(ctx, timeout)
		defer end()
	}
	id := c.config.ClientID
	if o.ClientID != nil {
		id = *o.ClientID
	}
	k, err := encodeKey(key, o.Hint, Ptr(id), c.config)
	if err != nil {
		return out, err
	}
	var expiry *uint64
	if ttl != 0 {
		expiry = Ptr(uint64(time.Now().Add(ttl).UnixMilli()))
	}
	err = c.backend.run(ctx, o, write, func(stub pb.HurriCacheGrpcServiceClient) error {
		var e error
		out, e = f(&session{ctx, stub, k, expiry, c.config})
		return e
	})
	return
}

func unary[Q proto.Message, R proto.Message](s *session, req Q, f func(context.Context, Q, ...grpc.CallOption) (R, error)) (res R, err error) {
	if err = s.ctx.Err(); err != nil {
		return res, status.FromContextError(err).Err()
	}
	if proto.Size(req) > s.config.MaxRequestBytes {
		return res, status.Error(codes.ResourceExhausted, "request exceeds MaxRequestBytes")
	}
	var trailers metadata.MD
	res, err = f(s.ctx, req, grpc.Trailer(&trailers), grpc.MaxCallRecvMsgSize(s.config.MaxDecodedBytes), grpc.MaxCallSendMsgSize(s.config.MaxRequestBytes))
	if err != nil {
		err = &RPCError{err, trailers.Copy()}
	}
	return
}
func partial(chunks, items int, err error) error {
	if err == nil {
		return nil
	}
	if chunks == 0 {
		return err
	}
	return &PartialError{chunks, items, err}
}
func hasPartial(err error) bool { var p *PartialError; return errors.As(err, &p) }
