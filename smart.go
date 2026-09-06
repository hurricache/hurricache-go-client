package hurricache

import (
	"context"
	"errors"
	pb "github.com/hurricache/hurricache-go-client/proto/cachepb"
	cpb "github.com/hurricache/hurricache-go-client/proto/coordinatorpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// SmartConfig configures coordinator discovery and routing.
// ConnectionFactory applies to both coordinators and data nodes.
type SmartConfig struct {
	Client            Config
	Mode              RoutingMode
	RefreshInterval   time.Duration
	DiscoveryTimeout  time.Duration
	ReadyTimeout      time.Duration
	ConnectionFactory ConnectionFactory
}

// SmartClient shares all Client operations and routes them through immutable topology snapshots.
type SmartClient struct {
	*Client
	routing *smartBackend
}
type routeKey struct {
	shard uint32
	role  cpb.NodeRole
}
type endpoint struct {
	connection     Connection
	stub           pb.HurriCacheGrpcServiceClient
	refs           int
	active, closed bool
}
type topology struct {
	shards  uint32
	routes  map[routeKey]*endpoint
	targets map[string]*endpoint
}
type smartBackend struct {
	mu                     sync.Mutex
	coordinators           []string
	coordinatorConnections map[string]Connection
	activeCoordinator      int
	config                 SmartConfig
	factory                ConnectionFactory
	snapshot               *topology
	nodes                  map[string]*endpoint
	closed                 bool
	ready                  chan struct{}
	readyOnce              sync.Once
	gate                   chan struct{}
	wake                   chan struct{}
	done                   chan struct{}
	work                   sync.WaitGroup
	life                   context.Context
	cancel                 context.CancelFunc
	round                  atomic.Uint32
}

// NewSmartClient starts background topology discovery. Ready waits for a valid snapshot.
func NewSmartClient(coordinators []string, config SmartConfig) (*SmartClient, error) {
	if len(coordinators) == 0 {
		return nil, status.Error(codes.InvalidArgument, "no coordinators")
	}
	for _, v := range coordinators {
		if v == "" {
			return nil, status.Error(codes.InvalidArgument, "empty coordinator")
		}
	}
	c, e := normalizeConfig(config.Client)
	if e != nil {
		return nil, e
	}
	config.Client = c
	if config.Mode > LBSmart || config.RefreshInterval < 0 || config.DiscoveryTimeout < 0 || config.ReadyTimeout < 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid smart configuration")
	}
	if config.RefreshInterval == 0 {
		config.RefreshInterval = 30 * time.Second
	}
	if config.DiscoveryTimeout == 0 {
		config.DiscoveryTimeout = c.Timeout
	}
	if config.ReadyTimeout == 0 {
		config.ReadyTimeout = 60 * time.Second
	}
	b := &smartBackend{coordinators: append([]string(nil), coordinators...), config: config, nodes: map[string]*endpoint{}, coordinatorConnections: map[string]Connection{}, ready: make(chan struct{}), gate: make(chan struct{}, 1), wake: make(chan struct{}, 1), done: make(chan struct{})}
	b.life, b.cancel = context.WithCancel(context.Background())
	b.factory = config.ConnectionFactory
	if b.factory == nil {
		b.factory = func(ctx context.Context, target string) (Connection, error) {
			if ctx.Err() != nil {
				return Connection{}, status.FromContextError(ctx.Err()).Err()
			}
			return dial(target, c)
		}
	}
	client := &SmartClient{newClient(c, b), b}
	go b.loop()
	return client, nil
}

// Ready waits for the first nonempty, valid topology, bounded by ctx and ReadyTimeout.
func (c *SmartClient) Ready(ctx context.Context) error { return c.routing.waitReady(ctx) }

// IsReady reports current discovery readiness; it becomes false on Close.
func (c *SmartClient) IsReady() bool {
	b := c.routing
	b.mu.Lock()
	defer b.mu.Unlock()
	return !b.closed && b.snapshot != nil
}

// Refresh discovers and atomically publishes topology. Failure retains the last good snapshot.
func (c *SmartClient) Refresh(ctx context.Context) error { return c.routing.refresh(ctx) }
func (b *smartBackend) target() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.coordinators[b.activeCoordinator]
}
func (b *smartBackend) waitReady(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, b.config.ReadyTimeout)
	defer cancel()
	select {
	case <-b.life.Done():
		return status.Error(codes.Canceled, "client closed")
	default:
	}
	select {
	case <-b.ready:
		return nil
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	case <-b.life.Done():
		return status.Error(codes.Canceled, "client closed")
	}
}
func (b *smartBackend) loop() {
	defer close(b.done)
	ticker := time.NewTicker(b.config.RefreshInterval)
	defer ticker.Stop()
	for {
		_ = b.refresh(b.life)
		select {
		case <-b.life.Done():
			return
		case <-ticker.C:
		case <-b.wake:
		}
	}
}
func (b *smartBackend) requestRefresh() {
	select {
	case b.wake <- struct{}{}:
	default:
	}
}

func (b *smartBackend) refresh(ctx context.Context) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return status.Error(codes.Canceled, "client closed")
	}
	b.work.Add(1)
	b.mu.Unlock()
	defer b.work.Done()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(b.life, cancel)
	defer stop()
	select {
	case b.gate <- struct{}{}:
		defer func() { <-b.gate }()
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}
	b.mu.Lock()
	start := b.activeCoordinator
	b.mu.Unlock()
	var last error
	for n := 0; n < len(b.coordinators); n++ {
		if ctx.Err() != nil {
			return status.FromContextError(ctx.Err()).Err()
		}
		index := (start + n) % len(b.coordinators)
		target := b.coordinators[index]
		attempt, end := context.WithTimeout(ctx, b.config.DiscoveryTimeout)
		snap, e := b.discover(attempt, target)
		if e == nil {
			e = b.publish(attempt, snap)
		}
		end()
		if e == nil {
			b.mu.Lock()
			b.activeCoordinator = index
			b.mu.Unlock()
			return nil
		}
		last = e
	}
	return last
}

type discovery struct {
	shards uint32
	peers  []*cpb.PeerRouting
}

func (b *smartBackend) discover(ctx context.Context, target string) (discovery, error) {
	var d discovery
	b.mu.Lock()
	conn, ok := b.coordinatorConnections[target]
	b.mu.Unlock()
	if !ok {
		var e error
		conn, e = b.factory(ctx, target)
		if e != nil {
			return d, e
		}
		if conn.Conn == nil {
			if conn.Close != nil {
				_ = conn.Close()
			}
			return d, status.Error(codes.InvalidArgument, "factory returned nil connection")
		}
		b.mu.Lock()
		if b.closed {
			b.mu.Unlock()
			if conn.Close != nil {
				_ = conn.Close()
			}
			return d, status.Error(codes.Canceled, "client closed")
		}
		b.coordinatorConnections[target] = conn
		b.mu.Unlock()
	}
	stream, e := cpb.NewCoordinatorServiceClient(conn.Conn).ProvideGlobalRoutingInfo(ctx, &cpb.Void{})
	if e != nil {
		return d, e
	}
	for {
		r, e := stream.Recv()
		if e == io.EOF {
			break
		}
		if e != nil {
			return d, &RPCError{e, stream.Trailer().Copy()}
		}
		if r.MaxShards == 0 || (d.shards != 0 && d.shards != r.MaxShards) {
			return d, status.Error(codes.DataLoss, "invalid or inconsistent shard count")
		}
		d.shards = r.MaxShards
		d.peers = append(d.peers, r.PeerRouting...)
	}
	if d.shards == 0 || len(d.peers) == 0 {
		return d, status.Error(codes.Unavailable, "empty topology")
	}
	for _, p := range d.peers {
		if p.Target == "" || (p.Role != cpb.NodeRole_MASTER && p.Role != cpb.NodeRole_BACKUP) {
			return d, status.Error(codes.DataLoss, "invalid route")
		}
		for _, shard := range p.PartitionIds {
			if shard >= d.shards {
				return d, status.Error(codes.DataLoss, "partition exceeds shard count")
			}
		}
	}
	return d, nil
}
func (b *smartBackend) publish(ctx context.Context, d discovery) error {
	var pinned []*endpoint
	defer func() {
		b.mu.Lock()
		for _, ep := range pinned {
			ep.refs--
		}
		b.retireLocked()
		b.mu.Unlock()
	}()
	snap := &topology{d.shards, map[routeKey]*endpoint{}, map[string]*endpoint{}}
	created := map[string]*endpoint{}
	cleanup := func() {
		for _, e := range created {
			if e.connection.Close != nil {
				_ = e.connection.Close()
			}
		}
	}
	for _, p := range d.peers {
		if ctx.Err() != nil {
			cleanup()
			return status.FromContextError(ctx.Err()).Err()
		}
		ep := snap.targets[p.Target]
		if ep == nil {
			b.mu.Lock()
			ep = b.nodes[p.Target]
			if ep != nil {
				ep.refs++
				pinned = append(pinned, ep)
			}
			b.mu.Unlock()
			if ep == nil {
				conn, e := b.factory(ctx, p.Target)
				if e != nil {
					cleanup()
					return e
				}
				if conn.Conn == nil {
					if conn.Close != nil {
						_ = conn.Close()
					}
					cleanup()
					return status.Error(codes.InvalidArgument, "factory returned nil connection")
				}
				ep = &endpoint{connection: conn, stub: pb.NewHurriCacheGrpcServiceClient(conn.Conn)}
				created[p.Target] = ep
			}
			snap.targets[p.Target] = ep
		}
		for _, shard := range p.PartitionIds {
			k := routeKey{shard, p.Role}
			if old := snap.routes[k]; old != nil && old != ep {
				cleanup()
				return status.Error(codes.DataLoss, "conflicting routes for shard and role")
			}
			snap.routes[k] = ep
		}
	}
	if len(snap.routes) == 0 {
		cleanup()
		return status.Error(codes.Unavailable, "topology contains no routes")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || ctx.Err() != nil {
		cleanup()
		return status.Error(codes.Canceled, "discovery canceled")
	}
	for _, e := range b.nodes {
		e.active = false
	}
	for target, ep := range snap.targets {
		ep.active = true
		b.nodes[target] = ep
	}
	b.snapshot = snap
	b.retireLocked()
	b.readyOnce.Do(func() { close(b.ready) })
	return nil
}
func (b *smartBackend) retireLocked() {
	for target, ep := range b.nodes {
		if !ep.active && ep.refs == 0 {
			delete(b.nodes, target)
			if !ep.closed && ep.connection.Close != nil {
				_ = ep.connection.Close()
			}
			ep.closed = true
		}
	}
}
func (b *smartBackend) run(ctx context.Context, o Options, write bool, action func(pb.HurriCacheGrpcServiceClient) error) error {
	if e := b.waitReady(ctx); e != nil {
		return e
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return status.Error(codes.Canceled, "client closed")
	}
	snap := b.snapshot
	// Pin every endpoint in this snapshot, including a possible trailer reroute.
	for _, ep := range snap.targets {
		ep.refs++
	}
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		for _, ep := range snap.targets {
			ep.refs--
		}
		b.retireLocked()
		b.mu.Unlock()
	}()
	var shard uint32
	if o.Hint != nil && o.Hint.WeakHash != nil {
		shard = *o.Hint.WeakHash % snap.shards
	} else {
		shard = (b.round.Add(1) & mathMaxInt32) % snap.shards
	}
	master, backup := snap.routes[routeKey{shard, cpb.NodeRole_MASTER}], snap.routes[routeKey{shard, cpb.NodeRole_BACKUP}]
	if master == nil && backup == nil {
		return status.Error(codes.Unavailable, "no route for selected shard")
	}
	mode := b.config.Mode
	if write {
		mode = MasterThenBackup
	}
	if o.Mode != nil {
		mode = *o.Mode
	}
	if master == nil {
		mode = Backup
	}
	if backup == nil {
		mode = Master
	}
	var first, second *endpoint
	switch mode {
	case Master:
		first = master
	case Backup:
		first = backup
	case MasterThenBackup:
		first, second = master, backup
	case LBSmart:
		first, second = master, backup
		if rand.IntN(2) == 1 {
			first, second = second, first
		}
	}
	if ctx.Err() != nil {
		return status.FromContextError(ctx.Err()).Err()
	}
	e := action(first.stub)
	if e == nil || hasPartial(e) {
		return e
	}
	if ctx.Err() != nil {
		return e
	}
	if status.Code(e) == codes.FailedPrecondition {
		var re *RPCError
		if errors.As(e, &re) {
			routes := re.Trailers.Get("x-fastcache-route")
			if len(routes) > 0 {
				if ep := snap.targets[routes[0]]; ep != nil && ep != first {
					return action(ep.stub)
				}
			}
		}
	} else if status.Code(e) == codes.Unavailable {
		b.requestRefresh()
		if second != nil && second != first {
			return action(second.stub)
		}
	}
	return e
}

const mathMaxInt32 uint32 = 1<<31 - 1

func (b *smartBackend) close() error {
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
	b.cancel()
	<-b.done
	b.work.Wait()
	b.mu.Lock()
	defer b.mu.Unlock()
	var result error
	for _, ep := range b.nodes {
		if !ep.closed && ep.connection.Close != nil {
			if e := ep.connection.Close(); e != nil {
				result = e
			}
		}
		ep.closed = true
	}
	for _, conn := range b.coordinatorConnections {
		if conn.Close != nil {
			if e := conn.Close(); e != nil {
				result = e
			}
		}
	}
	return result
}
