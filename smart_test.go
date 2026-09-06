package hurricache

import (
	"context"
	"errors"
	pb "github.com/hurricache/hurricache-go-client/proto/cachepb"
	cpb "github.com/hurricache/hurricache-go-client/proto/coordinatorpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type coordinatorService struct {
	cpb.UnimplementedCoordinatorServiceServer
	provide func(grpc.ServerStreamingServer[cpb.RoutingInfoData]) error
}

func (c *coordinatorService) ProvideGlobalRoutingInfo(_ *cpb.Void, g grpc.ServerStreamingServer[cpb.RoutingInfoData]) error {
	return c.provide(g)
}
func coordinatorTransport(t *testing.T, f func(grpc.ServerStreamingServer[cpb.RoutingInfoData]) error) *grpc.ClientConn {
	t.Helper()
	l := bufconn.Listen(1024 * 1024)
	s := grpc.NewServer()
	cpb.RegisterCoordinatorServiceServer(s, &coordinatorService{provide: f})
	go func() { _ = s.Serve(l) }()
	conn, e := grpc.NewClient("passthrough:///coordinator", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return l.DialContext(ctx) }), grpc.WithDisableRetry())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = conn.Close(); s.Stop(); _ = l.Close() })
	return conn
}
func topologyData(master, backup string) *cpb.RoutingInfoData {
	d := &cpb.RoutingInfoData{MaxShards: 2}
	for i, target := range []string{master, backup} {
		if target != "" {
			d.PeerRouting = append(d.PeerRouting, &cpb.PeerRouting{Target: target, Role: []cpb.NodeRole{cpb.NodeRole_MASTER, cpb.NodeRole_BACKUP}[i], PartitionIds: []uint32{0, 1}})
		}
	}
	return d
}
func waitReady(t *testing.T, c *SmartClient) {
	t.Helper()
	ctx, end := context.WithTimeout(context.Background(), time.Second)
	defer end()
	if e := c.Ready(ctx); e != nil {
		t.Fatal(e)
	}
}

func TestSmartModesAndBoundedFallback(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mode      RoutingMode
		fail      codes.Code
		route     string
		want      string
		wantCalls int
	}{
		{"master", Master, codes.OK, "", "m", 1}, {"backup", Backup, codes.OK, "", "b", 1},
		{"master_then_backup", MasterThenBackup, codes.OK, "", "m", 1},
		{"unavailable_fallback", MasterThenBackup, codes.Unavailable, "", "b", 2},
		{"master_no_fallback", Master, codes.Unavailable, "", "", 1},
		{"deadline_no_fallback", MasterThenBackup, codes.DeadlineExceeded, "", "", 1},
		{"reroute", Master, codes.FailedPrecondition, "b", "b", 2},
		{"missing_reroute", MasterThenBackup, codes.FailedPrecondition, "unknown", "", 1},
		{"self_reroute", MasterThenBackup, codes.FailedPrecondition, "m", "", 1},
		{"no_route_trailer", MasterThenBackup, codes.FailedPrecondition, "", "", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			nodes := map[string]*grpc.ClientConn{}
			for _, target := range []string{"m", "b"} {
				nodes[target] = transport(t, func(ctx context.Context, method string, r proto.Message) (any, error) {
					calls.Add(1)
					if target == "m" && tc.fail != codes.OK {
						if tc.route != "" {
							grpc.SetTrailer(ctx, metadata.Pairs("x-fastcache-route", tc.route))
						}
						return nil, status.Error(tc.fail, "first node")
					}
					return &pb.ValueResponse{ValueUnordered: plain(target)}, nil
				}, nil)
			}
			coord := coordinatorTransport(t, func(g grpc.ServerStreamingServer[cpb.RoutingInfoData]) error { return g.Send(topologyData("m", "b")) })
			factory := func(ctx context.Context, target string) (Connection, error) {
				if target == "c" {
					return Connection{Conn: coord}, nil
				}
				return Connection{Conn: nodes[target]}, nil
			}
			c, e := NewSmartClient([]string{"c"}, SmartConfig{Mode: tc.mode, ConnectionFactory: factory})
			if e != nil {
				t.Fatal(e)
			}
			defer c.Close()
			waitReady(t, c)
			r, e := c.GetValue(context.Background(), []byte("key"), Options{Hint: &KeyHint{WeakHash: Ptr(uint32(0xffffffff))}})
			if tc.want != "" {
				if e != nil || r.Value == nil || string(r.Value.Data) != tc.want {
					t.Fatalf("result %v err %v", r, e)
				}
			} else if e == nil {
				t.Fatal("failure not propagated")
			}
			if calls.Load() != int32(tc.wantCalls) {
				t.Errorf("calls %d want %d", calls.Load(), tc.wantCalls)
			}
		})
	}
}

func TestSmartPeriodicRefreshAndFallbackStops(t *testing.T) {
	var discoveries, masterCalls, backupCalls atomic.Int32
	m := transport(t, func(ctx context.Context, method string, r proto.Message) (any, error) {
		masterCalls.Add(1)
		return nil, status.Error(codes.Unavailable, "master")
	}, nil)
	b := transport(t, func(ctx context.Context, method string, r proto.Message) (any, error) {
		backupCalls.Add(1)
		grpc.SetTrailer(ctx, metadata.Pairs("x-fastcache-route", "m"))
		return nil, status.Error(codes.FailedPrecondition, "backup redirect")
	}, nil)
	refreshed := make(chan struct{}, 1)
	coord := coordinatorTransport(t, func(g grpc.ServerStreamingServer[cpb.RoutingInfoData]) error {
		if discoveries.Add(1) > 1 {
			select {
			case refreshed <- struct{}{}:
			default:
			}
		}
		return g.Send(topologyData("m", "b"))
	})
	conns := map[string]*grpc.ClientConn{"c": coord, "m": m, "b": b}
	c, e := NewSmartClient([]string{"c"}, SmartConfig{RefreshInterval: 20 * time.Millisecond, ConnectionFactory: func(ctx context.Context, target string) (Connection, error) {
		return Connection{Conn: conns[target]}, nil
	}})
	if e != nil {
		t.Fatal(e)
	}
	waitReady(t, c)
	select {
	case <-refreshed:
	case <-time.After(time.Second):
		t.Fatal("periodic refresh did not run")
	}
	_, e = c.GetValue(context.Background(), []byte("key"), Options{})
	if status.Code(e) != codes.FailedPrecondition || masterCalls.Load() != 1 || backupCalls.Load() != 1 {
		t.Fatalf("fallback recursed: %v master %d backup %d", e, masterCalls.Load(), backupCalls.Load())
	}
	_ = c.Close()
	count := discoveries.Load()
	time.Sleep(50 * time.Millisecond)
	if discoveries.Load() != count {
		t.Fatal("refresh continued after Close")
	}
}
func TestSmartEveryOperationDispatch(t *testing.T) {
	var which atomic.Value
	which.Store("")
	var shape atomic.Value
	shape.Store("values")
	nodes := map[string]*grpc.ClientConn{}
	for _, target := range []string{"m", "b"} {
		nodes[target] = transport(t, func(ctx context.Context, method string, r proto.Message) (any, error) {
			which.Store(target)
			return response(method), nil
		},
			func(r proto.Message, g grpc.ServerStreamingServer[pb.BatchValueResponse]) error {
				which.Store(target)
				return g.Send(testBatch(shape.Load().(string)))
			})
	}
	coord := coordinatorTransport(t, func(g grpc.ServerStreamingServer[cpb.RoutingInfoData]) error { return g.Send(topologyData("m", "b")) })
	c, e := NewSmartClient([]string{"c"}, SmartConfig{Mode: Backup, ConnectionFactory: func(ctx context.Context, target string) (Connection, error) {
		if target == "c" {
			return Connection{Conn: coord}, nil
		}
		return Connection{Conn: nodes[target]}, nil
	}})
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	waitReady(t, c)
	// This independent policy list is transcribed from execute(...) in Java.
	configured := strings.Fields("GetValue ExistKey Remove GetSize GetHead GetTail GetFront GetTTL CreateKeyValue UpdateKeyValue CreateQueue CreateList CreateVector CreateSet CreateOrderedSet CreateMap CreateOrderedMap CreateContainer GetElementAtPosition AtomicLoad GetContainerValue ContainsContainerKey StreamQueue StreamList StreamVector StreamSet StreamMap StreamOrderedMap StreamOrderedSet GetContainer GetElementInRange StreamElementInRangeUnordered StreamElementInRangeOrderedSet StreamElementInRangeOrdered")
	reads := map[string]bool{}
	for _, v := range configured {
		reads[v] = true
	}
	for _, tc := range operationCases() {
		t.Run(tc.name, func(t *testing.T) {
			shape.Store(tc.shape)
			which.Store("")
			args := []reflect.Value{reflect.ValueOf(context.Background()), reflect.ValueOf([]byte("key"))}
			for _, v := range tc.args {
				args = append(args, reflect.ValueOf(v))
			}
			args = append(args, reflect.ValueOf(Options{}))
			result := reflect.ValueOf(c).MethodByName(tc.name).Call(args)
			if !result[1].IsNil() {
				t.Fatal(result[1].Interface())
			}
			want := "m"
			if reads[tc.name] {
				want = "b"
			}
			if which.Load().(string) != want {
				t.Fatalf("dispatch %s want %s", which.Load(), want)
			}
		})
	}
}
func TestSmartUnsignedNoHintLBAndOverrides(t *testing.T) {
	var mu sync.Mutex
	var visited []string
	nodes := map[string]*grpc.ClientConn{}
	for _, target := range []string{"m0", "m1", "b0", "b1"} {
		nodes[target] = transport(t, func(ctx context.Context, m string, r proto.Message) (any, error) {
			mu.Lock()
			visited = append(visited, target)
			mu.Unlock()
			return response(m), nil
		}, nil)
	}
	d := &cpb.RoutingInfoData{MaxShards: 2}
	for i, target := range []string{"m0", "m1", "b0", "b1"} {
		role := cpb.NodeRole_MASTER
		if i > 1 {
			role = cpb.NodeRole_BACKUP
		}
		d.PeerRouting = append(d.PeerRouting, &cpb.PeerRouting{Target: target, Role: role, PartitionIds: []uint32{uint32(i % 2)}})
	}
	coord := coordinatorTransport(t, func(g grpc.ServerStreamingServer[cpb.RoutingInfoData]) error { return g.Send(d) })
	c, _ := NewSmartClient([]string{"c"}, SmartConfig{Mode: Master, ConnectionFactory: func(ctx context.Context, target string) (Connection, error) {
		if target == "c" {
			return Connection{Conn: coord}, nil
		}
		return Connection{Conn: nodes[target]}, nil
	}})
	defer c.Close()
	waitReady(t, c)
	for _, o := range []Options{{}, {Hint: &KeyHint{WeakHash: Ptr(uint32(0xffffffff))}}, {}, {Hint: &KeyHint{StrongHash: Ptr(uint32(0))}}} {
		if _, e := c.GetValue(context.Background(), []byte("key"), o); e != nil {
			t.Fatal(e)
		}
	}
	mu.Lock()
	got := append([]string(nil), visited...)
	mu.Unlock()
	if !reflect.DeepEqual(got, []string{"m1", "m1", "m0", "m1"}) {
		t.Fatalf("Java shard selection %v", got)
	}
	var wg sync.WaitGroup
	for i := 0; i < 80; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			mode := Master
			if i%2 != 0 {
				mode = Backup
			}
			_, e := c.AtomicAdd(context.Background(), []byte("key"), 1, Options{Mode: &mode, Hint: &KeyHint{WeakHash: Ptr(uint32(0))}})
			if e != nil {
				t.Error(e)
			}
		}(i)
	}
	wg.Wait()
	for i := 0; i < 80; i++ {
		if _, e := c.GetValue(context.Background(), []byte("key"), Options{Mode: Ptr(LBSmart), Hint: &KeyHint{WeakHash: Ptr(uint32(0))}}); e != nil {
			t.Fatal(e)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	count := map[string]int{}
	for _, v := range visited[4:84] {
		count[v]++
	}
	if count["m0"] != 40 || count["b0"] != 40 {
		t.Errorf("concurrent overrides leaked: %v", count)
	}
	count = map[string]int{}
	for _, v := range visited[84:] {
		count[v]++
	}
	if count["m0"] == 0 || count["b0"] == 0 {
		t.Errorf("LB did not select both roles: %v", count)
	}
}
func TestSmartDiscoveryRotationTopologyAndOwnership(t *testing.T) {
	var topologyState atomic.Pointer[cpb.RoutingInfoData]
	topologyState.Store(topologyData("m", "b"))
	var nodeStarted = make(chan struct{}, 1)
	var nodeRelease = make(chan struct{})
	master := transport(t, func(ctx context.Context, m string, r proto.Message) (any, error) {
		nodeStarted <- struct{}{}
		select {
		case <-nodeRelease:
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		return response(m), nil
	}, nil)
	backup := transport(t, func(ctx context.Context, m string, r proto.Message) (any, error) { return response(m), nil }, nil)
	bad := coordinatorTransport(t, func(g grpc.ServerStreamingServer[cpb.RoutingInfoData]) error {
		return status.Error(codes.Unavailable, "bad coordinator")
	})
	good := coordinatorTransport(t, func(g grpc.ServerStreamingServer[cpb.RoutingInfoData]) error { return g.Send(topologyState.Load()) })
	var mu sync.Mutex
	closed := map[string]int{}
	conns := map[string]*grpc.ClientConn{"bad": bad, "good": good, "m": master, "b": backup}
	c, _ := NewSmartClient([]string{"bad", "good"}, SmartConfig{Mode: Master, ConnectionFactory: func(ctx context.Context, target string) (Connection, error) {
		return Connection{Conn: conns[target], Close: func() error { mu.Lock(); closed[target]++; mu.Unlock(); return nil }}, nil
	}})
	waitReady(t, c)
	if c.Target() != "good" {
		t.Fatal("coordinator did not rotate")
	}
	done := make(chan error, 1)
	go func() { _, e := c.GetValue(context.Background(), []byte("key"), Options{}); done <- e }()
	<-nodeStarted
	topologyState.Store(topologyData("", "b"))
	if e := c.Refresh(context.Background()); e != nil {
		t.Fatal(e)
	}
	mu.Lock()
	closedEarly := closed["m"]
	mu.Unlock()
	if closedEarly != 0 {
		t.Fatal("removed endpoint closed during in-flight operation")
	}
	close(nodeRelease)
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	mu.Lock()
	closedAfter := closed["m"]
	mu.Unlock()
	if closedAfter != 1 {
		t.Fatal("retired endpoint not released")
	}
	// Role absence selects the remaining node even with Master configured.
	if _, e := c.GetValue(context.Background(), []byte("key"), Options{}); e != nil {
		t.Fatal(e)
	}
	topologyState.Store(&cpb.RoutingInfoData{MaxShards: 0})
	if e := c.Refresh(context.Background()); e == nil {
		t.Fatal("invalid topology accepted")
	}
	if _, e := c.GetValue(context.Background(), []byte("key"), Options{}); e != nil {
		t.Fatal("lost last good topology")
	}
	_ = c.Close()
	_ = c.Close()
	if c.IsReady() {
		t.Fatal("closed client ready")
	}
	mu.Lock()
	defer mu.Unlock()
	for _, target := range []string{"bad", "good", "m", "b"} {
		if closed[target] != 1 {
			t.Errorf("%s closed %d times", target, closed[target])
		}
	}
}
func TestSmartReadinessCancellationMissingRoutesAndPartialNoRetry(t *testing.T) {
	canceled := make(chan struct{}, 1)
	coord := coordinatorTransport(t, func(g grpc.ServerStreamingServer[cpb.RoutingInfoData]) error {
		<-g.Context().Done()
		canceled <- struct{}{}
		return status.FromContextError(g.Context().Err()).Err()
	})
	c, _ := NewSmartClient([]string{"c"}, SmartConfig{DiscoveryTimeout: time.Hour, ReadyTimeout: 20 * time.Millisecond, ConnectionFactory: func(ctx context.Context, _ string) (Connection, error) { return Connection{Conn: coord}, nil }})
	if e := c.Ready(context.Background()); status.Code(e) != codes.DeadlineExceeded {
		t.Fatal(e)
	}
	begin := time.Now()
	_ = c.Close()
	if time.Since(begin) > time.Second {
		t.Fatal("close did not cancel discovery")
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("discovery RPC not canceled")
	}
	var masterCalls, backupCalls atomic.Int32
	m := transport(t, func(ctx context.Context, method string, r proto.Message) (any, error) {
		if masterCalls.Add(1) == 2 {
			return nil, status.Error(codes.Unavailable, "second chunk")
		}
		return response(method), nil
	}, nil)
	b := transport(t, func(ctx context.Context, method string, r proto.Message) (any, error) {
		backupCalls.Add(1)
		return response(method), nil
	}, nil)
	d := topologyData("m", "b")
	for _, p := range d.PeerRouting {
		p.PartitionIds = []uint32{0}
	}
	available := coordinatorTransport(t, func(g grpc.ServerStreamingServer[cpb.RoutingInfoData]) error { return g.Send(d) })
	conns := map[string]*grpc.ClientConn{"c": available, "m": m, "b": b}
	c, _ = NewSmartClient([]string{"c"}, SmartConfig{Client: Config{MaxRequestBytes: 350}, ConnectionFactory: func(ctx context.Context, target string) (Connection, error) {
		return Connection{Conn: conns[target]}, nil
	}})
	defer c.Close()
	waitReady(t, c)
	if _, e := c.GetValue(context.Background(), []byte("key"), Options{Hint: &KeyHint{WeakHash: Ptr(uint32(1))}}); status.Code(e) != codes.Unavailable {
		t.Fatal("missing route accepted")
	}
	_, e := c.AddElementToTail(context.Background(), []byte("key"), batchValues(9), Options{Hint: &KeyHint{WeakHash: Ptr(uint32(0))}})
	var p *PartialError
	if !errors.As(e, &p) || masterCalls.Load() != 2 || backupCalls.Load() != 0 {
		t.Fatalf("partial mutation replayed: %v, master %d backup %d", e, masterCalls.Load(), backupCalls.Load())
	}
}
