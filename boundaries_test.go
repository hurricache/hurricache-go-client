package hurricache

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	pb "github.com/hurricache/hurricache-go-client/proto/cachepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"math"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"
)

func TestOptionPresenceDefaultsAndIntegerValidation(t *testing.T) {
	var request proto.Message
	conn := transport(t, func(ctx context.Context, m string, r proto.Message) (any, error) {
		request = r
		if _, ok := r.(*pb.LockRequest); ok {
			return &pb.LockResponse{Result: LockOK}, nil
		}
		return response(m), nil
	}, nil)
	c, _ := NewClientWithConnection(conn, Config{ClientID: 99, DefaultTTL: time.Minute})
	defer c.Close()
	if _, e := c.CreateKeyValue(context.Background(), []byte("k"), nil, Options{ClientID: Ptr(uint32(0)), TTL: Ptr(time.Duration(0))}); e != nil {
		t.Fatal(e)
	}
	req := request.(*pb.CreateRequest)
	if req.Key.ClientId == nil || *req.Key.ClientId != 0 || req.Key.KeyHint != nil || req.Value.Ttl != nil || req.Value.Value == nil {
		t.Fatalf("zero/absence semantics %v", req)
	}
	for _, kind := range []LockType{NoLock, WriteLock, ReadLock, GlobalLock} {
		if _, e := c.LockObject(context.Background(), []byte("k"), kind, time.Duration(math.MaxUint32)*time.Second, Options{}); e != nil {
			t.Fatal(e)
		}
		req := request.(*pb.LockRequest)
		if req.LockType != kind || req.GetLockDuration() != math.MaxUint32 || req.GetClientId() != 99 {
			t.Fatal(req)
		}
	}
	for _, duration := range []time.Duration{-time.Second, (time.Duration(math.MaxUint32) + 1) * time.Second} {
		if _, e := c.LockObject(context.Background(), []byte("k"), WriteLock, duration, Options{}); status.Code(e) != codes.InvalidArgument {
			t.Fatal(e)
		}
	}
	if _, e := c.AddElementToPosition(context.Background(), []byte("k"), []Payload{{Data: []byte("x")}}, math.MaxUint32, Options{}); status.Code(e) != codes.OutOfRange {
		t.Fatal(e)
	}
	if _, e := c.CreateMap(context.Background(), []byte("k"), nil, Options{Mode: Ptr(RoutingMode(255))}); status.Code(e) != codes.InvalidArgument {
		t.Fatal(e)
	}
}

func TestStreamInitialFailureAndCancellation(t *testing.T) {
	ready := make(chan struct{}, 1)
	stopped := make(chan struct{}, 1)
	conn := transport(t, nil, func(r proto.Message, g grpc.ServerStreamingServer[pb.BatchValueResponse]) error {
		if string(r.(*pb.GetRequest).Key.Payload.Payload) == "fail" {
			g.SetTrailer(metadata.Pairs("why", "initial"))
			return status.Error(codes.Unavailable, "initial failure")
		}
		ready <- struct{}{}
		<-g.Context().Done()
		stopped <- struct{}{}
		return status.FromContextError(g.Context().Err()).Err()
	})
	c, _ := NewClientWithConnection(conn, Config{})
	defer c.Close()
	r, e := c.StreamList(context.Background(), []byte("fail"), Options{})
	if status.Code(e) != codes.Unavailable || len(r) != 0 || hasPartial(e) {
		t.Fatalf("initial stream failure %v", e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, e := c.StreamList(ctx, []byte("wait"), Options{}); done <- e }()
	<-ready
	cancel()
	if e := <-done; status.Code(e) != codes.Canceled {
		t.Fatal(e)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("stream cancellation not delivered")
	}
}

func TestTLSOwnedConnectionAndConcurrentClose(t *testing.T) {
	key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, e := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if e != nil {
		t.Fatal(e)
	}
	certificate, e := x509.ParseCertificate(der)
	if e != nil {
		t.Fatal(e)
	}
	roots := x509.NewCertPool()
	roots.AddCert(certificate)
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer listener.Close()
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}, MinVersion: tls.VersionTLS12})), grpc.UnaryInterceptor(func(ctx context.Context, r any, info *grpc.UnaryServerInfo, _ grpc.UnaryHandler) (any, error) {
		return response(info.FullMethod), nil
	}))
	pb.RegisterHurriCacheGrpcServiceServer(server, &testService{})
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()
	c, e := NewClient(listener.Addr().String(), Config{TLS: &tls.Config{RootCAs: roots, ServerName: "localhost", MinVersion: tls.VersionTLS12}})
	if e != nil {
		t.Fatal(e)
	}
	if _, e := c.GetValue(context.Background(), []byte("key"), Options{}); e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Go(func() {
			if e := c.Close(); e != nil {
				t.Error(e)
			}
		})
	}
	wg.Wait()
	if _, e := c.GetValue(context.Background(), []byte("key"), Options{}); status.Code(e) != codes.Canceled {
		t.Fatal(e)
	}
	conn := c.backend.(*directBackend).connection.Conn
	if _, e := pb.NewHurriCacheGrpcServiceClient(conn).GetValue(context.Background(), &pb.GetRequest{}); status.Code(e) != codes.Canceled {
		t.Fatalf("owned connection not closed: %v", e)
	}
}
