package hurricache_test

import (
	"context"
	"fmt"
	hc "github.com/hurricache/hurricache-go-client"
	pb "github.com/hurricache/hurricache-go-client/proto/cachepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"net"
	"time"
)

type exampleServer struct {
	pb.UnimplementedHurriCacheGrpcServiceServer
}

func (exampleServer) GetValue(context.Context, *pb.GetRequest) (*pb.ValueResponse, error) {
	return &pb.ValueResponse{ValueUnordered: &pb.Value{Value: &pb.BinaryPayload{Payload: []byte("hello"), Size: 5}}}, nil
}
func ExampleClient_GetValue() {
	// A real in-process gRPC service makes this example executable without Docker.
	listener := bufconn.Listen(1024 * 1024)
	defer listener.Close()
	server := grpc.NewServer()
	pb.RegisterHurriCacheGrpcServiceServer(server, exampleServer{})
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()
	conn, err := grpc.NewClient("passthrough:///example", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	client, err := hc.NewClientWithConnection(conn, hc.Config{ClientID: 7})
	if err != nil {
		panic(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := client.GetValue(ctx, []byte("greeting"), hc.Options{})
	if err != nil {
		panic(err)
	}
	if result.Value != nil {
		fmt.Println(string(result.Value.Data))
	}
	// Output: hello
}
func ExampleOptions() {
	options := hc.Options{ClientID: hc.Ptr(uint32(0)), Hint: &hc.KeyHint{WeakHash: hc.Ptr(uint32(0))}, Mode: hc.Ptr(hc.Backup)}
	fmt.Println(options.ClientID != nil, *options.ClientID, options.Hint.StrongHash == nil)
	// Output: true 0 true
}
func ExampleNewClient() {
	c, err := hc.NewClient("127.0.0.1:50000", hc.Config{ClientID: 7, Timeout: 2 * time.Second})
	if err != nil {
		panic(err)
	}
	defer c.Close()
	fmt.Println(c.Target(), c.DefaultClientID())
	// Output: 127.0.0.1:50000 7
}
func ExampleNewSmartClient() {
	// Construction starts asynchronous discovery; Ready and each operation honor ctx.
	c, err := hc.NewSmartClient([]string{"127.0.0.1:50001"}, hc.SmartConfig{Mode: hc.MasterThenBackup})
	if err != nil {
		panic(err)
	}
	if err = c.Close(); err != nil {
		panic(err)
	}
	fmt.Println(c.IsReady())
	// Output: false
}
