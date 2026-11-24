package hurricache

import (
	"context"
	"encoding/binary"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// clientTSMetadataKey carries the client send time (Unix epoch ms, uint64
// little-endian). The server uses it to stamp write versions and to honestly
// reject writes delayed by the network (stale write).
// Matches the Java ClientSendTimeInterceptor.
const clientTSMetadataKey = "x-hc-client-ts-bin"

func withClientSendTime(ctx context.Context) context.Context {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(time.Now().UnixMilli()))
	return metadata.AppendToOutgoingContext(ctx, clientTSMetadataKey, string(b[:]))
}

func clientSendTimeUnaryInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoker(withClientSendTime(ctx), method, req, reply, cc, opts...)
	}
}

func clientSendTimeStreamInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return streamer(withClientSendTime(ctx), desc, cc, method, opts...)
	}
}
