#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
PROTOC="${PROTOC:-protoc}"
test "$("$PROTOC" --version)" = "libprotoc 34.1"
export GOBIN="$PWD/.tools"
export PATH="$GOBIN:$PATH"
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
"$PROTOC" -I proto --go_out=. --go_opt=module=github.com/hurricache/hurricache-go-client --go-grpc_out=. --go-grpc_opt=module=github.com/hurricache/hurricache-go-client proto/cache.proto proto/coordinator.proto
