// Command schema compares Java and server descriptor sets without treating method order as a change.
package main

import (
	"fmt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	"os"
)

func read(path string) *descriptorpb.FileDescriptorProto {
	b, e := os.ReadFile(path)
	if e != nil {
		panic(e)
	}
	var set descriptorpb.FileDescriptorSet
	if e = proto.Unmarshal(b, &set); e != nil {
		panic(e)
	}
	for _, f := range set.File {
		if f.GetName() == "cache.proto" {
			return f
		}
	}
	panic("cache.proto missing")
}
func main() {
	if len(os.Args) != 3 {
		panic("usage: schema java.pb server.pb")
	}
	a, b := read(os.Args[1]), read(os.Args[2])
	messages := map[string]*descriptorpb.DescriptorProto{}
	for _, m := range b.MessageType {
		messages[m.GetName()] = m
	}
	for _, m := range a.MessageType {
		if !proto.Equal(m, messages[m.GetName()]) {
			panic("message conflict: " + m.GetName())
		}
		delete(messages, m.GetName())
	}
	enums := map[string]*descriptorpb.EnumDescriptorProto{}
	for _, m := range b.EnumType {
		enums[m.GetName()] = m
	}
	for _, m := range a.EnumType {
		if !proto.Equal(m, enums[m.GetName()]) {
			panic("enum conflict: " + m.GetName())
		}
	}
	methods := map[string]*descriptorpb.MethodDescriptorProto{}
	for _, s := range b.Service {
		if s.GetName() == "HurriCacheGrpcService" {
			for _, m := range s.Method {
				methods[m.GetName()] = m
			}
		}
	}
	count := len(methods)
	for _, s := range a.Service {
		if s.GetName() == "HurriCacheGrpcService" {
			for _, m := range s.Method {
				if !proto.Equal(m, methods[m.GetName()]) {
					panic("RPC conflict: " + m.GetName())
				}
				delete(methods, m.GetName())
			}
		}
	}
	if len(methods) != 0 {
		panic("additional client RPCs in server")
	}
	fmt.Printf("All %d Java messages, %d enums and %d client RPCs match the server descriptors.\n", len(a.MessageType), len(a.EnumType), count)
	for name := range messages {
		fmt.Println("Server-only message:", name)
	}
}
