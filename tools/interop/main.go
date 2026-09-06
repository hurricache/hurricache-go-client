// Command interop writes or verifies cross-language fixtures on an explicitly supplied server.
package main

import (
	"bytes"
	"context"
	"fmt"
	hc "github.com/hurricache/hurricache-go-client"
	"os"
	"strconv"
	"strings"
	"time"
)

func data(size int, random bool) []byte {
	b := make([]byte, size)
	x := uint32(1234567)
	for i := range b {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		b[i] = 42
		if random {
			b[i] = byte(x)
		}
	}
	return b
}
func key(prefix, name string) []byte {
	s := prefix + "/" + name
	if name == "longkey" {
		s += "/" + strings.Repeat("x", 2048)
	}
	return []byte(s)
}
func must(e error) {
	if e != nil {
		panic(e)
	}
}
func main() {
	if len(os.Args) != 5 {
		panic("usage: interop write|read|cleanup target unique-prefix hints-file")
	}
	action, target, prefix, file := os.Args[1], os.Args[2], os.Args[3], os.Args[4]
	c, e := hc.NewClient(target, hc.Config{ClientID: 77, Timeout: 10 * time.Second})
	must(e)
	defer c.Close()
	ctx := context.Background()
	names := []string{"empty", "small", "at", "compressed", "random", "list", "ordered", "map"}
	hints := map[string]*hc.KeyHint{}
	if action != "write" {
		b, e := os.ReadFile(file)
		must(e)
		for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
			p := strings.Fields(line)
			w, e := strconv.ParseUint(p[1], 10, 32)
			must(e)
			s, e := strconv.ParseUint(p[2], 10, 32)
			must(e)
			hints[p[0]] = &hc.KeyHint{WeakHash: hc.Ptr(uint32(w)), StrongHash: hc.Ptr(uint32(s))}
		}
	}
	var lines []string
	for _, name := range names {
		fmt.Println(action, name)
		k := key(prefix, name)
		o := hc.Options{Hint: hints[name]}
		var hint *hc.KeyHint
		if action == "cleanup" {
			_, e := c.Remove(ctx, k, o)
			must(e)
			continue
		}
		n := 4096
		switch name {
		case "empty":
			n = 0
		case "small":
			n = 1023
		case "at":
			n = 1024
		}
		if action == "write" {
			switch name {
			case "list":
				hint, e = c.CreateList(ctx, k, []hc.Payload{{Data: data(20, false)}, {Data: data(4096, false)}}, o)
			case "ordered":
				hint, e = c.CreateOrderedSet(ctx, k, []hc.OrderedPayload{{Payload: hc.Payload{Data: data(20, false)}, Order: hc.Ptr(uint64(1))}, {Payload: hc.Payload{Data: data(40, false)}, Order: hc.Ptr(^uint64(0))}}, o)
			case "map":
				hint, e = c.CreateMap(ctx, k, []hc.Entry{{Key: hc.Payload{Data: []byte("entry")}, Value: hc.Payload{Data: data(4096, false)}}}, o)
			default:
				hint, e = c.CreateKeyValue(ctx, k, data(n, name == "random"), o)
			}
			must(e)
			lines = append(lines, fmt.Sprintf("%s %d %d", name, *hint.WeakHash, *hint.StrongHash))
		} else {
			switch name {
			case "list":
				v, e := c.StreamList(ctx, k, o)
				must(e)
				if len(v) != 2 || !bytes.Equal(v[1].Data, data(4096, false)) {
					panic("list")
				}
				_, e = c.AddElementToTail(ctx, k, []hc.Payload{{Data: []byte("from-go")}}, o)
				must(e)
			case "ordered":
				v, e := c.StreamOrderedSet(ctx, k, o)
				must(e)
				if len(v) != 2 || *v[1].Order != ^uint64(0) {
					panic("ordered")
				}
			case "map":
				v, e := c.StreamMap(ctx, k, o)
				must(e)
				if len(v) != 1 || string(v[0].Key.Data) != "entry" || !bytes.Equal(v[0].Value.Data, data(4096, false)) {
					panic("map")
				}
			default:
				v, e := c.GetValue(ctx, k, o)
				must(e)
				if v.Value == nil || !bytes.Equal(v.Value.Data, data(n, name == "random")) {
					panic(name)
				}
			}
		}
	}
	if action == "write" {
		must(os.WriteFile(file, []byte(strings.Join(lines, "\n")+"\n"), 0600))
	}
	fmt.Println("Go " + action + " interoperability passed")
}
