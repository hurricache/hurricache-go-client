// Command interop writes/reads the same typed fixtures as pinned Java Interop.
package main

import (
	"context"
	"fmt"
	hc "github.com/hurricache/hurricache-go-client"
	"math"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func data(n int, random bool) []byte {
	b := make([]byte, n)
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
func must(e error) {
	if e != nil {
		panic(e)
	}
}
func check(ok bool, message string) {
	if !ok {
		panic(message)
	}
}
func main() {
	if len(os.Args) != 6 && len(os.Args) != 7 {
		panic("usage: interop ready|write|read|cleanup target unique-prefix hints-file gzip|lz4 [comma-separated-cases]")
	}
	action, target, prefix, file, codecName := os.Args[1], os.Args[2], os.Args[3], os.Args[4], os.Args[5]
	codec := hc.CompressionGZIP
	if codecName == "lz4" {
		codec = hc.CompressionLZ4
	} else if codecName != "gzip" {
		panic("codec")
	}
	c, e := hc.NewClient(target, hc.Config{ClientID: 77, Timeout: 30 * time.Second, CompressionCodec: codec})
	must(e)
	defer c.Close()
	ctx := context.Background()
	if action == "ready" {
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		for {
			_, e = c.ContainsKey(ctx, []byte(prefix+"/ready"), hc.Options{Timeout: hc.Ptr(time.Second)})
			if e == nil {
				fmt.Println("Server gRPC readiness passed")
				return
			}
			select {
			case <-ctx.Done():
				panic(e)
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	hints := map[string]*hc.KeyHint{}
	if action != "write" {
		b, e := os.ReadFile(file)
		must(e)
		for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
			row := strings.Fields(line)
			w, e := strconv.ParseUint(row[1], 10, 32)
			must(e)
			s, e := strconv.ParseUint(row[2], 10, 32)
			must(e)
			hints[row[0]] = &hc.KeyHint{WeakHash: hc.Ptr(uint32(w)), StrongHash: hc.Ptr(uint32(s))}
		}
	}
	lines := []string{}
	allCases := strings.Fields("empty small at compressed random list ordered map orderedmap large largeorderedmap none int double mixed storedmixed atomic longkey weightmap weightset")
	cases := allCases
	if len(os.Args) == 7 {
		cases = strings.Split(os.Args[6], ",")
		for _, name := range cases {
			found := false
			for _, allowed := range allCases {
				if name == allowed {
					found = true
					break
				}
			}
			check(found, "unknown diagnostic case: "+name)
		}
	}
	for _, name := range cases {
		key := []byte(prefix + "/" + name)
		if name == "longkey" {
			key = append(key, []byte("/"+strings.Repeat("x", 2048))...)
		}
		o := hc.Options{Hint: hints[name]}
		if action == "cleanup" {
			_, e = c.Remove(ctx, key, o)
			must(e)
			continue
		}
		size := 4096
		switch name {
		case "empty":
			size = 0
		case "small":
			size = 1023
		case "at":
			size = 1024
		}
		value := hc.Bytes(data(size, name == "random"))
		switch name {
		case "none":
			value = hc.None()
		case "int":
			value = hc.Int(math.MinInt64)
		case "double":
			value = hc.Double(math.Copysign(0, -1))
		case "atomic":
			value = hc.Int(15)
		}
		d := hc.ContainerData{}
		kind := hc.Undefined
		switch name {
		case "list":
			kind = hc.List
			d.Values = []hc.Variant{hc.Bytes(data(20, false)), hc.Bytes(data(4096, false))}
		case "mixed":
			kind = hc.List
			d.Values = []hc.Variant{hc.None(), hc.Bytes(nil), hc.Int(0), hc.Double(1.25)}
		case "storedmixed":
			kind = hc.List
			d.Values = []hc.Variant{hc.Bytes(nil), hc.Int(0), hc.Double(0), hc.Double(1.25), hc.Bytes(data(4096, false))}
		case "large":
			kind = hc.List
			d.Values = make([]hc.Variant, 1024)
			for i := range d.Values {
				d.Values[i] = hc.Bytes(data(4096, true))
			}
		case "ordered":
			kind = hc.OrderedSet
			d.OrderedValues = []hc.OrderedVariant{hc.Ordered(hc.Bytes(data(4096, false)), hc.Ptr(uint64(1))), hc.Ordered(hc.Bytes(data(4097, false)), hc.Ptr(uint64(math.MaxUint64)))}
		case "map":
			kind = hc.Map
			d.Entries = []hc.Entry{{Key: hc.Payload{Data: []byte("entry")}, Value: hc.Bytes(data(4096, false))}}
		case "largeorderedmap":
			kind = hc.OrderedMap
			for i := 0; i < 1024; i++ {
				d.OrderedEntries = append(d.OrderedEntries, hc.OrderedEntry{Key: hc.OrderedPayload{Payload: hc.Payload{Data: []byte{0, byte(i >> 8), byte(i), 255}}, Order: hc.Ptr(uint64(i + 1))}, Value: hc.Bytes(data(4096, true))})
			}
		case "orderedmap":
			kind = hc.OrderedMap
			for i := 1; i <= 2; i++ {
				d.OrderedEntries = append(d.OrderedEntries, hc.OrderedEntry{Key: hc.OrderedPayload{Payload: hc.Payload{Data: data(4095+i, false)}, Order: hc.Ptr(uint64(i))}, Value: hc.Bytes(data(4095+i, true))})
			}
		case "weightmap", "weightset":
			kind = hc.OrderedMap
			if name == "weightset" {
				kind = hc.OrderedSet
			}
			for i := 1; i <= 2; i++ {
				order := uint64(5)
				if i == 2 {
					order = 7
				}
				v := hc.Double(1.25)
				if i == 2 {
					v = hc.Int(2)
				}
				d.OrderedEntries = append(d.OrderedEntries, hc.OrderedEntry{Key: hc.OrderedPayload{Payload: hc.Payload{Data: []byte("dup")}, Order: hc.Ptr(order)}, Value: v})
				d.OrderedValues = append(d.OrderedValues, hc.Ordered(hc.String("dup"), hc.Ptr(order)))
			}
			if kind == hc.OrderedMap {
				d.OrderedValues = nil
			} else {
				d.OrderedEntries = nil
			}
		}
		if action == "write" {
			var h *hc.KeyHint
			if kind == hc.Undefined {
				initial := value
				if name == "atomic" {
					initial = hc.Int(10)
				}
				h, e = c.Create(ctx, key, initial, o)
				must(e)
				if name == "atomic" {
					_, e = c.Add(ctx, key, int64(5), hc.Options{Hint: h})
				}
			} else {
				initial := d
				if name == "weightmap" {
					initial.OrderedEntries = append([]hc.OrderedEntry(nil), d.OrderedEntries...)
					for i := range initial.OrderedEntries {
						initial.OrderedEntries[i].Key.Order = hc.Ptr(uint64(i + 1))
					}
				}
				if name == "weightset" {
					initial.OrderedValues = []hc.OrderedVariant{hc.Ordered(hc.String("dup"), hc.Ptr(uint64(1))), hc.Ordered(hc.String("dup"), hc.Ptr(uint64(2)))}
				}
				h, e = c.CreateContainer(ctx, key, kind, initial, o)
				must(e)
				if name == "weightmap" || name == "weightset" {
					o.Hint = h
					k := hc.OrderedPayload{Payload: hc.Payload{Data: []byte("dup")}, Order: hc.Ptr(uint64(1))}
					ok, err := c.IncrementOrder(ctx, key, k, 4, o)
					must(err)
					check(ok, "increment order")
					k.Order = hc.Ptr(uint64(2))
					ok, err = c.UpdateOrder(ctx, key, k, 7, o)
					must(err)
					check(ok, "absolute order")
				}
			}
			must(e)
			check(h != nil && h.WeakHash != nil && h.StrongHash != nil, "hints")
			lines = append(lines, fmt.Sprintf("%s %d %d", name, *h.WeakHash, *h.StrongHash))
			continue
		}
		if kind == hc.Undefined {
			got, e := c.Get(ctx, key, o)
			must(e)
			check(got == value, "scalar "+name)
			if name == "atomic" {
				n, e := c.Exchange(ctx, key, int64(15), o)
				must(e)
				check(n == int64(15), "atomic exchange")
			}
			continue
		}
		got, e := c.GetContainer(ctx, key, o)
		must(e)
		check(reflect.DeepEqual(got.Values, d.Values) || len(got.Values)+len(d.Values) == 0, "typed list/continuations")
		check(reflect.DeepEqual(got.OrderedValues, d.OrderedValues) || len(got.OrderedValues)+len(d.OrderedValues) == 0, "ordered variants")
		check(len(got.Entries) == len(d.Entries) && len(got.OrderedEntries) == len(d.OrderedEntries), "map counts")
		for i, v := range got.Entries {
			want := d.Entries[i]
			check(string(v.Key.Data) == string(want.Key.Data) && v.Value == want.Value, "map pairing")
		}
		for i, v := range got.OrderedEntries {
			want := d.OrderedEntries[i]
			check(string(v.Key.Data) == string(want.Key.Data) && v.Value == want.Value && v.Key.Order != nil && *v.Key.Order == *want.Key.Order, "ordered map pairing")
		}
		if name == "weightmap" || name == "weightset" {
			k := hc.OrderedPayload{Payload: hc.Payload{Data: []byte("dup")}, Order: hc.Ptr(uint64(5))}
			ok, e := c.IncrementOrder(ctx, key, k, 1, o)
			must(e)
			check(ok, "reader increment")
			k.Order = hc.Ptr(uint64(6))
			ok, e = c.UpdateOrder(ctx, key, k, 5, o)
			must(e)
			check(ok, "reader restore")
		}
	}
	if action == "write" {
		must(os.WriteFile(file, []byte(strings.Join(lines, "\n")+"\n"), 0600))
	}
	fmt.Printf("Go live %s passed (%s; %s)\n", action, codecName, strings.Join(cases, ","))
}
