# HurriCache Go client

Concurrent Go clients for HurriCache, with direct connections and coordinator-based smart routing. The implementation covers the Java direct and smart client APIs at jdk-16 commit `fd2f89d56751b000a62565ef3ff0712040d5bbfd`, including convenience operations. The Jedis facade is excluded.

Requires **Go 1.26**. Transport, TLS, cancellation and streaming use [grpc-go](https://grpc.io/docs/languages/go/basics/); generated messages use [Go protobuf](https://protobuf.dev/reference/go/go-generated/); compression uses [pierrec/lz4](https://github.com/pierrec/lz4). There are no application-level general retries.

Read [API_PARITY.md](API_PARITY.md) for every Java overload, Go equivalent, routing policy and test. [FINDINGS.md](FINDINGS.md) records deliberate Java corrections, server defects and verification results.

## Installation

```sh
go get github.com/hurricache/hurricache-go-client
```

The import name is `hurricache`; examples use the alias `hc`. Generated bindings are committed, so consumers do not need protoc, Python, Java or Docker. For an unpublished local checkout, use a Go module `replace` directive pointing to this directory.

## Pinned standalone server

Run Docker's Linux engine:

```sh
docker run -d --name hurricache-dev -p 127.0.0.1:50000:50000 \
  alexaborisov/fastcache-standalone-noavx512:26.35@sha256:557567683c2026cc4454fea63e65a8ec7ecaa18abcfc7ce1f77f118d86424d86
```

If Windows reserves port 50000, use `-p 127.0.0.1::50000` and obtain the assigned port with `docker port hurricache-dev`. The image runs a standalone data service, not a coordinator.

**Pinned-server limitations:** compressed keys longer than 1,024 bytes crash this image in both Java and Go. Use shorter keys with 26.35. Map updates can return `Updated=false` even after the value changes. Reverse weighted ranges return incorrect results. Queue streaming is unsupported; queue push/pop operations work. These are documented and independently reproduced in [FINDINGS.md](FINDINGS.md).

## Direct quick start

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    hc "github.com/hurricache/hurricache-go-client"
)

func main() {
    client, err := hc.NewClient("127.0.0.1:50000", hc.Config{
        ClientID: 7,
        Timeout:  2 * time.Second,
    })
    if err != nil { log.Fatal(err) }
    defer client.Close()

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    key := []byte("example/greeting")
    hint, err := client.CreateKeyValue(ctx, key, []byte("hello"), hc.Options{
        TTL: hc.Ptr(time.Minute),
    })
    if err != nil { log.Fatal(err) }
    result, err := client.GetValue(ctx, key, hc.Options{Hint: hint})
    if err != nil { log.Fatal(err) }
    if result.Value != nil { fmt.Println(string(result.Value.Data)) }
    if _, err = client.Remove(ctx, key, hc.Options{Hint: hint}); err != nil {
        log.Fatal(err)
    }
}
```

`NewClient` opens an owned, lazily connected gRPC channel. Constructor success does not establish server readiness; the first RPC connects. The defaults are client ID zero, a one-second operation timeout, no expiration, a 3.5 MiB request limit, and a 64 MiB received-message/decoded-payload limit.

## Smart quick start

```go
smart, err := hc.NewSmartClient(
    []string{"coordinator-a:50001", "coordinator-b:50001"},
    hc.SmartConfig{
        Client: hc.Config{ClientID: 7, Timeout: 2 * time.Second},
        Mode: hc.MasterThenBackup,
    },
)
if err != nil { return err }
defer smart.Close()
if err := smart.Ready(ctx); err != nil { return err }

hint, err := smart.CreateKeyValue(ctx, []byte("example/smart"), []byte("hello"), hc.Options{})
if err != nil { return err }
result, err := smart.GetValue(ctx, []byte("example/smart"), hc.Options{
    Hint: hint,
    Mode: hc.Ptr(hc.Backup),
})
_ = result
return err
```

Both clients implement [`Operations`](interface.go). All methods accept `context.Context` first and return a typed result plus an error. Callers use goroutines for concurrency.

## Context, concurrency and ownership

The effective deadline is the earlier of the caller's deadline and the configured/per-call timeout. It bounds preprocessing, discovery readiness, all chunks, any permitted fallback and stream collection as one operation. `Options{Timeout: hc.Ptr(time.Duration(0))}` disables the client timeout while retaining caller cancellation. Negative timeouts and TTLs are rejected.

Clients may be shared across goroutines. Keep caller-owned argument slices, pointers and option data unchanged until the call returns. Results belong to the caller. Avoid mutating `SmartClient.Client`; it exposes the common method set through embedding.

```go
var wg sync.WaitGroup
for _, key := range keys {
    wg.Go(func() {
        _, err := client.GetValue(ctx, key, hc.Options{})
        if err != nil { log.Print(err) }
    })
}
wg.Wait()
```

`Close` is idempotent. It cancels pending client operations, stops refresh, cancels discovery, and closes owned connections. `NewClientWithConnection(conn, config)` borrows a `grpc.ClientConnInterface`; closing the client leaves that channel open. In smart clients, removed endpoints remain open until calls using the old topology finish.

`SmartConfig.ConnectionFactory` supports custom transports and ownership. Return `Connection{Conn: conn}` to borrow, or `Connection{Conn: conn, Close: conn.Close}` to transfer ownership. The factory must honor its context and return promptly after cancellation. Its Close callback should be prompt and must not re-enter the client. Return a separate ownership handle per target when transferring ownership.

For TLS, set `Config.TLS` to a configured `*tls.Config` (server name, trusted roots, and optionally client certificates). The configuration is cloned at construction; nested certificate data should remain immutable. Extra `DialOptions` permit interceptors and custom dialing. A supplied borrowed channel controls its own transport configuration and retry policy.

## Binary data, presence and metadata

Keys and values are `[]byte`. Convert string keys with `[]byte(text)`, matching Java's UTF-8 string overloads for valid Go UTF-8 strings. No serialization or hashing is imposed.

| Type | Meaning |
| --- | --- |
| `KeyHint` | Optional `WeakHash` and `StrongHash`, each a `*uint32`. WeakHash corresponds to the protocol spelling `week_hash`. |
| `Payload` | Decoded bytes plus optional expiration, lock, hint, compression and client ID metadata when supplied by the wire. |
| `OrderedPayload` | Payload plus optional `*uint64` order; the full unsigned range is retained. |
| `ValueResult` | Independent unordered/ordered value presence plus the response hint. |
| `Entry`, `OrderedEntry` | Binary map key/value pairs in server stream order. |
| `UpdateResult` | `Updated` and optional `Previous` are independent. |
| `AtomicResult`, `CASResult` | Signed 64-bit values, hints and optional CAS observed/expected value. |

A nil value pointer means the response did not include that value. A non-nil payload with zero-length `Data` is a present empty value. Server `NotFound` statuses remain errors; they are not silently converted to empty values.

`hc.Ptr(0)` expresses explicit zero, while nil preserves absence. A nil option field inherits the client default. Hint absence and a zero hint are different. Client ID zero is explicitly sent by default; all uint32 ID bits survive. Response fields that the server omits remain nil.

Input `Payload.ExpiresAtMillis` overrides the supplied collection-element expiration; `Payload.Lock` is encoded when present. Map-key hints and explicit key IDs can be supplied through entries. Response compression metadata describes how bytes arrived, even though `Data` is already decoded. These metadata fields are not instructions to reuse previously compressed bytes.

## TTL and locks

Creation/update TTLs are relative `time.Duration` values converted to absolute Unix milliseconds. Zero omits TTL for these operations. Container creation sets container expiration; Java-style map values and ordered-set elements also receive the creation TTL. `SetTTL` always supplies an absolute expiration, so a zero duration expires now. `GetTTL` returns both optional remaining duration and the original wire timestamp; unrepresentable durations return `OutOfRange`.

```go
ok, err := client.SetTTL(ctx, key, time.Minute, options)
if err != nil { return err }
ttl, err := client.GetTTL(ctx, key, options)
if err != nil { return err }
if ok && ttl.Remaining != nil { fmt.Println(*ttl.Remaining) }

lock, err := client.LockObject(ctx, key, hc.WriteLock, 10*time.Second, options)
if err != nil { return err }
if lock.Status != hc.LockOK { return fmt.Errorf("lock: %v", lock.Status) }
unlocked, err := client.UnlockObject(ctx, key, options)
_ = unlocked
return err
```

Lock modes are `NoLock`, `WriteLock`, `ReadLock`, and `GlobalLock`. The lock duration is truncated to whole seconds, matching Java. Negative durations or values exceeding uint32 seconds are rejected. The effective client ID is sent both in the key and lock request. A successful transport call can return `CantLock`, `CantUnlock` or `LockGenericError`; inspect the status and optional message.

## Operation families

The following fragments assume an existing client, context, key and `options := hc.Options{}`, inside an error-returning function. All overloads are consolidated into explicit arguments and options.

### Scalars

`CreateKeyValue`, `GetValue`, `GetAndDeleteValue`, `UpdateKeyValue`, `ExistKey`, `Remove`, and `GetSize` cover scalar creation, reads, replacement, existence, deletion and server size/count.

```go
previous, err := client.UpdateKeyValue(ctx, key, []byte("new value"), options)
if err != nil { return err }
if previous.Previous != nil { fmt.Println(string(previous.Previous.Data)) }
exists, err := client.ExistKey(ctx, key, options)
if err != nil { return err }
fmt.Println(exists)
deleted, err := client.GetAndDeleteValue(ctx, key, options)
_ = deleted
return err
```

### Queues, lists and vectors

`CreateQueue`, `CreateList`, `CreateVector` accept `[]Payload`, including nil for an empty container. Use `GetHead`/`GetFront`, `GetTail`, `GetAndRemoveFront`, `GetAndRemoveTail`, `RemoveHead`, `RemoveTail`, `AddElementToHead`, and `AddElementToTail` as supported by the container.

```go
hint, err := client.CreateQueue(ctx, key, []hc.Payload{{Data: []byte("job-1")}}, options)
if err != nil { return err }
options.Hint = hint
if _, err = client.AddElementToTail(ctx, key, []hc.Payload{{Data: []byte("job-2")}}, options); err != nil {
    return err
}
job, err := client.GetAndRemoveFront(ctx, key, options)
if err != nil { return err }
if job.Value != nil { fmt.Println(string(job.Value.Data)) }
return nil
```

The server applies head insertion once per supplied element: inserting `[a,b,c]` at the head produces `[c,b,a,...]`. Chunking preserves that same behavior. Tail insertion retains input order. `StreamList` and `StreamVector` collect decoded contents. `StreamQueue` is an extra Go convenience, but the pinned server rejects queue streaming.

### Positions and ranges

`Position.Start` and `End` are uint64. `End=nil` means one position; an explicit zero is retained. Range endpoints must be ascending. Insertion positions are uint32 because that request uses uint32 on the wire.

```go
item, err := client.GetElementAtPosition(ctx, key, hc.Position{Start: 0}, options)
if err != nil { return err }
_ = item
count, err := client.AddElementToPosition(ctx, key, []hc.Payload{{Data: []byte("inserted")}}, 1, options)
if err != nil { return err }
fmt.Println(count)
items, err := client.StreamElementInRangeUnordered(ctx, key, hc.List, 0, 2, options)
if err != nil { return err }
fmt.Println(len(items))
_, err = client.RemoveElementAtPosition(ctx, key, hc.Position{Start: 1, End: hc.Ptr(uint64(2))}, options)
return err
```

`GetAndRemoveElementAtPosition` pops at a position. `AddElementToPositionBefore` and `AddElementToPositionAfter` accept a pivot payload. Relative insertions retain the same order across chunks as one request. Concurrent mutations by other callers can change positions or pivots between requests; a chunked operation is not atomic.

### Sets and ordered sets

```go
hint, err := client.CreateSet(ctx, key, []hc.Payload{{Data: []byte("a")}}, options)
if err != nil { return err }
options.Hint = hint
if _, err = client.AddElement(ctx, key, []hc.Payload{{Data: []byte("b")}}, options); err != nil { return err }
removed, err := client.RemoveFromContainer(ctx, key, hc.Removal{
    Type: hc.Set,
    Values: []hc.Payload{{Data: []byte("a")}},
}, options)
fmt.Println(removed)
return err
```

`CreateOrderedSet` and `AddElementWithWeight` accept `[]OrderedPayload`. Missing input weights default to explicit zero, as Java does. `AddElementOrdered` and `AddElementOrderedSet` are boolean conveniences. `StreamSet` and `StreamOrderedSet` return the full decoded collection.

```go
hint, err := client.CreateOrderedSet(ctx, key, []hc.OrderedPayload{
    {Payload: hc.Payload{Data: []byte("low")}, Order: hc.Ptr(uint64(1))},
    {Payload: hc.Payload{Data: []byte("high")}, Order: hc.Ptr(uint64(100))},
}, options)
if err != nil { return err }
ranked, err := client.StreamElementInRangeOrderedSet(ctx, key, 1, 100, false, hc.Options{Hint: hint})
_ = ranked
return err
```

The `reverse` argument is transmitted, correcting Java's omission. The pinned server has a reverse-range defect; ascending ranges work. `StreamElementInRangeOrdered` is the ascending convenience.

### Maps and ordered maps

Entry slices make pair boundaries explicit and support arbitrary binary keys. Neither Go maps nor string conversion are used to represent binary-key results.

```go
hint, err := client.CreateMap(ctx, key, []hc.Entry{
    {Key: hc.Payload{Data: []byte{0, 255}}, Value: hc.Payload{Data: []byte("value")}},
}, options)
if err != nil { return err }
options.Hint = hint
entries, err := client.StreamMap(ctx, key, options)
if err != nil { return err }
fmt.Println(len(entries))
value, err := client.GetContainerValue(ctx, key, []byte{0, 255}, options)
_ = value
return err
```

`ContainsContainerKey`, `UpdateContainerValue`, `GetAndRemoveContainerValue` and `RemoveContainerKey` operate on one entry. `AddElementHashMap` accepts `[]Entry`. `CreateOrderedMap`, `StreamOrderedMap` and `AddElementOrderedMap` use `[]OrderedEntry` with weights on keys. Batch removals use `Removal{Type: hc.Map, Keys: ...}` or `hc.OrderedMap`. Input pairs are never truncated to the shorter of two arrays.

The pinned server's map update returns the previous bytes but can leave the success bit false. `UpdateResult` exposes both exactly; the client does not retry or reinterpret the false bit as proof that nothing changed.

### Atomics and CAS

```go
hint, err := client.AtomicCreate(ctx, key, 10, options)
if err != nil { return err }
options.Hint = hint
old, err := client.AtomicAdd(ctx, key, 2, options)
if err != nil { return err }
fmt.Println(old.Value) // 10: arithmetic/exchange operations return the previous value
cas, err := client.AtomicCompareAndSet(ctx, key, 12, 20, options)
if err != nil { return err }
if !cas.Swapped && cas.Expected != nil { fmt.Println(cas.Expected.Value) }
return nil
```

Also available: `AtomicLoad`, `AtomicLoadAndDelete`, `AtomicStore`, `AtomicExchange`, `AtomicSub`, `AtomicOr`, `AtomicAnd`, and `AtomicXor`. Values and masks use int64; no conversion to floating point occurs. Failed CAS retains the optional observed value and hints.

## Compression

Unordered keys and values **larger than** 1,024 bytes use raw LZ4 blocks. Exactly 1,024 bytes stays uncompressed. Payload size is the encoded length; `rawSize` is the original length. Incompressible inputs use a valid literal-only LZ4 block if the compressor returns no compressed block, maintaining Java's above-threshold metadata.

Ordered keys and values are transmitted uncompressed, matching Java. Returned compressed payloads and map keys are decoded transparently in both unary and streaming responses. Missing raw size, malformed blocks, mismatched payload size, and incorrect decoded length fail with errors. `MaxDecodedBytes` bounds each allocation; collection-returning helpers necessarily hold the whole accumulated collection and do not impose an aggregate collection-size limit.

## Chunking and errors

The default request limit is 3,670,016 bytes (3.5 MiB), calculated with `proto.Size` including request headers, repeated-field framing, keys and map pairs. Every individual item is checked before the first mutation. An oversized item returns `ResourceExhausted`. Compression occurs before wire-size measurement.

Creation uses one create request followed by additions for remaining items. Additions and removals preserve input/pair boundaries. All chunks share one operation deadline. Count-returning methods sum server-reported counts; successful boolean operations preserve Java's empty-input result. A server rejection stops further chunks.

On a later failure, `PartialError.CompletedChunks` reports acknowledged RPCs and `CompletedItems` reports input items in completed work, not a transactional rollback or the number of unique set changes. For streams they report completed batches/items; the returned collection contains the decoded prefix. A rejected boolean chunk can count as acknowledged without adding completed items. The failed RPC may also have executed; do not automatically replay a mutation.

```go
var partial *hc.PartialError
if errors.As(err, &partial) {
    fmt.Printf("%d chunks, %d input items completed\n", partial.CompletedChunks, partial.CompletedItems)
}
var rpc *hc.RPCError
if errors.As(err, &rpc) {
    fmt.Println(status.Code(err), rpc.Trailers.Get("x-fastcache-route"))
}
```

`RPCError` preserves the underlying gRPC status, details, trailers and unwrap chain. `status.Code(err)` works through both error types. No completed batch prefix is retried by smart routing.

## Smart routing details

Discovery calls `CoordinatorService.provideGlobalRoutingInfo`, consumes the entire stream, validates shard counts and routes, and publishes an immutable snapshot. Empty/inconsistent topology does not replace the last good snapshot. Multiple coordinators are tried in rotation on discovery failure.

Defaults: refresh every 30 seconds, each discovery attempt bounded by the client timeout, readiness bounded by 60 seconds. A caller deadline can shorten these waits. `Ready(ctx)` waits for the first valid topology; `IsReady()` reports whether a usable snapshot exists; `Refresh(ctx)` requests discovery immediately. Background refresh and on-demand refresh are serialized. Discovery readiness does not assert that every data node is reachable.

With a weak hint, the shard is `uint32(WeakHash) % maxShards`. Without it, Java's increment-before-selection counter is used: `(counter & 0x7fffffff) % maxShards`. Strong-only hints use this same counter. No client-side key hash is substituted.

| Mode | Initial target | On Unavailable |
| --- | --- | --- |
| `Master` | Master | Return error |
| `Backup` | Backup | Return error |
| `MasterThenBackup` | Master | Try backup once |
| `LBSmart` | Random master or backup | Try other role once |

As in Java, if only one role exists, it is selected regardless of mode. If neither role exists for the chosen shard, the call returns `Unavailable`; it does not choose another shard. Read/configured operations use `SmartConfig.Mode`. Java's operation-specific write group uses `MasterThenBackup`. In particular, scalar create/update/remove and container creation retain Java's configured-mode dispatch; they are not silently reassigned to the write group. The exhaustive policy is in [API_PARITY.md](API_PARITY.md).

`Options.Mode` explicitly overrides either policy for one call. This replaces Java's ineffective thread-local mode setter and is safe across goroutines.

A `FailedPrecondition` with `x-fastcache-route` may reroute once to a different target already present in the captured topology. Unknown, missing or self routes preserve the original error. A rerouted/fallback attempt is final; there is no recursive retry chain. Unavailable failures trigger refresh. Deadline and other statuses do not trigger unavailable fallback.

Fallback follows Java's bounded behavior and is not an exactly-once guarantee: a network failure can occur after a mutation executes. Once a batch/stream prefix has completed, the operation is never replayed on another node. A standalone server cannot validate replication, migration or real coordinator deployment; smart routing is tested with actual in-process gRPC coordinator and node services.

## Development and verification

```sh
go test ./...
go vet ./...
go test -race ./...
gofmt -l .
HURRICACHE_TEST_TARGET=127.0.0.1:50000 go test -run TestLive -v ./...
```

PowerShell live tests:

```powershell
$env:HURRICACHE_TEST_TARGET = "127.0.0.1:50000"
go test ./...
Remove-Item Env:HURRICACHE_TEST_TARGET
```

Live tests use random namespaced keys and remove only their own keys. Server-specific regression assertions explicitly check the documented 26.35 defects. The long-key crash probe is excluded from the regular suite. On Windows without a C compiler, run race tests in Linux:

```sh
docker run --rm -v "$PWD:/src:ro" -w /src golang:1.26.5 go test -race ./...
```

Executable examples are in [example_test.go](example_test.go). [protocol_test.go](protocol_test.go) covers every operation, field presence, compression and cancellation; [batch_test.go](batch_test.go) covers chunking, pairs and partial failures; [smart_test.go](smart_test.go) covers every dispatch policy, modes, reroutes, coordinator rotation and ownership. Live operation and ordering tests are in [integration_test.go](integration_test.go) and [live_boundaries_test.go](live_boundaries_test.go).

### Reproducible protobuf generation

Pinned tools: protoc **34.1**, protoc-gen-go **v1.36.11**, protoc-gen-go-grpc **v1.6.2**. Schemas are copied from the authoritative Java commit with only `go_package` added. Protobuf package and service names are unchanged.

```sh
PROTOC=/path/to/protoc sh tools/generate.sh
git diff --exit-code -- proto
```

```powershell
./tools/generate.ps1 -Protoc C:/path/to/protoc.exe
./tools/verify-generation.ps1 -Protoc C:/path/to/protoc.exe
```

Generators are installed into ignored `.tools/`. Both schema and generated files are committed. Runtime dependency versions are pinned in [go.mod](go.mod) and checksummed in [go.sum](go.sum).

To compare an extracted image schema, generate descriptor sets with protoc (`--descriptor_set_out=...`) from Java's cache.proto and the image's cache.proto, then run `go run ./tools/schema java.pb server.pb`. The comparator checks all existing messages/enums and every client RPC by name, ignoring service method ordering. The pinned image adds replication messages/service functionality only.

### Java interoperability

[tools/interop.ps1](tools/interop.ps1) generates Java bindings and compiles the exact reference client in `.work/java-interop/`, outside the Java checkout. It resolves dependencies from a copy of the Java POM, writes fixtures in each language, reads them in the other using transferred hints, exercises collection insertion and cleans up fixture keys.

```powershell
./tools/interop.ps1 -JavaCheckout C:/src/hurricache-java-client -JavaHome C:/Java/jdk-17 -Target 127.0.0.1:50000
```

The harness expects protoc 34.1 and protoc-gen-grpc-java 1.80.0 in the Java checkout's `target/protoc-plugins`; it regenerates bindings from the authoritative schemas rather than trusting stale target classes. The Java runtime dependencies come from the pinned POM (gRPC 1.82.1). JDK 17 runs the Java 16-compatible source. No source files in the Java checkout are changed.

The separate `Interop probe-longkey` action reproduces the known server crash and must only be run against a disposable server. It is not invoked by the interoperability script.
