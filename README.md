# HurriCache Go client

Concurrent direct and coordinator-routed clients targeting Java commit **`0a2b1d851405744e9390bcf5b5e8592177b121e4`**: **742 interface declarations**, concrete wrappers, typed models, and the active Jedis lifecycle adapters. This is the breaking **0.2.0** API/protocol migration.

Requires Go 1.26. Generated bindings are committed. Install with `go get github.com/hurricache/hurricache-go-client`; import as `hc`.

The November 11 standalone image starts and container CAS works, but NONE values, typed set equality, NaN ordering, full-width weighted lookup and live raw-LZ4 decoding still fail, including with current Java. Deterministic/offline checks pass; full live acceptance remains incomplete. See [FINDINGS.md](FINDINGS.md) for the exact digest and results. Real cluster, cluster failover, and replication acceptance remain unverified.

## Quick start

```go
client, err := hc.NewClient("127.0.0.1:50000", hc.Config{
    ClientID: 7,
    Timeout: 2 * time.Second,
})
if err != nil { return err }
defer client.Close()

ctx := context.Background()
key := []byte("example/greeting")
hint, err := client.Create(ctx, key, hc.String("hello"), hc.Options{
    TTL: hc.Ptr(time.Minute),
})
if err != nil { return err }
value, err := client.Get(ctx, key, hc.Options{Hint: hint})
if err != nil { return err }
body, err := value.Bytes() // checked accessor
if err != nil { return err }
fmt.Println(string(body))
```

`None()`, `Bytes([]byte)`, `String(string)`, `Int(int64)` and `Double(float64)` create immutable, comparable values. NONE, empty bytes, integer zero and double zero are distinct. Bytes copy input/output, all NaNs compare equal, and signed double zeros differ, matching Java value equality. `Ordered(value, order)` adds an optional `*uint64` order. Both models work as Go map keys.

```go
_, err = client.CreateList(ctx, []byte("mixed"), []hc.Variant{
    hc.None(), hc.Bytes(nil), hc.Int(0), hc.Double(1.25),
}, hc.Options{})
if err != nil { return err }
values, err := client.GetList(ctx, []byte("mixed"), hc.Options{})

_, err = client.Create(ctx, []byte("counter"), hc.Int(10), hc.Options{})
if err != nil { return err }
previous, err := client.Add(ctx, []byte("counter"), int64(2), hc.Options{})
// previous contains int64(10). Double operations return float64 inside any.

_, err = client.AddInContainer(ctx, []byte("mixed"), uint64(2), int64(3), hc.Options{})
// A []byte or string selector selects an entry key; an integer selects a position.
```

Map values are `Variant`; map keys remain `Payload` / `OrderedPayload`. Use `[]Entry` / `[]OrderedEntry` so stream ordering and duplicate byte keys at distinct orders are preserved.

```go
entry := hc.OrderedPayload{
    Payload: hc.Payload{Data: []byte("member")},
    Order: hc.Ptr(uint64(5)), // exact CURRENT order is required
}
ok, err := client.UpdateOrder(ctx, []byte("weights"), entry, 7, hc.Options{})
// IncrementOrder instead takes an int64 delta. Both preserve the member value.

popped, err := client.GetAndDeleteListElementsInRange(
    ctx, []byte("mixed"), nil, hc.Ptr(uint64(2)), false, hc.Options{},
)
```

## Smart routing and concurrency

```go
smart, err := hc.NewSmartClient([]string{"coordinator:50001"}, hc.SmartConfig{
    Client: hc.Config{ClientID: 7, Timeout: 2 * time.Second},
    Mode: hc.MasterThenBackup,
})
if err != nil { return err }
defer smart.Close()
if err := smart.Ready(ctx); err != nil { return err }
value, err := smart.Get(ctx, key, hc.Options{Hint: hint, Mode: hc.Ptr(hc.Backup)})
```

Both clients implement [Operations](interface.go) and can be shared across goroutines. Per-call `Options.Mode` replaces Java's thread-local routing override. Endpoints include IPv6 support. Topology refresh publishes a complete validated snapshot; in-flight calls retain their connections. Caller cancellation and one effective deadline cover encoding, readiness, all chunks, fallback and stream collection. No operation replays after acknowledged progress. `PartialError` reports acknowledged work and streaming calls return the decoded prefix. `RPCError` retains status details and trailers.

`NewClientWithConnection` borrows a gRPC connection; normal constructors own their connections. Close is idempotent and cancels pending work. `NewJedis` / `NewJedisPool` retain only Java's active local `Ping` and shared-client lifecycle; closing a resource or pool closes that shared client. Commented Redis commands are not implemented.

## Defaults and compression

Defaults: client ID 0, one-second timeout, no TTL, GZIP, 1024-byte payload threshold, 3.5 MiB serialized request limit, 64 MiB inbound and decoded limits. Explicit per-call ID zero is preserved. `Options.Timeout=hc.Ptr(time.Duration(0))` disables the client timeout and retains caller cancellation. Keep caller-owned slices and option pointers unchanged until the call returns.

`Config{CompressionCodec: hc.CompressionLZ4}` selects raw LZ4 blocks. Compression applies only to bytes, strictly above the threshold; numeric values never enter compression. The wire has no codec identifier: every writer, reader and server must use the same explicit codec. There is no codec sniffing, old-server fallback or general application retry. TLS and transport compression remain independent through gRPC dial options.

Batching preflights every serialized request, including continuation hints, before mutation. Map pairs stay together; continuations reuse one creation expiration and returned hints. Counts aggregate server results with overflow checks; empty insertions return zero without an RPC. Head chunks are sent in input order because the server reverses each head request; this preserves the result of one request across chunk boundaries. Pivot insertions advance their anchor after each chunk. These rules have live split-versus-single-request coverage.

TTL inputs use `time.Duration`; `GetTTL` returns remaining milliseconds or -1. Negative `SetTTL` clears expiration; zero expires now. Lock duration is encoded as an absolute Unix timestamp in seconds, following Java where the schema comment differs. Client ID controls lock ownership.

## Migrating to 0.2.0

This protocol is incompatible with the previous Go release and with the earlier Python draft's order-update schema. `BinaryPayload` now contains typed oneofs, coordinator targets are `Endpoint` messages, CAS observations are nested `ValueResponse`, and ordinary map updates use field **4**. Upgrade client and server together.

| Previous Go API | Current API / result |
| --- | --- |
| `CreateKeyValue(key, []byte, options)` | `Create(key, Bytes(data), options)`; returns hint and relay metadata |
| `GetValue` / `GetAndDeleteValue` | `Get` / `Pop`; returns `Variant` directly |
| `UpdateKeyValue` | `Replace`; returns previous `Variant` |
| `StreamList`, `StreamMap`, other streams | `GetList`, `GetMap`, other getters; typed values, ordered entry slices |
| `GetHead/GetTail`, `GetAndRemoveFront/Tail` | `PeekHead/PeekTail`, `PopHead/PopTail`; `Variant` |
| `RemoveHead/RemoveTail`, positional removal | `DeleteHead/DeleteTail`, `RemoveAt`; integer counts |
| Head/tail/pivot add methods and `*Count` helpers | `PushHead/PushTail`, `InsertBefore/InsertAfter`; integer counts |
| `AddElementToPosition`, `AddElementHashMap` | `InsertAt`, `PutAll`; integer counts |
| `GetContainerValue`, `GetAndRemoveContainerValue` | `GetInContainer`, `PopInContainer`; `Variant` |
| `UpdateContainerValue` | `Update`; previous `Variant`; `UpdateOrder` / `IncrementOrder` move exact ordered members |
| Integer `AtomicExchange/Add/Sub` | `Exchange/Add/Sub`; `int`/`int64`/`float64` operands, native `int64` or `float64` result inside `any` |
| `AtomicAnd/Or/Xor` | `And/Or/Xor`; integer-only operands/results |
| `AtomicCompareAndSet` | `CompareAndSet`; optional observed `Variant`, preserving numeric type and nested hints |
| `AtomicCreate/Store/Load/LoadAndDelete` | Removed; use `Create/Replace/Get/Pop` with `Int` or `Double` |
| `ValueResult`, `UpdateResult`, `AtomicResult`, `TTLResult` returns | Direct typed/native results; `GetTTL` now returns milliseconds / -1 |
| Positional / weighted range reads | `GetRange`, named weighted ranges; `PopRange` and `GetAndDelete...InRange` add destructive streaming reads |
| Java `setMode/clearMode` | Per-call `Options.Mode`; nil clears override |

Unambiguous old names remain deprecated aliases with current typed/native results. Collection inputs now use `[]Variant` / `[]OrderedVariant`; `Payload` remains for byte keys. There is no separate legacy-return mode. See [API_REFERENCE.md](API_REFERENCE.md) for signatures/defaults and [API_PARITY.md](API_PARITY.md) for every Java declaration and argument mapping.

## Verification

```powershell
go test -count=1 ./...
go vet ./...
python -m unittest discover -s tools -p test_parity.py
python tools/parity.py ../hurricache-java-client --check
python tools/reference.py --check
python tools/verify_consumer.py
powershell -File tools/verify-generation.ps1 -Protoc PATH_TO_PROTOC_34_1
powershell -File tools/interop.ps1 -JavaCheckout ../hurricache-java-client -JavaHome JDK_HOME -Offline
```

For live tests, first start a freshly pulled standalone image successfully, record its digest, and verify gRPC readiness. Set `HURRICACHE_TEST_TARGET` and run `go test -count=1 ./...`. Run once with `HURRICACHE_TEST_CODEC=gzip` and once with `HURRICACHE_TEST_CODEC=lz4`, using a matching server configuration for each codec. Without a codec filter, the live matrix defaults to both. Run `tools/interop.ps1` without `-Offline`, providing `-Target` and `-Codec gzip` / `lz4`, for bidirectional live acceptance. Offline fixtures alone cannot satisfy that gate.

## November 11 alignment

Container compare-and-set is included for map fields and ordered-map fields selected
by exact key and order. It uses write routing and leaves element TTL absent, matching
current Java. See API_REFERENCE.md for selectors and results.

Full queue-content getters are legacy conveniences unsupported by the server;
use queue size, peek and pop operations. The latest published server starts, but
full typed/LZ4 live acceptance still has server-side failures reproduced with Java.
See FINDINGS.md for current validation; earlier startup failures are historical.
