# Java / Go parity findings

Current Java baseline: `98225d9365e0ca7cea81e83116ed6f6ddba4294a`.
Scope: native standalone and smart clients. Jedis/Redis facades, performance
parity and live replicated-cluster validation are excluded. Standalone live
tests plus simulated-cluster tests are the agreed validation scope.

## Verification on October 20, 2026

The inventory maps all **472 Java interface declarations**, including overload
options, return shapes, lifecycle operations and smart routing. The operation
families already existed in Go. Integer-returning head/tail/pivot overloads now
map to their `*Count` equivalents; boolean compatibility helpers remain available.
Both schemas match this Java pin with only `go_package` added.

`tools/parity.py --check` reads immutable Git objects regardless of checkout HEAD,
compares generated output without writing it, and fails on stale output, missing
Go equivalents, missing matrix cases, unresolved routing or schema drift. It
reflects the Go contract and reads actual operation matrix cases through
`TestPublicContractCoverage`. Wrapper routing follows balanced Java method bodies
and nested helper calls. This exposed collection creation's `executeWrite` policy;
Go now uses `MasterThenBackup` for all collection creation, including continuations.
Scalar create/update/remove continue to use configured mode. Caller-local overrides
take precedence for every operation and are tested against both node roles.

| Check | Result |
| --- | --- |
| `go test -count=1 ./...` | Passed, including committed Java-produced GZIP/LZ4 fixtures; live cases skip without a target. |
| Full suite with `HURRICACHE_TEST_TARGET` set | Passed on the pinned GZIP digest below after verified gRPC readiness. |
| `go vet ./...` | Passed. |
| `go test -race -count=1 ./...` | Passed on Windows with portable Clang and cgo enabled. |
| Formatting / `git diff --check` | Passed. |
| Protobuf binding regeneration | All four files reproduced byte-for-byte; schemas unchanged. |
| Inventory reproducibility / generator regressions | Passed, including non-writing stale-output failure, count aliases, helper routing, absent equivalents/coverage and routing mismatch. |
| External-consumer module | Built using both clients, Operations and explicit codec configuration. |
| Offline Java → Go and Go → Java, GZIP | Passed. |
| Offline Java → Go and Go → Java, raw LZ4 | Passed. |
| Live Java → Go and Go → Java, GZIP | Passed, including repaired ordered-map reads, ascending/descending ranges, unsigned hints/weights, a 4 MiB list and a 4 MiB ordered map requiring continuation chunks. Every ordered-map key, value and position was checked in both directions. |
| Live LZ4 interoperability | **Pending: an identified LZ4-compatible server build is required.** |

Go is 1.26.5; Java compilation and execution used JDK 25.0.4.1 (the harness
requires JDK 24 or newer). Java bindings use protoc 34.1 and grpc-java 1.82.1;
runtime dependencies come from the pinned POM, including lz4-java 1.8.0 and
protobuf 4.34.1. Go uses grpc-go 1.83.0, protobuf 1.36.11 and
`github.com/pierrec/lz4/v4` **v4.1.31**. Go generators remain protobuf 1.36.11
and grpc 1.6.2. Reference sources, generated Java bindings and classes were built
outside the authoritative Java checkout, which was left untouched.

The first live attempt preceded server initialization and returned transport EOF.
After initialization, an actual gRPC call confirmed readiness and the full suite
and cross-language harness passed. The harness now checks readiness before writes.
Only the task-created disposable container is removed after validation; unrelated
containers are left running. Normal interoperability fixtures are cleaned in both
directions.

## Compression contract

`Config.CompressionCodec` is copied at construction and shared by direct/smart
operations. `CompressionGZIP` is enum zero and the default; `CompressionLZ4`
selects raw Java-compatible LZ4 blocks. Invalid values fail construction. There
are no per-call overrides, codec sniffing or automatic fallback. The protocol
contains compression presence and raw size, with **no codec identifier**: the
server and every reader of compressed data must agree on the codec.

Compression applies strictly when `length > threshold`. The default threshold is
1024; zero compresses every nonempty eligible payload; negative values fail.
LZ4 compression allocates the library block bound, so incompressible input still
produces a valid block. The client validates encoded and decoded sizes and checks
`MaxDecodedBytes` before decompression allocation. GZIP validates framing/checksum;
raw LZ4 has no checksum and cannot detect every same-size content corruption.

| Content | Payload compression |
| --- | --- |
| Top-level keys; scalar create/update values | Configured threshold |
| List/vector/queue/unordered-set elements; positional values and pivots | None |
| Ordinary-map creation/addition keys and values | None |
| Ordered-set creation/addition values | Configured threshold |
| Ordered-map creation/addition keys and values | Configured threshold |
| Single-entry lookup/removal keys; container-update values | None |
| Batch-removal keys | Configured threshold |
| Batch-removal values | None |

The offline harness calls the pinned Java `CompressionUtils` and repaired
`StreamBatchOrderedMapObserver`. Committed protobuf fixtures cover empty data,
threshold boundaries, repeated/incompressible input, compressed binary keys and
values and explicit client ID zero. Both languages verify bytes and iteration
order across multiple batches in both directions. Additional Go coverage exercises
zero/small/default thresholds, malformed blocks, missing/incorrect sizes, allocation
limits, exact request bounds, chunk pairing, cancellation before and after progress,
decoded stream prefixes, returned hints/TTL and prevention of smart replay after an
acknowledged chunk. The Go suite consumes fixtures without requiring Java.

The Java ordered-map observer now returns actual payload bytes, decompresses keys
and values, preserves insertion order with `LinkedHashMap` and rejects unpaired
batches. The previous Java reader blocker is resolved at this pin. Offline checks
verify the fix for both codecs; live GZIP checks confirm both directions.

## Server findings and remaining acceptance gate

Live GZIP validation uses:

```text
alexaborisov/fastcache-standalone-noavx512@sha256:e79c96e7610c10ba99c7c11f939c78154ff3a4e6d24689ffdd15f35d2d7b7b1e
```

The server reports build `a30e840-dirty`, commit `a30e840`, built September 3,
2026. Map-update status and reverse weighted ranges pass on this digest. Prior
GZIP long-key probes also passed; historical crashes on 26.35 do not describe
the current digest. Queue `getContainer` remains unsupported, although queue
creation, continuation, push/pop operations pass.

Lock requests carry an **absolute Unix-seconds expiration**, obtained by adding
the whole-second duration to current Unix time. TTL requests and returned lock
metadata use their documented millisecond fields. The September 12–13 observation
on this same digest found a lock still held after the requested expiration;
this remains a documented server limitation, not a client unit-conversion workaround.
Successful lock/unlock status and ownership checks pass the current live suite.

Deterministic in-process coordinators/nodes retain coverage of every operation,
all routing modes, overrides, unsigned/no-hint shard selection, failover, bounded
reroutes, discovery changes, ownership, cancellation and no replay after progress.
This does not establish live replication or migration correctness.

**Full functional-parity acceptance remains incomplete until a specifically
identified LZ4-compatible server build is supplied and both live interoperability
directions pass.** Offline raw-LZ4 codec interoperability is verified separately;
the GZIP-validated pinned server does not establish LZ4 server interoperability.

## Deliberate Go correctness adaptations

Go preserves context-first signatures, typed errors/results, unsigned protocol
values, optional-field presence, named removal keys/values and binary entry slices.
It preflights complete serialized requests, preserves chunk ordering and one
expiration, aggregates counts with overflow checks, and returns acknowledged partial
progress without replay. One context bounds discovery, encoding, chunks and any
permitted fallback. Owned/borrowed channels and immutable topology snapshots keep
concurrent lifecycle behavior explicit. These remain contract-preserving improvements;
Java estimates, mutable state and partial-error behavior are not reproduced.

---

The observations below apply only to their explicitly listed historical pins.

## Historical validation: previous Java / 26.35 pair

Baseline: Java `jdk-16` at `fd2f89d56751b000a62565ef3ff0712040d5bbfd`. Implementation targets Go 1.26 and covers the direct and smart client interfaces, all convenience overloads, and metadata-preserving collection equivalents. Jedis and client implementation of server-to-server replication are excluded. No CI configuration was added.

## Contract inventory

[API_PARITY.md](API_PARITY.md) maps **276 Java interface declarations/overloads** to Go methods and tests. [interface.go](interface.go) defines the shared `Operations` interface. `TestPublicContractCoverage` enforces that every operation has an entry in the protocol, smart dispatch and live matrices; both clients satisfy the interface at compile time.

The Go method set consolidates Java string/byte, hint, ID, timeout and TTL overloads. Defaults remain ID zero, timeout one second, and no expiration. Java futures become context-first synchronous Go calls; callers use goroutines. Byte-key maps become entry slices. Results retain optional metadata rather than coercing absent fields to zero.

### Schema comparison

The image is `alexaborisov/fastcache-standalone-noavx512:26.35`, pinned to:

```text
sha256:557567683c2026cc4454fea63e65a8ec7ecaa18abcfc7ce1f77f118d86424d86
```

Extracted `/app/cache.proto` was compared with Java's schema before implementation. Descriptor comparison using [tools/schema](tools/schema/main.go) confirmed:

```text
All 41 Java messages, 4 enums and 44 client RPCs match the server descriptors.
Server-only message: ReplicationRequest
Server-only message: ReplicationResponse
```

The server also adds the bidirectional `DataReplicationService.streamReplication` method. Four client RPCs appear in a different declaration order, which has no wire effect. There are no substantive client-facing schema conflicts. Committed schemas use the Java baseline with only Go package options added.

## Corrected Java defects and deliberate departures

All Java source links below are pinned to the authoritative commit, not a moving branch.

| Evidence in Java | Go behavior and regression coverage |
| --- | --- |
| [createMap/createOrderedMap](https://github.com/hurricache/hurricache-java-client/blob/fd2f89d56751b000a62565ef3ff0712040d5bbfd/src/main/java/com/hurricache/client/FastCacheAsyncSimpleClient.java#L317) break at the size limit and return without sending remaining pairs. | Creation sends all pairs in bounded requests. `TestBatchOrderingPairsAndCounts`, `TestLiveCompressionAndBatchRoundTrip/map_chunks`, `TestLiveChunkEquivalence/orderedMap`. |
| [createUnorderedContainer](https://github.com/hurricache/hurricache-java-client/blob/fd2f89d56751b000a62565ef3ff0712040d5bbfd/src/main/java/com/hurricache/client/FastCacheAsyncSimpleClient.java#L218) calls setKey(builder) before adding its hint to that builder. The request's built key can therefore omit the hint. | Hint is attached before the final request. `TestOperationContract/CreateQueue`, `CreateList`, `CreateVector`, `CreateSet`. |
| [ordered range](https://github.com/hurricache/hurricache-java-client/blob/fd2f89d56751b000a62565ef3ff0712040d5bbfd/src/main/java/com/hurricache/client/FastCacheAsyncSimpleClient.java#L486) accepts reverse but never sets the protobuf field. | Reverse is transmitted, with server behavior documented separately. `TestOperationContract/StreamElementInRangeOrderedSet`. |
| [map additions](https://github.com/hurricache/hurricache-java-client/blob/fd2f89d56751b000a62565ef3ff0712040d5bbfd/src/main/java/com/hurricache/client/FastCacheAsyncSimpleClient.java#L1130) set value size from key length and silently truncate mismatched lists with Math.min. | Entry slices keep pairs intact; size derives from actual value bytes. Unordered map insertion uses the same compression and key-ID rules as creation. `TestOperationContract/AddElementHashMap`, `AddElementOrderedMap`, live matrix and pair tests. |
| [StreamBatchMapObserver](https://github.com/hurricache/hurricache-java-client/blob/fd2f89d56751b000a62565ef3ff0712040d5bbfd/src/main/java/com/hurricache/utils/StreamBatchMapObserver.java#L15) copies compressed bytes directly and truncates unequal key/value counts. [Ordered map observer](https://github.com/hurricache/hurricache-java-client/blob/fd2f89d56751b000a62565ef3ff0712040d5bbfd/src/main/java/com/hurricache/utils/StreamBatchOrderedMapObserver.java#L27) serializes KeyBinaryPayload instead of returning its payload bytes. | Both map types decode binary bytes transparently, preserve order/metadata and reject malformed pairs. `TestStreamPrefixFailureAndCompression`, `TestCompressionPresenceAndMalformed`, ordered-map matrix and Java-to-Go map interoperability. |
| [sendTailInChunks](https://github.com/hurricache/hurricache-java-client/blob/fd2f89d56751b000a62565ef3ff0712040d5bbfd/src/main/java/com/hurricache/client/FastCacheAsyncSimpleClient.java#L865) can recurse without consuming an oversized item. Size estimates omit repeated-field framing and some headers. | Exact proto.Size accounting, preflight of every item, ResourceExhausted for an oversized item, bounded sequential chunks. `TestOversizedPreflightAndPartialFailure`, `TestExactSerializedLimit`, `TestLiveDefaultBatchLimit`. |
| [sendAddRequestInChunks](https://github.com/hurricache/hurricache-java-client/blob/fd2f89d56751b000a62565ef3ff0712040d5bbfd/src/main/java/com/hurricache/client/FastCacheAsyncSimpleClient.java#L936) advances the -1 sentinel between set chunks. The integer overload returns the final chunk length instead of an aggregate server result. | Preserve the uint32 sentinel for unpositioned set additions; positional requests advance correctly; sum server counts. `TestBatchOrderingPairsAndCounts`. |
| [Java chunk helpers](https://github.com/hurricache/hurricache-java-client/blob/fd2f89d56751b000a62565ef3ff0712040d5bbfd/src/main/java/com/hurricache/client/FastCacheAsyncSimpleClient.java#L827) use sleeps, recursive continuations and a fresh timeout for each request; failures can conceal partial progress. | One overall context bounds work. PartialError preserves acknowledged chunks/items and underlying status/trailers. A completed prefix is never replayed. `TestBatchDeadlineBoundsAllChunks`, `TestSmartReadinessCancellationMissingRoutesAndPartialNoRetry`. |
| [removeFromContainer overloads](https://github.com/hurricache/hurricache-java-client/blob/fd2f89d56751b000a62565ef3ff0712040d5bbfd/src/main/java/com/hurricache/client/intf/HurriCacheClientMapBased.java#L98) expose inconsistent key/value argument order. | Named Removal.Keys/Values eliminate positional ambiguity; single-key removal is RemoveContainerKey. Protocol/live removal matrix. |
| [CompressionUtils](https://github.com/hurricache/hurricache-java-client/blob/fd2f89d56751b000a62565ef3ff0712040d5bbfd/src/main/java/com/hurricache/utils/CompressionUtils.java#L69) reads default protobuf instances for missing values and ignores decoded-length mismatches. [Key-hint mapping](https://github.com/hurricache/hurricache-java-client/blob/fd2f89d56751b000a62565ef3ff0712040d5bbfd/src/main/java/com/hurricache/client/FastCacheAsyncSimpleClient.java#L144) uses getters that erase optional-field absence. | Empty and absent values are distinct; optional hashes stay optional. Compressed sizes are validated and allocations bounded. `TestCompressionPresenceAndMalformed`, `TestOptionPresenceDefaultsAndIntegerValidation`. |
| [setMode](https://github.com/hurricache/hurricache-java-client/blob/fd2f89d56751b000a62565ef3ff0712040d5bbfd/src/main/java/com/hurricache/client/FastCacheAsyncSmartClient.java#L665) writes currentModeOverride, but [execute](https://github.com/hurricache/hurricache-java-client/blob/fd2f89d56751b000a62565ef3ff0712040d5bbfd/src/main/java/com/hurricache/client/FastCacheAsyncSmartClient.java#L254) never reads it. | Immutable configured mode and per-call Options.Mode implement an effective override without goroutine-local mutable state. `TestSmartUnsignedNoHintLBAndOverrides`. |
| [topology replacement](https://github.com/hurricache/hurricache-java-client/blob/fd2f89d56751b000a62565ef3ff0712040d5bbfd/src/main/java/com/hurricache/client/FastCacheAsyncSmartClient.java#L145) closes removed channels while calls may still use them; readiness can outlive caller deadlines. [shutdown](https://github.com/hurricache/hurricache-java-client/blob/fd2f89d56751b000a62565ef3ff0712040d5bbfd/src/main/java/com/hurricache/client/FastCacheAsyncSimpleClient.java#L1206) also closes borrowed channels. | Immutable snapshots pin endpoints until calls finish. Context-bound readiness/discovery, idempotent Close, explicit owned/borrowed channels. `TestSmartDiscoveryRotationTopologyAndOwnership`, `TestErrorsCancellationAndBorrowedClose`, `TestTLSOwnedConnectionAndConcurrentClose`. |

Other intentional API adaptations: Go uint32/uint64 preserve the full unsigned protocol range; invalid negative durations and insertion overflow fail explicitly; TLS and configurable receive/decode limits are supported. All collection insert/remove families use chunking, including families for which Java sends one unbounded request. Stream errors return a decoded prefix with an error. Ordered input keys/values remain uncompressed, matching Java.

Smart operation-specific policies match Java, including configured-mode scalar create/update/remove and container creation. Only explicit per-call Mode overrides that policy. Rerouting and Unavailable fallback remain bounded to one additional attempt, and mutation ambiguity after a transport failure is not hidden.

## Live server findings: 26.35

These findings are **server limitations**, not corrected client return values.

| Finding | Reproduction / evidence | Client behavior |
| --- | --- | --- |
| Map update changes stored bytes but reports result=false. | Create map a=one, update a=replacement: previous=one, result=false, readback=replacement. `TestLiveServerObservations` and `TestLiveOperationMatrix/UpdateContainerValue`. | Return Updated=false and Previous=one independently. No retry. |
| Reverse weighted range is incorrect. | Ordered set weights [1,2,3], raw reverse=true with bounds [1,2] returns []; even reversed raw bounds [2,1] return [3,2]. Raw [0,4] and [4,0] return []. `TestLiveServerObservations`; protocol test proves Reverse is sent. | Preserve server result. Normal Go ranges use ascending bounds; recommend ascending queries on this image. |
| Queue getContainer is unsupported. | CreateQueue succeeds, getContainer returns Internal: Key not found or type is not correct. Queue push/pop and multi-chunk queue creation succeed. | Propagate Internal for the extra StreamQueue convenience; ordinary Java queue operations work. |
| Compressed long keys crash the server. | A key consisting of a short unique prefix plus 2,048 x bytes, with a 4,096-byte value, crashes createKeyValue. Reproduced separately with Go and authoritative Java CompressionUtils/client; Docker State.ExitCode=11, OOMKilled=false, logs contain CRASH DETECTED. | Keep Java-compatible encoding. Document the restriction; ordinary suite uses shorter keys. The isolated Java `Interop probe-longkey` action is retained for reproduction. |

Head insertion was checked carefully: one RPC inserting [a,b,c] produces [c,b,a,...]. Forward chunk traversal preserves this reversal. Relative-after insertion requires reverse chunk traversal because each RPC preserves its internal order. `TestLiveChunkEquivalence` verifies one-request versus chunked behavior for head, tail, position, before/after, queues, sets, ordered sets and ordered maps.

## Verification results

Verified on 2026-09-06 with Windows Go **1.26.5**, Docker Linux, and JDK **17.0.3.1** compiling the pinned Java source. Go module language version is 1.26.0.

| Check | Actual result |
| --- | --- |
| `go test ./...` | Passed; standalone tests skip when HURRICACHE_TEST_TARGET is absent. |
| Full suite with HURRICACHE_TEST_TARGET set to the pinned image | **287 test/subtest/example pass events, zero test failures, zero skipped tests**. Four helper/generated packages contain no tests. Includes every operation and explicit assertions for the server defects above. |
| Default 3.5 MiB batching | Passed with five 1 MiB incompressible vector items and complete decoded readback. |
| `go vet ./...` | Passed. |
| Final Linux `go test -race ./...` | Passed after all client and test changes (package result: 2.305s). |
| `gofmt -l .` | No output. |
| Pinned generator verification | All four generated Go binding files reproduced byte-for-byte. |
| Schema descriptors | 41 messages, four enums, 44 client RPCs match. |
| External consumer | Separate module with local replace successfully built using Client, SmartClient and Operations. |
| Java → Go and Go → Java | Passed: empty, 1,023-byte, 1,024-byte, compressed and incompressible 4,096-byte values; transferred unsigned hints; lists, ordered sets including uint64 maximum weights, map values and collection additions. All normal fixtures cleaned up. |
| Java authoritative checkout | Remained clean at the pinned commit. Harness sources/classes and generated Java bindings live outside it. |

Race verification uses `golang:1.26.5` on Linux because the Windows host has no C compiler/cgo configuration. The toolchain image resolved to `sha256:705e964a93a2fd2e75c7d59bb7d781b57e30f12293ffde5175c69229e18fb678`. The final Linux `go test -race ./...` passed. Live tests were run separately on Windows; the Linux race run exercises the in-process protocol and routing suite.

Smart cluster behavior is verified with real in-process gRPC coordinators and nodes, covering every operation's dispatch, all modes, unsigned/no-hint selection, overrides, coordinator failure, periodic refresh, topology changes, readiness timeout, known/unknown/self reroutes, missing shard routes, bounded fallback, partial operations, concurrent calls, and owned/borrowed cleanup. A live replicated cluster was not deployed; standalone tests do not establish replication or migration correctness.

Runtime dependencies: grpc-go v1.83.0, protobuf v1.36.11, pierrec/lz4/v4 v4.1.26. Go generators: protobuf v1.36.11 and grpc v1.6.2 with protoc 34.1. Java interoperability regenerated bindings with protoc 34.1/grpc-java generator 1.80.0 and resolved runtime dependencies from the pinned POM (gRPC 1.82.1).
