# Implementation and verification findings

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
