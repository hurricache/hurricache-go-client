# Java / Go parity findings

## Current alignment check - November 24, 2026

The server is now built from the current source tree (no published image pin) and
includes the fixes for the November 11 typed-value failures. The full Go suite
passes live in four configurations:

| Configuration | Result |
| --- | --- |
| Deterministic suite (no server) | Passed |
| Standalone, direct client (`HURRICACHE_TEST_TARGET`) | Passed |
| Standalone, smart client with in-process discovery (`HURRICACHE_TEST_SMART=1`) | Passed |
| Live 3-node cluster with real coordinators (`HURRICACHE_TEST_SMART=host:port,...`) | Passed, repeated runs stable |

The Python client passes the same standalone and live-cluster gates.

### Server contract clarifications adopted by the clients

- NONE is not storable. A scalar create with NONE is rejected with
  INVALID_ARGUMENT. Container creation drops NONE elements (partial insert):
  the remaining elements are stored and positions after a dropped element shift.
- Set equality is type-sensitive: `Int(0)` and `Double(0)` are distinct entries,
  all NaN payloads merge into one entry, and signed double zeros stay distinct.
- The server supports GZIP payload compression only. Raw LZ4 remains a
  client-side codec verified by offline fixtures; it is excluded from live runs.
- In a cluster a create is accepted by any node and relayed to the owning
  master. A relayed create is fire-and-forget: the response is OK with
  `WasRelayed=true`, validation happens on the owner, and the key becomes
  visible asynchronously (measured at ~50-100 ms). Clients and tests must poll
  for visibility before dependent operations.

### Client fixes in this round

- Smart reroute: the server places the `master[,backup]` route as one
  comma-joined string in the `x-fastcache-route` trailer (and in the status
  description). Both clients now split it and try each target; previously the
  whole string was looked up as a single endpoint and never matched.
- Chunked container creation: the continuation chunks are re-routed through the
  hint returned by the first chunk (matching Java's `createContainerWithChunks`
  with its replication delay), and NotFound continuations are retried with
  backoff until the relayed/replicated create lands on the owner. Previously
  every chunk went to the first chunk's endpoint, which fails with
  FAILED_PRECONDITION/NOT_FOUND whenever chunk 1 was relayed or accepted by a
  backup.
- Server-side: full-width (uint64) weighted lookup/removal in ordered
  containers was truncated to 32 bits in three handlers; fixed, so the maximum
  uint64 weight works.

## Historical evidence before November 24

## Historical alignment check - November 11, 2026

Python and Go 0.2.0 now match current Java `0a2b1d851405744e9390bcf5b5e8592177b121e4`:
**742 interface declarations, 319 concrete wrapper declarations, 68 typed-model
declarations, and 6 active Jedis declarations**. Java advanced from `1aa296e` with
container CAS. Both clients now expose `compare_and_set_in_container` /
`CompareAndSetInContainer` through their direct and smart transports (including
Python native async). No release, tag or client commit was published.

Container CAS uses the existing RPC and the new AtomicCas `keys` oneof:
`element_key` tag 4 for byte/string map fields, `ordered_element_key` tag 5 for
an OrderedPayload carrying an explicit exact order. It validates numeric int64
inputs/selectors before data RPCs, promotes mixed numeric operands to DOUBLE,
leaves element TTL absent as Java does, compresses byte keys only, uses write
routing, and preserves optional observed values/types/hints on failed CAS.
Positions and bools are not valid CAS selectors. Ordinary positional atomics
retain their existing selector behavior. Scalar CAS behavior remains unchanged.

Full queue-content reads are intentionally unsupported by the server contract.
The retained legacy get_queue/stream_queue and GetQueue/StreamQueue conveniences
do not expand that contract. Live tests require full queue reads to fail, then
verify queue cardinality and pop order without losing elements. Continuation
tests drain queues rather than reading their full contents. Queue size is fixed
in the fetched server; no queue-streaming server change was introduced here.

### Verification

| Check | November 11 result |
| --- | --- |
| Python deterministic suite | **1392 passed, 720 skipped**; skips are 496 explicit live opt-ins plus 224 direct-client instances of smart-only routing tests |
| Go deterministic suite, vet, race | Passed; race uses the existing Clang toolchain with bounded concurrency/memory |
| Ruff and mypy | Passed; mypy checks 17 source files |
| Current declaration inventories and API references | Passed; non-writing checks reject unmapped declarations/parameters and stale references |
| Protocol and generated bindings | Cache/coordinator descriptors match Java, Python, Go and server source, ignoring language-specific file options; regeneration is byte-for-byte reproducible |
| Offline Java/Python and Java/Go fixtures | Passed both directions under GZIP and raw LZ4 |
| Distribution checks | Python wheel/sdist build and isolated wheel installations with/without LZ4 passed; isolated archived Go consumer passed both codecs and new container CAS API |
| Live GZIP container CAS | Python **16 passed** across sync/native async/both smart forms; Go direct and smart MAP/ORDERED_MAP cases passed; independent current-Java map/ordered-map probes passed |
| Complete Python GZIP standalone suite | **180 passed, 68 failed**; no expected-failure exceptions or diagnostic filter |
| Complete Go GZIP standalone suites | Both direct and smart runs failed the remaining typed-value server checks; batching, TTL/locks, atomics, container CAS, order updates and destructive ranges passed |
| Independent current-Java API probes, GZIP | **8 passed, 5 failed**; failures reproduce NONE, set type equality, equal-weight NaN ordering, and full-width weighted lookup |
| Full live Java/Python and Java/Go fixture exchange | Attempted unfiltered; blocked at server rejection of NONE creation, so complete live bidirectional acceptance did not pass |
| Explicit live raw LZ4 | Ready server, but compressed keys/container setup fail in Java, Python and Go; setting the environment variable does not make this published image support LZ4 |

### Tested server and remaining failures

At validation start, Docker pull resolved latest to this immutable image, used
throughout both separate codec processes:

```text
alexaborisov/fastcache-standalone-noavx512@sha256:c15f06d8e4ccf3b0f5cafb5039acaaf44fe142459cb8a6fde1fdb5716374fbb2
```

Both containers reached application startup and gRPC readiness. Startup reports
`HC-26.44-5-gd9eeffa-dirty`, built November 10. The fetched server checkout is
`db0112520ee664f00272d94c2954d842d9dbb020` on `redis-connector`; the image's dirty
build label does not establish byte-for-byte correspondence with that checkout.
No patched executable was substituted and no server source was edited in this run.

Remaining failures reproduced by unchanged current Java API calls are:

- Scalar NONE creation: INVALID_ARGUMENT, `Can't insert data. Invalid type`.
- NONE in a collection: INVALID_ARGUMENT, `Incorrect type of object`.
- Integer zero and double zero in a set: one entry returned instead of two.
- Double zero and NaN in an ordered set at the same weight: one entry instead of two.
- The maximum uint64 weighted lookup: NOT_FOUND after successful creation.
- Raw-LZ4 compressed keys: DATA_LOSS, `Cant read key data protocol error`; large
  LZ4 map selectors also fail setup with INVALID_ARGUMENT. This was checked with
  `HURRICACHE_PAYLOAD_COMPRESSION=lz4` on a fresh separate process.

Java GZIP order-change lookup, ordinary vector pops, ordinary weighted read/pop,
compression, lock expiry, and both new container CAS forms pass. Several earlier
server defects are therefore corrected, but the published image still cannot
complete typed client acceptance. The clients do not hide these failures or
encode NONE as empty bytes. Protocol alignment does not imply server behavioral
acceptance. The old Docker startup/WSL outage is historical and not the current
blocker.

Smart live data RPCs use deterministic coordinator discovery pointing to the
actual standalone. Real cluster, failover and replication remain unverified.
Offline fixtures likewise do not complete live acceptance. The Java working
tree was cleanly fast-forwarded to current origin/jdk-16; Java source was not edited.

Exact commands/results, startup logs and image metadata are under
`D:/hurricache/materials/validation/clients-20261111`, including `provenance.json`,
`java-current-api-*.log`, `python-live-full-gzip.log`, and
`go-live-{,smart-}full-gzip.log`. Only task-owned containers were started/stopped;
the user's kind cluster was left running.

## Historical evidence before November 11

## Current migration: November 8, 2026

Target: Java **`1aa296eda50b8363d510c2bc05fe0fab2c5dd8cf`**, unchanged throughout this update. Go's previous baseline was `98225d9365e0ca7cea81e83116ed6f6ddba4294a`. The package now identifies itself as **0.2.0**; no tag or external release was published.

The inventory validates **734 interface declarations**, **307 concrete wrapper declarations**, **68 typed-model declarations**, and **6 active Jedis declarations**, including constructors. It reads immutable Git objects, reflects the compiled Go contract and actual operation-matrix cases, validates every parameter mapping, resolves smart policies through Java helpers and overloads, and rejects unknown declarations/parameters. Both clients implement the same context-first Operations interface; goroutines provide concurrency.

Both schemas were copied from the current pin with only go_package added, and all four generated Go files reproduce byte-for-byte. This is an incompatible protocol migration, with no fallback to old servers: typed BinaryPayload oneofs and value_type, nested CAS ValueResponse, coordinator Endpoint messages, renamed/deletion RPCs and the new range-deletion stream are adopted. Ordered entry updates use ordered_element_key tag 3 and new_order/delta_order alternatives; ordinary map-update values use tag **4**, replacing tag 3 even from the earlier Python draft.

Variant and OrderedVariant are immutable comparable values. NONE, empty bytes, numeric zero, int64 limits, doubles, canonical NaNs, signed zero and nullable order are tested. Scalar and replacement reads return Variant; collections carry typed values; byte map keys retain Payload/OrderedPayload. Entry slices preserve server order and duplicate bytes at distinct orders. Numeric atomics return native int64 or float64 inside any; bitwise operations remain int64. CAS preserves optional observed presence/type and nested hints. Selectors accept byte/string keys or nonnegative integer positions and set exactly one alternative, including position zero. Booleans and unsupported numeric operands are rejected before data RPC execution.

Current Java names include Create/Get/Replace/Pop, collection getters, Peek/Push/Pop/Delete head/tail, positional insertion/removal, typed map updates, scalar/container atomics, and destructive list/vector/ordered-set/map ranges. UpdateOrder and IncrementOrder require exact bytes plus current order, preserve values, and return bool. Counts now use integer results; obsolete AtomicCreate/Store/Load/LoadAndDelete APIs and legacy result envelopes were removed. Unambiguous old names remain deprecated aliases with the new results. The migration table is in README.md and signatures/defaults in API_REFERENCE.md.

All smart policies were checked against Java, including configured-mode scalar mutation and exact-weight pops, write dispatch for container atomics/order updates/destructive ranges, and per-call overrides. IPv6 Endpoint formatting, topology changes, role fallback, retained snapshots, cancellation, connection ownership and partial-progress replay prevention have deterministic coverage. Java's thread-local setMode/clearMode maps to Options.Mode instead of goroutine-local mutable state.

The previous Go safety protections remain: exact serialized-request preflight, paired entries, maximum continuation-hint reservation, returned hints/relay metadata, one creation expiration, checked count aggregation, one operation deadline, context cancellation, no general retries and no replay after acknowledged progress. A received destructive batch is progress even if local decoding fails. Head chunks run backwards; before/after pivot insertions advance the anchor as in the current Java standalone wrapper. Numeric values never enter compression. Both byte representations, GZIP/default and explicit raw LZ4, strict threshold boundaries, malformed data, size metadata and 64 MiB inbound/decoded limits are covered. Transport GZIP remains independent of payload LZ4.

The only active Java Jedis behavior is now included: local PONG, GetResource and shared-client Close. Commented Redis commands and application-level replication remain excluded; replication stubs were regenerated.

| Check | Current result |
| --- | --- |
| Deterministic `go test -count=1 ./...` | Passed: **379 leaf tests/subtests**, with **7 explicit live opt-in skips**. There are no inherited expected server failures. |
| `go vet ./...` and build | Passed. |
| `go test -race -p 1 -count=1 ./...` | Passed with the existing portable Clang toolchain and bounded memory/concurrency settings below. |
| Go formatting and Python tooling Ruff checks | Passed. |
| Protobuf regeneration | Four generated files reproduce byte-for-byte with protoc 34.1, protoc-gen-go 1.36.11 and protoc-gen-go-grpc 1.6.2. |
| Full inventory / API-reference non-writing checks | Passed. Generator regression tests reject unknown APIs/parameters, missing methods/cases, routing mismatch, stale output and accidental check-mode writes. |
| Isolated source-distribution consumer | Passed with GOWORK/GOPROXY disabled; the consumer imports an extracted v0.2.0 source archive and exercises both codecs, typed values, comparable ordered models, both client contracts and Jedis lifecycle. |
| Offline Java -> Go / Go -> Java, GZIP and raw LZ4 | Passed against unchanged current Java sources: typed values, NONE/empty/zero/NaN/signed-zero, nested CAS and zero hints, threshold/incompressible byte fixtures, ordered map pairing/order, compressed ordered selectors and all current update alternatives. |
| Live standalone round trips / bidirectional interoperability | **Blocked by standalone startup failure below. Not passed.** |
| Real cluster / failover / replication | Deferred and unverified. |

Toolchain: Go **1.26.5** on Windows/amd64; JDK **25.0.4.1** for Java compilation; current pinned POM dependencies including protobuf-java 4.34.1, grpc-java 1.82.1 and lz4-java 1.8.0. Go runtime dependencies are unchanged. The Java checkout was left untouched; reference sources, bindings and classes live under ignored .work/java-interop/1aa296eda50b8363d510c2bc05fe0fab2c5dd8cf.

The unrestricted first race run hit Windows ThreadSanitizer allocation error 1455 before completing. The full suite passed with CGO_ENABLED=1, the existing x86_64-w64-mingw32-clang.exe, GOMAXPROCS=2, GORACE=history_size=0, GOMEMLIMIT=128MiB and GOGC=20, using -race -p 1. This was an environment resource mitigation; all operation and concurrency tests still ran.

## Current standalone startup blocker

At live-validation start, `docker pull alexaborisov/fastcache-standalone-noavx512:latest` completed and returned:

```text
alexaborisov/fastcache-standalone-noavx512@sha256:553e630a1cc10de83235eef4486d5e0835c741abc84475f18ec3346e15b94684
```

The exact digest was started as disposable container go-parity-20261108, with a dynamically allocated localhost port. Container ID: `1fb24457bb90e5ca9499e197c00279e5f31730eddb094721c0f829eec72b94c2`. Its recorded state was:

```json
{"Status":"exited","Running":false,"OOMKilled":false,"ExitCode":132,"Error":"","StartedAt":"2026-11-08T11:28:40.439501738Z","FinishedAt":"2026-11-08T11:28:40.954276724Z"}
```

`docker logs --timestamps go-parity-20261108` returned **no application stdout/stderr**. An independent invocation of the server binary, without any Go/Java client, reproduced the failure:

```sh
docker run --rm --entrypoint /bin/sh \
  alexaborisov/fastcache-standalone-noavx512@sha256:553e630a1cc10de83235eef4486d5e0835c741abc84475f18ec3346e15b94684 \
  -c '/app/server-fastcache -config-file /app/cache-config.yaml; result=$?; echo server-exit=$result; exit $result'
```

```text
Illegal instruction (core dumped)
server-exit=132
```

The diagnostic line is shell-generated, not an application startup log. The faulting instruction has not been identified. The task-created stopped container was removed; unrelated containers were untouched. No live acceptance RPCs or interoperability writes were sent to this failed service.

Live acceptance must be rerun only after a pinned server starts and reaches gRPC readiness. The harness compiles current Java API writers and pinned Java wire-converter/codec readers; it checks typed scalars, mixed lists, atomics, compressed top-level/map keys, unsigned order, exact order moves and multi-request lists/maps in both directions. These wire readers do not assert that every Java high-level compressed-collection observer is correct. Run each codec with a matching server configuration and `HURRICACHE_TEST_CODEC=gzip` / `lz4`; the live matrix defaults to both when no codec filter is supplied. Offline fixtures and deterministic services cannot complete this gate.

## Historical record

The record below describes the previous October baseline and server image only. Its passing live results and server limitations are **historical evidence**, not current acceptance or expected failures for this migration.

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
