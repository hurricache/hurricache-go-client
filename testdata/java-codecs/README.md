These protobuf `BatchValueResponse` fixtures were generated from Java commit
`0a2b1d851405744e9390bcf5b5e8592177b121e4` using its `CompressionUtils`,
lz4-java 1.8.0, and protobuf 4.34.1 on JDK 25.0.4.1.

Each contains fourteen ordered binary key/value pairs: repeated and deterministic
incompressible bytes at lengths 0, 1, 1023, 1024, 1025, 4096 and 16384. The
threshold is 1024; key client ID is explicitly zero. Weights preserve pair identity.

`TestJavaCodecFixtures` decodes the bytes and verifies multi-batch ascending and
descending streams. The offline harness also writes Go-produced protobufs for
Java decoding through the repaired `StreamBatchOrderedMapObserver`. It exercises
scalar and ordered-value decoders and rejects unpaired map responses.

Regenerate and check both directions without a server:

```powershell
./tools/interop.ps1 -JavaCheckout ../hurricache-java-client -JavaHome C:/Java/jdk-25 -Offline
```

The harness reads pinned Git objects regardless of Java checkout HEAD and keeps
all generated sources and classes in `.work/java-interop/`.

The `.typed` siblings contain NONE, empty bytes, integer/double zero, signed
int64 limits, signed double zero, a finite double, compressed bytes and NaN.
The `.cas` siblings contain the current nested ValueResponse with a double
observation and an explicitly present zero weak hash. The `.updates` siblings
contain absolute-zero and signed-delta ordered entry updates plus a plain map
update using the new value tag 4. These cover compressed keys, exact current
order and client ID as well as both update oneofs.

`TestJavaTypedFixtures` checks these Java-produced messages and exports freshly
encoded Go equivalents for Java to read. Both codec directions use actual
pinned Java conversion/codec code. Offline evidence does not establish live
standalone interoperability; that gate is recorded in FINDINGS.md.
