These protobuf `BatchValueResponse` fixtures were generated from Java commit
`98225d9365e0ca7cea81e83116ed6f6ddba4294a` using its `CompressionUtils`,
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
