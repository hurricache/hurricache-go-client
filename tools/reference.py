"""Generate or non-destructively check the public Go API reference."""

import argparse
import re
from pathlib import Path

from parity import BASELINE

ROOT = Path(__file__).resolve().parents[1]


def render():
    contract = (ROOT / "interface.go").read_text(encoding="utf-8")
    declarations = []
    for line in contract.splitlines():
        if re.match(r"\s*[A-Z]\w*\(", line):
            declarations.append(line.strip())
    return "\n".join(
        [
            "# Go API reference",
            "",
            f"Version **0.2.0**; Java `{BASELINE}`.",
            "",
            "Client and SmartClient implement the identical Operations interface below. Calls accept context.Context, a byte key and Options; use []byte(text) for UTF-8 strings. Goroutines provide concurrency.",
            "Nil option pointers inherit Config defaults. Options.ClientID=Ptr(uint32(0)) retains explicit zero; Timeout=Ptr(time.Duration(0)) disables the client timeout while respecting caller cancellation. Config defaults: ID 0, timeout 1 second, TTL 0, GZIP, compression threshold 1024 bytes, maximum request 3.5 MiB, inbound/decoded limit 64 MiB. TLS and DialOptions configure transport independently of payload compression.",
            "Options.Mode overrides each operation's Java dispatch policy; nil uses that policy and SmartConfig.Mode. There is no mutable goroutine-local mode. SmartClient adds Ready(context.Context), IsReady() and Refresh(context.Context); owned node/coordinator connections close only when no active operation uses them. NewClientWithConnection borrows its connection.",
            "",
            "## Typed values",
            "",
            "None(), Bytes([]byte), String(string), Int(int64), Double(float64) create immutable comparable Variant values. Zero Variant is NONE. Type(), IsNone(), IsBytes(), IsInt(), IsDouble(), IsNumber() inspect type. Bytes(), Int64(), Float64(), Number() return (value,error), rejecting wrong types. Number() contains int64 or float64. Bytes copy caller input/output; NaNs compare equal and signed double zeros differ. Variant is safe as a Go map key.",
            "Ordered(Variant,*uint64) creates a comparable OrderedVariant; Value() returns its Variant and Order() returns a copy of its optional order. Variant methods are promoted. Missing order remains nil on reads and encodes as zero on writes.",
            "Payload{Data: []byte(...)} and OrderedPayload{Payload: ..., Order: Ptr(uint64(...))} represent byte map keys. Entry and OrderedEntry pair those keys with Variant values; slices preserve stream order and duplicate byte keys at distinct orders. ContainerData contains exactly one of Values, OrderedValues, Entries, OrderedEntries.",
            "",
            "## Operations",
            "",
            "```go",
            "type Operations interface {",
            *["    " + d for d in declarations],
            "}",
            "```",
            "",
            "Create/Get/Replace/Pop use Variant directly. GetOrderedSet and weighted set ranges return []OrderedVariant. Exchange/Add/Sub and their container versions accept int, int64 or float64 and return an int64 or float64 inside any; bitwise calls accept/return int64. Bool operands/selectors are rejected. Container selectors accept []byte/string entry keys or nonnegative int/int64/uint64 positions; exactly one protobuf alternative is set. CASResult.Expected is an optional Variant and Hint comes from the nested response; either double operand promotes both expected/new operands to double.",
            "Mutation counts use uint32, including zero on empty insertion without an RPC. Update returns the previous map-entry Variant. UpdateOrder/IncrementOrder require an OrderedPayload with the exact current order; the server returns bool and enforces arithmetic bounds while preserving the member's value.",
            "Head chunks stay in input order to preserve the server's per-request reversal across batches. Pivot insertions advance the anchor after each chunk. Every serialized request, including the full anchor and continuation hints, is checked before mutation.",
            "CompareAndSetInContainer selects a map field with []byte/string or an exact ordered-map field with OrderedPayload carrying Order. It uses write routing and leaves element TTL absent, matching Java. Positions/bools are rejected. GetQueue/StreamQueue are legacy conveniences unsupported by the server contract; use GetSize, PeekHead/PeekTail and PopHead/PopTail for queues.",
            "Position.Start/End encode unsigned range bounds; OmitStart and nil End represent absence. Destructive ranges omit zero bounds as in Java; named range methods accept *uint64 bounds. RemoveAt sends End only when it exceeds Start. SetTTL uses relative time.Duration; negative clears expiration, zero expires now; GetTTL returns remaining milliseconds or -1. Lock duration encodes absolute Unix seconds, matching Java despite the schema comment. Options.ClientID owns lock/unlock calls.",
            "Every operation has one effective deadline across preprocessing, discovery, all chunks, streaming and one permitted routing fallback. RPCError retains gRPC status and trailers. PartialError reports acknowledged chunks/items; a stream failure returns the decoded prefix and prohibits whole-operation replay. Received destructive batches count as progress even if local decoding fails.",
            "",
            "## Lifecycle adapters",
            "",
            "NewJedis(Operations) and NewJedisPool(Operations) wrap an existing direct/smart client. Jedis.Ping returns local PONG. GetResource returns a new wrapper sharing the client. Closing a resource or pool closes that client. No commented Java Redis commands or application replication API are implemented.",
            "",
            "Deprecated names remain only where argument binding is unambiguous and use current typed/native results. AtomicCreate/AtomicStore/AtomicLoad/AtomicLoadAndDelete are removed: use Create/Replace/Get/Pop with Int or Double. See README.md for migration examples and API_PARITY.md for every Java overload, argument adaptation and routing policy.",
            "",
        ]
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    text = render()
    path = ROOT / "API_REFERENCE.md"
    if args.check:
        if not path.exists() or path.read_text(encoding="utf-8") != text:
            raise SystemExit("API_REFERENCE.md is stale")
        print("Go API reference is current.")
    else:
        path.write_text(text, encoding="utf-8", newline="\n")


if __name__ == "__main__":
    main()
