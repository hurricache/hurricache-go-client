"""Generate/check full Java parity from immutable Git objects; --check never writes."""

import argparse
import json
import re
import subprocess
from pathlib import Path

BASELINE = "0a2b1d851405744e9390bcf5b5e8592177b121e4"
ROOT = Path(__file__).resolve().parents[1]
JAVA_ROOT = "src/main/java/com/hurricache/"
# This routing parser also sees private generic helpers. Public inventory uses
# unrestricted return types and top-level depth, so unknown APIs cannot disappear.
DECLARATION = re.compile(
    r"(?:default\s+)?(CompletableFuture<.*?>|Duration|int|String|void|byte\[\])\s+(\w+)\s*\((.*?)\)",
    re.DOTALL,
)
PUBLIC = re.compile(
    r"(?m)^[ \t]*(?P<mods>(?:(?:public|default|static|final|synchronized)\s+)*)(?P<result>[\w.\[\]]+(?:<[^;{}()]+>)?)\s+(?P<name>\w+)\s*\((?P<args>[^()]*)\)\s*(?:throws\s+[^;{]+)?(?=[;{])"
)
RENAMES = {
    "getDefaultClientId": "DefaultClientID",
    "getDefaultTimeout": "DefaultTimeout",
    "getDefaultTtl": "DefaultTTL",
    "getDefaultCompressionThreshold": "DefaultCompressionThreshold",
    "getTarget": "Target",
    "shutdown": "Close",
    "getTtl": "GetTTL",
    "setTtl": "SetTTL",
    "serializeKey": "UTF8",
    "getReadyFlag": "IsReady",
    "setMode": "Options.Mode",
    "clearMode": "Options.Mode",
}


def strip_comments(source):
    return re.sub(
        r"/\*.*?\*/|//[^\n]*",
        lambda m: re.sub(r"[^\n]", " ", m[0]),
        source,
        flags=re.DOTALL,
    )


def bodies(source):
    """Read balanced method bodies, including lambdas and chunking helpers."""
    source = strip_comments(source)
    result = {}
    for m in DECLARATION.finditer(source):
        pos = m.end()
        while pos < len(source) and source[pos].isspace():
            pos += 1
        if pos == len(source) or source[pos] != "{":
            continue
        start, depth = pos + 1, 1
        pos += 1
        while depth:
            if source[pos] == "{":
                depth += 1
            elif source[pos] == "}":
                depth -= 1
            pos += 1
        result.setdefault(m[2], []).append(source[start : pos - 1])
    return result


def resolve_policy(name, methods, visiting=frozenset()):
    if name in visiting:
        return set()
    policies = set()
    for body in methods.get(name, []):
        calls = set(re.findall(r"(?<![\w.])(\w+)\s*\(", body))
        # Treat execute/executeWrite as terminals: executeWrite delegates to execute.
        if "executeWrite" in calls:
            policies.add("MasterThenBackup")
        elif "execute" in calls:
            policies.add("configured mode")
        else:
            for call in calls & methods.keys():
                policies |= resolve_policy(call, methods, visiting | {name})
    return policies


def declarations(source, interface=False):
    source = strip_comments(source)
    masked = re.sub(
        r'"(?:\\.|[^"\\])*"' + r"|'(?:\\.|[^'\\])*'", lambda m: " " * len(m[0]), source
    )
    for match in PUBLIC.finditer(source):
        prefix = masked[: match.start()]
        if prefix.count("{") - prefix.count("}") == 1 and (
            interface or "public" in match["mods"].split()
        ):
            yield match, source.count("\n", 0, match.start()) + 1


def mapping(match):
    name, args = match["name"], match["args"]
    if name == "update" and "OrderedPayload elementKey" in args:
        return "UpdateOrder"
    if name == "containsKey" and "elementKey" in args:
        return "ContainsInContainer"
    if name == "remove" and "elementKey" in args:
        return "RemoveInContainer"
    return RENAMES.get(name, name[:1].upper() + name[1:])


PARAMETERS = {
    "hint": "Options.Hint",
    "keyHint": "Options.Hint",
    "clientId": "Options.ClientID",
    "timeout": "Options.Timeout",
    "ttl": "Options.TTL",
    "initialValue": "data",
    "data": "data",
    "container_keys": "data",
    "container_values": "data",
    "new_order": "newOrder",
    "newOrder": "newOrder",
    "expectedValue": "expected",
    "newValue": "newValue",
    "value": "value",
    "key": "key",
    "elementKey": "elementKey",
    "mask": "value",
    "delta": "value",
    "containerKey": "selector",
    "orderedKey": "selector",
    "pos": "pos",
    "start": "pos.Start",
    "end": "pos.End",
    "endPos": "pos.End",
    "startWeight": "start",
    "endWeight": "end",
    "reverse": "reverse",
    "containerType": "pos.Type",
    "type": "kind",
    "duration": "duration",
    "keys": "keys",
    "values": "values",
    "pivot": "pivot",
    "mode": "Options.Mode",
}


def argument_mapping(match, target, parameters, manifest):
    result = []
    for part in re.split(r",(?![^<]*>)", match["args"]):
        if not part.strip():
            continue
        name = part.split()[-1]
        if name not in PARAMETERS:
            raise ValueError(f"unmapped parameter {match['name']}({name})")
        mapped = PARAMETERS[name]
        if target == "SetTTL" and name == "ttl":
            mapped = "ttl"
        if target.endswith("InContainer") and name in ("containerKey", "pos"):
            mapped = "selector"
        if target == "IncrementOrder" and name == "delta":
            mapped = "delta"
        if target == "RemoveFromContainerByKey" and name in ("containerKey", "keys"):
            mapped = "containerKey"
        if target == "AddElementWithWeight" and name == "keys":
            mapped = "data"
        if name == "values" and "data" in parameters.get(target, set()):
            mapped = "data"
        if "Payload pos" in part or "Variant pos" in part:
            mapped = "pivot"
        if target == "GetRange" and name == "containerType":
            mapped = "pos.Type"
        if target == "GetRange" and name in ("start", "end"):
            mapped = "pos." + ("Start" if name == "start" else "End")
        if "InRange" in target and name in ("start", "end"):
            mapped = name
        if target == "RemoveAt" and name == "pos":
            mapped = "pos.Start"
        if target == "RemoveFromContainerByKeyAndValue" and name == "type":
            result.append("type: ignored by Java convenience overload")
            continue
        if mapped.startswith("Options."):
            if mapped not in manifest["Methods"]:
                raise ValueError(f"nonexistent Go option {mapped}")
        elif mapped.split(".")[0] not in parameters.get(target, set()):
            raise ValueError(
                f"unmapped parameter {match['name']}({name}) -> {target}.{mapped}"
            )
        result.append(
            f"{name} -> {mapped}"
            + (
                " (paired entry slice)"
                if name in ("container_keys", "container_values", "initialValue")
                and ("Map" in match["name"] or "Map<" in part)
                else ""
            )
        )
    return "; ".join(result) or "none"


MODEL_MEMBERS = {
    "ofBytes": "Bytes",
    "ofLong": "Int",
    "ofInt": "Int",
    "ofDouble": "Double",
    "none": "None",
    "type": "Type",
    "isNone": "IsNone",
    "isBytes": "IsBytes",
    "isInt": "IsInt",
    "isDouble": "IsDouble",
    "bytesValue": "Bytes",
    "requireBytes": "Bytes",
    "longValue": "Int64",
    "intValue": "Int64",
    "requireLong": "Int64",
    "doubleValue": "Float64",
    "requireDouble": "Float64",
    "numberValue": "Number",
    "variant": "Value",
    "getOrder": "Order",
    "hasOrder": "Order",
}


def model_mapping(owner, match, manifest):
    name = match["name"]
    known_parameters = {
        "value",
        "bytesValue",
        "longValue",
        "intValue",
        "doubleValue",
        "variant",
        "order",
        "o",
        "strong_hash",
        "week_hash",
        "wasRelayed",
    }
    for part in re.split(r",(?![^<]*>)", match["args"]):
        if part.strip() and part.split()[-1] not in known_parameters:
            raise ValueError(f"unmapped model parameter {owner}.{name}({part})")
    if name in ("equals", "hashCode", "toString"):
        if owner == "KeyHintData" and name == "equals":
            target = "KeyHint.Equal"
        elif owner in ("Payload", "OrderedPayload") and name == "equals":
            target = owner + ".Equal"
        elif name == "hashCode" and owner in (
            "Payload",
            "OrderedPayload",
            "KeyHintData",
        ):
            return (
                "entry slices / Equal (no JVM hashCode)",
                "byte keys retain optional order; hints compare hashes only; no byte-key Go map is required",
            )
        else:
            return (
                "native == / map key / fmt",
                "type-sensitive equality; canonical NaNs and distinct signed zeros for variants",
            )
    elif owner == "OrderedVariant" and name in ("of", "ofBytes", "ofInt", "ofDouble"):
        target = "Ordered"
    elif owner in ("Payload", "OrderedPayload"):
        if name == "of":
            target = (
                "OrderedPayload.Data" if owner == "OrderedPayload" else "Payload.Data"
            )
        elif name == "getValue":
            target = "Payload.Data"
        elif name == "getOrder":
            target = "OrderedPayload.Order"
        else:
            raise ValueError(f"unmapped model declaration {owner}.{name}")
    elif owner == "KeyHintData":
        fields = {
            "of": "WeakHash",
            "getWeek_hash": "WeakHash",
            "getStrong_hash": "StrongHash",
            "hasWeekHash": "WeakHash",
            "hasStrongHash": "StrongHash",
            "isWasRelayed": "WasRelayed",
        }
        if name not in fields:
            raise ValueError(f"unmapped model declaration {owner}.{name}")
        target = "KeyHint." + fields[name]
    else:
        if name not in MODEL_MEMBERS:
            raise ValueError(f"unmapped model declaration {owner}.{name}")
        member = MODEL_MEMBERS[name]
        target = (
            member
            if name in ("ofBytes", "ofLong", "ofInt", "ofDouble", "none")
            else owner + "." + member
        )
    if target not in manifest["Methods"]:
        raise ValueError(f"nonexistent model equivalent {target}")
    return (
        target,
        "immutable comparable variants; checked accessors; byte-key payload structs; optional order/hash fields",
    )


def generate(checkout):
    def git(*args):
        return subprocess.check_output(
            ["git", "-C", str(checkout), *args], text=True, encoding="utf-8"
        )

    git("cat-file", "-e", BASELINE + "^{commit}")
    paths = git("ls-tree", "-r", "--name-only", BASELINE, "src/main/java").splitlines()
    sources = {
        p: git("show", BASELINE + ":" + p)
        for p in paths
        if (
            re.search(r"/HurriCacheClient[^/]*\.java$", p)
            or re.search(r"/FastCacheAsync[^/]*Client\.java$", p)
            or p.rsplit("/", 1)[-1]
            in {
                x + ".java"
                for x in (
                    "Variant",
                    "OrderedVariant",
                    "Payload",
                    "OrderedPayload",
                    "KeyHintData",
                    "Jedis",
                    "JedisPool",
                )
            }
        )
    }
    for name in ("cache.proto", "coordinator.proto"):
        actual = re.sub(
            r"^option go_package = .*;\n",
            "",
            (ROOT / "proto" / name).read_text(encoding="utf-8"),
            flags=re.MULTILINE,
        ).strip()
        if actual != git("show", BASELINE + ":src/main/proto/" + name).strip():
            raise ValueError(f"schema drift: {name}")
    output = subprocess.check_output(
        ["go", "test", "-count=1", "-run", "^TestPublicContractCoverage$", "-v", "."],
        cwd=ROOT,
        text=True,
        encoding="utf-8",
    )
    match = re.search(r"PARITY_CONTRACT (\{[^\n]+\})", output)
    if not match:
        raise ValueError("missing reflected Go contract manifest")
    manifest = json.loads(match[1])
    cases, configured = set(manifest["Cases"]), set(manifest["Configured"])
    parameters = {}
    for path in ROOT.glob("*.go"):
        if path.name.endswith("_test.go"):
            continue
        for m in re.finditer(
            r"func\s*(?:\([^)]*\)\s*)?(\w+)\(([^)]*)\)",
            path.read_text(encoding="utf-8"),
        ):
            parameters[m[1]] = set(re.findall(r"\b\w+\b", m[2]))
    policies = {}
    smart_targets = {}
    for path, source in sources.items():
        if "HurriCacheClient" in path or path.endswith(
            "FastCacheAsyncSmartClient.java"
        ):
            for name, variants in bodies(source).items():
                policies.setdefault(name, []).extend(variants)
        if path.endswith("FastCacheAsyncSmartClient.java"):
            cleaned = strip_comments(source)
            for declaration, _ in declarations(source):
                pos = declaration.end()
                while pos < len(cleaned) and cleaned[pos].isspace():
                    pos += 1
                if pos == len(cleaned) or cleaned[pos] != "{":
                    continue
                start = pos + 1
                depth = 1
                pos += 1
                while depth:
                    if cleaned[pos] == "{":
                        depth += 1
                    elif cleaned[pos] == "}":
                        depth -= 1
                    pos += 1
                smart_targets.setdefault(mapping(declaration), []).append(
                    cleaned[start : pos - 1]
                )
    groups = {
        "Interfaces": [],
        "Concrete wrappers": [],
        "Typed models": [],
        "Active Jedis adapters": [],
    }
    for path, source in sorted(sources.items()):
        owner = Path(path).stem
        group = (
            "Interfaces"
            if owner.startswith("HurriCacheClient")
            else "Concrete wrappers"
            if owner.startswith("FastCacheAsync")
            else "Active Jedis adapters"
            if owner.startswith("Jedis")
            else "Typed models"
        )
        declarations_here = list(declarations(source, interface=group == "Interfaces"))
        # Constructors are separate declarations, with explicit option/ownership mappings.
        constructors = list(
            re.finditer(
                r"public\s+" + owner + r"\s*\(([^()]*)\)", strip_comments(source)
            )
        )
        for constructor in constructors:
            target = (
                "NewSmartClient"
                if "SmartClient" in owner
                else "NewJedisPool"
                if owner == "JedisPool"
                else "NewJedis"
                if owner == "Jedis"
                else "Ordered"
                if owner == "OrderedVariant"
                else "NewClient"
            )
            if owner in ("Payload", "OrderedPayload"):
                target = owner + ".Data"
            constructor_map = {
                "host": "target",
                "port": "target",
                "clientId": "Config.ClientID",
                "defaultClientId": "Config.ClientID",
                "timeout": "Config.Timeout",
                "duration": "Config.Timeout",
                "defaultCompressionThreshold": "Config.CompressionThreshold",
                "coordinatorAddresses": "coordinators",
                "coordinatorHost": "coordinators",
                "coordinatorPort": "coordinators",
                "cacheClient": "client",
                "delegate": "integrated exact-size batching",
                "channel": "NewClientWithConnection",
                "ownedChannel": "Connection.Close ownership",
                "variant": "value",
                "order": "order",
            }
            args = []
            constructor_map["value"] = "Data"
            for part in re.split(r",(?![^<]*>)", constructor[1]):
                if not part.strip():
                    continue
                name = part.split()[-1]
                if name not in constructor_map:
                    raise ValueError(f"unmapped constructor parameter {owner}({name})")
                args.append(f"{name} -> {constructor_map[name]}")
            line = source.count("\n", 0, constructor.start()) + 1
            url = f"https://github.com/hurricache/hurricache-java-client/blob/{BASELINE}/{path}#L{line}"
            groups[group].append(
                f"| [{' '.join(constructor[0].split())}]({url}) | `{target}` | {'; '.join(args)} | owned/borrowed client or shared wrapper lifecycle | local | TestPublicContractCoverage; TestJedisSharedLifecycle |"
            )
        for match, line in declarations_here:
            operation = "CompletableFuture" in match["result"]
            policy = "local / lifecycle"
            if group == "Typed models":
                target, adaptation = model_mapping(owner, match, manifest)
                model_arguments = {
                    "value": "value / Payload.Data",
                    "bytesValue": "value",
                    "intValue": "value",
                    "longValue": "value",
                    "doubleValue": "value",
                    "variant": "Ordered.value",
                    "order": "order pointer (nil retains absence)",
                    "o": "other value",
                    "strong_hash": "KeyHint.StrongHash",
                    "week_hash": "KeyHint.WeakHash",
                    "wasRelayed": "KeyHint.WasRelayed",
                }
                args = (
                    "; ".join(
                        f"{part.split()[-1]} -> {model_arguments[part.split()[-1]]}"
                        for part in re.split(r",(?![^<]*>)", match["args"])
                        if part.strip()
                    )
                    or "none"
                )
                evidence = "TestVariantSemantics; TestTypedCodecPresence"
            elif group == "Active Jedis adapters":
                members = {
                    "close": "Close",
                    "ping": "Ping",
                    "getResource": "GetResource",
                }
                if match["name"] not in members or match["args"].strip():
                    raise ValueError(f"unmapped active Jedis declaration {match[0]}")
                target = owner + "." + members[match["name"]]
                args = "none"
                adaptation = "shared client lifecycle; local PONG"
                evidence = "TestJedisSharedLifecycle"
                if target not in manifest["Methods"]:
                    raise ValueError(f"nonexistent Go equivalent {target}")
            else:
                target = mapping(match)
                if match["name"] in ("toString", "equals", "hashCode"):
                    target = "fmt"
                if target not in manifest["Methods"] and target not in ("UTF8", "fmt"):
                    raise ValueError(
                        f"unmapped declaration / nonexistent Go equivalent: {match[0]}"
                    )
                if target == "fmt":
                    if match["args"].strip() not in ("", "Object o"):
                        raise ValueError(
                            f"unmapped representation parameter {match[0]}"
                        )
                    args = "native pointer/value representation and equality"
                else:
                    args = (
                        "key -> []byte(text), UTF-8"
                        if target == "UTF8"
                        else argument_mapping(match, target, parameters, manifest)
                    )
                adaptation = (
                    "[]byte"
                    if target == "UTF8"
                    else "native Go representation"
                    if target == "fmt"
                    else manifest["Methods"][target]
                )
                evidence = "TestPublicContractCoverage; lifecycle/default tests"
                if operation:
                    if target not in cases:
                        raise ValueError(
                            f"nonexistent operation coverage case: {target}"
                        )
                    scoped = dict(policies)
                    if target in smart_targets:
                        scoped[match["name"]] = smart_targets[target]
                    resolved = resolve_policy(match["name"], scoped)
                    if len(resolved) != 1:
                        raise ValueError(
                            f"unresolved/ambiguous routing {match['name']}: {resolved}"
                        )
                    policy = resolved.pop()
                    want = (
                        "configured mode"
                        if target in configured
                        else "MasterThenBackup"
                    )
                    if policy != want:
                        raise ValueError(
                            f"smart dispatch differs for {target}: Java {policy}, Go {want}"
                        )
                    evidence = f"TestOperationContract/{target}; TestSmartEveryOperationDispatch/{target}; TestTypedScalarsAndCollectionsAcrossClients / numeric / order / range tests; live opt-in"
            url = f"https://github.com/hurricache/hurricache-java-client/blob/{BASELINE}/{path}#L{line}"
            groups[group].append(
                f"| [{' '.join(match[0].split())}]({url}) | `{target}` | {args} | `{adaptation}` | {policy} | {evidence} |"
            )
    expected = {
        "Interfaces": 742,
        "Concrete wrappers": 319,
        "Typed models": 68,
        "Active Jedis adapters": 6,
    }
    for group, rows in groups.items():
        if len(rows) != expected[group]:
            raise ValueError(
                f"expected {expected[group]} {group} declarations, found {len(rows)}"
            )
    sections = []
    for title, rows in groups.items():
        sections += [
            f"## {title}",
            "",
            f"{len(rows)} declarations.",
            "",
            "| Java declaration | Go equivalent | Argument mapping | Result adaptation | Smart default | Behavioral evidence |",
            "| --- | --- | --- | --- | --- | --- |",
            *rows,
            "",
        ]
    return "\n".join(
        [
            "# Java / Go API parity",
            "",
            f"Pinned Java commit `{BASELINE}`; **742 interface declarations**.",
            "",
            "Version 0.2.0 is a breaking API/protocol migration. There is no old-server fallback.",
            "Both concurrent Client and SmartClient implement Operations. Java futures become context-first calls; goroutines supply concurrency.",
            "All overload parameters map explicitly; defaults live in Config/Options. Explicit client ID zero is retained.",
            "Java thread-local setMode/clearMode becomes per-operation Options.Mode (nil clears the override), avoiding goroutine-local mutable state.",
            "Variant/OrderedVariant are immutable comparable values; checked accessors return errors. Map<Payload,Variant> becomes []Entry, and ordered maps become []OrderedEntry to preserve server order and duplicate ordered byte keys.",
            "Numeric results are native int64 or float64 in any; integer bitwise results are int64. Scalar reads/replacements are Variant; mutation counts are uint32; CASResult preserves optional observed Variant and nested hints.",
            "Positions/orders use unsigned wire ranges. Optional range bounds use pointers; destructive zero bounds are omitted as in Java. TTL/timeout/lock inputs use time.Duration; GetTTL returns remaining milliseconds or -1. Lock expiration is encoded as absolute Unix seconds.",
            "Create preflights actual serialized messages and maximum continuation hints before sending anything, pairs map elements, transfers returned hints and retains one expiration. Pivot insertion advances the anchor; head chunks stay in input order because the server reverses each request. Live split/single equivalence corrects Java's backward head-chunk traversal. No fallback/reroute replays acknowledged progress, including a destructive batch that fails local decoding.",
            "GZIP is default; raw LZ4 is explicit. Byte values/keys alone use compression; strict length > threshold and 64 MiB inbound/decoded limits remain. Transport compression is independent.",
            "Active Jedis/JedisPool lifecycle and local Ping are included; commented Redis commands and application-level replication are excluded. Replication bindings are regenerated. Live cluster/replication remain deferred; see FINDINGS.md for acceptance status.",
            "",
            *sections,
            "Regenerate: `python tools/parity.py JAVA_CHECKOUT`. Non-writing check: `python tools/parity.py JAVA_CHECKOUT --check`.",
            "",
        ]
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("java_checkout", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    try:
        output = generate(args.java_checkout.resolve())
        path = ROOT / "API_PARITY.md"
        if args.check:
            if not path.exists() or path.read_text(encoding="utf-8") != output:
                raise ValueError("API_PARITY.md is stale; regenerate it")
            print(
                "742 interface, 319 wrapper, 68 model and 6 active Jedis declarations verified; inventory is current."
            )
        else:
            path.write_text(output, encoding="utf-8", newline="\n")
            print("Wrote full validated Java / Go parity inventory.")
    except (ValueError, subprocess.CalledProcessError) as error:
        raise SystemExit(str(error)) from error


if __name__ == "__main__":
    main()
