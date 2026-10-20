"""Generate/check overload parity against immutable Java Git objects without checkout edits."""
from pathlib import Path
import argparse
import json
import re
import subprocess

BASELINE = "98225d9365e0ca7cea81e83116ed6f6ddba4294a"
ROOT = Path(__file__).resolve().parents[1]
JAVA_ROOT = "src/main/java/com/hurricache/"
DECLARATION = re.compile(r"(?:default\s+)?(CompletableFuture<.*?>|Duration|int|String|void|byte\[\])\s+(\w+)\s*\((.*?)\)", re.S)
RENAMES = {
    "setTtl": "SetTTL", "getTtl": "GetTTL", "getDefaultClientId": "DefaultClientID",
    "getDefaultTimeout": "DefaultTimeout", "getDefaultTtl": "DefaultTTL",
    "getDefaultCompressionThreshold": "DefaultCompressionThreshold", "getTarget": "Target",
    "shutdown": "Close", "serializeKey": "[]byte(text)",
    "addElementToHead": "AddElementToHeadCount", "addElementToTail": "AddElementToTailCount",
    "addElementToPositionBefore": "AddElementToPositionBeforeCount",
    "addElementToPositionAfter": "AddElementToPositionAfterCount",
}


def strip_comments(source):
    return re.sub(r"/\*.*?\*/|//[^\n]*", lambda m: re.sub(r"[^\n]", " ", m[0]), source, flags=re.S)


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
        result.setdefault(m[2], []).append(source[start:pos - 1])
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


def generate(checkout):
    def git(*args):
        return subprocess.check_output(["git", "-C", str(checkout), *args], text=True, encoding="utf-8")

    git("cat-file", "-e", BASELINE + "^{commit}")
    paths = git("ls-tree", "-r", "--name-only", BASELINE, JAVA_ROOT + "client/intf").splitlines()
    interfaces = {p: git("show", BASELINE + ":" + p) for p in paths if re.search(r"/HurriCacheClient[^/]*\.java$", p)}
    if not interfaces:
        raise ValueError("no Java interfaces found")
    for name in ("cache.proto", "coordinator.proto"):
        actual = (ROOT / "proto" / name).read_text(encoding="utf-8")
        actual = re.sub(r'^option go_package = .*;\n', '', actual, flags=re.M).strip()
        if actual != git("show", BASELINE + ":src/main/proto/" + name).strip():
            raise ValueError(f"schema drift: {name}")
    methods = {}
    for source in interfaces.values():
        for name, variants in bodies(source).items():
            methods.setdefault(name, []).extend(variants)
    methods.update(bodies(git("show", BASELINE + ":" + JAVA_ROOT + "client/FastCacheAsyncSmartClient.java")))
    output = subprocess.check_output(["go", "test", "-count=1", "-run", "^TestPublicContractCoverage$", "-v", "."], cwd=ROOT, text=True, encoding="utf-8")
    match = re.search(r"PARITY_CONTRACT (\{[^\n]+\})", output)
    if match is None:
        raise ValueError("TestPublicContractCoverage did not emit its parity manifest")
    manifest = json.loads(match[1])
    cases, configured = set(manifest["Cases"]), set(manifest["Configured"])
    tests = "\n".join(p.read_text(encoding="utf-8") for p in ROOT.glob("*_test.go"))
    for test in ("TestOperationContract", "TestSmartEveryOperationDispatch", "TestLiveOperationMatrix", "TestPublicContractCoverage"):
        if not re.search(r"func " + test + r"\(", tests):
            raise ValueError(f"nonexistent coverage test: {test}")
    rows = []
    for path, source in sorted(interfaces.items()):
        source = strip_comments(source)
        for m in DECLARATION.finditer(source):
            result, name, params = m.groups()
            go = RENAMES.get(name, name[0].upper() + name[1:])
            if name == "removeFromContainer" and "byte[] elementKey" in params:
                go = "RemoveContainerKey"
            if go != "[]byte(text)" and go not in manifest["Methods"]:
                raise ValueError(f"unmapped declaration / nonexistent Go equivalent: {m[0]}")
            asynchronous = result.startswith("CompletableFuture")
            policy = "n/a"
            if asynchronous:
                if go not in cases:
                    raise ValueError(f"nonexistent operation coverage case: {go}")
                policies = resolve_policy(name, methods)
                if len(policies) != 1:
                    raise ValueError(f"unresolved/ambiguous routing for {name}: {policies}")
                policy = policies.pop()
                want = "configured mode" if go in configured else "MasterThenBackup"
                if policy != want:
                    raise ValueError(f"smart dispatch differs for {go}: Java {policy}, Go test {want}")
            adaptations = ["key = []byte(text)"] if "String key" in params else []
            for param, field in (("hint", "Hint"), ("keyHint", "Hint"), ("clientId", "ClientID"), ("timeout", "Timeout"), ("ttl", "TTL")):
                if re.search(r"\b" + param + r"\b", params):
                    adaptations.append("Options." + field)
            if go == "RemoveFromContainer":
                adaptations.append("Removal.Keys / Values / Type")
            shape = "[]byte" if go == "[]byte(text)" else manifest["Methods"][go].split(") ")[-1]
            line = source.count("\n", 0, m.start()) + 1
            url = f"https://github.com/hurricache/hurricache-java-client/blob/{BASELINE}/{path}#L{line}"
            coverage = (f"TestOperationContract/{go}; TestSmartEveryOperationDispatch/{go}; TestLiveOperationMatrix/{go}" if asynchronous else "TestPublicContractCoverage; examples")
            rows.append(f"| [`{' '.join(m[0].split())}`]({url}) | `{go}` | `{shape}` | {'; '.join(adaptations) or 'defaults / explicit arguments'} | {policy} | `{coverage}` |")
    if len(rows) != 472:
        raise ValueError(f"expected 472 declarations at pinned baseline, found {len(rows)}")
    return "\n".join([
        "# Java / Go API parity", "",
        f"Authoritative baseline: Java commit `{BASELINE}`. Native standalone and smart clients; Jedis is excluded.",
        f"This inventory contains {len(rows)} interface declarations, including every overload.", "",
        "Java futures become context-first synchronous calls with typed results and errors. Omitted overload options inherit immutable client defaults; explicit zero uses pointers.",
        "String keys use UTF-8 byte conversion. Payload factories become struct literals; binary maps use entry slices preserving iteration order.",
        "Options.TTL and Options.Timeout use time.Duration; positions and ordering weights retain the full unsigned protocol range. Lock wire timestamps use absolute Unix seconds; TTL uses absolute Unix milliseconds.",
        "All operations are shared by Client and SmartClient through Operations. Options.Mode overrides the listed policy; SmartConfig.Mode supplies configured mode. Collection creation resolves through Java chunking helpers to MasterThenBackup.",
        "Return shapes preserve optional metadata: ValueResult carries values/hints, UpdateResult carries status and previous value, and AtomicResult/CASResult retain hints. Counts use uint32; boolean compatibility helpers remain available under their original names.",
        "shutdown maps to idempotent Close; target/default accessors and owned/borrowed connection constructors preserve lifecycle semantics. Smart readiness uses Ready/IsReady; Java setMode is represented by Options.Mode or SmartConfig.Mode.",
        "Coverage names are validated against reflected Go methods and actual operation matrix cases. Live coverage is opt-in and does not imply it has run. See FINDINGS.md for results and server limitations.", "",
        "| Java declaration | Go equivalent | Go return shape | Overload mapping | Smart default | Coverage |",
        "| --- | --- | --- | --- | --- | --- |", *rows, "",
        "## Standalone wrapper behavior and compatibility adaptations", "",
        "Collection creation uses one create followed by bounded additions, transfers the returned hint, preserves one expiration, and preflights complete serialized requests including continuation hints. Go retains exact sizing and partial progress instead of Java estimates and fresh per-chunk timeouts.", "",
        "Head/tail/pivot integer results map to Count methods. Count methods sum responses, return zero for empty input and reject overflow. Boolean compatibility helpers retain empty-input success and stop at a zero-count chunk.", "",
        "Additional conveniences include CreateContainer, GetContainer, GetElementInRange, StreamQueue, StreamSet, StreamOrderedSet, Ready, Refresh, IsReady and Ptr. No operation replays after acknowledged progress; fallback/reroute is bounded.", "",
        "GZIP is the default codec; CompressionLZ4 explicitly selects Java-compatible raw LZ4 blocks. Codec configuration must match the server and every reader. Full LZ4 live acceptance requires an identified compatible server build.", "",
        "Regenerate: `python tools/parity.py /path/to/hurricache-java-client`.",
        "Check without writing: `python tools/parity.py /path/to/hurricache-java-client --check`.", ""
    ])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("java_checkout", type=Path)
    parser.add_argument("--check", action="store_true", help="fail for stale or invalid inventory without writing it")
    args = parser.parse_args()
    try:
        output = generate(args.java_checkout.resolve())
        path = ROOT / "API_PARITY.md"
        if args.check:
            if not path.exists() or path.read_text(encoding="utf-8") != output:
                raise ValueError("API_PARITY.md is stale; regenerate it")
            print("472 Java overload mappings verified; inventory is current.")
        else:
            path.write_text(output, encoding="utf-8", newline="\n")
            print("Wrote 472 validated Java overload mappings.")
    except (ValueError, subprocess.CalledProcessError) as error:
        raise SystemExit(str(error)) from error


if __name__ == "__main__":
    main()
