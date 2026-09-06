"""Generate the Java overload parity reference from the pinned, read-only checkout."""
from pathlib import Path
import argparse
import re
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument("java_checkout", type=Path)
args = parser.parse_args()
baseline = "fd2f89d56751b000a62565ef3ff0712040d5bbfd"
if subprocess.check_output(["git", "-C", str(args.java_checkout), "rev-parse", "HEAD"], text=True).strip() != baseline:
    raise SystemExit("Java baseline mismatch")
root = Path(__file__).resolve().parents[1]
java = args.java_checkout / "src/main/java/com/hurricache"
base_url = f"https://github.com/hurricache/hurricache-java-client/blob/{baseline}/src/main/java/com/hurricache/"
rename = {"setTtl": "SetTTL", "getTtl": "GetTTL", "getDefaultClientId": "DefaultClientID",
          "getDefaultTimeout": "DefaultTimeout", "getDefaultTtl": "DefaultTTL", "getTarget": "Target",
          "shutdown": "Close", "serializeKey": "[]byte(string)", "getReadyFlag": "IsReady",
          "setMode": "Options.Mode"}
smart = (java / "client/FastCacheAsyncSmartClient.java").read_text(encoding="utf-8")
policies = {}
for m in re.finditer(r"public CompletableFuture<[^\n]+?\s+(\w+)\([^)]*\)\s*\{\s*return (executeWrite|execute)\(", smart, re.S):
    policies[m[1]] = "MasterThenBackup" if m[2] == "executeWrite" else "configured mode"

rows = []
for path in sorted((java / "client/intf").glob("HurriCacheClient*.java")):
    text = path.read_text(encoding="utf-8")
    # Preserve offsets/line numbers while removing comments.
    text = re.sub(r"/\*.*?\*/|//[^\n]*", lambda m: re.sub(r"[^\n]", " ", m[0]), text, flags=re.S)
    pattern = r"(?:default\s+)?(?:CompletableFuture<.*?>|Duration|int|String|void|byte\[\])\s+(\w+)\s*\((.*?)\)"
    for m in re.finditer(pattern, text, re.S):
        name = m[1]
        go = rename.get(name, name[0].upper() + name[1:])
        if name == "removeFromContainer" and "byte[] elementKey" in m[2]:
            go = "RemoveContainerKey"
        line = text.count("\n", 0, m.start()) + 1
        signature = " ".join(m[0].split())
        policy = policies.get(name, "n/a")
        if name == "streamElementInRangeOrdered": policy = "configured mode"
        if name == "addElementOrderedSet": policy = "MasterThenBackup"
        test = (f"TestOperationContract/{go}; TestSmartEveryOperationDispatch/{go}; TestLiveOperationMatrix/{go}"
                if "CompletableFuture" in m[0] else "TestPublicContractCoverage; examples")
        rows.append((signature, f"{base_url}client/intf/{path.name}#L{line}", go, policy, test))

out = [
    "# Java / Go API parity",
    "",
    f"Authoritative baseline: Java jdk-16 commit `{baseline}`. Jedis is excluded.",
    f"This inventory contains {len(rows)} interface declarations, including every overload.",
    "",
    "All asynchronous Java methods become synchronous Go calls with context first and typed result/error returns.",
    "String overloads use `[]byte(text)`; hint, client ID, timeout and TTL overloads use `Options`.",
    "Every Go operation below is available on both `Client` and `SmartClient` through `Operations`.",
    "The per-call `Options.Mode` overrides the listed policy. Java's `setMode` becomes this concurrency-safe option or the construction-time `SmartConfig.Mode`.",
    "",
    "Tests in the table are subtests in protocol_test.go, smart_test.go and integration_test.go.",
    "Live tests assert pinned-server defects explicitly; passing does not mean those server behaviors are correct.",
    "See [findings](FINDINGS.md) for deviations, evidence and server limitations.",
    "",
    "| Java declaration | Go equivalent | Smart default | Coverage |",
    "| --- | --- | --- | --- |",
]
for signature, url, go, policy, test in rows:
    out.append(f"| [`{signature}`]({url}) | `{go}` | {policy} | `{test}` |")
out += [
    "", "Additional Go conveniences: `CreateContainer`, `GetContainer`, `GetElementInRange`,",
    "`StreamQueue`, `StreamSet`, `StreamOrderedSet`, `Ready`, `Refresh`, `IsReady`,",
    "`NewClient`, `NewClientWithConnection`, `NewSmartClient` and `Ptr`.",
    "Streaming helpers return decoded slices. Maps use entry slices, preserving binary keys and server ordering.",
    "Payload factories become struct literals; `KeyHintData` becomes `KeyHint`; Java `AtomicCasRes` becomes `CASResult`.",
    "Java target/default accessors map to `Target`, `DefaultClientID`, `DefaultTimeout` and `DefaultTTL`.",
    "Java `shutdown` maps to idempotent `Close`; connection constructors map to the owned/borrowed constructors.",
    "Identity helpers (`equals`, `hashCode`, `toString`) use ordinary Go pointer identity and formatting.",
    "", "Regenerate: `python tools/parity.py /path/to/hurricache-java-client`.", ""
]
(root / "API_PARITY.md").write_text("\n".join(out), encoding="utf-8", newline="\n")
print(f"Wrote {len(rows)} Java overload mappings; {len(policies)} smart policies.")
