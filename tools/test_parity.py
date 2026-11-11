"""Regression checks for the inventory's failure paths and wrapper routing."""

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import parity


class ParityTests(unittest.TestCase):
    def test_routing_through_nested_chunk_helpers(self):
        methods = parity.bodies("""
          public CompletableFuture<KeyHintData> createList(byte[] key) {
            return createUnorderedContainerSmart(key, c -> c.createList(key));
          }
          private CompletableFuture<KeyHintData> createUnorderedContainerSmart(byte[] key) {
            return createContainerWithChunks(key);
          }
          private <K, V> CompletableFuture<KeyHintData> createContainerWithChunks(byte[] key) {
            // An execute call in a comment must not override the helper.
            return executeWrite(key, c -> c.createList(key));
          }
          public CompletableFuture<byte[]> getValue(byte[] key) {
            return execute(key, c -> c.getValue(key));
          }
        """)
        self.assertEqual(
            parity.resolve_policy("createList", methods), {"MasterThenBackup"}
        )
        self.assertEqual(
            parity.resolve_policy("getValue", methods), {"configured mode"}
        )
        self.assertEqual(parity.resolve_policy("unknown", methods), set())

    def test_check_never_writes_stale_or_current_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            output = root / "API_PARITY.md"
            output.write_text("stale", encoding="utf-8")
            with (
                patch.object(parity, "ROOT", root),
                patch.object(parity, "generate", return_value="expected\n"),
                patch("sys.argv", ["parity.py", tmp, "--check"]),
            ):
                with self.assertRaisesRegex(SystemExit, "stale"):
                    parity.main()
                self.assertEqual(output.read_text(encoding="utf-8"), "stale")
                output.write_text("expected\n", encoding="utf-8")
                before = output.stat().st_mtime_ns
                with contextlib.redirect_stdout(io.StringIO()):
                    parity.main()
                self.assertEqual(output.stat().st_mtime_ns, before)

    def test_overload_mapping_and_unknown_return_types(self):
        source = """interface C {
          CompletableFuture<Boolean> update(byte[] key, OrderedPayload elementKey, long new_order);
          CompletableFuture<Variant> update(byte[] key, byte[] elementKey, Variant value);
          NewUnrecognizedReturn newlyIntroduced(String futureParameter);
        }"""
        declarations = list(parity.declarations(source, interface=True))
        self.assertEqual(
            [parity.mapping(m) for m, _ in declarations],
            ["UpdateOrder", "Update", "NewlyIntroduced"],
        )
        self.assertEqual(len(declarations), 3)

    def test_unknown_arguments_and_models_fail(self):
        source = "interface C {\n CompletableFuture<Variant> get(byte[] key, int futureParameter);\n }"
        declaration, _ = next(parity.declarations(source, interface=True))
        with self.assertRaisesRegex(ValueError, "unmapped parameter"):
            parity.argument_mapping(
                declaration, "Get", {"Get": {"key"}}, {"Methods": {}}
            )
        source = "public class Variant {\n public FutureType surprise(int value) { return null; }\n }"
        declaration, _ = next(parity.declarations(source))
        with self.assertRaisesRegex(ValueError, "unmapped model declaration"):
            parity.model_mapping("Variant", declaration, {"Methods": {}})

    def test_missing_equivalent_and_coverage_fail(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "proto").mkdir()
            for name in ("cache.proto", "coordinator.proto"):
                (root / "proto" / name).write_text("schema\n", encoding="utf-8")
            (root / "client.go").write_text(
                "func(c *Client) Get(ctx context.Context, key []byte, o Options) (Variant,error) {}",
                encoding="utf-8",
            )
            path = parity.JAVA_ROOT + "client/intf/HurriCacheClientRaw.java"
            manifest = {"Methods": {}, "Cases": [], "Configured": []}

            def command(args, **kwargs):
                if args[0] == "go":
                    return "PARITY_CONTRACT " + json.dumps(manifest)
                if "ls-tree" in args:
                    return (
                        path
                        + "\n"
                        + parity.JAVA_ROOT
                        + "client/FastCacheAsyncSmartClient.java\n"
                    )
                if "show" in args:
                    if args[-1].endswith(".proto"):
                        return "schema\n"
                    if args[-1].endswith("FastCacheAsyncSmartClient.java"):
                        return "public class FastCacheAsyncSmartClient {\n public CompletableFuture<Variant> get(byte[] key) { return execute(key); }\n }"
                    return "interface HurriCacheClientRaw {\n CompletableFuture<Variant> get(byte[] key);\n }"
                return ""

            with (
                patch.object(parity, "ROOT", root),
                patch.object(parity.subprocess, "check_output", side_effect=command),
            ):
                with self.assertRaisesRegex(ValueError, "nonexistent Go equivalent"):
                    parity.generate(root)
                manifest["Methods"]["Get"] = (
                    "func(context.Context, []uint8, hurricache.Options) (hurricache.Variant, error)"
                )
                with self.assertRaisesRegex(
                    ValueError, "nonexistent operation coverage case"
                ):
                    parity.generate(root)
                manifest["Cases"] = ["Get"]
                with self.assertRaisesRegex(ValueError, "smart dispatch differs"):
                    parity.generate(root)


if __name__ == "__main__":
    unittest.main()
