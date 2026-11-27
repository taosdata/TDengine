# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
import subprocess
import time
from unittest.mock import patch

import macos_core_dumps


class TestCoreCollection(unittest.TestCase):
    def test_retains_matching_executable_and_debug_symbols(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            executable = root / "unit-test"
            executable.write_bytes(b"executable-content")
            output = root / "retained"
            output.mkdir()
            def generate_symbols(command, **kwargs):
                symbols = Path(command[-1]) / "Contents/Resources/DWARF"
                symbols.mkdir(parents=True)
                (symbols / "unit-test").write_bytes(b"symbols")
                return SimpleNamespace(returncode=0, stderr="")
            identity = "UUID: 12345678-1234-1234-1234-123456789ABC"
            with patch.object(macos_core_dumps.subprocess, "run", side_effect=generate_symbols) as dsymutil, \
                    patch.object(macos_core_dumps.subprocess, "check_output", side_effect=[identity, identity, "DW_TAG_compile_unit DW_TAG_subprogram"]):
                bundle = macos_core_dumps.retain_debug_bundle(executable, output, "core.123")
            self.assertEqual(Path(bundle["executable"]).read_bytes(), executable.read_bytes())
            self.assertEqual(Path(bundle["executable"]).stat().st_mode & 0o777, 0o600)
            self.assertTrue((Path(bundle["symbols"]) / "Contents/Resources/DWARF/unit-test").is_file())
            self.assertEqual(dsymutil.call_args.args[0][0], "dsymutil")

    def test_rejects_empty_or_mismatched_symbols(self):
        identity = "UUID: 12345678-1234-1234-1234-123456789ABC"
        for outputs in ([identity, identity, "empty"], [identity, "UUID: 00000000-0000-0000-0000-000000000000"]):
            with self.subTest(outputs=outputs), TemporaryDirectory() as directory:
                root = Path(directory)
                executable = root / "unit-test"
                executable.write_bytes(b"executable")
                (root / "core.123.dSYM/Contents/Resources/DWARF").mkdir(parents=True)
                with patch.object(macos_core_dumps.subprocess, "run", return_value=SimpleNamespace(stderr="")), \
                        patch.object(macos_core_dumps.subprocess, "check_output", side_effect=outputs):
                    with self.assertRaises(RuntimeError):
                        macos_core_dumps.retain_debug_bundle(executable, root, "core.123")

    def test_symbol_failure_retains_core_and_preserves_build(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            cores = root / "cores"
            cores.mkdir()
            core = cores / "core.123"
            core.write_bytes(b"raw-core")
            debug = root / "build"
            debug.mkdir()
            (debug / "object.o").write_bytes(b"debug object")
            output = root / "retained"
            macos_core_dumps.write_json(root / "core-state.json", {
                "archive_dir": str(output), "debug_dir": str(debug), "core_dir": str(cores),
                "started": 0, "existing": [], "binaries": ["/unit-test"]})
            with patch.object(macos_core_dumps.subprocess, "run", return_value=SimpleNamespace(returncode=0, stdout="[  0] /unit-test\n")), \
                    patch.object(macos_core_dumps, "retain_debug_bundle", side_effect=RuntimeError("empty dSYM")):
                macos_core_dumps.collect(root)
            self.assertEqual((output / "core.123").read_bytes(), b"raw-core")
            self.assertFalse(core.exists())
            records = json.loads((root / "core-archives.json").read_text())
            self.assertIn("symbols_error", records[0])
            self.assertTrue((root / "core-preserve-build").exists())
            macos_core_dumps.collect(root)
            self.assertEqual(json.loads((root / "core-archives.json").read_text()), records)
            self.assertTrue((root / "core-preserve-build").exists())
            macos_core_dumps.finish_collection(root)
            self.assertEqual((output / "debug/object.o").read_bytes(), b"debug object")
            self.assertFalse(debug.exists())
            self.assertTrue((output / "completed.json").exists())

    def test_successful_collection_does_not_preserve_build(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            debug = root / "debug"
            debug.mkdir()
            output = root / "retained"
            output.mkdir()
            macos_core_dumps.write_json(root / "core-state.json", {
                "archive_dir": str(output), "debug_dir": str(debug)})
            macos_core_dumps.finish_collection(root)
            self.assertTrue(debug.exists())
            self.assertFalse((output / "debug").exists())
            self.assertTrue((output / "completed.json").exists())

    def test_failed_setup_invalidates_previous_state(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "core-state.json").write_text("stale state")
            with patch.object(macos_core_dumps.subprocess, "check_output", side_effect=RuntimeError("missing ctest")):
                with self.assertRaises(RuntimeError):
                    macos_core_dumps.prepare(root, root)
            self.assertFalse((root / "core-state.json").exists())
            self.assertFalse(json.loads((root / "core-setup.json").read_text())["enabled"])

    def test_failed_core_does_not_block_next_core(self):
        for failure in ("identify", "copy", "delete"):
            with self.subTest(failure=failure), TemporaryDirectory() as directory:
                root = Path(directory)
                cores = root / "cores"
                cores.mkdir()
                for name in ("core.111", "core.222"):
                    (cores / name).write_bytes(b"content")
                (root / "core-state.json").write_text(json.dumps({
                    "archive_dir": str(root / "retained"), "core_dir": str(cores),
                    "started": 0, "existing": [], "binaries": ["/unit-test"]}))
                def identify(command, **kwargs):
                    if failure == "identify" and command[3].endswith("111"):
                        raise subprocess.TimeoutExpired(command, 60)
                    return SimpleNamespace(returncode=0, stdout="[  0] /unit-test\n")
                original_copy = macos_core_dumps.shutil.copyfileobj
                def copy_core(source, destination, **kwargs):
                    if failure == "copy" and source.name.endswith("111"):
                        raise OSError("disk full")
                    return original_copy(source, destination, **kwargs)
                def remove(core):
                    if failure == "delete" and core.name.endswith("111"):
                        raise PermissionError("cannot delete")
                    core.unlink()
                with patch.object(macos_core_dumps.subprocess, "run", side_effect=identify), \
                        patch.object(macos_core_dumps, "retain_debug_bundle", return_value={"executable": "/retained/unit-test", "symbols": "/retained/unit-test.dSYM"}), \
                        patch.object(macos_core_dumps.shutil, "copyfileobj", side_effect=copy_core), \
                        patch.object(macos_core_dumps, "remove_owned_core", side_effect=remove):
                    macos_core_dumps.collect(root)
                self.assertTrue((cores / "core.111").exists())
                self.assertFalse((cores / "core.222").exists())
                records = json.loads((root / "core-archives.json").read_text())
                self.assertEqual(len(records), 2 if failure == "delete" else 1)
                self.assertFalse(list((root / "retained").glob(".*.partial")))

    def test_cleanup_keeps_active_pinned_and_recent_archives(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            for job in range(1, 6):
                archive = root / f"job-{job}"
                archive.mkdir()
                macos_core_dumps.write_json(archive / "completed.json", {"finished": 0})
            (root / "job-2/KEEP").touch()
            (root / "job-3/completed.json").unlink()
            macos_core_dumps.write_json(root / "job-4/completed.json", {"finished": time.time()})
            (root / "job-5/.core.partial").touch()
            (root / "job-6").symlink_to(root / "job-1", target_is_directory=True)
            macos_core_dumps.cleanup_archives(root, 7, dry_run=True)
            self.assertTrue((root / "job-1").exists())
            macos_core_dumps.cleanup_archives(root, 7)
            self.assertFalse((root / "job-1").exists())
            for job in (2, 3, 4, 5):
                self.assertTrue((root / f"job-{job}").exists())
            self.assertTrue((root / "job-6").is_symlink())

    def test_setup_failure_does_not_block_ctest(self):
        import run_macos_ctest
        with TemporaryDirectory() as directory:
            with patch("sys.argv", ["run_macos_ctest.py", directory, directory]), \
                    patch.object(run_macos_ctest, "prepare_core_dumps", side_effect=RuntimeError("sysctl disabled")), \
                    patch.object(run_macos_ctest, "cleanup_taosd"), \
                    patch.object(run_macos_ctest, "start_taosd", return_value=(None, None, {})), \
                    patch.object(run_macos_ctest, "stop_taosd_process"), \
                    patch.object(run_macos_ctest, "run_group", return_value=(0, [])) as run_group, \
                    patch.object(run_macos_ctest, "write_junit"), \
                    patch.object(run_macos_ctest, "failed_cases", return_value=[]), \
                    patch.object(run_macos_ctest, "print_summary"), \
                    patch.object(run_macos_ctest, "print_failure_sections"), \
                    patch.object(run_macos_ctest, "write_html"):
                self.assertEqual(run_macos_ctest.main(), 0)
                self.assertEqual(run_group.call_count, 2)

    def test_only_new_owned_ctest_core_is_archived(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            cores = root / "system-cores"
            artifacts = root / "artifacts"
            cores.mkdir()
            artifacts.mkdir()
            binary = "/test/debug/build/bin/unit-test"
            old = cores / "core.old"
            own = cores / "core.123"
            unrelated = cores / "core.456"
            for core in (old, own, unrelated):
                core.write_bytes(b"core-content")
            (artifacts / "core-state.json").write_text(json.dumps({
                "archive_dir": str(root / "retained"), "core_dir": str(cores), "started": 0, "binaries": [binary], "uuids": {"12345678-1234-1234-1234-123456789ABC": binary},
                "existing": [str(old)]}))
            def identify(command, **kwargs):
                image = "12345678-1234-1234-1234-123456789ABC (0x1234)" if command[3] == str(own) else "/other/application"
                return SimpleNamespace(returncode=0, stdout=f"[  0] {image}\n")
            with patch.object(macos_core_dumps.subprocess, "run", side_effect=identify) as lldb, \
                    patch.object(macos_core_dumps, "retain_debug_bundle", return_value={"executable": "/retained/unit-test", "symbols": "/retained/unit-test.dSYM"}):
                macos_core_dumps.collect(artifacts)
            self.assertEqual(lldb.call_count, 2)
            self.assertTrue(old.exists())
            self.assertTrue(unrelated.exists())
            self.assertFalse(own.exists())
            saved = root / "retained/core.123"
            with saved.open("rb") as stream:
                self.assertEqual(stream.read(), b"core-content")
            self.assertEqual(saved.stat().st_mode & 0o777, 0o600)
            records = json.loads((artifacts / "core-archives.json").read_text())
            with patch.object(macos_core_dumps.subprocess, "run", side_effect=identify):
                macos_core_dumps.collect(artifacts)
            self.assertEqual(json.loads((artifacts / "core-archives.json").read_text()), records)

    def test_failed_identification_leaves_core_untouched(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            core = root / "core.123"
            core.write_bytes(b"core-content")
            (root / "core-state.json").write_text(json.dumps({
                "archive_dir": str(root / "retained"), "core_dir": directory, "started": 0, "binaries": ["/unit-test"],
                "existing": [str(root / "core-state.json")]}))
            with patch.object(macos_core_dumps.subprocess, "run", return_value=SimpleNamespace(returncode=1, stdout="")):
                macos_core_dumps.collect(root)
            self.assertTrue(core.exists())
            self.assertFalse((root / "cores").exists())

    def test_missing_state_does_not_scan_system_cores(self):
        with TemporaryDirectory() as directory:
            with patch.object(macos_core_dumps.subprocess, "run") as command:
                macos_core_dumps.collect(Path(directory))
            command.assert_not_called()


if __name__ == "__main__":
    unittest.main()
