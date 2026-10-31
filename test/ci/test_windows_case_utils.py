# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
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

import ast
import ntpath
import os
import re
import shlex
import shutil
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from windows_case_utils import (
    is_excluded_command, parse_case_line, pytest_command_args, pytest_log_name,
    resume_task_manifest, FQ_SMOKE_CASES, windows_task_manifest,
    is_pytest_collection_skip,
    is_pytest_only_skip,
)


class TestVisualStudioEnvironment(unittest.TestCase):
    def test_multiline_mr_description_is_preserved_without_creating_variables(self):
        import json
        source = Path(__file__).with_name("run_windows_ci.py")
        tree = ast.parse(source.read_text(encoding="utf-8"))
        function = next(node for node in tree.body
                        if isinstance(node, ast.FunctionDef) and node.name == "visual_studio_environment")
        description = "MR table\n| MATCH '^....$' | MB_CUR_MAX==1 |\nPATH=description only"
        payload = {"Path": "C:\\VS;C:\\Windows", "CI_MERGE_REQUEST_DESCRIPTION": description}
        result = SimpleNamespace(returncode=0,
            stdout="VS startup banner\n__TSDB_VS_ENV__" + json.dumps(payload) + "\n")
        with tempfile.TemporaryDirectory() as directory:
            vcvars = Path(directory) / "vcvarsall.bat"
            vcvars.touch()
            namespace = {"Path": Path, "os": SimpleNamespace(environ={"WIN_VCVARS": str(vcvars)}),
                         "sys": SimpleNamespace(executable="python.exe"), "tempfile": tempfile,
                         "json": json, "subprocess": SimpleNamespace(run=Mock(return_value=result), PIPE=-1, STDOUT=-2),
                         "fail": Mock(side_effect=RuntimeError("missing environment"))}
            exec(compile(ast.Module(body=[function], type_ignores=[]), str(source), "exec"), namespace)
            actual = namespace["visual_studio_environment"]()
        self.assertEqual(actual, {"PATH": payload["Path"], "CI_MERGE_REQUEST_DESCRIPTION": description})
        self.assertNotIn("MB_CUR_MAX", actual)


class TestCompatibilityReadiness(unittest.TestCase):
    def test_old_stream_stop_requires_complete_metadata_and_stopped_states(self):
        path = (Path(__file__).parents[1] /
                "cases/18-StreamProcessing/23-Compatibility/test_new_stream_compatibility.py")
        tree = ast.parse(path.read_text(encoding="utf-8"))
        methods = [node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)
                   and node.name in {"old_stream_states", "stop_old_streams"}]
        clock = Mock()
        clock.monotonic.return_value = 0
        namespace = {"DATABASE": "fixture", "time": clock}
        exec(compile(ast.Module(body=methods, type_ignores=[]), str(path), "exec"), namespace)
        def output(status="Running"):
            return ("taos> select stream_name, status from information_schema.ins_streams;\r\n"
                    " stream_name | status |\r\n==========================\r\n" +
                    "".join(f" {name} | {status} |\r\n" for name in ("s_count", "s_state", "s_inter")) +
                    "Query OK, 3 row(s) in set\r\n")
        for bad in ("", "Query OK, 0 row(s) in set", output().replace("s_inter", "unexpected"),
                    output() + " s_count | Running |\r\n"):
            cli = Mock(return_value=bad)
            case = SimpleNamespace(run_old_taos=cli)
            case.old_stream_states = lambda: namespace["old_stream_states"](case)
            with self.assertRaises(RuntimeError):
                namespace["stop_old_streams"](case)
            self.assertEqual(cli.call_count, 1)
        states = iter([output(), output(), output("Stopped")])
        cli = Mock(side_effect=lambda sql, **kwargs: next(states) if sql.startswith("select") else "Query OK")
        case = SimpleNamespace(run_old_taos=cli)
        case.old_stream_states = lambda: namespace["old_stream_states"](case)
        namespace["stop_old_streams"](case)
        self.assertEqual([call.args[0] for call in cli.call_args_list if call.args[0].startswith("stop")],
                         ["stop stream s_count", "stop stream s_state", "stop stream s_inter"])
        self.assertTrue(all(call.kwargs == {"database": "fixture"} for call in cli.call_args_list
                            if call.args[0].startswith("stop")))
        clock.monotonic.side_effect = [0, 31]
        cli.side_effect = lambda sql, **kwargs: output() if sql.startswith("select") else "Query OK"
        with self.assertRaisesRegex(RuntimeError, "did not stop"):
            namespace["stop_old_streams"](case)

    def test_tagref_old_mnode_gate_cannot_pass_on_wrong_version_or_ddl_result(self):
        path = Path(__file__).parents[1] / "cases/05-VirtualTables/test_vtable_tag_ref_rolling_upgrade.py"
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        method = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
                      and n.name == "test_vtable_tag_ref_rolling_upgrade")
        start = next(i for i, n in enumerate(method.body)
                     if "tdCb.updateNewVersion(" in (ast.get_source_segment(source, n) or ""))
        end = next(i for i in range(start, len(method.body))
                   if "PASS: mixed window: new-capability" in (ast.get_source_segment(source, method.body[i]) or ""))
        method.body = method.body[start:end]
        compatibility = Mock()
        namespace = {"tdCb": compatibility, "tdCom": Mock(), "tdSql": Mock(),
                     "tdLog": Mock(), "time": Mock(), "DB": "fixture_db",
                     "lastBigVersion": "3.4.3.0"}
        exec(compile(ast.Module(body=[method], type_ignores=[]), str(path), "exec"), namespace)
        for version, created, output, valid in (
            ("3.4.3.0.enterprise", False, "DB error: Version not compatible", True),
            ("3.4.3.3.alpha.enterprise", False, "DB error: Version not compatible", False),
            ("3.4.3.0.enterprise", True, "Create OK", False),
            ("3.4.3.0.enterprise", False, "DB error: syntax error", False),
        ):
            case = Mock(newServerVersion="3.4.3.3.alpha.enterprise")
            case.getDnodePath.return_value = ["node1", "node2", "node3"]
            case._server_version_via_shell.return_value = version
            case._create_gate_table_via_shell.return_value = (created, output)
            if valid:
                namespace[method.name](case)
                case._create_gate_table_via_shell.assert_called_once_with(6030)
                self.assertEqual(compatibility.updateNewVersion.call_args.args[1:], (["node2"], 0))
            else:
                with self.assertRaises(AssertionError):
                    namespace[method.name](case)

    def test_windows_concurrent_download_reuses_only_a_published_package(self):
        import importlib.util
        import io
        import threading
        from concurrent.futures import ThreadPoolExecutor
        from unittest.mock import patch

        source = Path(__file__).parents[3] / "taos-internal/utils/download_enterprise_package.py"
        spec = importlib.util.spec_from_file_location("windows_download_regression", source)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        payload = b"complete package fixture" * 100
        barrier, publish_lock = threading.Barrier(2), threading.Lock()
        copy, replace = module.shutil.copyfileobj, module.os.replace

        def simultaneous_copy(source, target):
            barrier.wait(timeout=5)
            copy(source, target)

        def concurrent_publish(source, target):
            with publish_lock:
                if Path(target).exists():
                    raise PermissionError("Windows concurrent destination replacement")
                replace(source, target)

        with tempfile.TemporaryDirectory() as directory, \
             patch.object(module.platform, "system", return_value="Windows"), \
             patch.object(module.urllib.request, "urlopen", side_effect=lambda *a, **kw: io.BytesIO(payload)), \
             patch.object(module.shutil, "copyfileobj", side_effect=simultaneous_copy), \
             patch.object(module.os, "replace", side_effect=concurrent_publish):
            downloader = module.EnterprisePackageDownloader()
            downloader.download_path = directory
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = [pool.submit(downloader.download_package, "3.4.3.0") for _ in range(2)]
                results = [future.result(timeout=10) for future in futures]
            self.assertEqual(results[0], results[1])
            self.assertEqual(Path(results[0]).read_bytes(), payload)
            self.assertEqual(list(Path(directory).iterdir()), [Path(results[0])])

    def test_windows_download_permission_failure_remains_visible_without_a_valid_cache(self):
        import importlib.util
        import io
        from unittest.mock import patch

        source = Path(__file__).parents[3] / "taos-internal/utils/download_enterprise_package.py"
        spec = importlib.util.spec_from_file_location("windows_download_failure", source)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        for force in (False, True):
            with self.subTest(force=force), tempfile.TemporaryDirectory() as directory, \
                 patch.object(module.platform, "system", return_value="Windows"), \
                 patch.object(module.urllib.request, "urlopen", side_effect=lambda *a, **kw: io.BytesIO(b"new package")), \
                 patch.object(module.os, "replace", side_effect=PermissionError("destination locked")):
                downloader = module.EnterprisePackageDownloader()
                downloader.download_path = directory
                target = Path(directory) / downloader.get_package_name("3.4.3.0")
                if force:
                    target.write_bytes(b"old package")
                with self.assertRaises(PermissionError):
                    downloader.download_package("3.4.3.0", force_download=force)
                self.assertEqual(list(Path(directory).glob("*.part")), [])
                if force:
                    self.assertEqual(target.read_bytes(), b"old package")
                else:
                    self.assertFalse(target.exists())

    def setUp(self):
        import subprocess
        source = Path(__file__).parents[1] / "new_test_framework/utils/compatibilityUtil.py"
        tree = ast.parse(source.read_text(encoding="utf-8"))
        cls = next(node for node in tree.body
                   if isinstance(node, ast.ClassDef) and node.name == "CompatibilityBase")
        method = next(node for node in cls.body
                      if isinstance(node, ast.FunctionDef) and node.name == "_wait_old_server_ready")
        self.clock = 0
        self.run = Mock()
        namespace = {"os": os, "re": re, "tdLog": SimpleNamespace(info=Mock()),
                     "time": SimpleNamespace(monotonic=lambda: self.clock, sleep=self.advance),
                     "subprocess": SimpleNamespace(run=self.run, TimeoutExpired=subprocess.TimeoutExpired)}
        exec(compile(ast.Module(body=[method], type_ignores=[]), str(source), "exec"), namespace)
        self.wait = namespace["_wait_old_server_ready"]
        self.server = SimpleNamespace(old_bin_dir="/old version/bin", old_lib_dir="/old version/driver")

    def advance(self, seconds):
        self.clock += seconds

    def test_data_setup_waits_for_successful_ready_response(self):
        import subprocess
        self.run.side_effect = [
            subprocess.CompletedProcess([], 0, "failed to connect", ""),
            subprocess.CompletedProcess([], 0, "| offline |", ""),
            subprocess.CompletedProcess([], 1, "| ready |", ""),
            subprocess.CompletedProcess([], 0, "| 1 | ready |", ""),
        ]
        self.wait(self.server, "/sim/cfg", timeout=5)
        self.assertEqual(self.run.call_count, 4)
        self.assertEqual(self.run.call_args.args[0],
                         [os.path.join(self.server.old_bin_dir, "taos.exe" if os.name == "nt" else "taos"), "-c", "/sim/cfg", "-s", "show dnodes"])
        env = self.run.call_args.kwargs["env"]
        if os.name == "nt":
            self.assertNotIn("LD_LIBRARY_PATH", env)
            self.assertTrue(env["PATH"].startswith(self.server.old_bin_dir + os.pathsep))
        else:
            self.assertEqual(env["LD_LIBRARY_PATH"], self.server.old_lib_dir)

    def test_unready_or_hanging_probe_fails_with_bounded_wait(self):
        import subprocess
        for hangs in (False, True):
            with self.subTest(hangs=hangs):
                self.clock = 0
                self.run.reset_mock(side_effect=True)
                self.run.return_value = subprocess.CompletedProcess([], 0, "connection failed", "")
                if hangs:
                    self.run.side_effect = subprocess.TimeoutExpired("old CLI", 1)
                with self.assertRaisesRegex(RuntimeError, "did not become ready in 3s"):
                    self.wait(self.server, "/sim/cfg", timeout=3)
                self.assertEqual(self.clock, 3)
                self.assertEqual(self.run.call_count, 3)

    @unittest.skipIf(os.name == "nt", "exercises POSIX package extraction")
    def test_linux_baseline_package_supports_modern_and_legacy_archives(self):
        import importlib.util
        import tarfile
        from unittest.mock import patch
        source = Path(__file__).parents[3] / "taos-internal/utils/download_enterprise_package.py"
        spec = importlib.util.spec_from_file_location("compat_package_regression", source)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        downloader = module.EnterprisePackageDownloader()
        with patch.object(module.platform, "system", return_value="Linux"), \
             patch.object(module.platform, "machine", return_value="x86_64"):
            self.assertIn("3.4.3.0", downloader.supported_versions)
            self.assertEqual(downloader.get_package_name("3.4.3.0"),
                             "tdengine-tsdb-enterprise-3.4.3.0-linux-x64.tar.xz")
            self.assertTrue(downloader.get_package_name("3.4.2.4").endswith(".tar.gz"))
            for version, nested in (("3.4.2.4", True), ("3.4.3.0", False)):
                with self.subTest(version=version), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    payload = root / "payload"
                    (payload / "bin").mkdir(parents=True)
                    (payload / "driver").mkdir()
                    (payload / "bin/taosd").write_text("baseline executable fixture")
                    tree = root / f"tdengine-tsdb-enterprise-{version}"
                    tree.mkdir()
                    if nested:
                        with tarfile.open(tree / "package.tar.gz", "w:gz") as archive:
                            archive.add(payload / "bin", arcname="bin")
                            archive.add(payload / "driver", arcname="driver")
                    else:
                        shutil.copytree(payload, tree, dirs_exist_ok=True)
                    outer = root / downloader.get_package_name(version)
                    with tarfile.open(outer, "w:gz" if nested else "w:xz") as archive:
                        archive.add(tree, arcname=tree.name)
                    shutil.rmtree(tree)
                    downloader.download_path = directory
                    with patch.object(downloader, "download_package", return_value=str(outer)):
                        binary, library = downloader.download_and_extract(version)
                    self.assertEqual((Path(binary) / "taosd").read_text(), "baseline executable fixture")
                    self.assertEqual(Path(library), tree / "driver")

                    # A stopped extractor can leave bin/ before driver/ exists.
                    shutil.rmtree(tree / "driver")
                    with patch.object(downloader, "download_package", return_value=str(outer)):
                        binary, library = downloader.download_and_extract(version)
                    self.assertEqual((Path(binary) / "taosd").read_text(), "baseline executable fixture")
                    self.assertTrue(Path(library).is_dir())

    @unittest.skipIf(os.name == "nt", "exercises POSIX shared compatibility cache")
    def test_concurrent_baseline_readers_wait_for_complete_extraction(self):
        import importlib.util
        import multiprocessing
        import queue
        import subprocess
        import tarfile
        from unittest.mock import patch

        source = Path(__file__).parents[3] / "taos-internal/utils/download_enterprise_package.py"
        spec = importlib.util.spec_from_file_location("compat_package_concurrency", source)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        context = multiprocessing.get_context("fork")
        extracting, release, reader_started = (context.Event() for _ in range(3))
        results = context.Queue()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            tree = root / "tdengine-tsdb-enterprise-3.4.3.0"
            (tree / "bin").mkdir(parents=True)
            (tree / "driver").mkdir()
            (tree / "bin/taosd").write_text("complete baseline")
            outer = root / "tdengine-tsdb-enterprise-3.4.3.0-linux-x64.tar.xz"
            with tarfile.open(outer, "w:xz") as archive:
                archive.add(tree, arcname=tree.name)
            shutil.rmtree(tree)

            def worker(first):
                downloader = module.EnterprisePackageDownloader()
                downloader.download_path = directory
                downloader.download_package = lambda *args: str(outer)
                run = subprocess.run

                def paused_extract(*args, **kwargs):
                    (tree / "bin").mkdir(parents=True)
                    extracting.set()
                    if not release.wait(10):
                        raise RuntimeError("reader did not release extraction")
                    return run(*args, **kwargs)

                try:
                    if first:
                        with patch.object(module.subprocess, "run", side_effect=paused_extract), \
                             patch.object(module.os, "system", side_effect=lambda cmd: paused_extract(cmd, shell=True).returncode):
                            binary, library = downloader.download_and_extract("3.4.3.0")
                    else:
                        reader_started.set()
                        binary, library = downloader.download_and_extract("3.4.3.0")
                    results.put((first, (Path(binary) / "taosd").read_text(), Path(library).is_dir()))
                except Exception as error:
                    results.put((first, str(error), False))

            with patch.object(module.platform, "system", return_value="Linux"):
                writers = [context.Process(target=worker, args=(first,)) for first in (True, False)]
                writers[0].start()
                try:
                    self.assertTrue(extracting.wait(5), "extractor did not reach the partial directory")
                    writers[1].start()
                    self.assertTrue(reader_started.wait(5))
                    with self.assertRaises(queue.Empty, msg="reader used an incomplete extraction"):
                        results.get(timeout=0.3)
                finally:
                    release.set()
                    for process in writers:
                        if process.pid is not None:
                            process.join(5)
                            if process.is_alive():
                                process.kill()
                                process.join()
                self.assertEqual(sorted(results.get(timeout=2) for _ in writers),
                                 [(False, "complete baseline", True), (True, "complete baseline", True)])
                self.assertTrue(all(process.exitcode == 0 for process in writers))

    def test_rbac_upgrade_cli_preserves_sql_and_rejects_reported_errors(self):
        source = Path(__file__).parents[1] / "cases/25-Privileges/test_priv_upgrade.py"
        tree = ast.parse(source.read_text(encoding="utf-8"))
        method = next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)
                      and node.name == "runCli")
        import subprocess
        for system in ("nt", "posix"):
            with self.subTest(system=system):
                run = Mock(return_value=subprocess.CompletedProcess([], 0, "Create OK", ""))
                namespace = {"os": SimpleNamespace(name=system, path=os.path, pathsep=";" if system == "nt" else ":",
                                                  environ={"PATH": "existing", "LD_PRELOAD": "wrong.so"}),
                             "subprocess": SimpleNamespace(run=run)}
                exec(compile(ast.Module(body=[method], type_ignores=[]), str(source), "exec"), namespace)
                sql = "CREATE USER example PASS 'a b&c$'"
                namespace[method.name](None, ("/old app/bin", "/old app/lib"), sql)
                args, options = run.call_args.args[0], run.call_args.kwargs
                self.assertEqual(args, [os.path.join("/old app/bin", "taos.exe" if system == "nt" else "taos"),
                                        "-h", "localhost", "-P", "6030", "-s", sql])
                self.assertFalse(options.get("shell", False))
                if system == "nt":
                    self.assertNotIn("LD_PRELOAD", options["env"])
                    self.assertTrue(options["env"]["PATH"].startswith("/old app/bin;/old app/lib;"))
                else:
                    self.assertEqual(options["env"]["LD_LIBRARY_PATH"], "/old app/lib")
                for returncode, stdout in [(0, "DB error: rejected"), (1, "startup failed")]:
                    run.return_value = subprocess.CompletedProcess([], returncode, stdout, "")
                    with self.assertRaisesRegex(RuntimeError, "CLI failed"):
                        namespace[method.name](None, ("/old app/bin", "/old app/lib"), sql)
                    self.assertEqual(namespace[method.name](None, ("/old app/bin", "/old app/lib"), sql, check=False).stdout,
                                     stdout)

    def test_stream_upgrade_old_cli_rejects_zero_exit_sql_errors(self):
        import subprocess
        source = Path(__file__).parents[1] / "cases/18-StreamProcessing/23-Compatibility/test_new_stream_compatibility.py"
        tree = ast.parse(source.read_text(encoding="utf-8"))
        method = next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)
                      and node.name == "run_old_taos")
        run = Mock(return_value=subprocess.CompletedProcess([], 0, "Create OK", ""))
        namespace = {"subprocess": SimpleNamespace(run=run, TimeoutExpired=subprocess.TimeoutExpired), "re": re}
        exec(compile(ast.Module(body=[method], type_ignores=[]), str(source), "exec"), namespace)
        case = SimpleNamespace(old_program=lambda name: "/old app/taos.exe", config_path="/test cfg",
                               old_client_env=lambda: {"PATH": "old libraries"})
        sql = "create user example pass 'a b&c$'"
        self.assertEqual(namespace[method.name](case, sql), "Create OK")
        self.assertEqual(run.call_args.args[0], ["/old app/taos.exe", "-c", "/test cfg", "-s", sql + ";"])
        for rc, stdout, stderr in [(0, "DB error: invalid option", ""), (0, "", "DB error: rejected"),
                                   (1, "startup failed", "")]:
            run.return_value = subprocess.CompletedProcess([], rc, stdout, stderr)
            with self.assertRaisesRegex(RuntimeError, "Old taos failed"):
                namespace[method.name](case, sql)

    def test_stream_upgrade_windows_server_keeps_startup_output(self):
        source = Path(__file__).parents[1] / "cases/18-StreamProcessing/23-Compatibility/test_new_stream_compatibility.py"
        tree = ast.parse(source.read_text(encoding="utf-8"))
        methods = [node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)
                   and node.name in {"start_windows_server", "log_windows_startup"}]
        logger = Mock()
        process = SimpleNamespace(pid=123, poll=lambda: 7)
        def launch(args, **kwargs):
            kwargs["stdout"].write(b"startup rejected database metadata\n")
            return process
        popen = Mock(side_effect=launch)
        namespace = {"Path": Path, "tdLog": logger,
                     "subprocess": SimpleNamespace(Popen=popen, STDOUT=-2, CREATE_NEW_CONSOLE=16)}
        exec(compile(ast.Module(body=methods, type_ignores=[]), str(source), "exec"), namespace)
        with tempfile.TemporaryDirectory() as directory:
            config = str(Path(directory) / "node space/cfg")
            environment = {"PATH": "old library path"}
            case = SimpleNamespace()
            namespace["start_windows_server"](case, "old app/taosd.exe", config, environment, "old-version")
            self.assertEqual(popen.call_args.args[0], ["old app/taosd.exe", "-c", config])
            self.assertEqual(popen.call_args.kwargs["creationflags"], 16)
            self.assertEqual(popen.call_args.kwargs["env"], environment)
            self.assertEqual(case.version_startup_log.read_text(), "startup rejected database metadata\n")
            namespace["log_windows_startup"](case)
            self.assertTrue(any("exit=7" in str(call) for call in logger.info.call_args_list))
            self.assertTrue(any("startup rejected database metadata" in str(call) for call in logger.info.call_args_list))

    def test_windows_rolling_baseline_launches_owned_nodes_and_reports_startup_failure(self):
        source = Path(__file__).parents[1] / "new_test_framework/utils/compatibilityUtil.py"
        tree = ast.parse(source.read_text(encoding="utf-8"))
        method = next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)
                      and node.name == "_start_windows_rolling_baseline")
        import subprocess
        for fails in (False, True):
            with self.subTest(startup_failure=fails), tempfile.TemporaryDirectory() as directory:
                nodes = [Path(directory) / f"dnode{i}" for i in (1, 2, 3)]
                for node in nodes:
                    (node / "cfg").mkdir(parents=True)
                    (node / "cfg" / "taos.cfg").write_text("fixture")
                    (node / "data").mkdir()
                    (node / "data" / "stale").touch()
                unrelated = Path(directory) / "unrelated"
                unrelated.mkdir()
                (unrelated / "keep").touch()
                processes = [Mock(pid=i, poll=Mock(return_value=None)) for i in (1, 2, 3)]
                launch = Mock(side_effect=processes)
                namespace = {"Path": Path, "os": os, "shutil": shutil, "tdLog": Mock(),
                             "subprocess": SimpleNamespace(Popen=launch, STDOUT=subprocess.STDOUT, CREATE_NEW_CONSOLE=16)}
                exec(compile(ast.Module(body=[method], type_ignores=[]), str(source), "exec"), namespace)
                ready = Mock(side_effect=RuntimeError("not ready") if fails else None)
                server = SimpleNamespace(old_bin_dir=str(Path(directory) / "old app"),
                                         _wait_old_server_ready=ready)
                if fails:
                    with self.assertRaisesRegex(RuntimeError, "not ready"):
                        namespace[method.name](server, list(map(str, nodes)))
                else:
                    self.assertTrue(namespace[method.name](server, list(map(str, nodes))))
                ready.assert_called_once_with(str(nodes[0] / "cfg"))
                for node, call, process in zip(nodes, launch.call_args_list, processes):
                    self.assertEqual(call.args[0], [str(Path(server.old_bin_dir) / "taosd.exe"), "-c", str(node / "cfg")])
                    self.assertNotIn("LD_LIBRARY_PATH", call.kwargs["env"])
                    self.assertTrue(call.kwargs["env"]["PATH"].startswith(server.old_bin_dir + os.pathsep))
                    self.assertEqual(list((node / "data").iterdir()), [])
                    self.assertTrue((node / "log" / "baseline-startup.log").is_file())
                    self.assertEqual(process.kill.call_count, int(fails))
                    self.assertEqual(process.wait.call_count, int(fails))
                self.assertTrue((unrelated / "keep").is_file())

    def test_upgrade_replaces_only_the_process_for_the_requested_config(self):
        source = Path(__file__).parents[1] / "new_test_framework/utils/compatibilityUtil.py"
        tree = ast.parse(source.read_text(encoding="utf-8"))
        cls = next(node for node in tree.body if isinstance(node, ast.ClassDef)
                   and node.name == "CompatibilityBase")
        method = next(node for node in cls.body if isinstance(node, ast.FunctionDef)
                      and node.name == "_upgrade_dnode")
        import subprocess
        with tempfile.TemporaryDirectory() as directory:
            dnode = Path(directory) / "dnode1"
            (dnode / "cfg").mkdir(parents=True)
            (dnode / "log").mkdir()
            unrelated = Mock(info={"name": "taosd", "cmdline": ["/other/taosd", "-c", "/other/cfg"]})
            target = Mock(pid=42, info={"name": "taosd", "cmdline":
                                       ["/old/taosd", "-c", str(dnode / "cfg") + os.sep]})
            processes = Mock(return_value=[unrelated, target])
            actions = Mock()
            actions.attach_mock(target.kill, "kill")
            actions.attach_mock(target.wait, "wait")
            actions.attach_mock(Mock(return_value=Mock(pid=43)), "launch")
            namespace = {"os": os, "subprocess": SimpleNamespace(Popen=actions.launch, STDOUT=subprocess.STDOUT,
                                                                 CREATE_NEW_CONSOLE=0x10),
                         "psutil": SimpleNamespace(process_iter=processes, NoSuchProcess=ProcessLookupError,
                                                   AccessDenied=PermissionError), "tdLog": Mock()}
            exec(compile(ast.Module(body=[method], type_ignores=[]), str(source), "exec"), namespace)
            upgrade = namespace[method.name]
            upgrade(None, "/new/build", str(dnode))
            unrelated.kill.assert_not_called()
            self.assertEqual([call[0] for call in actions.mock_calls], ["kill", "wait", "launch"])
            target.wait.assert_called_once_with(timeout=60)
            if os.name == "nt":
                self.assertEqual(actions.launch.call_args.kwargs["creationflags"], 0x10)
                self.assertNotIn("start_new_session", actions.launch.call_args.kwargs)
            else:
                self.assertTrue(actions.launch.call_args.kwargs["start_new_session"])
                self.assertNotIn("creationflags", actions.launch.call_args.kwargs)
            self.assertEqual(actions.launch.call_args.args[0],
                             [os.path.join("/new/build", "build", "bin", "taosd.exe" if os.name == "nt" else "taosd"), "-c",
                              os.path.normcase(os.path.realpath(dnode / "cfg"))])
            processes.return_value = [unrelated]
            with self.assertRaisesRegex(RuntimeError, "found 0"):
                upgrade(None, "/new/build", str(dnode))
            self.assertEqual(actions.launch.call_count, 1)

    def test_rolling_upgrade_cannot_pass_without_a_baseline(self):
        source = (Path(__file__).parents[1] / "cases/18-StreamProcessing/23-Compatibility/"
                  "test_compatibility_rolling_upgrade.py")
        tree = ast.parse(source.read_text(encoding="utf-8"))
        cls = next(node for node in tree.body if isinstance(node, ast.ClassDef))
        method = next(node for node in cls.body if isinstance(node, ast.FunctionDef)
                      and node.name == "test_compatibility_rolling_upgrade")
        for system in ("Windows", "Linux"):
            with self.subTest(system=system):
                log = Mock()
                dnodes = Mock()
                installer = Mock(return_value=False)
                namespace = {"platform": SimpleNamespace(system=lambda: system),
                             "pytest": SimpleNamespace(skip=Mock(side_effect=RuntimeError("unsupported lifecycle")),
                                                       fail=Mock(side_effect=RuntimeError("missing baseline"))),
                             "tdLog": log, "tdDnodes": dnodes,
                             "tdCb": SimpleNamespace(installTaosdForRollingUpgrade=installer)}
                exec(compile(ast.Module(body=[method], type_ignores=[]), str(source), "exec"), namespace)
                case = SimpleNamespace(host="localhost", getLastBigVersion=lambda: "3.4.3.0",
                                       getDnodePath=lambda: ["/sim/dnode1/"])
                expected = "unsupported lifecycle" if system == "Windows" else "missing baseline"
                with self.assertRaisesRegex(RuntimeError, expected):
                    namespace[method.name](case)
                log.printNoPrefix.assert_not_called()
                if system == "Windows":
                    installer.assert_not_called()
                    dnodes.stopAll.assert_not_called()
                else:
                    installer.assert_called_once()


class TestWindowsCaseUtils(unittest.TestCase):
    def test_large_archive_keeps_full_original_and_marks_evidence_gaps(self):
        import json
        import random
        import zipfile

        source = Path(__file__).with_name("run_windows_ci.py")
        function = next(n for n in ast.parse(source.read_text(encoding="utf-8")).body
                        if isinstance(n, ast.FunctionDef) and n.name == "copy_pytest_archive")
        namespace = {"shutil": shutil, "zipfile": zipfile, "json": json}
        exec(compile(ast.Module(body=[function], type_ignores=[]), str(source), "exec"), namespace)
        payload = random.Random(7).randbytes(20000)
        with tempfile.TemporaryDirectory() as directory:
            original = Path(directory) / "full.zip"
            published = Path(directory) / "published.zip"
            name = "case-evidence/sim/dnode1/log/taosdlog.0"
            with zipfile.ZipFile(original, "w", zipfile.ZIP_DEFLATED) as archive:
                archive.writestr(name, payload)
                archive.writestr("case.log", "original failure and assertions\n")
            original_bytes = original.read_bytes()
            namespace[function.name](original, published, max_archive_bytes=4096,
                                     max_evidence_file_bytes=256)
            self.assertEqual(original.read_bytes(), original_bytes)
            self.assertLessEqual(published.stat().st_size, 4096)
            with zipfile.ZipFile(published) as archive:
                self.assertEqual(archive.read("case.log"), b"original failure and assertions\n")
                diagnostic = archive.read(name)
                self.assertTrue(diagnostic.startswith(payload[:128]))
                self.assertTrue(diagnostic.endswith(payload[-128:]))
                manifest = json.loads(archive.read("evidence-manifest.json"))
                self.assertEqual(manifest["full_archive_on_runner"], str(original))
                self.assertEqual(manifest["clipped_files"][0]["retained_ranges"],
                                 [[0, 128], [19872, 20000]])

    def test_direct_pytest_registration_accepts_explicit_case_timeout(self):
        line = ",,n,.,CASE_TIMEOUT=1800 pytest cases/17-DataSubscription/02-Consume/test_tmq_params.py --timeout=1800"
        command, timeout = parse_case_line(line)
        self.assertEqual(command, "pytest cases/17-DataSubscription/02-Consume/test_tmq_params.py --timeout=1800")
        self.assertEqual(timeout, 1800)
        args = pytest_command_args(command, timeout)
        self.assertEqual(args.count("--timeout=1800"), 1)
        self.assertNotIn("CASE_TIMEOUT=1800", args)

    @unittest.skipUnless(os.name == "nt", "requires Windows path handling")
    def test_build_cleanup_removes_files_beyond_max_path(self):
        source = Path(__file__).with_name("run_windows_ci.py")
        tree = ast.parse(source.read_text(encoding="utf-8"))
        function = next(node for node in tree.body
                        if isinstance(node, ast.FunctionDef) and node.name == "remove_build_directory")
        # Isolate process cleanup so this filesystem regression can run safely
        # alongside a build. The deletion itself uses the real Windows APIs.
        namespace = {"os": os, "shutil": shutil, "stop_processes": Mock(),
                     "stop_build_processes": Mock(),
                     "subprocess": SimpleNamespace(run=Mock(), DEVNULL=-3),
                     "time": SimpleNamespace(sleep=Mock()),
                     "fail": Mock(side_effect=RuntimeError("build cleanup failed"))}
        exec(compile(ast.Module(body=[function], type_ignores=[]), str(source), "exec"), namespace)
        with tempfile.TemporaryDirectory() as directory:
            debug = Path(directory) / "debug"
            extended = "\\\\?\\" + str(debug)
            deep = extended + "\\cargo-registry-source" * 18
            os.makedirs(deep)
            filename = deep + "\\assembly.S"
            self.assertGreater(len(filename), 300)
            with open(filename, "w", encoding="utf-8") as fixture:
                fixture.write("long-path cleanup fixture\n")
            try:
                namespace["remove_build_directory"](debug)
                self.assertFalse(debug.exists())
            finally:
                if os.path.exists(extended):
                    shutil.rmtree(extended)

    def test_artifacts_keep_failures_when_checkpoint_has_no_duration(self):
        import html
        import json
        import zipfile
        import xml.etree.ElementTree as ElementTree

        source = Path(__file__).with_name("run_windows_ci.py")
        tree = ast.parse(source.read_text(encoding="utf-8"))
        functions = [node for node in tree.body if isinstance(node, ast.FunctionDef)
                     and node.name in {"parse_result_records", "parse_results", "collect_artifacts", "latest_log_file", "copy_pytest_archive"}]
        namespace = {"Path": Path, "shutil": shutil, "html": html, "json": json,
                     "zipfile": zipfile, "ElementTree": ElementTree,
                     "pytest_log_name": pytest_log_name}
        exec(compile(ast.Module(body=functions, type_ignores=[]), str(source), "exec"), namespace)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            debug, logs, artifacts = root / "debug", root / "logs", root / "artifacts"
            debug.mkdir()
            logs.mkdir()
            (logs / "resume-checkpoint.json").write_text(json.dumps({"completed": [
                {"command": "pytest cases/history_pass.py", "status": "Success"},
                {"command": "pytest cases/history_fail.py", "status": "Failed", "duration": 12.5},
            ]}))
            (logs / "result_current.txt").write_text(
                "FAILED\tpytest cases/current_fail.py\t\t\t3.5s\n")
            namespace["collect_artifacts"](debug, logs, artifacts)
            suite = ElementTree.parse(artifacts / "pytest/junit-pytest-windows.xml").getroot()
            self.assertEqual(suite.attrib["tests"], "3")
            self.assertEqual(suite.attrib["failures"], "2")
            self.assertEqual(suite.attrib["errors"], "0")
            cases = {case.attrib["name"]: case for case in suite.findall("testcase")}
            self.assertEqual(cases["pytest cases/history_pass.py"].attrib["time"], "0")
            self.assertIsNotNone(cases["pytest cases/history_fail.py"].find("failure"))
            self.assertIsNotNone(cases["pytest cases/current_fail.py"].find("failure"))
            self.assertTrue((artifacts / "pytest/index.html").is_file())

    def test_mysql_cleanup_preserves_existing_and_closes_only_new_instances(self):
        import sys
        from unittest.mock import patch
        source = Path(__file__).with_name("run_windows_ci.py")
        tree = ast.parse(source.read_text(encoding="utf-8"))
        function = next(node for node in tree.body
                        if isinstance(node, ast.FunctionDef) and node.name == "stop_owned_fq_mysql")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            setup = root / "source/taos-community/test/cases/09-DataQuerying/19-FederatedQuery/ensure_ext_env.ps1"
            setup.parent.mkdir(parents=True)
            setup.write_text("$MysqlPass = Get-EnvOr 'TEST_MYSQL_PASSWORD' 'test-only'\n")
            base = root / "mysql/8.0"
            base.mkdir(parents=True)
            (base / "my.ini").write_text("[mysqld]\nport=13306\n")
            process = Mock()
            process.create_time.return_value = 2.0
            psutil = SimpleNamespace(Process=Mock(return_value=process), NoSuchProcess=ProcessLookupError,
                                     CONN_LISTEN="LISTEN", net_connections=Mock(return_value=[
                                         SimpleNamespace(pid=101, status="LISTEN", laddr=SimpleNamespace(port=13306))]))
            execute = Mock(return_value=SimpleNamespace(returncode=0))
            current = {(100, 1.0): base}
            namespace = {"Path": Path, "os": os, "re": re,
                         "running_fq_mysql": lambda: current,
                         "subprocess": SimpleNamespace(run=execute)}
            exec(compile(ast.Module(body=[function], type_ignores=[]), str(source), "exec"), namespace)
            with patch.dict(sys.modules, {"psutil": psutil}):
                namespace["stop_owned_fq_mysql"](current.copy(), root)
                execute.assert_not_called()
                # A newly observed child of an existing instance is preserved too.
                current[(101, 2.0)] = base
                namespace["stop_owned_fq_mysql"]({(100, 1.0): base}, root)
                execute.assert_not_called()
                current = {(101, 2.0): base}
                with patch.dict(os.environ, {"TEST_MYSQL_PASSWORD": ""}):
                    namespace["stop_owned_fq_mysql"]({}, root)
                self.assertEqual(execute.call_args.kwargs["env"]["MYSQL_PWD"], "test-only")
                self.assertEqual(execute.call_count, 1)
                self.assertIn("shutdown", execute.call_args.args[0])
                self.assertIn("--port=13306", execute.call_args.args[0])
                self.assertNotIn("test-only", " ".join(execute.call_args.args[0]))
                process.wait.assert_called_once_with(timeout=30)
                execute.reset_mock()
                psutil.net_connections.return_value[0].pid = 999
                with self.assertRaisesRegex(RuntimeError, "not owned"):
                    namespace["stop_owned_fq_mysql"]({}, root)
                execute.assert_not_called()

    def test_smoke_and_full_use_separate_real_inventories(self):
        ci_dir = Path(__file__).parent
        exclusions = [line.strip() for line in (ci_dir / "win_ignore_cases").read_text().splitlines()
                      if line.strip() and not line.lstrip().startswith("#")]
        smoke = windows_task_manifest(ci_dir)
        rows = [line for line in smoke.splitlines() if line.strip() and not line.startswith("#")]
        self.assertEqual(len(rows), 25)
        for line in rows:
            command, timeout = parse_case_line(line)
            self.assertTrue((ci_dir.parent / shlex.split(command)[1]).is_file(), command)
            if any(case in command for case in FQ_SMOKE_CASES):
                self.assertFalse(is_excluded_command(command, exclusions, "smoke"))
                self.assertTrue(is_excluded_command(command, exclusions, "full"))
                self.assertIn(timeout, (3600, 7200))
        self.assertTrue(all(case in smoke for case in FQ_SMOKE_CASES))
        full = windows_task_manifest(ci_dir, "full")
        self.assertGreater(len(full.splitlines()), 1000)
        self.assertTrue(is_excluded_command(
            "pytest cases/09-DataQuerying/19-FederatedQuery/test_other.py", exclusions, "smoke"))
        with self.assertRaises(ValueError):
            windows_task_manifest(ci_dir, "invalid")

    def test_fq_provisioning_follows_selected_suite_and_filter(self):
        import json
        source = Path(__file__).with_name("run_windows_ci.py")
        tree = ast.parse(source.read_text(encoding="utf-8"))
        function = next(node for node in tree.body
                        if isinstance(node, ast.FunctionDef) and node.name == "run_pytest_cases")
        for suite, case_filter, requires_fq in (("smoke", "", True),
                                                ("smoke", "local.py", False),
                                                ("full", "", False)):
            with self.subTest(suite=suite, case_filter=case_filter), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                ci = root / "source/taos-community/test/ci"
                ci.mkdir(parents=True)
                local = ",,y,.,pytest cases/local.py\n"
                (ci / "cases.task").write_text(local)
                (ci / "win_cases.task").write_text(local + ",,n,.,pytest " + FQ_SMOKE_CASES[0] + ",7200\n")
                provision, dependencies = Mock(), Mock()
                execute = Mock(return_value=SimpleNamespace(returncode=0))
                namespace = {"Path": Path, "os": SimpleNamespace(environ={}), "json": json,
                             "windows_task_manifest": windows_task_manifest, "FQ_SMOKE_CASES": FQ_SMOKE_CASES,
                             "ensure_fq_pytest_dependencies": dependencies,
                             "provision_federated_query_env": provision,
                             "run": execute, "sys": SimpleNamespace(executable="python.exe")}
                exec(compile(ast.Module(body=[function], type_ignores=[]), str(source), "exec"), namespace)
                self.assertEqual(namespace["run_pytest_cases"](root, {}, root, False, case_filter, suite), 0)
                self.assertEqual(provision.called, requires_fq)
                self.assertEqual(dependencies.called, requires_fq)
                self.assertTrue((root / "windows-cases.task").is_file())
                if case_filter:
                    self.assertNotIn(FQ_SMOKE_CASES[0], (root / "windows-cases.task").read_text())

    def test_legacy_timeout_column_preserves_command_and_env_commas(self):
        line = ",,n,.,FQ_REQUIRED_SERVICES=postgres,mysql CASE_TIMEOUT=5400 ./ci/pytest.sh pytest cases/a.py,7200"
        self.assertEqual(parse_case_line(line), ("pytest cases/a.py", 5400))
        self.assertEqual(parse_case_line(",,n,.,./ci/pytest.sh pytest cases/a.py,7200"), ("pytest cases/a.py", 7200))
        self.assertEqual(parse_case_line(",,n,.,pytest cases/a.py -k 'first,second'"),
                         ("pytest cases/a.py -k 'first,second'", 1800))

    def test_all_skipped_commands_are_not_successful_coverage(self):
        self.assertTrue(is_pytest_only_skip(0, "=== 2 skipped in 0.04s ===\n"))
        self.assertTrue(is_pytest_only_skip(0, "=== 1 skipped, 1 warning in 0.04s ===\n"))
        self.assertTrue(is_pytest_only_skip(5, "collected 0 items / 1 skipped\n"))
        self.assertFalse(is_pytest_only_skip(0, "=== 1 passed, 2 skipped in 0.04s ===\n"))
        self.assertFalse(is_pytest_only_skip(1, "=== 2 skipped in 0.04s ===\n"))
        self.assertFalse(is_pytest_only_skip(5, "collected 0 items\n"))

    def test_explicit_module_skip_is_distinct_from_empty_selection(self):
        skipped = 'collected 0 items / 1 skipped\nSKIPPED [1] case.py: missing baseline'
        self.assertTrue(is_pytest_collection_skip(5, skipped))
        self.assertFalse(is_pytest_collection_skip(5, 'collected 0 items'))
        self.assertFalse(is_pytest_collection_skip(5, 'collected 2 items / 1 skipped'))
        self.assertFalse(is_pytest_collection_skip(2, skipped + '\nERROR collecting case.py'))

    def test_resume_skips_success_and_failure_but_keeps_other_arguments(self):
        manifest = (",,y,.,./ci/pytest.sh pytest cases/a.py\n"
                    ",,n,.,pytest cases/a.py -Q 2\n"
                    ",,n,.,pytest cases/b.py\n"
                    ",,n,cases,bash 83-DocTest/python.sh\n")
        checkpoint = {"completed": [
            {"command": "pytest cases/a.py", "status": "Success"},
            {"command": "pytest cases/b.py", "status": "Failed"},
        ]}
        remaining, count = resume_task_manifest(manifest, checkpoint)
        self.assertEqual(count, 2)
        self.assertEqual(remaining, ",,n,.,pytest cases/a.py -Q 2\n"
                         ",,n,cases,bash 83-DocTest/python.sh\n")

    def test_resume_rejects_unfinished_or_stale_records(self):
        manifest = ",,n,.,pytest cases/a.py\n"
        for record in [{"command": "pytest cases/a.py", "status": "Running"},
                       {"command": "pytest cases/missing.py", "status": "Success"}]:
            with self.subTest(record=record), self.assertRaises(ValueError):
                resume_task_manifest(manifest, {"completed": [record]})

    def test_node_id_matches_file_exclusion(self):
        self.assertTrue(is_excluded_command(
            "pytest cases/fq.py::TestFq::test_query", ["cases/fq.py"]))

    def test_directory_and_glob_exclusions(self):
        self.assertTrue(is_excluded_command(
            "pytest cases/fq/a.py::TestFq", ["cases/fq/"]))
        self.assertFalse(is_excluded_command(
            "pytest cases/fq_extra/a.py", ["cases/fq/"]))
        self.assertTrue(is_excluded_command(
            "pytest cases/stream/test_fs_01.py", ["cases/stream/test_fs_*.py"]))

    def test_all_command_arguments_are_checked(self):
        self.assertTrue(is_excluded_command(
            "pytest cases/local.py cases/fq.py::TestFq", ["cases/fq.py"]))
        self.assertTrue(is_excluded_command(
            "env FQ_REQUIRED_SERVICES=mysql,pg ./ci/pytest.sh pytest cases/a.py",
            ["FQ_REQUIRED_SERVICES=*"]))

    def test_direct_pytest_with_priority(self):
        self.assertEqual(parse_case_line("100,,n,.,pytest cases/a.py -N 3"),
                         ("pytest cases/a.py -N 3", 1800))

    def test_windows_benchmark_timeout_and_explicit_override(self):
        command = "pytest cases/81-Tools/03-Benchmark/test_benchmark_query_rest.py -B"
        self.assertEqual(parse_case_line("100,,y,.," + command), (command, 7200))
        self.assertEqual(parse_case_line(
            "100,,y,.,CASE_TIMEOUT=3600 ./ci/pytest.sh " + command),
            (command, 3600))

    def test_wrapped_command_preserves_commas_and_arguments(self):
        self.assertEqual(parse_case_line(
            "100,,y,.,CASE_TIMEOUT=3600 env FQ_REQUIRED_SERVICES=mysql,pg "
            "./ci/pytest.sh pytest cases/a.py::TestA::test_a -k 'a or b' --timeout=3600"),
            ("pytest cases/a.py::TestA::test_a -k 'a or b' --timeout=3600", 3600))

    def test_malformed_and_unsupported_commands_fail(self):
        for line in ["bad row", ",,z,.,pytest cases/a.py", ",,n,.,bash tool.sh"]:
            with self.subTest(line=line), self.assertRaises(ValueError):
                parse_case_line(line)

    def test_log_names_are_windows_safe_and_distinct(self):
        commands = ["pytest cases/a.py::TestA::test_a -k 'a or b'",
                    "pytest cases/a.py::TestA::test_b",
                    "pytest cases/other/a.py::TestA::test_b"]
        names = [pytest_log_name(command) for command in commands]
        self.assertEqual(len(names), len(set(names)))
        for name in names:
            self.assertIsNone(re.search(r'[<>:"/\\|?*]', name))
            self.assertLess(len(name), 255)

    def test_quoted_pytest_expression_is_one_argument(self):
        arguments = pytest_command_args("pytest cases/a.py -k 'not test_other'")
        self.assertEqual(arguments[1:],
                         ["-m", "pytest", "cases/a.py", "-k", "not test_other", "--clean"])

    def test_windows_deadline_overrides_pytest_ini_default(self):
        for timeout in (1800, 7200):
            with self.subTest(timeout=timeout):
                command, deadline = parse_case_line(
                    f",,y,.,CASE_TIMEOUT={timeout} ./ci/pytest.sh pytest cases/a.py")
                arguments = pytest_command_args(command, timeout=deadline)
                self.assertIn(f"--timeout={timeout}", arguments)

    def test_explicit_pytest_timeout_is_preserved(self):
        for option in ("--timeout=3600", "--timeout 3600"):
            with self.subTest(option=option):
                arguments = pytest_command_args(f"pytest cases/a.py {option}", timeout=7200)
                self.assertNotIn("--timeout=7200", arguments)
                self.assertEqual(arguments[4:-1], shlex.split(option))

    def test_current_full_inventory_is_parseable(self):
        ci = Path(__file__).resolve().parent
        exclusions = [line.strip() for line in (ci / "win_ignore_cases").read_text(encoding="utf-8").splitlines()
                      if line.strip() and not line.startswith("#")]
        selected = []
        names = {}
        for task_file in [ci / "cases.task", ci / "win_cases.task"]:
            for line in task_file.read_text(encoding="utf-8").splitlines():
                if not line.strip() or line.lstrip().startswith("#"):
                    continue
                command = line.split(",", 4)[4]
                if is_excluded_command(command, exclusions):
                    continue
                command, timeout = parse_case_line(line)
                self.assertGreater(timeout, 0)
                selected.append(command)
                name = pytest_log_name(command)
                if name in names:
                    self.assertEqual(names[name], command)
                names[name] = command
                for argument in shlex.split(command):
                    if argument.startswith("cases/"):
                        self.assertTrue((ci.parent / argument.split("::")[0]).is_file(), argument)
        self.assertGreater(len(selected), 1000)
        self.assertTrue(any("18-StreamProcessing/" in command for command in selected))
        self.assertTrue(any("test_stream_vtable_agg_optimize.py" in command for command in selected))
        for command in selected:
            self.assertNotIn("19-FederatedQuery/", command)
            self.assertNotIn("/test_fs_", command)
            self.assertNotIn("/03-ExtSource/", command)


class TestWindowsFrameworkPortability(unittest.TestCase):
    def setUp(self):
        self.utils = Path(__file__).resolve().parent.parent / "new_test_framework/utils"

    def test_failed_case_logs_survive_sim_cleanup_without_copying_data(self):
        from unittest.mock import patch

        source = Path(__file__).with_name("run_win_cases.py")
        tree = ast.parse(source.read_text(encoding="utf-8"))
        function = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                        and node.name == "preserve_case_server_logs")
        logger = Mock()
        namespace = {"os": os, "shutil": shutil, "logger": logger}
        exec(compile(ast.Module(body=[function], type_ignores=[]), str(source), "exec"), namespace)
        preserve = namespace["preserve_case_server_logs"]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sim, saved = root / "sim", root / "saved"
            for node in ("dnode1", "dnode2"):
                for section in ("cfg", "log", "data"):
                    (sim / node / section).mkdir(parents=True)
                (sim / node / "cfg/taos.cfg").write_text("firstEp localhost:6030")
                (sim / node / "log/taosdlog.0").write_text("SHOW db.streams received")
                (sim / node / "data/sdb.data").write_bytes(b"database payload")
            outside = root / "other-task.log"
            outside.write_text("unrelated server")
            if os.name != "nt":
                (sim / "dnode1/log/unrelated.log").symlink_to(outside)
                (sim / "dnode1/log/data-alias").symlink_to(sim / "dnode1/data", target_is_directory=True)
            preserve(str(sim), str(saved))
            shutil.rmtree(sim)
            self.assertEqual(len(list(saved.rglob("*.*"))), 4)
            for node in ("dnode1", "dnode2"):
                self.assertEqual((saved / node / "log/taosdlog.0").read_text(), "SHOW db.streams received")
                self.assertTrue((saved / node / "cfg/taos.cfg").is_file())
                self.assertFalse((saved / node / "data").exists())
            self.assertFalse((saved / "dnode1/log/unrelated.log").exists())
            with patch.object(shutil, "copy2", side_effect=PermissionError("locked diagnostic")):
                preserve(str(saved), str(root / "locked"))
            self.assertEqual(logger.warning.call_count, 4)
            preserve(str(root / "missing"), str(root / "unused"))

    def test_startup_log_follows_replacement_and_truncation(self):
        path = self.utils / "server/dnode.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        function = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
                        and n.name == "start")
        for change in ("replace", "truncate", "no_marker"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as directory:
                binary = Path(directory) / "taosd"
                binary.touch()
                log = Path(directory) / "taosdlog.0"
                changed = False
                ticks = 0

                def now():
                    nonlocal ticks
                    ticks += 1
                    return ticks

                def launch(command):
                    log.write_text("booting " + "x" * 300 + "\n")
                    return 0

                def update_log():
                    nonlocal changed
                    if changed or change == "no_marker":
                        return
                    changed = True
                    contents = "dnode:1, from offline to online\n"
                    if change == "replace":
                        replacement = log.with_suffix(".new")
                        replacement.write_text(contents)
                        os.replace(replacement, log)
                    else:
                        log.write_text(contents)

                class StartupLog:
                    def __init__(self, *args, **kwargs):
                        self.file = open(*args, **kwargs)

                    def __enter__(self):
                        return self

                    def __exit__(self, *args):
                        self.file.close()
                        update_log()

                    def read(self):
                        result = self.file.read()
                        # Windows denies replacement while this read handle is
                        # open; let the writer replace it after __exit__.
                        if os.name != "nt" or change != "replace":
                            update_log()
                        return result

                    def readline(self):
                        result = self.file.readline()
                        if os.name != "nt" or change != "replace":
                            update_log()
                        return result

                logger = Mock()
                logger.exit.side_effect = SystemExit
                namespace = {"os": SimpleNamespace(path=os.path, system=launch, environ={}),
                             "platform": SimpleNamespace(system=lambda: "Linux"),
                             "time": SimpleNamespace(time=now, sleep=Mock()),
                             "sleep": Mock(), "tdLog": logger, "open": StartupLog}
                exec(compile(ast.Module(body=[function], type_ignores=[]), str(path), "exec"), namespace)
                node = SimpleNamespace(binPath=str(binary), deployed=1, valgrind=0,
                                       asan=False, remoteIP="", cfgDir=directory,
                                       logDir=directory, index=1, running=0)
                if change == "no_marker":
                    with self.assertRaises(SystemExit):
                        namespace["start"](node)
                else:
                    namespace["start"](node)
                    logger.exit.assert_not_called()

    def test_missing_startup_log_uses_framework_failure(self):
        path = self.utils / "server/dnode.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        function = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
                        and n.name == "start")
        logger = Mock()
        def stop_with_message(message):
            raise SystemExit(message)
        logger.exit.side_effect = stop_with_message
        namespace = {"os": SimpleNamespace(path=os.path, system=Mock(return_value=0)),
                     "platform": SimpleNamespace(system=lambda: "Linux"),
                     "time": SimpleNamespace(sleep=Mock()), "sleep": Mock(), "tdLog": logger}
        exec(compile(ast.Module(body=[function], type_ignores=[]), str(path), "exec"), namespace)
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / "taosd"
            binary.touch()
            node = SimpleNamespace(binPath=str(binary), deployed=1, valgrind=0,
                                   asan=False, remoteIP="", cfgDir=directory,
                                   logDir=directory, index=1, running=0)
            # Existing restart helpers catch SystemExit; FileNotFoundError
            # bypasses their bounded retry when the server has no log yet.
            with self.assertRaisesRegex(SystemExit, "startup log.*taosdlog[.]0"):
                namespace["start"](node)

    def test_query_normalization_preserves_result_data(self):
        path = self.utils / "common.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        function = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
                        and n.name == "_normalize_windows_query_output")
        function.decorator_list = []
        namespace = {"re": re}
        exec(compile(ast.Module(body=[function], type_ignores=[]), str(path), "exec"), namespace)
        output = ("Welcome to the TDengine TSDB Command Line Interface\r\n"
                  "Copyright TDengine\r\nQuery OK\r\nExec cost: 1.23s\r\n"
                  "taos>  \r\ntaos> select * from t;   \r\n"
                  "| -12.34 | NULL | unchanged (abc) |\r\n"
                  "cost=1.23(2.34)..3.45(4.56) fetch_cost=1.23(2.34)\r\n")
        self.assertEqual(namespace[function.name](output),
                         "taos> select * from t;\n"
                         "| -12.34 | NULL | unchanged (abc) |\n \n")

    def test_merged_query_result_preserves_data_and_cli_failures(self):
        import subprocess

        path = self.utils / "common.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        original = next(n for n in tree.body if isinstance(n, ast.ClassDef)
                        and n.name == "TDCom")
        original.body = [n for n in original.body
                         if (isinstance(n, ast.FunctionDef) and n.name in {
                             "generate_query_result", "_filter_result_output"})
                         or (isinstance(n, ast.Assign) and any(
                             isinstance(t, ast.Name) and t.id.startswith("_RESULT_NOISE_")
                             for t in n.targets))]
        namespace = {"os": os, "re": re, "subprocess": subprocess,
                     "platform": SimpleNamespace(system=lambda: "Windows"), "tdLog": Mock()}
        exec(compile(ast.Module(body=[original], type_ignores=[]), str(path), "exec"), namespace)
        common = namespace["TDCom"]()
        common.getClientCfgPath = lambda: "client cfg"
        common._taos_cli_path = lambda: "build bin/taos.exe"
        transcript = ("Welcome to the TDengine TSDB Command Line Interface\r\n"
                      "taos> \r\ntaos> select 1;   \r\n"
                      "| -12.34 | NULL | unchanged (abc) |\r\nQuery OK\r\n")
        with tempfile.TemporaryDirectory() as directory:
            from unittest.mock import patch

            expected = Path(directory) / "actual.result"
            input_file = Path(directory) / "input.sql"
            input_file.write_text("select 1;\n", encoding="utf-8")
            original_open = open
            def redirected_open(filename, *args, **kwargs):
                if filename == "./temp_merge_fixture.result":
                    filename = expected
                return original_open(filename, *args, **kwargs)
            common._run_taos_cli = Mock(return_value=(transcript, 0, ""))
            with patch("builtins.open", side_effect=redirected_open):
                common.generate_query_result(str(input_file), "merge_fixture")
                self.assertEqual(expected.read_text(encoding="utf-8"),
                                 "taos> select 1;\n| -12.34 | NULL | unchanged (abc) |\n")
                for rc in (1, -1):
                    common._run_taos_cli.return_value = (transcript, rc, "connection failed")
                    with self.assertRaises(subprocess.CalledProcessError) as error:
                        common.generate_query_result(str(input_file), "merge_fixture")
                    self.assertEqual(error.exception.returncode, rc)
                    self.assertEqual(error.exception.stderr, "connection failed")

    def test_tmq_launch_and_stop_only_owned_process(self):
        path = self.utils / "tmqUtil.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        original = next(n for n in tree.body if isinstance(n, ast.ClassDef)
                        and n.name == "TMQCom")
        methods = [n for n in original.body if isinstance(n, ast.FunctionDef)
                   and n.name in {"__init__", "startTmqSimProcess", "stopTmqSimProcess"}]
        original.body = methods
        process = Mock(pid=99, returncode=None)
        process.poll.return_value = None
        subprocess = SimpleNamespace(Popen=Mock(return_value=process), DEVNULL=-3,
                                     CREATE_NEW_CONSOLE=16, STARTF_USESHOWWINDOW=1, SW_HIDE=0,
                                     STARTUPINFO=lambda: SimpleNamespace(dwFlags=0),
                                     run=Mock(return_value=SimpleNamespace(returncode=0)))
        common = Mock()
        namespace = {"os": os, "shutil": shutil, "subprocess": subprocess,
                     "signal": SimpleNamespace(CTRL_BREAK_EVENT=1), "time": Mock(),
                     "sys": SimpleNamespace(executable="python.exe"),
                     "platform": SimpleNamespace(system=lambda: "Windows"),
                     "tdLog": Mock(), "tdCom": common}
        exec(compile(ast.Module(body=[original], type_ignores=[]), str(path), "exec"), namespace)
        with tempfile.TemporaryDirectory(prefix="tmq paths ") as root:
            binaries = Path(root) / "build/bin"
            binaries.mkdir(parents=True)
            (binaries / "tmq_sim.exe").write_bytes(b"fixture")
            config = str(Path(root) / "client cfg")
            common.getBuildPath.return_value = root
            common.getClientCfgPath.return_value = config
            tmq = namespace[original.name]()
            tmq.startTmqSimProcess(30, "db1", alias=1, snapshot=1)
            arguments = subprocess.Popen.call_args.args[0]
            self.assertEqual(arguments[0], str(binaries / "tmq_sim_new.exe"))
            self.assertEqual(arguments[2], config)
            self.assertEqual((binaries / "tmq_sim_new.exe").read_bytes(), b"fixture")
            tmq.stopTmqSimProcess("tmq_sim")
            subprocess.run.assert_not_called()
            tmq.stopTmqSimProcess("tmq_sim_new")
            subprocess.run.assert_called_once()
            self.assertEqual(subprocess.run.call_args.args[0][-1], "99")
            process.wait.assert_called_once_with(timeout=120)
            process.poll.return_value = 2
            process.returncode = 2
            with self.assertRaisesRegex(RuntimeError, "status 2"):
                tmq.startTmqSimProcess(30, "db1")


class TestInterpNearFallback(unittest.TestCase):
    def test_point_range_checks_actual_source_distance_and_fallback(self):
        from datetime import datetime, timedelta, timezone

        path = Path(__file__).resolve().parent.parent / "cases/09-DataQuerying/12-Interp/test_query_interp_fill.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        original = next(n for n in tree.body if isinstance(n, ast.ClassDef))
        original.body = [n for n in original.body if isinstance(n, ast.FunctionDef)
                         and n.name in {"datetime_add_tz", "binary_search_ts", "distance",
                                        "is_nearest", "check_result_for_near"}]
        namespace = {"re": re, "timedelta": timedelta,
                     "get_localzone": lambda: timezone.utc,
                     "tdLog": SimpleNamespace(exit=lambda message: self.fail(message))}
        exec(compile(ast.Module(body=[original], type_ignores=[]), str(path), "exec"), namespace)
        case = namespace[original.name]()
        target = datetime.fromtimestamp(1537146146964 / 1000, timezone.utc)
        sql = "select ... range(1537146146964, 1h) fill(near, 1, 2)"
        fallback = (None, target, 1, 2, True)
        outside = [(target + timedelta(hours=1, milliseconds=1), 11, 22)]
        case.check_result_for_near([fallback], outside, sql, "select source")
        case.check_result_for_near([], [], sql, "select source")
        boundary = [(target + timedelta(hours=1), 11, 22)]
        real = (boundary[0][0], target, 11, 22, True)
        case.check_result_for_near([real], boundary, sql, "select source")
        for rows, sources in [
            ([fallback], boundary),
            ([fallback], []),
            ([(None, target, 9, 2, True)], outside),
            ([(None, target, 1, 2, False)], outside),
            ([(outside[0][0], target, 11, 22, True)], outside),
            ([(None, target + timedelta(milliseconds=1), 1, 2, True)], outside),
            ([], outside),
            ([fallback, fallback], outside),
        ]:
            with self.subTest(rows=rows, sources=sources):
                with self.assertRaises(AssertionError):
                    case.check_result_for_near(rows, sources, sql, "select source")
        with self.assertRaises(AssertionError):
            case.check_result_for_near([fallback], [], "select ... fill(near)", "select source")


class TestWindowsForceStop(unittest.TestCase):
    def test_ctrl_break_refuses_shared_console_before_broadcast(self):
        import ctypes
        import sys
        import time
        from unittest.mock import patch
        path = Path(__file__).parents[1] / "new_test_framework/utils/server/win_process.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        function = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                        and node.name == "_send_ctrl_break_event")
        runner = Mock(return_value=SimpleNamespace(returncode=0))
        namespace = {"sys": sys, "subprocess": SimpleNamespace(run=runner, CREATE_NEW_CONSOLE=0x10)}
        exec(compile(ast.Module(body=[function], type_ignores=[]), str(path), "exec"), namespace)
        namespace[function.name](42)
        script = runner.call_args.args[0][2]
        self.assertEqual(runner.call_args.kwargs["creationflags"], 0x10)
        kernel = Mock()
        fake_ctypes = SimpleNamespace(
            WinDLL=lambda *args, **kwargs: kernel,
            c_ulong=ctypes.c_ulong, c_int=ctypes.c_int,
            WINFUNCTYPE=lambda *args: lambda callback: callback,
        )
        def attached(pids):
            def populate(buffer, size):
                for index, pid in enumerate(pids):
                    buffer[index] = pid
                return len(pids)
            return populate
        with patch.dict(sys.modules, {"ctypes": fake_ctypes}), \
             patch.object(sys, "argv", ["helper", "42"]), patch.object(time, "sleep"):
            for pids in ([42, os.getpid(), 77], [], list(range(257))):
                # An oversized count must fail without writing past the API buffer.
                kernel.GetConsoleProcessList.side_effect = attached(pids) if len(pids) < 256 else None
                kernel.GetConsoleProcessList.return_value = len(pids)
                with self.assertRaisesRegex(RuntimeError, "console"):
                    exec(script, {})
                kernel.GenerateConsoleCtrlEvent.assert_not_called()
            kernel.GetConsoleProcessList.side_effect = attached([42, os.getpid()])
            exec(script, {})
            kernel.GenerateConsoleCtrlEvent.assert_called_once_with(1, 0)

    def setUp(self):
        self.config = r"C:\workspace\tsdb\sim\dnode1\cfg"
        self.psutil = SimpleNamespace(
            process_iter=Mock(return_value=[]),
            wait_procs=Mock(return_value=([], [])),
            NoSuchProcess=type("NoSuchProcess", (Exception,), {}),
            AccessDenied=type("AccessDenied", (Exception,), {}),
        )
        self.log = Mock()
        server = Path(__file__).resolve().parent.parent / "new_test_framework/utils/server"
        tree = ast.parse((server / "win_process.py").read_text(encoding="utf-8"))
        function = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                        and node.name == "force_stop_taosd_windows")
        namespace = {"ntpath": ntpath, "psutil": self.psutil, "logger": self.log}
        exec(compile(ast.Module(body=[function], type_ignores=[]),
                     str(server / "win_process.py"), "exec"), namespace)
        self.force_stop = namespace[function.name]
        lookup = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                      and node.name == "_find_taosd_pid")
        exec(compile(ast.Module(body=[lookup], type_ignores=[]),
                     str(server / "win_process.py"), "exec"), namespace)
        self.find_pid = namespace[lookup.name]
        tree = ast.parse((server / "dnode.py").read_text(encoding="utf-8"))
        function = next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)
                        and node.name == "forcestop")
        namespace = {"platform": SimpleNamespace(system=lambda: "Windows"),
                     "tdLog": self.log, "force_stop_taosd_windows": self.force_stop}
        exec(compile(ast.Module(body=[function], type_ignores=[]),
                     str(server / "dnode.py"), "exec"), namespace)
        self.stop_node = namespace[function.name]

    def process(self, pid, config, name="taosd.exe"):
        process = Mock()
        process.pid = pid
        process.info = {"pid": pid, "name": name}
        process.cmdline.return_value = [name, "-c", config]
        return process

    def test_force_stop_matches_exact_config_and_waits(self):
        target = self.process(1, "c:/WORKSPACE/tsdb/sim/dnode1/cfg/")
        second = self.process(2, self.config)
        others = [self.process(10, self.config.replace("dnode1", "dnode10")),
                  self.process(11, self.config.replace("tsdb", "another-repo")),
                  self.process(12, self.config, "python.exe")]
        self.psutil.process_iter.return_value = [target, second, *others]
        self.assertEqual(self.force_stop(self.config, timeout=4), 2)
        target.kill.assert_called_once_with()
        second.kill.assert_called_once_with()
        for process in others:
            process.kill.assert_not_called()
        self.psutil.wait_procs.assert_called_once_with([target, second], timeout=4)

    def test_graceful_lookup_normalizes_parent_components_and_matches_exact_config(self):
        target = self.process(1, "C:/workspace/tsdb/sim/dnode1/cfg")
        others = [self.process(10, self.config.replace("dnode1", "dnode10")),
                  self.process(11, self.config.replace("tsdb", "another-repo")),
                  self.process(12, self.config, "python.exe")]
        self.psutil.process_iter.return_value = [*others, target]
        path = "C:/workspace/tsdb/debug/../sim/dnode1/cfg/"
        self.assertEqual(self.find_pid(config_dir=path), 1)
        self.assertEqual(self.find_pid(dnode_index=1), 11)
        self.psutil.process_iter.return_value = others[:1]
        self.assertIsNone(self.find_pid(config_dir=path, dnode_index=1))
        self.assertIsNone(self.find_pid(dnode_index=1))
        target.cmdline.return_value = ["taosd.exe", '-c"C:/workspace/tsdb/sim/dnode1/cfg"']
        self.psutil.process_iter.return_value = [target]
        self.assertEqual(self.find_pid(config_dir=path), 1)

    def test_old_user_cleanup_rejects_syntax_and_connection_errors(self):
        path = Path(__file__).resolve().parent.parent / "cases/25-Privileges/test_priv_upgrade.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        function = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
                        and n.name == "dropOldUser")
        namespace = {}
        exec(compile(ast.Module(body=[function], type_ignores=[]), str(path), "exec"), namespace)
        case = SimpleNamespace(runCli=Mock())
        for output in ("Drop OK", "DB error: Invalid user [0x80000351]",
                       "DB error: Invalid user [0x0351]"):
            case.runCli.return_value = SimpleNamespace(returncode=0, stdout=output, stderr="")
            namespace[function.name](case, ("old bin", "old lib"), "fixture_user")
        for rc, output in ((0, 'DB error: syntax error [0x80002600]'),
                           (0, 'DB error: permission denied'),
                           (1, 'Unable to establish connection')):
            case.runCli.return_value = SimpleNamespace(returncode=rc, stdout=output, stderr="")
            with self.assertRaises(RuntimeError):
                namespace[function.name](case, ("old bin", "old lib"), "fixture_user")
        self.assertEqual(case.runCli.call_args.args[1], "DROP USER fixture_user")

    def test_force_stop_accepts_attached_config_argument(self):
        target = self.process(1, self.config)
        target.cmdline.return_value = ["taosd.exe", "-c" + self.config]
        self.psutil.process_iter.return_value = [target]
        self.assertEqual(self.force_stop(self.config), 1)
        target.kill.assert_called_once_with()

    def test_force_stop_handles_process_exit_race(self):
        target = self.process(1, self.config)
        target.kill.side_effect = self.psutil.NoSuchProcess()
        self.psutil.process_iter.return_value = [target]
        self.assertEqual(self.force_stop(self.config), 1)
        self.psutil.wait_procs.assert_called_once()

    def test_force_stop_does_not_hide_access_denied(self):
        for operation in ("cmdline", "kill"):
            with self.subTest(operation=operation):
                target = self.process(1, self.config)
                getattr(target, operation).side_effect = self.psutil.AccessDenied()
                self.psutil.process_iter.return_value = [target]
                with self.assertRaises(RuntimeError):
                    self.force_stop(self.config)

    def test_force_stop_rejects_missing_config(self):
        with self.assertRaises(ValueError):
            self.force_stop(None)
        self.psutil.process_iter.assert_not_called()

    def test_force_stop_already_absent_is_idempotent(self):
        self.assertEqual(self.force_stop(self.config), 0)

    def test_dnode_marks_stopped_only_after_confirmed_exit(self):
        for still_alive in (False, True):
            with self.subTest(still_alive=still_alive):
                target = self.process(1, self.config)
                self.psutil.process_iter.return_value = [target]
                self.psutil.wait_procs.return_value = ([], [target] if still_alive else [])
                node = SimpleNamespace(asan=False, remoteIP="", valgrind=0,
                                       running=1, index=1, cfgDir=self.config)
                if still_alive:
                    with self.assertRaisesRegex(RuntimeError, "still running"):
                        self.stop_node(node)
                    self.assertEqual(node.running, 1)
                else:
                    self.stop_node(node)
                    self.assertEqual(node.running, 0)


if __name__ == "__main__":
    unittest.main()
