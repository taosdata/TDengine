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

import test_rewrite_endpoint_apply as apply_case

from new_test_framework.utils import tdLog

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

import pytest


class TestRewriteEndpointOther:
    """Startup-side behaviour of the endpoint rewrite

    The rewrite itself and the WAL / report checks live in
    test_rewrite_endpoint_apply.py. This file keeps the cases that only look at
    what a leftover or retired file does to a normal start, so each file stays
    short enough for the CI time limit.

    The instance helpers come from the apply case: it owns the fixture, the
    start/stop logic and the content snapshots.
    """

    def setup_class(self):
        self.case = apply_case.TestRewriteEndpointApply()
        self.case.setup_class()
        self.binary = self.case.binary
        tdLog.debug(f"start to execute {__file__}")

    def run_to_exit(self, root, cfg):
        """Run taosd and wait for it to exit on its own."""
        return subprocess.run([self.binary, "-c", str(cfg)], cwd=root,
                              env=self.case.native_environment(), capture_output=True,
                              text=True, timeout=180)

    def write_instance(self, root, extra_data_dir=None):
        """Create a single instance configuration and return its paths."""
        cfg, data, logs = [root / part for part in ["cfg", "data", "log"]]
        paths = [cfg, data, logs]
        if extra_data_dir is not None:
            paths.append(extra_data_dir)
        for path in paths:
            path.mkdir()
        port = self.case.free_ports(1)[0]
        disk = ""
        if extra_data_dir is not None:
            disk = f"dataDir {extra_data_dir} 0 0\n"
        (cfg / "taos.cfg").write_text(
            f"dataDir {data} 0 1\n{disk}"
            f"logDir {logs}\nfqdn localhost\nserverPort {port}\n"
            f"firstEp localhost:{port}\n"
            "numOfCores 2\nnumOfRpcThreads 2\nnumOfTaskQueueThreads 4\n"
            "udf 0\ntelemetryReporting 0\nmonitor 0\naudit 0\n",
            encoding="utf-8")
        return cfg, data, logs, port

    def set_local_cfg(self, data, **values):
        """Edit the persisted local configuration that overrides taos.cfg."""
        path = data / "dnode/config/local.json"
        content = json.loads(path.read_text())
        content["configs"].update(values)
        path.write_text(json.dumps(content))

    def do_ep_json_retired(self):
        with tempfile.TemporaryDirectory(prefix="rw-epjson-") as tmp:
            root = Path(tmp)
            cfg, data, logs, port = self.write_instance(root)

            # A first run creates the data directory.
            self.case.serve_once(root, cfg, port)
            before = [(node["fqdn"], node["port"]) for node in
                      json.loads((data / "dnode/dnode.json").read_text())["dnodes"]]

            epJson = data / "dnode/ep.json"
            epJson.write_text(json.dumps({"dnodes": [{
                "id": 1, "fqdn": "localhost", "port": port,
                "new_fqdn": "127.0.0.1", "new_port": port}]}))
            log = self.case.serve_once(root, cfg, port)
            assert str(epJson) in log, log
            assert "--rewrite-endpoints-precheck" in log, log
            assert "--rewrite-endpoints-apply" in log, log

            # ep.json must not map anything any more, and must not be renamed.
            after = [(node["fqdn"], node["port"]) for node in
                     json.loads((data / "dnode/dnode.json").read_text())["dnodes"]]
            assert after == before, (before, after)
            assert epJson.exists()

            # Changing the endpoint by hand must be refused, pointing at the
            # offline rewrite commands instead.
            epJson.unlink()
            (cfg / "taos.cfg").write_text(
                (cfg / "taos.cfg").read_text()
                .replace("fqdn localhost", "fqdn 127.0.0.1")
                .replace(f"firstEp localhost:{port}", f"firstEp 127.0.0.1:{port}"),
                encoding="utf-8")
            self.set_local_cfg(data, fqdn="127.0.0.1",
                               firstEp=f"127.0.0.1:{port}")

            result = self.run_to_exit(root, cfg)
            output = result.stdout + result.stderr
            assert "need to be reconfigured" in output, output
            assert "--rewrite-endpoints-apply" in output, output
        print("legacy ep.json ignored and reported .................. [passed]")

    def do_startup_guard(self):
        with tempfile.TemporaryDirectory(prefix="rw-guard-") as tmp:
            root = Path(tmp)
            disk = root / "disk"
            cfg, data, logs, port = self.write_instance(root, disk)

            # A first run creates the data directory.
            self.case.serve_once(root, cfg, port)

            # A backup next to taos.cfg, under the data directory, and under a
            # vnode on a secondary disk must all stop the startup.
            leftovers = [
                (cfg / "taos.cfg.before-rewrite-endpoints", None),
                (data / "mnode/data/sdb.data.before-rewrite-endpoints", None),
                (disk / "vnode/vnode2/sync/raft_config.json.before-rewrite-endpoints",
                 disk / "vnode"),
            ]
            for leftover, cleanup in leftovers:
                leftover.parent.mkdir(parents=True, exist_ok=True)
                leftover.write_bytes(b"backup")
                result = self.run_to_exit(root, cfg)
                output = result.stdout + result.stderr
                assert result.returncode != 0, (leftover, output)
                assert "backup left behind" in output, output
                assert str(leftover) in output, output
                assert "Changing the Address of a Node" in output, output
                leftover.unlink()
                if cleanup is not None:
                    shutil.rmtree(cleanup, ignore_errors=True)

            # A half done rewrite must not be inspected or continued through the
            # offline commands either: precheck and apply are refused as well.
            leftover = cfg / "taos.cfg.before-rewrite-endpoints"
            leftover.write_bytes(b"backup")
            reports = root / "reports"
            reports.mkdir()
            content = self.case.content_state(root)
            result = subprocess.run(
                [self.binary, "-c", str(cfg), "--rewrite-endpoints-precheck",
                 str(reports), "--map", f"localhost:{port}=localhost:{port}"],
                cwd=root, env=self.case.native_environment(), capture_output=True,
                text=True, timeout=180)
            output = result.stdout + result.stderr
            assert result.returncode != 0, output
            assert "precheck failed: 1 endpoint rewrite backup" in output, output
            assert str(leftover) in output, output
            assert not (reports / "precheck.dnode1.json").exists(), output
            assert self.case.content_state(root) == content

            result = subprocess.run(
                [self.binary, "-c", str(cfg), "--rewrite-endpoints-apply",
                 str(reports)],
                cwd=root, env=self.case.native_environment(), capture_output=True,
                text=True, timeout=180)
            output = result.stdout + result.stderr
            assert result.returncode != 0, output
            assert "apply failed: 1 endpoint rewrite backup" in output, output
            assert self.case.content_state(root) == content
            leftover.unlink()

            # Once the backups are gone the instance starts again.
            self.case.serve_once(root, cfg, port)
        print("leftover backups block start, precheck and apply .... [passed]")

    @pytest.mark.skipif(os.name != "posix",
                        reason="requires graceful POSIX daemon shutdown")
    def test_rewrite_endpoint_ep_json_retired(self):
        """Legacy ep.json is ignored and reported

        1. Start a private taosd and stop it normally.
        2. Write a legacy ep.json that maps the local endpoint elsewhere.
        3. Start taosd again: the endpoint must not change.
        4. The log must warn about the leftover ep.json, name its path and
           point at the offline rewrite commands.
        5. Changing the endpoint by hand without the command must fail and
           point at the offline rewrite commands as well.

        Catalog:
            - Others

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-27 Bomin Zhang Add ep.json retirement coverage.

        """
        self.do_ep_json_retired()

    @pytest.mark.skipif(os.name != "posix",
                        reason="requires graceful POSIX daemon shutdown")
    def test_rewrite_endpoint_startup_guard(self):
        """Leftover rewrite backups block start, precheck and apply

        1. Start a private taosd with a secondary level-0 disk and stop it.
        2. Leave a backup next to taos.cfg, under the data directory and under
           a vnode on the secondary disk: startup must stop each time.
        3. The report must name every leftover file and point at the manual.
        4. The offline precheck and apply commands must be refused as well, and
           must not write a report or modify any file.
        5. Remove the backups: the instance starts again.

        Catalog:
            - Others

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-28 Bomin Zhang Add leftover backup guard for start, precheck
              and apply.

        """
        self.do_startup_guard()
