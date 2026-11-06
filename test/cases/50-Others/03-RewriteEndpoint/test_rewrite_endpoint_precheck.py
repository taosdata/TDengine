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

from new_test_framework.utils import tdLog, tdSql, etool

from contextlib import contextmanager
import ctypes
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import shlex
import socket
import struct
import subprocess
import sys
import tempfile
import time

import pytest


class TestRewriteEndpointPrecheck:
    sync_noop = 0x0617
    alter_confirm = 0x024B
    leader_transfer = 0x0627
    mnode_apply = 0x019D

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        root = Path(__file__).resolve().parents[4]
        candidates = [
            os.environ.get("TAOSD_BIN"),
            root.parents[1] / "debug/build/bin/taosd",
            root / "debug/build/bin/taosd",
            shutil.which("taosd"),
        ]
        cls.binary = next(
            str(path) for path in candidates if path and Path(path).is_file()
        )

    def write_json(self, path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value), encoding="utf-8")

    def crc32c(self, data):
        crc = 0xFFFFFFFF
        for byte in data:
            crc ^= byte
            for _ in range(8):
                crc = (crc >> 1) ^ (0x82F63B78 if crc & 1 else 0)
        return crc ^ 0xFFFFFFFF

    def write_wal(self, directory, version=10, term=2, disk_id=-1):
        self.write_wal_records(
            directory, [(version, 0, b"endpoint-precheck", 0)],
            snapshot=version, term=term, disk_id=disk_id,
        )

    def write_wal_records(self, directory, records, snapshot, term=2, disk_id=-1):
        directory.mkdir(parents=True, exist_ok=True)
        log, index = b"", b""
        for version, msg_type, body, flags in records:
            head = struct.pack("<qqihBbQQ", version, 0, len(body),
                               msg_type, flags, 0, 0, term)
            index += struct.pack("<qq", version, len(log))
            log += (struct.pack("<QII", 0xFAFBFCFDF4F3F2F1,
                                self.crc32c(head), self.crc32c(body))
                    + head + body)
        first, last = records[0][0], records[-1][0]
        (directory / f"{first:020}.log").write_bytes(log)
        (directory / f"{first:020}.idx").write_bytes(index)
        meta = {
            "meta": {
                "firstVer": str(first),
                "snapshotVer": str(snapshot),
                "commitVer": str(last),
                "lastVer": str(last),
                "keepVersion": "-1",
            },
            "files": [{
                "firstVer": str(first), "lastVer": str(last),
                "createTs": "0", "closeTs": "0", "fileSize": str(len(log)),
                "diskLevel": 0, "diskId": disk_id,
            }],
        }
        self.write_json(directory / "meta-ver1", meta)

    def make_fixture(self, root, mnode=False, multi_disk=False, snode=False):
        cfg, data, reports = root / "cfg", root / "data", root / "reports"
        cfg.mkdir()
        data.mkdir()
        reports.mkdir()
        (data / ".running").write_bytes(b"existing-lock-content")
        nodes = [
            {"id": 1, "fqdn": "old-a.invalid", "port": 6030, "isMnode": int(mnode)},
            {"id": 2, "fqdn": "old-b.invalid", "port": 6030, "isMnode": int(mnode)},
        ]
        settings = {
            "fqdn": "old-a.invalid", "serverPort": 6030,
            "firstEp": "old-a.invalid:6030", "secondEp": "old-b.invalid:6030",
        }
        disk_cfg = f"dataDir {data} 0 1\n"
        disk_id = 0
        vnode_disk = data
        if multi_disk:
            vnode_disk = root / "second"
            vnode_disk.mkdir()
            disk_cfg += f"dataDir {vnode_disk} 0 0\n"
            disk_id = 1
        (cfg / "taos.cfg").write_text(
            disk_cfg + "\n".join(f"{k} {v}" for k, v in settings.items()) + "\n",
            encoding="utf-8",
        )
        self.write_json(data / "dnode/dnode.json", {
            "dnodeId": 1, "clusterId": "12345", "dropped": 0, "dnodes": nodes,
        })
        self.write_json(data / "dnode/config/local.json", {"configs": settings})
        self.write_json(data / "vnode/vnodes.json", {"vnodes": [{
            "vgId": 2, "diskPrimary": disk_id, "dropped": 0,
            "vgVersion": 1,
        }]})
        members = [{
            "nodeId": str(node["id"]), "clusterId": "12345",
            "nodeFqdn": node["fqdn"], "nodePort": str(node["port"]),
            "isReplica": "true",
        } for node in nodes]
        raft = {"RaftCfg": {
            "SSyncCfg": {"changeVersion": 1, "nodeInfo": members},
            "lastConfigIndex": "0",
        }}
        vnode = vnode_disk / "vnode/vnode2"
        self.write_json(vnode / "vnode.json", {
            "config": {
                "vgId": "2", "syncCfg.nodeInfo": members,
                "syncCfg.changeVersion": "1",
                "hashChange": "0",
                "wal.encryptAlgorithm": "0", "wal.encryptAlgrName": "",
            },
            "state": {"commit version": "10", "commit term": "2", "commit ID": "7"},
        })
        self.write_json(vnode / "sync/raft_config.json", raft)
        self.write_wal(vnode / "wal", disk_id=disk_id if multi_disk else -1)
        if mnode:
            self.write_json(data / "mnode/mnode.json", {
                "deployed": 1, "encrypted": 0,
                "replicas": [{"id": n["id"], "fqdn": n["fqdn"], "port": n["port"]}
                             for n in nodes],
            })
            mnode_raft = json.loads(json.dumps(raft))
            for member in mnode_raft["RaftCfg"]["SSyncCfg"]["nodeInfo"]:
                member["clusterId"] = "0"
            self.write_json(data / "mnode/sync/raft_config.json", mnode_raft)
            self.write_wal(data / "mnode/wal")
            self.write_sdb(data / "mnode/data/sdb.data", nodes)
        if snode:
            self.write_json(data / "snode/snode1/snode.json", {
                "snodeId": "1",
                **{key: {
                    "nodeId": str(nodes[index]["id"]), "numOfEps": "1",
                    "eps": [{"fqdn": nodes[index]["fqdn"], "port": 6030}],
                } for key, index in [("leader0", 0), ("leader1", 1), ("replica", 1)]},
            })
        return cfg, data, reports, vnode

    def write_sdb(self, path, nodes, unfinished=False):
        # The persisted SDB header is 928 bytes, independent of SDB_MAX.
        snapshot = struct.pack("<qqqq", 1, 10, 2, 0) + bytes(896)
        for node in nodes:
            payload = (
                struct.pack("<iqqH", node["id"], 0, 0, node["port"])
                + node["fqdn"].encode().ljust(128, b"\0") + bytes(24 + 40)
            )
            raw = struct.pack("<bbbbI", 6, 4, 1, 0, len(payload)) + payload
            snapshot += raw + struct.pack("<I", self.crc32c(raw))
        if unfinished:
            raw = struct.pack("<bbbbI", 0, 4, 1, 0, 0)
            snapshot += raw + struct.pack("<I", self.crc32c(raw))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(snapshot)

    def source_state(self, root):
        return {
            str(p.relative_to(root)): (
                hashlib.sha256(p.read_bytes()).hexdigest(), p.stat().st_mode,
                p.stat().st_mtime_ns,
            ) if p.is_file() else None
            for p in root.rglob("*") if "reports" not in p.relative_to(root).parts
        }

    def run_precheck(self, cfg, reports, extra=None):
        args = [
            self.binary, "-c", str(cfg), "--rewrite-endpoints-precheck",
            str(reports), "--map", "old-a.invalid:6030=localhost:7030",
        ]
        if extra:
            args += extra
        return subprocess.run(args, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, text=True, timeout=120,
                              cwd=cfg.parent, env=self.native_environment())

    def read_report(self, reports, dnode_id=1):
        name = f"precheck.dnode{dnode_id}.json"
        assert {path.name for path in reports.iterdir()} == {name}
        report = json.loads((reports / name).read_text())
        assert report["dnodeId"] == dnode_id
        assert "changes" not in report
        assert set(report) <= {
            "version", "status", "generatedAt", "clusterId", "dnodeId",
            "mapping", "mnode", "vnodes", "errors",
        }
        groups = report.get("vnodes", []) + ([report["mnode"]]
                    if report.get("mnode") is not None else [])
        for group in groups:
            assert "configVersion" not in group and "configIndex" not in group
            if report["status"] == "passed":
                assert "safeLowerBound" in group
                assert max(-1, int(group["snapshotIndex"]) - 20) <= int(
                    group["safeLowerBound"]) <= int(group["snapshotIndex"])
        return report

    def do_passed_report(self):
        with tempfile.TemporaryDirectory(prefix="rewrite-precheck-") as tmp:
            root = Path(tmp)
            cfg, data, reports, vnode = self.make_fixture(root, mnode=True, snode=True)
            before = self.source_state(root)
            result = self.run_precheck(cfg, reports)
            assert result.returncode == 0, result.stdout
            report = self.read_report(reports)
            assert report["status"] == "passed"
            assert report["version"] and report["clusterId"] == "12345"
            assert report["mnode"]["snapshotIndex"] == "10"
            assert report["vnodes"][0]["snapshotIndex"] == "10"
            assert report["vnodes"][0]["snapshotTerm"] == "2"
            assert report["mapping"][1]["source"] == report["mapping"][1]["target"]
            assert not report["errors"]
            assert "old-a.invalid:6030 -> localhost:7030" in result.stdout
            assert "SDB_DNODE.fqdn/port" in result.stdout
            assert "snode.json" in result.stdout
            assert "leader0: old-a.invalid:6030 -> localhost:7030" in result.stdout
            assert "ENDPOINTS REWRITE PREVIEW:" in result.stdout
            assert re.search(r"endpoint rewrite preview: \d+ endpoints in \d+ files",
                             result.stdout)
            assert result.stdout.index("endpoint rewrite preview:") < result.stdout.index(
                "PRECHECK RESULT: ")
            assert "PRECHECK RESULT: " in result.stdout
            assert self.source_state(root) == before
            first = report["mapping"]
            result = self.run_precheck(cfg, reports)
            assert result.returncode == 0, result.stdout
            assert self.read_report(reports)["mapping"] == first
            assert "old-a.invalid:6030 -> localhost:7030" in result.stdout
            assert self.source_state(root) == before
        print("passed report, preview and read-only repeat ........ [passed]")

    def do_rejected_states(self):
        mutations = [
            ("unflushed", lambda cfg, data, vnode: self.update_json(
                vnode / "vnode.json", lambda j: j["state"].update({"commit version": "9"}))),
            ("uncommitted", lambda cfg, data, vnode: self.update_json(
                vnode / "wal/meta-ver1", lambda j: j["meta"].update({"lastVer": "11"}))),
            ("term", lambda cfg, data, vnode: self.update_json(
                vnode / "vnode.json", lambda j: j["state"].update({"commit term": "3"}))),
            ("torn-log", lambda cfg, data, vnode:
                (vnode / "wal/00000000000000000010.log").write_bytes(b"short")),
            ("missing-index", lambda cfg, data, vnode:
                (vnode / "wal/00000000000000000010.idx").unlink()),
            ("extra-log", lambda cfg, data, vnode:
                (vnode / "wal/00000000000000000011.log").write_bytes(b"extra")),
            ("extra-vnode", lambda cfg, data, vnode:
                (data / "vnode/vnode3").mkdir()),
            ("pending-meta", lambda cfg, data, vnode:
                (vnode / "wal/meta-ver.tmp").write_text("{}")),
            ("bad-latest-meta", lambda cfg, data, vnode:
                (vnode / "wal/meta-ver2").write_text("{")),
            ("malformed-version", lambda cfg, data, vnode: self.update_json(
                vnode / "wal/meta-ver1", lambda j: j["meta"].update({"lastVer": "10junk"}))),
            ("missing-config", lambda cfg, data, vnode: (vnode / "vnode.json").unlink()),
            ("wrong-address", lambda cfg, data, vnode: self.update_json(
                data / "dnode/config/local.json",
                lambda j: j["configs"].update({"fqdn": "wrong.invalid"}))),
        ]
        for name, mutate in mutations:
            with tempfile.TemporaryDirectory(prefix="rewrite-precheck-") as tmp:
                root = Path(tmp)
                cfg, data, reports, vnode = self.make_fixture(root)
                mutate(cfg, data, vnode)
                before = self.source_state(root)
                result = self.run_precheck(cfg, reports)
                assert result.returncode != 0, (name, result.stdout)
                report = self.read_report(reports)
                assert report["status"] == "failed" and report["errors"], name
                if name == "unflushed":
                    errors = [error for error in report["errors"]
                              if error["check"] == "snapshot/WAL boundaries"]
                    assert errors and errors[0]["file"].endswith("/wal")
                    assert "S=" in errors[0]["message"] and "C=" in errors[0]["message"]
                if name == "missing-config":
                    assert any(error["file"] == str(vnode / "vnode.json")
                               for error in report["errors"])
                assert self.source_state(root) == before, name
        print("invalid configuration and WAL boundaries .......... [passed]")

    def update_json(self, path, update):
        value = json.loads(path.read_text())
        update(value)
        self.write_json(path, value)

    def run_precheck_args(self, cfg, reports, args):
        reports.mkdir(exist_ok=True)
        return subprocess.run(
            [self.binary, "-c", str(cfg), "--rewrite-endpoints-precheck",
             str(reports)] + args,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            timeout=120, cwd=cfg.parent, env=self.native_environment())

    def do_mapping_collisions(self):
        # A new endpoint that equals the endpoint of a dnode that is not being
        # changed leaves two dnodes with the same address, so it is rejected.
        with tempfile.TemporaryDirectory(prefix="rewrite-precheck-") as tmp:
            root = Path(tmp)
            cfg, data, reports, vnode = self.make_fixture(root)
            before = self.source_state(root)
            result = self.run_precheck_args(cfg, reports, [
                "--map", "old-a.invalid:6030=old-b.invalid:6030",
            ])
            assert result.returncode != 0, result.stdout
            assert "unique final endpoints" in result.stdout, result.stdout
            report = self.read_report(reports)
            assert report["status"] == "failed" and report["errors"]
            assert self.source_state(root) == before

        # The command line itself rejects two mappings for the same endpoint,
        # so no report is written at all.
        for name, args, message in [
            ("duplicate-source", ["--map", "old-a.invalid:6030=localhost:7030",
                                  "--map", "old-a.invalid:6030=localhost:7031"],
             "duplicate source or target endpoint"),
            ("duplicate-target", ["--map", "old-a.invalid:6030=localhost:7030",
                                  "--map", "old-b.invalid:6030=localhost:7030"],
             "duplicate source or target endpoint"),
        ]:
            with tempfile.TemporaryDirectory(prefix="rewrite-precheck-") as tmp:
                root = Path(tmp)
                cfg, data, reports, vnode = self.make_fixture(root)
                before = self.source_state(root)
                result = self.run_precheck_args(cfg, reports, args)
                assert result.returncode != 0, (name, result.stdout)
                assert message in result.stdout, (name, result.stdout)
                assert not list(reports.iterdir()), name
                assert self.source_state(root) == before, name

        # Changing both dnodes to two distinct new addresses is consistent:
        # the final set is unique, so the check has to accept it.
        with tempfile.TemporaryDirectory(prefix="rewrite-precheck-") as tmp:
            root = Path(tmp)
            cfg, data, reports, vnode = self.make_fixture(root)
            before = self.source_state(root)
            result = self.run_precheck_args(cfg, reports, [
                "--map", "old-a.invalid:6030=localhost:7030",
                "--map", "old-b.invalid:6030=localhost:7031",
            ])
            assert result.returncode == 0, result.stdout
            report = self.read_report(reports)
            assert [entry["target"] for entry in report["mapping"]] == [
                {"fqdn": "localhost", "port": 7030},
                {"fqdn": "localhost", "port": 7031}], report["mapping"]
            assert self.source_state(root) == before
        print("mapping collisions and duplicate --map entries ........ [passed]")

    def do_mnode_and_mapping_errors(self):
        for name in ["transaction", "sdb-checksum", "source-missing", "target-collision"]:
            with tempfile.TemporaryDirectory(prefix="rewrite-precheck-") as tmp:
                root = Path(tmp)
                cfg, data, reports, vnode = self.make_fixture(root, mnode=True)
                extra = None
                if name == "transaction":
                    nodes = json.loads((data / "dnode/dnode.json").read_text())["dnodes"]
                    self.write_sdb(data / "mnode/data/sdb.data", nodes, unfinished=True)
                elif name == "sdb-checksum":
                    path = data / "mnode/data/sdb.data"
                    path.write_bytes(path.read_bytes()[:-1] + b"\xff")
                elif name == "source-missing":
                    extra = ["--map", "absent.invalid:6030=localhost:7031"]
                else:
                    self.update_json(data / "dnode/dnode.json",
                        lambda j: j["dnodes"][1].update({"fqdn": "localhost", "port": 7030}))
                before = self.source_state(root)
                result = self.run_precheck(cfg, reports, extra)
                assert result.returncode != 0, (name, result.stdout)
                report = self.read_report(reports)
                assert report["status"] == "failed" and report["errors"]
                assert self.source_state(root) == before
        print("mnode transactions, checksum and mapping errors ... [passed]")

    def do_identity_failures(self):
        def remove_self(nodes):
            nodes["dnodes"] = [node for node in nodes["dnodes"] if node["id"] != 1]

        cases = [
            ("missing", lambda data: (data / "dnode/dnode.json").unlink()),
            ("malformed", lambda data: (data / "dnode/dnode.json").write_text("{")),
            ("no-dnodes", lambda data: self.update_json(
                data / "dnode/dnode.json", lambda j: j.update({"dnodes": []}))),
            ("self-absent", lambda data: self.update_json(
                data / "dnode/dnode.json", remove_self)),
            ("dropped", lambda data: self.update_json(
                data / "dnode/dnode.json", lambda j: j.update({"dropped": 1}))),
            ("bad-cluster-id", lambda data: self.update_json(
                data / "dnode/dnode.json", lambda j: j.update({"clusterId": "0"}))),
            ("duplicate-dnode", lambda data: self.update_json(
                data / "dnode/dnode.json", lambda j: j["dnodes"].append(
                    dict(j["dnodes"][1])))),
        ]
        for name, mutate in cases:
            with tempfile.TemporaryDirectory(prefix="rewrite-precheck-") as tmp:
                root = Path(tmp)
                cfg, data, reports, vnode = self.make_fixture(root)
                mutate(data)
                before = self.source_state(root)
                result = self.run_precheck(cfg, reports)
                assert result.returncode != 0, (name, result.stdout)
                assert "endpoint rewrite precheck failed" in result.stdout, name
                # The identity is read before any check runs, so no report is
                # written at all rather than a report with stale identity.
                assert not list(reports.iterdir()), name
                assert self.source_state(root) == before, name
        # A file written before `dropped` existed means "not dropped".
        with tempfile.TemporaryDirectory(prefix="rewrite-precheck-") as tmp:
            root = Path(tmp)
            cfg, data, reports, vnode = self.make_fixture(root)
            self.update_json(
                data / "dnode/dnode.json", lambda j: j.pop("dropped"))
            result = self.run_precheck(cfg, reports)
            assert result.returncode == 0, result.stdout
            assert self.read_report(reports)["status"] == "passed"
        print("dnode identity errors stop the precheck ........... [passed]")

    def do_snode_failures(self):
        cases = [
            ("count", lambda snode: snode["leader0"].update({"numOfEps": "2"}),
             "configuration endpoints"),
            ("endpoint", lambda snode: snode["leader0"]["eps"][0].update(
                {"fqdn": "old-b.invalid"}), "leader0"),
            ("identity", lambda snode: snode.update({"snodeId": "2"}),
             "configuration endpoints"),
            ("missing-endpoint", lambda snode: snode["leader1"].update(
                {"eps": []}), "configuration endpoints"),
        ]
        for name, mutate, check in cases:
            with tempfile.TemporaryDirectory(prefix="rewrite-precheck-") as tmp:
                root = Path(tmp)
                cfg, data, reports, vnode = self.make_fixture(root, snode=True)
                self.update_json(data / "snode/snode1/snode.json", mutate)
                before = self.source_state(root)
                result = self.run_precheck(cfg, reports)
                assert result.returncode != 0, (name, result.stdout)
                report = self.read_report(reports)
                assert report["status"] == "failed" and report["errors"], name
                assert any(error["check"] == check
                           for error in report["errors"]), (name, report["errors"])
                assert self.source_state(root) == before, name
        print("snode endpoint and identity errors are rejected ...... [passed]")

    def do_multi_disk_and_trimmed(self):
        with tempfile.TemporaryDirectory(prefix="rewrite-precheck-") as tmp:
            root = Path(tmp)
            cfg, data, reports, vnode = self.make_fixture(root, multi_disk=True)
            before = self.source_state(root)
            result = self.run_precheck(cfg, reports)
            assert result.returncode == 0, result.stdout
            assert self.source_state(root) == before
            wal = vnode / "wal"
            (wal / "00000000000000000010.log").unlink()
            (wal / "00000000000000000010.idx").unlink()
            self.write_json(wal / "meta-ver1", {
                "meta": {"firstVer": "11", "lastVer": "10", "snapshotVer": "10", "commitVer": "10"},
                "files": [],
            })
            before = self.source_state(root)
            result = self.run_precheck(cfg, reports)
            assert result.returncode == 0, result.stdout
            assert self.source_state(root) == before
            self.write_json(wal / "meta-ver1", {
                "meta": {"firstVer": "11", "lastVer": "10", "snapshotVer": "10", "commitVer": "10"},
                "files": [{"firstVer": "11", "lastVer": "-1", "fileSize": "0",
                           "diskLevel": 0, "diskId": 1}],
            })
            (wal / "00000000000000000011.log").write_bytes(b"")
            (wal / "00000000000000000011.idx").write_bytes(b"")
            before = self.source_state(root)
            result = self.run_precheck(cfg, reports)
            assert result.returncode == 0, result.stdout
            assert self.source_state(root) == before
        print("multi-disk vnode and trimmed WAL .................. [passed]")

    def do_running_and_large_versions(self):
        with tempfile.TemporaryDirectory(prefix="rewrite-precheck-") as tmp:
            root = Path(tmp)
            cfg, data, reports, vnode = self.make_fixture(root)
            if os.name == "posix":
                import fcntl

                with (data / ".running").open("rb") as lock:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    before = self.source_state(root)
                    result = self.run_precheck(cfg, reports)
                    assert result.returncode != 0, result.stdout
                    report = self.read_report(reports)
                    assert report["status"] == "failed"
                    assert "instance stopped and locked" in result.stdout
                    assert self.source_state(root) == before
            large = 9007199254740993
            self.write_wal(vnode / "wal", version=large, term=large)
            (vnode / "wal/00000000000000000010.log").unlink()
            (vnode / "wal/00000000000000000010.idx").unlink()
            self.update_json(vnode / "vnode.json", lambda j: j["state"].update({
                "commit version": str(large), "commit term": str(large),
            }))
            before = self.source_state(root)
            result = self.run_precheck(cfg, reports)
            assert result.returncode == 0, result.stdout
            group = self.read_report(reports)["vnodes"][0]
            assert group["snapshotIndex"] == str(large)
            assert group["snapshotTerm"] == str(large)
            assert self.source_state(root) == before
        print("running instance refusal and 64-bit precision ..... [passed]")

    def do_empty_vnode_inventory(self):
        with tempfile.TemporaryDirectory(prefix="rewrite-precheck-") as tmp:
            root = Path(tmp)
            cfg, data, reports, vnode = self.make_fixture(root, mnode=True)
            shutil.rmtree(vnode)
            (data / "vnode/vnodes.json").unlink()
            before = self.source_state(root)
            result = self.run_precheck(cfg, reports)
            assert result.returncode == 0, result.stdout
            report = self.read_report(reports)
            assert report["vnodes"] == []
            assert self.source_state(root) == before
        print("empty vnode directory without inventory .......... [passed]")

    def do_safe_wal_tails(self):
        header = struct.pack("<ii", 8, 2)
        historical = (10, 0, b"persisted-business-record", 0)
        noop = (11, self.sync_noop, header, 0)
        confirm = (12, self.alter_confirm, header, 0)
        last = (13, self.sync_noop, header, 0)
        cases = [
            ("noop", [historical, noop], True),
            ("noop-confirm-noop", [historical, noop, confirm, last], True),
            ("unsafe-middle", [historical, noop, (12, 0, header, 0), last], False),
            ("wrong-vgid", [historical, (11, self.sync_noop, struct.pack("<ii", 8, 3), 0)], False),
            ("wrong-header-length", [historical, (11, self.sync_noop, struct.pack("<ii", 9, 2), 0)], False),
            ("wrong-body-length", [historical, (11, self.sync_noop, header + b"x", 0)], False),
            ("transaction-flag", [historical, (11, self.sync_noop, bytes(16) + header, 128)], False),
            ("hash-change", [historical, noop, confirm, last], False),
            ("hash-change-missing", [historical, noop, confirm, last], False),
            ("uncommitted", [historical, noop, confirm, last], True),
            ("missing-tail", [(12, self.sync_noop, header, 0)], False),
            ("bad-middle-checksum", [historical, noop, confirm, last], False),
            ("bad-middle-index", [historical, noop, confirm, last], False),
            ("multi-segment", [historical, noop, confirm, last], True),
        ]
        for name, records, passed in cases:
            with tempfile.TemporaryDirectory(prefix="rewrite-precheck-") as tmp:
                root = Path(tmp)
                cfg, data, reports, vnode = self.make_fixture(root)
                wal = vnode / "wal"
                if name == "missing-tail":
                    (wal / "00000000000000000010.log").unlink()
                    (wal / "00000000000000000010.idx").unlink()
                if name == "multi-segment":
                    self.write_wal_records(wal, records[:2], snapshot=10)
                    first_meta = json.loads((wal / "meta-ver1").read_text())
                    self.write_wal_records(wal, records[2:], snapshot=10)
                    self.update_json(wal / "meta-ver1", lambda j: (
                        j["meta"].update({"firstVer": "10"}),
                        j["files"].insert(0, first_meta["files"][0]),
                    ))
                else:
                    self.write_wal_records(wal, records, snapshot=10)
                if name == "hash-change":
                    self.update_json(vnode / "vnode.json",
                                     lambda j: j["config"].update({"hashChange": "1"}))
                elif name == "hash-change-missing":
                    self.update_json(vnode / "vnode.json",
                                     lambda j: j["config"].pop("hashChange"))
                elif name == "uncommitted":
                    self.update_json(wal / "meta-ver1",
                                     lambda j: j["meta"].update({"commitVer": "12"}))
                elif name in {"bad-middle-checksum", "bad-middle-index"}:
                    index_path = wal / "00000000000000000010.idx"
                    index = bytearray(index_path.read_bytes())
                    _, middle_offset = struct.unpack_from("<qq", index, 2 * 16)
                    if name == "bad-middle-checksum":
                        path = wal / "00000000000000000010.log"
                        log = bytearray(path.read_bytes())
                        log[middle_offset + 56] ^= 1
                        path.write_bytes(log)
                    else:
                        struct.pack_into("<q", index, 2 * 16 + 8, middle_offset + 1)
                        index_path.write_bytes(index)
                before = self.source_state(root)
                result = self.run_precheck(cfg, reports)
                report = self.read_report(reports)
                assert (result.returncode == 0) == passed, (name, result.stdout)
                assert report["status"] == ("passed" if passed else "failed"), name
                assert self.source_state(root) == before, name
        for msg_type, passed in [(self.sync_noop, True), (self.alter_confirm, False)]:
            with tempfile.TemporaryDirectory(prefix="rewrite-precheck-") as tmp:
                root = Path(tmp)
                cfg, data, reports, _ = self.make_fixture(root, mnode=True)
                self.write_wal_records(data / "mnode/wal", [
                    historical, (11, msg_type, struct.pack("<ii", 8, 1), 0),
                ], snapshot=10)
                before = self.source_state(root)
                result = self.run_precheck(cfg, reports)
                assert (result.returncode == 0) == passed, result.stdout
                assert self.source_state(root) == before
        print("safe committed tails and unsafe tail rejection ... [passed]")

    def leader_transfer_body(self, vg_id):
        class NodeInfo(ctypes.Structure):
            _fields_ = [
                ("cluster_id", ctypes.c_int64), ("node_id", ctypes.c_int32),
                ("port", ctypes.c_uint16), ("fqdn", ctypes.c_char * 128),
                ("role", ctypes.c_int),
            ]

        class RaftId(ctypes.Structure):
            _fields_ = [("address", ctypes.c_uint64), ("vg_id", ctypes.c_int32)]

        class Transfer(ctypes.Structure):
            _fields_ = [
                ("size", ctypes.c_uint32), ("vg_id", ctypes.c_int32),
                ("msg_type", ctypes.c_uint32), ("node", NodeInfo),
                ("leader", RaftId),
            ]

        value = Transfer()
        value.size = ctypes.sizeof(value)
        value.vg_id = vg_id
        value.msg_type = self.leader_transfer
        value.node.cluster_id, value.node.node_id = 12345, 1
        value.node.port, value.node.fqdn = 6030, b"old-a.invalid"
        value.leader.address = (12345 << 32) | 1
        value.leader.vg_id = vg_id
        return bytes(value)

    def do_safe_lookback(self):
        header = struct.pack("<ii", 8, 2)
        cases = [
            ("at-limit", [(v, self.sync_noop, header, 0) for v in range(80, 107)],
             105, 85, True),
            ("unsafe-boundary",
             [(100, 0, b"business", 0)] +
             [(v, self.sync_noop, header, 0) for v in range(101, 108)],
             105, 100, True),
            ("retained-limit", [(105, self.sync_noop, header, 0)],
             105, 104, True),
            ("unsafe-at-s", [(105, 0, b"business", 0),
                             (106, self.sync_noop, header, 0)], 105, 105, True),
            ("uncommitted-transfer", [
                (105, 0, b"business", 0),
                (106, self.leader_transfer, self.leader_transfer_body(2), 0),
                (107, self.leader_transfer, self.leader_transfer_body(2), 0),
             ], 105, 105, True),
            ("invalid-transfer", [
                (105, 0, b"business", 0),
                (106, self.leader_transfer, self.leader_transfer_body(3), 0),
             ], 105, None, False),
            ("bad-backward-crc", [
                (100, 0, b"business", 0),
                *[(v, self.sync_noop, header, 0) for v in range(101, 108)],
             ], 105, None, False),
        ]
        for name, records, snapshot, expected, passed in cases:
            with tempfile.TemporaryDirectory(prefix="rewrite-precheck-") as tmp:
                root = Path(tmp)
                cfg, data, reports, vnode = self.make_fixture(root)
                wal = vnode / "wal"
                for path in [wal / "00000000000000000010.log",
                             wal / "00000000000000000010.idx"]:
                    path.unlink()
                self.write_wal_records(wal, records, snapshot=snapshot)
                self.update_json(vnode / "vnode.json", lambda j: j["state"].update({
                    "commit version": str(snapshot),
                }))
                # These fields are neither needed nor emitted by precheck.
                self.update_json(vnode / "vnode.json",
                                 lambda j: j["config"].pop("syncCfg.changeVersion"))
                self.update_json(vnode / "sync/raft_config.json", lambda j: (
                    j["RaftCfg"].pop("lastConfigIndex"),
                    j["RaftCfg"]["SSyncCfg"].pop("changeVersion"),
                ))
                if name == "uncommitted-transfer":
                    self.update_json(wal / "meta-ver1", lambda j:
                                     j["meta"].update({"commitVer": "105"}))
                elif name == "bad-backward-crc":
                    idx = (wal / "00000000000000000100.idx").read_bytes()
                    _, offset = struct.unpack_from("<qq", idx, 3 * 16)
                    log_path = wal / "00000000000000000100.log"
                    log = bytearray(log_path.read_bytes())
                    log[offset + 56] ^= 1
                    log_path.write_bytes(log)
                before = self.source_state(root)
                result = self.run_precheck(cfg, reports)
                report = self.read_report(reports)
                assert (result.returncode == 0) == passed, (name, result.stdout)
                if passed:
                    assert report["vnodes"][0]["safeLowerBound"] == str(expected), name
                assert self.source_state(root) == before
        for vg_id in [1, 2]:
            with tempfile.TemporaryDirectory(prefix="rewrite-precheck-") as tmp:
                root = Path(tmp)
                cfg, data, reports, vnode = self.make_fixture(root, mnode=True)
                wal = data / "mnode/wal" if vg_id == 1 else vnode / "wal"
                self.write_wal_records(wal, [
                    (10, self.leader_transfer, self.leader_transfer_body(vg_id), 0),
                    (11, self.sync_noop, struct.pack("<ii", 8, vg_id), 0),
                ], snapshot=10)
                result = self.run_precheck(cfg, reports)
                assert result.returncode == 0, result.stdout
                report = self.read_report(reports)
                group = report["mnode"] if vg_id == 1 else report["vnodes"][0]
                assert group["safeLowerBound"] == "9"
        print("20-record lookback, trim boundary and transfers ... [passed]")

    def do_encrypted_config(self):
        taosk = Path(self.binary).with_name("taosk")
        version = subprocess.run([self.binary, "-V"], capture_output=True,
                                 text=True, timeout=10)
        if os.name != "posix" or not taosk.is_file() or "Enterprise" not in version.stdout:
            tdLog.info("enterprise encryption fixture tools unavailable")
            return
        with tempfile.TemporaryDirectory(prefix="rewrite-precheck-") as tmp:
            root = Path(tmp)
            cfg, data, reports, vnode = self.make_fixture(root, mnode=True)
            result = subprocess.run([
                str(taosk), "-d", str(data),
                "--encrypt-server", "fixture-svr-key",
                "--encrypt-database", "fixture-db-key",
                "--encrypt-config", "--encrypt-metadata",
            ], capture_output=True, text=True, timeout=120, cwd=root,
                env=self.native_environment())
            assert result.returncode == 0, "fixture key generation failed"
            editor = shlex.join([
                sys.executable, "-c",
                'import pathlib,sys; p=pathlib.Path(sys.argv[1]); '
                'p.write_bytes(p.read_bytes()+b"\\n")',
            ])
            for path in [
                data / "dnode/config/local.json",
                vnode / "vnode.json", vnode / "sync/raft_config.json",
                vnode / "wal/meta-ver1", data / "mnode/mnode.json",
            ]:
                result = subprocess.run([
                    str(taosk), "-d", str(data), "--edit-file", str(path),
                ], env={**self.native_environment(), "EDITOR": editor}, input="",
                    capture_output=True, text=True, timeout=120, cwd=root)
                assert result.returncode == 0, "encrypted fixture write failed"
                assert path.read_bytes().startswith(b"tdEncrypt")
            before = self.source_state(root)
            result = self.run_precheck(cfg, reports)
            assert result.returncode == 0, result.stdout
            assert self.source_state(root) == before
            report = self.read_report(reports)
            assert report["status"] == "passed"
        print("CFG_KEY decryption without source migration ...... [passed]")

    def native_environment(self):
        """Environment for the isolated instance and for its helper processes.

        The cases start their own instance and drive it from short-lived Python
        children. Under the CI ASan build both keep allocations reachable at
        exit, so leak detection is turned off here: it reports the interpreter's
        own objects and the instance's lifetime allocations, which says nothing
        about the code under test but does make every child exit non-zero.
        Address errors are still reported.
        """
        env = {key: value for key, value in os.environ.items()
               if not key.startswith("TAOS_")}
        library_dir = str(Path(self.binary).parent.parent / "lib")
        env["LD_LIBRARY_PATH"] = library_dir + os.pathsep + env.get("LD_LIBRARY_PATH", "")
        asan = [option for option in env.get("ASAN_OPTIONS", "").split(":")
                if option and not option.startswith("detect_leaks=")]
        asan.append("detect_leaks=0")
        env["ASAN_OPTIONS"] = ":".join(asan)
        return env

    @contextmanager
    def native_daemon(self, root, cfg, port):
        with (root / "native-startup.log").open("a") as output:
            process = subprocess.Popen(
                [self.binary, "-c", str(cfg)], cwd=root,
                env=self.native_environment(), stdout=output,
                stderr=subprocess.STDOUT,
            )
            try:
                deadline = time.monotonic() + 120
                while time.monotonic() < deadline:
                    if process.poll() is not None:
                        raise AssertionError((root / "native-startup.log").read_text()[-4000:])
                    identity = root / "data/dnode/dnode.json"
                    if identity.exists():
                        try:
                            with socket.create_connection(("localhost", port), timeout=0.2):
                                break
                        except (ConnectionRefusedError, TimeoutError):
                            pass
                    time.sleep(0.1)
                else:
                    raise AssertionError("isolated native instance did not become ready")
                yield
            finally:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=120)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
                        raise
                assert process.returncode == 0, "isolated native instance did not stop normally"

    def native_sql(self, root, cfg, port, statements):
        script = """
import json
import sys
import taos
connection = taos.connect(host="localhost", port=int(sys.argv[2]), config=sys.argv[1])
try:
    cursor = connection.cursor()
    try:
        for statement in json.loads(sys.argv[3]):
            cursor.execute(statement)
    finally:
        cursor.close()
finally:
    connection.close()
"""
        result = subprocess.run(
            [sys.executable, "-c", script, str(cfg), str(port), json.dumps(statements)],
            cwd=root, env=self.native_environment(), capture_output=True,
            text=True, timeout=180,
        )
        assert result.returncode == 0, result.stdout + result.stderr

    def native_precheck(self, root, cfg, reports, port):
        before = self.source_state(root)
        result = subprocess.run([
            self.binary, "-c", str(cfg), "--rewrite-endpoints-precheck",
            str(reports), "--map", f"localhost:{port}=localhost:{port + 1}",
        ], cwd=root, env=self.native_environment(), capture_output=True,
            text=True, timeout=120)
        assert self.source_state(root) == before
        report = self.read_report(reports)
        assert report["vnodes"], result.stdout
        for group in report["vnodes"]:
            vg_id = group["vgId"]
            path = root / "data/vnode" / f"vnode{vg_id}"
            state = json.loads((path / "vnode.json").read_text())["state"]
            meta_file = max((path / "wal").glob("meta-ver[0-9]*"),
                            key=lambda p: int(p.name[8:]))
            meta = json.loads(meta_file.read_text())["meta"]
            assert group["snapshotIndex"] == state["commit version"]
            assert group["snapshotTerm"] == state["commit term"]
            assert group["wal"] == {key: meta[key] for key in
                                    ["snapshotVer", "commitVer", "lastVer"]}
        return result, report

    def native_alloc_records(self, data):
        wal = data / "mnode/wal"
        result = []
        for path in sorted(wal.glob("*.log")):
            with path.open("rb") as log:
                while True:
                    header = log.read(56)
                    if not header:
                        break
                    assert len(header) == 56
                    fields = struct.unpack("<QIIqqihBbQQ", header)
                    body = log.read(fields[5])
                    assert len(body) == fields[5]
                    if fields[6] == self.mnode_apply and b"alloc-txn-seq" in body:
                        result.append(body)
        assert result, "native startup did not persist alloc-txn-seq"
        return result

    def do_native_alloc_replay(self, records):
        for source in records[:2]:
            nested = source.index(bytes([51, 4, 1, 0]))  # SDB_TXN_SEQ raw.
            callback = source.index(b"alloc-txn-seq") - 12
            cases = [("original", source, True)]
            wrong_table = bytearray(source)
            wrong_table[nested] = 6  # Same operation name, but a dnode action.
            cases.append(("wrong-table", bytes(wrong_table), False))
            callback_body = bytearray(source)
            struct.pack_into("<i", callback_body, callback, 1)
            cases.append(("callback", bytes(callback_body), False))
            wrong_id = bytearray(source)
            wrong_id[nested + 16] = 2  # Encoded sequence-object ID 1, not 0.
            cases.append(("wrong-sequence-id", bytes(wrong_id), False))
            oversized = bytearray(source)
            struct.pack_into("<i", oversized, nested + 4, len(source))
            cases.append(("oversized-embedded-raw", bytes(oversized), False))
            for name, body, passed in cases:
                with tempfile.TemporaryDirectory(prefix="rewrite-precheck-") as tmp:
                    root = Path(tmp)
                    cfg, data, reports, _ = self.make_fixture(root, mnode=True)
                    self.write_wal_records(data / "mnode/wal", [
                        (10, 0, b"persisted-business-record", 0),
                        (11, self.mnode_apply, body, 0),
                    ], snapshot=10)
                    self.update_json(data / "mnode/wal/meta-ver1",
                                     lambda j: j["meta"].update({"commitVer": "10"}))
                    before = self.source_state(root)
                    result = self.run_precheck(cfg, reports)
                    report = self.read_report(reports)
                    assert (result.returncode == 0) == passed, (name, result.stdout)
                    assert report["status"] == ("passed" if passed else "failed")
                    assert self.source_state(root) == before
        print("native alloc-txn-seq replay and forged variants ... [passed]")

    def do_native_database(self):
        with tempfile.TemporaryDirectory(prefix="rw-native-") as tmp:
            root = Path(tmp)
            cfg, data, logs, reports = [root / part for part in
                                        ["cfg", "data", "logs", "reports"]]
            for path in [cfg, data, logs, reports]:
                path.mkdir()
            with socket.socket() as available:
                available.bind(("127.0.0.1", 0))
                port = available.getsockname()[1]
            (cfg / "taos.cfg").write_text(
                f"dataDir {data}\nlogDir {logs}\nfqdn localhost\nserverPort {port}\n"
                f"firstEp localhost:{port}\nnumOfCores 2\nnumOfRpcThreads 2\n"
                "numOfTaskQueueThreads 4\nudf 0\ntelemetryReporting 0\nmonitor 0\naudit 0\n",
                encoding="utf-8",
            )
            with self.native_daemon(root, cfg, port):
                self.native_sql(root, cfg, port, [
                    "create database rewrite_native keep 36500",
                ])
            result, report = self.native_precheck(root, cfg, reports, port)
            assert result.returncode == 0 and report["status"] == "passed", result.stdout
            assert report["errors"] == []
            assert f"localhost:{port} -> localhost:{port + 1}" in result.stdout
            assert any(int(group["snapshotIndex"]) < int(group["wal"]["lastVer"])
                       for group in report["vnodes"])
            for group in report["vnodes"]:
                print(f"native create-only shutdown: vgId={group['vgId']}, "
                      f"S={group['snapshotIndex']}, C={group['wal']['commitVer']}, "
                      f"L={group['wal']['lastVer']}, status=passed")
            self.do_native_alloc_replay(self.native_alloc_records(data))
            with self.native_daemon(root, cfg, port):
                self.native_sql(root, cfg, port, [
                    "create table rewrite_native.t (ts timestamp, value int)",
                    "insert into rewrite_native.t values (1750046400000, 7)",
                    "flush database rewrite_native",
                ])
            result, report = self.native_precheck(root, cfg, reports, port)
            assert result.returncode == 0 and report["status"] == "passed", result.stdout
        print("native CREATE DATABASE, write, FLUSH and shutdown . [passed]")

    @pytest.mark.skipif(os.name != "posix",
                        reason="requires graceful POSIX daemon shutdown")
    def test_rewrite_endpoint_native_database(self):
        """Native database shutdown prechecks

        1. Start a private taosd, create a real database, then stop normally.
        2. Accept committed noop and eligible alter-confirm tails without flush.
        3. Restart, write fixed-timestamp data, flush, stop and pass precheck.
        4. Verify that every precheck leaves all source files unchanged.
        5. Validate native allocation records and reject unsafe action variants.

        Catalog:
            - Others

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 Bomin Zhang Add native database precheck regression.

        """
        self.do_native_database()

    def test_rewrite_endpoint_precheck(self):
        """Offline endpoint precheck reports

        1. Check successful reports, complete mappings and change previews.
        2. Reject inconsistent configuration, transactions and WAL boundaries.
        3. Verify repeated execution never modifies source files.
        4. Reject a dnode.json that is missing, malformed or inconsistent.
        5. Reject snode files with an invalid endpoint set or identity.
        6. Reject a new endpoint that collides with an unchanged dnode.
        7. Cover secondary disks and legally trimmed logs.
        8. Check instance locking and lossless 64-bit recovery positions.
        9. Accept empty vnode inventories and decrypt encrypted configuration.
        10. Allow only validated, committed safe tails across WAL segments.

        Catalog:
            - Others

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-26 Bomin Zhang Initial offline precheck coverage.

        """
        self.do_passed_report()
        self.do_rejected_states()
        self.do_mnode_and_mapping_errors()
        self.do_identity_failures()
        self.do_snode_failures()
        self.do_mapping_collisions()
        self.do_multi_disk_and_trimmed()
        self.do_running_and_large_versions()
        self.do_empty_vnode_inventory()
        self.do_safe_wal_tails()
        self.do_safe_lookback()
        self.do_encrypted_config()
