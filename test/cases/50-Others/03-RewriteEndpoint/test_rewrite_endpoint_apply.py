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

import test_rewrite_endpoint_precheck as precheck

from new_test_framework.utils import tdLog

import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time

import pytest

DNODE_COUNT = "select count(*) from information_schema.ins_dnodes where status = 'ready'"
MNODE_COUNT = "select count(*) from information_schema.ins_mnodes where status = 'ready'"
DNODE_IDS = "select id from information_schema.ins_dnodes"
VNODE_COUNT = "select count(*) from information_schema.ins_vnodes"


class TestRewriteEndpointApply:
    """Offline endpoint apply, reusing the precheck fixture builder.

    The synthetic fixture and the report readers live in the precheck case, so
    this case only adds the apply helper and its assertions.
    """

    helper = precheck.TestRewriteEndpointPrecheck

    def setup_class(self):
        root = Path(__file__).resolve().parents[4]
        candidates = [
            os.environ.get("TAOSD_BIN"),
            root.parents[1] / "debug/build/bin/taosd",
            root / "debug/build/bin/taosd",
            shutil.which("taosd"),
        ]
        self.binary = next(
            str(path) for path in candidates if path and Path(path).is_file()
        )
        tdLog.debug(f"start to execute {__file__}")

    def make_fixture(self, root, mnode=True, snode=True, multi_disk=False):
        return self.helper.make_fixture(self, root, mnode=mnode, snode=snode,
                                        multi_disk=multi_disk)

    def run_precheck(self, cfg, reports):
        return self.helper.run_precheck(self, cfg, reports)

    def read_report(self, reports, dnode_id=1):
        return self.helper.read_report(self, reports, dnode_id)

    def source_state(self, root):
        return self.helper.source_state(self, root)

    def native_environment(self):
        return self.helper.native_environment(self)

    def native_daemon(self, root, cfg, port):
        return self.helper.native_daemon(self, root, cfg, port)

    def native_sql(self, root, cfg, port, statements, host="localhost"):
        script = """
import json
import sys
import taos
connection = taos.connect(host=sys.argv[4], port=int(sys.argv[2]),
                          config=sys.argv[1])
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
            [sys.executable, "-c", script, str(cfg), str(port),
             json.dumps(statements), host],
            cwd=root, env=self.native_environment(), capture_output=True,
            text=True, timeout=180,
        )
        assert result.returncode == 0, result.stdout + result.stderr

    def native_query(self, root, cfg, port, statement, host="localhost"):
        script = """
import json
import sys
import taos
connection = taos.connect(host=sys.argv[4], port=int(sys.argv[2]),
                          config=sys.argv[1])
try:
    cursor = connection.cursor()
    try:
        cursor.execute(sys.argv[3])
        print(json.dumps([list(row) for row in cursor.fetchall()], default=str))
    finally:
        cursor.close()
finally:
    connection.close()
"""
        result = subprocess.run(
            [sys.executable, "-c", script, str(cfg), str(port), statement, host],
            cwd=root, env=self.native_environment(), capture_output=True,
            text=True, timeout=180,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        return json.loads(result.stdout.strip().splitlines()[-1])

    def update_json(self, path, update):
        return self.helper.update_json(self, path, update)

    def write_json(self, path, value):
        return self.helper.write_json(self, path, value)

    def crc32c(self, data):
        return self.helper.crc32c(self, data)

    def write_wal(self, directory, version=10, term=2, disk_id=-1):
        return self.helper.write_wal(self, directory, version, term, disk_id)

    def write_wal_records(self, directory, records, snapshot, term=2,
                          disk_id=-1):
        return self.helper.write_wal_records(self, directory, records,
                                             snapshot, term, disk_id)

    def write_sdb(self, path, nodes, unfinished=False):
        return self.helper.write_sdb(self, path, nodes, unfinished)

    def content_state(self, root):
        return {
            str(path.relative_to(root)): hashlib.sha256(
                path.read_bytes()).hexdigest()
            for path in root.rglob("*")
            if path.is_file() and not any(part.startswith("reports") for part in path.relative_to(root).parts)
        }

    def run_precheck_map(self, cfg, reports, mapping):
        reports.mkdir(exist_ok=True)
        args = [self.binary, "-c", str(cfg), "--rewrite-endpoints-precheck", str(reports), "--map", mapping]
        return subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                              timeout=120, cwd=cfg.parent, env=self.native_environment())

    def run_apply(self, cfg, reports, extra=None):
        args = [self.binary, "-c", str(cfg), "--rewrite-endpoints-apply", str(reports)]
        if extra:
            args += extra
        return subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                              timeout=180, cwd=cfg.parent, env=self.native_environment())

    def free_ports(self, count):
        holders, ports = [], []
        for _ in range(count):
            holder = socket.socket()
            holder.bind(("127.0.0.1", 0))
            holders.append(holder)
            ports.append(holder.getsockname()[1])
        for holder in holders:
            holder.close()
        return sorted(ports)

    def write_instance_cfg(self, root, index, port, first_port):
        cfg = root / f"d{index}/cfg"
        for part in [cfg, root / f"d{index}/data", root / f"d{index}/log"]:
            part.mkdir(parents=True, exist_ok=True)
        (cfg / "taos.cfg").write_text(
            f"dataDir {root}/d{index}/data\nlogDir {root}/d{index}/log\n"
            f"fqdn localhost\nserverPort {port}\n"
            f"firstEp localhost:{first_port}\n"
            "numOfCores 2\nnumOfRpcThreads 2\nnumOfTaskQueueThreads 4\n"
            "supportVnodes 64\nudf 0\ntelemetryReporting 0\nmonitor 0\n"
            "audit 0\n",
            encoding="utf-8",
        )
        return cfg

    def instance_log(self, root, index):
        return root / f"d{index}/log/stdout.log"

    def start_instance(self, root, index, port, first_port=None, prepare=False):
        """Start one instance, optionally writing its configuration first.

        Restarts reuse the configuration that is already on disk, so the
        rewritten taos.cfg is what the restarted instance reads.
        """
        cfg = root / f"d{index}/cfg"
        if prepare:
            cfg = self.write_instance_cfg(root, index, port, first_port)
        output = self.instance_log(root, index).open("a")
        proc = subprocess.Popen([self.binary, "-c", str(cfg)], cwd=root,
                                env=self.native_environment(), stdout=output,
                                stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                raise AssertionError(
                    f"dnode {index} exited on port {port}:\n" +
                    self.instance_log(root, index).read_text(errors="replace")[-4000:])
            for host in ("localhost", "127.0.0.1"):
                try:
                    with socket.create_connection((host, port), timeout=0.2):
                        return proc, cfg
                except OSError:
                    pass
            time.sleep(0.2)
        raise AssertionError(f"dnode {index} did not listen on {port}")

    def stop_instances(self, procs):
        for proc in procs:
            if proc.poll() is None:
                proc.send_signal(signal.SIGTERM)
        for proc in procs:
            try:
                code = proc.wait(timeout=180)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
                raise AssertionError("taosd did not stop gracefully")
            assert code == 0, f"taosd exited with {code}"

    def cluster_query(self, root, cfg, port, statement, host="localhost"):
        """Query the cluster, returning None while it is still starting."""
        try:
            return self.native_query(root, cfg, port, statement, host)
        except AssertionError:
            return None

    def wait_cluster(self, root, cfg, port, dnodes, mnodes=1, host="localhost"):
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            ready = self.cluster_query(root, cfg, port, DNODE_COUNT, host)
            if ready and ready[0][0] >= dnodes:
                if mnodes <= 1:
                    return
                ready = self.cluster_query(root, cfg, port, MNODE_COUNT, host)
                if ready and ready[0][0] >= mnodes:
                    return
            time.sleep(1)
        raise AssertionError(f"cluster did not reach {dnodes} dnodes and {mnodes} mnodes")

    def run_instance_command(self, root, index, args):
        return subprocess.run([self.binary, "-c", str(root / f"d{index}/cfg")] + args, cwd=root,
                              env=self.native_environment(), capture_output=True, text=True,
                              timeout=180)

    def serve_once(self, root, cfg, port):
        """Start one instance, wait until it is ready, then stop it and return its log."""
        logFile = root / "log/console.log"
        identity = root / "data/dnode/dnode.json"
        with logFile.open("a") as output:
            process = subprocess.Popen([self.binary, "-c", str(cfg)], cwd=root,
                                       env=self.native_environment(), stdout=output,
                                       stderr=subprocess.STDOUT)
            try:
                deadline = time.monotonic() + 120
                while time.monotonic() < deadline:
                    if process.poll() is not None:
                        raise AssertionError(f"taosd exited early:\n{logFile.read_text(errors='replace')}")
                    if identity.exists():
                        try:
                            with socket.create_connection(("localhost", port), timeout=0.2):
                                break
                        except OSError:
                            pass
                    time.sleep(0.2)
                else:
                    raise AssertionError(f"taosd did not become ready on {port}")
                # Let the first deploy persist its metadata.
                time.sleep(2)
            finally:
                self.stop_instances([process])
        return logFile.read_text(errors="replace")

    def precheck_cluster(self, root, ids, reports, mapping):
        """Precheck every dnode and give each one the complete report set."""
        for index, report_dir in zip(ids, reports):
            shutil.rmtree(report_dir, ignore_errors=True)
            report_dir.mkdir()
            args = ["--rewrite-endpoints-precheck", str(report_dir)] + mapping
            result = self.run_instance_command(root, index, args)
            assert result.returncode == 0, result.stdout
            report = json.loads((report_dir / f"precheck.dnode{index}.json").read_text())
            assert report["status"] == "passed", report["errors"]
            assert report["mnode"] is not None
            assert len(report["mnode"]["members"]) == 3, report["mnode"]
            assert len(report["vnodes"]) == 3, report["vnodes"]
        for index, report_dir in zip(ids, reports):
            for peer in ids:
                if peer != index:
                    shutil.copy(reports[peer - 1] / f"precheck.dnode{peer}.json", report_dir)

    def do_real_cluster_apply(self):
        with tempfile.TemporaryDirectory(prefix="rw-cluster-") as tmp:
            root = Path(tmp)
            ports = self.free_ports(6)
            old, new = ports[:3], ports[3:]
            # The rewrite changes both the name and the port, which is the
            # case this tool exists for.
            new_host = "127.0.0.1"
            procs, configs = [], []
            proc, cfg = self.start_instance(root, 1, old[0], old[0], prepare=True)
            procs.append(proc)
            configs.append(cfg)
            self.wait_cluster(root, configs[0], old[0], dnodes=1)
            for index in (2, 3):
                proc, cfg = self.start_instance(root, index, old[index - 1], old[0], prepare=True)
                procs.append(proc)
                configs.append(cfg)
            self.native_sql(root, configs[0], old[0], [
                f"create dnode 'localhost:{old[1]}'",
                f"create dnode 'localhost:{old[2]}'",
            ])
            self.wait_cluster(root, configs[0], old[0], dnodes=3)
            ids = self.native_query(root, configs[0], old[0], DNODE_IDS)
            ids = sorted(int(row[0]) for row in ids)
            assert ids == [1, 2, 3], ids
            self.native_sql(root, configs[0], old[0], [f"create mnode on dnode {index}" for index in ids[1:]])
            self.wait_cluster(root, configs[0], old[0], dnodes=3, mnodes=3)
            self.native_sql(root, configs[0], old[0], [
                "create database rw_cluster replica 3 vgroups 3",
                "create table rw_cluster.t (ts timestamp, value int)",
                "insert into rw_cluster.t values (1750046400000, 1)",
                "insert into rw_cluster.t values (1750046401000, 2)",
            ] + [f"insert into rw_cluster.t values ({1750046402000 + n}, {n})"
                 for n in range(50)] + ["flush database rw_cluster"])
            before = self.native_query(root, configs[0], old[0], "select count(*) from rw_cluster.t")
            assert before[0][0] == 52, before
            vnodes = self.native_query(root, configs[0], old[0], VNODE_COUNT)
            assert vnodes[0][0] == 9, vnodes
            time.sleep(5)
            self.stop_instances(procs)
            procs = []

            mapping = []
            for source, target_port in zip(old, new):
                mapping += ["--map", f"localhost:{source}={new_host}:{target_port}"]
            reports = [root / f"reports{index}" for index in ids]
            self.precheck_cluster(root, ids, reports, mapping)

            # Every instance needs the complete report set, so a partial set is
            # rejected without touching anything.
            content = self.content_state(root)
            (reports[0] / f"precheck.dnode{ids[-1]}.json").unlink()
            result = self.run_instance_command(root, 1, ["--rewrite-endpoints-apply", str(reports[0])])
            assert result.returncode != 0, result.stdout
            assert "apply failed" in result.stdout, result.stdout
            assert self.content_state(root) == content
            shutil.copy(reports[-1] / f"precheck.dnode{ids[-1]}.json", reports[0])

            # Writing after the precheck makes the reports stale, so the apply
            # must refuse even with a complete report set.
            procs = [self.start_instance(root, index, old[index - 1])[0] for index in ids]
            self.wait_cluster(root, root / "d1/cfg", old[0], dnodes=3, mnodes=3)
            self.native_sql(root, root / "d1/cfg", old[0], [
                "insert into rw_cluster.t values (1750046460000, 99)",
                "flush database rw_cluster",
            ])
            before = self.native_query(root, root / "d1/cfg", old[0], "select count(*) from rw_cluster.t")
            assert before[0][0] == 53, before
            self.stop_instances(procs)
            procs = []
            content = self.content_state(root)
            result = self.run_instance_command(root, 1, ["--rewrite-endpoints-apply", str(reports[0])])
            assert result.returncode != 0, result.stdout
            assert "apply failed" in result.stdout, result.stdout
            assert self.content_state(root) == content

            self.precheck_cluster(root, ids, reports, mapping)
            for index, report_dir in zip(ids, reports):
                args = ["--rewrite-endpoints-apply", str(report_dir)]
                result = self.run_instance_command(root, index, args)
                assert result.returncode == 0, result.stdout
                assert "endpoint rewrite applied" in result.stdout
                assert "ENDPOINTS REWRITE APPLIED:" in result.stdout
                assert "SDB_DNODE.fqdn/port" in result.stdout
                leftovers = list((root / f"d{index}").rglob("*.before-rewrite-endpoints"))
                assert not leftovers, leftovers
                cfg_text = (root / f"d{index}/cfg/taos.cfg").read_text()
                assert f"fqdn {new_host}" in cfg_text, cfg_text
                assert f"serverPort {new[index - 1]}" in cfg_text, cfg_text
                assert f"firstEp {new_host}:{new[0]}" in cfg_text, cfg_text

            procs = [self.start_instance(root, index, new[index - 1])[0] for index in ids]
            self.wait_cluster(root, root / "d1/cfg", new[0], dnodes=3, mnodes=3, host=new_host)
            after = self.native_query(root, root / "d1/cfg", new[0], "select count(*) from rw_cluster.t",
                                      new_host)
            assert after[0][0] == before[0][0], (before, after)
            dnodes = self.native_query(root, root / "d1/cfg", new[0], "show dnodes", new_host)
            endpoints = {row[1] for row in dnodes}
            assert endpoints == {f"{new_host}:{port}" for port in new}, endpoints
            mnodes = self.native_query(root, root / "d1/cfg", new[0], "show mnodes", new_host)
            assert {row[1] for row in mnodes} == endpoints, mnodes
            vnodes = self.native_query(root, root / "d1/cfg", new[0], VNODE_COUNT, new_host)
            assert vnodes[0][0] == 9, vnodes
            self.stop_instances(procs)
        print("real 3-dnode cluster apply and restart ............... [passed]")

    def collect_reports(self, cfg, reports):
        """Precheck the local instance and add dnode 2's copy of the report."""
        result = self.run_precheck(cfg, reports)
        assert result.returncode == 0, result.stdout
        report = self.read_report(reports)
        assert report["status"] == "passed"
        second = json.loads(json.dumps(report))
        second["dnodeId"] = 2
        self.write_json(reports / "precheck.dnode2.json", second)
        return report

    def do_encrypted_apply(self):
        taosk = Path(self.binary).with_name("taosk")
        version = subprocess.run([self.binary, "-V"], capture_output=True,
                                 text=True, timeout=10)
        if (os.name != "posix" or not taosk.is_file() or
                "Enterprise" not in version.stdout):
            tdLog.info("enterprise encryption tooling is unavailable")
            return
        with tempfile.TemporaryDirectory(prefix="rw-encrypted-") as tmp:
            root = Path(tmp)
            cfg, data, logs, reports = [root / part for part in
                                        ["cfg", "data", "log", "reports"]]
            for path in [cfg, data, logs, reports]:
                path.mkdir()
            with socket.socket() as available:
                available.bind(("127.0.0.1", 0))
                port = available.getsockname()[1]
            (cfg / "taos.cfg").write_text(
                f"dataDir {data}\nlogDir {logs}\nfqdn localhost\n"
                f"serverPort {port}\nfirstEp localhost:{port}\n"
                "numOfCores 2\nnumOfRpcThreads 2\nnumOfTaskQueueThreads 4\n"
                "udf 0\ntelemetryReporting 0\nmonitor 0\naudit 0\n",
                encoding="utf-8",
            )
            result = subprocess.run([str(taosk), "-c", str(cfg), "--encrypt-server", "--encrypt-database",
                                     "--encrypt-config", "--encrypt-metadata", "--encrypt-data"],
                                    capture_output=True, text=True, timeout=120, cwd=root,
                                    env=self.native_environment())
            assert result.returncode == 0, result.stdout + result.stderr
            with self.native_daemon(root, cfg, port):
                self.native_sql(root, cfg, port, [
                    "create database rw_encrypted keep 36500",
                    "create table rw_encrypted.t (ts timestamp, value int)",
                    "insert into rw_encrypted.t values (1750046400000, 7)",
                    "flush database rw_encrypted",
                ])
            local = data / "dnode/config/local.json"
            identity = data / "dnode/dnode.json"
            assert local.read_bytes().startswith(b"tdEncrypt")
            assert identity.read_bytes().startswith(b"tdEncrypt")

            def precheck(target_dir, source, target):
                target_dir.mkdir(exist_ok=True)
                result = subprocess.run([
                    self.binary, "-c", str(cfg), "--rewrite-endpoints-precheck", str(target_dir),
                    "--map", f"localhost:{source}=localhost:{target}",
                ], cwd=root, env=self.native_environment(), capture_output=True, text=True, timeout=120)
                assert result.returncode == 0, result.stdout
                report = json.loads((target_dir / "precheck.dnode1.json").read_text())
                assert report["status"] == "passed", report["errors"]
                assert report["mnode"] is not None
                assert report["vnodes"], report

            precheck(reports, port, port + 1)
            result = self.run_apply(cfg, reports)
            assert result.returncode == 0, result.stdout
            assert "endpoint rewrite applied" in result.stdout
            # Encrypted files stay encrypted, and no plaintext copy is left.
            assert local.read_bytes().startswith(b"tdEncrypt")
            assert identity.read_bytes().startswith(b"tdEncrypt")
            assert not list(root.rglob("*.before-rewrite-endpoints"))
            assert not list(root.rglob("*.tmp"))
            precheck(root / "next-reports", port + 1, port + 2)
            # The rewritten encrypted instance has to start and keep working.
            with self.native_daemon(root, cfg, port + 1):
                rows = self.native_query(root, cfg, port + 1, "select count(*) from rw_encrypted.t")
                assert rows[0][0] == 1, rows
                self.native_sql(root, cfg, port + 1, [
                    "insert into rw_encrypted.t values (1750046500000, 5)",
                    "flush database rw_encrypted",
                ])
                rows = self.native_query(root, cfg, port + 1, "select count(*) from rw_encrypted.t")
                assert rows[0][0] == 2, rows
        print("encrypted instance apply and restart ................. [passed]")

    def do_apply_success(self):
        with tempfile.TemporaryDirectory(prefix="rewrite-apply-") as tmp:
            root = Path(tmp)
            cfg, data, reports, vnode = self.make_fixture(root)
            self.collect_reports(cfg, reports)
            sdb = data / "mnode/data/sdb.data"
            sdb_before = sdb.read_bytes()
            result = self.run_apply(cfg, reports)
            assert result.returncode == 0, result.stdout
            assert "old-a.invalid:6030 -> localhost:7030" in result.stdout
            assert "endpoint rewrite applied" in result.stdout
            for message in ["fqdn/serverPort", "dnodes[].fqdn/port",
                            "replicas[].fqdn/port",
                            "nodeInfo[].nodeFqdn/nodePort",
                            "SDB_DNODE.fqdn/port", "leader0"]:
                assert message in result.stdout, message
            assert not list(root.rglob("*.before-rewrite-endpoints"))
            assert not list(root.rglob("*.tmp"))
            cfg_text = (cfg / "taos.cfg").read_text()
            for line in ["fqdn localhost", "serverPort 7030",
                         "firstEp localhost:7030",
                         "secondEp old-b.invalid:6030"]:
                assert line in cfg_text, line
            configs = json.loads((data / "dnode/config/local.json").read_text())["configs"]
            assert configs["fqdn"] == "localhost"
            assert configs["serverPort"] == 7030
            assert configs["firstEp"] == "localhost:7030"
            assert configs["secondEp"] == "old-b.invalid:6030"
            identity = json.loads((data / "dnode/dnode.json").read_text())
            assert [(node["fqdn"], node["port"]) for node in identity["dnodes"]] == [
                ("localhost", 7030), ("old-b.invalid", 6030)]
            replicas = json.loads((data / "mnode/mnode.json").read_text())["replicas"]
            assert [(node["fqdn"], node["port"]) for node in replicas] == [
                ("localhost", 7030), ("old-b.invalid", 6030)]
            for path in [data / "mnode/sync/raft_config.json", vnode / "sync/raft_config.json"]:
                raft = json.loads(path.read_text())["RaftCfg"]["SSyncCfg"]
                assert [(m["nodeFqdn"], m["nodePort"]) for m in raft["nodeInfo"]] == [
                    ("localhost", "7030"), ("old-b.invalid", "6030")], path
            version = json.loads((vnode / "vnode.json").read_text())["config"]
            assert [(m["nodeFqdn"], m["nodePort"]) for m in version["syncCfg.nodeInfo"]] == [
                ("localhost", "7030"), ("old-b.invalid", "6030")]
            snode = json.loads((data / "snode/snode1/snode.json").read_text())
            leader = snode["leader0"]["eps"][0]
            assert (leader["fqdn"], leader["port"]) == ("localhost", 7030)
            assert snode["leader1"]["eps"][0]["fqdn"] == "old-b.invalid"
            assert sdb.read_bytes() != sdb_before
            # The rewritten instance must still pass a fresh precheck, which
            # validates the snapshot, the Raft configuration and the WAL.
            result = self.run_precheck_map(cfg, root / "next-reports", "localhost:7030=localhost:8030")
            assert result.returncode == 0, result.stdout
            report = self.read_report(root / "next-reports")
            assert report["status"] == "passed"
            assert report["mapping"][0]["source"] == {"fqdn": "localhost", "port": 7030}
            assert "localhost:7030 -> localhost:8030" in result.stdout
        print("apply rewrites every file and stays consistent .... [passed]")

    def do_apply_multi_disk(self):
        with tempfile.TemporaryDirectory(prefix="rewrite-apply-") as tmp:
            root = Path(tmp)
            cfg, data, reports, vnode = self.make_fixture(root, multi_disk=True)
            assert vnode.parent != data
            self.collect_reports(cfg, reports)
            result = self.run_apply(cfg, reports)
            assert result.returncode == 0, result.stdout
            members = json.loads((vnode / "sync/raft_config.json").read_text())
            members = members["RaftCfg"]["SSyncCfg"]["nodeInfo"]
            assert [(m["nodeFqdn"], m["nodePort"]) for m in members] == [
                ("localhost", "7030"), ("old-b.invalid", "6030")]
            config = json.loads((vnode / "vnode.json").read_text())["config"]
            assert config["syncCfg.nodeInfo"][0]["nodeFqdn"] == "localhost"
            assert not list(root.rglob("*.before-rewrite-endpoints"))
        print("apply rewrites vnodes on secondary disks ............. [passed]")

    def do_apply_snode_variants(self):
        """A snode group may hold several endpoints and any role assignment."""
        with tempfile.TemporaryDirectory(prefix="rewrite-apply-") as tmp:
            root = Path(tmp)
            cfg, data, reports, vnode = self.make_fixture(root)
            path = data / "snode/snode1/snode.json"
            self.update_json(path, lambda json: json.update({
                # leader0 holds two endpoints, both of the local dnode.
                "leader0": {"nodeId": "1", "numOfEps": "2", "eps": [
                    {"fqdn": "old-a.invalid", "port": 6030},
                    {"fqdn": "old-a.invalid", "port": 6030}]},
                # The remaining roles are swapped compared with the default.
                "leader1": {"nodeId": "2", "numOfEps": "1", "eps": [
                    {"fqdn": "old-b.invalid", "port": 6030}]},
                "replica": {"nodeId": "1", "numOfEps": "1", "eps": [
                    {"fqdn": "old-a.invalid", "port": 6030}]},
            }))
            self.collect_reports(cfg, reports)
            result = self.run_apply(cfg, reports)
            assert result.returncode == 0, result.stdout
            assert result.stdout.count(
                "leader0: old-a.invalid:6030 -> localhost:7030") == 2, result.stdout
            assert result.stdout.count(
                "replica: old-a.invalid:6030 -> localhost:7030") == 1, result.stdout
            assert "leader1: " not in result.stdout, result.stdout
            snode = json.loads(path.read_text())
            assert [(ep["fqdn"], ep["port"]) for ep in snode["leader0"]["eps"]] == [
                ("localhost", 7030), ("localhost", 7030)]
            assert [(ep["fqdn"], ep["port"]) for ep in snode["leader1"]["eps"]] == [
                ("old-b.invalid", 6030)]
            assert [(ep["fqdn"], ep["port"]) for ep in snode["replica"]["eps"]] == [
                ("localhost", 7030)]
            assert not list(root.rglob("*.before-rewrite-endpoints"))
        print("apply rewrites multi-endpoint and swapped snode roles [passed]")

    def do_apply_snode_unused_group(self):
        """A snode group without endpoints omits the eps field entirely.

        This is the shape taosd writes for a dnode that no snode group uses, so
        it has to be accepted and left alone.
        """
        with tempfile.TemporaryDirectory(prefix="rewrite-apply-") as tmp:
            root = Path(tmp)
            cfg, data, reports, vnode = self.make_fixture(root)
            path = data / "snode/snode1/snode.json"
            self.write_json(path, {
                "deployed": 1, "snodeId": "1",
                **{key: {"nodeId": "0", "inUse": "0", "numOfEps": "0"}
                   for key in ["leader0", "leader1", "replica"]},
            })
            before = path.read_bytes()
            self.collect_reports(cfg, reports)
            result = self.run_apply(cfg, reports)
            assert result.returncode == 0, result.stdout
            assert path.read_bytes() == before
            assert "leader0: " not in result.stdout, result.stdout
            assert not list(root.rglob("*.before-rewrite-endpoints"))
        print("apply accepts a snode group without endpoints ....... [passed]")

    def do_apply_snode_mixed_groups(self):
        """A file that needs a rewrite may also hold unused snode groups.

        taosd omits `eps` for a role that no snode group uses. Such a file has
        to be rewritten for the groups that do use an endpoint, while the
        unused groups keep their original shape.
        """
        with tempfile.TemporaryDirectory(prefix="rewrite-apply-") as tmp:
            root = Path(tmp)
            cfg, data, reports, vnode = self.make_fixture(root)
            path = data / "snode/snode1/snode.json"
            self.write_json(path, {
                "deployed": 1, "snodeId": "1",
                "leader0": {"nodeId": "1", "inUse": "1", "numOfEps": "1",
                            "eps": [{"fqdn": "old-a.invalid", "port": 6030}]},
                "leader1": {"nodeId": "2", "inUse": "0", "numOfEps": "0"},
                "replica": {"nodeId": "0", "inUse": "0", "numOfEps": "0"},
            })
            self.collect_reports(cfg, reports)
            result = self.run_apply(cfg, reports)
            assert result.returncode == 0, result.stdout
            assert result.stdout.count(
                "leader0: old-a.invalid:6030 -> localhost:7030") == 1, result.stdout
            snode = json.loads(path.read_text())
            assert [(ep["fqdn"], ep["port"])
                    for ep in snode["leader0"]["eps"]] == [("localhost", 7030)]
            assert snode["leader1"] == {"nodeId": "2", "inUse": "0",
                                        "numOfEps": "0"}
            assert snode["replica"] == {"nodeId": "0", "inUse": "0",
                                        "numOfEps": "0"}
            assert not list(root.rglob("*.before-rewrite-endpoints"))
        print("apply rewrites a used group next to unused ones ... [passed]")

    def do_apply_without_mnode(self):
        with tempfile.TemporaryDirectory(prefix="rewrite-apply-") as tmp:
            root = Path(tmp)
            cfg, data, reports, vnode = self.make_fixture(root, mnode=False)
            report = self.collect_reports(cfg, reports)
            assert report["mnode"] is None
            assert not (data / "mnode").exists()
            result = self.run_apply(cfg, reports)
            assert result.returncode == 0, result.stdout
            assert "SDB_DNODE" not in result.stdout
            assert "endpoint rewrite applied" in result.stdout
            assert not list(root.rglob("*.before-rewrite-endpoints"))
            assert not (data / "mnode").exists()
        print("apply without a local mnode .......................... [passed]")

    def do_apply_crlf_config(self):
        """taos.cfg written with CRLF line endings must be rewritten in place."""
        with tempfile.TemporaryDirectory(prefix="rewrite-apply-") as tmp:
            root = Path(tmp)
            cfg, data, reports, vnode = self.make_fixture(root)
            self.collect_reports(cfg, reports)
            text = (cfg / "taos.cfg").read_bytes().replace(b"\n", b"\r\n")
            (cfg / "taos.cfg").write_bytes(text)
            result = self.run_apply(cfg, reports)
            assert result.returncode == 0, result.stdout
            assert "endpoint rewrite applied" in result.stdout
            rewritten = (cfg / "taos.cfg").read_bytes()
            assert b"fqdn localhost\r\n" in rewritten, rewritten
            assert b"fqdn localhost\n" not in rewritten.replace(b"fqdn localhost\r\n", b"")
            assert b"serverPort 7030\r\n" in rewritten, rewritten
            assert b"firstEp localhost:7030\r\n" in rewritten, rewritten
            assert b"secondEp old-b.invalid:6030\r\n" in rewritten, rewritten
            assert b"\n" not in rewritten.replace(b"\r\n", b"")
            assert not list(root.rglob("*.before-rewrite-endpoints"))
        print("apply accepts a CRLF taos.cfg ........................ [passed]")

    def do_apply_second_ep(self):
        """firstEp and secondEp must point at a dnode the mapping knows.

        The apply stage re-reads the configuration files, so an endpoint that
        was edited between the precheck and the apply is judged on its current
        value rather than on the stored change set.
        """
        with tempfile.TemporaryDirectory(prefix="rewrite-apply-") as tmp:
            root = Path(tmp)
            cfg, data, reports, vnode = self.make_fixture(root)
            self.collect_reports(cfg, reports)
            # A typo in the peer port: the endpoint is no longer any dnode.
            text = (cfg / "taos.cfg").read_text().replace(
                "secondEp old-b.invalid:6030", "secondEp old-b.invalid:6031")
            (cfg / "taos.cfg").write_text(text, encoding="utf-8")
            before = self.content_state(root)
            result = self.run_apply(cfg, reports)
            assert result.returncode != 0, result.stdout
            assert "apply failed" in result.stdout, result.stdout
            assert "secondEp" in result.stdout, result.stdout
            assert "is not a mapped endpoint" in result.stdout, result.stdout
            assert self.content_state(root) == before
            assert not list(root.rglob("*.before-rewrite-endpoints"))

            # The same file is rejected by a fresh precheck, so the user finds
            # out before collecting the reports again.
            result = self.run_precheck(cfg, root / "second-reports")
            assert result.returncode != 0, result.stdout
            assert "is not a mapped endpoint" in result.stdout, result.stdout

        with tempfile.TemporaryDirectory(prefix="rewrite-apply-") as tmp:
            root = Path(tmp)
            cfg, data, reports, vnode = self.make_fixture(root)
            self.collect_reports(cfg, reports)
            # Pointing secondEp at another dnode that the mapping does carry is
            # correct, so the rewrite follows the mapping and updates it.
            text = (cfg / "taos.cfg").read_text().replace(
                "secondEp old-b.invalid:6030", "secondEp old-a.invalid:6030")
            (cfg / "taos.cfg").write_text(text, encoding="utf-8")
            result = self.run_apply(cfg, reports)
            assert result.returncode == 0, result.stdout
            rewritten = (cfg / "taos.cfg").read_text()
            assert "secondEp localhost:7030" in rewritten, rewritten
            assert "old-a.invalid" not in rewritten, rewritten
            assert not list(root.rglob("*.before-rewrite-endpoints"))
        print("apply judges firstEp/secondEp on their current value . [passed]")

    def do_apply_rejections(self):
        def remove_second(cfg, data, reports, vnode):
            (reports / "precheck.dnode2.json").unlink()

        def change_mapping(cfg, data, reports, vnode):
            self.update_json(reports / "precheck.dnode2.json",
                             lambda j: j["mapping"][1]["target"].update(
                                 {"port": 6031}))

        def stale_report(cfg, data, reports, vnode):
            self.update_json(reports / "precheck.dnode1.json",
                             lambda j: j["vnodes"][0].update({"snapshotTerm": "3"}))

        def other_version(cfg, data, reports, vnode):
            self.update_json(reports / "precheck.dnode1.json",
                             lambda j: j.update({"version": "3.0.0.0"}))

        def not_passed(cfg, data, reports, vnode):
            self.update_json(reports / "precheck.dnode2.json",
                             lambda j: j.update({"status": "failed"}))

        def left_over_backup(cfg, data, reports, vnode):
            (data / "dnode/config/local.json.before-rewrite-endpoints").write_bytes(b"leftover")

        def changed_source(cfg, data, reports, vnode):
            self.update_json(data / "dnode/config/local.json",
                             lambda j: j["configs"].update({"fqdn": "wrong.invalid"}))

        def snapshot_write_blocked(cfg, data, reports, vnode):
            # Force the last file to fail after the JSON files are written.
            (data / "mnode/data/sdb.data.tmp").mkdir()

        def non_canonical_name(cfg, data, reports, vnode):
            (reports / "precheck.dnode2.json").rename(reports / "precheck.dnode02.json")

        def foreign_identity(cfg, data, reports, vnode):
            # The file name says dnode 3, the content still describes dnode 2.
            (reports / "precheck.dnode2.json").rename(reports / "precheck.dnode3.json")

        def foreign_cluster(cfg, data, reports, vnode):
            self.update_json(reports / "precheck.dnode2.json",
                             lambda j: j.update({"clusterId": "999"}))

        def extra_report(cfg, data, reports, vnode):
            # A report copied in from another cluster: one dnode too many.
            copy = json.loads((reports / "precheck.dnode1.json").read_text())
            copy["dnodeId"] = 3
            self.write_json(reports / "precheck.dnode3.json", copy)

        def vnode_removed(cfg, data, reports, vnode):
            shutil.rmtree(vnode)
            (data / "vnode/vnodes.json").unlink()

        def mnode_added(cfg, data, reports, vnode):
            # Both reports claim the dnode has no mnode, but it has one.
            for path in (reports / "precheck.dnode1.json",
                         reports / "precheck.dnode2.json"):
                self.update_json(path, lambda j: j.update({"mnode": None}))

        cases = [
            ("missing-file", remove_second, 0, False),
            ("mapping", change_mapping, 0, False),
            ("stale-report", stale_report, 0, False),
            ("version", other_version, 0, False),
            ("failed-status", not_passed, 0, False),
            ("leftover-backup", left_over_backup, 1, False),
            ("changed-source", changed_source, 0, False),
            ("rollback", snapshot_write_blocked, 0, True),
            ("report-name", non_canonical_name, 0, False),
            ("report-identity", foreign_identity, 0, False),
            ("report-cluster", foreign_cluster, 0, False),
            ("extra-report", extra_report, 0, False),
            ("inventory-vnode", vnode_removed, 0, False),
            ("inventory-mnode", mnode_added, 0, False),
        ]
        expected = {
            "report-name": "unexpected precheck file name",
            "report-identity": "has an invalid identity",
            "report-cluster": "belongs to another cluster or version",
            "extra-report": "has 3 precheck files for 2 dnodes",
            "inventory-vnode": "inventory changed since the precheck",
            "inventory-mnode": "inventory changed since the precheck",
        }
        for name, mutate, backups, restored in cases:
            with tempfile.TemporaryDirectory(prefix="rewrite-apply-") as tmp:
                root = Path(tmp)
                cfg, data, reports, vnode = self.make_fixture(root)
                self.collect_reports(cfg, reports)
                mutate(cfg, data, reports, vnode)
                before = self.source_state(root)
                content = self.content_state(root)
                result = self.run_apply(cfg, reports)
                assert result.returncode != 0, (name, result.stdout)
                assert "apply failed" in result.stdout, (name, result.stdout)
                if name in expected:
                    assert expected[name] in result.stdout, (name, result.stdout)
                # A pre-existing backup is never touched; otherwise every
                # backup must be removed again.
                assert len(list(root.rglob("*.before-rewrite-endpoints"))) == backups, name
                assert self.content_state(root) == content, name
                if not restored:
                    assert self.source_state(root) == before, name
        with tempfile.TemporaryDirectory(prefix="rewrite-apply-") as tmp:
            root = Path(tmp)
            cfg, data, reports, vnode = self.make_fixture(root)
            self.collect_reports(cfg, reports)
            before = self.source_state(root)
            if os.name == "posix":
                import fcntl

                with (data / ".running").open("rb") as lock:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    result = self.run_apply(cfg, reports)
                    assert result.returncode != 0, result.stdout
                    assert "instance stopped and locked" in result.stdout
                    assert self.source_state(root) == before
        with tempfile.TemporaryDirectory(prefix="rewrite-apply-") as tmp:
            root = Path(tmp)
            cfg, data, reports, vnode = self.make_fixture(root)
            self.collect_reports(cfg, reports)
            before = self.source_state(root)
            result = self.run_apply(cfg, reports, extra=["--map", "localhost:7030=localhost:8030"])
            assert result.returncode != 0, result.stdout
            assert self.source_state(root) == before
        print("apply rejects incomplete, stale and unsafe inputs ..... [passed]")

    def do_native_apply(self):
        with tempfile.TemporaryDirectory(prefix="rw-native-apply-") as tmp:
            root = Path(tmp)
            cfg, data, logs, reports = [root / part for part in ["cfg", "data", "logs", "reports"]]
            for path in [cfg, data, logs, reports]:
                path.mkdir()
            with socket.socket() as available:
                available.bind(("127.0.0.1", 0))
                port = available.getsockname()[1]
            (cfg / "taos.cfg").write_text(
                f"dataDir {data}\nlogDir {logs}\nfqdn localhost\n"
                f"serverPort {port}\nfirstEp localhost:{port}\n"
                "numOfCores 2\nnumOfRpcThreads 2\nnumOfTaskQueueThreads 4\n"
                "udf 0\ntelemetryReporting 0\nmonitor 0\naudit 0\n",
                encoding="utf-8",
            )
            with self.native_daemon(root, cfg, port):
                self.native_sql(root, cfg, port, [
                    "create database rw_native_apply keep 36500",
                    "create table rw_native_apply.t (ts timestamp, value int)",
                    "insert into rw_native_apply.t values (1750046400000, 7)",
                    "flush database rw_native_apply",
                ])
            result, report = self.helper.native_precheck(self, root, cfg, reports, port)
            assert result.returncode == 0 and report["status"] == "passed", result.stdout
            result = self.run_apply(cfg, reports)
            assert result.returncode == 0, result.stdout
            assert "endpoint rewrite applied" in result.stdout
            assert not list(root.rglob("*.before-rewrite-endpoints"))
            cfg_text = (cfg / "taos.cfg").read_text()
            assert f"serverPort {port + 1}" in cfg_text
            assert f"firstEp localhost:{port + 1}" in cfg_text
            # The rewritten instance has to start on the new endpoint and the
            # metadata has to report it.
            with self.native_daemon(root, cfg, port + 1):
                rows = self.native_query(root, cfg, port + 1, "show dnodes")
                assert rows, rows
                endpoints = [str(cell) for row in rows for cell in row]
                assert any(f"localhost:{port + 1}" in cell for cell in endpoints), endpoints
                assert not any(f"localhost:{port}" == cell or cell.endswith(f":{port}") for cell in endpoints)
                rows = self.native_query(root, cfg, port + 1, "select count(*) from rw_native_apply.t")
                assert rows[0][0] == 1, rows
        print("native apply restarts on the new endpoint ............ [passed]")

    @pytest.mark.skipif(os.name != "posix",
                        reason="uses fcntl to simulate a running instance")
    def test_rewrite_endpoint_apply(self):
        """Offline endpoint apply

        1. Apply a complete precheck set and rewrite every persisted endpoint.
        2. Verify backups are removed and all files hold the new endpoints.
        3. Verify untouched dnodes keep their addresses.
        4. Verify the snapshot and the rewritten instance pass a new precheck.
        5. Rewrite vnodes that reside on secondary disks.
        6. Rewrite snode groups with several endpoints and swapped roles, a
           group without endpoints, and a file that mixes both shapes.
        7. Rewrite an instance without a local mnode.
        8. Rewrite a taos.cfg that uses CRLF line endings, keeping the CRLF.
        9. Judge firstEp/secondEp on the value found at apply time: reject an
           endpoint that is no longer any dnode, follow the mapping otherwise.
        10. Reject incomplete, inconsistent, foreign or stale precheck file sets.
        11. Reject a local mnode or vnode inventory that changed after the precheck.
        12. Reject a running instance and leftover backup files.
        13. Roll back every file when the last write fails.
        14. Rewrite a CFG_KEY/META_KEY encrypted instance and keep it usable.

        Catalog:
            - Others

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-27 Bomin Zhang Add offline apply coverage.

        """
        self.do_apply_success()
        self.do_apply_multi_disk()
        self.do_apply_snode_variants()
        self.do_apply_snode_unused_group()
        self.do_apply_snode_mixed_groups()
        self.do_apply_without_mnode()
        self.do_apply_crlf_config()
        self.do_apply_second_ep()
        self.do_apply_rejections()
        self.do_encrypted_apply()

    @pytest.mark.skipif(os.name != "posix",
                        reason="requires graceful POSIX daemon shutdown")
    def test_rewrite_endpoint_native_apply(self):
        """Offline endpoint apply on a native database

        1. Start a private taosd, write data, flush and stop it normally.
        2. Precheck the stopped instance and apply the collected reports.
        3. Start the rewritten instance on the new endpoint.
        4. Verify the metadata and the data through the client.

        Catalog:
            - Others

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-27 Bomin Zhang Add native apply regression.

        """
        self.do_native_apply()

    @pytest.mark.skipif(os.name != "posix",
                        reason="requires graceful POSIX daemon shutdown")
    def test_rewrite_endpoint_cluster_apply(self):
        """Offline endpoint apply on a real 3-dnode cluster

        1. Build a real cluster: 3 dnodes, an mnode replica set of 3 and
           3-replica vgroups, then write and flush data.
        2. Stop the cluster and precheck every dnode offline.
        3. Reject an incomplete report set without touching any file.
        4. Restart, write once more and stop: the stale reports must be
           rejected as well.
        5. Precheck again, change both the FQDN and the port, apply on every
           dnode and verify no backup is left.
        6. Restart from the rewritten configuration: dnodes, mnodes, vgroups
           and data must all come back on the new addresses.

        Catalog:
            - Others

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-27 Bomin Zhang Add real multi-dnode cluster coverage.

        """
        self.do_real_cluster_apply()

    def build_wal_cluster(self, root):
        """Build a 3-dnode cluster with 3 mnodes and one 3-replica vgroup."""
        ports = self.free_ports(6)
        old, new = ports[:3], ports[3:]
        procs, configs = [], []
        proc, cfg = self.start_instance(root, 1, old[0], old[0], prepare=True)
        procs.append(proc)
        configs.append(cfg)
        self.wait_cluster(root, configs[0], old[0], dnodes=1)
        for index in (2, 3):
            proc, cfg = self.start_instance(root, index, old[index - 1], old[0],
                                            prepare=True)
            procs.append(proc)
            configs.append(cfg)
        self.native_sql(root, configs[0], old[0], [
            f"create dnode 'localhost:{old[1]}'",
            f"create dnode 'localhost:{old[2]}'",
        ])
        self.wait_cluster(root, configs[0], old[0], dnodes=3)
        self.native_sql(root, configs[0], old[0],
                        [f"create mnode on dnode {index}" for index in (2, 3)])
        self.wait_cluster(root, configs[0], old[0], dnodes=3, mnodes=3)
        self.native_sql(root, configs[0], old[0], [
            "create database wal_del replica 3 vgroups 1",
            "create table wal_del.t (ts timestamp, value int)",
            "insert into wal_del.t values (1750046400000, 1)",
            "insert into wal_del.t values (1750046401000, 2)",
            "flush database wal_del",
        ])
        # Let every replica apply what it received, so no replica is left with a
        # business record above its persisted position when the cluster stops.
        time.sleep(10)
        self.stop_instances(procs)
        return old, new

    def remove_wal_dirs(self, root, targets, scope):
        """Delete whole WAL directories of the given dnodes and return them."""
        removed = []
        for index in targets:
            candidates = []
            if scope in ("mnode", "both"):
                candidates.append(root / f"d{index}/data/mnode/wal")
            if scope in ("vnode", "both"):
                candidates.extend(sorted(
                    (root / f"d{index}/data/vnode").glob("vnode*/wal")))
            for path in candidates:
                assert path.is_dir(), path
                shutil.rmtree(path)
                removed.append(path)
        return removed

    def assert_wal_absent_group(self, group, where):
        """A missing WAL is reported as S == C == L with the last unsafe log S-1."""
        index = int(group["snapshotIndex"])
        assert group["wal"] == {"snapshotVer": str(index),
                                "commitVer": str(index),
                                "lastVer": str(index)}, (where, group)
        assert int(group["safeLowerBound"]) == max(-1, index - 1), (where, group)

    def do_wal_removed_apply(self):
        scenarios = [
            ("one node mnode WAL", [1], "mnode"),
            ("one node vnode WAL", [1], "vnode"),
            ("one node all WAL", [1], "both"),
            ("all nodes all WAL", [1, 2, 3], "both"),
        ]
        for label, targets, scope in scenarios:
            with tempfile.TemporaryDirectory(prefix="rw-wal-") as tmp:
                root = Path(tmp)
                old, new = self.build_wal_cluster(root)
                removed = self.remove_wal_dirs(root, targets, scope)
                assert removed, label

                mapping = []
                for source, target in zip(old, new):
                    mapping += ["--map", f"localhost:{source}=localhost:{target}"]
                reports = [root / f"pre{index}" for index in (1, 2, 3)]
                for index, report_dir in zip((1, 2, 3), reports):
                    report_dir.mkdir()
                    result = self.run_instance_command(
                        root, index,
                        ["--rewrite-endpoints-precheck", str(report_dir)] + mapping)
                    assert result.returncode == 0, (label, index, result.stdout)
                    report = json.loads(
                        (report_dir / f"precheck.dnode{index}.json").read_text())
                    assert report["status"] == "passed", (label, report["errors"])
                    if index not in targets:
                        continue
                    if scope in ("mnode", "both"):
                        self.assert_wal_absent_group(report["mnode"],
                                                     (label, index, "mnode"))
                    if scope in ("vnode", "both"):
                        assert report["vnodes"], (label, index)
                        for group in report["vnodes"]:
                            self.assert_wal_absent_group(group, (label, index))

                for index, report_dir in zip((1, 2, 3), reports):
                    for peer in (1, 2, 3):
                        if peer != index:
                            shutil.copy(
                                reports[peer - 1] / f"precheck.dnode{peer}.json",
                                report_dir)
                for index, report_dir in zip((1, 2, 3), reports):
                    result = self.run_instance_command(
                        root, index,
                        ["--rewrite-endpoints-apply", str(report_dir)])
                    assert result.returncode == 0, (label, index, result.stdout)
                    text = (root / f"d{index}/cfg/taos.cfg").read_text()
                    assert f"serverPort {new[index - 1]}" in text, (label, text)
                assert not list(root.rglob("*.before-rewrite-endpoints")), label
            print(f"apply accepts a deleted {label} .......... [passed]")

    @pytest.mark.skipif(os.name != "posix",
                        reason="requires graceful POSIX daemon shutdown")
    def test_rewrite_endpoint_wal_removed(self):
        """Endpoint apply when the WAL was deleted

        A missing WAL is a normal state, not an error: the tool reports the
        persisted position as the commit and last position, and the record just
        below it as the last unverified one.

        1. Delete the mnode WAL of one dnode only.
        2. Delete the vnode WAL of one dnode only.
        3. Delete both the mnode and the vnode WAL of one dnode.
        4. Delete the mnode and vnode WAL of every dnode.

        For every case the precheck must pass, every group whose WAL was removed
        must report S == C == L with safeLowerBound == S - 1, and the apply must
        rewrite the cluster.

        Catalog:
            - Others

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-28 Bomin Zhang Add deleted WAL coverage on a real cluster.

        """
        self.do_wal_removed_apply()

    def build_running_cluster(self, root):
        """Build a 3-dnode, 3-mnode cluster with one flushed database, kept running."""
        ports = self.free_ports(6)
        old, new = ports[:3], ports[3:]
        procs, configs = [], []
        proc, cfg = self.start_instance(root, 1, old[0], old[0], prepare=True)
        procs.append(proc)
        configs.append(cfg)
        self.wait_cluster(root, configs[0], old[0], dnodes=1)
        for index in (2, 3):
            proc, cfg = self.start_instance(root, index, old[index - 1], old[0], prepare=True)
            procs.append(proc)
            configs.append(cfg)
        self.native_sql(root, configs[0], old[0], [
            f"create dnode 'localhost:{old[1]}'",
            f"create dnode 'localhost:{old[2]}'",
        ])
        self.wait_cluster(root, configs[0], old[0], dnodes=3)
        self.native_sql(root, configs[0], old[0], [f"create mnode on dnode {index}" for index in (2, 3)])
        self.wait_cluster(root, configs[0], old[0], dnodes=3, mnodes=3)
        self.native_sql(root, configs[0], old[0], [
            "create database diverge replica 3 vgroups 1",
            "create table diverge.t (ts timestamp, value int)",
            "insert into diverge.t values (1750046400000, 1)",
            "flush database diverge",
        ])
        time.sleep(10)
        return procs, configs, old, new

    def run_sql_on_survivors(self, root, cfg, port, statements, seconds=120):
        """Run statements on the surviving dnodes, waiting for a new mnode leader."""
        deadline = time.monotonic() + seconds
        last = ""
        while time.monotonic() < deadline:
            try:
                self.native_sql(root, cfg, port, statements)
                return
            except AssertionError as exc:
                last = str(exc)[-400:]
                time.sleep(2)
        raise AssertionError(f"the surviving dnodes did not accept the change: {last}")

    def do_divergent_reports(self):
        scenarios = [
            ("mnode", ["create topic diverge_topic with meta as database diverge",
                       "flush mnode"]),
            ("vnode", ["insert into diverge.t values (1750046402000, 2)",
                       "insert into diverge.t values (1750046403000, 3)",
                       "flush database diverge"]),
        ]
        for group, statements in scenarios:
            with tempfile.TemporaryDirectory(prefix="rw-diverge-") as tmp:
                root = Path(tmp)
                procs, configs, old, new = self.build_running_cluster(root)
                try:
                    # One dnode stops normally while the other two keep accepting
                    # changes, so the stopped dnode falls behind in the raft log.
                    self.stop_instances([procs[0]])
                    self.run_sql_on_survivors(root, configs[1], old[1], statements)
                    time.sleep(10)
                    self.stop_instances(procs[1:])
                    procs = []

                    mapping = []
                    for source, target in zip(old, new):
                        mapping += ["--map", f"localhost:{source}=localhost:{target}"]
                    reports = [root / f"pre{index}" for index in (1, 2, 3)]
                    behind, ahead = [], []
                    for index, report_dir in zip((1, 2, 3), reports):
                        report_dir.mkdir()
                        result = self.run_instance_command(
                            root, index, ["--rewrite-endpoints-precheck", str(report_dir)] + mapping)
                        assert result.returncode == 0, (group, index, result.stdout)
                        report = json.loads((report_dir / f"precheck.dnode{index}.json").read_text())
                        assert report["status"] == "passed", (group, report["errors"])
                        if group == "mnode":
                            position = int(report["mnode"]["snapshotIndex"])
                        else:
                            position = int(report["vnodes"][0]["snapshotIndex"])
                        (behind if index == 1 else ahead).append(position)
                    # The stopped dnode has to be behind, otherwise the case proves nothing.
                    assert max(behind) < min(ahead), (group, behind, ahead)

                    for index, report_dir in zip((1, 2, 3), reports):
                        for peer in (1, 2, 3):
                            if peer != index:
                                shutil.copy(reports[peer - 1] / f"precheck.dnode{peer}.json", report_dir)
                    before = self.content_state(root)
                    result = self.run_instance_command(
                        root, 1, ["--rewrite-endpoints-apply", str(reports[0])])
                    output = result.stdout
                    assert result.returncode != 0, (group, output)
                    assert f"apply failed: {group}" in output, (group, output)
                    assert "cannot be recovered safely" in output, (group, output)
                    assert self.content_state(root) == before, group
                finally:
                    self.stop_instances([proc for proc in procs if proc.poll() is None])
            print(f"apply rejects diverged {group} reports ............... [passed]")

    @pytest.mark.skipif(os.name != "posix",
                        reason="requires graceful POSIX daemon shutdown")
    def test_rewrite_endpoint_divergent_reports(self):
        """Endpoint apply when one dnode stopped before the last change

        A dnode that is stopped normally while the others keep running falls
        behind in the raft log. Every dnode still passes its own precheck,
        because each one is self-consistent, but the collected set shows that
        the stopped dnode cannot catch up from the others.

        1. Build a 3-dnode, 3-mnode cluster with one flushed database.
        2. Stop one dnode, then change mnode metadata on the survivors: create a
           topic and flush the mnode.
        3. Stop the survivors and precheck every dnode: all pass. The collected
           reports show the stopped dnode's mnode position below the survivors.
        4. Apply: it is refused because the mnode group cannot be recovered
           safely, and no file is modified.
        5. Repeat with a vnode change instead: write data and flush the
           database, then the vnode group is the one that blocks the apply.

        Catalog:
            - Others

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-28 Bomin Zhang Add diverged per-dnode report coverage.

        """
        self.do_divergent_reports()
