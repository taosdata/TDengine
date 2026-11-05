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
import sys
import tempfile
import time

import pytest

BASE_MS = 1750046400000
WINDOW_MS = 1000


class TestRewriteEndpointService:
    """Endpoint rewrite against a real instance that runs real services

    The offline mechanics live in the precheck and apply cases. This file keeps
    the cases that need a real deployment with snode, stream and topic, the case
    that inspects the transaction records a real mnode leaves on disk, and the
    case that rewrites a real multi-dnode cluster with its own snodes.
    """

    def setup_class(self):
        self.case = apply_case.TestRewriteEndpointApply()
        self.case.setup_class()
        self.binary = self.case.binary
        tdLog.debug(f"start to execute {__file__}")

    def write_instance(self, root):
        """Create a single dnode instance configuration and return its paths."""
        cfg, data, logs, reports = [root / part for part in
                                    ["cfg", "data", "log", "reports"]]
        for path in [cfg, data, logs, reports]:
            path.mkdir()
        port = self.case.free_ports(1)[0]
        (cfg / "taos.cfg").write_text(
            f"dataDir {data}\nlogDir {logs}\nfqdn localhost\nserverPort {port}\n"
            f"firstEp localhost:{port}\n"
            "numOfCores 2\nnumOfRpcThreads 2\nnumOfTaskQueueThreads 4\n"
            "udf 0\ntelemetryReporting 0\nmonitor 0\naudit 0\n",
            encoding="utf-8")
        return cfg, data, reports, port

    def consume(self, root, port, topic, group, rows, seconds=120):
        """Consume a topic in a child process and return the row count."""
        script = """
import json
import sys
import time

from taos.tmq import Consumer

port, topic, group, want, seconds = (sys.argv[1], sys.argv[2], sys.argv[3],
                                     int(sys.argv[4]), int(sys.argv[5]))
consumer = Consumer({
    "group.id": group,
    "client.id": group,
    "td.connect.ip": "localhost",
    "td.connect.port": port,
    "td.connect.user": "root",
    "td.connect.pass": "taosdata",
    "enable.auto.commit": "true",
    "auto.commit.interval.ms": "500",
    "auto.offset.reset": "earliest",
    "experimental.snapshot.enable": "false",
})
total = 0
try:
    consumer.subscribe([topic])
    deadline = time.monotonic() + seconds
    while total < want and time.monotonic() < deadline:
        result = consumer.poll(1)
        if not result:
            continue
        if result.error() is not None:
            raise SystemExit(f"tmq poll error: {result.error()}")
        for block in result.value():
            total += len(block.fetchall())
finally:
    consumer.close()
print(json.dumps({"rows": total}))
"""
        result = subprocess.run(
            [sys.executable, "-c", script, str(port), topic, group, str(rows),
             str(seconds)],
            cwd=root, env=self.case.native_environment(), capture_output=True,
            text=True, timeout=seconds + 120)
        assert result.returncode == 0, result.stdout + result.stderr
        return json.loads(result.stdout.strip().splitlines()[-1])["rows"]

    def wait_for(self, check, message, seconds=120):
        """Poll a query helper until it returns a truthy value."""
        deadline = time.monotonic() + seconds
        last = None
        while time.monotonic() < deadline:
            try:
                value = check()
            except AssertionError as exc:
                last = str(exc)[-300:]
                value = None
            if value:
                return value
            time.sleep(2)
        raise AssertionError(f"{message} (last error: {last})")

    def wait_ready(self, root, cfg, port):
        """A fresh instance accepts DDL only once its dnode reports ready."""
        self.wait_for(
            lambda: self.case.native_query(
                root, cfg, port,
                "select status from information_schema.ins_dnodes")[0][0] == "ready"
            or None,
            "dnode did not become ready")

    def wait_stream(self, root, cfg, port):
        """Wait until the stream is deployed and no longer starting up."""
        def check():
            rows = self.case.native_query(
                root, cfg, port,
                "select status from information_schema.ins_streams "
                "where stream_name = 's_count'")
            return rows and rows[0][0] in ("Running", "Idle") or None

        return self.wait_for(check, "stream was not deployed")

    def write_batch(self, root, cfg, port, first_window, windows=5):
        """Write one row per window, starting at the given window offset."""
        values = ", ".join(
            f"({BASE_MS + (first_window + n) * WINDOW_MS}, {n})"
            for n in range(windows))
        self.case.native_sql(root, cfg, port,
                             [f"insert into svc.meters values {values}"])

    def feed_stream(self, root, cfg, port, first_window, batches=2, windows=5):
        """Feed the stream and return the next unused window offset.

        A stream is driven by the data it receives, so the results of one batch
        are computed when the next batch arrives. Two batches are written to
        make sure every window of the first one has been emitted.
        """
        for batch in range(batches):
            self.write_batch(root, cfg, port, first_window + batch * windows,
                             windows)
            time.sleep(4)
        return first_window + batches * windows

    def create_services(self, root, cfg, port):
        """Deploy snode, a database, a topic and a stream, then feed the stream."""
        self.wait_ready(root, cfg, port)
        self.case.native_sql(root, cfg, port, [
            "create database svc keep 36500",
            "create table svc.meters (ts timestamp, v int)",
            "create topic svc_topic as select ts, v from svc.meters",
            "create snode on dnode 1",
            "create stream svc.s_count interval(1s) sliding(1s) "
            "from svc.meters stream_options(max_delay(3s)) into svc.s_result "
            "as select _twstart, count(*) from svc.meters",
        ])
        return self.wait_stream(root, cfg, port)

    def stream_rows(self, root, cfg, port):
        return self.case.native_query(root, cfg, port,
                                      "select count(*) from svc.s_result")[0][0]

    def service_state(self, root, cfg, port):
        """Collect the answers the restarted instance has to give."""
        return {
            "rows": self.case.native_query(
                root, cfg, port, "select count(*) from svc.meters")[0][0],
            "streams": self.case.native_query(
                root, cfg, port,
                "select stream_name from information_schema.ins_streams"),
            "topics": self.case.native_query(
                root, cfg, port,
                "select topic_name from information_schema.ins_topics"),
            "snodes": self.case.native_query(
                root, cfg, port,
                "select id from information_schema.ins_snodes"),
            "vnodes": self.case.native_query(
                root, cfg, port,
                "select count(*) from information_schema.ins_vnodes")[0][0],
        }

    def do_service_chain(self):
        with tempfile.TemporaryDirectory(prefix="rw-service-") as tmp:
            root = Path(tmp)
            cfg, data, reports, old = self.write_instance(root)
            new = old + 1
            with self.case.native_daemon(root, cfg, old):
                self.create_services(root, cfg, old)
                before_vnodes = self.case.native_query(
                    root, cfg, old,
                    "select count(*) from information_schema.ins_vnodes")[0][0]
                next_window = self.feed_stream(root, cfg, old, 0)
                before_stream = self.wait_for(
                    lambda: self.stream_rows(root, cfg, old) or None,
                    "stream produced no result")
                assert self.consume(root, old, "svc_topic", "svc_group", 10) == 10
                self.case.native_sql(root, cfg, old, [
                    "flush database svc",
                    "flush mnode",
                ])
                time.sleep(3)
                self.case.native_sql(root, cfg, old, ["flush database svc"])

            result = self.case.helper.native_precheck(
                self.case, root, cfg, reports, old)
            assert result[0].returncode == 0 and result[1]["status"] == "passed", \
                result[0].stdout
            assert result[1]["mnode"] is not None
            assert result[1]["vnodes"], result[1]

            result = self.case.run_apply(cfg, reports)
            assert result.returncode == 0, result.stdout
            assert (cfg / "taos.cfg").read_text().count(f"serverPort {new}") == 1
            # The real snode configuration is part of the rewrite.
            snode = data / "snode/snode1/snode.json"
            assert snode.is_file(), snode
            assert all(ep["port"] == new
                       for role in json.loads(snode.read_text()).values()
                       if isinstance(role, dict)
                       for ep in role.get("eps", []))

            with self.case.native_daemon(root, cfg, new):
                after = self.service_state(root, cfg, new)
                assert after["rows"] == 10, after
                assert [row[0] for row in after["streams"]] == ["s_count"], after
                assert [row[0] for row in after["topics"]] == ["svc_topic"], after
                assert after["snodes"] == [[1]], after
                assert after["vnodes"] == before_vnodes, after
                dnodes = self.case.native_query(root, cfg, new, "show dnodes")
                assert [row[1] for row in dnodes] == [f"localhost:{new}"], dnodes

                # The stream has to keep computing on the new endpoint.
                self.feed_stream(root, cfg, new, next_window)
                grown = self.wait_for(
                    lambda: (self.stream_rows(root, cfg, new) or 0) > before_stream
                    and self.stream_rows(root, cfg, new),
                    f"stream produced no new result above {before_stream}")
                tdLog.info(f"stream rows grew from {before_stream} to {grown}")
                # The subscription has to continue with the same group, and the
                # rows written after the rewrite have to arrive.
                assert self.consume(root, new, "svc_topic", "svc_group", 10) == 10
                assert self.case.native_query(
                    root, cfg, new, "select count(*) from svc.meters")[0][0] == 20
        print("snode, stream and subscription survive the rewrite ...[passed]")

    def do_unfinished_transaction(self):
        with tempfile.TemporaryDirectory(prefix="rw-transaction-") as tmp:
            root = Path(tmp)
            cfg, data, reports, port = self.write_instance(root)
            with self.case.native_daemon(root, cfg, port):
                # A real mnode logs every DDL as an mnode transaction, so this
                # leaves a real transaction history behind.
                self.create_services(root, cfg, port)
                self.case.native_sql(root, cfg, port, [
                    "flush database svc",
                    "flush mnode",
                ])
                time.sleep(3)
                self.case.native_sql(root, cfg, port, ["flush database svc"])

            sdb = data / "mnode/data/sdb.data"
            original = sdb.read_bytes()
            nodes = [{"id": node["id"], "fqdn": node["fqdn"], "port": node["port"]}
                     for node in json.loads(
                         (data / "dnode/dnode.json").read_text())["dnodes"]]

            # Every transaction of a normally stopped instance is finished, so
            # the precheck of the real mnode snapshot passes.
            result, report = self.case.helper.native_precheck(
                self.case, root, cfg, reports, port)
            assert result.returncode == 0 and report["status"] == "passed", \
                result.stdout

            # Add one unfinished transaction to the real snapshot: the tool has
            # to refuse before it touches anything.
            self.case.write_sdb(sdb, nodes, unfinished=True)
            before = self.case.content_state(root)
            result = self.case.run_precheck_map(
                cfg, reports / "tx", f"localhost:{port}=localhost:{port + 1}")
            assert result.returncode != 0, result.stdout
            report = json.loads((reports / "tx/precheck.dnode1.json").read_text())
            assert report["status"] == "failed", report
            assert any("no unfinished transactions" == error["check"]
                       for error in report["errors"]), report["errors"]
            assert self.case.content_state(root) == before

            result = self.case.run_apply(cfg, reports / "tx")
            assert result.returncode != 0, result.stdout
            assert "apply failed" in result.stdout, result.stdout
            assert self.case.content_state(root) == before

            # Restoring the snapshot makes the same instance pass again: the
            # unfinished transaction was the only problem.
            sdb.write_bytes(original)
            (reports / "final").mkdir()
            result, report = self.case.helper.native_precheck(
                self.case, root, cfg, reports / "final", port)
            assert result.returncode == 0 and report["status"] == "passed", \
                result.stdout
        print("finished transactions pass, unfinished ones refuse ...[passed]")

    def start_cluster(self, root):
        """Start a 3 dnode / 3 mnode cluster and return its processes and ports."""
        ports = self.case.free_ports(6)
        old, new = ports[:3], ports[3:]
        procs, configs = [], []
        proc, cfg = self.case.start_instance(root, 1, old[0], old[0],
                                             prepare=True)
        procs.append(proc)
        configs.append(cfg)
        self.case.wait_cluster(root, configs[0], old[0], dnodes=1)
        for index in (2, 3):
            proc, cfg = self.case.start_instance(root, index, old[index - 1],
                                                 old[0], prepare=True)
            procs.append(proc)
            configs.append(cfg)
        self.case.native_sql(root, configs[0], old[0], [
            f"create dnode 'localhost:{old[1]}'",
            f"create dnode 'localhost:{old[2]}'",
        ])
        self.case.wait_cluster(root, configs[0], old[0], dnodes=3)
        self.case.native_sql(root, configs[0], old[0],
                             [f"create mnode on dnode {index}"
                              for index in (2, 3)])
        self.case.wait_cluster(root, configs[0], old[0], dnodes=3, mnodes=3)
        return procs, configs, old, new

    def stream_state(self, root, cfg, port):
        return self.case.native_query(
            root, cfg, port,
            "select status from information_schema.ins_streams "
            "where stream_name = 's_count'")

    def snode_endpoints(self, root, index):
        """Return every endpoint a snode file records, with its role name."""
        path = root / f"d{index}/data/snode/snode{index}/snode.json"
        assert path.is_file(), path
        return [(role_name, ep)
                for role_name, role in json.loads(path.read_text()).items()
                if isinstance(role, dict)
                for ep in role.get("eps", [])]

    def do_cluster_service_chain(self):
        with tempfile.TemporaryDirectory(prefix="rw-cluster-service-") as tmp:
            root = Path(tmp)
            procs, configs, old, new = self.start_cluster(root)
            try:
                # Two snodes are needed for the endpoint sets to be filled in:
                # a single snode has no leader/replica, so every role keeps
                # numOfEps 0 and its snode.json has nothing to rewrite.
                self.case.native_sql(root, configs[0], old[0], [
                    "create snode on dnode 1",
                    "create snode on dnode 2",
                    "create database svc replica 3 vgroups 1 keep 36500",
                    "create table svc.meters (ts timestamp, v int)",
                    "create topic svc_topic as select ts, v from svc.meters",
                    "create stream svc.s_count interval(1s) sliding(1s) "
                    "from svc.meters stream_options(max_delay(3s)) "
                    "into svc.s_result "
                    "as select _twstart, count(*) from svc.meters",
                ])
                self.wait_stream(root, configs[0], old[0])

                written = 0
                for batch in range(3):
                    self.write_batch(root, configs[0], old[0], batch * 5)
                    written += 5
                    time.sleep(6)
                before_stream = self.wait_for(
                    lambda: self.stream_rows(root, configs[0], old[0]) or None,
                    "stream produced no result", seconds=300)
                assert self.consume(root, old[0], "svc_topic", "svc_group",
                                    written) == written
                before = {
                    "rows": self.case.native_query(
                        root, configs[0], old[0],
                        "select count(*) from svc.meters")[0][0],
                    "vnodes": self.case.native_query(
                        root, configs[0], old[0],
                        "select count(*) from information_schema.ins_vnodes")[0][0],
                }
                assert before["rows"] == written, before

                # The manual asks for stream processing to be paused before the
                # flush. With the stream running, the flush makes it compute the
                # windows that are still open, and that submit record lands after
                # the snapshot the same flush wrote, so the tail stays unsafe.
                self.case.native_sql(root, configs[0], old[0],
                                     ["stop stream svc.s_count"])
                time.sleep(3)
                assert self.stream_state(root, configs[0], old[0]) == [["Stopped"]]

                self.case.native_sql(root, configs[0], old[0],
                                     ["flush database svc", "flush mnode"])
                time.sleep(3)
                self.case.native_sql(root, configs[0], old[0],
                                     ["flush database svc"])
                self.case.stop_instances(procs)
                procs = []

                mapping = []
                for source, target in zip(old, new):
                    mapping += ["--map",
                                f"localhost:{source}=localhost:{target}"]
                reports = [root / f"pre{index}" for index in (1, 2, 3)]
                for index, report_dir in zip((1, 2, 3), reports):
                    report_dir.mkdir()
                    result = self.case.run_instance_command(
                        root, index,
                        ["--rewrite-endpoints-precheck", str(report_dir)]
                        + mapping)
                    assert result.returncode == 0, result.stdout
                    report = json.loads(
                        (report_dir / f"precheck.dnode{index}.json").read_text())
                    assert report["status"] == "passed", report["errors"]
                    assert report["mnode"] is not None
                    assert len(report["mnode"]["members"]) == 3, report["mnode"]
                    assert len(report["vnodes"]) == 1, report["vnodes"]
                # Every node needs the complete result set before it applies.
                for index, report_dir in zip((1, 2, 3), reports):
                    for peer in (1, 2, 3):
                        if peer != index:
                            shutil.copy(
                                reports[peer - 1] / f"precheck.dnode{peer}.json",
                                report_dir)

                for index, report_dir in zip((1, 2, 3), reports):
                    result = self.case.run_instance_command(
                        root, index,
                        ["--rewrite-endpoints-apply", str(report_dir)])
                    assert result.returncode == 0, result.stdout
                    assert "endpoint rewrite applied" in result.stdout
                    leftovers = list((root / f"d{index}").rglob(
                        "*.before-rewrite-endpoints"))
                    assert not leftovers, leftovers

                # Each snode names the other dnode, so the rewrite has to follow
                # the mapping across nodes, not only on the local one.
                for index in (1, 2):
                    endpoints = self.snode_endpoints(root, index)
                    assert endpoints, f"snode {index} has no endpoint to check"
                    assert all(int(ep["port"]) in new
                               for _, ep in endpoints), endpoints

                procs = [self.case.start_instance(root, index, new[index - 1])[0]
                         for index in (1, 2, 3)]
                self.case.wait_cluster(root, root / "d1/cfg", new[0], dnodes=3,
                                       mnodes=3)

                def query(statement):
                    return self.case.native_query(root, root / "d1/cfg", new[0],
                                                  statement)

                endpoints = sorted(row[1] for row in query("show dnodes"))
                assert endpoints == sorted(f"localhost:{port}" for port in new), \
                    endpoints
                assert sorted(row[1] for row in query("show mnodes")) == endpoints
                snodes = query(
                    "select id, endpoint from information_schema.ins_snodes")
                assert len(snodes) == 2, snodes
                assert all(any(str(row[1]).endswith(f":{port}") for port in new)
                           for row in snodes), snodes
                after = {
                    "rows": query("select count(*) from svc.meters")[0][0],
                    "vnodes": query(
                        "select count(*) from information_schema.ins_vnodes")[0][0],
                }
                assert after == before, (before, after)
                assert [row[0] for row in query(
                    "select stream_name from information_schema.ins_streams")] \
                    == ["s_count"]
                assert [row[0] for row in query(
                    "select topic_name from information_schema.ins_topics")] \
                    == ["svc_topic"]

                # Pausing the stream is part of the procedure, so its state is
                # rewritten like any other metadata: it stays stopped across the
                # restart and does not resume by itself.
                assert self.stream_state(root, root / "d1/cfg", new[0]) \
                    == [["Stopped"]]
                self.case.native_sql(root, root / "d1/cfg", new[0],
                                     ["start stream svc.s_count"])
                self.wait_stream(root, root / "d1/cfg", new[0])

                for batch in range(3, 5):
                    self.write_batch(root, root / "d1/cfg", new[0], batch * 5)
                    time.sleep(6)
                grown = self.wait_for(
                    lambda: self.case.native_query(
                        root, root / "d1/cfg", new[0],
                        "select count(*) from svc.s_result")[0][0] > before_stream
                    and self.case.native_query(
                        root, root / "d1/cfg", new[0],
                        "select count(*) from svc.s_result")[0][0],
                    "the stream did not compute new results", seconds=300)
                tdLog.info(f"stream rows grew from {before_stream} to {grown}")
                assert self.consume(root, new[0], "svc_topic", "svc_group",
                                    10) == 10
                assert query("select count(*) from svc.meters")[0][0] == 25
            finally:
                if procs:
                    self.case.stop_instances(procs)
        print("3-dnode cluster with two snodes survives the rewrite "
              "[passed]")

    @pytest.mark.skipif(os.name != "posix",
                        reason="requires graceful POSIX daemon shutdown")
    def test_rewrite_endpoint_service_chain(self):
        """Rewrite an instance that runs snode, a stream and a topic

        1. Deploy snode, a database, a stream and a topic on a real instance,
           then write data, consume the topic and flush.
        2. Stop the instance and precheck it: the real stream and topic
           transactions are finished, so the precheck passes.
        3. Apply and verify the real snode configuration was rewritten too.
        4. Restart on the new endpoint: data, stream, topic and snode are still
           listed and the dnode reports the new endpoint.
        5. Write more data: the stream computes new results and a consumer that
           joins the same group receives the new rows.

        Catalog:
            - Others

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-28 Bomin Zhang Add snode, stream and subscription coverage.

        """
        self.do_service_chain()

    @pytest.mark.skipif(os.name != "posix",
                        reason="requires graceful POSIX daemon shutdown")
    def test_rewrite_endpoint_unfinished_transaction(self):
        """Unfinished mnode transactions block precheck and apply

        1. Run a real instance through a set of DDL statements, so its mnode
           snapshot holds a real transaction history, then stop it normally.
        2. Precheck passes: all of those transactions are finished.
        3. Add one unfinished transaction to the real snapshot.
        4. Precheck and apply are refused with `no unfinished transactions`,
           and no file is modified.
        5. Restore the snapshot: the same instance passes again, which shows
           that the transaction record was the only reason for the refusal.

        Catalog:
            - Others

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-28 Bomin Zhang Add unfinished transaction coverage on a real
              mnode snapshot.

        """
        self.do_unfinished_transaction()

    @pytest.mark.skipif(os.name != "posix",
                        reason="requires graceful POSIX daemon shutdown")
    def test_rewrite_endpoint_cluster_service(self):
        """Rewrite a 3-dnode cluster that runs two snodes and a stream

        1. Build a 3 dnode / 3 mnode cluster and deploy two snodes, a three
           replica database, a stream and a topic on it.
        2. Write data, consume the topic, then pause stream processing and
           flush, as the procedure requires.
        3. Precheck every dnode, collect the complete result set on each node
           and apply there. Every snode names the other dnode, so the rewrite
           has to follow the mapping across nodes.
        4. Restart the cluster on the new endpoints: dnodes, mnodes, snodes,
           data and vnode count all match the state before the rewrite.
        5. The paused stream stays stopped after the restart; starting it again
           lets it compute the new data, and the same consumer group continues.

        Catalog:
            - Others

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-10-29 Bomin Zhang Add multi-dnode cluster coverage with
              cross-node snode endpoints.

        """
        self.do_cluster_service_chain()
