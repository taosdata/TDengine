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

from new_test_framework.utils import tdLog, tdSql, tdStream, sc

from pathlib import Path
from new_test_framework.utils.log_reader import LogReader
import re
import sys
import time

import pytest


class TestStreamVtableDropUnrelatedDb:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.db = "vt_drop_stream"
        cls.src_db = "vt_drop_source"
        cls.unrelated_db = "vt_drop_unrelated"
        cls.stream = "s_drop_unrelated"

    def wait_status(self, status):
        tdSql.checkResultsByArray(
            "select status from information_schema.ins_streams "
            f"where db_name='{self.db}' and stream_name='{self.stream}'",
            [(status,)],
            retry=60,
        )

    def insert_window(self, peak, source="m1"):
        # This is a live stream without FILL_HISTORY: input must follow its
        # current deployment start time, including after STOP/START.
        ts = int(time.time() * 1000)
        tdSql.execute(
            f"insert into {self.src_db}.{source} values "
            f"({ts}, 60, 1) ({ts + 1000}, {peak}, 1) ({ts + 2000}, 50, 0)"
        )

    def check_output(self, peaks):
        tdSql.checkResultsByArray(
            f"select temperature, equipment from {self.db}.out_event order by ts",
            [(peak, self.equipment) for peak in peaks],
            retry=60,
        )

    def check_coverage_diagnostic(self, reader, stream_id, new_ids):
        pattern = re.compile(
            rf"stream:{stream_id} cannot recover metadata group:\d+, current vgroup:(\d+) "
            r"db:.*vt_drop_mid is absent from persisted scan coverage; replan required"
        )
        for _ in range(30):
            for line in reader.read().splitlines():
                match = pattern.search(line)
                if match and int(match.group(1)) in new_ids:
                    return
            time.sleep(1)
        tdLog.exit(f"missing metadata coverage diagnostic for stream {stream_id}, new vgroups {new_ids}")

    def do_drop_unrelated_db(self, chain, split):
        self.equipment = "machine-2" if chain else "machine-1"
        tdStream.ensureSnode()
        chain_dbs = ("vt_drop_leaf", "vt_drop_mid")
        databases = (self.src_db, *chain_dbs, self.db, self.unrelated_db)
        created_databases = []
        stream_created = False
        try:
            for db in databases:
                placement = " dnodes '1'" if db in (self.src_db, self.db) else ""
                tdSql.execute(f"create database {db} vgroups 2 buffer 8{placement}")
                created_databases.append(db)
            tdSql.query(
                "select vgroup_id from information_schema.ins_vgroups "
                f"where db_name='{self.unrelated_db}' order by vgroup_id"
            )
            tdSql.checkRows(2)
            removed = [row[0] for row in tdSql.queryResult]
            removed_ids = ",".join(str(vg) for vg in removed)
            tdSql.execute(
                f"create stable {self.src_db}.meters "
                "(ts timestamp, temp double, running int) tags (equipment varchar(32))"
            )
            tdSql.execute(
                f"create table {self.src_db}.m1 using {self.src_db}.meters tags ('machine-1')"
            )
            tdSql.execute(
                f"create table {self.src_db}.m2 using {self.src_db}.meters tags ('machine-2')"
            )
            ref_table = f"{self.src_db}.m2"
            for db in chain_dbs:
                tdSql.execute(
                    f"create stable {db}.vst (ts timestamp, temp double, running int) "
                    "tags (equipment varchar(32)) virtual 1"
                )
                tdSql.execute(
                    f"create vtable {db}.v1 (temp from {ref_table}.temp, "
                    f"running from {ref_table}.running) using {db}.vst "
                    f"tags (from {ref_table}.equipment)"
                )
                ref_table = f"{db}.v1"
            if not chain:
                ref_table = f"{self.src_db}.m1"
            tdSql.execute(
                f"create stable {self.db}.vst (ts timestamp, temp double, running int) "
                "tags (low double, high double, mode nchar(32), equipment varchar(32)) virtual 1"
            )
            tdSql.execute(
                f"create vtable {self.db}.v1 (temp from {ref_table}.temp, "
                f"running from {ref_table}.running) using {self.db}.vst "
                f"tags (45, 55, 'temperature', from {ref_table}.equipment)"
            )
            tdSql.execute(
                f"create stream {self.db}.{self.stream} "
                "event_window(start with running > 0 and temp > high "
                "end with running = 0 or temp <= high) "
                f"from {self.db}.v1 into {self.db}.out_event "
                "as select _twstart as ts, last(temp) as temperature, "
                f"last(equipment) as equipment from {self.db}.v1 "
                "where ts >= _twstart and ts < _twend"
            )
            stream_created = True
            tdSql.query(
                "select stream_id from information_schema.ins_streams "
                f"where db_name='{self.db}' and stream_name='{self.stream}'"
            )
            tdSql.checkRows(1)
            stream_id = tdSql.getData(0, 0)
            self.wait_status("Running")
            self.insert_window(65, "m2" if chain else "m1")
            self.check_output([65])
            peaks = [65]
            if split:
                tdSql.execute(f"stop stream {self.db}.{self.stream}")
                self.wait_status("Stopped")
                tdSql.query(
                    "select vgroup_id from information_schema.ins_tables "
                    "where db_name='vt_drop_mid' and table_name='v1'"
                )
                tdSql.checkRows(1)
                split_id = tdSql.getData(0, 0)
                tdSql.query(
                    "select vgroup_id from information_schema.ins_vgroups "
                    "where db_name='vt_drop_mid'"
                )
                old_ids = {row[0] for row in tdSql.queryResult}
                tdSql.execute(f"split vgroup {split_id} force")
                tdSql.checkResultsByArray(
                    "select count(*) from information_schema.ins_vgroups "
                    f"where vgroup_id={split_id}", [(0,)], retry=60
                )
                tdSql.checkResultsByArray(
                    "select count(*) from information_schema.ins_vgroups "
                    "where db_name='vt_drop_mid'", [(3,)], retry=60
                )
                tdSql.checkResultsByArray(
                    "select count(*) from information_schema.ins_tables "
                    "where db_name='vt_drop_mid' and table_name='v1' "
                    f"and vgroup_id <> {split_id}", [(1,)], retry=60
                )
                tdSql.query(
                    "select vgroup_id from information_schema.ins_vgroups "
                    "where db_name='vt_drop_mid'"
                )
                new_ids = {row[0] for row in tdSql.queryResult} - old_ids
                assert len(new_ids) == 2
            if not chain:
                # Rebind through an intermediate virtual table in a database
                # outside the stream's reference closure at creation time.
                tdSql.execute(
                    f"alter vtable {self.db}.v1 alter column temp set vt_drop_mid.v1.temp"
                )
                tdSql.execute(
                    f"alter vtable {self.db}.v1 alter column running set vt_drop_mid.v1.running"
                )
                if split:
                    reader = LogReader(
                        str(Path(sc.dnodeLogPath(node)) / "taosdlog.*")
                        for node in range(1, 4)
                    )
                    tdSql.execute(f"start stream {self.db}.{self.stream}")
                    time.sleep(10)
                    self.insert_window(97, "m2")
                    time.sleep(5)
                    self.wait_status("Failed")
                    tdSql.query(
                        "select message from information_schema.ins_streams "
                        f"where db_name='{self.db}' and stream_name='{self.stream}'"
                    )
                    assert "Last error: Mnode stream not available now" in tdSql.getData(0, 0)
                    self.check_coverage_diagnostic(reader, format(int(stream_id, 16), "x"), new_ids)
                    tdSql.query(
                        "select count(*) from information_schema.ins_stream_tasks "
                        f"where stream_id='{stream_id}' and `type`='Runner'"
                    )
                    tdSql.checkData(0, 0, 0)
                    self.check_output(peaks)
                    return
                time.sleep(3)
                self.insert_window(67, "m2")
                peaks.append(67)
                self.check_output(peaks)
            # The virtual-stream guard remains effective; FORCE is intentional
            # because this database has no data or tag references from the stream.
            tdSql.checkResultsByArray(
                "select distinct node_id from information_schema.ins_stream_tasks "
                f"where stream_id='{stream_id}' and `type`='Reader' "
                f"and node_id in ({removed_ids}) order by node_id",
                [(vg,) for vg in sorted(removed)],
                retry=30,
            )
            tdSql.error(f"drop database {self.unrelated_db}", expectedErrno=0x700F)
            tdSql.execute(f"drop database {self.unrelated_db} force")
            created_databases.remove(self.unrelated_db)
            for table in ("ins_vgroups", "ins_vnodes"):
                tdSql.checkResultsByArray(
                    f"select count(*) from information_schema.{table} "
                    f"where vgroup_id in ({removed_ids})",
                    [(0,)],
                    retry=30,
                )

            tdSql.query(
                "select task_id from information_schema.ins_stream_tasks "
                f"where stream_id='{stream_id}' and `type`='Trigger'"
            )
            tdSql.checkRows(1)
            old_trigger = tdSql.getData(0, 0)
            tdSql.execute(f"stop stream {self.db}.{self.stream}")
            self.wait_status("Stopped")
            tdSql.execute(f"start stream {self.db}.{self.stream}")
            # START acknowledges the lifecycle update before redeployment.
            # Wait for the new trigger incarnation before sending live input.
            tdSql.checkResultsByArray(
                "select count(*) from information_schema.ins_stream_tasks "
                f"where stream_id='{stream_id}' and `type`='Trigger' "
                f"and status='Running' and task_id <> '{old_trigger}'",
                [(1,)],
                retry=60,
            )
            self.wait_status("Running")
            tdSql.query(
                "select count(*) from information_schema.ins_stream_tasks "
                f"where stream_id='{stream_id}' and node_id in ({removed_ids}) "
                "and `type`='Reader'"
            )
            tdSql.checkData(0, 0, 0)
            self.insert_window(66, "m2")
            self.check_output(peaks + [66])
        finally:
            original_error = sys.exc_info()[1]
            cleanup_errors = []
            cleanup_sql = []
            if stream_created:
                cleanup_sql.append(f"drop stream if exists {self.db}.{self.stream}")
            cleanup_sql.extend(f"drop database if exists {db} force" for db in reversed(created_databases))
            for statement in cleanup_sql:
                try:
                    tdSql.execute(statement)
                except Exception as error:
                    cleanup_errors.append(f"{statement}: {error}")
            if cleanup_errors:
                message = "cleanup failures: " + "; ".join(cleanup_errors)
                if original_error is not None:
                    tdLog.info(message)
                else:
                    raise RuntimeError(message)
        tdLog.info("virtual-child stream survives unrelated database removal [passed]")

    @pytest.mark.parametrize("chain,split", [(False, False), (True, False), (False, True)])
    def test_stream_vtable_drop_unrelated_db(self, chain, split):
        """Virtual-child event streams survive deletion of an unrelated database.

        1. Verify direct and cross-database multi-hop data/TAG references with stored limits.
        2. Rebind direct references through another database, then drop an unrelated database with FORCE and confirm its vgroups disappear.
        3. STOP/START the original stream and require Running and new event output,
           without Reader tasks on the deleted vgroups.
        4. Split an intermediate database vgroup and rebind to a distinct leaf;
           refuse deployment when the persisted metadata coverage is incomplete.

        Catalog:
            - Streams:VirtualTable

        Since: v3.4.4.0

        Labels: common,ci,stream,vtable

        Jira: None

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7178294524

        History:
            - 2026-11-24 Codex Add regression for stale metadata-scan vgroups.
        """
        self.do_drop_unrelated_db(chain, split)
