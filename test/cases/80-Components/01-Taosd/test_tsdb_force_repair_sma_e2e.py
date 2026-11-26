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

import os
import re
import time

from new_test_framework.utils import tdDnodes, tdLog, tdSql
from test_tsdb_force_repair_base import TsdbForceRepairBase


class TestTsdbForceRepairSmaE2E(TsdbForceRepairBase):
    dbname = "tsdb_repair_sma_e2e"
    block_rows = 200
    base_ts = 1600000000000

    def _prepare_sma_fixture(self):
        rows = self.block_rows
        tdSql.execute(f"drop database if exists {self.dbname}")
        tdSql.execute(
            f"create database {self.dbname} vgroups 1 keep 36500 duration 10 stt_trigger 1 "
            f"minrows {rows} maxrows {rows}"
        )
        tdSql.execute(f"create table {self.dbname}.t(ts timestamp, v int, w int) sma(v)")
        for block, (base, w) in enumerate(((1000, 1), (2000, 2), (3000, 3))):
            values = ",".join(
                f"({self.base_ts + block * 1000 + i},{base + i},{w})" for i in range(rows)
            )
            tdSql.execute(f"insert into {self.dbname}.t values {values}")
        tdSql.execute(f"flush database {self.dbname}")
        vnode_id = self._get_vnode_id_for_db(self.dbname, table_name="t")
        sma_path = None
        deadline = time.time() + 30
        while time.time() < deadline:
            sma_path = self._find_first_tsdb_file(vnode_id, ".sma")
            if sma_path is not None:
                break
            time.sleep(1)
        tdSql.checkEqual(sma_path is not None, True)
        fid = self._parse_fid_from_tsdb_path(sma_path)
        tdSql.checkEqual(fid is not None, True)
        return {"dbname": self.dbname, "vnode_id": vnode_id, "fid": fid, "sma_path": sma_path}

    def _assert_expected_results(self):
        # Three 200-row blocks; the last block holds the newest timestamp and
        # the largest v, and every block carries its own w group.
        tdSql.query(f"select count(*) from {self.dbname}.t")
        tdSql.checkData(0, 0, 3 * self.block_rows)
        tdSql.query(f"select last(v) from {self.dbname}.t")
        tdSql.checkData(0, 0, 3000 + self.block_rows - 1)
        tdSql.query(f"select last(v) from {self.dbname}.t partition by w")
        tdSql.checkRows(3)
        tdSql.checkEqual(
            sorted(row[0] for row in tdSql.queryResult),
            [1000 + self.block_rows - 1, 2000 + self.block_rows - 1, 3000 + self.block_rows - 1],
        )
        tdSql.query(f"select count(*) from {self.dbname}.t where v is not null")
        tdSql.checkData(0, 0, 3 * self.block_rows)
        tdSql.query(f"select top(v,2) from {self.dbname}.t")
        tdSql.checkEqual(
            sorted(row[0] for row in tdSql.queryResult),
            [3000 + self.block_rows - 2, 3000 + self.block_rows - 1],
        )
        tdSql.query(f"select max(v) from {self.dbname}.t")
        tdSql.checkData(0, 0, 3000 + self.block_rows - 1)

    def _scan_stats(self, sql):
        tdSql.query(f"explain analyze verbose true {sql}")
        text = "\n".join(str(row[0]) for row in tdSql.queryResult)
        stats = {}
        for key in ("total_blocks", "sma_load_blocks", "check_rows"):
            matched = re.search(rf"{key}=(\d+)", text)
            assert matched, f"'{key}' missing from explain analyze output:\n{text}"
            stats[key] = int(matched.group(1))
        return stats

    def _append_block_to_t(self, dbname, ts_offset, value_base, w):
        values = ",".join(
            f"({self.base_ts + ts_offset + i},{value_base + i},{w})"
            for i in range(self.block_rows)
        )
        tdSql.execute(f"insert into {dbname}.t values {values}")

    def _append_block_to_t2(self, dbname, ts_offset, value_base):
        values = ",".join(
            f"({self.base_ts + ts_offset + i},{value_base + i})"
            for i in range(self.block_rows)
        )
        tdSql.execute(f"insert into {dbname}.t2 values {values}")

    def _assert_answers_after_append(self, dbname):
        rows = 4 * self.block_rows
        tdSql.query(f"select count(*) from {dbname}.t")
        tdSql.checkData(0, 0, rows)
        tdSql.query(f"select count(*) from {dbname}.t where v is not null")
        tdSql.checkData(0, 0, rows)
        tdSql.query(f"select last(v) from {dbname}.t")
        tdSql.checkData(0, 0, 4000 + self.block_rows - 1)
        tdSql.query(f"select max(v) from {dbname}.t")
        tdSql.checkData(0, 0, 4000 + self.block_rows - 1)
        tdSql.query(f"select top(v,2) from {dbname}.t")
        tdSql.checkEqual(
            sorted(row[0] for row in tdSql.queryResult),
            [4000 + self.block_rows - 2, 4000 + self.block_rows - 1],
        )
        tdSql.query(f"select last(v) from {dbname}.t partition by w")
        tdSql.checkRows(4)
        tdSql.checkEqual(
            sorted(row[0] for row in tdSql.queryResult),
            [
                1000 + self.block_rows - 1,
                2000 + self.block_rows - 1,
                3000 + self.block_rows - 1,
                4000 + self.block_rows - 1,
            ],
        )

    def test_repair_without_sma_same_table_write_keeps_old_answers(self):
        """ Fun: appending to the repaired table keeps its old answers

        1. Build the block-SMA fixture and run a head-only repair after the
           .sma file is removed.
        2. Append a new block to the repaired table itself, which recreates the
           .sma and rewrites the carried blocks (same-table carry path).
        3. LAST/MAX/TOP/count and the partitioned LAST over the four blocks must
           be correct, and the block SMA must be usable again.

        Catalog:
            - Function:Selection

        Since: v3.4.4.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-11-25 Yihao Deng Cover the same-table carry path after a repair

        """
        fixture = self._prepare_sma_fixture()
        dbname = fixture["dbname"]
        vnode_id = fixture["vnode_id"]
        fid = fixture["fid"]
        self._assert_expected_results()
        try:
            tdDnodes.stop(1)
            time.sleep(2)
            os.remove(fixture["sma_path"])
            code, _ = self._run_force_repair(
                vnode_id, fid, strategy="head_only_rebuild", timeout_sec=30
            )
        finally:
            self._restart_taosd_and_wait_ready(dbname=dbname)
        tdSql.checkEqual(code, 0)
        self._assert_expected_results()

        self._append_block_to_t(dbname, 500000, 4000, 4)
        tdSql.execute(f"flush database {dbname}")
        deadline = time.time() + 30
        sma_after = None
        while time.time() < deadline:
            sma_after = self._find_first_tsdb_file(vnode_id, ".sma")
            if sma_after is not None:
                break
            time.sleep(1)
        tdSql.checkEqual(sma_after is not None, True)
        tdSql.checkEqual(self._parse_fid_from_tsdb_path(sma_after), fid)

        self._assert_answers_after_append(dbname)
        stats = self._scan_stats(f"select top(v,2) from {dbname}.t")
        tdLog.info("stats after same-table append: %s" % stats)
        tdSql.checkEqual(stats["sma_load_blocks"] > 0, True)
        tdSql.execute(f"drop database if exists {dbname}")

    def test_repeated_writes_without_repair_keep_statistics(self):
        """ Fun: repeated writes without a repair keep the block SMA usable

        Control for the repair scenarios: write another table and append to the
        main table without touching the .sma file. Answers and the block SMA
        must stay correct, so the normal (zero-copy) carry path stays guarded.

        Catalog:
            - Function:Selection

        Since: v3.4.4.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-11-25 Yihao Deng Control for the repair scenarios

        """
        fixture = self._prepare_sma_fixture()
        dbname = fixture["dbname"]
        self._assert_expected_results()
        baseline = self._scan_stats(f"select top(v,2) from {dbname}.t")
        tdSql.checkEqual(baseline["sma_load_blocks"] > 0, True)

        tdSql.execute(f"create table {dbname}.t2(ts timestamp, v int) sma(v)")
        self._append_block_to_t2(dbname, 100000, 0)
        tdSql.execute(f"flush database {dbname}")
        time.sleep(2)
        self._assert_expected_results()

        self._append_block_to_t(dbname, 500000, 4000, 4)
        tdSql.execute(f"flush database {dbname}")
        time.sleep(2)
        self._assert_answers_after_append(dbname)
        stats = self._scan_stats(f"select top(v,2) from {dbname}.t")
        tdLog.info("stats after normal appends: %s" % stats)
        tdSql.checkEqual(stats["sma_load_blocks"] > 0, True)
        tdSql.execute(f"drop database if exists {dbname}")

    def test_head_only_repair_without_sma_keeps_queries_correct(self):
        """ Fun: head-only repair that drops the sma file keeps LAST/MAX/TOP correct

        1. Build a table with block SMA on v and three flushed blocks.
        2. Remove the .sma file and run a head_only_rebuild repair on that file set.
        3. Restart and verify the file set is read without the sma file: no crash, and
           LAST, MAX, TOP, the partitioned LAST and the IS NOT NULL count all return the
           same answers as before the repair (the missing statistics are unknown, not
           all-NULL).

        Catalog:
            - Function:Selection

        Since: v3.4.4.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-11-23 Yihao Deng Add regression for queries on a file set repaired without sma

        """
        fixture = self._prepare_sma_fixture()
        dbname = fixture["dbname"]
        vnode_id = fixture["vnode_id"]
        fid = fixture["fid"]
        self._assert_expected_results()
        try:
            tdDnodes.stop(1)
            time.sleep(2)
            tdSql.checkEqual(os.path.exists(fixture["sma_path"]), True)
            os.remove(fixture["sma_path"])
            code, _ = self._run_force_repair(
                vnode_id, fid, strategy="head_only_rebuild", timeout_sec=30
            )
        finally:
            self._restart_taosd_and_wait_ready(dbname=dbname)
        tdSql.checkEqual(code, 0)
        tdSql.checkEqual(os.path.exists(fixture["sma_path"]), False)
        self._assert_expected_results()
        tdSql.execute(f"drop database if exists {dbname}")

    def test_repair_without_sma_keeps_results_after_new_write(self):
        """ Fun: a repaired file set whose sma file reappears keeps LAST/COUNT correct

        1. Build the same fixture with block sma on v and three flushed blocks.
        2. Remove the .sma file, run head_only_rebuild, restart, and verify results.
        3. Write another table in the same vnode and flush so a .sma file exists
           again for the repaired file set.
        4. Verify the original table still returns the same LAST/MAX/TOP/count
           answers once the recreated .sma file is present again, so empty or
           stale block records can never be read as valid statistics just
           because a .sma file exists.

        Catalog:
            - Function:Selection

        Since: v3.4.4.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-11-25 Yihao Deng Extend the repair regression to the sma-reappears state

        """
        fixture = self._prepare_sma_fixture()
        dbname = fixture["dbname"]
        vnode_id = fixture["vnode_id"]
        fid = fixture["fid"]
        self._assert_expected_results()
        try:
            tdDnodes.stop(1)
            time.sleep(2)
            os.remove(fixture["sma_path"])
            code, _ = self._run_force_repair(
                vnode_id, fid, strategy="head_only_rebuild", timeout_sec=30
            )
        finally:
            self._restart_taosd_and_wait_ready(dbname=dbname)
        tdSql.checkEqual(code, 0)
        self._assert_expected_results()

        # Make a sma file appear again in the same file set (same fid) by writing
        # another table and flushing. The dangerous state is a recreated .sma
        # next to records the repair left without statistics.
        tdSql.execute(f"create table {dbname}.t2(ts timestamp, v int) sma(v)")
        values = ",".join(
            f"({self.base_ts + 100000 + i},{i})" for i in range(self.block_rows)
        )
        tdSql.execute(f"insert into {dbname}.t2 values {values}")
        tdSql.execute(f"flush database {dbname}")

        deadline = time.time() + 30
        sma_after = None
        while time.time() < deadline:
            sma_after = self._find_first_tsdb_file(vnode_id, ".sma")
            if sma_after is not None:
                break
            time.sleep(1)
        tdSql.checkEqual(sma_after is not None, True)
        fid_after = self._parse_fid_from_tsdb_path(sma_after)
        tdLog.info(
            "sma after repair+write: %s (fid %s, repaired fid %s)"
            % (sma_after, fid_after, fid)
        )
        tdSql.checkEqual(fid_after, fid)
        self._assert_expected_results()

        # The carried blocks were rewritten with recomputed statistics, so the
        # block SMA is usable again (no permanent loss of pruning after a repair).
        stats = self._scan_stats(f"select top(v,2) from {dbname}.t")
        tdLog.info("stats after sma recreation: %s" % stats)
        tdSql.checkEqual(stats["sma_load_blocks"] > 0, True)

        # COMPACT DATABASE is enterprise-only (PAR_ENTERPRISE_ONLY), so the
        # community CI cannot force an explicit merge. Repeat the write/flush
        # cycle instead: the first write recovered the statistics above and the
        # later commits carry the records forward, they must stay correct. With
        # stt_trigger=1 the background merge folds the new STT files in between
        # the cycles.
        for round in range(2):
            values = ",".join(
                f"({self.base_ts + (round + 2) * 100000 + i},{i})"
                for i in range(self.block_rows)
            )
            tdSql.execute(f"insert into {dbname}.t2 values {values}")
            tdSql.execute(f"flush database {dbname}")
            time.sleep(3)
            deadline = time.time() + 30
            while time.time() < deadline:
                if self._find_first_tsdb_file(vnode_id, ".sma") is not None:
                    break
                time.sleep(1)
            self._assert_expected_results()

        tdSql.execute(f"drop database if exists {dbname}")
