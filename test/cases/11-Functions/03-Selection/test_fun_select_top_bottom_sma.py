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

import re
import time

from new_test_framework.utils import tdLog, tdSql

TYPE_MATRIX = (
    ("tinyint", "tinyint"),
    ("smallint", "smallint"),
    ("int", "int"),
    ("bigint", "bigint"),
    ("tinyint unsigned", "utinyint"),
    ("smallint unsigned", "usmallint"),
    ("int unsigned", "uint"),
    ("bigint unsigned", "ubigint"),
    ("float", "float"),
    ("double", "double"),
)
LIMITS = (1, 3, 10, 100)


class TestSelectTopBottomSma:
    dbname = "top_bottom_sma"
    block_rows = 200
    large_block_rows = 40000
    large_insert_batch = 2000

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def prepare_db(self):
        tdSql.prepare(
            dbname=self.dbname,
            drop=True,
            vgroups=1,
            precision="ms",
            keep=36500,
            duration=10,
            stt_trigger=1,
            minrows=self.block_rows,
            maxrows=self.block_rows,
        )

    def insert_block(self, table, start_ts, value):
        values = ",".join(
            f"({start_ts + i},{value},{start_ts + i})" for i in range(self.block_rows)
        )
        tdSql.execute(f"insert into {table} values {values}")

    def insert_value_block(self, table, start_ts, value):
        values = ",".join(
            f"({start_ts + i},{value})" for i in range(self.block_rows)
        )
        tdSql.execute(f"insert into {table} values {values}")

    def flush_db(self, dbname=None):
        tdSql.execute(f"flush database {dbname or self.dbname}")
        time.sleep(0.5)

    def query_values(self, sql):
        tdSql.query(sql)
        return [row[0] for row in tdSql.queryResult]

    def check_top_bottom(
        self, table, limit=3, where_clause=None, column="v", expected_nonnull=False
    ):
        suffix = f" where {where_clause}" if where_clause else ""
        expected_suffix = suffix
        if expected_nonnull:
            expected_suffix += " and" if suffix else " where"
            expected_suffix += f" {column} is not null"
        for function, direction in (("top", "desc"), ("bottom", "asc")):
            sql = f"select {function}({column},{limit}) from {table}{suffix}"
            actual = sorted(
                self.query_values(sql), reverse=function == "top"
            )
            expected = self.query_values(
                f"select {column} from {table}{expected_suffix} "
                f"order by {column} {direction} limit {limit}"
            )
            tdSql.checkEqual(actual, expected)

    def scan_stats(self, sql):
        tdSql.query(f"explain analyze verbose true {sql}")
        text = "\n".join(str(row[0]) for row in tdSql.queryResult)
        stats = {}
        for key in ("total_blocks", "sma_load_blocks", "check_rows"):
            matched = re.search(rf"{key}=(\d+)", text)
            assert matched, f"'{key}' missing from explain analyze output:\n{text}"
            stats[key] = int(matched.group(1))
        return stats

    def create_synthetic_table(self, stable, column_type, initial, later):
        child = f"{stable}_child"
        tdSql.execute(
            f"create stable {stable} "
            f"(ts timestamp,sma_col int,val {column_type}) "
            f"tags(t int) sma(sma_col)"
        )
        tdSql.execute(f"create table {child} using {stable} tags(1)")
        first = ",".join(
            f"({1600000000000 + i},{i},{initial})" for i in range(self.block_rows)
        )
        second = ",".join(
            f"({1600000100000 + i},{self.block_rows + i},{later})"
            for i in range(self.block_rows)
        )
        tdSql.execute(f"insert into {child} values {first}")
        tdSql.execute(f"insert into {child} values {second}")
        self.flush_db()
        self.check_top_bottom(child, column="val")

    def do_synthetic_sma_fallback(self):
        self.prepare_db()
        self.create_synthetic_table("positive_vals", "bigint", 1, 1000)
        self.create_synthetic_table("negative_vals", "bigint", -1, -1000)
        self.create_synthetic_table(
            "unsigned_vals", "bigint unsigned", 0, 18446744073709551615
        )
        # val has no persisted SMA and no other queried column has it either,
        # so a block SMA read has nothing to offer: TOP reads both blocks
        # instead of pruning the second one without touching the .sma file.
        stats = self.scan_stats("select top(val,3) from positive_vals_child")
        tdSql.checkEqual(stats["total_blocks"], 2)
        tdSql.checkEqual(stats["check_rows"], 2 * self.block_rows)
        tdSql.checkEqual(stats["sma_load_blocks"], 0)

        # Projecting sma_col makes the per-block LAST_NULL_ONLY read useful
        # again. val has no persisted SMA there, so it must stay unknown rather
        # than be synthesized as all-NULL, and TOP still reads both blocks.
        stats = self.scan_stats("select top(val,3),sma_col from positive_vals_child")
        tdSql.checkEqual(stats["check_rows"], 2 * self.block_rows)
        tdSql.checkEqual(stats["sma_load_blocks"], 2)

        # MAX without a window takes the NORMAL SMA load, which refuses the
        # block when a queried column has no SMA, so both blocks are read as
        # data and the real maximum is returned instead of NULL.
        tdSql.query("select max(val) from positive_vals_child")
        tdSql.checkData(0, 0, 1000)
        stats = self.scan_stats("select max(val) from positive_vals_child")
        tdSql.checkEqual(stats["total_blocks"], 2)
        tdSql.checkEqual(stats["check_rows"], 2 * self.block_rows)

    def create_block_table(self, table, blocks, column_type="int"):
        tdSql.execute(
            f"create table {table}(ts timestamp,v {column_type},w bigint) sma(v)"
        )
        for start_ts, value in blocks:
            self.insert_block(table, start_ts, value)
        self.flush_db()

    def do_three_block_prune(self):
        self.create_block_table(
            "skip_top", ((0, 100), (self.block_rows, 1), (2 * self.block_rows, 200))
        )
        self.check_top_bottom("skip_top")
        stats = self.scan_stats("select top(v,3) from skip_top")
        tdSql.checkEqual(stats["total_blocks"], 3)
        tdSql.checkEqual(stats["check_rows"], 2 * self.block_rows)
        tdSql.checkEqual(stats["sma_load_blocks"], 3)

        self.create_block_table(
            "skip_bottom", ((0, 100), (self.block_rows, 200), (2 * self.block_rows, 1))
        )
        self.check_top_bottom("skip_bottom")
        stats = self.scan_stats("select bottom(v,3) from skip_bottom")
        tdSql.checkEqual(stats["total_blocks"], 3)
        tdSql.checkEqual(stats["check_rows"], 2 * self.block_rows)

    def do_mixed_top_bottom_not_ignored(self):
        # Two TOP/BOTTOM functions in one select list: a block rejected by
        # both must be skipped for this block only. Putting the table on the
        # scan ignore list would drop later blocks that can still change
        # either heap.
        table = "mixed_top_bottom"
        tdSql.execute(
            f"create table {table}(ts timestamp,v int,w int) sma(v,w)"
        )
        for start_ts, v, w in (
            (0, 100, 50),
            (self.block_rows, 1, 100),
            (2 * self.block_rows, 1, 1),
            (3 * self.block_rows, 200, 200),
        ):
            values = ",".join(
                f"({start_ts + i},{v},{w})" for i in range(self.block_rows)
            )
            tdSql.execute(f"insert into {table} values {values}")
        self.flush_db()

        tdSql.query(f"select top(v,3),bottom(w,3) from {table}")
        tdSql.checkRows(3)
        tdSql.checkEqual(
            [row[0] for row in tdSql.queryResult], [200, 200, 200]
        )
        tdSql.checkEqual(
            [row[1] for row in tdSql.queryResult], [1, 1, 1]
        )
        # The second block is rejected by both functions, but the third and
        # fourth blocks must still be read.
        stats = self.scan_stats(f"select top(v,3),bottom(w,3) from {table}")
        tdSql.checkEqual(stats["total_blocks"], 4)
        tdSql.checkEqual(stats["check_rows"], 3 * self.block_rows)

    def do_equal_threshold_prune(self):
        # A block whose extreme value equals the current heap threshold must
        # still be read: the unpruned path replaces a kept row when a tied row
        # arrives, and TOP/BOTTOM also return the sibling columns of the kept
        # row, so skipping the block would change the output. Only a strictly
        # worse extreme may be pruned.
        self.create_block_table(
            "equal_top", ((0, 100), (self.block_rows, 100))
        )
        self.check_top_bottom("equal_top")
        stats = self.scan_stats("select top(v,3) from equal_top")
        tdSql.checkEqual(stats["total_blocks"], 2)
        tdSql.checkEqual(stats["check_rows"], 2 * self.block_rows)

        self.create_block_table(
            "equal_bottom", ((0, 100), (self.block_rows, 100))
        )
        self.check_top_bottom("equal_bottom")
        stats = self.scan_stats("select bottom(v,3) from equal_bottom")
        tdSql.checkEqual(stats["check_rows"], 2 * self.block_rows)

    def do_subsidiary_prune(self):
        table = "subsidiary_top"
        tdSql.execute(f"create table {table}(ts timestamp,v int,w bigint) sma(v)")
        values_one = ",".join(f"({i},{1000 + i},{i})" for i in range(self.block_rows))
        start = self.block_rows
        values_two = ",".join(
            f"({start + i},1,{start + i})" for i in range(self.block_rows)
        )
        start = 2 * self.block_rows
        values_three = ",".join(
            f"({start + i},{5000 + i},{i})" for i in range(self.block_rows)
        )
        tdSql.execute(f"insert into {table} values {values_one}")
        tdSql.execute(f"insert into {table} values {values_two}")
        tdSql.execute(f"insert into {table} values {values_three}")
        self.flush_db()

        tdSql.query(f"select top(v,3),ts,w from {table}")
        top_rows = list(tdSql.queryResult)
        expected = self.query_values(
            f"select v from {table} order by v desc limit 3"
        )
        actual = [row[0] for row in top_rows]
        tdSql.checkEqual(sorted(actual, reverse=True), expected)
        for value, ts, collateral in top_rows:
            expected_w = value - 5000 if value >= 5000 else value - 1000
            tdSql.checkEqual(collateral, expected_w)
            tdSql.query(f"select ts,w from {table} where v = {value}")
            tdSql.checkRows(1)
            row_ts, row_w = tdSql.queryResult[0]
            tdSql.checkEqual(row_w, collateral)
            tdSql.checkEqual(row_ts, ts)
        stats = self.scan_stats(f"select top(v,3) from {table}")
        tdSql.checkEqual(stats["check_rows"], 2 * self.block_rows)
        # Projecting ts and w, which have no SMA, must not disable pruning: the
        # shared LAST_NULL_ONLY read still carries v's real statistics and
        # leaves the others unknown, so the middle block is still skipped.
        stats = self.scan_stats(f"select top(v,3),ts,w from {table}")
        tdSql.checkEqual(stats["total_blocks"], 3)
        tdSql.checkEqual(stats["sma_load_blocks"], 3)
        tdSql.checkEqual(stats["check_rows"], 2 * self.block_rows)

        table = "subsidiary_bottom"
        tdSql.execute(f"create table {table}(ts timestamp,v int,w bigint) sma(v)")
        tdSql.execute(f"insert into {table} values {values_one}")
        start = self.block_rows
        values_high = ",".join(
            f"({start + i},{5000 + i},{start + i})" for i in range(self.block_rows)
        )
        start = 2 * self.block_rows
        values_low = ",".join(
            f"({start + i},{i},{start + i})" for i in range(self.block_rows)
        )
        tdSql.execute(f"insert into {table} values {values_high}")
        tdSql.execute(f"insert into {table} values {values_low}")
        self.flush_db()
        tdSql.query(f"select bottom(v,3),ts,w from {table}")
        bottom_rows = list(tdSql.queryResult)
        expected = self.query_values(
            f"select v from {table} order by v asc limit 3"
        )
        actual = [row[0] for row in bottom_rows]
        tdSql.checkEqual(sorted(actual), expected)
        for value, ts, collateral in bottom_rows:
            expected_w = value - 1000 if value >= 1000 else value + 2 * self.block_rows
            tdSql.checkEqual(collateral, expected_w)
            tdSql.query(f"select ts,w from {table} where v = {value}")
            tdSql.checkRows(1)
            row_ts, row_w = tdSql.queryResult[0]
            tdSql.checkEqual(row_w, collateral)
            tdSql.checkEqual(row_ts, ts)
        stats = self.scan_stats(f"select bottom(v,3) from {table}")
        tdSql.checkEqual(stats["check_rows"], 2 * self.block_rows)
        stats = self.scan_stats(f"select bottom(v,3),ts,w from {table}")
        tdSql.checkEqual(stats["total_blocks"], 3)
        tdSql.checkEqual(stats["sma_load_blocks"], 3)
        tdSql.checkEqual(stats["check_rows"], 2 * self.block_rows)

    def create_group_stable(self, stable, group_values):
        tdSql.execute(
            f"create stable {stable}(ts timestamp,v int) tags(gid int) sma(v)"
        )
        for gid, blocks in enumerate(group_values):
            child = f"{stable}_c{gid}"
            tdSql.execute(f"create table {child} using {stable} tags({gid})")
            for start_ts, value in blocks:
                self.insert_value_block(child, start_ts, value)
        self.flush_db()

    def check_group_top_bottom(self, stable, function, direction):
        expected = []
        for gid in (0, 1):
            child = f"{stable}_c{gid}"
            expected.append(
                [gid, sorted(
                    self.query_values(
                        f"select v from {child} order by v {direction} limit 3"
                    ),
                    reverse=function == "top",
                )]
            )
        tdSql.query(
            f"select gid,{function}(v,3) from {stable} partition by gid"
        )
        actual = {}
        for gid, value in tdSql.queryResult:
            actual.setdefault(gid, []).append(value)
        for gid, values in expected:
            tdSql.checkEqual(sorted(actual.get(gid, [])), sorted(values))
        # Each child has three blocks; only one block per child is rejectable
        # for the chosen data, so 4 of 6 blocks (800 rows) must be read.
        stats = self.scan_stats(
            f"select gid,{function}(v,3) from {stable} partition by gid"
        )
        tdSql.checkEqual(stats["total_blocks"], 6)
        tdSql.checkEqual(stats["check_rows"], 4 * self.block_rows)

    def do_group_isolation(self):
        top_blocks = {
            0: ((0, 100), (self.block_rows, 1), (2 * self.block_rows, 200)),
            1: ((0, 1), (self.block_rows, 100), (2 * self.block_rows, 50)),
        }
        self.create_group_stable("grp_top", [top_blocks[0], top_blocks[1]])
        self.check_group_top_bottom("grp_top", "top", "desc")

        bottom_blocks = {
            0: ((0, -100), (self.block_rows, -1), (2 * self.block_rows, -200)),
            1: ((0, -1), (self.block_rows, -100), (2 * self.block_rows, -50)),
        }
        self.create_group_stable("grp_bottom", [bottom_blocks[0], bottom_blocks[1]])
        self.check_group_top_bottom("grp_bottom", "bottom", "asc")

    def do_fallback_cases(self):
        tdSql.execute("create table short_values(ts timestamp,v int,w bigint) sma(v)")
        tdSql.execute("insert into short_values values (0,100,0),(1,50,1)")
        self.flush_db()
        self.check_top_bottom("short_values", limit=5)

        table = "null_block"
        tdSql.execute(f"create table {table}(ts timestamp,v int,w bigint) sma(v)")
        first = ",".join(
            f"({i},{100 if i < 3 else 'NULL'},{i})" for i in range(self.block_rows)
        )
        second = ",".join(
            f"({self.block_rows + i},NULL,{self.block_rows + i})"
            for i in range(self.block_rows)
        )
        third = ",".join(
            f"({2 * self.block_rows + i},200,{2 * self.block_rows + i})"
            for i in range(self.block_rows)
        )
        tdSql.execute(f"insert into {table} values {first}")
        tdSql.execute(f"insert into {table} values {second}")
        tdSql.execute(f"insert into {table} values {third}")
        self.flush_db()
        self.check_top_bottom(table, expected_nonnull=True)
        stats = self.scan_stats(f"select top(v,3) from {table}")
        tdSql.checkEqual(stats["total_blocks"], 3)
        tdSql.checkEqual(stats["check_rows"], 3 * self.block_rows)

        self.create_block_table(
            "filtered",
            ((0, 100), (self.block_rows, 1), (2 * self.block_rows, 200)),
        )
        self.check_top_bottom("filtered", where_clause="w >= 0")
        stats = self.scan_stats("select top(v,3) from filtered where w >= 0")
        tdSql.checkEqual(stats["check_rows"], 3 * self.block_rows)

    def do_max_dynamic_prune(self):
        table = "max_prune"
        tdSql.execute(f"create table {table}(ts timestamp,v int,w bigint) sma(v)")
        self.insert_block(table, 0, 100)
        self.insert_block(table, self.block_rows, 1)
        self.flush_db()

        tdSql.query(f"select max(v) from {table}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 100)
        # MAX keeps its dynamic prune: the second block is rejected by SMA.
        stats = self.scan_stats(f"select max(v) from {table}")
        tdSql.checkEqual(stats["total_blocks"], 2)
        tdSql.checkEqual(stats["check_rows"], 0)
        tdSql.checkEqual(stats["sma_load_blocks"], 2)

    def do_mixed_top_max_rejected(self):
        # The parser rejects mixing TOP/BOTTOM with MAX in one select list, so
        # the shared LAST_NULL_ONLY SMA mixed case cannot reach the executor.
        for function in ("top", "bottom"):
            tdSql.error(
                f"select {function}(v,3),max(v) from max_prune",
                expectedErrno=0x8000264F,
                expectErrInfo="mixed with other multi-row functions",
                fullMatched=False,
            )
        # TOP/BOTTOM cannot share a select list with an aggregate such as LAST
        # either, so the LAST+TOP case never reaches block pruning.
        for function in ("top", "bottom"):
            tdSql.error(
                f"select last(v),{function}(v,3) from max_prune",
                expectedErrno=0x8000264F,
                expectErrInfo="Multi-rows function",
                fullMatched=False,
            )

    def do_last_max_synthetic_no_prune(self):
        # v has persisted SMA, u does not. The older block holds the larger u
        # but cannot update LAST. The mixed LAST+MAX query keeps the prune
        # kinds apart (aggPruneKindsConflict) and stays on the data path, so no
        # block SMA is read and neither block is skipped.
        table = "last_max_prune"
        tdSql.execute(f"create table {table}(ts timestamp,v int,u int) sma(v)")
        values = {
            0: ",".join(f"({i},10,1000)" for i in range(self.block_rows)),
            self.block_rows: ",".join(
                f"({self.block_rows + i},1,1)" for i in range(self.block_rows)
            ),
        }
        tdSql.execute(f"insert into {table} values {values[0]}")
        tdSql.execute(f"insert into {table} values {values[self.block_rows]}")
        self.flush_db()

        tdSql.query(f"select last(v),max(u) from {table}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 1000)
        stats = self.scan_stats(f"select last(v),max(u) from {table}")
        tdSql.checkEqual(stats["total_blocks"], 2)
        tdSql.checkEqual(stats["check_rows"], 2 * self.block_rows)
        tdSql.checkEqual(stats["sma_load_blocks"], 0)

    def do_type_limit_matrix(self):
        for column_type, alias in TYPE_MATRIX:
            table = f"matrix_{alias}"
            self.create_block_table(
                table,
                ((0, 100), (self.block_rows, 50), (2 * self.block_rows, 120)),
                column_type=column_type,
            )
            for limit in LIMITS:
                self.check_top_bottom(table, limit=limit)
            stats = self.scan_stats(f"select top(v,3) from {table}")
            tdSql.checkEqual(stats["check_rows"], 2 * self.block_rows)

    def insert_large_block(self, table, start_ts, value):
        for offset in range(0, self.large_block_rows, self.large_insert_batch):
            values = ",".join(
                f"({start_ts + offset + i},{value},{offset + i})"
                for i in range(self.large_insert_batch)
            )
            tdSql.execute(f"insert into {table} values {values}")

    def do_large_block_sma_fallback(self):
        db = "top_bottom_sma_big"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(
            f"create database {db} precision 'ms' vgroups 1 keep 36500 "
            f"duration 10 minrows {self.block_rows} "
            f"maxrows {self.large_block_rows} replica 1"
        )
        tdSql.execute(f"use {db}")
        for table, first_value, last_value, function in (
            ("large_top", 100, 200, "top"),
            ("large_bottom", -100, -200, "bottom"),
        ):
            tdSql.execute(f"create table {table}(ts timestamp,v int,w int) sma(w)")
            for start_ts, value in (
                (0, first_value),
                (self.large_block_rows, last_value),
            ):
                self.insert_large_block(table, start_ts, value)
                self.flush_db(db)
            self.check_top_bottom(table)
            stats = self.scan_stats(f"select {function}(v,3) from {table}")
            tdSql.checkEqual(stats["total_blocks"], 2)
            tdSql.checkEqual(stats["check_rows"], 2 * self.large_block_rows)
            # v has no persisted SMA and w is not queried, so the block SMA
            # read is skipped and neither block may be pruned.
            tdSql.checkEqual(stats["sma_load_blocks"], 0)
        tdSql.execute(f"use {self.dbname}")
        tdSql.execute(f"drop database {db}")

    def create_interval_table(self, table, first_value, second_value):
        tdSql.execute(f"create table {table}(ts timestamp,v int) sma(v)")
        self.insert_value_block(table, 0, first_value)
        self.insert_value_block(table, 1000, second_value)
        self.flush_db()

    def check_interval_windows(self, table, function, first_value, second_value):
        tdSql.query(f"select {function}(v,3) from {table} interval(1s)")
        actual = sorted(row[0] for row in tdSql.queryResult)
        expected = sorted([first_value] * 3 + [second_value] * 3)
        tdSql.checkEqual(actual, expected)

        # Compare window starts as unix milliseconds so the check does not
        # depend on the connection or server display timezone.
        tdSql.query(
            f"select to_unixtimestamp(_wstart),{function}(v,3) "
            f"from {table} interval(1s)"
        )
        tdSql.checkRows(6)
        windows = {}
        for window_start, value in tdSql.queryResult:
            windows.setdefault(int(window_start), set()).add(value)
        tdSql.checkEqual(sorted(windows.keys()), [0, 1000])
        tdSql.checkEqual(list(windows[0]), [first_value])
        tdSql.checkEqual(list(windows[1000]), [second_value])

        stats = self.scan_stats(f"select {function}(v,3) from {table} interval(1s)")
        tdSql.checkEqual(stats["total_blocks"], 2)
        tdSql.checkEqual(stats["check_rows"], 2 * self.block_rows)
        tdSql.checkEqual(stats["sma_load_blocks"], 0)

    def do_interval_cases(self):
        self.create_interval_table("interval_top", 100, 1)
        self.check_interval_windows("interval_top", "top", 100, 1)
        self.create_interval_table("interval_bottom", 1, 100)
        self.check_interval_windows("interval_bottom", "bottom", 1, 100)

        # MAX-only interval queries use the separate SMA_LOAD path, which is
        # not the group-keyed dynamic prune used by TOP/BOTTOM.
        table = "interval_max"
        self.create_interval_table(table, 100, 1)
        tdSql.query(f"select to_unixtimestamp(_wstart),max(v) from {table} interval(1s)")
        tdSql.checkRows(2)
        tdSql.checkEqual([row[0] for row in tdSql.queryResult], [0, 1000])
        tdSql.checkEqual([row[1] for row in tdSql.queryResult], [100, 1])
        stats = self.scan_stats(f"select max(v) from {table} interval(1s)")
        tdSql.checkEqual(stats["total_blocks"], 2)
        tdSql.checkEqual(stats["check_rows"], 0)
        tdSql.checkEqual(stats["sma_load_blocks"], 2)

    def test_func_select_top_bottom_sma(self):
        """ Fun: TOP/BOTTOM with SMA on a different column

        1. Create stables that persist SMA only for a separate column.
        2. Flush consecutive blocks whose values must update an existing heap.
        3. Compare TOP/BOTTOM with ORDER BY for positive, negative, and unsigned values.
        4. Verify a column without SMA keeps unknown statistics so TOP loads both
           blocks and MAX cannot prune away real values, that projecting such a
           column does not disable pruning on an SMA column, and that the block
           SMA read is skipped when no queried column has SMA.

        Catalog:
            - Function:Selection

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-10-28 Yihao Deng Add regression for synthesized SMA pruning
            - 2026-11-23 Yihao Deng Keep SMA-disabled columns unknown during LAST_NULL_ONLY reads
            - 2026-11-23 Yihao Deng Skip the block SMA read when no queried column has SMA

        """
        self.do_synthetic_sma_fallback()

    def test_func_select_top_bottom_prune(self):
        """ Fun: TOP/BOTTOM block pruning with a real SMA

        1. Fill the heap with the first block and skip a rejectable middle block.
        2. Keep reading later blocks that contain a better value.
        3. Cover equal thresholds, subsidiary columns (which must not disable
           pruning), group isolation and fallbacks.
        4. Verify a block rejected by two mixed TOP/BOTTOM functions is skipped
           for that block only, not for the whole table.
        5. Cover all-null synthesized SMA on blocks larger than INT16_MAX rows.
        6. Run TOP/BOTTOM over a numeric type and k matrix against ORDER BY.
        7. Verify synthesized SMA never prunes for a mixed LAST+MAX query and
           that the parser rejects TOP/BOTTOM mixed with MAX or LAST.

        Catalog:
            - Function:Selection

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-11-07 Yihao Deng Add pruning and fallback regressions
            - 2026-11-14 Yihao Deng Cover mixed LAST+MAX synthesized SMA

        """
        self.prepare_db()
        self.do_three_block_prune()
        self.do_mixed_top_bottom_not_ignored()
        self.do_equal_threshold_prune()
        self.do_subsidiary_prune()
        self.do_group_isolation()
        self.do_fallback_cases()
        self.do_large_block_sma_fallback()
        self.do_max_dynamic_prune()
        self.do_mixed_top_max_rejected()
        self.do_last_max_synthetic_no_prune()
        self.do_type_limit_matrix()

    def test_func_select_top_bottom_interval(self):
        """ Fun: TOP/BOTTOM block pruning must not cross interval windows

        1. Build two 1s windows whose values are in disjoint ranges.
        2. Query TOP/BOTTOM without and with _wstart.
        3. Verify both windows are complete and interval scans load data without SMA.

        Catalog:
            - Function:Selection

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-11-07 Yihao Deng Add interval fallback regression

        """
        self.prepare_db()
        self.do_interval_cases()
