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

import os


class TestPartitionFirstStateWindow:
    DB = os.getenv("PF_STATE_TEST_DB", "pf_state_window")
    STB = "meters"
    TABLE_COUNT = 12
    ROWS_PER_TABLE = 6
    BASE_TS = 1750000000000
    STEP_MS = 1000

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # --- util ---

    def _prepare_db(self, vgroups):
        tdSql.execute(f"drop database if exists {self.DB}")
        tdSql.execute(
            f"create database {self.DB} vgroups {vgroups} precision 'ms'"
        )
        tdSql.execute(f"use {self.DB}")
        tdSql.execute(
            f"create stable {self.STB} ("
            "ts timestamp, seq int primary key, val int, st int, category int) "
            "tags (gid int, region varchar(16))"
        )

        states = (0, 0, 1, 1, 0, 0)
        for table_index in range(self.TABLE_COUNT):
            gid = "null" if table_index == self.TABLE_COUNT - 1 else table_index % 4
            table_name = f"d{table_index}"
            tdSql.execute(
                f"create table {table_name} using {self.STB} "
                f"tags ({gid}, 'r{table_index % 2}')"
            )
            values = []
            for row_index, state in enumerate(states):
                ts = self.BASE_TS + row_index * self.STEP_MS
                seq = table_index * 100 + row_index
                val = table_index * 10 + row_index
                category = (
                    "null"
                    if table_index == self.TABLE_COUNT - 1
                    and row_index == len(states) - 1
                    else str((table_index + row_index) % 4)
                )
                values.append(f"({ts}, {seq}, {val}, {state}, {category})")
            if table_index % 2:
                values.reverse()
            tdSql.execute(
                f"insert into {table_name} values " + " ".join(values)
            )

        tdSql.execute(f"flush database {self.DB}")

    def _explain_text(self, sql, verbose=False):
        explain = "explain verbose true" if verbose else "explain"
        tdSql.query(f"{explain} {sql}")
        return "\n".join(
            str(tdSql.getData(row, 0)) for row in range(tdSql.getRows())
        )

    def _prepare_sort_group_db(self, vgroups, with_pk):
        tdSql.execute(f"drop database if exists {self.DB}")
        tdSql.execute(f"create database {self.DB} vgroups {vgroups}")
        tdSql.execute(f"use {self.DB}")
        pk_column = "seq int primary key, " if with_pk else ""
        tdSql.execute(
            f"create stable {self.STB} (ts timestamp, {pk_column}"
            "val int, st int, category int) tags (gid int)"
        )
        # The first window spans timestamps and, with a PK, distinct keys at
        # the same timestamp. A time key must never become a partition key.
        rows = [(0, 1, 10, 0), (1000, 1, 20, 0), (2000, 1, 40, 1)]
        if with_pk:
            rows = [(0, 1, 10, 0), (0, 2, 20, 0),
                    (1000, 1, 30, 0), (2000, 1, 40, 1)]
        for table_index in range(self.TABLE_COUNT):
            category = "null" if table_index % 2 else "7"
            table_name = f"d{table_index}"
            tdSql.execute(
                f"create table {table_name} using {self.STB} tags (1)"
            )
            values = []
            for offset, seq, val, state in reversed(rows):
                pk_value = f"{seq}, " if with_pk else ""
                values.append(
                    f"({self.BASE_TS + offset}, {pk_value}"
                    f"{val}, {state}, {category})"
                )
            tdSql.execute(f"insert into {table_name} values " + " ".join(values))
        tdSql.execute(f"flush database {self.DB}")

    def _check_sort_group_windows(self, partition, projection, expected):
        for hint in ("", "partition_first()", "sort_for_group()",
                     "partition_first() sort_for_group()"):
            hint_clause = f"/*+ {hint} */ " if hint else ""
            sql = (
                f"select {hint_clause}{projection}, first(st), count(*), "
                f"sum(val) from {self.STB} partition by {partition} "
                f"state_window(st) order by {partition}, _wstart"
            )
            tdSql.checkDataMem(sql, expected)

    # --- impl ---

    def do_plan_shape(self):
        self._prepare_db(vgroups=1)
        plain_sql = (
            f"select gid, _wstart, count(*) from {self.STB} "
            "partition by gid state_window(st)"
        )
        hinted_sql = (
            f"select /*+ partition_first() */ gid, _wstart, count(*) "
            f"from {self.STB} partition by gid state_window(st)"
        )

        plain_plan = self._explain_text(plain_sql)
        hinted_plan = self._explain_text(hinted_sql)
        tdLog.info(f"default plan:\n{plain_plan}")
        tdLog.info(f"partition-first plan:\n{hinted_plan}")

        tdSql.checkAssert(
            "Table Merge Scan" in plain_plan or "TableMergeScan" in plain_plan
        )
        tdSql.checkAssert("Partition" not in plain_plan)
        tdSql.checkAssert(
            "Table Merge Scan" in hinted_plan or "TableMergeScan" in hinted_plan
        )
        tdSql.checkAssert("Partition" in hinted_plan)

        self._prepare_db(vgroups=3)
        plain_sql = (
            f"select category, _wstart, count(*) from {self.STB} "
            "partition by category state_window(st)"
        )
        hinted_sql = (
            f"select /*+ partition_first() */ category, _wstart, count(*) "
            f"from {self.STB} partition by category state_window(st)"
        )
        plain_plan = self._explain_text(plain_sql, verbose=True)
        hinted_plan = self._explain_text(hinted_sql, verbose=True)
        tdSql.checkAssert("Partition" in plain_plan)
        tdSql.checkAssert("Partition" in hinted_plan)
        tdSql.checkAssert(
            "Table Merge Scan" in hinted_plan or "TableMergeScan" in hinted_plan
        )
        tdSql.checkAssert("Merge Key: _group_id asc, ts asc" in plain_plan)
        tdSql.checkAssert("Merge Key: _group_id asc, ts asc" in hinted_plan)
        tdSql.checkAssert(", seq asc" in plain_plan)
        tdSql.checkAssert(", seq asc" in hinted_plan)

        print("partition-first state-window plan ............... [ passed ]")

    def do_tag_partition_parity(self):
        for vgroups in (1, 3):
            self._prepare_db(vgroups)
            plain_sql = (
                f"select gid, _wstart, count(*), sum(val) from {self.STB} "
                "partition by gid state_window(st) order by gid, _wstart"
            )
            hinted_sql = (
                f"select /*+ partition_first() */ gid, _wstart, count(*), "
                f"sum(val) from {self.STB} partition by gid state_window(st) "
                "order by gid, _wstart"
            )
            tdSql.checkResultsBySql(hinted_sql, plain_sql, retry=1)
            tdSql.query(hinted_sql)
            total_rows = sum(row[2] for row in tdSql.queryResult)
            expected_rows = self.TABLE_COUNT * self.ROWS_PER_TABLE
            tdSql.checkEqual(total_rows, expected_rows)

            plain_sql = (
                f"select gid, region, _wstart, count(*) from {self.STB} "
                "partition by gid, region state_window(st) "
                "order by gid, region, _wstart"
            )
            hinted_sql = (
                f"select /*+ partition_first() */ gid, region, _wstart, "
                f"count(*) from {self.STB} partition by gid, region "
                "state_window(st) order by gid, region, _wstart"
            )
            tdSql.checkResultsBySql(hinted_sql, plain_sql, retry=1)

        print("partition-first tag parity ...................... [ passed ]")

    def do_column_partition_parity(self):
        for vgroups in (1, 3):
            self._prepare_db(vgroups)
            plain_sql = (
                f"select category, _wstart, count(*), sum(val) from {self.STB} "
                "partition by category state_window(st) "
                "order by category, _wstart"
            )
            hinted_sql = (
                f"select /*+ partition_first() */ category, _wstart, "
                f"count(*), sum(val) from {self.STB} partition by category "
                "state_window(st) order by category, _wstart"
            )
            tdSql.checkResultsBySql(hinted_sql, plain_sql, retry=1)
            tdSql.query(hinted_sql)
            tdSql.checkEqual(
                sum(row[2] for row in tdSql.queryResult),
                self.TABLE_COUNT * self.ROWS_PER_TABLE,
            )

            plain_sql = (
                f"select gid, category, _wstart, count(*) from {self.STB} "
                "partition by gid, category state_window(st) "
                "order by gid, category, _wstart"
            )
            hinted_sql = (
                f"select /*+ partition_first() */ gid, category, _wstart, "
                f"count(*) from {self.STB} partition by gid, category "
                "state_window(st) order by gid, category, _wstart"
            )
            tdSql.checkResultsBySql(hinted_sql, plain_sql, retry=1)

        print("partition-first column parity ................... [ passed ]")

    def do_tbname_partition_parity(self):
        self._prepare_db(vgroups=3)
        plain_sql = (
            f"select tbname, _wstart, count(*), sum(val) from {self.STB} "
            "partition by tbname state_window(st) order by tbname, _wstart"
        )
        hinted_sql = (
            f"select /*+ partition_first() */ tbname, _wstart, count(*), "
            f"sum(val) from {self.STB} partition by tbname state_window(st) "
            "order by tbname, _wstart"
        )
        tdSql.checkResultsBySql(hinted_sql, plain_sql, retry=1)
        tdSql.query(hinted_sql)
        tdSql.checkEqual(
            sum(row[2] for row in tdSql.queryResult),
            self.TABLE_COUNT * self.ROWS_PER_TABLE,
        )

        print("partition-first tbname parity ................... [ passed ]")

    def do_filter_boundaries(self):
        self._prepare_db(vgroups=3)
        plain_sql = (
            f"select gid, _wstart, count(*) from {self.STB} "
            f"where ts = {self.BASE_TS} partition by gid "
            "state_window(st) order by gid, _wstart"
        )
        hinted_sql = (
            f"select /*+ partition_first() */ gid, _wstart, count(*) "
            f"from {self.STB} where ts = {self.BASE_TS} partition by gid "
            "state_window(st) order by gid, _wstart"
        )
        tdSql.checkResultsBySql(hinted_sql, plain_sql, retry=1)
        tdSql.query(hinted_sql)
        tdSql.checkEqual(sum(row[2] for row in tdSql.queryResult), self.TABLE_COUNT)

        for table_index in range(self.TABLE_COUNT):
            seq = 10000 + table_index
            # Tie timestamps within a state to exercise second-PK ordering.
            # Distinct states use distinct window starts: changing how the
            # executor identifies same-start windows is outside this feature.
            window_index = table_index // 2
            ts = self.BASE_TS + window_index * self.STEP_MS
            tdSql.execute(
                f"insert into d{table_index} values "
                f"({ts}, {seq}, {seq}, {window_index % 2}, 7)"
            )
        tdSql.execute(f"flush database {self.DB}")

        expected = [
            [7, 10000 + index, (index // 2) % 2,
             min(2, self.TABLE_COUNT - index)]
            for index in range(0, self.TABLE_COUNT, 2)
        ]
        for hint in ("", "partition_first()", "sort_for_group()",
                     "partition_first() sort_for_group()"):
            hint_clause = f"/*+ {hint} */ " if hint else ""
            sql = (
                f"select {hint_clause}category, first(seq) as start_seq, "
                f"first(st), count(*) from {self.STB} where category = 7 "
                "partition by category state_window(st) order by start_seq"
            )
            tdSql.checkDataMem(sql, expected)

        plain_sql = (
            f"select gid, _wstart, count(*) from {self.STB} "
            f"where ts < {self.BASE_TS} partition by gid state_window(st)"
        )
        hinted_sql = (
            f"select /*+ partition_first() */ gid, _wstart, count(*) "
            f"from {self.STB} where ts < {self.BASE_TS} "
            "partition by gid state_window(st)"
        )
        tdSql.checkResultsBySql(hinted_sql, plain_sql, retry=1)
        tdSql.query(hinted_sql)
        tdSql.checkRows(0)

        print("partition-first filter boundaries ............... [ passed ]")

    def do_sort_group_identity(self):
        per_group = self.TABLE_COUNT // 2
        for vgroups in (1, 3):
            for with_pk in (False, True):
                self._prepare_sort_group_db(vgroups, with_pk)
                count = 3 if with_pk else 2
                value_sum = 60 if with_pk else 30
                expected = []
                for category in (None, 7):
                    expected.extend([
                        [category, 0, count * per_group, value_sum * per_group],
                        [category, 1, per_group, 40 * per_group],
                    ])
                self._check_sort_group_windows("category", "category", expected)
                self._check_sort_group_windows(
                    "gid, category", "gid, category", [[1] + row for row in expected]
                )
                self._check_sort_group_windows(
                    "category + 1", "category + 1",
                    [[None if row[0] is None else 8] + row[1:] for row in expected],
                )

                # Explicit user partition keys must remain part of groupId,
                # even when they are also timestamp/second-PK ordering keys.
                expected = []
                for category in (None, 7):
                    for offset, state, rows, total in (
                        (0, 0, 2 if with_pk else 1, 30 if with_pk else 10),
                        (1000, 0, 1, 30 if with_pk else 20),
                        (2000, 1, 1, 40),
                    ):
                        expected.append([
                            category, self.BASE_TS + offset, state,
                            rows * per_group, total * per_group,
                        ])
                self._check_sort_group_windows(
                    "category, ts", "category, cast(ts as bigint)", expected
                )
                if with_pk:
                    expected = []
                    for category in (None, 7):
                        expected.extend([
                            [category, 1, 0, 2 * per_group, 40 * per_group],
                            [category, 1, 1, per_group, 40 * per_group],
                            [category, 2, 0, per_group, 20 * per_group],
                        ])
                    self._check_sort_group_windows(
                        "category, seq", "category, seq", expected
                    )
                    expected = []
                    for category in (None, 7):
                        for offset, seq, state, val in (
                            (0, 1, 0, 10), (0, 2, 0, 20),
                            (1000, 1, 0, 30), (2000, 1, 1, 40),
                        ):
                            expected.append([
                                category, self.BASE_TS + offset, seq, state,
                                per_group, val * per_group,
                            ])
                    self._check_sort_group_windows(
                        "category, ts, seq",
                        "category, cast(ts as bigint), seq", expected
                    )

        print("sort-for-group state-window identity ............ [ passed ]")

    # --- main ---

    def test_partition_first_state_window(self):
        """PARTITION_FIRST keeps tag partition above state-window merge scan

        1. Verify default and hinted physical plan shapes
        2. Compare tag and multi-tag results on one and three vnodes
        3. Compare regular-column and mixed tag-column partitions
        4. Compare tbname partition results
        5. Cover NULL values, duplicate timestamps, primary keys, and empty input
        6. Keep continuous state windows and explicit timestamp/PK partitions

        Catalog:
            - Query

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-24 Peng Rongkun Add partition-first state-window coverage
            - 2026-08-25 Peng Rongkun Add regular-column partition coverage
            - 2026-09-03 Peng Rongkun Cover sort-for-group and out-of-order writes
            - 2026-09-07 Peng Rongkun Use framework assertions for result checks
            - 2026-09-10 Peng Rongkun Separate sort keys from partition identity
            - 2026-09-17 Peng Rongkun Keep boundary coverage within existing window semantics

        """
        self.do_plan_shape()
        self.do_tag_partition_parity()
        self.do_column_partition_parity()
        self.do_tbname_partition_parity()
        self.do_filter_boundaries()
        self.do_sort_group_identity()
