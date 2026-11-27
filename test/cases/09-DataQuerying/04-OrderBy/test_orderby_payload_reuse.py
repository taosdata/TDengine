# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL-3.0-or-later") as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.

from collections import defaultdict

from new_test_framework.utils import tdLog, tdSql


class TestOrderByPayloadReuse:
    dbname = "orderby_payload"
    rows = 30000
    # Independent NULL expectations from the generator contract (i from 0):
    # msg is NULL on i % 13 == 0; d is NULL on i % 17 == 0 and i % 13 != 0.
    msg_nulls = len(range(0, rows, 13))
    d_nulls = len(range(0, rows, 17)) - len(range(0, rows, 13 * 17))
    msg_or_d_nulls = msg_nulls + d_nulls
    spill_rows = 60000

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def prepare_db(self):
        tdSql.prepare(dbname=self.dbname, drop=True, vgroups=1, precision="ms")

    def build_table(self):
        # Enough rows to spill the sort buffer into several output blocks, so
        # the operator reuses its tuple/output blocks many times per query.
        tdSql.execute(f"create table {self.dbname}.t(ts timestamp, k int, msg nchar(64), "
                      f"b binary(32), d double)")
        batch = 2000
        for start in range(0, self.rows, batch):
            vals = []
            for i in range(start, min(start + batch, self.rows)):
                if i % 13 == 0:
                    vals.append(f"({1700000000000 + i},{i % 1000},NULL,NULL,{i}.5)")
                elif i % 17 == 0:
                    vals.append(f"({1700000000000 + i},{i % 1000},'m{i}','b{i}',NULL)")
                else:
                    vals.append(f"({1700000000000 + i},{i % 1000},'m{i % 97}','b{i % 31}',{i}.5)")
            tdSql.execute(f"insert into {self.dbname}.t values " + ",".join(vals))

    def check_payload_follows_key(self):
        # Sorting on the integer key moves whole rows: every column, including
        # the variable-width payload and its NULL rows, must travel with its key.
        tdSql.query(f"select k, ts, msg, b, d from {self.dbname}.t")
        unordered = [tuple(row) for row in tdSql.queryResult]
        tdSql.checkRows(self.rows)

        tdSql.query(f"select k, ts, msg, b, d from {self.dbname}.t order by k")
        ordered = [tuple(row) for row in tdSql.queryResult]
        tdSql.checkRows(self.rows)
        tdSql.checkEqual([row[0] for row in ordered], sorted(row[0] for row in ordered))
        tdSql.checkEqual(sorted(unordered), sorted(ordered))

        nulls_after = sum(1 for row in ordered if row[2] is None or row[4] is None)
        # Independent expectation, not a self-comparison: the generator makes
        # exactly msg_or_d_nulls rows with a NULL in msg or d.
        tdSql.checkEqual(nulls_after, self.msg_or_d_nulls)

    def check_varlen_key(self):
        # A variable-width sort key takes the generic comparator plus the varlen
        # gather branch; NULL rows must be kept and the rest stay ordered.
        for direction in ("asc", "desc"):
            tdSql.query(f"select msg, k from {self.dbname}.t order by msg {direction}")
            tdSql.checkRows(self.rows)
            values = [row[0] for row in tdSql.queryResult]
            nonnull = [v for v in values if v is not None]
            tdSql.checkEqual(nonnull, sorted(nonnull, reverse=(direction == "desc")))
            # Independent expectation: exactly msg_nulls generated NULL keys.
            tdSql.checkEqual(len(nonnull), self.rows - self.msg_nulls)
            # ASC treats NULL as smallest (head), DESC puts it at the tail.
            if direction == "asc":
                tdSql.checkEqual(values[: self.msg_nulls], [None] * self.msg_nulls)
            else:
                tdSql.checkEqual(values[-self.msg_nulls:], [None] * self.msg_nulls)

    def check_repeated_runs(self):
        # A stale buffer in the reused tuple block shows up on a later fill, so
        # the same query must return identical rows every time it is executed.
        tdSql.query(f"select k, ts, msg, b, d from {self.dbname}.t order by k desc")
        first = [tuple(row) for row in tdSql.queryResult]
        for _ in range(3):
            tdSql.query(f"select k, ts, msg, b, d from {self.dbname}.t order by k desc")
            tdSql.checkEqual([tuple(row) for row in tdSql.queryResult], first)

    def build_spill_table(self):
        # Wide rows so one query holds several times the 4 MiB sort buffer
        # (numOfPages=1024, pageSize>=4096): the sort handle then sorts and
        # flushes repeatedly, re-gathering into the same SBlockSortScratch
        # across rounds, which the narrow rows in t can never reach.
        tdSql.execute(f"create table {self.dbname}.spill(ts timestamp, k int, msg nchar(256))")
        batch = 2000
        payload = "x" * 200
        for start in range(0, self.spill_rows, batch):
            vals = []
            for i in range(start, min(start + batch, self.spill_rows)):
                vals.append(f"({1700000000000 + i},{i % 997},'{payload}{i % 10}')")
            tdSql.execute(f"insert into {self.dbname}.spill values " + ",".join(vals))

    def check_spill_reuse(self):
        # Same-handle multi-round sort: verify the row multiset, the key order
        # and run-to-run stability once the scratch has been flushed and reused.
        tdSql.query(f"select k, msg from {self.dbname}.spill")
        unordered = [tuple(row) for row in tdSql.queryResult]
        tdSql.checkRows(self.spill_rows)
        tdSql.query(f"select k, msg from {self.dbname}.spill order by k")
        ordered = [tuple(row) for row in tdSql.queryResult]
        tdSql.checkRows(self.spill_rows)
        tdSql.checkEqual([row[0] for row in ordered], sorted(row[0] for row in ordered))
        tdSql.checkEqual(sorted(unordered), sorted(ordered))
        for _ in range(2):
            tdSql.query(f"select k, msg from {self.dbname}.spill order by k")
            tdSql.checkEqual([tuple(row) for row in tdSql.queryResult], ordered)

    def check_key_groups(self):
        # Tie order is unspecified; the per-key row multiset is not.
        tdSql.query(f"select k, ts, msg from {self.dbname}.t order by k")
        ordered = [tuple(row) for row in tdSql.queryResult]
        tdSql.query(f"select k, ts, msg from {self.dbname}.t")
        unordered = [tuple(row) for row in tdSql.queryResult]

        def grouped(rows):
            buckets = defaultdict(list)
            for row in rows:
                buckets[row[0]].append(row)
            return {k: sorted(v) for k, v in buckets.items()}

        tdSql.checkEqual(grouped(ordered), grouped(unordered))

    def test_orderby_payload_reuse(self):
        """ Fun: ORDER BY keeps variable-width payload and NULLs across reused blocks

        1. Build a table larger than the sort buffer with a NULL pattern in the
           variable-width columns.
        2. Order by the integer key and verify the full row multiset, the NULL
           count (independently computed from the generator) and the key order
           survive the gather into reused output blocks.
        3. Order by the variable-width column in both directions and check the
           generated NULL count and NULL placement.
        4. Repeat the same query and require identical results on every run.
        5. Build a wide-row table several times the 4 MiB sort buffer and
           verify one handle sorts and flushes repeatedly while reusing the
           same scratch: multiset, order and run-to-run stability.

        Since: v3.5.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-11-11 Yihao Deng Add payload and block reuse coverage for 6537430348
            - 2026-11-16 Yihao Deng Add independent NULL counts and same-handle spill coverage per review

        """
        self.prepare_db()
        self.build_table()
        self.check_payload_follows_key()
        self.check_varlen_key()
        self.check_key_groups()
        self.check_repeated_runs()
        self.build_spill_table()
        self.check_spill_reuse()
