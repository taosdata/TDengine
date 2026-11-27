# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, version 3.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import os
import re
import time

from new_test_framework.utils import tdLog, tdSql
from new_test_framework.utils.srvCtl import sc


class TestOrderedTopNSuffix:
    """Verify composite-primary-key Top-N sorting and plan boundaries."""

    # The suffix observability lines are emitted at trace level (135 covers
    # debug only, so add DEBUG_TRACE=8). Keep logs synchronous so per-task
    # assertions cannot match a previous query's late write.
    updatecfgDict = {"qDebugFlag": 143, "asyncLog": 0}

    DB = "test_ordered_topn_suffix"
    BASE_TS = 1700000000000
    LOG_SKIP = "suffix skip, ordered rows"
    LOG_SORT_FLAGS = "sort flags, tsPkSort:"
    TASK_ID_PATTERN = r"TID:0x[0-9a-f]+ QID:0x[0-9a-f]+"

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def _query_rows(self, sql):
        tdSql.query(sql, show=1)
        return [
            tuple(tdSql.getData(row, col) for col in range(len(tdSql.queryResult[row])))
            for row in range(tdSql.getRows())
        ]

    def _explain(self, sql, analyze=False):
        prefix = "explain analyze " if analyze else "explain "
        tdSql.query(prefix + sql, show=1)
        return "\n".join(str(tdSql.getData(row, 0)) for row in range(tdSql.getRows()))

    def _setup_data(self):
        tdSql.execute(f"drop database if exists {self.DB}", show=1)
        tdSql.execute(f"create database {self.DB} vgroups 1 precision 'ms'", show=1)
        tdSql.execute(
            f"create table {self.DB}.pk (ts timestamp, id int primary key, v int)",
            show=1,
        )
        rows = [
            (2000, 1, 200),
            (0, 2, 20),
            (1000, 2, 120),
            (0, 1, 10),
            (2000, 2, 210),
            (1000, 1, 110),
            (0, 3, 30),
            (3000, 1, 300),
            (1000, 3, 130),
            (2000, 3, 230),
            (3000, 2, 310),
            (3000, 3, 320),
        ]
        values = " ".join(
            f"({self.BASE_TS + offset},{key},{value})" for offset, key, value in rows
        )
        tdSql.execute(f"insert into {self.DB}.pk values {values}", show=1)
        tdSql.execute(f"flush database {self.DB}", show=1)

    def _check_topn_results(self):
        cases = [
            ("ts, id", 5, 0),
            ("ts desc, id desc", 5, 0),
            ("ts, id", 5, 2),
            ("ts, id desc", 5, 0),
            ("ts, v", 5, 0),
        ]
        for order_by, limit, offset in cases:
            full_sql = f"select ts, id, v from {self.DB}.pk order by {order_by}"
            full_rows = self._query_rows(full_sql)
            topn_sql = full_sql + f" limit {limit} offset {offset}"
            topn_rows = self._query_rows(topn_sql)
            assert topn_rows == full_rows[offset:offset + limit], (
                f"Top-N differs from full ordering for {order_by}: "
                f"{topn_rows} != {full_rows[offset:offset + limit]}"
            )

        asc_ids = [row[1] for row in self._query_rows(
            f"select ts, id, v from {self.DB}.pk order by ts, id limit 5"
        )]
        desc_ids = [row[1] for row in self._query_rows(
            f"select ts, id, v from {self.DB}.pk order by ts desc, id desc limit 5"
        )]
        assert asc_ids == [1, 2, 3, 1, 2], asc_ids
        assert desc_ids == [3, 2, 1, 3, 2], desc_ids

    def _check_plan_boundaries(self):
        asc_sql = f"select ts, id, v from {self.DB}.pk order by ts, id limit 5"
        asc_plan = self._explain(asc_sql, analyze=True)
        tdLog.info(f"composite-PK ASC Top-N plan:\n{asc_plan}")
        assert "Sort" in asc_plan and "Table Scan" in asc_plan, asc_plan
        assert "Sort Key: ts id" in asc_plan, asc_plan
        assert "order=[asc|1 desc|0]" in asc_plan, asc_plan

        desc_sql = f"select ts, id, v from {self.DB}.pk order by ts desc, id desc limit 5"
        desc_plan = self._explain(desc_sql, analyze=True)
        tdLog.info(f"composite-PK DESC Top-N plan:\n{desc_plan}")
        assert "Sort Key: ts id" in desc_plan, desc_plan
        assert "order=[asc|1 desc|0]" in desc_plan, desc_plan

        natural_sql = f"select ts, id, v from {self.DB}.pk order by ts limit 5"
        natural_plan = self._explain(natural_sql)
        tdLog.info(f"natural primary-ts Top-N plan:\n{natural_plan}")
        assert "Table Scan" in natural_plan, natural_plan
        assert "Sort" not in natural_plan, natural_plan

    def _assert_topn_matches_full(self, table, order_by, limit, offset=0, select="ts, v"):
        full_sql = f"select {select} from {self.DB}.{table} order by {order_by}"
        full_rows = self._query_rows(full_sql)
        topn_rows = self._query_rows(full_sql + f" limit {limit} offset {offset}")
        assert topn_rows == full_rows[offset:offset + limit], (
            f"Top-N differs for {table} ORDER BY {order_by}: "
            f"{topn_rows} != {full_rows[offset:offset + limit]}"
        )

    def _assert_topn_matches_pq_baseline(self, table, order_by, baseline_order_by, limit, select):
        topn_sql = f"select {select} from {self.DB}.{table} order by {order_by} limit {limit}"
        base_sql = f"select {select} from {self.DB}.{table} order by {baseline_order_by} limit {limit}"
        topn_rows = self._query_rows(topn_sql)
        base_rows = self._query_rows(base_sql)
        assert topn_rows == base_rows, (
            f"Top-N differs from the plain PQ baseline for {table} ORDER BY {order_by}: "
            f"{topn_rows} != {base_rows}"
        )

    def _log_since_query_start(self, sql, marker, timeout=30):
        """Run sql and return the taosd log text written while it executes.

        Logs are written synchronously by this test's configuration, so every
        line after the recorded byte offset belongs to this query. The poll
        keeps the assertion stable if a build enables asynchronous logging.
        """
        log_path = os.path.join(sc.dnodeLogPath(1), "taosdlog.0")
        offset = os.path.getsize(log_path) if os.path.exists(log_path) else 0
        self._query_rows(sql)

        deadline = time.time() + timeout
        text = ""
        while time.time() < deadline:
            try:
                with open(log_path, "rb") as log:
                    log.seek(offset)
                    text = log.read().decode("utf-8", errors="ignore")
            except FileNotFoundError:
                text = ""
            if marker in text:
                break
            time.sleep(0.2)
        return text

    def _marker_task_ids(self, text, marker):
        """Return the task ids of all lines containing marker."""
        tasks = set()
        for line in text.splitlines():
            if marker in line:
                match = re.search(self.TASK_ID_PATTERN, line)
                if match:
                    tasks.add(match.group(0))
        return tasks

    def _check_skip_reason(self, sql, reason):
        """Assert sql reached the ordered-suffix skip with the given reason.

        All skip lines of this query must belong to one task, report a positive
        skipped row count, and carry the expected reason tag (plan or
        threshold). Returns the parsed skipped counts.
        """
        log = self._log_since_query_start(sql, self.LOG_SKIP)
        tasks = self._marker_task_ids(log, self.LOG_SKIP)
        assert len(tasks) == 1, f"skip not bound to one query task: {tasks}; sql: {sql}"
        task = tasks.pop()
        lines = [
            line
            for line in log.splitlines()
            if task in line and self.LOG_SKIP in line
        ]
        counts = []
        for line in lines:
            match = re.search(r"skipped:(\d+)", line)
            if match:
                counts.append(int(match.group(1)))
        assert counts and min(counts) > 0, (
            f"ordered-suffix skip must report a positive skipped count, got: {counts}; sql: {sql}"
        )
        assert lines and all(f"reason:{reason}" in line for line in lines), (
            f"skip lines must report reason:{reason}, got: {lines}; sql: {sql}"
        )
        return counts

    def _setup_runtime_data(self):
        tdSql.execute(f"drop database if exists {self.DB}", show=1)
        tdSql.execute(f"create database {self.DB} vgroups 1 precision 'ms'", show=1)
        tdSql.execute(f"create table {self.DB}.runtime_rows (ts timestamp, v bigint)", show=1)

        # Each range is ordered by v, but the second range is better for ASC Top-N.
        # The 9000 rows span multiple storage blocks; a block-local skip must not
        # prevent the later, better block from entering the bounded queue.
        for start in range(0, 9000, 500):
            values = []
            for i in range(start, min(start + 500, 9000)):
                v = 1000000 + i if i < 4500 else i - 4500
                values.append(f"({self.BASE_TS + i},{v})")
            tdSql.execute(
                f"insert into {self.DB}.runtime_rows values " + " ".join(values),
                show=0,
            )

        tdSql.execute(f"create table {self.DB}.late_rows (ts timestamp, v bigint)", show=1)
        late_values = [0, 1, 2, 3, 4, 5, 6, 7, 100, 8, -1, 9]
        values = " ".join(
            f"({self.BASE_TS + i},{v})" for i, v in enumerate(late_values)
        )
        tdSql.execute(f"insert into {self.DB}.late_rows values {values}", show=1)

        tdSql.execute(f"create table {self.DB}.null_rows (ts timestamp, v bigint)", show=1)
        # The queue is full after the first four rows (LIMIT 3 + one spare), so
        # the first strict rejection happens at v=100, row index 4. The NULL and
        # the better -1 that follows it are therefore inside the short-circuited
        # suffix: NULLS LAST must skip the NULL, while NULLS FIRST must admit it,
        # and -1 must displace the threshold in both cases.
        null_values = [0, 1, 2, 3, 100, None, -1, 101]
        values = " ".join(
            f"({self.BASE_TS + i},{'NULL' if v is None else v})"
            for i, v in enumerate(null_values)
        )
        tdSql.execute(f"insert into {self.DB}.null_rows values {values}", show=1)

        tdSql.execute(
            f"create table {self.DB}.varchar_pk "
            "(ts timestamp, id varchar(80) primary key, v int)",
            show=1,
        )
        prefix = "common_prefix_" + "x" * 60
        values = " ".join(
            f"({self.BASE_TS + i},'{prefix}{i:03d}',{i})" for i in range(12)
        )
        tdSql.execute(f"insert into {self.DB}.varchar_pk values {values}", show=1)

        tdSql.execute(
            f"create table {self.DB}.int_pk (ts timestamp, id int primary key, v int)",
            show=1,
        )
        values = " ".join(
            f"({self.BASE_TS + offset},{key},{value})"
            for offset, key, value in [
                (0, 1, 10), (0, 2, 20), (1000, 1, 30),
                (2000, 2, 40), (3000, 1, 50), (3000, 2, 60),
            ]
        )
        tdSql.execute(f"insert into {self.DB}.int_pk values {values}", show=1)

        tdSql.execute(
            f"create stable {self.DB}.equal_stb "
            "(ts timestamp, id int primary key, v int) tags (gid int)",
            show=1,
        )
        # Six rows per child, with distinct payloads for identical (ts, id) keys.
        # LIMIT 4 (queue capacity 5) over the merged stream rejects a strictly
        # worse key after the first five rows, so the short-circuited suffix
        # contains both an equal complete key and larger keys. The payloads make
        # the tie selection observable.
        for child in range(2):
            tdSql.execute(
                f"create table {self.DB}.equal_{child} using {self.DB}.equal_stb tags ({child})",
                show=1,
            )
            base = 10 if child == 0 else 110
            values = " ".join(
                f"({self.BASE_TS + offset},{key},{base + idx})"
                for idx, (offset, key) in enumerate(
                    [(0, 1), (0, 2), (1000, 1), (2000, 1), (2000, 2), (3000, 1)]
                )
            )
            tdSql.execute(f"insert into {self.DB}.equal_{child} values {values}", show=1)
        tdSql.execute(f"flush database {self.DB}", show=1)

    def _check_runtime_suffix_boundaries(self):
        # Single TIMESTAMP ASC: an ordered block suffix is skipped, then the next
        # block supplies all five better rows. OFFSET exercises the same PQ path.
        self._assert_topn_matches_full("runtime_rows", "cast(v as timestamp)", 5)
        self._assert_topn_matches_full("runtime_rows", "cast(v as timestamp)", 5, 2)

        # Single TIMESTAMP DESC: -v is descending inside each input range, so the
        # suffix check follows DESC comparator semantics rather than physical ASC.
        self._assert_topn_matches_full("runtime_rows", "cast(-v as timestamp) desc", 5)

        # The first suffix validation sees a late inversion. Processing must resume
        # from the rejected row so that the final -1 still displaces an older tuple.
        self._assert_topn_matches_full("late_rows", "cast(v as timestamp)", 3)

        # NULL keys after the strict rejection point exercise the NULL branch of
        # the threshold comparator: NULLS LAST skips the NULL, NULLS FIRST admits
        # it, and the better -1 that follows must displace the threshold in both
        # cases.
        self._assert_topn_matches_full("null_rows", "cast(v as timestamp) nulls last", 3, select="v")
        self._assert_topn_matches_full("null_rows", "cast(v as timestamp) nulls first", 3, select="v")

        # VARCHAR PK takes the conservative row-wise fallback with identical output.
        self._assert_topn_matches_full(
            "varchar_pk", "ts, id", 5, select="ts, id, v"
        )

        # Two children hold identical full (ts, id) keys with distinct payloads
        # and enough rows to trigger a strict rejection. Compare the optimized
        # path with the plain PQ path (expression key disables tsPkSort) so the
        # tie selection is validated against the existing comparator, not just a
        # full-ordering slice.
        self._assert_topn_matches_pq_baseline(
            "equal_stb", "ts, id", "ts, id + 0", 4, select="ts, id, v"
        )

    def test_runtime_suffix_boundaries(self):
        """Runtime suffix checks preserve better rows and all key boundaries

        Exercise single-timestamp ASC and DESC suffix skipping across multiple
        storage blocks, late-inversion fallback, equal full keys, NULL, OFFSET,
        and conservative VARCHAR-primary-key fallback. Compare every Top-N result
        with the corresponding full ordering to catch skipped better rows.

        Catalog:
            - Query

        Since: v3.4.3.0

        Labels: common,ci

        Jira: TD-25971

        History:
            - 2026-11-02 yihaoDeng Add runtime suffix and fallback boundaries.
        """
        self._setup_runtime_data()
        self._check_runtime_suffix_boundaries()

    def test_composite_pk_topn_suffix(self):
        """Composite-PK Top-N preserves results and sort plan boundaries

        Cover duplicate timestamps with exact (ts, id) ordering in both directions,
        LIMIT/OFFSET, mixed-direction and ordinary two-column fallback sorts. Verify
        that the surviving Sort retains its scan direction and that natural primary-ts
        Top-N still uses the existing Sort-elimination plan.

        Catalog:
            - Query

        Since: v3.4.3.0

        Labels: common,ci

        Jira: TD-25971

        History:
            - 2026-10-29 yihaoDeng Add composite-PK Top-N regression coverage.
        """
        self._setup_data()
        self._check_topn_results()
        self._check_plan_boundaries()

    def test_suffix_guard_observability(self):
        """Suffix guard reports plan/threshold skips and variable-width fallback

        Enable query trace logging and assert on per-task log lines that both
        short-circuit paths actually execute: composite (ts, INT PK) Top-N over
        a forward table scan must report reason:plan, while expression-key
        ASC/DESC, NULL-after-rejection and equal-key Top-N must report
        reason:threshold. VARCHAR-primary-key Top-N must take the conservative
        fallback and never enter the short-circuit.

        Catalog:
            - Query

        Since: v3.4.3.0

        Labels: common,ci

        Jira: TD-25971

        History:
            - 2026-11-07 yihaoDeng Add log-level suffix guard assertions.
            - 2026-11-12 yihaoDeng Assert plan-proven and threshold paths (P1-A/P1-B).
            - 2026-11-14 yihaoDeng Assert trace-level suffix guards.
        """
        self._setup_runtime_data()

        # The suffix guard lines are emitted at trace level. The framework
        # default (135) can persist in the dnode-local config and override this
        # class's updatecfgDict, so raise the runtime flag explicitly before
        # asserting on the log stream.
        tdSql.execute("alter dnode 1 'qDebugFlag 143'", show=1)

        # P1-B: expression keys cannot be proven ordered by the plan, so the
        # runtime heap-threshold short-circuit must run.
        self._check_skip_reason(
            f"select ts, v from {self.DB}.runtime_rows order by cast(v as timestamp) limit 5",
            "threshold",
        )
        self._check_skip_reason(
            f"select ts, v from {self.DB}.runtime_rows "
            "order by cast(-v as timestamp) desc limit 5",
            "threshold",
        )

        # P1-A: the composite (ts, INT PK) sort sits directly on a forward
        # table scan, so the plan proves full-key order and the suffix is
        # skipped without re-validation.
        self._check_skip_reason(
            f"select ts, id, v from {self.DB}.int_pk order by ts, id limit 3",
            "plan",
        )

        # P3 NULL gap: the NULLs sit after the strict rejection point, so both
        # NULL orders must be handled by the threshold comparator.
        self._check_skip_reason(
            f"select v from {self.DB}.null_rows "
            "order by cast(v as timestamp) nulls last limit 3",
            "threshold",
        )
        self._check_skip_reason(
            f"select v from {self.DB}.null_rows "
            "order by cast(v as timestamp) nulls first limit 3",
            "threshold",
        )

        # P3 equal-key gap: a strict rejection is followed by complete equal
        # keys; the threshold path must process them (the result test compares
        # the tie choice with the plain PQ baseline).
        self._check_skip_reason(
            f"select ts, id, v from {self.DB}.equal_stb order by ts, id limit 4",
            "threshold",
        )

        # A VARCHAR primary key fails the shared (ts, pk) shape predicate at
        # plan time (fixed-width PK required), so tsPkSort is off and the sort
        # never reaches the ordered-suffix short-circuit. The sort operator
        # traces both flags at open, which binds the following absence check to
        # this query's task.
        var_sql = f"select ts, id, v from {self.DB}.varchar_pk order by ts, id limit 5"
        var_log = self._log_since_query_start(var_sql, self.LOG_SORT_FLAGS)
        var_tasks = self._marker_task_ids(var_log, self.LOG_SORT_FLAGS)
        assert len(var_tasks) == 1, (
            f"VARCHAR PK sort flags not bound to one query task: {var_tasks}"
        )
        task = var_tasks.pop()
        assert not any(
            task in line and self.LOG_SKIP in line for line in var_log.splitlines()
        ), "VARCHAR PK must not enter the ordered-suffix short-circuit"
