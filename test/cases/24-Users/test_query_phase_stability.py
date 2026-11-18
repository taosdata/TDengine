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

import threading
import time

import taos

from new_test_framework.utils import tdLog, tdSql


class TestQueryPhaseStability:
    """Regression tests for show queries phase_state / phase_start_time.

    A query fetches a large result set one data block at a time. Those internal
    fetch cycles must not be reported as new execution phases: while the query
    stays in the same phase, phase_start_time must stay unchanged.
    """

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def _get_index(self, col_names, name):
        return col_names.index(name) if name in col_names else -1

    def _sample_query_phases(self, query, match, duration):
        """Run query on a second connection and poll show queries for it.

        Returns (query_start_time, samples) where every sample is
        (sample_time, phase_state, phase_start_time).
        """
        consumer_conn = taos.connect(host=self.host, port=self.port, config=self.cfg_path)
        errors = []

        def consume():
            try:
                cursor = consumer_conn.cursor()
                cursor.execute(query)
                cursor.fetchall()
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        worker = threading.Thread(target=consume, daemon=True)
        worker.start()
        start = time.time()
        samples = []
        try:
            deadline = start + duration
            while time.time() < deadline and worker.is_alive():
                tdSql.query("show queries")
                col_names = [desc[0] for desc in tdSql.cursor.description]
                sql_idx = self._get_index(col_names, "sql")
                phase_idx = self._get_index(col_names, "phase_state")
                time_idx = self._get_index(col_names, "phase_start_time")
                if sql_idx < 0 or phase_idx < 0 or time_idx < 0:
                    break
                for row in range(tdSql.getRows()):
                    if match in str(tdSql.getData(row, sql_idx)):
                        samples.append(
                            (time.time(), tdSql.getData(row, phase_idx), tdSql.getData(row, time_idx))
                        )
                time.sleep(0.1)
        finally:
            worker.join(timeout=30)
            consumer_conn.close()

        if errors:
            raise errors[0]
        return start, samples

    @staticmethod
    def _main_phase(phase):
        return str(phase).split("/")[0]

    def test_phase_start_time_stable_during_fetch(self):
        """ Fun: phase_start_time must not move while fetch phase is unchanged

        Consume a large result set slowly from one connection while polling
        show queries from another one. The query stays in the fetch stage for
        the whole consumption window, so show queries must report the main phase
        'fetch' with one and the same phase_start_time; the internal
        in_progress/returned sub-states must not leak into the output. Before
        the fix the scheduler re-entered fetch/in_progress on every data block
        and phase_start_time advanced on every heartbeat.

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-11-07 Yihao Deng fix defect 7159092216

        """
        db = "db_phase_stable"
        table = f"{db}.phase_fetch"
        tdLog.info("=============== test phase start time stable during fetch")

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"create table {table} (ts timestamp, v int)")
        for batch in range(40):
            values = ",".join(f"(now+{batch * 1000 + i}a, {i})" for i in range(1000))
            tdSql.execute(f"insert into {table} values {values}")

        consumer_conn = taos.connect(host=self.host, port=self.port, config=self.cfg_path)
        stop = threading.Event()
        errors = []

        def consume():
            try:
                cursor = consumer_conn.cursor()
                cursor.execute(f"select * from {table}")
                while not stop.is_set():
                    rows = cursor.fetchmany(100)
                    if not rows:
                        break
                    # Slow consumer: keep the query in the fetch stage long
                    # enough to be sampled by several heartbeats.
                    time.sleep(0.03)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        worker = threading.Thread(target=consume, daemon=True)
        worker.start()

        samples = []
        deadline = time.time() + 15
        try:
            while time.time() < deadline and worker.is_alive():
                tdSql.query("show queries")
                col_names = [desc[0] for desc in tdSql.cursor.description]
                sql_idx = self._get_index(col_names, "sql")
                phase_idx = self._get_index(col_names, "phase_state")
                time_idx = self._get_index(col_names, "phase_start_time")
                if sql_idx < 0 or phase_idx < 0 or time_idx < 0:
                    break
                for row in range(tdSql.getRows()):
                    if "phase_fetch" in str(tdSql.getData(row, sql_idx)):
                        samples.append((tdSql.getData(row, phase_idx), tdSql.getData(row, time_idx)))
                time.sleep(0.1)
        finally:
            stop.set()
            worker.join(timeout=30)
            consumer_conn.close()

        if errors:
            raise errors[0]

        fetch_samples = [s for s in samples if isinstance(s[0], str) and s[0].startswith("fetch")]
        tdLog.info(f"collected {len(samples)} samples, {len(fetch_samples)} in fetch stage: {samples}")
        # The consumer keeps the query in the fetch stage for about 12s
        # (40000 rows, 100 rows + 30ms per iteration), i.e. several heartbeat
        # intervals, so at least 3 observations are expected even on slow hosts.
        assert len(fetch_samples) >= 3, (
            f"expected at least 3 fetch-stage samples to validate phase stability, got "
            f"{len(fetch_samples)} out of {len(samples)} samples; the consumer should keep the "
            f"query in the fetch stage for several heartbeat intervals. samples: {samples}"
        )

        # show queries reports the fetch stage as one main phase. The internal
        # in_progress/returned sub-states must not leak into the output, and the
        # start time must not be re-stamped while the phase is unchanged.
        phases = {phase for phase, _ in fetch_samples}
        assert phases == {"fetch"}, (
            f"fetch stage must be reported as the main phase 'fetch', got phases: {sorted(phases)}; "
            f"samples: {fetch_samples}"
        )
        start_times = {start_time for _, start_time in fetch_samples}
        assert len(start_times) == 1, (
            f"phase_start_time of 'fetch' changed while the phase stayed the same: {start_times}; "
            f"samples: {fetch_samples}"
        )

        tdSql.execute(f"drop database if exists {db}")
        print("test phase start time stable during fetch ....................... [passed]")

    def test_done_phase_after_full_fetch(self):
        """ Fun: fully consumed result reports done with a stable start time

        Fetch every row of a query but keep the result and its request alive.
        show queries must report phase_state = done and must keep
        phase_start_time unchanged across consecutive polls. Before the fix the
        scheduler overwrote done with fetch/returned, so a finished query kept
        reporting the fetch phase.

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-11-07 Yihao Deng fix defect 7159092216

        """
        db = "db_phase_done"
        table = f"{db}.phase_done"
        tdLog.info("=============== test done phase after full fetch")

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"create table {table} (ts timestamp, v int)")
        for batch in range(10):
            values = ",".join(f"(now+{batch * 1000 + i}a, {i})" for i in range(1000))
            tdSql.execute(f"insert into {table} values {values}")

        holder_conn = taos.connect(host=self.host, port=self.port, config=self.cfg_path)
        samples = []
        try:
            cursor = holder_conn.cursor()
            cursor.execute(f"select * from {table}")
            rows = cursor.fetchall()
            assert len(rows) == 10000, f"expected 10000 rows, got {len(rows)}"

            # Keep holder_conn (and therefore the request) alive while sampling.
            deadline = time.time() + 10
            while time.time() < deadline:
                tdSql.query("show queries")
                col_names = [desc[0] for desc in tdSql.cursor.description]
                sql_idx = self._get_index(col_names, "sql")
                phase_idx = self._get_index(col_names, "phase_state")
                time_idx = self._get_index(col_names, "phase_start_time")
                if sql_idx < 0 or phase_idx < 0 or time_idx < 0:
                    break
                for row in range(tdSql.getRows()):
                    if "phase_done" in str(tdSql.getData(row, sql_idx)):
                        samples.append((tdSql.getData(row, phase_idx), tdSql.getData(row, time_idx)))
                done_samples = [s for s in samples if s[0] == "done"]
                if len(done_samples) >= 3:
                    break
                time.sleep(0.2)
        finally:
            holder_conn.close()

        tdLog.info(f"done-phase samples: {samples}")
        done_samples = [s for s in samples if s[0] == "done"]
        assert len(done_samples) >= 2, (
            f"expected the finished query to be reported as done for several polls, got samples: {samples}"
        )
        start_times = {start_time for _, start_time in done_samples}
        assert len(start_times) == 1, (
            f"phase_start_time of done changed while the phase stayed the same: {start_times}; "
            f"samples: {done_samples}"
        )
        first_done = next(i for i, s in enumerate(samples) if s[0] == "done")
        assert all(phase == "done" for phase, _ in samples[first_done:]), (
            f"phase must stay done after the result was fully fetched, got samples: {samples}"
        )

        tdSql.execute(f"drop database if exists {db}")
        print("test done phase after full fetch ....................... [passed]")

    def test_subquery_parent_phase_reaches_execute(self):
        """ Fun: parent job reports execute while a subquery executes

        A scalar subquery runs as a separate scheduler subjob that is not
        registered in jobRef. While the subjob executes, show queries must
        report the parent in the execute stage (execute/waiting_children),
        never the stale schedule/planning or schedule/node_selection phase it
        was left at when the subjob was handed off, and must never move back
        to a schedule phase afterwards.

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-11-07 Yihao Deng fix defect 7159092216

        """
        db = "db_phase_subq"
        tdLog.info("=============== test subquery parent phase reaches execute")

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 4")
        tdSql.execute(f"create table {db}.t1 (ts timestamp, v int)")
        tdSql.execute(f"create table {db}.t2 (ts timestamp, x int)")
        tdSql.execute(f"insert into {db}.t1 values (now, 1), (now+1a, 2)")
        tdSql.execute(f"insert into {db}.t2 values (now, 10), (now+1a, 20)")

        # The scalar subquery is materialised as a subjob that sleeps 8s, so the
        # parent stays in the subjob-waiting state for several heartbeats.
        query = f"select (select sleep(8) from {db}.t2 limit 1) from {db}.t1 limit 1"
        start, samples = self._sample_query_phases(query, "sleep(8)", 9)

        tdLog.info(f"subquery parent samples: {samples}")
        # The subquery runs for 8s, i.e. several heartbeat intervals.
        assert len(samples) >= 2, (
            f"expected at least 2 samples while the subquery executes, got: {samples}"
        )

        execute_samples = [s for s in samples if self._main_phase(s[1]) == "execute"]
        assert execute_samples, (
            f"parent job must report the execute stage while the subquery executes, got samples: {samples}"
        )
        # The subjob starts immediately, so the first execute report must arrive
        # within a couple of heartbeat intervals. Before the fix the parent stayed
        # in schedule/planning for the whole 8s execution.
        first_execute_delay = execute_samples[0][0] - start
        assert first_execute_delay <= 2.5, (
            f"parent job took {first_execute_delay:.2f}s to report execute, expected <= 2.5s; "
            f"samples: {samples}"
        )

        # A schedule sample before the first execute report is tolerated (the
        # initial handoff happens between two atomic stores); it must not appear
        # afterwards.
        first_execute_idx = next(
            i for i, s in enumerate(samples) if self._main_phase(s[1]) == "execute"
        )
        schedule_after_execute = [
            s for s in samples[first_execute_idx:] if str(s[1]).startswith("schedule")
        ]
        assert not schedule_after_execute, (
            f"parent job must not fall back to the schedule stage once it reached execute, "
            f"got: {schedule_after_execute}; all samples: {samples}"
        )

        # The parent is promoted to execute/waiting exactly once while the
        # subjob runs; that start time must be unique. Later execute sub-phase
        # changes (data_query/waiting) are legitimate and are not compared here.
        waiting_times = {s[2] for s in samples if s[1] == "execute/waiting"}
        assert waiting_times, (
            f"expected the parent to report execute/waiting while the subjob runs, got: {samples}"
        )
        assert len(waiting_times) == 1, (
            f"execute/waiting phase_start_time must be stamped once per query, got {waiting_times}; "
            f"samples: {samples}"
        )

        main_order = {"execute": 5, "fetch": 6, "done": 7}
        ranks = [main_order[p] for p in (self._main_phase(s[1]) for s in samples) if p in main_order]
        assert ranks == sorted(ranks), (
            f"parent phase must not regress once it reaches execute, got: "
            f"{[self._main_phase(s[1]) for s in samples]}; samples: {samples}"
        )

        tdSql.execute(f"drop database if exists {db}")
        print("test subquery parent phase reaches execute ....................... [passed]")

    def test_multi_subquery_parent_phase_stable(self):
        """ Fun: parent keeps one execute/waiting start time across subjobs

        Two scalar subqueries run as two sequential subjobs. Launching the
        second subjob must not restart the parent's execute/waiting timer: the
        promotion happens only while the parent is still below execute, so the
        whole subjob window has a single phase_start_time.

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-11-07 Yihao Deng fix defect 7159092216

        """
        db = "db_phase_subq_multi"
        tdLog.info("=============== test multi subquery parent phase stable")

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 4")
        tdSql.execute(f"create table {db}.t1 (ts timestamp, v int)")
        tdSql.execute(f"create table {db}.t2 (ts timestamp, x int)")
        tdSql.execute(f"insert into {db}.t1 values (now, 1), (now+1a, 2)")
        tdSql.execute(f"insert into {db}.t2 values (now, 10), (now+1a, 20)")

        # Two sequential 4s subjobs: the second launch happens around t+4s and
        # must keep the start time stamped at the first launch.
        query = (
            f"select (select sleep(4) from {db}.t2 limit 1), "
            f"(select sleep(4) from {db}.t2 limit 1) from {db}.t1 limit 1"
        )
        start, samples = self._sample_query_phases(query, "sleep(4)", 9)

        tdLog.info(f"multi subquery parent samples: {samples}")
        waiting = [s for s in samples if s[1] == "execute/waiting"]
        assert len(waiting) >= 2, (
            f"expected at least 2 execute/waiting samples across the two subjobs, got: {samples}"
        )

        waiting_times = {s[2] for s in waiting}
        assert len(waiting_times) == 1, (
            f"execute/waiting phase_start_time must not be re-stamped when the second subjob "
            f"starts, got {waiting_times}; samples: {samples}"
        )

        # The two sleep(4) subjobs run sequentially, so the first window ends and
        # the second one starts around t+4s. Require a sample from each window:
        # two late samples could both carry the timestamp stamped by the second
        # launch, and the uniqueness check above would still pass without ever
        # proving that the first window kept its own start time.
        assert any(s[0] - start < 4.0 for s in waiting), (
            f"expected an execute/waiting sample from the first subjob window (< t+4s), got: {samples}"
        )
        assert any(s[0] - start >= 5.0 for s in waiting), (
            f"expected execute/waiting samples from the second subjob window (>= t+5s), got: {samples}"
        )

        first_execute_idx = next(
            i for i, s in enumerate(samples) if self._main_phase(s[1]) == "execute"
        )
        schedule_after_execute = [
            s for s in samples[first_execute_idx:] if str(s[1]).startswith("schedule")
        ]
        assert not schedule_after_execute, (
            f"parent job must not fall back to the schedule stage between subjobs, "
            f"got: {schedule_after_execute}; samples: {samples}"
        )

        tdSql.execute(f"drop database if exists {db}")
        print("test multi subquery parent phase stable ....................... [passed]")

    def test_multilevel_merge_reports_merge_phase(self):
        """ Fun: a plain multi-level plan reports execute/merge_query

        A scan -> remote merge plan without subjobs promotes the job to
        execute/waiting when a scan finishes, and the merge task is dispatched
        once its dependencies are satisfied. show queries must then report
        execute/merge_query while the merge runs; before the fix the waiting
        guard kept the job in execute/waiting for the whole merge and only fetch
        advanced it. sleep() keeps the merge busy for several heartbeat
        intervals so the phase can be sampled.

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-11-16 Yihao Deng fix defect 7159092216

        """
        db = "db_phase_remote_merge"
        tdLog.info("=============== test multilevel merge reports merge phase")

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 4")
        tdSql.execute(f"create stable {db}.stb (ts timestamp, v int) tags (g int)")
        for i in range(8):
            tdSql.execute(f"create table {db}.ct{i} using {db}.stb tags ({i})")
            tdSql.execute(f"insert into {db}.ct{i} values (now, {i}), (now+1s, {i})")

        # Aggregation runs as a scan level in the vgroups plus a merge level on
        # the coordinator; sleep(6) keeps that merge level running for several
        # heartbeats (the aggregate produces one row).
        query = f"select count(*), sleep(6) from {db}.stb"
        _, samples = self._sample_query_phases(query, "sleep(6)", 8)

        tdLog.info(f"multilevel merge samples: {samples}")
        merge_samples = [s for s in samples if s[1] == "execute/merge_query"]
        assert merge_samples, (
            f"multi-level merge must be reported as execute/merge_query while it runs, got samples: {samples}"
        )
        first_merge_idx = next(i for i, s in enumerate(samples) if s[1] == "execute/merge_query")
        stale_waiting = [s for s in samples[first_merge_idx:] if s[1] == "execute/waiting"]
        assert not stale_waiting, (
            f"phase must not fall back to execute/waiting once the merge was dispatched, got: "
            f"{stale_waiting}; all samples: {samples}"
        )

        tdSql.execute(f"drop database if exists {db}")
        print("test multilevel merge reports merge phase ....................... [passed]")

    def cleanup_class(cls):
        tdLog.info(f"cleanup {__file__}")
        tdSql.execute("drop database if exists db_phase_stable")
        tdSql.execute("drop database if exists db_phase_done")
        tdSql.execute("drop database if exists db_phase_subq")
        tdSql.execute("drop database if exists db_phase_subq_multi")
        tdSql.execute("drop database if exists db_phase_remote_merge")
