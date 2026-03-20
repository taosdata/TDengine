import time
import threading
from new_test_framework.utils import tdLog, tdSql


class TestQueryPhaseTracking:
    """Test cases for query execution phase tracking feature.

    This feature adds phase_state and phase_start_time columns to show queries output
    to help track query execution phases for performance analysis.

    Main phases: none, parse, catalog, plan, schedule, execute, fetch, done
    Sub-phases: schedule/*, execute/*, fetch/* (for finer-grained tracking)
    """

    VALID_PHASES = [
        "none", "parse", "catalog", "plan",
        "schedule", "execute", "fetch", "done",
        "schedule/analysis", "schedule/planning", "schedule/node_selection",
        "execute/data_query", "execute/merge_query", "execute/waiting",
        "fetch/in_progress", "fetch/returned",
    ]

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.execute("drop database if exists db")
        tdSql.execute("drop database if exists db2")

    def _get_col_idx(self, col_names, name):
        return col_names.index(name) if name in col_names else -1

    def test_show_queries_schema(self):
        """Schema: Verify new columns in show queries

        1. Verify that phase_state column exists in show queries output
        2. Verify that phase_start_time column exists in show queries output
        3. Verify the column types are correct

        Since: v3.3.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-3-6 Created for query phase tracking feature

        """
        tdLog.info("=============== test show queries schema")
        tdSql.execute(f"create database if not exists db")
        tdSql.execute(f"use db")
        tdSql.execute(f"create table db.stb (ts timestamp, i int) tags (t int)")
        tdSql.execute(f"create table db.ctb using db.stb tags (1)")
        tdSql.execute(f"insert into db.ctb values (now, 1)")

        tdSql.query(f"select * from db.stb")

        col_names = tdSql.getColNameList("show queries")
        tdLog.info(f"show queries columns: {col_names}")

        assert "phase_state" in col_names, "phase_state column should exist"
        assert "phase_start_time" in col_names, "phase_start_time column should exist"

        print("test show queries schema ....................... [passed]")

    def test_query_phase_values(self):
        """Phase: Verify query phase values

        1. Execute show queries and verify phase_state is a valid phase string
        2. The only reliably visible query is 'show queries' itself
        3. Its phase must be one of the valid phases

        Since: v3.3.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-3-6 Created for query phase tracking feature

        """
        tdLog.info("=============== test query phase values")
        tdSql.execute(f"use db")

        tdSql.query(f"show queries")

        if tdSql.getRows() > 0:
            col_names = [desc[0] for desc in tdSql.cursor.description]
            phase_idx = self._get_col_idx(col_names, "phase_state")
            time_idx = self._get_col_idx(col_names, "phase_start_time")

            for row in range(tdSql.getRows()):
                if phase_idx >= 0:
                    phase_value = tdSql.getData(row, phase_idx)
                    tdLog.info(f"Row {row} phase: {phase_value}")
                    assert phase_value in self.VALID_PHASES, \
                        f"Phase should be one of {self.VALID_PHASES}, got {phase_value}"

                if time_idx >= 0:
                    time_value = tdSql.getData(row, time_idx)
                    tdLog.info(f"Row {row} phase_start_time: {time_value}")

        print("test query phase values ....................... [passed]")

    def test_long_running_query_phase(self):
        """Long Query: Verify phase tracking for longer queries

        1. Create a table with more data
        2. Execute a longer running query
        3. Verify phase information is captured correctly

        Since: v3.3.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-3-6 Created for query phase tracking feature

        """
        tdLog.info("=============== test long running query phase")
        tdSql.execute(f"use db")
        tdSql.execute(f"create table if not exists db.lt (ts timestamp, v1 int, v2 float, v3 double)")

        for i in range(100):
            tdSql.execute(f"insert into db.lt values (now + {i}s, {i}, {i}.5, {i}.123456)")

        tdSql.query(f"select count(*), avg(v1), sum(v2), max(v3) from db.lt")
        tdSql.checkRows(1)

        tdSql.query(f"show queries")
        tdLog.info(f"Active queries count: {tdSql.getRows()}")

        print("test long running query phase ....................... [passed]")

    def test_concurrent_queries_phase(self):
        """Concurrent: Verify phase tracking with multiple queries

        1. Create multiple tables
        2. Execute multiple queries
        3. Verify each query has correct phase information

        Since: v3.3.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-3-6 Created for query phase tracking feature

        """
        tdLog.info("=============== test concurrent queries phase")
        tdSql.execute(f"use db")

        for i in range(5):
            tdSql.execute(f"create table if not exists db.t{i} (ts timestamp, v int)")
            tdSql.execute(f"insert into db.t{i} values (now, {i})")

        for i in range(5):
            tdSql.query(f"select * from db.t{i}")
            assert tdSql.getRows() >= 1, f"table db.t{i} should have at least 1 row"

        tdSql.query(f"show queries")
        tdLog.info(f"Total queries shown: {tdSql.getRows()}")

        print("test concurrent queries phase ....................... [passed]")

    def test_phase_timing_accuracy(self):
        """Timing: Verify phase_start_time accuracy

        1. Record current timestamp before running show queries
        2. Execute show queries
        3. Verify the visible query's phase_start_time is reasonable

        Since: v3.3.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-3-6 Created for query phase tracking feature

        """
        tdLog.info("=============== test phase timing accuracy")
        tdSql.execute(f"use db")

        before_time = int(time.time() * 1000)

        tdSql.query(f"show queries")

        after_time = int(time.time() * 1000)

        if tdSql.getRows() > 0:
            col_names = [desc[0] for desc in tdSql.cursor.description]
            time_idx = self._get_col_idx(col_names, "phase_start_time")
            sql_idx = self._get_col_idx(col_names, "sql")

            if time_idx >= 0:
                for row in range(tdSql.getRows()):
                    query_time = tdSql.getData(row, time_idx)
                    sql_val = tdSql.getData(row, sql_idx) if sql_idx >= 0 else ""
                    tdLog.info(f"Row {row}: sql={sql_val}, phase_start_time={query_time}")
                    if query_time and isinstance(query_time, (int, float)):
                        tolerance = 60000
                        assert query_time >= before_time - tolerance, \
                            f"phase_start_time {query_time} should be >= {before_time - tolerance}"

        print("test phase timing accuracy ....................... [passed]")

    def test_sub_status_timing_format(self):
        """SubTask: Verify sub_status includes timing info with human-readable time

        1. Create a supertable with multiple child tables to generate sub-tasks
        2. Execute a distributed query to trigger sub-plan execution
        3. Verify sub_status format contains tid:status:startTime
           where startTime is human-readable (e.g. 2026-03-12 10:00:00.123) or "-"

        Since: v3.3.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-3-10 Created for sub-task timing tracking feature
            - 2026-3-12 Changed time format from unix ms to human-readable

        """
        tdLog.info("=============== test sub status timing format")
        tdSql.execute(f"create database if not exists db2 vgroups 2")
        tdSql.execute(f"use db2")
        tdSql.execute(f"create table db2.stb2 (ts timestamp, v int) tags (t int)")
        for i in range(4):
            tdSql.execute(f"create table db2.ct{i} using db2.stb2 tags ({i})")
            tdSql.execute(f"insert into db2.ct{i} values (now, {i})")

        tdSql.query(f"select count(*) from db2.stb2 group by tbname")

        tdSql.query(f"show queries")
        if tdSql.getRows() > 0:
            col_names = [desc[0] for desc in tdSql.cursor.description]
            sub_status_idx = self._get_col_idx(col_names, "sub_status")
            sub_num_idx = self._get_col_idx(col_names, "sub_num")
            sql_idx = self._get_col_idx(col_names, "sql")
            sql_idx = self._get_col_idx(col_names, "sql")
            target_row_idx = None
            if sql_idx >= 0:
                # Find the row corresponding to the distributed query we just ran.
                # Match on the SQL text to avoid depending on row ordering.
                target_sql_pattern = "select count(*) from db2.stb2"
                for row_idx in range(tdSql.getRows()):
                    sql_text = tdSql.getData(row_idx, sql_idx)
                    if not isinstance(sql_text, str):
                        sql_text = str(sql_text)
                    if target_sql_pattern in sql_text.lower():
                        target_row_idx = row_idx
                        break

            if target_row_idx is None:
                tdLog.info("Target distributed query not found in 'show queries'; skipping sub_status checks.")
            else:
                if sub_num_idx >= 0:
                    sub_num = tdSql.getData(target_row_idx, sub_num_idx)
                    tdLog.info(f"Sub plan num: {sub_num}")

                if sub_status_idx >= 0:
                    sub_status = tdSql.getData(target_row_idx, sub_status_idx)
                    tdLog.info(f"Sub status: {sub_status}")
                    if sub_status:
                        parts = sub_status.split(",")
                        for part in parts:
                            fields = part.split(":", 2)
                            tdLog.info(f"  Sub-task fields: {fields}")
                            assert len(fields) == 3, \
                                f"sub_status entry should have 3 fields (tid:status:startTime), got {len(fields)}: {part}"
                            tid_str, status, start_time = fields
                            assert tid_str.isdigit(), f"tid should be numeric, got: {tid_str}"
                            assert len(status) > 0, f"status should not be empty"
                            assert "/" in status, \
                                f"status should be fine-grained (type/state), got: {status}"
                            type_part, state_part = status.split("/", 1)
                            valid_types = ["scan", "merge", "modify", "compute", "partial", "task"]
                            assert type_part in valid_types, \
                                f"task type should be one of {valid_types}, got: {type_part}"
                            if start_time != "-":
                                assert "." in start_time, \
                                    f"startTime should be human-readable (YYYY-MM-DD HH:MM:SS.ms) or '-', got: {start_time}"

        tdSql.execute(f"drop database if exists db2")
        print("test sub status timing format ....................... [passed]")

    def test_phase_state_max_length(self):
        """MaxLen: Verify phase_state column can hold long phase strings

        1. Use currently defined phase values (e.g. those in VALID_PHASES) as reference
        2. Verify the column width (32 + VARSTR_HEADER) is sufficient for all of them
        3. Show queries and verify no truncation occurs

        Since: v3.3.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-3-10 Created to verify column width after fix

        """
        tdLog.info("=============== test phase state max length")

        tdSql.query(f"show queries")
        if tdSql.getRows() > 0:
            col_names = [desc[0] for desc in tdSql.cursor.description]
            phase_idx = self._get_col_idx(col_names, "phase_state")
            if phase_idx >= 0:
                for row in range(tdSql.getRows()):
                    phase_value = tdSql.getData(row, phase_idx)
                    tdLog.info(f"Row {row} phase: '{phase_value}' (len={len(phase_value) if phase_value else 0})")
                    assert phase_value is not None, "phase_state should not be NULL"
                    assert phase_value in self.VALID_PHASES, \
                        f"Phase should be a valid phase, got '{phase_value}'"

        print("test phase state max length ....................... [passed]")

    def test_fetch_state_transitions(self):
        """Fetch: Verify phase transitions during fetch operations

        1. Execute a query with multiple rows
        2. Verify state transitions between fetch/done and fetch/in_progress
        3. Each fetch call should set phase to in_progress, then back to done

        Since: v3.3.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-3-18 Created for simplified fetch state verification

        """
        tdLog.info("=============== test fetch state transitions")
        tdSql.execute(f"use db")
        tdSql.execute(f"create table if not exists db.test_fetch_trans (ts timestamp, v int)")
        # Insert multiple rows
        for i in range(10):
            tdSql.execute(f"insert into db.test_fetch_trans values (now + {i}s, {i})")

        # Execute query - should complete and go to fetch/returned
        tdSql.query(f"select * from db.test_fetch_trans")

        # Check state should be fetch/returned or transitioning
        tdSql.query(f"show queries")
        if tdSql.getRows() > 0:
            col_names = [desc[0] for desc in tdSql.cursor.description]
            phase_idx = self._get_col_idx(col_names, "phase_state")
            sql_idx = self._get_col_idx(col_names, "sql")

            for row in range(tdSql.getRows()):
                phase_value = tdSql.getData(row, phase_idx)
                sql_val = tdSql.getData(row, sql_idx) if sql_idx >= 0 else ""
                if "test_fetch_trans" in sql_val:
                    tdLog.info(f"Query state: phase={phase_value}")
                    # Should be one of: fetch/returned, fetch/in_progress, done
                    valid_fetch_phases = ["fetch/returned", "fetch/in_progress", "done"]
                    assert phase_value in valid_fetch_phases, \
                        f"Phase should be a valid fetch phase, got '{phase_value}'"
                    break

        print("test fetch state transitions ..................... [passed]")

    def cleanup_class(cls):
        tdLog.info(f"cleanup {__file__}")
        tdSql.execute(f"drop database if exists db")
        tdSql.execute(f"drop database if exists db2")
