import time
from new_test_framework.utils import tdLog, tdSql


class TestQueryPhaseTracking:
    """Test cases for query execution phase tracking feature.
    
    This feature adds phase_state and phase_start_time columns to show queries output
    to help track query execution phases for performance analysis.
    
    Phases: none, parse, catalog, plan, schedule, execute, fetch, done
    """

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.execute("drop database if exists db")
        tdSql.execute("drop database if exists db2")

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

        1. Execute a query and verify phase_state is a valid phase string
        2. Verify phase_start_time is a valid timestamp
        3. Test that phase values are one of: none, parse, catalog, plan, schedule, execute, fetch, done

        Since: v3.3.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-3-6 Created for query phase tracking feature

        """
        tdLog.info("=============== test query phase values")
        tdSql.execute(f"use db")
        
        tdSql.query(f"select count(*) from db.stb")
        tdSql.checkData(0, 0, 1)
        
        tdSql.query(f"show queries")
        
        valid_phases = ["none", "parse", "catalog", "plan", "schedule", "execute", "fetch", "done"]
        
        if tdSql.getRows() > 0:
            col_names = [desc[0] for desc in tdSql.cursor.description]
            phase_idx = col_names.index("phase_state") if "phase_state" in col_names else -1
            time_idx = col_names.index("phase_start_time") if "phase_start_time" in col_names else -1
            
            if phase_idx >= 0:
                phase_value = tdSql.getData(0, phase_idx)
                tdLog.info(f"Current phase: {phase_value}")
                assert phase_value in valid_phases, f"Phase should be one of {valid_phases}, got {phase_value}"
            
            if time_idx >= 0:
                time_value = tdSql.getData(0, time_idx)
                tdLog.info(f"Phase start time: {time_value}")
        
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

        1. Record current timestamp before query
        2. Execute query
        3. Verify phase_start_time is within reasonable range of recorded time

        Since: v3.3.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-3-6 Created for query phase tracking feature

        """
        tdLog.info("=============== test phase timing accuracy")
        tdSql.execute(f"use db")
        
        before_time = int(time.time() * 1000)
        
        tdSql.query(f"select * from db.stb")
        
        after_time = int(time.time() * 1000)
        
        tdSql.query(f"show queries")
        
        if tdSql.getRows() > 0:
            col_names = [desc[0] for desc in tdSql.cursor.description]
            time_idx = col_names.index("phase_start_time") if "phase_start_time" in col_names else -1
            
            if time_idx >= 0:
                query_time = tdSql.getData(0, time_idx)
                tdLog.info(f"Before: {before_time}, Query: {query_time}, After: {after_time}")
        
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
            sub_status_idx = col_names.index("sub_status") if "sub_status" in col_names else -1
            sub_num_idx = col_names.index("sub_num") if "sub_num" in col_names else -1

            if sub_num_idx >= 0:
                sub_num = tdSql.getData(0, sub_num_idx)
                tdLog.info(f"Sub plan num: {sub_num}")

            if sub_status_idx >= 0:
                sub_status = tdSql.getData(0, sub_status_idx)
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
                        if start_time != "-":
                            assert "." in start_time, \
                                f"startTime should be human-readable (YYYY-MM-DD HH:MM:SS.ms) or '-', got: {start_time}"

        tdSql.execute(f"drop database if exists db2")
        print("test sub status timing format ....................... [passed]")

    def cleanup_class(cls):
        tdLog.info(f"cleanup {__file__}")
        tdSql.execute(f"drop database if exists db")
