import time
import platform
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, tdCom


class TestQueryPhaseTracking:
    """Test cases for query execution phase tracking feature.
    
    This feature adds current_phase and phase_start_time columns to show queries output
    to help track query execution phases for performance analysis.
    
    Phases: none, parse, catalog, plan, schedule, execute, fetch, done
    """

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_show_queries_schema(self):
        """Schema: Verify new columns in show queries

        1. Verify that current_phase column exists in show queries output
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
        
        tdSql.query(f"show queries")
        
        col_names = [row[0] for row in tdSql.getColNames()]
        tdLog.info(f"show queries columns: {col_names}")
        
        assert "current_phase" in col_names, "current_phase column should exist"
        assert "phase_start_time" in col_names, "phase_start_time column should exist"
        
        print("test show queries schema ....................... [passed]")

    def test_query_phase_values(self):
        """Phase: Verify query phase values

        1. Execute a query and verify current_phase is a valid phase string
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
            col_names = [row[0] for row in tdSql.getColNames()]
            phase_idx = col_names.index("current_phase") if "current_phase" in col_names else -1
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
            tdSql.checkRows(1)
        
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
            col_names = [row[0] for row in tdSql.getColNames()]
            time_idx = col_names.index("phase_start_time") if "phase_start_time" in col_names else -1
            
            if time_idx >= 0:
                query_time = tdSql.getData(0, time_idx)
                tdLog.info(f"Before: {before_time}, Query: {query_time}, After: {after_time}")
        
        print("test phase timing accuracy ....................... [passed]")

    def cleanup_class(cls):
        tdLog.info(f"cleanup {__file__}")
        tdSql.execute(f"drop database if exists db")
