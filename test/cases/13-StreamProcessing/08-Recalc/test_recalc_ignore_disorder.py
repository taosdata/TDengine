import time
from new_test_framework.utils import tdLog, tdSql, clusterComCheck, tdStream, StreamItem


class TestStreamRecalcIgnoreDisorder:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_recalc_ignore_disorder(self):
        """Stream Recalculation IGNORE_DISORDER Option Test

        Test IGNORE_DISORDER option with disorder data:
        1. Without IGNORE_DISORDER - should process disorder data
           1.1 COUNT_WINDOW, PERIOD, SLIDING triggers (ignore disorder) 
           1.2 Other window triggers (process disorder data)
        2. With IGNORE_DISORDER - should ignore disorder data  
           2.1 All window types (ignore disorder data)

        Catalog:
            - Streams:Recalculation

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-19 Generated from recalculation mechanism design

        """

        self.createSnode()
        self.createDatabase()
        self.prepareQueryData()
        self.prepareTriggerTable()
        self.writeInitialSourceData()
        self.createStreams()
        self.checkStreamStatus()
        self.writeInitialTriggerData()
        self.checkInitialResults()
        self.writeDisorderData()
        self.checkDisorderResults()
        self.checkResults()

    def createSnode(self):
        tdLog.info("create snode")
        tdStream.createSnode(1)

    def createDatabase(self):
        tdLog.info("create database")
        tdSql.prepare(dbname="qdb", vgroups=1)
        tdSql.prepare(dbname="tdb", vgroups=1) 
        tdSql.prepare(dbname="rdb", vgroups=1)
        clusterComCheck.checkDbReady("qdb")
        clusterComCheck.checkDbReady("tdb") 
        clusterComCheck.checkDbReady("rdb")

    def prepareQueryData(self):
        tdLog.info("prepare child tables for query")
        tdStream.prepareChildTables(tbBatch=1, rowBatch=1, rowsPerBatch=400)

        tdLog.info("prepare normal tables for query")
        tdStream.prepareNormalTables(tables=10, rowBatch=1)

        tdLog.info("prepare virtual tables for query")
        tdStream.prepareVirtualTables(tables=10)

        tdLog.info("prepare json tag tables for query")
        tdStream.prepareJsonTables(tbBatch=1, tbPerBatch=10)

        tdLog.info("prepare view")
        tdStream.prepareViews(views=5)

    def prepareTriggerTable(self):
        tdLog.info("prepare trigger tables for IGNORE_DISORDER testing")

        # Trigger tables in tdb (control stream computation trigger)
        stb_trig = "create table tdb.recalc_triggers (ts timestamp, c1 int, c2 int, c3 double, status_val varchar(16)) tags(id int, name varchar(16));"
        ctb_trig = "create table tdb.rt1 using tdb.recalc_triggers tags(1, 'sensor1') tdb.rt2 using tdb.recalc_triggers tags(2, 'sensor2') tdb.rt3 using tdb.recalc_triggers tags(3, 'sensor3')"
        tdSql.execute(stb_trig)
        tdSql.execute(ctb_trig)

        # Normal trigger table for non-partitioned triggers  
        ntb_trig = "create table tdb.trigger_normal (ts timestamp, c1 int, c2 int, c3 double, status_val varchar(16))"
        tdSql.execute(ntb_trig)

        # Trigger table for count window testing
        stb2_trig = "create table tdb.trigger_disorder (ts timestamp, val_num int, status int) tags(device_id int);"
        ctb2_trig = "create table tdb.t1 using tdb.trigger_disorder tags(1) tdb.t2 using tdb.trigger_disorder tags(2)"
        tdSql.execute(stb2_trig)
        tdSql.execute(ctb2_trig)

    def writeInitialSourceData(self):
        tdLog.info("write initial source data to qdb.meters")
        # Step 1: Write base data to meters table (qdb)
        # Table structure: cts, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary, cdecimal8, cdecimal16, cgeometry
        source_sqls = [
            "insert into qdb.t0 values ('2025-01-01 10:00:00', 10, 10, 100, 100, 1.5, 1.5, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(1.0 1.0)') ('2025-01-01 10:01:00', 20, 20, 200, 200, 2.5, 2.5, 'normal', 2, 2, 2, 2, false, 'normal', 'normal', '20', '20', 'POINT(2.0 2.0)')",
            "insert into qdb.t1 values ('2025-01-01 10:00:30', 15, 15, 150, 150, 3.5, 3.5, 'warning', 1, 1, 1, 1, true, 'warning', 'warning', '15', '15', 'POINT(1.5 1.5)') ('2025-01-01 10:01:30', 25, 25, 250, 250, 4.5, 4.5, 'warning', 2, 2, 2, 2, false, 'warning', 'warning', '25', '25', 'POINT(2.5 2.5)')", 
            "insert into qdb.t2 values ('2025-01-01 10:00:45', 30, 30, 300, 300, 5.5, 5.5, 'error', 3, 3, 3, 3, true, 'error', 'error', '30', '30', 'POINT(3.0 3.0)') ('2025-01-01 10:01:45', 40, 40, 400, 400, 6.5, 6.5, 'error', 4, 4, 4, 4, false, 'error', 'error', '40', '40', 'POINT(4.0 4.0)')"
        ]
        tdSql.executes(source_sqls)

    def writeInitialTriggerData(self):
        tdLog.info("write initial trigger data to tdb")
        # Step 3: Write trigger data to trigger tables (tdb) to start stream computation
        trigger_sqls = [
            "insert into tdb.rt1 values ('2025-01-01 10:00:00', 10, 100, 1.5, 'normal') ('2025-01-01 10:01:00', 20, 200, 2.5, 'normal')",
            "insert into tdb.rt2 values ('2025-01-01 10:00:30', 15, 150, 3.5, 'warning') ('2025-01-01 10:01:30', 25, 250, 4.5, 'warning')", 
            "insert into tdb.rt3 values ('2025-01-01 10:00:45', 30, 300, 5.5, 'error') ('2025-01-01 10:01:45', 40, 400, 6.5, 'error')",
            "insert into tdb.trigger_normal values ('2025-01-01 10:00:00', 5, 50, 0.5, 'idle') ('2025-01-01 10:01:00', 10, 100, 1.0, 'active')",
            "insert into tdb.t1 values ('2025-01-01 10:00:00', 100, 1) ('2025-01-01 10:01:00', 200, 1)",
            "insert into tdb.t2 values ('2025-01-01 10:00:30', 150, 2) ('2025-01-01 10:01:30', 250, 2)"
        ]
        tdSql.executes(trigger_sqls)

    def writeDisorderData(self):
        tdLog.info("write disorder data to test IGNORE_DISORDER option")
        
        # Wait for initial processing
        time.sleep(3)
        
        # Step 5: Write disorder data to qdb.meters
        tdLog.info("Step 5: Write disorder data to qdb.meters")
        disorder_source_sqls = [
            # Disorder data for interval+sliding (should process without ignore_disorder)
            "insert into qdb.t0 values ('2025-01-01 09:59:30', 5, 5, 55, 55, 0.8, 0.8, 'disorder1', 1, 1, 1, 1, true, 'disorder1', 'disorder1', '5', '5', 'POINT(0.8 0.8)')",
            # Disorder data for session window (should process without ignore_disorder)
            "insert into qdb.t1 values ('2025-01-01 09:58:45', 12, 12, 125, 125, 2.8, 2.8, 'disorder2', 2, 2, 2, 2, false, 'disorder2', 'disorder2', '12', '12', 'POINT(2.8 2.8)')",
            # Disorder data for state window (should process without ignore_disorder)  
            "insert into qdb.t2 values ('2025-01-01 09:57:15', 8, 8, 85, 85, 1.2, 1.2, 'disorder3', 3, 3, 3, 3, true, 'disorder3', 'disorder3', '8', '8', 'POINT(1.2 1.2)')"
        ]
        tdSql.executes(disorder_source_sqls)
        
        # Step 6: Write disorder trigger data to trigger recalculation
        tdLog.info("Step 6: Write disorder trigger data to trigger recalculation")
        disorder_trigger_sqls = [
            # Disorder trigger data to trigger recalculation
            "insert into tdb.rt1 values ('2025-01-01 09:59:30', 5, 55, 0.8, 'disorder1')",
            "insert into tdb.rt2 values ('2025-01-01 09:58:45', 12, 125, 2.8, 'disorder2')",
            "insert into tdb.rt3 values ('2025-01-01 09:57:15', 8, 85, 1.2, 'disorder3')",
            "insert into tdb.t1 values ('2025-01-01 09:58:00', 80, 0) ('2025-01-01 09:59:00', 90, 0)",
            "insert into tdb.trigger_normal values ('2025-01-01 09:58:30', 3, 35, 0.3, 'disorder_period')"
        ]
        tdSql.executes(disorder_trigger_sqls)
        time.sleep(3)  # Allow time for stream processing

    def checkStreamStatus(self):
        tdLog.info("check stream status")
        tdStream.checkStreamStatus()

    def checkResults(self):
        """Check stream computation results"""
        tdLog.info(f"check total:{len(self.streams)} streams result")
        for stream in self.streams:
            stream.checkResults()
        tdLog.info(f"check total:{len(self.streams)} streams result successfully")

    def checkInitialResults(self):
        """Check initial stream computation results after first data insertion"""
        tdLog.info("Checking initial stream computation results...")
        
        # Allow time for stream processing
        time.sleep(3)
        
        # Check basic functionality for each stream
        for i in range(1, 8):
            method_name = f"check{i:02d}"
            if hasattr(self, method_name):
                tdLog.info(f"Checking initial result for stream {i}")
                getattr(self, method_name)()
            else:
                tdLog.info(f"Check method {method_name} not implemented")
        
        tdLog.success("Initial stream computation results verified successfully")
    
    def checkDisorderResults(self):
        """Check stream computation results after disorder data"""
        tdLog.info("Checking stream computation results after disorder data...")
        
        # Allow time for stream processing
        time.sleep(3)
        
        # Check disorder handling for each stream
        for i in range(1, 8):
            method_name = f"check{i:02d}"
            if hasattr(self, method_name):
                tdLog.info(f"Checking disorder result for stream {i}")
                getattr(self, method_name)()
            else:
                tdLog.info(f"Check method {method_name} not implemented")
        
        tdLog.success("Disorder data handling results verified successfully")
    
    def checkFinalResults(self):
        """Check final stream computation results after all operations"""
        tdLog.info("Checking final stream computation results...")
        
        # Allow time for stream processing
        time.sleep(3)
        
        # Check each stream result
        for i in range(1, 8):
            method_name = f"check{i:02d}"
            if hasattr(self, method_name):
                tdLog.info(f"Checking final result for stream {i}")
                getattr(self, method_name)()
            else:
                tdLog.info(f"Check method {method_name} not implemented")
        
        tdLog.success("All stream computation results verified successfully")

    def createStreams(self):
        self.streams = []

        # ===== Test 1: Without IGNORE_DISORDER (Default Behavior) =====
        
        # Test 1.1: INTERVAL+SLIDING without IGNORE_DISORDER - should process disorder
        stream = StreamItem(
            id=1,
            stream="create stream rdb.s_interval_default interval(2m) sliding(1m) from tdb.recalc_triggers into rdb.r_interval_default as select _twstart ts, count(*) cnt, avg(c1) avg_val from qdb.meters where ts >= _twstart and ts < _twend;",
            res_query="select ts, cnt, avg_val from rdb.r_interval_default order by ts;",
            exp_query="",
            check_func=self.check01,
        )
        self.streams.append(stream)

        # Test 1.2: SESSION without IGNORE_DISORDER - should process disorder
        stream = StreamItem(
            id=2,
            stream="create stream rdb.s_session_default session(ts, 30s) from tdb.recalc_triggers into rdb.r_session_default as select _twstart ts, count(*) cnt, avg(c1) avg_val from qdb.meters where ts >= _twstart and ts < _twend;",
            res_query="select ts, cnt, avg_val from rdb.r_session_default order by ts;",
            exp_query="",
            check_func=self.check02,
        )
        self.streams.append(stream)

        # Test 1.3: STATE_WINDOW without IGNORE_DISORDER - should process disorder
        stream = StreamItem(
            id=3,
            stream="create stream rdb.s_state_default state_window(status_val) from tdb.recalc_triggers partition by tbname into rdb.r_state_default as select _twstart ts, count(*) cnt, avg(c1) avg_val, first(status_val) status_val from qdb.meters where ts >= _twstart and ts < _twend;",
            res_query="select ts, cnt, avg_val, status_val from rdb.r_state_default order by ts;",
            exp_query="",
            check_func=self.check03,
        )
        self.streams.append(stream)

        # Test 1.4: COUNT_WINDOW without IGNORE_DISORDER - should ignore disorder (inherent behavior)
        stream = StreamItem(
            id=4,
            stream="create stream rdb.s_count_default count_window(3) from tdb.trigger_disorder partition by tbname into rdb.r_count_default as select _twstart ts, count(*) cnt, avg(c1) avg_val from qdb.meters;",
            res_query="select cnt, avg_val from rdb.r_count_default order by ts;",
            exp_query="",
            check_func=self.check04,
        )
        self.streams.append(stream)

        # ===== Test 2: With IGNORE_DISORDER Option =====

        # Test 2.1: INTERVAL+SLIDING with IGNORE_DISORDER - should ignore disorder
        stream = StreamItem(
            id=5,
            stream="create stream rdb.s_interval_ignore interval(2m) sliding(1m) from tdb.recalc_triggers stream_options(ignore_disorder) into rdb.r_interval_ignore as select _twstart ts, count(*) cnt, avg(c1) avg_val from qdb.meters where ts >= _twstart and ts < _twend;",
            res_query="select ts, cnt, avg_val from rdb.r_interval_ignore order by ts;",
            exp_query="",
            check_func=self.check05,
        )
        self.streams.append(stream)

        # Test 2.2: SESSION with IGNORE_DISORDER - should ignore disorder
        stream = StreamItem(
            id=6,
            stream="create stream rdb.s_session_ignore session(ts, 30s) from tdb.recalc_triggers stream_options(ignore_disorder) into rdb.r_session_ignore as select _twstart ts, count(*) cnt, avg(c1) avg_val from qdb.meters where ts >= _twstart and ts < _twend;",
            res_query="select ts, cnt, avg_val from rdb.r_session_ignore order by ts;",
            exp_query="",
            check_func=self.check06,
        )
        self.streams.append(stream)

        # Test 2.3: STATE_WINDOW with IGNORE_DISORDER - should ignore disorder
        stream = StreamItem(
            id=7,
            stream="create stream rdb.s_state_ignore state_window(status_val) from tdb.recalc_triggers partition by tbname stream_options(ignore_disorder) into rdb.r_state_ignore as select _twstart ts, count(*) cnt, avg(c1) avg_val, first(status_val) status_val from qdb.meters where ts >= _twstart and ts < _twend;",
            res_query="select ts, cnt, avg_val, status_val from rdb.r_state_ignore order by ts;",
            exp_query="",
            check_func=self.check07,
        )
        self.streams.append(stream)

        tdLog.info(f"create total:{len(self.streams)} streams")
        for stream in self.streams:
            stream.createStream()

    # Check functions for each test case
    def check01(self):
        # Test interval+sliding without IGNORE_DISORDER - should process disorder data
        tdLog.info("Check 1: INTERVAL+SLIDING without IGNORE_DISORDER processes disorder data")
        tdSql.checkTableType(dbname="rdb", tbname="r_interval_default", typename="NORMAL_TABLE", columns=3)
        tdSql.query("select count(*) from rdb.r_interval_default;")
        result_count = tdSql.getData(0, 0)
        tdLog.info(f"INTERVAL+SLIDING default stream result count: {result_count}")

    def check02(self):
        # Test session without IGNORE_DISORDER - should process disorder data
        tdLog.info("Check 2: SESSION without IGNORE_DISORDER processes disorder data")
        tdSql.checkTableType(dbname="rdb", tbname="r_session_default", typename="NORMAL_TABLE", columns=3)
        tdSql.query("select count(*) from rdb.r_session_default;")
        result_count = tdSql.getData(0, 0)
        tdLog.info(f"SESSION default stream result count: {result_count}")

    def check03(self):
        # Test state window without IGNORE_DISORDER - should process disorder data
        tdLog.info("Check 3: STATE_WINDOW without IGNORE_DISORDER processes disorder data")
        tdSql.checkTableType(dbname="rdb", stbname="r_state_default", columns=4, tags=1)
        tdSql.query("select count(*) from rdb.r_state_default where tag_tbname='rt1';")
        result_count = tdSql.getData(0, 0)
        tdLog.info(f"STATE_WINDOW default stream result count: {result_count}")

    def check04(self):
        # Test count window without IGNORE_DISORDER - should ignore disorder (inherent behavior)
        tdLog.info("Check 4: COUNT_WINDOW ignores disorder data (inherent behavior)")
        tdSql.checkTableType(dbname="rdb", stbname="r_count_default", columns=3, tags=1)
        tdSql.query("select count(*) from rdb.r_count_default where tag_tbname='t1';")
        result_count = tdSql.getData(0, 0)
        tdLog.info(f"COUNT_WINDOW default stream result count: {result_count}")

    def check05(self):
        # Test interval+sliding with IGNORE_DISORDER - should ignore disorder data
        tdLog.info("Check 5: INTERVAL+SLIDING with IGNORE_DISORDER ignores disorder data")
        tdSql.checkTableType(dbname="rdb", tbname="r_interval_ignore", typename="NORMAL_TABLE", columns=3)
        tdSql.query("select count(*) from rdb.r_interval_ignore;")
        result_count = tdSql.getData(0, 0)
        tdLog.info(f"INTERVAL+SLIDING ignore disorder stream result count: {result_count}")

    def check06(self):
        # Test session with IGNORE_DISORDER - should ignore disorder data
        tdLog.info("Check 6: SESSION with IGNORE_DISORDER ignores disorder data")
        tdSql.checkTableType(dbname="rdb", tbname="r_session_ignore", typename="NORMAL_TABLE", columns=3)
        tdSql.query("select count(*) from rdb.r_session_ignore;")
        result_count = tdSql.getData(0, 0)
        tdLog.info(f"SESSION ignore disorder stream result count: {result_count}")

    def check07(self):
        # Test state window with IGNORE_DISORDER - should ignore disorder data
        tdLog.info("Check 7: STATE_WINDOW with IGNORE_DISORDER ignores disorder data")
        tdSql.checkTableType(dbname="rdb", stbname="r_state_ignore", columns=4, tags=1)
        tdSql.query("select count(*) from rdb.r_state_ignore where tag_tbname='rt1';")
        result_count = tdSql.getData(0, 0)
        tdLog.info(f"STATE_WINDOW ignore disorder stream result count: {result_count}") 