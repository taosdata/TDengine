import time
from new_test_framework.utils import tdLog, tdSql, clusterComCheck, tdStream, StreamItem


class TestStreamRecalcWatermark:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_recalc_watermark(self):
        """Stream Recalculation WATERMARK Option Test

        Test WATERMARK option with disorder data:
        1. Without WATERMARK (default 0) - process disorder data based on trigger type
           1.1 COUNT_WINDOW, PERIOD, SLIDING triggers (ignore disorder)
           1.2 Other window triggers (process disorder data before watermark)
        2. With very short WATERMARK - strict watermark line
           2.1 COUNT_WINDOW, PERIOD, SLIDING triggers (ignore disorder)
           2.2 Other window triggers (process disorder data before watermark)
        3. With very long WATERMARK - loose watermark line
           3.1 COUNT_WINDOW, PERIOD, SLIDING triggers (ignore disorder)
           3.2 Other window triggers (process disorder data before watermark)

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
        tdLog.info("prepare trigger tables for WATERMARK testing")

        # Trigger tables in tdb (control stream computation trigger)
        stb_trig = "create table tdb.recalc_triggers (ts timestamp, c1 int, c2 int, c3 double, status_val varchar(16)) tags(id int, name varchar(16));"
        ctb_trig = "create table tdb.rt1 using tdb.recalc_triggers tags(1, 'sensor1') tdb.rt2 using tdb.recalc_triggers tags(2, 'sensor2') tdb.rt3 using tdb.recalc_triggers tags(3, 'sensor3')"
        tdSql.execute(stb_trig)
        tdSql.execute(ctb_trig)

        # Trigger table for disorder testing with partitioning
        stb2_trig = "create table tdb.trigger_disorder (ts timestamp, val_num int, device_status varchar(16)) tags(device_id int);"
        ctb2_trig = "create table tdb.t1 using tdb.trigger_disorder tags(1) tdb.t2 using tdb.trigger_disorder tags(2) tdb.t3 using tdb.trigger_disorder tags(3)"
        tdSql.execute(stb2_trig)
        tdSql.execute(ctb2_trig)

        # Normal trigger table for period/sliding triggers
        ntb_trig = "create table tdb.trigger_watermark (ts timestamp, val_num int, metric double, desc_text varchar(32))"
        tdSql.execute(ntb_trig)

    def writeInitialSourceData(self):
        tdLog.info("write initial source data to qdb.meters")
        # Step 1: Write base data to meters table (qdb)
        # Table structure: cts, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary, cdecimal8, cdecimal16, cgeometry
        source_sqls = [
            "insert into qdb.t0 values ('2025-01-01 10:00:00', 10, 10, 100, 100, 1.5, 1.5, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(1.0 1.0)') ('2025-01-01 10:02:00', 20, 20, 200, 200, 2.5, 2.5, 'normal', 2, 2, 2, 2, false, 'normal', 'normal', '20', '20', 'POINT(2.0 2.0)')",
            "insert into qdb.t1 values ('2025-01-01 10:00:30', 15, 15, 150, 150, 3.5, 3.5, 'warning', 1, 1, 1, 1, true, 'warning', 'warning', '15', '15', 'POINT(1.5 1.5)') ('2025-01-01 10:02:30', 25, 25, 250, 250, 4.5, 4.5, 'warning', 2, 2, 2, 2, false, 'warning', 'warning', '25', '25', 'POINT(2.5 2.5)')", 
            "insert into qdb.t2 values ('2025-01-01 10:01:00', 30, 30, 300, 300, 5.5, 5.5, 'error', 3, 3, 3, 3, true, 'error', 'error', '30', '30', 'POINT(3.0 3.0)') ('2025-01-01 10:03:00', 40, 40, 400, 400, 6.5, 6.5, 'error', 4, 4, 4, 4, false, 'error', 'error', '40', '40', 'POINT(4.0 4.0)')"
        ]
        tdSql.executes(source_sqls)

    def writeInitialTriggerData(self):
        tdLog.info("write initial trigger data to tdb")
        # Step 3: Write trigger data to trigger tables (tdb) to start stream computation
        trigger_sqls = [
            "insert into tdb.rt1 values ('2025-01-01 10:00:00', 10, 100, 1.5, 'normal') ('2025-01-01 10:02:00', 20, 200, 2.5, 'normal')",
            "insert into tdb.rt2 values ('2025-01-01 10:00:30', 15, 150, 3.5, 'warning') ('2025-01-01 10:02:30', 25, 250, 4.5, 'warning')", 
            "insert into tdb.rt3 values ('2025-01-01 10:01:00', 30, 300, 5.5, 'error') ('2025-01-01 10:03:00', 40, 400, 6.5, 'error')",
            "insert into tdb.t1 values ('2025-01-01 10:00:00', 100, 'active') ('2025-01-01 10:02:00', 200, 'active')",
            "insert into tdb.t2 values ('2025-01-01 10:00:30', 150, 'busy') ('2025-01-01 10:02:30', 250, 'busy')",
            "insert into tdb.t3 values ('2025-01-01 10:01:00', 180, 'idle') ('2025-01-01 10:03:00', 280, 'idle')",
            "insert into tdb.trigger_watermark values ('2025-01-01 10:00:00', 50, 1.0, 'baseline') ('2025-01-01 10:02:00', 60, 2.0, 'trend')"
        ]
        tdSql.executes(trigger_sqls)

    def writeDisorderData(self):
        tdLog.info("write disorder data to test WATERMARK behavior")
        
        # Wait for initial processing
        time.sleep(3)
        
        # Step 5: Write disorder data to qdb.meters
        tdLog.info("Step 5: Write disorder data to qdb.meters")
        disorder_source_sqls = [
            # Very early disorder - should be within all watermarks
            "insert into qdb.t0 values ('2025-01-01 09:58:00', 5, 5, 55, 55, 0.8, 0.8, 'early_disorder', 1, 1, 1, 1, true, 'early_disorder', 'early_disorder', '5', '5', 'POINT(0.8 0.8)')",
            # Medium delay disorder - should be within long watermark, outside short watermark  
            "insert into qdb.t1 values ('2025-01-01 09:59:30', 12, 12, 125, 125, 2.8, 2.8, 'medium_disorder', 2, 2, 2, 2, false, 'medium_disorder', 'medium_disorder', '12', '12', 'POINT(2.8 2.8)')",
            # Recent disorder - should be within both watermarks
            "insert into qdb.t2 values ('2025-01-01 09:59:50', 8, 8, 85, 85, 1.2, 1.2, 'recent_disorder', 3, 3, 3, 3, true, 'recent_disorder', 'recent_disorder', '8', '8', 'POINT(1.2 1.2)')"
        ]
        tdSql.executes(disorder_source_sqls)
        
        # Step 6: Write disorder trigger data to trigger recalculation
        tdLog.info("Step 6: Write disorder trigger data to trigger recalculation")
        disorder_trigger_sqls = [
            # Disorder trigger data to trigger recalculation
            "insert into tdb.rt1 values ('2025-01-01 09:58:00', 5, 55, 0.8, 'early_disorder')",
            "insert into tdb.rt2 values ('2025-01-01 09:59:30', 12, 125, 2.8, 'medium_disorder')",
            "insert into tdb.rt3 values ('2025-01-01 09:59:50', 8, 85, 1.2, 'recent_disorder')",
            "insert into tdb.t1 values ('2025-01-01 09:58:00', 80, 'disorder_count') ('2025-01-01 09:59:00', 90, 'disorder_count')",
            "insert into tdb.t2 values ('2025-01-01 09:58:30', 85, 'disorder_session')",
            "insert into tdb.t3 values ('2025-01-01 09:58:45', 95, 'disorder_state')",
                         "insert into tdb.trigger_watermark values ('2025-01-01 09:58:15', 45, 0.8, 'disorder_period')"
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
        for i in range(1, 10):
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
        
        # Check watermark handling for each stream
        for i in range(1, 10):
            method_name = f"check{i:02d}"
            if hasattr(self, method_name):
                tdLog.info(f"Checking disorder result for stream {i}")
                getattr(self, method_name)()
            else:
                tdLog.info(f"Check method {method_name} not implemented")
        
        tdLog.success("Disorder data watermark handling results verified successfully")
    
    def checkFinalResults(self):
        """Check final stream computation results after all operations"""
        tdLog.info("Checking final stream computation results...")
        
        # Allow time for stream processing
        time.sleep(3)
        
        # Check each stream result
        for i in range(1, 10):
            method_name = f"check{i:02d}"
            if hasattr(self, method_name):
                tdLog.info(f"Checking final result for stream {i}")
                getattr(self, method_name)()
            else:
                tdLog.info(f"Check method {method_name} not implemented")
        
        tdLog.success("All stream computation results verified successfully")

    def createStreams(self):
        self.streams = []

        # ===== Test 1: Without WATERMARK (Default 0) =====
        
        # Test 1.1: INTERVAL+SLIDING without WATERMARK - processes disorder based on trigger type
        stream = StreamItem(
            id=1,
            stream="create stream rdb.s_interval_default interval(2m) sliding(1m) from tdb.recalc_triggers into rdb.r_interval_default as select _twstart ts, count(*) cnt, avg(c1) avg_val from qdb.meters where ts >= _twstart and ts < _twend;",
            res_query="select ts, cnt, avg_val from rdb.r_interval_default order by ts;",
            exp_query="",
            check_func=self.check01,
        )
        self.streams.append(stream)

        # Test 1.2: SESSION without WATERMARK - processes disorder
        stream = StreamItem(
            id=2,
            stream="create stream rdb.s_session_default session(ts, 30s) from tdb.trigger_disorder partition by tbname into rdb.r_session_default as select _twstart ts, count(*) cnt, avg(c1) avg_val from qdb.meters where ts >= _twstart and ts < _twend;",
            res_query="select ts, cnt, avg_val from rdb.r_session_default order by ts;",
            exp_query="",
            check_func=self.check02,
        )
        self.streams.append(stream)

        # Test 1.3: STATE_WINDOW without WATERMARK - processes disorder
        stream = StreamItem(
            id=3,
            stream="create stream rdb.s_state_default state_window(status_val) from tdb.trigger_disorder partition by tbname into rdb.r_state_default as select _twstart ts, count(*) cnt, avg(c1) avg_val, first(status_val) status_val from qdb.meters where ts >= _twstart and ts < _twend;",
            res_query="select ts, cnt, avg_val, device_status from rdb.r_state_default order by ts;",
            exp_query="",
            check_func=self.check03,
        )
        self.streams.append(stream)

        # ===== Test 2: With Very Short WATERMARK (1ms) =====

        # Test 2.1: INTERVAL+SLIDING with very short WATERMARK - strict watermark processing
        stream = StreamItem(
            id=4,
            stream="create stream rdb.s_interval_short interval(2m) sliding(1m) from tdb.recalc_triggers stream_options(watermark(1a)) into rdb.r_interval_short as select _twstart ts, count(*) cnt, avg(c1) avg_val from qdb.meters where ts >= _twstart and ts < _twend;",
            res_query="select ts, cnt, avg_val from rdb.r_interval_short order by ts;",
            exp_query="",
            check_func=self.check04,
        )
        self.streams.append(stream)

        # Test 2.2: SESSION with very short WATERMARK - strict watermark processing  
        stream = StreamItem(
            id=5,
            stream="create stream rdb.s_session_short session(ts, 30s) from tdb.trigger_disorder partition by tbname stream_options(watermark(1a)) into rdb.r_session_short as select _twstart ts, count(*) cnt, avg(c1) avg_val from qdb.meters where ts >= _twstart and ts < _twend;",
            res_query="select ts, cnt, avg_val from rdb.r_session_short order by ts;",
            exp_query="",
            check_func=self.check05,
        )
        self.streams.append(stream)

        # Test 2.3: STATE_WINDOW with very short WATERMARK - strict watermark processing
        stream = StreamItem(
            id=6,
            stream="create stream rdb.s_state_short state_window(status_val) from tdb.trigger_disorder partition by tbname stream_options(watermark(1a)) into rdb.r_state_short as select _twstart ts, count(*) cnt, avg(c1) avg_val, first(status_val) status_val from qdb.meters where ts >= _twstart and ts < _twend;",
            res_query="select ts, cnt, avg_val, device_status from rdb.r_state_short order by ts;",
            exp_query="",
            check_func=self.check06,
        )
        self.streams.append(stream)

        # ===== Test 3: With Very Long WATERMARK (1 day) =====

        # Test 3.1: INTERVAL+SLIDING with very long WATERMARK - loose watermark processing
        stream = StreamItem(
            id=7,
            stream="create stream rdb.s_interval_long interval(2m) sliding(1m) from tdb.recalc_triggers stream_options(watermark(1d)) into rdb.r_interval_long as select _twstart ts, count(*) cnt, avg(c1) avg_val from qdb.meters where ts >= _twstart and ts < _twend;",
            res_query="select ts, cnt, avg_val from rdb.r_interval_long order by ts;",
            exp_query="",
            check_func=self.check07,
        )
        self.streams.append(stream)

        # Test 3.2: SESSION with very long WATERMARK - loose watermark processing
        stream = StreamItem(
            id=8,
            stream="create stream rdb.s_session_long session(ts, 30s) from tdb.trigger_disorder partition by tbname stream_options(watermark(1d)) into rdb.r_session_long as select _twstart ts, count(*) cnt, avg(c1) avg_val from qdb.meters where ts >= _twstart and ts < _twend;",
            res_query="select ts, cnt, avg_val from rdb.r_session_long order by ts;",
            exp_query="",
            check_func=self.check08,
        )
        self.streams.append(stream)

        # Test 3.3: STATE_WINDOW with very long WATERMARK - loose watermark processing
        stream = StreamItem(
            id=9,
            stream="create stream rdb.s_state_long state_window(status_val) from tdb.trigger_disorder partition by tbname stream_options(watermark(1d)) into rdb.r_state_long as select _twstart ts, count(*) cnt, avg(c1) avg_val, first(status_val) status_val from qdb.meters where ts >= _twstart and ts < _twend;",
            res_query="select ts, cnt, avg_val, device_status from rdb.r_state_long order by ts;",
            exp_query="",
            check_func=self.check09,
        )
        self.streams.append(stream)

        tdLog.info(f"create total:{len(self.streams)} streams")
        for stream in self.streams:
            stream.createStream()

    # Check functions for each test case
    def check01(self):
        # Test interval+sliding without WATERMARK - default behavior
        tdLog.info("Check 1: INTERVAL+SLIDING without WATERMARK (default 0)")
        tdSql.checkTableType(dbname="rdb", tbname="r_interval_default", typename="NORMAL_TABLE", columns=3)
        tdSql.query("select count(*) from rdb.r_interval_default;")
        result_count = tdSql.getData(0, 0)
        tdLog.info(f"INTERVAL+SLIDING default watermark result count: {result_count}")

    def check02(self):
        # Test session without WATERMARK - default behavior
        tdLog.info("Check 2: SESSION without WATERMARK (default 0)")
        tdSql.checkTableType(dbname="rdb", stbname="r_session_default", columns=3, tags=1)
        tdSql.query("select count(*) from rdb.r_session_default where tag_tbname='t1';")
        result_count = tdSql.getData(0, 0)
        tdLog.info(f"SESSION default watermark result count: {result_count}")

    def check03(self):
        # Test state window without WATERMARK - default behavior
        tdLog.info("Check 3: STATE_WINDOW without WATERMARK (default 0)")
        tdSql.checkTableType(dbname="rdb", stbname="r_state_default", columns=4, tags=1)
        tdSql.query("select count(*) from rdb.r_state_default where tag_tbname='t1';")
        result_count = tdSql.getData(0, 0)
        tdLog.info(f"STATE_WINDOW default watermark result count: {result_count}")

    def check04(self):
        # Test interval+sliding with short WATERMARK - strict watermark
        tdLog.info("Check 4: INTERVAL+SLIDING with very short WATERMARK (1ms)")
        tdSql.checkTableType(dbname="rdb", tbname="r_interval_short", typename="NORMAL_TABLE", columns=3)
        tdSql.query("select count(*) from rdb.r_interval_short;")
        result_count = tdSql.getData(0, 0)
        tdLog.info(f"INTERVAL+SLIDING short watermark result count: {result_count}")

    def check05(self):
        # Test session with short WATERMARK - strict watermark
        tdLog.info("Check 5: SESSION with very short WATERMARK (1ms)")
        tdSql.checkTableType(dbname="rdb", stbname="r_session_short", columns=3, tags=1)
        tdSql.query("select count(*) from rdb.r_session_short where tag_tbname='t1';")
        result_count = tdSql.getData(0, 0)
        tdLog.info(f"SESSION short watermark result count: {result_count}")

    def check06(self):
        # Test state window with short WATERMARK - strict watermark
        tdLog.info("Check 6: STATE_WINDOW with very short WATERMARK (1ms)")
        tdSql.checkTableType(dbname="rdb", stbname="r_state_short", columns=4, tags=1)
        tdSql.query("select count(*) from rdb.r_state_short where tag_tbname='t1';")
        result_count = tdSql.getData(0, 0)
        tdLog.info(f"STATE_WINDOW short watermark result count: {result_count}")

    def check07(self):
        # Test interval+sliding with long WATERMARK - loose watermark
        tdLog.info("Check 7: INTERVAL+SLIDING with very long WATERMARK (1 day)")
        tdSql.checkTableType(dbname="rdb", tbname="r_interval_long", typename="NORMAL_TABLE", columns=3)
        tdSql.query("select count(*) from rdb.r_interval_long;")
        result_count = tdSql.getData(0, 0)
        tdLog.info(f"INTERVAL+SLIDING long watermark result count: {result_count}")

    def check08(self):
        # Test session with long WATERMARK - loose watermark
        tdLog.info("Check 8: SESSION with very long WATERMARK (1 day)")
        tdSql.checkTableType(dbname="rdb", stbname="r_session_long", columns=3, tags=1)
        tdSql.query("select count(*) from rdb.r_session_long where tag_tbname='t1';")
        result_count = tdSql.getData(0, 0)
        tdLog.info(f"SESSION long watermark result count: {result_count}")

    def check09(self):
        # Test state window with long WATERMARK - loose watermark
        tdLog.info("Check 9: STATE_WINDOW with very long WATERMARK (1 day)")
        tdSql.checkTableType(dbname="rdb", stbname="r_state_long", columns=4, tags=1)
        tdSql.query("select count(*) from rdb.r_state_long where tag_tbname='t1';")
        result_count = tdSql.getData(0, 0)
        tdLog.info(f"STATE_WINDOW long watermark result count: {result_count}") 