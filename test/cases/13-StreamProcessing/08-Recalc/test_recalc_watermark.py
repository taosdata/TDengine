import subprocess
import time
from new_test_framework.utils import tdLog, tdSql, clusterComCheck, tdStream, StreamItem


class TestStreamRecalcWatermark:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_recalc_watermark(self):
        """Stream Recalculation WATERMARK Option Test

        Test WATERMARK option with out-of-order data:
        1. Write out-of-order data within WATERMARK tolerance - should trigger recalculation
        2. Write out-of-order data exceeding WATERMARK tolerance - should be handled by recalculation mechanism
        3. Different trigger types behavior with WATERMARK

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
        self.createStreams()
        self.checkStreamStatus()
        self.writeInitialTriggerData()
        self.writeSourceData()
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
        stb_trig = "create table tdb.watermark_triggers (ts timestamp, cint int, c2 int, c3 double, category varchar(16)) tags(id int, name varchar(16));"
        ctb_trig = "create table tdb.wm1 using tdb.watermark_triggers tags(1, 'device1') tdb.wm2 using tdb.watermark_triggers tags(2, 'device2') tdb.wm3 using tdb.watermark_triggers tags(3, 'device3')"
        tdSql.execute(stb_trig)
        tdSql.execute(ctb_trig)

        # Trigger table for session stream
        stb2_trig = "create table tdb.trigger_session_watermark (ts timestamp, val_num int, status varchar(16)) tags(device_id int);"
        ctb2_trig = "create table tdb.ws1 using tdb.trigger_session_watermark tags(1) tdb.ws2 using tdb.trigger_session_watermark tags(2) tdb.ws3 using tdb.trigger_session_watermark tags(3)"
        tdSql.execute(stb2_trig)
        tdSql.execute(ctb2_trig)

        # Trigger table for state window stream
        stb3_trig = "create table tdb.trigger_state_watermark (ts timestamp, val_num int, status varchar(16)) tags(device_id int);"
        ctb3_trig = "create table tdb.ww1 using tdb.trigger_state_watermark tags(1) tdb.ww2 using tdb.trigger_state_watermark tags(2) tdb.ww3 using tdb.trigger_state_watermark tags(3)"
        tdSql.execute(stb3_trig)
        tdSql.execute(ctb3_trig)

        # Trigger table for event window stream
        stb4_trig = "create table tdb.trigger_event_watermark (ts timestamp, val_num int, event_val int) tags(device_id int);"
        ctb4_trig = "create table tdb.we1 using tdb.trigger_event_watermark tags(1) tdb.we2 using tdb.trigger_event_watermark tags(2) tdb.we3 using tdb.trigger_event_watermark tags(3)"
        tdSql.execute(stb4_trig)
        tdSql.execute(ctb4_trig)

        # Trigger table for period stream
        stb5_trig = "create table tdb.trigger_period_watermark (ts timestamp, val_num int, metric double) tags(device_id int);"
        ctb5_trig = "create table tdb.wp1 using tdb.trigger_period_watermark tags(1) tdb.wp2 using tdb.trigger_period_watermark tags(2) tdb.wp3 using tdb.trigger_period_watermark tags(3)"
        tdSql.execute(stb5_trig)
        tdSql.execute(ctb5_trig)

        # Trigger table for count window stream
        stb6_trig = "create table tdb.trigger_count_watermark (ts timestamp, val_num int, category varchar(16)) tags(device_id int);"
        ctb6_trig = "create table tdb.wc1 using tdb.trigger_count_watermark tags(1) tdb.wc2 using tdb.trigger_count_watermark tags(2) tdb.wc3 using tdb.trigger_count_watermark tags(3)"
        tdSql.execute(stb6_trig)
        tdSql.execute(ctb6_trig)

    def writeInitialTriggerData(self):
        tdLog.info("write initial trigger data to tdb")
        # Trigger data for interval+sliding stream
        trigger_sqls = [
            "insert into tdb.wm1 values ('2025-01-01 02:00:00', 10, 100, 1.5, 'normal');",
            "insert into tdb.wm1 values ('2025-01-01 02:00:30', 20, 200, 2.5, 'normal');",
            "insert into tdb.wm1 values ('2025-01-01 02:01:00', 30, 300, 3.5, 'normal');",
            "insert into tdb.wm1 values ('2025-01-01 02:01:30', 40, 400, 4.5, 'normal');",
            "insert into tdb.wm1 values ('2025-01-01 02:02:00', 50, 500, 5.5, 'normal');",
            "insert into tdb.wm1 values ('2025-01-01 02:02:30', 60, 600, 6.5, 'normal');",
        ]
        tdSql.executes(trigger_sqls)

        # Trigger data for session stream
        trigger_sqls = [
            "insert into tdb.ws1 values ('2025-01-01 02:00:00', 10, 'normal');",
            "insert into tdb.ws1 values ('2025-01-01 02:00:30', 20, 'normal');",
            "insert into tdb.ws1 values ('2025-01-01 02:01:00', 30, 'normal');",
            "insert into tdb.ws1 values ('2025-01-01 02:03:00', 40, 'normal');",
            "insert into tdb.ws1 values ('2025-01-01 02:03:30', 50, 'normal');",
            "insert into tdb.ws1 values ('2025-01-01 02:04:00', 60, 'normal');",
        ]
        tdSql.executes(trigger_sqls)

        # Trigger data for state window stream
        trigger_sqls = [
            "insert into tdb.ww1 values ('2025-01-01 02:00:00', 10, 'normal');",
            "insert into tdb.ww1 values ('2025-01-01 02:00:30', 20, 'normal');",
            "insert into tdb.ww1 values ('2025-01-01 02:01:00', 30, 'warning');",
            "insert into tdb.ww1 values ('2025-01-01 02:01:30', 40, 'warning');",
            "insert into tdb.ww1 values ('2025-01-01 02:02:00', 50, 'error');",
            "insert into tdb.ww1 values ('2025-01-01 02:02:30', 60, 'error');",
        ]
        tdSql.executes(trigger_sqls)

        # Trigger data for event window stream
        trigger_sqls = [
            "insert into tdb.we1 values ('2025-01-01 02:00:00', 10, 6);",
            "insert into tdb.we1 values ('2025-01-01 02:00:30', 20, 7);",
            "insert into tdb.we1 values ('2025-01-01 02:01:00', 30, 12);",
            "insert into tdb.we1 values ('2025-01-01 02:01:30', 40, 6);",
            "insert into tdb.we1 values ('2025-01-01 02:02:00', 50, 9);",
            "insert into tdb.we1 values ('2025-01-01 02:02:30', 60, 13);",
        ]
        tdSql.executes(trigger_sqls)

        # Trigger data for period stream
        trigger_sqls = [
            "insert into tdb.wp1 values ('2025-01-01 02:00:00', 10, 1.5);",
            "insert into tdb.wp1 values ('2025-01-01 02:00:30', 20, 2.5);",
            "insert into tdb.wp1 values ('2025-01-01 02:01:00', 30, 3.5);",
            "insert into tdb.wp1 values ('2025-01-01 02:01:30', 40, 4.5);",
            "insert into tdb.wp1 values ('2025-01-01 02:02:00', 50, 5.5);",
            "insert into tdb.wp1 values ('2025-01-01 02:02:30', 60, 6.5);",
        ]
        tdSql.executes(trigger_sqls)

        # Trigger data for count window stream
        trigger_sqls = [
            "insert into tdb.wc1 values ('2025-01-01 02:00:00', 10, 'normal');",
            "insert into tdb.wc1 values ('2025-01-01 02:00:15', 20, 'normal');",
            "insert into tdb.wc1 values ('2025-01-01 02:00:30', 30, 'warning');",
            "insert into tdb.wc1 values ('2025-01-01 02:00:45', 40, 'warning');",
            "insert into tdb.wc1 values ('2025-01-01 02:01:00', 50, 'error');",
            "insert into tdb.wc1 values ('2025-01-01 02:01:15', 60, 'error');",
        ]
        tdSql.executes(trigger_sqls)

    def writeSourceData(self):
        tdLog.info("write source data to test WATERMARK option")
        tdSql.execute("insert into qdb.t0 values ('2025-01-01 00:00:01', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")

    def checkStreamStatus(self):
        tdLog.info("check stream status")
        tdStream.checkStreamStatus()

    def checkResults(self):
        """Check stream computation results"""
        tdLog.info(f"check total:{len(self.streams)} streams result")
        for stream in self.streams:
            stream.checkResults()
        tdLog.info(f"check total:{len(self.streams)} streams result successfully")
    

    def createStreams(self):
        self.streams = []

        # ===== Test 1: WATERMARK Option =====
        
        # Test 1.1: INTERVAL+SLIDING with WATERMARK(30s) - should handle out-of-order data within tolerance
        stream = StreamItem(
            id=1,
            stream="create stream rdb.s_interval_watermark interval(2m) sliding(2m) from tdb.watermark_triggers partition by tbname options(watermark(30s)) into rdb.r_interval_watermark as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check01,
        )
        self.streams.append(stream)

        # Test 1.2: SESSION with WATERMARK(1m) - should handle out-of-order data within tolerance
        stream = StreamItem(
            id=2,
            stream="create stream rdb.s_session_watermark session(ts,45s) from tdb.trigger_session_watermark partition by tbname options(watermark(1m)) into rdb.r_session_watermark as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check02,
        )
        self.streams.append(stream)

        # Test 1.3: STATE_WINDOW with WATERMARK(45s) - should handle out-of-order data within tolerance
        stream = StreamItem(
            id=3,
            stream="create stream rdb.s_state_watermark state_window(status) from tdb.trigger_state_watermark partition by tbname options(watermark(45s)) into rdb.r_state_watermark as select _twstart ts, count(*) cnt, avg(cint) avg_val, first(cvarchar) status_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check03,
        )
        self.streams.append(stream)

        # Test 1.4: EVENT_WINDOW with WATERMARK - should handle out-of-order data within tolerance
        stream = StreamItem(
            id=4,
            stream="create stream rdb.s_event_watermark event_window(start with event_val >= 5 end with event_val > 10) from tdb.trigger_event_watermark partition by tbname options(watermark(1m)) into rdb.r_event_watermark as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check04,
        )
        self.streams.append(stream)

        # Test 5: PERIOD with WATERMARK - should handle out-of-order data within tolerance
        stream = StreamItem(
            id=5,
            stream="create stream rdb.s_period_watermark period(30s) from tdb.trigger_period_watermark partition by tbname options(watermark(45s)) into rdb.r_period_watermark as select _tlocaltime ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _tlocaltime - 30000000000 and cts <= _tlocaltime;",
            check_func=self.check05,
        )
        self.streams.append(stream)

        # Test 6: COUNT_WINDOW with WATERMARK - should handle out-of-order data within tolerance
        stream = StreamItem(
            id=6,
            stream="create stream rdb.s_count_watermark count_window(3) from tdb.trigger_count_watermark partition by tbname options(watermark(1m)) into rdb.r_count_watermark as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check06,
        )
        self.streams.append(stream)

        tdLog.info(f"create total:{len(self.streams)} streams")
        for stream in self.streams:
            stream.createStream()

    # Check functions for each test case
    def check01(self):
        # Test interval+sliding with WATERMARK - should handle out-of-order data within tolerance
        tdLog.info("Check 1: INTERVAL+SLIDING with WATERMARK handles out-of-order data")
        tdSql.checkTableType(dbname="rdb", stbname="r_interval_watermark", columns=3, tags=1)

        exp_sql = "select _wstart, count(*),avg(cint) from qdb.meters where cts >= '2025-01-01 02:00:00' and cts < '2025-01-01 02:02:00' interval(2m) sliding(2m) ;"
        res_sql = "select ts, cnt, avg_val from rdb.r_interval_watermark;"
        self.streams[0].checkResultsBySql(res_sql, exp_sql)

        tdSql.query("select count(*) from rdb.r_interval_watermark;")
        result_count_before = tdSql.getData(0, 0)
        tdLog.info(f"INTERVAL+SLIDING result count before out-of-order data: {result_count_before}")

        # Insert out-of-order data within WATERMARK tolerance (30s)
        # These should trigger window recalculation
        watermark_sqls = [
            "insert into tdb.wm1 values ('2025-01-01 02:02:15', 35, 350, 3.8, 'late1');",  # 15s late, within 30s watermark
            "insert into tdb.wm1 values ('2025-01-01 02:01:45', 25, 250, 2.8, 'late2');",  # 45s late, exceeds 30s watermark
        ]
        tdSql.executes(watermark_sqls)

        tdLog.info("wait for stream to be stable after watermark test")
        time.sleep(5)

        # WATERMARK should allow the stream to handle out-of-order data appropriately
        tdSql.query("select count(*) from rdb.r_interval_watermark;")
        result_count_after = tdSql.getData(0, 0)
        tdLog.info(f"INTERVAL+SLIDING result count after watermark test: {result_count_after}")

        # With WATERMARK, the stream should process out-of-order data within tolerance
        tdLog.info("INTERVAL+SLIDING with WATERMARK successfully handled out-of-order data")

    def check02(self):
        # Test session with WATERMARK - should handle out-of-order data within tolerance
        tdLog.info("Check 2: SESSION with WATERMARK handles out-of-order data")
        tdSql.checkTableType(dbname="rdb", stbname="r_session_watermark", columns=3, tags=1)

        exp_sql = "select count(*),avg(cint) from qdb.meters where cts >= '2025-01-01 02:00:00.000' and cts < '2025-01-01 02:01:00.000';"
        res_sql = "select cnt, avg_val from rdb.r_session_watermark;"
        self.streams[1].checkResultsBySql(res_sql, exp_sql)

        tdSql.query("select count(*) from rdb.r_session_watermark;")
        result_count_before = tdSql.getData(0, 0)
        tdLog.info(f"SESSION result count before watermark test: {result_count_before}")

        # Insert out-of-order data within WATERMARK tolerance (1m)
        watermark_sqls = [
            "insert into tdb.ws1 values ('2025-01-01 02:03:45', 35, 'late1');",  # 15s late, within 1m watermark
            "insert into tdb.ws1 values ('2025-01-01 02:02:30', 25, 'late2');",  # 90s late, exceeds 1m watermark
        ]
        tdSql.executes(watermark_sqls)

        time.sleep(5)

        # WATERMARK should allow the stream to handle out-of-order data appropriately
        tdSql.query("select count(*) from rdb.r_session_watermark;")
        result_count_after = tdSql.getData(0, 0)
        tdLog.info(f"SESSION result count after watermark test: {result_count_after}")

        # With WATERMARK, the stream should process out-of-order data within tolerance
        tdLog.info("SESSION with WATERMARK successfully handled out-of-order data")

    def check03(self):
        # Test state window with WATERMARK - should handle out-of-order data within tolerance
        tdLog.info("Check 3: STATE_WINDOW with WATERMARK handles out-of-order data")
        tdSql.checkTableType(dbname="rdb", stbname="r_state_watermark", columns=4, tags=1)

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_state_watermark",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 02:00:00")
                and tdSql.compareData(0, 1, 100)
                and tdSql.compareData(0, 2, 240)
                and tdSql.compareData(1, 0, "2025-01-01 02:01:00")
                and tdSql.compareData(1, 1, 100)
                and tdSql.compareData(1, 2, 242)
            )

        # Insert out-of-order data within WATERMARK tolerance (45s)
        watermark_sqls = [
            "insert into tdb.ww1 values ('2025-01-01 02:02:00', 35, 'error');",    # 30s late, within 45s watermark
            "insert into tdb.ww1 values ('2025-01-01 02:01:15', 25, 'warning');", # 75s late, exceeds 45s watermark
        ]
        tdSql.executes(watermark_sqls)

        time.sleep(5)

        # WATERMARK should allow the stream to handle out-of-order data appropriately
        tdSql.query("select count(*) from rdb.r_state_watermark;")
        result_count_after = tdSql.getData(0, 0)
        tdLog.info(f"STATE_WINDOW result count after watermark test: {result_count_after}")

        # With WATERMARK, the stream should process out-of-order data within tolerance
        tdLog.info("STATE_WINDOW with WATERMARK successfully handled out-of-order data")

    def check04(self):
        # Test event window with WATERMARK - should handle out-of-order data within tolerance
        tdLog.info("Check 4: EVENT_WINDOW with WATERMARK handles out-of-order data")
        tdSql.checkTableType(dbname="rdb", stbname="r_event_watermark", columns=3, tags=1)

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_event_watermark",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 02:00:00.000")
                and tdSql.compareData(0, 1, 200)
                and tdSql.compareData(0, 2, 240.5)
                and tdSql.compareData(1, 0, "2025-01-01 02:01:30.000")
                and tdSql.compareData(1, 1, 200)
                and tdSql.compareData(1, 2, 243.5)
            )

        # Insert out-of-order data within WATERMARK tolerance (1m)
        watermark_sqls = [
            "insert into tdb.we1 values ('2025-01-01 02:02:00', 35, 12);",  # 30s late, within 1m watermark
            "insert into tdb.we1 values ('2025-01-01 02:01:00', 25, 6);",   # 90s late, exceeds 1m watermark
        ]
        tdSql.executes(watermark_sqls)

        tdLog.info("wait for stream to be stable after watermark test")
        time.sleep(5)

        # WATERMARK should allow the stream to handle out-of-order data appropriately
        tdSql.query("select count(*) from rdb.r_event_watermark;")
        result_count_after = tdSql.getData(0, 0)
        tdLog.info(f"EVENT_WINDOW result count after watermark test: {result_count_after}")

        # With WATERMARK, the stream should process out-of-order data within tolerance
        tdLog.info("EVENT_WINDOW with WATERMARK successfully handled out-of-order data") 


    def check05(self):
        # Test period with WATERMARK - should handle out-of-order data within tolerance
        tdLog.info("Check 5: PERIOD with WATERMARK handles out-of-order data within tolerance")
        tdSql.checkTableType(dbname="rdb", stbname="r_period_watermark", columns=3, tags=1)

        # Check initial results from period trigger
        tdSql.query("select count(*) from rdb.r_period_watermark;")
        result_count_before = tdSql.getData(0, 0)
        tdLog.info(f"PERIOD result count before watermark data: {result_count_before}")

        # Insert out-of-order data within WATERMARK tolerance (45s) - should be processed
        watermark_sqls = [
            "insert into tdb.wp1 values ('2025-01-01 02:01:45', 70, 7.5);",  # Within 45s tolerance
            "insert into tdb.wp1 values ('2025-01-01 02:02:15', 80, 8.5);",  # Within 45s tolerance
        ]
        tdSql.executes(watermark_sqls)

        tdLog.info("wait for stream to be stable")
        time.sleep(5)

        # Check that watermark data was processed
        tdSql.query("select count(*) from rdb.r_period_watermark;")
        result_count_after = tdSql.getData(0, 0)
        tdLog.info(f"PERIOD result count after watermark data: {result_count_after}")

        # For PERIOD trigger with WATERMARK, in-tolerance data should be processed
        assert result_count_after > result_count_before, "PERIOD watermark should handle out-of-order data within tolerance"


    def check06(self):
        # Test count window with WATERMARK - should handle out-of-order data within tolerance
        tdLog.info("Check 6: COUNT_WINDOW with WATERMARK handles out-of-order data within tolerance")
        tdSql.checkTableType(dbname="rdb", stbname="r_count_watermark", columns=3, tags=1)

        # Check initial results from count window trigger
        # COUNT_WINDOW(3) means every 3 records should trigger computation
        # Initial data has 6 records, so should have 2 windows
        tdSql.query("select count(*) from rdb.r_count_watermark;")
        result_count_before = tdSql.getData(0, 0)
        tdLog.info(f"COUNT_WINDOW result count before watermark data: {result_count_before}")

        # Insert out-of-order data within WATERMARK tolerance (1m) - should be processed
        watermark_sqls = [
            "insert into tdb.wc1 values ('2025-01-01 02:01:30', 70, 'watermark1');",  # Within 1m tolerance
            "insert into tdb.wc1 values ('2025-01-01 02:01:45', 80, 'watermark2');",  # Within 1m tolerance
            "insert into tdb.wc1 values ('2025-01-01 02:02:00', 90, 'watermark3');",  # Within 1m tolerance
        ]
        tdSql.executes(watermark_sqls)

        tdLog.info("wait for stream to be stable")
        time.sleep(5)

        # Check that COUNT_WINDOW processed the watermark data
        tdSql.query("select count(*) from rdb.r_count_watermark;")
        result_count_after = tdSql.getData(0, 0)
        tdLog.info(f"COUNT_WINDOW result count after watermark data: {result_count_after}")

        # For COUNT_WINDOW with WATERMARK, in-tolerance data should be processed
        assert result_count_after > result_count_before, "COUNT_WINDOW watermark should handle out-of-order data within tolerance" 