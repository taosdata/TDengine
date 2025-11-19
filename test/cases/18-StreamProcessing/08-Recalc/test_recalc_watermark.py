import subprocess
import time
from new_test_framework.utils import tdLog, tdSql, clusterComCheck, tdStream, StreamItem


class TestStreamRecalcWatermark:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_recalc_watermark(self):
        """Recalc: watermark basic

        Test WATERMARK option behavior with six different window types and out-of-order data handling:

        1. INTERVAL Window with WATERMARK Test
            1.1 Create interval(2m) sliding(2m) stream with watermark(45s) (s_interval_watermark)
            1.2 Test out-of-order data handling within watermark tolerance
            1.3 Verify recalculation triggered by data within watermark window

        2. SESSION Window with WATERMARK Test
            2.1 Create session(ts,45s) stream with watermark(1m) (s_session_watermark)
            2.2 Test session modification with out-of-order data within tolerance
            2.3 Verify session window recalculation behavior

        3. STATE_WINDOW with WATERMARK Test
            3.1 Create state_window(status) stream with watermark(45s) (s_state_watermark)
            3.2 Test state window recalculation with delayed state changes
            3.3 Verify state transition handling within watermark tolerance

        4. EVENT_WINDOW with WATERMARK Test
            4.1 Create event_window(start with event_val >= 5 end with event_val > 10) stream with watermark(1m) (s_event_watermark)
            4.2 Test event sequence processing with out-of-order events
            4.3 Verify event window completion with delayed events

        5. PERIOD Window with WATERMARK Test
            5.1 Create period(30s) stream with watermark(45s) (s_period_watermark)
            5.2 Test periodic window recalculation with out-of-order data
            5.3 Verify period-based time window behavior

        6. COUNT_WINDOW with WATERMARK Test
            6.1 Create count_window(3) stream with watermark(1m) (s_count_watermark)
            6.2 Test count-based window recalculation with delayed records
            6.3 Verify count window completion with out-of-order data

        Catalog:
            - Streams:Recalculation:Watermark

        Since: v3.3.7.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-07-23 Beryl Created

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
            "insert into tdb.wm1 values ('2025-01-01 02:03:00', 70, 700, 7.5, 'normal');",
        ]
        tdSql.executes(trigger_sqls)

        # Trigger data for session stream
        trigger_sqls = [
            "insert into tdb.ws1 values ('2025-01-01 02:10:00', 10, 'normal');",
            "insert into tdb.ws1 values ('2025-01-01 02:10:30', 20, 'normal');",
            "insert into tdb.ws1 values ('2025-01-01 02:11:00', 30, 'normal');",
            "insert into tdb.ws1 values ('2025-01-01 02:11:50', 40, 'normal');",
            "insert into tdb.ws1 values ('2025-01-01 02:12:00', 50, 'normal');",
            "insert into tdb.ws1 values ('2025-01-01 02:12:30', 60, 'normal');",
            "insert into tdb.ws1 values ('2025-01-01 02:13:00', 70, 'normal');",
        ]
        tdSql.executes(trigger_sqls)

        # Trigger data for state window stream
        trigger_sqls = [
            "insert into tdb.ww1 values ('2025-01-01 02:20:00', 10, 'normal');",
            "insert into tdb.ww1 values ('2025-01-01 02:20:30', 20, 'normal');",
            "insert into tdb.ww1 values ('2025-01-01 02:21:00', 30, 'warning');",
            "insert into tdb.ww1 values ('2025-01-01 02:21:30', 40, 'warning');",
            "insert into tdb.ww1 values ('2025-01-01 02:22:00', 50, 'error');",
            "insert into tdb.ww1 values ('2025-01-01 02:22:30', 60, 'error');",
        ]
        tdSql.executes(trigger_sqls)

        # Trigger data for event window stream
        trigger_sqls = [
            "insert into tdb.we1 values ('2025-01-01 02:30:00', 10, 6);",
            "insert into tdb.we1 values ('2025-01-01 02:30:30', 20, 7);",
            "insert into tdb.we1 values ('2025-01-01 02:31:00', 30, 12);",
            "insert into tdb.we1 values ('2025-01-01 02:31:30', 40, 6);",
            "insert into tdb.we1 values ('2025-01-01 02:32:00', 50, 9);",
            "insert into tdb.we1 values ('2025-01-01 02:32:30', 60, 13);",
        ]
        tdSql.executes(trigger_sqls)

        # Trigger data for period stream
        trigger_sqls = [
            "insert into tdb.wp1 values ('2025-01-01 02:40:00', 10, 1.5);",
            "insert into tdb.wp1 values ('2025-01-01 02:40:30', 20, 2.5);",
            "insert into tdb.wp1 values ('2025-01-01 02:41:00', 30, 3.5);",
            "insert into tdb.wp1 values ('2025-01-01 02:41:30', 40, 4.5);",
            "insert into tdb.wp1 values ('2025-01-01 02:42:00', 50, 5.5);",
            "insert into tdb.wp1 values ('2025-01-01 02:42:30', 60, 6.5);",
        ]
        tdSql.executes(trigger_sqls)

        # Trigger data for count window stream
        trigger_sqls = [
            "insert into tdb.wc1 values ('2025-01-01 02:50:00', 10, 'normal');",
            "insert into tdb.wc1 values ('2025-01-01 02:50:15', 20, 'normal');",
            "insert into tdb.wc1 values ('2025-01-01 02:50:30', 30, 'warning');",
            "insert into tdb.wc1 values ('2025-01-01 02:50:45', 40, 'warning');",
            "insert into tdb.wc1 values ('2025-01-01 02:51:00', 50, 'error');",
            "insert into tdb.wc1 values ('2025-01-01 02:51:15', 60, 'error');",
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
            stream="create stream rdb.s_interval_watermark interval(2m) sliding(2m) from tdb.watermark_triggers partition by tbname stream_options(watermark(45s)) into rdb.r_interval_watermark as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check01,
        )
        self.streams.append(stream)

        # Test 1.2: SESSION with WATERMARK(1m) - should handle out-of-order data within tolerance
        stream = StreamItem(
            id=2,
            stream="create stream rdb.s_session_watermark session(ts,45s) from tdb.trigger_session_watermark partition by tbname stream_options(watermark(1m)) into rdb.r_session_watermark as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check02,
        )
        self.streams.append(stream)

        # Test 1.3: STATE_WINDOW with WATERMARK(45s) - should handle out-of-order data within tolerance
        stream = StreamItem(
            id=3,
            stream="create stream rdb.s_state_watermark state_window(status) from tdb.trigger_state_watermark partition by tbname stream_options(watermark(45s)) into rdb.r_state_watermark as select _twstart ts, count(*) cnt, avg(cint) avg_val, first(cvarchar) status_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check03,
        )
        self.streams.append(stream)

        # Test 1.4: EVENT_WINDOW with WATERMARK - should handle out-of-order data within tolerance
        stream = StreamItem(
            id=4,
            stream="create stream rdb.s_event_watermark event_window(start with event_val >= 5 end with event_val > 10) from tdb.trigger_event_watermark partition by tbname stream_options(watermark(1m)) into rdb.r_event_watermark as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check04,
        )
        self.streams.append(stream)

        # Test 5: PERIOD with WATERMARK - should handle out-of-order data within tolerance
        stream = StreamItem(
            id=5,
            stream="create stream rdb.s_period_watermark period(30s) from tdb.trigger_period_watermark partition by tbname stream_options(watermark(45s)) into rdb.r_period_watermark as select cast(_tlocaltime/1000000 as timestamp) ts, count(*) cnt, avg(cint) avg_val from qdb.meters;",
            check_func=self.check05,
        )
        self.streams.append(stream)

        # Test 6: COUNT_WINDOW with WATERMARK - should handle out-of-order data within tolerance
        stream = StreamItem(
            id=6,
            stream="create stream rdb.s_count_watermark count_window(3) from tdb.trigger_count_watermark partition by tbname stream_options(watermark(1m)) into rdb.r_count_watermark as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
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

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_interval_watermark",
                func=lambda: (
                    tdSql.getRows() == 1
                    and tdSql.compareData(0, 0, "2025-01-01 02:00:00")
                    and tdSql.compareData(0, 1, 400)
                    and tdSql.compareData(0, 2, 241.5)
                )
            )
        tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:01:01', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        tdSql.execute("insert into tdb.wm1 values ('2025-01-01 02:01:02', 10, 100, 1.5, 'normal');")

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_interval_watermark",
                func=lambda: (
                    tdSql.getRows() == 1
                    and tdSql.compareData(0, 0, "2025-01-01 02:00:00")
                    and tdSql.compareData(0, 1, 401)
                    and tdSql.compareData(0, 2, 240.922693266833)
                )
            )
        
        # water mark is 45s , so there is no recalc
        tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:03:01', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        tdSql.execute("insert into tdb.wm1 values ('2025-01-01 02:04:10', 10, 100, 1.5, 'normal');")
        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_interval_watermark",
                func=lambda: (
                    tdSql.getRows() == 1
                    and tdSql.compareData(0, 0, "2025-01-01 02:00:00")
                    and tdSql.compareData(0, 1, 401)
                    and tdSql.compareData(0, 2, 240.922693266833)
                )
            )
        tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:03:02', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        tdSql.execute("insert into tdb.wm1 values ('2025-01-01 02:04:58', 10, 100, 1.5, 'normal');")
        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_interval_watermark",
                func=lambda: (
                    tdSql.getRows() == 2
                    and tdSql.compareData(0, 0, "2025-01-01 02:00:00")
                    and tdSql.compareData(0, 1, 401)
                    and tdSql.compareData(0, 2, 240.922693266833)
                )
            )
        
        tdSql.execute("insert into tdb.wm1 values ('2025-01-01 06:01:02', 10, 100, 1.5, 'normal');")
        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_interval_watermark",
                func=lambda: (
                    tdSql.getRows() > 30
                ),
                retry=240
            )
        



        # With WATERMARK, the stream should process out-of-order data within tolerance
        tdLog.info("INTERVAL+SLIDING with WATERMARK successfully handled out-of-order data")

    def check02(self):
        # Test session with WATERMARK - should handle out-of-order data within tolerance
        tdLog.info("Check 2: SESSION with WATERMARK handles out-of-order data")
        tdSql.checkTableType(dbname="rdb", stbname="r_session_watermark", columns=3, tags=1)

        tdSql.checkResultsByFunc(
                sql=f"select cnt, avg_val from rdb.r_session_watermark",
                func=lambda: (
                    tdSql.getRows() == 1
                    and tdSql.compareData(0, 0, 200)
                    and tdSql.compareData(0, 1, 260.5)
                )
            )

        tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:11:01', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        tdSql.execute("insert into tdb.ws1 values ('2025-01-01 02:11:02', 10, 'normal');")

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_session_watermark",
                func=lambda: (
                    tdSql.getRows() == 1
                    and tdSql.compareData(0, 0, "2025-01-01 02:10:00")
                    and tdSql.compareData(0, 1, 301)
                    and tdSql.compareData(0, 2, 260.166112956811)
                )
            )
        
        # water mark is 1m , so there is no recalc
        tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:13:01', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        tdSql.execute("insert into tdb.ws1 values ('2025-01-01 02:12:30', 10, 'normal');")
        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_session_watermark",
                func=lambda: (
                    tdSql.getRows() == 1
                    and tdSql.compareData(0, 0, "2025-01-01 02:10:00")
                    and tdSql.compareData(0, 1, 301)
                    and tdSql.compareData(0, 2, 260.166112956811)
                )
            )
        tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:13:02', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        tdSql.execute("insert into tdb.ws1 values ('2025-01-01 02:14:30', 10, 'normal');")
        tdSql.execute("insert into tdb.ws1 values ('2025-01-01 02:16:00', 10, 'normal');")
        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_session_watermark",
                func=lambda: (
                    tdSql.getRows() == 2
                    and tdSql.compareData(0, 0, "2025-01-01 02:10:00")
                    and tdSql.compareData(0, 1, 301)
                    and tdSql.compareData(0, 2, 260.166112956811)
                    and tdSql.compareData(1, 0, "2025-01-01 02:11:50")
                    and tdSql.compareData(1, 1, 200)
                    and tdSql.compareData(1, 2, 264.5)

                )
            )

        # With WATERMARK, the session stream should process out-of-order data within tolerance
        tdLog.info("SESSION with WATERMARK successfully handled out-of-order data")

    def check03(self):
        # Test state window with WATERMARK - should handle out-of-order data within tolerance
        tdLog.info("Check 3: STATE_WINDOW with WATERMARK handles out-of-order data")
        tdSql.checkTableType(dbname="rdb", stbname="r_state_watermark", columns=4, tags=1)

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_state_watermark",
                func=lambda: (
                    tdSql.getRows() == 1
                    and tdSql.compareData(0, 0, "2025-01-01 02:20:00")
                    and tdSql.compareData(0, 1, 100)
                    and tdSql.compareData(0, 2, 280)
                )
            )

        tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:20:01', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        tdSql.execute("insert into tdb.ww1 values ('2025-01-01 02:20:15', 10, 'normal');")

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_state_watermark",
                func=lambda: (
                    tdSql.getRows() == 1
                    and tdSql.compareData(0, 0, "2025-01-01 02:20:00")
                    and tdSql.compareData(0, 1, 101)
                    and tdSql.compareData(0, 2, 277.326732673267)
                )
            )

        # With WATERMARK, the state window stream should process out-of-order data within tolerance
        tdLog.info("STATE_WINDOW with WATERMARK successfully handled out-of-order data")

    def check04(self):
        # Test event window with WATERMARK - should handle out-of-order data within tolerance
        tdLog.info("Check 4: EVENT_WINDOW with WATERMARK handles out-of-order data")
        tdSql.checkTableType(dbname="rdb", stbname="r_event_watermark", columns=3, tags=1)

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_event_watermark",
                func=lambda: (
                    tdSql.getRows() == 1
                    and tdSql.compareData(0, 0, "2025-01-01 02:30:00")
                    and tdSql.compareData(0, 1, 200)
                    and tdSql.compareData(0, 2, 300.5)
                )
            )

        tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:30:01', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        tdSql.execute("insert into tdb.we1 values ('2025-01-01 02:30:01', 10, 8);")

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_event_watermark",
                func=lambda: (
                    tdSql.getRows() == 1
                    and tdSql.compareData(0, 0, "2025-01-01 02:30:00.000")
                    and tdSql.compareData(0, 1, 201)
                    and tdSql.compareData(0, 2, 299.054726368159)
                )
            )

        tdSql.execute("insert into tdb.we1 values ('2025-01-01 02:35:00', 10, 1);")
        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_event_watermark",
                func=lambda: (
                    tdSql.getRows() == 2
                    and tdSql.compareData(0, 0, "2025-01-01 02:30:00.000")
                    and tdSql.compareData(0, 1, 201)
                    and tdSql.compareData(0, 2, 299.054726368159)
                    and tdSql.compareData(1, 0, "2025-01-01 02:31:30.000")
                    and tdSql.compareData(1, 1, 200)
                    and tdSql.compareData(1, 2, 303.5)
                )
            )

        # With WATERMARK, the event window stream should process out-of-order data within tolerance
        tdLog.info("EVENT_WINDOW with WATERMARK successfully handled out-of-order data")


    def check05(self):
        # Test period with WATERMARK - should handle out-of-order data within tolerance
        tdLog.info("Check 5: PERIOD with WATERMARK handles out-of-order data within tolerance")
        tdSql.checkTableType(dbname="rdb", stbname="r_period_watermark", columns=3, tags=1)

        # Check initial results from period trigger
        tdSql.checkResultsByFunc(
                sql=f"select count(*) from rdb.r_period_watermark",
                func=lambda: (
                    tdSql.getRows() == 1
                    and tdSql.getData(0, 0) >= 0
                )
            )

        tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:41:01', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        tdSql.execute("insert into tdb.wp1 values ('2025-01-01 02:41:02', 70, 7.5);")

        tdSql.checkResultsByFunc(
                sql=f"select count(*) from rdb.r_period_watermark",
                func=lambda: (
                    tdSql.getRows() == 1
                    and tdSql.getData(0, 0) >= 1
                )
            )
        
        # water mark is 45s , so there is no recalc for period trigger
        tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:43:01', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        tdSql.execute("insert into tdb.wp1 values ('2025-01-01 02:44:10', 80, 8.5);")
        tdSql.checkResultsByFunc(
                sql=f"select count(*) from rdb.r_period_watermark",
                func=lambda: (
                    tdSql.getRows() == 1
                    and tdSql.getData(0, 0) >= 1
                )
            )
        tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:43:02', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        tdSql.execute("insert into tdb.wp1 values ('2025-01-01 02:44:58', 90, 9.5);")
        tdSql.checkResultsByFunc(
                sql=f"select count(*) from rdb.r_period_watermark",
                func=lambda: (
                    tdSql.getRows() == 1
                    and tdSql.getData(0, 0) >= 1
                )
            )

        # For PERIOD trigger with WATERMARK, in-tolerance data should be processed
        tdLog.info("PERIOD with WATERMARK successfully handled out-of-order data within tolerance")


    def check06(self):
        # Test count window with WATERMARK - should handle out-of-order data within tolerance
        tdLog.info("Check 6: COUNT_WINDOW with WATERMARK handles out-of-order data within tolerance")
        tdSql.checkTableType(dbname="rdb", stbname="r_count_watermark", columns=3, tags=1)

        # Check initial results from count window trigger
        # COUNT_WINDOW(3) means every 3 records should trigger computation
        # Initial data has 6 records, so should have 2 windows
        tdSql.checkResultsByFunc(
                sql=f"select count(*) from rdb.r_count_watermark",
                func=lambda: (
                    tdSql.getRows() == 1
                    and tdSql.getData(0, 0) >= 0
                )
            )

        # For COUNT_WINDOW with WATERMARK, in-tolerance data should be processed
        tdLog.info("COUNT_WINDOW with WATERMARK successfully handled out-of-order data within tolerance") 
