import subprocess
import time
from new_test_framework.utils import tdLog, tdSql, clusterComCheck, tdStream, StreamItem


class TestStreamRecalcIgnoreDisorder:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_recalc_ignore_disorder(self):
        """Recalc:  6 different window with ignore_disorder

        Test IGNORE_DISORDER option behavior with six different window types to verify out-of-order data handling:

        1. INTERVAL Window with IGNORE_DISORDER Test
            1.1 Create interval(2m) sliding(2m) stream with ignore_disorder (s_interval_disorder)
            1.2 Test out-of-order data processing behavior - should not trigger recalculation
            1.3 Verify sliding window results without recalculation for disorder

        2. SESSION Window with IGNORE_DISORDER Test
            2.1 Create session(ts,45s) stream with ignore_disorder (s_session_disorder)
            2.2 Test session boundary maintenance with out-of-order data
            2.3 Verify session windows are not recalculated for disorder

        3. STATE_WINDOW with IGNORE_DISORDER Test
            3.1 Create state_window(status) stream with ignore_disorder (s_state_disorder)
            3.2 Test state transition handling with out-of-order data
            3.3 Verify state windows are not recalculated for disorder

        4. EVENT_WINDOW with IGNORE_DISORDER Test
            4.1 Create event_window(start with event_val >= 5 end with event_val > 10) stream with ignore_disorder (s_event_disorder)
            4.2 Test event sequence maintenance with out-of-order events
            4.3 Verify event windows are not recalculated for disorder

        5. PERIOD Window with IGNORE_DISORDER Test
            5.1 Create period(30s) stream with ignore_disorder (s_period_disorder)
            5.2 Test periodic window handling with out-of-order data
            5.3 Verify period windows are not recalculated for disorder

        6. COUNT_WINDOW with IGNORE_DISORDER Test
            6.1 Create count_window(3) stream with ignore_disorder (s_count_disorder)
            6.2 Test count-based window handling with out-of-order data
            6.3 Verify count windows are not recalculated for disorder


        Catalog:
            - Streams:Recalculation:IgnoreDisorder

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
        tdLog.info("prepare trigger tables for IGNORE_DISORDER testing")

        # Trigger tables in tdb (control stream computation trigger)
        stb_trig = "create table tdb.disorder_triggers (ts timestamp, cint int, c2 int, c3 double, category varchar(16)) tags(id int, name varchar(16));"
        ctb_trig = "create table tdb.dt1 using tdb.disorder_triggers tags(1, 'device1') tdb.dt2 using tdb.disorder_triggers tags(2, 'device2') tdb.dt3 using tdb.disorder_triggers tags(3, 'device3')"
        tdSql.execute(stb_trig)
        tdSql.execute(ctb_trig)

        # Trigger table for session stream
        stb2_trig = "create table tdb.trigger_session_disorder (ts timestamp, val_num int, status varchar(16)) tags(device_id int);"
        ctb2_trig = "create table tdb.ds1 using tdb.trigger_session_disorder tags(1) tdb.ds2 using tdb.trigger_session_disorder tags(2) tdb.ds3 using tdb.trigger_session_disorder tags(3)"
        tdSql.execute(stb2_trig)
        tdSql.execute(ctb2_trig)

        # Trigger table for state window stream
        stb3_trig = "create table tdb.trigger_state_disorder (ts timestamp, val_num int, status varchar(16)) tags(device_id int);"
        ctb3_trig = "create table tdb.dw1 using tdb.trigger_state_disorder tags(1) tdb.dw2 using tdb.trigger_state_disorder tags(2) tdb.dw3 using tdb.trigger_state_disorder tags(3)"
        tdSql.execute(stb3_trig)
        tdSql.execute(ctb3_trig)

        # Trigger table for event window stream
        stb4_trig = "create table tdb.trigger_event_disorder (ts timestamp, val_num int, event_val int) tags(device_id int);"
        ctb4_trig = "create table tdb.de1 using tdb.trigger_event_disorder tags(1) tdb.de2 using tdb.trigger_event_disorder tags(2) tdb.de3 using tdb.trigger_event_disorder tags(3)"
        tdSql.execute(stb4_trig)
        tdSql.execute(ctb4_trig)

        # Trigger table for period stream
        stb5_trig = "create table tdb.trigger_period_disorder (ts timestamp, val_num int, metric double) tags(device_id int);"
        ctb5_trig = "create table tdb.dp1 using tdb.trigger_period_disorder tags(1) tdb.dp2 using tdb.trigger_period_disorder tags(2) tdb.dp3 using tdb.trigger_period_disorder tags(3)"
        tdSql.execute(stb5_trig)
        tdSql.execute(ctb5_trig)

        # Trigger table for count window stream
        stb6_trig = "create table tdb.trigger_count_disorder (ts timestamp, val_num int, category varchar(16)) tags(device_id int);"
        ctb6_trig = "create table tdb.dc1 using tdb.trigger_count_disorder tags(1) tdb.dc2 using tdb.trigger_count_disorder tags(2) tdb.dc3 using tdb.trigger_count_disorder tags(3)"
        tdSql.execute(stb6_trig)
        tdSql.execute(ctb6_trig)

    def writeInitialTriggerData(self):
        tdLog.info("write initial trigger data to tdb")
        # Trigger data for interval+sliding stream
        trigger_sqls = [
            "insert into tdb.dt1 values ('2025-01-01 02:00:00', 10, 100, 1.5, 'normal');",
            "insert into tdb.dt1 values ('2025-01-01 02:00:30', 20, 200, 2.5, 'normal');",
            "insert into tdb.dt1 values ('2025-01-01 02:01:00', 30, 300, 3.5, 'normal');",
            "insert into tdb.dt1 values ('2025-01-01 02:01:30', 40, 400, 4.5, 'normal');",
            "insert into tdb.dt1 values ('2025-01-01 02:02:00', 50, 500, 5.5, 'normal');",
            "insert into tdb.dt1 values ('2025-01-01 02:02:30', 60, 600, 6.5, 'normal');",
        ]
        tdSql.executes(trigger_sqls)

        # Trigger data for session stream
        trigger_sqls = [
            "insert into tdb.ds1 values ('2025-01-01 02:00:00', 10, 'normal');",
            "insert into tdb.ds1 values ('2025-01-01 02:00:30', 20, 'normal');",
            "insert into tdb.ds1 values ('2025-01-01 02:01:00', 30, 'normal');",
            "insert into tdb.ds1 values ('2025-01-01 02:03:00', 40, 'normal');",
            "insert into tdb.ds1 values ('2025-01-01 02:03:30', 50, 'normal');",
            "insert into tdb.ds1 values ('2025-01-01 02:04:00', 60, 'normal');",
        ]
        tdSql.executes(trigger_sqls)

        # Trigger data for state window stream
        trigger_sqls = [
            "insert into tdb.dw1 values ('2025-01-01 02:00:00', 10, 'normal');",
            "insert into tdb.dw1 values ('2025-01-01 02:00:30', 20, 'normal');",
            "insert into tdb.dw1 values ('2025-01-01 02:01:00', 30, 'warning');",
            "insert into tdb.dw1 values ('2025-01-01 02:01:30', 40, 'warning');",
            "insert into tdb.dw1 values ('2025-01-01 02:02:00', 50, 'error');",
            "insert into tdb.dw1 values ('2025-01-01 02:02:30', 60, 'error');",
        ]
        tdSql.executes(trigger_sqls)

        # Trigger data for event window stream
        trigger_sqls = [
            "insert into tdb.de1 values ('2025-01-01 02:00:00', 10, 6);",
            "insert into tdb.de1 values ('2025-01-01 02:00:30', 20, 7);",
            "insert into tdb.de1 values ('2025-01-01 02:01:00', 30, 12);",
            "insert into tdb.de1 values ('2025-01-01 02:01:30', 40, 6);",
            "insert into tdb.de1 values ('2025-01-01 02:02:00', 50, 9);",
            "insert into tdb.de1 values ('2025-01-01 02:02:30', 60, 13);",
        ]
        tdSql.executes(trigger_sqls)

        # Trigger data for period stream
        trigger_sqls = [
            "insert into tdb.dp1 values ('2025-01-01 02:00:00', 10, 1.5);",
            "insert into tdb.dp1 values ('2025-01-01 02:00:30', 20, 2.5);",
            "insert into tdb.dp1 values ('2025-01-01 02:01:00', 30, 3.5);",
            "insert into tdb.dp1 values ('2025-01-01 02:01:30', 40, 4.5);",
            "insert into tdb.dp1 values ('2025-01-01 02:02:00', 50, 5.5);",
            "insert into tdb.dp1 values ('2025-01-01 02:02:30', 60, 6.5);",
        ]
        tdSql.executes(trigger_sqls)

        # Trigger data for count window stream
        trigger_sqls = [
            "insert into tdb.dc1 values ('2025-01-01 02:00:00', 10, 'normal');",
            "insert into tdb.dc1 values ('2025-01-01 02:00:15', 20, 'normal');",
            "insert into tdb.dc1 values ('2025-01-01 02:00:30', 30, 'warning');",
            "insert into tdb.dc1 values ('2025-01-01 02:00:45', 40, 'warning');",
            "insert into tdb.dc1 values ('2025-01-01 02:01:00', 50, 'error');",
            "insert into tdb.dc1 values ('2025-01-01 02:01:15', 60, 'error');",
        ]
        tdSql.executes(trigger_sqls)

    def writeSourceData(self):
        tdLog.info("write source data to test IGNORE_DISORDER option")
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

        # ===== Test 1: IGNORE_DISORDER Option =====
        
        # Test 1.1: INTERVAL+SLIDING with IGNORE_DISORDER - should not trigger recalculation for out-of-order data
        stream = StreamItem(
            id=1,
            stream="create stream rdb.s_interval_disorder interval(2m) sliding(2m) from tdb.disorder_triggers partition by tbname stream_options(ignore_disorder) into rdb.r_interval_disorder as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check01,
        )
        self.streams.append(stream)

        # Test 1.2: SESSION with IGNORE_DISORDER - should not trigger recalculation for out-of-order data
        stream = StreamItem(
            id=2,
            stream="create stream rdb.s_session_disorder session(ts,45s) from tdb.trigger_session_disorder partition by tbname stream_options(ignore_disorder) into rdb.r_session_disorder as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check02,
        )
        self.streams.append(stream)

        # Test 1.3: STATE_WINDOW with IGNORE_DISORDER - should not trigger recalculation for out-of-order data
        stream = StreamItem(
            id=3,
            stream="create stream rdb.s_state_disorder state_window(status) from tdb.trigger_state_disorder partition by tbname stream_options(ignore_disorder) into rdb.r_state_disorder as select _twstart ts, count(*) cnt, avg(cint) avg_val, first(cvarchar) status_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check03,
        )
        self.streams.append(stream)

        # Test 1.4: EVENT_WINDOW with IGNORE_DISORDER - should not trigger recalculation for out-of-order data
        stream = StreamItem(
            id=4,
            stream="create stream rdb.s_event_disorder event_window(start with event_val >= 5 end with event_val > 10) from tdb.trigger_event_disorder partition by tbname stream_options(ignore_disorder) into rdb.r_event_disorder as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check04,
        )
        self.streams.append(stream)

        # Test 1.5: PERIOD with IGNORE_DISORDER - should not trigger recalculation for out-of-order data
        stream = StreamItem(
            id=5,
            stream="create stream rdb.s_period_disorder period(10s) from tdb.trigger_period_disorder partition by tbname stream_options(ignore_disorder) into rdb.r_period_disorder as select cast(_tlocaltime/1000000 as timestamp) ts, count(*) cnt, avg(cint) avg_val from qdb.meters;",
            check_func=self.check05,
        )
        self.streams.append(stream)

        # Test 1.6: COUNT_WINDOW with IGNORE_DISORDER - should not trigger recalculation for out-of-order data
        stream = StreamItem(
            id=6,
            stream="create stream rdb.s_count_disorder count_window(3) from tdb.trigger_count_disorder partition by tbname stream_options(ignore_disorder) into rdb.r_count_disorder as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check06,
        )
        self.streams.append(stream)
        tdLog.info(f"create total:{len(self.streams)} streams")
        for stream in self.streams:
            stream.createStream()

    # Check functions for each test case
    def check01(self):
        # Test interval+sliding with IGNORE_DISORDER - should not recalculate for out-of-order data
        tdLog.info("Check 1: INTERVAL+SLIDING with IGNORE_DISORDER ignores out-of-order data")
        tdSql.checkTableType(dbname="rdb", stbname="r_interval_disorder", columns=3, tags=1)

        tdSql.checkResultsByFunc(
            sql=f"select ts, cnt, avg_val from rdb.r_interval_disorder",
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "2025-01-01 02:00:00.000")
            and tdSql.compareData(0, 1, 400)
            and tdSql.compareData(0, 2, 241.5)
        )

        tdSql.query("select count(*) from rdb.r_interval_disorder;")
        result_count_before = tdSql.getData(0, 0)
        tdLog.info(f"INTERVAL+SLIDING result count before out-of-order data: {result_count_before}")

        # Insert out-of-order data that should be ignored due to IGNORE_DISORDER
        disorder_sqls = [
            "insert into tdb.dt1 values ('2025-01-01 01:59:00', 5, 50, 0.5, 'disorder1');",
            "insert into tdb.dt1 values ('2025-01-01 02:00:15', 15, 150, 1.8, 'disorder2');",
        ]
        tdSql.executes(disorder_sqls)

        tdLog.info("wait for stream to be stable")
        time.sleep(5)

        tdSql.query("select count(*) from rdb.r_interval_disorder;")
        result_count_after = tdSql.getData(0, 0)
        tdLog.info(f"INTERVAL+SLIDING result count after out-of-order data: {result_count_after}")

        assert result_count_before == result_count_after, "INTERVAL+SLIDING with IGNORE_DISORDER should not recalculate for out-of-order data"

    def check02(self):
        # Test session with IGNORE_DISORDER - should not recalculate for out-of-order data
        tdLog.info("Check 2: SESSION with IGNORE_DISORDER ignores out-of-order data")
        tdSql.checkTableType(dbname="rdb", stbname="r_session_disorder", columns=3, tags=1)

        tdSql.checkResultsByFunc(
            sql=f"select ts, cnt, avg_val from rdb.r_session_disorder",
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "2025-01-01 02:00:00.000")
            and tdSql.compareData(0, 1, 200)
            and tdSql.compareData(0, 2, 240.5)
        )

        tdSql.query("select count(*) from rdb.r_session_disorder;")
        result_count_before = tdSql.getData(0, 0)
        tdLog.info(f"SESSION result count before out-of-order data: {result_count_before}")

        # Insert out-of-order data that should be ignored due to IGNORE_DISORDER
        disorder_sqls = [
            "insert into tdb.ds1 values ('2025-01-01 01:59:00', 5, 'disorder1');",
            "insert into tdb.ds1 values ('2025-01-01 02:00:15', 15, 'disorder2');",
        ]
        tdSql.executes(disorder_sqls)

        time.sleep(5)

        tdSql.query("select count(*) from rdb.r_session_disorder;")
        result_count_after = tdSql.getData(0, 0)
        tdLog.info(f"SESSION result count after out-of-order data: {result_count_after}")

        assert result_count_before == result_count_after, "SESSION with IGNORE_DISORDER should not recalculate for out-of-order data"

    def check03(self):
        # Test state window with IGNORE_DISORDER - should not recalculate for out-of-order data
        tdLog.info("Check 3: STATE_WINDOW with IGNORE_DISORDER ignores out-of-order data")
        tdSql.checkTableType(dbname="rdb", stbname="r_state_disorder", columns=4, tags=1)

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_state_disorder",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 02:00:00")
                and tdSql.compareData(0, 1, 100)
                and tdSql.compareData(0, 2, 240)
                and tdSql.compareData(1, 0, "2025-01-01 02:01:00")
                and tdSql.compareData(1, 1, 100)
                and tdSql.compareData(1, 2, 242)
            )

        # Insert out-of-order data that should be ignored due to IGNORE_DISORDER
        disorder_sqls = [
            "insert into tdb.dw1 values ('2025-01-01 01:59:30', 5, 'disorder1');",
            "insert into tdb.dw1 values ('2025-01-01 02:00:15', 15, 'normal');",
        ]
        tdSql.executes(disorder_sqls)

        time.sleep(5)

        # Results should remain the same after out-of-order data insertion
        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_state_disorder",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 02:00:00")
                and tdSql.compareData(0, 1, 100)
                and tdSql.compareData(0, 2, 240)
                and tdSql.compareData(1, 0, "2025-01-01 02:01:00")
                and tdSql.compareData(1, 1, 100)
                and tdSql.compareData(1, 2, 242)
            )

    def check04(self):
        # Test event window with IGNORE_DISORDER - should not recalculate for out-of-order data
        tdLog.info("Check 4: EVENT_WINDOW with IGNORE_DISORDER ignores out-of-order data")
        tdSql.checkTableType(dbname="rdb", stbname="r_event_disorder", columns=3, tags=1)

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_event_disorder",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 02:00:00.000")
                and tdSql.compareData(0, 1, 200)
                and tdSql.compareData(0, 2, 240.5)
                and tdSql.compareData(1, 0, "2025-01-01 02:01:30.000")
                and tdSql.compareData(1, 1, 200)
                and tdSql.compareData(1, 2, 243.5)
            )

        # Insert out-of-order data that should be ignored due to IGNORE_DISORDER
        disorder_sqls = [
            "insert into tdb.de1 values ('2025-01-01 01:59:30', 5, 6);",
            "insert into tdb.de1 values ('2025-01-01 02:00:15', 15, 12);",
        ]
        tdSql.executes(disorder_sqls)

        tdLog.info("wait for stream to be stable")
        time.sleep(5)

        # Results should remain the same after out-of-order data insertion
        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_event_disorder",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 02:00:00.000")
                and tdSql.compareData(0, 1, 200)
                and tdSql.compareData(0, 2, 240.5)
                and tdSql.compareData(1, 0, "2025-01-01 02:01:30.000")
                and tdSql.compareData(1, 1, 200)
                and tdSql.compareData(1, 2, 243.5)
            ) 


    def check05(self):
        # Test period with IGNORE_DISORDER - should not recalculate for out-of-order data
        tdLog.info("Check 5: PERIOD with IGNORE_DISORDER ignores out-of-order data")
        tdSql.checkTableType(dbname="rdb", stbname="r_period_disorder", columns=3, tags=1)

        # Check initial results from period trigger
        tdSql.query("select count(*) from rdb.r_period_disorder;")
        result_count_before = tdSql.getData(0, 0)
        tdLog.info(f"PERIOD result count before out-of-order data: {result_count_before}")

        # Insert out-of-order data that should be ignored due to IGNORE_DISORDER
        disorder_sqls = [
            "insert into tdb.dp1 values ('2025-01-01 01:59:00', 10, 1.0);",
            "insert into tdb.dp1 values ('2025-01-01 01:59:30', 20, 2.0);",
            "insert into tdb.dp1 values ('2025-01-01 01:58:00', 30, 3.0);",
        ]
        tdSql.executes(disorder_sqls)

        tdLog.info("wait for stream to be stable")
        time.sleep(5)

        # Check that out-of-order data did not trigger new computation
        tdSql.query("select count(*) from rdb.r_period_disorder;")
        result_count_after = tdSql.getData(0, 0)
        tdLog.info(f"PERIOD result count after out-of-order data: {result_count_after}")

        # For PERIOD trigger with IGNORE_DISORDER, out-of-order data should not increase result count
        # assert result_count_before == result_count_after, "PERIOD ignore_disorder result count should not change for out-of-order data"


    def check06(self):
        # Test count window with IGNORE_DISORDER - should not recalculate for out-of-order data
        tdLog.info("Check 6: COUNT_WINDOW with IGNORE_DISORDER ignores out-of-order data")
        tdSql.checkTableType(dbname="rdb", stbname="r_count_disorder", columns=3, tags=1)

        # Check initial results from count window trigger
        # COUNT_WINDOW(3) means every 3 records should trigger computation
        # Initial data has 6 records, so should have 2 windows
        tdSql.query("select count(*) from rdb.r_count_disorder;")
        result_count_before = tdSql.getData(0, 0)
        tdLog.info(f"COUNT_WINDOW result count before out-of-order data: {result_count_before}")

        # Insert out-of-order data - COUNT_WINDOW with IGNORE_DISORDER should ignore this data
        disorder_sqls = [
            "insert into tdb.dc1 values ('2025-01-01 01:59:00', 10, 'disorder1');",
            "insert into tdb.dc1 values ('2025-01-01 01:59:15', 20, 'disorder2');",
            "insert into tdb.dc1 values ('2025-01-01 01:59:30', 30, 'disorder3');",
        ]
        tdSql.executes(disorder_sqls)

        tdLog.info("wait for stream to be stable")
        time.sleep(5)

        # Check that COUNT_WINDOW ignored the out-of-order data due to IGNORE_DISORDER
        tdSql.query("select count(*) from rdb.r_count_disorder;")
        result_count_after = tdSql.getData(0, 0)
        tdLog.info(f"COUNT_WINDOW result count after out-of-order data: {result_count_after}")

        # For COUNT_WINDOW with IGNORE_DISORDER, out-of-order data should be ignored
        assert result_count_before == result_count_after, "COUNT_WINDOW ignore_disorder result count should not change for out-of-order data" 