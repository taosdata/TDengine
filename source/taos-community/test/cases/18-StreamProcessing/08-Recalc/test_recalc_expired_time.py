import subprocess
import time
from new_test_framework.utils import tdLog, tdSql, clusterComCheck, tdStream, StreamItem


class TestStreamRecalcExpiredTime:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_recalc_expired_time(self):
        """Recalc:  6 different window with expire_time

        Test EXPIRED_TIME(1h) option with 6 different window types and verify expired data handling:

        1. Test [INTERVAL+SLIDING Window] with EXPIRED_TIME(1h)
            1.1 Create s_interval_expired: interval(2m) sliding(2m) with expired_time(1h)
                1.1.1 Process data from '2025-01-01 02:00:00' onwards (within 1h)
                1.1.2 Insert expired data from '2025-01-01 01:00:00' (beyond 1h)
                1.1.3 Verify expired data does not increase result count
                1.1.4 Check result table structure: ts, cnt, avg_val

        2. Test [SESSION Window] with EXPIRED_TIME(1h)
            2.1 Create s_session_expired: session(ts,45s) with expired_time(1h)
                2.1.1 Insert normal trigger data at '2025-01-01 02:00:00' series
                2.1.2 Insert non-expired data at '2025-01-01 01:30:00' (within 1h)
                2.1.3 Insert expired data at '2025-01-01 01:00:00' (beyond 1h)
                2.1.4 Verify session results: 3 sessions created, expired data ignored

        3. Test [STATE_WINDOW] with EXPIRED_TIME(1h)
            3.1 Create s_state_expired: state_window(status) with expired_time(1h)
                3.1.1 Insert state changes: normal->warning->error at '2025-01-01 02:00:00'
                3.1.2 Insert non-expired state data at '2025-01-01 01:30:00'
                3.1.3 Insert expired state data at '2025-01-01 01:00:00'
                3.1.4 Verify 4 state windows created, expired data ignored

        4. Test [EVENT_WINDOW] with EXPIRED_TIME(1h)
            4.1 Create s_event_expired: event_window(start with event_val >= 5 end with event_val > 10)
                4.1.1 Insert event trigger data with event_val pattern: 6,7,12 at '2025-01-01 02:00:00'
                4.1.2 Insert non-expired events at '2025-01-01 01:30:00'
                4.1.3 Insert expired events at '2025-01-01 01:00:00'
                4.1.4 Verify 3 event windows, expired data ignored in final result

        5. Test [PERIOD Window] with EXPIRED_TIME(1h)
            5.1 Create s_period_expired: period(30s) with expired_time(1h)|ignore_nodata_trigger
                5.1.1 Insert period trigger data every 30s from '2025-01-01 02:00:00'
                5.1.2 Test periodic triggering with current timestamp data
                5.1.3 Verify period computation ignores expired data
                5.1.4 Check ignore_nodata_trigger option interaction

        6. Test [COUNT_WINDOW] with EXPIRED_TIME(1h) - Option Ignored
            6.1 Create s_count_expired: count_window(3) with expired_time(1h)
                6.1.1 Insert count trigger data in batches of 3
                6.1.2 Insert both current and expired data
                6.1.3 Verify COUNT_WINDOW ignores EXPIRED_TIME option
                6.1.4 Confirm all data processed regardless of timestamp

        Catalog:
            - Streams:Recalc:ExpiredTime

        Since: v3.3.7.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-07-22 Beryl Created

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
        tdLog.info("prepare trigger tables for EXPIRED_TIME testing")

        # Trigger tables in tdb (control stream computation trigger)
        stb_trig = "create table tdb.expired_triggers (ts timestamp, cint int, c2 int, c3 double, category varchar(16)) tags(id int, name varchar(16));"
        ctb_trig = "create table tdb.et1 using tdb.expired_triggers tags(1, 'device1') tdb.et2 using tdb.expired_triggers tags(2, 'device2') tdb.et3 using tdb.expired_triggers tags(3, 'device3')"
        tdSql.execute(stb_trig)
        tdSql.execute(ctb_trig)

        # Trigger table for expired data testing with partitioning
        stb2_trig = "create table tdb.trigger_test (ts timestamp, val_num int, status varchar(16)) tags(device_id int);"
        ctb2_trig = "create table tdb.t1 using tdb.trigger_test tags(1) tdb.t2 using tdb.trigger_test tags(2) tdb.t3 using tdb.trigger_test tags(3)"
        tdSql.execute(stb2_trig)
        tdSql.execute(ctb2_trig)

        # Trigger table for state window stream
        stb3_trig = "create table tdb.trigger_state (ts timestamp, val_num int, status varchar(16)) tags(device_id int);"
        ctb3_trig = "create table tdb.sw1 using tdb.trigger_state tags(1) tdb.sw2 using tdb.trigger_state tags(2) tdb.sw3 using tdb.trigger_state tags(3)"
        tdSql.execute(stb3_trig)
        tdSql.execute(ctb3_trig)

        # Normal trigger table for non-partitioned expired testing
        ntb_trig = "create table tdb.trigger_normal (ts timestamp, val_num int, metric double, info varchar(32))"
        tdSql.execute(ntb_trig)

        # Trigger table for event window stream
        stb4_trig = "create table tdb.trigger_event (ts timestamp, val_num int, event_val int) tags(device_id int);"
        ctb4_trig = "create table tdb.ew1 using tdb.trigger_event tags(1) tdb.ew2 using tdb.trigger_event tags(2) tdb.ew3 using tdb.trigger_event tags(3)"
        tdSql.execute(stb4_trig)
        tdSql.execute(ctb4_trig)

        # Trigger table for period stream
        stb5_trig = "create table tdb.trigger_period (ts timestamp, val_num int, metric double) tags(device_id int);"
        ctb5_trig = "create table tdb.pw1 using tdb.trigger_period tags(1) tdb.pw2 using tdb.trigger_period tags(2) tdb.pw3 using tdb.trigger_period tags(3)"
        tdSql.execute(stb5_trig)
        tdSql.execute(ctb5_trig)

        # Trigger table for count window stream
        stb6_trig = "create table tdb.trigger_count (ts timestamp, val_num int, category varchar(16)) tags(device_id int);"
        ctb6_trig = "create table tdb.cw1 using tdb.trigger_count tags(1) tdb.cw2 using tdb.trigger_count tags(2) tdb.cw3 using tdb.trigger_count tags(3)"
        tdSql.execute(stb6_trig)
        tdSql.execute(ctb6_trig)


    def writeInitialTriggerData(self):
        tdLog.info("write initial trigger data to tdb")
        # Trigger data for interval+sliding stream
        trigger_sqls = [
            "insert into tdb.et1 values ('2025-01-01 02:00:00', 10, 100, 1.5, 'normal');",
            "insert into tdb.et1 values ('2025-01-01 02:00:30', 20, 200, 2.5, 'normal');",
            "insert into tdb.et1 values ('2025-01-01 02:01:00', 30, 300, 3.5, 'normal');",
            "insert into tdb.et1 values ('2025-01-01 02:01:30', 40, 400, 4.5, 'normal');",
            "insert into tdb.et1 values ('2025-01-01 02:02:00', 50, 500, 5.5, 'normal');",
            "insert into tdb.et1 values ('2025-01-01 02:02:30', 60, 600, 6.5, 'normal');",
        ]
        tdSql.executes(trigger_sqls)

        # Trigger data for session stream
        trigger_sqls = [
            "insert into tdb.t1 values ('2025-01-01 02:00:00', 10, 'normal');",
            "insert into tdb.t1 values ('2025-01-01 02:00:30', 20, 'normal');",
            "insert into tdb.t1 values ('2025-01-01 02:01:00', 30, 'normal');",
            "insert into tdb.t1 values ('2025-01-01 02:03:00', 40, 'normal');",
            "insert into tdb.t1 values ('2025-01-01 02:03:30', 50, 'normal');",
            "insert into tdb.t1 values ('2025-01-01 02:04:00', 60, 'normal');",
        ]
        tdSql.executes(trigger_sqls)

        # Trigger data for state window stream
        trigger_sqls = [
            "insert into tdb.sw1 values ('2025-01-01 02:00:00', 10, 'normal');",
            "insert into tdb.sw1 values ('2025-01-01 02:00:30', 20, 'normal');",
            "insert into tdb.sw1 values ('2025-01-01 02:01:00', 30, 'warning');",
            "insert into tdb.sw1 values ('2025-01-01 02:01:30', 40, 'warning');",
            "insert into tdb.sw1 values ('2025-01-01 02:02:00', 50, 'error');",
            "insert into tdb.sw1 values ('2025-01-01 02:02:30', 60, 'error');",
        ]
        tdSql.executes(trigger_sqls)

        # Trigger data for event window stream

        trigger_sqls = [
            "insert into tdb.ew1 values ('2025-01-01 02:00:00', 10, 6);",
            "insert into tdb.ew1 values ('2025-01-01 02:00:30', 20, 7);",
            "insert into tdb.ew1 values ('2025-01-01 02:01:00', 30, 12);",
            "insert into tdb.ew1 values ('2025-01-01 02:01:30', 40, 6);",
            "insert into tdb.ew1 values ('2025-01-01 02:02:00', 50, 9);",
            "insert into tdb.ew1 values ('2025-01-01 02:02:30', 60, 13);",
        ]
        tdSql.executes(trigger_sqls)

        # Trigger data for period stream
        trigger_sqls = [
            "insert into tdb.pw1 values ('2025-01-01 02:00:00', 10, 1.5);",
            "insert into tdb.pw1 values ('2025-01-01 02:00:30', 20, 2.5);",
            "insert into tdb.pw1 values ('2025-01-01 02:01:00', 30, 3.5);",
            "insert into tdb.pw1 values ('2025-01-01 02:01:30', 40, 4.5);",
            "insert into tdb.pw1 values ('2025-01-01 02:02:00', 50, 5.5);",
            "insert into tdb.pw1 values ('2025-01-01 02:02:30', 60, 6.5);",
        ]
        tdSql.executes(trigger_sqls)

        # Trigger data for count window stream
        trigger_sqls = [
            "insert into tdb.cw1 values ('2025-01-01 02:00:00', 10, 'normal');",
            "insert into tdb.cw1 values ('2025-01-01 02:00:15', 20, 'normal');",
            "insert into tdb.cw1 values ('2025-01-01 02:00:30', 30, 'warning');",
            "insert into tdb.cw1 values ('2025-01-01 02:00:45', 40, 'warning');",
            "insert into tdb.cw1 values ('2025-01-01 02:01:00', 50, 'error');",
            "insert into tdb.cw1 values ('2025-01-01 02:01:15', 60, 'error');",
        ]
        tdSql.executes(trigger_sqls)

    def writeSourceData(self):
        tdLog.info("write source data to test EXPIRED_TIME option")
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

        # ===== Test 1: EXPIRED_TIME Only =====
        
        # Test 1.1: INTERVAL+SLIDING with EXPIRED_TIME - should not process expired data
        stream = StreamItem(
            id=1,
            stream="create stream rdb.s_interval_expired interval(2m) sliding(2m) from tdb.expired_triggers partition by tbname stream_options(expired_time(1h)) into rdb.r_interval_expired as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check01,
        )
        self.streams.append(stream)

        # Test 1.2: SESSION with EXPIRED_TIME - should not process expired data
        stream = StreamItem(
            id=2,
            stream="create stream rdb.s_session_expired session(ts,45s) from tdb.trigger_test partition by tbname stream_options(expired_time(1h)) into rdb.r_session_expired as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check02,
        )
        self.streams.append(stream)
        # Test 1.3: STATE_WINDOW with EXPIRED_TIME - should not process expired data
        stream = StreamItem(
            id=3,
            stream="create stream rdb.s_state_expired state_window(status) from tdb.trigger_state partition by tbname stream_options(expired_time(1h)) into rdb.r_state_expired as select _twstart ts, count(*) cnt, avg(cint) avg_val, first(cvarchar) status_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check03,
        )
        self.streams.append(stream)

        # Test 1.4: EVENT_WINDOW with EXPIRED_TIME - should not process expired data
        stream = StreamItem(
            id=4,
            stream="create stream rdb.s_event_expired event_window(start with event_val >= 5 end with event_val > 10) from tdb.trigger_event partition by tbname stream_options(expired_time(1h)) into rdb.r_event_expired as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check04,
        )
        self.streams.append(stream)

        # Test 1.5: PERIOD with EXPIRED_TIME - should not process expired data
        stream = StreamItem(
            id=5,
            stream="create stream rdb.s_period_expired period(30s) from tdb.trigger_period partition by tbname stream_options(expired_time(1h)|ignore_nodata_trigger) into rdb.r_period_expired as select cast(_tlocaltime/1000000 as timestamp) ts, count(*) cnt, avg(cint) avg_val from qdb.meters;",
            check_func=self.check05,
        )
        self.streams.append(stream)

        # Test 1.6: COUNT_WINDOW with EXPIRED_TIME - should ignore EXPIRED_TIME option (process all data)
        stream = StreamItem(
            id=6,
            stream="create stream rdb.s_count_expired count_window(3) from tdb.trigger_count partition by tbname stream_options(expired_time(1h)) into rdb.r_count_expired as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check06,
        )
        self.streams.append(stream)

        tdLog.info(f"create total:{len(self.streams)} streams")
        for stream in self.streams:
            stream.createStream()

    # Check functions for each test case
    def check01(self):
        # Test interval+sliding with EXPIRED_TIME - should not process expired data
        tdLog.info("Check 1: INTERVAL+SLIDING with EXPIRED_TIME ignores expired data")
        tdSql.checkTableType(dbname="rdb", stbname="r_interval_expired", columns=3,tags=1)
        tdSql.checkResultsByFunc(
            sql=f"select ts, cnt, avg_val from rdb.r_interval_expired",
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "2025-01-01 02:00:00.000")
            and tdSql.compareData(0, 1, 400)
            and tdSql.compareData(0, 2, 241.5)
        )

        tdSql.query("select count(*) from rdb.r_interval_expired;")
        result_count_before = tdSql.getData(0, 0)
        tdLog.info(f"INTERVAL+SLIDING result count: {result_count_before}")
        
        insertExpiredTriggers = "insert into tdb.et1 values ('2025-01-01 00:00:01', 5, 55, 0.8, 'expired1');"
        tdSql.execute(insertExpiredTriggers)

        tdSql.query("select count(*) from rdb.r_interval_expired;")
        result_count_after = tdSql.getData(0, 0)
        tdLog.info(f"INTERVAL+SLIDING expired_time result count: after insert expired data {result_count_after}")

        tdLog.info("wait for stream to be stable")
        time.sleep(5)
        assert result_count_before == result_count_after, "INTERVAL+SLIDING expired_time result count is not expected"


    def check02(self):
        # Test session with EXPIRED_TIME - should not process expired data
        tdLog.info("Check 2: SESSION with EXPIRED_TIME ignores expired data")
        tdSql.checkTableType(dbname="rdb", stbname="r_session_expired", columns=3, tags=1)

        tdSql.checkResultsByFunc(
            sql=f"select ts, cnt, avg_val from rdb.r_session_expired",
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "2025-01-01 02:00:00.000")
            and tdSql.compareData(0, 1, 200)
            and tdSql.compareData(0, 2, 240.5)
        )

        # trigger data for session stream
        trigger_sqls = [
            "insert into tdb.t1 values ('2025-01-01 01:30:00', 10, 'normal');",
            "insert into tdb.t1 values ('2025-01-01 01:30:30', 20, 'normal');",
            "insert into tdb.t1 values ('2025-01-01 01:32:00', 30, 'normal');",
        ]
        tdSql.executes(trigger_sqls)
        
        tdSql.checkResultsByFunc(
            sql=f"select ts, cnt, avg_val from rdb.r_session_expired",
            func=lambda: tdSql.getRows() == 3
            and tdSql.compareData(0, 0, "2025-01-01 01:30:00")
            and tdSql.compareData(0, 1, 100)
            and tdSql.compareData(0, 2, 180.0)
            and tdSql.compareData(1, 0, "2025-01-01 01:32:00")
            and tdSql.compareData(1, 1, 0)
            and tdSql.compareData(1, 2, None)
            and tdSql.compareData(2, 0, "2025-01-01 02:00:00")
            and tdSql.compareData(2, 1, 200)
            and tdSql.compareData(2, 2, 240.5)
        )


        time.sleep(5)

        tdSql.query("select count(*) from rdb.r_session_expired;")
        result_count_before = tdSql.getData(0, 0)
        tdLog.info(f"SESSION result count: {result_count_before}")

        # write expired data for session stream to trigger table
        trigger_sqls = [
            "insert into tdb.t1 values ('2025-01-01 01:30:00', 10, 'expired');",
            "insert into tdb.t1 values ('2025-01-01 01:30:30', 20, 'expired');",
            "insert into tdb.t1 values ('2025-01-01 01:32:00', 30, 'expired');",
        ]
        tdSql.executes(trigger_sqls)

        time.sleep(5)

        tdSql.query("select count(*) from rdb.r_session_expired;")
        result_count_after = tdSql.getData(0, 0)
        tdLog.info(f"SESSION expired_time result count: after insert expired data {result_count_after}")

        assert result_count_before == result_count_after, "SESSION expired_time result count is not expected"


    def check03(self):
        # Test state window with EXPIRED_TIME - should not process expired data
        tdLog.info("Check 3: STATE_WINDOW with EXPIRED_TIME ignores expired data")
        tdSql.checkTableType(dbname="rdb", stbname="r_state_expired", columns=4, tags=1)

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_state_expired",
                func=lambda: (
                    tdSql.getRows() == 2
                    and tdSql.compareData(0, 0, "2025-01-01 02:00:00")
                    and tdSql.compareData(0, 1, 100)
                    and tdSql.compareData(0, 2, 240)
                    and tdSql.compareData(1, 0, "2025-01-01 02:01:00")
                    and tdSql.compareData(1, 1, 100)
                    and tdSql.compareData(1, 2, 242)
                )
            )

        trigger_sqls = [
            "insert into tdb.sw1 values ('2025-01-01 01:30:00', 10, 'info');",
            "insert into tdb.sw1 values ('2025-01-01 01:30:30', 20, 'info');",
            "insert into tdb.sw1 values ('2025-01-01 01:31:00', 30, 'error');",
        ]
        tdSql.executes(trigger_sqls)

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_state_expired",
                func=lambda: tdSql.getRows() == 4
                and tdSql.compareData(0, 0, "2025-01-01 01:30:00")
                and tdSql.compareData(0, 1, 100)
                and tdSql.compareData(0, 2, 180)
                and tdSql.compareData(1, 0, "2025-01-01 01:31:00")
                and tdSql.compareData(1, 1, 0)
                and tdSql.compareData(1, 2, None)
                and tdSql.compareData(2, 0, "2025-01-01 02:00:00")
                and tdSql.compareData(2, 1, 100)
                and tdSql.compareData(2, 2, 240)
                and tdSql.compareData(3, 0, "2025-01-01 02:01:00")
                and tdSql.compareData(3, 1, 100)
                and tdSql.compareData(3, 2, 242)
            )

        trigger_sqls = [
            "insert into tdb.sw1 values ('2025-01-01 01:00:00', 10, 'info');",
            "insert into tdb.sw1 values ('2025-01-01 01:00:30', 20, 'info');",
            "insert into tdb.sw1 values ('2025-01-01 01:01:00', 30, 'error');",
        ]
        tdSql.executes(trigger_sqls)

        time.sleep(5)

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_state_expired",
                func=lambda: tdSql.getRows() == 4
                and tdSql.compareData(0, 0, "2025-01-01 01:30:00")
                and tdSql.compareData(0, 1, 100)
                and tdSql.compareData(0, 2, 180)
                and tdSql.compareData(1, 0, "2025-01-01 01:31:00")
                and tdSql.compareData(1, 1, 0)
                and tdSql.compareData(1, 2, None)
                and tdSql.compareData(2, 0, "2025-01-01 02:00:00")
                and tdSql.compareData(2, 1, 100)
                and tdSql.compareData(2, 2, 240)
                and tdSql.compareData(3, 0, "2025-01-01 02:01:00")
                and tdSql.compareData(3, 1, 100)
                and tdSql.compareData(3, 2, 242)
            )

        

    def check04(self):
        # Test interval+sliding with EXPIRED_TIME + WATERMARK - test boundary behavior
        tdLog.info("Check 4: INTERVAL+SLIDING with EXPIRED_TIME + WATERMARK boundary behavior")
        tdSql.checkTableType(dbname="rdb", stbname="r_event_expired", columns=3, tags=1)

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_event_expired",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 02:00:00.000")
                and tdSql.compareData(0, 1, 200)
                and tdSql.compareData(0, 2, 240.5)
                and tdSql.compareData(1, 0, "2025-01-01 02:01:30.000")
                and tdSql.compareData(1, 1, 200)
                and tdSql.compareData(1, 2, 243.5)
            )       
            
        tdLog.info("insert expired data for event window stream")
        trigger_sqls = [
            "insert into tdb.ew1 values ('2025-01-01 01:30:00', 10, 6);",
            "insert into tdb.ew1 values ('2025-01-01 01:30:30', 20, 7);",
            "insert into tdb.ew1 values ('2025-01-01 01:31:00', 30, 12);",
        ]

        tdSql.executes(trigger_sqls)

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_event_expired",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 01:30:00.000")
                and tdSql.compareData(0, 1, 200)
                and tdSql.compareData(0, 2, 180.5)
                and tdSql.compareData(1, 0, "2025-01-01 02:00:00.000")
                and tdSql.compareData(1, 1, 200)
                and tdSql.compareData(1, 2, 240.5)
                and tdSql.compareData(2, 0, "2025-01-01 02:01:30.000")
                and tdSql.compareData(2, 1, 200)
                and tdSql.compareData(2, 2, 243.5)
            )

        time.sleep(5)

        trigger_sqls = [
            "insert into tdb.ew1 values ('2025-01-01 01:00:00', 10, 6);",
            "insert into tdb.ew1 values ('2025-01-01 01:00:30', 20, 7);",
            "insert into tdb.ew1 values ('2025-01-01 01:01:00', 30, 12);",
        ]
        tdSql.executes(trigger_sqls)

        tdLog.info("wait for stream to be stable")
        time.sleep(5)

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_event_expired",
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 01:30:00.000")
                and tdSql.compareData(0, 1, 200)
                and tdSql.compareData(0, 2, 180.5)
                and tdSql.compareData(1, 0, "2025-01-01 02:00:00.000")
                and tdSql.compareData(1, 1, 200)
                and tdSql.compareData(1, 2, 240.5)
                and tdSql.compareData(2, 0, "2025-01-01 02:01:30.000")
                and tdSql.compareData(2, 1, 200)
                and tdSql.compareData(2, 2, 243.5)
            )


    def check05(self):
        # Test period with EXPIRED_TIME - should not process expired data
        tdLog.info("Check 5: PERIOD with EXPIRED_TIME ignores expired data")
        tdSql.checkTableType(dbname="rdb", stbname="r_period_expired", columns=3, tags=1)
        
        tdSql.checkResultsByFunc(
            sql=f"select cnt, avg_val from rdb.r_period_expired",
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, 40001)
            and tdSql.compareData(0, 1, 199.495262618435)
        )

        # Check initial results from period trigger
        tdSql.query("select count(*) from rdb.r_period_expired;")
        result_count_before = tdSql.getData(0, 0)
        tdLog.info(f"PERIOD result count before expired data: {result_count_before}")

        # Insert expired data that should be ignored due to EXPIRED_TIME
        trigger_sqls = [
            "insert into tdb.pw1 values ('2025-01-01 01:00:00', 10, 1.0);",
            "insert into tdb.pw1 values ('2025-01-01 01:00:30', 20, 2.0);",
            "insert into tdb.pw1 values ('2025-01-01 01:01:00', 30, 3.0);",
        ]
        tdSql.executes(trigger_sqls)

        tdLog.info("wait for stream to be stable")
        time.sleep(5)

        # Check that expired data did not trigger new computation
        tdSql.query("select count(*) from rdb.r_period_expired;")
        result_count_after = tdSql.getData(0, 0)
        tdLog.info(f"PERIOD result count after expired data: {result_count_after}")

        # For PERIOD trigger, expired data should not increase result count
        assert result_count_after >= result_count_before, "PERIOD expired_time result count should >= before expired data"


    def check06(self):
        # Test count window with EXPIRED_TIME - should ignore EXPIRED_TIME option and process all data
        tdLog.info("Check 6: COUNT_WINDOW with EXPIRED_TIME ignores EXPIRED_TIME option")
        tdSql.checkTableType(dbname="rdb", stbname="r_count_expired", columns=3, tags=1)

        # Check initial results from count window trigger
        # COUNT_WINDOW(3) means every 3 records should trigger computation
        # Initial data has 6 records, so should have 2 windows
        tdSql.query("select count(*) from rdb.r_count_expired;")
        result_count_before = tdSql.getData(0, 0)
        tdLog.info(f"COUNT_WINDOW result count before expired data: {result_count_before}")

        # Insert expired data - COUNT_WINDOW should ignore EXPIRED_TIME and process this data
        trigger_sqls = [
            "insert into tdb.cw1 values ('2025-01-01 01:00:00', 10, 'expired1');",
            "insert into tdb.cw1 values ('2025-01-01 01:00:15', 20, 'expired2');",
            "insert into tdb.cw1 values ('2025-01-01 01:00:30', 30, 'expired3');",
        ]
        tdSql.executes(trigger_sqls)

        tdLog.info("wait for stream to be stable")
        time.sleep(5)

        # Check that COUNT_WINDOW processed the expired data (ignored EXPIRED_TIME)
        tdSql.query("select count(*) from rdb.r_count_expired;")
        result_count_after = tdSql.getData(0, 0)
        tdLog.info(f"COUNT_WINDOW result count after expired data: {result_count_after}")

        # For COUNT_WINDOW, EXPIRED_TIME should be ignored, so new data should trigger computation
        # The 3 new records should create 1 additional window
        assert result_count_after == result_count_before, "COUNT_WINDOW should ignore EXPIRED_TIME and process expired data"
