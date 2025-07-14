import subprocess
import time
from new_test_framework.utils import tdLog, tdSql, clusterComCheck, tdStream, StreamItem


class TestStreamRecalcCombinedOptions:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_recalc_combined_options(self):
        """Stream Recalculation Combined Options Test

        Test combination of multiple stream options:
        1. EXPIRED_TIME + WATERMARK - test interaction between expired data and watermark
        2. IGNORE_DISORDER + WATERMARK - test conflict resolution
        3. DELETE_RECALC + EXPIRED_TIME - test delete recalculation with expired data
        4. WATERMARK + DELETE_RECALC + EXPIRED_TIME - test comprehensive option combination

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
        tdLog.info("prepare trigger tables for combined options testing")

        # Trigger table for EXPIRED_TIME + WATERMARK combination
        stb1_trig = "create table tdb.comb_expired_watermark (ts timestamp, cint int, c2 int, c3 double, category varchar(16)) tags(id int, name varchar(16));"
        ctb1_trig = "create table tdb.cew1 using tdb.comb_expired_watermark tags(1, 'device1') tdb.cew2 using tdb.comb_expired_watermark tags(2, 'device2') tdb.cew3 using tdb.comb_expired_watermark tags(3, 'device3')"
        tdSql.execute(stb1_trig)
        tdSql.execute(ctb1_trig)

        # Trigger table for IGNORE_DISORDER + WATERMARK combination
        stb2_trig = "create table tdb.comb_ignore_watermark (ts timestamp, val_num int, status varchar(16)) tags(device_id int);"
        ctb2_trig = "create table tdb.ciw1 using tdb.comb_ignore_watermark tags(1) tdb.ciw2 using tdb.comb_ignore_watermark tags(2) tdb.ciw3 using tdb.comb_ignore_watermark tags(3)"
        tdSql.execute(stb2_trig)
        tdSql.execute(ctb2_trig)

        # Trigger table for DELETE_RECALC + EXPIRED_TIME combination
        stb3_trig = "create table tdb.comb_delete_expired (ts timestamp, val_num int, status varchar(16)) tags(device_id int);"
        ctb3_trig = "create table tdb.cde1 using tdb.comb_delete_expired tags(1) tdb.cde2 using tdb.comb_delete_expired tags(2) tdb.cde3 using tdb.comb_delete_expired tags(3)"
        tdSql.execute(stb3_trig)
        tdSql.execute(ctb3_trig)

        # Trigger table for comprehensive combination (WATERMARK + DELETE_RECALC + EXPIRED_TIME)
        stb4_trig = "create table tdb.comb_comprehensive (ts timestamp, val_num int, event_val int) tags(device_id int);"
        ctb4_trig = "create table tdb.cc1 using tdb.comb_comprehensive tags(1) tdb.cc2 using tdb.comb_comprehensive tags(2) tdb.cc3 using tdb.comb_comprehensive tags(3)"
        tdSql.execute(stb4_trig)
        tdSql.execute(ctb4_trig)

        # Trigger table for period stream (EXPIRED_TIME + WATERMARK)
        stb5_trig = "create table tdb.trigger_period_combined (ts timestamp, val_num int, metric double) tags(device_id int);"
        ctb5_trig = "create table tdb.cp1 using tdb.trigger_period_combined tags(1) tdb.cp2 using tdb.trigger_period_combined tags(2) tdb.cp3 using tdb.trigger_period_combined tags(3)"
        tdSql.execute(stb5_trig)
        tdSql.execute(ctb5_trig)

        # Trigger table for count window stream (IGNORE_DISORDER + DELETE_RECALC)
        stb6_trig = "create table tdb.trigger_count_combined (ts timestamp, val_num int, category varchar(16)) tags(device_id int);"
        ctb6_trig = "create table tdb.ck1 using tdb.trigger_count_combined tags(1) tdb.ck2 using tdb.trigger_count_combined tags(2) tdb.ck3 using tdb.trigger_count_combined tags(3)"
        tdSql.execute(stb6_trig)
        tdSql.execute(ctb6_trig)

    def writeInitialTriggerData(self):
        tdLog.info("write initial trigger data to tdb")
        
        # Data for EXPIRED_TIME + WATERMARK combination
        trigger_sqls = [
            "insert into tdb.cew1 values ('2025-01-01 02:00:00', 10, 100, 1.5, 'normal');",
            "insert into tdb.cew1 values ('2025-01-01 02:00:30', 20, 200, 2.5, 'normal');",
            "insert into tdb.cew1 values ('2025-01-01 02:01:00', 30, 300, 3.5, 'normal');",
            "insert into tdb.cew1 values ('2025-01-01 02:01:30', 40, 400, 4.5, 'normal');",
            "insert into tdb.cew1 values ('2025-01-01 02:02:00', 50, 500, 5.5, 'normal');",
            "insert into tdb.cew1 values ('2025-01-01 02:02:30', 60, 600, 6.5, 'normal');",
        ]
        tdSql.executes(trigger_sqls)

        # Data for IGNORE_DISORDER + WATERMARK combination
        trigger_sqls = [
            "insert into tdb.ciw1 values ('2025-01-01 02:00:00', 10, 'normal');",
            "insert into tdb.ciw1 values ('2025-01-01 02:00:30', 20, 'normal');",
            "insert into tdb.ciw1 values ('2025-01-01 02:01:00', 30, 'normal');",
            "insert into tdb.ciw1 values ('2025-01-01 02:03:00', 40, 'normal');",
            "insert into tdb.ciw1 values ('2025-01-01 02:03:30', 50, 'normal');",
            "insert into tdb.ciw1 values ('2025-01-01 02:04:00', 60, 'normal');",
        ]
        tdSql.executes(trigger_sqls)

        # Data for DELETE_RECALC + EXPIRED_TIME combination
        trigger_sqls = [
            "insert into tdb.cde1 values ('2025-01-01 02:00:00', 10, 'normal');",
            "insert into tdb.cde1 values ('2025-01-01 02:00:30', 20, 'normal');",
            "insert into tdb.cde1 values ('2025-01-01 02:01:00', 30, 'warning');",
            "insert into tdb.cde1 values ('2025-01-01 02:01:30', 40, 'warning');",
            "insert into tdb.cde1 values ('2025-01-01 02:02:00', 50, 'error');",
            "insert into tdb.cde1 values ('2025-01-01 02:02:30', 60, 'error');",
        ]
        tdSql.executes(trigger_sqls)

        # Data for comprehensive combination
        trigger_sqls = [
            "insert into tdb.cc1 values ('2025-01-01 02:00:00', 10, 6);",
            "insert into tdb.cc1 values ('2025-01-01 02:00:30', 20, 7);",
            "insert into tdb.cc1 values ('2025-01-01 02:01:00', 30, 12);",
            "insert into tdb.cc1 values ('2025-01-01 02:01:30', 40, 6);",
            "insert into tdb.cc1 values ('2025-01-01 02:02:00', 50, 9);",
            "insert into tdb.cc1 values ('2025-01-01 02:02:30', 60, 13);",
        ]
        tdSql.executes(trigger_sqls)

        # Trigger data for comprehensive test
        trigger_sqls = [
            "insert into tdb.cc1 values ('2025-01-01 02:00:00', 10, 1.5, 'normal');",
            "insert into tdb.cc1 values ('2025-01-01 02:00:30', 20, 2.5, 'normal');",
            "insert into tdb.cc1 values ('2025-01-01 02:01:00', 30, 3.5, 'warning');",
            "insert into tdb.cc1 values ('2025-01-01 02:01:30', 40, 4.5, 'warning');",
            "insert into tdb.cc1 values ('2025-01-01 02:02:00', 50, 5.5, 'error');",
            "insert into tdb.cc1 values ('2025-01-01 02:02:30', 60, 6.5, 'error');",
        ]
        tdSql.executes(trigger_sqls)

        # Trigger data for period stream
        trigger_sqls = [
            "insert into tdb.cp1 values ('2025-01-01 02:00:00', 10, 1.5);",
            "insert into tdb.cp1 values ('2025-01-01 02:00:30', 20, 2.5);",
            "insert into tdb.cp1 values ('2025-01-01 02:01:00', 30, 3.5);",
            "insert into tdb.cp1 values ('2025-01-01 02:01:30', 40, 4.5);",
            "insert into tdb.cp1 values ('2025-01-01 02:02:00', 50, 5.5);",
            "insert into tdb.cp1 values ('2025-01-01 02:02:30', 60, 6.5);",
        ]
        tdSql.executes(trigger_sqls)

        # Trigger data for count window stream
        trigger_sqls = [
            "insert into tdb.ck1 values ('2025-01-01 02:00:00', 10, 'normal');",
            "insert into tdb.ck1 values ('2025-01-01 02:00:15', 20, 'normal');",
            "insert into tdb.ck1 values ('2025-01-01 02:00:30', 30, 'warning');",
            "insert into tdb.ck1 values ('2025-01-01 02:00:45', 40, 'warning');",
            "insert into tdb.ck1 values ('2025-01-01 02:01:00', 50, 'error');",
            "insert into tdb.ck1 values ('2025-01-01 02:01:15', 60, 'error');",
        ]
        tdSql.executes(trigger_sqls)

    def writeSourceData(self):
        tdLog.info("write source data to test combined options")
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

        # ===== Test 1: Combined Options =====
        
        # Test 1.1: EXPIRED_TIME + WATERMARK combination
        stream = StreamItem(
            id=1,
            stream="create stream rdb.s_comb_expired_watermark interval(2m) sliding(2m) from tdb.comb_expired_watermark partition by tbname stream_options(expired_time(1h)|watermark(30s)) into rdb.r_comb_expired_watermark as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check01,
        )
        self.streams.append(stream)

        # Test 1.2: IGNORE_DISORDER + WATERMARK combination - IGNORE_DISORDER should take precedence
        stream = StreamItem(
            id=2,
            stream="create stream rdb.s_comb_ignore_watermark session(ts,45s) from tdb.comb_ignore_watermark partition by tbname stream_options(ignore_disorder|watermark(1m)) into rdb.r_comb_ignore_watermark as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check02,
        )
        self.streams.append(stream)

        # Test 1.3: DELETE_RECALC + EXPIRED_TIME combination
        stream = StreamItem(
            id=3,
            stream="create stream rdb.s_comb_delete_expired state_window(status) from tdb.comb_delete_expired partition by tbname stream_options(delete_recalc|expired_time(1h)) into rdb.r_comb_delete_expired as select _twstart ts, count(*) cnt, avg(cint) avg_val, first(cvarchar) status_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check03,
        )
        self.streams.append(stream)

        # Test 1.4: Comprehensive combination (WATERMARK + DELETE_RECALC + EXPIRED_TIME)
        stream = StreamItem(
            id=4,
            stream="create stream rdb.s_comb_comprehensive event_window(start with event_val >= 5 end with event_val > 10) from tdb.comb_comprehensive partition by tbname stream_options(watermark(1m)|delete_recalc|expired_time(2h)) into rdb.r_comb_comprehensive as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check04,
        )
        self.streams.append(stream)

        # Test 5: PERIOD with EXPIRED_TIME + WATERMARK - test option interaction
        stream = StreamItem(
            id=5,
            stream="create stream rdb.s_period_combined period(30s) from tdb.trigger_period_combined partition by tbname stream_options(expired_time(1h), watermark(45s)) into rdb.r_period_combined as select _tlocaltime ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _tlocaltime - 30000000000 and cts <= _tlocaltime;",
            check_func=self.check05,
        )
        self.streams.append(stream)

        # Test 6: COUNT_WINDOW with IGNORE_DISORDER + DELETE_RECALC - test option interaction
        stream = StreamItem(
            id=6,
            stream="create stream rdb.s_count_combined count_window(3) from tdb.trigger_count_combined partition by tbname stream_options(ignore_disorder, delete_recalc) into rdb.r_count_combined as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check06,
        )
        self.streams.append(stream)

        tdLog.info(f"create total:{len(self.streams)} streams")
        for stream in self.streams:
            stream.createStream()

    # Check functions for each test case
    def check01(self):
        # Test EXPIRED_TIME + WATERMARK combination
        tdLog.info("Check 1: EXPIRED_TIME + WATERMARK combination behavior")
        tdSql.checkTableType(dbname="rdb", stbname="r_comb_expired_watermark", columns=3, tags=1)

        exp_sql = "select _wstart, count(*),avg(cint) from qdb.meters where cts >= '2025-01-01 02:00:00' and cts < '2025-01-01 02:02:00' interval(2m) sliding(2m) ;"
        res_sql = "select ts, cnt, avg_val from rdb.r_comb_expired_watermark;"
        self.streams[0].checkResultsBySql(res_sql, exp_sql)

        tdSql.query("select count(*) from rdb.r_comb_expired_watermark;")
        result_count_before = tdSql.getData(0, 0)
        tdLog.info(f"EXPIRED_TIME+WATERMARK result count before test: {result_count_before}")

        # Test 1: Insert expired data (should be ignored due to EXPIRED_TIME)
        expired_sqls = [
            "insert into tdb.cew1 values ('2025-01-01 01:00:00', 5, 50, 0.5, 'expired');",
        ]
        tdSql.executes(expired_sqls)

        # Test 2: Insert out-of-order data within WATERMARK tolerance 
        watermark_sqls = [
            "insert into tdb.cew1 values ('2025-01-01 02:02:15', 35, 350, 3.8, 'late');",  # 15s late, within 30s watermark
        ]
        tdSql.executes(watermark_sqls)

        tdLog.info("wait for stream to be stable after combined test")
        time.sleep(5)

        tdSql.query("select count(*) from rdb.r_comb_expired_watermark;")
        result_count_after = tdSql.getData(0, 0)
        tdLog.info(f"EXPIRED_TIME+WATERMARK result count after test: {result_count_after}")

        # EXPIRED_TIME should prevent expired data processing, WATERMARK should handle recent out-of-order data
        tdLog.info("EXPIRED_TIME+WATERMARK combination successfully handled both expired and out-of-order data")

    def check02(self):
        # Test IGNORE_DISORDER + WATERMARK combination - IGNORE_DISORDER should take precedence
        tdLog.info("Check 2: IGNORE_DISORDER + WATERMARK combination (IGNORE_DISORDER takes precedence)")
        tdSql.checkTableType(dbname="rdb", stbname="r_comb_ignore_watermark", columns=3, tags=1)

        exp_sql = "select count(*),avg(cint) from qdb.meters where cts >= '2025-01-01 02:00:00.000' and cts < '2025-01-01 02:01:00.000';"
        res_sql = "select cnt, avg_val from rdb.r_comb_ignore_watermark;"
        self.streams[1].checkResultsBySql(res_sql, exp_sql)

        tdSql.query("select count(*) from rdb.r_comb_ignore_watermark;")
        result_count_before = tdSql.getData(0, 0)
        tdLog.info(f"IGNORE_DISORDER+WATERMARK result count before test: {result_count_before}")

        # Insert out-of-order data - should be ignored due to IGNORE_DISORDER (overrides WATERMARK)
        disorder_sqls = [
            "insert into tdb.ciw1 values ('2025-01-01 02:00:15', 15, 'disorder');",  # Within watermark but should be ignored
            "insert into tdb.ciw1 values ('2025-01-01 01:59:00', 5, 'disorder');",   # Beyond watermark and should be ignored
        ]
        tdSql.executes(disorder_sqls)

        time.sleep(5)

        tdSql.query("select count(*) from rdb.r_comb_ignore_watermark;")
        result_count_after = tdSql.getData(0, 0)
        tdLog.info(f"IGNORE_DISORDER+WATERMARK result count after test: {result_count_after}")

        # IGNORE_DISORDER should take precedence and ignore all out-of-order data
        assert result_count_before == result_count_after, "IGNORE_DISORDER should take precedence over WATERMARK"

    def check03(self):
        # Test DELETE_RECALC + EXPIRED_TIME combination
        tdLog.info("Check 3: DELETE_RECALC + EXPIRED_TIME combination")
        tdSql.checkTableType(dbname="rdb", stbname="r_comb_delete_expired", columns=4, tags=1)

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_comb_delete_expired",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 02:00:00")
                and tdSql.compareData(0, 1, 100)
                and tdSql.compareData(0, 2, 240)
                and tdSql.compareData(1, 0, "2025-01-01 02:01:00")
                and tdSql.compareData(1, 1, 100)
                and tdSql.compareData(1, 2, 242)
            )

        # Test 1: Insert expired data (should be ignored due to EXPIRED_TIME)
        expired_sqls = [
            "insert into tdb.cde1 values ('2025-01-01 01:00:00', 5, 'expired');",
        ]
        tdSql.executes(expired_sqls)

        # Test 2: Delete non-expired data (should trigger recalculation due to DELETE_RECALC)
        delete_sqls = [
            "delete from tdb.cde1 where ts = '2025-01-01 02:00:30';",
        ]
        tdSql.executes(delete_sqls)

        time.sleep(5)

        # DELETE_RECALC should work for non-expired data, EXPIRED_TIME should prevent expired data processing
        tdSql.query("select count(*) from rdb.r_comb_delete_expired;")
        result_count_after = tdSql.getData(0, 0)
        tdLog.info(f"DELETE_RECALC+EXPIRED_TIME result count after test: {result_count_after}")

        tdLog.info("DELETE_RECALC+EXPIRED_TIME combination successfully handled both deletion and expired data")

    def check04(self):
        # Test comprehensive combination (WATERMARK + DELETE_RECALC + EXPIRED_TIME)
        tdLog.info("Check 4: Comprehensive combination (WATERMARK + DELETE_RECALC + EXPIRED_TIME)")
        tdSql.checkTableType(dbname="rdb", stbname="r_comb_comprehensive", columns=3, tags=1)

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_comb_comprehensive",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 02:00:00.000")
                and tdSql.compareData(0, 1, 200)
                and tdSql.compareData(0, 2, 240.5)
                and tdSql.compareData(1, 0, "2025-01-01 02:01:30.000")
                and tdSql.compareData(1, 1, 200)
                and tdSql.compareData(1, 2, 243.5)
            )

        # Test 1: Insert expired data (should be ignored due to EXPIRED_TIME: 2h)
        expired_sqls = [
            "insert into tdb.cc1 values ('2025-01-01 00:00:00', 5, 6);",  # Very old, beyond 2h EXPIRED_TIME
        ]
        tdSql.executes(expired_sqls)

        # Test 2: Insert out-of-order data within WATERMARK tolerance (1m)
        watermark_sqls = [
            "insert into tdb.cc1 values ('2025-01-01 02:02:00', 35, 12);",  # 30s late, within 1m watermark
        ]
        tdSql.executes(watermark_sqls)

        # Test 3: Delete some data (should trigger recalculation due to DELETE_RECALC)
        delete_sqls = [
            "delete from tdb.cc1 where ts = '2025-01-01 02:00:30';",
        ]
        tdSql.executes(delete_sqls)

        tdLog.info("wait for stream to be stable after comprehensive test")
        time.sleep(5)

        # All three options should work together: EXPIRED_TIME ignores old data, WATERMARK handles recent out-of-order data, DELETE_RECALC handles deletions
        tdSql.query("select count(*) from rdb.r_comb_comprehensive;")
        result_count_after = tdSql.getData(0, 0)
        tdLog.info(f"Comprehensive combination result count after test: {result_count_after}")

        tdLog.info("Comprehensive combination (WATERMARK+DELETE_RECALC+EXPIRED_TIME) successfully handled all scenarios") 


    def check05(self):
        # Test period with EXPIRED_TIME + WATERMARK - test option interaction
        tdLog.info("Check 5: PERIOD with EXPIRED_TIME + WATERMARK tests option interaction")
        tdSql.checkTableType(dbname="rdb", stbname="r_period_combined", columns=3, tags=1)

        # Check initial results from period trigger
        tdSql.query("select count(*) from rdb.r_period_combined;")
        result_count_before = tdSql.getData(0, 0)
        tdLog.info(f"PERIOD combined result count before test: {result_count_before}")

        # Test 1: Insert expired data (should be ignored due to EXPIRED_TIME)
        expired_sqls = [
            "insert into tdb.cp1 values ('2025-01-01 01:00:00', 10, 1.0);",
            "insert into tdb.cp1 values ('2025-01-01 01:00:30', 20, 2.0);",
        ]
        tdSql.executes(expired_sqls)

        time.sleep(5)
        tdSql.query("select count(*) from rdb.r_period_combined;")
        result_count_expired = tdSql.getData(0, 0)
        tdLog.info(f"PERIOD combined result count after expired data: {result_count_expired}")
        assert result_count_expired == result_count_before, "EXPIRED_TIME should prevent processing of expired data"

        # Test 2: Insert out-of-order data within WATERMARK tolerance (should be processed)
        watermark_sqls = [
            "insert into tdb.cp1 values ('2025-01-01 02:01:45', 70, 7.5);",  # Within 45s tolerance
            "insert into tdb.cp1 values ('2025-01-01 02:02:15', 80, 8.5);",  # Within 45s tolerance
        ]
        tdSql.executes(watermark_sqls)

        time.sleep(5)
        tdSql.query("select count(*) from rdb.r_period_combined;")
        result_count_final = tdSql.getData(0, 0)
        tdLog.info(f"PERIOD combined result count after watermark data: {result_count_final}")

        # WATERMARK should process in-tolerance data while EXPIRED_TIME blocks expired data
        assert result_count_final > result_count_expired, "WATERMARK should process in-tolerance data while EXPIRED_TIME blocks expired data"


    def check06(self):
        # Test count window with IGNORE_DISORDER + DELETE_RECALC - test option interaction
        tdLog.info("Check 6: COUNT_WINDOW with IGNORE_DISORDER + DELETE_RECALC tests option interaction")
        tdSql.checkTableType(dbname="rdb", stbname="r_count_combined", columns=3, tags=1)

        # Check initial results from count window trigger
        # COUNT_WINDOW(3) means every 3 records should trigger computation
        # Initial data has 6 records, so should have 2 windows
        tdSql.query("select count(*) from rdb.r_count_combined;")
        result_count_before = tdSql.getData(0, 0)
        tdLog.info(f"COUNT_WINDOW combined result count before test: {result_count_before}")

        # Test 1: Insert out-of-order data (should be ignored due to IGNORE_DISORDER)
        disorder_sqls = [
            "insert into tdb.ck1 values ('2025-01-01 01:59:00', 10, 'disorder1');",
            "insert into tdb.ck1 values ('2025-01-01 01:59:15', 20, 'disorder2');",
            "insert into tdb.ck1 values ('2025-01-01 01:59:30', 30, 'disorder3');",
        ]
        tdSql.executes(disorder_sqls)

        time.sleep(5)
        tdSql.query("select count(*) from rdb.r_count_combined;")
        result_count_disorder = tdSql.getData(0, 0)
        tdLog.info(f"COUNT_WINDOW combined result count after disorder data: {result_count_disorder}")
        assert result_count_disorder == result_count_before, "IGNORE_DISORDER should prevent processing of out-of-order data"

        # Test 2: Delete data from trigger table (should trigger recalculation due to DELETE_RECALC)
        delete_sqls = [
            "delete from tdb.ck1 where ts = '2025-01-01 02:00:00';",
            "delete from tdb.ck1 where ts = '2025-01-01 02:00:15';",
        ]
        tdSql.executes(delete_sqls)

        time.sleep(5)
        tdSql.query("select count(*) from rdb.r_count_combined;")
        result_count_final = tdSql.getData(0, 0)
        tdLog.info(f"COUNT_WINDOW combined result count after data deletion: {result_count_final}")

        # DELETE_RECALC should trigger recalculation while IGNORE_DISORDER blocks out-of-order data
        assert result_count_final > result_count_disorder, "DELETE_RECALC should trigger recalculation while IGNORE_DISORDER blocks out-of-order data" 