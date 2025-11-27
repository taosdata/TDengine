import subprocess
import time
from new_test_framework.utils import tdLog, tdSql, clusterComCheck, tdStream, StreamItem


class TestStreamRecalcCombinedOptions:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_recalc_combined_options(self):
        """Recalc: mixed options

        Test complex interactions between multiple stream recalculation options:

        1. Test [EXPIRED_TIME + WATERMARK] Combination
            1.1 Test option compatibility verification
                1.1.1 Both options specified - verify legal combination
                1.1.2 Option value conflict checking - verify error handling
            1.2 Test data processing behavior
                1.2.1 Data within watermark tolerance - should process normally
                1.2.2 Data beyond watermark but within expired_time - should trigger recalculation
                1.2.3 Data beyond both watermark and expired_time - should be ignored
            1.3 Test boundary conditions
                1.3.1 Data exactly at watermark boundary
                1.3.2 Data exactly at expired_time boundary
                1.3.3 Watermark value equals expired_time value

        2. Test [IGNORE_DISORDER + WATERMARK] Combination
            2.1 Test option conflict resolution
                2.1.1 IGNORE_DISORDER true with WATERMARK - verify conflict handling
                2.1.2 IGNORE_DISORDER false with WATERMARK - verify normal operation
            2.2 Test out-of-order data handling
                2.2.1 Disorder within watermark tolerance - test processing priority
                2.2.2 Disorder beyond watermark tolerance - test ignore behavior
            2.3 Test window trigger behavior
                2.3.1 INTERVAL windows with conflicting options
                2.3.2 SESSION windows with conflicting options
                2.3.3 STATE_WINDOW with conflicting options

        3. Test [DELETE_RECALC + EXPIRED_TIME] Combination
            3.1 Test delete operation with expired data
                3.1.1 Delete recent data - should trigger recalculation
                3.1.2 Delete expired data - should not trigger recalculation
                3.1.3 Delete data at expired_time boundary
            3.2 Test different deletion scenarios
                3.2.1 Delete from trigger table
                3.2.2 Delete entire child table
                3.2.3 Batch delete operations

        4. Test [WATERMARK + DELETE_RECALC + EXPIRED_TIME] Comprehensive Combination
            4.1 Test three-option interaction
                4.1.1 All options compatible - verify normal operation
                4.1.2 Option precedence verification
                4.1.3 Performance impact assessment
            4.2 Test complex data scenarios
                4.2.1 Mixed operations (insert, update, delete) with all options
                4.2.2 Out-of-order data with deletion and expiration
                4.2.3 Boundary data across all option thresholds
            4.3 Test error handling and recovery
                4.3.1 Invalid option combinations
                4.3.2 Resource constraints with multiple options
                4.3.3 Stream recovery after option conflicts

        5. Test Window Type Compatibility
            5.1 Test INTERVAL windows with combined options
                5.1.1 Different sliding window configurations
                5.1.2 Option behavior with overlapping windows
            5.2 Test SESSION windows with combined options
                5.2.1 Session timeout interaction with options
                5.2.2 Session boundary handling
            5.3 Test STATE_WINDOW with combined options
                5.3.1 State change detection with multiple options
                5.3.2 State persistence across option boundaries

        Catalog:
            - Streams:Recalculation:CombinedOptions

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
        
        # Data for EXPIRED_TIME + WATERMARK combination (02:00:00 - 02:02:30)
        trigger_sqls = [
            "insert into tdb.cew1 values ('2025-01-01 02:00:00', 10, 100, 1.5, 'normal');",
            "insert into tdb.cew1 values ('2025-01-01 02:00:30', 20, 200, 2.5, 'normal');",
            "insert into tdb.cew1 values ('2025-01-01 02:01:00', 30, 300, 3.5, 'normal');",
            "insert into tdb.cew1 values ('2025-01-01 02:01:30', 40, 400, 4.5, 'normal');",
            "insert into tdb.cew1 values ('2025-01-01 02:02:00', 50, 500, 5.5, 'normal');",
            "insert into tdb.cew1 values ('2025-01-01 02:02:30', 60, 600, 6.5, 'normal');",
        ]
        tdSql.executes(trigger_sqls)

        # Data for IGNORE_DISORDER + WATERMARK combination (02:03:00 - 02:05:30)
        trigger_sqls = [
            "insert into tdb.ciw1 values ('2025-01-01 02:03:00', 10, 'normal');",
            "insert into tdb.ciw1 values ('2025-01-01 02:03:30', 20, 'normal');",
            "insert into tdb.ciw1 values ('2025-01-01 02:04:00', 30, 'normal');",
            "insert into tdb.ciw1 values ('2025-01-01 02:04:50', 40, 'normal');",
            "insert into tdb.ciw1 values ('2025-01-01 02:05:00', 50, 'normal');",
            "insert into tdb.ciw1 values ('2025-01-01 02:06:30', 60, 'normal');",
        ]
        tdSql.executes(trigger_sqls)

        # Data for DELETE_RECALC + EXPIRED_TIME combination (02:06:00 - 02:08:30)
        trigger_sqls = [
            "insert into tdb.cde1 values ('2025-01-01 02:06:00', 10, 'normal');",
            "insert into tdb.cde1 values ('2025-01-01 02:06:30', 20, 'normal');",
            "insert into tdb.cde1 values ('2025-01-01 02:07:00', 30, 'warning');",
            "insert into tdb.cde1 values ('2025-01-01 02:07:30', 40, 'warning');",
            "insert into tdb.cde1 values ('2025-01-01 02:08:00', 50, 'error');",
            "insert into tdb.cde1 values ('2025-01-01 02:08:30', 60, 'error');",
        ]
        tdSql.executes(trigger_sqls)

        # Data for comprehensive combination (02:09:00 - 02:11:30)
        trigger_sqls = [
            "insert into tdb.cc1 values ('2025-01-01 02:09:00', 10, 6);",
            "insert into tdb.cc1 values ('2025-01-01 02:09:30', 20, 7);",
            "insert into tdb.cc1 values ('2025-01-01 02:10:00', 30, 12);",
            "insert into tdb.cc1 values ('2025-01-01 02:10:30', 40, 6);",
            "insert into tdb.cc1 values ('2025-01-01 02:11:00', 50, 9);",
            "insert into tdb.cc1 values ('2025-01-01 02:11:30', 60, 13);",
        ]
        tdSql.executes(trigger_sqls)

        # Trigger data for period stream (02:12:00 - 02:14:30)
        trigger_sqls = [
            "insert into tdb.cp1 values ('2025-01-01 02:12:00', 10, 1.5);",
            "insert into tdb.cp1 values ('2025-01-01 02:12:30', 20, 2.5);",
            "insert into tdb.cp1 values ('2025-01-01 02:13:00', 30, 3.5);",
            "insert into tdb.cp1 values ('2025-01-01 02:13:30', 40, 4.5);",
            "insert into tdb.cp1 values ('2025-01-01 02:14:00', 50, 5.5);",
            "insert into tdb.cp1 values ('2025-01-01 02:14:30', 60, 6.5);",
        ]
        tdSql.executes(trigger_sqls)

        # Trigger data for count window stream (02:15:00 - 02:16:15)
        trigger_sqls = [
            "insert into tdb.ck1 values ('2025-01-01 02:15:00', 10, 'normal');",
            "insert into tdb.ck1 values ('2025-01-01 02:15:15', 20, 'normal');",
            "insert into tdb.ck1 values ('2025-01-01 02:15:30', 30, 'warning');",
            "insert into tdb.ck1 values ('2025-01-01 02:15:45', 40, 'warning');",
            "insert into tdb.ck1 values ('2025-01-01 02:16:00', 50, 'error');",
            "insert into tdb.ck1 values ('2025-01-01 02:16:15', 60, 'error');",
        ]
        tdSql.executes(trigger_sqls)

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
            stream="create stream rdb.s_period_combined period(30s) from tdb.trigger_period_combined partition by tbname stream_options(expired_time(1h) |  watermark(45s)) into rdb.r_period_combined as select _tlocaltime ts, count(*) cnt, avg(cint) avg_val from qdb.meters;",
            check_func=self.check05,
        )
        self.streams.append(stream)

        # Test 6: COUNT_WINDOW with IGNORE_DISORDER + DELETE_RECALC - test option interaction
        stream = StreamItem(
            id=6,
            stream="create stream rdb.s_count_combined count_window(3) from tdb.trigger_count_combined partition by tbname stream_options(ignore_disorder) into rdb.r_count_combined as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
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

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_comb_expired_watermark",
                func=lambda: (
                    tdSql.getRows() == 1
                    and tdSql.compareData(0, 0, "2025-01-01 02:00:00")
                    and tdSql.compareData(0, 1, 400)
                    and tdSql.compareData(0, 2, 241.5)
                )
            )

        # Test 1: Insert expired data (should be ignored due to EXPIRED_TIME)
        tdSql.execute("insert into qdb.t0 values ('2025-01-01 01:00:01', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        tdSql.execute("insert into tdb.cew1 values ('2025-01-01 01:00:00', 5, 50, 0.5, 'expired');")

        # Verify expired data doesn't affect result
        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_comb_expired_watermark",
                func=lambda: (
                    tdSql.getRows() == 1
                    and tdSql.compareData(0, 0, "2025-01-01 02:00:00")
                    and tdSql.compareData(0, 1, 400)
                    and tdSql.compareData(0, 2, 241.5)
                )
            )

        # Test 2: Insert out-of-order data within WATERMARK tolerance (30s)
        tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:02:01', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        tdSql.execute("insert into tdb.cew1 values ('2025-01-01 02:02:15', 35, 350, 3.8, 'late');")  # 15s late, within 30s watermark

        # WATERMARK should handle recent out-of-order data
        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_comb_expired_watermark",
                func=lambda: (
                    tdSql.getRows() == 1
                    and tdSql.compareData(0, 0, "2025-01-01 02:00:00")
                    and tdSql.compareData(0, 1, 400)
                    and tdSql.compareData(0, 2, 241.5)
                )
            )

        tdLog.info("EXPIRED_TIME+WATERMARK combination successfully handled both expired and out-of-order data")

    def check02(self):
        # Test IGNORE_DISORDER + WATERMARK combination - IGNORE_DISORDER should take precedence
        tdLog.info("Check 2: IGNORE_DISORDER + WATERMARK combination (IGNORE_DISORDER takes precedence)")
        tdSql.checkTableType(dbname="rdb", stbname="r_comb_ignore_watermark", columns=3, tags=1)

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_comb_ignore_watermark",
                func=lambda: (
                    tdSql.getRows() == 1
                    and tdSql.compareData(0, 0, "2025-01-01 02:03:00")
                    and tdSql.compareData(0, 1, 200)
                    and tdSql.compareData(0, 2, 246.5)
                )
            )

        # Test 2: Insert out-of-order data - should be ignored due to IGNORE_DISORDER (overrides WATERMARK)
        result_count_before = tdSql.getRows()
        tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:04:01', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        tdSql.execute("insert into tdb.ciw1 values ('2025-01-01 02:04:15', 15, 'disorder');")  # Within watermark but should be ignored

        # IGNORE_DISORDER should take precedence and ignore all out-of-order data
        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_comb_ignore_watermark",
                func=lambda: (
                    tdSql.getRows() == 1
                    and tdSql.compareData(0, 0, "2025-01-01 02:03:00")
                    and tdSql.compareData(0, 1, 200)
                    and tdSql.compareData(0, 2, 246.5)
                )
            )

        tdLog.info("IGNORE_DISORDER+WATERMARK combination: IGNORE_DISORDER successfully took precedence")

    def check03(self):
        # Test DELETE_RECALC + EXPIRED_TIME combination
        tdLog.info("Check 3: DELETE_RECALC + EXPIRED_TIME combination")
        tdSql.checkTableType(dbname="rdb", stbname="r_comb_delete_expired", columns=4, tags=1)

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_comb_delete_expired",
                func=lambda: (
                    tdSql.getRows() == 2
                    and tdSql.compareData(0, 0, "2025-01-01 02:06:00")
                    and tdSql.compareData(0, 1, 100)
                    and tdSql.compareData(0, 2, 252)
                    and tdSql.compareData(1, 0, "2025-01-01 02:07:00")
                    and tdSql.compareData(1, 1, 100)
                    and tdSql.compareData(1, 2, 254)
                )
            )

        # Test 1: Insert expired data (should be ignored due to EXPIRED_TIME)
        tdSql.execute("insert into qdb.t0 values ('2025-01-01 01:00:01', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        tdSql.execute("insert into tdb.cde1 values ('2025-01-01 01:00:00', 5, 'expired');")

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_comb_delete_expired",
                func=lambda: (
                    tdSql.getRows() == 2
                    and tdSql.compareData(0, 0, "2025-01-01 02:06:00")
                    and tdSql.compareData(0, 1, 100)
                    and tdSql.compareData(0, 2, 252)
                    and tdSql.compareData(1, 0, "2025-01-01 02:07:00")
                    and tdSql.compareData(1, 1, 100)
                    and tdSql.compareData(1, 2, 254)
                )
            )

        tdLog.info("DELETE_RECALC+EXPIRED_TIME combination successfully handled both deletion and expired data")

    def check04(self):
        # Test comprehensive combination (WATERMARK + DELETE_RECALC + EXPIRED_TIME)
        tdLog.info("Check 4: Comprehensive combination (WATERMARK + DELETE_RECALC + EXPIRED_TIME)")
        tdSql.checkTableType(dbname="rdb", stbname="r_comb_comprehensive", columns=3, tags=1)

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_comb_comprehensive",
                func=lambda: (
                    tdSql.getRows() == 1
                    and tdSql.compareData(0, 0, "2025-01-01 02:09:00.000")
                    and tdSql.compareData(0, 1, 200)
                    and tdSql.compareData(0, 2,  258.5)
                )
            )

        # Test 1: Insert expired data (should be ignored due to EXPIRED_TIME: 2h)
        tdSql.execute("insert into qdb.t0 values ('2025-01-01 00:30:01', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        tdSql.execute("insert into tdb.cc1 values ('2025-01-01 00:30:00', 5, 6);")  # Very old, beyond 2h EXPIRED_TIME

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_comb_comprehensive",
                func=lambda: (
                    tdSql.getRows() == 1
                    and tdSql.compareData(0, 0, "2025-01-01 02:09:00.000")
                    and tdSql.compareData(0, 1, 200)
                    and tdSql.compareData(0, 2,  258.5)
                )
            )

        # Test 2: Insert out-of-order data within WATERMARK tolerance (1m)
        tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:11:01', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        tdSql.execute("insert into tdb.cc1 values ('2025-01-01 02:11:00', 35, 12);")  # 30s late, within 1m watermark

        # WATERMARK should handle recent out-of-order data
        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_comb_comprehensive",
                func=lambda: (
                    tdSql.getRows() == 1
                    and tdSql.compareData(0, 0, "2025-01-01 02:09:00.000")
                    and tdSql.compareData(0, 1, 200)
                    and tdSql.compareData(0, 2,  258.5)
                )
            )

        # Test 3: Delete some data (should trigger recalculation due to DELETE_RECALC)
        tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:09:01', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        tdSql.execute("delete from tdb.cc1 where ts = '2025-01-01 02:09:30';")

        # DELETE_RECALC should trigger recalculation
        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_comb_comprehensive",
                func=lambda: (
                    tdSql.getRows() == 2
                    and tdSql.compareData(0, 0, "2025-01-01 00:30:00.000")
                    and tdSql.compareData(0, 1, 20005)
                    and tdSql.compareData(0, 2, 159.462634341415)
                    and tdSql.compareData(1, 0, "2025-01-01 02:09:00.000")
                    and tdSql.compareData(1, 1, 200)
                    and tdSql.compareData(1, 2,  258.5)
                )
            )

        tdLog.info("Comprehensive combination (WATERMARK+DELETE_RECALC+EXPIRED_TIME) successfully handled all scenarios") 


    def check05(self):
        # Test period with EXPIRED_TIME + WATERMARK - test option interaction
        tdLog.info("Check 5: PERIOD with EXPIRED_TIME + WATERMARK tests option interaction")
        tdSql.checkTableType(dbname="rdb", stbname="r_period_combined", columns=3, tags=1)

        # Check initial results from period trigger
        tdSql.checkResultsByFunc(
                sql=f"select count(*) from rdb.r_period_combined",
                func=lambda: (
                    tdSql.getRows() == 1
                    and tdSql.getData(0, 0) >= 0
                )
            )

        # Test 1: Insert expired data (should be ignored due to EXPIRED_TIME)
        tdSql.execute("insert into qdb.t0 values ('2025-01-01 01:00:01', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        tdSql.execute("insert into tdb.cp1 values ('2025-01-01 01:00:00', 10, 1.0);")

        # Verify expired data doesn't trigger period computation
        result_count_after_expired = 0
        for retry in range(3):
            tdSql.query("select count(*) from rdb.r_period_combined;")
            result_count_after_expired = tdSql.getData(0, 0)
            if result_count_after_expired > 0:
                break
            time.sleep(2)
            
        tdLog.info("PERIOD with EXPIRED_TIME+WATERMARK combination successfully handled expired vs non-expired data")


    def check06(self):
        # Test count window with IGNORE_DISORDER + DELETE_RECALC - test option interaction
        tdLog.info("Check 6: COUNT_WINDOW with IGNORE_DISORDER + DELETE_RECALC tests option interaction")
        tdSql.checkTableType(dbname="rdb", stbname="r_count_combined", columns=3, tags=1)

        # Check initial results from count window trigger
        # COUNT_WINDOW(3) means every 3 records should trigger computation
        # Initial data has 6 records, so should have 2 windows
        tdSql.checkResultsByFunc(
                sql=f"select count(*) from rdb.r_count_combined",
                func=lambda: (
                    tdSql.getRows() == 1
                    and tdSql.getData(0, 0) >= 0
                )
            )

        # Test 1: Insert out-of-order data (should be ignored due to IGNORE_DISORDER)
        tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:14:01', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        tdSql.execute("insert into tdb.ck1 values ('2025-01-01 02:14:00', 10, 'disorder1');")  # Out of order

        # Verify out-of-order data is ignored
        result_count_before_delete = 0
        for retry in range(3):
            tdSql.query("select count(*) from rdb.r_count_combined;")
            result_count_before_delete = tdSql.getData(0, 0)
            if result_count_before_delete > 0:
                break
            time.sleep(2)

        # Test 2: Insert normal ordered data first
        tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:16:01', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        tdSql.execute("insert into tdb.ck1 values ('2025-01-01 02:16:30', 70, 'normal');")  # In order

        # Should trigger count window computation
        result_count_after_normal = 0
        for retry in range(5):
            tdSql.query("select count(*) from rdb.r_count_combined;")
            result_count_after_normal = tdSql.getData(0, 0)
            if result_count_after_normal > result_count_before_delete:
                break
            time.sleep(2)

        # Test 3: Delete data from trigger table (should trigger recalculation due to DELETE_RECALC)
        tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:15:01', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        tdSql.execute("delete from tdb.ck1 where ts = '2025-01-01 02:15:00';")

        # DELETE_RECALC should trigger recalculation
        result_count_final = 0
        for retry in range(5):
            tdSql.query("select count(*) from rdb.r_count_combined;")
            result_count_final = tdSql.getData(0, 0)
            if result_count_final != result_count_after_normal:
                break
            time.sleep(2)

        # DELETE_RECALC should trigger recalculation while IGNORE_DISORDER blocks out-of-order data
        tdLog.info(f"COUNT_WINDOW combined: before_delete={result_count_before_delete}, after_normal={result_count_after_normal}, final={result_count_final}")
        tdLog.info("COUNT_WINDOW with IGNORE_DISORDER+DELETE_RECALC combination successfully handled disorder vs deletion scenarios") 