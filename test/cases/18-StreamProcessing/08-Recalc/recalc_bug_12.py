import subprocess
import time
from new_test_framework.utils import tdLog, tdSql, clusterComCheck, tdStream, StreamItem


class TestStreamRecalcManual:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_recalc_manual(self):
        """Stream Manual Recalculation Test

        Test manual recalculation functionality:
        1. Manual recalculation with time range - should recalculate specified time period
        2. Manual recalculation without end time - should recalculate from start time to current
        3. Different trigger types behavior with manual recalculation
        4. Edge cases and error handling


        Since: v3.0.0.0

        Labels: common,ci,skip

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
        tdLog.info("prepare trigger tables for manual recalculation testing")

        # Trigger tables in tdb (control stream computation trigger)
        stb_trig = "create table tdb.manual_triggers (ts timestamp, cint int, c2 int, c3 double, category varchar(16)) tags(id int, name varchar(16));"
        ctb_trig = "create table tdb.mt1 using tdb.manual_triggers tags(1, 'device1') tdb.mt2 using tdb.manual_triggers tags(2, 'device2') tdb.mt3 using tdb.manual_triggers tags(3, 'device3')"
        tdSql.execute(stb_trig)
        tdSql.execute(ctb_trig)

        # Trigger table for session stream
        stb2_trig = "create table tdb.trigger_session_manual (ts timestamp, val_num int, status varchar(16)) tags(device_id int);"
        ctb2_trig = "create table tdb.sm1 using tdb.trigger_session_manual tags(1) tdb.sm2 using tdb.trigger_session_manual tags(2) tdb.sm3 using tdb.trigger_session_manual tags(3)"
        tdSql.execute(stb2_trig)
        tdSql.execute(ctb2_trig)

        # Trigger table for state window stream
        stb3_trig = "create table tdb.trigger_state_manual (ts timestamp, val_num int, status varchar(16)) tags(device_id int);"
        ctb3_trig = "create table tdb.sw1 using tdb.trigger_state_manual tags(1) tdb.sw2 using tdb.trigger_state_manual tags(2) tdb.sw3 using tdb.trigger_state_manual tags(3)"
        tdSql.execute(stb3_trig)
        tdSql.execute(ctb3_trig)

        # Trigger table for event window stream
        stb4_trig = "create table tdb.trigger_event_manual (ts timestamp, val_num int, event_val int) tags(device_id int);"
        ctb4_trig = "create table tdb.em1 using tdb.trigger_event_manual tags(1) tdb.em2 using tdb.trigger_event_manual tags(2) tdb.em3 using tdb.trigger_event_manual tags(3)"
        tdSql.execute(stb4_trig)
        tdSql.execute(ctb4_trig)

        # Trigger table for count window stream
        stb5_trig = "create table tdb.trigger_count_manual (ts timestamp, val_num int, category varchar(16)) tags(device_id int);"
        ctb5_trig = "create table tdb.cm1 using tdb.trigger_count_manual tags(1) tdb.cm2 using tdb.trigger_count_manual tags(2) tdb.cm3 using tdb.trigger_count_manual tags(3)"
        tdSql.execute(stb5_trig)
        tdSql.execute(ctb5_trig)

        # Trigger table for sliding stream
        stb6_trig = "create table tdb.trigger_sliding_manual (ts timestamp, val_num int, metric double) tags(device_id int);"
        ctb6_trig = "create table tdb.sl1 using tdb.trigger_sliding_manual tags(1) tdb.sl2 using tdb.trigger_sliding_manual tags(2) tdb.sl3 using tdb.trigger_sliding_manual tags(3)"
        tdSql.execute(stb6_trig)
        tdSql.execute(ctb6_trig)

    def writeInitialTriggerData(self):
        tdLog.info("write initial trigger data to tdb")
        
        # Trigger data for interval+sliding stream
        trigger_sqls = [
            "insert into tdb.mt1 values ('2025-01-01 02:00:00', 10, 100, 1.5, 'normal');",
            "insert into tdb.mt1 values ('2025-01-01 02:00:30', 20, 200, 2.5, 'normal');",
            "insert into tdb.mt1 values ('2025-01-01 02:01:00', 30, 300, 3.5, 'normal');",
            "insert into tdb.mt1 values ('2025-01-01 02:01:30', 40, 400, 4.5, 'normal');",
            "insert into tdb.mt1 values ('2025-01-01 02:02:00', 50, 500, 5.5, 'normal');",
            "insert into tdb.mt1 values ('2025-01-01 02:02:30', 60, 600, 6.5, 'normal');",
            "insert into tdb.mt1 values ('2025-01-01 02:03:00', 70, 700, 7.5, 'normal');",
        ]
        tdSql.executes(trigger_sqls)

        # Trigger data for session stream
        trigger_sqls = [
            "insert into tdb.sm1 values ('2025-01-01 02:10:00', 10, 'normal');",
            "insert into tdb.sm1 values ('2025-01-01 02:10:30', 20, 'normal');",
            "insert into tdb.sm1 values ('2025-01-01 02:11:00', 30, 'normal');",
            "insert into tdb.sm1 values ('2025-01-01 02:11:30', 40, 'normal');",
            "insert into tdb.sm1 values ('2025-01-01 02:12:00', 50, 'normal');",
            "insert into tdb.sm1 values ('2025-01-01 02:12:30', 60, 'normal');",
        ]
        tdSql.executes(trigger_sqls)

        # Trigger data for state window stream
        trigger_sqls = [
            "insert into tdb.sw1 values ('2025-01-01 02:20:00', 10, 'normal');",
            "insert into tdb.sw1 values ('2025-01-01 02:20:30', 20, 'normal');",
            "insert into tdb.sw1 values ('2025-01-01 02:21:00', 30, 'warning');",
            "insert into tdb.sw1 values ('2025-01-01 02:21:30', 40, 'warning');",
            "insert into tdb.sw1 values ('2025-01-01 02:22:00', 50, 'error');",
            "insert into tdb.sw1 values ('2025-01-01 02:22:30', 60, 'error');",
        ]
        tdSql.executes(trigger_sqls)

        # Trigger data for event window stream
        trigger_sqls = [
            "insert into tdb.em1 values ('2025-01-01 02:30:00', 10, 6);",
            "insert into tdb.em1 values ('2025-01-01 02:30:30', 20, 7);",
            "insert into tdb.em1 values ('2025-01-01 02:31:00', 30, 12);",
            "insert into tdb.em1 values ('2025-01-01 02:31:30', 40, 6);",
            "insert into tdb.em1 values ('2025-01-01 02:32:00', 50, 9);",
            "insert into tdb.em1 values ('2025-01-01 02:32:30', 60, 13);",
        ]
        tdSql.executes(trigger_sqls)

        # Trigger data for count window stream
        trigger_sqls = [
            "insert into tdb.cm1 values ('2025-01-01 02:40:00', 10, 'normal');",
            "insert into tdb.cm1 values ('2025-01-01 02:40:15', 20, 'normal');",
            "insert into tdb.cm1 values ('2025-01-01 02:40:30', 30, 'warning');",
            "insert into tdb.cm1 values ('2025-01-01 02:40:45', 40, 'warning');",
            "insert into tdb.cm1 values ('2025-01-01 02:41:00', 50, 'error');",
            "insert into tdb.cm1 values ('2025-01-01 02:41:15', 60, 'error');",
        ]
        tdSql.executes(trigger_sqls)

        # Trigger data for sliding stream
        trigger_sqls = [
            "insert into tdb.sl1 values ('2025-01-01 02:50:00', 10, 1.5);",
            "insert into tdb.sl1 values ('2025-01-01 02:50:30', 20, 2.5);",
            "insert into tdb.sl1 values ('2025-01-01 02:51:00', 30, 3.5);",
            "insert into tdb.sl1 values ('2025-01-01 02:51:30', 40, 4.5);",
            "insert into tdb.sl1 values ('2025-01-01 02:52:00', 50, 5.5);",
            "insert into tdb.sl1 values ('2025-01-01 02:52:30', 60, 6.5);",
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

        # ===== Test 1: Manual Recalculation for Different Trigger Types =====
        
        # Test 1.1: INTERVAL+SLIDING stream for manual recalculation
        stream = StreamItem(
            id=1,
            stream="create stream rdb.s_interval_manual interval(2m) sliding(2m) from tdb.manual_triggers partition by tbname into rdb.r_interval_manual as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check01,
        )
        self.streams.append(stream)

        tdLog.info(f"create total:{len(self.streams)} streams")
        for stream in self.streams:
            stream.createStream()

    # Check functions for each test case
    def check01(self):
        # Test interval+sliding with manual recalculation
        tdLog.info("Check 1: INTERVAL+SLIDING manual recalculation")
        tdSql.checkTableType(dbname="rdb", stbname="r_interval_manual", columns=3, tags=1)

        # Write source data for testing
        tdLog.info("write source data for manual recalculation testing")
        tdSql.execute("insert into qdb.t0 values ('2025-01-01 00:00:01', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")

        # Check initial results
        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_interval_manual",
                func=lambda: (
                    tdSql.getRows() >= 1
                    and tdSql.compareData(0, 0, "2025-01-01 02:00:00")
                    and tdSql.compareData(0, 1, 400)
                    and tdSql.compareData(0, 2, 241.5)
                )
            )

        tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:00:01', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")

        # Test 1: Manual recalculation with time range
        tdLog.info("Test manual recalculation with time range")
        tdSql.execute("recalculate stream rdb.s_interval_manual from '2025-01-01 02:00:00';")
        
        # Verify results after recalculation
        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_interval_manual",
                func=lambda: (
                    tdSql.getRows() == 1
                    and tdSql.compareData(0, 0, "2025-01-01 02:00:00")
                    and tdSql.compareData(0, 1, 401)
                    and tdSql.compareData(0, 2, 240.922693266833)
                )
            )
        # Test 2: Manual recalculation with time range and end time
        tdSql.execute("insert into tdb.mt1 values ('2025-01-01 02:04:00', 10, 100, 1.5, 'normal');")

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_interval_manual",
                func=lambda: (
                    tdSql.getRows() == 2
                    and tdSql.compareData(0, 0, "2025-01-01 02:00:00")
                    and tdSql.compareData(0, 1, 401)
                    and tdSql.compareData(0, 2,  240.922693266833)
                    and tdSql.compareData(1, 0, "2025-01-01 02:02:00")
                    and tdSql.compareData(1, 1, 400)
                    and tdSql.compareData(1, 2, 245.5)
                )
            )
        
        tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:00:02', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:02:03', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        tdSql.execute("recalculate stream rdb.s_interval_manual from '2025-01-01 02:00:00' to '2025-01-01 02:01:00';")
        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_interval_manual",
                func=lambda: (
                    tdSql.getRows() == 2
                    and tdSql.compareData(0, 0, "2025-01-01 02:00:00")
                    and tdSql.compareData(0, 1, 402)
                    and tdSql.compareData(0, 2,  240.348258706468)
                    and tdSql.compareData(1, 0, "2025-01-01 02:02:00")
                    and tdSql.compareData(1, 1, 400)
                    and tdSql.compareData(1, 2, 245.5)
                )
            )