import subprocess
import time
from new_test_framework.utils import tdLog, tdSql, clusterComCheck, tdStream, StreamItem


class TestStreamRecalcWithOptions:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_recalc_with_options(self):
        """Recalc: manual with options

        Test manual recalculation functionality combined with four different stream options:

        1. Manual Recalculation with WATERMARK Option Test
            1.1 Create interval(2m) sliding(2m) stream with watermark(30s) (s_watermark_interval)
            1.2 Test manual recalculation behavior within watermark tolerance
            1.3 Verify watermark option interaction with manual recalc commands

        2. Manual Recalculation with EXPIRED_TIME Option Test
            2.1 Create interval(2m) sliding(2m) stream with expired_time(5m) (s_expired_interval)
            2.2 Test manual recalculation for expired data processing
            2.3 Verify expired_time option behavior during manual recalc

        3. Manual Recalculation with IGNORE_DISORDER Option Test
            3.1 Create interval(2m) sliding(2m) stream with ignore_disorder (s_disorder_interval)
            3.2 Test manual recalculation for previously ignored out-of-order data
            3.3 Verify disorder handling during manual recalc operations

        4. Manual Recalculation without DELETE_RECALC Option Test
            4.1 Create interval(2m) sliding(2m) stream without DELETE_RECALC (s_delete_interval)
            4.2 Test manual recalculation behavior after data deletion
            4.3 Verify recalculation consistency without automatic deletion handling

        Catalog:
            - Streams:Recalculation:ManualWithOptions

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
        tdLog.info("prepare trigger tables for manual recalculation with options testing")

        # Trigger tables for WATERMARK testing
        stb_watermark = "create table tdb.watermark_triggers (ts timestamp, cint int, c2 int, c3 double, category varchar(16)) tags(id int, name varchar(16));"
        ctb_watermark = "create table tdb.wm1 using tdb.watermark_triggers tags(1, 'device1') tdb.wm2 using tdb.watermark_triggers tags(2, 'device2') tdb.wm3 using tdb.watermark_triggers tags(3, 'device3')"
        tdSql.execute(stb_watermark)
        tdSql.execute(ctb_watermark)

        # Trigger tables for EXPIRED_TIME testing
        stb_expired = "create table tdb.expired_triggers (ts timestamp, cint int, c2 int, c3 double, category varchar(16)) tags(id int, name varchar(16));"
        ctb_expired = "create table tdb.exp1 using tdb.expired_triggers tags(1, 'device1') tdb.exp2 using tdb.expired_triggers tags(2, 'device2') tdb.exp3 using tdb.expired_triggers tags(3, 'device3')"
        tdSql.execute(stb_expired)
        tdSql.execute(ctb_expired)

        # Trigger tables for IGNORE_DISORDER testing
        stb_disorder = "create table tdb.disorder_triggers (ts timestamp, cint int, c2 int, c3 double, category varchar(16)) tags(id int, name varchar(16));"
        ctb_disorder = "create table tdb.dis1 using tdb.disorder_triggers tags(1, 'device1') tdb.dis2 using tdb.disorder_triggers tags(2, 'device2') tdb.dis3 using tdb.disorder_triggers tags(3, 'device3')"
        tdSql.execute(stb_disorder)
        tdSql.execute(ctb_disorder)

        # Trigger tables for DELETE_RECALC testing
        stb_delete = "create table tdb.delete_triggers (ts timestamp, cint int, c2 int, c3 double, category varchar(16)) tags(id int, name varchar(16));"
        ctb_delete = "create table tdb.del1 using tdb.delete_triggers tags(1, 'device1') tdb.del2 using tdb.delete_triggers tags(2, 'device2') tdb.del3 using tdb.delete_triggers tags(3, 'device3')"
        tdSql.execute(stb_delete)
        tdSql.execute(ctb_delete)

        # Additional trigger tables for session window with options
        stb_session_opt = "create table tdb.session_opt_triggers (ts timestamp, val_num int, status varchar(16)) tags(device_id int);"
        ctb_session_opt = "create table tdb.so1 using tdb.session_opt_triggers tags(1) tdb.so2 using tdb.session_opt_triggers tags(2) tdb.so3 using tdb.session_opt_triggers tags(3)"
        tdSql.execute(stb_session_opt)
        tdSql.execute(ctb_session_opt)

    def writeInitialTriggerData(self):
        tdLog.info("write initial trigger data to tdb")
        
        # Initial data for WATERMARK stream testing
        trigger_sqls = [
            "insert into tdb.wm1 values ('2025-01-01 02:00:00', 10, 100, 1.5, 'normal');",
            "insert into tdb.wm1 values ('2025-01-01 02:00:30', 20, 200, 2.5, 'normal');",
            "insert into tdb.wm1 values ('2025-01-01 02:01:00', 30, 300, 3.5, 'normal');",
            "insert into tdb.wm1 values ('2025-01-01 02:01:30', 40, 400, 4.5, 'normal');",
            "insert into tdb.wm1 values ('2025-01-01 02:02:00', 50, 500, 5.5, 'normal');",
            "insert into tdb.wm1 values ('2025-01-01 02:02:30', 60, 600, 6.5, 'normal');",
        ]
        tdSql.executes(trigger_sqls)

        # Initial data for EXPIRED_TIME stream testing
        trigger_sqls = [
            "insert into tdb.exp1 values ('2025-01-01 02:10:00', 10, 100, 1.5, 'normal');",
            "insert into tdb.exp1 values ('2025-01-01 02:10:30', 20, 200, 2.5, 'normal');",
            "insert into tdb.exp1 values ('2025-01-01 02:11:00', 30, 300, 3.5, 'normal');",
            "insert into tdb.exp1 values ('2025-01-01 02:11:30', 40, 400, 4.5, 'normal');",
            "insert into tdb.exp1 values ('2025-01-01 02:12:00', 50, 500, 5.5, 'normal');",
            "insert into tdb.exp1 values ('2025-01-01 02:12:30', 60, 600, 6.5, 'normal');",
        ]
        tdSql.executes(trigger_sqls)

        # Initial data for IGNORE_DISORDER stream testing  
        trigger_sqls = [
            "insert into tdb.dis1 values ('2025-01-01 02:20:00', 10, 100, 1.5, 'normal');",
            "insert into tdb.dis1 values ('2025-01-01 02:20:30', 20, 200, 2.5, 'normal');",
            "insert into tdb.dis1 values ('2025-01-01 02:21:00', 30, 300, 3.5, 'normal');",
            "insert into tdb.dis1 values ('2025-01-01 02:21:30', 40, 400, 4.5, 'normal');",
            "insert into tdb.dis1 values ('2025-01-01 02:22:00', 50, 500, 5.5, 'normal');",
            "insert into tdb.dis1 values ('2025-01-01 02:22:30', 60, 600, 6.5, 'normal');",
        ]
        tdSql.executes(trigger_sqls)

        # Initial data for DELETE_RECALC stream testing
        trigger_sqls = [
            "insert into tdb.del1 values ('2025-01-01 02:30:00', 10, 100, 1.5, 'normal');",
            "insert into tdb.del1 values ('2025-01-01 02:30:30', 20, 200, 2.5, 'normal');",
            "insert into tdb.del1 values ('2025-01-01 02:31:00', 30, 300, 3.5, 'normal');",
            "insert into tdb.del1 values ('2025-01-01 02:31:50', 40, 400, 4.5, 'normal');",
            "insert into tdb.del1 values ('2025-01-01 02:32:00', 50, 500, 5.5, 'normal');",
            "insert into tdb.del1 values ('2025-01-01 02:32:30', 60, 600, 6.5, 'normal');",
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

        # ===== Test 1: Manual Recalculation with WATERMARK =====
        
        # Test 1.1: INTERVAL stream with WATERMARK - manual recalculation within tolerance
        stream = StreamItem(
            id=1,
            stream="create stream rdb.s_watermark_interval interval(2m) sliding(2m) from tdb.watermark_triggers partition by tbname stream_options(watermark(30s)) into rdb.r_watermark_interval as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check01,
        )
        self.streams.append(stream)


        # ===== Test 2: Manual Recalculation with EXPIRED_TIME =====
        
        # Test 2.1: INTERVAL stream with EXPIRED_TIME - manual recalculation for expired data
        stream = StreamItem(
            id=3,
            stream="create stream rdb.s_expired_interval interval(2m) sliding(2m) from tdb.expired_triggers partition by tbname stream_options(expired_time(5m)) into rdb.r_expired_interval as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check03,
        )
        self.streams.append(stream)

        # ===== Test 3: Manual Recalculation with IGNORE_DISORDER =====
        
        # Test 3.1: INTERVAL stream with IGNORE_DISORDER - manual recalculation for ignored disordered data
        stream = StreamItem(
            id=4,
            stream="create stream rdb.s_disorder_interval interval(2m) sliding(2m) from tdb.disorder_triggers partition by tbname stream_options(ignore_disorder) into rdb.r_disorder_interval as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check04,
        )
        self.streams.append(stream)

        # ===== Test 4: Manual Recalculation without DELETE_RECALC =====
        
        # Test 4.1: INTERVAL stream without DELETE_RECALC - manual recalculation for deleted data
        stream = StreamItem(
            id=5,
            stream="create stream rdb.s_delete_interval session(ts,45s) from tdb.delete_triggers partition by tbname into rdb.r_delete_interval as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check05,
        )
        self.streams.append(stream)

        tdLog.info(f"create total:{len(self.streams)} streams")
        for stream in self.streams:
            stream.createStream()

    # Check functions for each test case
    def check01(self):
        # Test WATERMARK with manual recalculation
        tdLog.info("Check 1: WATERMARK manual recalculation")
        tdSql.checkTableType(dbname="rdb", stbname="r_watermark_interval", columns=3, tags=1)

        # Check initial results
        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_watermark_interval",
                func=lambda: (
                    tdSql.getRows() == 1
                    and tdSql.compareData(0, 0, "2025-01-01 02:00:00")
                    and tdSql.compareData(0, 1, 400)
                    and tdSql.compareData(0, 2, 241.5)
                )
            )

        # Test 1: Write disordered data within WATERMARK tolerance (30s)
        tdLog.info("Write disordered data within WATERMARK tolerance")
        tdSql.execute("insert into tdb.wm1 values ('2025-01-01 02:04:00', 25, 250, 2.75, 'normal');")
        tdSql.execute("insert into tdb.wm1 values ('2025-01-01 02:04:15', 35, 350, 3.75, 'normal');")
        tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:03:15', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        
        # Manual recalculation within WATERMARK range - should recalculate
        tdLog.info("Test WATERMARK manual recalculation - within tolerance range")
        tdSql.execute("recalculate stream rdb.s_watermark_interval from '2025-01-01 02:00:00' to '2025-01-01 02:05:00';")
        
        # recalc can not recalc the data that is inside the watermark range
        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_watermark_interval",
                func=lambda: (
                    tdSql.getRows() == 1
                    and tdSql.compareData(0, 0, "2025-01-01 02:00:00")
                    and tdSql.compareData(0, 1, 400)
                    and tdSql.compareData(0, 2, 241.5)
                )
            )

    def check03(self):
        # Test EXPIRED_TIME with manual recalculation
        tdLog.info("Check 3: EXPIRED_TIME manual recalculation")
        tdSql.checkTableType(dbname="rdb", stbname="r_expired_interval", columns=3, tags=1)

        # Check initial results
        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_expired_interval",
                func=lambda: (
                    tdSql.getRows() == 1
                    and tdSql.compareData(0, 0, "2025-01-01 02:10:00")
                    and tdSql.compareData(0, 1, 400)
                    and tdSql.compareData(0, 2, 261.5)
                )
            )

        
        # Test 2: Write expired data (older than 5m)
        tdLog.info("Write expired data beyond EXPIRED_TIME")
        # Note: This test simulates that we're now at a much later time, making earlier data expired
        # In real scenario, the current stream processing time would determine what's expired
        tdSql.execute("insert into tdb.exp1 values ('2025-01-01 02:04:00', 15, 150, 1.75, 'expired');")
        
        time.sleep(5)
        # Manual recalculation for expired data - should still work since manual recalc bypasses expiry
        tdLog.info("Test EXPIRED_TIME manual recalculation - expired data")
        tdSql.execute("recalculate stream rdb.s_expired_interval from '2025-01-01 02:04:00' to '2025-01-01 02:14:00';")
        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_expired_interval",
                func=lambda: (
                    tdSql.getRows() == 4
                    and tdSql.compareData(0, 0, "2025-01-01 02:04:00")
                    and tdSql.compareData(0, 1, 400)
                    and tdSql.compareData(0, 2, 249.5)
                    and tdSql.compareData(1, 0, "2025-01-01 02:06:00")
                    and tdSql.compareData(1, 1, 400)
                    and tdSql.compareData(1, 2, 253.5)
                    and tdSql.compareData(2, 0, "2025-01-01 02:08:00")
                    and tdSql.compareData(2, 1, 400)
                    and tdSql.compareData(2, 2, 257.5)
                    and tdSql.compareData(3, 0, "2025-01-01 02:10:00")
                    and tdSql.compareData(3, 1, 400)
                    and tdSql.compareData(3, 2, 261.5)
                )
            )

    def check04(self):
        # Test IGNORE_DISORDER with manual recalculation
        tdLog.info("Check 4: IGNORE_DISORDER manual recalculation")
        tdSql.checkTableType(dbname="rdb", stbname="r_disorder_interval", columns=3, tags=1)

        # Check initial results
        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_disorder_interval",
                func=lambda: (
                    tdSql.getRows() == 1
                    and tdSql.compareData(0, 0, "2025-01-01 02:20:00")
                    and tdSql.compareData(0, 1, 400)
                    and tdSql.compareData(0, 2, 281.5)
                )
            )

        # Test 1: Write disordered data that would normally be ignored
        tdLog.info("Write disordered data that is normally ignored")
        tdSql.execute("insert into tdb.dis1 values ('2025-01-01 02:18:00', 25, 250, 2.75, 'disorder');")
        
        # Manual recalculation - should process ignored disordered data
        tdLog.info("Test IGNORE_DISORDER manual recalculation - should process ignored data")
        tdSql.execute("recalculate stream rdb.s_disorder_interval from '2025-01-01 02:16:00' to '2025-01-01 02:24:00';")

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_disorder_interval",
                func=lambda: (
                    tdSql.getRows() == 2
                    and tdSql.compareData(0, 0, "2025-01-01 02:18:00")
                    and tdSql.compareData(0, 1, 400)
                    and tdSql.compareData(0, 2, 277.5)
                    and tdSql.compareData(1, 0, "2025-01-01 02:20:00")
                    and tdSql.compareData(1, 1, 400)
                    and tdSql.compareData(1, 2, 281.5)
                )
            )
        time.sleep(2)
        

    def check05(self):
        # Test manual recalculation without DELETE_RECALC option
        tdLog.info("Check 5: Manual recalculation without DELETE_RECALC")
        tdSql.checkTableType(dbname="rdb", stbname="r_delete_interval", columns=3, tags=1)

        # Check initial results
        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_delete_interval",
                func=lambda: (
                    tdSql.getRows() == 1
                    and tdSql.compareData(0, 0, "2025-01-01 02:30:00")
                    and tdSql.compareData(0, 1, 200)
                    and tdSql.compareData(0, 2, 300.5)
                )
            )
        
        tdSql.execute("delete from tdb.del1 where ts = '2025-01-01 02:30:30';")
        
        tdSql.execute("recalculate stream rdb.s_delete_interval from '2025-01-01 02:28:00' to '2025-01-01 02:34:00';")
        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_delete_interval",
                func=lambda: (
                    tdSql.getRows() == 2
                    and tdSql.compareData(0, 0, "2025-01-01 02:30:00")
                    and tdSql.compareData(0, 1, 0)
                    and tdSql.compareData(0, 2, None)
                    and tdSql.compareData(1, 0, "2025-01-01 02:31:00")
                    and tdSql.compareData(1, 1, 0)
                    and tdSql.compareData(1, 2, None)
                )
            )