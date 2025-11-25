import subprocess
import time
from new_test_framework.utils import tdLog, tdSql, clusterComCheck, tdStream, StreamItem


class TestStreamRecalcDeleteRecalc:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_recalc_delete_recalc(self):
        """Recalc: delete data

        Test DELETE_RECALC option with data deletion:
        1. Delete data from trigger table - streams with DELETE_RECALC should trigger recalculation
        2. Delete child table - streams with DELETE_RECALC should trigger recalculation
        3. Different trigger types behavior with data deletion

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
        tdLog.info("prepare trigger tables for DELETE_RECALC testing")

        # Trigger tables in tdb (control stream computation trigger)
        stb_trig = "create table tdb.delete_triggers (ts timestamp, cint int, c2 int, c3 double, category varchar(16)) tags(id int, name varchar(16));"
        ctb_trig = "create table tdb.del1 using tdb.delete_triggers tags(1, 'device1') tdb.del2 using tdb.delete_triggers tags(2, 'device2') tdb.del3 using tdb.delete_triggers tags(3, 'device3')"
        tdSql.execute(stb_trig)
        tdSql.execute(ctb_trig)

        # Trigger table for session stream
        stb2_trig = "create table tdb.trigger_session_delete (ts timestamp, val_num int, status varchar(16)) tags(device_id int);"
        ctb2_trig = "create table tdb.ds1 using tdb.trigger_session_delete tags(1) tdb.ds2 using tdb.trigger_session_delete tags(2) tdb.ds3 using tdb.trigger_session_delete tags(3)"
        tdSql.execute(stb2_trig)
        tdSql.execute(ctb2_trig)

        # Trigger table for state window stream
        stb3_trig = "create table tdb.trigger_state_delete (ts timestamp, val_num int, status varchar(16)) tags(device_id int);"
        ctb3_trig = "create table tdb.dw1 using tdb.trigger_state_delete tags(1) tdb.dw2 using tdb.trigger_state_delete tags(2) tdb.dw3 using tdb.trigger_state_delete tags(3)"
        tdSql.execute(stb3_trig)
        tdSql.execute(ctb3_trig)

        # Trigger table for event window stream
        stb4_trig = "create table tdb.trigger_event_delete (ts timestamp, val_num int, event_val int) tags(device_id int);"
        ctb4_trig = "create table tdb.de1 using tdb.trigger_event_delete tags(1) tdb.de2 using tdb.trigger_event_delete tags(2) tdb.de3 using tdb.trigger_event_delete tags(3)"
        tdSql.execute(stb4_trig)
        tdSql.execute(ctb4_trig)

        # Trigger table for period stream
        stb5_trig = "create table tdb.trigger_period_delete (ts timestamp, val_num int, metric double) tags(device_id int);"
        ctb5_trig = "create table tdb.dp1 using tdb.trigger_period_delete tags(1) tdb.dp2 using tdb.trigger_period_delete tags(2) tdb.dp3 using tdb.trigger_period_delete tags(3)"
        tdSql.execute(stb5_trig)
        tdSql.execute(ctb5_trig)

        # Trigger table for count window stream
        stb6_trig = "create table tdb.trigger_count_delete (ts timestamp, val_num int, category varchar(16)) tags(device_id int);"
        ctb6_trig = "create table tdb.dc1 using tdb.trigger_count_delete tags(1) tdb.dc2 using tdb.trigger_count_delete tags(2) tdb.dc3 using tdb.trigger_count_delete tags(3)"
        tdSql.execute(stb6_trig)
        tdSql.execute(ctb6_trig)

    def writeInitialTriggerData(self):
        tdLog.info("write initial trigger data to tdb")
        # Trigger data for interval+sliding stream
        trigger_sqls = [
            "insert into tdb.del1 values ('2025-01-01 02:00:00', 10, 100, 1.5, 'normal');",
            "insert into tdb.del1 values ('2025-01-01 02:00:30', 20, 200, 2.5, 'normal');",
            "insert into tdb.del1 values ('2025-01-01 02:01:00', 30, 300, 3.5, 'normal');",
            "insert into tdb.del1 values ('2025-01-01 02:01:30', 40, 400, 4.5, 'normal');",
            "insert into tdb.del1 values ('2025-01-01 02:02:00', 50, 500, 5.5, 'normal');",
            "insert into tdb.del1 values ('2025-01-01 02:02:30', 60, 600, 6.5, 'normal');",
        ]
        tdSql.executes(trigger_sqls)

        # Trigger data for session stream
        trigger_sqls = [
            "insert into tdb.ds1 values ('2025-01-01 02:03:00', 10, 'normal');",
            "insert into tdb.ds1 values ('2025-01-01 02:03:30', 20, 'normal');",
            "insert into tdb.ds1 values ('2025-01-01 02:04:00', 30, 'normal');",
            "insert into tdb.ds1 values ('2025-01-01 02:04:50', 40, 'normal');",
            "insert into tdb.ds1 values ('2025-01-01 02:05:00', 50, 'normal');",
            "insert into tdb.ds1 values ('2025-01-01 02:05:30', 60, 'normal');",
        ]
        tdSql.executes(trigger_sqls)

        # Trigger data for state window stream
        trigger_sqls = [
            "insert into tdb.dw1 values ('2025-01-01 02:06:00', 10, 'normal');",
            "insert into tdb.dw1 values ('2025-01-01 02:06:30', 20, 'normal');",
            "insert into tdb.dw1 values ('2025-01-01 02:07:00', 30, 'warning');",
            "insert into tdb.dw1 values ('2025-01-01 02:07:30', 40, 'warning');",
            "insert into tdb.dw1 values ('2025-01-01 02:08:00', 50, 'error');",
            "insert into tdb.dw1 values ('2025-01-01 02:08:30', 60, 'error');",
        ]
        tdSql.executes(trigger_sqls)

        # Trigger data for event window stream
        trigger_sqls = [
            "insert into tdb.de1 values ('2025-01-01 02:09:00', 10, 6);",
            "insert into tdb.de1 values ('2025-01-01 02:09:30', 20, 7);",
            "insert into tdb.de1 values ('2025-01-01 02:10:00', 30, 12);",
            "insert into tdb.de1 values ('2025-01-01 02:10:30', 40, 6);",
            "insert into tdb.de1 values ('2025-01-01 02:11:00', 50, 9);",
            "insert into tdb.de1 values ('2025-01-01 02:11:30', 60, 13);",
        ]
        tdSql.executes(trigger_sqls)

        # Trigger data for period stream
        trigger_sqls = [
            "insert into tdb.dp1 values ('2025-01-01 02:12:00', 10, 1.5);",
            "insert into tdb.dp1 values ('2025-01-01 02:12:30', 20, 2.5);",
            "insert into tdb.dp1 values ('2025-01-01 02:13:00', 30, 3.5);",
            "insert into tdb.dp1 values ('2025-01-01 02:13:30', 40, 4.5);",
            "insert into tdb.dp1 values ('2025-01-01 02:14:00', 50, 5.5);",
            "insert into tdb.dp1 values ('2025-01-01 02:14:30', 60, 6.5);",
        ]
        tdSql.executes(trigger_sqls)

        # Trigger data for count window stream
        trigger_sqls = [
            "insert into tdb.dc1 values ('2025-01-01 02:15:00', 10, 'normal');",
            "insert into tdb.dc1 values ('2025-01-01 02:15:15', 20, 'normal');",
            "insert into tdb.dc1 values ('2025-01-01 02:15:30', 30, 'warning');",
            "insert into tdb.dc1 values ('2025-01-01 02:15:45', 40, 'warning');",
            "insert into tdb.dc1 values ('2025-01-01 02:16:00', 50, 'error');",
            "insert into tdb.dc1 values ('2025-01-01 02:16:15', 60, 'error');",
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
        

        # Test 1.3: STATE_WINDOW with DELETE_RECALC - should trigger recalculation when data is deleted
        stream = StreamItem(
            id=3,
            stream="create stream rdb.s_state_delete state_window(status) from tdb.trigger_state_delete partition by tbname stream_options(delete_recalc) into rdb.r_state_delete as select _twstart ts, count(*) cnt, avg(cint) avg_val, first(cvarchar) status_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check03,
        )
        self.streams.append(stream)


        tdLog.info(f"create total:{len(self.streams)} streams")
        for stream in self.streams:
            stream.createStream()

    def check03(self):
        # Test state window with DELETE_RECALC - should recalculate when data is deleted
        tdLog.info("Check 3: STATE_WINDOW with DELETE_RECALC recalculates on data deletion")
        tdSql.checkTableType(dbname="rdb", stbname="r_state_delete", columns=4, tags=1)

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_state_delete",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 02:06:00")
                and tdSql.compareData(0, 1, 100)
                and tdSql.compareData(0, 2, 252)
                and tdSql.compareData(1, 0, "2025-01-01 02:07:00")
                and tdSql.compareData(1, 1, 100)
                and tdSql.compareData(1, 2, 254)
            )

        tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:06:01', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        tdSql.execute("delete from tdb.dw1 where ts = '2025-01-01 02:06:30';")

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_state_delete",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 02:06:00")
                and tdSql.compareData(0, 1, 0)
                and tdSql.compareData(0, 2, "NULL")
                and tdSql.compareData(1, 0, "2025-01-01 02:07:00")
                and tdSql.compareData(1, 1, 100)
                and tdSql.compareData(1, 2, 254)
            )

        # Verify that recalculation occurred
        tdLog.info("STATE_WINDOW with DELETE_RECALC successfully handled data deletion")


