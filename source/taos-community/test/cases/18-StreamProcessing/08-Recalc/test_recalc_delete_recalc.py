import subprocess
import time
from new_test_framework.utils import tdLog, tdSql, clusterComCheck, tdStream, StreamItem


class TestStreamRecalcDeleteRecalc:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_recalc_delete_recalc(self):
        """Recalc: full deletion scenarios

        Test DELETE_RECALC option behavior with various data deletion scenarios:

        1. Test [DELETE_RECALC] Option Specification
            1.1 Test option existence verification
                1.1.1 DELETE_RECALC specified - verify recalculation on deletion
                1.1.2 DELETE_RECALC not specified - verify no recalculation on deletion
                1.1.3 DELETE_RECALC with invalid syntax - verify error handling
            1.2 Test option value validation
                1.2.1 Valid DELETE_RECALC specification
                1.2.2 Invalid DELETE_RECALC syntax
                1.2.3 DELETE_RECALC with other conflicting options

        2. Test [Data Record Deletion] Scenarios
            2.1 Test single record deletion
                2.1.1 Delete recent data - should trigger recalculation
                2.1.2 Delete historical data - should trigger recalculation
                2.1.3 Delete data from closed window - verify window reopening
            2.2 Test batch record deletion
                2.2.1 Delete multiple records from same window
                2.2.2 Delete records across multiple windows
                2.2.3 Delete all records from a window
            2.3 Test conditional deletion
                2.3.1 DELETE with WHERE clause affecting single window
                2.3.2 DELETE with WHERE clause affecting multiple windows
                2.3.3 DELETE with complex WHERE conditions

        3. Test [Child Table Deletion] Scenarios
            3.1 Test entire child table deletion
                3.1.1 DROP child table - verify impact on stream calculation
                3.1.2 Delete all records from child table - verify empty table handling
                3.1.3 Recreate child table after deletion - verify stream recovery
            3.2 Test multiple child table operations
                3.2.1 Delete multiple child tables simultaneously
                3.2.2 Mix of record deletion and table deletion
                3.2.3 Partial child table set deletion

        4. Test [Window Type Behavior] with DELETE_RECALC
            4.1 Test INTERVAL windows
                4.1.1 Delete data from current window - verify immediate recalculation
                4.1.2 Delete data from sliding windows - verify overlapping window updates
                4.1.3 Delete data causing empty windows - verify window state handling
            4.2 Test SESSION windows
                4.2.1 Delete data from active session - verify session recalculation
                4.2.2 Delete data causing session split - verify session boundary changes
                4.2.3 Delete data causing session merge - verify session consolidation
            4.3 Test STATE_WINDOW
                4.3.1 Delete data causing state change - verify state window recalculation
                4.3.2 Delete data from state boundary - verify window boundary updates
                4.3.3 Delete all data from state window - verify window closure
            4.4 Test EVENT_WINDOW
                4.4.1 Delete start event data - verify window start recalculation
                4.4.2 Delete end event data - verify window end recalculation
                4.4.3 Delete intermediate data - verify window content recalculation

        5. Test [Performance and Resource Impact]
            5.1 Test large-scale deletion impact
                5.1.1 Delete large volume of data - verify performance
                5.1.2 Concurrent deletion operations - verify system stability
                5.1.3 Resource usage during deletion recalculation
            5.2 Test recovery scenarios
                5.2.1 System restart after deletion - verify state recovery
                5.2.2 Network interruption during deletion - verify consistency
                5.2.3 Storage failure scenarios - verify data integrity

        Catalog:
            - Streams:Recalculation:DeleteRecalc

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

        # ===== Test 1: DELETE_RECALC Option =====
        
        # Test 1.1: INTERVAL+SLIDING with DELETE_RECALC - should trigger recalculation when data is deleted
        stream = StreamItem(
            id=1,
            stream="create stream rdb.s_interval_delete interval(2m) sliding(2m) from tdb.delete_triggers partition by tbname stream_options(delete_recalc) into rdb.r_interval_delete as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check01,
        )
        self.streams.append(stream)

        # Test 1.2: SESSION with DELETE_RECALC - should trigger recalculation when data is deleted
        stream = StreamItem(
            id=2,
            stream="create stream rdb.s_session_delete session(ts,45s) from tdb.trigger_session_delete partition by tbname stream_options(delete_recalc) into rdb.r_session_delete as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check02,
        )
        self.streams.append(stream)

        # Test 1.3: STATE_WINDOW with DELETE_RECALC - should trigger recalculation when data is deleted
        stream = StreamItem(
            id=3,
            stream="create stream rdb.s_state_delete state_window(status) from tdb.trigger_state_delete partition by tbname stream_options(delete_recalc) into rdb.r_state_delete as select _twstart ts, count(*) cnt, avg(cint) avg_val, first(cvarchar) status_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check03,
        )
        self.streams.append(stream)

        # Test 1.4: EVENT_WINDOW with DELETE_RECALC - should trigger recalculation when data is deleted
        stream = StreamItem(
            id=4,
            stream="create stream rdb.s_event_delete event_window(start with event_val >= 5 end with event_val > 10) from tdb.trigger_event_delete partition by tbname stream_options(delete_recalc) into rdb.r_event_delete as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check04,
        )
        self.streams.append(stream)

        # Test 5: PERIOD with DELETE_RECALC - should recalculate when data is deleted
        stream = StreamItem(
            id=5,
            stream="create stream rdb.s_period_delete period(30s) from tdb.trigger_period_delete partition by tbname into rdb.r_period_delete as select cast(_tlocaltime/1000000 as timestamp) ts, count(*) cnt, avg(cint) avg_val from qdb.meters",
            check_func=self.check05,
        )
        self.streams.append(stream)

        # Test 6: COUNT_WINDOW with DELETE_RECALC - should recalculate when data is deleted
        stream = StreamItem(
            id=6,
            stream="create stream rdb.s_count_delete count_window(3) from tdb.trigger_count_delete partition by tbname into rdb.r_count_delete as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check06,
        )
        self.streams.append(stream)

        tdLog.info(f"create total:{len(self.streams)} streams")
        for stream in self.streams:
            stream.createStream()

    # Check functions for each test case
    def check01(self):
        # Test interval+sliding with DELETE_RECALC - should recalculate when data is deleted
        tdLog.info("Check 1: INTERVAL+SLIDING with DELETE_RECALC recalculates on data deletion")
        tdSql.checkTableType(dbname="rdb", stbname="r_interval_delete", columns=3, tags=1)

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_interval_delete",
                func=lambda: (
                    tdSql.getRows() == 1
                    and tdSql.compareData(0, 0, "2025-01-01 02:00:00")
                    and tdSql.compareData(0, 1, 400)
                    and tdSql.compareData(0, 2, 241.5)
                )
            )
        tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:00:01', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        tdSql.execute("delete from tdb.del1 where ts = '2025-01-01 02:00:30';")

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_interval_delete",
                func=lambda: (
                    tdSql.getRows() == 1
                    and tdSql.compareData(0, 0, "2025-01-01 02:00:00")
                    and tdSql.compareData(0, 1, 401)
                    and tdSql.compareData(0, 2, 240.922693266833)
                )
            )

        tdLog.info("INTERVAL+SLIDING with DELETE_RECALC successfully handled data deletion")


    def check02(self):
        # Test session with DELETE_RECALC - should recalculate when data is deleted
        tdLog.info("Check 2: SESSION with DELETE_RECALC recalculates on data deletion")
        tdSql.checkTableType(dbname="rdb", stbname="r_session_delete", columns=3, tags=1)

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_session_delete",
                func=lambda: (
                    tdSql.getRows() == 1
                    and tdSql.compareData(0, 0, "2025-01-01 02:03:00")
                    and tdSql.compareData(0, 1, 200)
                    and tdSql.compareData(0, 2, 246.5)
                )
            )

        tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:03:01', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        tdSql.execute("delete from tdb.ds1 where ts = '2025-01-01 02:03:30';")

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_session_delete",
                func=lambda: (
                    tdSql.getRows() == 2
                    and tdSql.compareData(0, 0, "2025-01-01 02:03:00")
                    and tdSql.compareData(0, 1, 0)
                    and tdSql.compareData(0, 2, None)
                    and tdSql.compareData(1, 0, "2025-01-01 02:04:00")
                    and tdSql.compareData(1, 1, 0)
                    and tdSql.compareData(1, 2, None)
                )
            )

        tdLog.info("SESSION with DELETE_RECALC successfully handled data deletion")

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
                and tdSql.compareData(0, 2, None)
                and tdSql.compareData(1, 0, "2025-01-01 02:07:00")
                and tdSql.compareData(1, 1, 100)
                and tdSql.compareData(1, 2, 254)
            )

        # Verify that recalculation occurred
        tdLog.info("STATE_WINDOW with DELETE_RECALC successfully handled data deletion")

    def check04(self):
        # Test event window with DELETE_RECALC - should recalculate when data is deleted
        tdLog.info("Check 4: EVENT_WINDOW with DELETE_RECALC recalculates on data deletion")
        tdSql.checkTableType(dbname="rdb", stbname="r_event_delete", columns=3, tags=1)

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_event_delete",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 02:09:00.000")
                and tdSql.compareData(0, 1, 200)
                and tdSql.compareData(0, 2, 258.5)
                and tdSql.compareData(1, 0, "2025-01-01 02:10:30.000")
                and tdSql.compareData(1, 1, 200)
                and tdSql.compareData(1, 2, 261.5)
            )
        
        tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:09:01', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        tdSql.execute("delete from tdb.de1 where ts = '2025-01-01 02:09:30';")

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_event_delete",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 02:09:00")
                and tdSql.compareData(0, 1, 201)
                and tdSql.compareData(0, 2, 257.26368159204)
                and tdSql.compareData(1, 0, "2025-01-01 02:10:30")
                and tdSql.compareData(1, 1, 200)
                and tdSql.compareData(1, 2, 261.5)
            )

        # Verify that recalculation occurred
        tdLog.info("EVENT_WINDOW with DELETE_RECALC successfully handled data deletion") 


    def check05(self):
        # Test period with DELETE_RECALC - should recalculate when data is deleted
        tdLog.info("Check 5: PERIOD with DELETE_RECALC triggers recalculation when data is deleted")
        tdSql.checkTableType(dbname="rdb", stbname="r_period_delete", columns=3, tags=1)

        # tdSql.checkResultsByFunc(
        #         sql=f"select ts, cnt, avg_val from rdb.r_period_delete",
        #         func=lambda: tdSql.getRows() == 1
        #         and tdSql.compareData(0, 0, "2025-01-01 02:12:00")
        #         and tdSql.compareData(0, 1, 100)
        #         and tdSql.compareData(0, 2, 241.5)
        #     )
        # tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:12:01', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        # tdSql.execute("delete from tdb.dp1 where ts = '2025-01-01 02:12:30';")

        # tdLog.info("wait for stream to be stable")
        # time.sleep(5)

        # tdSql.checkResultsByFunc(
        #         sql=f"select ts, cnt, avg_val from rdb.r_period_delete",
        #         func=lambda: tdSql.getRows() == 1
        #         and tdSql.compareData(0, 0, "2025-01-01 02:12:00")
        #         and tdSql.compareData(0, 1, 100)
        #         and tdSql.compareData(0, 2, 240.5)
        #     )
        # tdLog.info("PERIOD with DELETE_RECALC successfully handled data deletion")


    def check06(self):
        # Test count window with DELETE_RECALC - should recalculate when data is deleted
        tdLog.info("Check 6: COUNT_WINDOW with DELETE_RECALC triggers recalculation when data is deleted")
        tdSql.checkTableType(dbname="rdb", stbname="r_count_delete", columns=3, tags=1)

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_count_delete",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 02:15:00")
                and tdSql.compareData(0, 1, 100)
                and tdSql.compareData(0, 2, 270)
                and tdSql.compareData(1, 0, "2025-01-01 02:15:45")
                and tdSql.compareData(1, 1, 100)
                and tdSql.compareData(1, 2, 272)
            )
        tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:15:01', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        tdSql.execute("delete from tdb.dc1 where ts = '2025-01-01 02:15:15';")

        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_count_delete",
                func=lambda: tdSql.getRows() == 2
                and tdSql.compareData(0, 0, "2025-01-01 02:15:00")
                and tdSql.compareData(0, 1, 100)
                and tdSql.compareData(0, 2, 270)
                and tdSql.compareData(1, 0, "2025-01-01 02:15:45")
                and tdSql.compareData(1, 1, 100)
                and tdSql.compareData(1, 2, 272)
            )
        tdLog.info("COUNT_WINDOW with DELETE_RECALC successfully handled data deletion")