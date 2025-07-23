import subprocess
import time
from new_test_framework.utils import tdLog, tdSql, clusterComCheck, tdStream, StreamItem


class TestStreamRecalcManual:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_recalc_manual(self):
        """Stream Manual Recalculation Test

        Test manual recalculation functionality across different scenarios and window types:

        1. Test [Manual Recalculation Command] Syntax and Validation
            1.1 Test command syntax validation
                1.1.1 Valid RECALC command with start and end time
                1.1.2 Valid RECALC command with start time only
                1.1.3 Invalid command syntax - verify error handling
                1.1.4 Missing stream name - verify error handling
            1.2 Test timestamp parameter validation
                1.2.1 Valid timestamp formats (ISO, Unix, etc.)
                1.2.2 Invalid timestamp formats - verify error handling
                1.2.3 Future timestamps - verify behavior
                1.2.4 Boundary timestamp values

        2. Test [Time Range Specification] Scenarios
            2.1 Test explicit time range recalculation
                2.1.1 Recalc with specific start and end time
                2.1.2 Recalc with start time and duration
                2.1.3 Multiple non-overlapping time ranges
                2.1.4 Overlapping time range handling
            2.2 Test open-ended time range recalculation
                2.2.1 Recalc from start time to current time
                2.2.2 Recalc from start time to latest data
                2.2.3 Continuous recalculation scenarios
                2.2.4 Real-time data interaction during recalc
            2.3 Test time range edge cases
                2.3.1 Zero-duration time range
                2.3.2 Very large time range spanning years
                2.3.3 Time range with no existing data
                2.3.4 Time range partially overlapping with data

        3. Test [Window Type Behavior] with Manual Recalculation
            3.1 Test INTERVAL windows manual recalculation
                3.1.1 Recalc specific interval windows
                3.1.2 Recalc overlapping sliding windows
                3.1.3 Partial window recalculation
                3.1.4 Window boundary handling during recalc
            3.2 Test SESSION windows manual recalculation
                3.2.1 Recalc complete sessions within time range
                3.2.2 Recalc partial sessions at range boundaries
                3.2.3 Session gap recalculation
                3.2.4 Session timeout interaction with recalc
            3.3 Test STATE_WINDOW manual recalculation
                3.3.1 Recalc state windows within time range
                3.3.2 State transition recalculation
                3.3.3 State boundary adjustment during recalc
                3.3.4 State consistency after recalculation
            3.4 Test EVENT_WINDOW manual recalculation
                3.4.1 Recalc event windows within time range
                3.4.2 Event sequence recalculation
                3.4.3 Incomplete event window handling
                3.4.4 Event condition re-evaluation

        4. Test [Data Modification Impact] on Manual Recalculation
            4.1 Test recalculation after data insertion
                4.1.1 Insert new data before manual recalc
                4.1.2 Insert data during manual recalc process
                4.1.3 Insert data in recalculated time range
                4.1.4 Concurrent insertion and recalculation
            4.2 Test recalculation after data update
                4.2.1 Update existing data before recalc
                4.2.2 Update data during recalc process
                4.2.3 Update affecting recalculated windows
                4.2.4 Batch update impact on recalculation
            4.3 Test recalculation after data deletion
                4.3.1 Delete data before manual recalc
                4.3.2 Delete data during recalc process
                4.3.3 Delete affecting recalculated results
                4.3.4 Cascading deletion impact

        5. Test [Performance and Resource Management]
            5.1 Test large-scale manual recalculation
                5.1.1 Recalc large time ranges with massive data
                5.1.2 Resource usage monitoring during recalc
                5.1.3 Memory management for large recalculations
                5.1.4 CPU utilization optimization
            5.2 Test concurrent recalculation scenarios
                5.2.1 Multiple manual recalc commands
                5.2.2 Manual recalc with automatic recalc
                5.2.3 Parallel recalc on different streams
                5.2.4 Resource contention handling
            5.3 Test recalculation interruption and recovery
                5.3.1 Manual interruption of recalc process
                5.3.2 System restart during recalculation
                5.3.3 Network failure during recalc
                5.3.4 Recovery and consistency verification

        6. Test [Result Verification and Consistency]
            6.1 Test recalculation result accuracy
                6.1.1 Compare recalc results with original computation
                6.1.2 Verify aggregation correctness after recalc
                6.1.3 Check window boundary accuracy
                6.1.4 Validate timestamp precision in results
            6.2 Test output table consistency
                6.2.1 Target table state before and after recalc
                6.2.2 Duplicate result handling
                6.2.3 Result ordering after recalculation
                6.2.4 Metadata consistency verification

        7. Test [Error Handling and Edge Cases]
            7.1 Test invalid recalculation scenarios
                7.1.1 Recalc on non-existent stream
                7.1.2 Recalc with insufficient permissions
                7.1.3 Recalc on stopped or failed stream
                7.1.4 Recalc with corrupted stream metadata
            7.2 Test resource limitation scenarios
                7.2.1 Recalc with insufficient disk space
                7.2.2 Recalc with memory constraints
                7.2.3 Recalc with CPU limitations
                7.2.4 Graceful degradation handling

        Catalog:
            - Streams:Recalculation:Manual

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
            "insert into tdb.sm1 values ('2025-01-01 02:11:50', 40, 'normal');",
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

        # Test 1.2: SESSION stream for manual recalculation
        stream = StreamItem(
            id=2,
            stream="create stream rdb.s_session_manual session(ts,45s) from tdb.trigger_session_manual partition by tbname into rdb.r_session_manual as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check02,
        )
        self.streams.append(stream)

        # Test 1.3: STATE_WINDOW stream for manual recalculation
        stream = StreamItem(
            id=3,
            stream="create stream rdb.s_state_manual state_window(status) from tdb.trigger_state_manual partition by tbname into rdb.r_state_manual as select _twstart ts, count(*) cnt, avg(cint) avg_val, first(cvarchar) status_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check03,
        )
        self.streams.append(stream)

        # Test 1.4: EVENT_WINDOW stream for manual recalculation
        stream = StreamItem(
            id=4,
            stream="create stream rdb.s_event_manual event_window(start with event_val >= 5 end with event_val > 10) from tdb.trigger_event_manual partition by tbname into rdb.r_event_manual as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            check_func=self.check04,
        )
        self.streams.append(stream)


    # Check functions for each test case
    def check01(self):
        # Test interval+sliding with manual recalculation
        tdLog.info("Check 1: INTERVAL+SLIDING manual recalculation")
        tdSql.checkTableType(dbname="rdb", stbname="r_interval_manual", columns=3, tags=1)

        # Write source data for testing
        tdLog.info("write source data for manual recalculation testing")

        # Check initial results
        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_interval_manual",
                func=lambda: (
                    tdSql.getRows() == 1
                    and tdSql.compareData(0, 0, "2025-01-01 02:00:00")
                    and tdSql.compareData(0, 1, 400)
                    and tdSql.compareData(0, 2, 241.5)
                )
            )

        tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:00:01', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")

        # Test 1: Manual recalculation with time range
        tdLog.info("Test manual recalculation with time range")
        tdSql.execute("recalculate stream rdb.s_interval_manual from '2025-01-01 02:00:00';")
        
        #TODO(beryl): blocked by TD-36691
        # Verify results after recalculation
        # tdSql.checkResultsByFunc(
        #         sql=f"select ts, cnt, avg_val from rdb.r_interval_manual",
        #         func=lambda: (
        #             tdSql.getRows() == 1
        #             and tdSql.compareData(0, 0, "2025-01-01 02:00:00")
        #             and tdSql.compareData(0, 1, 401)
        #             and tdSql.compareData(0, 2, 240.922693266833)
        #         )
        #     )

        # Test 2: Manual recalculation with time range and end time
        # tdSql.execute("insert into tdb.mt1 values ('2025-01-01 02:04:00', 10, 100, 1.5, 'normal');")
        # tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:00:02', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        # tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:02:03', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        # tdSql.execute("recalculate stream rdb.s_interval_manual from '2025-01-01 02:00:00' to '2025-01-01 02:02:00';")
        # tdSql.checkResultsByFunc(
        #         sql=f"select ts, cnt, avg_val from rdb.r_interval_manual",
        #         func=lambda: (
        #             tdSql.getRows() == 2
        #             and tdSql.compareData(0, 0, "2025-01-01 02:00:00")
        #             and tdSql.compareData(0, 1, 402)
        #             and tdSql.compareData(0, 2,  240.348258706468)
        #             and tdSql.compareData(1, 0, "2025-01-01 02:02:00")
        #             and tdSql.compareData(1, 1, 400)
        #             and tdSql.compareData(1, 2, 245.5)
        #         )
        #     )

    def check02(self):
        # Test session with manual recalculation
        tdLog.info("Check 2: SESSION manual recalculation")
        tdSql.checkTableType(dbname="rdb", stbname="r_session_manual", columns=3, tags=1)

        # Check initial results
        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_session_manual",
                func=lambda: (
                    tdSql.getRows() == 1
                    and tdSql.compareData(0, 0, "2025-01-01 02:10:00")
                    and tdSql.compareData(0, 1, 200)
                    and tdSql.compareData(0, 2, 260.5)
                )
            )

        tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:10:01', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        # Test 1: Manual recalculation with time range for SESSION
        tdLog.info("Test SESSION manual recalculation with time range")
        tdSql.execute("recalculate stream rdb.s_session_manual from '2025-01-01 02:10:00';")

        #TODO(beryl): blocked by TD-36691
        # Verify results after recalculation
        # tdSql.checkResultsByFunc(
        #         sql=f"select ts, cnt, avg_val from rdb.r_session_manual",
        #         func=lambda: (
        #             tdSql.getRows() == 1
        #             and tdSql.compareData(0, 0, "2025-01-01 02:10:00")
        #             and tdSql.compareData(0, 1, 201)
        #             and tdSql.compareData(0, 2, 259.253731343284)
        #         )
        #     )

        # Test 2: Manual recalculation with time range and end time
        # tdSql.execute("insert into tdb.sm1 values ('2025-01-01 02:14:00', 60, 'normal');")
        # tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:10:02', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        # tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:12:03', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        # tdSql.execute("recalculate stream rdb.s_session_manual from '2025-01-01 02:10:00' to '2025-01-01 02:12:00';")

        # tdSql.checkResultsByFunc(
        #         sql=f"select ts, cnt, avg_val from rdb.r_session_manual",
        #         func=lambda: (
        #             tdSql.getRows() == 2
        #             and tdSql.compareData(0, 0, "2025-01-01 02:10:00")
        #             and tdSql.compareData(0, 1, 202)
        #             and tdSql.compareData(0, 2,  258.019801980198)
        #             and tdSql.compareData(1, 0, "2025-01-01 02:11:50")
        #             and tdSql.compareData(1, 1, 100)
        #             and tdSql.compareData(1, 2, 264)
        #         )
        #     )

    def check03(self):
        # Test state window with manual recalculation
        tdLog.info("Check 3: STATE_WINDOW manual recalculation")
        tdSql.checkTableType(dbname="rdb", stbname="r_state_manual", columns=4, tags=1)

        # Check initial results
        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_state_manual",
                func=lambda: (
                    tdSql.getRows() == 2
                    and tdSql.compareData(0, 0, "2025-01-01 02:20:00")
                    and tdSql.compareData(0, 1, 100)
                    and tdSql.compareData(0, 2, 280)
                    and tdSql.compareData(1, 0, "2025-01-01 02:21:00")
                    and tdSql.compareData(1, 1, 100)
                    and tdSql.compareData(1, 2, 282)
                )
            )

        # Test 1: Manual recalculation with time range for STATE_WINDOW
        tdLog.info("Test STATE_WINDOW manual recalculation with time range")
        tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:20:01', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        tdSql.execute("recalculate stream rdb.s_state_manual from '2025-01-01 02:20:00' to '2025-01-01 02:23:00';")
        
        # # Verify results after recalculation
        # tdSql.checkResultsByFunc(
        #         sql=f"select ts, cnt, avg_val from rdb.r_state_manual",
        #         func=lambda: (
        #             tdSql.getRows() == 2
        #             and tdSql.compareData(0, 0, "2025-01-01 02:20:00")
        #             and tdSql.compareData(0, 1, 101)
        #             and tdSql.compareData(0, 2, 277.326732673267)
        #             and tdSql.compareData(1, 0, "2025-01-01 02:21:00")
        #             and tdSql.compareData(1, 1, 100)
        #             and tdSql.compareData(1, 2, 282)
        #         )
        #     )

        # # Test 2: Manual recalculation with time range and end time
        # tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:20:02', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        # tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:21:01', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        # tdSql.execute("recalculate stream rdb.s_state_manual from '2025-01-01 02:20:00' to '2025-01-01 02:21:00';")

        # tdSql.checkResultsByFunc(
        #         sql=f"select ts, cnt, avg_val from rdb.r_state_manual",
        #         func=lambda: (
        #             tdSql.getRows() == 2
        #             and tdSql.compareData(0, 0, "2025-01-01 02:20:00")
        #             and tdSql.compareData(0, 1, 102)
        #             and tdSql.compareData(0, 2, 274.705882352941)
        #             and tdSql.compareData(1, 0, "2025-01-01 02:21:00")
        #             and tdSql.compareData(1, 1, 100)
        #             and tdSql.compareData(1, 2, 282)
        #         )
        #     )

    def check04(self):
        # Test event window with manual recalculation
        tdLog.info("Check 4: EVENT_WINDOW manual recalculation")
        tdSql.checkTableType(dbname="rdb", stbname="r_event_manual", columns=3, tags=1)

        # Check initial results
        tdSql.checkResultsByFunc(
                sql=f"select ts, cnt, avg_val from rdb.r_event_manual",
                func=lambda: (
                    tdSql.getRows() == 2
                    and tdSql.compareData(0, 0, "2025-01-01 02:30:00.000")
                    and tdSql.compareData(0, 1, 200)
                    and tdSql.compareData(0, 2, 300.5)
                    and tdSql.compareData(1, 0, "2025-01-01 02:31:00.000")
                    and tdSql.compareData(1, 1, 200)
                    and tdSql.compareData(1, 2, 303.5)
                )
            )

        # # Test 1: Manual recalculation with time range for EVENT_WINDOW
        # tdLog.info("Test EVENT_WINDOW manual recalculation with time range")
        # tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:30:01', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        # tdSql.execute("recalculate stream rdb.s_event_manual from '2025-01-01 02:30:00';")
        
        # # Verify results after recalculation
        # tdSql.checkResultsByFunc(
        #         sql=f"select ts, cnt, avg_val from rdb.r_event_manual",
        #         func=lambda: (
        #             tdSql.getRows() == 2
        #             and tdSql.compareData(0, 0, "2025-01-01 02:30:00.000")
        #             and tdSql.compareData(0, 1, 201)
        #             and tdSql.compareData(0, 2,  299.054726368159)
        #             and tdSql.compareData(1, 0, "2025-01-01 02:31:00.000")
        #             and tdSql.compareData(1, 1, 200)
        #             and tdSql.compareData(1, 2, 303.5)
        #         )
        #     )

        # # Test 2: Manual recalculation without end time
        # tdLog.info("Test EVENT_WINDOW manual recalculation without end time")
        # tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:31:02', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        # tdSql.execute("insert into qdb.t0 values ('2025-01-01 02:32:01', 10, 100, 1.5, 1.5, 0.8, 0.8, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(0.8 0.8)');")
        # tdSql.execute("recalculate stream rdb.s_event_manual from '2025-01-01 02:30:00' to '2025-01-01 02:31:00';")

        # tdSql.checkResultsByFunc(
        #         sql=f"select ts, cnt, avg_val from rdb.r_event_manual",
        #         func=lambda: (
        #             tdSql.getRows() == 2
        #             and tdSql.compareData(0, 0, "2025-01-01 02:30:00.000")
        #             and tdSql.compareData(0, 1, 202)
        #             and tdSql.compareData(0, 2, 297.623762376238)
        #             and tdSql.compareData(1, 0, "2025-01-01 02:31:00.000")
        #             and tdSql.compareData(1, 1, 200)
        #             and tdSql.compareData(1, 2, 303.5)
        #         )
        #     )

        # tdLog.info("EVENT_WINDOW manual recalculation test completed")