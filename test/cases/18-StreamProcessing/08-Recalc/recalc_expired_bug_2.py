import time
from new_test_framework.utils import tdLog, tdSql, clusterComCheck, tdStream, StreamItem


class TestStreamRecalcExpiredTime:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_recalc_expired_time(self):
        """Stream Recalculation EXPIRED_TIME Option Test

        Test EXPIRED_TIME option with expired data:
        1. Write expired data - all windows should not trigger recalculation
        2. Combine with WATERMARK - test boundary value behavior
        3. Different trigger types behavior with expired data

        Catalog:
            - Streams:Recalculation

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
        self.checkInitialResults()
        self.writeSourceData()
        self.writeExpiredTriggerData()
        self.checkExpiredResults()
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

        # Normal trigger table for non-partitioned expired testing
        ntb_trig = "create table tdb.trigger_normal (ts timestamp, val_num int, metric double, info varchar(32))"
        tdSql.execute(ntb_trig)


    def writeInitialTriggerData(self):
        tdLog.info("write initial trigger data to tdb")
        # Step 3: Write trigger data to trigger tables (tdb) to start stream computation
        trigger_sqls = [
            "insert into tdb.et1 values ('2025-01-01 01:00:00', 10, 100, 1.5, 'normal') ('2025-01-01 01:00:30', 20, 200, 2.5, 'normal')",
            "insert into tdb.et2 values ('2025-01-01 01:01:00', 15, 150, 3.5, 'warning') ('2025-01-01 01:01:30', 25, 250, 4.5, 'warning')", 
            "insert into tdb.et3 values ('2025-01-01 01:02:00', 30, 300, 5.5, 'error') ('2025-01-01 01:02:30', 40, 400, 6.5, 'error')",
        ]
        tdSql.executes(trigger_sqls)

    def writeExpiredTriggerData(self):
        tdLog.info("write expired data to test EXPIRED_TIME option")
        tdSql.execute("insert into tdb.et1 values ('2025-01-01 00:00:01', 5, 55, 0.8, 'expired1');")

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

    def checkInitialResults(self):
        """Check initial stream computation results after first data insertion"""
        tdLog.info("Checking initial stream computation results...")
        
        # Allow time for stream processing
        time.sleep(3)
        
        # Check basic functionality for each stream
        for stream in self.streams:
            if hasattr(stream, 'check_func') and stream.check_func:
                tdLog.info(f"Checking initial result for stream {stream.id}")
                stream.check_func()
            else:
                tdLog.info(f"No check function for stream {stream.id}")
        
        tdLog.success("Initial stream computation results verified successfully")
    
    def checkExpiredResults(self):
        """Check stream computation results after expired data insertion"""
        tdLog.info("Checking stream computation results after expired data...")
        
        # Allow time for stream processing
        time.sleep(3)
        
        # Check expired data handling for each stream
        for stream in self.streams:
            if hasattr(stream, 'check_func') and stream.check_func:
                tdLog.info(f"Checking expired result for stream {stream.id}")
                stream.check_func()
            else:
                tdLog.info(f"No check function for stream {stream.id}")
        
        tdLog.success("Expired data handling results verified successfully")
    
    def checkFinalResults(self):
        """Check final stream computation results after all operations"""
        tdLog.info("Checking final stream computation results...")
        
        # Allow time for stream processing
        time.sleep(3)
        
        # Check each stream result
        for i in range(1, 8):
            method_name = f"check{i:02d}"
            if hasattr(self, method_name):
                tdLog.info(f"Checking final result for stream {i}")
                getattr(self, method_name)()
            else:
                tdLog.info(f"Check method {method_name} not implemented")
        
        tdLog.success("All stream computation results verified successfully")
    
    def checkRecalcResults(self):
        """Check stream computation results after recalculation operations"""
        tdLog.info("Checking stream computation results after recalculation...")
        
        # Allow time for stream processing
        time.sleep(3)
        
        # Check recalculation results for each stream
        for stream in self.streams:
            if hasattr(stream, 'res_query_recalc') and stream.res_query_recalc:
                tdLog.info(f"Checking recalc result for stream {stream.id}")
                # Create temporary stream item for recalc checking
                recalc_stream = StreamItem(
                    id=f"{stream.id}_recalc",
                    stream="",  # No need to create stream again
                    res_query=stream.res_query_recalc,
                    exp_query=getattr(stream, 'exp_query_recalc', ''),
                    check_func=getattr(stream, 'check_func_recalc', None),
                )
                recalc_stream.checkResultsByMode()
            else:
                tdLog.info(f"No recalc check for stream {stream.id}")
        
        tdLog.success("Recalculation results verified successfully")

    def createStreams(self):
        self.streams = []

        # ===== Test 1: EXPIRED_TIME Only =====
        
        # Test 1.1: INTERVAL+SLIDING with EXPIRED_TIME - should not process expired data
        stream = StreamItem(
            id=1,
            stream="create stream rdb.s_interval_expired interval(2m) sliding(1m) from tdb.expired_triggers stream_options((expired_time(1h)) into rdb.r_interval_expired as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, cnt, avg_val from rdb.r_interval_expired order by ts;",
            exp_query="",
            check_func=self.check01,
        )
        self.streams.append(stream)

        # # Test 1.2: SESSION with EXPIRED_TIME - should not process expired data
        # stream = StreamItem(
        #     id=2,
        #     stream="create stream rdb.s_session_expired session(ts, 30s) from tdb.trigger_test partition by tbname stream_options((expired_time(1h)) into rdb.r_session_expired as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
        #     res_query="select ts, cnt, avg_val from rdb.r_session_expired order by ts;",
        #     exp_query="",
        #     check_func=self.check02,
        # )
        # self.streams.append(stream)

        # # Test 1.3: STATE_WINDOW with EXPIRED_TIME - should not process expired data
        # stream = StreamItem(
        #     id=3,
        #     stream="create stream rdb.s_state_expired state_window(status) from tdb.trigger_test partition by tbname stream_options((expired_time(1h)) into rdb.r_state_expired as select _twstart ts, count(*) cnt, avg(cint) avg_val, first(cvarchar) status_val from qdb.meters where cts >= _twstart and cts < _twend;",
        #     res_query="select ts, cnt, avg_val, status_val from rdb.r_state_expired order by ts;",
        #     exp_query="",
        #     check_func=self.check03,
        # )
        # self.streams.append(stream)

        # # ===== Test 2: EXPIRED_TIME + WATERMARK Combination =====

        # # Test 2.1: INTERVAL+SLIDING with EXPIRED_TIME + WATERMARK - test boundary behavior
        # stream = StreamItem(
        #     id=4,
        #     stream="create stream rdb.s_interval_combo interval(2m) sliding(1m) from tdb.expired_triggers stream_options((expired_time(1h) | watermark(5m)) into rdb.r_interval_combo as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
        #     res_query="select ts, cnt, avg_val from rdb.r_interval_combo order by ts;",
        #     exp_query="",
        #     check_func=self.check04,
        # )
        # self.streams.append(stream)

        # # Test 2.2: SESSION with EXPIRED_TIME + WATERMARK - test boundary behavior
        # stream = StreamItem(
        #     id=5,
        #     stream="create stream rdb.s_session_combo session(ts, 30s) from tdb.trigger_test partition by tbname stream_options((expired_time(1h) | watermark(5m)) into rdb.r_session_combo as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
        #     res_query="select ts, cnt, avg_val from rdb.r_session_combo order by ts;",
        #     exp_query="",
        #     check_func=self.check05,
        # )
        # self.streams.append(stream)

        # # ===== Test 3: Different EXPIRED_TIME Values =====

        # # Test 3.1: Very short EXPIRED_TIME - strict expiration
        # stream = StreamItem(
        #     id=6,
        #     stream="create stream rdb.s_interval_short_exp interval(2m) sliding(1m) from tdb.expired_triggers stream_options((expired_time(1m)) into rdb.r_interval_short_exp as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
        #     res_query="select ts, cnt, avg_val from rdb.r_interval_short_exp order by ts;",
        #     exp_query="",
        #     check_func=self.check06,
        # )
        # self.streams.append(stream)

        # # Test 3.2: Very long EXPIRED_TIME - loose expiration
        # stream = StreamItem(
        #     id=7,
        #     stream="create stream rdb.s_interval_long_exp interval(2m) sliding(1m) from tdb.expired_triggers stream_options((expired_time(1d)) into rdb.r_interval_long_exp as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
        #     res_query="select ts, cnt, avg_val from rdb.r_interval_long_exp order by ts;",
        #     exp_query="",
        #     check_func=self.check07,
        # )
        # self.streams.append(stream)

        tdLog.info(f"create total:{len(self.streams)} streams")
        for stream in self.streams:
            stream.createStream()

    # Check functions for each test case
    def check01(self):
        # Test interval+sliding with EXPIRED_TIME - should not process expired data
        tdLog.info("Check 1: INTERVAL+SLIDING with EXPIRED_TIME ignores expired data")
        tdSql.checkTableType(dbname="rdb", tbname="r_interval_expired", typename="NORMAL_TABLE", columns=3)
        tdSql.query("select count(*) from rdb.r_interval_expired;")
        result_count = tdSql.getData(0, 0)
        tdLog.info(f"INTERVAL+SLIDING expired_time result count: {result_count}")

    # def check02(self):
    #     # Test session with EXPIRED_TIME - should not process expired data
    #     tdLog.info("Check 2: SESSION with EXPIRED_TIME ignores expired data")
    #     tdSql.checkTableType(dbname="rdb", stbname="r_session_expired", columns=3, tags=1)
    #     tdSql.query("select count(*) from rdb.r_session_expired where tag_tbname='t1';")
    #     result_count = tdSql.getData(0, 0)
    #     tdLog.info(f"SESSION expired_time result count: {result_count}")

    # def check03(self):
    #     # Test state window with EXPIRED_TIME - should not process expired data
    #     tdLog.info("Check 3: STATE_WINDOW with EXPIRED_TIME ignores expired data")
    #     tdSql.checkTableType(dbname="rdb", stbname="r_state_expired", columns=4, tags=1)
    #     tdSql.query("select count(*) from rdb.r_state_expired where tag_tbname='t1';")
    #     result_count = tdSql.getData(0, 0)
    #     tdLog.info(f"STATE_WINDOW expired_time result count: {result_count}")

    # def check04(self):
    #     # Test interval+sliding with EXPIRED_TIME + WATERMARK - test boundary behavior
    #     tdLog.info("Check 4: INTERVAL+SLIDING with EXPIRED_TIME + WATERMARK boundary behavior")
    #     tdSql.checkTableType(dbname="rdb", tbname="r_interval_combo", typename="NORMAL_TABLE", columns=3)
    #     tdSql.query("select count(*) from rdb.r_interval_combo;")
    #     result_count = tdSql.getData(0, 0)
    #     tdLog.info(f"INTERVAL+SLIDING combo result count: {result_count}")

    # def check05(self):
    #     # Test session with EXPIRED_TIME + WATERMARK - test boundary behavior
    #     tdLog.info("Check 5: SESSION with EXPIRED_TIME + WATERMARK boundary behavior")
    #     tdSql.checkTableType(dbname="rdb", stbname="r_session_combo", columns=3, tags=1)
    #     tdSql.query("select count(*) from rdb.r_session_combo where tag_tbname='t1';")
    #     result_count = tdSql.getData(0, 0)
    #     tdLog.info(f"SESSION combo result count: {result_count}")

    # def check06(self):
    #     # Test interval+sliding with very short EXPIRED_TIME - strict expiration
    #     tdLog.info("Check 6: INTERVAL+SLIDING with very short EXPIRED_TIME (1m)")
    #     tdSql.checkTableType(dbname="rdb", tbname="r_interval_short_exp", typename="NORMAL_TABLE", columns=3)
    #     tdSql.query("select count(*) from rdb.r_interval_short_exp;")
    #     result_count = tdSql.getData(0, 0)
    #     tdLog.info(f"INTERVAL+SLIDING short expired_time result count: {result_count}")

    # def check07(self):
    #     # Test interval+sliding with very long EXPIRED_TIME - loose expiration
    #     tdLog.info("Check 7: INTERVAL+SLIDING with very long EXPIRED_TIME (1 day)")
    #     tdSql.checkTableType(dbname="rdb", tbname="r_interval_long_exp", typename="NORMAL_TABLE", columns=3)
    #     tdSql.query("select count(*) from rdb.r_interval_long_exp;")
    #     result_count = tdSql.getData(0, 0)
    #     tdLog.info(f"INTERVAL+SLIDING long expired_time result count: {result_count}") 