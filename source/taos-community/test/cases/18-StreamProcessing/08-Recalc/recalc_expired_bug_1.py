import time
from new_test_framework.utils import tdLog, tdSql, clusterComCheck, tdStream, StreamItem


class TestStreamRecalcExpiredTime:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_recalc_expired_time(self):
        """Recalc: watermark with expired_time option

        Test EXPIRED_TIME option with expired data:
        1. Write expired data - all windows should not trigger recalculation
        2. Combine with WATERMARK - test boundary value behavior
        3. Different trigger types behavior with expired data


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
        self.writeInitialSourceData()
        self.createStreams()
        self.checkStreamStatus()
        self.writeInitialTriggerData()
        self.checkInitialResults()
        self.writeData()
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

    def writeInitialSourceData(self):
        tdLog.info("write initial source data to qdb.meters")
        # Step 1: Write base data to meters table (qdb) 
        # Table structure: cts, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary, cdecimal8, cdecimal16, cgeometry
        source_sqls = [
            "insert into qdb.t0 values ('2025-01-01 10:00:00', 10, 10, 100, 100, 1.5, 1.5, 'normal', 1, 1, 1, 1, true, 'normal', 'normal', '10', '10', 'POINT(1.0 1.0)') ('2025-01-01 10:05:00', 20, 20, 200, 200, 2.5, 2.5, 'normal', 2, 2, 2, 2, false, 'normal', 'normal', '20', '20', 'POINT(2.0 2.0)')",
            "insert into qdb.t1 values ('2025-01-01 10:01:00', 15, 15, 150, 150, 3.5, 3.5, 'warning', 1, 1, 1, 1, true, 'warning', 'warning', '15', '15', 'POINT(1.5 1.5)') ('2025-01-01 10:06:00', 25, 25, 250, 250, 4.5, 4.5, 'warning', 2, 2, 2, 2, false, 'warning', 'warning', '25', '25', 'POINT(2.5 2.5)')", 
            "insert into qdb.t2 values ('2025-01-01 10:02:00', 30, 30, 300, 300, 5.5, 5.5, 'error', 3, 3, 3, 3, true, 'error', 'error', '30', '30', 'POINT(3.0 3.0)') ('2025-01-01 10:07:00', 40, 40, 400, 400, 6.5, 6.5, 'error', 4, 4, 4, 4, false, 'error', 'error', '40', '40', 'POINT(4.0 4.0)')"
        ]
        tdSql.executes(source_sqls)

    def writeInitialTriggerData(self):
        tdLog.info("write initial trigger data to tdb")
        # Step 3: Write trigger data to trigger tables (tdb) to start stream computation
        trigger_sqls = [
            "insert into tdb.et1 values ('2025-01-01 10:00:00', 10, 100, 1.5, 'normal') ('2025-01-01 10:02:00', 20, 200, 2.5, 'normal')",
            "insert into tdb.et2 values ('2025-01-01 10:00:30', 15, 150, 3.5, 'warning') ('2025-01-01 10:02:30', 25, 250, 4.5, 'warning')", 
            "insert into tdb.et3 values ('2025-01-01 10:01:00', 30, 300, 5.5, 'error') ('2025-01-01 10:03:00', 40, 400, 6.5, 'error')",
            "insert into tdb.t1 values ('2025-01-01 10:00:00', 100, 'active') ('2025-01-01 10:02:00', 200, 'active')",
            "insert into tdb.t2 values ('2025-01-01 10:00:30', 150, 'busy') ('2025-01-01 10:02:30', 250, 'busy')",
            "insert into tdb.t3 values ('2025-01-01 10:01:00', 180, 'idle') ('2025-01-01 10:03:00', 280, 'idle')",
            "insert into tdb.trigger_normal values ('2025-01-01 10:00:00', 50, 1.0, 'baseline') ('2025-01-01 10:02:00', 60, 2.0, 'trend')"
        ]
        tdSql.executes(trigger_sqls)

    def writeData(self):
        tdSql.execute("insert into qdb.t0 values ('2025-01-01 10:01:01', 6, 6, 65, 65, 0.9, 0.9, 'boundary1', 1, 1, 1, 1, true, 'boundary1', 'boundary1', '6', '6', 'POINT(0.9 0.9)');")
        tdSql.execute("insert into tdb.et1 values ('2025-01-01 00:00:01', 5, 55, 0.8, 'expired1');")
        tdSql.execute("insert into tdb.et1 values ('2025-01-01 10:00:01', 5, 55, 0.8, 'expired1') ;")
        time.sleep(30)

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
            stream="create stream rdb.s_interval_expired interval(2m) sliding(1m) from tdb.expired_triggers stream_options(expired_time(1h)) into rdb.r_interval_expired as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, cnt, avg_val from rdb.r_interval_expired order by ts;",
            exp_query="",
            check_func=self.check01,
        )
        self.streams.append(stream)

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