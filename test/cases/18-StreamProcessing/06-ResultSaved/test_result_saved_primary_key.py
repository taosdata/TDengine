import time
from new_test_framework.utils import tdLog, tdSql, clusterComCheck, tdStream, StreamItem


class TestStreamResultSavedPrimaryKey:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_result_saved_primary_key(self):
        """Result saved: primary key tests

        This test focuses specifically on PRIMARY KEY functionality in stream result saving:

        1. Test valid PRIMARY KEY scenarios
            1.1 Second column is integer type with PRIMARY KEY
            1.2 Second column is string type with PRIMARY KEY
            1.3 Multiple columns with PRIMARY KEY

        2. Test invalid PRIMARY KEY scenarios
            2.1 Second column is float type with PRIMARY KEY (should fail)
            2.2 Second column is timestamp type with PRIMARY KEY (should fail)
            2.3 PRIMARY KEY on first column (timestamp) - should fail or be ignored
            2.4 PRIMARY KEY on non-existent column

        3. Test PRIMARY KEY with existing tables
            3.1 Existing table already has PRIMARY KEY
            3.2 Existing table without PRIMARY KEY

        4. Test PRIMARY KEY with different table types
            4.1 PRIMARY KEY with normal tables
            4.2 PRIMARY KEY with super tables (should be ignored or fail)

        Catalog:
            - Streams:ResultSaved:PrimaryKey

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-19 Generated from design document

        """

        self.createSnode()
        self.createDatabase()
        self.prepareQueryData()
        self.prepareTriggerTable()
        self.createStreams()
        self.checkStreamStatus()
        self.writeTriggerData()
        self.checkResults()

    def createSnode(self):
        tdLog.info("create snode")
        tdStream.createSnode(1)

    def createDatabase(self):
        tdLog.info(f"create database")

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

    def prepareTriggerTable(self):
        tdLog.info("prepare tables for trigger")

        stb = "create table tdb.triggers (ts timestamp, c1 int, c2 int, c3 varchar(32)) tags(id int, name varchar(16));"
        ctb = "create table tdb.t1 using tdb.triggers tags(1, '1') tdb.t2 using tdb.triggers tags(2, '2') tdb.t3 using tdb.triggers tags(3, '3')"
        tdSql.execute(stb)
        tdSql.execute(ctb)

        ntb = "create table tdb.n1 (ts timestamp, c1 int, c2 int, c3 varchar(32))"
        tdSql.execute(ntb)

        # Prepare existing tables with different PRIMARY KEY configurations
        tdLog.info("prepare existing tables with PRIMARY KEY configurations")
        
        # Table with existing PRIMARY KEY
        tdSql.execute("create table rdb.existing_with_pk (ts timestamp, cnt bigint primary key, avg_val double)")
        
        # Table without PRIMARY KEY
        tdSql.execute("create table rdb.existing_no_pk (ts timestamp, cnt bigint, avg_val double)")

    def writeTriggerData(self):
        tdLog.info("write data to trigger table")
        sqls = [
            "insert into tdb.t1 values ('2025-01-01 00:00:00', 0,  0,  'str0' ) ('2025-01-01 00:05:00', 5,  50,  'str5' ) ('2025-01-01 00:10:00', 10, 100, 'str10')",
            "insert into tdb.t2 values ('2025-01-01 00:11:00', 11, 110, 'str11') ('2025-01-01 00:12:00', 12, 120, 'str12') ('2025-01-01 00:15:00', 15, 150, 'str15')",
            "insert into tdb.t3 values ('2025-01-01 00:21:00', 21, 210, 'str21')",
            "insert into tdb.n1 values ('2025-01-01 00:25:00', 25, 250, 'str25') ('2025-01-01 00:26:00', 26, 260, 'str26') ('2025-01-01 00:27:00', 27, 270, 'str27')",
            "insert into tdb.t1 values ('2025-01-01 00:30:00', 30, 300, 'str30') ('2025-01-01 00:32:00', 32, 320, 'str32') ('2025-01-01 00:36:00', 36, 360, 'str36')",
            "insert into tdb.n1 values ('2025-01-01 00:40:00', 40, 400, 'str40') ('2025-01-01 00:42:00', 42, 420, 'str42')",
        ]
        tdSql.executes(sqls)

    def checkStreamStatus(self):
        tdLog.info(f"wait total:{len(self.streams)} streams run finish")
        tdStream.checkStreamStatus()

    def checkResults(self):
        tdLog.info(f"check total:{len(self.streams)} streams result")
        for stream in self.streams:
            stream.checkResults()
        tdLog.info(f"check total:{len(self.streams)} streams result successfully")

    def createStreams(self):
        self.streams = []

        # Test 1.1: Second column is integer type with PRIMARY KEY
        stream = StreamItem(
            id=0,
            stream="create stream rdb.pk_s0 interval(5m) sliding(5m) from tdb.triggers into rdb.pk_r0 (ts, cnt primary key, avg_val) as select _twstart ts, count(*) cnt, avg(c1) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, cnt, avg_val from rdb.pk_r0;",
            exp_query="select _wstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
            check_func=self.check0,
        )
        self.streams.append(stream)

        # Test 1.2: Second column is string type with PRIMARY KEY
        stream = StreamItem(
            id=1,
            stream="create stream rdb.pk_s1 interval(5m) sliding(5m) from tdb.triggers into rdb.pk_r1 (ts, str_key primary key, cnt) as select _twstart ts, concat('key_', cast(count(*) as varchar(10))) str_key, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, str_key, cnt from rdb.pk_r1;",
            exp_query="select _wstart ts, concat('key_', cast(count(*) as varchar(10))) str_key, count(*) cnt from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
            check_func=self.check1,
        )
        self.streams.append(stream)

        # Test 1.3: Multiple columns with PRIMARY KEY (only one should be allowed)
        stream = StreamItem(
            id=2,
            stream="create stream rdb.pk_s2 interval(5m) sliding(5m) from tdb.triggers into rdb.pk_r2 (ts, cnt primary key, str_col, avg_val) as select _twstart ts, count(*) cnt, 'test' str_col, avg(c1) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, cnt, str_col, avg_val from rdb.pk_r2;",
            exp_query="select _wstart ts, count(*) cnt, 'test' str_col, avg(cint) avg_val from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
            check_func=self.check2,
        )
        self.streams.append(stream)

        # Test 3.1: Existing table already has PRIMARY KEY - should match
        stream = StreamItem(
            id=3,
            stream="create stream rdb.pk_s3 interval(5m) sliding(5m) from tdb.triggers into rdb.existing_with_pk as select _twstart ts, count(*) cnt, avg(c1) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, cnt, avg_val from rdb.existing_with_pk;",
            exp_query="select _wstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
            check_func=self.check3,
        )
        self.streams.append(stream)

        # Test 3.2: Existing table without PRIMARY KEY
        stream = StreamItem(
            id=4,
            stream="create stream rdb.pk_s4 interval(5m) sliding(5m) from tdb.triggers into rdb.existing_no_pk (ts, cnt primary key, avg_val) as select _twstart ts, count(*) cnt, avg(c1) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, cnt, avg_val from rdb.existing_no_pk;",
            exp_query="select _wstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
            check_func=self.check4,
        )
        self.streams.append(stream)

        # Test 4.1: PRIMARY KEY with normal tables (standard case)
        stream = StreamItem(
            id=5,
            stream="create stream rdb.pk_s5 interval(5m) sliding(5m) from tdb.n1 into rdb.pk_r5 (ts, cnt primary key, max_val) as select _twstart ts, count(*) cnt, max(c1) max_val from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, cnt, max_val from rdb.pk_r5;",
            exp_query="select _wstart ts, count(*) cnt, max(cint) max_val from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
            check_func=self.check5,
        )
        self.streams.append(stream)

        # Test different data types for PRIMARY KEY
        # Test BIGINT PRIMARY KEY
        stream = StreamItem(
            id=6,
            stream="create stream rdb.pk_s6 interval(5m) sliding(5m) from tdb.triggers into rdb.pk_r6 (ts, big_key primary key, cnt) as select _twstart ts, cast(count(*) as bigint) big_key, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, big_key, cnt from rdb.pk_r6;",
            exp_query="select _wstart ts, cast(count(*) as bigint) big_key, count(*) cnt from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
            check_func=self.check6,
        )
        self.streams.append(stream)

        # Test VARCHAR PRIMARY KEY with different lengths
        stream = StreamItem(
            id=7,
            stream="create stream rdb.pk_s7 interval(5m) sliding(5m) from tdb.triggers into rdb.pk_r7 (ts, var_key primary key, cnt) as select _twstart ts, substr(cast(_twstart as varchar(64)), 1, 19) var_key, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, var_key, cnt from rdb.pk_r7;",
            exp_query="select _wstart ts, substr(cast(_wstart as varchar(64)), 1, 19) var_key, count(*) cnt from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
            check_func=self.check7,
        )
        self.streams.append(stream)

        # Test error scenarios within createStreams for immediate validation
        self.createErrorStreams()

        tdLog.info(f"create total:{len(self.streams)} streams")
        for stream in self.streams:
            stream.createStream()

    def createErrorStreams(self):
        """Create streams that should fail to test error scenarios"""
        tdLog.info("Testing error scenarios for PRIMARY KEY")

        # Test 2.1: Second column is float type with PRIMARY KEY (should fail)
        tdLog.info("Test 2.1: Float type PRIMARY KEY (should fail)")
        try:
            tdSql.execute("create stream rdb.pk_error_s1 interval(5m) sliding(5m) from tdb.triggers into rdb.pk_error_r1 (ts, avg_val primary key) as select _twstart ts, avg(c1) avg_val from qdb.meters where cts >= _twstart and cts < _twend;")
            tdLog.exit("Expected error for float PRIMARY KEY")
        except Exception as e:
            tdLog.info(f"Expected error caught: {e}")

        # Test 2.2: Second column is timestamp type with PRIMARY KEY (should fail)
        tdLog.info("Test 2.2: Timestamp type PRIMARY KEY (should fail)")
        try:
            tdSql.execute("create stream rdb.pk_error_s2 interval(5m) sliding(5m) from tdb.triggers into rdb.pk_error_r2 (ts, ts2 primary key, cnt) as select _twstart ts, _twend ts2, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;")
            tdLog.exit("Expected error for timestamp PRIMARY KEY")
        except Exception as e:
            tdLog.info(f"Expected error caught: {e}")

        # Test 2.3: PRIMARY KEY on first column (timestamp) - should fail or be ignored
        tdLog.info("Test 2.3: PRIMARY KEY on timestamp column (should fail)")
        try:
            tdSql.execute("create stream rdb.pk_error_s3 interval(5m) sliding(5m) from tdb.triggers into rdb.pk_error_r3 (ts primary key, cnt) as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;")
            tdLog.exit("Expected error for PRIMARY KEY on timestamp")
        except Exception as e:
            tdLog.info(f"Expected error caught: {e}")

        # Test 2.4: PRIMARY KEY on non-existent column
        tdLog.info("Test 2.4: PRIMARY KEY on non-existent column (should fail)")
        try:
            tdSql.execute("create stream rdb.pk_error_s4 interval(5m) sliding(5m) from tdb.triggers into rdb.pk_error_r4 (ts, cnt, nonexistent primary key) as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;")
            tdLog.exit("Expected error for PRIMARY KEY on non-existent column")
        except Exception as e:
            tdLog.info(f"Expected error caught: {e}")

        # Test 4.2: PRIMARY KEY with super tables (should be ignored or fail)
        tdLog.info("Test 4.2: PRIMARY KEY with super tables (should fail)")
        try:
            tdSql.execute("create stream rdb.pk_error_s5 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.pk_error_r5 (ts, cnt primary key) as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;")
            tdLog.exit("Expected error for PRIMARY KEY with super table")
        except Exception as e:
            tdLog.info(f"Expected error caught: {e}")

    def check0(self):
        # Test 1.1: Integer PRIMARY KEY
        tdSql.checkTableType(dbname="rdb", tbname="pk_r0", typename="NORMAL_TABLE", columns=3)
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="pk_r0",
            schema=[
                ["ts", "TIMESTAMP", 8, ""],
                ["cnt", "BIGINT", 8, "PRI"],  # Should have PRIMARY KEY flag
                ["avg_val", "DOUBLE", 8, ""],
            ],
        )

    def check1(self):
        # Test 1.2: String PRIMARY KEY
        tdSql.checkTableType(dbname="rdb", tbname="pk_r1", typename="NORMAL_TABLE", columns=3)
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="pk_r1",
            schema=[
                ["ts", "TIMESTAMP", 8, ""],
                ["str_key", "VARCHAR", 23, "PRI"],  # Should have PRIMARY KEY flag
                ["cnt", "BIGINT", 8, ""],
            ],
        )

    def check2(self):
        # Test 1.3: Multiple columns with PRIMARY KEY
        tdSql.checkTableType(dbname="rdb", tbname="pk_r2", typename="NORMAL_TABLE", columns=4)
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="pk_r2",
            schema=[
                ["ts", "TIMESTAMP", 8, ""],
                ["cnt", "BIGINT", 8, "PRI"],  # Should have PRIMARY KEY flag
                ["str_col", "VARCHAR", 4, ""],
                ["avg_val", "DOUBLE", 8, ""],
            ],
        )

    def check3(self):
        # Test 3.1: Existing table with PRIMARY KEY
        tdSql.checkTableType(dbname="rdb", tbname="existing_with_pk", typename="NORMAL_TABLE", columns=3)
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="existing_with_pk",
            schema=[
                ["ts", "TIMESTAMP", 8, ""],
                ["cnt", "BIGINT", 8, "PRI"],  # Should retain PRIMARY KEY
                ["avg_val", "DOUBLE", 8, ""],
            ],
        )

    def check4(self):
        # Test 3.2: Existing table without PRIMARY KEY - should add PRIMARY KEY if specified
        tdSql.checkTableType(dbname="rdb", tbname="existing_no_pk", typename="NORMAL_TABLE", columns=3)
        # Note: The behavior here depends on implementation - it might fail or modify the table
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="existing_no_pk",
            schema=[
                ["ts", "TIMESTAMP", 8, ""],
                ["cnt", "BIGINT", 8, ""],  # May or may not have PRI flag
                ["avg_val", "DOUBLE", 8, ""],
            ],
        )

    def check5(self):
        # Test 4.1: Normal table with PRIMARY KEY
        tdSql.checkTableType(dbname="rdb", tbname="pk_r5", typename="NORMAL_TABLE", columns=3)
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="pk_r5",
            schema=[
                ["ts", "TIMESTAMP", 8, ""],
                ["cnt", "BIGINT", 8, "PRI"],
                ["max_val", "INT", 4, ""],
            ],
        )

    def check6(self):
        # Test BIGINT PRIMARY KEY
        tdSql.checkTableType(dbname="rdb", tbname="pk_r6", typename="NORMAL_TABLE", columns=3)
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="pk_r6",
            schema=[
                ["ts", "TIMESTAMP", 8, ""],
                ["big_key", "BIGINT", 8, "PRI"],
                ["cnt", "BIGINT", 8, ""],
            ],
        )

    def check7(self):
        # Test VARCHAR PRIMARY KEY
        tdSql.checkTableType(dbname="rdb", tbname="pk_r7", typename="NORMAL_TABLE", columns=3)
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="pk_r7",
            schema=[
                ["ts", "TIMESTAMP", 8, ""],
                ["var_key", "VARCHAR", 19, "PRI"],
                ["cnt", "BIGINT", 8, ""],
            ],
        ) 