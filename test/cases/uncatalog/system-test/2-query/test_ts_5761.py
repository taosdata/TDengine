from new_test_framework.utils import tdLog, tdSql

class TestTs5761:
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        cls.dbname = 'db'
        cls.stbname = 'st'

    def prepareData(self):
        # db
        tdSql.execute(f"create database db;")
        tdSql.execute(f"use db")

        # super tableUNSIGNED
        tdSql.execute("CREATE TABLE st( time TIMESTAMP, c1 BIGINT, c2 smallint, c3 double, c4 int UNSIGNED, c5 bool, c6 binary(32), c7 nchar(32)) tags(t1 binary(32), t2 nchar(32))")
        tdSql.execute("create table t1 using st tags('1', '1.7')")
        tdSql.execute("create table t2 using st tags('0', '')")
        tdSql.execute("create table t3 using st tags('1', 'er')")

        # create index for all tags
        tdSql.execute("INSERT INTO t1 VALUES (1641024000000, 1, 1, 1, 1, 1, '1', '1.7')")
        tdSql.execute("INSERT INTO t1 VALUES (1641024000001, 0, 0, 1.7, 0, 0, '0', '')")
        tdSql.execute("INSERT INTO t1 VALUES (1641024000002, 1, 1, 1, 1, 1, '1', 'er')")
        tdSql.execute("INSERT INTO t2 VALUES (1641024000002, 1, 1, 1, 1, 1, '1', 'er')")
        tdSql.execute("INSERT INTO t3 VALUES (1641024000002, 1, 1, 1, 1, 1, '1', 'er')")

        tdSql.execute("CREATE TABLE stt( time TIMESTAMP, c1 BIGINT, c2 timestamp, c3 int, c4 int UNSIGNED, c5 bool, c6 binary(32), c7 nchar(32)) tags(t1 binary(32), t2 nchar(32))")
        tdSql.execute("create table tt1 using stt tags('1', '1.7')")

        # create index for all tags
        tdSql.execute("INSERT INTO tt1 VALUES (1641024000000, 9223372036854775807, 1641024000000, 1, 1, 1, '1', '1.7')")

    def check(self):
        tdSql.query(f"SELECT * FROM tt1 WHERE c1 in (1.7, 9223372036854775803, '')")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM tt1 WHERE c1 = 9223372036854775803")
        tdSql.checkRows(0)

        tdSql.query(f"SELECT * FROM t1 WHERE c1 = 1.7")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM t1 WHERE c1 in (1.7, 2)")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM t1 WHERE c1 not in (1.7, 2)")
        tdSql.checkRows(3)

        tdSql.query(f"SELECT * FROM t1 WHERE c2 = 1.7")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM t1 WHERE c2 in (1.7, 2)")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM t1 WHERE c2 not in (1.7, 2)")
        tdSql.checkRows(3)

        tdSql.query(f"SELECT * FROM t1 WHERE c3 = 1.7")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT * FROM t1 WHERE c3 in (1.7, 2)")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT * FROM t1 WHERE c3 not in (1.7, 2)")
        tdSql.checkRows(2)

        tdSql.query(f"SELECT * FROM t1 WHERE c4 = 1.7")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM t1 WHERE c4 in (1.7, 2)")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM t1 WHERE c4 not in (1.7, 2)")
        tdSql.checkRows(3)

        tdSql.query(f"SELECT * FROM t1 WHERE c5 = 1.7")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM t1 WHERE c5 in (1.7, 2)")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM t1 WHERE c5 not in (1.7, 2)")
        tdSql.checkRows(3)

        tdSql.query(f"SELECT * FROM t1 WHERE c6 = 1.7")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM t1 WHERE c6 in (1.7, 2)")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM t1 WHERE c6 not in (1.7, 2)")
        tdSql.checkRows(3)

        tdSql.query(f"SELECT * FROM t1 WHERE c6 = 1")
        tdSql.checkRows(2)
        tdSql.query(f"SELECT * FROM t1 WHERE c6 in (1, 2)")
        tdSql.checkRows(2)
        tdSql.query(f"SELECT * FROM t1 WHERE c6 not in (1, 2)")
        tdSql.checkRows(1)

        tdSql.query(f"SELECT * FROM t1 WHERE c6 = 0")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT * FROM t1 WHERE c6 in (0, 2)")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT * FROM t1 WHERE c6 not in (0, 2)")
        tdSql.checkRows(2)
        tdSql.query(f"SELECT * FROM t1 WHERE c6 not in (0, 2, 'sef')")
        tdSql.checkRows(2)

        tdSql.query(f"SELECT * FROM t1 WHERE c7 = 1.7")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT * FROM t1 WHERE c7 in (1.7, 2)")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT * FROM t1 WHERE c7 not in (1.7, 2)")
        tdSql.checkRows(2)

        tdSql.query(f"SELECT * FROM t1 WHERE c7 = 0")
        tdSql.checkRows(2)
        tdSql.query(f"SELECT * FROM t1 WHERE c7 in (0, 2)")
        tdSql.checkRows(2)
        tdSql.query(f"SELECT * FROM t1 WHERE c7 not in (0, 2)")
        tdSql.checkRows(1)

        tdSql.query(f"SELECT * FROM t1 WHERE c7 = ''")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT * FROM t1 WHERE c7 in ('', 2)")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT * FROM t1 WHERE c7 not in ('', 2)")
        tdSql.checkRows(2)

        tdSql.query(f"SELECT * FROM st WHERE t2 in ('', 2)")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT * FROM st WHERE t2 not in ('', 2)")
        tdSql.checkRows(4)

        tdSql.query(f"SELECT * FROM st WHERE t1 in ('d343', 0, 2)")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT * FROM st WHERE t1 in (0, 2)")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT * FROM st WHERE t1 not in (0, 2)")
        tdSql.checkRows(4)

    def test_ts_5761(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx

        """

        self.prepareData()
        self.check()

        #tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

