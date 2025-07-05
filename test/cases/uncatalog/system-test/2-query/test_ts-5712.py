from new_test_framework.utils import tdLog, tdSql

class TestTs5712:
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

        # super table
        tdSql.execute("CREATE TABLE t1( time TIMESTAMP, c1 BIGINT);")

        # create index for all tags
        tdSql.execute("INSERT INTO t1(time, c1) VALUES (1641024000000, 0)")
        tdSql.execute("INSERT INTO t1(time, c1) VALUES (1641024000001, 0)")

    def check(self):
        tdSql.query(f"SELECT CAST(time AS BIGINT) FROM t1 WHERE (1 - time) > 0")
        tdSql.checkRows(0)

    def test_ts_5712(self):
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
