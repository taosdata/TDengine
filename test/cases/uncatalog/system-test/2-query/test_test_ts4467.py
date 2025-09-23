from new_test_framework.utils import tdLog, tdSql

import itertools

class TestTestTs4467:
    """Verify the jira TS-4467
    """
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)

    def prepareData(self):
        # db
        tdSql.execute("create database if not exists db")
        tdSql.execute("use db")

        # table
        tdSql.execute("create table t (ts timestamp, c1 varchar(16));")

        # insert data
        sql = "insert into t values"
        for i in range(6):
            sql += f"(now+{str(i+1)}s, '{'name' + str(i+1)}')"
        sql += ";"
        tdSql.execute(sql)
        tdLog.debug("insert data successfully")

    def test_test_ts4467(self):
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

        # join query with order by
        sql = "select * from t t1, (select * from t order by ts limit 5) t2 where t1.ts = t2.ts;"
        tdSql.query(sql)
        tdSql.checkRows(5)

        sql = "select * from t t1, (select * from t order by ts desc limit 5) t2 where t1.ts = t2.ts;"
        tdSql.query(sql)
        tdSql.checkRows(5)

        sql = "select * from t t1, (select * from t order by ts limit 5) t2 where t1.ts = t2.ts order by t1.ts;"
        tdSql.query(sql)
        res1 = tdSql.queryResult
        tdLog.debug("res1: %s" % str(res1))

        sql = "select * from t t1, (select * from t order by ts limit 5) t2 where t1.ts = t2.ts order by t1.ts desc;"
        tdSql.query(sql)
        res2 = tdSql.queryResult
        tdLog.debug("res2: %s" % str(res2))
        assert(len(res1) == len(res2) and res1[0][0] == res2[4][0])

        # test ts-5613
        sql = "select cast(2<>3 as int) from t"
        tdSql.query(sql)
        tdSql.checkData(0,0,1)

        sql = "select cast(2 not in(3) as int) from t"
        tdSql.query(sql)
        tdSql.checkData(0,0,1)

        sql = "select cast(2 is NULL as int) from t"
        tdSql.query(sql)
        tdSql.checkData(0,0,0)

        #tdSql.close()
        tdLog.success("%s successfully executed" % __file__)
