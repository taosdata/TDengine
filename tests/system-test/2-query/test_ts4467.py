import random
import itertools
from util.log import *
from util.cases import *
from util.sql import *
from util.sqlset import *
from util import constant
from util.common import *


class TDTestCase:
    """Verify the jira TS-4467
    """
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

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

    def run(self):
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

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
