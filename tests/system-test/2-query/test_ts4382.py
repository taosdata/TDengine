import random
import string
from util.log import *
from util.cases import *
from util.sql import *
from util.sqlset import *
from util import constant
from util.common import *


class TDTestCase:
    """Verify the jira TS-4382
    """
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.dbname = 'db'
        self.stbname = 'st'
        self.ctbname_list = ["ct1", "ct2"]
        self.tag_value_list = ['{"instance":"100"}', '{"instance":"200"}']

    def prepareData(self):
        # db
        tdSql.execute("create database {};".format(self.dbname))
        tdSql.execute("use {};".format(self.dbname))
        tdLog.debug("Create database %s" % self.dbname)

        # super table
        tdSql.execute("create table {} (ts timestamp, col1 int) tags (t1 json);".format(self.stbname))
        tdLog.debug("Create super table %s" % self.stbname)

        # child table
        for i in range(len(self.ctbname_list)):
            tdSql.execute("create table {} using {} tags('{}');".format(self.ctbname_list[i], self.stbname, self.tag_value_list[i]))
        tdLog.debug("Create child table %s" % self.ctbname_list)

        # insert data
        tdSql.execute("insert into {} values(now, 1)(now+1s, 2)".format(self.ctbname_list[0]))
        tdSql.execute("insert into {} values(now, null)(now+1s, null)".format(self.ctbname_list[1]))

    def run(self):
        self.prepareData()
        sql_list = [
            # super table query with correct tag name of json type
            {
                "sql": "select to_char(ts, 'yyyy-mm-dd hh24:mi:ss') as time, irate(col1) from st group by to_char(ts, 'yyyy-mm-dd hh24:mi:ss'), t1->'instance' order by time;",
                "result_check": "0.0"
            },
            # child table query with incorrect tag name of json type
            {
                "sql": "select to_char(ts, 'yyyy-mm-dd hh24:mi:ss') as time, irate(col1) from ct1 group by to_char(ts, 'yyyy-mm-dd hh24:mi:ss'), t1->'name' order by time;",
                "result_check": "None"
            },
            # child table query with null value
            {
                "sql": "select ts, avg(col1) from ct2 group by ts, t1->'name' order by ts;",
                "result_check": "None"
            }
        ]
        for sql_dic in sql_list:
            tdSql.query(sql_dic["sql"])
            tdLog.debug("execute sql: %s" % sql_dic["sql"])
            for item in [row[1] for row in tdSql.queryResult]:
                if sql_dic["result_check"] in str(item):
                    tdLog.debug("Check query result of '{}' successfully".format(sql_dic["sql"]))
                    break

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
