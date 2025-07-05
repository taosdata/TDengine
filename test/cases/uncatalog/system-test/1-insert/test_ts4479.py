import os
import sys 
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import tdDnodes
from math import inf
import taos


class TDTestCase:
    """Verify inserting varbinary type data of ts-4479
    """
    def init(self, conn, logSql, replicaVer=1):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), True)
        self.conn = conn
        self.db_name = "db"
        self.stable_name = "st"

    def run(self):
        tdSql.execute("create database if not exists %s" % self.db_name)
        tdSql.execute("use %s" % self.db_name)
        # create super table
        tdSql.execute("create table %s (ts timestamp, c1 varbinary(65517)) tags (t1 varbinary(16382))" % self.stable_name)

        # varbinary tag length is more than 16382
        tag = os.urandom(16383).hex()
        tdSql.error("create table ct using st tags(%s);" % ('\\x' + tag))

        # create child table with max column and tag length
        child_table_list = []
        for i in range(2):
            child_table_name = "ct_" + str(i+1)
            child_table_list.append(child_table_name)
            tag = os.urandom(16382).hex()
            tdSql.execute("create table %s using st tags('%s');" % (child_table_name, '\\x' + tag))
            tdLog.info("create table %s successfully" % child_table_name)

        # varbinary column length is more than 65517
        value = os.urandom(65518).hex()
        tdSql.error("insert into ct_1 values(now, '\\x%s');" % value)

        # insert data
        for i in range(10):
            sql = "insert into table_name values"
            for j in range(5):
                value = os.urandom(65517).hex()
                sql += "(now+%ss, '%s')," % (str(j+1), '\\x' + value)
            for child_table in child_table_list:
                tdSql.execute(sql.replace("table_name", child_table))
                tdLog.info("Insert data into %s successfully" % child_table)
            tdLog.info("Insert data round %s successfully" % str(i+1))
        tdSql.execute("flush database %s" % self.db_name)

        # insert \\x to varbinary column
        tdSql.execute("insert into ct_1 values(now, '\\x');")
        tdSql.query("select * from ct_1 where c1 = '\\x';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, b'')

        # insert \\x to varbinary tag
        tdSql.execute("create table ct_3 using st tags('\\x');")
        tdSql.execute("insert into ct_3 values(now, '\\x45');")
        tdSql.query("select * from st where t1='';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, b'')

    def stop(self):
        tdSql.execute("drop database if exists %s" % self.db_name)
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
