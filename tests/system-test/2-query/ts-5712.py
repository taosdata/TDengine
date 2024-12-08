
import taos

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)
        self.dbname = 'db'
        self.stbname = 'st'

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

    def run(self):
        self.prepareData()
        self.check()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")



tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
