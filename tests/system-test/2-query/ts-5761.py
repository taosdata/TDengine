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

        # super tableUNSIGNED
        tdSql.execute("CREATE TABLE t1( time TIMESTAMP, c1 BIGINT, c2 smallint, c3 double, c4 int UNSIGNED, c5 bool);")

        # create index for all tags
        tdSql.execute("INSERT INTO t1 VALUES (1641024000000, 1, 1, 1, 1, 1)")

    def check(self):
        tdSql.query(f"SELECT * FROM t1 WHERE c1 in (1.7)")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM t1 WHERE c2 in (1.7)")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM t1 WHERE c3 in (1.7)")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM t1 WHERE c4 in (1.7)")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM t1 WHERE c5 in (1.7)")
        tdSql.checkRows(1)

    def run(self):
        self.prepareData()
        self.check()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")



tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())