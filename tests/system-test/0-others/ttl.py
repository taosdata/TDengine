import time
from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *


class TDTestCase:
    updatecfgDict = {'ttlUnit': 1, "ttlPushInterval": 1, "ttlChangeOnWrite": 0}

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)
        self.ttl = 5
        self.dbname = "test"

    def check_ttl_result(self):
        tdSql.execute(f'create database {self.dbname}')
        tdSql.execute(f'create table {self.dbname}.t1(ts timestamp, c1 int)')
        tdSql.execute(f'create table {self.dbname}.t2(ts timestamp, c1 int) ttl {self.ttl}')
        tdSql.query(f'show {self.dbname}.tables')
        tdSql.checkRows(2)
        tdSql.execute(f'flush database {self.dbname}')

        time.sleep(self.ttl + 2)
        tdSql.query(f'show {self.dbname}.tables')
        tdSql.checkRows(1)

    def run(self):
        self.check_ttl_result()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
