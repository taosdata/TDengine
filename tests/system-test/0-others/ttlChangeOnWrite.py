import time
from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *


class TDTestCase:
    updatecfgDict = {'ttlUnit': 1, "ttlPushInterval": 3, "ttlChangeOnWrite": 1, "trimVDbIntervalSec": 360,
                     "ttlFlushThreshold": 100, "ttlBatchDropNum": 10}

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)
        self.ttl = 5
        self.tables = 100
        self.dbname = "test"

    def check_batch_drop_num(self):
        tdSql.execute(f'create database {self.dbname} vgroups 1')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'create table stb(ts timestamp, c1 int) tags(t1 int)')
        for i in range(self.tables):
            tdSql.execute(f'create table t{i} using stb tags({i}) ttl {self.ttl}')

        tdSql.execute(f'flush database {self.dbname}')
        time.sleep(self.ttl + self.updatecfgDict['ttlPushInterval'] + 1)
        tdSql.query('show tables')
        tdSql.checkRows(90)

    def check_ttl_result(self):
        tdSql.execute(f'drop database if exists {self.dbname}')
        tdSql.execute(f'create database {self.dbname}')
        tdSql.execute(f'create table {self.dbname}.t1(ts timestamp, c1 int)')
        tdSql.execute(f'create table {self.dbname}.t2(ts timestamp, c1 int) ttl {self.ttl}')
        tdSql.query(f'show {self.dbname}.tables')
        tdSql.checkRows(2)

        tdSql.execute(f'flush database {self.dbname}')
        time.sleep(self.ttl - 1)
        tdSql.execute(f'insert into {self.dbname}.t2 values(now, 1)');

        tdSql.execute(f'flush database {self.dbname}')
        time.sleep(self.ttl - 1)
        tdSql.query(f'show {self.dbname}.tables')
        tdSql.checkRows(2)

        tdSql.execute(f'flush database {self.dbname}')
        time.sleep(self.ttl * 2)
        tdSql.query(f'show {self.dbname}.tables')
        tdSql.checkRows(1)

    def run(self):
        self.check_batch_drop_num()
        self.check_ttl_result()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
