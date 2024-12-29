import taos
import sys
import time
import socket
import os
import threading
import psutil
import platform
from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def compact_auto_dispatch(self):
        tdLog.info("compact_auto_dispatch")
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db duration 1d compact_interval 10m compact_time_range -2d,-1d compact_time_offset 10")
        tdSql.execute("create table db.stb (ts timestamp, a int) tags (tag1 binary(8))")
        tdSql.execute("create table db.c1 using db.stb tags('c1')")
        nowSec = time.time()
        for i in range(2):
            insertSql = f"insert into db.c1 values (%d, 1) tags('c1')" %(1000*(nowSec - 8640*i))
            tdLog.info(f"sql: {insertSql}")
            tdSql.execute(insertSql)
        tdSql.execute("select count(*) from db.c1")
        tdSql.checkRows(100)

    def checkShowCreateWithTimeout(self, db, expectResult, timeout=30):
        result = False
        for i in range(timeout):
            tdSql.query(f'show create database `%s`' %(db))
            tdSql.checkEqual(tdSql.queryResult[0][0], db)
            if expectResult in tdSql.queryResult[0][1]:
                result = True
                break
            time.sleep(1)
        if result == False:
            raise Exception(f"Unexpected result of 'show create database `{db}`':{tdSql.queryResult[0][1]}, expect:{expectResult}")


    def run(self):
        self.compact_auto_dispatch()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
