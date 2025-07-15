import taos
import sys
import time
import socket
import os
import threading

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *
from taos.tmq import *
sys.path.append("./7-tmq")
from tmqCommon import *

class TDTestCase:
    updatecfgDict = {'debugFlag': 135, 'asynclog': 0}
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        #tdSql.init(conn.cursor(), logSql)  # output sql.txt file

    def run(self):
        tdSql.execute(f'create database if not exists db_33225')
        tdSql.execute(f'use db_33225')
        tdSql.execute(f'create stable if not exists s33225 (ts timestamp, c1 int, c2 int) tags (t binary(32), t2 int)')
        tdSql.execute(f'insert into t1 using s33225 tags("__devicid__", 1) values(1669092069068, 0, 1)')

        tdSql.execute("create topic db_33225_topic as select ts,c1,t2 from s33225")

        tdSql.execute(f'alter table s33225 modify column c2 COMPRESS "zlib"')
        tdSql.execute(f'create index dex1 on s33225(t2)')

        return

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())