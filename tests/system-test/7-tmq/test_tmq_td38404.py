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
from taos import *

sys.path.append("./7-tmq")
from tmqCommon import *

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        #tdSql.init(conn.cursor(), logSql)  # output sql.txt file

    def test(self):
        tdSql.execute(f'create database if not exists db_td38404 vgroups 1')
        tdSql.execute(f'use db_td38404')
        tdSql.execute(f'create stable if not exists s5466 (ts timestamp, c1 int, c2 int) tags (t binary(32), t2 nchar(4))')
        tdSql.execute(f'insert into t1 using s5466 tags("","") values(1669092069068, 0, 1)')

        tdSql.execute("create topic db_38404_topic with meta as database db_td38404")
        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_td38404'%(buildPath)
        # cmdStr = '/Users/mingming/code/TDengine2/debug/build/bin/tmq_td38404'
        tdLog.info(cmdStr)
        os.system(cmdStr)


    def run(self):
        self.test()


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
