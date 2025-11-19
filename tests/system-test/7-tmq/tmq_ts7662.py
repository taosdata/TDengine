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
        tdSql.execute(f'create database if not exists db_7662 vgroups 1')
        tdSql.execute(f'use db_7662')
        tdSql.execute(f'create stable if not exists ts7662 (ts timestamp, c1 decimal(8,2)) tags (t binary(32))')
        tdSql.execute(f'create table t1 using ts7662 tags("t1") t2 using ts7662 tags("t2")')

        tdSql.execute("create topic topic_ts7662 with meta as stable ts7662 where t='t1'")
        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_ts7662'%(buildPath)
        tdLog.info(cmdStr)
        ret = os.system(cmdStr)
        if ret != 0:
            tdLog.exit("varbinary_test ret != 0")

        return

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
