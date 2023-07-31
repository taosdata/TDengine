
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
sys.path.append("./7-tmq")
from tmqCommon import *

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def checkData(self):
        tdSql.execute('use db_raw')
        tdSql.query("select * from d1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 120)

        tdSql.query("select * from d2")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        return

    def check(self):
        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/write_raw_block_test'%(buildPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)

        self.checkData()

        return

    def run(self):
        tdSql.prepare()
        self.check()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
