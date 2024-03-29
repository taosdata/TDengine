
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
        retCode = os.system(cmdStr)
        # run program code from system return , 0 is success
        runCode = retCode & 0xFF
        # program retur code from main function
        progCode = retCode >> 8

        tdLog.info(f"{cmdStr} ret={retCode} runCode={runCode} progCode={progCode}")

        if runCode != 0:
            tdLog.exit(f"run {cmdStr} failed, have system error.")
            return
        
        if progCode != 0:
            tdLog.exit(f"{cmdStr} found problem, return code = {progCode}.")
            return

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
