
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

    def run(self):
        tdSql.prepare()
        buildPath = tdCom.getBuildPath()
        cmdStr1 = '%s/build/bin/taosBenchmark -i 50 -B 1 -t 1000 -n 100000 -y &'%(buildPath)
        tdLog.info(cmdStr1)
        os.system(cmdStr1)
        time.sleep(15)

        cmdStr2 = '%s/build/bin/tmq_offset_test &'%(buildPath)
        tdLog.info(cmdStr2)
        os.system(cmdStr2)

        time.sleep(20)

        os.system("kill -9 `pgrep taosBenchmark`")
        result = os.system("kill -9 `pgrep tmq_offset_test`")
        if result != 0:
            tdLog.exit("tmq_offset_test error!")

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
