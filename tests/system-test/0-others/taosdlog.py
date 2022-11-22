import taos
import sys
import time
import os

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *

class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files or "taosd.exe" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/build/bin")]
                    break
        return buildPath

    def run(self):  # sourcery skip: extract-duplicate-method, remove-redundant-fstring
        tdSql.prepare()
        # time.sleep(2)
        tdSql.query("create user testpy pass 'testpy'")

        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        logPath = buildPath + "/../sim/dnode1/log"
        tdLog.info("log path: %s" % logPath)

        tdDnodes.stop(1)
        time.sleep(2)
        tdSql.error("select * from information_schema.ins_databases")
        os.system("rm -rf  %s" % logPath)
        if os.path.exists(logPath) == True:
            tdLog.exit("log pat still exist!")

        tdDnodes.start(1)
        time.sleep(2)
        if os.path.exists(logPath) != True:
            tdLog.exit("log pat is not generated!")

        tdSql.query("select * from information_schema.ins_databases")

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
