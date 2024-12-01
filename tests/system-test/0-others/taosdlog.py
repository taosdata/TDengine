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
        tdSql.prepare()
        self.buildPath = self.getBuildPath()
        if (self.buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % self.buildPath)
        self.logPath = self.buildPath + "/../sim/dnode1/log"
        tdLog.info("log path: %s" % self.logPath)

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

    def logPathBasic(self):  # sourcery skip: extract-duplicate-method, remove-redundant-fstring
        tdSql.query("create user testpy pass 'testpy'")

        tdDnodes.stop(1)
        time.sleep(2)
        tdSql.error("select * from information_schema.ins_databases")
        tdSql.checkRows(2)
        os.system("rm -rf  %s" % self.logPath)
        if os.path.exists(self.logPath) == True:
            tdLog.exit("log path still exist!")

        tdDnodes.start(1)
        time.sleep(2)
        if os.path.exists(self.logPath) != True:
            tdLog.exit("log path is not generated!")

        tdSql.query("select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def prepareCfg(self, cfgPath, cfgDict):
        with open(cfgPath + "/taos.cfg", "w") as f:
            for key in cfgDict:
                f.write("%s %s\n" % (key, cfgDict[key]))

    def check_function_1(self):
        # Implementation of check function 1
        tdLog.info("Running check function 1")
        # Add your check logic here

    def check_function_2(self):
        # Implementation of check function 2
        tdLog.info("Running check function 2")
        # Add your check logic here

    def check_function_3(self):
        # Implementation of check function 3
        tdLog.info("Running check function 3")
        # Add your check logic here

    def prepareCheckFunctions(self):
        self.check_functions = {
            "check_function_1": self.check_function_1,
            "check_function_2": self.check_function_2,
            "check_function_3": self.check_function_3
        }

    def checkLogOutput(self):
        self.prepareCheckFunctions()
        for key, check_func in self.check_functions.items():
            print(f"Running {key}")
            check_func()

    def run(self):
        # self.logPathBasic()
        self.checkLogOutput()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
