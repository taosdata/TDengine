import random
import string
from util.log import *
from util.cases import *
from util.sql import *
from util.common import *
from util.sqlset import *
import numpy as np


class TDTestCase:
    updatecfgDict = {'slowLogThresholdTest': ''}
    updatecfgDict["slowLogThresholdTest"]  = 0
    
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        
    def getPath(self, tool="taosBenchmark"):
        if (platform.system().lower() == 'windows'):
            tool = tool + ".exe"
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        paths = []
        for root, dirs, files in os.walk(projPath):
            if ((tool) in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    paths.append(os.path.join(root, tool))
                    break
        if (len(paths) == 0):
            tdLog.exit("taosBenchmark not found!")
            return
        else:
            tdLog.info("taosBenchmark found in %s" % paths[0])
            return paths[0]

    def taosBenchmark(self, param):
        binPath = self.getPath()
        cmd = f"{binPath} {param}"
        tdLog.info(cmd)
        os.system(cmd)
        
    def testSlowQuery(self):
        self.taosBenchmark(" -d db -t 2 -v 2 -n 1000000 -y")
        sql = "select count(*) from db.meters"
        for i in range(10):            
            tdSql.query(sql)
            tdSql.checkData(0, 0, 2 * 1000000)

    def run(self):
        self.testSlowQuery()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
