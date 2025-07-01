import sys
import os
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *
import time
from datetime import datetime

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

        self.rowNum = 10
        self.ts = 1537146000000

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
        
    def run(self):
        binPath = self.getPath()
        tdLog.debug("insert full data block that has first time '2021-10-02 00:00:00.001' and flush db")
        os.system(f"{binPath} -f ./2-query/megeFileSttQuery.json")
        tdSql.execute("flush database db;")
        
        tdLog.debug("insert only a piece of data that is behind the time that already exists and flush db")
        tdSql.execute("insert into  db.d0 values ('2021-10-01 23:59:59.990',12.793,208,0.84) ;")
        tdSql.execute("flush database db;")
        tdLog.debug("check data")
        sleep(1)
        tdSql.query("select count(*) from db.d0;")
        tdSql.checkData(0,0,10001)
        tdSql.execute("drop database db;")





        tdLog.debug("insert full data block that has first time '2021-10-02 00:00:00.001' and flush db")
        os.system(f"{binPath} -f ./2-query/megeFileSttQuery.json")
        tdSql.execute("flush database db;")
        
        tdLog.debug("insert  four pieces of disorder data, and the time range covers the data file that was previously placed on disk and flush db")
        os.system(f"{binPath} -f ./2-query/megeFileSttQueryUpdate.json")
        tdSql.execute("flush database db;")
        tdLog.debug("check data")
        tdSql.query("select count(*) from db.d0;",queryTimes=3)
        tdSql.checkData(0,0,10004)
        tdSql.query("select ts from db.d0 limit 5;")
        tdSql.checkData(0, 0, '2021-10-02 00:00:00.001')
        tdSql.checkData(1, 0, '2021-10-02 00:01:00.000')

        tdLog.debug("update the same disorder data and flush db")
        os.system(f"{binPath} -f ./2-query/megeFileSttQueryUpdate.json")
        tdSql.query("select ts from db.d0 limit 5;")
        tdSql.checkData(0, 0, '2021-10-02 00:00:00.001')
        tdSql.checkData(1, 0, '2021-10-02 00:01:00.000')        
        tdSql.execute("flush database db;")
        tdSql.query("select ts from db.d0 limit 5;")
        tdSql.checkData(0, 0, '2021-10-02 00:00:00.001')
        tdSql.checkData(1, 0, '2021-10-02 00:01:00.000')

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
