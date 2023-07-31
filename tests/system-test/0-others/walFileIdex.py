
import taos
import sys
import time
import socket
import os
import platform
if platform.system().lower() == 'windows':
    import wexpect as taosExpect
else:
    import pexpect as taosExpect

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.cluster import *

class TDTestCase:

    def init(self, conn, logSql):
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

    def preData(self):
        # database\stb\tb\chiild-tb\rows\topics
        tdSql.execute("create user testpy pass 'testpy'")
        tdSql.execute("drop database if exists db0;")
        tdSql.execute("create database db0 WAL_RETENTION_PERIOD -1 WAL_RETENTION_SIZE -1  ;")
        tdSql.execute("use db0;")
        tdSql.execute("create table if not exists db0.stb (ts timestamp, c1 int, c2 float, c3 double) tags (t1 int unsigned);")
        tdSql.execute("create table db0.ct1 using db0.stb tags(1000);")
        tdSql.execute("create table db0.ct2 using db0.stb tags(2000);")
        tdSql.execute("create table if not exists db0.ntb (ts timestamp, c1 int, c2 float, c3 double) ;")
        tdSql.query("show db0.stables;")
        tdSql.execute("insert into db0.ct1 values(now+0s, 10, 2.0, 3.0);")
        tdSql.execute("insert into db0.ct1 values(now+1s, 11, 2.1, 3.1)(now+2s, 12, 2.2, 3.2)(now+3s, 13, 2.3, 3.3);")
        tdSql.execute("insert into db0.ntb values(now+2s, 10, 2.0, 3.0);")
        tdSql.execute("create sma index sma_index_name1 on db0.stb function(max(c1),max(c2),min(c1)) interval(6m,10s) sliding(6m);")
        tdSql.execute("create topic tpc1 as select * from db0.ct2; ")


        #stream
        tdSql.execute("drop database if exists source_db;")
        tdSql.query("create database source_db vgroups 3 wal_retention_period 3600;")
        tdSql.query("use source_db")
        tdSql.query("create table if not exists source_db.stb (ts timestamp, k int) tags (a int);")
        tdSql.query("create table source_db.ct1 using source_db.stb tags(1000);create table source_db.ct2 using source_db.stb tags(2000);create table source_db.ct3 using source_db.stb tags(3000);")
        tdSql.query("create stream s1 into source_db.output_stb as select _wstart AS start, min(k), max(k), sum(k) from source_db.stb interval(10m);")

    def run(self):  
        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        cfgPath = buildPath + "/../sim/psim/cfg"
        taosdCfgPath = buildPath + "/../sim/dnode1/cfg"
        walPath = buildPath + "/../sim/dnode1/data/vnode/vnode*/wal/"
        walFilePath = buildPath + "/../sim/dnode1/data/vnode/vnode2/wal/"

        tdLog.info("insert data")
        self.preData()
        tdDnodes.stop(1)
        time.sleep(2)
        tdLog.info("delete wal filePath")
        # os.system("rm -rf %s/meta-ver*"%walPath)
        os.system("rm -rf %s/*.idx"%walPath)
        os.system("rm -rf %s/*.log"%walPath)
        tdDnodes.start(1)
        tdDnodes.stop(1)
        time.sleep(2)    
        tdLog.info(" modify wal Index file")
        os.system(" echo \"1231abcasep\" >> %s/00000000000000000000.idx"%walFilePath)
        os.system(" echo \"1231abcasep\" >> %s/00000000000000000000.log"%walFilePath)
        tdDnodes.start(1)
        tdDnodes.stop(1)
        
    
    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
