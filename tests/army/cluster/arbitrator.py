import taos
import sys
import os
import subprocess
import glob
import shutil
import time

from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.srvCtl import *
from frame.caseBase import *
from frame import *
from frame.autogen import *
from frame import epath
# from frame.server.dnodes import *
# from frame.server.cluster import *


class TDTestCase(TBase):
    
    def init(self, conn, logSql, replicaVar=1):
        updatecfgDict = {'dDebugFlag':131}
        super(TDTestCase, self).init(conn, logSql, replicaVar=1, checkColName="c1")
        
        self.valgrind = 0
        self.db = "test"
        self.stb = "meters"
        self.childtable_count = 10
        tdSql.init(conn.cursor(), logSql)  

    def run(self):
        tdSql.execute('CREATE DATABASE db vgroups 1 replica 2;')

        time.sleep(1)

        tdSql.execute("use db;")

        tdSql.execute("CREATE STABLE meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);")

        tdSql.execute("CREATE TABLE d0 USING meters TAGS (\"California.SanFrancisco\", 2);");

        count = 0

        while count < 100:        
            tdSql.query("show arbgroups;")

            if tdSql.getData(0, 4) == 1:
                break

            tdLog.info("wait 1 seconds for is sync")
            time.sleep(1)

            count += 1
            

        tdSql.query("show db.vgroups;")

        if(tdSql.getData(0, 4) == "follower") and (tdSql.getData(0, 6) == "leader"):
            tdLog.info("stop dnode2")
            sc.dnodeStop(2)

        if(tdSql.getData(0, 6) == "follower") and (tdSql.getData(0, 4) == "leader"):
            tdLog.info("stop dnode 3")
            sc.dnodeStop(3)

        
        count = 0
        while count < 100:
            tdSql.query("show db.vgroups;")

            if(tdSql.getData(0, 4) == "assigned ") or (tdSql.getData(0, 6) == "assigned "):
                break
            
            tdLog.info("wait 1 seconds for set assigned")
            time.sleep(1)

            count += 1

        tdSql.execute("INSERT INTO d0 VALUES (NOW, 10.3, 219, 0.31);")

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
