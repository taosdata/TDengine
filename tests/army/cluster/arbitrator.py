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
        tdLog.info(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), logSql)  

    def run(self):
        tdLog.info("create database")
        tdSql.execute('CREATE DATABASE db vgroups 1 replica 2;')

        if self.waitTransactionZero() is False:
            tdLog.exit(f"create db transaction not finished")
            return False

        time.sleep(1)

        tdSql.execute("use db;")

        tdLog.info("create stable")
        tdSql.execute("CREATE STABLE meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);")

        if self.waitTransactionZero() is False:
            tdLog.exit(f"create stable transaction not finished")
            return False

        tdLog.info("create table")
        tdSql.execute("CREATE TABLE d0 USING meters TAGS (\"California.SanFrancisco\", 2);");

        count = 0

        tdLog.info("waiting vgroup is sync")
        while count < 100:        
            tdSql.query("show arbgroups;")

            if tdSql.getData(0, 4) == True:
                break

            tdLog.info("wait %d seconds for is sync"%count)
            time.sleep(1)

            count += 1

        if count == 100:
            tdLog.exit("arbgroup sync failed")
            return 
            
        count = 0
        while count < 100:
            tdSql.query("show db.vgroups;")

            if(tdSql.getData(0, 4) == "follower") and (tdSql.getData(0, 7) == "leader"):
                tdLog.info("stop dnode2")
                sc.dnodeStop(2)
                break

            if(tdSql.getData(0, 7) == "follower") and (tdSql.getData(0, 4) == "leader"):
                tdLog.info("stop dnode 3")
                sc.dnodeStop(3)
                break
            
            time.sleep(1)
            count += 1

        if count == 100:
            tdLog.exit("check leader and stop node failed")
            return    
        
        count = 0
        while count < 100:
            tdSql.query("show db.vgroups;")

            if(tdSql.getData(0, 4) == "assigned ") or (tdSql.getData(0, 7) == "assigned "):
                break
            
            tdLog.info("wait %d seconds for set assigned"%count)
            time.sleep(1)

            count += 1
        
        if count == 100:
            tdLog.exit("check assigned failed")
            return

        tdSql.execute("INSERT INTO d0 VALUES (NOW, 10.3, 219, 0.31);")

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
