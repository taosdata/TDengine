# pytest --clean --skip_stop cases/18-StreamProcessing/31-OldTsimCases/test_oldcase_lihui_taosdShell_new.py -N 5

import time
from datetime import datetime
import os
import sys
import psutil
import platform
if platform.system().lower() == 'windows':
    import wexpect as taosExpect
else:
    import pexpect as taosExpect
    
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    cluster,
    tdCom
)

class TestOthersOldCaseTaosdshell:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_others_oldcase_taosdShell(self):
        """OldPy: shell create stream

        test taosd shell command

        Catalog:
            - Streams:OldPyCases

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-6-16 lihui from old cases

        """

        tdStream.createSnode()

        # streams = []
        # streams.append(self.Basic0())

        # tdStream.checkAll(streams)
        self.runCase()


    def get_process_pid(self,processname):
        if platform.system().lower() == 'windows':
            pids = psutil.process_iter()
            for pid in pids:
                if(pid.name() == processname):
                    return pid.pid
            return 0
        else:
            process_info_list = []
            process = os.popen('ps -A | grep %s'% processname)
            process_info = process.read()
            tdLog.info(f"process: {process}")
            for i in process_info.split(' '):
                if i != "":
                    process_info_list.append(i)
            print(process_info_list)
            if len(process_info_list) != 0 :
                pid = int(process_info_list[0])
            else :
                pid = 0
            return pid

    def checkAndstopPro(self, processName, startAction, count = 10):
        for i in range(count):
            taosdPid=self.get_process_pid(processName)
            print("taosdPid:",taosdPid)
            if taosdPid != 0  and   taosdPid != ""  :
                tdLog.info("stop taosd %s ,kill pid :%s "%(startAction,taosdPid))
                for j in range(count):
                    os.system("kill -9 %d"%taosdPid)
                    taosdPid=self.get_process_pid(processName)
                    print("taosdPid2:",taosdPid)
                    if taosdPid == 0  or   taosdPid == ""  :
                        tdLog.info("taosd %s is stoped "%startAction)
                        return
            else:
                tdLog.info( "wait start taosd ,times: %d "%i)
            time.sleep(1)
        else :
            tdLog.exit("taosd %s is not running "%startAction)


    def taosdCommandStop(self,startAction,taosdCmdRun):
        processName= "taosd"
        count = 10
        if platform.system().lower() == 'windows':
            processName="taosd.exe"
        taosdCmd = taosdCmdRun + startAction
        tdLog.printNoPrefix("%s"%taosdCmd)
        if platform.system().lower() == 'windows':
            cmd = f"mintty -h never {taosdCmd}"
            os.system(cmd)
        else:
            logTime=datetime.now().strftime('%Y%m%d_%H%M%S_%f')
            os.system(f"nohup {taosdCmd}  >  {logTime}.log  2>&1 &  ")
            self.checkAndstopPro(processName,startAction,count)
            os.system(f"rm -rf  {logTime}.log")


    def taosdCommandExe(self,startAction,taosdCmdRun):
        taosdCmd = taosdCmdRun + startAction
        tdLog.printNoPrefix("%s"%taosdCmd)
        os.system(f"{taosdCmd}")

    # def create(self):
    def runCase(self):
        
        self.db = "db0"
        self.sdb = "source_db"
        
        # database\stb\tb\chiild-tb\rows\topics
        tdSql.execute(f"create user testpy pass 'testpy243#@'")
        tdSql.execute(f"drop database if exists {self.db};")
        tdSql.execute(f"create database {self.db} wal_retention_period 3600;")
        tdSql.execute(f"use {self.db};")
        tdSql.execute(f"create table if not exists {self.db}.stb (ts timestamp, c1 int, c2 float, c3 double) tags (t1 int unsigned);")
        tdSql.execute(f"create table {self.db}.ct1 using {self.db}.stb tags(1000);")
        tdSql.execute(f"create table {self.db}.ct2 using {self.db}.stb tags(2000);")
        tdSql.execute(f"create table if not exists {self.db}.ntb (ts timestamp, c1 int, c2 float, c3 double) ;")
        tdSql.query(f"show {self.db}.stables;")
        tdSql.execute(f"insert into {self.db}.ct1 values(now+0s, 10, 2.0, 3.0);")
        tdSql.execute(f"insert into {self.db}.ct1 values(now+1s, 11, 2.1, 3.1)(now+2s, 12, 2.2, 3.2)(now+3s, 13, 2.3, 3.3);")
        tdSql.execute(f"insert into {self.db}.ntb values(now+2s, 10, 2.0, 3.0);")
        tdSql.execute(f"create topic tpc1 as select * from {self.db}.ct2; ")

        #stream
        tdSql.execute(f"drop database if exists {self.sdb};")
        tdSql.query(f"create database {self.sdb} vgroups 3 wal_retention_period 3600;")
        tdSql.query(f"use {self.sdb}")
        tdSql.query(f"create table if not exists {self.sdb}.stb (ts timestamp, k int) tags (a int);")
        tdSql.query(f"create table {self.sdb}.ct1 using {self.sdb}.stb tags(1000);create table {self.sdb}.ct2 using {self.sdb}.stb tags(2000);create table {self.sdb}.ct3 using {self.sdb}.stb tags(3000);")
        tdSql.query(f"create stream s1 interval(10m) sliding(10m) from {self.sdb}.stb partition by tbname into {self.sdb}.output_stb as select _twstart AS startts, min(k), max(k), sum(k) from %%trows;")

        #TD-19944 -Q=3
        # tdsqlN=tdCom.newTdSql()

        tdSql.query(f"select * from {self.sdb}.stb")
        tdSql.query(f"select * from {self.db}.stb")

    # def insert1(self):
    #     pass

    # def check1(self):

        buildPath = tdCom.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        cfgPath = buildPath + "/../sim/psim/cfg"
        taosdCfgPath = buildPath + "/../sim/dnode1/cfg"

        taosdCmdRun= buildPath + '/build/bin/taosd'
        tdLog.info("cfgPath: %s" % cfgPath)
        # keyDict['h'] = self.hostname
        # keyDict['c'] = cfgPath
        # keyDict['P'] = self.serverPort
        tdDnodes=cluster.dnodes
        for i in range(len(tdDnodes)):
            tdDnodes[i].stoptaosd()              
        
        startAction = " -s -c " + taosdCfgPath 
        tdLog.printNoPrefix("================================ parameter: %s"%startAction)
        self.taosdCommandExe(startAction,taosdCmdRun)
        os.system(" rm -rf sdb.json ") 

        startAction = " --help"
        tdLog.printNoPrefix("================================ parameter: %s"%startAction)
        self.taosdCommandExe(startAction,taosdCmdRun)

        startAction = " -h"
        tdLog.printNoPrefix("================================ parameter: %s"%startAction)
        self.taosdCommandExe(startAction,taosdCmdRun)

        startAction=" -a  jsonFile:./taosdCaseTmp.json"
        tdLog.printNoPrefix("================================ parameter: %s"%startAction)

        if platform.system().lower() == 'windows':
            os.system("echo {\"queryPolicy\":\"3\"} > taosdCaseTmp.json")
        else:
            os.system("echo \'{\"queryPolicy\":\"3\"}\' > taosdCaseTmp.json")
        self.taosdCommandStop(startAction,taosdCmdRun)

        startAction = " -a  jsonFile:./taosdCaseTmp.json -C "
        tdLog.printNoPrefix("================================ parameter: %s"%startAction)
        self.taosdCommandExe(startAction,taosdCmdRun)

        os.system("rm -rf  taosdCaseTmp.json") 

        startAction = " -c " + taosdCfgPath 
        tdLog.printNoPrefix("================================ parameter: %s"%startAction)
        self.taosdCommandStop(startAction,taosdCmdRun)



        startAction = " -e  TAOS_QUERY_POLICY=2 "
        tdLog.printNoPrefix("================================ parameter: %s"%startAction)
        self.taosdCommandStop(startAction,taosdCmdRun)


        startAction=f" -E taosdCaseTmp{os.sep}.env"
        tdLog.printNoPrefix("================================ parameter: %s"%startAction)
        os.system(" mkdir -p taosdCaseTmp ")
        os.system("echo TAOS_QUERY_POLICY=3 > taosdCaseTmp/.env ")
        self.taosdCommandStop(startAction,taosdCmdRun)
        os.system(" rm -rf taosdCaseTmp ")

        startAction = " -V"
        tdLog.printNoPrefix("================================ parameter: %s"%startAction)
        self.taosdCommandExe(startAction,taosdCmdRun)

        startAction = " -k"
        tdLog.printNoPrefix("================================ parameter: %s"%startAction)
        self.taosdCommandExe(startAction,taosdCmdRun)
        return
