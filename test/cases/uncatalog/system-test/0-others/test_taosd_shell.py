from new_test_framework.utils import tdLog, tdSql, tdDnodes, tdCom, cluster
import taos
import sys
import time
from datetime import datetime
import socket
import os
import platform
if platform.system().lower() == 'windows':
    import wexpect as taosExpect
else:
    import pexpect as taosExpect


class TestTaosdShell:

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

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

    def preData(self):
        # database\stb\tb\chiild-tb\rows\topics
        tdSql.execute("create user testpy pass 'testpy243#@'")
        tdSql.execute("drop database if exists db0;")
        tdSql.execute("create database db0 wal_retention_period 3600;")
        tdSql.execute("use db0;")
        tdSql.execute("create table if not exists db0.stb (ts timestamp, c1 int, c2 float, c3 double) tags (t1 int unsigned);")
        tdSql.execute("create table db0.ct1 using db0.stb tags(1000);")
        tdSql.execute("create table db0.ct2 using db0.stb tags(2000);")
        tdSql.execute("create table if not exists db0.ntb (ts timestamp, c1 int, c2 float, c3 double) ;")
        tdSql.query("show db0.stables;")
        tdSql.execute("insert into db0.ct1 values(now+0s, 10, 2.0, 3.0);")
        tdSql.execute("insert into db0.ct1 values(now+1s, 11, 2.1, 3.1)(now+2s, 12, 2.2, 3.2)(now+3s, 13, 2.3, 3.3);")
        tdSql.execute("insert into db0.ntb values(now+2s, 10, 2.0, 3.0);")
        tdSql.execute("create topic tpc1 as select * from db0.ct2; ")


        #stream
        tdSql.execute("drop database if exists source_db;")
        tdSql.query("create database source_db vgroups 3 wal_retention_period 3600;")
        tdSql.query("use source_db")
        tdSql.query("create table if not exists source_db.stb (ts timestamp, k int) tags (a int);")
        tdSql.query("create table source_db.ct1 using source_db.stb tags(1000);create table source_db.ct2 using source_db.stb tags(2000);create table source_db.ct3 using source_db.stb tags(3000);")
        if platform.system().lower() == 'windows':
            pass
        else:
            tdSql.query("create stream s1 into source_db.output_stb as select _wstart AS startts, min(k), max(k), sum(k) from source_db.stb interval(10m);")


        #TD-19944 -Q=3
        tdsqlN=tdCom.newTdSql()

        tdsqlN.query("select * from source_db.stb")
        tdsqlN.query("select * from db0.stb")

    def test_taosd_shell(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx
        """
        # tdSql.prepare()
        # time.sleep(2)
        self.preData()
        #tdLog.info ("hostname: %s" % hostname)

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
    
        tdLog.success(f"{__file__} successfully executed")

