from ssl import ALERT_DESCRIPTION_CERTIFICATE_UNOBTAINABLE
from numpy import row_stack
import taos
import sys
import time
import os

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import TDDnodes
from util.dnodes import TDDnode
from util.cluster import *
from util.common import *
sys.path.append("./6-cluster")
from clusterCommonCreate import *
from clusterCommonCheck import clusterComCheck
from pathlib import Path
from taos.tmq import Consumer


import time
import socket
import subprocess
from multiprocessing import Process
import threading
import time
import inspect
import ctypes

BASEVERSION = "3.0.5.0"

class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug(f"start to excute {__file__}")
        self.TDDnodes = None
        tdSql.init(conn.cursor())
        self.host = socket.gethostname()
        self.replicaVar = int(replicaVar)

    def checkProcessPid(self,processName):
        i=0
        while i<60:
            print(f"wait stop {processName}")
            processPid = subprocess.getstatusoutput(f'ps aux|grep {processName} |grep -v "grep"|awk \'{{print $2}}\'')[1]
            print(f"times:{i},{processName}-pid:{processPid}")
            if(processPid == ""):
                break
            i += 1
            sleep(1)
        else:
            print(f'this processName is not stoped in 60s')

            
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
                    buildPath = root[:len(root)-len("/build/bin")]
                    break
        return buildPath

    def getCfgPath(self):
        buildPath = self.getBuildPath()
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            cfgPath = buildPath + "/../sim/dnode1/cfg/"
        else:
            cfgPath = buildPath + "/../sim/dnode1/cfg/"

        return cfgPath

    def installTaosd(self,bPath,cPath):
        # os.system(f"rmtaos && mkdir -p {self.getBuildPath()}/build/lib/temp &&  mv {self.getBuildPath()}/build/lib/libtaos.so*  {self.getBuildPath()}/build/lib/temp/ ")
        # os.system(f" mv {bPath}/build  {bPath}/build_bak ")
        # os.system(f"mv {self.getBuildPath()}/build/lib/libtaos.so  {self.getBuildPath()}/build/lib/libtaos.so_bak ")
        # os.system(f"mv {self.getBuildPath()}/build/lib/libtaos.so.1  {self.getBuildPath()}/build/lib/libtaos.so.1_bak ")
        
        packagePath = "/usr/local/src/"
        dataPath = cPath + "/../data/"
        packageName = "TDengine-server-"+  BASEVERSION + "-Linux-x64.tar.gz"
        packageTPath = packageName.split("-Linux-")[0]
        my_file = Path(f"{packagePath}/{packageName}")
        if not  my_file.exists():
            print(f"{packageName} is not exists")
            tdLog.info(f"cd {packagePath} &&  wget https://www.tdengine.com/assets-download/3.0/{packageName}")
            os.system(f"cd {packagePath} &&  wget https://www.tdengine.com/assets-download/3.0/{packageName}")
        else: 
            print(f"{packageName} has been exists")
        os.system(f" cd {packagePath} &&  tar xvf  {packageName} && cd {packageTPath} &&  ./install.sh  -e no  " )
        # tdDnodes.stop(1)
        # print(f"start taosd: rm -rf {dataPath}/*  && nohup taosd -c {cPath} & ")
        # os.system(f"rm -rf {dataPath}/*  && nohup taosd -c {cPath} & " )
        # sleep(5)


    def buildTaosd(self,bPath):
        # os.system(f"mv {bPath}/build_bak  {bPath}/build ")
        os.system(f" cd {bPath} ")

    def is_list_same_as_ordered_list(self,unordered_list, ordered_list):
        sorted_list = sorted(unordered_list)
        return sorted_list == ordered_list

    def insertAllData(self,cPath):
        tableNumbers=100
        recordNumbers1=100
        recordNumbers2=1000
        tdLog.info(f"insertAllData")
        tdLog.info(f" LD_LIBRARY_PATH=/usr/lib  taosBenchmark -t {tableNumbers} -c {cPath} -n {recordNumbers1} -a 3 -y -k '-1' -z 5 ")
        os.system(f"LD_LIBRARY_PATH=/usr/lib taosBenchmark -t {tableNumbers} -c {cPath} -n {recordNumbers1} -a 3 -y -k '-1' -z 5 ")
        # os.system(f"LD_LIBRARY_PATH=/usr/lib taos -s 'use test;create stream current_stream into current_stream_output_stb as select _wstart as `start`, _wend as wend, max(current) as max_current from meters where voltage <= 220 interval (5s);' ")
        # os.system('LD_LIBRARY_PATH=/usr/lib taos -s  "use test;create stream power_stream into power_stream_output_stb as select ts, concat_ws(\\".\\", location, tbname) as meter_location, current*voltage*cos(phase) as active_power, current*voltage*sin(phase) as reactive_power from meters partition by tbname;" ')
        # os.system('LD_LIBRARY_PATH=/usr/lib taos -s  "use test;show streams;" ')
        print(f"sed -i 's/\/etc\/taos/{cPath}/' 0-others/compa4096.json ")
        
        os.system(f"sed -i 's/\/etc\/taos/{cPath}/' 0-others/compa4096.json ")
        os.system('LD_LIBRARY_PATH=/usr/lib taos -s  "alter database test WAL_RETENTION_PERIOD 1000" ')
        os.system('LD_LIBRARY_PATH=/usr/lib taos -s  "create topic if not exists tmq_test_topic  as select  current,voltage,phase from test.meters where voltage <= 106 and current <= 5;" ')
        os.system('LD_LIBRARY_PATH=/usr/lib taos -s  "use test;show topics;" ')
        tdLog.info(" LD_LIBRARY_PATH=/usr/lib  taosBenchmark -f 0-others/compa4096.json -y  ")
        os.system("LD_LIBRARY_PATH=/usr/lib  taosBenchmark -f 0-others/compa4096.json -y")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -s 'flush database db4096 '")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -f 0-others/TS-3131.tsql")

    def insertData(self,countstart,countstop):
        # fisrt add data : db\stable\childtable\general table

        for couti in range(countstart,countstop):
            tdLog.debug("drop database if exists db%d" %couti)
            tdSql.execute("drop database if exists db%d" %couti)
            print("create database if not exists db%d replica 1 duration 300" %couti)
            tdSql.execute("create database if not exists db%d replica 1 duration 300" %couti)
            tdSql.execute("use db%d" %couti)
            tdSql.execute(
            '''create table stb1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            tags (t1 int)
            '''
            )
            tdSql.execute(
                '''
                create table t1
                (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
                '''
            )
            for i in range(4):
                tdSql.execute(f'create table ct{i+1} using stb1 tags ( {i+1} )')


    def fiveDnodeThreeMnode(self,dnodeNumbers,mnodeNums,restartNumbers,stopRole):
        tdLog.printNoPrefix("======== test case 1: ")
        paraDict = {'dbName':     'db0_0',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    4,
                    'replica':    1,
                    'stbName':    'stb',
                    'stbNumbers': 2,
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1}, {'type': 'binary', 'len':20, 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1}, {'type': 'binary', 'len':20, 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbNum':     200,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    "rowsPerTbl": 1000,
                    "batchNum": 5000
                    }
        hostname = socket.gethostname()
        dnodeNumbers=int(dnodeNumbers)

        tdLog.info("first check dnode and mnode")
        tdSql=tdCom.newTdSql()
        tdSql.query("select * from information_schema.ins_dnodes;")
        tdSql.checkData(0,1,'%s:6030'%self.host)

        tdLog.printNoPrefix(f"==========step1:prepare  cluster of {dnodeNumbers} dnodes whith old version-{BASEVERSION} ")
        
        scriptsPath = os.path.dirname(os.path.realpath(__file__))
        distro_id = distro.id()
        if distro_id == "alpine":
            tdLog.info(f"alpine skip Roll test")
            return True
        if platform.system().lower() == 'windows':
            tdLog.info(f"Windows skip Roll test")
            return True

        tdLog.info("====step1.1:stop all taosd and clear data dir,then start all old taosd  ====")

        bPath = self.getBuildPath()
        cPath = self.getCfgPath()
        tdDnodes=cluster.dnodes
        for i in range(dnodeNumbers):
            tdDnodes[i].stoptaosd()
        self.installTaosd(bPath,cPath)
        for i in range(dnodeNumbers):
            dnode_cfgPath = tdDnodes[i].cfgDir     
            dnode_dataPath = tdDnodes[i].dataDir
            os.system(f"rm -rf {dnode_dataPath}/*  && nohup taosd -c {dnode_cfgPath} & ")
    
        tdLog.info("====step1.2: create dnode on cluster ====")
        
        for i in range(1,dnodeNumbers):
            dnode_id = tdDnodes[i].cfgDict["fqdn"] +  ":" + tdDnodes[i].cfgDict["serverPort"]
            os.system(f" LD_LIBRARY_PATH=/usr/lib taos -s  'create dnode \"{dnode_id}\" ' ")

        os.system(" LD_LIBRARY_PATH=/usr/lib taos -s  'show dnodes' ")   
        sleep(5)     
        tdLog.info("====step1.3: insert data, includes time data, tmq and stream ====")
        tableNumbers=100
        recordNumbers1=100
        recordNumbers2=1000

        dbname = "test"
        stb = f"{dbname}.meters"
        # os.system("echo 'debugFlag 143' > /etc/taos/taos.cfg ")
        threads=[]
        threads.append(threading.Thread(target=self.insertAllData, args=(cPath,)))
        for tr in threads:
            tr.start()
        sleep(10)
        tdLog.printNoPrefix("==========step2:start to rolling upgdade ")
        for i in range(dnodeNumbers):
            tdDnodes[i].running = 1
            tdDnodes[i].stoptaosd()
            sleep(2)
            tdDnodes[i].starttaosd()      

        for tr in threads:
            tr.join()

        tdsql=tdCom.newTdSql()
        print(tdsql)
        tdsql.query("select * from information_schema.ins_dnodes;")
        tdLog.info(tdsql.queryResult)
        tdsql.checkData(2,1,'%s:6230'%self.host)
        tdSql=tdCom.newTdSql()
        clusterComCheck.checkDnodes(dnodeNumbers)

        tdsql.query(f"SELECT SERVER_VERSION();")
        nowServerVersion=tdsql.queryResult[0][0]
        tdLog.info(f"New server version is {nowServerVersion}")
        tdsql.query(f"SELECT CLIENT_VERSION();")
        nowClientVersion=tdsql.queryResult[0][0]
        tdLog.info(f"New client version is {nowClientVersion}")

        tdLog.printNoPrefix(f"==========step3:prepare and check data in new version-{nowServerVersion}")
        tdsql.query(f"select count(*) from {stb}")
        tdsql.checkData(0,0,tableNumbers*recordNumbers1)
        tdsql.query(f"select count(*) from db4096.stb0")
        tdsql.checkData(0,0,50000)

        # tdsql.query("show streams;")
        # tdsql.checkRows(2)
        tdsql.query("select *,tbname from d0.almlog where mcid='m0103';")
        tdsql.checkRows(6)
        expectList = [0,3003,20031,20032,20033,30031]
        resultList = []
        for i in range(6):
            resultList.append(tdsql.queryResult[i][3])
        print(resultList)
        if self.is_list_same_as_ordered_list(resultList,expectList):
            print("The unordered list is the same as the ordered list.")
        else:
            tdlog.error("The unordered list is not the same as the ordered list.")
        tdsql.execute("insert into test.d80 values (now+1s, 11, 103, 0.21);")
        tdsql.execute("insert into test.d9 values (now+5s, 4.3, 104, 0.4);")

        conn = taos.connect()

        consumer = Consumer(
            {
                "group.id": "tg75",
                "client.id": "124",
                "td.connect.user": "root",
                "td.connect.pass": "taosdata",
                "enable.auto.commit": "true",
                "experimental.snapshot.enable":  "true",
            }
        )
        consumer.subscribe(["tmq_test_topic"])

        while True:
            res = consumer.poll(10)
            if not res:
                break
            err = res.error()
            if err is not None:
                raise err
            val = res.value()

            for block in val:
                print(block.fetchall())
        tdsql.query("show topics;")
        tdsql.checkRows(1)

        
        # #check mnode status
        # tdLog.info("check mnode status")
        # clusterComCheck.checkMnodeStatus(mnodeNums)


    def run(self):
        # print(self.master_dnode.cfgDict)
        self.fiveDnodeThreeMnode(dnodeNumbers=3,mnodeNums=3,restartNumbers=2,stopRole='dnode')

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
