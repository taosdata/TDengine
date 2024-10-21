from ssl import ALERT_DESCRIPTION_CERTIFICATE_UNOBTAINABLE
from numpy import row_stack
import taos
import sys
import time
import os
import platform
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
import json

BASEVERSION = "3.1.1.0"

class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug(f"start to excute {__file__}")
        self.TDDnodes = None
        tdSql.init(conn.cursor())
        self.host = socket.gethostname()
        self.replicaVar = int(replicaVar)
        self.deletedDataSql= '''drop database if exists deldata;create database deldata duration 300;use deldata;
                            create table deldata.stb1 (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp) tags (t1 int);
                            create table deldata.ct1 using deldata.stb1 tags ( 1 );
                            insert into deldata.ct1 values ( now()-0s, 0, 0, 0, 0, 0.0, 0.0, 0, 'binary0', 'nchar0', now()+0a ) ( now()-10s, 1, 11111, 111, 11, 1.11, 11.11, 1, 'binary1', 'nchar1', now()+1a ) ( now()-20s, 2, 22222, 222, 22, 2.22, 22.22, 0, 'binary2', 'nchar2', now()+2a ) ( now()-30s, 3, 33333, 333, 33, 3.33, 33.33, 1, 'binary3', 'nchar3', now()+3a );
                            select avg(c1) from deldata.ct1;
                            delete from deldata.stb1;
                            flush database deldata;
                            insert into deldata.ct1 values ( now()-0s, 0, 0, 0, 0, 0.0, 0.0, 0, 'binary0', 'nchar0', now()+0a ) ( now()-10s, 1, 11111, 111, 11, 1.11, 11.11, 1, 'binary1', 'nchar1', now()+1a ) ( now()-20s, 2, 22222, 222, 22, 2.22, 22.22, 0, 'binary2', 'nchar2', now()+2a ) ( now()-30s, 3, 33333, 333, 33, 3.33, 33.33, 1, 'binary3', 'nchar3', now()+3a );
                            delete from deldata.ct1;'''

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
        if platform.system() == "Linux" and platform.machine() == "aarch64":
            packageName = "TDengine-server-"+  BASEVERSION + "-Linux-arm64.tar.gz"
        else:
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
        os.system(f" cd {bPath}/ && make install ")

    def is_list_same_as_ordered_list(self,unordered_list, ordered_list):
        sorted_list = sorted(unordered_list)
        return sorted_list == ordered_list

    def insertAllData(self,cPath,dbname,tableNumbers,recordNumbers):
        tdLog.info(f"insertAllData")
        # tdLog.info(f" LD_LIBRARY_PATH=/usr/lib  taosBenchmark -d dbtest -t {tableNumbers} -c {cPath} -n {recordNumbers} -v 2 -a 3 -y -k 10 -z 5 ")
        # os.system(f"LD_LIBRARY_PATH=/usr/lib taosBenchmark -d dbtest -t {tableNumbers} -c {cPath} -n {recordNumbers} -v 2 -a 3 -y -k 10 -z 5 ")
        
        print(f"sed -i 's/\"cfgdir\".*/\"cfgdir\": \"{cPath}\",/' 6-cluster/rollup.json  && sed -i '0,/\"name\":.*/s/\"name\":.*/\"name\": \"{dbname}\",/' 6-cluster/rollup.json && sed -i 's/\"childtable_count\":.*/\"childtable_count\": {tableNumbers},/' 6-cluster/rollup.json && sed -i 's/\"insert_rows\":.*/\"insert_rows\": {recordNumbers},/' 6-cluster/rollup.json" )
        os.system(f"sed -i 's/\"cfgdir\".*/\"cfgdir\": \"{cPath}\",/' 6-cluster/rollup.json  && sed -i '0,/\"name\":.*/s/\"name\":.*/\"name\": \"{dbname}\",/' 6-cluster/rollup.json && sed -i 's/\"childtable_count\":.*/\"childtable_count\": {tableNumbers},/' 6-cluster/rollup.json && sed -i 's/\"insert_rows\":.*/\"insert_rows\": {recordNumbers},/' 6-cluster/rollup.json")   
        print("LD_LIBRARY_PATH=/usr/lib taosBenchmark -f 6-cluster/rollup.json -y -k 10 -z 5")
        os.system("LD_LIBRARY_PATH=/usr/lib taosBenchmark -f 6-cluster/rollup.json -y -k 10 -z 5 ")  


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



    def run(self):
        print("123")
        # os.system("taosBenchmark -t 10000 -n 100000 -r 3 ")
        # self.fiveDnodeThreeMnode(dnodeNumbers=3,mnodeNums=3,restartNumbers=2,stopRole='dnode')

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
