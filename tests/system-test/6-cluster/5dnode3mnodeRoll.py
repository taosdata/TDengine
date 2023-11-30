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
        sleep(5)     
        os.system(" LD_LIBRARY_PATH=/usr/lib taos -s  'show dnodes' ")   

        for i in range(2,dnodeNumbers+1):
            os.system(f" LD_LIBRARY_PATH=/usr/lib taos -s  'create mnode on dnode {i} ' ")
        sleep(10)     
        os.system(" LD_LIBRARY_PATH=/usr/lib taos -s  'show mnodes' ")   

        tdLog.info("====step1.3: insert data, includes time data, tmq and stream ====")
        tableNumbers1=100
        recordNumbers1=100000
        recordNumbers2=1000

        dbname = "dbtest"
        stb = f"{dbname}.meters"
        cPath_temp=cPath.replace("/","\/")

        # os.system("echo 'debugFlag 143' > /etc/taos/taos.cfg ")
        # create database and tables 
        print(f"sed -i 's/\"cfgdir\".*/\"cfgdir\": \"{cPath_temp}\",/' 6-cluster/rollup_db.json  && sed -i '0,/\"name\":.*/s/\"name\":.*/\"name\": \"{dbname}\",/' 6-cluster/rollup_db.json ")
        os.system(f"sed -i 's/\"cfgdir\".*/\"cfgdir\": \"{cPath_temp}\",/' 6-cluster/rollup_db.json  && sed -i '0,/\"name\":.*/s/\"name\":.*/\"name\": \"{dbname}\",/' 6-cluster/rollup_db.json") 
        print("LD_LIBRARY_PATH=/usr/lib taosBenchmark -f 6-cluster/rollup_db.json -y ")
        os.system("LD_LIBRARY_PATH=/usr/lib taosBenchmark -f 6-cluster/rollup_db.json -y")             
        # insert data 
        tdLog.info(f" LD_LIBRARY_PATH=/usr/lib  taosBenchmark -d test -t {tableNumbers1} -c {cPath} -n {recordNumbers2} -v 2 -a 3 -y -k 10 -z 5 ")
        os.system(f"LD_LIBRARY_PATH=/usr/lib taosBenchmark -d test -t {tableNumbers1} -c {cPath} -n {recordNumbers2} -v 2 -a 3 -y -k 10 -z 5 ")
        
        # os.system(f"LD_LIBRARY_PATH=/usr/lib taos -s 'use test;create stream current_stream into current_stream_output_stb as select _wstart as `start`, _wend as wend, max(current) as max_current from meters where voltage <= 220 interval (5s);' ")
        # os.system(f'LD_LIBRARY_PATH=/usr/lib taos -s  "use test;create stream power_stream into power_stream_output_stb as select ts, concat_ws(\\".\\", location, tbname) as meter_location, current*voltage*cos(phase) as active_power, current*voltage*sin(phase) as reactive_power from meters partition by tbname;" ')
        # os.system(f'LD_LIBRARY_PATH=/usr/lib taos -s  "use test;show streams;" ')
        os.system(f'LD_LIBRARY_PATH=/usr/lib taos -s  "alter database test WAL_RETENTION_PERIOD 1000" ')
        os.system(f'LD_LIBRARY_PATH=/usr/lib taos -s  "create topic if not exists tmq_test_topic  as select  current,voltage,phase from test.meters where voltage <= 106 and current <= 5;" ')
        os.system(f'LD_LIBRARY_PATH=/usr/lib taos -s  "use test;show topics;" ')
        
        print(f"sed -i 's/\"cfgdir\".*/\"cfgdir\": \"{cPath_temp}\",/' 0-others/compa4096.json ")
        os.system(f"sed -i 's/\"cfgdir\".*/\"cfgdir\": \"{cPath_temp}\",/'0-others/compa4096.json ")
        tdLog.info(" LD_LIBRARY_PATH=/usr/lib  taosBenchmark -f 0-others/compa4096.json -y   -k 10 -z 5  ")
        os.system("LD_LIBRARY_PATH=/usr/lib  taosBenchmark -f 0-others/compa4096.json -y   -k 10 -z 5 ")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -s 'flush database db4096 '")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -f 0-others/TS-3131.tsql")
        # self.buildTaosd(bPath)

        # add deleted  data
        os.system(f'LD_LIBRARY_PATH=/usr/lib taos -s "{self.deletedDataSql}" ')

        threads=[]
        threads.append(threading.Thread(target=self.insertAllData, args=(cPath_temp,dbname,tableNumbers1,recordNumbers1)))
        for tr in threads:
            tr.start()
         # when  inserting data porcess has been started  up ,we can upgrade taosd
        sleep(5)
        tdLog.printNoPrefix("==========step2:start to rolling upgdade ")
        for i in range(dnodeNumbers):
            tdDnodes[i].running = 1
            tdDnodes[i].stoptaosd()
            sleep(2)
            tdDnodes[i].starttaosd()      

        for tr in threads:
            tr.join()

        tdLog.printNoPrefix(f"==========step3:check dnode status ")
        #  wait 10s for  taosd cluster  ready
        sleep(10)
        tdsql=tdCom.newTdSql()
        tdsql.query("select * from information_schema.ins_dnodes;")
        tdLog.info(tdsql.queryResult)
        tdsql.checkData(2,1,'%s:6230'%self.host)
        clusterComCheck.checkDnodes(dnodeNumbers)

        tdsql1=tdCom.newTdSql()
        tdsql1.query(f"SELECT SERVER_VERSION();")
        nowServerVersion=tdsql1.queryResult[0][0]
        tdLog.printNoPrefix(f"==========step4:prepare and check data in new version-{nowServerVersion}")

        tdLog.info(f"New server version is {nowServerVersion}")
        tdsql1.query(f"SELECT CLIENT_VERSION();")
        nowClientVersion=tdsql1.queryResult[0][0]
        tdLog.info(f"New client version is {nowClientVersion}")

        tdsql1.query(f"select count(*) from {stb}")
        tdsql1.checkData(0,0,tableNumbers1*recordNumbers1)
        tdsql1.query(f"select count(*) from db4096.stb0")
        tdsql1.checkData(0,0,50000)

        # checkout deleted data
        tdsql.execute("insert into deldata.ct1 values ( now()-0s, 0, 0, 0, 0, 0.0, 0.0, 0, 'binary0', 'nchar0', now()+0a ) ( now()-10s, 1, 11111, 111, 11, 1.11, 11.11, 1, 'binary1', 'nchar1', now()+1a ) ( now()-20s, 2, 22222, 222, 22, 2.22, 22.22, 0, 'binary2', 'nchar2', now()+2a ) ( now()-30s, 3, 33333, 333, 33, 3.33, 33.33, 1, 'binary3', 'nchar3', now()+3a );")
        tdsql.query("flush database deldata;select avg(c1) from deldata.ct1;")


        # tdsql1.query("show streams;")
        # tdsql1.checkRows(2)
        tdsql1.query("select *,tbname from d0.almlog where mcid='m0103';")
        tdsql1.checkRows(6)
        expectList = [0,3003,20031,20032,20033,30031]
        resultList = []
        for i in range(6):
            resultList.append(tdsql1.queryResult[i][3])
        print(resultList)
        if self.is_list_same_as_ordered_list(resultList,expectList):
            print("The unordered list is the same as the ordered list.")
        else:
            tdlog.error("The unordered list is not the same as the ordered list.")
        tdsql1.execute(f"insert into test.d80 values (now+1s, 11, 103, 0.21);")
        tdsql1.execute(f"insert into test.d9 values (now+5s, 4.3, 104, 0.4);")

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
        tdsql1.query("show topics;")
        tdsql1.checkRows(1)

        
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
