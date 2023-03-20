from urllib.parse import uses_relative
import taos
import sys
import os
import time
import inspect
from taos.tmq import Consumer

from pathlib import Path
from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.dnodes import TDDnodes
from util.dnodes import TDDnode
from util.cluster import *
import subprocess

BASEVERSION = "3.0.1.8"
class TDTestCase:
    def caseDescription(self):
        '''
        3.0 data compatibility test 
        case1: basedata version is 3.0.1.8
        '''
        return

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
    
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
        tdDnodes.stop(1)
        print(f"start taosd: rm -rf {dataPath}/*  && nohup taosd -c {cPath} & ")
        os.system(f"rm -rf {dataPath}/*  && nohup taosd -c {cPath} & " )
        sleep(5)


    def buildTaosd(self,bPath):
        # os.system(f"mv {bPath}/build_bak  {bPath}/build ")
        os.system(f" cd {bPath}  ")


    def run(self):
        scriptsPath = os.path.dirname(os.path.realpath(__file__))
        distro_id = distro.id()
        if distro_id == "alpine":
            tdLog.info(f"alpine skip compatibility test")
            return True
        bPath = self.getBuildPath()
        cPath = self.getCfgPath()
        dbname = "test"
        stb = f"{dbname}.meters"
        self.installTaosd(bPath,cPath)
        os.system("echo 'debugFlag 143' > /etc/taos/taos.cfg ")
        tableNumbers=100
        recordNumbers1=100
        recordNumbers2=1000

        # tdsqlF=tdCom.newTdSql()
        # print(tdsqlF)
        # tdsqlF.query(f"SELECT SERVER_VERSION();")
        # print(tdsqlF.query(f"SELECT SERVER_VERSION();"))
        # oldServerVersion=tdsqlF.queryResult[0][0]
        # tdLog.info(f"Base server version is {oldServerVersion}")
        # tdsqlF.query(f"SELECT CLIENT_VERSION();")
        # # the oldClientVersion can't be updated in the same python process,so the version is new compiled verison
        # oldClientVersion=tdsqlF.queryResult[0][0]
        # tdLog.info(f"Base client version is {oldClientVersion}")
        # baseVersion = "3.0.1.8"

        tdLog.printNoPrefix(f"==========step1:prepare and check data in old version-{BASEVERSION}")
        tdLog.info(f" LD_LIBRARY_PATH=/usr/lib  taosBenchmark -t {tableNumbers} -n {recordNumbers1} -y  ")
        os.system(f"LD_LIBRARY_PATH=/usr/lib taosBenchmark -t {tableNumbers} -n {recordNumbers1} -y  ")
        os.system(f"LD_LIBRARY_PATH=/usr/lib taos -s 'use test;create stream current_stream into current_stream_output_stb as select _wstart as `start`, _wend as wend, max(current) as max_current from meters where voltage <= 220 interval (5s);' ")
        os.system('LD_LIBRARY_PATH=/usr/lib taos -s  "use test;create stream power_stream into power_stream_output_stb as select ts, concat_ws(\\".\\", location, tbname) as meter_location, current*voltage*cos(phase) as active_power, current*voltage*sin(phase) as reactive_power from meters partition by tbname;" ')
        os.system('LD_LIBRARY_PATH=/usr/lib taos -s  "use test;show streams;" ')
        os.system(f"sed -i 's/\/etc\/taos/{cPath}/' 0-others/tmqBasic.json ")
        # os.system("LD_LIBRARY_PATH=/usr/lib  taosBenchmark -f 0-others/tmqBasic.json -y ")
        os.system('LD_LIBRARY_PATH=/usr/lib taos -s  "create topic if not exists tmq_test_topic  as select  current,voltage,phase from test.meters where voltage <= 106 and current <= 5;" ')
        os.system('LD_LIBRARY_PATH=/usr/lib taos -s  "use test;show topics;" ')

        tdLog.info(" LD_LIBRARY_PATH=/usr/lib  taosBenchmark -f 0-others/compa4096.json -y  ")
        os.system("LD_LIBRARY_PATH=/usr/lib  taosBenchmark -f 0-others/compa4096.json -y")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -s 'flush database db4096 '")
        cmd = f" LD_LIBRARY_PATH={bPath}/build/lib  {bPath}/build/bin/taos -h localhost ;"
        if os.system(cmd) == 0:
            raise Exception("failed to execute system command. cmd: %s" % cmd)
                
        os.system("pkill  taosd")   # make sure all the data are saved in disk.
        self.checkProcessPid("taosd")


        tdLog.printNoPrefix("==========step2:update new version ")
        self.buildTaosd(bPath)
        tdDnodes.start(1)
        sleep(1)
        tdsql=tdCom.newTdSql()
        print(tdsql)
        cmd = f" LD_LIBRARY_PATH=/usr/lib  taos -h localhost ;"
        if os.system(cmd) == 0:
            raise Exception("failed to execute system command. cmd: %s" % cmd)
        
        tdsql.query(f"SELECT SERVER_VERSION();")
        nowServerVersion=tdsql.queryResult[0][0]
        tdLog.info(f"New server version is {nowServerVersion}")
        tdsql.query(f"SELECT CLIENT_VERSION();")
        nowClientVersion=tdsql.queryResult[0][0]
        tdLog.info(f"New client version is {nowClientVersion}")

        tdLog.printNoPrefix(f"==========step3:prepare and check data in new version-{nowServerVersion}")
        tdsql.query(f"select count(*) from {stb}")
        tdsql.checkData(0,0,tableNumbers*recordNumbers1)    
        # tdsql.query("show streams;")
        # os.system(f"taosBenchmark -t {tableNumbers} -n {recordNumbers2} -y  ")
        # tdsql.query("show streams;")
        # tdsql.query(f"select count(*) from {stb}")
        # tdsql.checkData(0,0,tableNumbers*recordNumbers2)
        tdsql.query(f"select count(*) from db4096.stb0")
        tdsql.checkData(0,0,50000)

        tdsql=tdCom.newTdSql()
        tdLog.printNoPrefix(f"==========step4:verify backticks in taos Sql-TD18542")
        tdsql.execute("drop database if exists db")
        tdsql.execute("create database db")
        tdsql.execute("use db")
        tdsql.execute("create stable db.stb1 (ts timestamp, c1 int) tags (t1 int);")
        tdsql.execute("insert into db.ct1 using db.stb1 TAGS(1) values(now(),11);")
        tdsql.error(" insert into `db.ct2` using db.stb1 TAGS(9) values(now(),11);")
        tdsql.error(" insert into db.`db.ct2` using db.stb1 TAGS(9) values(now(),11);")
        tdsql.execute("insert into `db`.ct3 using db.stb1 TAGS(3) values(now(),13);")
        tdsql.query("select * from db.ct3")
        tdsql.checkData(0,1,13)
        tdsql.execute("insert into db.`ct4` using db.stb1 TAGS(4) values(now(),14);")
        tdsql.query("select * from db.ct4")
        tdsql.checkData(0,1,14)
        print(1)
        tdsql=tdCom.newTdSql()
        tdsql.query("describe  information_schema.ins_databases;")
        qRows=tdsql.queryRows   
        comFlag=True
        j=0 
        while comFlag: 
            for i in  range(qRows) :
                if tdsql.queryResult[i][0] == "retentions" :
                    print("parameters include retentions")
                    comFlag=False
                    break
                else :
                    comFlag=True 
                    j=j+1
            if j == qRows:
                print("parameters don't include retentions")
                caller = inspect.getframeinfo(inspect.stack()[0][0])
                args = (caller.filename, caller.lineno)
                tdLog.exit("%s(%d) failed" % args)
        tdsql.query("show streams;")
        tdsql.checkRows(2)
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
    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
