from urllib.parse import uses_relative
import taos
import sys
import os
import time
import platform
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

BASEVERSION = "3.0.2.3"
class TDTestCase:
    def caseDescription(self):
        f'''
        3.0 data compatibility test 
        case1: basedata version is {BASEVERSION}
        '''
        return

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        self.deletedDataSql= '''drop database if exists deldata;create database deldata duration 300 stt_trigger 4; ;use deldata;
                            create table deldata.stb1 (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp) tags (t1 int);
                            create table deldata.ct1 using deldata.stb1 tags ( 1 );
                            insert into deldata.ct1 values ( now()-0s, 0, 0, 0, 0, 0.0, 0.0, 0, 'binary0', 'nchar0', now()+0a ) ( now()-10s, 1, 11111, 111, 11, 1.11, 11.11, 1, 'binary1', 'nchar1', now()+1a ) ( now()-20s, 2, 22222, 222, 22, 2.22, 22.22, 0, 'binary2', 'nchar2', now()+2a ) ( now()-30s, 3, 33333, 333, 33, 3.33, 33.33, 1, 'binary3', 'nchar3', now()+3a );
                            select avg(c1) from deldata.ct1;
                            delete from deldata.stb1;
                            flush database deldata;
                            insert into deldata.ct1 values ( now()-0s, 0, 0, 0, 0, 0.0, 0.0, 0, 'binary0', 'nchar0', now()+0a ) ( now()-10s, 1, 11111, 111, 11, 1.11, 11.11, 1, 'binary1', 'nchar1', now()+1a ) ( now()-20s, 2, 22222, 222, 22, 2.22, 22.22, 0, 'binary2', 'nchar2', now()+2a ) ( now()-30s, 3, 33333, 333, 33, 3.33, 33.33, 1, 'binary3', 'nchar3', now()+3a );
                            delete from deldata.ct1;
                            insert into deldata.ct1 values ( now()-0s, 0, 0, 0, 0, 0.0, 0.0, 0, 'binary0', 'nchar0', now()+0a );
                            flush database deldata;'''   
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
        tdDnodes.stop(1)
        print(f"start taosd: rm -rf {dataPath}/*  && nohup taosd -c {cPath} & ")
        os.system(f"rm -rf {dataPath}/*  && nohup taosd -c {cPath} & " )
        sleep(5)


    def buildTaosd(self,bPath):
        # os.system(f"mv {bPath}/build_bak  {bPath}/build ")
        os.system(f" cd {bPath} ")

    def is_list_same_as_ordered_list(self,unordered_list, ordered_list):
        sorted_list = sorted(unordered_list)
        return sorted_list == ordered_list
        
    def run(self):
        scriptsPath = os.path.dirname(os.path.realpath(__file__))
        distro_id = distro.id()
        if distro_id == "alpine":
            tdLog.info(f"alpine skip compatibility test")
            return True
        if platform.system().lower() == 'windows':
            tdLog.info(f"Windows skip compatibility test")
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
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -s 'flush database test '")
        os.system("LD_LIBRARY_PATH=/usr/lib  taosBenchmark -f 0-others/com_alltypedata.json -y")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -s 'flush database curdb '")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -s 'select count(*) from curdb.meters '")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -s 'select sum(fc) from curdb.meters '")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -s 'select avg(ic) from curdb.meters '")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -s 'select min(ui) from curdb.meters '")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -s 'select max(bi) from curdb.meters '")

        # os.system(f"LD_LIBRARY_PATH=/usr/lib taos -s 'use test;create stream current_stream into current_stream_output_stb as select _wstart as `start`, _wend as wend, max(current) as max_current from meters where voltage <= 220 interval (5s);' ")
        # os.system('LD_LIBRARY_PATH=/usr/lib taos -s  "use test;create stream power_stream into power_stream_output_stb as select ts, concat_ws(\\".\\", location, tbname) as meter_location, current*voltage*cos(phase) as active_power, current*voltage*sin(phase) as reactive_power from meters partition by tbname;" ')
        # os.system('LD_LIBRARY_PATH=/usr/lib taos -s  "use test;show streams;" ')
        os.system(f"sed -i 's/\/etc\/taos/{cPath}/' 0-others/tmqBasic.json ")
        # os.system("LD_LIBRARY_PATH=/usr/lib  taosBenchmark -f 0-others/tmqBasic.json -y ")
        os.system('LD_LIBRARY_PATH=/usr/lib taos -s  "create topic if not exists tmq_test_topic  as select  current,voltage,phase from test.meters where voltage <= 106 and current <= 5;" ')
        os.system('LD_LIBRARY_PATH=/usr/lib taos -s  "use test;show topics;" ')

        tdLog.info(" LD_LIBRARY_PATH=/usr/lib  taosBenchmark -f 0-others/compa4096.json -y  ")
        os.system("LD_LIBRARY_PATH=/usr/lib  taosBenchmark -f 0-others/compa4096.json -y")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -s 'flush database db4096 '")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -f 0-others/TS-3131.tsql")

        # add deleted  data
        os.system(f'LD_LIBRARY_PATH=/usr/lib taos -s "{self.deletedDataSql}" ')


        cmd = f" LD_LIBRARY_PATH={bPath}/build/lib  {bPath}/build/bin/taos -h localhost ;"
        tdLog.info(f"new  client version  connect to old version taosd, commad return value:{cmd}")
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
        
        # checkout db4096
        tdsql.query("select count(*) from db4096.stb0")
        tdsql.checkData(0,0,50000)
        
        # checkout deleted data
        tdsql.execute("insert into deldata.ct1 values ( now()-0s, 0, 0, 0, 0, 0.0, 0.0, 0, 'binary0', 'nchar0', now()+0a ) ( now()-10s, 1, 11111, 111, 11, 1.11, 11.11, 1, 'binary1', 'nchar1', now()+1a ) ( now()-20s, 2, 22222, 222, 22, 2.22, 22.22, 0, 'binary2', 'nchar2', now()+2a ) ( now()-30s, 3, 33333, 333, 33, 3.33, 33.33, 1, 'binary3', 'nchar3', now()+3a );")
        tdsql.execute("flush database deldata;")
        tdsql.query("select avg(c1) from deldata.ct1;")


        tdsql=tdCom.newTdSql()
        tdLog.printNoPrefix("==========step4:verify backticks in taos Sql-TD18542")
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

        #check retentions
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

        # check stream
        tdsql.query("show streams;")
        tdsql.checkRows(0)

        #check TS-3131
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
            tdLog.exit("The unordered list is not the same as the ordered list.")
        tdsql.execute("insert into test.d80 values (now+1s, 11, 103, 0.21);")
        tdsql.execute("insert into test.d9 values (now+5s, 4.3, 104, 0.4);")


        # check tmq
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
