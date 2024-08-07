from urllib.parse import uses_relative
import taos
import taosws
import sys
import os
import time
import platform
import inspect
from taos.tmq import Consumer
from taos.tmq import *

from pathlib import Path
from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.dnodes import TDDnodes
from util.dnodes import TDDnode
from util.cluster import *
import subprocess

BASEVERSION = "3.2.0.0"
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
        self.deletedDataSql= '''drop database if exists deldata;create database deldata duration 300 stt_trigger 1; ;use deldata;
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

    def installTaosd(self, bPath, cPath, package_type="community"):
        # os.system(f"rmtaos && mkdir -p {self.getBuildPath()}/build/lib/temp &&  mv {self.getBuildPath()}/build/lib/libtaos.so*  {self.getBuildPath()}/build/lib/temp/ ")
        # os.system(f" mv {bPath}/build  {bPath}/build_bak ")
        # os.system(f"mv {self.getBuildPath()}/build/lib/libtaos.so  {self.getBuildPath()}/build/lib/libtaos.so_bak ")
        # os.system(f"mv {self.getBuildPath()}/build/lib/libtaos.so.1  {self.getBuildPath()}/build/lib/libtaos.so.1_bak ")
        
        packagePath = "/usr/local/src/"
        dataPath = cPath + "/../data/"
        packageType = "server"
        if package_type == "community" :
            packageType = "server"
        elif package_type == "enterprise":
            packageType = "enterprise"
        if platform.system() == "Linux" and platform.machine() == "aarch64":
            packageName = "TDengine-"+ packageType + "-" + BASEVERSION + "-Linux-arm64.tar.gz"
        else:
            packageName = "TDengine-"+ packageType + "-" + BASEVERSION + "-Linux-x64.tar.gz"
        packageTPath = packageName.split("-Linux-")[0]
        my_file = Path(f"{packagePath}/{packageName}")
        if not  my_file.exists():
            print(f"{packageName} is not exists")
            tdLog.info(f"cd {packagePath} &&  wget https://www.tdengine.com/assets-download/3.0/{packageName}")
            os.system(f"cd {packagePath} &&  wget https://www.tdengine.com/assets-download/3.0/{packageName}")
        else: 
            print(f"{packageName} has been exists")
        os.system(f" cd {packagePath} &&  tar xvf  {packageName} && cd {packageTPath} &&  ./install.sh  -e no  " )
        
        os.system(f"pkill -9 taosd" )
        print(f"start taosd: rm -rf {dataPath}/*  && nohup /usr/bin/taosd -c {cPath} & ")
        os.system(f"rm -rf {dataPath}/*  && nohup  /usr/bin/taosd -c {cPath} & " )
        os.system(f"killall taosadapter" )
        os.system(f"cp /etc/taos/taosadapter.toml {cPath}/taosadapter.toml  " )
        taosadapter_cfg = cPath + "/taosadapter.toml"
        taosadapter_log_path = cPath + "/../log/"
        print(f"taosadapter_cfg:{taosadapter_cfg},taosadapter_log_path:{taosadapter_log_path} ")
        self.alter_string_in_file(taosadapter_cfg,"#path = \"/var/log/taos\"",f"path = \"{taosadapter_log_path}\"")
        self.alter_string_in_file(taosadapter_cfg,"taosConfigDir = \"\"",f"taosConfigDir = \"{cPath}\"")
        print("/usr/bin/taosadapter --version")
        os.system(f"  /usr/bin/taosadapter --version" )
        print(f" LD_LIBRARY_PATH=/usr/lib -c  {taosadapter_cfg} 2>&1 & ")
        os.system(f" LD_LIBRARY_PATH=/usr/lib  /usr/bin/taosadapter -c  {taosadapter_cfg} 2>&1 & " )
        sleep(5)


    def buildTaosd(self,bPath):
        # os.system(f"mv {bPath}/build_bak  {bPath}/build ")
        os.system(f" cd {bPath} ")

    def is_list_same_as_ordered_list(self,unordered_list, ordered_list):
        sorted_list = sorted(unordered_list)
        return sorted_list == ordered_list

    def alter_string_in_file(self,file,old_str,new_str):
        """
        replace str in file
        :param file
        :param old_str
        :param new_str
        :return:
        """
        file_data = ""
        with open(file, "r", encoding="utf-8") as f:
            for line in f:
                if old_str in line:
                    line = line.replace(old_str,new_str)
                file_data += line
        with open(file,"w",encoding="utf-8") as f:
            f.write(file_data)

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
        # package_type = "enterprise"
        package_type = "community"
        self.installTaosd(bPath,cPath,package_type)
        # os.system(f"echo 'debugFlag 143' >> {cPath}/taos.cfg ")
        tableNumbers=100
        recordNumbers1=1000
        recordNumbers2=1000

        tdLog.printNoPrefix(f"==========step1:prepare and check data in old version-{BASEVERSION}")
        tdLog.info(f" LD_LIBRARY_PATH=/usr/lib  taosBenchmark -t {tableNumbers} -n {recordNumbers1} -v 1 -O 5  -y ")
        os.system(f"LD_LIBRARY_PATH=/usr/lib taosBenchmark -t {tableNumbers} -n {recordNumbers1} -v 1 -O 5  -y  ")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -s 'alter database test   keep 365000 '")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -s 'alter database test  cachemodel \"both\" '")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -s 'select last(*) from test.meters '")        
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -s 'flush database test '")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -s \"insert into test.d1 values (now+1s, 11, 190, 0.21), (now+2s, 11, 190, 0.21), (now+3s, 11, 190, 0.21), ('2015-07-14 08:39:59.001', 11, 190, 0.21), ('2032-08-14 08:39:59.001 ', 11, 190, 0.21) test.d3  values  (now+6s, 11, 190, 0.21), (now+7s, 11, 190, 0.21), (now+8s, 11, 190, 0.21), ('2033-07-14 08:39:59.000', 119, 191, 0.25) test.d3  (ts) values ('2033-07-14 08:39:58.000');\"")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -s 'select last(*) from test.meters '")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -s 'flush database test '")

        os.system("LD_LIBRARY_PATH=/usr/lib  taos -s \"insert into test.d1 values (now+11s, 11, 190, 0.21), (now+12s, 11, 190, 0.21), (now+13s, 11, 190, 0.21), (now+14s, 11, 190, 0.21), (now+15s, 11, 190, 0.21) test.d3  values  (now+16s, 11, 190, 0.21), (now+17s, 11, 190, 0.21), (now+18s, 11, 190, 0.21), (now+19s, 119, 191, 0.25) test.d3  (ts) values (now+20s);\"")
        os.system("LD_LIBRARY_PATH=/usr/lib  taosBenchmark -f 0-others/com_alltypedata.json -y")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -s 'flush database curdb '")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -s 'alter database curdb  cachemodel \"both\" '")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -s 'select count(*) from curdb.meters '")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -s 'select last(*) from curdb.meters '")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -s 'select sum(fc) from curdb.meters '")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -s 'select avg(ic) from curdb.meters '")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -s 'select min(ui) from curdb.meters '")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -s 'select max(bi) from curdb.meters '")

        os.system(f"LD_LIBRARY_PATH=/usr/lib taos -s 'use test;create stream current_stream into current_stream_output_stb as select _wstart as `start`, _wend as wend, max(current) as max_current from meters where voltage <= 220 interval (5s);' ")
        os.system('LD_LIBRARY_PATH=/usr/lib taos -s  "use test;create stream power_stream trigger at_once  into power_stream_output_stb as select ts, concat_ws(\\".\\", location, tbname) as meter_location, current*voltage*cos(phase) as active_power, current*voltage*sin(phase) as reactive_power from meters partition by tbname;" ')
        os.system('LD_LIBRARY_PATH=/usr/lib taos -s  "use test;show streams;" ')

        self.alter_string_in_file("0-others/tmqBasic.json", "/etc/taos/", cPath)
        # os.system("LD_LIBRARY_PATH=/usr/lib  taosBenchmark -f 0-others/tmqBasic.json -y ")
        # create db/stb/select topic

        db_topic = "db_test_topic"
        os.system(f'LD_LIBRARY_PATH=/usr/lib taos -s  "create topic if not exists {db_topic} with meta as database test" ')

        stable_topic = "stable_test_meters_topic"
        os.system(f'LD_LIBRARY_PATH=/usr/lib taos -s  "create topic if not exists {stable_topic}  as stable test.meters where tbname like \\"d3\\";" ')

        select_topic = "select_test_meters_topic"
        topic_select_sql = "select current,voltage,phase from test.meters where voltage >= 10;"
        os.system(f'LD_LIBRARY_PATH=/usr/lib taos -s  "create topic if not exists {select_topic}  as {topic_select_sql}" ')

        os.system('LD_LIBRARY_PATH=/usr/lib taos -s  "use test;show topics;" ')
        os.system(f"  /usr/bin/taosadapter --version " )        
        consumer_dict = {
            "group.id": "g1",
            "td.connect.websocket.scheme": "ws",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false",
        }

        consumer = taosws.Consumer(consumer_dict)
        try:
            consumer.subscribe([select_topic])
        except TmqError:
            tdLog.exit(f"subscribe error")
        first_consumer_rows = 0
        while True:
            message = consumer.poll(timeout=1.0)
            if message:
                for block in message:
                    first_consumer_rows += block.nrows()
            else:
                tdLog.notice("message is null and break")
                break
            consumer.commit(message)
            tdLog.debug(f"topic:{select_topic} ,first consumer rows is {first_consumer_rows} in old version")
            break

        consumer.close()
        
        tdLog.info(" LD_LIBRARY_PATH=/usr/lib  taosBenchmark -f 0-others/compa4096.json -y  ")
        os.system("LD_LIBRARY_PATH=/usr/lib  taosBenchmark -f 0-others/compa4096.json -y")
        os.system("LD_LIBRARY_PATH=/usr/lib  taosBenchmark -f 0-others/all_insertmode_alltypes.json -y")

        # os.system("LD_LIBRARY_PATH=/usr/lib  taos -s 'flush database db4096 '")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -f 0-others/TS-3131.tsql")

        # add deleted  data
        os.system(f'LD_LIBRARY_PATH=/usr/lib taos -s "{self.deletedDataSql}" ')


        cmd = f" LD_LIBRARY_PATH={bPath}/build/lib  {bPath}/build/bin/taos -h localhost ;"
        tdLog.info(f"new  client version  connect to old version taosd, commad return value:{cmd}")
        if os.system(cmd) == 0:
            raise Exception("failed to execute system command. cmd: %s" % cmd)
                
        os.system("pkill  -9  taosd")   # make sure all the data are saved in disk.
        os.system("pkill  -9  taos") 
        self.checkProcessPid("taosd")
        os.system("pkill  -9   taosadapter")   # make sure all the data are saved in disk.
        self.checkProcessPid("taosadapter")

        tdLog.printNoPrefix("==========step2:update new version ")
        self.buildTaosd(bPath)
        tdDnodes.start(1)
        sleep(1)
        tdsql=tdCom.newTdSql()
        print(tdsql)
        cmd = f" LD_LIBRARY_PATH=/usr/lib  taos -h localhost ;"
        print(os.system(cmd))
        if os.system(cmd) == 0:
            raise Exception("failed to execute system command. cmd: %s" % cmd)
        
        tdsql.query(f"SELECT SERVER_VERSION();")
        nowServerVersion=tdsql.queryResult[0][0]
        tdLog.info(f"New server version is {nowServerVersion}")
        tdsql.query(f"SELECT CLIENT_VERSION();")
        nowClientVersion=tdsql.queryResult[0][0]
        tdLog.info(f"New client version is {nowClientVersion}")

        tdLog.printNoPrefix(f"==========step3:prepare and check data in new version-{nowServerVersion}")
        tdsql.query(f"select last(*) from curdb.meters")
        tdLog.info(tdsql.queryResult)
        tdsql.query(f"select * from db_all_insert_mode.sml_json")    
        tdsql.checkRows(16)
    
        tdsql.query(f"select * from db_all_insert_mode.sml_line")     
        tdsql.checkRows(16)   
        tdsql.query(f"select * from db_all_insert_mode.sml_telnet")  
        tdsql.checkRows(16)    
        tdsql.query(f"select * from db_all_insert_mode.rest")    
        tdsql.checkRows(16)    
        tdsql.query(f"select * from db_all_insert_mode.stmt")  
        tdsql.checkRows(16)  
        tdsql.query(f"select * from db_all_insert_mode.sml_rest_json")    
        tdsql.checkRows(16)  
        tdsql.query(f"select * from db_all_insert_mode.sml_rest_line")    
        tdsql.checkRows(16)      
        tdsql.query(f"select * from db_all_insert_mode.sml_rest_telnet")    
        tdsql.checkRows(16)  

        tdsql.query(f"select count(*) from {stb}")
        tdsql.checkData(0,0,tableNumbers*recordNumbers1+20)
        tdsql.query("show streams;")
        tdsql.checkRows(2)
        


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
        tdsql.checkRows(2)

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


        # check database test and last
        # first check
        
        tdsql.query(f"select last(*) from test.meters group by tbname")
        tdLog.info(tdsql.queryResult)
        # tdsql.checkRows(tableNumbers)
        
        tdsql.query(f"select last_row(*) from test.meters group by tbname")
        tdLog.info(tdsql.queryResult)
        # tdsql.checkRows(tableNumbers)

        tdsql.query(f"select last_row(*) from test.meters partition by tbname")
        tdLog.info(tdsql.queryResult)
        # tdsql.checkRows(tableNumbers)
        
        tdsql.query(f"select last(*) from test.meters")
        tdLog.info(tdsql.queryResult)
        tdsql.checkData(0,0,"2033-07-14 08:39:59.000")
        tdsql.checkData(0,1,119) 
        tdsql.checkData(0,2,191)
        tdsql.checkData(0,3,0.25)
        
        tdsql.query(f"select last_row(*) from test.meters")
        tdLog.info(tdsql.queryResult)
        tdsql.checkData(0,0,"2033-07-14 08:39:59.000")
        tdsql.checkData(0,1,119) 
        tdsql.checkData(0,2,191)
        tdsql.checkData(0,3,0.25)

        tdsql.query(f"select last(*) from test.d1")
        tdLog.info(tdsql.queryResult)       
        tdsql.checkData(0,0,"2032-08-14 08:39:59.001")
        tdsql.checkData(0,1,11) 
        tdsql.checkData(0,2,190)
        tdsql.checkData(0,3,0.21)      

        # update data and check
        tdsql.execute("insert into test.d2 values ('2033-07-14 08:39:59.002', 139, 182, 1.10) (now+2s, 12, 191, 0.22) test.d2  (ts) values ('2033-07-14 08:39:59.003');")
        tdsql.execute("insert into test.d2 values (now+5s, 4.3, 104, 0.4);")

        tdsql.query(f"select last(*) from test.meters")
        tdLog.info(tdsql.queryResult)
        tdsql.checkData(0,0,"2033-07-14 08:39:59.003")
        tdsql.checkData(0,1,139) 
        tdsql.checkData(0,2,182)
        tdsql.checkData(0,3,1.10)

        # repeately insert data and check
        tdsql.execute("insert into test.d1 values (now+1s, 11, 190, 0.21) (now+2s, 12, 191, 0.22) ('2033-07-14 08:40:01.001', 16, 180, 0.53);")

        tdsql.query(f"select last(*) from test.d1")
        tdLog.info(tdsql.queryResult)
        tdsql.checkData(0,0,"2033-07-14 08:40:01.001")
        tdsql.checkData(0,1,16)
        tdsql.checkData(0,2,180)
        tdsql.checkData(0,3,0.53)
        
        tdsql.query(f"select last(*) from test.meters")
        tdLog.info(tdsql.queryResult)
        tdsql.checkData(0,0,"2033-07-14 08:40:01.001")
        tdsql.checkData(0,1,16)
        tdsql.checkData(0,2,180)
        tdsql.checkData(0,3,0.53)

        tdsql.query(f"select last_row(*) from test.meters")
        tdLog.info(tdsql.queryResult)
        tdsql.checkData(0,0,"2033-07-14 08:40:01.001")
        tdsql.checkData(0,1,16)
        tdsql.checkData(0,2,180)
        tdsql.checkData(0,3,0.53)

        # check tmq
        conn = taos.connect()

        consumer = Consumer(
            {
                "group.id": "g1",
                "td.connect.user": "root",
                "td.connect.pass": "taosdata",
                "enable.auto.commit": "true",
                "experimental.snapshot.enable":  "true",
            }
        )
        consumer.subscribe([select_topic])
        consumer_rows = 0
        while True:
            message = consumer.poll(timeout=1.0)
            tdLog.info(f" null {message}")
            if message:
                for block in message:
                    consumer_rows += block.nrows()
                tdLog.info(f"consumer rows is {consumer_rows}")
            else:
                print("consumer has completed and break")
                break
        consumer.close()
        tdsql.query(f"{topic_select_sql}")
        all_rows = tdsql.queryRows
        if consumer_rows < all_rows - first_consumer_rows :
            tdLog.exit(f"consumer rows is {consumer_rows}, less than {all_rows - first_consumer_rows}")
        tdsql.query("show topics;")
        tdsql.checkRows(3)
        tdsql.execute(f"drop topic {select_topic};",queryTimes=10)
        tdsql.execute(f"drop topic {db_topic};",queryTimes=10)
        tdsql.execute(f"drop topic {stable_topic};",queryTimes=10)

        os.system(f" LD_LIBRARY_PATH={bPath}/build/lib  {bPath}/build/bin/taosBenchmark -t {tableNumbers} -n {recordNumbers2} -y  ")
        tdsql.query(f"select count(*) from {stb}")
        tdsql.checkData(0,0,tableNumbers*recordNumbers2)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
