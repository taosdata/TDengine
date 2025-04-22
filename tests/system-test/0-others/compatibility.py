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

# Define the list of base versions to test
BASE_VERSIONS = ["3.3.0.0","3.3.5.0","3.3.5.0","3.3.6.0"]  # Add more versions as needed

class TDTestCase:
    def caseDescription(self):
        f'''
        TDengine Data Compatibility Test 
        Testing compatibility from the following base versions to current version: {BASE_VERSIONS}
        '''
        return

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        self.deletedDataSql= '''drop database if exists deldata;create database deldata duration 100 stt_trigger 1; ;use deldata;
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
            time.sleep(1)
        else:
            print(f'this processName is not stopped in 60s')

            
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

    # Modified installTaosd to accept version parameter
    def installTaosd(self, bPath, cPath, base_version, package_type="community"):
        packagePath = "/usr/local/src/"
        dataPath = cPath + "/../data/"
        packageType = "server"
        if package_type == "community" :
            packageType = "server"
        elif package_type == "enterprise":
            packageType = "enterprise"
            
        if platform.system() == "Linux" and platform.machine() == "aarch64":
            packageName = "TDengine-"+ packageType + "-" + base_version + "-Linux-arm64.tar.gz"
        else:
            packageName = "TDengine-"+ packageType + "-" + base_version + "-Linux-x64.tar.gz"
            
        # Determine download URL
        major_minor = '.'.join(base_version.split('.')[:2])  # Get the first two segments of version, e.g., "3.2"
        download_url = f"https://www.taosdata.com/assets-download/{major_minor}/{packageName}"
        
        packageTPath = packageName.split("-Linux-")[0]
        my_file = Path(f"{packagePath}/{packageName}")
        if not my_file.exists():
            print(f"{packageName} is not exists")
            tdLog.info(f"cd {packagePath} && wget {download_url}")
            os.system(f"cd {packagePath} && wget {download_url}")
        else: 
            print(f"{packageName} has been exists")
            
        os.system(f" cd {packagePath} && tar xvf {packageName} && cd {packageTPath} && ./install.sh -e no")
        
        os.system(f"pkill -9 taosd")
        print(f"start taosd: rm -rf {dataPath}/* && nohup /usr/bin/taosd -c {cPath} &")
        os.system(f"rm -rf {dataPath}/* && nohup /usr/bin/taosd -c {cPath} &")
        os.system(f"killall taosadapter")
        os.system(f"cp /etc/taos/taosadapter.toml {cPath}/taosadapter.toml")
        taosadapter_cfg = cPath + "/taosadapter.toml"
        taosadapter_log_path = cPath + "/../log/"
        print(f"taosadapter_cfg:{taosadapter_cfg}, taosadapter_log_path:{taosadapter_log_path}")
        self.alter_string_in_file(taosadapter_cfg,"#path = \"/var/log/taos\"",f"path = \"{taosadapter_log_path}\"")
        self.alter_string_in_file(taosadapter_cfg,"taosConfigDir = \"\"",f"taosConfigDir = \"{cPath}\"")
        print("/usr/bin/taosadapter --version")
        os.system(f"/usr/bin/taosadapter --version")
        print(f"LD_LIBRARY_PATH=/usr/lib -c {taosadapter_cfg} 2>&1 &")
        os.system(f"LD_LIBRARY_PATH=/usr/lib /usr/bin/taosadapter -c {taosadapter_cfg} 2>&1 &")
        time.sleep(5)


    def buildTaosd(self,bPath):
        os.system(f"cd {bPath}")

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
        try:
            import distro
            distro_id = distro.id()
            if distro_id == "alpine":
                tdLog.info(f"alpine skip compatibility test")
                return True
        except ImportError:
            tdLog.info("Cannot import distro module, skipping distro check")
            
        if platform.system().lower() == 'windows':
            tdLog.info(f"Windows skip compatibility test")
            return True
            
        bPath = self.getBuildPath()
        cPath = self.getCfgPath()
        
        # Execute test for each base version
        for base_version in BASE_VERSIONS:
            tdLog.printNoPrefix(f"========== Start testing compatibility with base version {base_version} ==========")
            
            try:
                dbname = "test"
                stb = f"{dbname}.meters"
                package_type = "community"  # or "enterprise"
                
                # Install and start old version
                tdLog.info(f"Installing base version: {base_version}")
                self.installTaosd(bPath, cPath, base_version, package_type)
                
                tableNumbers = 100
                recordNumbers1 = 1000
                recordNumbers2 = 1000

                tdLog.printNoPrefix(f"==========step1: Prepare and check data in old version ({base_version})")
                tdLog.info(f"LD_LIBRARY_PATH=/usr/lib taosBenchmark -t {tableNumbers} -n {recordNumbers1} -v 1 -O 5 -y")
                os.system(f"LD_LIBRARY_PATH=/usr/lib taosBenchmark -t {tableNumbers} -n {recordNumbers1} -v 1 -O 5 -y")
                os.system("LD_LIBRARY_PATH=/usr/lib taos -s 'alter database test keep 365000'")
                os.system("LD_LIBRARY_PATH=/usr/lib taos -s 'alter database test cachemodel \"both\"'")
                os.system("LD_LIBRARY_PATH=/usr/lib taos -s 'select last(*) from test.meters'")        
                os.system("LD_LIBRARY_PATH=/usr/lib taos -s 'flush database test'")
                os.system("LD_LIBRARY_PATH=/usr/lib taos -s \"insert into test.d1 values (now+1s, 11, 190, 0.21), (now+2s, 11, 190, 0.21), (now+3s, 11, 190, 0.21), ('2015-07-14 08:39:59.001', 11, 190, 0.21), ('2032-08-14 08:39:59.001 ', 11, 190, 0.21) test.d3 values (now+6s, 11, 190, 0.21), (now+7s, 11, 190, 0.21), (now+8s, 11, 190, 0.21), ('2033-07-14 08:39:59.000', 119, 191, 0.25) test.d3 (ts) values ('2033-07-14 08:39:58.000');\"")
                os.system("LD_LIBRARY_PATH=/usr/lib taos -s 'select last(*) from test.meters'")
                os.system("LD_LIBRARY_PATH=/usr/lib taos -s 'flush database test'")

                os.system("LD_LIBRARY_PATH=/usr/lib taos -s \"insert into test.d1 values (now+11s, 11, 190, 0.21), (now+12s, 11, 190, 0.21), (now+13s, 11, 190, 0.21), (now+14s, 11, 190, 0.21), (now+15s, 11, 190, 0.21) test.d3 values (now+16s, 11, 190, 0.21), (now+17s, 11, 190, 0.21), (now+18s, 11, 190, 0.21), (now+19s, 119, 191, 0.25) test.d3 (ts) values (now+20s);\"")
                os.system("LD_LIBRARY_PATH=/usr/lib taosBenchmark -f 0-others/com_alltypedata.json -y")
                os.system("LD_LIBRARY_PATH=/usr/lib taos -s 'flush database curdb'")
                os.system("LD_LIBRARY_PATH=/usr/lib taos -s 'alter database curdb cachemodel \"both\"'")
                os.system("LD_LIBRARY_PATH=/usr/lib taos -s 'select count(*) from curdb.meters'")
                os.system("LD_LIBRARY_PATH=/usr/lib taos -s 'select last(*) from curdb.meters'")
                os.system("LD_LIBRARY_PATH=/usr/lib taos -s 'select sum(fc) from curdb.meters'")
                os.system("LD_LIBRARY_PATH=/usr/lib taos -s 'select avg(ic) from curdb.meters'")
                os.system("LD_LIBRARY_PATH=/usr/lib taos -s 'select min(ui) from curdb.meters'")
                os.system("LD_LIBRARY_PATH=/usr/lib taos -s 'select max(bi) from curdb.meters'")

                os.system(f"LD_LIBRARY_PATH=/usr/lib taos -s 'use test;create stream current_stream into current_stream_output_stb as select _wstart as `start`, _wend as wend, max(current) as max_current from meters where voltage <= 220 interval (5s);'")
                os.system('LD_LIBRARY_PATH=/usr/lib taos -s "use test;create stream power_stream trigger at_once into power_stream_output_stb as select ts, concat_ws(\\".\\", location, tbname) as meter_location, current*voltage*cos(phase) as active_power, current*voltage*sin(phase) as reactive_power from meters partition by tbname;"')
                os.system('LD_LIBRARY_PATH=/usr/lib taos -s "use test;show streams;"')

                self.alter_string_in_file("0-others/tmqBasic.json", "/etc/taos/", cPath)
                
                # Create topics
                db_topic = "db_test_topic"
                os.system(f'LD_LIBRARY_PATH=/usr/lib taos -s "create topic if not exists {db_topic} with meta as database test"')

                stable_topic = "stable_test_meters_topic"
                os.system(f'LD_LIBRARY_PATH=/usr/lib taos -s "create topic if not exists {stable_topic} as stable test.meters where tbname like \\"d3\\";"')

                select_topic = "select_test_meters_topic"
                topic_select_sql = "select current,voltage,phase from test.meters where voltage >= 10;"
                os.system(f'LD_LIBRARY_PATH=/usr/lib taos -s "create topic if not exists {select_topic} as {topic_select_sql}"')

                os.system('LD_LIBRARY_PATH=/usr/lib taos -s "use test;show topics;"')
                os.system(f"/usr/bin/taosadapter --version")
                
                # Use Consumer to read some messages
                consumer_dict = {
                    "group.id": "g1",
                    "td.connect.websocket.scheme": "ws",
                    "td.connect.user": "root",
                    "td.connect.pass": "taosdata",
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": "false",
                }

                first_consumer_rows = 0
                try:
                    consumer = taosws.Consumer(consumer_dict)
                    consumer.subscribe([select_topic])
                    
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
                except Exception as e:
                    tdLog.info(f"Consumer error: {e}")
                
                # Run other benchmark tests
                tdLog.info("LD_LIBRARY_PATH=/usr/lib taosBenchmark -f 0-others/compa4096.json -y")
                os.system("LD_LIBRARY_PATH=/usr/lib taosBenchmark -f 0-others/compa4096.json -y")
                os.system("LD_LIBRARY_PATH=/usr/lib taosBenchmark -f 0-others/all_insertmode_alltypes.json -y")

                os.system("LD_LIBRARY_PATH=/usr/lib taos -f 0-others/TS-3131.tsql")

                # Add deleted data test
                os.system(f'LD_LIBRARY_PATH=/usr/lib taos -s "{self.deletedDataSql}"')

                # Test current client connecting to old version server
                cmd = f"LD_LIBRARY_PATH={bPath}/build/lib {bPath}/build/bin/taos -h localhost;"
                tdLog.info(f"new client version connect to old version taosd, commad return value:{cmd}")
                if os.system(cmd) == 0:
                    raise Exception("failed to execute system command. cmd: %s" % cmd)
                        
                # Stop old version service
                os.system("pkill -9 taosd")   
                os.system("pkill -9 taos") 
                self.checkProcessPid("taosd")
                os.system("pkill -9 taosadapter")
                self.checkProcessPid("taosadapter")

                # Start new version service
                tdLog.printNoPrefix(f"==========step2: Update to new version (from {base_version})==========")
                self.buildTaosd(bPath)
                tdDnodes.start(1)
                time.sleep(1)
                
                tdsql = tdCom.newTdSql()
                cmd = f"LD_LIBRARY_PATH=/usr/lib taos -h localhost;"
                if os.system(cmd) == 0:
                    raise Exception("failed to execute system command. cmd: %s" % cmd)
                
                # Get current version info
                tdsql.query(f"SELECT SERVER_VERSION();")
                nowServerVersion = tdsql.queryResult[0][0]
                tdLog.info(f"New server version is {nowServerVersion}")
                tdsql.query(f"SELECT CLIENT_VERSION();")
                nowClientVersion = tdsql.queryResult[0][0]
                tdLog.info(f"New client version is {nowClientVersion}")

                # Verify data and functionality
                tdLog.printNoPrefix(f"==========step3: Verify data in new version ({nowServerVersion}) (from {base_version})==========")
                tdsql.query(f"select last(*) from curdb.meters")
                tdLog.info(tdsql.queryResult)
                
                # Verify db_all_insert_mode data
                tables = ["sml_json", "sml_line", "sml_telnet", "rest", "stmt", 
                          "sml_rest_json", "sml_rest_line", "sml_rest_telnet"]
                for table in tables:
                    tdsql.query(f"select * from db_all_insert_mode.{table}")
                    tdsql.checkRows(16)

                # Verify test table data
                tdsql.query(f"select count(*) from {stb}")
                tdsql.checkData(0, 0, tableNumbers*recordNumbers1+20)
                
                # Verify streams
                tdsql.query("show streams;")
                tdsql.checkRows(2)
                
                # Verify db4096
                tdsql.query("select count(*) from db4096.stb0")
                tdsql.checkData(0, 0, 50000)
                
                # Verify deleted data
                tdsql.execute("insert into deldata.ct1 values ( now()-0s, 0, 0, 0, 0, 0.0, 0.0, 0, 'binary0', 'nchar0', now()+0a ) ( now()-10s, 1, 11111, 111, 11, 1.11, 11.11, 1, 'binary1', 'nchar1', now()+1a ) ( now()-20s, 2, 22222, 222, 22, 2.22, 22.22, 0, 'binary2', 'nchar2', now()+2a ) ( now()-30s, 3, 33333, 333, 33, 3.33, 33.33, 1, 'binary3', 'nchar3', now()+3a );")
                tdsql.execute("flush database deldata;")
                tdsql.query("select avg(c1) from deldata.ct1;")

                # Verify backtick
                tdsql = tdCom.newTdSql()
                tdLog.printNoPrefix("==========step4: Verify backtick (TD18542) (from {base_version})==========")
                tdsql.execute("drop database if exists db")
                tdsql.execute("create database db")
                tdsql.execute("use db")
                tdsql.execute("create stable db.stb1 (ts timestamp, c1 int) tags (t1 int);")
                tdsql.execute("insert into db.ct1 using db.stb1 TAGS(1) values(now(),11);")
                tdsql.error("insert into `db.ct2` using db.stb1 TAGS(9) values(now(),11);")
                tdsql.error("insert into db.`db.ct2` using db.stb1 TAGS(9) values(now(),11);")
                tdsql.execute("insert into `db`.ct3 using db.stb1 TAGS(3) values(now(),13);")
                tdsql.query("select * from db.ct3")
                tdsql.checkData(0, 1, 13)
                tdsql.execute("insert into db.`ct4` using db.stb1 TAGS(4) values(now(),14);")
                tdsql.query("select * from db.ct4")
                tdsql.checkData(0, 1, 14)

                # Check retentions
                tdsql = tdCom.newTdSql()
                tdsql.query("describe information_schema.ins_databases;")
                qRows = tdsql.queryRows   
                comFlag = True
                j = 0
                for i in range(qRows):
                    if tdsql.queryResult[i][0] == "retentions":
                        print("parameters include retentions")
                        comFlag = False
                        break
                    else:
                        j = j + 1
                
                if j == qRows:
                    print("parameters don't include retentions")
                    caller = inspect.getframeinfo(inspect.stack()[0][0])
                    args = (caller.filename, caller.lineno)
                    tdLog.exit("%s(%d) failed" % args)

                # Check stream
                tdsql.query("show streams;")
                tdsql.checkRows(2)

                # Check TS-3131
                tdsql.query("select *,tbname from d0.almlog where mcid='m0103';")
                tdsql.checkRows(6)
                expectList = [0, 3003, 20031, 20032, 20033, 30031]
                resultList = []
                for i in range(6):
                    resultList.append(tdsql.queryResult[i][3])
                print(resultList)
                if self.is_list_same_as_ordered_list(resultList, expectList):
                    print("The unordered list is the same as the ordered list.")
                else:
                    tdLog.exit("The unordered list is not the same as the ordered list.")

                # Check test database and last function
                tdsql.query(f"select last(*) from test.meters group by tbname")
                tdLog.info(tdsql.queryResult)
                
                tdsql.query(f"select last_row(*) from test.meters group by tbname")
                tdLog.info(tdsql.queryResult)

                tdsql.query(f"select last_row(*) from test.meters partition by tbname")
                tdLog.info(tdsql.queryResult)
                
                tdsql.query(f"select last(*) from test.meters")
                tdLog.info(tdsql.queryResult)
                tdsql.checkData(0, 0, "2033-07-14 08:39:59.000")
                tdsql.checkData(0, 1, 119) 
                tdsql.checkData(0, 2, 191)
                tdsql.checkData(0, 3, 0.25)
                
                tdsql.query(f"select last_row(*) from test.meters")
                tdLog.info(tdsql.queryResult)
                tdsql.checkData(0, 0, "2033-07-14 08:39:59.000")
                tdsql.checkData(0, 1, 119) 
                tdsql.checkData(0, 2, 191)
                tdsql.checkData(0, 3, 0.25)

                tdsql.query(f"select last(*) from test.d1")
                tdLog.info(tdsql.queryResult)       
                tdsql.checkData(0, 0, "2032-08-14 08:39:59.001")
                tdsql.checkData(0, 1, 11) 
                tdsql.checkData(0, 2, 190)
                tdsql.checkData(0, 3, 0.21)      

                # Update data and check
                tdsql.execute("insert into test.d2 values ('2033-07-14 08:39:59.002', 139, 182, 1.10) (now+2s, 12, 191, 0.22) test.d2 (ts) values ('2033-07-14 08:39:59.003');")
                tdsql.execute("insert into test.d2 values (now+5s, 4.3, 104, 0.4);")

                tdsql.query(f"select last(*) from test.meters")
                tdLog.info(tdsql.queryResult)
                tdsql.checkData(0, 0, "2033-07-14 08:39:59.003")
                tdsql.checkData(0, 1, 139) 
                tdsql.checkData(0, 2, 182)
                tdsql.checkData(0, 3, 1.10)

                # Insert data again and check
                tdsql.execute("insert into test.d1 values (now+1s, 11, 190, 0.21) (now+2s, 12, 191, 0.22) ('2033-07-14 08:40:01.001', 16, 180, 0.53);")

                tdsql.query(f"select last(*) from test.d1")
                tdLog.info(tdsql.queryResult)
                tdsql.checkData(0, 0, "2033-07-14 08:40:01.001")
                tdsql.checkData(0, 1, 16)
                tdsql.checkData(0, 2, 180)
                tdsql.checkData(0, 3, 0.53)
                
                tdsql.query(f"select last(*) from test.meters")
                tdLog.info(tdsql.queryResult)
                tdsql.checkData(0, 0, "2033-07-14 08:40:01.001")
                tdsql.checkData(0, 1, 16)
                tdsql.checkData(0, 2, 180)
                tdsql.checkData(0, 3, 0.53)

                tdsql.query(f"select last_row(*) from test.meters")
                tdLog.info(tdsql.queryResult)
                tdsql.checkData(0, 0, "2033-07-14 08:40:01.001")
                tdsql.checkData(0, 1, 16)
                tdsql.checkData(0, 2, 180)
                tdsql.checkData(0, 3, 0.53)

                # Check configuration modification
                tdsql.execute('alter all dnodes "debugFlag 131"')
                tdsql.execute('alter dnode 1 "debugFlag 143"')
                tdsql.execute('alter local "debugFlag 131"')

                # Check tmq
                conn = taos.connect()

                consumer = Consumer(
                    {
                        "group.id": "g1",
                        "td.connect.user": "root",
                        "td.connect.pass": "taosdata",
                        "enable.auto.commit": "true",
                        "experimental.snapshot.enable": "true",
                    }
                )
                consumer.subscribe([select_topic])
                consumer_rows = 0
                while True:
                    message = consumer.poll(timeout=1.0)
                    tdLog.info(f"null {message}")
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
                if consumer_rows < all_rows - first_consumer_rows:
                    tdLog.exit(f"consumer rows is {consumer_rows}, less than {all_rows - first_consumer_rows}")
                tdsql.query("show topics;")
                tdsql.checkRows(3)
                tdsql.execute(f"drop topic {select_topic};", queryTimes=10)
                tdsql.execute(f"drop topic {db_topic};", queryTimes=10)
                tdsql.execute(f"drop topic {stable_topic};", queryTimes=10)

                # Use new version to add more data
                os.system(f"LD_LIBRARY_PATH={bPath}/build/lib {bPath}/build/bin/taosBenchmark -t {tableNumbers} -n {recordNumbers2} -y")
                tdsql.query(f"select count(*) from {stb}")
                tdsql.checkData(0, 0, tableNumbers*recordNumbers2)
                
                # Clean up for next version test
                tdLog.info(f"Stopping TDengine to prepare for next version test")
                tdDnodes.stop(1)
                os.system("pkill -9 taosd || true")
                os.system("pkill -9 taosadapter || true")
                self.checkProcessPid("taosd")
                self.checkProcessPid("taosadapter")
                time.sleep(2)
                
                tdLog.success(f"Compatibility test with base version {base_version} completed successfully")
                
            except Exception as e:
                tdLog.info(f"Compatibility test with base version {base_version} failed: {e}")
                # Make sure to stop TDengine for next test
                tdDnodes.stop(1)
                os.system("pkill -9 taosd || true")
                os.system("pkill -9 taosadapter || true")
                self.checkProcessPid("taosd")
                self.checkProcessPid("taosadapter")
                time.sleep(2)
                # Uncomment the line below if you want to stop testing after a version fails
                # raise

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
