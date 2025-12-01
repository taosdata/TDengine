###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
import taos
import taosws
import sys
import os
import time
import platform
import subprocess

from pathlib import Path
from .log import *
from .sql import *
from .server.dnodes import *
from .common import *
from taos.tmq import Consumer
from new_test_framework.utils import clusterComCheck


deletedDataSql = '''drop database if exists deldata;create database deldata duration 100 stt_trigger 1; ;use deldata;
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

tableNumbers=100
recordNumbers1=1000
recordNumbers2=1000
first_consumer_rows=0

topic_select_sql = "select current,voltage,phase from test.meters where voltage >= 10;"
select_topic = "select_test_meters_topic"
db_topic = "db_test_topic"
stable_topic = "stable_test_meters_topic"
dbname = "test"
stb = f"{dbname}.meters"

class CompatibilityBase:

    def checkProcessPid(self,processName):
        tdLog.info(f"checkProcessPid {processName}")
        i=0
        while i<60:
            tdLog.info(f"wait stop {processName}")
            processPid = subprocess.getstatusoutput(f'ps aux|grep {processName} |grep -v "grep"|awk \'{{print $2}}\'')[1]
            tdLog.info(f"times:{i},{processName}-pid:{processPid}")
            if(processPid == ""):
                break
            i += 1
            time.sleep(1)
        else:
            tdLog.info(f'this processName is not stopped in 60s')

    def version_compare(self, version1, version2):
        """
        Compare two version strings.
        Returns 1 if version1 > version2, -1 if version1 < version2, 0 if equal
        """
        v1_parts = [int(x) for x in version1.split('.')]
        v2_parts = [int(x) for x in version2.split('.')]
        
        # Pad shorter version with zeros
        max_len = max(len(v1_parts), len(v2_parts))
        v1_parts.extend([0] * (max_len - len(v1_parts)))
        v2_parts.extend([0] * (max_len - len(v2_parts)))
        
        for v1, v2 in zip(v1_parts, v2_parts):
            if v1 > v2:
                return 1
            elif v1 < v2:
                return -1
        return 0

    # Modified installTaosd to accept version parameter
    def installTaosdForRollingUpgrade(self, dnodePaths, base_version):
        packagePath = "/usr/local/src/"
        
        # New download URL format
        if platform.system() == "Linux" and platform.machine() == "aarch64":
            packageName = f"tdengine-tsdb-oss-{base_version}-linux-arm64.tar.gz"
            download_url = f"https://downloads.taosdata.com/tdengine-tsdb-oss/{base_version}/{packageName}"
        else:
            packageName = f"tdengine-tsdb-oss-{base_version}-linux-x64.tar.gz"
            download_url = f"https://downloads.taosdata.com/tdengine-tsdb-oss/{base_version}/{packageName}"
            
        tdLog.info(f"wget {download_url}")
        
        # Extract package name without extension for installation
        packageTPath = packageName.replace("-linux-x64.tar.gz", "")
        my_file = Path(f"{packagePath}/{packageName}")
        if not my_file.exists():
            print(f"{packageName} is not exists")
            tdLog.info(f"cd {packagePath} && wget {download_url}")
            ret_code = os.system(f"cd {packagePath} && wget {download_url}")
            if ret_code != 0:
                return False
            
            # Check if file was actually downloaded
            my_file = Path(f"{packagePath}/{packageName}")
            if not my_file.exists():
                return False
        else: 
            print(f"{packageName} has been exists")
            
        install_ret = os.system(f" cd {packagePath} && tar xvf {packageName} && cd {packageTPath} && ./install.sh -e no")
        if install_ret != 0:
            return False
        
        for dnodePath in dnodePaths:
            tdLog.info(f"start taosd: rm -rf {dnodePath}data/* && nohup /usr/bin/taosd -c {dnodePath}cfg/ &")
            os.system(f"rm -rf {dnodePath}data/* && nohup /usr/bin/taosd -c {dnodePath}cfg/ &")
            os.system(f"killall taosadapter")
            os.system(f"cp /etc/taos/taosadapter.toml {dnodePath}cfg/taosadapter.toml")
            taosadapter_cfg = dnodePath + "cfg/taosadapter.toml"
            taosadapter_log_path = dnodePath + "log/"
            print(f"taosadapter_cfg:{taosadapter_cfg}, taosadapter_log_path:{taosadapter_log_path}")
            self.alter_string_in_file(taosadapter_cfg,"#path = \"/var/log/taos\"",f"path = \"{taosadapter_log_path}\"")
            self.alter_string_in_file(taosadapter_cfg,"taosConfigDir = \"\"",f"taosConfigDir = \"{dnodePath}cfg/\"")
            print("/usr/bin/taosadapter --version")
            os.system(f"/usr/bin/taosadapter --version")
            print(f"LD_LIBRARY_PATH=/usr/lib -c {taosadapter_cfg} 2>&1 &")
            os.system(f"LD_LIBRARY_PATH=/usr/lib /usr/bin/taosadapter -c {taosadapter_cfg} 2>&1 &")
            time.sleep(5)
        
        return True

    def installTaosd(self, bPath, cPath, base_version):
        packagePath = "/usr/local/src/"
        dataPath = cPath + "/../data/"
        packageType = "server"

        # Check if version is 3.3.7.0 or later
        if self.version_compare(base_version, "3.3.7.0") >= 0:
            # Use new download URL format for 3.3.7.0 and later versions
            if platform.system() == "Linux" and platform.machine() == "aarch64":
                packageName = f"tdengine-tsdb-oss-{base_version}-linux-arm64.tar.gz"
                download_url = f"https://downloads.taosdata.com/tdengine-tsdb-oss/{base_version}/{packageName}"
            else:
                packageName = f"tdengine-tsdb-oss-{base_version}-linux-x64.tar.gz"
                download_url = f"https://downloads.taosdata.com/tdengine-tsdb-oss/{base_version}/{packageName}"
            
            # Extract package name without extension for installation
            packageTPath = packageName.replace("-linux-x64.tar.gz", "")
        else:
            # Use old download URL format for versions before 3.3.7.0
            if platform.system() == "Linux" and platform.machine() == "aarch64":
                packageName = "TDengine-"+ packageType + "-" + base_version + "-Linux-arm64.tar.gz"
            else:
                packageName = "TDengine-"+ packageType + "-" + base_version + "-Linux-x64.tar.gz"
                
            # Determine download URL
            download_url = f"https://www.taosdata.com/assets-download/3.0/{packageName}"
            packageTPath = packageName.split("-Linux-")[0]
            
        tdLog.info(f"wget {download_url}")
        
        my_file = Path(f"{packagePath}/{packageName}")
        if not my_file.exists():
            print(f"{packageName} is not exists")
            tdLog.info(f"cd {packagePath} && wget {download_url}")
            os.system(f"cd {packagePath} && wget {download_url}")
        else: 
            print(f"{packageName} has been exists")
            
        os.system(f" cd {packagePath} && tar xvf {packageName} && cd {packageTPath} && ./install.sh -e no")
        
        os.system(f"pkill -9 taosd")
        self.checkProcessPid("taosd")

        print(f"start taosd: rm -rf {dataPath}/* && nohup /usr/bin/taosd -c {cPath} &")
        os.system(f"rm -rf {dataPath}/* && nohup /usr/bin/taosd -c {cPath} &")
        os.system(f"killall taosadapter")
        self.checkProcessPid("taosadapter")
        
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

    def killAllDnodes(self):
        tdLog.info("kill all dnodes")
        tdLog.info("kill taosd")
        os.system(f"pkill -9 taosd")
        tdLog.info("kill taos")
        os.system(f"pkill -9 taos") 
        tdLog.info("check taosd")
        self.checkProcessPid("taosd")
        tdLog.info("kill taosadapter")
        os.system(f"pkill  -9   taosadapter")
        tdLog.info("check taosadapter")
        self.checkProcessPid("taosadapter")

    def prepareDataOnOldVersion(self, base_version, bPath,corss_major_version):
        time.sleep(5)
        global dbname, stb, first_consumer_rows
        tdLog.printNoPrefix(f"==========step1:prepare and check data in old version-{base_version}")
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
        os.system("LD_LIBRARY_PATH=/usr/lib  taosBenchmark -f cases/18-StreamProcessing/30-OldPyCases/json/com_alltypedata.json -y")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -s 'flush database curdb '")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -s 'alter database curdb  cachemodel \"both\" '")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -s 'select count(*) from curdb.meters '")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -s 'select last(*) from curdb.meters '")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -s 'select sum(fc) from curdb.meters '")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -s 'select avg(ic) from curdb.meters '")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -s 'select min(ui) from curdb.meters '")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -s 'select max(bi) from curdb.meters '")

        # Stream processing functionality removed - migrated to new stream computing framework

        # create db/stb/select topic
        os.system(f'LD_LIBRARY_PATH=/usr/lib taos -s  "create topic if not exists {db_topic} with meta as database test" ')

        os.system(f'LD_LIBRARY_PATH=/usr/lib taos -s  "create topic if not exists {stable_topic}  as stable test.meters where tbname like \\"d3\\";" ')

        
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
        
        tdLog.info(" LD_LIBRARY_PATH=/usr/lib  taosBenchmark -f cases/18-StreamProcessing/30-OldPyCases/json/compa4096.json -y  ")
        os.system("LD_LIBRARY_PATH=/usr/lib  taosBenchmark -f cases/18-StreamProcessing/30-OldPyCases/json/compa4096.json -y")
        os.system("LD_LIBRARY_PATH=/usr/lib  taosBenchmark -f cases/18-StreamProcessing/30-OldPyCases/json/all_insertmode_alltypes.json -y")

        # os.system("LD_LIBRARY_PATH=/usr/lib  taos -s 'flush database db4096 '")
        os.system("LD_LIBRARY_PATH=/usr/lib  taos -f cases/18-StreamProcessing/30-OldPyCases/json/TS-3131.tsql")

        # add deleted  data
        os.system(f'LD_LIBRARY_PATH=/usr/lib taos -s "{deletedDataSql}" ')

        if corss_major_version:
            cmd = f" LD_LIBRARY_PATH={bPath}/build/lib  {bPath}/build/bin/taos -h localhost ;"
            tdLog.info(f"new  client version  connect to old version taosd, commad return value:{cmd}")
            if os.system(cmd) == 0:
                raise Exception("failed to execute system command. cmd: %s" % cmd)
        
    def updateNewVersion(self, bPath, cPaths, upgrade):
        tdLog.printNoPrefix("==========step2:update new version ")
        # upgrade only one dnode
        if upgrade == 0:
            tdLog.info("upgrade all dnodes")
            status, output = subprocess.getstatusoutput(f'ps aux|grep taosd |grep -v "grep"|awk \'{{print $2}}\'')
            if status != 0:
                tdLog.error(f"Command to get PIDs failed with status {status}: {output}")
                return 
            found_pids = []
            if output:
                found_pids = [pid for pid in output.strip().split('\n') if pid] 
            tdLog.info(f"Found PIDs: {found_pids} for 'upgrade all dnodes' scenario.")
            if len(found_pids) == 0:
                tdLog.info("No taosd process found keep going")
            else: 
                pid_to_kill_for_this_dnode = found_pids[0]
                tdLog.info(f"Killing taosd process, pid:{pid_to_kill_for_this_dnode} (for cPaths[{0}])")
                os.system(f"kill -9 {pid_to_kill_for_this_dnode}")
                self.checkProcessPid(pid_to_kill_for_this_dnode)

            tdLog.info(f"Starting taosd using cPath: {cPaths[0]}")
            tdLog.info(f"{bPath}/build/bin/taosd -c {cPaths[0]}cfg/ > /dev/null 2>&1 &")
            os.system(f"{bPath}/build/bin/taosd -c {cPaths[0]}cfg/ > /dev/null 2>&1 &")
        # upgrade all dnodes
        elif upgrade == 1:
            tdLog.info("upgrade all dnodes")
            status, output = subprocess.getstatusoutput(f'ps aux|grep taosd |grep -v "grep"|awk \'{{print $2}}\'')
            if status != 0:
                tdLog.error(f"Command to get PIDs failed with status {status}: {output}")
                return 
            found_pids = []
            if output:
                found_pids = [pid for pid in output.strip().split('\n') if pid] 
            tdLog.info(f"Found PIDs: {found_pids} for 'upgrade all dnodes' scenario.")
            # Determine the number of dnodes to manage, based on cPaths or a max like 3 (original implication)
            # Let's use the length of cPaths as the primary guide for how many dnodes to manage.
            num_dnodes_to_manage = len(cPaths) if cPaths else 0
            if num_dnodes_to_manage == 0:
                tdLog.warning("cPaths is empty or not provided. Cannot upgrade all dnodes.")
                return
            for i in range(num_dnodes_to_manage):
                pid_to_kill_for_this_dnode = None
                if i < len(found_pids):
                    pid_to_kill_for_this_dnode = found_pids[i]
                if pid_to_kill_for_this_dnode:
                    tdLog.info(f"Killing taosd process, pid:{pid_to_kill_for_this_dnode} (for cPaths[{i}])")
                    os.system(f"kill -9 {pid_to_kill_for_this_dnode}")
                else:
                    tdLog.info(f"No running taosd PID found to kill for cPaths[{i}] (or fewer PIDs found than cPaths entries).")
                self.checkProcessPid(pid_to_kill_for_this_dnode)
                tdLog.info(f"Starting taosd using cPath: {cPaths[i]}")
                tdLog.info(f"{bPath}/build/bin/taosd -c {cPaths[i]}cfg/ > /dev/null 2>&1 &")
                os.system(f"{bPath}/build/bin/taosd -c {cPaths[i]}cfg/ > /dev/null 2>&1 &")
        # no rolling upgrade
        elif upgrade == 2:
            tdLog.info("no upgrade mode")
            self.buildTaosd(bPath)
            tdLog.info(f"nohup {bPath}/build/bin/taosd -c {cPaths[0]} > /dev/null 2>&1 &")
            os.system(f"nohup {bPath}/build/bin/taosd -c {cPaths[0]} > /dev/null 2>&1 &")
        self.checkstatus()

    def checkTagSizeAndAlterStb(self,tdsql):
        tdsql.query("select * from information_schema.ins_tags where db_name = 'db_all_insert_mode'")
        for i in range(tdsql.queryRows):
            tag_type = tdsql.queryResult[i][4]
            if "NCHAR" not in tag_type:
                continue
            
            tag_size =  int(tag_type.split('(')[1].split(')')[0])
            tag_value = tdsql.queryResult[i][5]
            if len(tag_value) > tag_size:
                new_tag_size = tag_size
                while new_tag_size < len(tag_value):
                    new_tag_size = new_tag_size * 2
                db_name = tdsql.queryResult[i][1]
                stable_name = tdsql.queryResult[i][2]
                tag_name = tdsql.queryResult[i][3]
                if new_tag_size <= tag_size:
                    continue
                tdLog.info(f"ALTER STABLE {db_name}.{stable_name} MODIFY TAG {tag_name} nchar({new_tag_size})")
                tdLog.info(f"current tag_value is {tag_value} and tag value len is {len(tag_value)} and tag_size is {tag_size}")
                tdsql.execute(f"ALTER STABLE {db_name}.{stable_name} MODIFY TAG {tag_name} nchar({new_tag_size})")
                #check tag size
                max_try_times = 100
                try_times = 0
                while try_times < max_try_times:
                    tdLog.info(f"select * from information_schema.ins_tags where db_name = '{db_name}' and stable_name = '{stable_name}' and tag_name = '{tag_name}'")
                    tdsql.query(f"select * from information_schema.ins_tags where db_name = '{db_name}' and stable_name = '{stable_name}' and tag_name = '{tag_name}'")
                    real_tag_type = tdsql.queryResult[0][4]
                    real_tag_size =  int(real_tag_type.split('(')[1].split(')')[0])
                    if real_tag_size == new_tag_size:
                        tdLog.info(f"success to alter tag size from {tag_size} to {new_tag_size}")
                        break
                    time.sleep(0.5)
                    try_times += 1
                self.checkTagSizeAndAlterStb(tdsql)



    def verifyData(self,corss_major_version):
        tdLog.printNoPrefix(f"==========step3:prepare and check data in new version")
        time.sleep(1)
        tdsql=tdCom.newTdSql()
        print(tdsql)
        if corss_major_version:
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

        
        tdsql.query(f"select last(*) from curdb.meters")
        tdLog.info(tdsql.queryResult)

        # deal table schema is too old issue
        self.checkTagSizeAndAlterStb(tdsql)

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
        # Stream verification removed - migrated to new stream computing framework
        


        # checkout db4096
        tdsql.query("select count(*) from db4096.stb0")
        tdsql.checkData(0,0,50000)
        
        # checkout deleted data
        tdsql.execute("insert into deldata.ct1 values ( now()-0s, 0, 0, 0, 0, 0.0, 0.0, 0, 'binary0', 'nchar0', now()+0a ) ( now()-10s, 1, 11111, 111, 11, 1.11, 11.11, 1, 'binary1', 'nchar1', now()+1a ) ( now()-20s, 2, 22222, 222, 22, 2.22, 22.22, 0, 'binary2', 'nchar2', now()+2a ) ( now()-30s, 3, 33333, 333, 33, 3.33, 33.33, 1, 'binary3', 'nchar3', now()+3a );")
        tdsql.execute("flush database deldata;")
        tdsql.query("select avg(c1) from deldata.ct1;")

    def verifyBackticksInTaosSql(self,bPath):
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
                import inspect
                caller = inspect.getframeinfo(inspect.stack()[0][0])
                args = (caller.filename, caller.lineno)
                tdLog.exit("%s(%d) failed" % args)

        # Stream checking removed - migrated to new stream computing framework

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

        # check alter config
        tdsql.execute('alter all dnodes "debugFlag 131"')
        tdsql.execute('alter dnode 1 "debugFlag 143"')
        tdsql.execute('alter local "debugFlag 131"')

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

    def checkstatus(self,retry_times=30):
        
        # sleep before check status to avoid dnodes not ready issue
        time.sleep(10)
        tdsql=tdCom.newTdSql()
        dnodes_ready = False
        for i in range(retry_times):
            tdsql.query("show dnodes;")
            dnode_nums = tdsql.queryRows
            ready_nums = 0

            for j in range(tdsql.queryRows):
                if tdsql.queryResult[j][4] == "ready":
                    ready_nums += 1
            if ready_nums == dnode_nums:
                dnodes_ready = True
                break

            time.sleep(1)

        if not dnodes_ready:
            tdLog.exit(f"dnodes are not ready in {retry_times}s")
        tdLog.info(f"dnodes are ready in {retry_times}s")

        modes_ready = False
        for i in range(retry_times):
            tdsql.query("show mnodes;")
            mnode_nums = tdsql.queryRows
            ready_nums = 0
            for j in range(tdsql.queryRows):
                if tdsql.queryResult[j][3] == "ready":
                    ready_nums += 1
            if ready_nums == mnode_nums:
                modes_ready = True
                break
            time.sleep(1)

        if not modes_ready:
            tdLog.exit(f"mnodes are not ready in {retry_times}s")
        tdLog.info(f"mnodes are ready in {retry_times}s")

    
        vnodes_ready = False
        for i in range(retry_times):
            tdsql.query("show vnodes;")
            vnode_nums = tdsql.queryRows
            ready_nums = 0
            for j in range(tdsql.queryRows):
                if str(tdsql.queryResult[j][6]).lower() == "true":
                    ready_nums += 1
            if ready_nums == vnode_nums:
                vnodes_ready = True
                break
            time.sleep(1)
    
        if not vnodes_ready:
            tdLog.exit(f"vnodes are not ready in {retry_times}s") 
        tdLog.info(f"vnodes are ready in {retry_times}s")

# Create instance for compatibility
tdCb = CompatibilityBase() 