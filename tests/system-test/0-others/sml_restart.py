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

class TDTestCase:
    updatecfgDict = {'debugFlag': 135, 'asynclog': 0}
    clientCfgDict = {'debugFlag': 135, 'asynclog': 0}
    updatecfgDict["clientCfg"] = clientCfgDict
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def run(self):

        os.system("LD_LIBRARY_PATH=/usr/lib  taosBenchmark -f 0-others/part_insertmode.json -y")
        os.system("pkill  -9  taosd")   # make sure all the data are saved in disk.
        os.system("pkill  -9  taos") 

        tdDnodes.start(1)
        sleep(1)

        tdsql=tdCom.newTdSql()
        tdsql.query(f"SELECT SERVER_VERSION();")
        nowServerVersion=tdsql.queryResult[0][0]
        tdLog.info(f"New server version is {nowServerVersion}")
        tdsql.query(f"SELECT CLIENT_VERSION();")
        nowClientVersion=tdsql.queryResult[0][0]
        tdLog.info(f"New client version is {nowClientVersion}")

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
                tdLog.info(f"ALTER STABLE {db_name}.{stable_name} MODIFY TAG {tag_name} nchar({new_tag_size})")
                tdsql.execute(f"ALTER STABLE {db_name}.{stable_name} MODIFY TAG {tag_name} nchar({new_tag_size})")

        # check data
        tdsql.query(f"select * from db_all_insert_mode.sml_json")    
        tdsql.checkRows(16)    
        tdsql.query(f"select * from db_all_insert_mode.sml_line")     
        tdsql.checkRows(16)   
        tdsql.query(f"select * from db_all_insert_mode.sml_telnet")  
        tdsql.checkRows(16)    
        tdsql.query(f"select * from db_all_insert_mode.stmt")  
        tdsql.checkRows(16)  



    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
