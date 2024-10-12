import taos
import sys
import time
import socket
import os
import threading

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *
from taos.tmq import *
sys.path.append("./7-tmq")
from tmqCommon import *

class TDTestCase:
    updatecfgDict = {'debugFlag': 135, 'asynclog': 0}
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        #tdSql.init(conn.cursor(), logSql)  # output sql.txt file

    def run(self):
        tdSql.execute(f'create database if not exists db_5466')
        tdSql.execute(f'use db_5466')
        tdSql.execute(f'CREATE STABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)')
        tdSql.execute("INSERT INTO d1001 USING meters TAGS('California.SanFrancisco', 2) VALUES('2018-10-05 14:38:05.000',10.30000,219,0.31000)")

        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_td32526'%(buildPath)
        # tdLog.info(cmdStr)
        # os.system(cmdStr)
        #
        # tdSql.execute("drop topic db_5466_topic")
        tdSql.execute(f'alter stable meters add column  item_tags nchar(500)')
        tdSql.execute(f'alter stable meters add column  new_col nchar(100)')
        tdSql.execute("create topic db_5466_topic as select * from db_5466.meters")

        tdSql.execute("INSERT INTO d1001 USING meters TAGS('California.SanFrancisco', 2) VALUES('2018-10-06 14:38:05.000',10.30000,219,0.31000, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', '1')")

        tdLog.info(cmdStr)
        os.system(cmdStr)

        return

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())