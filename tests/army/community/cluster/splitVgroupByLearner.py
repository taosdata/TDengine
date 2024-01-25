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

import sys
import time
import random

import taos
import frame
import frame.etool
import json
import threading

from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *
from frame.autogen import *
from frame.srvCtl import *

class TDTestCase(TBase):
    def configJsonFile(self, fileName, dbName, vgroups, replica, newFileName='', insert_rows=10000000, timestamp_step=10000):
        with open(fileName, 'r') as f:
            data = json.load(f)
        if len(newFileName) == 0:
            newFileName = fileName

        data['databases']['dbinfo']['name'] = dbName
        data['databases']['dbinfo']['vgroups'] = vgroups
        data['databases']['dbinfo']['replica'] = replica
        data['databases']['dbinfo']['replica'] = replica
        data['databases']['super_tables']['insert_rows'] = insert_rows
        data['databases']['super_tables']['timestamp_step'] = timestamp_step
        json_data = json.dumps(data)
        with open(newFileName, "w") as file:
            file.write(json_data)

    def splitVgroupThread(self, configFile, event):
        # self.insertData(configFile)
        event.wait()
        tdSql.execute('ALTER DATABASE db1 REPLICA 3')
        time.sleep(5)
        param_list = tdSql.query('show vgroups')
        vgroupId = None
        for param in param_list:
           vgroupId = param[0]
        tdSql.execute(f"split vgroup {vgroupId}")
        # self.configJsonFile(configFile, 'db1', 1, 1, configFile, 100000000)
        # self.insertData(configFile)

    def dnodeNodeStopThread(self, event):
        event.wait()
        time.sleep(10)
        on = 2
        for i in range(5):
            if i % 2 == 0:
                on = 2
            else:
                on = 3
            sc.dnodeStop(on)
            time.sleep(5)
            sc.dnodeStart(on)
            time.sleep(5)


    def dbInsertThread(self, configFile, event):
        self.insertData(configFile)
        event.set()
        self.configJsonFile(configFile, 'db', 2, 3, configFile, 100000000)
        self.insertData(configFile)

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), logSql)  # output sql.txt file
        self.configJsonFile('splitVgroupByLearner.json', 'db', 1, 1, 'splitVgroupByLearner.json', 1000000)

    def insertData(self, configFile):
        tdLog.info(f"insert data.")
        # taosBenchmark run
        jfile = etool.curFile(__file__, configFile)
        etool.benchMark(json = jfile)

    # run
    def run(self):
        tdLog.debug(f"start to excute {__file__}")
        event = threading.Event
        t1 = threading.Thread(target=self.splitVgroupThread, args=('splitVgroupByLearner1.json', event))
        t2 = threading.Thread(target=self.dbInsertThread, args=('splitVgroupByLearner.json'))
        t3 = threading.Thread(target=self.dnodeNodeStopThread, args=(event))
        t1.join()
        t2.join()
        t3.join()
        tdLog.success(f"{__file__} successfully executed")

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())