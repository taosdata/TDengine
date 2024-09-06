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
    updatecfgDict = {
        'slowLogScope' : "others"
    }

    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug(f"start to init {__file__}")
        self.replicaVar = int(replicaVar)
        tdSql.init(conn.cursor(), logSql)  # output sql.txt file
        self.configJsonFile('splitVgroupByLearner.json', 'db', 1, 1, 'splitVgroupByLearner.json', 100000)

    def configJsonFile(self, fileName, dbName, vgroups, replica, newFileName='', insert_rows=100000,
                       timestamp_step=10000):
        tdLog.debug(f"configJsonFile {fileName}")
        filePath = etool.curFile(__file__, fileName)
        with open(filePath, 'r') as f:
            data = json.load(f)

        if len(newFileName) == 0:
            newFileName = fileName

        data['databases'][0]['dbinfo']['name'] = dbName
        data['databases'][0]['dbinfo']['vgroups'] = vgroups
        data['databases'][0]['dbinfo']['replica'] = replica
        data['databases'][0]['super_tables'][0]['insert_rows'] = insert_rows
        data['databases'][0]['super_tables'][0]['timestamp_step'] = timestamp_step
        json_data = json.dumps(data)
        filePath = etool.curFile(__file__, newFileName)
        with open(filePath, "w") as file:
            file.write(json_data)

        tdLog.debug(f"configJsonFile {json_data}")

    def splitVgroupThread(self, configFile, event):
        # self.insertData(configFile)
        event.wait()
        time.sleep(5)
        tdLog.debug("splitVgroupThread start")
        tdSql.execute('ALTER DATABASE db REPLICA 3')
        time.sleep(5)
        tdSql.execute('use db')
        rowLen = tdSql.query('show vgroups')
        if rowLen > 0:
            vgroupId = tdSql.getData(0, 0)
            tdLog.debug(f"splitVgroupThread vgroupId:{vgroupId}")
            tdSql.execute(f"split vgroup {vgroupId}")
        else:
            tdLog.exit("get vgroupId fail!")
            # self.configJsonFile(configFile, 'db1', 1, 1, configFile, 100000000)
        # self.insertData(configFile)

    def dnodeNodeStopThread(self, event):
        event.wait()
        tdLog.debug("dnodeNodeStopThread start")
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
        tdLog.debug(f"dbInsertThread start {configFile}")
        self.insertData(configFile)

        event.set()
        tdLog.debug(f"dbInsertThread first end {event}")
        self.configJsonFile(configFile, 'db', 2, 3, configFile, 100000)
        self.insertData(configFile)

    def insertData(self, configFile):
        tdLog.info(f"insert data.")
        # taosBenchmark run
        jfile = etool.curFile(__file__, configFile)
        etool.benchMark(json=jfile)

    # run
    def run(self):
        tdLog.debug(f"start to excute {__file__}")
        event = threading.Event()
        t1 = threading.Thread(target=self.splitVgroupThread, args=('splitVgroupByLearner.json', event))
        t2 = threading.Thread(target=self.dbInsertThread, args=('splitVgroupByLearner.json', event))
        t3 = threading.Thread(target=self.dnodeNodeStopThread, args=(event))
        t1.start()
        t2.start()
        t3.start()
        tdLog.debug("threading started!!!!!")
        t1.join()
        t2.join()
        t3.join()
        tdLog.success(f"{__file__} successfully executed")

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())