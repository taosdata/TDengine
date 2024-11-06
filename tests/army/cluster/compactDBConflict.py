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
import frame.etool
from frame.caseBase import *
from frame.cases import *
from frame import *
import json
import threading


class TDTestCase(TBase):
  def init(self, conn, logSql, replicaVar=1):
        tdLog.debug(f"start to init {__file__}")
        self.replicaVar = int(replicaVar)
        tdSql.init(conn.cursor(), logSql)  # output sql.txt file
        self.configJsonFile('splitVgroupByLearner.json', 'db', 4, 1, 'splitVgroupByLearner.json', 100000)

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

  def insertData(self, configFile):
    tdLog.info(f"insert data.")
    # taosBenchmark run
    jfile = etool.curFile(__file__, configFile)
    etool.benchMark(json=jfile)

  def run(self):
        tdLog.debug(f"start to excute {__file__}")

        self.insertData('splitVgroupByLearner.json')

        tdSql.execute('use db')

        t0 = threading.Thread(target=self.compactDBThread)
        t0.start()
        tdLog.debug("threading started!!!!!")
        tdSql.error('ALTER DATABASE db REPLICA 3;', expectErrInfo="Transaction not completed due to conflict with compact")
        t0.join()
        
        t1 = threading.Thread(target=self.compactDBThread)
        t1.start()
        tdLog.debug("threading started!!!!!")
        time.sleep(1)
        tdSql.error('REDISTRIBUTE VGROUP 5 DNODE 1;', expectErrInfo="Transaction not completed due to conflict with compact")
        t1.join()

        t2 = threading.Thread(target=self.compactDBThread)
        t2.start()
        tdLog.debug("threading started!!!!!")
        rowLen = tdSql.query('show vgroups')
        if rowLen > 0:
            vgroupId = tdSql.getData(0, 0)
            tdLog.debug(f"splitVgroupThread vgroupId:{vgroupId}")
            tdSql.error('REDISTRIBUTE VGROUP 5 DNODE 1;', expectErrInfo="Transaction not completed due to conflict with compact")
        t2.join()

        t3 = threading.Thread(target=self.compactDBThread)
        t3.start()
        tdLog.debug("threading started!!!!!")
        time.sleep(1)
        tdSql.error('BALANCE VGROUP;', expectErrInfo="Transaction not completed due to conflict with compact")
        t3.join()

        t4 = threading.Thread(target=self.splitVgroupThread)
        t4.start()
        tdLog.debug("threading started!!!!!")
        time.sleep(1)
        tdSql.error('compact database db;', expectErrInfo="Conflict transaction not completed")
        t4.join()
        
        t5 = threading.Thread(target=self.RedistributeVGroups)
        t5.start()
        tdLog.debug("threading started!!!!!")
        time.sleep(1)
        tdSql.error('compact database db;', expectErrInfo="Conflict transaction not completed")
        t5.join()

        t6 = threading.Thread(target=self.balanceVGROUPThread)
        t6.start()
        tdLog.debug("threading started!!!!!")
        time.sleep(1)
        tdSql.error('compact database db;', expectErrInfo="Conflict transaction not completed")
        t6.join()

        t7 = threading.Thread(target=self.alterDBThread)
        t7.start()
        tdLog.debug("threading started!!!!!")
        time.sleep(1)
        tdSql.error('compact database db;', expectErrInfo="Conflict transaction not completed")
        t7.join()


  def compactDBThread(self):
    tdLog.info("compact db start")
    tdSql.execute('compact DATABASE db')
    if self.waitCompactsZero() is False:
            tdLog.info(f"compact not finished")

  def alterDBThread(self):
    tdLog.info("alter db start")
    tdSql.execute('ALTER DATABASE db REPLICA 3')
    if self.waitTransactionZero() is False:
            tdLog.info(f"transaction not finished")

  def balanceVGROUPThread(self):
    tdLog.info("balance VGROUP start")
    tdSql.execute('BALANCE VGROUP')
    if self.waitTransactionZero() is False:
            tdLog.info(f"transaction not finished")

  def RedistributeVGroups(self):
        sql = f"REDISTRIBUTE VGROUP 5 DNODE 1"
        tdSql.execute(sql, show=True)
        if self.waitTransactionZero() is False:
            tdLog.exit(f"{sql} transaction not finished")
            return False
  
        sql = f"REDISTRIBUTE VGROUP 4 DNODE 1"
        tdSql.execute(sql, show=True)
        if self.waitTransactionZero() is False:
            tdLog.exit(f"{sql} transaction not finished")
            return False
        
        sql = f"REDISTRIBUTE VGROUP 3 DNODE 1"
        tdSql.execute(sql, show=True)
        if self.waitTransactionZero() is False:
            tdLog.exit(f"{sql} transaction not finished")
            return False
        
        return True

  def splitVgroupThread(self):
        rowLen = tdSql.query('show vgroups')
        if rowLen > 0:
            vgroupId = tdSql.getData(0, 0)
            tdLog.debug(f"splitVgroupThread vgroupId:{vgroupId}")
            tdSql.execute(f"split vgroup {vgroupId}")
        else:
            tdLog.exit("get vgroupId fail!")
        if self.waitTransactionZero() is False:
            tdLog.info(f"transaction not finished")

  def stop(self):
    tdSql.close()
    tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())