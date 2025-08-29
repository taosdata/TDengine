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

from new_test_framework.utils import tdLog, tdSql
import os
import time

class TestRow64kinsertbycsv:
    updatecfgDict = {'maxSQLLength':1048576,'debugFlag': 143 ,"querySmaOptimize":1}
    
    def init(self, conn, logSql, replicaVar):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.testcasePath = os.path.split(__file__)[0]
        self.testcasePath = self.testcasePath.replace('\\', '//')
        self.testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))
        
        now = time.time()
        self.ts = int(round(now * 1000))
        self.num = 100

    def insertFromCsvOfLength65500(self):  
        
        tdLog.info('test insert from csv of length 65500')
        os.system(f"taosBenchmark -f {self.testcasePath}//tableColumn4096.json")
        
        tdSql.execute(f"insert into db4096.ctb00 file '{self.testcasePath}//tableColumn4096csvLength64k.csv'")
        tdSql.query("select count(*) from db4096.ctb00")
        tdSql.checkData(0, 0, 1)
        tdSql.query("select length(c4092) from db4096.ctb00")
        tdSql.checkData(0, 0, 16375)


    def test_row64kInsertByCsv(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
        - xxx:xxx

        History:
        - xxx
        - xxx

        """
        tdSql.prepare()
        
        startTime_all = time.time() 
        self.insertFromCsvOfLength65500()
        endTime_all = time.time()
        print("total time %ds" % (endTime_all - startTime_all))
        
        tdLog.success(f"{__file__} successfully executed")
        