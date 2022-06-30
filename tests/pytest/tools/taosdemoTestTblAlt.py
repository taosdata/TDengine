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
import os
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *
import threading
import time


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.numberOfTables = 8
        self.numberOfRecords = 1000000

    def getPath(self, tool="taosBenchmark"):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        paths = []
        for root, dirs, files in os.walk(projPath):
            if ((tool) in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    paths.append(os.path.join(root, tool))
                    break
        if (len(paths) == 0):
            return ""
        return paths[0]

    def insertDataAndAlterTable(self, threadID):
        binPath = self.getPath("taosBenchmark")
        if (binPath == ""):
            tdLog.exit("taosBenchmark not found!")
        else:
            tdLog.info("taosBenchmark found in %s" % binPath)

        if(threadID == 0):
            print("%s -y -t %d -n %d -b INT,INT,INT,INT" %
                  (binPath, self.numberOfTables, self.numberOfRecords))
            os.system("%s -y -t %d -n %d -b INT,INT,INT,INT" %
                      (binPath, self.numberOfTables, self.numberOfRecords))
        if(threadID == 1):
            time.sleep(2)
            print("use test")
            max_try = 100
            count = 0
            while (count < max_try):
                try:
                    tdSql.execute("use test")
                    break
                except Exception as e:
                    tdLog.info("use database test failed")
                    time.sleep(2)
                    count += 1
                    print("try %d times" % count)
                    continue

            # check if all the tables have heen created
            count = 0
            while (count < max_try):
                try:
                    tdSql.query("show tables")
                except Exception as e:
                    tdLog.info("show tables test failed")
                    time.sleep(2)
                    count += 1
                    print("try %d times" % count)
                    continue

                rows = tdSql.queryRows
                print("number of tables: %d" % rows)
                if(rows == self.numberOfTables):
                    break
                time.sleep(1)
            # check if there are any records in the last created table
            count = 0
            while (count < max_try):
                print("query started")
                print("try %d times" % count)
                try:
                    tdSql.query("select * from test.d7")
                except Exception as e:
                    tdLog.info("select * test failed")
                    time.sleep(2)
                    count += 1
                    print("try %d times" % count)
                    continue

                rows = tdSql.queryRows
                print("number of records: %d" % rows)
                if(rows > 0):
                    break
                time.sleep(1)

            print("alter table test.meters add column c10 int")
            tdSql.execute("alter table test.meters add column c10 int")
            print("insert into test.d7 values (now, 1, 2, 3, 4, 0)")
            tdSql.execute("insert into test.d7 values (now, 1, 2, 3, 4, 0)")

    def run(self):
        tdSql.prepare()

        t1 = threading.Thread(target=self.insertDataAndAlterTable, args=(0, ))
        t2 = threading.Thread(target=self.insertDataAndAlterTable, args=(1, ))

        t1.start()
        t2.start()
        t1.join()
        t2.join()

        time.sleep(3)

        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, self.numberOfRecords * self.numberOfTables + 1)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
