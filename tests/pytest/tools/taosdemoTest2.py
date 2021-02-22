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

        self.numberOfTables = 10
        self.numberOfRecords = 1000000

    def insertDataAndAlterTable(self, threadID):
        if(threadID == 0):
            os.system("taosdemo -y -t %d -n %d -x" %
                      (self.numberOfTables, self.numberOfRecords))
        if(threadID == 1):
            time.sleep(2)
            print("use test")
            tdSql.execute("use test")
            # check if all the tables have heen created
            while True:
                tdSql.query("show tables")
                rows = tdSql.queryRows
                print("number of tables: %d" % rows)
                if(rows == self.numberOfTables):
                    break
                time.sleep(1)
            # check if there are any records in the last created table
            while True:
                print("query started")
                tdSql.query("select * from test.t9")
                rows = tdSql.queryRows
                print("number of records: %d" % rows)
                if(rows > 0):
                    break
                time.sleep(1)
            print("alter table test.meters add column f4 int")
            tdSql.execute("alter table test.meters add column f4 int")
            print("insert into test.t0 values (now, 1, 2, 3, 4)")
            tdSql.execute("insert into test.t0 values (now, 1, 2, 3, 4)")

    def run(self):
        tdSql.prepare()

        t1 = threading.Thread(target=self.insertDataAndAlterTable, args=(0, ))
        t2 = threading.Thread(target=self.insertDataAndAlterTable, args=(1, ))

        t1.start()
        t2.start()
        t1.join()
        t2.join()

        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, self.numberOfRecords * self.numberOfTables + 1)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
