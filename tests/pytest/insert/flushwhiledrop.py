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
import threading
from util.log import *
from util.cases import *
from util.sql import *
from time import sleep


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self.numberOfRecords = 15000
        self.ts = 1601481600000

    def run(self):
        tdSql.execute('create database test cache 1 blocks 3')
        tdSql.execute('use test')
        tdSql.execute('create table tb(ts timestamp, c1 timestamp, c2 int, c3 bigint, c4 float, c5 double, c6 binary(8), c7 smallint, c8 tinyint, c9 bool, c10 nchar(8))')
        threads = []
        t1 = threading.Thread(target=self.insertAndFlush, args=())
        threads.append(t1)
        t2 = threading.Thread(target=self.drop, args=())
        threads.append(t2)
        for t in threads:
            t.setDaemon(True)
            t.start()
        for t in threads:
            t.join() 

    def insertAndFlush(self):
        finish = 0
        currts = self.ts

        while(finish < self.numberOfRecords):
            sql = "insert into tb values"
            for i in range(finish, self.numberOfRecords):
                sql += "(%d, 1019774612, 29931, 1442173978, 165092.468750, 1128.643179, 'MOCq1pTu', 18405, 82, 0, 'g0A6S0Fu')" % (currts + i)
                finish = i + 1
                if (1048576 - len(sql)) < 16384:
                    break
            tdSql.execute(sql)
        
    def drop(self):
        sleep(30)
        tdSql.execute('drop database test')

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())