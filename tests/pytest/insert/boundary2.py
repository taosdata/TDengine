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

import random
import string
import time
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql
from util.dnodes import tdDnodes

class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.ts = 1538548685000

    def get_random_string(self, length):
        letters = string.ascii_lowercase
        result_str = ''.join(random.choice(letters) for i in range(length))
        return result_str

    def run(self):
        tdSql.prepare()

        startTime = time.time()
        print("==============step1")
        sql = "create table stb(ts timestamp, "
        for i in range(1022):
            sql += "col%d binary(14), " % (i + 1)
        sql += "col1023 binary(22))"        
        tdSql.execute(sql)

        for i in range(4096):
            sql = "insert into stb values(%d, "
            for j in range(1022):
                str = "'%s', " % self.get_random_string(14)                
                sql += str
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i))

        tdSql.query("select * from stb")
        tdSql.checkRows(4096)

        tdDnodes.stop(1)
        tdDnodes.start(1)
        
        tdSql.query("select * from stb")
        tdSql.checkRows(4096)

        endTime = time.time()

        print("total time %ds" % (endTime - startTime))

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
