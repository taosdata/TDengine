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
import taos
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()
        
        tdSql.execute(
            "create table if not exists st (ts timestamp, tagtype int) tags(dev nchar(50))")
        tdSql.execute(
            'CREATE TABLE if not exists dev_001 using st tags("dev_01")')
        tdSql.execute(
            'CREATE TABLE if not exists dev_002 using st tags("dev_02")')        

        tdSql.execute(
            """INSERT INTO dev_001(ts, tagtype) VALUES('2020-05-13 10:00:00.000', 1),
            ('2020-05-13 10:00:00.001', 1)
             dev_002 VALUES('2020-05-13 10:00:00.001', 1)""")

        for i in range(10):
            for j in range(1000):
                tdSql.query("select * from db.st")      
            tdLog.sleep(10)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
