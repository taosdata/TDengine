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
from util.dnodes import tdDnodes

class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.ts = 1538548685000

    def run(self):
        tdSql.prepare()

        print("==============step1")
        tdSql.execute(
            "CREATE TABLE IF NOT EXISTS ampere (ts TIMESTAMP(8),ampere DOUBLE(8)) TAGS (device_name BINARY(50),build_id BINARY(50),project_id BINARY(50),alias BINARY(50))")
        tdSql.execute("insert into d1001 using ampere tags('test', '2', '2', '2') VALUES (now, 123)")
        tdSql.execute("ALTER TABLE ampere ADD TAG variable_id BINARY(50)")

        print("==============step2")

        tdSql.execute("insert into d1002 using ampere tags('test', '2', '2', '2', 'test') VALUES (now, 124)")

        tdSql.query("select * from ampere")
        tdSql.checkRows(2)
        tdSql.checkData(0, 6, None)
        tdSql.checkData(1, 6, 'test')        

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
