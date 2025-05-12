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

        print("==============step1")
        tdSql.execute(
            "create stable t6 (ts timestamp,val int,flow nchar(36)) tags(dev nchar(36),dev1 nchar(36),dev2 nchar(36))")
        tdSql.execute("insert into t6004 using t6 (dev,dev1,dev2) tags ('b50c79bc-b102-48e6-bda1-4212263e46d0','b50c79bc-b102-48e6-bda1-4212263e46d0', 'b50c79bc-b102-48e6-bda1-4212263e46d0') values(now,1,'b50c79bc-b102-48e6-bda1-4212263e46d0')")


        print("==============step2")
        tdSql.query("select * from t6 where dev='b50c79bc-b102-48e6-bda1-4212263e46d0'")
        tdSql.checkRows(1)

        tdSql.query("select * from t6 where dev1='b50c79bc-b102-48e6-bda1-4212263e46d0'")
        tdSql.checkRows(1)

        tdSql.query("select * from t6 where dev2='b50c79bc-b102-48e6-bda1-4212263e46d0'")
        tdSql.checkRows(1)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
