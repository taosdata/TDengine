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
                
        ### test case for TD-1758 ###
        print("==============step1")
        tdSql.execute(
            "create table t0(ts timestamp, c int)")
        tdSql.execute(
            'create table t1(ts timestamp, c binary(1))')
        tdSql.execute(
            "insert into t0 values(now,1) t1 values(now,'0')(now+1a,'1')(now+2a,'2')(now+3a,'3')(now+4a,'4')")

        print("==============step2")

        tdSql.query("select * from t0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)

        tdSql.query("select * from t1")
        tdSql.checkRows(5)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
