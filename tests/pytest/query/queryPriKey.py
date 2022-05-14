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
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()
        tdSql.execute("drop database if exists tdb")
        tdSql.execute("create database if not exists tdb keep 3650")
        tdSql.execute("use tdb")

        tdSql.execute(
            "create table stb1 (time timestamp, c1 int) TAGS (t1 int)"
        )

        tdSql.execute(
            "insert into t1 using stb1 tags(1) values (now - 1m, 1)"
        )
        tdSql.execute(
            "insert into t1 using stb1 tags(1) values (now - 2m, 2)"
        )
        tdSql.execute(
            "insert into t1 using stb1 tags(1) values (now - 3m, 3)"
        )

        res = tdSql.getColNameList("select count(*) from t1 interval(1m)")
        assert res[0] == 'time'

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
