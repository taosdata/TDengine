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
from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        print("==============step1")
        tdSql.execute(
            "create table if not exists st (ts timestamp, tagtype int, name nchar(16)) tags(dev nchar(50))")
        tdSql.execute(
            'CREATE TABLE if not exists dev_001 using st tags("dev_01")')
        tdSql.execute(
            'CREATE TABLE if not exists dev_002 using st tags("dev_02")')

        print("==============step2")

        tdSql.execute(
            """INSERT INTO dev_001(ts, tagtype, name) VALUES('2020-05-13 10:00:00.000', 1, 'first'),('2020-05-13 10:00:00.001', 2, 'second'),
            ('2020-05-13 10:00:00.002', 3, 'third') dev_002 VALUES('2020-05-13 10:00:00.003', 1, 'first'), ('2020-05-13 10:00:00.004', 2, 'second'),
            ('2020-05-13 10:00:00.005', 3, 'third')""")

        # query first .. as ..
        tdSql.error("select first(*) as one from st")

        # query last .. as ..
        tdSql.error("select last(*) as latest from st")

        # query last row .. as ..
        tdSql.error("select last_row as latest from st")

        # query distinct on normal colnum
        tdSql.error("select distinct tagtype from st")

        # query .. order by non-time field
        tdSql.error("select * from st order by name")

        # TD-2133
        tdSql.error("select diff(tagtype),bottom(tagtype,1) from dev_001")

        # TD-2190
        tdSql.error("select min(tagtype),max(tagtype) from dev_002 interval(1n) fill(prev)")

        # TD-2208
        tdSql.error("select diff(tagtype),top(tagtype,1) from dev_001")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
