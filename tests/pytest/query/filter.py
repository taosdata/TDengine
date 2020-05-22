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
        tdSql.init(conn.cursor())

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

        # > for timestamp type
        tdSql.query("select * from db.st where ts > '2020-05-13 10:00:00.002'")
        tdSql.checkRows(3)

        # > for numeric type
        tdSql.query("select * from db.st where tagtype > 2")
        tdSql.checkRows(2)

        # < for timestamp type
        tdSql.query("select * from db.st where ts < '2020-05-13 10:00:00.002'")
        tdSql.checkRows(2)

        # < for numeric type
        tdSql.query("select * from db.st where tagtype < 2")
        tdSql.checkRows(2)

        # >= for timestamp type
        tdSql.query("select * from db.st where ts >= '2020-05-13 10:00:00.002'")
        tdSql.checkRows(4)

        # >= for numeric type
        tdSql.query("select * from db.st where tagtype >= 2")
        tdSql.checkRows(4)

        # <= for timestamp type
        tdSql.query("select * from db.st where ts <= '2020-05-13 10:00:00.002'")
        tdSql.checkRows(3)

        # <= for numeric type
        tdSql.query("select * from db.st where tagtype <= 2")
        tdSql.checkRows(4)

        # = for timestamp type
        tdSql.query("select * from db.st where ts = '2020-05-13 10:00:00.002'")
        tdSql.checkRows(1)

        # = for numeric type
        tdSql.query("select * from db.st where tagtype = 2")
        tdSql.checkRows(2)

        # = for nchar type
        tdSql.query("select * from db.st where name = 'first'")
        tdSql.checkRows(2)

        # <> for timestamp type
        tdSql.query("select * from db.st where ts <> '2020-05-13 10:00:00.002'")
        #tdSql.checkRows(4)

        # <> for numeric type
        tdSql.query("select * from db.st where tagtype <> 2")
        tdSql.checkRows(4)

        # <> for nchar type
        tdSql.query("select * from db.st where name <> 'first'")
        tdSql.checkRows(4)
        
        # % for nchar type
        tdSql.query("select * from db.st where name like 'fi%'")
        tdSql.checkRows(2)

        # - for nchar type
        tdSql.query("select * from db.st where name like '_econd'")
        tdSql.checkRows(2)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
