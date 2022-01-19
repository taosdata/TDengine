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
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database if not exists db keep 3650")
        tdSql.execute("use db")

        tdSql.execute(
            "create table stb1 (ts TIMESTAMP, id INT, col1 NCHAR(20), col2 BINARY(30), col3 FLOAT) TAGS (tid INT, name BINARY(20))"
        )

        tdSql.execute(
            "insert into tb1 using stb1 tags(1, 'ABC') values (now - 1m, 1, '北京', '朝阳', 3.141)"
        )

        tdSql.execute(
            "insert into tb1 using stb1 tags(1, 'ABC') values (now, 2, NULL, NULL, 3.141)"
        )

        tdSql.query(
            "select * from (select * from stb1) where col1 = '北京'"
        )

        tdSql.checkData(0, 2, '北京') 

        tdSql.execute(
            "create table normal1 (ts TIMESTAMP, id INT, col1 NCHAR(20), col2 BINARY(30), col3 FLOAT)"
        )

        tdSql.execute(
            "insert into normal1 values (now - 1m, 1, '北京', '朝阳', 3.141)"
        )

        tdSql.execute(
            "insert into normal1 values (now, 1, NULL, NULL, 3.141)"
        )

        tdSql.query(
            "select * from (select * from normal1) where col1 = '北京'"
        )

        tdSql.checkData(0, 2, '北京') 

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
