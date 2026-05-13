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


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        tdSql.error('create table tb (ts timestamp, col nchar(1022))')
        tdSql.execute('create table tb (ts timestamp, col nchar(1021))')
        tdSql.execute("insert into tb values (now, 'taosdata')")
        tdSql.query("select * from tb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 'taosdata')

        with open("../../README.md", "r") as inputFile:
            data = inputFile.read(1021).replace(
                "\n",
                " ").replace(
                "\\",
                " ").replace(
                "\'",
                " ").replace(
                "\"",
                " ").replace(
                    "[",
                    " ").replace(
                        "]",
                        " ").replace(
                            "!",
                " ")

        tdLog.info("insert %d length data: %s" % (len(data), data))

        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(2)
        tdSql.checkData(1, 1, data)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
