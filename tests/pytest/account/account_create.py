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
        tdSql.query("select * from information_schema.ins_users")
        rows = tdSql.queryRows

        tdSql.execute("create user test PASS 'test' ")
        tdSql.query("select * from information_schema.ins_users")
        tdSql.checkRows(rows + 1)

        tdSql.error("create user tdenginetdenginetdengine PASS 'test' ")

        tdSql.error("create user tdenginet PASS '1234512345123456' ")

        try:
            tdSql.execute("create account a&cc PASS 'pass123'")
        except Exception as e:
            print("create account a&cc PASS 'pass123'")
            return

        tdLog.exit("drop built-in user is error.")
        

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
