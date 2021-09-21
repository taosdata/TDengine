###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, db_test.stored, transmitted,
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
        tdLog.info("create database and table")
        tdSql.execute("create database db_json_tag_test")
        tdSql.execute(
            "create table if not exists db_json_tag_test.jsons1(ts timestamp, dataInt int, dataStr nchar(50)) tags(jtag json)")
        tdSql.execute(
            "CREATE TABLE if not exists db_json_tag_test.jsons1_1 using db_json_tag_test.jsons1 tags('{\"loc\":\"fff\",\"id\":5}')")
        tdSql.execute(
            "insert into jsons1_2 using db_json_tag_test.jsons1 tags('{\"num\":5,\"location\":\"beijing\"}' values (now, 1, 'sss')")

        print("==============step2")
        tdLog.info("select table")

        tdSql.query("select * from db_json_tag_test.jsons1")
        tdSql.checkRows(1)

        tdSql.query("select jtag from db_json_tag_test.jsons1_1")
        tdSql.checkRows(1)

        print("==============step3")
        tdLog.info("alter stable add tag")
        tdSql.execute(
            "ALTER STABLE db_json_tag_test.jsons1 add COLUMN tag2 nchar(20)")

        tdSql.execute(
            "ALTER STABLE db_json_tag_test.jsons1 drop COLUMN jtag")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
