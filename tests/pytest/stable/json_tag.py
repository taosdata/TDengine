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
        tdSql.execute("create table if not exists db_json_tag_test.jsons1(ts timestamp, dataInt int, dataStr nchar(50)) tags(jtag json(64))")
        tdSql.execute("CREATE TABLE if not exists db_json_tag_test.jsons1_1 using db_json_tag_test.jsons1 tags('{\"loc\":\"fff\",\"id\":5}')")
        tdSql.error("CREATE TABLE if not exists db_json_tag_test.jsons1_3 using db_json_tag_test.jsons1 tags(3333)")
        tdSql.execute("insert into db_json_tag_test.jsons1_2 using db_json_tag_test.jsons1 tags('{\"num\":5,\"location\":\"beijing\"}') values (now, 2, 'json2')")
        tdSql.error("insert into db_json_tag_test.jsons1_4 using db_json_tag_test.jsons1 tags(3)")
        tdSql.execute("insert into db_json_tag_test.jsons1_1 values(now, 1, 'json1')")
        tdSql.execute("insert into db_json_tag_test.jsons1_3 using db_json_tag_test.jsons1 tags('{\"num\":34,\"location\":\"beijing\",\"level\":\"l1\"}') values (now, 3, 'json3')")
        tdSql.execute("insert into db_json_tag_test.jsons1_4 using db_json_tag_test.jsons1 tags('{\"class\":55,\"location\":\"beijing\",\"name\":\"name4\"}') values (now, 4, 'json4')")

        print("==============step2")
        tdLog.info("alter stable add tag")
        tdSql.error("ALTER STABLE db_json_tag_test.jsons1 add tag tag2 nchar(20)")

        tdSql.error("ALTER STABLE db_json_tag_test.jsons1 drop tag jtag")

        tdSql.error("ALTER TABLE db_json_tag_test.jsons1_1 SET TAG jtag=4")

        tdSql.execute("ALTER TABLE db_json_tag_test.jsons1_1 SET TAG jtag='{\"sex\":\"femail\",\"age\":35}'")
        tdSql.query("select jtag from db_json_tag_test.jsons1_1")
        tdSql.checkData(0, 0, "{\"sex\":\"femail\",\"age\":35}")

        print("==============step3")
        tdLog.info("select table")

        tdSql.query("select * from db_json_tag_test.jsons1")
        tdSql.checkRows(4)

        tdSql.error("select * from db_json_tag_test.jsons1 where jtag->'location'=4")

        tdSql.query("select * from db_json_tag_test.jsons1 where jtag->'location'='beijing'")
        tdSql.checkRows(3)

        tdSql.query("select jtag->'location' from db_json_tag_test.jsons1_2")
        tdSql.checkData(0, 0, "beijing")


        tdSql.query("select jtag->'num' from db_json_tag_test.jsons1 where jtag->'level'='l1'")
        tdSql.checkData(0, 0, 34)

        tdSql.query("select jtag->'location' from db_json_tag_test.jsons1")
        tdSql.checkRows(4)

        tdSql.query("select jtag from db_json_tag_test.jsons1_1")
        tdSql.checkRows(1)

        tdSql.query("select * from db_json_tag_test.jsons1 where jtag?'sex' or jtag?'num'")
        tdSql.checkRows(3)

        tdSql.query("select * from db_json_tag_test.jsons1 where jtag?'sex' or jtag?'numww'")
        tdSql.checkRows(1)

        tdSql.query("select * from db_json_tag_test.jsons1 where jtag?'sex' and jtag?'num'")
        tdSql.checkRows(0)

        tdSql.query("select jtag->'sex' from db_json_tag_test.jsons1 where jtag?'sex' or jtag?'num'")
        tdSql.checkData(0, 0, "femail")
        tdSql.checkRows(3)



    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
