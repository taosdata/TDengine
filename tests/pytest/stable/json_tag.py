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
        tdSql.execute("create table if not exists db_json_tag_test.jsons1(ts timestamp, dataInt int, dataStr nchar(50)) tags(jtag json(128))")
        tdSql.execute("CREATE TABLE if not exists db_json_tag_test.jsons1_1 using db_json_tag_test.jsons1 tags('{\"loc\":\"fff\",\"id\":5}')")
        tdSql.error("CREATE TABLE if not exists db_json_tag_test.jsons1_3 using db_json_tag_test.jsons1 tags(3333)")
        tdSql.execute("insert into db_json_tag_test.jsons1_2 using db_json_tag_test.jsons1 tags('{\"num\":5,\"location\":\"beijing\"}') values (now, 2, 'json2')")
        tdSql.error("insert into db_json_tag_test.jsons1_4 using db_json_tag_test.jsons1 tags(3)")
        tdSql.execute("insert into db_json_tag_test.jsons1_1 values(now, 1, 'json1')")
        tdSql.execute("insert into db_json_tag_test.jsons1_3 using db_json_tag_test.jsons1 tags('{\"num\":34,\"location\":\"beijing\",\"level\":\"l1\"}') values (now, 3, 'json3')")
        tdSql.execute("insert into db_json_tag_test.jsons1_4 using db_json_tag_test.jsons1 tags('{\"class\":55,\"location\":\"shanghai\",\"name\":\"name4\"}') values (now, 4, 'json4')")

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

        # error test
        #tdSql.error("select * from db_json_tag_test.jsons1 where jtag->'location'=4")
        tdSql.error("select * from db_json_tag_test.jsons1 where jtag->location='beijing'")
        tdSql.error("select * from db_json_tag_test.jsons1 where jtag->'location'")
        tdSql.error("select jtag->location from db_json_tag_test.jsons1")
        tdSql.error("select jtag?location from db_json_tag_test.jsons1")
        tdSql.error("select * from db_json_tag_test.jsons1 where jtag?location")
        tdSql.error("select * from db_json_tag_test.jsons1 where jtag?'location'='beijing'")

        # test select condition
        tdSql.query("select jtag->'location' from db_json_tag_test.jsons1_2")
        tdSql.checkData(0, 0, "beijing")

        tdSql.query("select jtag->'location' from db_json_tag_test.jsons1")
        tdSql.checkRows(4)

        tdSql.query("select jtag from db_json_tag_test.jsons1_1")
        tdSql.checkRows(1)

        # test json string value
        tdSql.query("select * from db_json_tag_test.jsons1 where jtag->'location'='beijing'")
        tdSql.checkRows(2)

        tdSql.query("select * from db_json_tag_test.jsons1 where jtag->'location'!='beijing'")
        tdSql.checkRows(1)

        tdSql.query("select jtag->'num' from db_json_tag_test.jsons1 where jtag->'level'='l1'")
        tdSql.checkData(0, 0, 34)

        # test json number value
        tdSql.query("select *,tbname from db_json_tag_test.jsons1 where jtag->'class'>5 and jtag->'class'<9")
        tdSql.checkRows(0)

        tdSql.query("select *,tbname from db_json_tag_test.jsons1 where jtag->'class'>5 and jtag->'class'<92")
        tdSql.checkRows(1)

        # test where condition
        tdSql.query("select * from db_json_tag_test.jsons1 where jtag?'sex' or jtag?'num'")
        tdSql.checkRows(3)

        tdSql.query("select * from db_json_tag_test.jsons1 where jtag?'sex' or jtag?'numww'")
        tdSql.checkRows(1)

        tdSql.query("select * from db_json_tag_test.jsons1 where jtag?'sex' and jtag?'num'")
        tdSql.checkRows(0)

        tdSql.query("select jtag->'sex' from db_json_tag_test.jsons1 where jtag?'sex' or jtag?'num'")
        tdSql.checkData(0, 0, "femail")
        tdSql.checkRows(3)

        tdSql.query("select *,tbname from db_json_tag_test.jsons1 where jtag->'location'='beijing'")
        tdSql.checkRows(2)

        tdSql.query("select *,tbname from db_json_tag_test.jsons1 where jtag->'num'=5 or jtag?'sex'")
        tdSql.checkRows(2)

        # test with tbname
        tdSql.query("select * from db_json_tag_test.jsons1 where tbname = 'jsons1_1'")
        tdSql.checkRows(1)

        tdSql.query("select * from db_json_tag_test.jsons1 where tbname = 'jsons1_1' or jtag?'num'")
        tdSql.checkRows(3)

        tdSql.query("select * from db_json_tag_test.jsons1 where tbname = 'jsons1_1' and jtag?'num'")
        tdSql.checkRows(0)

        tdSql.query("select * from db_json_tag_test.jsons1 where tbname = 'jsons1_1' or jtag->'num'=5")
        tdSql.checkRows(2)

        # test where condition like
        tdSql.query("select *,tbname from db_json_tag_test.jsons1 where jtag->'location' like 'bei%'")
        tdSql.checkRows(2)

        tdSql.query("select *,tbname from db_json_tag_test.jsons1 where jtag->'location' like 'bei%' and jtag->'location'='beijin'")
        tdSql.checkRows(0)

        tdSql.query("select *,tbname from db_json_tag_test.jsons1 where jtag->'location' like 'bei%' or jtag->'location'='beijin'")
        tdSql.checkRows(2)

        tdSql.query("select *,tbname from db_json_tag_test.jsons1 where jtag->'location' like 'bei%' and jtag->'num'=34")
        tdSql.checkRows(1)

        tdSql.query("select *,tbname from db_json_tag_test.jsons1 where (jtag->'location' like 'bei%' or jtag->'num'=34) and jtag->'class'=55")
        tdSql.checkRows(0)

        # test where condition in
        tdSql.query("select * from db_json_tag_test.jsons1 where jtag->'location' in ('beijing')")
        tdSql.checkRows(2)

        tdSql.query("select * from db_json_tag_test.jsons1 where jtag->'num' in (5,34)")
        tdSql.checkRows(2)

        tdSql.error("select * from db_json_tag_test.jsons1 where jtag->'num' in ('5',34)")

        tdSql.query("select * from db_json_tag_test.jsons1 where jtag->'location' in ('shanghai') and jtag->'class'=55")
        tdSql.checkRows(1)

        # test where condition match
        tdSql.query("select * from db_json_tag_test.jsons1 where jtag->'location' match 'jin$'")
        tdSql.checkRows(0)

        tdSql.query("select * from db_json_tag_test.jsons1 where jtag->'location' match 'jin'")
        tdSql.checkRows(2)

        tdSql.query("select * from db_json_tag_test.jsons1 where datastr match 'json and jtag->'location' match 'jin'")
        tdSql.checkRows(2)

        tdSql.error("select * from db_json_tag_test.jsons1 where jtag->'num' match '5'")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
