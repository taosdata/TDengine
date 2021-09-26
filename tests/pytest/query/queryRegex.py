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
from util.dnodes import tdDnodes


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()
        print("==============step1")
        ##2021-09-17 For jira: https://jira.taosdata.com:18080/browse/TD-6585
        tdSql.execute(
            "create stable if not exists stb_test(ts timestamp,c0 binary(32)) tags(t0 binary(32))"
        )
        tdSql.execute(
            'create table if not exists stb_1 using stb_test tags("abcdefgasdfg12346")'
        )
        tdLog.info('insert into stb_1 values("2021-09-13 10:00:00.001","abcefdasdqwerxasdazx12345"')
        tdSql.execute('insert into stb_1 values("2021-09-13 10:00:00.001","abcefdasdqwerxasdazx12345")')


        tdSql.query('select * from stb_test where tbname match "asd"')
        tdSql.checkRows(0)

        tdSql.query('select * from stb_test where tbname nmatch "asd"')
        tdSql.checkRows(1)

        tdSql.query('select * from stb_test where c0 match "abc"')
        tdSql.checkRows(1)
        tdSql.checkData(0,1,"abcefdasdqwerxasdazx12345")

        tdSql.query('select * from stb_test where c0 nmatch "abc"')
        tdSql.checkRows(0)

        tdSql.error('select * from stb_test where c0 match abc')

        tdSql.error('select * from stb_test where c0 nmatch abc')


        tdSql.execute('insert into stb_1 values("2020-10-13 10:00:00.001","abcd\\\efgh")')
        tdSql.query("select * from stb_1 where c0 match '\\\\'")
        tdSql.checkRows(1)






    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
