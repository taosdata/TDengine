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
        tdSql.prepare()
        
        tdSql.execute(
            "create table stest(ts timestamp,size INT,filenum INT) tags (appname binary(500),tenant binary(500))")
        tdSql.execute(
            "insert into test1 using stest tags('test1','aaa') values ('2020-09-04 16:53:54.003',210,3)")
        tdSql.execute(
            "insert into test2 using stest tags('test1','aaa') values ('2020-09-04 16:53:56.003',210,3)")
        tdSql.execute(
            "insert into test11 using stest tags('test11','bbb') values ('2020-09-04 16:53:57.003',210,3)")
        tdSql.execute(
            "insert into test12 using stest tags('test11','bbb') values ('2020-09-04 16:53:58.003',210,3)")
        tdSql.execute(
            "insert into test21 using stest tags('test21','ccc') values ('2020-09-04 16:53:59.003',210,3)")
        tdSql.execute(
            "insert into test22 using stest tags('test21','ccc') values ('2020-09-04 16:54:54.003',210,3)")

        #2021-09-17 For jira: https://jira.taosdata.com:18080/browse/TD-6085
        tdSql.query("select last(size),appname from stest where tbname in ('test1','test2','test11')")
        tdSql.checkRows(1)

        #2021-09-17 For jira: https://jira.taosdata.com:18080/browse/TD-6314
        tdSql.query("select _block_dist() from stest")
        tdSql.checkRows(1)

        tdSql.query("select sum(size) from stest interval(1d) group by appname")        
        tdSql.checkRows(3)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
