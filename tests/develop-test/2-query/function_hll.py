###################################################################
#  Copyright (c) 2021 by TAOS Technologies, Inc.
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
    def caseDescription(self):
        '''
        case1<markwang>: [TD-13893] hyperloglog unique
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self._conn = conn

    def run(self):
        print("running {}".format(__file__))
        tdSql.execute("drop database if exists hll")
        tdSql.execute("create database if not exists hll")
        tdSql.execute('use hll')

        tdSql.execute('create table shll (ts timestamp, dbig bigint, dsmall smallint, dbool bool, dtiny tinyint unsigned, dfloat float, ddouble double, dnchar nchar(4093), dbinary binary(64), dtime timestamp) tags (tbinary nchar(4093), tint int)')
        tdSql.execute('create table hll1 using shll tags ("t1", 1)')
        tdSql.execute('create table hll2 using shll tags ("t2", 2)')

        tdSql.execute('insert into hll1 values("2021-10-17 00:31:31", 1, -3276, true, 253, 3.32333, 4.984392323, "你好", "sddd", 333) ("2022-01-24 00:31:32", 1, -32767, false, 254, NULL, 4.982392323, "你好吗", "sdf",2323)')
        tdSql.execute('insert into hll2 values("2021-10-15 00:31:33", 1, NULL, true, 23, 3.4, 4.982392323, "你好吗", "sdf", 333) ("2021-12-24 00:31:34", 2, 32767, NULL, NULL, NULL, 4.982392323, NULL, "sddd", NULL) ("2022-01-01 08:00:05", 19, 3276, true, 2, 3.323222, 4.92323, "试试", "sddd", 1645434434000)')
        tdSql.execute('insert into hll2 values("2021-10-17 00:31:31", NULL, 32767, true, 123, 3.323232333, 4.2, NULL, NULL, NULL) ("2022-01-01 08:00:06", NULL, NULL, NULL, 35, 3.323232333, NULL, "试试", NULL, 1645434434000) ("2022-01-01 08:00:07", 9, 54, true, 25, 3.32333, NULL, "试试", NULL, 1645434434001)')

        ## test normal table
        tdSql.query('select hyperloglog(ts) from hll2')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 6)

        tdSql.query('select hyperloglog(dbig) from hll2')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4)

        tdSql.query('select hyperloglog(dsmall) from hll2')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)

        tdSql.query('select hyperloglog(dbool) from hll2')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query('select hyperloglog(dtiny) from hll2')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 5)

        tdSql.query('select hyperloglog(dfloat) from hll2')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4)

        tdSql.query('select hyperloglog(ddouble) from hll2')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)

        tdSql.query('select hyperloglog(dnchar) from hll2')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

        tdSql.query('select hyperloglog(dbinary) from hll2')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

        ## test super table
        tdSql.query('select hyperloglog(dnchar) from shll')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)

        # test group by
        #group by column
        tdSql.query('select hyperloglog(dnchar) from shll group by dnchar')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)

        tdSql.query('select hyperloglog(dsmall) from shll group by dnchar')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 2)

        tdSql.query('select hyperloglog(dsmall) from hll2 group by dnchar')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 0)
        tdSql.checkData(2, 0, 2)

        #group by tbname
        tdSql.query('select hyperloglog(dsmall) from shll group by tbname')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 3)

        #group by tag
        tdSql.query('select hyperloglog(dnchar) from shll group by tint')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 2)

        #test order by


        #order by column [desc]
        tdSql.query('select hyperloglog(dnchar) from shll group by dnchar order by dnchar desc')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 0)

        #order by tag
        tdSql.query('select hyperloglog(dsmall) from shll group by tint order by tint desc')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 2)

        # error
        tdSql.error("select hyperloglog(ts,1) from shll")

        #interval
        tdSql.query('select hyperloglog(dnchar) from shll interval(1s)')
        tdSql.checkRows(7)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 0, "2021-12-24 00:31:34")
        tdSql.checkData(2, 1, 0)
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(4, 1, 1)
        tdSql.checkData(5, 1, 1)
        tdSql.checkData(6, 1, 1)

        tdSql.query('select hyperloglog(dnchar) from shll interval(1w)')
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 1, 0)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 1, 1)


        #state_window
        tdSql.query('select hyperloglog(dnchar) from hll2 state_window(dsmall)')
        tdSql.checkRows(5)

        #session
        tdSql.query('select hyperloglog(dbinary) from hll2 session(ts,2w)')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2021-10-15 00:31:33")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, "2021-12-24 00:31:34")
        tdSql.checkData(1, 1, 1)

        #where
        tdSql.query('select hyperloglog(dbinary) from shll where dnchar="试试"')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.query('select hyperloglog(dbinary) from shll where ts <= "2022-01-01 08:00:05"')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

        #slimit/soffset
        tdSql.query('select hyperloglog(dsmall) from shll group by dnchar slimit 2 soffset 2')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 2)

        #limit/offset
        tdSql.query('select hyperloglog(dnchar) from shll interval(1s) limit 1,3')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2021-10-17 00:31:31")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 0)
        tdSql.checkData(2, 1, 1)

        #having
        tdSql.query('select hyperloglog(dsmall) from shll group by dnchar having hyperloglog(dsmall)>1')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

        #subquery
        tdSql.query('select hyperloglog(dbinary) from (select dbinary from shll where dnchar = "试试")')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        #union
        tdSql.query('select hyperloglog(dtiny) from hll1 union all select hyperloglog(dtiny) from hll2')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 5)

        #join
        tdSql.execute('create table shll1 (ts timestamp, dbig bigint, dsmall smallint, dbool bool, dtiny tinyint unsigned, dfloat float, ddouble double, dnchar nchar(4093), dbinary binary(64), dtime timestamp) tags (tbinary nchar(4093), tint int)')
        tdSql.execute('create table hll11 using shll1 tags ("t1", 1)')

        tdSql.execute('insert into hll11 values("2021-10-17 00:31:31", 1, -3276, true, 253, 3.32333, 4.984392323, "你好", "sddd", 333) ("2022-01-24 00:31:32", 1, -32767, false, 254, NULL, 4.982392323, "你好吗", "sdf",2323)')

        tdSql.query('select hyperloglog(shll1.ddouble) from shll, shll1 where shll.ts=shll1.ts and shll.tint=shll1.tint')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())

