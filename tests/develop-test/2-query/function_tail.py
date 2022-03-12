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
        case1<markwang>: [TD-11214] tail unique
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self._conn = conn

    def run(self):
        print("running {}".format(__file__))
        tdSql.execute("drop database if exists tail")
        tdSql.execute("create database if not exists tail")
        tdSql.execute('use tail')

        tdSql.execute('create table stail (ts timestamp, dbig bigint, dsmall smallint, dbool bool, dtiny tinyint unsigned, dfloat float, ddouble double, dnchar nchar(4093), dbinary binary(64), dtime timestamp) tags (tbinary nchar(4093), tint int)')
        tdSql.execute('create table tail1 using stail tags ("t1", 1)')
        tdSql.execute('create table tail2 using stail tags ("t2", 2)')

        tdSql.execute('insert into tail1 values("2021-10-17 00:31:31", 1, -3276, true, 253, 3.32333, 4.984392323, "你好", "sddd", 333) ("2022-01-24 00:31:32", 1, -32767, false, 254, NULL, 4.982392323, "你好吗", "sdf",2323)')
        tdSql.execute('insert into tail2 values("2021-10-15 00:31:33", 1, NULL, true, 23, 3.4, 4.982392323, "你好吗", "sdf", 333) ("2021-12-24 00:31:34", 2, 32767, NULL, NULL, NULL, 4.982392323, NULL, "sddd", NULL) ("2022-01-01 08:00:05", 19, 3276, true, 2, 3.323222, 4.92323, "试试", "sddd", 1645434434000)')
        tdSql.execute('insert into tail2 values("2021-10-17 00:31:31", NULL, 32767, true, 123, 3.323232333, 4.2, NULL, NULL, NULL) ("2022-01-01 08:00:06", NULL, NULL, NULL, 35, 3.323232333, NULL, "试试", NULL, 1645434434000) ("2022-01-01 08:00:07", 9, 54, true, 25, 3.32333, NULL, "试试", NULL, 1645434434001)')

        ## test normal table
        tdSql.query('select tail(dnchar,2) from tail2')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2022-01-01 08:00:06")
        tdSql.checkData(0, 1, "试试")
        tdSql.checkData(1, 0, "2022-01-01 08:00:07")
        tdSql.checkData(1, 1, "试试")

        tdSql.query('select tail(dnchar,20) from tail2')
        tdSql.checkRows(6)

        tdSql.query('select tail(dnchar,2,2) from tail2')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2021-12-24 00:31:34")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, "2022-01-01 08:00:05")
        tdSql.checkData(1, 1, "试试")

        tdSql.query('select tail(dnchar,20,2) from tail2')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2021-10-15 00:31:33")
        tdSql.checkData(0, 1, "你好吗")

        tdSql.query('select tail(dnchar,2,20) from tail2')
        tdSql.checkRows(0)
        ## test super table
        tdSql.query('select ts,tail(dtiny,2),tbinary,tint,tbname from stail')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2022-01-01 08:00:07")
        tdSql.checkData(0, 2, 25)
        tdSql.checkData(0, 3, "t2")
        tdSql.checkData(1, 0, "2022-01-24 00:31:32")
        tdSql.checkData(1, 2, 254)
        tdSql.checkData(1, 3, "t1")

        tdSql.query('select ts,tail(dtiny,2,4),tbinary,tint from stail')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2021-10-17 00:31:31")
        tdSql.checkData(0, 2, 253)
        tdSql.checkData(1, 0, "2021-12-24 00:31:34")
        tdSql.checkData(1, 2, None)

        tdSql.query('select ts,tail(dtiny,2,40),tbinary,tint from stail')
        tdSql.checkRows(0)

        tdSql.query('select ts,tail(dtiny,20,4),tbinary,tint from stail')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2021-10-15 00:31:33")
        tdSql.checkData(0, 2, 23)

        tdSql.query('select tail(dbool,2) from stail')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2022-01-01 08:00:07")
        tdSql.checkData(1, 0, "2022-01-24 00:31:32")

        tdSql.query('select tail(dtiny,2) from stail')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2022-01-01 08:00:07")
        tdSql.checkData(1, 0, "2022-01-24 00:31:32")

        tdSql.query('select tail(ddouble,2) from stail')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2022-01-01 08:00:07")
        tdSql.checkData(1, 0, "2022-01-24 00:31:32")

        tdSql.query('select tail(dfloat,2) from stail')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2022-01-01 08:00:07")
        tdSql.checkData(1, 0, "2022-01-24 00:31:32")
        # test group by
        #group by column
        tdSql.query('select tail(dtiny,2) from tail2 group by dnchar')
        tdSql.checkRows(5)
        tdSql.checkData(2, 0, "2021-10-15 00:31:33")
        tdSql.checkData(2, 1, 23)
        tdSql.checkData(1, 0, "2021-12-24 00:31:34")
        tdSql.checkData(1, 1, None)
        tdSql.checkData(4, 0, "2022-01-01 08:00:07")
        tdSql.checkData(4, 1, 25)
        tdSql.query('select tail(dtiny,2,1) from tail2 group by dnchar')
        tdSql.checkRows(4)
        tdSql.query('select tail(dtiny,2) from stail group by dnchar')
        tdSql.checkRows(7)
        tdSql.checkData(0, 0, "2021-10-17 00:31:31")
        tdSql.checkData(0, 1, 123)
        tdSql.checkData(2, 0, "2021-10-17 00:31:31")
        tdSql.checkData(2, 1, 253)
        tdSql.checkData(6, 0, "2022-01-01 08:00:07")
        tdSql.checkData(6, 1, 25)
        tdSql.query('select tail(dtiny,2,1) from stail group by dnchar')
        tdSql.checkRows(5)

        #group by tbname
        tdSql.query('select tail(dfloat,2) from stail group by tbname')
        tdSql.checkRows(4)
        tdSql.checkData(1, 0, "2022-01-24 00:31:32")
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 0, "2022-01-01 08:00:06")
        tdSql.checkData(3, 1, 3.32333)
        #group by tag
        tdSql.query('select tail(dfloat,2) from stail group by tbinary')
        tdSql.checkRows(4)
        tdSql.checkData(1, 0, "2022-01-24 00:31:32")
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 0, "2022-01-01 08:00:06")
        tdSql.checkData(3, 1, 3.32333)
        tdSql.checkData(3, 2, "t2")

        #group by ts
        tdSql.query('select tail(dfloat, 2) from stail group by ts')
        tdSql.checkRows(8)

        #group by tag,column
        tdSql.query('select tail(dfloat, 3) from stail group by tbinary,dnchar')
        tdSql.checkRows(8)

        #test order by
        #order by ts [desc]
        tdSql.query('select tail(dbool, 3) from stail order by ts')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2022-01-01 08:00:06")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, "2022-01-01 08:00:07")
        tdSql.checkData(1, 1, True)
        tdSql.query('select tail(dbool, 3) from stail order by ts desc')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2022-01-24 00:31:32")
        tdSql.checkData(0, 1, False)
        tdSql.checkData(1, 0, "2022-01-01 08:00:07")
        tdSql.checkData(1, 1, True)


        #order by column [desc]
        tdSql.query('select tail(dtiny, 3) from stail order by dtiny desc')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2022-01-24 00:31:32")
        tdSql.checkData(0, 1, 254)
        tdSql.checkData(1, 0, "2022-01-01 08:00:06")
        tdSql.checkData(1, 1, 35)
        tdSql.checkData(2, 0, "2022-01-01 08:00:07")
        tdSql.checkData(2, 1, 25)

        #order by tag [desc]
        tdSql.query('select tail(dtiny, 3) from stail group by tbinary order by tbinary desc')
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 1, 35)
        tdSql.checkData(2, 1, 25)
        tdSql.checkData(3, 1, 253)
        tdSql.checkData(4, 1, 254)
        #order by tag, ts [desc]
        tdSql.query('select tail(dtiny, 3) from stail group by tint order by tint,ts desc')
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 254)
        tdSql.checkData(1, 1, 253)
        tdSql.checkData(2, 1, 25)
        tdSql.checkData(3, 1, 35)
        tdSql.checkData(4, 1, 2)
        tdSql.checkData(0, 0, "2022-01-24 00:31:32")
        tdSql.checkData(1, 0, "2021-10-17 00:31:31")
        tdSql.checkData(2, 0, "2022-01-01 08:00:07")
        tdSql.checkData(3, 0, "2022-01-01 08:00:06")
        tdSql.checkData(4, 0, "2022-01-01 08:00:05")


        #test group by xx order by xx
        tdSql.query('select tail(dbinary, 3) from stail group by dbig order by ts desc')
        tdSql.checkRows(8)
        tdSql.checkData(7, 0, "2022-01-01 08:00:05")
        tdSql.checkData(7, 1, "sddd")
        tdSql.checkData(1, 0, "2021-10-17 00:31:31")
        tdSql.checkData(1, 1, None)
        tdSql.checkData(3, 0, "2021-10-17 00:31:31")
        tdSql.checkData(3, 1, "sddd")

        # error
        tdSql.error("select tail(ts) from stail")
        tdSql.error("select tail(ts,0) from stail")
        tdSql.error("select tail(ts,101) from stail")
        tdSql.error("select tail(ts,1,101) from stail")
        tdSql.query("select tail(dbool,1,0) from stail")
        tdSql.error("select tail(ts,1,-1) from stail")
        tdSql.error("select tail(dtiny,2),top(dtiny,1) from stail")
        tdSql.error("select tail(dtiny,2),first(dtiny) from stail")
        tdSql.error("select tail(dnchar,2) from (select dnchar from stail)")

        #interval
        tdSql.query('select tail(dtiny,2) from tail2 interval(1s)')
        tdSql.checkRows(6)
        tdSql.checkData(1, 0, "2021-10-17 00:31:31")
        tdSql.checkData(1, 1, 123)
        tdSql.checkData(3, 0, "2022-01-01 08:00:05")
        tdSql.checkData(3, 1, 2)

        tdSql.query('select tail(dtiny,2) from tail2 interval(1w)')
        tdSql.checkRows(5)
        tdSql.query('select tail(dtiny,2) from tail2 interval(5s) sliding(1s)')
        tdSql.checkRows(27)

        tdSql.query('select tail(dtiny,2,1) from tail2 interval(1w)')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2021-10-15 00:31:33")
        tdSql.checkData(0, 1, 23)
        tdSql.checkData(1, 0, "2022-01-01 08:00:05")
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 0, "2022-01-01 08:00:06")
        tdSql.checkData(2, 1, 35)

        tdSql.query('select tail(dnchar,2) from stail interval(1w)')
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, "2021-10-17 00:31:31")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(2, 0, "2021-12-24 00:31:34")
        tdSql.checkData(2, 1, None)
        tdSql.checkData(5, 0, "2022-01-24 00:31:32")
        tdSql.checkData(5, 1, "你好吗")

        tdSql.query('select tail(dnchar,2,1) from stail interval(1w)')
        tdSql.checkRows(4)
        tdSql.query('select tail(dnchar,2) from stail interval(1w) sliding(5d)')
        tdSql.checkRows(9)

        #state_window
        tdSql.query('select tail(dtiny,1) from tail2 state_window(dbool)')
        tdSql.checkRows(5)

        #session
        tdSql.query('select tail(dtiny,1) from tail2 session(ts,1w)')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2021-10-17 00:31:31")
        tdSql.checkData(0, 1, 123)
        tdSql.checkData(1, 0, "2021-12-24 00:31:34")
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 0, "2022-01-01 08:00:07")
        tdSql.checkData(2, 1, 25)

        #where
        tdSql.query('select tail(dnchar,2,1) from stail where dtiny >2')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2022-01-01 08:00:06")
        tdSql.checkData(0, 1, "试试")
        tdSql.query('select tail(dnchar,2,1) from stail where ts < "2022-01-01 08:00:07"')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2021-12-24 00:31:34")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, "2022-01-01 08:00:05")
        tdSql.checkData(1, 1, "试试")

        #slimit/soffset
        tdSql.query('select tail(dfloat,2) from stail group by dtiny slimit 2 soffset 1')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2021-10-15 00:31:33")
        tdSql.checkData(0, 1, 3.4)
        tdSql.checkData(1, 0, "2022-01-01 08:00:07")
        tdSql.checkData(1, 1, 3.32333)

        #limit/offset
        tdSql.query('select tail(dnchar,2) from stail where ts < "2022-01-01 08:00:07" limit 1,29')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2022-01-01 08:00:06")
        tdSql.checkData(0, 1, "试试")

        #having
        tdSql.query('select tail(dfloat,2) from stail group by tbinary having tail(dfloat,2) > 3.32324')
        tdSql.checkRows(2)

        #subquery
        tdSql.query('select tail(dnchar,2) from (select ts,dnchar from stail)')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2022-01-01 08:00:07")
        tdSql.checkData(0, 1, "试试")
        tdSql.checkData(1, 0, "2022-01-24 00:31:32")
        tdSql.checkData(1, 1, "你好吗")
        tdSql.query('select tail(dnchar,2,1) from (select tail(dnchar,4) as dnchar from stail)')
        tdSql.checkRows(2)
        tdSql.checkData(1, 0, "2022-01-01 08:00:07")
        tdSql.checkData(1, 1, "试试")

        tdSql.query('select tail(dbig, 3) from (select * from stail) order by ts')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2022-01-01 08:00:06")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, "2022-01-01 08:00:07")
        tdSql.checkData(1, 1, 9)

        tdSql.query('select tail(dbig, 3) from (select * from stail) order by ts desc')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2022-01-24 00:31:32")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, "2022-01-01 08:00:07")
        tdSql.checkData(1, 1, 9)

        #union
        tdSql.query('select tail(dtiny,2) from tail1 union all select tail(dtiny,2) from tail2')
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 253)
        tdSql.checkData(3, 1, 25)

        #join
        tdSql.execute('create table stail1 (ts timestamp, dbig bigint, dsmall smallint, dbool bool, dtiny tinyint unsigned, dfloat float, ddouble double, dnchar nchar(4093), dbinary binary(64), dtime timestamp) tags (tbinary nchar(4093), tint int)')
        tdSql.execute('create table tail11 using stail1 tags ("t1", 1)')

        tdSql.execute('insert into tail11 values("2021-10-17 00:31:31", 1, -3276, true, 253, 3.32333, 4.984392323, "你好", "sddd", 333) ("2022-01-24 00:31:32", 1, -32767, false, 254, NULL, 4.982392323, "你好吗", "sdf",2323)')

        tdSql.query('select tail(stail.dbool, 1) from stail, stail1 where stail.ts=stail1.ts and stail.tint=stail1.tint')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2022-01-24 00:31:32")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())

