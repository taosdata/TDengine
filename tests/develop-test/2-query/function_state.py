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
        case1<markwang>: [TD-11210] function stateCount stateDuration
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self._conn = conn

    def run(self):
        print("running {}".format(__file__))
        tdSql.execute("drop database if exists statef")
        tdSql.execute("create database if not exists statef PRECISION 'ns'")
        tdSql.execute('use statef')

        tdSql.execute('create table sstatef (ts timestamp, dbig bigint, dsmall smallint, dbool bool, dtiny tinyint unsigned, dfloat float, ddouble double, dnchar nchar(4093), dbinary binary(64), dtime timestamp) tags (tbinary nchar(4093), tint int)')
        tdSql.execute('create table statef1 using sstatef tags ("t1", 1)')
        tdSql.execute('create table statef2 using sstatef tags ("t2", 2)')

        tdSql.execute('insert into statef1 values("2021-10-17 00:31:31", 1, -3276, true, 253, 3.32333, 4.984392323, "你好", "sddd", 333) ("2022-01-24 00:31:32", 1, -32767, false, 254, NULL, 4.982392323, "你好吗", "sdf",2323)')
        tdSql.execute('insert into statef2 values("2021-10-15 00:31:33", 1, NULL, true, 23, 3.4, 4.982392323, "你好吗", "sdf", 333) ("2021-12-24 00:31:34", 2, 32767, NULL, NULL, NULL, 4.982392323, NULL, "sddd", NULL) ("2022-01-01 08:00:05", 19, 3276, true, 2, 3.323222, 4.92323, "试试", "sddd", 1645434434000)')
        tdSql.execute('insert into statef2 values("2021-10-17 00:31:31", NULL, 32767, true, 123, 3.323232333, 4.2, NULL, NULL, NULL) ("2022-01-01 08:00:06", NULL, NULL, NULL, 35, 3.323232333, NULL, "试试", NULL, 1645434434000) ("2022-01-01 08:00:07", 9, 54, true, 25, 3.32333, NULL, "试试", NULL, 1645434434001)')

        # error
        tdSql.error("select stateCount(ts,LE,4.923230000) from statef2")
        tdSql.error("select stateCount(dbool,LE,4.923230000) from statef2")
        tdSql.error("select stateCount(dnchar,LE,4.923230000) from statef2")
        tdSql.error("select stateCount(dbinary,LE,4.923230000) from statef2")
        tdSql.error("select stateCount(dtime,LE,4.923230000) from statef2")
        tdSql.error("select stateCount(tint,LE,4.923230000) from statef2")
        tdSql.error("select stateCount(tbinary,LE,4.923230000) from statef2")
        tdSql.error("select stateCount(tbinary,ew,4.923230000) from statef2")
        tdSql.error("select stateCount(tbinary,23,4.923230000) from statef2")
        tdSql.query("select stateCount(dtiny,le,1e3) from statef2")
        tdSql.error("select stateCount(dtiny,le,1e3) from statef")
        tdSql.error("select stateDuration(dtiny,le,1e3) from statef")
        tdSql.query("select stateDuration(dtiny,le,1e3) from statef2")
        tdSql.error("select stateCount(dtiny,le,'1e3') from statef2")
        tdSql.error("select stateCount(dtiny,le,le) from statef2")
        tdSql.error("select stateDuration(dtiny,le,le) from statef2")
        tdSql.error("select stateCount(dtiny,le,2,1s) from statef2")
        tdSql.error("select stateDuration(dtiny,le,2,1) from statef2")
        tdSql.error("select stateDuration(dtiny,le,2,'1s') from statef2")
        tdSql.error("select stateDuration(dtiny,le,2,2s) from statef2")

        tdSql.error("select stateCount(dtiny,le,1e3),top(dtiny,1) from statef2")
        tdSql.error("select stateCount(dtiny,le,1e3),first(dbig) from statef2")
        tdSql.error("select stateCount(dtiny,le,1e3),ceil(dsmall) from statef2")

        #interval
        tdSql.error('select stateCount(dtiny,ne,9.0) from statef2 interval(1s)')
        tdSql.error('select stateDuration(dtiny,ne,9.0,1s) from statef2 interval(1s)')
        #state_window
        tdSql.error('select stateCount(dtiny,ne,9.0) from statef2 state_window(dbool)')
        tdSql.error('select stateDuration(dtiny,ne,9.0,1s) from statef2 state_window(dbool)')
        #session
        tdSql.error('select stateCount(dtiny,ne,9.0) from statef2 session(ts,1w)')
        tdSql.error('select stateDuration(dtiny,ne,9.0,1s) from statef2 session(ts,1w)')

        tdSql.error('select stateDuration(dfloat,Ge,3.32323) from (select dfloat from statef2)')
        tdSql.error('select stateCount(dfloat,Ge,3.32323) from (select dfloat from statef2)')

        ## test normal table
        tdSql.query('select stateCount(dtiny,GT,10) from statef2')
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, "2021-10-15 00:31:33")
        tdSql.checkData(0, 1, 23)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, "2021-10-17 00:31:31")
        tdSql.checkData(1, 1, 123)
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(2, 0, "2021-12-24 00:31:34")
        tdSql.checkData(2, 1, None)
        tdSql.checkData(2, 2, None)
        tdSql.checkData(3, 0, "2022-01-01 08:00:05")
        tdSql.checkData(3, 1, 2)
        tdSql.checkData(3, 2, -1)
        tdSql.checkData(4, 0, "2022-01-01 08:00:06")
        tdSql.checkData(4, 1, 35)
        tdSql.checkData(4, 2, 1)
        tdSql.checkData(5, 0, "2022-01-01 08:00:07")
        tdSql.checkData(5, 1, 25)
        tdSql.checkData(5, 2, 2)

        tdSql.query('select dtiny,ts,stateCount(dtiny,GT,10),*,tbinary from statef2')
        tdSql.checkRows(6)
        tdSql.checkData(0, 1, "2021-10-15 00:31:33")
        tdSql.checkData(1, 2, 123)
        tdSql.checkData(2, 6, 2)
        tdSql.checkData(3, 15, "t2")

        tdSql.query('select stateCount(dtiny,LT,10) from statef2')
        tdSql.checkRows(6)
        tdSql.checkData(0, 2, -1)
        tdSql.checkData(1, 2, -1)
        tdSql.checkData(3, 2, 1)
        tdSql.checkData(4, 2, -1)

        tdSql.query('select stateCount(ddouble,LE,4.923230000) from statef2')
        tdSql.checkRows(6)
        tdSql.checkData(0, 2, -1)
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(2, 2, -1)
        tdSql.checkData(3, 2, 1)
        tdSql.checkData(4, 2, None)
        tdSql.checkData(5, 2, None)

        tdSql.query('select stateCount(dfloat,Ge,3.32323) from statef2')
        tdSql.checkRows(6)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(2, 2, None)
        tdSql.checkData(3, 2, -1)
        tdSql.checkData(4, 2, 1)
        tdSql.checkData(5, 2, 2)

        tdSql.query('select stateCount(dsmall,eq,3276.0) from statef2')
        tdSql.checkRows(6)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 2, -1)
        tdSql.checkData(2, 2, -1)
        tdSql.checkData(3, 2, 1)
        tdSql.checkData(4, 2, None)
        tdSql.checkData(5, 2, -1)

        tdSql.query('select stateCount(dbig,ne,9.0) from statef2')
        tdSql.checkRows(6)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 2, None)
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(3, 2, 3)
        tdSql.checkData(4, 2, None)
        tdSql.checkData(5, 2, -1)

        tdSql.query('select stateDuration(dtiny,ne,9.0) from statef2')
        tdSql.checkRows(6)
        tdSql.checkData(0, 2, 0)
        tdSql.checkData(1, 2, 172798)
        tdSql.checkData(2, 2, None)
        tdSql.checkData(3, 2, 6766112)
        tdSql.checkData(4, 2, 6766113)
        tdSql.checkData(5, 2, 6766114)

        tdSql.query('select stateDuration(dtiny,ne,9.0,1h) from statef2')
        tdSql.checkRows(6)
        tdSql.checkData(0, 2, 0)
        tdSql.checkData(1, 2, 47)
        tdSql.checkData(2, 2, None)
        tdSql.checkData(3, 2, 1879)
        tdSql.checkData(4, 2, 1879)
        tdSql.checkData(5, 2, 1879)

        tdSql.query('select stateDuration(dtiny,ne,9.0,1m) from statef2')
        tdSql.checkRows(6)
        tdSql.checkData(0, 2, 0)
        tdSql.checkData(1, 2, 2879)
        tdSql.checkData(2, 2, None)
        tdSql.checkData(3, 2, 112768)
        tdSql.checkData(4, 2, 112768)
        tdSql.checkData(5, 2, 112768)

        ## test super table
        tdSql.query('select stateDuration(dtiny,ne,9.0,1s) from sstatef group by tbname')
        tdSql.checkRows(8)
        tdSql.checkData(0, 2, 0)
        tdSql.checkData(1, 2, 8553601)

        #where
        tdSql.query('select stateCount(dfloat,Ge,3.32323) from statef2 where dfloat >3.32323')
        tdSql.checkRows(4)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(2, 2, 3)
        tdSql.checkData(3, 2, 4)

        tdSql.query('select stateDuration(dfloat,Ge,3.32323) from statef2 where dfloat <3.4')
        tdSql.checkRows(4)
        tdSql.checkData(0, 2, 0)
        tdSql.checkData(1, 2, -1)
        tdSql.checkData(2, 2, 0)
        tdSql.checkData(3, 2, 1)

        tdSql.query('select stateDuration(dfloat,Ge,3.32323,1m) from statef2 where dfloat <3.4')
        tdSql.checkRows(4)
        tdSql.checkData(3, 2, 0)

        #slimit/soffset
        tdSql.query('select stateDuration(dtiny,ne,9.0,1s) from sstatef group by tbname slimit 2 soffset 1')
        tdSql.checkRows(6)

        #limit/offset
        tdSql.query('select stateCount(dfloat,Ge,3.32323) from statef2 limit 1,2')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2021-10-17 00:31:31")
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(1, 2, None)

        #having
        tdSql.query('select stateDuration(dtiny,ne,9.0,1s) from sstatef group by tbname having stateDuration(dtiny,ne,9.0,1s) > 0')

        #subquery
        tdSql.error('select stateDuration(dfloat,Ge,3.32323) from (select ts,dfloat from statef2)')

        #union
        tdSql.query('select stateCount(dfloat,Ge,3.32323) from statef1 union all select stateCount(dfloat,Ge,3.32323) from statef2')
        tdSql.checkRows(8)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(3, 2, 2)

        #join
        tdSql.execute('create table sstatef1 (ts timestamp, dbig bigint, dsmall smallint, dbool bool, dtiny tinyint unsigned, dfloat float, ddouble double, dnchar nchar(4093), dbinary binary(64), dtime timestamp) tags (tbinary nchar(4093), tint int)')
        tdSql.execute('create table statef11 using sstatef1 tags ("t1", 1)')

        tdSql.execute('insert into statef11 values("2021-10-17 00:31:31", 1, -3276, true, 253, 3.32333, 4.984392323, "你好", "sddd", 333) ("2022-01-24 00:31:32", 1, -32767, false, 254, NULL, 4.982392323, "你好吗", "sdf",2323)')

        tdSql.error('select stateCount(sstatef.dsmall,eq,3276.0) from sstatef, sstatef1 where sstatef.ts=sstatef1.ts and sstatef.tint=sstatef1.tint')

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())

