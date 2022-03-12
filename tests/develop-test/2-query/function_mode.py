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
        case1<markwang>: [TD-10987] function mode
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self._conn = conn

    def run(self):
        print("running {}".format(__file__))
        tdSql.execute("drop database if exists mode")
        tdSql.execute("create database if not exists mode")
        tdSql.execute('use mode')

        tdSql.execute('create table smode (ts timestamp, voltage bigint, num smallint, dbool bool, dtiny tinyint unsigned, dfloat float, ddouble double, dchar nchar(4093), dbinary binary(64), dtime timestamp) tags (location binary(30), groupid int)')
        tdSql.execute('create table D001 using smode tags ("Beijing.Chaoyang", 1)')
        tdSql.execute('create table D002 using smode tags ("Beijing.haidian", 2)')
        tdSql.execute('create table D003 using smode tags ("Beijing.Daxing", 3)')

        tdSql.execute('insert into D001 values("2021-10-17 00:31:31", 1, -3276, true, NULL, 3.32333, 4.984392323, "你好", "sddd", 333) ("2022-01-24 00:31:31", 1, -32767, false, NULL, NULL, 4.982392323, "你好吗", "sdf",2323)')
        tdSql.execute('insert into D002 values("2021-11-17 00:31:31", 1, NULL, true, NULL, 3.4, 4.982392323, "你好吗", "sdf", 333) ("2021-12-24 00:31:31", 2, 32767, NULL, NULL, NULL, 4.982392323, NULL, "sddd", NULL) ("2022-01-01 08:00:01", 19, 3276, true, NULL, 3.323222, 4.92323, "试试", "sddd", 1645434434000)')

        # error
        tdSql.error("select mode(ts) from smode")
        tdSql.error("select ts, mode(voltage) from smode")
        tdSql.error("select mode(voltage),groupid from smode")
        tdSql.error("select mode(voltage),top(voltage,1) from smode")
        tdSql.error("select mode(voltage),ceil(voltage) from smode")

        # test normal table
        tdSql.query('select mode(voltage),sum(voltage) from d001')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 2)
        tdSql.query('select mode(num) from d001')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)
        tdSql.query('select mode(dbool) from d001')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)
        tdSql.query('select mode(dtiny) from d001')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)
        tdSql.query('select mode(dfloat) from d001')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3.32333)
        tdSql.query('select mode(ddouble) from d002')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4.982392323)
        tdSql.query('select mode(dbinary) from d002')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "sddd")
        tdSql.query('select mode(dtiny) from d003')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)
        tdSql.query('select mode(dbinary) from d003')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)


        # test super table
        tdSql.query('select mode(voltage),min(dfloat) from smode')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.query('select mode(num),mode(voltage) from smode')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, 1)
        tdSql.query('select mode(dbool) from smode')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, True)
        tdSql.query('select mode(dtiny) from smode')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)
        tdSql.query('select mode(dfloat) from smode')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)
        tdSql.query('select mode(ddouble) from smode')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4.982392323)
        tdSql.query('select mode(dbinary) from smode')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "sddd")
        tdSql.query('select mode(dchar) from smode')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "你好吗")

        # test group by
        #group by column
        tdSql.query('select mode(num) from d002 group by dbinary')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.execute('insert into D002 values("2021-11-17 20:31:31", 1, 3276, true, NULL, 3.32322, 4.982392323, "你好吗", "sdf", 333)')
        tdSql.query('select mode(num) from d002 group by dbinary')
        tdSql.checkRows(2)
        tdSql.checkData(1, 0, 3276)
        tdSql.checkData(0, 0, None)
        tdSql.query('select mode(dfloat) from d002 group by dbinary')
        tdSql.checkRows(2)
        tdSql.checkData(1, 0, None)
        tdSql.query('select mode(dchar) from d002 group by dbinary')
        tdSql.checkRows(2)
        tdSql.checkData(1, 0, "你好吗")
        tdSql.checkData(0, 0, "试试")
        tdSql.query('select mode(dchar) from smode group by dchar')
        tdSql.checkRows(4)
        tdSql.query('select mode(dbool) from smode group by dchar')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, True)
        tdSql.checkData(2, 0, True)
        tdSql.checkData(3, 0, True)

        #group by tbname
        tdSql.query('select mode(dchar) from smode group by tbname')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, "d001")
        tdSql.checkData(1, 0, "你好吗")
        tdSql.checkData(1, 1, "d002")

        #group by tag
        tdSql.query('select mode(ddouble) from smode group by location order by location desc')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 4.982392323)
        tdSql.checkData(0, 1, "Beijing.haidian")
        tdSql.checkData(1, 0, None)
        tdSql.checkData(1, 1, "Beijing.Chaoyang")

        #group by tag,column
        tdSql.query('select mode(ddouble) from smode group by location,dchar')
        tdSql.checkRows(5)


        #test order by
        #order by tag [desc]
        tdSql.query('select mode(voltage) from smode group by groupid order by groupid desc')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 1, 1)
        #order by tbname [desc]
        tdSql.query('select mode(dchar) from smode group by tbname order by tbname desc')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "你好吗")
        tdSql.checkData(0, 1, "d002")
        tdSql.checkData(1, 0, None)
        tdSql.checkData(1, 1, "d001")

        #where
        tdSql.query('select mode(voltage) from smode where voltage > 9')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 19)
        tdSql.query('select mode(voltage) from smode where num > 9')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        #interval
        tdSql.query('select mode(voltage) from smode interval(1s)')
        tdSql.checkRows(6)
        tdSql.query('select mode(voltage) from smode interval(1y)')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2021-01-01 00:00:00")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, "2022-01-01 00:00:00")
        tdSql.checkData(1, 1, None)
        tdSql.query('select mode(voltage) from smode interval(1n)')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2021-10-01 00:00:00")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, "2021-11-01 00:00:00")
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 0, "2021-12-01 00:00:00")
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(3, 0, "2022-01-01 00:00:00")
        tdSql.checkData(3, 1, None)

        tdSql.query('select mode(voltage) from smode where ts > "2021-09-01 00:00:00" and ts <"2022-02-02 00:00:00" interval(1n) fill(prev)')
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, "2021-09-01 00:00:00")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(3, 0, "2021-12-01 00:00:00")
        tdSql.checkData(3, 1, 2)
        tdSql.checkData(5, 0, "2022-02-01 00:00:00")
        tdSql.checkData(5, 1, 2)

        #session
        tdSql.query('select mode(voltage) from d002 session(ts,1w)')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2021-11-17 00:31:31")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, "2021-12-24 00:31:31")
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 0, "2022-01-01 08:00:01")
        tdSql.checkData(2, 1, 19)

        #state_window
        tdSql.query('select mode(dfloat) from d002 state_window(voltage)')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)

        #slimit/soffset
        tdSql.query('select mode(dchar) from smode group by tbname slimit 2 soffset 1')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "你好吗")
        tdSql.checkData(0, 1, "d002")

        #limit/offset
        tdSql.query('select mode(ddouble) from smode where voltage > 1 limit 2 offset 0')
        tdSql.checkRows(1)

        #having
        tdSql.query('select mode(ddouble) from smode group by location having mode(ddouble)>3')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4.982392323)
        tdSql.checkData(0, 1, "Beijing.haidian")

        #subquery
        tdSql.query('select mode(ddouble) from (select * from smode where voltage = 1)')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4.982392323)

        #union
        tdSql.query('select mode(voltage) from d001 union all select mode(voltage) from d002')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)

        #join
        tdSql.execute('create table smode1 (ts timestamp, voltage bigint, num smallint, dbool bool, dtiny tinyint unsigned, dfloat float, ddouble double, dchar nchar(4093), dbinary binary(64), dtime timestamp) tags (location binary(30), groupid int)')
        tdSql.execute('create table D011 using smode1 tags ("Beijing.Chaoyang", 1)')

        tdSql.execute('insert into D011 values("2021-10-17 00:31:31", 1, -3276, true, NULL, 3.32333, 4.984392323, "你好", "sddd", 333) ("2022-01-24 00:31:31", 1, -32767, false, NULL, NULL, 4.982392323, "你好吗", "sdf",2323)')
        tdSql.query('select mode(smode.voltage) from smode,smode1 where smode.ts=smode1.ts and smode.groupid=smode1.groupid')
        tdSql.checkRows(1)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())

