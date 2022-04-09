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
        case1<markwang>: [TD-11208] function unique
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self._conn = conn

    def run(self):
        print("running {}".format(__file__))
        tdSql.execute("drop database if exists unique")
        tdSql.execute("create database if not exists unique")
        tdSql.execute('use unique')

        tdSql.execute('create table unique (ts timestamp, voltage bigint,num int) tags (location binary(30), groupid int)')
        tdSql.execute('create table D001 using unique tags ("Beijing.Chaoyang", 1)')
        tdSql.execute('create table D002 using unique tags ("Beijing.haidian", 2)')
        tdSql.execute('create table D003 using unique tags ("Beijing.Tongzhou", 3)')
        tdSql.execute('create table D004 using unique tags ("Beijing.Tongzhou", 4)')

        tdSql.execute('insert into D001 values("2021-10-17 00:31:31", 1, 2) ("2022-01-24 00:31:31", 1, 2)')
        tdSql.execute('insert into D002 values("2021-10-17 00:31:31", 1, 2) ("2021-12-24 00:31:31", 2, 2) ("2021-12-24 01:31:31", 19, 2)')
        tdSql.execute('insert into D003 values("2021-10-17 00:31:31", 1, 2) ("2021-12-24 00:31:31", 1, 2) ("2021-12-24 01:31:31", 9, 2)')

        # test normal table
        print(tdSql.getResult('select unique(voltage) from d003'))
        tdSql.query('select unique(voltage) from d003')
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 9)

        tdSql.query('select ts,unique(voltage),ts,groupid,location,tbname from d003')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2021-10-17 00:31:31")
        tdSql.checkData(0, 1, "2021-10-17 00:31:31")
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, "2021-10-17 00:31:31")
        tdSql.checkData(0, 4, 3)
        tdSql.checkData(0, 5, "Beijing.Tongzhou")
        tdSql.checkData(0, 6, "d003")
        tdSql.checkData(1, 0, "2021-12-24 01:31:31")
        tdSql.checkData(1, 1, "2021-12-24 01:31:31")
        tdSql.checkData(1, 2, 9)
        tdSql.checkData(1, 3, "2021-12-24 01:31:31")
        tdSql.checkData(1, 4, 3)
        tdSql.checkData(1, 5, "Beijing.Tongzhou")
        tdSql.checkData(1, 6, "d003")

        # test super table
        tdSql.query('select unique(voltage),tbname from unique')
        tdSql.checkRows(4)
        tdSql.query('select unique(voltage),_c0 from unique')
        tdSql.checkRows(4)

        tdSql.query('select unique(voltage) from unique')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2021-10-17 00:31:31")
        tdSql.checkData(1, 0, "2021-12-24 00:31:31")
        tdSql.checkData(2, 0, "2021-12-24 01:31:31")
        tdSql.checkData(3, 0, "2021-12-24 01:31:31")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 1, 9)
        tdSql.checkData(3, 1, 19)

        tdSql.query('select ts,unique(voltage),ts,groupid,location,tbname from unique')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2021-10-17 00:31:31")
        tdSql.checkData(0, 1, "2021-10-17 00:31:31")
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, "2021-10-17 00:31:31")
        tdSql.checkData(0, 4, 1)
        tdSql.checkData(0, 5, "Beijing.Chaoyang")
        tdSql.checkData(0, 6, "d001")

        tdSql.checkData(1, 0, "2021-12-24 00:31:31")
        tdSql.checkData(1, 1, "2021-12-24 00:31:31")
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(1, 3, "2021-12-24 00:31:31")
        tdSql.checkData(1, 4, 2)
        tdSql.checkData(1, 5, "Beijing.haidian")
        tdSql.checkData(1, 6, "d002")

        tdSql.checkData(2, 0, "2021-12-24 01:31:31")
        tdSql.checkData(2, 1, "2021-12-24 01:31:31")
        tdSql.checkData(2, 2, 9)
        tdSql.checkData(2, 3, "2021-12-24 01:31:31")
        tdSql.checkData(2, 4, 3)
        tdSql.checkData(2, 5, "Beijing.Tongzhou")
        tdSql.checkData(2, 6, "d003")

        tdSql.checkData(3, 0, "2021-12-24 01:31:31")
        tdSql.checkData(3, 1, "2021-12-24 01:31:31")
        tdSql.checkData(3, 2, 19)
        tdSql.checkData(3, 3, "2021-12-24 01:31:31")
        tdSql.checkData(3, 4, 2)
        tdSql.checkData(3, 5, "Beijing.haidian")
        tdSql.checkData(3, 6, "d002")

        tdSql.execute('insert into D004 values("2021-10-15 00:00:01", 10, 2) ("2021-12-24 00:21:31", 5, 2) ("2021-12-25 01:31:31", 9, 4)')

        # test group by
        #group by column
        tdSql.query('select ts,unique(num) from d003 group by voltage')
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, "2021-10-17 00:31:31")
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(1, 1, "2021-12-24 01:31:31")
        tdSql.checkData(1, 2, 2)
        tdSql.query('select ts,unique(num) from unique group by voltage')
        tdSql.checkRows(7)
        #group by tbname
        tdSql.query('select ts,unique(voltage) from unique group by tbname')
        tdSql.checkRows(9)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(8, 2, 9)
        #group by tag
        tdSql.query('select ts,unique(voltage) from unique group by location')
        tdSql.checkRows(8)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(5, 3, "Beijing.haidian")
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, "Beijing.Chaoyang")
        tdSql.checkData(2, 2, 1)
        tdSql.checkData(2, 3, "Beijing.Tongzhou")
        #group by ts
        tdSql.query('select ts,unique(voltage) from unique group by ts')
        tdSql.checkRows(9)
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(4, 2, 2)
        tdSql.checkData(8, 2, 1)
        tdSql.checkData(6, 2, 19)
        tdSql.checkData(7, 2, 9)
        #group by tag,column
        tdSql.query('select ts,unique(voltage) from unique group by location,num')
        tdSql.checkRows(9)


        #test order by
        #order by ts [desc]
        tdSql.query('select unique(voltage) from unique order by ts desc')
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, "2021-12-24 01:31:31")
        tdSql.checkData(0, 1, 19)
        tdSql.checkData(5, 0, "2021-10-15 00:00:01")
        tdSql.checkData(5, 1, 10)
        tdSql.query('select unique(voltage) from unique order by ts')
        tdSql.checkRows(6)
        tdSql.checkData(5, 0, "2021-12-24 01:31:31")
        tdSql.checkData(5, 1, 19)
        tdSql.checkData(0, 0, "2021-10-15 00:00:01")
        tdSql.checkData(0, 1, 10)
        #order by column [desc]
        tdSql.query('select unique(voltage) from unique order by voltage desc')
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, "2021-12-24 01:31:31")
        tdSql.checkData(0, 1, 19)
        tdSql.checkData(5, 0, "2021-10-17 00:31:31")
        tdSql.checkData(5, 1, 1)
        tdSql.query('select unique(voltage) from unique order by voltage')
        tdSql.checkRows(6)
        tdSql.checkData(5, 0, "2021-12-24 01:31:31")
        tdSql.checkData(5, 1, 19)
        tdSql.checkData(0, 0, "2021-10-17 00:31:31")
        tdSql.checkData(0, 1, 1)
        #order by tag [desc]
        tdSql.query('select unique(voltage) from unique group by groupid order by groupid desc')
        tdSql.checkRows(9)
        tdSql.checkData(2, 1, 9)
        tdSql.checkData(2, 2, 4)
        tdSql.checkData(4, 1, 9)
        tdSql.checkData(4, 2, 3)
        tdSql.checkData(8, 1, 1)
        tdSql.checkData(8, 2, 1)
        #order by tbname [desc]
        tdSql.query('select unique(voltage) from unique group by tbname order by tbname desc')
        tdSql.checkRows(9)
        tdSql.checkData(2, 1, 9)
        tdSql.checkData(2, 2, "d004")
        tdSql.checkData(4, 1, 9)
        tdSql.checkData(4, 2, "d003")
        tdSql.checkData(8, 1, 1)
        tdSql.checkData(8, 2, "d001")
        #order by tag, ts [desc]
        tdSql.query('select unique(voltage) from unique group by groupid order by groupid,ts desc')
        tdSql.checkRows(9)


        #test group by xx order by xx
        tdSql.query('select unique(voltage) from unique group by num order by ts desc')
        tdSql.checkRows(7)
        tdSql.checkData(6, 0, "2021-12-25 01:31:31")
        tdSql.checkData(6, 1, 9)
        tdSql.checkData(0, 0, "2021-12-24 01:31:31")
        tdSql.checkData(0, 1, 19)

        # error
        tdSql.error("select unique(ts) from unique")
        tdSql.error("select unique(voltage) from unique interval(1s)")
        tdSql.error("select unique(voltage) from unique interval(1s) fill(value)")
        tdSql.error("select unique(voltage) from unique session(ts, 20s)")
        tdSql.error("select unique(voltage) from unique state_window(num)")
        tdSql.error("select unique(voltage),top(voltage,1) from unique")
        tdSql.error("select unique(voltage),first(voltage) from unique")
        tdSql.error("select unique(voltage),ceil(voltage) from unique")
        tdSql.error("select unique(voltage),sum(voltage) from unique")
        tdSql.error("select unique(voltage),unique(num) from unique")

        #where
        tdSql.query('select unique(voltage) from unique where voltage > 9')
        tdSql.checkRows(2)
        tdSql.query('select unique(voltage) from unique where num > 9')
        tdSql.checkRows(0)

        #slimit/soffset
        tdSql.query('select ts,unique(voltage) from unique group by tbname slimit 2 soffset 2')
        tdSql.checkRows(5)
        tdSql.checkData(0, 3, "d003")
        tdSql.checkData(2, 3, "d004")

        #limit/offset
        tdSql.query('select unique(voltage) from unique where voltage > 2 limit 2 offset 1')
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(1, 1, 9)

        #having
        tdSql.query('select unique(voltage) from unique group by num having unique(voltage)>5')
        tdSql.checkRows(4)

        #subquery
        tdSql.query('select unique(num) from (select * from unique where voltage > 1)')
        tdSql.checkRows(2)

        tdSql.query('select unique(num) from (select * from unique) order by ts')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2021-10-15 00:00:01")
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 0, "2021-12-25 01:31:31")
        tdSql.checkData(1, 1, 4)

        tdSql.query('select unique(num) from (select * from unique) order by ts desc')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2021-12-25 01:31:31")
        tdSql.checkData(0, 1, 4)
        tdSql.checkData(1, 0, "2021-10-15 00:00:01")
        tdSql.checkData(1, 1, 2)

        #union
        tdSql.query('select unique(voltage) from d002 union all select unique(voltage) from d003')
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(3, 1, 1)

        tdSql.execute('create table unique1 (ts timestamp, voltage bigint,num smallint, dbool bool, dtiny tinyint unsigned, dfloat float, ddouble double, dchar nchar(64), dbinary binary(64), dtime timestamp) tags (jtag json)')
        tdSql.execute('create table D011 using unique1 tags (\'{"k1":"v1"}\')')
        tdSql.execute('create table D012 using unique1 tags (\'{"k1":"v1","k2":7}\')')
        tdSql.execute('create table D013 using unique1 tags (\'{"k3":"v3"}\')')

        tdSql.execute('insert into D011 values("2021-10-17 00:31:31", 1, -3276, true, 253, 3.32333, 4.984392323, "你好", "sddd", 333) ("2022-01-24 00:31:31", 1, -32767, false, 254, NULL, 4.982392323, "你好吗", "sdf",2323)')
        tdSql.execute('insert into D012 values("2021-10-17 00:31:31", 1, NULL, true, 23, 3.4, 4.982392323, "你好吗", "sdf", 333) ("2021-12-24 00:31:31", 2, 32767, NULL, NULL, NULL, 4.982392323, NULL, "sddd", NULL) ("2022-01-01 08:00:01", 19, 3276, true, 2, 3.323222, 4.92323, "试试", "sddd", 1645434434000)')
        tdSql.execute('insert into D013 values("2021-10-17 00:31:31", NULL, 32767, true, 123, 3.323232333, 4.2, NULL, NULL, NULL) ("2022-01-01 08:00:02", NULL, NULL, NULL, 35, 3.323232333, NULL, "试试", NULL, 1645434434000) ("2022-01-01 08:00:03", 9, 54, true, 25, 3.32333, NULL, "试试", NULL, 1645434434001)')

        #null/nchar/binary
        tdSql.query('select unique(dchar),jtag from unique1')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2021-10-17 00:31:31")
        tdSql.checkData(0, 1, "你好")
        tdSql.checkData(2, 0, "2021-10-17 00:31:31")
        tdSql.checkData(2, 1, None)
        tdSql.query('select unique(voltage) from unique1 group by dchar')
        tdSql.checkRows(7)

        tdSql.query('select unique(dbinary) from unique1')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2021-10-17 00:31:31")
        tdSql.checkData(0, 1, "sddd")
        tdSql.checkData(2, 0, "2021-10-17 00:31:31")
        tdSql.checkData(2, 1, None)

        tdSql.query('select unique(dbool) from unique1')
        tdSql.checkRows(3)

        tdSql.query('select unique(dtiny) from unique1')
        tdSql.checkRows(8)

        tdSql.query('select unique(ddouble) from unique1')
        tdSql.checkRows(5)

        tdSql.query('select unique(dfloat) from unique1')
        tdSql.checkRows(5)

        tdSql.query('select unique(dtime) from unique1')
        tdSql.checkRows(5)

        #join
        tdSql.execute('create table unique2 (ts timestamp, voltage bigint,num int) tags (location binary(30), groupid int)')
        tdSql.execute('create table D021 using unique2 tags ("Beijing.Chaoyang", 1)')

        tdSql.execute('insert into D021 values("2021-10-17 00:31:31", 1, 2) ("2022-01-24 00:31:31", 1, 2)')
        tdSql.query('select unique(unique.voltage) from unique, unique2 where unique.ts=unique2.ts and unique.groupid=unique2.groupid')
        tdSql.checkRows(1)

        #TD-14104
        ts = 1642592221000
        sql = "insert into D004 values"
        for i in range(3000):
            sql += " (%d,%d,%d)"%(ts + i*1000, i, i)
        tdSql.execute(sql)
        tdSql.query("select unique(num) from (select * from unique)")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())

