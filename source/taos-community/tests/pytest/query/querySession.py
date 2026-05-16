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
from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

    def run(self):
        tdSql.prepare()

        print("==============step1")
        tdSql.execute(
            "create table if not exists st (ts timestamp, tagtype int) tags(dev nchar(50), tag2 binary(16))")
        tdSql.execute(
            'CREATE TABLE if not exists dev_001 using st tags("dev_01", "tag_01")')
        tdSql.execute(
            'CREATE TABLE if not exists dev_002 using st tags("dev_02", "tag_02")')

        print("==============step2")

        tdSql.execute(
            """INSERT INTO dev_001 VALUES('2020-05-13 10:00:00.000', 1)('2020-05-13 10:00:00.005', 2)('2020-05-13 10:00:00.011', 3)
            ('2020-05-13 10:00:01.011', 4)('2020-05-13 10:00:01.611', 5)('2020-05-13 10:00:02.612', 6)
            ('2020-05-13 10:01:02.612', 7)('2020-05-13 10:02:02.612', 8)('2020-05-13 10:03:02.613', 9)
            ('2020-05-13 11:00:00.000', 10)('2020-05-13 12:00:00.000', 11)('2020-05-13 13:00:00.001', 12)
            ('2020-05-14 13:00:00.001', 13)('2020-05-15 14:00:00.000', 14)('2020-05-20 10:00:00.000', 15)
            ('2020-05-27 10:00:00.001', 16) dev_002 VALUES('2020-05-13 10:00:00.000', 1)('2020-05-13 10:00:00.005', 2)('2020-05-13 10:00:00.009', 3)('2020-05-13 10:00:00.0021', 4)
            ('2020-05-13 10:00:00.031', 5)('2020-05-13 10:00:00.036', 6)('2020-05-13 10:00:00.51', 7)
            """)

        # session(ts,5a)
        tdSql.query("select count(*) from dev_001 session(ts,5a)")
        tdSql.checkRows(15)
        tdSql.checkData(0, 1, 2)

        # session(ts,5a) main query
        tdSql.query("select count(*) from (select * from dev_001) session(ts,5a)")
        tdSql.checkRows(15)
        tdSql.checkData(0, 1, 2)


        # session(ts,1s)
        tdSql.query("select count(*) from dev_001 session(ts,1s)")
        tdSql.checkRows(12)
        tdSql.checkData(0, 1, 5)

        # session(ts,1s) main query
        tdSql.query("select count(*) from (select * from dev_001) session(ts,1s)")
        tdSql.checkRows(12)
        tdSql.checkData(0, 1, 5)

        tdSql.query("select count(*) from dev_001 session(ts,1000a)")
        tdSql.checkRows(12)
        tdSql.checkData(0, 1, 5)

        tdSql.query("select count(*) from  (select * from dev_001) session(ts,1000a)")
        tdSql.checkRows(12)
        tdSql.checkData(0, 1, 5)

        # session(ts,1m)
        tdSql.query("select count(*) from dev_001 session(ts,1m)")
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 8)

        # session(ts,1m)
        tdSql.query("select count(*) from (select * from dev_001) session(ts,1m)")
        tdSql.checkRows(9)
        tdSql.checkData(0, 1, 8)

        # session(ts,1h)
        tdSql.query("select count(*) from dev_001 session(ts,1h)")
        tdSql.checkRows(6)
        tdSql.checkData(0, 1, 11)

        # session(ts,1h)
        tdSql.query("select count(*) from (select * from dev_001) session(ts,1h)")
        tdSql.checkRows(6)
        tdSql.checkData(0, 1, 11)

        # session(ts,1d)
        tdSql.query("select count(*) from dev_001 session(ts,1d)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 13)

        # session(ts,1d)
        tdSql.query("select count(*) from (select * from dev_001) session(ts,1d)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 13)

        # session(ts,1w)
        tdSql.query("select count(*) from dev_001 session(ts,1w)")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 15)

        # session(ts,1w)
        tdSql.query("select count(*) from (select * from dev_001) session(ts,1w)")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 15)

        # session with where 
        tdSql.query("select count(*),first(tagtype),last(tagtype),avg(tagtype),sum(tagtype),min(tagtype),max(tagtype),leastsquares(tagtype, 1, 1),spread(tagtype),stddev(tagtype),percentile(tagtype,0)  from dev_001 where ts <'2020-05-20 0:0:0' session(ts,1d)")

        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 13)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 13)
        tdSql.checkData(0, 4, 7)
        tdSql.checkData(0, 5, 91)
        tdSql.checkData(0, 6, 1)
        tdSql.checkData(0, 7, 13)
        tdSql.checkData(0, 8, '{slop:1.000000, intercept:0.000000}')
        tdSql.checkData(0, 9, 12)
        tdSql.checkData(0, 10, 3.741657387)
        tdSql.checkData(0, 11, 1)
        tdSql.checkData(1, 11, 14)

        # session with where main

        tdSql.query("select count(*),first(tagtype),last(tagtype),avg(tagtype),sum(tagtype),min(tagtype),max(tagtype),leastsquares(tagtype, 1, 1) from (select * from dev_001 where ts <'2020-05-20 0:0:0') session(ts,1d)")

        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 13)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 13)
        tdSql.checkData(0, 4, 7)
        tdSql.checkData(0, 5, 91)
        tdSql.checkData(0, 6, 1)
        tdSql.checkData(0, 7, 13)
        tdSql.checkData(0, 8, '{slop:1.000000, intercept:0.000000}')

        # tdsql err
        tdSql.error("select * from dev_001 session(ts,1w)")
        tdSql.error("select count(*) from st session(ts,1w)")
        tdSql.error("select count(*) from dev_001 group by tagtype session(ts,1w) ")
        tdSql.error("select count(*) from dev_001 session(ts,1n)")
        tdSql.error("select count(*) from dev_001 session(ts,1y)")
        tdSql.error("select count(*) from dev_001 session(ts,0s)")
        tdSql.error("select count(*) from dev_001 session(i,1y)")
        tdSql.error("select count(*) from dev_001 session(ts,1d) where ts <'2020-05-20 0:0:0'")

        #test precision us 
        tdSql.execute("create database test precision 'us'")
        tdSql.execute("use test")
        tdSql.execute("create table dev_001 (ts timestamp ,i timestamp ,j int)")
        tdSql.execute("insert into dev_001 values(1623046993681000,now,1)(1623046993681001,now+1s,2)(1623046993681002,now+2s,3)(1623046993681004,now+5s,4)")

        # session(ts,1u)
        tdSql.query("select count(*) from dev_001 session(ts,1u)")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 3)
        tdSql.error("select count(*) from dev_001 session(i,1s)")
        # test second timestamp fileds
        tdSql.execute("create table secondts(ts timestamp,t2 timestamp,i int)")
        tdSql.error("select count(*) from secondts session(t2,2s)")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
