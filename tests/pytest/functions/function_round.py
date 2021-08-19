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
import numpy as np


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

    def run(self):
        tdSql.prepare()
        tdSql.execute("create stable super (ts timestamp, c1 timestamp, c2 int, c3 bigint, c4 float, c5 double, c6 binary(8), c7 smallint, c8 tinyint, c9 bool, c10 nchar(8)) tags (t1 int, t2 bigint, t3 float, t4 double, t5 binary(8), t6 smallint, t7 tinyint, t8 bool, t9 nchar(8))")
        tdSql.execute("create table t1 using super tags (1, 8, 1.0, 1.0, 'abcdefgh', 1, 1, 1, 'abcdeffh')")
        tdSql.execute("insert into t1 values (1537146000000, 1537146000000, 1, 1, 1.0, 1.0, 'abcdefgh',1,1,1,'abcdefgh')")
        tdSql.execute("insert into t1 values (1537146000001, 1537146000000, 1, 1, 1.1, 1.1, 'abcdefgh',1,1,1,'abcdefgh')")
        tdSql.execute("insert into t1 values (1537146000002, 1537146000000, 1, 1, 1.2, 1.2, 'abcdefgh',1,1,1,'abcdefgh')")
        tdSql.execute("insert into t1 values (1537146000003, 1537146000000, 1, 1, 1.3, 1.3, 'abcdefgh',1,1,1,'abcdefgh')")
        tdSql.execute("insert into t1 values (1537146000004, 1537146000000, 1, 1, 1.4, 1.4, 'abcdefgh',1,1,1,'abcdefgh')")
        tdSql.execute("insert into t1 values (1537146000005, 1537146000000, 1, 1, 1.5, 1.5, 'abcdefgh',1,1,1,'abcdefgh')")
        tdSql.execute("insert into t1 values (1537146000006, 1537146000000, 1, 1, 1.6, 1.6, 'abcdefgh',1,1,1,'abcdefgh')")
        tdSql.execute("insert into t1 values (1537146000007, 1537146000000, 1, 1, 1.7, 1.7, 'abcdefgh',1,1,1,'abcdefgh')")
        tdSql.execute("insert into t1 values (1537146000008, 1537146000000, 1, 1, 1.8, 1.8, 'abcdefgh',1,1,1,'abcdefgh')")
        tdSql.execute("insert into t1 values (1537146000009, 1537146000000, 1, 1, 1.9, 1.9, 'abcdefgh',1,1,1,'abcdefgh')")
        tdSql.execute("insert into t1 values (1537146000010, 1537146000000, 1, 1, 1.5444444, 1.544444, 'abcdefgh',1,1,1,'abcdefgh')")
        tdSql.execute("insert into t1 values (1537146000011, 1537146000000, -1, -1, -1.0, -1.0, 'abcdefgh',-1,-1,0,'abcdefgh')")
        tdSql.execute("insert into t1 values (1537146000012, 1537146000000, -1, -1, -1.1, -1.1, 'abcdefgh',-1,-1,0,'abcdefgh')")
        tdSql.execute("insert into t1 values (1537146000013, 1537146000000, -1, -1, -1.2, -1.2, 'abcdefgh',-1,-1,0,'abcdefgh')")
        tdSql.execute("insert into t1 values (1537146000014, 1537146000000, -1, -1, -1.3, -1.3, 'abcdefgh',-1,-1,0,'abcdefgh')")
        tdSql.execute("insert into t1 values (1537146000015, 1537146000000, -1, -1, -1.4, -1.4, 'abcdefgh',-1,-1,0,'abcdefgh')")
        tdSql.execute("insert into t1 values (1537146000016, 1537146000000, -1, -1, -1.5, -1.5, 'abcdefgh',-1,-1,0,'abcdefgh')")
        tdSql.execute("insert into t1 values (1537146000017, 1537146000000, -1, -1, -1.6, -1.6, 'abcdefgh',-1,-1,0,'abcdefgh')")
        tdSql.execute("insert into t1 values (1537146000018, 1537146000000, -1, -1, -1.7, -1.7, 'abcdefgh',-1,-1,0,'abcdefgh')")
        tdSql.execute("insert into t1 values (1537146000019, 1537146000000, -1, -1, -1.8, -1.8, 'abcdefgh',-1,-1,0,'abcdefgh')")
        tdSql.execute("insert into t1 values (1537146000020, 1537146000000, -1, -1, -1.9, -1.9, 'abcdefgh',-1,-1,0,'abcdefgh')")
        tdSql.execute("insert into t1 values (1537146000021, 1537146000000, -1, -1, -1.5444444, -1.544444, 'abcdefgh',-1,-1,0,'abcdefgh')")

        # tags does not support round
        # for stable
        tdSql.error("select round(t1) from db.super")
        tdSql.error("select round(t2) from db.super")
        tdSql.error("select round(t3) from db.super")
        tdSql.error("select round(t4) from db.super")
        tdSql.error("select round(t5) from db.super")
        tdSql.error("select round(t6) from db.super")
        tdSql.error("select round(t7) from db.super")
        tdSql.error("select round(t8) from db.super")
        tdSql.error("select round(t9) from db.super")

        # for table
        tdSql.error("select round(t1) from db.t1")
        tdSql.error("select round(t2) from db.t1")
        tdSql.error("select round(t3) from db.t1")
        tdSql.error("select round(t4) from db.t1")
        tdSql.error("select round(t5) from db.t1")
        tdSql.error("select round(t6) from db.t1")
        tdSql.error("select round(t7) from db.t1")
        tdSql.error("select round(t8) from db.t1")
        tdSql.error("select round(t9) from db.t1")


        # check support columns
        # for stable
        tdSql.error("select round(ts) from db.super")
        tdSql.error("select round(c1) from db.super")
        tdSql.query("select round(c2) from db.super")
        tdSql.checkRows(22)
        for i in range(22):
            if i < 11:
                tdSql.checkData(i,0,1)
            else:
                tdSql.checkData(i,0,-1)
        tdSql.query("select round(c3) from db.super")
        tdSql.checkRows(22)
        for i in range(22):
            if i < 11:
                tdSql.checkData(i,0,1)
            else:
                tdSql.checkData(i,0,-1)
        tdSql.query("select round(c4) from db.super")
        tdSql.checkRows(22)
        for i in range(22):
            if 0 <= i <= 4:
                tdSql.checkData(i,0,1)
            if 4 < i < 11:
                tdSql.checkData(i,0,2)
            if 11 <= i <= 15:
                tdSql.checkData(i,0,-1)
            if i > 15:
                tdSql.checkData(i,0,-2)
        tdSql.query("select round(c5) from db.super")
        tdSql.checkRows(22)
        for i in range(22):
            if 0 <= i <= 4:
                tdSql.checkData(i,0,1)
            if 4 < i < 11:
                tdSql.checkData(i,0,2)
            if 11 <= i <= 15:
                tdSql.checkData(i,0,-1)
            if i > 15:
                tdSql.checkData(i,0,-2)
        tdSql.error("select round(c6) from db.super")
        tdSql.query("select round(c7) from db.super")
        tdSql.checkRows(22)
        for i in range(22):
            if i < 11:
                tdSql.checkData(i,0,1)
            else:
                tdSql.checkData(i,0,-1)
        tdSql.query("select round(c8) from db.super")
        tdSql.checkRows(22)
        for i in range(22):
            if i < 11:
                tdSql.checkData(i,0,1)
            else:
                tdSql.checkData(i,0,-1)
        tdSql.error("select round(c9) from db.super")
        tdSql.error("select round(c10) from db.super")

        # for table
        tdSql.error("select round(ts) from db.t1")
        tdSql.error("select round(c1) from db.t1")
        tdSql.query("select round(c2) from db.t1")
        tdSql.checkRows(22)
        for i in range(22):
            if i < 11:
                tdSql.checkData(i,0,1)
            else:
                tdSql.checkData(i,0,-1)
        tdSql.query("select round(c3) from db.t1")
        tdSql.checkRows(22)
        for i in range(22):
            if i < 11:
                tdSql.checkData(i,0,1)
            else:
                tdSql.checkData(i,0,-1)
        tdSql.query("select round(c4) from db.t1")
        tdSql.checkRows(22)
        for i in range(22):
            if 0 <= i <= 4:
                tdSql.checkData(i,0,1)
            if 4 < i < 11:
                tdSql.checkData(i,0,2)
            if 11 <= i <= 15:
                tdSql.checkData(i,0,-1)
            if i > 15:
                tdSql.checkData(i,0,-2)
        tdSql.query("select round(c5) from db.t1")
        tdSql.checkRows(22)
        for i in range(22):
            if 0 <= i <= 4:
                tdSql.checkData(i,0,1)
            if 4 < i < 11:
                tdSql.checkData(i,0,2)
            if 11 <= i <= 15:
                tdSql.checkData(i,0,-1)
            if i > 15:
                tdSql.checkData(i,0,-2)
        tdSql.error("select round(c6) from db.t1")
        tdSql.query("select round(c7) from db.t1")
        tdSql.checkRows(22)
        for i in range(22):
            if i < 11:
                tdSql.checkData(i,0,1)
            else:
                tdSql.checkData(i,0,-1)
        tdSql.query("select round(c8) from db.t1")
        tdSql.checkRows(22)
        for i in range(22):
            if i < 11:
                tdSql.checkData(i,0,1)
            else:
                tdSql.checkData(i,0,-1)
        tdSql.error("select round(c9) from db.t1")
        tdSql.error("select round(c10) from db.t1")

        # does not support aggregation
        # for super table
        tdSql.error("select max(round(c2)) from db.super")
        tdSql.error("select max(round(c3)) from db.super")
        tdSql.error("select max(round(c4)) from db.super")
        tdSql.error("select max(round(c5)) from db.super")
        tdSql.error("select max(round(c7)) from db.super")
        tdSql.error("select max(round(c8)) from db.super")

        tdSql.error("select round(max(c2)) from db.super")
        tdSql.error("select round(max(c3)) from db.super")
        tdSql.error("select round(max(c4)) from db.super")
        tdSql.error("select round(max(c5)) from db.super")
        tdSql.error("select round(max(c7)) from db.super")
        tdSql.error("select round(max(c8)) from db.super")

        tdSql.error("select min(round(c2)) from db.super")
        tdSql.error("select min(round(c3)) from db.super")
        tdSql.error("select min(round(c4)) from db.super")
        tdSql.error("select min(round(c5)) from db.super")
        tdSql.error("select min(round(c7)) from db.super")
        tdSql.error("select min(round(c8)) from db.super")

        tdSql.error("select round(min(c2)) from db.super")
        tdSql.error("select round(min(c3)) from db.super")
        tdSql.error("select round(min(c4)) from db.super")
        tdSql.error("select round(min(c5)) from db.super")
        tdSql.error("select round(min(c7)) from db.super")
        tdSql.error("select round(min(c8)) from db.super")

        tdSql.error("select avg(round(c2)) from db.super")
        tdSql.error("select avg(round(c3)) from db.super")
        tdSql.error("select avg(round(c4)) from db.super")
        tdSql.error("select avg(round(c5)) from db.super")
        tdSql.error("select avg(round(c7)) from db.super")
        tdSql.error("select avg(round(c8)) from db.super")

        tdSql.error("select round(avg(c2)) from db.super")
        tdSql.error("select round(avg(c3)) from db.super")
        tdSql.error("select round(avg(c4)) from db.super")
        tdSql.error("select round(avg(c5)) from db.super")
        tdSql.error("select round(avg(c7)) from db.super")
        tdSql.error("select round(avg(c8)) from db.super")

        tdSql.error("select last(round(c2)) from db.super")
        tdSql.error("select last(round(c3)) from db.super")
        tdSql.error("select last(round(c4)) from db.super")
        tdSql.error("select last(round(c5)) from db.super")
        tdSql.error("select last(round(c7)) from db.super")
        tdSql.error("select last(round(c8)) from db.super")

        tdSql.error("select round(last(c2)) from db.super")
        tdSql.error("select round(last(c3)) from db.super")
        tdSql.error("select round(last(c4)) from db.super")
        tdSql.error("select round(last(c5)) from db.super")
        tdSql.error("select round(last(c7)) from db.super")
        tdSql.error("select round(last(c8)) from db.super")
        
        tdSql.error("select last_row(round(c2)) from db.super")
        tdSql.error("select last_row(round(c3)) from db.super")
        tdSql.error("select last_row(round(c4)) from db.super")
        tdSql.error("select last_row(round(c5)) from db.super")
        tdSql.error("select last_row(round(c7)) from db.super")
        tdSql.error("select last_row(round(c8)) from db.super")

        tdSql.error("select round(last_row(c2)) from db.super")
        tdSql.error("select round(last_row(c3)) from db.super")
        tdSql.error("select round(last_row(c4)) from db.super")
        tdSql.error("select round(last_row(c5)) from db.super")
        tdSql.error("select round(last_row(c7)) from db.super")
        tdSql.error("select round(last_row(c8)) from db.super")

        tdSql.error("select first(round(c2)) from db.super")
        tdSql.error("select first(round(c3)) from db.super")
        tdSql.error("select first(round(c4)) from db.super")
        tdSql.error("select first(round(c5)) from db.super")
        tdSql.error("select first(round(c7)) from db.super")
        tdSql.error("select first(round(c8)) from db.super")

        tdSql.error("select round(first(c2)) from db.super")
        tdSql.error("select round(first(c3)) from db.super")
        tdSql.error("select round(first(c4)) from db.super")
        tdSql.error("select round(first(c5)) from db.super")
        tdSql.error("select round(first(c7)) from db.super")
        tdSql.error("select round(first(c8)) from db.super")

        tdSql.error("select diff(round(c2)) from db.super")
        tdSql.error("select diff(round(c3)) from db.super")
        tdSql.error("select diff(round(c4)) from db.super")
        tdSql.error("select diff(round(c5)) from db.super")
        tdSql.error("select diff(round(c7)) from db.super")
        tdSql.error("select diff(round(c8)) from db.super")

        tdSql.error("select round(diff(c2)) from db.super")
        tdSql.error("select round(diff(c3)) from db.super")
        tdSql.error("select round(diff(c4)) from db.super")
        tdSql.error("select round(diff(c5)) from db.super")
        tdSql.error("select round(diff(c7)) from db.super")
        tdSql.error("select round(diff(c8)) from db.super")

        tdSql.error("select percentile(round(c2), 0) from db.super")
        tdSql.error("select percentile(round(c3), 0) from db.super")
        tdSql.error("select percentile(round(c4), 0) from db.super")
        tdSql.error("select percentile(round(c5), 0) from db.super")
        tdSql.error("select percentile(round(c7), 0) from db.super")
        tdSql.error("select percentile(round(c8), 0) from db.super")

        tdSql.error("select round(percentile(c2, 0)) from db.super")
        tdSql.error("select round(percentile(c3, 0)) from db.super")
        tdSql.error("select round(percentile(c4, 0)) from db.super")
        tdSql.error("select round(percentile(c5, 0)) from db.super")
        tdSql.error("select round(percentile(c7, 0)) from db.super")
        tdSql.error("select round(percentile(c8, 0)) from db.super")

        tdSql.error("select derivate(round(c2),1s, 1) from db.super")
        tdSql.error("select derivate(round(c3),1s, 1) from db.super")
        tdSql.error("select derivate(round(c4),1s, 1) from db.super")
        tdSql.error("select derivate(round(c5),1s, 1) from db.super")
        tdSql.error("select derivate(round(c7),1s, 1) from db.super")
        tdSql.error("select derivate(round(c8),1s, 1) from db.super")

        tdSql.error("select round(derivate(c2,1s, 1)) from db.super")
        tdSql.error("select round(derivate(c3,1s, 1)) from db.super")
        tdSql.error("select round(derivate(c4,1s, 1)) from db.super")
        tdSql.error("select round(derivate(c5,1s, 1)) from db.super")
        tdSql.error("select round(derivate(c7,1s, 1)) from db.super")
        tdSql.error("select round(derivate(c8,1s, 1)) from db.super")

        tdSql.error("select leastsquares(round(c2),1, 1) from db.super")
        tdSql.error("select leastsquares(round(c3),1, 1) from db.super")
        tdSql.error("select leastsquares(round(c4),1, 1) from db.super")
        tdSql.error("select leastsquares(round(c5),1, 1) from db.super")
        tdSql.error("select leastsquares(round(c7),1, 1) from db.super")
        tdSql.error("select leastsquares(round(c8),1, 1) from db.super")

        tdSql.error("select round(leastsquares(c2,1, 1)) from db.super")
        tdSql.error("select round(leastsquares(c3,1, 1)) from db.super")
        tdSql.error("select round(leastsquares(c4,1, 1)) from db.super")
        tdSql.error("select round(leastsquares(c5,1, 1)) from db.super")
        tdSql.error("select round(leastsquares(c7,1, 1)) from db.super")
        tdSql.error("select round(leastsquares(c8,1, 1)) from db.super")

        tdSql.error("select count(round(c2)) from db.super")
        tdSql.error("select count(round(c3)) from db.super")
        tdSql.error("select count(round(c4)) from db.super")
        tdSql.error("select count(round(c5)) from db.super")
        tdSql.error("select count(round(c7)) from db.super")
        tdSql.error("select count(round(c8)) from db.super")

        tdSql.error("select round(count(c2)) from db.super")
        tdSql.error("select round(count(c3)) from db.super")
        tdSql.error("select round(count(c4)) from db.super")
        tdSql.error("select round(count(c5)) from db.super")
        tdSql.error("select round(count(c7)) from db.super")
        tdSql.error("select round(count(c8)) from db.super")

        tdSql.error("select twa(round(c2)) from db.super where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select twa(round(c3)) from db.super where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select twa(round(c4)) from db.super where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select twa(round(c5)) from db.super where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select twa(round(c7)) from db.super where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select twa(round(c8)) from db.super where ts <= 1537146000021 and ts >= 1537146000000")

        tdSql.error("select round(twa(c2)) from db.super where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select round(twa(c3)) from db.super where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select round(twa(c4)) from db.super where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select round(twa(c5)) from db.super where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select round(twa(c7)) from db.super where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select round(twa(c8)) from db.super where ts <= 1537146000021 and ts >= 1537146000000")

        tdSql.error("select stddev(round(c2)) from db.super")
        tdSql.error("select stddev(round(c3)) from db.super")
        tdSql.error("select stddev(round(c4)) from db.super")
        tdSql.error("select stddev(round(c5)) from db.super")
        tdSql.error("select stddev(round(c7)) from db.super")
        tdSql.error("select stddev(round(c8)) from db.super")

        tdSql.error("select round(stddev(c2)) from db.super")
        tdSql.error("select round(stddev(c3)) from db.super")
        tdSql.error("select round(stddev(c4)) from db.super")
        tdSql.error("select round(stddev(c5)) from db.super")
        tdSql.error("select round(stddev(c7)) from db.super")
        tdSql.error("select round(stddev(c8)) from db.super")

        tdSql.error("select spread(round(c2)) from db.super")
        tdSql.error("select spread(round(c3)) from db.super")
        tdSql.error("select spread(round(c4)) from db.super")
        tdSql.error("select spread(round(c5)) from db.super")
        tdSql.error("select spread(round(c7)) from db.super")
        tdSql.error("select spread(round(c8)) from db.super")

        tdSql.error("select round(spread(c2)) from db.super")
        tdSql.error("select round(spread(c3)) from db.super")
        tdSql.error("select round(spread(c4)) from db.super")
        tdSql.error("select round(spread(c5)) from db.super")
        tdSql.error("select round(spread(c7)) from db.super")
        tdSql.error("select round(spread(c8)) from db.super")

        tdSql.error("select round(c2 + 1) from db.super")
        tdSql.error("select round(c3 + 1) from db.super")
        tdSql.error("select round(c4 + 1) from db.super")
        tdSql.error("select round(c5 + 1) from db.super")
        tdSql.error("select round(c7 + 1) from db.super")
        tdSql.error("select round(c8 + 1) from db.super")

        tdSql.error("select bottom(round(c2), 2) from db.super")
        tdSql.error("select bottom(round(c3), 2) from db.super")
        tdSql.error("select bottom(round(c4), 2) from db.super")
        tdSql.error("select bottom(round(c5), 2) from db.super")
        tdSql.error("select bottom(round(c7), 2) from db.super")
        tdSql.error("select bottom(round(c8), 2) from db.super")

        tdSql.error("select round(bottom(c2, 2)) from db.super")
        tdSql.error("select round(bottom(c3, 2)) from db.super")
        tdSql.error("select round(bottom(c4, 2)) from db.super")
        tdSql.error("select round(bottom(c5, 2)) from db.super")
        tdSql.error("select round(bottom(c7, 2)) from db.super")
        tdSql.error("select round(bottom(c8, 2)) from db.super")

        tdSql.error("select round(c2) + round(c3) from db.super")
        tdSql.error("select round(c3) + round(c4) from db.super")
        tdSql.error("select round(c4) + round(c5) from db.super")
        tdSql.error("select round(c5) + round(c7) from db.super")
        tdSql.error("select round(c7) + round(c8) from db.super")
        tdSql.error("select round(c8) + round(c2) from db.super")

        tdSql.error("select round(c2 + c3) from db.super")
        tdSql.error("select round(c3 + c4) from db.super")
        tdSql.error("select round(c4 + c5) from db.super")
        tdSql.error("select round(c5 + c7) from db.super")
        tdSql.error("select round(c7 + c8) from db.super")
        tdSql.error("select round(c8 + c2) from db.super")

        # for table
        tdSql.error("select max(round(c2)) from db.t1")
        tdSql.error("select max(round(c3)) from db.t1")
        tdSql.error("select max(round(c4)) from db.t1")
        tdSql.error("select max(round(c5)) from db.t1")
        tdSql.error("select max(round(c7)) from db.t1")
        tdSql.error("select max(round(c8)) from db.t1")

        tdSql.error("select round(max(c2)) from db.t1")
        tdSql.error("select round(max(c3)) from db.t1")
        tdSql.error("select round(max(c4)) from db.t1")
        tdSql.error("select round(max(c5)) from db.t1")
        tdSql.error("select round(max(c7)) from db.t1")
        tdSql.error("select round(max(c8)) from db.t1")

        tdSql.error("select min(round(c2)) from db.t1")
        tdSql.error("select min(round(c3)) from db.t1")
        tdSql.error("select min(round(c4)) from db.t1")
        tdSql.error("select min(round(c5)) from db.t1")
        tdSql.error("select min(round(c7)) from db.t1")
        tdSql.error("select min(round(c8)) from db.t1")

        tdSql.error("select round(min(c2)) from db.t1")
        tdSql.error("select round(min(c3)) from db.t1")
        tdSql.error("select round(min(c4)) from db.t1")
        tdSql.error("select round(min(c5)) from db.t1")
        tdSql.error("select round(min(c7)) from db.t1")
        tdSql.error("select round(min(c8)) from db.t1")

        tdSql.error("select avg(round(c2)) from db.t1")
        tdSql.error("select avg(round(c3)) from db.t1")
        tdSql.error("select avg(round(c4)) from db.t1")
        tdSql.error("select avg(round(c5)) from db.t1")
        tdSql.error("select avg(round(c7)) from db.t1")
        tdSql.error("select avg(round(c8)) from db.t1")

        tdSql.error("select round(avg(c2)) from db.t1")
        tdSql.error("select round(avg(c3)) from db.t1")
        tdSql.error("select round(avg(c4)) from db.t1")
        tdSql.error("select round(avg(c5)) from db.t1")
        tdSql.error("select round(avg(c7)) from db.t1")
        tdSql.error("select round(avg(c8)) from db.t1")

        tdSql.error("select last(round(c2)) from db.t1")
        tdSql.error("select last(round(c3)) from db.t1")
        tdSql.error("select last(round(c4)) from db.t1")
        tdSql.error("select last(round(c5)) from db.t1")
        tdSql.error("select last(round(c7)) from db.t1")
        tdSql.error("select last(round(c8)) from db.t1")

        tdSql.error("select round(last(c2)) from db.t1")
        tdSql.error("select round(last(c3)) from db.t1")
        tdSql.error("select round(last(c4)) from db.t1")
        tdSql.error("select round(last(c5)) from db.t1")
        tdSql.error("select round(last(c7)) from db.t1")
        tdSql.error("select round(last(c8)) from db.t1")
        
        tdSql.error("select last_row(round(c2)) from db.t1")
        tdSql.error("select last_row(round(c3)) from db.t1")
        tdSql.error("select last_row(round(c4)) from db.t1")
        tdSql.error("select last_row(round(c5)) from db.t1")
        tdSql.error("select last_row(round(c7)) from db.t1")
        tdSql.error("select last_row(round(c8)) from db.t1")

        tdSql.error("select round(last_row(c2)) from db.t1")
        tdSql.error("select round(last_row(c3)) from db.t1")
        tdSql.error("select round(last_row(c4)) from db.t1")
        tdSql.error("select round(last_row(c5)) from db.t1")
        tdSql.error("select round(last_row(c7)) from db.t1")
        tdSql.error("select round(last_row(c8)) from db.t1")

        tdSql.error("select first(round(c2)) from db.t1")
        tdSql.error("select first(round(c3)) from db.t1")
        tdSql.error("select first(round(c4)) from db.t1")
        tdSql.error("select first(round(c5)) from db.t1")
        tdSql.error("select first(round(c7)) from db.t1")
        tdSql.error("select first(round(c8)) from db.t1")

        tdSql.error("select round(first(c2)) from db.t1")
        tdSql.error("select round(first(c3)) from db.t1")
        tdSql.error("select round(first(c4)) from db.t1")
        tdSql.error("select round(first(c5)) from db.t1")
        tdSql.error("select round(first(c7)) from db.t1")
        tdSql.error("select round(first(c8)) from db.t1")

        tdSql.error("select diff(round(c2)) from db.t1")
        tdSql.error("select diff(round(c3)) from db.t1")
        tdSql.error("select diff(round(c4)) from db.t1")
        tdSql.error("select diff(round(c5)) from db.t1")
        tdSql.error("select diff(round(c7)) from db.t1")
        tdSql.error("select diff(round(c8)) from db.t1")

        tdSql.error("select round(diff(c2)) from db.t1")
        tdSql.error("select round(diff(c3)) from db.t1")
        tdSql.error("select round(diff(c4)) from db.t1")
        tdSql.error("select round(diff(c5)) from db.t1")
        tdSql.error("select round(diff(c7)) from db.t1")
        tdSql.error("select round(diff(c8)) from db.t1")

        tdSql.error("select percentile(round(c2), 0) from db.t1")
        tdSql.error("select percentile(round(c3), 0) from db.t1")
        tdSql.error("select percentile(round(c4), 0) from db.t1")
        tdSql.error("select percentile(round(c5), 0) from db.t1")
        tdSql.error("select percentile(round(c7), 0) from db.t1")
        tdSql.error("select percentile(round(c8), 0) from db.t1")

        tdSql.error("select round(percentile(c2, 0)) from db.t1")
        tdSql.error("select round(percentile(c3, 0)) from db.t1")
        tdSql.error("select round(percentile(c4, 0)) from db.t1")
        tdSql.error("select round(percentile(c5, 0)) from db.t1")
        tdSql.error("select round(percentile(c7, 0)) from db.t1")
        tdSql.error("select round(percentile(c8, 0)) from db.t1")

        tdSql.error("select derivate(round(c2),1s, 1) from db.t1")
        tdSql.error("select derivate(round(c3),1s, 1) from db.t1")
        tdSql.error("select derivate(round(c4),1s, 1) from db.t1")
        tdSql.error("select derivate(round(c5),1s, 1) from db.t1")
        tdSql.error("select derivate(round(c7),1s, 1) from db.t1")
        tdSql.error("select derivate(round(c8),1s, 1) from db.t1")

        tdSql.error("select round(derivate(c2,1s, 1)) from db.t1")
        tdSql.error("select round(derivate(c3,1s, 1)) from db.t1")
        tdSql.error("select round(derivate(c4,1s, 1)) from db.t1")
        tdSql.error("select round(derivate(c5,1s, 1)) from db.t1")
        tdSql.error("select round(derivate(c7,1s, 1)) from db.t1")
        tdSql.error("select round(derivate(c8,1s, 1)) from db.t1")

        tdSql.error("select leastsquares(round(c2),1, 1) from db.t1")
        tdSql.error("select leastsquares(round(c3),1, 1) from db.t1")
        tdSql.error("select leastsquares(round(c4),1, 1) from db.t1")
        tdSql.error("select leastsquares(round(c5),1, 1) from db.t1")
        tdSql.error("select leastsquares(round(c7),1, 1) from db.t1")
        tdSql.error("select leastsquares(round(c8),1, 1) from db.t1")

        tdSql.error("select round(leastsquares(c2,1, 1)) from db.t1")
        tdSql.error("select round(leastsquares(c3,1, 1)) from db.t1")
        tdSql.error("select round(leastsquares(c4,1, 1)) from db.t1")
        tdSql.error("select round(leastsquares(c5,1, 1)) from db.t1")
        tdSql.error("select round(leastsquares(c7,1, 1)) from db.t1")
        tdSql.error("select round(leastsquares(c8,1, 1)) from db.t1")

        tdSql.error("select count(round(c2)) from db.t1")
        tdSql.error("select count(round(c3)) from db.t1")
        tdSql.error("select count(round(c4)) from db.t1")
        tdSql.error("select count(round(c5)) from db.t1")
        tdSql.error("select count(round(c7)) from db.t1")
        tdSql.error("select count(round(c8)) from db.t1")

        tdSql.error("select round(count(c2)) from db.t1")
        tdSql.error("select round(count(c3)) from db.t1")
        tdSql.error("select round(count(c4)) from db.t1")
        tdSql.error("select round(count(c5)) from db.t1")
        tdSql.error("select round(count(c7)) from db.t1")
        tdSql.error("select round(count(c8)) from db.t1")

        tdSql.error("select twa(round(c2)) from db.t1 where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select twa(round(c3)) from db.t1 where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select twa(round(c4)) from db.t1 where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select twa(round(c5)) from db.t1 where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select twa(round(c7)) from db.t1 where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select twa(round(c8)) from db.t1 where ts <= 1537146000021 and ts >= 1537146000000")

        tdSql.error("select round(twa(c2)) from db.t1 where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select round(twa(c3)) from db.t1 where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select round(twa(c4)) from db.t1 where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select round(twa(c5)) from db.t1 where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select round(twa(c7)) from db.t1 where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select round(twa(c8)) from db.t1 where ts <= 1537146000021 and ts >= 1537146000000")

        tdSql.error("select stddev(round(c2)) from db.t1")
        tdSql.error("select stddev(round(c3)) from db.t1")
        tdSql.error("select stddev(round(c4)) from db.t1")
        tdSql.error("select stddev(round(c5)) from db.t1")
        tdSql.error("select stddev(round(c7)) from db.t1")
        tdSql.error("select stddev(round(c8)) from db.t1")

        tdSql.error("select round(stddev(c2)) from db.t1")
        tdSql.error("select round(stddev(c3)) from db.t1")
        tdSql.error("select round(stddev(c4)) from db.t1")
        tdSql.error("select round(stddev(c5)) from db.t1")
        tdSql.error("select round(stddev(c7)) from db.t1")
        tdSql.error("select round(stddev(c8)) from db.t1")

        tdSql.error("select spread(round(c2)) from db.t1")
        tdSql.error("select spread(round(c3)) from db.t1")
        tdSql.error("select spread(round(c4)) from db.t1")
        tdSql.error("select spread(round(c5)) from db.t1")
        tdSql.error("select spread(round(c7)) from db.t1")
        tdSql.error("select spread(round(c8)) from db.t1")

        tdSql.error("select round(spread(c2)) from db.t1")
        tdSql.error("select round(spread(c3)) from db.t1")
        tdSql.error("select round(spread(c4)) from db.t1")
        tdSql.error("select round(spread(c5)) from db.t1")
        tdSql.error("select round(spread(c7)) from db.t1")
        tdSql.error("select round(spread(c8)) from db.t1")

        tdSql.error("select round(c2 + 1) from db.t1")
        tdSql.error("select round(c3 + 1) from db.t1")
        tdSql.error("select round(c4 + 1) from db.t1")
        tdSql.error("select round(c5 + 1) from db.t1")
        tdSql.error("select round(c7 + 1) from db.t1")
        tdSql.error("select round(c8 + 1) from db.t1")

        tdSql.error("select bottom(round(c2), 2) from db.t1")
        tdSql.error("select bottom(round(c3), 2) from db.t1")
        tdSql.error("select bottom(round(c4), 2) from db.t1")
        tdSql.error("select bottom(round(c5), 2) from db.t1")
        tdSql.error("select bottom(round(c7), 2) from db.t1")
        tdSql.error("select bottom(round(c8), 2) from db.t1")

        tdSql.error("select round(bottom(c2, 2)) from db.t1")
        tdSql.error("select round(bottom(c3, 2)) from db.t1")
        tdSql.error("select round(bottom(c4, 2)) from db.t1")
        tdSql.error("select round(bottom(c5, 2)) from db.t1")
        tdSql.error("select round(bottom(c7, 2)) from db.t1")
        tdSql.error("select round(bottom(c8, 2)) from db.t1")

        tdSql.error("select round(c2) + round(c3) from db.t1")
        tdSql.error("select round(c3) + round(c4) from db.t1")
        tdSql.error("select round(c4) + round(c5) from db.t1")
        tdSql.error("select round(c5) + round(c7) from db.t1")
        tdSql.error("select round(c7) + round(c8) from db.t1")
        tdSql.error("select round(c8) + round(c2) from db.t1")

        tdSql.error("select round(c2 + c3) from db.t1")
        tdSql.error("select round(c3 + c4) from db.t1")
        tdSql.error("select round(c4 + c5) from db.t1")
        tdSql.error("select round(c5 + c7) from db.t1")
        tdSql.error("select round(c7 + c8) from db.t1")
        tdSql.error("select round(c8 + c2) from db.t1")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())