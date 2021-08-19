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
        tdSql.execute("insert into t1 values (1537146000010, 1537146000000, 1, 1, 1.4444445, 1.444445, 'abcdefgh',1,1,1,'abcdefgh')")
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
        tdSql.execute("insert into t1 values (1537146000021, 1537146000000, -1, -1, -1.4444445, -1.444445, 'abcdefgh',-1,-1,0,'abcdefgh')")

        # tags does not support floor
        # for stable
        tdSql.error("select floor(t1) from db.super")
        tdSql.error("select floor(t2) from db.super")
        tdSql.error("select floor(t3) from db.super")
        tdSql.error("select floor(t4) from db.super")
        tdSql.error("select floor(t5) from db.super")
        tdSql.error("select floor(t6) from db.super")
        tdSql.error("select floor(t7) from db.super")
        tdSql.error("select floor(t8) from db.super")
        tdSql.error("select floor(t9) from db.super")

        # for table
        tdSql.error("select floor(t1) from db.t1")
        tdSql.error("select floor(t2) from db.t1")
        tdSql.error("select floor(t3) from db.t1")
        tdSql.error("select floor(t4) from db.t1")
        tdSql.error("select floor(t5) from db.t1")
        tdSql.error("select floor(t6) from db.t1")
        tdSql.error("select floor(t7) from db.t1")
        tdSql.error("select floor(t8) from db.t1")
        tdSql.error("select floor(t9) from db.t1")


        # check support columns
        # for stable
        tdSql.error("select floor(ts) from db.super")
        tdSql.error("select floor(c1) from db.super")
        tdSql.query("select floor(c2) from db.super")
        tdSql.checkRows(22)
        for i in range(22):
            if i < 11:
                tdSql.checkData(i,0,1)
            else:
                tdSql.checkData(i,0,-1)
        tdSql.query("select floor(c3) from db.super")
        tdSql.checkRows(22)
        for i in range(22):
            if i < 11:
                tdSql.checkData(i,0,1)
            else:
                tdSql.checkData(i,0,-1)
        tdSql.query("select floor(c4) from db.super")
        tdSql.checkRows(22)
        for i in range(22):
            if i == 0:
                tdSql.checkData(0,0,1)
            if 0 < i < 11:
                tdSql.checkData(i,0,1)
            if i == 11:
                tdSql.checkData(11,0,-1)
            if i > 11:
                tdSql.checkData(i,0,-2)
        tdSql.query("select floor(c5) from db.super")
        tdSql.checkRows(22)
        for i in range(22):
            if i == 0:
                tdSql.checkData(0,0,1)
            if 0 < i < 11:
                tdSql.checkData(i,0,1)
            if i == 11:
                tdSql.checkData(11,0,-1)
            if i > 11:
                tdSql.checkData(i,0,-2)
        tdSql.error("select floor(c6) from db.super")
        tdSql.query("select floor(c7) from db.super")
        tdSql.checkRows(22)
        for i in range(22):
            if i < 11:
                tdSql.checkData(i,0,1)
            else:
                tdSql.checkData(i,0,-1)
        tdSql.query("select floor(c8) from db.super")
        tdSql.checkRows(22)
        for i in range(22):
            if i < 11:
                tdSql.checkData(i,0,1)
            else:
                tdSql.checkData(i,0,-1)
        tdSql.error("select floor(c9) from db.super")
        tdSql.error("select floor(c10) from db.super")

        # for table
        tdSql.error("select floor(ts) from db.t1")
        tdSql.error("select floor(c1) from db.t1")
        tdSql.query("select floor(c2) from db.t1")
        tdSql.checkRows(22)
        for i in range(22):
            if i < 11:
                tdSql.checkData(i,0,1)
            else:
                tdSql.checkData(i,0,-1)
        tdSql.query("select floor(c3) from db.t1")
        tdSql.checkRows(22)
        for i in range(22):
            if i < 11:
                tdSql.checkData(i,0,1)
            else:
                tdSql.checkData(i,0,-1)
        tdSql.query("select floor(c4) from db.t1")
        tdSql.checkRows(22)
        for i in range(22):
            if i == 0:
                tdSql.checkData(0,0,1)
            if 0 < i < 11:
                tdSql.checkData(i,0,1)
            if i == 11:
                tdSql.checkData(11,0,-1)
            if i > 11:
                tdSql.checkData(i,0,-2)
        tdSql.query("select floor(c5) from db.t1")
        tdSql.checkRows(22)
        for i in range(22):
            if i == 0:
                tdSql.checkData(0,0,1)
            if 0 < i < 11:
                tdSql.checkData(i,0,1)
            if i == 11:
                tdSql.checkData(11,0,-1)
            if i > 11:
                tdSql.checkData(i,0,-2)
        tdSql.error("select floor(c6) from db.t1")
        tdSql.query("select floor(c7) from db.t1")
        tdSql.checkRows(22)
        for i in range(22):
            if i < 11:
                tdSql.checkData(i,0,1)
            else:
                tdSql.checkData(i,0,-1)
        tdSql.query("select floor(c8) from db.t1")
        tdSql.checkRows(22)
        for i in range(22):
            if i < 11:
                tdSql.checkData(i,0,1)
            else:
                tdSql.checkData(i,0,-1)
        tdSql.error("select floor(c9) from db.t1")
        tdSql.error("select floor(c10) from db.t1")

        # does not support aggregation
        # for super table
        tdSql.error("select max(floor(c2)) from db.super")
        tdSql.error("select max(floor(c3)) from db.super")
        tdSql.error("select max(floor(c4)) from db.super")
        tdSql.error("select max(floor(c5)) from db.super")
        tdSql.error("select max(floor(c7)) from db.super")
        tdSql.error("select max(floor(c8)) from db.super")

        tdSql.error("select floor(max(c2)) from db.super")
        tdSql.error("select floor(max(c3)) from db.super")
        tdSql.error("select floor(max(c4)) from db.super")
        tdSql.error("select floor(max(c5)) from db.super")
        tdSql.error("select floor(max(c7)) from db.super")
        tdSql.error("select floor(max(c8)) from db.super")

        tdSql.error("select min(floor(c2)) from db.super")
        tdSql.error("select min(floor(c3)) from db.super")
        tdSql.error("select min(floor(c4)) from db.super")
        tdSql.error("select min(floor(c5)) from db.super")
        tdSql.error("select min(floor(c7)) from db.super")
        tdSql.error("select min(floor(c8)) from db.super")

        tdSql.error("select floor(min(c2)) from db.super")
        tdSql.error("select floor(min(c3)) from db.super")
        tdSql.error("select floor(min(c4)) from db.super")
        tdSql.error("select floor(min(c5)) from db.super")
        tdSql.error("select floor(min(c7)) from db.super")
        tdSql.error("select floor(min(c8)) from db.super")

        tdSql.error("select avg(floor(c2)) from db.super")
        tdSql.error("select avg(floor(c3)) from db.super")
        tdSql.error("select avg(floor(c4)) from db.super")
        tdSql.error("select avg(floor(c5)) from db.super")
        tdSql.error("select avg(floor(c7)) from db.super")
        tdSql.error("select avg(floor(c8)) from db.super")

        tdSql.error("select floor(avg(c2)) from db.super")
        tdSql.error("select floor(avg(c3)) from db.super")
        tdSql.error("select floor(avg(c4)) from db.super")
        tdSql.error("select floor(avg(c5)) from db.super")
        tdSql.error("select floor(avg(c7)) from db.super")
        tdSql.error("select floor(avg(c8)) from db.super")

        tdSql.error("select last(floor(c2)) from db.super")
        tdSql.error("select last(floor(c3)) from db.super")
        tdSql.error("select last(floor(c4)) from db.super")
        tdSql.error("select last(floor(c5)) from db.super")
        tdSql.error("select last(floor(c7)) from db.super")
        tdSql.error("select last(floor(c8)) from db.super")

        tdSql.error("select floor(last(c2)) from db.super")
        tdSql.error("select floor(last(c3)) from db.super")
        tdSql.error("select floor(last(c4)) from db.super")
        tdSql.error("select floor(last(c5)) from db.super")
        tdSql.error("select floor(last(c7)) from db.super")
        tdSql.error("select floor(last(c8)) from db.super")
        
        tdSql.error("select last_row(floor(c2)) from db.super")
        tdSql.error("select last_row(floor(c3)) from db.super")
        tdSql.error("select last_row(floor(c4)) from db.super")
        tdSql.error("select last_row(floor(c5)) from db.super")
        tdSql.error("select last_row(floor(c7)) from db.super")
        tdSql.error("select last_row(floor(c8)) from db.super")

        tdSql.error("select floor(last_row(c2)) from db.super")
        tdSql.error("select floor(last_row(c3)) from db.super")
        tdSql.error("select floor(last_row(c4)) from db.super")
        tdSql.error("select floor(last_row(c5)) from db.super")
        tdSql.error("select floor(last_row(c7)) from db.super")
        tdSql.error("select floor(last_row(c8)) from db.super")

        tdSql.error("select first(floor(c2)) from db.super")
        tdSql.error("select first(floor(c3)) from db.super")
        tdSql.error("select first(floor(c4)) from db.super")
        tdSql.error("select first(floor(c5)) from db.super")
        tdSql.error("select first(floor(c7)) from db.super")
        tdSql.error("select first(floor(c8)) from db.super")

        tdSql.error("select floor(first(c2)) from db.super")
        tdSql.error("select floor(first(c3)) from db.super")
        tdSql.error("select floor(first(c4)) from db.super")
        tdSql.error("select floor(first(c5)) from db.super")
        tdSql.error("select floor(first(c7)) from db.super")
        tdSql.error("select floor(first(c8)) from db.super")

        tdSql.error("select diff(floor(c2)) from db.super")
        tdSql.error("select diff(floor(c3)) from db.super")
        tdSql.error("select diff(floor(c4)) from db.super")
        tdSql.error("select diff(floor(c5)) from db.super")
        tdSql.error("select diff(floor(c7)) from db.super")
        tdSql.error("select diff(floor(c8)) from db.super")

        tdSql.error("select floor(diff(c2)) from db.super")
        tdSql.error("select floor(diff(c3)) from db.super")
        tdSql.error("select floor(diff(c4)) from db.super")
        tdSql.error("select floor(diff(c5)) from db.super")
        tdSql.error("select floor(diff(c7)) from db.super")
        tdSql.error("select floor(diff(c8)) from db.super")

        tdSql.error("select percentile(floor(c2), 0) from db.super")
        tdSql.error("select percentile(floor(c3), 0) from db.super")
        tdSql.error("select percentile(floor(c4), 0) from db.super")
        tdSql.error("select percentile(floor(c5), 0) from db.super")
        tdSql.error("select percentile(floor(c7), 0) from db.super")
        tdSql.error("select percentile(floor(c8), 0) from db.super")

        tdSql.error("select floor(percentile(c2, 0)) from db.super")
        tdSql.error("select floor(percentile(c3, 0)) from db.super")
        tdSql.error("select floor(percentile(c4, 0)) from db.super")
        tdSql.error("select floor(percentile(c5, 0)) from db.super")
        tdSql.error("select floor(percentile(c7, 0)) from db.super")
        tdSql.error("select floor(percentile(c8, 0)) from db.super")

        tdSql.error("select derivate(floor(c2),1s, 1) from db.super")
        tdSql.error("select derivate(floor(c3),1s, 1) from db.super")
        tdSql.error("select derivate(floor(c4),1s, 1) from db.super")
        tdSql.error("select derivate(floor(c5),1s, 1) from db.super")
        tdSql.error("select derivate(floor(c7),1s, 1) from db.super")
        tdSql.error("select derivate(floor(c8),1s, 1) from db.super")

        tdSql.error("select floor(derivate(c2,1s, 1)) from db.super")
        tdSql.error("select floor(derivate(c3,1s, 1)) from db.super")
        tdSql.error("select floor(derivate(c4,1s, 1)) from db.super")
        tdSql.error("select floor(derivate(c5,1s, 1)) from db.super")
        tdSql.error("select floor(derivate(c7,1s, 1)) from db.super")
        tdSql.error("select floor(derivate(c8,1s, 1)) from db.super")

        tdSql.error("select leastsquares(floor(c2),1, 1) from db.super")
        tdSql.error("select leastsquares(floor(c3),1, 1) from db.super")
        tdSql.error("select leastsquares(floor(c4),1, 1) from db.super")
        tdSql.error("select leastsquares(floor(c5),1, 1) from db.super")
        tdSql.error("select leastsquares(floor(c7),1, 1) from db.super")
        tdSql.error("select leastsquares(floor(c8),1, 1) from db.super")

        tdSql.error("select floor(leastsquares(c2,1, 1)) from db.super")
        tdSql.error("select floor(leastsquares(c3,1, 1)) from db.super")
        tdSql.error("select floor(leastsquares(c4,1, 1)) from db.super")
        tdSql.error("select floor(leastsquares(c5,1, 1)) from db.super")
        tdSql.error("select floor(leastsquares(c7,1, 1)) from db.super")
        tdSql.error("select floor(leastsquares(c8,1, 1)) from db.super")

        tdSql.error("select count(floor(c2)) from db.super")
        tdSql.error("select count(floor(c3)) from db.super")
        tdSql.error("select count(floor(c4)) from db.super")
        tdSql.error("select count(floor(c5)) from db.super")
        tdSql.error("select count(floor(c7)) from db.super")
        tdSql.error("select count(floor(c8)) from db.super")

        tdSql.error("select floor(count(c2)) from db.super")
        tdSql.error("select floor(count(c3)) from db.super")
        tdSql.error("select floor(count(c4)) from db.super")
        tdSql.error("select floor(count(c5)) from db.super")
        tdSql.error("select floor(count(c7)) from db.super")
        tdSql.error("select floor(count(c8)) from db.super")

        tdSql.error("select twa(floor(c2)) from db.super where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select twa(floor(c3)) from db.super where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select twa(floor(c4)) from db.super where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select twa(floor(c5)) from db.super where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select twa(floor(c7)) from db.super where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select twa(floor(c8)) from db.super where ts <= 1537146000021 and ts >= 1537146000000")

        tdSql.error("select floor(twa(c2)) from db.super where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select floor(twa(c3)) from db.super where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select floor(twa(c4)) from db.super where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select floor(twa(c5)) from db.super where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select floor(twa(c7)) from db.super where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select floor(twa(c8)) from db.super where ts <= 1537146000021 and ts >= 1537146000000")

        tdSql.error("select stddev(floor(c2)) from db.super")
        tdSql.error("select stddev(floor(c3)) from db.super")
        tdSql.error("select stddev(floor(c4)) from db.super")
        tdSql.error("select stddev(floor(c5)) from db.super")
        tdSql.error("select stddev(floor(c7)) from db.super")
        tdSql.error("select stddev(floor(c8)) from db.super")

        tdSql.error("select floor(stddev(c2)) from db.super")
        tdSql.error("select floor(stddev(c3)) from db.super")
        tdSql.error("select floor(stddev(c4)) from db.super")
        tdSql.error("select floor(stddev(c5)) from db.super")
        tdSql.error("select floor(stddev(c7)) from db.super")
        tdSql.error("select floor(stddev(c8)) from db.super")

        tdSql.error("select spread(floor(c2)) from db.super")
        tdSql.error("select spread(floor(c3)) from db.super")
        tdSql.error("select spread(floor(c4)) from db.super")
        tdSql.error("select spread(floor(c5)) from db.super")
        tdSql.error("select spread(floor(c7)) from db.super")
        tdSql.error("select spread(floor(c8)) from db.super")

        tdSql.error("select floor(spread(c2)) from db.super")
        tdSql.error("select floor(spread(c3)) from db.super")
        tdSql.error("select floor(spread(c4)) from db.super")
        tdSql.error("select floor(spread(c5)) from db.super")
        tdSql.error("select floor(spread(c7)) from db.super")
        tdSql.error("select floor(spread(c8)) from db.super")

        tdSql.error("select floor(c2 + 1) from db.super")
        tdSql.error("select floor(c3 + 1) from db.super")
        tdSql.error("select floor(c4 + 1) from db.super")
        tdSql.error("select floor(c5 + 1) from db.super")
        tdSql.error("select floor(c7 + 1) from db.super")
        tdSql.error("select floor(c8 + 1) from db.super")

        tdSql.error("select bottom(floor(c2), 2) from db.super")
        tdSql.error("select bottom(floor(c3), 2) from db.super")
        tdSql.error("select bottom(floor(c4), 2) from db.super")
        tdSql.error("select bottom(floor(c5), 2) from db.super")
        tdSql.error("select bottom(floor(c7), 2) from db.super")
        tdSql.error("select bottom(floor(c8), 2) from db.super")

        tdSql.error("select floor(bottom(c2, 2)) from db.super")
        tdSql.error("select floor(bottom(c3, 2)) from db.super")
        tdSql.error("select floor(bottom(c4, 2)) from db.super")
        tdSql.error("select floor(bottom(c5, 2)) from db.super")
        tdSql.error("select floor(bottom(c7, 2)) from db.super")
        tdSql.error("select floor(bottom(c8, 2)) from db.super")

        tdSql.error("select floor(c2) + floor(c3) from db.super")
        tdSql.error("select floor(c3) + floor(c4) from db.super")
        tdSql.error("select floor(c4) + floor(c5) from db.super")
        tdSql.error("select floor(c5) + floor(c7) from db.super")
        tdSql.error("select floor(c7) + floor(c8) from db.super")
        tdSql.error("select floor(c8) + floor(c2) from db.super")

        tdSql.error("select floor(c2 + c3) from db.super")
        tdSql.error("select floor(c3 + c4) from db.super")
        tdSql.error("select floor(c4 + c5) from db.super")
        tdSql.error("select floor(c5 + c7) from db.super")
        tdSql.error("select floor(c7 + c8) from db.super")
        tdSql.error("select floor(c8 + c2) from db.super")

        # for table
        tdSql.error("select max(floor(c2)) from db.t1")
        tdSql.error("select max(floor(c3)) from db.t1")
        tdSql.error("select max(floor(c4)) from db.t1")
        tdSql.error("select max(floor(c5)) from db.t1")
        tdSql.error("select max(floor(c7)) from db.t1")
        tdSql.error("select max(floor(c8)) from db.t1")

        tdSql.error("select floor(max(c2)) from db.t1")
        tdSql.error("select floor(max(c3)) from db.t1")
        tdSql.error("select floor(max(c4)) from db.t1")
        tdSql.error("select floor(max(c5)) from db.t1")
        tdSql.error("select floor(max(c7)) from db.t1")
        tdSql.error("select floor(max(c8)) from db.t1")

        tdSql.error("select min(floor(c2)) from db.t1")
        tdSql.error("select min(floor(c3)) from db.t1")
        tdSql.error("select min(floor(c4)) from db.t1")
        tdSql.error("select min(floor(c5)) from db.t1")
        tdSql.error("select min(floor(c7)) from db.t1")
        tdSql.error("select min(floor(c8)) from db.t1")

        tdSql.error("select floor(min(c2)) from db.t1")
        tdSql.error("select floor(min(c3)) from db.t1")
        tdSql.error("select floor(min(c4)) from db.t1")
        tdSql.error("select floor(min(c5)) from db.t1")
        tdSql.error("select floor(min(c7)) from db.t1")
        tdSql.error("select floor(min(c8)) from db.t1")

        tdSql.error("select avg(floor(c2)) from db.t1")
        tdSql.error("select avg(floor(c3)) from db.t1")
        tdSql.error("select avg(floor(c4)) from db.t1")
        tdSql.error("select avg(floor(c5)) from db.t1")
        tdSql.error("select avg(floor(c7)) from db.t1")
        tdSql.error("select avg(floor(c8)) from db.t1")

        tdSql.error("select floor(avg(c2)) from db.t1")
        tdSql.error("select floor(avg(c3)) from db.t1")
        tdSql.error("select floor(avg(c4)) from db.t1")
        tdSql.error("select floor(avg(c5)) from db.t1")
        tdSql.error("select floor(avg(c7)) from db.t1")
        tdSql.error("select floor(avg(c8)) from db.t1")

        tdSql.error("select last(floor(c2)) from db.t1")
        tdSql.error("select last(floor(c3)) from db.t1")
        tdSql.error("select last(floor(c4)) from db.t1")
        tdSql.error("select last(floor(c5)) from db.t1")
        tdSql.error("select last(floor(c7)) from db.t1")
        tdSql.error("select last(floor(c8)) from db.t1")

        tdSql.error("select floor(last(c2)) from db.t1")
        tdSql.error("select floor(last(c3)) from db.t1")
        tdSql.error("select floor(last(c4)) from db.t1")
        tdSql.error("select floor(last(c5)) from db.t1")
        tdSql.error("select floor(last(c7)) from db.t1")
        tdSql.error("select floor(last(c8)) from db.t1")
        
        tdSql.error("select last_row(floor(c2)) from db.t1")
        tdSql.error("select last_row(floor(c3)) from db.t1")
        tdSql.error("select last_row(floor(c4)) from db.t1")
        tdSql.error("select last_row(floor(c5)) from db.t1")
        tdSql.error("select last_row(floor(c7)) from db.t1")
        tdSql.error("select last_row(floor(c8)) from db.t1")

        tdSql.error("select floor(last_row(c2)) from db.t1")
        tdSql.error("select floor(last_row(c3)) from db.t1")
        tdSql.error("select floor(last_row(c4)) from db.t1")
        tdSql.error("select floor(last_row(c5)) from db.t1")
        tdSql.error("select floor(last_row(c7)) from db.t1")
        tdSql.error("select floor(last_row(c8)) from db.t1")

        tdSql.error("select first(floor(c2)) from db.t1")
        tdSql.error("select first(floor(c3)) from db.t1")
        tdSql.error("select first(floor(c4)) from db.t1")
        tdSql.error("select first(floor(c5)) from db.t1")
        tdSql.error("select first(floor(c7)) from db.t1")
        tdSql.error("select first(floor(c8)) from db.t1")

        tdSql.error("select floor(first(c2)) from db.t1")
        tdSql.error("select floor(first(c3)) from db.t1")
        tdSql.error("select floor(first(c4)) from db.t1")
        tdSql.error("select floor(first(c5)) from db.t1")
        tdSql.error("select floor(first(c7)) from db.t1")
        tdSql.error("select floor(first(c8)) from db.t1")

        tdSql.error("select diff(floor(c2)) from db.t1")
        tdSql.error("select diff(floor(c3)) from db.t1")
        tdSql.error("select diff(floor(c4)) from db.t1")
        tdSql.error("select diff(floor(c5)) from db.t1")
        tdSql.error("select diff(floor(c7)) from db.t1")
        tdSql.error("select diff(floor(c8)) from db.t1")

        tdSql.error("select floor(diff(c2)) from db.t1")
        tdSql.error("select floor(diff(c3)) from db.t1")
        tdSql.error("select floor(diff(c4)) from db.t1")
        tdSql.error("select floor(diff(c5)) from db.t1")
        tdSql.error("select floor(diff(c7)) from db.t1")
        tdSql.error("select floor(diff(c8)) from db.t1")

        tdSql.error("select percentile(floor(c2), 0) from db.t1")
        tdSql.error("select percentile(floor(c3), 0) from db.t1")
        tdSql.error("select percentile(floor(c4), 0) from db.t1")
        tdSql.error("select percentile(floor(c5), 0) from db.t1")
        tdSql.error("select percentile(floor(c7), 0) from db.t1")
        tdSql.error("select percentile(floor(c8), 0) from db.t1")

        tdSql.error("select floor(percentile(c2, 0)) from db.t1")
        tdSql.error("select floor(percentile(c3, 0)) from db.t1")
        tdSql.error("select floor(percentile(c4, 0)) from db.t1")
        tdSql.error("select floor(percentile(c5, 0)) from db.t1")
        tdSql.error("select floor(percentile(c7, 0)) from db.t1")
        tdSql.error("select floor(percentile(c8, 0)) from db.t1")

        tdSql.error("select derivate(floor(c2),1s, 1) from db.t1")
        tdSql.error("select derivate(floor(c3),1s, 1) from db.t1")
        tdSql.error("select derivate(floor(c4),1s, 1) from db.t1")
        tdSql.error("select derivate(floor(c5),1s, 1) from db.t1")
        tdSql.error("select derivate(floor(c7),1s, 1) from db.t1")
        tdSql.error("select derivate(floor(c8),1s, 1) from db.t1")

        tdSql.error("select floor(derivate(c2,1s, 1)) from db.t1")
        tdSql.error("select floor(derivate(c3,1s, 1)) from db.t1")
        tdSql.error("select floor(derivate(c4,1s, 1)) from db.t1")
        tdSql.error("select floor(derivate(c5,1s, 1)) from db.t1")
        tdSql.error("select floor(derivate(c7,1s, 1)) from db.t1")
        tdSql.error("select floor(derivate(c8,1s, 1)) from db.t1")

        tdSql.error("select leastsquares(floor(c2),1, 1) from db.t1")
        tdSql.error("select leastsquares(floor(c3),1, 1) from db.t1")
        tdSql.error("select leastsquares(floor(c4),1, 1) from db.t1")
        tdSql.error("select leastsquares(floor(c5),1, 1) from db.t1")
        tdSql.error("select leastsquares(floor(c7),1, 1) from db.t1")
        tdSql.error("select leastsquares(floor(c8),1, 1) from db.t1")

        tdSql.error("select floor(leastsquares(c2,1, 1)) from db.t1")
        tdSql.error("select floor(leastsquares(c3,1, 1)) from db.t1")
        tdSql.error("select floor(leastsquares(c4,1, 1)) from db.t1")
        tdSql.error("select floor(leastsquares(c5,1, 1)) from db.t1")
        tdSql.error("select floor(leastsquares(c7,1, 1)) from db.t1")
        tdSql.error("select floor(leastsquares(c8,1, 1)) from db.t1")

        tdSql.error("select count(floor(c2)) from db.t1")
        tdSql.error("select count(floor(c3)) from db.t1")
        tdSql.error("select count(floor(c4)) from db.t1")
        tdSql.error("select count(floor(c5)) from db.t1")
        tdSql.error("select count(floor(c7)) from db.t1")
        tdSql.error("select count(floor(c8)) from db.t1")

        tdSql.error("select floor(count(c2)) from db.t1")
        tdSql.error("select floor(count(c3)) from db.t1")
        tdSql.error("select floor(count(c4)) from db.t1")
        tdSql.error("select floor(count(c5)) from db.t1")
        tdSql.error("select floor(count(c7)) from db.t1")
        tdSql.error("select floor(count(c8)) from db.t1")

        tdSql.error("select twa(floor(c2)) from db.t1 where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select twa(floor(c3)) from db.t1 where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select twa(floor(c4)) from db.t1 where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select twa(floor(c5)) from db.t1 where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select twa(floor(c7)) from db.t1 where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select twa(floor(c8)) from db.t1 where ts <= 1537146000021 and ts >= 1537146000000")

        tdSql.error("select floor(twa(c2)) from db.t1 where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select floor(twa(c3)) from db.t1 where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select floor(twa(c4)) from db.t1 where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select floor(twa(c5)) from db.t1 where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select floor(twa(c7)) from db.t1 where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select floor(twa(c8)) from db.t1 where ts <= 1537146000021 and ts >= 1537146000000")

        tdSql.error("select stddev(floor(c2)) from db.t1")
        tdSql.error("select stddev(floor(c3)) from db.t1")
        tdSql.error("select stddev(floor(c4)) from db.t1")
        tdSql.error("select stddev(floor(c5)) from db.t1")
        tdSql.error("select stddev(floor(c7)) from db.t1")
        tdSql.error("select stddev(floor(c8)) from db.t1")

        tdSql.error("select floor(stddev(c2)) from db.t1")
        tdSql.error("select floor(stddev(c3)) from db.t1")
        tdSql.error("select floor(stddev(c4)) from db.t1")
        tdSql.error("select floor(stddev(c5)) from db.t1")
        tdSql.error("select floor(stddev(c7)) from db.t1")
        tdSql.error("select floor(stddev(c8)) from db.t1")

        tdSql.error("select spread(floor(c2)) from db.t1")
        tdSql.error("select spread(floor(c3)) from db.t1")
        tdSql.error("select spread(floor(c4)) from db.t1")
        tdSql.error("select spread(floor(c5)) from db.t1")
        tdSql.error("select spread(floor(c7)) from db.t1")
        tdSql.error("select spread(floor(c8)) from db.t1")

        tdSql.error("select floor(spread(c2)) from db.t1")
        tdSql.error("select floor(spread(c3)) from db.t1")
        tdSql.error("select floor(spread(c4)) from db.t1")
        tdSql.error("select floor(spread(c5)) from db.t1")
        tdSql.error("select floor(spread(c7)) from db.t1")
        tdSql.error("select floor(spread(c8)) from db.t1")

        tdSql.error("select floor(c2 + 1) from db.t1")
        tdSql.error("select floor(c3 + 1) from db.t1")
        tdSql.error("select floor(c4 + 1) from db.t1")
        tdSql.error("select floor(c5 + 1) from db.t1")
        tdSql.error("select floor(c7 + 1) from db.t1")
        tdSql.error("select floor(c8 + 1) from db.t1")

        tdSql.error("select bottom(floor(c2), 2) from db.t1")
        tdSql.error("select bottom(floor(c3), 2) from db.t1")
        tdSql.error("select bottom(floor(c4), 2) from db.t1")
        tdSql.error("select bottom(floor(c5), 2) from db.t1")
        tdSql.error("select bottom(floor(c7), 2) from db.t1")
        tdSql.error("select bottom(floor(c8), 2) from db.t1")

        tdSql.error("select floor(bottom(c2, 2)) from db.t1")
        tdSql.error("select floor(bottom(c3, 2)) from db.t1")
        tdSql.error("select floor(bottom(c4, 2)) from db.t1")
        tdSql.error("select floor(bottom(c5, 2)) from db.t1")
        tdSql.error("select floor(bottom(c7, 2)) from db.t1")
        tdSql.error("select floor(bottom(c8, 2)) from db.t1")

        tdSql.error("select floor(c2) + floor(c3) from db.t1")
        tdSql.error("select floor(c3) + floor(c4) from db.t1")
        tdSql.error("select floor(c4) + floor(c5) from db.t1")
        tdSql.error("select floor(c5) + floor(c7) from db.t1")
        tdSql.error("select floor(c7) + floor(c8) from db.t1")
        tdSql.error("select floor(c8) + floor(c2) from db.t1")

        tdSql.error("select floor(c2 + c3) from db.t1")
        tdSql.error("select floor(c3 + c4) from db.t1")
        tdSql.error("select floor(c4 + c5) from db.t1")
        tdSql.error("select floor(c5 + c7) from db.t1")
        tdSql.error("select floor(c7 + c8) from db.t1")
        tdSql.error("select floor(c8 + c2) from db.t1")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())