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

        # tags does not support ceil
        # for stable
        tdSql.error("select ceil(t1) from db.super")
        tdSql.error("select ceil(t2) from db.super")
        tdSql.error("select ceil(t3) from db.super")
        tdSql.error("select ceil(t4) from db.super")
        tdSql.error("select ceil(t5) from db.super")
        tdSql.error("select ceil(t6) from db.super")
        tdSql.error("select ceil(t7) from db.super")
        tdSql.error("select ceil(t8) from db.super")
        tdSql.error("select ceil(t9) from db.super")

        # for table
        tdSql.error("select ceil(t1) from db.t1")
        tdSql.error("select ceil(t2) from db.t1")
        tdSql.error("select ceil(t3) from db.t1")
        tdSql.error("select ceil(t4) from db.t1")
        tdSql.error("select ceil(t5) from db.t1")
        tdSql.error("select ceil(t6) from db.t1")
        tdSql.error("select ceil(t7) from db.t1")
        tdSql.error("select ceil(t8) from db.t1")
        tdSql.error("select ceil(t9) from db.t1")


        # check support columns
        # for stable
        tdSql.error("select ceil(ts) from db.super")
        tdSql.error("select ceil(c1) from db.super")
        tdSql.query("select ceil(c2) from db.super")
        tdSql.checkRows(22)
        for i in range(22):
            if i < 11:
                tdSql.checkData(i,0,1)
            else:
                tdSql.checkData(i,0,-1)
        tdSql.query("select ceil(c3) from db.super")
        tdSql.checkRows(22)
        for i in range(22):
            if i < 11:
                tdSql.checkData(i,0,1)
            else:
                tdSql.checkData(i,0,-1)
        tdSql.query("select ceil(c4) from db.super")
        tdSql.checkRows(22)
        for i in range(22):
            if i == 0:
                tdSql.checkData(0,0,1)
            if 0 < i < 11:
                tdSql.checkData(i,0,2)
            if i == 11:
                tdSql.checkData(11,0,-1)
            if i > 11:
                tdSql.checkData(i,0,-1)
        tdSql.query("select ceil(c5) from db.super")
        tdSql.checkRows(22)
        for i in range(22):
            if i == 0:
                tdSql.checkData(0,0,1)
            if 0 < i < 11:
                tdSql.checkData(i,0,2)
            if i == 11:
                tdSql.checkData(11,0,-1)
            if i > 11:
                tdSql.checkData(i,0,-1)
        tdSql.error("select ceil(c6) from db.super")
        tdSql.query("select ceil(c7) from db.super")
        tdSql.checkRows(22)
        for i in range(22):
            if i < 11:
                tdSql.checkData(i,0,1)
            else:
                tdSql.checkData(i,0,-1)
        tdSql.query("select ceil(c8) from db.super")
        tdSql.checkRows(22)
        for i in range(22):
            if i < 11:
                tdSql.checkData(i,0,1)
            else:
                tdSql.checkData(i,0,-1)
        tdSql.error("select ceil(c9) from db.super")
        tdSql.error("select ceil(c10) from db.super")

        # for table
        tdSql.error("select ceil(ts) from db.t1")
        tdSql.error("select ceil(c1) from db.t1")
        tdSql.query("select ceil(c2) from db.t1")
        tdSql.checkRows(22)
        for i in range(22):
            if i < 11:
                tdSql.checkData(i,0,1)
            else:
                tdSql.checkData(i,0,-1)
        tdSql.query("select ceil(c3) from db.t1")
        tdSql.checkRows(22)
        for i in range(22):
            if i < 11:
                tdSql.checkData(i,0,1)
            else:
                tdSql.checkData(i,0,-1)
        tdSql.query("select ceil(c4) from db.t1")
        tdSql.checkRows(22)
        for i in range(22):
            if i == 0:
                tdSql.checkData(0,0,1)
            if 0 < i < 11:
                tdSql.checkData(i,0,2)
            if i == 11:
                tdSql.checkData(11,0,-1)
            if i > 11:
                tdSql.checkData(i,0,-1)
        tdSql.query("select ceil(c5) from db.t1")
        tdSql.checkRows(22)
        for i in range(22):
            if i == 0:
                tdSql.checkData(0,0,1)
            if 0 < i < 11:
                tdSql.checkData(i,0,2)
            if i == 11:
                tdSql.checkData(11,0,-1)
            if i > 11:
                tdSql.checkData(i,0,-1)
        tdSql.error("select ceil(c6) from db.t1")
        tdSql.query("select ceil(c7) from db.t1")
        tdSql.checkRows(22)
        for i in range(22):
            if i < 11:
                tdSql.checkData(i,0,1)
            else:
                tdSql.checkData(i,0,-1)
        tdSql.query("select ceil(c8) from db.t1")
        tdSql.checkRows(22)
        for i in range(22):
            if i < 11:
                tdSql.checkData(i,0,1)
            else:
                tdSql.checkData(i,0,-1)
        tdSql.error("select ceil(c9) from db.t1")
        tdSql.error("select ceil(c10) from db.t1")

        # does not support aggregation
        # for super table
        tdSql.error("select max(ceil(c2)) from db.super")
        tdSql.error("select max(ceil(c3)) from db.super")
        tdSql.error("select max(ceil(c4)) from db.super")
        tdSql.error("select max(ceil(c5)) from db.super")
        tdSql.error("select max(ceil(c7)) from db.super")
        tdSql.error("select max(ceil(c8)) from db.super")

        tdSql.error("select ceil(max(c2)) from db.super")
        tdSql.error("select ceil(max(c3)) from db.super")
        tdSql.error("select ceil(max(c4)) from db.super")
        tdSql.error("select ceil(max(c5)) from db.super")
        tdSql.error("select ceil(max(c7)) from db.super")
        tdSql.error("select ceil(max(c8)) from db.super")

        tdSql.error("select min(ceil(c2)) from db.super")
        tdSql.error("select min(ceil(c3)) from db.super")
        tdSql.error("select min(ceil(c4)) from db.super")
        tdSql.error("select min(ceil(c5)) from db.super")
        tdSql.error("select min(ceil(c7)) from db.super")
        tdSql.error("select min(ceil(c8)) from db.super")

        tdSql.error("select ceil(min(c2)) from db.super")
        tdSql.error("select ceil(min(c3)) from db.super")
        tdSql.error("select ceil(min(c4)) from db.super")
        tdSql.error("select ceil(min(c5)) from db.super")
        tdSql.error("select ceil(min(c7)) from db.super")
        tdSql.error("select ceil(min(c8)) from db.super")

        tdSql.error("select avg(ceil(c2)) from db.super")
        tdSql.error("select avg(ceil(c3)) from db.super")
        tdSql.error("select avg(ceil(c4)) from db.super")
        tdSql.error("select avg(ceil(c5)) from db.super")
        tdSql.error("select avg(ceil(c7)) from db.super")
        tdSql.error("select avg(ceil(c8)) from db.super")

        tdSql.error("select ceil(avg(c2)) from db.super")
        tdSql.error("select ceil(avg(c3)) from db.super")
        tdSql.error("select ceil(avg(c4)) from db.super")
        tdSql.error("select ceil(avg(c5)) from db.super")
        tdSql.error("select ceil(avg(c7)) from db.super")
        tdSql.error("select ceil(avg(c8)) from db.super")

        tdSql.error("select last(ceil(c2)) from db.super")
        tdSql.error("select last(ceil(c3)) from db.super")
        tdSql.error("select last(ceil(c4)) from db.super")
        tdSql.error("select last(ceil(c5)) from db.super")
        tdSql.error("select last(ceil(c7)) from db.super")
        tdSql.error("select last(ceil(c8)) from db.super")

        tdSql.error("select ceil(last(c2)) from db.super")
        tdSql.error("select ceil(last(c3)) from db.super")
        tdSql.error("select ceil(last(c4)) from db.super")
        tdSql.error("select ceil(last(c5)) from db.super")
        tdSql.error("select ceil(last(c7)) from db.super")
        tdSql.error("select ceil(last(c8)) from db.super")
        
        tdSql.error("select last_row(ceil(c2)) from db.super")
        tdSql.error("select last_row(ceil(c3)) from db.super")
        tdSql.error("select last_row(ceil(c4)) from db.super")
        tdSql.error("select last_row(ceil(c5)) from db.super")
        tdSql.error("select last_row(ceil(c7)) from db.super")
        tdSql.error("select last_row(ceil(c8)) from db.super")

        tdSql.error("select ceil(last_row(c2)) from db.super")
        tdSql.error("select ceil(last_row(c3)) from db.super")
        tdSql.error("select ceil(last_row(c4)) from db.super")
        tdSql.error("select ceil(last_row(c5)) from db.super")
        tdSql.error("select ceil(last_row(c7)) from db.super")
        tdSql.error("select ceil(last_row(c8)) from db.super")

        tdSql.error("select first(ceil(c2)) from db.super")
        tdSql.error("select first(ceil(c3)) from db.super")
        tdSql.error("select first(ceil(c4)) from db.super")
        tdSql.error("select first(ceil(c5)) from db.super")
        tdSql.error("select first(ceil(c7)) from db.super")
        tdSql.error("select first(ceil(c8)) from db.super")

        tdSql.error("select ceil(first(c2)) from db.super")
        tdSql.error("select ceil(first(c3)) from db.super")
        tdSql.error("select ceil(first(c4)) from db.super")
        tdSql.error("select ceil(first(c5)) from db.super")
        tdSql.error("select ceil(first(c7)) from db.super")
        tdSql.error("select ceil(first(c8)) from db.super")

        tdSql.error("select diff(ceil(c2)) from db.super")
        tdSql.error("select diff(ceil(c3)) from db.super")
        tdSql.error("select diff(ceil(c4)) from db.super")
        tdSql.error("select diff(ceil(c5)) from db.super")
        tdSql.error("select diff(ceil(c7)) from db.super")
        tdSql.error("select diff(ceil(c8)) from db.super")

        tdSql.error("select ceil(diff(c2)) from db.super")
        tdSql.error("select ceil(diff(c3)) from db.super")
        tdSql.error("select ceil(diff(c4)) from db.super")
        tdSql.error("select ceil(diff(c5)) from db.super")
        tdSql.error("select ceil(diff(c7)) from db.super")
        tdSql.error("select ceil(diff(c8)) from db.super")

        tdSql.error("select percentile(ceil(c2), 0) from db.super")
        tdSql.error("select percentile(ceil(c3), 0) from db.super")
        tdSql.error("select percentile(ceil(c4), 0) from db.super")
        tdSql.error("select percentile(ceil(c5), 0) from db.super")
        tdSql.error("select percentile(ceil(c7), 0) from db.super")
        tdSql.error("select percentile(ceil(c8), 0) from db.super")

        tdSql.error("select ceil(percentile(c2, 0)) from db.super")
        tdSql.error("select ceil(percentile(c3, 0)) from db.super")
        tdSql.error("select ceil(percentile(c4, 0)) from db.super")
        tdSql.error("select ceil(percentile(c5, 0)) from db.super")
        tdSql.error("select ceil(percentile(c7, 0)) from db.super")
        tdSql.error("select ceil(percentile(c8, 0)) from db.super")

        tdSql.error("select derivate(ceil(c2),1s, 1) from db.super")
        tdSql.error("select derivate(ceil(c3),1s, 1) from db.super")
        tdSql.error("select derivate(ceil(c4),1s, 1) from db.super")
        tdSql.error("select derivate(ceil(c5),1s, 1) from db.super")
        tdSql.error("select derivate(ceil(c7),1s, 1) from db.super")
        tdSql.error("select derivate(ceil(c8),1s, 1) from db.super")

        tdSql.error("select ceil(derivate(c2,1s, 1)) from db.super")
        tdSql.error("select ceil(derivate(c3,1s, 1)) from db.super")
        tdSql.error("select ceil(derivate(c4,1s, 1)) from db.super")
        tdSql.error("select ceil(derivate(c5,1s, 1)) from db.super")
        tdSql.error("select ceil(derivate(c7,1s, 1)) from db.super")
        tdSql.error("select ceil(derivate(c8,1s, 1)) from db.super")

        tdSql.error("select leastsquares(ceil(c2),1, 1) from db.super")
        tdSql.error("select leastsquares(ceil(c3),1, 1) from db.super")
        tdSql.error("select leastsquares(ceil(c4),1, 1) from db.super")
        tdSql.error("select leastsquares(ceil(c5),1, 1) from db.super")
        tdSql.error("select leastsquares(ceil(c7),1, 1) from db.super")
        tdSql.error("select leastsquares(ceil(c8),1, 1) from db.super")

        tdSql.error("select ceil(leastsquares(c2,1, 1)) from db.super")
        tdSql.error("select ceil(leastsquares(c3,1, 1)) from db.super")
        tdSql.error("select ceil(leastsquares(c4,1, 1)) from db.super")
        tdSql.error("select ceil(leastsquares(c5,1, 1)) from db.super")
        tdSql.error("select ceil(leastsquares(c7,1, 1)) from db.super")
        tdSql.error("select ceil(leastsquares(c8,1, 1)) from db.super")

        tdSql.error("select count(ceil(c2)) from db.super")
        tdSql.error("select count(ceil(c3)) from db.super")
        tdSql.error("select count(ceil(c4)) from db.super")
        tdSql.error("select count(ceil(c5)) from db.super")
        tdSql.error("select count(ceil(c7)) from db.super")
        tdSql.error("select count(ceil(c8)) from db.super")

        tdSql.error("select ceil(count(c2)) from db.super")
        tdSql.error("select ceil(count(c3)) from db.super")
        tdSql.error("select ceil(count(c4)) from db.super")
        tdSql.error("select ceil(count(c5)) from db.super")
        tdSql.error("select ceil(count(c7)) from db.super")
        tdSql.error("select ceil(count(c8)) from db.super")

        tdSql.error("select twa(ceil(c2)) from db.super where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select twa(ceil(c3)) from db.super where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select twa(ceil(c4)) from db.super where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select twa(ceil(c5)) from db.super where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select twa(ceil(c7)) from db.super where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select twa(ceil(c8)) from db.super where ts <= 1537146000021 and ts >= 1537146000000")

        tdSql.error("select ceil(twa(c2)) from db.super where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select ceil(twa(c3)) from db.super where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select ceil(twa(c4)) from db.super where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select ceil(twa(c5)) from db.super where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select ceil(twa(c7)) from db.super where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select ceil(twa(c8)) from db.super where ts <= 1537146000021 and ts >= 1537146000000")

        tdSql.error("select stddev(ceil(c2)) from db.super")
        tdSql.error("select stddev(ceil(c3)) from db.super")
        tdSql.error("select stddev(ceil(c4)) from db.super")
        tdSql.error("select stddev(ceil(c5)) from db.super")
        tdSql.error("select stddev(ceil(c7)) from db.super")
        tdSql.error("select stddev(ceil(c8)) from db.super")

        tdSql.error("select ceil(stddev(c2)) from db.super")
        tdSql.error("select ceil(stddev(c3)) from db.super")
        tdSql.error("select ceil(stddev(c4)) from db.super")
        tdSql.error("select ceil(stddev(c5)) from db.super")
        tdSql.error("select ceil(stddev(c7)) from db.super")
        tdSql.error("select ceil(stddev(c8)) from db.super")

        tdSql.error("select spread(ceil(c2)) from db.super")
        tdSql.error("select spread(ceil(c3)) from db.super")
        tdSql.error("select spread(ceil(c4)) from db.super")
        tdSql.error("select spread(ceil(c5)) from db.super")
        tdSql.error("select spread(ceil(c7)) from db.super")
        tdSql.error("select spread(ceil(c8)) from db.super")

        tdSql.error("select ceil(spread(c2)) from db.super")
        tdSql.error("select ceil(spread(c3)) from db.super")
        tdSql.error("select ceil(spread(c4)) from db.super")
        tdSql.error("select ceil(spread(c5)) from db.super")
        tdSql.error("select ceil(spread(c7)) from db.super")
        tdSql.error("select ceil(spread(c8)) from db.super")

        tdSql.error("select ceil(c2 + 1) from db.super")
        tdSql.error("select ceil(c3 + 1) from db.super")
        tdSql.error("select ceil(c4 + 1) from db.super")
        tdSql.error("select ceil(c5 + 1) from db.super")
        tdSql.error("select ceil(c7 + 1) from db.super")
        tdSql.error("select ceil(c8 + 1) from db.super")

        tdSql.error("select bottom(ceil(c2), 2) from db.super")
        tdSql.error("select bottom(ceil(c3), 2) from db.super")
        tdSql.error("select bottom(ceil(c4), 2) from db.super")
        tdSql.error("select bottom(ceil(c5), 2) from db.super")
        tdSql.error("select bottom(ceil(c7), 2) from db.super")
        tdSql.error("select bottom(ceil(c8), 2) from db.super")

        tdSql.error("select ceil(bottom(c2, 2)) from db.super")
        tdSql.error("select ceil(bottom(c3, 2)) from db.super")
        tdSql.error("select ceil(bottom(c4, 2)) from db.super")
        tdSql.error("select ceil(bottom(c5, 2)) from db.super")
        tdSql.error("select ceil(bottom(c7, 2)) from db.super")
        tdSql.error("select ceil(bottom(c8, 2)) from db.super")

        tdSql.error("select ceil(c2) + ceil(c3) from db.super")
        tdSql.error("select ceil(c3) + ceil(c4) from db.super")
        tdSql.error("select ceil(c4) + ceil(c5) from db.super")
        tdSql.error("select ceil(c5) + ceil(c7) from db.super")
        tdSql.error("select ceil(c7) + ceil(c8) from db.super")
        tdSql.error("select ceil(c8) + ceil(c2) from db.super")

        tdSql.error("select ceil(c2 + c3) from db.super")
        tdSql.error("select ceil(c3 + c4) from db.super")
        tdSql.error("select ceil(c4 + c5) from db.super")
        tdSql.error("select ceil(c5 + c7) from db.super")
        tdSql.error("select ceil(c7 + c8) from db.super")
        tdSql.error("select ceil(c8 + c2) from db.super")

        # for table
        tdSql.error("select max(ceil(c2)) from db.t1")
        tdSql.error("select max(ceil(c3)) from db.t1")
        tdSql.error("select max(ceil(c4)) from db.t1")
        tdSql.error("select max(ceil(c5)) from db.t1")
        tdSql.error("select max(ceil(c7)) from db.t1")
        tdSql.error("select max(ceil(c8)) from db.t1")

        tdSql.error("select ceil(max(c2)) from db.t1")
        tdSql.error("select ceil(max(c3)) from db.t1")
        tdSql.error("select ceil(max(c4)) from db.t1")
        tdSql.error("select ceil(max(c5)) from db.t1")
        tdSql.error("select ceil(max(c7)) from db.t1")
        tdSql.error("select ceil(max(c8)) from db.t1")

        tdSql.error("select min(ceil(c2)) from db.t1")
        tdSql.error("select min(ceil(c3)) from db.t1")
        tdSql.error("select min(ceil(c4)) from db.t1")
        tdSql.error("select min(ceil(c5)) from db.t1")
        tdSql.error("select min(ceil(c7)) from db.t1")
        tdSql.error("select min(ceil(c8)) from db.t1")

        tdSql.error("select ceil(min(c2)) from db.t1")
        tdSql.error("select ceil(min(c3)) from db.t1")
        tdSql.error("select ceil(min(c4)) from db.t1")
        tdSql.error("select ceil(min(c5)) from db.t1")
        tdSql.error("select ceil(min(c7)) from db.t1")
        tdSql.error("select ceil(min(c8)) from db.t1")

        tdSql.error("select avg(ceil(c2)) from db.t1")
        tdSql.error("select avg(ceil(c3)) from db.t1")
        tdSql.error("select avg(ceil(c4)) from db.t1")
        tdSql.error("select avg(ceil(c5)) from db.t1")
        tdSql.error("select avg(ceil(c7)) from db.t1")
        tdSql.error("select avg(ceil(c8)) from db.t1")

        tdSql.error("select ceil(avg(c2)) from db.t1")
        tdSql.error("select ceil(avg(c3)) from db.t1")
        tdSql.error("select ceil(avg(c4)) from db.t1")
        tdSql.error("select ceil(avg(c5)) from db.t1")
        tdSql.error("select ceil(avg(c7)) from db.t1")
        tdSql.error("select ceil(avg(c8)) from db.t1")

        tdSql.error("select last(ceil(c2)) from db.t1")
        tdSql.error("select last(ceil(c3)) from db.t1")
        tdSql.error("select last(ceil(c4)) from db.t1")
        tdSql.error("select last(ceil(c5)) from db.t1")
        tdSql.error("select last(ceil(c7)) from db.t1")
        tdSql.error("select last(ceil(c8)) from db.t1")

        tdSql.error("select ceil(last(c2)) from db.t1")
        tdSql.error("select ceil(last(c3)) from db.t1")
        tdSql.error("select ceil(last(c4)) from db.t1")
        tdSql.error("select ceil(last(c5)) from db.t1")
        tdSql.error("select ceil(last(c7)) from db.t1")
        tdSql.error("select ceil(last(c8)) from db.t1")
        
        tdSql.error("select last_row(ceil(c2)) from db.t1")
        tdSql.error("select last_row(ceil(c3)) from db.t1")
        tdSql.error("select last_row(ceil(c4)) from db.t1")
        tdSql.error("select last_row(ceil(c5)) from db.t1")
        tdSql.error("select last_row(ceil(c7)) from db.t1")
        tdSql.error("select last_row(ceil(c8)) from db.t1")

        tdSql.error("select ceil(last_row(c2)) from db.t1")
        tdSql.error("select ceil(last_row(c3)) from db.t1")
        tdSql.error("select ceil(last_row(c4)) from db.t1")
        tdSql.error("select ceil(last_row(c5)) from db.t1")
        tdSql.error("select ceil(last_row(c7)) from db.t1")
        tdSql.error("select ceil(last_row(c8)) from db.t1")

        tdSql.error("select first(ceil(c2)) from db.t1")
        tdSql.error("select first(ceil(c3)) from db.t1")
        tdSql.error("select first(ceil(c4)) from db.t1")
        tdSql.error("select first(ceil(c5)) from db.t1")
        tdSql.error("select first(ceil(c7)) from db.t1")
        tdSql.error("select first(ceil(c8)) from db.t1")

        tdSql.error("select ceil(first(c2)) from db.t1")
        tdSql.error("select ceil(first(c3)) from db.t1")
        tdSql.error("select ceil(first(c4)) from db.t1")
        tdSql.error("select ceil(first(c5)) from db.t1")
        tdSql.error("select ceil(first(c7)) from db.t1")
        tdSql.error("select ceil(first(c8)) from db.t1")

        tdSql.error("select diff(ceil(c2)) from db.t1")
        tdSql.error("select diff(ceil(c3)) from db.t1")
        tdSql.error("select diff(ceil(c4)) from db.t1")
        tdSql.error("select diff(ceil(c5)) from db.t1")
        tdSql.error("select diff(ceil(c7)) from db.t1")
        tdSql.error("select diff(ceil(c8)) from db.t1")

        tdSql.error("select ceil(diff(c2)) from db.t1")
        tdSql.error("select ceil(diff(c3)) from db.t1")
        tdSql.error("select ceil(diff(c4)) from db.t1")
        tdSql.error("select ceil(diff(c5)) from db.t1")
        tdSql.error("select ceil(diff(c7)) from db.t1")
        tdSql.error("select ceil(diff(c8)) from db.t1")

        tdSql.error("select percentile(ceil(c2), 0) from db.t1")
        tdSql.error("select percentile(ceil(c3), 0) from db.t1")
        tdSql.error("select percentile(ceil(c4), 0) from db.t1")
        tdSql.error("select percentile(ceil(c5), 0) from db.t1")
        tdSql.error("select percentile(ceil(c7), 0) from db.t1")
        tdSql.error("select percentile(ceil(c8), 0) from db.t1")

        tdSql.error("select ceil(percentile(c2, 0)) from db.t1")
        tdSql.error("select ceil(percentile(c3, 0)) from db.t1")
        tdSql.error("select ceil(percentile(c4, 0)) from db.t1")
        tdSql.error("select ceil(percentile(c5, 0)) from db.t1")
        tdSql.error("select ceil(percentile(c7, 0)) from db.t1")
        tdSql.error("select ceil(percentile(c8, 0)) from db.t1")

        tdSql.error("select derivate(ceil(c2),1s, 1) from db.t1")
        tdSql.error("select derivate(ceil(c3),1s, 1) from db.t1")
        tdSql.error("select derivate(ceil(c4),1s, 1) from db.t1")
        tdSql.error("select derivate(ceil(c5),1s, 1) from db.t1")
        tdSql.error("select derivate(ceil(c7),1s, 1) from db.t1")
        tdSql.error("select derivate(ceil(c8),1s, 1) from db.t1")

        tdSql.error("select ceil(derivate(c2,1s, 1)) from db.t1")
        tdSql.error("select ceil(derivate(c3,1s, 1)) from db.t1")
        tdSql.error("select ceil(derivate(c4,1s, 1)) from db.t1")
        tdSql.error("select ceil(derivate(c5,1s, 1)) from db.t1")
        tdSql.error("select ceil(derivate(c7,1s, 1)) from db.t1")
        tdSql.error("select ceil(derivate(c8,1s, 1)) from db.t1")

        tdSql.error("select leastsquares(ceil(c2),1, 1) from db.t1")
        tdSql.error("select leastsquares(ceil(c3),1, 1) from db.t1")
        tdSql.error("select leastsquares(ceil(c4),1, 1) from db.t1")
        tdSql.error("select leastsquares(ceil(c5),1, 1) from db.t1")
        tdSql.error("select leastsquares(ceil(c7),1, 1) from db.t1")
        tdSql.error("select leastsquares(ceil(c8),1, 1) from db.t1")

        tdSql.error("select ceil(leastsquares(c2,1, 1)) from db.t1")
        tdSql.error("select ceil(leastsquares(c3,1, 1)) from db.t1")
        tdSql.error("select ceil(leastsquares(c4,1, 1)) from db.t1")
        tdSql.error("select ceil(leastsquares(c5,1, 1)) from db.t1")
        tdSql.error("select ceil(leastsquares(c7,1, 1)) from db.t1")
        tdSql.error("select ceil(leastsquares(c8,1, 1)) from db.t1")

        tdSql.error("select count(ceil(c2)) from db.t1")
        tdSql.error("select count(ceil(c3)) from db.t1")
        tdSql.error("select count(ceil(c4)) from db.t1")
        tdSql.error("select count(ceil(c5)) from db.t1")
        tdSql.error("select count(ceil(c7)) from db.t1")
        tdSql.error("select count(ceil(c8)) from db.t1")

        tdSql.error("select ceil(count(c2)) from db.t1")
        tdSql.error("select ceil(count(c3)) from db.t1")
        tdSql.error("select ceil(count(c4)) from db.t1")
        tdSql.error("select ceil(count(c5)) from db.t1")
        tdSql.error("select ceil(count(c7)) from db.t1")
        tdSql.error("select ceil(count(c8)) from db.t1")

        tdSql.error("select twa(ceil(c2)) from db.t1 where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select twa(ceil(c3)) from db.t1 where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select twa(ceil(c4)) from db.t1 where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select twa(ceil(c5)) from db.t1 where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select twa(ceil(c7)) from db.t1 where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select twa(ceil(c8)) from db.t1 where ts <= 1537146000021 and ts >= 1537146000000")

        tdSql.error("select ceil(twa(c2)) from db.t1 where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select ceil(twa(c3)) from db.t1 where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select ceil(twa(c4)) from db.t1 where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select ceil(twa(c5)) from db.t1 where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select ceil(twa(c7)) from db.t1 where ts <= 1537146000021 and ts >= 1537146000000")
        tdSql.error("select ceil(twa(c8)) from db.t1 where ts <= 1537146000021 and ts >= 1537146000000")

        tdSql.error("select stddev(ceil(c2)) from db.t1")
        tdSql.error("select stddev(ceil(c3)) from db.t1")
        tdSql.error("select stddev(ceil(c4)) from db.t1")
        tdSql.error("select stddev(ceil(c5)) from db.t1")
        tdSql.error("select stddev(ceil(c7)) from db.t1")
        tdSql.error("select stddev(ceil(c8)) from db.t1")

        tdSql.error("select ceil(stddev(c2)) from db.t1")
        tdSql.error("select ceil(stddev(c3)) from db.t1")
        tdSql.error("select ceil(stddev(c4)) from db.t1")
        tdSql.error("select ceil(stddev(c5)) from db.t1")
        tdSql.error("select ceil(stddev(c7)) from db.t1")
        tdSql.error("select ceil(stddev(c8)) from db.t1")

        tdSql.error("select spread(ceil(c2)) from db.t1")
        tdSql.error("select spread(ceil(c3)) from db.t1")
        tdSql.error("select spread(ceil(c4)) from db.t1")
        tdSql.error("select spread(ceil(c5)) from db.t1")
        tdSql.error("select spread(ceil(c7)) from db.t1")
        tdSql.error("select spread(ceil(c8)) from db.t1")

        tdSql.error("select ceil(spread(c2)) from db.t1")
        tdSql.error("select ceil(spread(c3)) from db.t1")
        tdSql.error("select ceil(spread(c4)) from db.t1")
        tdSql.error("select ceil(spread(c5)) from db.t1")
        tdSql.error("select ceil(spread(c7)) from db.t1")
        tdSql.error("select ceil(spread(c8)) from db.t1")

        tdSql.error("select ceil(c2 + 1) from db.t1")
        tdSql.error("select ceil(c3 + 1) from db.t1")
        tdSql.error("select ceil(c4 + 1) from db.t1")
        tdSql.error("select ceil(c5 + 1) from db.t1")
        tdSql.error("select ceil(c7 + 1) from db.t1")
        tdSql.error("select ceil(c8 + 1) from db.t1")

        tdSql.error("select bottom(ceil(c2), 2) from db.t1")
        tdSql.error("select bottom(ceil(c3), 2) from db.t1")
        tdSql.error("select bottom(ceil(c4), 2) from db.t1")
        tdSql.error("select bottom(ceil(c5), 2) from db.t1")
        tdSql.error("select bottom(ceil(c7), 2) from db.t1")
        tdSql.error("select bottom(ceil(c8), 2) from db.t1")

        tdSql.error("select ceil(bottom(c2, 2)) from db.t1")
        tdSql.error("select ceil(bottom(c3, 2)) from db.t1")
        tdSql.error("select ceil(bottom(c4, 2)) from db.t1")
        tdSql.error("select ceil(bottom(c5, 2)) from db.t1")
        tdSql.error("select ceil(bottom(c7, 2)) from db.t1")
        tdSql.error("select ceil(bottom(c8, 2)) from db.t1")

        tdSql.error("select ceil(c2) + ceil(c3) from db.t1")
        tdSql.error("select ceil(c3) + ceil(c4) from db.t1")
        tdSql.error("select ceil(c4) + ceil(c5) from db.t1")
        tdSql.error("select ceil(c5) + ceil(c7) from db.t1")
        tdSql.error("select ceil(c7) + ceil(c8) from db.t1")
        tdSql.error("select ceil(c8) + ceil(c2) from db.t1")

        tdSql.error("select ceil(c2 + c3) from db.t1")
        tdSql.error("select ceil(c3 + c4) from db.t1")
        tdSql.error("select ceil(c4 + c5) from db.t1")
        tdSql.error("select ceil(c5 + c7) from db.t1")
        tdSql.error("select ceil(c7 + c8) from db.t1")
        tdSql.error("select ceil(c8 + c2) from db.t1")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())