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
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database  if not exists db keep 36500")
        tdSql.execute("use db")

        tdLog.printNoPrefix("==========step1:create table && insert data")
        # timestamp list:
        #   0->"1970-01-01 08:00:00" | -28800000->"1970-01-01 00:00:00" | -946800000000->"1940-01-01 00:00:00"
        ts1 = 0
        ts2 = -28800000
        ts3 = -946800000000
        tdSql.execute(
            "create table stb2ts (ts timestamp, ts1 timestamp, ts2 timestamp, c1 int, ts3 timestamp) TAGS(t1 int)"
        )
        tdSql.execute("create table t2ts1 using stb2ts tags(1)")

        tdSql.execute("insert into t2ts1 values (now, now, now, 1, now)")
        tdSql.execute("insert into t2ts1 values (now-1m, now-1m, now-1m, 1, now-1m)")
        tdSql.execute(f"insert into t2ts1 values ({ts1}, {ts1}, {ts1}, 1, {ts1})")
        # tdSql.execute(f"insert into t2ts1 values ({ts2}, {ts2}, {ts2}, 1, {ts2})")
        # tdSql.execute(f"insert into t2ts1 values ({ts3}, {ts3}, {ts3}, 1, {ts3})")

        tdLog.printNoPrefix("==========step2:query")
        time.sleep(1)
        # query primary key timestamp column
        tdSql.execute("select * from t2ts1 where ts < now")
        ts_len1 = len(tdSql.cursor.fetchall())
        tdSql.execute("select * from t2ts1 where ts <= now")
        ts_len2 = len(tdSql.cursor.fetchall())
        tdSql.execute("select * from t2ts1 where ts > now")
        ts_len3 = len(tdSql.cursor.fetchall())
        tdSql.execute("select * from t2ts1 where ts >= now")
        ts_len4 = len(tdSql.cursor.fetchall())
        tdSql.execute("select * from t2ts1 where ts = now")
        ts_len5 = len(tdSql.cursor.fetchall())
        # tdSql.execute("select * from t2ts1 where ts <> now")
        ts_len6 = 3
        tdSql.execute("select * from t2ts1 where ts between 0 and now")
        ts_len7 = len(tdSql.cursor.fetchall())
        tdSql.execute("select * from t2ts1 where ts between now and now+100d")
        ts_len8 = len(tdSql.cursor.fetchall())

        # query noemal timestamp column
        tdSql.query("select * from t2ts1 where ts1 < now")
        tdSql.checkRows(ts_len1)
        tdSql.query("select * from t2ts1 where ts2 < now")
        tdSql.checkRows(ts_len1)
        tdSql.query("select * from t2ts1 where ts3 < now")
        tdSql.checkRows(ts_len1)

        tdSql.query("select * from t2ts1 where ts1 <= now")
        tdSql.checkRows(ts_len2)
        tdSql.query("select * from t2ts1 where ts2 <= now")
        tdSql.checkRows(ts_len2)
        tdSql.query("select * from t2ts1 where ts3 <= now")
        tdSql.checkRows(ts_len2)

        tdSql.query("select * from t2ts1 where ts1 > now")
        tdSql.checkRows(ts_len3)
        tdSql.query("select * from t2ts1 where ts2 > now")
        tdSql.checkRows(ts_len3)
        tdSql.query("select * from t2ts1 where ts3 > now")
        tdSql.checkRows(ts_len3)

        tdSql.query("select * from t2ts1 where ts1 >= now")
        tdSql.checkRows(ts_len4)
        tdSql.query("select * from t2ts1 where ts2 >= now")
        tdSql.checkRows(ts_len4)
        tdSql.query("select * from t2ts1 where ts3 >= now")
        tdSql.checkRows(ts_len4)

        tdSql.query("select * from t2ts1 where ts1 = now")
        tdSql.checkRows(ts_len5)
        tdSql.query("select * from t2ts1 where ts2 = now")
        tdSql.checkRows(ts_len5)
        tdSql.query("select * from t2ts1 where ts2 = now")
        tdSql.checkRows(ts_len5)

        tdSql.query("select * from t2ts1 where ts1 <> now")
        tdSql.checkRows(ts_len6)
        tdSql.query("select * from t2ts1 where ts2 <> now")
        tdSql.checkRows(ts_len6)
        tdSql.query("select * from t2ts1 where ts3 <> now")
        tdSql.checkRows(ts_len6)

        tdSql.query("select * from t2ts1 where ts1 between 0 and now")
        tdSql.checkRows(ts_len7)
        tdSql.query("select * from t2ts1 where ts2 between 0 and now")
        tdSql.checkRows(ts_len7)
        tdSql.query("select * from t2ts1 where ts3 between 0 and now")
        tdSql.checkRows(ts_len7)

        tdSql.query("select * from t2ts1 where ts1 between now and now+100d")
        tdSql.checkRows(ts_len8)
        tdSql.query("select * from t2ts1 where ts2 between now and now+100d")
        tdSql.checkRows(ts_len8)
        tdSql.query("select * from t2ts1 where ts3 between now and now+100d")
        tdSql.checkRows(ts_len8)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())