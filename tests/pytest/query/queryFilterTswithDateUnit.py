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

    def insertnow(self, tsp1, tsp2, tsp3):

        tdSql.execute(
            "create table stbts (ts timestamp, ts1 timestamp, c1 int, ts2 timestamp) TAGS(t1 int)"
        )
        tdSql.execute("create table tts1 using stbts tags(1)")

        tdSql.execute("insert into tts1 values (now+1d, now+1d, 6, now+1d)")
        tdSql.execute("insert into tts1 values (now, now, 5, now)")
        tdSql.execute("insert into tts1 values (now-1d, now-1d, 4, now-1d)")
        tdSql.execute(f"insert into tts1 values ({tsp1}, {tsp1}, 3, {tsp1})")
        tdSql.execute(f"insert into tts1 values ({tsp2}, {tsp2}, 2, {tsp2})")
        tdSql.execute(f"insert into tts1 values ({tsp3}, {tsp3}, 1, {tsp3})")


    def querynow(self):

        tdLog.printNoPrefix("==========step query: execute query operation")
        time.sleep(1)

        cols = ["ts", "ts1", "ts2"]

        for col in cols:
            tdSql.error(f" select * from tts1 where {col} = 1d ")
            tdSql.error(f" select * from tts1 where {col} < -1d ")
            tdSql.error(f" select * from tts1 where {col} > 1d ")
            tdSql.error(f" select * from tts1 where {col} >= -1d ")
            tdSql.error(f" select * from tts1 where {col} <= 1d ")
            tdSql.error(f" select * from tts1 where {col} <> 1d ")

            tdSql.error(f" select * from tts1 where {col} = -1m ")
            tdSql.error(f" select * from tts1 where {col} < 1m ")
            tdSql.error(f" select * from tts1 where {col} > 1m ")
            tdSql.error(f" select * from tts1 where {col} >= -1m ")
            tdSql.error(f" select * from tts1 where {col} <= 1m ")
            tdSql.error(f" select * from tts1 where {col} <> 1m ")

            tdSql.error(f" select * from tts1 where {col} = -1s ")
            tdSql.error(f" select * from tts1 where {col} < 1s ")
            tdSql.error(f" select * from tts1 where {col} > 1s ")
            tdSql.error(f" select * from tts1 where {col} >= -1s ")
            tdSql.error(f" select * from tts1 where {col} <= 1s ")
            tdSql.error(f" select * from tts1 where {col} <> 1s ")

            tdSql.error(f" select * from tts1 where {col} = -1a ")
            tdSql.error(f" select * from tts1 where {col} < 1a ")
            tdSql.error(f" select * from tts1 where {col} > 1a ")
            tdSql.error(f" select * from tts1 where {col} >= -1a ")
            tdSql.error(f" select * from tts1 where {col} <= 1a ")
            tdSql.error(f" select * from tts1 where {col} <> 1a ")

            tdSql.error(f" select * from tts1 where {col} = -1h ")
            tdSql.error(f" select * from tts1 where {col} < 1h ")
            tdSql.error(f" select * from tts1 where {col} > 1h ")
            tdSql.error(f" select * from tts1 where {col} >= -1h ")
            tdSql.error(f" select * from tts1 where {col} <= 1h ")
            tdSql.error(f" select * from tts1 where {col} <> 1h ")

            tdSql.error(f" select * from tts1 where {col} = -1w ")
            tdSql.error(f" select * from tts1 where {col} < 1w ")
            tdSql.error(f" select * from tts1 where {col} > 1w ")
            tdSql.error(f" select * from tts1 where {col} >= -1w ")
            tdSql.error(f" select * from tts1 where {col} <= 1w ")
            tdSql.error(f" select * from tts1 where {col} <> 1w ")

            tdSql.error(f" select * from tts1 where {col} = -1u ")
            tdSql.error(f" select * from tts1 where {col} < 1u ")
            tdSql.error(f" select * from tts1 where {col} > 1u ")
            tdSql.error(f" select * from tts1 where {col} >= -1u ")
            tdSql.error(f" select * from tts1 where {col} <= 1u ")
            tdSql.error(f" select * from tts1 where {col} <> u ")

            tdSql.error(f" select * from tts1 where {col} = 0d ")
            tdSql.error(f" select * from tts1 where {col} < 0s ")
            tdSql.error(f" select * from tts1 where {col} > 0a ")
            tdSql.error(f" select * from tts1 where {col} >= 0m ")
            tdSql.error(f" select * from tts1 where {col} <= 0h ")
            tdSql.error(f" select * from tts1 where {col} <> 0u ")
            tdSql.error(f" select * from tts1 where {col} <> 0w ")

            tdSql.error(f" select * from tts1 where {col} = 1m+1h ")
            tdSql.error(f" select * from tts1 where {col} < 1w-1d ")
            tdSql.error(f" select * from tts1 where {col} > 0a/1u ")
            tdSql.error(f" select * from tts1 where {col} >= 1d/0s ")
            tdSql.error(f" select * from tts1 where {col} <= 1s*1a ")
            tdSql.error(f" select * from tts1 where {col} <> 0w/0d ")

            tdSql.error(f" select * from tts1 where {col} = 1m+1h ")
            tdSql.error(f" select * from tts1 where {col} < 1w-1d ")
            tdSql.error(f" select * from tts1 where {col} > 0a/1u ")
            tdSql.error(f" select * from tts1 where {col} >= 1d/0s ")
            tdSql.error(f" select * from tts1 where {col} <= 1s*1a ")
            tdSql.error(f" select * from tts1 where {col} <> 0w/0d ")

            tdSql.error(f" select * from tts1 where {col} = 1u+1 ")
            tdSql.error(f" select * from tts1 where {col} < 1a-1 ")
            tdSql.error(f" select * from tts1 where {col} > 1s*1 ")
            tdSql.error(f" select * from tts1 where {col} >= 1m/1 ")
            tdSql.error(f" select * from tts1 where {col} <= 1h/0 ")
            tdSql.error(f" select * from tts1 where {col} <> 0/1d ")
            tdSql.error(f" select * from tts1 where {col} <> 1w+'2010-01-01 00:00:00' ")

            tdSql.error(f" select * from tts1 where {col} = 1-1h ")
            tdSql.error(f" select * from tts1 where {col} < 1w-d ")
            tdSql.error(f" select * from tts1 where {col} > 0/u ")
            tdSql.error(f" select * from tts1 where {col} >= d/s ")
            tdSql.error(f" select * from tts1 where {col} <= 1/a ")
            tdSql.error(f" select * from tts1 where {col} <> d/1 ")

    def run(self):
        tdSql.execute("drop database if exists dbms")
        tdSql.execute("drop database if exists dbus")

        # timestamp list:
        #   0 -> "1970-01-01 08:00:00" | -28800000 -> "1970-01-01 00:00:00" | -946800000000 -> "1940-01-01 00:00:00"
        #   -631180800000 -> "1950-01-01 00:00:00"

        tdLog.printNoPrefix("==========step1:create table precision ms && insert data && query")
        # create databases precision is ms
        tdSql.execute("create database  if not exists dbms keep 36500")
        tdSql.execute("use dbms")
        tsp1 = 0
        tsp2 = -28800000
        tsp3 = -946800000000
        self.insertnow(tsp1,tsp2,tsp3)
        self.querynow()

        tdLog.printNoPrefix("==========step2:create table precision us && insert data && query")
        # create databases precision is us
        tdSql.execute("create database  if not exists dbus keep 36500 precision 'us' ")
        tdSql.execute("use dbus")
        tsp2 = tsp2 * 1000
        tsp3 = tsp3 * 1000
        self.insertnow(tsp1,tsp2,tsp3)
        self.querynow()

        
    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
