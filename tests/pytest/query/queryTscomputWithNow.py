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

    def inertnow(self):
        tsp1 = 0
        tsp2 = -28800000
        tsp3 = -946800000000

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
        interval_day1 = (datetime.date.today() - datetime.date(1970, 1, 1)).days
        interval_day2 = (datetime.date.today() - datetime.date(1940, 1, 1)).days

        tdLog.printNoPrefix("==========step query: execute query operation")
        time.sleep(1)
        tdSql.execute(" select * from tts1 where ts > now+1d ")
        ts_len1 = len(tdSql.cursor.fetchall())
        tdSql.execute(" select * from tts1 where ts < now+1d ")
        ts_len2 = len(tdSql.cursor.fetchall())
        tdSql.execute(" select * from tts1 where ts > now-1d ")
        ts_len3 = len(tdSql.cursor.fetchall())
        tdSql.execute(" select * from tts1 where ts < now-1d ")
        ts_len4 = len(tdSql.cursor.fetchall())
        tdSql.execute(f" select * from tts1 where ts > now-{interval_day1+1}d ")
        ts_len5 = len(tdSql.cursor.fetchall())
        tdSql.execute(f" select * from tts1 where ts < now-{interval_day1+1}d ")
        ts_len6 = len(tdSql.cursor.fetchall())
        tdSql.execute(f" select * from tts1 where ts > now-{interval_day1-1}d ")
        ts_len7 = len(tdSql.cursor.fetchall())
        tdSql.execute(f" select * from tts1 where ts < now-{interval_day1-1}d ")
        ts_len8 = len(tdSql.cursor.fetchall())
        tdSql.execute(f" select * from tts1 where ts > now-{interval_day2+1}d ")
        ts_len9 = len(tdSql.cursor.fetchall())
        tdSql.execute(f" select * from tts1 where ts < now-{interval_day2+1}d ")
        ts_len10 = len(tdSql.cursor.fetchall())
        tdSql.execute(f" select * from tts1 where ts > now-{interval_day2-1}d ")
        ts_len11 = len(tdSql.cursor.fetchall())
        tdSql.execute(f" select * from tts1 where ts < now-{interval_day2-1}d ")
        ts_len12 = len(tdSql.cursor.fetchall())

        tdSql.query(" select * from tts1 where ts1 > now+1d ")
        tdSql.checkRows(ts_len1)
        tdSql.query(" select * from tts1 where ts2 > now+1440m ")
        tdSql.checkRows(ts_len1)

        tdSql.query(" select * from tts1 where ts1 < now+1d ")
        tdSql.checkRows(ts_len2)
        tdSql.query(" select * from tts1 where ts2 < now+1440m ")
        tdSql.checkRows(ts_len2)

        tdSql.query(" select * from tts1 where ts1 > now-1d ")
        tdSql.checkRows(ts_len3)
        tdSql.query(" select * from tts1 where ts2 > now-1440m ")
        tdSql.checkRows(ts_len3)

        tdSql.query(" select * from tts1 where ts1 < now-1d ")
        tdSql.checkRows(ts_len4)
        tdSql.query(" select * from tts1 where ts2 < now-1440m ")
        tdSql.checkRows(ts_len4)

        tdSql.query(f" select * from tts1 where ts1 > now-{interval_day1+1}d ")
        tdSql.checkRows(ts_len5)
        tdSql.query(f" select * from tts1 where ts2 > now-{(interval_day1+1)*1440}m " )
        tdSql.checkRows(ts_len5)

        tdSql.query(f" select * from tts1 where ts1 < now-{interval_day1+1}d ")
        tdSql.checkRows(ts_len6)
        tdSql.query(f" select * from tts1 where ts2 < now-{(interval_day1+1)*1440}m ")
        tdSql.checkRows(ts_len6)

        tdSql.query(f" select * from tts1 where ts1 > now-{interval_day1-1}d ")
        tdSql.checkRows(ts_len7)
        tdSql.query(f" select * from tts1 where ts2 > now-{(interval_day1-1)*1440}m ")
        tdSql.checkRows(ts_len7)

        tdSql.query(f" select * from tts1 where ts1 < now-{interval_day1-1}d ")
        tdSql.checkRows(ts_len8)
        tdSql.query(f" select * from tts1 where ts2 < now-{(interval_day1-1)*1440}m ")
        tdSql.checkRows(ts_len8)

        tdSql.query(f" select * from tts1 where ts1 > now-{interval_day2 + 1}d ")
        tdSql.checkRows(ts_len9)
        tdSql.query(f" select * from tts1 where ts2 > now-{(interval_day2 + 1)*1440}m ")
        tdSql.checkRows(ts_len9)

        tdSql.query(f" select * from tts1 where ts1 < now-{interval_day2 + 1}d ")
        tdSql.checkRows(ts_len10)
        tdSql.query(f" select * from tts1 where ts2 < now-{(interval_day2 + 1)*1440}m ")
        tdSql.checkRows(ts_len10)

        tdSql.query(f" select * from tts1 where ts1 > now-{interval_day2 - 1}d ")
        tdSql.checkRows(ts_len11)
        tdSql.query(f" select * from tts1 where ts2 > now-{(interval_day2 - 1)*1440}m ")
        tdSql.checkRows(ts_len11)

        tdSql.query(f" select * from tts1 where ts1 < now-{interval_day2 - 1}d ")
        tdSql.checkRows(ts_len12)
        tdSql.query(f" select * from tts1 where ts2 < now-{(interval_day2 - 1)*1440}m ")
        tdSql.checkRows(ts_len12)



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
        self.inertnow()
        self.querynow()

        tdLog.printNoPrefix("==========step2:create table precision us && insert data && query")
        # create databases precision is us
        tdSql.execute("create database  if not exists dbus keep 36500 precision 'us' ")
        tdSql.execute("use dbus")
        self.inertnow()
        self.querynow()

        tdSql.query("select * from information_schema.ins_dnodes")
        index = tdSql.getData(0, 0)
        tdDnodes.stop(index)
        tdDnodes.start(index)

        tdLog.printNoPrefix("==========step3:after wal, query table precision ms")
        tdSql.execute("use dbus")
        self.querynow()

        tdLog.printNoPrefix("==========step4: query table precision us")
        tdSql.execute("use dbus")
        self.querynow()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())