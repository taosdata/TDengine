###################################################################
#           Copyright (c) 2025 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

from new_test_framework.utils import tdLog, tdSql, tdStream, StreamItem

class TestStateWindowExtend:
    # init
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.info(f"start to excute {__file__}")
    
    def prepare_data(self):
        tdSql.prepare()
        tdSql.execute("drop database if exists testdb")
        tdSql.execute("create database if not exists testdb", show=True)
        tdSql.execute("use testdb")
        values = """
                 ('2025-09-01 10:00:00', null,    20),
                 ('2025-09-01 10:00:01', 'a',     23.5),
                 ('2025-09-01 10:00:02', 'a',     25.9),
                 ('2025-09-01 10:02:15', null,    26),
                 ('2025-09-01 10:02:45', 'a',     28),
                 ('2025-09-01 10:04:00', null,    24.3),
                 ('2025-09-01 10:05:00', null,    null),
                 ('2025-09-01 11:01:10', 'b',     18),
                 ('2025-09-01 12:03:22', 'b',     14.4),
                 ('2025-09-01 12:20:19', 'a',     17.7),
                 ('2025-09-01 13:00:00', 'a',     null),
                 ('2025-09-01 14:00:00', null,    22.3),
                 ('2025-09-01 18:18:18', 'b',     18.18),
                 ('2025-09-01 20:00:00', 'b',     19.5),
                 ('2025-09-02 08:00:00', null,    9.9)
                 """
        # normal table
        tdSql.execute("create table ntb (ts timestamp, s varchar(10), v double)", show=True)
        tdSql.execute(f"insert into ntb values {values}", show=True)

        # super table
        tdSql.execute("create table stb (ts timestamp, s varchar(10), v double) tags (gid int)", show=True)
        tdSql.execute("create table ctb1 using stb tags (1)", show=True)
        tdSql.execute("create table ctb2 using stb tags (2)", show=True)
        tdSql.execute(f"insert into ctb1 values {values}", show=True)
        tdSql.execute(f"insert into ctb2 values {values}", show=True)

        # db precision ns
        tdSql.execute("drop database if exists testdb_ns")
        tdSql.execute("create database if not exists testdb_ns precision 'ns'")
        tdSql.execute("use testdb_ns")
        tdSql.execute("create table ntb (ts timestamp, s varchar(10), v double)", show=True)
        tdSql.execute(f"insert into ntb values {values}", show=True)

    def check_extend_normal_table(self):
        tdSql.execute("use testdb")
        # no extend, default 0
        tdSql.query('''select _wstart, _wduration, _wend, count(*), count(s), count(v),
                    avg(v), first(v), cols(last(v), ts), cols(last_row(v), ts)
                    from ntb state_window(s)''', show=True)
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2025-09-01 10:00:01.000")
        tdSql.checkData(0, 1, 164000)
        tdSql.checkData(0, 2, "2025-09-01 10:02:45.000")
        tdSql.checkData(0, 3, 4)
        tdSql.checkData(0, 4, 3)
        tdSql.checkData(0, 5, 4)
        tdSql.checkData(0, 6, 25.85)
        tdSql.checkData(0, 7, 23.5)
        tdSql.checkData(0, 8, "2025-09-01 10:02:45.000")
        tdSql.checkData(0, 9, "2025-09-01 10:02:45.000")
        tdSql.checkData(1, 0, "2025-09-01 11:01:10.000")
        tdSql.checkData(1, 1, 3732000)
        tdSql.checkData(1, 2, "2025-09-01 12:03:22.000")
        tdSql.checkData(1, 3, 2)
        tdSql.checkData(1, 4, 2)
        tdSql.checkData(1, 5, 2)
        tdSql.checkData(1, 6, 16.2)
        tdSql.checkData(1, 7, 18)
        tdSql.checkData(1, 8, "2025-09-01 12:03:22.000")
        tdSql.checkData(1, 9, "2025-09-01 12:03:22.000")
        tdSql.checkData(2, 0, "2025-09-01 12:20:19.000")
        tdSql.checkData(2, 1, 2381000)
        tdSql.checkData(2, 2, "2025-09-01 13:00:00.000")
        tdSql.checkData(2, 3, 2)
        tdSql.checkData(2, 4, 2)
        tdSql.checkData(2, 5, 1)
        tdSql.checkData(2, 6, 17.7)
        tdSql.checkData(2, 7, 17.7)
        tdSql.checkData(2, 8, "2025-09-01 12:20:19.000")
        tdSql.checkData(2, 9, "2025-09-01 13:00:00.000")
        tdSql.checkData(3, 0, "2025-09-01 18:18:18.000")
        tdSql.checkData(3, 1, 6102000)
        tdSql.checkData(3, 2, "2025-09-01 20:00:00.000")
        tdSql.checkData(3, 3, 2)
        tdSql.checkData(3, 4, 2)
        tdSql.checkData(3, 5, 2)
        tdSql.checkData(3, 6, 18.84)
        tdSql.checkData(3, 7, 18.18)
        tdSql.checkData(3, 8, "2025-09-01 20:00:00.000")
        tdSql.checkData(3, 9, "2025-09-01 20:00:00.000")

        # extend = 0
        tdSql.query("select _wstart, _wduration, _wend, count(*), count(s), count(v), \
                    avg(v), first(v), cols(last(v), ts), cols(last_row(v), ts) \
                    from ntb state_window(s, 0)", show=True)
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2025-09-01 10:00:01.000")
        tdSql.checkData(0, 1, 164000)
        tdSql.checkData(0, 2, "2025-09-01 10:02:45.000")
        tdSql.checkData(0, 3, 4)
        tdSql.checkData(0, 4, 3)
        tdSql.checkData(0, 5, 4)
        tdSql.checkData(0, 6, 25.85)
        tdSql.checkData(0, 7, 23.5)
        tdSql.checkData(0, 8, "2025-09-01 10:02:45.000")
        tdSql.checkData(0, 9, "2025-09-01 10:02:45.000")
        tdSql.checkData(1, 0, "2025-09-01 11:01:10.000")
        tdSql.checkData(1, 1, 3732000)
        tdSql.checkData(1, 2, "2025-09-01 12:03:22.000")
        tdSql.checkData(1, 3, 2)
        tdSql.checkData(1, 4, 2)
        tdSql.checkData(1, 5, 2)
        tdSql.checkData(1, 6, 16.2)
        tdSql.checkData(1, 7, 18)
        tdSql.checkData(1, 8, "2025-09-01 12:03:22.000")
        tdSql.checkData(1, 9, "2025-09-01 12:03:22.000")
        tdSql.checkData(2, 0, "2025-09-01 12:20:19.000")
        tdSql.checkData(2, 1, 2381000)
        tdSql.checkData(2, 2, "2025-09-01 13:00:00.000")
        tdSql.checkData(2, 3, 2)
        tdSql.checkData(2, 4, 2)
        tdSql.checkData(2, 5, 1)
        tdSql.checkData(2, 6, 17.7)
        tdSql.checkData(2, 7, 17.7)
        tdSql.checkData(2, 8, "2025-09-01 12:20:19.000")
        tdSql.checkData(2, 9, "2025-09-01 13:00:00.000")
        tdSql.checkData(3, 0, "2025-09-01 18:18:18.000")
        tdSql.checkData(3, 1, 6102000)
        tdSql.checkData(3, 2, "2025-09-01 20:00:00.000")
        tdSql.checkData(3, 3, 2)
        tdSql.checkData(3, 4, 2)
        tdSql.checkData(3, 5, 2)
        tdSql.checkData(3, 6, 18.84)
        tdSql.checkData(3, 7, 18.18)
        tdSql.checkData(3, 8, "2025-09-01 20:00:00.000")
        tdSql.checkData(3, 9, "2025-09-01 20:00:00.000")
        
        # extend = 1
        tdSql.query('''select _wstart, _wduration, _wend, count(*), count(s), count(v),
                    avg(v), first(v), cols(last(v), ts), cols(last_row(v), ts)
                    from ntb state_window(s, 1)''', show=True)
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2025-09-01 10:00:01.000")
        tdSql.checkData(0, 1, 3668999)
        tdSql.checkData(0, 2, "2025-09-01 11:01:09.999")
        tdSql.checkData(0, 3, 6)
        tdSql.checkData(0, 4, 3)
        tdSql.checkData(0, 5, 5)
        tdSql.checkData(0, 6, 25.54)
        tdSql.checkData(0, 7, 23.5)
        tdSql.checkData(0, 8, "2025-09-01 10:04:00.000")
        tdSql.checkData(0, 9, "2025-09-01 10:05:00.000")
        tdSql.checkData(1, 0, "2025-09-01 11:01:10.000")
        tdSql.checkData(1, 1, 4748999)
        tdSql.checkData(1, 2, "2025-09-01 12:20:18.999")
        tdSql.checkData(1, 3, 2)
        tdSql.checkData(1, 4, 2)
        tdSql.checkData(1, 5, 2)
        tdSql.checkData(1, 6, 16.2)
        tdSql.checkData(1, 7, 18)
        tdSql.checkData(1, 8, "2025-09-01 12:03:22.000")
        tdSql.checkData(1, 9, "2025-09-01 12:03:22.000")
        tdSql.checkData(2, 0, "2025-09-01 12:20:19.000")
        tdSql.checkData(2, 1, 21478999)
        tdSql.checkData(2, 2, "2025-09-01 18:18:17.999")
        tdSql.checkData(2, 3, 3)
        tdSql.checkData(2, 4, 2)
        tdSql.checkData(2, 5, 2)
        tdSql.checkData(2, 6, 20)
        tdSql.checkData(2, 7, 17.7)
        tdSql.checkData(2, 8, "2025-09-01 14:00:00.000")
        tdSql.checkData(2, 9, "2025-09-01 14:00:00.000")
        tdSql.checkData(3, 0, "2025-09-01 18:18:18.000")
        tdSql.checkData(3, 1, 49302000)
        tdSql.checkData(3, 2, "2025-09-02 08:00:00.000")
        tdSql.checkData(3, 3, 3)
        tdSql.checkData(3, 4, 2)
        tdSql.checkData(3, 5, 3)
        tdSql.checkData(3, 6, 15.86)
        tdSql.checkData(3, 7, 18.18)
        tdSql.checkData(3, 8, "2025-09-02 08:00:00.000")
        tdSql.checkData(3, 9, "2025-09-02 08:00:00.000")

        # extend = 1, with time range
        tdSql.query('''select _wstart, _wduration, _wend, count(*), count(s), count(v),
                    avg(v), first(v), cols(last(v), ts), cols(last_row(v), ts)
                    from ntb where ts >= '2025-09-01 11:00:00.000'
                    and ts <= '2025-09-01 13:00:00.000'
                    state_window(s, 1)''', show=True)
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2025-09-01 11:01:10.000")
        tdSql.checkData(0, 1, 4748999)
        tdSql.checkData(0, 2, "2025-09-01 12:20:18.999")
        tdSql.checkData(0, 5, 2)
        tdSql.checkData(0, 6, 16.2)
        tdSql.checkData(0, 7, 18)
        tdSql.checkData(0, 8, "2025-09-01 12:03:22.000")
        tdSql.checkData(0, 9, "2025-09-01 12:03:22.000")
        tdSql.checkData(1, 0, "2025-09-01 12:20:19.000")
        tdSql.checkData(1, 1, 2381000)
        tdSql.checkData(1, 2, "2025-09-01 13:00:00.000")
        tdSql.checkData(1, 5, 1)
        tdSql.checkData(1, 6, 17.7)
        tdSql.checkData(1, 7, 17.7)
        tdSql.checkData(1, 8, "2025-09-01 12:20:19.000")
        tdSql.checkData(1, 9, "2025-09-01 13:00:00.000")

        # extend = 2
        tdSql.query('''select _wstart, _wduration, _wend, count(*), count(s), count(v),
                    avg(v), first(v), cols(last(v), ts), cols(last_row(v), ts)
                    from ntb state_window(s, 2)''', show=True)
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2025-09-01 10:00:00.000")
        tdSql.checkData(0, 1, 165000)
        tdSql.checkData(0, 2, "2025-09-01 10:02:45.000")
        tdSql.checkData(0, 3, 5)
        tdSql.checkData(0, 4, 3)
        tdSql.checkData(0, 5, 5)
        tdSql.checkData(0, 6, 24.68)
        tdSql.checkData(0, 7, 20)
        tdSql.checkData(0, 8, "2025-09-01 10:02:45.000")
        tdSql.checkData(0, 9, "2025-09-01 10:02:45.000")
        tdSql.checkData(1, 0, "2025-09-01 10:02:45.001")
        tdSql.checkData(1, 1, 7236999)
        tdSql.checkData(1, 2, "2025-09-01 12:03:22.000")
        tdSql.checkData(1, 3, 4)
        tdSql.checkData(1, 4, 2)
        tdSql.checkData(1, 5, 3)
        tdSql.checkData(1, 6, 18.9)
        tdSql.checkData(1, 7, 24.3)
        tdSql.checkData(1, 8, "2025-09-01 12:03:22.000")
        tdSql.checkData(1, 9, "2025-09-01 12:03:22.000")
        tdSql.checkData(2, 0, "2025-09-01 12:03:22.001")
        tdSql.checkData(2, 1, 3397999)
        tdSql.checkData(2, 2, "2025-09-01 13:00:00.000")
        tdSql.checkData(2, 3, 2)
        tdSql.checkData(2, 4, 2)
        tdSql.checkData(2, 5, 1)
        tdSql.checkData(2, 6, 17.7)
        tdSql.checkData(2, 7, 17.7)
        tdSql.checkData(2, 8, "2025-09-01 12:20:19.000")
        tdSql.checkData(2, 9, "2025-09-01 13:00:00.000")
        tdSql.checkData(3, 0, "2025-09-01 13:00:00.001")
        tdSql.checkData(3, 1, 25199999)
        tdSql.checkData(3, 2, "2025-09-01 20:00:00.000")
        tdSql.checkData(3, 3, 3)
        tdSql.checkData(3, 4, 2)
        tdSql.checkData(3, 5, 3)
        tdSql.checkData(3, 6, 19.9933333333333)
        tdSql.checkData(3, 7, 22.3)
        tdSql.checkData(3, 8, "2025-09-01 20:00:00.000")
        tdSql.checkData(3, 9, "2025-09-01 20:00:00.000")

        # extend = 2, with time range
        tdSql.query('''select _wstart, _wduration, _wend, count(*), count(s), count(v),
                    avg(v), first(v), cols(last(v), ts), cols(last_row(v), ts)
                    from ntb where ts >= '2025-09-01 11:00:00.000'
                    and ts <= '2025-09-01 14:00:00.000'
                    state_window(s, 2)''', show=True)
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2025-09-01 11:01:10.000")
        tdSql.checkData(0, 1, 3732000)
        tdSql.checkData(0, 2, "2025-09-01 12:03:22.000")
        tdSql.checkData(0, 3, 2)
        tdSql.checkData(0, 4, 2)
        tdSql.checkData(0, 5, 2)
        tdSql.checkData(0, 6, 16.2)
        tdSql.checkData(0, 7, 18)
        tdSql.checkData(0, 8, "2025-09-01 12:03:22.000")
        tdSql.checkData(0, 9, "2025-09-01 12:03:22.000")
        tdSql.checkData(1, 0, "2025-09-01 12:03:22.001")
        tdSql.checkData(1, 1, 3397999)
        tdSql.checkData(1, 2, "2025-09-01 13:00:00.000")
        tdSql.checkData(1, 3, 2)
        tdSql.checkData(1, 4, 2)
        tdSql.checkData(1, 5, 1)
        tdSql.checkData(1, 6, 17.7)
        tdSql.checkData(1, 7, 17.7)
        tdSql.checkData(1, 8, "2025-09-01 12:20:19.000")
        tdSql.checkData(1, 9, "2025-09-01 13:00:00.000")

    def check_extend_super_table(self):
        tdSql.execute("use testdb")
        # no extend, default 0
        tdSql.error("select _wstart, _wduration, _wend, count(*), count(s), count(v), \
                    avg(v), first(v), cols(last(v), ts), cols(last_row(v), ts) \
                    from stb state_window(s)", show=True)

        # extend = 0
        tdSql.error("select _wstart, _wduration, _wend, count(*), count(s), count(v), \
                    avg(v), first(v), cols(last(v), ts), cols(last_row(v), ts) \
                    from stb state_window(s, 0)", show=True)
        
        # extend = 1
        tdSql.error("select _wstart, _wduration, _wend, count(*), count(s), count(v), \
                    avg(v), first(v), cols(last(v), ts), cols(last_row(v), ts) \
                    from stb state_window(s, 1)", show=True)

        # extend = 1, with time range
        tdSql.error("select _wstart, _wduration, _wend, count(*), count(s), count(v), \
                    avg(v), first(v), cols(last(v), ts), cols(last_row(v), ts) \
                    from stb where ts >= '2025-09-01 11:00:00.000' \
                    and ts <= '2025-09-01 13:00:00.000' \
                    state_window(s, 1)", show=True)

        # extend = 2
        tdSql.error("select _wstart, _wduration, _wend, count(*), count(s), count(v), \
                    avg(v), first(v), cols(last(v), ts), cols(last_row(v), ts) \
                    from stb state_window(s, 2)", show=True)

        # extend = 2, with time range
        tdSql.error("select _wstart, _wduration, _wend, count(*), count(s), count(v), \
                    avg(v), first(v), cols(last(v), ts), cols(last_row(v), ts) \
                    from stb where ts >= '2025-09-01 11:00:00.000' \
                    and ts <= '2025-09-01 14:00:00.000' \
                    state_window(s, 2)", show=True)

    def check_extend_ns_db(self):
        tdSql.execute("use testdb_ns")
        # extend = 1
        tdSql.query('''select _wstart, _wduration, _wend, count(*), count(s), count(v),
                    avg(v), first(v), cols(last(v), ts), cols(last_row(v), ts)
                    from testdb_ns.ntb state_window(s, 1)''', show=True)
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2025-09-01 10:00:01.000000000")
        tdSql.checkData(0, 1, 3668999999999)
        tdSql.checkData(0, 2, "2025-09-01 11:01:09.999999999")
        tdSql.checkData(0, 3, 6)
        tdSql.checkData(0, 4, 3)
        tdSql.checkData(0, 5, 5)
        tdSql.checkData(0, 6, 25.54)
        tdSql.checkData(0, 7, 23.5)
        tdSql.checkData(0, 8, "2025-09-01 10:04:00.000000000")
        tdSql.checkData(0, 9, "2025-09-01 10:05:00.000000000")
        tdSql.checkData(1, 0, "2025-09-01 11:01:10.000000000")
        tdSql.checkData(1, 1, 4748999999999)
        tdSql.checkData(1, 2, "2025-09-01 12:20:18.999999999")
        tdSql.checkData(1, 3, 2)
        tdSql.checkData(1, 4, 2)
        tdSql.checkData(1, 5, 2)
        tdSql.checkData(1, 6, 16.2)
        tdSql.checkData(1, 7, 18)
        tdSql.checkData(1, 8, "2025-09-01 12:03:22.000000000")
        tdSql.checkData(1, 9, "2025-09-01 12:03:22.000000000")
        tdSql.checkData(2, 0, "2025-09-01 12:20:19.000000000")
        tdSql.checkData(2, 1, 21478999999999)
        tdSql.checkData(2, 2, "2025-09-01 18:18:17.999999999")
        tdSql.checkData(2, 3, 3)
        tdSql.checkData(2, 4, 2)
        tdSql.checkData(2, 5, 2)
        tdSql.checkData(2, 6, 20)
        tdSql.checkData(2, 7, 17.7)
        tdSql.checkData(2, 8, "2025-09-01 14:00:00.000000000")
        tdSql.checkData(2, 9, "2025-09-01 14:00:00.000000000")
        tdSql.checkData(3, 0, "2025-09-01 18:18:18.000000000")
        tdSql.checkData(3, 1, 49302000000000)
        tdSql.checkData(3, 2, "2025-09-02 08:00:00.000000000")
        tdSql.checkData(3, 3, 3)
        tdSql.checkData(3, 4, 2)
        tdSql.checkData(3, 5, 3)
        tdSql.checkData(3, 6, 15.86)
        tdSql.checkData(3, 7, 18.18)
        tdSql.checkData(3, 8, "2025-09-02 08:00:00.000000000")
        tdSql.checkData(3, 9, "2025-09-02 08:00:00.000000000")

    def check_wrong_input(self):
        tdSql.execute("use testdb")
        tdSql.error("select count(*) from ntb state_window(s, -1)")
        tdSql.error("select count(*) from ntb state_window(s, 3)")
        tdSql.error("select count(*) from ntb state_window(s, '3')")
        tdSql.error("select count(*) from ntb state_window(s, '1')")
        tdSql.error("select count(*) from ntb state_window(s, 'abc')")
        tdSql.error("select count(*) from ntb state_window(s, true)")
        tdSql.error("select count(*) from ntb state_window(s, 1d)")
        tdSql.error("select count(*) from ntb state_window(s, 1.5)")
        tdSql.error("select count(*) from ntb state_window(s, *)")
        tdSql.error("select count(*) from ntb state_window(s, '2025-09-03')")
        tdSql.error("select count(*) from ntb state_window(s, 1+1)")
        tdSql.error("select count(*) from ntb state_window(s, 1*1)")
        tdSql.error("select count(*) from ntb state_window(s, case when now < 2025-01-01 then 0 else 1)")

    def check_stream_computing_normal_table(self):
        tdSql.execute("use testdb")

        # create streams
        streams: list[StreamItem] = []
        stream = StreamItem (
            id=0,
            stream="create stream scn0 count_window(5) from ntb into res_scn0 as \
                        select _wstart, _wduration, _wend, _twstart, _twend, \
                        count(*) cnt_all, count(s) cnt_s, count(v) cnt_v, avg(v) avg_v \
                        from ntb where ts >= _twstart and ts <= _twend \
                        state_window(s, 0)",
            res_query="select _wstart, _wduration, _wend, cnt_all, cnt_s, cnt_v, avg_v from res_scn0 \
                        where _wend <= '2025-09-02 08:00:04.000'",
            exp_query="select _wstart, _wduration, _wend, count(*), count(s), count(v), avg(v) from ntb \
                        where ts >= '2025-09-02 08:00:00.000' and ts <= '2025-09-02 08:00:04.000' state_window(s)",
        )
        streams.append(stream)

        stream = StreamItem (
            id=1,
            stream="create stream scn1 count_window(5) from ntb into res_scn1 as \
                        select _wstart, _wduration, _wend, _twstart, _twend, \
                        count(*) cnt_all, count(s) cnt_s, count(v) cnt_v, avg(v) avg_v \
                        from ntb where ts >= _twstart and ts <= _twend \
                        state_window(s, 1)",
            res_query="select _wstart, _wduration, _wend, cnt_all, cnt_s, cnt_v, avg_v from res_scn1 \
                        where _wend <= '2025-09-02 08:00:04.000'",
            exp_query="select _wstart, _wduration, _wend, count(*), count(s), count(v), avg(v) from ntb \
                        where ts >= '2025-09-02 08:00:00.000' and ts <= '2025-09-02 08:00:04.000' state_window(s, 1)",
        )
        streams.append(stream)

        stream = StreamItem (
            id=2,
            stream='''create stream scn2 count_window(5) from ntb into res_scn2 as
                        select _wstart, _wduration, _wend, _twstart, _twend,
                        count(*) cnt_all, count(s) cnt_s, count(v) cnt_v, avg(v) avg_v
                        from ntb where ts >= _twstart and ts <= _twend
                        state_window(s, 2)''',
            res_query="select _wstart, _wduration, _wend, cnt_all, cnt_s, cnt_v, avg_v from res_scn2 \
                        where _wend <= '2025-09-02 08:00:04.000'",
            exp_query="select _wstart, _wduration, _wend, count(*), count(s), count(v), avg(v) from ntb \
                        where ts >= '2025-09-02 08:00:00.000' and ts <= '2025-09-02 08:00:04.000' state_window(s, 2)",
        )
        streams.append(stream)

        # start streams
        for s in streams:
            s.createStream()
        tdStream.checkStreamStatus()

        # insert data
        tdSql.execute("insert into ntb values('2025-09-02 08:00:00.000', 'a', 3.3)")
        tdSql.execute("insert into ntb values('2025-09-02 08:00:01.000', 'a', 4.4)")
        tdSql.execute("insert into ntb values('2025-09-02 08:00:02.000', 'b', 5.5)")
        tdSql.execute("insert into ntb values('2025-09-02 08:00:03.000', 'a', 2.2)")
        tdSql.execute("insert into ntb values('2025-09-02 08:00:04.000', null, 1.1)")
        tdSql.execute("insert into ntb values('2025-09-02 08:00:05.000', null, 6.6)")
        tdSql.execute("insert into ntb values('2025-09-02 08:00:06.000', 'b', 9.9)")
        tdSql.execute("insert into ntb values('2025-09-02 08:00:07.000', null, 7.7)")
        tdSql.execute("insert into ntb values('2025-09-02 08:00:08.000', 'a', 8.8)")
        tdSql.execute("insert into ntb values('2025-09-02 08:00:09.000', 'a', 4.4)")
        tdSql.execute("insert into ntb values('2025-09-02 08:00:10.000', 'b', 5.5)")

        # check results
        for s in streams:
            s.checkResults()

    def check_stream_computing_super_table(self):
        tdSql.execute("use testdb")

        # create streams
        streams: list[StreamItem] = []
        stream = StreamItem (
            id=3,
            stream="create stream scs0 interval(5s) sliding(5s) from stb \
                        partition by tbname into res_scs0 as \
                        select _wstart, _wduration, _wend, _twstart, _twend, \
                        count(*) cnt_all, count(s) cnt_s, count(v) cnt_v, avg(v) avg_v \
                        from %%tbname where ts >= _twstart and ts < _twend \
                        state_window(s, 0)",
            res_query="select _wstart, _wduration, _wend, cnt_all, cnt_s, cnt_v, avg_v from res_scs0 \
                        where _wend < '2025-09-02 08:00:05.000'",
            exp_query="select _wstart, _wduration, _wend, count(*), count(s), count(v), avg(v) from ctb1 \
                        where ts >= '2025-09-02 08:00:00.000' and ts < '2025-09-02 08:00:05.000' state_window(s)",
        )
        streams.append(stream)

        stream = StreamItem (
            id=4,
            stream="create stream scs1 interval(5s) sliding(5s) from stb \
                        partition by tbname into res_scs1 as \
                        select _wstart, _wduration, _wend, _twstart, _twend, \
                        count(*) cnt_all, count(s) cnt_s, count(v) cnt_v, avg(v) avg_v \
                        from %%tbname where ts >= _twstart and ts < _twend \
                        state_window(s, 1)",
            res_query="select _wstart, _wduration, _wend, cnt_all, cnt_s, cnt_v, avg_v from res_scs1 \
                        where _wend < '2025-09-02 08:00:05.000'",
            exp_query="select _wstart, _wduration, _wend, count(*), count(s), count(v), avg(v) from ctb1 \
                        where ts >= '2025-09-02 08:00:00.000' and ts < '2025-09-02 08:00:05.000' state_window(s, 1)",
        )
        streams.append(stream)

        stream = StreamItem (
            id=5,
            stream="create stream scs2 interval(5s) sliding(5s) from stb \
                        partition by tbname into res_scs2 as \
                        select _wstart, _wduration, _wend, _twstart, _twend, \
                        count(*) cnt_all, count(s) cnt_s, count(v) cnt_v, avg(v) avg_v \
                        from %%tbname where ts >= _twstart and ts < _twend \
                        state_window(s, 2)",
            res_query="select _wstart, _wduration, _wend, cnt_all, cnt_s, cnt_v, avg_v from res_scs2 \
                        where _wend < '2025-09-02 08:00:05.000'",
            exp_query="select _wstart, _wduration, _wend, count(*), count(s), count(v), avg(v) from ctb1 \
                        where ts >= '2025-09-02 08:00:00.000' and ts < '2025-09-02 08:00:05.000' state_window(s, 2)",
        )
        streams.append(stream)

        # start streams
        for s in streams:
            s.createStream()
        tdStream.checkStreamStatus()

        # insert data
        tdSql.execute("insert into ctb1 values('2025-09-02 08:00:00.000', 'a', 3.3)")
        tdSql.execute("insert into ctb1 values('2025-09-02 08:00:01.000', 'a', 4.4)")
        tdSql.execute("insert into ctb1 values('2025-09-02 08:00:02.000', 'b', 5.5)")
        tdSql.execute("insert into ctb1 values('2025-09-02 08:00:03.000', 'a', 2.2)")
        tdSql.execute("insert into ctb1 values('2025-09-02 08:00:04.000', null, 1.1)")
        tdSql.execute("insert into ctb1 values('2025-09-02 08:00:05.000', null, 6.6)")
        tdSql.execute("insert into ctb1 values('2025-09-02 08:00:06.000', 'b', 9.9)")
        tdSql.execute("insert into ctb1 values('2025-09-02 08:00:07.000', null, 7.7)")
        tdSql.execute("insert into ctb1 values('2025-09-02 08:00:08.000', 'a', 8.8)")
        tdSql.execute("insert into ctb1 values('2025-09-02 08:00:09.000', 'a', 4.4)")
        tdSql.execute("insert into ctb1 values('2025-09-02 08:00:10.000', 'b', 5.5)")

        # check results
        # for s in streams:
        #     s.checkResults()

    def check_stream_trigger_normal_table(self):
        tdSql.execute("use testdb")

        # create streams
        streams: list[StreamItem] = []
        stream = StreamItem (
            id=6,
            stream="create stream stn0 state_window(s, 0) from ntb into res_stn0 as \
                        select _twstart wstart, _twduration wdur, _twend wend, count(*) cnt_all, count(s) cnt_s, \
                        count(v) cnt_v, avg(v) avg_v from ntb where ts >= _twstart and ts <= _twend",
            res_query="select wstart, wdur, wend, cnt_all, cnt_s, cnt_v, avg_v from res_stn0",
            exp_query="select _wstart wstart, _wduration wdur, _wend wend, count(*), count(s), count(v), avg(v) from ntb \
                        where ts >= '2025-09-03 08:00:00.000' and ts <= '2025-09-03 08:00:10.000' state_window(s) limit 5",
        )
        streams.append(stream)

        stream = StreamItem (
            id=7,
            stream="create stream stn1 state_window(s, 1) from ntb into res_stn1 as \
                        select _twstart wstart, _twduration wdur, _twend wend, count(*) cnt_all, count(s) cnt_s, \
                        count(v) cnt_v, avg(v) avg_v from ntb where ts >= _twstart and ts <= _twend",
            res_query="select wstart, wdur, wend, cnt_all, cnt_s, cnt_v, avg_v from res_stn1",
            exp_query="select _wstart wstart, _wduration wdur, _wend wend, count(*), count(s), count(v), avg(v) from ntb \
                        where ts >= '2025-09-03 08:00:00.000' and ts <= '2025-09-03 08:00:10.000' state_window(s, 1) limit 5",
        )
        streams.append(stream)

        stream = StreamItem (
            id=8,
            stream="create stream stn2 state_window(s, 2) from ntb into res_stn2 as \
                        select _twstart wstart, _twduration wdur, _twend wend, count(*) cnt_all, count(s) cnt_s, \
                        count(v) cnt_v, avg(v) avg_v from ntb where ts >= _twstart and ts <= _twend",
            res_query="select wstart, wdur, wend, cnt_all, cnt_s, cnt_v, avg_v from res_stn2",
            exp_query="select _wstart wstart, _wduration wdur, _wend wend, count(*), count(s), count(v), avg(v) from ntb \
                        where ts >= '2025-09-03 08:00:00.000' and ts <= '2025-09-03 08:00:10.000' state_window(s, 2) limit 5",
        )
        streams.append(stream)

        # start streams
        for s in streams:
            s.createStream()
        tdStream.checkStreamStatus("stn0")
        tdStream.checkStreamStatus("stn1")
        tdStream.checkStreamStatus("stn2")

        # insert data
        tdSql.execute("insert into ntb values('2025-09-03 08:00:00.000', null, 3.3)")
        tdSql.execute("insert into ntb values('2025-09-03 08:00:01.000', 'a', 4.4)")
        tdSql.execute("insert into ntb values('2025-09-03 08:00:02.000', 'b', 5.5)")
        tdSql.execute("insert into ntb values('2025-09-03 08:00:03.000', 'a', 2.2)")
        tdSql.execute("insert into ntb values('2025-09-03 08:00:04.000', null, 1.1)")
        tdSql.execute("insert into ntb values('2025-09-03 08:00:05.000', null, 6.6)")
        tdSql.execute("insert into ntb values('2025-09-03 08:00:06.000', 'b', 9.9)")
        tdSql.execute("insert into ntb values('2025-09-03 08:00:07.000', null, 7.7)")
        tdSql.execute("insert into ntb values('2025-09-03 08:00:08.000', 'a', 8.8)")
        tdSql.execute("insert into ntb values('2025-09-03 08:00:09.000', null, 4.4)")
        tdSql.execute("insert into ntb values('2025-09-03 08:00:10.000', 'b', 5.5)")

        # check results
        for s in streams:
            s.checkResults()
    
    def check_stream_trigger_super_table(self):
        tdSql.execute("use testdb")

        # create streams
        streams: list[StreamItem] = []
        stream = StreamItem (
            id=9,
            stream="create stream sts0 state_window(s, 0) from stb partition by tbname into res_sts0 as \
                        select _twstart wstart, _twduration wdur, _twend wend, count(*) cnt_all, count(s) cnt_s, \
                        count(v) cnt_v, avg(v) avg_v from %%tbname where ts >= _twstart and ts <= _twend",
            res_query="select wstart, wdur, wend, cnt_all, cnt_s, cnt_v, avg_v from res_sts0",
            exp_query="select _wstart wstart, _wduration wdur, _wend wend, count(*), count(s), count(v), avg(v) from ctb1 \
                        where ts >= '2025-09-03 08:00:00.000' and ts <= '2025-09-03 08:00:10.000' state_window(s) limit 5",
        )
        streams.append(stream)

        stream = StreamItem (
            id=10,
            stream="create stream sts1 state_window(s, 1) from stb partition by tbname into res_sts1 as \
                        select _twstart wstart, _twduration wdur, _twend wend, count(*) cnt_all, count(s) cnt_s, \
                        count(v) cnt_v, avg(v) avg_v from %%tbname where ts >= _twstart and ts <= _twend",
            res_query="select wstart, wdur, wend, cnt_all, cnt_s, cnt_v, avg_v from res_sts1",
            exp_query="select _wstart wstart, _wduration wdur, _wend wend, count(*), count(s), count(v), avg(v) from ctb1 \
                        where ts >= '2025-09-03 08:00:00.000' and ts <= '2025-09-03 08:00:10.000' state_window(s, 1) limit 5",
        )
        streams.append(stream)

        stream = StreamItem (
            id=11,
            stream="create stream sts2 state_window(s, 2) from stb partition by tbname into res_sts2 as \
                        select _twstart wstart, _twduration wdur, _twend wend, count(*) cnt_all, count(s) cnt_s, \
                        count(v) cnt_v, avg(v) avg_v from %%tbname where ts >= _twstart and ts <= _twend",
            res_query="select wstart, wdur, wend, cnt_all, cnt_s, cnt_v, avg_v from res_sts2",
            exp_query="select _wstart wstart, _wduration wdur, _wend wend, count(*), count(s), count(v), avg(v) from ctb1 \
                        where ts >= '2025-09-03 08:00:00.000' and ts <= '2025-09-03 08:00:10.000' state_window(s, 2) limit 5",
        )
        streams.append(stream)

        # start streams
        for s in streams:
            s.createStream()
        tdStream.checkStreamStatus()

        # insert data
        tdSql.execute("insert into ctb1 values('2025-09-03 08:00:00.000', null, 3.3)")
        tdSql.execute("insert into ctb1 values('2025-09-03 08:00:01.000', 'a', 4.4)")
        tdSql.execute("insert into ctb1 values('2025-09-03 08:00:02.000', 'b', 5.5)")
        tdSql.execute("insert into ctb1 values('2025-09-03 08:00:03.000', 'a', 2.2)")
        tdSql.execute("insert into ctb1 values('2025-09-03 08:00:04.000', null, 1.1)")
        tdSql.execute("insert into ctb1 values('2025-09-03 08:00:05.000', null, 6.6)")
        tdSql.execute("insert into ctb1 values('2025-09-03 08:00:06.000', 'b', 9.9)")
        tdSql.execute("insert into ctb1 values('2025-09-03 08:00:07.000', null, 7.7)")
        tdSql.execute("insert into ctb1 values('2025-09-03 08:00:08.000', 'a', 8.8)")
        tdSql.execute("insert into ctb1 values('2025-09-03 08:00:09.000', null, 4.4)")
        tdSql.execute("insert into ctb1 values('2025-09-03 08:00:10.000', 'b', 5.5)")

        # check results
        for s in streams:
            s.checkResults()

    # run
    def test_state_window_extend(self):
        """summary: test extend parameter in state window

        description: test extend parameter in state window
            in both batch query and stream computing scenarios

        Since: v3.3.8.0

        Labels: state window, extend, stream

        Jira: TS-7129

        Catalog:
            - xxx:xxx

        History:
            - 2025-09-04 Tony Zhang: create this test

        """

        self.prepare_data()
        tdStream.createSnode()

        self.check_wrong_input()
        self.check_extend_normal_table()
        self.check_extend_super_table()
        self.check_extend_ns_db()

        self.check_stream_computing_normal_table()
        self.check_stream_computing_super_table()
        self.check_stream_trigger_normal_table()
        self.check_stream_trigger_super_table()
