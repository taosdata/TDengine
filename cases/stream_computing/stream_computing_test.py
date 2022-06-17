###################################################################
#           Copyright (c) 2020 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

from taostest import TDCase, T
from taostest.util.common import TDCom
from taostest.util.remote import Remote
import time
import sys
import random

class TestVgroups(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.case_name = None
        self.date_time = self.tdCom.genTs()[0]
        self.latency_log = self.run_log_dir + "/latency.log"
        self.tbname = None
        self.tdCom.drop_all_db()
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdCom.createDb(dbname=dbname, vgroups=10)

    def write_latency(self, msg):
        with open(self.latency_log, 'a') as f:
            f.write(f'{msg}\n')

    def check_stream_res(self, sql, expected_res):
        self.tdSql.query(sql)
        latency = 0
        if self.tdSql.query_row == expected_res:
            self.write_latency(latency)

        while self.tdSql.query_row != expected_res:
            self.tdSql.query(sql)
            if latency < 2:
                latency += 0.01
                time.sleep(0.01)
            else:
                self.tdSql.checkEqual(self.tdSql.query_row, expected_res)
            if self.tdSql.query_row == expected_res:
                self.write_latency(latency)
                return latency
    
    def check_query_data(self, sql1, sql2):
        self.tdSql.query(sql1)
        res1 = self.tdSql.query_data
        self.tdSql.query(sql2)
        res2 = self.tdSql.query_data
        self.tdSql.checkEqual(res1, res2)

    def check_stream_field_type(self, sql, input_function):
        self.tdSql.query(sql)
        res = self.tdSql.query_data
        if input_function in ["acos", "asin", "atan", "cos", "log", "pow", "sin", "sqrt", "tan"]:
            self.tdSql.checkEqual(res[1][1], "DOUBLE")
            self.tdSql.checkEqual(res[2][1], "DOUBLE")
        elif input_function in ["lower", "ltrim", "rtrim", "upper"]:
            self.tdSql.checkEqual(res[1][1], "VARCHAR")
            self.tdSql.checkEqual(res[2][1], "VARCHAR")
            self.tdSql.checkEqual(res[3][1], "NCHAR")
        elif input_function in ["char_length", "length"]:
            self.tdSql.checkEqual(res[1][1], "BIGINT")
            self.tdSql.checkEqual(res[2][1], "BIGINT")
            self.tdSql.checkEqual(res[3][1], "BIGINT")
        elif input_function in ["concat", "concat_ws"]:
            self.tdSql.checkEqual(self.tdSql.query_data[1][1], "VARCHAR")
            self.tdSql.checkEqual(self.tdSql.query_data[2][1], "NCHAR")
            self.tdSql.checkEqual(self.tdSql.query_data[3][1], "NCHAR")
            self.tdSql.checkEqual(self.tdSql.query_data[4][1], "NCHAR")
        elif input_function in ["substr"]:
            self.tdSql.checkEqual(res[1][1], "VARCHAR")
            self.tdSql.checkEqual(res[2][1], "VARCHAR")
            self.tdSql.checkEqual(res[3][1], "VARCHAR")
            self.tdSql.checkEqual(res[4][1], "NCHAR")
        else:
            self.tdSql.checkEqual(res[1][1], "INT")
            self.tdSql.checkEqual(res[2][1], "DOUBLE")
            

    def check_stream(self, sql1, sql2, expected_count):
        self.write_latency(f'sql: {sql1}')
        self.check_stream_res(sql1, expected_count)
        self.check_query_data(sql1, sql2)

    def data_filter(self):
        self.case_name = sys._getframe().f_code.co_name
        self.tdSql.execute('create table if not exists data_filter_stb (ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 float, c6 double, c7 binary(100), c8 nchar(200), c9 bool, c10 tinyint unsigned, c11 smallint unsigned, c12 int unsigned, c13 bigint unsigned) tags (t1 tinyint, t2 smallint, t3 int, t4 bigint, t5 float, t6 double, t7 binary(100), t8 nchar(200), t9 bool, t10 tinyint unsigned, t11 smallint unsigned, t12 int unsigned, t13 bigint unsigned)')
        self.tdSql.execute('create table if not exists data_filter_ct1 using data_filter_stb tags (1, 2, 3, 4, 5.5, 6.6, "binary7", "nchar8", true, 11, 12, 13, 14)')
        self.tdSql.execute('create table if not exists data_filter_tb (ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 float, c6 double, c7 binary(100), c8 nchar(200), c9 bool, c10 tinyint unsigned, c11 smallint unsigned, c12 int unsigned, c13 bigint unsigned)')

        self.write_latency(self.case_name)
        filter_sql = f'ts >= {self.date_time}+1s and c1 = 1 or c2 > 1 and c3 != 4 or c4 <= 3 and c5 <> 0 or c6 is not Null or c7 is Null or \
            c8 between "na" and "nchar4" and c8 not between "bi" and "binary" and c8 match "nchar[19]" and c8 nmatch "nchar[25]" or c9 = True or \
            c10 in (1, 2, 3) or c10 not in (6, 7) and c8 like "nch%" and c7 not like "bina_" and c11 <= 10 or c12 is Null or c13 >= 4'
        # stb
        self.tdSql.execute(f'create stream stb_data_filter_stream trigger at_once into output_data_filter_stb as select * from data_filter_stb where {filter_sql};')
        # ctb
        self.tdSql.execute(f'create stream ctb_data_filter_stream trigger at_once into output_data_filter_ctb as select * from data_filter_ct1 where {filter_sql};')
        # tb
        self.tdSql.execute(f'create stream tb_data_filter_stream trigger at_once into output_data_filter_tb as select * from data_filter_tb where {filter_sql};')
        # insert data
        count = 1
        step_count = 1
        for i in range(1, 20):
            if i % 2 == 0:
                step_count += i
                for j in range(count, step_count):
                    self.tdSql.execute(f'insert into data_filter_ct1 values ({self.date_time}+{j}s, 1, 2, 5, 5, 1.1, 1.1, "binary6", "nchar6", true, 5, 6, 7, 8);')
                    self.tdSql.execute(f'insert into data_filter_tb values ({self.date_time}+{j}s, 1, 2, 5, 5, 1.1, 1.1, "binary6", "nchar6", true, 5, 6, 7, 8);')
                count += i
            else:
                step_count += 1
                for i in range(2):
                    self.tdSql.execute(f'insert into data_filter_ct1 values ({self.date_time}+{count}s, 1, 2, 5, 5, 1.1, 1.1, "binary6", "nchar6", true, 5, 6, 7, 8);')
                    self.tdSql.execute(f'insert into data_filter_tb values ({self.date_time}+{count}s, 1, 2, 5, 5, 1.1, 1.1, "binary6", "nchar6", true, 5, 6, 7, 8);')
                count += 1
            select_elm = "c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13"
            # check result
            self.check_stream(f'select {select_elm} from output_data_filter_stb where {filter_sql};', f'select {select_elm} from data_filter_stb where {filter_sql};', count-1)
            self.check_stream(f'select {select_elm} from output_data_filter_ctb where {filter_sql};', f'select {select_elm} from data_filter_ct1 where {filter_sql};', count-1)
            self.check_stream(f'select {select_elm} from output_data_filter_tb where {filter_sql};', f'select {select_elm} from data_filter_tb where {filter_sql};', count-1)

    def downsampling(self):
        self.case_name = sys._getframe().f_code.co_name
        self.tdSql.execute('create table if not exists downsampling_stb (ts timestamp, c1 int, c2 double, c3 varchar(100), c4 bool) tags (t1 int, t2 double, t3 varchar(100), t4 bool);')
        self.tdSql.execute('create table downsampling_ct1 using downsampling_stb tags(10, 10.1, "Beijing", True);')
        # self.tdSql.execute(f'create table ownsampling_ct2 using downsampling_stb tags(20, 20.2, "TIANJIN", False);')
        # self.tdSql.execute(f'create table ownsampling_ct3 using downsampling_stb tags(30, 30.3, "HeBei", False);')
        self.tdSql.execute('create table if not exists downsampling_tb (ts timestamp, c1 int, c2 double, c3 varchar(100), c4 bool);')
        # ! TD-16571 histogram
        # ! TD-16570 last_row(c1)
        # ! now() timezone() to_iso8601(1)
        function_list = ["min(c1)", "max(c2)", "sum(c1)", "first(c1)", "last(c1)", "apercentile(c1, 50)", "avg(c1)", "count(c1)", "leastsquares(c1, 1, 2)", "spread(c1)", "stddev(c2)", "hyperloglog(c3)", 
           "timediff(1, 0, 1h)", "timetruncate(_wstartts, 1m)", "to_iso8601(1)", 'to_unixtimestamp("1970-01-01T08:00:00+08:00")']
        # function_list = ['to_unixtimestamp("1970-01-01T08:00:00+08:00")']
        # function_list = ["to_iso8601(1)"]
        # function_list = ["min(c1)", "max(c2)", "sum(c1)", "first(c1)", "last(c1)", "apercentile(c1, 50)", "last_row(c1)", "avg(c1)", "count(c1)", "leastsquares(c1, 1, 2)", "spread(c1)", "stddev(c2)", "hyperloglog(c3)", 
        #     'histogram(c1, "user_input", "[1, 3, 5, 7]", 0)', "now()", "timediff(1, 0, 1h)", "timetruncate(_wstartts, 1m)", "timezone()", "today()", "to_iso8601(1)",  'to_unixtimestamp("1970-01-01T08:00:00+08:00")']
        output_select_str = ','.join(list(map(lambda x:f'`{x}`', function_list)))
        source_select_str = ','.join(function_list)
        self.write_latency(self.case_name)
        # stb
        self.tdSql.execute(f'create stream stb_downsampling_stream trigger at_once into output_downsampling_stb as select _wstartts AS start, {source_select_str} from downsampling_stb interval(10m);')
        # ctb
        self.tdSql.execute(f'create stream ctb_downsampling_stream trigger at_once into output_downsampling_ctb as select _wstartts AS start, {source_select_str} from downsampling_ct1 interval(10m);')
        # tb
        self.tdSql.execute(f'create stream tb_downsampling_stream trigger at_once into output_downsampling_tb as select _wstartts AS start, {source_select_str} from downsampling_tb interval(10m);')
        for tbname in ["downsampling_ct1", "downsampling_tb"]:
            self.tdSql.execute(f'insert into {tbname} values (1653547828591, 100, 100.1, "Beijing", True);')
            self.tdSql.execute(f'insert into {tbname} values (1653547828591+1s, -100, -100.1, "Tianjin", False);')
            self.tdSql.execute(f'insert into {tbname} values (1653547828591+2s, 50, 50.3, "HeBei", False);')

        self.check_stream(f'select start, {output_select_str} from output_downsampling_stb;', f'select _wstartts AS start, {source_select_str} from downsampling_stb interval(10m);', 1)
        self.check_stream(f'select start, {output_select_str} from output_downsampling_ctb;', f'select _wstartts AS start, {source_select_str} from downsampling_ct1 interval(10m);', 1)
        self.check_stream(f'select start, {output_select_str} from output_downsampling_tb;', f'select _wstartts AS start, {source_select_str} from downsampling_tb interval(10m);', 1)
        

        # self.check_stream('select start, `min(c1)`, `max(c2)`, `sum(c1)`, `first(c1)`, `last(c1)`, `apercentile(c1, 50)` from output_downsampling_stb;', 'select _wstartts AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from downsampling_stb interval(10m);', 1)
        # self.check_stream('select start, `min(c1)`, `max(c2)`, `sum(c1)`, `first(c1)`, `last(c1)`, `apercentile(c1, 50)` from output_downsampling_ctb;', 'select _wstartts AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from downsampling_ct1 interval(10m);', 1)
        # self.check_stream('select start, `min(c1)`, `max(c2)`, `sum(c1)`, `first(c1)`, `last(c1)`, `apercentile(c1, 50)` from output_downsampling_tb;', 'select _wstartts AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from downsampling_tb interval(10m);', 1)
        # self.write_latency('sql: select * from output_downsampling_stb;')

        # self.check_stream_res('select * from output_downsampling_stb;', 1)
        # self.check_query_data('select start, `min(c1)`, `max(c2)`, `sum(c1)` from output_downsampling_stb;', 'select _wstartts AS start, min(c1), max(c2), sum(c1) from downsampling_stb interval(10m);')
        count = 1
        step_count = 1
        for i in range(1, 10):
            if i % 2 == 0:
                step_count += i
                for j in range(count, step_count):
                    self.tdSql.execute(f'insert into downsampling_ct1 values (1653547828591+{j}0m, 60, 60.3, "heilongjiang", True);')
                    self.tdSql.execute(f'insert into downsampling_tb values (1653547828591+{j}1m, 70, 70.3, "heilongjiang", True);')
                count += i
                # expectd_res = count - 1
            else:
                step_count += 1
                for i in range(2):
                    self.tdSql.execute(f'insert into downsampling_ct1 values (1653547828591+{count}1m, 60, 60.3, "heilongjiang", True);')
                    self.tdSql.execute(f'insert into downsampling_tb values (1653547828591+{count}1m, 70, 70.3, "heilongjiang", True);')
                count += 1
                # expectd_res = count
            # check result
            self.check_stream(f'select start, {output_select_str} from output_downsampling_stb;', f'select _wstartts AS start, {source_select_str} from downsampling_stb interval(10m);', count)
            self.check_stream(f'select start, {output_select_str} from output_downsampling_ctb;', f'select _wstartts AS start, {source_select_str} from downsampling_ct1 interval(10m);', count)
            self.check_stream(f'select start, {output_select_str} from output_downsampling_tb;', f'select _wstartts AS start, {source_select_str} from downsampling_tb interval(10m);', count)
            # self.check_stream('select start, `min(c1)`, `max(c2)`, `sum(c1)`, `first(c1)`, `last(c1)`, `apercentile(c1, 50)` from output_downsampling_stb;', 'select _wstartts AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from downsampling_stb interval(10m);', count)
            # self.check_stream('select start, `min(c1)`, `max(c2)`, `sum(c1)`, `first(c1)`, `last(c1)`, `apercentile(c1, 50)` from output_downsampling_ctb;', 'select _wstartts AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from downsampling_ct1 interval(10m);', count)
            # self.check_stream('select start, `min(c1)`, `max(c2)`, `sum(c1)`, `first(c1)`, `last(c1)`, `apercentile(c1, 50)` from output_downsampling_tb;', 'select _wstartts AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from downsampling_tb interval(10m);', count)
        #     self.check_stream(f'select {select_elm} from output_data_filter_stb where {filter_sql};', f'select {select_elm} from data_filter_stb where {filter_sql};', count-1)
        #     self.check_stream(f'select {select_elm} from output_data_filter_ctb where {filter_sql};', f'select {select_elm} from data_filter_ct1 where {filter_sql};', count-1)
        #     self.check_stream(f'select {select_elm} from output_data_filter_tb where {filter_sql};', f'select {select_elm} from data_filter_tb where {filter_sql};', count-1)


        # self.tdSql.execute(f'insert into downsampling_ct1 values (1653547828591+10m, 60, 60.3, "heilongjiang", True);')
        # self.tdSql.execute(f'insert into downsampling_ct1 values (1653547828591+11m, 70, 70.3, "JiLin", True);')
        # self.write_latency('sql: select * from output_downsampling_stb;')
        # self.check_stream_res('select * from output_downsampling_stb;', 2)
        # self.check_query_data('select start, `min(c1)`, `max(c2)`, `sum(c1)` from output_downsampling_stb;', 'select _wstartts AS start, min(c1), max(c2), sum(c1) from downsampling_stb interval(10m);')
        # self.tdSql.execute(f'insert into downsampling_ct1 values (1653547828591+21m, 70, 70.3, "JiLin", True);')
        # self.write_latency('sql: select * from output_downsampling_stb;')
        # self.check_stream_res('select * from output_downsampling_stb;', 3)
        # self.check_query_data('select start, `min(c1)`, `max(c2)`, `sum(c1)` from output_downsampling_stb;', 'select _wstartts AS start, min(c1), max(c2), sum(c1) from downsampling_stb interval(10m);')

    def bottom_function(self):
        self.case_name = sys._getframe().f_code.co_name
        self.tdSql.execute('create table if not exists bottom_stb (ts timestamp, c1 int, c2 double, c3 varchar(100), c4 bool) tags (t1 int, t2 double, t3 varchar(100), t4 bool);')
        self.tdSql.execute('create table bottom_ct1 using bottom_stb tags(10, 10.1, "Beijing", True);')
        self.tdSql.execute('create table if not exists bottom_tb (ts timestamp, c1 int, c2 double, c3 varchar(100), c4 bool);')
        self.write_latency(self.case_name)
        # stb
        self.tdSql.execute(f'create stream stb_bottom_stream trigger at_once into output_bottom_stb as select _wstartts AS start, bottom(c1, 2) from bottom_stb interval(10m);')
        # ctb
        self.tdSql.execute(f'create stream ctb_bottom_stream trigger at_once into output_bottom_ctb as select _wstartts AS start, bottom(c1, 2) from bottom_ct1 interval(10m);')
        # tb
        self.tdSql.execute(f'create stream tb_bottom_stream trigger at_once into output_bottom_tb as select _wstartts AS start, bottom(c1, 2) from bottom_tb interval(10m);')
        for tbname in ["bottom_ct1", "bottom_tb"]:
            self.tdSql.execute(f'insert into {tbname} values (1653547828591, 100, 100.1, "Beijing", True);')
            self.tdSql.execute(f'insert into {tbname} values (1653547828591+1s, -100, -100.1, "Tianjin", False);')
            self.tdSql.execute(f'insert into {tbname} values (1653547828591+2s, 50, 50.3, "HeBei", False);')

        self.check_stream('select start, `bottom(c1, 2)` from output_bottom_stb;', 'select _wstartts AS start, bottom(c1, 2) from bottom_stb interval(10m);', 1)
        self.check_stream('select start, `bottom(c1, 2)` from output_bottom_ctb;', 'select _wstartts AS start, bottom(c1, 2) from bottom_ct1 interval(10m);', 1)
        self.check_stream('select start, `bottom(c1, 2)` from output_bottom_tb;', 'select _wstartts AS start, bottom(c1, 2) from bottom_tb interval(10m);', 1)
        count = 1
        step_count = 1
        for i in range(1, 10):
            if i % 2 == 0:
                step_count += i
                for j in range(count, step_count):
                    self.tdSql.execute(f'insert into bottom_ct1 values (1653547828591+{j}0m, 60, 60.3, "heilongjiang", True);')
                    self.tdSql.execute(f'insert into bottom_tb values (1653547828591+{j}1m, 70, 70.3, "heilongjiang", True);')
                count += i
            else:
                step_count += 1
                for i in range(2):
                    self.tdSql.execute(f'insert into bottom_ct1 values (1653547828591+{count}1m, 60, 60.3, "heilongjiang", True);')
                    self.tdSql.execute(f'insert into bottom_tb values (1653547828591+{count}1m, 70, 70.3, "heilongjiang", True);')
                count += 1
            # check result
            self.check_stream('select start, `bottom(c1, 2)` from output_bottom_stb;', 'select _wstartts AS start, bottom(c1, 2) from bottom_stb interval(10m);', count)
            self.check_stream('select start, `bottom(c1, 2)` from output_bottom_ctb;', 'select _wstartts AS start, bottom(c1, 2) from bottom_ct1 interval(10m);', count)
            self.check_stream('select start, `bottom(c1, 2)` from output_bottom_tb;', 'select _wstartts AS start, bottom(c1, 2) from bottom_tb interval(10m);', count)

    def top_function(self):
        self.case_name = sys._getframe().f_code.co_name
        self.tdSql.execute('create table if not exists top_stb (ts timestamp, c1 int, c2 double, c3 varchar(100), c4 bool) tags (t1 int, t2 double, t3 varchar(100), t4 bool);')
        self.tdSql.execute('create table top_ct1 using top_stb tags(10, 10.1, "Beijing", True);')
        self.tdSql.execute('create table if not exists top_tb (ts timestamp, c1 int, c2 double, c3 varchar(100), c4 bool);')

        self.write_latency(self.case_name)
        # stb
        self.tdSql.execute(f'create stream stb_top_stream trigger at_once into output_top_stb as select _wstartts AS start, top(c1, 3) from top_stb interval(10m);')
        # ctb
        self.tdSql.execute(f'create stream ctb_top_stream trigger at_once into output_top_ctb as select _wstartts AS start, top(c1, 3) from top_ct1 interval(10m);')
        # tb
        self.tdSql.execute(f'create stream tb_top_stream trigger at_once into output_top_tb as select _wstartts AS start, top(c1, 3) from top_tb interval(10m);')
        for tbname in ["top_ct1", "top_tb"]:
            self.tdSql.execute(f'insert into {tbname} values (1653547828591, 100, 100.1, "Beijing", True);')
            self.tdSql.execute(f'insert into {tbname} values (1653547828591+1s, -100, -100.1, "Tianjin", False);')
            self.tdSql.execute(f'insert into {tbname} values (1653547828591+2s, 50, 50.3, "HeBei", False);')

        self.check_stream('select start, `top(c1, 3)` from output_top_stb;', 'select _wstartts AS start, top(c1, 3) from top_stb interval(10m);', 1)
        self.check_stream('select start, `top(c1, 3)` from output_top_ctb;', 'select _wstartts AS start, top(c1, 3) from top_ct1 interval(10m);', 1)
        self.check_stream('select start, `top(c1, 3)` from output_top_tb;', 'select _wstartts AS start, top(c1, 3) from top_tb interval(10m);', 1)
        count = 1
        step_count = 1
        for i in range(1, 10):
            if i % 2 == 0:
                step_count += i
                for j in range(count, step_count):
                    self.tdSql.execute(f'insert into top_ct1 values (1653547828591+{j}0m, 60, 60.3, "heilongjiang", True);')
                    self.tdSql.execute(f'insert into top_tb values (1653547828591+{j}1m, 70, 70.3, "heilongjiang", True);')
                count += i
            else:
                step_count += 1
                for i in range(2):
                    self.tdSql.execute(f'insert into top_ct1 values (1653547828591+{count}1m, 60, 60.3, "heilongjiang", True);')
                    self.tdSql.execute(f'insert into top_tb values (1653547828591+{count}1m, 70, 70.3, "heilongjiang", True);')
                count += 1
            # check result
            self.check_stream('select start, `top(c1, 3)` from output_top_stb;', 'select _wstartts AS start, top(c1, 3) from top_stb interval(10m);', count)
            self.check_stream('select start, `top(c1, 3)` from output_top_ctb;', 'select _wstartts AS start, top(c1, 3) from top_ct1 interval(10m);', count)
            self.check_stream('select start, `top(c1, 3)` from output_top_tb;', 'select _wstartts AS start, top(c1, 3) from top_tb interval(10m);', count)

    def state_window_function(self):
        self.case_name = sys._getframe().f_code.co_name
        self.tdSql.execute('create table if not exists state_window_stb (ts timestamp, c1 int, c2 double, c3 varchar(100), c4 bool) tags (t1 int, t2 double, t3 varchar(100), t4 bool);')
        self.tdSql.execute('create table state_window_ct1 using state_window_stb tags(10, 10.1, "Beijing", True);')
        self.tdSql.execute('create table if not exists state_window_tb (ts timestamp, c1 int, c2 double, c3 varchar(100), c4 bool);')
        
        self.write_latency(self.case_name)
        # stb not supported
        # self.tdSql.execute(f'create stream stb_state_window_stream into output_state_window_stb as select _wstartts AS start, max(c1) from state_window_stb state_window(c1);')
        # ctb
        self.tdSql.execute(f'create stream ctb_state_window_stream trigger at_once into output_state_window_ctb as select _wstartts AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from state_window_ct1 state_window(c1);')
        # tb
        self.tdSql.execute(f'create stream tb_state_window_stream trigger at_once into output_state_window_tb as select _wstartts AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from state_window_tb state_window(c1);')
        for tbname in ["state_window_ct1", "state_window_tb"]:
            self.tdSql.execute(f'insert into {tbname} values (1653547828591, 100, 100.1, "Beijing", True);')
            self.tdSql.execute(f'insert into {tbname} values (1653547828591+1s, -100, -100.1, "Tianjin", False);')
            self.tdSql.execute(f'insert into {tbname} values (1653547828591+2s, 50, 50.3, "HeBei", False);')

        # self.check_stream('select start, `min(c1)` from output_state_window_stb;', 'select _wstartts AS start, max(c1) from state_window_stb state_window(c1);', 1)
        self.check_stream('select start, `min(c1)`, `max(c2)`, `sum(c1)`, `first(c1)`, `last(c1)`, `apercentile(c1, 50)` from output_state_window_ctb;', 'select _wstartts AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from state_window_ct1 state_window(c1);', 3)
        self.check_stream('select start, `min(c1)`, `max(c2)`, `sum(c1)`, `first(c1)`, `last(c1)`, `apercentile(c1, 50)` from output_state_window_tb;', 'select _wstartts AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from state_window_tb state_window(c1);', 3)
        count = 3
        step_count = 1
        for i in range(1, 10):
            if i % 2 == 0:
                step_count += i
                for j in range(count, step_count):
                    self.tdSql.execute(f'insert into state_window_ct1 values (1653547828591+{j}0m, 60, 60.3, "heilongjiang", True);')
                    self.tdSql.execute(f'insert into state_window_tb values (1653547828591+{j}1m, 70, 70.3, "heilongjiang", True);')
                count += i
            else:
                step_count += 1
                for i in range(2):
                    self.tdSql.execute(f'insert into state_window_ct1 values (1653547828591+{count}1m, 60, 60.3, "heilongjiang", True);')
                    self.tdSql.execute(f'insert into state_window_tb values (1653547828591+{count}1m, 70, 70.3, "heilongjiang", True);')
                count += 1
            # check result
            # stb not supported
            # self.check_stream('select start, `min(c1)` from output_state_window_stb;', 'select _wstartts AS start, max(c1) from state_window_stb state_window(c1);', 1)
            self.check_stream('select start, `min(c1)`, `max(c2)`, `sum(c1)`, `first(c1)`, `last(c1)`, `apercentile(c1, 50)` from output_state_window_ctb;', 'select _wstartts AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from state_window_ct1 state_window(c1);', 4)
            self.check_stream('select start, `min(c1)`, `max(c2)`, `sum(c1)`, `first(c1)`, `last(c1)`, `apercentile(c1, 50)` from output_state_window_tb;', 'select _wstartts AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from state_window_tb state_window(c1);', 4)

    def session_window(self):
        self.case_name = sys._getframe().f_code.co_name
        self.tdSql.execute('create table if not exists session_window_stb (ts timestamp, c1 int, c2 double, c3 varchar(100), c4 bool) tags (t1 int, t2 double, t3 varchar(100), t4 bool);')
        self.tdSql.execute('create table session_window_ct1 using session_window_stb tags(10, 10.1, "Beijing", True);')
        self.tdSql.execute('create table if not exists session_window_tb (ts timestamp, c1 int, c2 double, c3 varchar(100), c4 bool);')
        self.write_latency(self.case_name)
        function_list = ["min(c1)", "max(c2)", "sum(c1)", "first(c1)", "last(c1)", "apercentile(c1, 50)"]
        for test_function in function_list:
            # # stb
            # self.tdSql.execute(f'create stream stb_session_window_stream into output_session_window_stb as select min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from session_window_stb session(ts, 10m);')
            # ctb
            function_name = test_function.split('(')[0]
            self.tdSql.execute(f'create stream ctb_session_window_{function_name}_stream trigger at_once into output_session_window_{function_name}_ctb as select ts, {test_function} from session_window_ct1 session(ts, 10m);')
            # tb
            self.tdSql.execute(f'create stream tb_session_window_{function_name}_stream trigger at_once into output_session_window_{function_name}_tb as select ts, {test_function} from session_window_tb session(ts, 10m);')
            for tbname in ["session_window_ct1", "session_window_tb"]:
                self.tdSql.execute(f'insert into {tbname} values (1653547828591, 100, 100.1, "Beijing", True);')
                self.tdSql.execute(f'insert into {tbname} values (1653547828591+1s, -100, -100.1, "Tianjin", False);')
                self.tdSql.execute(f'insert into {tbname} values (1653547828591+2s, 50, 50.3, "HeBei", False);')

            # self.check_stream('select `min(c1)`, `max(c2)`, `sum(c1)`, `first(c1)`, `last(c1)`, `apercentile(c1, 50)` from output_session_window_stb;', 'select min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from session_window_stb session(ts, 10m);', 1)
            self.check_stream(f'select ts, `{test_function}` from output_session_window_{function_name}_ctb;', f'select ts, {test_function} from session_window_ct1 session(ts, 10m);', 2)
            self.check_stream(f'select ts, `{test_function}` from output_session_window_{function_name}_tb;', f'select ts, {test_function} from session_window_tb session(ts, 10m);', 2)
            # self.write_latency('sql: select * from output_session_window_stb;')

            # self.check_stream_res('select * from output_session_window_stb;', 1)
            # self.check_query_data('select `min(c1)`, `max(c2)`, `sum(c1)` from output_session_window_stb;', 'select min(c1), max(c2), sum(c1) from session_window_stb session(ts, 10m);')
            count = 2
            step_count = 1
            for i in range(1, 10):
                if i % 2 == 0:
                    step_count += i
                    for j in range(count, step_count):
                        self.tdSql.execute(f'insert into session_window_ct1 values (1653547828591+{j}0m, 60, 60.3, "heilongjiang", True);')
                        self.tdSql.execute(f'insert into session_window_tb values (1653547828591+{j}1m, 70, 70.3, "heilongjiang", True);')
                    count += i
                    # expectd_res = count - 1
                else:
                    step_count += 1
                    for i in range(2):
                        self.tdSql.execute(f'insert into session_window_ct1 values (1653547828591+{count}1m, 60, 60.3, "heilongjiang", True);')
                        self.tdSql.execute(f'insert into session_window_tb values (1653547828591+{count}1m, 70, 70.3, "heilongjiang", True);')
                    count += 1
                    # expectd_res = count
                # check result
                # self.check_stream('select `min(c1)`, `max(c2)`, `sum(c1)`, `first(c1)`, `last(c1)`, `apercentile(c1, 50)` from output_session_window_stb;', 'select min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from session_window_stb session(ts, 10m);', count)
                self.check_stream(f'select ts, `{test_function}` from output_session_window_{function_name}_ctb;', f'select ts, {test_function} from session_window_ct1 session(ts, 10m);', count)
                self.check_stream(f'select ts, `{test_function}` from output_session_window_{function_name}_tb;', f'select ts, {test_function} from session_window_tb session(ts, 10m);', count)

    def scalar_function(self):
        # self.prepare_stream_data()
        self.case_name = sys._getframe().f_code.co_name
        self.tdSql.execute('create table if not exists scalar_stb (ts timestamp, c1 int, c2 double, c3 binary(20), c4 binary(20), c5 nchar(20)) tags (t1 int);')
        self.tdSql.execute('create table scalar_ct1 using scalar_stb tags(10);')
        # self.tdSql.execute(f'create table scalar_ct2 using scalar_stb tags(-20);')
        # self.tdSql.execute(f'create table scalar_ct3 using scalar_stb tags(0);')
        self.tdSql.execute('create table if not exists scalar_tb (ts timestamp, c1 int, c2 double, c3 binary(20), c4 binary(20), c5 nchar(20));')

        self.write_latency(self.case_name)
        math_function_list = ["abs", "acos", "asin", "atan", "ceil", "cos", "floor", "log", "pow", "round", "sin", "sqrt", "tan"]
        # string_function_list = ["lower", "ltrim", "rtrim", "substr", "upper"]
        # ! TD-16624 commit out
        # string_function_list = ["char_length", "concat", "concat_ws", "length", "lower", "ltrim", "rtrim", "substr", "upper"]
        # string_function_list = ["char_length", "concat", "concat_ws", "length", "lower", "ltrim", "rtrim", "substr"]
        for math_function in math_function_list:
            if math_function in ["log", "pow"]:
                self.tdSql.execute(f'create stream stb_{math_function}_stream trigger at_once into output_{math_function}_stb as select ts, {math_function}(c1, 2), {math_function}(c2, 2), c3 from scalar_stb;')
                self.tdSql.execute(f'create stream ctb_{math_function}_stream trigger at_once into output_{math_function}_ctb as select ts, {math_function}(c1, 2), {math_function}(c2, 2), c3 from scalar_ct1;')
                self.tdSql.execute(f'create stream tb_{math_function}_stream trigger at_once into output_{math_function}_tb as select ts, {math_function}(c1, 2), {math_function}(c2, 2), c3 from scalar_tb;')
            else:
                self.tdSql.execute(f'create stream stb_{math_function}_stream trigger at_once into output_{math_function}_stb as select ts, {math_function}(c1), {math_function}(c2), c3 from scalar_stb;')
                self.tdSql.execute(f'create stream ctb_{math_function}_stream trigger at_once into output_{math_function}_ctb as select ts, {math_function}(c1), {math_function}(c2), c3 from scalar_ct1;')
                self.tdSql.execute(f'create stream tb_{math_function}_stream trigger at_once into output_{math_function}_tb as select ts, {math_function}(c1), {math_function}(c2), c3 from scalar_tb;')
            self.check_stream_field_type(f"describe output_{math_function}_stb", math_function)
            self.check_stream_field_type(f"describe output_{math_function}_ctb", math_function)
            self.check_stream_field_type(f"describe output_{math_function}_tb", math_function)
        # for string_function in string_function_list:
        #     if string_function == "concat":
        #         self.tdSql.execute(f'create stream stb_{string_function}_stream into output_{string_function}_stb as select ts, {string_function}(c3, c4), {string_function}(c3, c5), {string_function}(c4, c5), {string_function}(c3, c4, c5) from scalar_stb;')
        #         self.tdSql.execute(f'create stream ctb_{string_function}_stream into output_{string_function}_ctb as select ts, {string_function}(c3, c4), {string_function}(c3, c5), {string_function}(c4, c5), {string_function}(c3, c4, c5) from scalar_ct1;')
        #         self.tdSql.execute(f'create stream tb_{string_function}_stream into output_{string_function}_tb as select ts, {string_function}(c3, c4), {string_function}(c3, c5), {string_function}(c4, c5), {string_function}(c3, c4, c5) from scalar_tb;')
        #     elif string_function == "concat_ws":
        #         self.tdSql.execute(f'create stream stb_{string_function}_stream into output_{string_function}_stb as select ts, {string_function}("aND", c3, c4), {string_function}("and", c3, c5), {string_function}("And", c4, c5), {string_function}("AND", c3, c4, c5) from scalar_stb;')
        #         self.tdSql.execute(f'create stream ctb_{string_function}_stream into output_{string_function}_ctb as select ts, {string_function}("aND", c3, c4), {string_function}("and", c3, c5), {string_function}("And", c4, c5), {string_function}("AND", c3, c4, c5) from scalar_ct1;')
        #         self.tdSql.execute(f'create stream tb_{string_function}_stream into output_{string_function}_tb as select ts, {string_function}("aND", c3, c4), {string_function}("and", c3, c5), {string_function}("And", c4, c5), {string_function}("AND", c3, c4, c5) from scalar_tb;')
        #     elif string_function == "substr":
        #         self.tdSql.execute(f'create stream stb_{string_function}_stream into output_{string_function}_stb as select ts, {string_function}(c3, 2), {string_function}(c3, 2, 2), {string_function}(c4, 5, 1), {string_function}(c5, 3, 4) from scalar_stb;')
        #         self.tdSql.execute(f'create stream ctb_{string_function}_stream into output_{string_function}_ctb as select ts, {string_function}(c3, 2), {string_function}(c3, 2, 2), {string_function}(c4, 5, 1), {string_function}(c5, 3, 4) from scalar_ct1;')
        #         self.tdSql.execute(f'create stream tb_{string_function}_stream into output_{string_function}_tb as select ts, {string_function}(c3, 2), {string_function}(c3, 2, 2), {string_function}(c4, 5, 1), {string_function}(c5, 3, 4) from scalar_tb;')
        #     else:
        #         self.tdSql.execute(f'create stream stb_{string_function}_stream into output_{string_function}_stb as select ts, {string_function}(c3), {string_function}(c4), {string_function}(c5) from scalar_stb;')
        #         self.tdSql.execute(f'create stream ctb_{string_function}_stream into output_{string_function}_ctb as select ts, {string_function}(c3), {string_function}(c4), {string_function}(c5) from scalar_ct1;')
        #         self.tdSql.execute(f'create stream tb_{string_function}_stream into output_{string_function}_tb as select ts, {string_function}(c3), {string_function}(c4), {string_function}(c5) from scalar_tb;')
        #     self.check_stream_field_type(f"describe output_{string_function}_stb", string_function)
        #     self.check_stream_field_type(f"describe output_{string_function}_ctb", string_function)
        #     self.check_stream_field_type(f"describe output_{string_function}_tb", string_function)

        # for tbname in ["scalar_ct1", "scalar_tb"]:
        #     self.tdSql.execute(f'insert into {tbname} values ({self.date_time}, 100, 100.1, "beijing", "taos", "Taos");')
        #     self.tdSql.execute(f'insert into {tbname} values ({self.date_time}+1s, -50, -50.1, "tianjin", "taosdata", "Taosdata");')
        #     self.tdSql.execute(f'insert into {tbname} values ({self.date_time}+2s, 0, Null, "hebei", "TDengine", Null);')

        count = 1
        step_count = 1
        for i in range(1, 20):
            if i % 2 == 0:
                step_count += i
                for j in range(count, step_count):
                    self.tdSql.execute(f'insert into scalar_ct1 values ({self.date_time}+{j}s, 100, -100.1, "hebei", Null, "Bigdata");')
                    self.tdSql.execute(f'insert into scalar_tb values ({self.date_time}+{j}s, 100, -100.1, "heBei", Null, "Bigdata");')
                count += i
            else:
                step_count += 1
                for i in range(2):
                    self.tdSql.execute(f'insert into scalar_ct1 values ({self.date_time}+{count}s, -50, 50.1, "beiJing", "TDengine", "taos");')
                    self.tdSql.execute(f'insert into scalar_tb values ({self.date_time}+{count}s, -50, 50.1, "beiJing", "TDengine", "taos");')
                count += 1
            for math_function in math_function_list:
                if math_function == "log" or math_function == "pow":
                    self.check_stream(f'select `{math_function}(c1, 2)`, `{math_function}(c2, 2)` from output_{math_function}_stb;', f'select {math_function}(c1, 2), {math_function}(c2, 2) from scalar_stb;', count-1)
                    self.check_stream(f'select `{math_function}(c1, 2)`, `{math_function}(c2, 2)` from output_{math_function}_ctb;', f'select {math_function}(c1, 2), {math_function}(c2, 2) from scalar_ct1;', count-1)
                    self.check_stream(f'select `{math_function}(c1, 2)`, `{math_function}(c2, 2)` from output_{math_function}_tb;', f'select {math_function}(c1, 2), {math_function}(c2, 2) from scalar_tb;', count-1)
                else:
                    self.check_stream(f'select `{math_function}(c1)`, `{math_function}(c2)` from output_{math_function}_stb;', f'select {math_function}(c1), {math_function}(c2) from scalar_stb;', count-1)
                    self.check_stream(f'select `{math_function}(c1)`, `{math_function}(c2)` from output_{math_function}_ctb;', f'select {math_function}(c1), {math_function}(c2) from scalar_ct1;', count-1)
                    self.check_stream(f'select `{math_function}(c1)`, `{math_function}(c2)` from output_{math_function}_tb;', f'select {math_function}(c1), {math_function}(c2) from scalar_tb;', count-1)
            # for string_function in string_function_list:
            #     if string_function == "concat":
            #         self.check_stream(f'select `{string_function}(c3, c4)`, `{string_function}(c3, c5)`, `{string_function}(c4, c5)`, `{string_function}(c3, c4, c5)` from output_{string_function}_stb;', f'select {string_function}(c3, c4), {string_function}(c3, c5), {string_function}(c4, c5), {string_function}(c3, c4, c5) from scalar_stb;', count-1)
            #         self.check_stream(f'select `{string_function}(c3, c4)`, `{string_function}(c3, c5)`, `{string_function}(c4, c5)`, `{string_function}(c3, c4, c5)` from output_{string_function}_ctb;', f'select {string_function}(c3, c4), {string_function}(c3, c5), {string_function}(c4, c5), {string_function}(c3, c4, c5) from scalar_ct1;', count-1)
            #         self.check_stream(f'select `{string_function}(c3, c4)`, `{string_function}(c3, c5)`, `{string_function}(c4, c5)`, `{string_function}(c3, c4, c5)` from output_{string_function}_tb;', f'select {string_function}(c3, c4), {string_function}(c3, c5), {string_function}(c4, c5), {string_function}(c3, c4, c5) from scalar_tb;', count-1)
            #     elif string_function == "concat_ws":
            #         self.check_stream(f'select `{string_function}("aND", c3, c4)`, `{string_function}("and", c3, c5)`, `{string_function}("And", c4, c5)`, `{string_function}("AND", c3, c4, c5)` from output_{string_function}_stb;', f'select {string_function}("aND", c3, c4), {string_function}("and", c3, c5), {string_function}("And", c4, c5), {string_function}("AND", c3, c4, c5) from scalar_stb;', count-1)
            #         self.check_stream(f'select `{string_function}("aND", c3, c4)`, `{string_function}("and", c3, c5)`, `{string_function}("And", c4, c5)`, `{string_function}("AND", c3, c4, c5)` from output_{string_function}_ctb;', f'select {string_function}("aND", c3, c4), {string_function}("and", c3, c5), {string_function}("And", c4, c5), {string_function}("AND", c3, c4, c5) from scalar_ct1;', count-1)
            #         self.check_stream(f'select `{string_function}("aND", c3, c4)`, `{string_function}("and", c3, c5)`, `{string_function}("And", c4, c5)`, `{string_function}("AND", c3, c4, c5)` from output_{string_function}_tb;', f'select {string_function}("aND", c3, c4), {string_function}("and", c3, c5), {string_function}("And", c4, c5), {string_function}("AND", c3, c4, c5) from scalar_tb;', count-1)
            #     elif string_function == "substr":
            #         self.check_stream(f'select `{string_function}(c3, 2)`, `{string_function}(c3, 2, 2)`, `{string_function}(c4, 5, 1)`, `{string_function}(c5, 3, 4)` from output_{string_function}_stb;', f'select {string_function}(c3, 2), {string_function}(c3, 2, 2), {string_function}(c4, 5, 1), {string_function}(c5, 3, 4) from scalar_stb;', count-1)
            #         self.check_stream(f'select `{string_function}(c3, 2)`, `{string_function}(c3, 2, 2)`, `{string_function}(c4, 5, 1)`, `{string_function}(c5, 3, 4)` from output_{string_function}_ctb;', f'select {string_function}(c3, 2), {string_function}(c3, 2, 2), {string_function}(c4, 5, 1), {string_function}(c5, 3, 4) from scalar_ct1;', count-1)
            #         self.check_stream(f'select `{string_function}(c3, 2)`, `{string_function}(c3, 2, 2)`, `{string_function}(c4, 5, 1)`, `{string_function}(c5, 3, 4)` from output_{string_function}_tb;', f'select {string_function}(c3, 2), {string_function}(c3, 2, 2), {string_function}(c4, 5, 1), {string_function}(c5, 3, 4) from scalar_tb;', count-1)
            #     else:
            #         self.check_stream(f'select `{string_function}(c3)`, `{string_function}(c4)`, `{string_function}(c5)` from output_{string_function}_stb;', f'select {string_function}(c3), {string_function}(c4), {string_function}(c5) from scalar_stb;', count-1)
            #         self.check_stream(f'select `{string_function}(c3)`, `{string_function}(c4)`, `{string_function}(c5)` from output_{string_function}_ctb;', f'select {string_function}(c3), {string_function}(c4), {string_function}(c5) from scalar_ct1;', count-1)
            #         self.check_stream(f'select `{string_function}(c3)`, `{string_function}(c4)`, `{string_function}(c5)` from output_{string_function}_tb;', f'select {string_function}(c3), {string_function}(c4), {string_function}(c5) from scalar_tb;', count-1)
            
        # count = 1
        # step_count = 1
        # for i in range(1, 20):
        #     if i % 2 == 0:
        #         step_count += i
        #         for j in range(count, step_count):
        #             self.tdSql.execute(f'insert into scalar_ct1 values ({self.date_time}+{j}s, -1, 1, "hebei", Null, "Bigdata");')
        #             self.tdSql.execute(f'insert into scalar_tb values ({self.date_time}+{j}s, -1, 1, "hebei", Null, "Bigdata");')
        #         count += i
        #     else:
        #         step_count += 1
        #         for i in range(2):
        #             self.tdSql.execute(f'insert into scalar_ct1 values ({self.date_time}+{count}s, -1, 1, "hebei", Null, "Bigdata");')
        #             self.tdSql.execute(f'insert into scalar_tb values ({self.date_time}+{count}s, -1, 1, "hebei", Null, "Bigdata");')
        #         count += 1
        #     # check result
        #     self.check_stream(f'select {select_elm} from output_data_filter_stb where {filter_sql};', f'select {select_elm} from data_filter_stb where {filter_sql};', count-1)
        #     self.check_stream(f'select {select_elm} from output_data_filter_ctb where {filter_sql};', f'select {select_elm} from data_filter_ct1 where {filter_sql};', count-1)
        #     self.check_stream(f'select {select_elm} from output_data_filter_tb where {filter_sql};', f'select {select_elm} from data_filter_tb where {filter_sql};', count-1)       



        # self.tdSql.execute(f'insert into scalar_ct1 values ({self.date_time}+3s, -1, 1, "hebei", Null, "Bigdata");')
        # for math_function in math_function_list:
        #     if math_function == "log" or math_function == "pow":
        #         self.check_stream(f'select `{math_function}(c1, 2)`, `{math_function}(c2, 2)` from output_{math_function}_stb;', f'select {math_function}(c1, 2), {math_function}(c2, 2) from scalar_stb;', 4)
        #     else:
        #         self.check_stream(f'select `{math_function}(c1)`, `{math_function}(c2)` from output_{math_function}_stb;', f'select {math_function}(c1), {math_function}(c2) from scalar_stb;', 4)
        
        # for string_function in string_function_list:
        #     if string_function == "concat":
        #         self.check_stream(f'select `{string_function}(c3, c4)`, `{string_function}(c3, c5)`, `{string_function}(c4, c5)`, `{string_function}(c3, c4, c5)` from output_{string_function}_stb;', f'select {string_function}(c3, c4), {string_function}(c3, c5), {string_function}(c4, c5), {string_function}(c3, c4, c5) from scalar_stb;', 4)
        #     elif string_function == "concat_ws":
        #         self.check_stream(f'select `{string_function}("aND", c3, c4)`, `{string_function}("and", c3, c5)`, `{string_function}("And", c4, c5)`, `{string_function}("AND", c3, c4, c5)` from output_{string_function}_stb;', f'select {string_function}("aND", c3, c4), {string_function}("and", c3, c5), {string_function}("And", c4, c5), {string_function}("AND", c3, c4, c5) from scalar_stb;', 4)
        #     elif string_function == "substr":
        #         self.check_stream(f'select `{string_function}(c3, 2)`, `{string_function}(c3, 2, 2)`, `{string_function}(c4, 5, 1)`, `{string_function}(c5, 3, 4)` from output_{string_function}_stb;', f'select {string_function}(c3, 2), {string_function}(c3, 2, 2), {string_function}(c4, 5, 1), {string_function}(c5, 3, 4) from scalar_stb;', 4)
        #     else:
        #         self.check_stream(f'select `{string_function}(c3)`, `{string_function}(c4)`, `{string_function}(c5)` from output_{string_function}_stb;', f'select {string_function}(c3), {string_function}(c4), {string_function}(c5) from scalar_stb;', 4)

    def life_cycle(self):
        self.case_name = sys._getframe().f_code.co_name
        self.tdCom.createDb(dbname="long_life_cycle", vgroups=1, duration="14400m", keep="14400m")
        self.tdCom.createDb(dbname="short_life_cycle", vgroups=1, duration="7200m", keep="7200m")
        self.tdSql.execute('create table if not exists long_life_cycle.life_cycle_stb (ts timestamp, c1 int, c2 double, c3 binary(20), c4 nchar(20), c5 nchar(20)) tags (t1 int);')
        self.tdSql.execute('create table long_life_cycle.life_cycle_ct1 using long_life_cycle.life_cycle_stb tags(10);')
        self.tdSql.execute('create table if not exists short_life_cycle.life_cycle_stb (ts timestamp, c1 int, c2 double, c3 binary(20), c4 nchar(20), c5 nchar(20)) tags (t1 int);')
        self.tdSql.execute('create table short_life_cycle.life_cycle_ct1 using short_life_cycle.life_cycle_stb tags(10);')
        self.tdSql.execute('create table if not exists long_life_cycle.life_cycle_tb (ts timestamp, c1 int, c2 double, c3 binary(20), c4 nchar(20), c5 nchar(20));')
        self.tdSql.execute('create table if not exists short_life_cycle.life_cycle_tb (ts timestamp, c1 int, c2 double, c3 binary(20), c4 nchar(20), c5 nchar(20));')

        self.write_latency(self.case_name)
        # stb
        self.tdSql.execute(f'create stream stb_life_cycle_stream trigger at_once into short_life_cycle.output_life_cycle_stb as select * from long_life_cycle.life_cycle_stb;')
        # ctb
        self.tdSql.execute(f'create stream ctb_life_cycle_stream trigger at_once into short_life_cycle.output_life_cycle_ctb as select * from long_life_cycle.life_cycle_ct1;')
        # # tb
        self.tdSql.execute(f'create stream tb_life_cycle_stream trigger at_once into short_life_cycle.output_life_cycle_tb as select * from long_life_cycle.life_cycle_tb;')
        
        self.tdSql.execute(f'insert into long_life_cycle.life_cycle_ct1 values ({self.date_time}, 100, 100.1, "beijing", "taos", "Taos");')
        self.tdSql.execute(f'insert into long_life_cycle.life_cycle_ct1 values ({self.date_time}-1d, -50, -50.1, "tianjin", "taosdata", "Taosdata");')
        self.tdSql.execute(f'insert into long_life_cycle.life_cycle_ct1 values ({self.date_time}-2d, 0, Null, "hebei", "TDengine", Null);')
        self.tdSql.execute(f'insert into long_life_cycle.life_cycle_tb values ({self.date_time}, 100, 100.1, "beijing", "taos", "Taos");')
        self.tdSql.execute(f'insert into long_life_cycle.life_cycle_tb values ({self.date_time}-1d, -50, -50.1, "tianjin", "taosdata", "Taosdata");')
        self.tdSql.execute(f'insert into long_life_cycle.life_cycle_tb values ({self.date_time}-2d, 0, Null, "hebei", "TDengine", Null);')
        self.check_stream('select ts, c1, c2, c3, c4, c5 from short_life_cycle.output_life_cycle_stb;', 'select ts, c1, c2, c3, c4, c5 from long_life_cycle.life_cycle_stb;', 3)
        self.check_stream('select ts, c1, c2, c3, c4, c5 from short_life_cycle.output_life_cycle_ctb;', 'select ts, c1, c2, c3, c4, c5 from long_life_cycle.life_cycle_ct1;', 3)
        self.check_stream('select ts, c1, c2, c3, c4, c5 from short_life_cycle.output_life_cycle_tb;', 'select ts, c1, c2, c3, c4, c5 from long_life_cycle.life_cycle_tb;', 3)
        self.tdSql.execute(f'insert into long_life_cycle.life_cycle_ct1 values ({self.date_time}-7d, -50, -50.1, "tianjin", "taosdata", "Taosdata");')
        self.tdSql.execute(f'insert into long_life_cycle.life_cycle_tb values ({self.date_time}-7d, -50, -50.1, "tianjin", "taosdata", "Taosdata");')
        for tbname in ["stb", "ct1", "tb"]:
            self.tdSql.query(f'select ts, c1, c2, c3, c4, c5 from long_life_cycle.life_cycle_{tbname};')
            self.tdSql.checkEqual(self.tdSql.query_row, 4)
            if tbname == "ct1":
                tbname = "ctb"
            self.tdSql.query(f'select ts, c1, c2, c3, c4, c5 from short_life_cycle.output_life_cycle_{tbname};')
            self.tdSql.checkEqual(self.tdSql.query_row, 3)

    def stream_tandem(self):
        self.case_name = sys._getframe().f_code.co_name
        self.tdSql.execute('create table if not exists tandem_stb1 (ts timestamp, c1 int, c2 double, c3 binary(20), c4 binary(20), c5 nchar(20)) tags (t1 int);')
        self.tdSql.execute('create table tandem_ct1 using tandem_stb1 tags(1);')
        self.tdSql.execute('create table if not exists tandem_stb2 (ts timestamp, c1 int, c2 double, c3 binary(20), c4 binary(20), c5 nchar(20)) tags (t1 int);')
        self.tdSql.execute('create table tandem_ct2 using tandem_stb2 tags(1);')
        self.write_latency(self.case_name)
        self.tdSql.execute(f'create stream tandem_stream1 trigger at_once into output_tandem_stream_stb1 as select ts, concat(c3, c4) c3, concat(c3, c5) c4 , concat(c4, c5) c5 from tandem_stb1;')
        self.tdSql.execute(f'create stream tandem_stream2 trigger at_once into output_tandem_stream_stb2 as select ts, char_length(c3) c3, char_length(c4) c4, char_length(c5) c5 from output_tandem_stream_stb1;')
        self.tdSql.execute(f'insert into tandem_ct1 values ({self.date_time}, 100, 100.1, "beijing", "taos", "Taos");')
        self.tdSql.execute(f'insert into tandem_ct1 values ({self.date_time}+1s, -50, -50.1, "tianjin", "taosdata", "Taosdata");')
        self.tdSql.execute(f'insert into tandem_ct1 values ({self.date_time}+2s, 0, Null, "hebei", "TDengine", Null);')

    def disorder_data(self):
        self.case_name = sys._getframe().f_code.co_name
        self.tdSql.execute('create table if not exists disorder_data_stb (ts timestamp, c1 int, c2 double, c3 varchar(100), c4 bool) tags (t1 int, t2 double, t3 varchar(100), t4 bool);')
        self.tdSql.execute('create table disorder_data_ct1 using disorder_data_stb tags(10, 10.1, "Beijing", True);')
        self.tdSql.execute('create table if not exists disorder_data_tb (ts timestamp, c1 int, c2 double, c3 varchar(100), c4 bool);')
        self.write_latency(self.case_name)
        # # stb
        # self.tdSql.execute(f'create stream stb_disorder_data_stream into output_disorder_data_stb as select count(*) from disorder_data_stb;')
        # # ctb
        # self.tdSql.execute(f'create stream ctb_disorder_data_stream into output_disorder_data_ctb as select count(*) from disorder_data_ct1;')
        # # tb
        # self.tdSql.execute(f'create stream tb_disorder_data_stream into output_disorder_data_tb as select count(*) from disorder_data_tb;')
        # stb
        self.tdSql.execute(f'create stream stb_disorder_data_stream into output_disorder_data_stb as select * from disorder_data_stb;')
        # ctb
        self.tdSql.execute(f'create stream ctb_disorder_data_stream into output_disorder_data_ctb as select * from disorder_data_ct1;')
        # tb
        self.tdSql.execute(f'create stream tb_disorder_data_stream into output_disorder_data_tb as select * from disorder_data_tb;')
        for tbname in ["disorder_data_ct1", "disorder_data_tb"]:
            self.tdSql.execute(f'insert into {tbname} values (1653547828591, 100, 100.1, "Beijing", True);')
            self.tdSql.execute(f'insert into {tbname} values (1653547828591+1s, -100, -100.1, "Tianjin", False);')
            self.tdSql.execute(f'insert into {tbname} values (1653547828591+2s, 50, 50.3, "HeBei", False);')

        self.check_stream('select count(*) from output_disorder_data_stb;', 'select count(*) from disorder_data_stb;', 1)
        self.check_stream('select count(*) from output_disorder_data_ctb;', 'select count(*) from disorder_data_ct1;', 1)
        self.check_stream('select count(*) from output_disorder_data_tb;', 'select count(*) from disorder_data_tb;', 1)
        timestamp = self.tdCom.genTs("ms")[0]
        ts_list = list()
        for i in range(1, 98):
            ts = timestamp - 1000 + i
            ts_list.append(ts)
        random.shuffle(ts_list)
        # ts_counter = 3
        for ts in ts_list:
            # ts_counter += 1
            self.tdSql.execute(f'insert into disorder_data_ct1 values ({ts}, 60, 60.3, "heilongjiang", True)')
            self.tdSql.execute(f'insert into disorder_data_tb values ({ts}, 60, 60.3, "heilongjiang", True)')
            self.check_stream('select count(*) from output_disorder_data_stb;', 'select count(*) from disorder_data_stb;', 1)
            self.check_stream('select count(*) from output_disorder_data_ctb;', 'select count(*) from disorder_data_ct1;', 1)
            self.check_stream('select count(*) from output_disorder_data_tb;', 'select count(*) from disorder_data_tb;', 1)

    def run(self) -> bool:
        self.downsampling()
        # # self.bottom_function()
        # # self.top_function()
        self.state_window_function()
        # self.session_window()
        # # # ! TD-16145
        # self.scalar_function()
        self.data_filter()
        self.life_cycle()
        # # ! TD-16617
        # # self.stream_tandem()
        # self.disorder_data()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            vgroups check <jayden>: [TD-14991] : vgroups check;
            """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Database.Create, T.Write.TaoscSql.Database.Alter

