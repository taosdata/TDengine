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
        self.tbname = None
        self.date_time = self.tdCom.genTs()[0]
        self.tdCom.stream_latency_log = self.run_log_dir + "/latency.log"
        self.tdCom.drop_all_streams()
        self.tdCom.drop_all_db()
        self.tdCom.createDb(vgroups=10)
        self.range_count = 20
        self.des_table_suffix = "_output"
        self.stream_suffix = "_stream"
        # ! TD-16571 histogram
        # ! TD-16570 last_row(c1)
        # ! now() timezone() to_iso8601(1)
        self.downsampling_function_list = ["min(c1)", "max(c2)", "sum(c3)", "first(c4)", "last(c5)", "apercentile(c6, 50)", "avg(c7)", "count(c8)", "leastsquares(c1, 1, 2)", "spread(c1)", 
        "stddev(c2)", "hyperloglog(c11)", "timediff(1, 0, 1h)", "timetruncate(_wstartts, 1m)", "to_iso8601(1)", 'to_unixtimestamp("1970-01-01T08:00:00+08:00")']

    def data_filter(self):
        self.case_name = sys._getframe().f_code.co_name
        dataDict = {
            "stb_name" : f"{self.case_name}_stb",
            "ctb_name" : f"{self.case_name}_ct1",
            "tb_name" : f"{self.case_name}_tb1",
            "source_select_elm" : "*",
            "des_select_elm" : "c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13",
            "filter_sql" : f'ts >= {self.date_time}+1s and c1 = 1 or c2 > 1 and c3 != 4 or c4 <= 3 and c9 <> 0 or c10 is not Null or c11 is Null or \
                c12 between "na" and "nchar4" and 118 not between "bi" and "binary" and c12 match "nchar[19]" and c12 nmatch "nchar[25]" or c13 = True or \
                c5 in (1, 2, 3) or c6 not in (6, 7) and c12 like "nch%" and c11 not like "bina_" and c6 <= 10 or c12 is Null or c8 >= 4'
        }

        # create stb/ctb/tb
        self.tdCom.create_stable(stbname=dataDict["stb_name"])
        self.tdCom.create_ctable(stbname=dataDict["stb_name"], ctbname=dataDict["ctb_name"])
        self.tdCom.create_table(tbname=dataDict["tb_name"])

        self.tdCom.write_latency(self.case_name)
        
        # create stb/ctb/tb stream
        self.tdCom.create_stream(stream_name=f'{dataDict["stb_name"]}{self.stream_suffix}', des_table=f'{dataDict["stb_name"]}{self.des_table_suffix}', source_sql=f'select {dataDict["source_select_elm"]} from {dataDict["stb_name"]} where {dataDict["filter_sql"]}')
        self.tdCom.create_stream(stream_name=f'{dataDict["ctb_name"]}{self.stream_suffix}', des_table=f'{dataDict["ctb_name"]}{self.des_table_suffix}', source_sql=f'select {dataDict["source_select_elm"]} from {dataDict["ctb_name"]} where {dataDict["filter_sql"]}')
        self.tdCom.create_stream(stream_name=f'{dataDict["tb_name"]}{self.stream_suffix}', des_table=f'{dataDict["tb_name"]}{self.des_table_suffix}', source_sql=f'select {dataDict["source_select_elm"]} from {dataDict["tb_name"]} where {dataDict["filter_sql"]}')

        # insert data
        count = 1
        step_count = 1
        for i in range(1, self.range_count):
            if i % 2 == 0:
                step_count += i
                for j in range(count, step_count):
                    self.tdCom.insert_rows(tbname=dataDict["ctb_name"] ,ts_value=f'{self.date_time}+{j}s')
                    self.tdCom.insert_rows(tbname=dataDict["tb_name"] ,ts_value=f'{self.date_time}+{j}s')
                count += i
            else:
                step_count += 1
                for i in range(2):
                    self.tdCom.insert_rows(tbname=dataDict["ctb_name"] ,ts_value=f'{self.date_time}+{count}s')
                    self.tdCom.insert_rows(tbname=dataDict["tb_name"] ,ts_value=f'{self.date_time}+{count}s')
                count += 1
            # check result
            self.tdCom.check_stream(f'select {dataDict["des_select_elm"]} from {dataDict["stb_name"]}{self.des_table_suffix} where {dataDict["filter_sql"]};', f'select {dataDict["des_select_elm"]} from {dataDict["stb_name"]} where {dataDict["filter_sql"]};', count-1)
            self.tdCom.check_stream(f'select {dataDict["des_select_elm"]} from {dataDict["ctb_name"]}{self.des_table_suffix} where {dataDict["filter_sql"]};', f'select {dataDict["des_select_elm"]} from {dataDict["ctb_name"]} where {dataDict["filter_sql"]};', count-1)
            self.tdCom.check_stream(f'select {dataDict["des_select_elm"]} from {dataDict["tb_name"]}{self.des_table_suffix} where {dataDict["filter_sql"]};', f'select {dataDict["des_select_elm"]} from {dataDict["tb_name"]} where {dataDict["filter_sql"]};', count-1)

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
        output_select_str = ','.join(list(map(lambda x:f'`{x}`', self.downsampling_function_list)))
        source_select_str = ','.join(self.downsampling_function_list)
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

    def trigger_window_close(self):
        self.case_name = sys._getframe().f_code.co_name
        self.tdSql.execute('create table if not exists trigger_window_close_stb (ts timestamp, c1 int, c2 double, c3 varchar(100), c4 bool) tags (t1 int, t2 double, t3 varchar(100), t4 bool);')
        self.tdSql.execute('create table trigger_window_close_ct1 using trigger_window_close_stb tags(10, 10.1, "Beijing", True);')
        self.tdSql.execute('create table if not exists trigger_window_close_tb (ts timestamp, c1 int, c2 double, c3 varchar(100), c4 bool);')
        
        self.write_latency(self.case_name)
        # stb not supported
        # self.tdSql.execute(f'create stream stb_trigger_window_close_stream into output_trigger_window_close_stb as select _wstartts AS start, max(c1) from trigger_window_close_stb trigger_window_close(c1);')
        # ctb
        self.tdSql.execute(f'create stream ctb_trigger_window_close_stream trigger window_close into output_trigger_window_close_ctb as select _wstartts AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from trigger_window_close_ct1 state_window(c1);')
        # tb
        self.tdSql.execute(f'create stream tb_trigger_window_close_stream trigger window_close into output_trigger_window_close_tb as select _wstartts AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from trigger_window_close_tb state_window(c1);')
        for tbname in ["trigger_window_close_ct1", "trigger_window_close_tb"]:
            self.tdSql.execute(f'insert into {tbname} values (1653547828591, 100, 100.1, "Beijing", True);')
            self.tdSql.execute(f'insert into {tbname} values (1653547828591+1s, -100, -100.1, "Tianjin", False);')
            self.tdSql.execute(f'insert into {tbname} values (1653547828591+2s, 50, 50.3, "HeBei", False);')

        # self.check_stream('select start, `min(c1)` from output_trigger_window_close_stb;', 'select _wstartts AS start, max(c1) from trigger_window_close_stb trigger_window_close(c1);', 1)
        self.check_stream('select start, `min(c1)`, `max(c2)`, `sum(c1)`, `first(c1)`, `last(c1)`, `apercentile(c1, 50)` from output_trigger_window_close_ctb;', 'select _wstartts AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from trigger_window_close_ct1 state_window(c1)  limit 2;', 2)
        self.check_stream('select start, `min(c1)`, `max(c2)`, `sum(c1)`, `first(c1)`, `last(c1)`, `apercentile(c1, 50)` from output_trigger_window_close_tb;', 'select _wstartts AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from trigger_window_close_tb state_window(c1)  limit 2;', 2)
        count = 3
        step_count = 1
        for i in range(1, 10):
            if i % 2 == 0:
                step_count += i
                for j in range(count, step_count):
                    self.tdSql.execute(f'insert into trigger_window_close_ct1 values (1653547828591+{j}0m, 60, 60.3, "heilongjiang", True);')
                    self.tdSql.execute(f'insert into trigger_window_close_tb values (1653547828591+{j}1m, 70, 70.3, "heilongjiang", True);')
                count += i
            else:
                step_count += 1
                for i in range(2):
                    self.tdSql.execute(f'insert into trigger_window_close_ct1 values (1653547828591+{count}1m, 60, 60.3, "heilongjiang", True);')
                    self.tdSql.execute(f'insert into trigger_window_close_tb values (1653547828591+{count}1m, 70, 70.3, "heilongjiang", True);')
                count += 1
            # check result
            # stb not supported
            # self.check_stream('select start, `min(c1)` from output_trigger_window_close_stb;', 'select _wstartts AS start, max(c1) from trigger_window_close_stb trigger_window_close(c1);', 1)
            self.check_stream('select start, `min(c1)`, `max(c2)`, `sum(c1)`, `first(c1)`, `last(c1)`, `apercentile(c1, 50)` from output_trigger_window_close_ctb;', 'select _wstartts AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from trigger_window_close_ct1 state_window(c1) limit 3;', 3)
            self.check_stream('select start, `min(c1)`, `max(c2)`, `sum(c1)`, `first(c1)`, `last(c1)`, `apercentile(c1, 50)` from output_trigger_window_close_tb;', 'select _wstartts AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from trigger_window_close_tb state_window(c1) limit 3;', 3)

    def trigger_max_delay(self):
        self.case_name = sys._getframe().f_code.co_name
        self.tdSql.execute('create table if not exists trigger_max_delay_stb (ts timestamp, c1 int, c2 double, c3 varchar(100), c4 bool) tags (t1 int, t2 double, t3 varchar(100), t4 bool);')
        self.tdSql.execute('create table trigger_max_delay_ct1 using trigger_max_delay_stb tags(10, 10.1, "Beijing", True);')
        self.tdSql.execute('create table if not exists trigger_max_delay_tb (ts timestamp, c1 int, c2 double, c3 varchar(100), c4 bool);')
        
        self.tdCom.write_latency(self.case_name)
        # stb not supported
        # self.tdSql.execute(f'create stream stb_trigger_max_delay_stream into output_trigger_max_delay_stb as select _wstartts AS start, max(c1) from trigger_max_delay_stb trigger_max_delay(c1);')
        # ctb
        self.tdSql.execute(f'create stream ctb_trigger_max_delay_stream trigger max_delay 2s into output_trigger_max_delay_ctb as select _wstartts AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from trigger_max_delay_ct1 state_window(c1);')
        # tb
        self.tdSql.execute(f'create stream tb_trigger_max_delay_stream trigger max_delay 2s into output_trigger_max_delay_tb as select _wstartts AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from trigger_max_delay_tb state_window(c1);')
        for tbname in ["trigger_max_delay_ct1", "trigger_max_delay_tb"]:
            self.tdSql.execute(f'insert into {tbname} values (1653547828591, 100, 100.1, "Beijing", True);')
            self.tdSql.execute(f'insert into {tbname} values (1653547828591+1s, -100, -100.1, "Tianjin", False);')
            self.tdSql.execute(f'insert into {tbname} values (1653547828591+2s, 50, 50.3, "HeBei", False);')
        time.sleep(2)
        # self.check_stream('select start, `min(c1)` from output_trigger_max_delay_stb;', 'select _wstartts AS start, max(c1) from trigger_max_delay_stb trigger_max_delay(c1);', 1)
        self.tdCom.check_stream('select start, `min(c1)`, `max(c2)`, `sum(c1)`, `first(c1)`, `last(c1)`, `apercentile(c1, 50)` from output_trigger_max_delay_ctb;', 'select _wstartts AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from trigger_max_delay_ct1 state_window(c1);', 3)
        self.tdCom.check_stream('select start, `min(c1)`, `max(c2)`, `sum(c1)`, `first(c1)`, `last(c1)`, `apercentile(c1, 50)` from output_trigger_max_delay_tb;', 'select _wstartts AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from trigger_max_delay_tb state_window(c1);', 3)
        count = 3
        step_count = 1
        for i in range(1, 20):
            if i % 2 == 0:
                step_count += i
                for j in range(count, step_count):
                    self.tdSql.execute(f'insert into trigger_max_delay_ct1 values (1653547828591+{j}0s, 60, 60.3, "heilongjiang", True);')
                    self.tdSql.execute(f'insert into trigger_max_delay_tb values (1653547828591+{j}1s, 70, 70.3, "heilongjiang", True);')
                count += i
            else:
                step_count += 1
                for i in range(2):
                    self.tdSql.execute(f'insert into trigger_max_delay_ct1 values (1653547828591+{count}1s, 60, 60.3, "heilongjiang", True);')
                    self.tdSql.execute(f'insert into trigger_max_delay_tb values (1653547828591+{count}1s, 70, 70.3, "heilongjiang", True);')
                count += 1
            # check result
            # stb not supported
            # self.check_stream('select start, `min(c1)` from output_trigger_max_delay_stb;', 'select _wstartts AS start, max(c1) from trigger_max_delay_stb trigger_max_delay(c1);', 1)
            time.sleep(2)
            self.tdCom.check_stream('select start, `min(c1)`, `max(c2)`, `sum(c1)`, `first(c1)`, `last(c1)`, `apercentile(c1, 50)` from output_trigger_max_delay_ctb;', 'select _wstartts AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from trigger_max_delay_ct1 state_window(c1);', 4)
            self.tdCom.check_stream('select start, `min(c1)`, `max(c2)`, `sum(c1)`, `first(c1)`, `last(c1)`, `apercentile(c1, 50)` from output_trigger_max_delay_tb;', 'select _wstartts AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from trigger_max_delay_tb state_window(c1);', 4)


    def watermark_window_close_order(self):
        self.case_name = sys._getframe().f_code.co_name
        dataDict = {
            "stb_name" : f"{self.case_name}_stb",
            "ctb_name" : f"{self.case_name}_ct1",
            "tb_name" : f"{self.case_name}_tb1",
            "interval" : 10,
            "watermark": "17s",
            "start_ts": 1655903478508
        }
        self.date_time = dataDict["start_ts"]
        # create stb/ctb/tb
        self.tdCom.create_stable(stbname=dataDict["stb_name"])
        self.tdCom.create_ctable(stbname=dataDict["stb_name"], ctbname=dataDict["ctb_name"])
        self.tdCom.create_table(tbname=dataDict["tb_name"])

        self.tdCom.write_latency(self.case_name)
        output_select_str = ','.join(list(map(lambda x:f'`{x}`', self.downsampling_function_list)))
        source_select_str = ','.join(self.downsampling_function_list)
        # create stb/ctb/tb stream
        self.tdCom.create_stream(stream_name=f'{dataDict["stb_name"]}{self.stream_suffix}', des_table=f'{dataDict["stb_name"]}{self.des_table_suffix}', source_sql=f'select _wstartts AS start, {source_select_str}  from {dataDict["stb_name"]} interval({dataDict["interval"]}s)', trigger_mode="window_close", watermark=dataDict["watermark"])
        self.tdCom.create_stream(stream_name=f'{dataDict["ctb_name"]}{self.stream_suffix}', des_table=f'{dataDict["ctb_name"]}{self.des_table_suffix}', source_sql=f'select _wstartts AS start, {source_select_str}  from {dataDict["ctb_name"]} interval({dataDict["interval"]}s)', trigger_mode="window_close", watermark=dataDict["watermark"])
        self.tdCom.create_stream(stream_name=f'{dataDict["tb_name"]}{self.stream_suffix}', des_table=f'{dataDict["tb_name"]}{self.des_table_suffix}', source_sql=f'select _wstartts AS start, {source_select_str}  from {dataDict["tb_name"]} interval({dataDict["interval"]}s)', trigger_mode="window_close", watermark=dataDict["watermark"])

        # insert data
        self.tdCom.insert_rows(tbname=dataDict["ctb_name"], ts_value=self.date_time)
        self.tdCom.insert_rows(tbname=dataDict["tb_name"], ts_value=self.date_time)
        for tbname in [dataDict["stb_name"], dataDict["ctb_name"], dataDict["tb_name"]]:
            self.tdSql.query(f'select _wstartts AS start, {source_select_str}  from {tbname} interval({dataDict["interval"]}s)')
            self.tdSql.checkEqual(self.tdSql.query_row, 1)
        for tbname in [f'{dataDict["stb_name"]}{self.des_table_suffix}', f'{dataDict["ctb_name"]}{self.des_table_suffix}', f'{dataDict["tb_name"]}{self.des_table_suffix}']:
            self.tdSql.query(f'select start, {output_select_str} from {tbname}')
            self.tdSql.checkEqual(self.tdSql.query_row, 0)

        self.tdCom.insert_rows(tbname=dataDict["ctb_name"], ts_value=f'{self.date_time}+{dataDict["interval"]+1}s')
        self.tdCom.insert_rows(tbname=dataDict["tb_name"], ts_value=f'{self.date_time}+{dataDict["interval"]+1}s')
        for tbname in [dataDict["stb_name"], dataDict["ctb_name"], dataDict["tb_name"]]:
            self.tdSql.query(f'select _wstartts AS start, {source_select_str}  from {tbname} interval({dataDict["interval"]}s)')
            self.tdSql.checkEqual(self.tdSql.query_row, 2)
        for tbname in [f'{dataDict["stb_name"]}{self.des_table_suffix}', f'{dataDict["ctb_name"]}{self.des_table_suffix}', f'{dataDict["tb_name"]}{self.des_table_suffix}']:
            self.tdSql.query(f'select start, {output_select_str} from {tbname}')
            self.tdSql.checkEqual(self.tdSql.query_row, 0)
        
        self.tdCom.insert_rows(tbname=dataDict["ctb_name"], ts_value=f'{self.date_time}+{dataDict["interval"]+7}s')
        self.tdCom.insert_rows(tbname=dataDict["tb_name"], ts_value=f'{self.date_time}+{dataDict["interval"]+7}s')
        for tbname in [dataDict["stb_name"], dataDict["ctb_name"], dataDict["tb_name"]]:
            self.tdSql.query(f'select _wstartts AS start, {source_select_str}  from {tbname} interval({dataDict["interval"]}s)')
            self.tdSql.checkEqual(self.tdSql.query_row, 3)
        for tbname in [f'{dataDict["stb_name"]}{self.des_table_suffix}', f'{dataDict["ctb_name"]}{self.des_table_suffix}', f'{dataDict["tb_name"]}{self.des_table_suffix}']:
            self.tdSql.query(f'select start, {output_select_str} from {tbname}')
            self.tdSql.checkEqual(self.tdSql.query_row, 0)
        self.tdCom.insert_rows(tbname=dataDict["ctb_name"], ts_value=f'{self.date_time}+{dataDict["interval"]+8}s')
        self.tdCom.insert_rows(tbname=dataDict["tb_name"], ts_value=f'{self.date_time}+{dataDict["interval"]+8}s')
        for tbname in [dataDict["stb_name"], dataDict["ctb_name"], dataDict["tb_name"]]:
            self.tdSql.query(f'select _wstartts AS start, {source_select_str}  from {tbname} interval({dataDict["interval"]}s)')
            self.tdSql.checkEqual(self.tdSql.query_row, 3)
        for tbname in [f'{dataDict["stb_name"]}{self.des_table_suffix}', f'{dataDict["ctb_name"]}{self.des_table_suffix}', f'{dataDict["tb_name"]}{self.des_table_suffix}']:
            self.tdSql.query(f'select start, {output_select_str} from {tbname}')
            self.tdSql.checkEqual(self.tdSql.query_row, 0)
        self.tdCom.insert_rows(tbname=dataDict["ctb_name"], ts_value=f'{self.date_time}+{dataDict["interval"]+10}s')
        self.tdCom.insert_rows(tbname=dataDict["tb_name"], ts_value=f'{self.date_time}+{dataDict["interval"]+10}s')
        for tbname in [dataDict["stb_name"], dataDict["ctb_name"], dataDict["tb_name"]]:
            self.tdSql.query(f'select _wstartts AS start, {source_select_str}  from {tbname} interval({dataDict["interval"]}s)')
            self.tdSql.checkEqual(self.tdSql.query_row, 3)
        for tbname in [f'{dataDict["stb_name"]}{self.des_table_suffix}', f'{dataDict["ctb_name"]}{self.des_table_suffix}', f'{dataDict["tb_name"]}{self.des_table_suffix}']:
            self.tdSql.query(f'select start, {output_select_str} from {tbname}')
            self.tdSql.checkEqual(self.tdSql.query_row, 1)
        # # insert data
        # count = 1
        # step_count = 1
        # for i in range(1, self.range_count):
        #     if i % 2 == 0:
        #         step_count += i
        #         for j in range(count, step_count):
        #             self.tdCom.insert_rows(tbname=dataDict["ctb_name"] ,ts_value=f'{self.date_time}+{j}s')
        #             self.tdCom.insert_rows(tbname=dataDict["tb_name"] ,ts_value=f'{self.date_time}+{j}s')
        #         count += i
        #     else:
        #         step_count += 1
        #         for i in range(2):
        #             self.tdCom.insert_rows(tbname=dataDict["ctb_name"] ,ts_value=f'{self.date_time}+{count}s')
        #             self.tdCom.insert_rows(tbname=dataDict["tb_name"] ,ts_value=f'{self.date_time}+{count}s')
        #         count += 1
        #     # check result
        #     self.tdCom.check_stream(f'select {dataDict["des_select_elm"]} from {dataDict["stb_name"]}{self.des_table_suffix} where {dataDict["filter_sql"]};', f'select {dataDict["des_select_elm"]} from {dataDict["stb_name"]} where {dataDict["filter_sql"]};', count-1)
        #     self.tdCom.check_stream(f'select {dataDict["des_select_elm"]} from {dataDict["ctb_name"]}{self.des_table_suffix} where {dataDict["filter_sql"]};', f'select {dataDict["des_select_elm"]} from {dataDict["ctb_name"]} where {dataDict["filter_sql"]};', count-1)
        #     self.tdCom.check_stream(f'select {dataDict["des_select_elm"]} from {dataDict["tb_name"]}{self.des_table_suffix} where {dataDict["filter_sql"]};', f'select {dataDict["des_select_elm"]} from {dataDict["tb_name"]} where {dataDict["filter_sql"]};', count-1)


        # self.tdSql.execute('create table if not exists downsampling_stb (ts timestamp, c1 int, c2 double, c3 varchar(100), c4 bool) tags (t1 int, t2 double, t3 varchar(100), t4 bool);')
        # self.tdSql.execute('create table downsampling_ct1 using downsampling_stb tags(10, 10.1, "Beijing", True);')
        # # self.tdSql.execute(f'create table ownsampling_ct2 using downsampling_stb tags(20, 20.2, "TIANJIN", False);')
        # # self.tdSql.execute(f'create table ownsampling_ct3 using downsampling_stb tags(30, 30.3, "HeBei", False);')
        # self.tdSql.execute('create table if not exists downsampling_tb (ts timestamp, c1 int, c2 double, c3 varchar(100), c4 bool);')
        # # ! TD-16571 histogram
        # # ! TD-16570 last_row(c1)
        # # ! now() timezone() to_iso8601(1)
        # function_list = ["min(c1)", "max(c2)", "sum(c1)", "first(c1)", "last(c1)", "apercentile(c1, 50)", "avg(c1)", "count(c1)", "leastsquares(c1, 1, 2)", "spread(c1)", "stddev(c2)", "hyperloglog(c3)", 
        #    "timediff(1, 0, 1h)", "timetruncate(_wstartts, 1m)", "to_iso8601(1)", 'to_unixtimestamp("1970-01-01T08:00:00+08:00")']
        # # function_list = ['to_unixtimestamp("1970-01-01T08:00:00+08:00")']
        # # function_list = ["to_iso8601(1)"]
        # # function_list = ["min(c1)", "max(c2)", "sum(c1)", "first(c1)", "last(c1)", "apercentile(c1, 50)", "last_row(c1)", "avg(c1)", "count(c1)", "leastsquares(c1, 1, 2)", "spread(c1)", "stddev(c2)", "hyperloglog(c3)", 
        # #     'histogram(c1, "user_input", "[1, 3, 5, 7]", 0)', "now()", "timediff(1, 0, 1h)", "timetruncate(_wstartts, 1m)", "timezone()", "today()", "to_iso8601(1)",  'to_unixtimestamp("1970-01-01T08:00:00+08:00")']
        # output_select_str = ','.join(list(map(lambda x:f'`{x}`', self.downsampling_function_list)))
        # source_select_str = ','.join(self.downsampling_function_list)
        # self.write_latency(self.case_name)
        # # stb
        # self.tdSql.execute(f'create stream stb_downsampling_stream trigger at_once into output_downsampling_stb as select _wstartts AS start, {source_select_str} from downsampling_stb interval(10m);')
        # # ctb
        # self.tdSql.execute(f'create stream ctb_downsampling_stream trigger at_once into output_downsampling_ctb as select _wstartts AS start, {source_select_str} from downsampling_ct1 interval(10m);')
        # # tb
        # self.tdSql.execute(f'create stream tb_downsampling_stream trigger at_once into output_downsampling_tb as select _wstartts AS start, {source_select_str} from downsampling_tb interval(10m);')
        # for tbname in ["downsampling_ct1", "downsampling_tb"]:
        #     self.tdSql.execute(f'insert into {tbname} values (1653547828591, 100, 100.1, "Beijing", True);')
        #     self.tdSql.execute(f'insert into {tbname} values (1653547828591+1s, -100, -100.1, "Tianjin", False);')
        #     self.tdSql.execute(f'insert into {tbname} values (1653547828591+2s, 50, 50.3, "HeBei", False);')

        # self.check_stream(f'select start, {output_select_str} from output_downsampling_stb;', f'select _wstartts AS start, {source_select_str} from downsampling_stb interval(10m);', 1)
        # self.check_stream(f'select start, {output_select_str} from output_downsampling_ctb;', f'select _wstartts AS start, {source_select_str} from downsampling_ct1 interval(10m);', 1)
        # self.check_stream(f'select start, {output_select_str} from output_downsampling_tb;', f'select _wstartts AS start, {source_select_str} from downsampling_tb interval(10m);', 1)

    def run(self) -> bool:
        # self.downsampling()
        # self.state_window_function()
        # # self.session_window()
        # # # # ! TD-16145
        # # self.scalar_function()
        # self.data_filter()
        # self.life_cycle()
        # # # ! TD-16617
        # # # self.stream_tandem()
        # # self.disorder_data()
        # self.trigger_window_close()
        # self.trigger_max_delay()
        self.watermark_window_close_order()

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

