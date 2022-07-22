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
import os
import random

class StreamComputingTest(TDCase):
    def init(self):
        self.stream_case_env_root = os.path.join(os.environ["TEST_ROOT"], "cases/stream_computing")
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.taospy_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taospy")
        self._fqdn = self.taospy_setting["fqdn"][0]
        self.case_name = None
        self.tbname = None
        self.precision = "ms"
        self.offset = 1000
        
        self.case_name = str()
        self.stb_name = str()
        self.ctb_name = str()
        self.tb_name = str()
        self.stb_stream_des_table = str()
        self.ctb_stream_des_table = str()
        self.tb_stream_des_table = str()

        self.range_count = 10
        self.des_table_suffix = "_output"
        self.stream_suffix = "_stream"
        self.vgroups = 1
        self.update = True
        self.delete = False
        
        # ! apercentile(c6, 50) "avg(c7)" "timetruncate(_wstart, 1m)" "timediff(1, 0, 1h)" TD-16878 TD-16877 TD-16876 TD-16869
        self.partition_by_downsampling_function_list = ["min(c1)", "max(c2)", "sum(c3)", "first(c4)", "last(c5)", "count(c8)", "spread(c1)", 
        "stddev(c2)", "hyperloglog(c11)", "min(t1)", "max(t2)", "sum(t3)", "first(t4)", "last(t5)", "count(t8)", "spread(t1)", "stddev(t2)"]
        # self.partition_by_downsampling_function_list = ["min(c1)", "max(c2)", "sum(c3)", "first(c4)", "last(c5)", "count(c8)", "spread(c1)",
        # "stddev(c2)", "hyperloglog(c11)", "min(t1)", "max(t2)", "sum(t3)", "first(t4)", "last(t5)", "count(t8)", "spread(t1)", "stddev(t2)", "hyperloglog(t11)"]
        # self.partition_by_downsampling_function_list = ["min(c1)", "max(c2)", "sum(c3)", "first(c4)", "last(c5)", "count(c8)", "spread(c1)", ]
        # ! TD-16571 histogram
        # ! TD-16570 last_row(c1)
        # ! now() timezone() to_iso8601(now)
        self.downsampling_function_list = ["min(c1)", "max(c2)", "sum(c3)", "first(c4)", "last(c5)", "apercentile(c6, 50)", "avg(c7)", "count(c8)", "spread(c1)", 
        "stddev(c2)", "hyperloglog(c11)", "timediff(1, 0, 1h)", "timetruncate(_wstart, 1m)", "timezone()", "to_iso8601(1)", 'to_unixtimestamp("1970-01-01T08:00:00+08:00")', "min(t1)", "max(t2)", "sum(t3)",
        "first(t4)", "last(t5)", "apercentile(t6, 50)", "avg(t7)", "count(t8)", "spread(t1)", "stddev(t2)", "hyperloglog(t11)"]
        self.udf_function_list = ["min(udf1(c1))", "max(udf1(c2))", "sum(udf1(c3))", "first(udf1(c4))", "last(udf1(c5))", "apercentile(udf1(c6), 50)", "avg(udf1(c7))", "count(udf1(c8))", "spread(udf1(c1))", 
        "stddev(udf1(c2))", "hyperloglog(udf1(c11))", "timediff(1, 0, 1h)", "timetruncate(_wstart, 1m)", "to_iso8601(1)", 'to_unixtimestamp("1970-01-01T08:00:00+08:00")', "min(udf1(t1))", "max(udf1(t2))", "sum(udf1(t3))",
        "first(udf1(t4))", "last(udf1(t5))", "apercentile(udf1(t6), 50)", "avg(udf1(t7))", "count(udf1(t8))", "spread(udf1(t1))", "stddev(udf1(t2))", "hyperloglog(udf1(t11))"]
        self.udf_function_list = ["min(udf1(c1))", "max(udf1(c2))", "sum(udf1(c3))", "apercentile(udf1(c6), 50)", "avg(udf1(c7))", "count(udf1(c8))", "spread(udf1(c1))",
        "stddev(udf1(c2))", "hyperloglog(udf1(c11))", "timediff(1, 0, 1h)", "timetruncate(_wstart, 1m)", "to_iso8601(1)", 'to_unixtimestamp("1970-01-01T08:00:00+08:00")']
        # self.downsampling_function_list = ["min(c1)", "max(c2)", "sum(c1)", "first(c1)", "last(c1)", "apercentile(c1, 50)", "last_row(c1)", "avg(c1)", "count(c1)", "spread(c1)", "stddev(c2)", "hyperloglog(c3)", 
        #     'histogram(c1, "user_input", "[1, 3, 5, 7]", 0)', "now()", "timediff(1, 0, 1h)", "timetruncate(_wstart, 1m)", "timezone()", "today()", "to_iso8601(now)",  'to_unixtimestamp("1970-01-01T08:00:00+08:00")']
        self.stb_output_select_str = ','.join(list(map(lambda x:f'`{x}`', self.downsampling_function_list)))
        self.stb_source_select_str = ','.join(self.downsampling_function_list)
        self.tb_output_select_str = ','.join(list(map(lambda x:f'`{x}`', self.downsampling_function_list[0:16])))
        self.tb_source_select_str = ','.join(self.downsampling_function_list[0:16])

        self.partition_by_stb_output_select_str = ','.join(list(map(lambda x:f'`{x}`', self.partition_by_downsampling_function_list)))
        self.partition_by_stb_source_select_str = ','.join(self.partition_by_downsampling_function_list)

        self.udf_stb_output_select_str = ','.join(list(map(lambda x:f'`{x}`', self.udf_function_list)))
        self.udf_stb_source_select_str = ','.join(self.udf_function_list)
        self.udf_tb_output_select_str = ','.join(list(map(lambda x:f'`{x}`', self.udf_function_list[0:16])))
        self.udf_tb_source_select_str = ','.join(self.udf_function_list[0:16])

        self.date_time = self.tdCom.genTs(precision=self.precision)[0]
        self.stb_data_filter_sql = f'ts >= {self.date_time}+1s and c1 = 1 or c2 > 1 and c3 != 4 or c4 <= 3 and c9 <> 0 or c10 is not Null or c11 is Null or \
                c12 between "na" and "nchar4" and c11 not between "bi" and "binary" and c12 match "nchar[19]" and c12 nmatch "nchar[25]" or c13 = True or \
                c5 in (1, 2, 3) or c6 not in (6, 7) and c12 like "nch%" and c11 not like "bina_" and c6 < 10 or c12 is Null or c8 >= 4 and t1 = 1 or t2 > 1 \
                and t3 != 4 or c4 <= 3 and t9 <> 0 or t10 is not Null or t11 is Null or t12 between "na" and "nchar4" and t11 not between "bi" and "binary" \
                or t12 match "nchar[19]" or t12 nmatch "nchar[25]" or t13 = True or t5 in (1, 2, 3) or t6 not in (6, 7) and t12 like "nch%" \
                and t11 not like "bina_" and t6 <= 10 or t12 is Null or t8 >= 4'
        self.tb_data_filter_sql = self.stb_data_filter_sql.partition(" and t1")[0]

        self.filter_source_select_elm = "*"
        self.stb_filter_des_select_elm = "ts, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13"
        self.tb_filter_des_select_elm = self.stb_filter_des_select_elm.partition(", t1")[0]

        self.state_window_range = list()
        self.interation = 10
        self.udf1 = "/tmp/libudf1.so"
        self.udf2 = "/tmp/libudf2.so"


    def build_udf_so(self):
        self._remote.cmd(self._fqdn, [f'gcc -fPIC -shared -o {self.udf1} {self.stream_case_env_root}/udf1.c', f'gcc -fPIC -shared -o {self.udf2} {self.stream_case_env_root}/udf2.c'])

    def set_precision_offset(self, precision):
        if precision == "ms":
            self.offset = 1000
        elif precision == "us":
            self.offset = 1000000
        elif precision == "ns":
            self.offset = 1000000000
        else:
            pass

    # def alter_source_table(self, interval, precision=None, vgroups=1):
    #     self.case_name = sys._getframe().f_code.co_name
    #     self.prepare_data(interval=interval, precision=precision, vgroups=vgroups)
    #     self.tdCom.write_latency(self.case_name)
    #     self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select _wstart AS start, {self.stb_source_select_str}  from {self.stb_name} interval({self.dataDict["interval"]}s)', trigger_mode="at_once")
    #     self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstart AS start, {self.stb_source_select_str}  from {self.ctb_name} interval({self.dataDict["interval"]}s)', trigger_mode="at_once")
    #     self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstart AS start, {self.tb_source_select_str}  from {self.tb_name} interval({self.dataDict["interval"]}s)', trigger_mode="at_once")
    #     for i in range(self.range_count):
    #         ctb_name = self.tdCom.get_long_name()
    #         self.tdCom.create_ctable(stbname=self.stb_name, ctbname=ctb_name)
    #         self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=str(self.date_time+self.dataDict["interval"])+f'+{i*10}s')
    #         self.date_time += 1
    #         self.tdCom.insert_rows(tbname=self.tb_name, ts_value=str(self.date_time+self.dataDict["interval"])+f'+{i*10}s')
    #         self.date_time += 1
    #         self.tdCom.insert_rows(tbname=ctb_name, ts_value=str(self.date_time+self.dataDict["interval"])+f'+{i*10}s')
    #         self.date_time += 1
    #     self.tdSql.execute(f'alter stable {self.stb_name} add column c22 int')
    #     self.tdSql.execute(f'alter stable {self.stb_name} add tag t22 binary(5)')
    #     self.tdSql.execute(f'alter table {self.ctb_name} set tag t3 = "0"')
    #     for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
    #         if tbname != self.tb_name:
    #             self.tdCom.check_query_data(f'select start, {self.stb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS start, {self.stb_source_select_str}  from {tbname} interval({self.dataDict["interval"]}s)')
    #         else:
    #             self.tdCom.check_query_data(f'select start, {self.tb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS start, {self.tb_source_select_str}  from {tbname} interval({self.dataDict["interval"]}s)')


    def data_filter(self):
        self.case_name = sys._getframe().f_code.co_name

        self.prepare_data()
        self.tdCom.write_latency(self.case_name)

        # create stb/ctb/tb stream
        self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select {self.filter_source_select_elm} from {self.stb_name} where {self.stb_data_filter_sql}', trigger_mode="at_once")
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select {self.filter_source_select_elm} from {self.ctb_name} where {self.stb_data_filter_sql}', trigger_mode="at_once")
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select {self.filter_source_select_elm} from {self.tb_name} where {self.tb_data_filter_sql}', trigger_mode="at_once")

        # insert data
        count = 1
        step_count = 1
        for i in range(1, self.range_count):
            if i % 2 == 0:
                step_count += i
                for j in range(count, step_count):
                    self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=f'{self.date_time}+{j}s')
                    self.tdCom.insert_rows(tbname=self.tb_name, ts_value=f'{self.date_time}+{j}s')
                    if self.update:
                        self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=f'{self.date_time}+{j}s')
                        self.tdCom.insert_rows(tbname=self.tb_name, ts_value=f'{self.date_time}+{j}s')
                count += i
            else:
                ts_value = str(self.date_time)+f'+{count}s'
                ts_cast_delete_value = self.tdCom.time_cast(ts_value)
                step_count += 1
                for i in range(2):
                    self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=ts_value)
                    self.tdCom.insert_rows(tbname=self.tb_name, ts_value=ts_value)
                    if self.delete:
                        self.tdCom.delete_rows(tbname=self.ctb_name, start_ts=ts_cast_delete_value)
                        self.tdCom.delete_rows(tbname=self.tb_name, start_ts=ts_cast_delete_value)
                count += 1
            # check result
            self.tdCom.check_stream(f'select {self.stb_filter_des_select_elm} from {self.stb_stream_des_table};', f'select {self.filter_source_select_elm} from {self.stb_name} where {self.stb_data_filter_sql};', count-1)
            self.tdCom.check_stream(f'select {self.tb_filter_des_select_elm} from {self.ctb_stream_des_table};', f'select {self.filter_source_select_elm} from {self.ctb_name} where {self.stb_data_filter_sql};', count-1)
            self.tdCom.check_stream(f'select {self.tb_filter_des_select_elm} from {self.tb_stream_des_table};', f'select {self.filter_source_select_elm} from {self.tb_name} where {self.tb_data_filter_sql};', count-1)

    def at_once_interval(self, interval):
        self.case_name = sys._getframe().f_code.co_name
        self.prepare_data(interval=interval)
        self.tdCom.write_latency(self.case_name)
        self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select _wstart AS start, {self.stb_source_select_str}  from {self.stb_name} interval({self.dataDict["interval"]}s)', trigger_mode="at_once")
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstart AS start, {self.stb_source_select_str}  from {self.ctb_name} interval({self.dataDict["interval"]}s)', trigger_mode="at_once")
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstart AS start, {self.tb_source_select_str}  from {self.tb_name} interval({self.dataDict["interval"]}s)', trigger_mode="at_once")
        for i in range(self.range_count):
            ts_value = str(self.date_time+self.dataDict["interval"])+f'+{i*10}s'
            ts_cast_delete_value = self.tdCom.time_cast(ts_value)
            # ctb_name = self.tdCom.get_long_name()
            # self.tdCom.create_ctable(stbname=self.stb_name, ctbname=ctb_name)
            self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=ts_value)
            if self.update and i%2 == 0:
                self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=ts_value)
            if self.delete and i%2 != 0:
                self.tdCom.delete_rows(tbname=self.ctb_name, start_ts=ts_cast_delete_value)
            self.date_time += 1
            self.tdCom.insert_rows(tbname=self.tb_name, ts_value=ts_value)
            if self.update and i%2 == 0:
                self.tdCom.insert_rows(tbname=self.tb_name, ts_value=ts_value)
            if self.delete and i%2 != 0:
                self.tdCom.delete_rows(tbname=self.tb_name, start_ts=ts_cast_delete_value)
            self.date_time += 1
            # self.tdCom.insert_rows(tbname=ctb_name, ts_value=ts_value)
            # if self.update and i%2 == 0:
            #     self.tdCom.insert_rows(tbname=ctb_name, ts_value=ts_value)
            # if self.delete and i%2 != 0:
            #     self.tdCom.delete_rows(tbname=ctb_name, start_ts=ts_cast_delete_value)
            # self.date_time += 1
            for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
                if tbname != self.tb_name:
                    self.tdCom.check_query_data(f'select start, {self.stb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS start, {self.stb_source_select_str}  from {tbname} interval({self.dataDict["interval"]}s)')
                else:
                    self.tdCom.check_query_data(f'select start, {self.tb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS start, {self.tb_source_select_str}  from {tbname} interval({self.dataDict["interval"]}s)')

    def at_once_state_window(self, state_window):
        self.case_name = sys._getframe().f_code.co_name
        self.prepare_data(state_window=state_window)
        state_window_col_name = self.dataDict["state_window"]
        self.tdCom.write_latency(self.case_name)
        self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select _wstart AS start, {self.stb_source_select_str}  from {self.stb_name} state_window({state_window_col_name})', trigger_mode="at_once")
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstart AS start, {self.stb_source_select_str}  from {self.ctb_name} state_window({state_window_col_name})', trigger_mode="at_once")
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstart AS start, {self.tb_source_select_str}  from {self.tb_name} state_window({state_window_col_name})', trigger_mode="at_once")
        range_times = self.range_count
        state_window_max = self.dataDict['state_window_max']
        for i in range(range_times):
            state_window_value = random.randint(int((i)*state_window_max/range_times), int((i+1)*state_window_max/range_times))
            for i in range(2, range_times+3):
                self.tdSql.execute(f'insert into {self.ctb_name} (ts, {state_window_col_name}) values ({self.date_time}, {state_window_value})')
                if self.update and i%2 == 0:
                    self.tdSql.execute(f'insert into {self.ctb_name} (ts, {state_window_col_name}) values ({self.date_time}, {state_window_value})')
                if self.delete and i%2 != 0:
                    dt = f'cast({self.date_time-1} as timestamp)'
                    self.tdSql.execute(f'delete from {self.ctb_name} where ts = {dt}')
                self.tdSql.execute(f'insert into {self.tb_name} (ts, {state_window_col_name}) values ({self.date_time}, {state_window_value})')
                if self.update and i%2 == 0:
                    self.tdSql.execute(f'insert into {self.tb_name} (ts, {state_window_col_name}) values ({self.date_time}, {state_window_value})')
                if self.delete and i%2 != 0:
                    self.tdSql.execute(f'delete from {self.tb_name} where ts = {dt}')
                self.date_time += 1
                
        for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
            if tbname != self.tb_name:
                self.tdCom.check_query_data(f'select start, {self.stb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS start, {self.stb_source_select_str}  from {tbname} state_window({state_window_col_name})')
            else:
                self.tdCom.check_query_data(f'select start, {self.tb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS start, {self.tb_source_select_str}  from {tbname} state_window({state_window_col_name})')

    def at_once_session(self, session):
        self.case_name = sys._getframe().f_code.co_name
        self.prepare_data(session=session)
        self.tdCom.write_latency(self.case_name)
        # create stb/ctb/tb stream
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstart AS start, {self.stb_source_select_str}  from {self.ctb_name} session(ts, {self.dataDict["session"]}s)', trigger_mode="at_once")
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstart AS start, {self.tb_source_select_str}  from {self.tb_name} session(ts, {self.dataDict["session"]}s)', trigger_mode="at_once")
        for i in range(self.range_count):
            ctb_name = self.tdCom.get_long_name()
            self.tdCom.create_ctable(stbname=self.stb_name, ctbname=ctb_name)
            
            if i == 0:
                window_close_ts = self.cal_watermark_window_close_session_endts(self.date_time, session=session)
            else:
                self.date_time = window_close_ts + 1
                window_close_ts = self.cal_watermark_window_close_session_endts(self.date_time, session=session)
            for ts_value in [self.date_time, window_close_ts]:
                self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=ts_value)
                if self.update and i%2 == 0:
                    self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=ts_value)
                if self.delete and i%2 != 0:
                    dt = f'cast({self.date_time-1} as timestamp)'
                    self.tdCom.delete_rows(tbname=self.ctb_name, start_ts=dt)
                ts_value += 1
                self.tdCom.insert_rows(tbname=self.tb_name, ts_value=ts_value)
                if self.update and i%2 == 0:
                    self.tdCom.insert_rows(tbname=self.tb_name, ts_value=ts_value)
                if self.delete and i%2 != 0:
                    dt = f'cast({self.date_time-1} as timestamp)'
                    self.tdCom.delete_rows(tbname=self.tb_name, start_ts=dt)
                ts_value += 1
                self.tdCom.insert_rows(tbname=ctb_name, ts_value=ts_value)
                if self.update and i%2 == 0:
                    self.tdCom.insert_rows(tbname=ctb_name, ts_value=ts_value)
                if self.delete and i%2 != 0:
                    dt = f'cast({self.date_time-1} as timestamp)'
                    self.tdCom.delete_rows(tbname=ctb_name, start_ts=dt)
                ts_value += 1
            
            for tbname in [self.ctb_name, self.tb_name]:
                if tbname != self.tb_name:
                    self.tdCom.check_query_data(f'select start, {self.stb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS start, {self.stb_source_select_str}  from {tbname} session(ts, {self.dataDict["session"]}s)')
                else:
                    self.tdCom.check_query_data(f'select start, {self.tb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS start, {self.tb_source_select_str}  from {tbname} session(ts, {self.dataDict["session"]}s)')

    def window_close_interval(self, interval, watermark=None):
        self.case_name = sys._getframe().f_code.co_name
        if watermark is not None:
            self.case_name = "watermark" + sys._getframe().f_code.co_name
        self.prepare_data(interval=interval, watermark=watermark)
        self.tdCom.write_latency(self.case_name)
        if watermark is not None:
            watermark_value = f'{self.dataDict["watermark"]}s'
        else:
            watermark_value = None
        # create stb/ctb/tb stream
        self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select _wstart AS start, {self.stb_source_select_str}  from {self.stb_name} interval({self.dataDict["interval"]}s)', trigger_mode="window_close", watermark=watermark_value)
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstart AS start, {self.stb_source_select_str}  from {self.ctb_name} interval({self.dataDict["interval"]}s)', trigger_mode="window_close", watermark=watermark_value)
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstart AS start, {self.tb_source_select_str}  from {self.tb_name} interval({self.dataDict["interval"]}s)', trigger_mode="window_close", watermark=watermark_value)

        for i in range(self.range_count):
            if i == 0:
                if watermark is not None:
                    window_close_ts = self.cal_watermark_window_close_interval_endts(self.date_time, self.dataDict['interval'], self.dataDict['watermark'])
                else:
                    window_close_ts = self.cal_watermark_window_close_interval_endts(self.date_time, self.dataDict['interval'])
            else:
                self.date_time = window_close_ts + self.offset
                window_close_ts += self.dataDict['interval']*self.offset
            for num in range(int(window_close_ts/self.offset-self.date_time/self.offset)):
                ts_value=self.date_time+num*self.offset
                self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=ts_value)
                self.tdCom.insert_rows(tbname=self.tb_name, ts_value=ts_value)
                if self.update and i%2 == 0:
                    self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=ts_value)
                    self.tdCom.insert_rows(tbname=self.tb_name, ts_value=ts_value)

                if self.delete and i%2 != 0:
                    dt = f'cast({ts_value-num*self.offset} as timestamp)'
                    self.tdCom.delete_rows(tbname=self.ctb_name, start_ts=dt)

                for tbname in [self.stb_stream_des_table, self.ctb_stream_des_table, self.tb_stream_des_table]:
                    if tbname != self.tb_stream_des_table:
                        self.tdSql.query(f'select start, {self.stb_output_select_str} from {tbname}')
                    else:
                        self.tdSql.query(f'select start, {self.tb_output_select_str} from {tbname}')
                    self.tdSql.checkEqual(self.tdSql.query_row, i)
            
            self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=window_close_ts-1)
            self.tdCom.insert_rows(tbname=self.tb_name, ts_value=window_close_ts-1)
            if self.update and i%2 == 0:
                self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=window_close_ts-1)
                self.tdCom.insert_rows(tbname=self.tb_name, ts_value=window_close_ts-1)
            for tbname in [self.stb_stream_des_table, self.ctb_stream_des_table, self.tb_stream_des_table]:
                if tbname != self.tb_stream_des_table:
                    self.tdSql.query(f'select start, {self.stb_output_select_str} from {tbname}')
                else:
                    self.tdSql.query(f'select start, {self.tb_output_select_str} from {tbname}')

                self.tdSql.checkEqual(self.tdSql.query_row, i)

            self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=window_close_ts)
            self.tdCom.insert_rows(tbname=self.tb_name, ts_value=window_close_ts)
            if self.update and i%2 == 0:
                self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=window_close_ts)
                self.tdCom.insert_rows(tbname=self.tb_name, ts_value=window_close_ts)
            # for tbname in [stb_stream_des_table, ctb_stream_des_table, tb_stream_des_table]:
            for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
                if tbname != self.tb_name:
                    self.tdCom.check_stream(f'select start, {self.stb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS start, {self.stb_source_select_str}  from {tbname} interval({self.dataDict["interval"]}s) limit {i+1}', i+1)
                else:
                    self.tdCom.check_stream(f'select start, {self.tb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS start, {self.tb_source_select_str}  from {tbname} interval({self.dataDict["interval"]}s) limit {i+1}', i+1)

        # window_close_ts = self.cal_watermark_window_close_endts(self.date_time, dataDict['interval'], dataDict['watermark'])

        # for num in range(int(window_close_ts/1000-self.date_time/1000)):
        #     self.tdCom.insert_rows(tbname=dataDict["ctb_name"], ts_value=self.date_time+num*1000)
        #     self.tdCom.insert_rows(tbname=dataDict["tb_name"], ts_value=self.date_time+num*1000)
        #     for tbname in [stb_stream_des_table, ctb_stream_des_table, tb_stream_des_table]:
        #         self.tdSql.query(f'select start, {output_select_str} from {tbname}')
        #         self.tdSql.checkEqual(self.tdSql.query_row, 0)
        
        # self.tdCom.insert_rows(tbname=dataDict["ctb_name"], ts_value=window_close_ts-1)
        # self.tdCom.insert_rows(tbname=dataDict["tb_name"], ts_value=window_close_ts-1)
        # for tbname in [stb_stream_des_table, ctb_stream_des_table, tb_stream_des_table]:
        #     self.tdSql.query(f'select start, {output_select_str} from {tbname}')
        #     self.tdSql.checkEqual(self.tdSql.query_row, 0)

        # self.tdCom.insert_rows(tbname=dataDict["ctb_name"], ts_value=window_close_ts)
        # self.tdCom.insert_rows(tbname=dataDict["tb_name"], ts_value=window_close_ts)
        # # for tbname in [stb_stream_des_table, ctb_stream_des_table, tb_stream_des_table]:
        # for tbname in [stb_name, ctb_name, tb_name]:
        #     self.tdCom.check_stream(f'select start, {output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS start, {source_select_str}  from {tbname} interval({dataDict["interval"]}s) limit 1', 1)

        # self.date_time = window_close_ts + 1000
        # window_close_ts += dataDict['interval']*1000
        # for num in range(int(window_close_ts/1000-self.date_time/1000)):
        #     self.tdCom.insert_rows(tbname=dataDict["ctb_name"], ts_value=self.date_time+num*1000)
        #     self.tdCom.insert_rows(tbname=dataDict["tb_name"], ts_value=self.date_time+num*1000)
        #     for tbname in [stb_stream_des_table, ctb_stream_des_table, tb_stream_des_table]:
        #         self.tdSql.query(f'select start, {output_select_str} from {tbname}')
        #         self.tdSql.checkEqual(self.tdSql.query_row, 1)
        # self.tdCom.insert_rows(tbname=dataDict["ctb_name"], ts_value=window_close_ts-1)
        # self.tdCom.insert_rows(tbname=dataDict["tb_name"], ts_value=window_close_ts-1)
        # for tbname in [stb_stream_des_table, ctb_stream_des_table, tb_stream_des_table]:
        #     self.tdSql.query(f'select start, {output_select_str} from {tbname}')
        #     self.tdSql.checkEqual(self.tdSql.query_row, 1)
        # self.tdCom.insert_rows(tbname=dataDict["ctb_name"], ts_value=window_close_ts)
        # self.tdCom.insert_rows(tbname=dataDict["tb_name"], ts_value=window_close_ts)
        # for tbname in [stb_name, ctb_name, tb_name]:
        #     self.tdCom.check_stream(f'select start, {output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS start, {source_select_str}  from {tbname} interval({dataDict["interval"]}s) limit 2', 2)
        
        
        # self.tdCom.insert_rows(tbname=dataDict["ctb_name"], ts_value=window_close_ts)
        # self.tdCom.insert_rows(tbname=dataDict["tb_name"], ts_value=window_close_ts)

    def window_close_session(self, session):
        self.case_name = sys._getframe().f_code.co_name
        self.prepare_data(session=session)
        self.date_time = self.dataDict["start_ts"]

        self.tdCom.write_latency(self.case_name)
        # create stb/ctb/tb stream
        self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select _wstart AS start, {self.stb_source_select_str}  from {self.stb_name} session(ts, {self.dataDict["session"]}s)', trigger_mode="window_close")
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstart AS start, {self.stb_source_select_str}  from {self.ctb_name} session(ts, {self.dataDict["session"]}s)', trigger_mode="window_close")
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstart AS start, {self.tb_source_select_str}  from {self.tb_name} session(ts, {self.dataDict["session"]}s)', trigger_mode="window_close")
        for i in range(self.range_count):
            if i == 0:
                window_close_ts = self.cal_watermark_window_close_session_endts(self.date_time, self.dataDict['session'])
            else:
                self.date_time = window_close_ts + 1
                window_close_ts = self.cal_watermark_window_close_session_endts(self.date_time, self.dataDict['session'])
            for ts_value in [self.date_time, window_close_ts]:
                self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=ts_value)
                self.tdCom.insert_rows(tbname=self.tb_name, ts_value=ts_value)
                # if self.update and i%2 == 0:
                #     self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=ts_value)
                #     self.tdCom.insert_rows(tbname=self.tb_name, ts_value=ts_value)
            
            for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
                if tbname != self.tb_name:
                    self.tdCom.check_stream(f'select start, {self.stb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS start, {self.stb_source_select_str}  from {tbname} session(ts, {self.dataDict["session"]}s) limit {i+1}', i+1)
                else:
                    self.tdCom.check_stream(f'select start, {self.tb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS start, {self.tb_source_select_str}  from {tbname} session(ts, {self.dataDict["session"]}s) limit {i+1}', i+1)




    # TODO split --------- before is new
    def trigger_window_close(self):
        self.case_name = sys._getframe().f_code.co_name
        self.tdSql.execute('create table if not exists trigger_window_close_stb (ts timestamp, c1 int, c2 double, c3 varchar(100), c4 bool) tags (t1 int, t2 double, t3 varchar(100), t4 bool);')
        self.tdSql.execute('create table trigger_window_close_ct1 using trigger_window_close_stb tags(10, 10.1, "Beijing", True);')
        self.tdSql.execute('create table if not exists trigger_window_close_tb (ts timestamp, c1 int, c2 double, c3 varchar(100), c4 bool);')
        
        self.tdCom.write_latency(self.case_name)
        # stb not supported
        # self.tdSql.execute(f'create stream stb_trigger_window_close_stream into output_trigger_window_close_stb as select _wstart AS start, max(c1) from trigger_window_close_stb trigger_window_close(c1);')
        # ctb
        self.tdSql.execute(f'create stream ctb_trigger_window_close_stream trigger window_close into output_trigger_window_close_ctb as select _wstart AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from trigger_window_close_ct1 state_window(c1);')
        # tb
        self.tdSql.execute(f'create stream tb_trigger_window_close_stream trigger window_close into output_trigger_window_close_tb as select _wstart AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from trigger_window_close_tb state_window(c1);')
        for tbname in ["trigger_window_close_ct1", "trigger_window_close_tb"]:
            self.tdSql.execute(f'insert into {tbname} values (1653547828591, 100, 100.1, "Beijing", True);')
            self.tdSql.execute(f'insert into {tbname} values (1653547828591+1s, -100, -100.1, "Tianjin", False);')
            self.tdSql.execute(f'insert into {tbname} values (1653547828591+2s, 50, 50.3, "HeBei", False);')

        # self.tdCom.check_stream('select start, `min(c1)` from output_trigger_window_close_stb;', 'select _wstart AS start, max(c1) from trigger_window_close_stb trigger_window_close(c1);', 1)
        self.tdCom.check_stream('select start, `min(c1)`, `max(c2)`, `sum(c1)`, `first(c1)`, `last(c1)`, `apercentile(c1, 50)` from output_trigger_window_close_ctb;', 'select _wstart AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from trigger_window_close_ct1 state_window(c1)  limit 2;', 2)
        self.tdCom.check_stream('select start, `min(c1)`, `max(c2)`, `sum(c1)`, `first(c1)`, `last(c1)`, `apercentile(c1, 50)` from output_trigger_window_close_tb;', 'select _wstart AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from trigger_window_close_tb state_window(c1)  limit 2;', 2)
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
            # self.tdCom.check_stream('select start, `min(c1)` from output_trigger_window_close_stb;', 'select _wstart AS start, max(c1) from trigger_window_close_stb trigger_window_close(c1);', 1)
            self.tdCom.check_stream('select start, `min(c1)`, `max(c2)`, `sum(c1)`, `first(c1)`, `last(c1)`, `apercentile(c1, 50)` from output_trigger_window_close_ctb;', 'select _wstart AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from trigger_window_close_ct1 state_window(c1) limit 3;', 3)
            self.tdCom.check_stream('select start, `min(c1)`, `max(c2)`, `sum(c1)`, `first(c1)`, `last(c1)`, `apercentile(c1, 50)` from output_trigger_window_close_tb;', 'select _wstart AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from trigger_window_close_tb state_window(c1) limit 3;', 3)

    # def state_window_function(self):
    #     # ! TD-16806 after this bug is fixed, state_window_function will be deleted
    #     self.case_name = sys._getframe().f_code.co_name
    #     self.tdSql.execute('create table state_window_ct1 using state_window_stb tags(10, 10.1, "Beijing", True);')
    #     self.tdSql.execute('create table if not exists state_window_tb (ts timestamp, c1 int, c2 double, c3 varchar(100), c4 bool);')
        
    #     self.tdCom.write_latency(self.case_name)
    #     # stb not supported
    #     # self.tdSql.execute(f'create stream stb_state_window_stream into output_state_window_stb as select _wstart AS start, max(c1) from state_window_stb state_window(c1);')
    #     # ctb
    #     self.tdSql.execute(f'create stream ctb_state_window_stream trigger at_once into output_state_window_ctb as select _wstart AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from state_window_ct1 state_window(c1);')
    #     # tb
    #     self.tdSql.execute(f'create stream tb_state_window_stream trigger at_once into output_state_window_tb as select _wstart AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from state_window_tb state_window(c1);')
    #     for tbname in ["state_window_ct1", "state_window_tb"]:
    #         self.tdSql.execute(f'insert into {tbname} values (1653547828591, 100, 100.1, "Beijing", True);')
    #         self.tdSql.execute(f'insert into {tbname} values (1653547828591+1s, -100, -100.1, "Tianjin", False);')
    #         self.tdSql.execute(f'insert into {tbname} values (1653547828591+2s, 50, 50.3, "HeBei", False);')

    #     # self.tdCom.check_stream('select start, `min(c1)` from output_state_window_stb;', 'select _wstart AS start, max(c1) from state_window_stb state_window(c1);', 1)
    #     self.tdCom.check_stream('select start, `min(c1)`, `max(c2)`, `sum(c1)`, `first(c1)`, `last(c1)`, `apercentile(c1, 50)` from output_state_window_ctb;', 'select _wstart AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from state_window_ct1 state_window(c1);', 3)
    #     self.tdCom.check_stream('select start, `min(c1)`, `max(c2)`, `sum(c1)`, `first(c1)`, `last(c1)`, `apercentile(c1, 50)` from output_state_window_tb;', 'select _wstart AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from state_window_tb state_window(c1);', 3)
    #     count = 3
    #     step_count = 1
    #     for i in range(1, 10):
    #         if i % 2 == 0:
    #             step_count += i
    #             for j in range(count, step_count):
    #                 self.tdSql.execute(f'insert into state_window_ct1 values (1653547828591+{j}0m, 60, 60.3, "heilongjiang", True);')
    #                 self.tdSql.execute(f'insert into state_window_tb values (1653547828591+{j}1m, 70, 70.3, "heilongjiang", True);')
    #             count += i
    #         else:
    #             step_count += 1
    #             for i in range(2):
    #                 self.tdSql.execute(f'insert into state_window_ct1 values (1653547828591+{count}1m, 60, 60.3, "heilongjiang", True);')
    #                 self.tdSql.execute(f'insert into state_window_tb values (1653547828591+{count}1m, 70, 70.3, "heilongjiang", True);')
    #             count += 1
    #         # check result
    #         # stb not supported
    #         # self.tdCom.check_stream('select start, `min(c1)` from output_state_window_stb;', 'select _wstart AS start, max(c1) from state_window_stb state_window(c1);', 1)
    #         self.tdCom.check_stream('select start, `min(c1)`, `max(c2)`, `sum(c1)`, `first(c1)`, `last(c1)`, `apercentile(c1, 50)` from output_state_window_ctb;', 'select _wstart AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from state_window_ct1 state_window(c1);', 4)
    #         self.tdCom.check_stream('select start, `min(c1)`, `max(c2)`, `sum(c1)`, `first(c1)`, `last(c1)`, `apercentile(c1, 50)` from output_state_window_tb;', 'select _wstart AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from state_window_tb state_window(c1);', 4)


    def scalar_function(self):
        # self.prepare_stream_data()
        self.case_name = sys._getframe().f_code.co_name
        self.prepare_data()
        self.tdSql.execute('create table if not exists scalar_stb (ts timestamp, c1 int, c2 double, c3 binary(20), c4 binary(20), c5 nchar(20)) tags (t1 int);')
        self.tdSql.execute('create table scalar_ct1 using scalar_stb tags(10);')
        # self.tdSql.execute(f'create table scalar_ct2 using scalar_stb tags(-20);')
        # self.tdSql.execute(f'create table scalar_ct3 using scalar_stb tags(0);')
        self.tdSql.execute('create table if not exists scalar_tb (ts timestamp, c1 int, c2 double, c3 binary(20), c4 binary(20), c5 nchar(20));')

        # self.tdCom.write_latency(self.case_name)
        math_function_list = ["abs", "acos", "asin", "atan", "ceil", "cos", "floor", "log", "pow", "round", "sin", "sqrt", "tan"]
        string_function_list = ["char_length", "concat", "concat_ws", "length", "lower", "ltrim", "rtrim", "substr", "upper"]
        for math_function in math_function_list:
            if math_function in ["log", "pow"]:
                self.tdSql.execute(f'create stream stb_{math_function}_stream trigger at_once into output_{math_function}_stb as select ts, {math_function}(c1, 2), {math_function}(c2, 2), c3 from scalar_stb;')
                self.tdSql.execute(f'create stream ctb_{math_function}_stream trigger at_once into output_{math_function}_ctb as select ts, {math_function}(c1, 2), {math_function}(c2, 2), c3 from scalar_ct1;')
                self.tdSql.execute(f'create stream tb_{math_function}_stream trigger at_once into output_{math_function}_tb as select ts, {math_function}(c1, 2), {math_function}(c2, 2), c3 from scalar_tb;')
            else:
                self.tdSql.execute(f'create stream stb_{math_function}_stream trigger at_once into output_{math_function}_stb as select ts, {math_function}(c1), {math_function}(c2), c3 from scalar_stb;')
                self.tdSql.execute(f'create stream ctb_{math_function}_stream trigger at_once into output_{math_function}_ctb as select ts, {math_function}(c1), {math_function}(c2), c3 from scalar_ct1;')
                self.tdSql.execute(f'create stream tb_{math_function}_stream trigger at_once into output_{math_function}_tb as select ts, {math_function}(c1), {math_function}(c2), c3 from scalar_tb;')
            self.tdCom.check_stream_field_type(f"describe output_{math_function}_stb", math_function)
            self.tdCom.check_stream_field_type(f"describe output_{math_function}_ctb", math_function)
            self.tdCom.check_stream_field_type(f"describe output_{math_function}_tb", math_function)
        for string_function in string_function_list:
            if string_function == "concat":
                self.tdSql.execute(f'create stream stb_{string_function}_stream into output_{string_function}_stb as select ts, {string_function}(c3, c4), {string_function}(c3, c5), {string_function}(c4, c5), {string_function}(c3, c4, c5) from scalar_stb;')
                self.tdSql.execute(f'create stream ctb_{string_function}_stream into output_{string_function}_ctb as select ts, {string_function}(c3, c4), {string_function}(c3, c5), {string_function}(c4, c5), {string_function}(c3, c4, c5) from scalar_ct1;')
                self.tdSql.execute(f'create stream tb_{string_function}_stream into output_{string_function}_tb as select ts, {string_function}(c3, c4), {string_function}(c3, c5), {string_function}(c4, c5), {string_function}(c3, c4, c5) from scalar_tb;')
            elif string_function == "concat_ws":
                self.tdSql.execute(f'create stream stb_{string_function}_stream into output_{string_function}_stb as select ts, {string_function}("aND", c3, c4), {string_function}("and", c3, c5), {string_function}("And", c4, c5), {string_function}("AND", c3, c4, c5) from scalar_stb;')
                self.tdSql.execute(f'create stream ctb_{string_function}_stream into output_{string_function}_ctb as select ts, {string_function}("aND", c3, c4), {string_function}("and", c3, c5), {string_function}("And", c4, c5), {string_function}("AND", c3, c4, c5) from scalar_ct1;')
                self.tdSql.execute(f'create stream tb_{string_function}_stream into output_{string_function}_tb as select ts, {string_function}("aND", c3, c4), {string_function}("and", c3, c5), {string_function}("And", c4, c5), {string_function}("AND", c3, c4, c5) from scalar_tb;')
            elif string_function == "substr":
                self.tdSql.execute(f'create stream stb_{string_function}_stream into output_{string_function}_stb as select ts, {string_function}(c3, 2), {string_function}(c3, 2, 2), {string_function}(c4, 5, 1), {string_function}(c5, 3, 4) from scalar_stb;')
                self.tdSql.execute(f'create stream ctb_{string_function}_stream into output_{string_function}_ctb as select ts, {string_function}(c3, 2), {string_function}(c3, 2, 2), {string_function}(c4, 5, 1), {string_function}(c5, 3, 4) from scalar_ct1;')
                self.tdSql.execute(f'create stream tb_{string_function}_stream into output_{string_function}_tb as select ts, {string_function}(c3, 2), {string_function}(c3, 2, 2), {string_function}(c4, 5, 1), {string_function}(c5, 3, 4) from scalar_tb;')
            else:
                self.tdSql.execute(f'create stream stb_{string_function}_stream into output_{string_function}_stb as select ts, {string_function}(c3), {string_function}(c4), {string_function}(c5) from scalar_stb;')
                self.tdSql.execute(f'create stream ctb_{string_function}_stream into output_{string_function}_ctb as select ts, {string_function}(c3), {string_function}(c4), {string_function}(c5) from scalar_ct1;')
                self.tdSql.execute(f'create stream tb_{string_function}_stream into output_{string_function}_tb as select ts, {string_function}(c3), {string_function}(c4), {string_function}(c5) from scalar_tb;')
            self.tdCom.check_stream_field_type(f"describe output_{string_function}_stb", string_function)
            self.tdCom.check_stream_field_type(f"describe output_{string_function}_ctb", string_function)
            self.tdCom.check_stream_field_type(f"describe output_{string_function}_tb", string_function)

        for tbname in ["scalar_ct1", "scalar_tb"]:
            self.tdSql.execute(f'insert into {tbname} values ({self.date_time}, 100, 100.1, "beijing", "taos", "Taos");')
            self.tdSql.execute(f'insert into {tbname} values ({self.date_time}+1s, -50, -50.1, "tianjin", "taosdata", "Taosdata");')
            self.tdSql.execute(f'insert into {tbname} values ({self.date_time}+2s, 0, Null, "hebei", "TDengine", Null);')

        count = 1
        step_count = 1
        for i in range(1, 20):
            print(i)
            time.sleep(1)
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
                    self.tdCom.check_query_data(f'select `{math_function}(c1, 2)`, `{math_function}(c2, 2)` from output_{math_function}_stb;', f'select {math_function}(c1, 2), {math_function}(c2, 2) from scalar_stb;')
                    self.tdCom.check_query_data(f'select `{math_function}(c1, 2)`, `{math_function}(c2, 2)` from output_{math_function}_ctb;', f'select {math_function}(c1, 2), {math_function}(c2, 2) from scalar_ct1;')
                    self.tdCom.check_query_data(f'select `{math_function}(c1, 2)`, `{math_function}(c2, 2)` from output_{math_function}_tb;', f'select {math_function}(c1, 2), {math_function}(c2, 2) from scalar_tb;')
                else:
                    self.tdCom.check_query_data(f'select `{math_function}(c1)`, `{math_function}(c2)` from output_{math_function}_stb;', f'select {math_function}(c1), {math_function}(c2) from scalar_stb;')
                    self.tdCom.check_query_data(f'select `{math_function}(c1)`, `{math_function}(c2)` from output_{math_function}_ctb;', f'select {math_function}(c1), {math_function}(c2) from scalar_ct1;')
                    self.tdCom.check_query_data(f'select `{math_function}(c1)`, `{math_function}(c2)` from output_{math_function}_tb;', f'select {math_function}(c1), {math_function}(c2) from scalar_tb;')
            # for string_function in string_function_list:
            #     if string_function == "concat":
            #         self.tdCom.check_stream(f'select `{string_function}(c3, c4)`, `{string_function}(c3, c5)`, `{string_function}(c4, c5)`, `{string_function}(c3, c4, c5)` from output_{string_function}_stb;', f'select {string_function}(c3, c4), {string_function}(c3, c5), {string_function}(c4, c5), {string_function}(c3, c4, c5) from scalar_stb;', count-1)
            #         self.tdCom.check_stream(f'select `{string_function}(c3, c4)`, `{string_function}(c3, c5)`, `{string_function}(c4, c5)`, `{string_function}(c3, c4, c5)` from output_{string_function}_ctb;', f'select {string_function}(c3, c4), {string_function}(c3, c5), {string_function}(c4, c5), {string_function}(c3, c4, c5) from scalar_ct1;', count-1)
            #         self.tdCom.check_stream(f'select `{string_function}(c3, c4)`, `{string_function}(c3, c5)`, `{string_function}(c4, c5)`, `{string_function}(c3, c4, c5)` from output_{string_function}_tb;', f'select {string_function}(c3, c4), {string_function}(c3, c5), {string_function}(c4, c5), {string_function}(c3, c4, c5) from scalar_tb;', count-1)
            #     elif string_function == "concat_ws":
            #         self.tdCom.check_stream(f'select `{string_function}("aND", c3, c4)`, `{string_function}("and", c3, c5)`, `{string_function}("And", c4, c5)`, `{string_function}("AND", c3, c4, c5)` from output_{string_function}_stb;', f'select {string_function}("aND", c3, c4), {string_function}("and", c3, c5), {string_function}("And", c4, c5), {string_function}("AND", c3, c4, c5) from scalar_stb;', count-1)
            #         self.tdCom.check_stream(f'select `{string_function}("aND", c3, c4)`, `{string_function}("and", c3, c5)`, `{string_function}("And", c4, c5)`, `{string_function}("AND", c3, c4, c5)` from output_{string_function}_ctb;', f'select {string_function}("aND", c3, c4), {string_function}("and", c3, c5), {string_function}("And", c4, c5), {string_function}("AND", c3, c4, c5) from scalar_ct1;', count-1)
            #         self.tdCom.check_stream(f'select `{string_function}("aND", c3, c4)`, `{string_function}("and", c3, c5)`, `{string_function}("And", c4, c5)`, `{string_function}("AND", c3, c4, c5)` from output_{string_function}_tb;', f'select {string_function}("aND", c3, c4), {string_function}("and", c3, c5), {string_function}("And", c4, c5), {string_function}("AND", c3, c4, c5) from scalar_tb;', count-1)
            #     elif string_function == "substr":
            #         self.tdCom.check_stream(f'select `{string_function}(c3, 2)`, `{string_function}(c3, 2, 2)`, `{string_function}(c4, 5, 1)`, `{string_function}(c5, 3, 4)` from output_{string_function}_stb;', f'select {string_function}(c3, 2), {string_function}(c3, 2, 2), {string_function}(c4, 5, 1), {string_function}(c5, 3, 4) from scalar_stb;', count-1)
            #         self.tdCom.check_stream(f'select `{string_function}(c3, 2)`, `{string_function}(c3, 2, 2)`, `{string_function}(c4, 5, 1)`, `{string_function}(c5, 3, 4)` from output_{string_function}_ctb;', f'select {string_function}(c3, 2), {string_function}(c3, 2, 2), {string_function}(c4, 5, 1), {string_function}(c5, 3, 4) from scalar_ct1;', count-1)
            #         self.tdCom.check_stream(f'select `{string_function}(c3, 2)`, `{string_function}(c3, 2, 2)`, `{string_function}(c4, 5, 1)`, `{string_function}(c5, 3, 4)` from output_{string_function}_tb;', f'select {string_function}(c3, 2), {string_function}(c3, 2, 2), {string_function}(c4, 5, 1), {string_function}(c5, 3, 4) from scalar_tb;', count-1)
            #     else:
            #         self.tdCom.check_stream(f'select `{string_function}(c3)`, `{string_function}(c4)`, `{string_function}(c5)` from output_{string_function}_stb;', f'select {string_function}(c3), {string_function}(c4), {string_function}(c5) from scalar_stb;', count-1)
            #         self.tdCom.check_stream(f'select `{string_function}(c3)`, `{string_function}(c4)`, `{string_function}(c5)` from output_{string_function}_ctb;', f'select {string_function}(c3), {string_function}(c4), {string_function}(c5) from scalar_ct1;', count-1)
            #         self.tdCom.check_stream(f'select `{string_function}(c3)`, `{string_function}(c4)`, `{string_function}(c5)` from output_{string_function}_tb;', f'select {string_function}(c3), {string_function}(c4), {string_function}(c5) from scalar_tb;', count-1)
            
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
        #     self.tdCom.check_stream(f'select {select_elm} from output_data_filter_stb where {filter_sql};', f'select {select_elm} from data_filter_stb where {filter_sql};', count-1)
        #     self.tdCom.check_stream(f'select {select_elm} from output_data_filter_ctb where {filter_sql};', f'select {select_elm} from data_filter_ct1 where {filter_sql};', count-1)
        #     self.tdCom.check_stream(f'select {select_elm} from output_data_filter_tb where {filter_sql};', f'select {select_elm} from data_filter_tb where {filter_sql};', count-1)       



        # self.tdSql.execute(f'insert into scalar_ct1 values ({self.date_time}+3s, -1, 1, "hebei", Null, "Bigdata");')
        # for math_function in math_function_list:
        #     if math_function == "log" or math_function == "pow":
        #         self.tdCom.check_stream(f'select `{math_function}(c1, 2)`, `{math_function}(c2, 2)` from output_{math_function}_stb;', f'select {math_function}(c1, 2), {math_function}(c2, 2) from scalar_stb;', 4)
        #     else:
        #         self.tdCom.check_stream(f'select `{math_function}(c1)`, `{math_function}(c2)` from output_{math_function}_stb;', f'select {math_function}(c1), {math_function}(c2) from scalar_stb;', 4)
        
        # for string_function in string_function_list:
        #     if string_function == "concat":
        #         self.tdCom.check_stream(f'select `{string_function}(c3, c4)`, `{string_function}(c3, c5)`, `{string_function}(c4, c5)`, `{string_function}(c3, c4, c5)` from output_{string_function}_stb;', f'select {string_function}(c3, c4), {string_function}(c3, c5), {string_function}(c4, c5), {string_function}(c3, c4, c5) from scalar_stb;', 4)
        #     elif string_function == "concat_ws":
        #         self.tdCom.check_stream(f'select `{string_function}("aND", c3, c4)`, `{string_function}("and", c3, c5)`, `{string_function}("And", c4, c5)`, `{string_function}("AND", c3, c4, c5)` from output_{string_function}_stb;', f'select {string_function}("aND", c3, c4), {string_function}("and", c3, c5), {string_function}("And", c4, c5), {string_function}("AND", c3, c4, c5) from scalar_stb;', 4)
        #     elif string_function == "substr":
        #         self.tdCom.check_stream(f'select `{string_function}(c3, 2)`, `{string_function}(c3, 2, 2)`, `{string_function}(c4, 5, 1)`, `{string_function}(c5, 3, 4)` from output_{string_function}_stb;', f'select {string_function}(c3, 2), {string_function}(c3, 2, 2), {string_function}(c4, 5, 1), {string_function}(c5, 3, 4) from scalar_stb;', 4)
        #     else:
        #         self.tdCom.check_stream(f'select `{string_function}(c3)`, `{string_function}(c4)`, `{string_function}(c5)` from output_{string_function}_stb;', f'select {string_function}(c3), {string_function}(c4), {string_function}(c5) from scalar_stb;', 4)

    def life_cycle(self):
        self.case_name = sys._getframe().f_code.co_name
        self.prepare_data()
        self.tdCom.createDb(dbname="long_life_cycle", vgroups=1, duration="14400m", keep="14400m")
        self.tdCom.createDb(dbname="short_life_cycle", vgroups=1, duration="7200m", keep="7200m")
        self.tdSql.execute('create table if not exists long_life_cycle.life_cycle_stb (ts timestamp, c1 int, c2 double, c3 binary(20), c4 nchar(20), c5 nchar(20)) tags (t1 int);')
        self.tdSql.execute('create table long_life_cycle.life_cycle_ct1 using long_life_cycle.life_cycle_stb tags(10);')
        self.tdSql.execute('create table if not exists short_life_cycle.life_cycle_stb (ts timestamp, c1 int, c2 double, c3 binary(20), c4 nchar(20), c5 nchar(20)) tags (t1 int);')
        self.tdSql.execute('create table short_life_cycle.life_cycle_ct1 using short_life_cycle.life_cycle_stb tags(10);')
        self.tdSql.execute('create table if not exists long_life_cycle.life_cycle_tb (ts timestamp, c1 int, c2 double, c3 binary(20), c4 nchar(20), c5 nchar(20));')
        self.tdSql.execute('create table if not exists short_life_cycle.life_cycle_tb (ts timestamp, c1 int, c2 double, c3 binary(20), c4 nchar(20), c5 nchar(20));')

        self.tdCom.write_latency(self.case_name)
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
        self.tdCom.check_stream('select ts, c1, c2, c3, c4, c5 from short_life_cycle.output_life_cycle_stb;', 'select ts, c1, c2, c3, c4, c5 from long_life_cycle.life_cycle_stb;', 3)
        self.tdCom.check_stream('select ts, c1, c2, c3, c4, c5 from short_life_cycle.output_life_cycle_ctb;', 'select ts, c1, c2, c3, c4, c5 from long_life_cycle.life_cycle_ct1;', 3)
        self.tdCom.check_stream('select ts, c1, c2, c3, c4, c5 from short_life_cycle.output_life_cycle_tb;', 'select ts, c1, c2, c3, c4, c5 from long_life_cycle.life_cycle_tb;', 3)
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
        self.prepare_data()
        self.tdSql.execute('create table if not exists tandem_stb1 (ts timestamp, c1 int, c2 double, c3 binary(20), c4 binary(20), c5 nchar(20)) tags (t1 int);')
        self.tdSql.execute('create table tandem_ct1 using tandem_stb1 tags(1);')
        self.tdSql.execute('create table if not exists tandem_stb2 (ts timestamp, c1 int, c2 double, c3 binary(20), c4 binary(20), c5 nchar(20)) tags (t1 int);')
        self.tdSql.execute('create table tandem_ct2 using tandem_stb2 tags(1);')
        self.tdCom.write_latency(self.case_name)
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
        self.tdCom.write_latency(self.case_name)
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

        self.tdCom.check_stream('select count(*) from output_disorder_data_stb;', 'select count(*) from disorder_data_stb;', 1)
        self.tdCom.check_stream('select count(*) from output_disorder_data_ctb;', 'select count(*) from disorder_data_ct1;', 1)
        self.tdCom.check_stream('select count(*) from output_disorder_data_tb;', 'select count(*) from disorder_data_tb;', 1)
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
            self.tdCom.check_stream('select count(*) from output_disorder_data_stb;', 'select count(*) from disorder_data_stb;', 1)
            self.tdCom.check_stream('select count(*) from output_disorder_data_ctb;', 'select count(*) from disorder_data_ct1;', 1)
            self.tdCom.check_stream('select count(*) from output_disorder_data_tb;', 'select count(*) from disorder_data_tb;', 1)

    

    def trigger_max_delay(self):
        self.case_name = sys._getframe().f_code.co_name
        self.tdSql.execute('create table if not exists trigger_max_delay_stb (ts timestamp, c1 int, c2 double, c3 varchar(100), c4 bool) tags (t1 int, t2 double, t3 varchar(100), t4 bool);')
        self.tdSql.execute('create table trigger_max_delay_ct1 using trigger_max_delay_stb tags(10, 10.1, "Beijing", True);')
        self.tdSql.execute('create table if not exists trigger_max_delay_tb (ts timestamp, c1 int, c2 double, c3 varchar(100), c4 bool);')
        
        self.tdCom.write_latency(self.case_name)
        # stb not supported
        # self.tdSql.execute(f'create stream stb_trigger_max_delay_stream into output_trigger_max_delay_stb as select _wstart AS start, max(c1) from trigger_max_delay_stb trigger_max_delay(c1);')
        # ctb
        self.tdSql.execute(f'create stream ctb_trigger_max_delay_stream trigger max_delay 2s into output_trigger_max_delay_ctb as select _wstart AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from trigger_max_delay_ct1 state_window(c1);')
        # tb
        self.tdSql.execute(f'create stream tb_trigger_max_delay_stream trigger max_delay 2s into output_trigger_max_delay_tb as select _wstart AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from trigger_max_delay_tb state_window(c1);')
        for tbname in ["trigger_max_delay_ct1", "trigger_max_delay_tb"]:
            self.tdSql.execute(f'insert into {tbname} values (1653547828591, 100, 100.1, "Beijing", True);')
            self.tdSql.execute(f'insert into {tbname} values (1653547828591+1s, -100, -100.1, "Tianjin", False);')
            self.tdSql.execute(f'insert into {tbname} values (1653547828591+2s, 50, 50.3, "HeBei", False);')
        time.sleep(2)
        # self.tdCom.check_stream('select start, `min(c1)` from output_trigger_max_delay_stb;', 'select _wstart AS start, max(c1) from trigger_max_delay_stb trigger_max_delay(c1);', 1)
        self.tdCom.check_stream('select start, `min(c1)`, `max(c2)`, `sum(c1)`, `first(c1)`, `last(c1)`, `apercentile(c1, 50)` from output_trigger_max_delay_ctb;', 'select _wstart AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from trigger_max_delay_ct1 state_window(c1);', 3)
        self.tdCom.check_stream('select start, `min(c1)`, `max(c2)`, `sum(c1)`, `first(c1)`, `last(c1)`, `apercentile(c1, 50)` from output_trigger_max_delay_tb;', 'select _wstart AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from trigger_max_delay_tb state_window(c1);', 3)
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
            # self.tdCom.check_stream('select start, `min(c1)` from output_trigger_max_delay_stb;', 'select _wstart AS start, max(c1) from trigger_max_delay_stb trigger_max_delay(c1);', 1)
            time.sleep(2)
            self.tdCom.check_stream('select start, `min(c1)`, `max(c2)`, `sum(c1)`, `first(c1)`, `last(c1)`, `apercentile(c1, 50)` from output_trigger_max_delay_ctb;', 'select _wstart AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from trigger_max_delay_ct1 state_window(c1);', 4)
            self.tdCom.check_stream('select start, `min(c1)`, `max(c2)`, `sum(c1)`, `first(c1)`, `last(c1)`, `apercentile(c1, 50)` from output_trigger_max_delay_tb;', 'select _wstart AS start, min(c1), max(c2), sum(c1), first(c1), last(c1), apercentile(c1, 50) from trigger_max_delay_tb state_window(c1);', 4)

    def cal_watermark_window_close_interval_endts(self, start_ts, interval, watermark=None):
        """cal endts for close window

        :param start_ts: [start timestamp: self.date_time]
        :type start_ts: [epoch time]
        :param interval: [second level]
        :type interval: [s]
        :param watermark: [second level and > interval]
        :type watermark: [s]
        :param precision: [default "ms" and only support "ms" now]
        :type precision: str, optional
        """
        if watermark is not None:
            return int(start_ts/self.offset)*self.offset + (interval - (int(start_ts/self.offset))%interval)*self.offset + watermark*self.offset
        else:
            return int(start_ts/self.offset)*self.offset + (interval - (int(start_ts/self.offset))%interval)*self.offset

    
    def cal_watermark_window_close_session_endts(self, start_ts, watermark=None, session=None):
        """cal endts for close window

        :param start_ts: [start timestamp: self.date_time]
        :type start_ts: [epoch time]
        :param watermark: [second level and > session]
        :type watermark: [s]
        :param precision: [default "ms" and only support "ms" now]
        :type precision: str, optional
        """
        if watermark is not None:
            return start_ts + watermark*self.offset + 1
        else:
            return start_ts + session + 1

    def clean_env(self):
        self.tdCom.drop_all_streams()
        self.tdCom.drop_all_db()

    def prepare_data(self, interval=None, watermark=None, session=None, state_window=None, state_window_max=127, interation=3, range_count=None, precision="ms"):
        self.clean_env()
        self.dataDict = {
            "stb_name" : f"{self.case_name}_stb",
            "ctb_name" : f"{self.case_name}_ct1",
            "tb_name" : f"{self.case_name}_tb1",
            "interval" : interval,
            "watermark": watermark,
            "session": session,
            "state_window": state_window,
            "state_window_max": state_window_max,
            "iteration": interation,
            "range_count": range_count,
            "start_ts": 1655903478508,
        }
        if range_count is not None:
            self.range_count = range_count
        if precision is not None:
            self.precision = precision
        self.set_precision_offset(self.precision)

        self.stb_name = self.dataDict["stb_name"]
        self.ctb_name = self.dataDict["ctb_name"]
        self.tb_name = self.dataDict["tb_name"]
        self.stb_stream_des_table = f'{self.stb_name}{self.des_table_suffix}'
        self.ctb_stream_des_table = f'{self.ctb_name}{self.des_table_suffix}'
        self.tb_stream_des_table = f'{self.tb_name}{self.des_table_suffix}'
        self.date_time = self.tdCom.genTs(precision=self.precision)[0]
        self.tdCom.stream_latency_log = self.run_log_dir + "/latency.log"
        
        self.tdCom.createDb(vgroups=self.vgroups, precision=self.precision)
        self.tdCom.create_stable(stbname=self.stb_name)
        self.tdCom.create_ctable(stbname=self.stb_name, ctbname=self.ctb_name)
        self.tdCom.create_table(tbname=self.tb_name)

    def udf_interval_order(self, interval, precision=None, vgroups=1, udf_size=8):
        self.case_name = sys._getframe().f_code.co_name
        self.prepare_data(interval=interval, precision=precision, vgroups=vgroups)
        self.tdCom.drop_all_udfs()
        self.tdCom.write_latency(self.case_name)
        udf1 = "udf1"
        udf2 = "udf2"
        self.build_udf_so()
        self.tdCom.create_udf(udf1, self.udf1, udf_size)
        # create stb/ctb/tb stream
        self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select _wstart AS start, {self.udf_stb_source_select_str}  from {self.stb_name} interval({self.dataDict["interval"]}s)')
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstart AS start, {self.udf_stb_source_select_str}  from {self.ctb_name} interval({self.dataDict["interval"]}s)')
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstart AS start, {self.udf_tb_source_select_str}  from {self.tb_name} interval({self.dataDict["interval"]}s)')

        # insert data
        count = 1
        step_count = 1
        for i in range(1, self.range_count):
            ctb_name = self.tdCom.get_long_name()
            self.tdCom.create_ctable(stbname=self.stb_name, ctbname=ctb_name)
            if i % 2 == 0:
                step_count += i
                for j in range(count, step_count):
                    self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=f'{self.date_time}+{j}s')
                count += i
            else:
                step_count += 1
                for i in range(2):
                    self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=f'{self.date_time}+{count}s')
                count += 1
            # check result
            self.tdCom.check_query_data(f'select start, {self.udf_stb_output_select_str} from {self.stb_name}{self.des_table_suffix}', f'select _wstart AS start, {self.udf_stb_source_select_str}  from {self.stb_name} partition by tbname interval({self.dataDict["interval"]}s)')
            self.tdCom.check_query_data(f'select start, {self.udf_stb_output_select_str} from {self.ctb_name}{self.des_table_suffix}', f'select _wstart AS start, {self.udf_stb_source_select_str}  from {self.ctb_name} partition by tbname interval({self.dataDict["interval"]}s)')
            self.tdCom.check_query_data(f'select start, {self.udf_tb_output_select_str} from {self.tb_name}{self.des_table_suffix}', f'select _wstart AS start, {self.udf_tb_source_select_str}  from {self.tb_name} partition by tbname interval({self.dataDict["interval"]}s)')

    def partitionby_interval_order(self, interval, precision=None, vgroups=1):
        self.case_name = sys._getframe().f_code.co_name
        self.prepare_data(interval=interval, precision=precision, vgroups=vgroups)
        self.tdCom.write_latency(self.case_name)
        ctb_name_list = list()
        for i in range(1, self.range_count):
            ctb_name = self.tdCom.get_long_name()
            ctb_name_list.append(ctb_name)
            self.tdCom.create_ctable(stbname=self.stb_name, ctbname=ctb_name)

        # create stb/ctb/tb stream
        self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select _wstart AS start, {self.partition_by_stb_source_select_str}  from {self.stb_name} partition by tbname interval({self.dataDict["interval"]}s)')
        # insert data
        count = 1
        step_count = 1
        for i in range(1, self.range_count):
            ctb_name = self.tdCom.get_long_name()
            self.tdCom.create_ctable(stbname=self.stb_name, ctbname=ctb_name)
            if i % 2 == 0:
                step_count += i
                for j in range(count, step_count):
                    self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=f'{self.date_time}+{j}s')
                    for ctb_name in ctb_name_list:
                        self.tdCom.insert_rows(tbname=ctb_name, ts_value=f'{self.date_time}+{j}s')
                count += i
            else:
                step_count += 1
                for i in range(2):
                    self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=f'{self.date_time}+{count}s')
                    for ctb_name in ctb_name_list:
                        self.tdCom.insert_rows(tbname=ctb_name, ts_value=f'{self.date_time}+{count}s')
                count += 1
            # check result
            for colname in self.partition_by_downsampling_function_list:
                self.tdCom.check_query_data(f'select `{colname}` from {self.stb_name}{self.des_table_suffix} order by `{colname}`;', f'select {colname}  from {self.stb_name} partition by tbname interval({self.dataDict["interval"]}s) order by `{colname}`;')

    def window_close_state_window_order(self, state_window, interation, vgroups=1):
        self.case_name = sys._getframe().f_code.co_name
        self.prepare_data(interation=interation, state_window=state_window, vgroups=vgroups)
        state_window_col_name = self.dataDict["state_window"]
        self.tdCom.write_latency(self.case_name)
        self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select _wstart AS start, {self.stb_source_select_str}  from {self.stb_name} state_window({state_window_col_name})', trigger_mode="window_close")
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstart AS start, {self.stb_source_select_str}  from {self.ctb_name} state_window({state_window_col_name})', trigger_mode="window_close")
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstart AS start, {self.tb_source_select_str}  from {self.tb_name} state_window({state_window_col_name})', trigger_mode="window_close")
        range_times = self.dataDict['iteration']
        state_window_max = self.dataDict['state_window_max']
        for i in range(range_times):
            state_window_value = random.randint(int((i)*state_window_max/range_times), int((i+1)*state_window_max/range_times))
            for i in range(2, range_times+3):
                self.tdSql.execute(f'insert into {self.ctb_name} (ts, {state_window_col_name}) values ({self.date_time}, {state_window_value})')
                self.tdSql.execute(f'insert into {self.tb_name} (ts, {state_window_col_name}) values ({self.date_time}, {state_window_value})')
                self.date_time += 1
                # ! TD-16806

    def max_delay_state_window_order(self, state_window, interation, vgroups=1):
        self.case_name = sys._getframe().f_code.co_name
        # TODO




    


    def max_delay_interval_order(self, interval, interation, max_delay, watermark=None, precision=None, vgroups=1):
        self.case_name = sys._getframe().f_code.co_name
        if watermark is not None:
            self.case_name = "watermark" + sys._getframe().f_code.co_name
        self.prepare_data(interval=interval, watermark=watermark, interation=interation, precision=precision, vgroups=vgroups)
        self.tdCom.write_latency(self.case_name)

        if watermark is not None:
            watermark_value = f'{self.dataDict["watermark"]}s'
        else:
            watermark_value = None
        max_delay_value = f'{self.tdCom.trans_time_to_s(max_delay)}s'
        # create stb/ctb/tb stream
        self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select _wstart AS start, {self.stb_source_select_str}  from {self.stb_name} interval({self.dataDict["interval"]}s)', trigger_mode="max_delay", watermark=watermark_value, max_delay=max_delay_value)
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstart AS start, {self.stb_source_select_str}  from {self.ctb_name} interval({self.dataDict["interval"]}s)', trigger_mode="max_delay", watermark=watermark_value, max_delay=max_delay_value)
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstart AS start, {self.tb_source_select_str}  from {self.tb_name} interval({self.dataDict["interval"]}s)', trigger_mode="max_delay", watermark=watermark_value, max_delay=max_delay_value)
        init_num = 0
        for i in range(self.dataDict['iteration']):
            if i == 0:
                if watermark is not None:
                    window_close_ts = self.cal_watermark_window_close_interval_endts(self.date_time, self.dataDict['interval'], self.dataDict['watermark'])
                else:
                    window_close_ts = self.cal_watermark_window_close_interval_endts(self.date_time, self.dataDict['interval'])
            else:
                self.date_time = window_close_ts + self.offset
                window_close_ts += self.dataDict['interval']*self.offset
            for num in range(int(window_close_ts/self.offset-self.date_time/self.offset)):
                self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=self.date_time+num*self.offset)
                self.tdCom.insert_rows(tbname=self.tb_name, ts_value=self.date_time+num*self.offset)
                for tbname in [self.stb_stream_des_table, self.ctb_stream_des_table, self.tb_stream_des_table]:
                    if tbname != self.tb_stream_des_table:
                        self.tdSql.query(f'select start, {self.stb_output_select_str} from {tbname}')
                    else:
                        self.tdSql.query(f'select start, {self.tb_output_select_str} from {tbname}')
                    self.tdSql.checkEqual(self.tdSql.query_row, init_num)
            
            self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=window_close_ts-1)
            self.tdCom.insert_rows(tbname=self.tb_name, ts_value=window_close_ts-1)
            for tbname in [self.stb_stream_des_table, self.ctb_stream_des_table, self.tb_stream_des_table]:
                if tbname != self.tb_stream_des_table:
                    self.tdSql.query(f'select start, {self.stb_output_select_str} from {tbname}')
                else:
                    self.tdSql.query(f'select start, {self.tb_output_select_str} from {tbname}')
                self.tdSql.checkEqual(self.tdSql.query_row, init_num)

            self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=window_close_ts)
            self.tdCom.insert_rows(tbname=self.tb_name, ts_value=window_close_ts)
            if i == 0:
                init_num = 2 + i
                if watermark is not None:
                    init_num += 1
            else:
                init_num += 1
            
            for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
                if tbname != self.tb_name:
                    self.tdCom.check_stream(f'select start, {self.stb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS start, {self.stb_source_select_str}  from {tbname} interval({self.dataDict["interval"]}s)', init_num, max_delay)
                else:
                    self.tdCom.check_stream(f'select start, {self.tb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS start, {self.tb_source_select_str}  from {tbname} interval({self.dataDict["interval"]}s)', init_num, max_delay)

    

    def max_delay_session_order(self, session, interation, max_delay, precision=None, vgroups=1):
        # select * from ource or destination is synchronous, there is no point in delay
        self.case_name = sys._getframe().f_code.co_name
        self.prepare_data(session=session, interation=interation, precision=precision, vgroups=vgroups)
        self.date_time = self.dataDict["start_ts"]

        self.tdCom.write_latency(self.case_name)
        max_delay_value = f'{self.tdCom.trans_time_to_s(max_delay)}s'
        # create stb/ctb/tb stream
        self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select _wstart AS start, {self.stb_source_select_str}  from {self.stb_name} session(ts, {self.dataDict["session"]}s)', trigger_mode="max_delay", max_delay=max_delay_value)
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstart AS start, {self.stb_source_select_str}  from {self.ctb_name} session(ts, {self.dataDict["session"]}s)', trigger_mode="max_delay", max_delay=max_delay_value)
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstart AS start, {self.tb_source_select_str}  from {self.tb_name} session(ts, {self.dataDict["session"]}s)', trigger_mode="max_delay", max_delay=max_delay_value)
        for i in range(self.dataDict['iteration']):
            if i == 0:
                window_close_ts = self.cal_watermark_window_close_session_endts(self.date_time, self.dataDict['session'])
            else:
                self.date_time = window_close_ts + 1
                window_close_ts = self.cal_watermark_window_close_session_endts(self.date_time, self.dataDict['session'])
            for ts_value in [self.date_time, window_close_ts]:
                self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=ts_value)
                self.tdCom.insert_rows(tbname=self.tb_name, ts_value=ts_value)
            
            for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
                if tbname != self.tb_name:
                    self.tdCom.check_stream(f'select start, {self.stb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS start, {self.stb_source_select_str}  from {tbname} session(ts, {self.dataDict["session"]}s) limit {i+1}', i+1)
                else:
                    self.tdCom.check_stream(f'select start, {self.tb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS start, {self.tb_source_select_str}  from {tbname} session(ts, {self.dataDict["session"]}s) limit {i+1}', i+1)

    def watermark_window_close_session_order(self, session, watermark, interation, precision=None, vgroups=1):
        self.case_name = sys._getframe().f_code.co_name
        self.prepare_data(session=session, watermark=watermark, interation=interation, interval=None, precision=precision, vgroups=vgroups)
        self.date_time = self.dataDict["start_ts"]
        # dataDict = {
        #     "stb_name" : f"{self.case_name}_stb",
        #     "ctb_name" : f"{self.case_name}_ct1",
        #     "tb_name" : f"{self.case_name}_tb1",
        #     "interval" : random.randint(10, 15),
        #     "watermark": random.randint(15, 20),
        #     "iteration": 3,
        #     # "start_ts": 1655903478508
        # }
        # # self.date_time = dataDict["start_ts"]
        # stb_name = dataDict["stb_name"]
        # ctb_name = dataDict["ctb_name"]
        # tb_name = dataDict["tb_name"]
        # stb_stream_des_table = f'{stb_name}{self.des_table_suffix}'
        # ctb_stream_des_table = f'{ctb_name}{self.des_table_suffix}'
        # tb_stream_des_table = f'{tb_name}{self.des_table_suffix}'

        self.tdCom.write_latency(self.case_name)
        # create stb/ctb/tb stream
        self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select _wstart AS start, {self.stb_source_select_str}  from {self.stb_name} session(ts, {self.dataDict["session"]}s)', trigger_mode="window_close", watermark=f'{self.dataDict["watermark"]}s')
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstart AS start, {self.stb_source_select_str}  from {self.ctb_name} session(ts, {self.dataDict["session"]}s)', trigger_mode="window_close", watermark=f'{self.dataDict["watermark"]}s')
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstart AS start, {self.tb_source_select_str}  from {self.tb_name} session(ts, {self.dataDict["session"]}s)', trigger_mode="window_close", watermark=f'{self.dataDict["watermark"]}s')
        for i in range(self.dataDict['iteration']):
            if i == 0:
                window_close_ts = self.cal_watermark_window_close_session_endts(self.date_time, self.dataDict['watermark'])
            else:
                self.date_time = window_close_ts + 1
                window_close_ts = self.cal_watermark_window_close_session_endts(self.date_time, self.dataDict['watermark'])
                # window_close_ts += self.dataDict['interval']*self.offset
            # for num in range(int(window_close_ts/self.offset-self.date_time/self.offset)):
            #     self.tdCom.insert_rows(tbname=self.dataDict["ctb_name"], ts_value=self.date_time+num*self.offset)
            #     self.tdCom.insert_rows(tbname=self.dataDict["tb_name"], ts_value=self.date_time+num*self.offset)
            #     for tbname in [self.stb_stream_des_table, self.ctb_stream_des_table, self.tb_stream_des_table]:
            #         self.tdSql.query(f'select start, {self.output_select_str} from {tbname}')
            #         self.tdSql.checkEqual(self.tdSql.query_row, i)
            

            for ts_value in [self.date_time, window_close_ts-1]:
                self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=ts_value)
                self.tdCom.insert_rows(tbname=self.tb_name, ts_value=ts_value)
                for tbname in [self.stb_stream_des_table, self.ctb_stream_des_table, self.tb_stream_des_table]:
                    if tbname != self.tb_stream_des_table:
                        self.tdSql.query(f'select start, {self.stb_output_select_str} from {tbname}')
                    else:
                        self.tdSql.query(f'select start, {self.tb_output_select_str} from {tbname}')
                    self.tdSql.checkEqual(self.tdSql.query_row, i)

            self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=window_close_ts)
            self.tdCom.insert_rows(tbname=self.tb_name, ts_value=window_close_ts)
            # for tbname in [stb_stream_des_table, ctb_stream_des_table, tb_stream_des_table]:
            for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
                if tbname != self.tb_name:
                    self.tdCom.check_stream(f'select start, {self.stb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS start, {self.stb_source_select_str}  from {tbname} session(ts, {self.dataDict["session"]}s) limit {i+1}', i+1)
                else:
                    self.tdCom.check_stream(f'select start, {self.tb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS start, {self.tb_source_select_str}  from {tbname} session(ts, {self.dataDict["session"]}s) limit {i+1}', i+1)

    def watermark_max_delay_session_order(self, session, watermark, interation, max_delay, precision=None, vgroups=1):
        self.case_name = sys._getframe().f_code.co_name
        self.prepare_data(session=session, watermark=watermark, interation=interation, interval=None, precision=precision, vgroups=vgroups)
        self.date_time = self.dataDict["start_ts"]

        self.tdCom.write_latency(self.case_name)
        max_delay_value = f'{self.tdCom.trans_time_to_s(max_delay)}s'
        # create stb/ctb/tb stream
        self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select _wstart AS start, {self.stb_source_select_str}  from {self.stb_name} session(ts, {self.dataDict["session"]}s)', trigger_mode="max_delay", watermark=f'{self.dataDict["watermark"]}s', max_delay=max_delay_value)
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstart AS start, {self.stb_source_select_str}  from {self.ctb_name} session(ts, {self.dataDict["session"]}s)', trigger_mode="max_delay", watermark=f'{self.dataDict["watermark"]}s', max_delay=max_delay_value)
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstart AS start, {self.tb_source_select_str}  from {self.tb_name} session(ts, {self.dataDict["session"]}s)', trigger_mode="max_delay", watermark=f'{self.dataDict["watermark"]}s', max_delay=max_delay_value)
        init_num = 0
        for i in range(self.dataDict['iteration']):
            if i == 0:
                window_close_ts = self.cal_watermark_window_close_session_endts(self.date_time, self.dataDict['watermark'])
            else:
                self.date_time = window_close_ts + 1
                window_close_ts = self.cal_watermark_window_close_session_endts(self.date_time, self.dataDict['watermark'])

            for ts_value in [self.date_time, window_close_ts-1]:
                self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=ts_value)
                self.tdCom.insert_rows(tbname=self.tb_name, ts_value=ts_value)
                for tbname in [self.stb_stream_des_table, self.ctb_stream_des_table, self.tb_stream_des_table]:
                    if tbname != self.tb_stream_des_table:
                        self.tdSql.query(f'select start, {self.stb_output_select_str} from {tbname}')
                    else:
                        self.tdSql.query(f'select start, {self.tb_output_select_str} from {tbname}')
                    self.tdSql.checkEqual(self.tdSql.query_row, init_num)

            self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=window_close_ts)
            self.tdCom.insert_rows(tbname=self.tb_name, ts_value=window_close_ts)
            # for tbname in [stb_stream_des_table, ctb_stream_des_table, tb_stream_des_table]:
            if i == 0:
                init_num = 2 + i
            else:
                init_num += 1
            for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
                if tbname != self.tb_name:
                    self.tdCom.check_stream(f'select start, {self.stb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS start, {self.stb_source_select_str}  from {tbname} session(ts, {self.dataDict["session"]}s)', init_num, max_delay)
                else:
                    self.tdCom.check_stream(f'select start, {self.tb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS start, {self.tb_source_select_str}  from {tbname} session(ts, {self.dataDict["session"]}s)', init_num, max_delay)


    def run(self):
        # ! TD-16915
        # self.data_filter()
        # self.at_once_interval(interval=random.randint(10, 15))
        # # self.alter_source_table(interval=random.randint(10, 15))
        # self.at_once_state_window(state_window="c1")
        # self.at_once_session(session=random.randint(10, 15))
        # self.window_close_interval(interval=random.randint(10, 15), watermark=None)
        # self.window_close_interval(interval=random.randint(10, 15), watermark=random.randint(15, 20))
        self.window_close_session(session=random.randint(10, 15))
        # self.watermark_window_close_session_order(session=random.randint(10, 15), watermark=random.randint(20, 30), interation=self.interation)
        # self.downsampling()
        # self.state_window_function()
        # # # self.session_window()
        # self.scalar_function()
        # self.life_cycle()
        # self.stream_tandem()
        # # self.disorder_data()
        # self.trigger_window_close()
        # self.trigger_max_delay()
        # TODO unfinished
        # self.udf_interval_order(interval=10)
        # self.partitionby_interval_order(interval=10)
        # self.window_close_state_window_order(state_window="c1", interation=self.interation)
        

        # self.max_delay_interval_order(interval=random.randint(10, 15), watermark=None, max_delay=f"{random.randint(1, 3)}s", interation=self.interation, vgroups=10)
        # self.max_delay_interval_order(interval=10, watermark=17, max_delay=f"2s", interation=10)
        # self.max_delay_session_order(session=random.randint(10, 15), max_delay=f"2s", interation=self.interation)
        # self.watermark_max_delay_session_order(session=random.randint(10, 15), watermark=random.randint(20, 30), max_delay=f"{random.randint(1, 3)}s", interation=self.interation)
        # self.data_filter()
        # self.life_cycle()
        # TODO confirm
        # self.downsampling()
        # self.state_window_function()
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

