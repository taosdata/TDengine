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

from copy import deepcopy
from taostest import TDCase, T
from taostest.util.common import TDCom
from taostest.util.remote import Remote
import time
import sys
import os
import random
from taostest.components import TaosD

class StreamComputingTest(TDCase):
    def init(self):
        self.stream_case_env_root = os.path.join(os.environ["TEST_ROOT"], "cases/stream_computing")
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self._remote)

        self.taospy_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taospy")
        self._fqdn = self.taospy_setting["fqdn"][0]
        
        self.taosd_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taosd")
        self.fqdn = self.taosd_setting["fqdn"][0]
        self.vnode_dir = self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"] + "/vnode"
        self.endpoint = self.taosd_setting["spec"]["config"]["firstEP"]

        self.cfg = self.tdCom.Boundary.DB_PARAM_VGROUPS_CONFIG

        self.case_name = None
        self.tbname = None
        self.precision = "ms"
        self.offset = 1000
        
        self.case_name = str()
        self.dbname = "stream_test"
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
            print(start_ts)
            print(session)
            return start_ts + session*self.offset + 1

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
        
        self.tdCom.createDb(dbname=self.dbname, vgroups=self.vgroups, precision=self.precision)
        self.tdCom.create_stable(dbname=self.dbname, stbname=self.stb_name)
        self.tdCom.create_ctable(dbname=self.dbname, stbname=self.stb_name, ctbname=self.ctb_name)
        self.tdCom.create_table(dbname=self.dbname, tbname=self.tb_name)

    def data_filter(self, need_return=False):
        self.update = False
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
        if need_return:
            return count

    def life_cycle(self, long_duration="14400m"):
        self.case_name = sys._getframe().f_code.co_name
        long_life_cycle_db = "long_life_cycle_db"
        long_life_cycle_stb = "long_life_cycle_stb"
        long_life_cycle_ctb = "long_life_cycle_ctb"
        long_life_cycle_tb = "long_life_cycle_tb"
        short_life_cycle_db = "short_life_cycle_db"
        short_life_cycle_stb = "short_life_cycle_stb"
        short_life_cycle_ctb = "short_life_cycle_ctb"
        short_life_cycle_tb = "short_life_cycle_tb"
        stb_stream_name = "stb_life_cycle_stream"
        ctb_stream_name = "ctb_life_cycle_stream"
        tb_stream_name = "tb_life_cycle_stream"
        stb_stream_target_tbname = f'{short_life_cycle_db}.output_life_cycle_stb'
        ctb_stream_target_tbname = f'{short_life_cycle_db}.output_life_cycle_ctb'
        tb_stream_target_tbname = f'{short_life_cycle_db}.output_life_cycle_tb'
        stb_source_sql = f'select * from {long_life_cycle_db}.{long_life_cycle_stb}'
        ctb_source_sql = f'select * from {long_life_cycle_db}.{long_life_cycle_ctb}'
        tb_source_sql = f'select * from {long_life_cycle_db}.{long_life_cycle_tb}'
        long_duration_time = int(''.join(list(filter(str.isdigit, long_duration))))
        short_duration_time = int(long_duration_time/2)
        short_duration = long_duration.replace(str(long_duration_time), str(short_duration_time))
        cal_duration_ts = self.tdCom.trans_time_to_s(short_duration) * self.offset

        self.prepare_data()
        self.tdCom.createDb(dbname=long_life_cycle_db, vgroups=self.vgroups, duration=long_duration, keep=long_duration)
        self.tdCom.createDb(dbname=short_life_cycle_db, vgroups=self.vgroups, duration=short_duration, keep=short_duration)
        self.tdCom.create_stable(dbname=long_life_cycle_db, stbname=long_life_cycle_stb)
        self.tdCom.create_ctable(dbname=long_life_cycle_db, stbname=long_life_cycle_stb, ctbname=long_life_cycle_ctb)
        self.tdCom.create_table(dbname=long_life_cycle_db, tbname=long_life_cycle_tb)
        self.tdCom.create_stable(dbname=short_life_cycle_db, stbname=short_life_cycle_stb)
        self.tdCom.create_ctable(dbname=short_life_cycle_db, stbname=short_life_cycle_stb, ctbname=short_life_cycle_ctb)
        self.tdCom.create_table(dbname=short_life_cycle_db, tbname=short_life_cycle_tb)
        self.tdCom.create_stream(stream_name=stb_stream_name, des_table=stb_stream_target_tbname, source_sql=stb_source_sql)
        self.tdCom.create_stream(stream_name=ctb_stream_name, des_table=ctb_stream_target_tbname, source_sql=ctb_source_sql)
        self.tdCom.create_stream(stream_name=tb_stream_name, des_table=tb_stream_target_tbname, source_sql=tb_source_sql)
        count = 1
        for i in range(1, self.range_count):
            self.tdCom.insert_rows(dbname=long_life_cycle_db, tbname=long_life_cycle_ctb, ts_value=self.date_time-cal_duration_ts+i*self.offset*self.offset, need_null=True)
            self.tdCom.insert_rows(dbname=long_life_cycle_db, tbname=long_life_cycle_tb, ts_value=self.date_time-cal_duration_ts+i*self.offset*self.offset, need_null=True)
            count += 1
        expected_res = count - 1
        self.tdCom.check_stream(f'select {self.stb_filter_des_select_elm} from {stb_stream_target_tbname};', f'select  {self.stb_filter_des_select_elm} from {long_life_cycle_db}.{long_life_cycle_stb};', expected_res)
        self.tdCom.check_stream(f'select {self.tb_filter_des_select_elm} from {ctb_stream_target_tbname};', f'select  {self.tb_filter_des_select_elm} from {long_life_cycle_db}.{long_life_cycle_ctb};', expected_res)
        self.tdCom.check_stream(f'select {self.tb_filter_des_select_elm} from {tb_stream_target_tbname};', f'select  {self.tb_filter_des_select_elm} from {long_life_cycle_db}.{long_life_cycle_tb};', expected_res)
        count = expected_res
        new_expected_res = count
        for i in range(self.range_count):
            self.tdCom.insert_rows(dbname=long_life_cycle_db, tbname=long_life_cycle_ctb, ts_value=self.date_time-cal_duration_ts-i*self.offset*self.offset, need_null=True)
            self.tdCom.insert_rows(dbname=long_life_cycle_db, tbname=long_life_cycle_tb, ts_value=self.date_time-cal_duration_ts-i*self.offset*self.offset, need_null=True)
            new_expected_res += 1
        # self.tdCom.check_stream(f'select {self.stb_filter_des_select_elm} from {stb_stream_target_tbname} limit {count};', f'select  {self.stb_filter_des_select_elm} from {long_life_cycle_db}.{long_life_cycle_stb} limit {count};', count)
        # self.tdCom.check_stream(f'select {self.tb_filter_des_select_elm} from {ctb_stream_target_tbname} limit {count};', f'select  {self.tb_filter_des_select_elm} from {long_life_cycle_db}.{long_life_cycle_ctb} limit {count};', count)
        # self.tdCom.check_stream(f'select {self.tb_filter_des_select_elm} from {tb_stream_target_tbname} limit {count};', f'select  {self.tb_filter_des_select_elm} from {long_life_cycle_db}.{long_life_cycle_tb} limit {count};', count)
        for tbname in [stb_stream_target_tbname, ctb_stream_target_tbname, tb_stream_target_tbname]:
            if tbname == stb_stream_target_tbname:
                select_elm = self.stb_filter_des_select_elm
            else:
                select_elm = self.tb_filter_des_select_elm
            self.tdSql.query(f'select {select_elm} from {tbname};')
            self.tdSql.checkEqual(self.tdSql.query_row, count)
        for tbname in [f'{long_life_cycle_db}.{long_life_cycle_stb}', f'{long_life_cycle_db}.{long_life_cycle_ctb}', f'{long_life_cycle_db}.{long_life_cycle_tb}']:
            if tbname == f'{long_life_cycle_db}.{long_life_cycle_stb}':
                select_elm = self.stb_filter_des_select_elm
            else:
                select_elm = self.tb_filter_des_select_elm
            self.tdSql.query(f'select {select_elm} from {tbname};')
            self.tdSql.checkEqual(self.tdSql.query_row, new_expected_res)

    def stream_tandem(self):
        self.case_name = sys._getframe().f_code.co_name
        source_tandem_db = "source_tandem_db"
        target_tandem_db = "target_tandem_db"

        source_tandem_stb = "source_tandem_stb"
        source_tandem_ctb = "source_tandem_ctb"
        source_tandem_tb = "source_tandem_tb"
        target_tandem_stb = "target_tandem_stb"
        target_tandem_ctb = "target_tandem_ctb"
        target_tandem_tb = "target_tandem_tb"

        source_stb_stream_name = "source_stb_tandem_stream"
        source_ctb_stream_name = "source_ctb_tandem_stream"
        source_tb_stream_name = "source_tb_tandem_stream"
        target_stb_stream_name = "target_stb_tandem_stream"
        target_ctb_stream_name = "target_ctb_tandem_stream"
        target_tb_stream_name = "target_tb_tandem_stream"

        source_stb_stream_target_tbname = f'{source_tandem_db}.output_tandem_stb'
        source_ctb_stream_target_tbname = f'{source_tandem_db}.output_tandem_ctb'
        source_tb_stream_target_tbname = f'{source_tandem_db}.output_tandem_tb'
        target_stb_stream_target_tbname = f'{target_tandem_db}.output_tandem_stb'
        target_ctb_stream_target_tbname = f'{target_tandem_db}.output_tandem_ctb'
        target_tb_stream_target_tbname = f'{target_tandem_db}.output_tandem_tb'


        source_stb_source_sql = f'select * from {source_tandem_db}.{source_tandem_stb}'
        source_ctb_source_sql = f'select * from {source_tandem_db}.{source_tandem_ctb}'
        source_tb_source_sql = f'select * from {source_tandem_db}.{source_tandem_tb}'
        target_stb_source_sql = f'select * from {source_stb_stream_target_tbname}'
        target_ctb_source_sql = f'select * from {source_ctb_stream_target_tbname}'
        target_tb_source_sql = f'select * from {source_tb_stream_target_tbname}'

        self.prepare_data()
        self.tdCom.createDb(dbname=source_tandem_db, vgroups=self.vgroups)
        self.tdCom.createDb(dbname=target_tandem_db, vgroups=self.vgroups)

        self.tdCom.create_stable(dbname=source_tandem_db, stbname=source_tandem_stb)
        self.tdCom.create_ctable(dbname=source_tandem_db, stbname=source_tandem_stb, ctbname=source_tandem_ctb)
        self.tdCom.create_table(dbname=source_tandem_db, tbname=source_tandem_tb)
        self.tdCom.create_stable(dbname=target_tandem_db, stbname=target_tandem_stb)
        self.tdCom.create_ctable(dbname=target_tandem_db, stbname=target_tandem_stb, ctbname=target_tandem_ctb)
        self.tdCom.create_table(dbname=target_tandem_db, tbname=target_tandem_tb)

        self.tdCom.create_stream(stream_name=source_stb_stream_name, des_table=source_stb_stream_target_tbname, source_sql=source_stb_source_sql)
        self.tdCom.create_stream(stream_name=source_ctb_stream_name, des_table=source_ctb_stream_target_tbname, source_sql=source_ctb_source_sql)
        self.tdCom.create_stream(stream_name=source_tb_stream_name, des_table=source_tb_stream_target_tbname, source_sql=source_tb_source_sql)
        self.tdCom.create_stream(stream_name=target_stb_stream_name, des_table=target_stb_stream_target_tbname, source_sql=target_stb_source_sql)
        self.tdCom.create_stream(stream_name=target_ctb_stream_name, des_table=target_ctb_stream_target_tbname, source_sql=target_ctb_source_sql)
        self.tdCom.create_stream(stream_name=target_tb_stream_name, des_table=target_tb_stream_target_tbname, source_sql=target_tb_source_sql)
        count = 0
        for i in range(self.range_count):
            self.tdCom.insert_rows(dbname=source_tandem_db, tbname=source_tandem_ctb, ts_value=self.date_time+i, need_null=True)
            self.tdCom.insert_rows(dbname=source_tandem_db, tbname=source_tandem_tb, ts_value=self.date_time+i, need_null=True)
            count += 1
        for tbname in [target_stb_stream_target_tbname, target_ctb_stream_target_tbname, target_tb_stream_target_tbname]:
            if tbname == target_stb_stream_target_tbname:
                select_elm = self.stb_filter_des_select_elm
                source_tb = source_stb_stream_target_tbname
            elif tbname == target_ctb_stream_target_tbname:
                select_elm = self.tb_filter_des_select_elm
                source_tb = source_ctb_stream_target_tbname
            else:
                select_elm = self.tb_filter_des_select_elm
                source_tb = source_tb_stream_target_tbname
            self.tdCom.check_stream(f'select {select_elm} from {tbname};', f'select {select_elm} from {source_tb};', count)

    def udf_interval_order(self, interval, udf_size=8):
        self.case_name = sys._getframe().f_code.co_name
        self.prepare_data(interval=interval)
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
        # self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select _wstart AS start, {self.stb_source_select_str}  from {self.stb_name} state_window({state_window_col_name})', trigger_mode="at_once")
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
                
        # for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
        for tbname in [self.ctb_name, self.tb_name]:
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
                    self.tdCom.delete_rows(tbname=self.tb_name, start_ts=dt)

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
                window_close_ts = self.cal_watermark_window_close_session_endts(self.date_time, session=self.dataDict['session'])
            else:
                self.date_time = window_close_ts + 1
                window_close_ts = self.cal_watermark_window_close_session_endts(self.date_time, session=self.dataDict['session'])
            for ts_value in [self.date_time, window_close_ts]:
                self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=ts_value)
                self.tdCom.insert_rows(tbname=self.tb_name, ts_value=ts_value)
                if self.update and i%2 == 0:
                    self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=ts_value)
                    self.tdCom.insert_rows(tbname=self.tb_name, ts_value=ts_value)
                if self.delete and i%2 != 0:
                    dt = f'cast({self.date_time-1} as timestamp)'
                    self.tdCom.delete_rows(tbname=self.ctb_name, start_ts=dt)
                    self.tdCom.delete_rows(tbname=self.tb_name, start_ts=dt)
            for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
                if tbname != self.tb_name:
                    self.tdCom.check_stream(f'select start, {self.stb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS start, {self.stb_source_select_str}  from {tbname} session(ts, {self.dataDict["session"]}s) limit {i+1}', i+1)
                else:
                    self.tdCom.check_stream(f'select start, {self.tb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS start, {self.tb_source_select_str}  from {tbname} session(ts, {self.dataDict["session"]}s) limit {i+1}', i+1)

    def window_close_state_window(self, state_window):
        self.case_name = sys._getframe().f_code.co_name
        self.prepare_data(state_window=state_window)
        state_window_col_name = self.dataDict["state_window"]
        self.tdCom.write_latency(self.case_name)
        # self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select _wstart AS start, {self.stb_source_select_str}  from {self.stb_name} state_window({state_window_col_name})', trigger_mode="window_close")
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstart AS start, {self.stb_source_select_str}  from {self.ctb_name} state_window({state_window_col_name})', trigger_mode="window_close")
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstart AS start, {self.tb_source_select_str}  from {self.tb_name} state_window({state_window_col_name})', trigger_mode="window_close")
        state_window_max = self.dataDict['state_window_max']
        for i in range(self.range_count):
            state_window_value = random.randint(int((i)*state_window_max/self.range_count), int((i+1)*state_window_max/self.range_count))
            for j in range(2, self.range_count+3):
                self.tdSql.execute(f'insert into {self.ctb_name} (ts, {state_window_col_name}) values ({self.date_time}, {state_window_value})')
                self.tdSql.execute(f'insert into {self.tb_name} (ts, {state_window_col_name}) values ({self.date_time}, {state_window_value})')
                if self.update and i%2 == 0:
                    self.tdSql.execute(f'insert into {self.ctb_name} (ts, {state_window_col_name}) values ({self.date_time}, {state_window_value})')
                    self.tdSql.execute(f'insert into {self.tb_name} (ts, {state_window_col_name}) values ({self.date_time}, {state_window_value})')
                if self.delete and i%2 != 0:
                    dt = f'cast({self.date_time-1} as timestamp)'
                    self.tdCom.delete_rows(tbname=self.ctb_name, start_ts=dt)
                    self.tdCom.delete_rows(tbname=self.tb_name, start_ts=dt)
                self.date_time += 1
            # for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
            for tbname in [self.ctb_name, self.tb_name]:
                if tbname != self.tb_name:
                    self.tdCom.check_stream(f'select start, {self.stb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS start, {self.stb_source_select_str}  from {tbname} state_window({state_window_col_name}) limit {i}', i)
                else:
                    self.tdCom.check_stream(f'select start, {self.tb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS start, {self.tb_source_select_str}  from {tbname} state_window({state_window_col_name}) limit {i}', i)

    def watermark_max_delay_interval(self, interval, max_delay, watermark=None):
        self.case_name = sys._getframe().f_code.co_name
        if watermark is not None:
            self.case_name = "watermark" + sys._getframe().f_code.co_name
        self.prepare_data(interval=interval, watermark=watermark)
        self.tdCom.write_latency(self.case_name)
        self.date_time = 1658921623245
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
                self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=self.date_time+num*self.offset)
                self.tdCom.insert_rows(tbname=self.tb_name, ts_value=self.date_time+num*self.offset)
                if self.update and i%2 == 0:
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
            if self.update and i%2 == 0:
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
            if self.update and i%2 == 0:
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

    def watermark_window_close_session(self, session, watermark):
        self.case_name = sys._getframe().f_code.co_name
        if watermark is not None:
            self.case_name = "watermark" + sys._getframe().f_code.co_name
        self.prepare_data(session=session, watermark=watermark)
        self.date_time = self.dataDict["start_ts"]
        self.tdCom.write_latency(self.case_name)
        if watermark is not None:
            watermark_value = f'{self.dataDict["watermark"]}s'
        else:
            watermark_value = None
        # create stb/ctb/tb stream
        # self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select _wstart AS start, {self.stb_source_select_str}  from {self.stb_name} session(ts, {self.dataDict["session"]}s)', trigger_mode="window_close", watermark=watermark_value)
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstart AS start, {self.stb_source_select_str}  from {self.ctb_name} session(ts, {self.dataDict["session"]}s)', trigger_mode="window_close", watermark=watermark_value)
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstart AS start, {self.tb_source_select_str}  from {self.tb_name} session(ts, {self.dataDict["session"]}s)', trigger_mode="window_close", watermark=watermark_value)
        for i in range(self.range_count):
            if i == 0:
                window_close_ts = self.cal_watermark_window_close_session_endts(self.date_time, self.dataDict['watermark'], self.dataDict['session'])
            else:
                self.date_time = window_close_ts + 1
                window_close_ts = self.cal_watermark_window_close_session_endts(self.date_time, self.dataDict['watermark'], self.dataDict['session'])
            if watermark_value is not None:
                expected_value = i + 1
                for ts_value in [self.date_time, window_close_ts-1]:
                    self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=ts_value)
                    self.tdCom.insert_rows(tbname=self.tb_name, ts_value=ts_value)
                    if self.update and i%2 == 0:
                        self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=ts_value)
                        self.tdCom.insert_rows(tbname=self.tb_name, ts_value=ts_value)
                    # for tbname in [self.stb_stream_des_table, self.ctb_stream_des_table, self.tb_stream_des_table]:
                    for tbname in [self.ctb_stream_des_table, self.tb_stream_des_table]:
                        if tbname != self.tb_stream_des_table:
                            self.tdSql.query(f'select start, {self.stb_output_select_str} from {tbname}')
                        else:
                            self.tdSql.query(f'select start, {self.tb_output_select_str} from {tbname}')
                        self.tdSql.checkEqual(self.tdSql.query_row, i)
            else:
                expected_value = i
            self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=window_close_ts)
            self.tdCom.insert_rows(tbname=self.tb_name, ts_value=window_close_ts)
            if self.update and i%2 == 0:
                self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=window_close_ts)
                self.tdCom.insert_rows(tbname=self.tb_name, ts_value=window_close_ts)
            # for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
            for tbname in [self.ctb_name, self.tb_name]:
                if tbname != self.tb_name:
                    self.tdCom.check_stream(f'select start, {self.stb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS start, {self.stb_source_select_str}  from {tbname} session(ts, {self.dataDict["session"]}s) limit {expected_value}', expected_value)
                else:
                    self.tdCom.check_stream(f'select start, {self.tb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS start, {self.tb_source_select_str}  from {tbname} session(ts, {self.dataDict["session"]}s) limit {expected_value}', expected_value)

    def watermark_max_delay_session(self, session, watermark, max_delay):
        self.case_name = sys._getframe().f_code.co_name
        if watermark is not None:
            self.case_name = "watermark" + sys._getframe().f_code.co_name
        self.prepare_data(session=session, watermark=watermark)
        self.date_time = self.dataDict["start_ts"]

        self.tdCom.write_latency(self.case_name)
        if watermark is not None:
            watermark_value = f'{self.dataDict["watermark"]}s'
        else:
            watermark_value = None
        max_delay_value = f'{self.tdCom.trans_time_to_s(max_delay)}s'
        # create stb/ctb/tb stream
        # self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select _wstart AS start, {self.stb_source_select_str}  from {self.stb_name} session(ts, {self.dataDict["session"]}s)', trigger_mode="max_delay", watermark=watermark_value, max_delay=max_delay_value)
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstart AS start, {self.stb_source_select_str}  from {self.ctb_name} session(ts, {self.dataDict["session"]}s)', trigger_mode="max_delay", watermark=watermark_value, max_delay=max_delay_value)
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstart AS start, {self.tb_source_select_str}  from {self.tb_name} session(ts, {self.dataDict["session"]}s)', trigger_mode="max_delay", watermark=watermark_value, max_delay=max_delay_value)
        init_num = 0
        for i in range(self.range_count):
            if i == 0:
                window_close_ts = self.cal_watermark_window_close_session_endts(self.date_time, self.dataDict['watermark'], self.dataDict['session'])
            else:
                self.date_time = window_close_ts + 1
                window_close_ts = self.cal_watermark_window_close_session_endts(self.date_time, self.dataDict['watermark'], self.dataDict['session'])

            if watermark_value is not None:
                for ts_value in [self.date_time, window_close_ts-1]:
                    self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=ts_value)
                    self.tdCom.insert_rows(tbname=self.tb_name, ts_value=ts_value)
                    if self.update and i%2 == 0:
                        self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=ts_value)
                        self.tdCom.insert_rows(tbname=self.tb_name, ts_value=ts_value)
                    # for tbname in [self.stb_stream_des_table, self.ctb_stream_des_table, self.tb_stream_des_table]:
                    for tbname in [self.ctb_stream_des_table, self.tb_stream_des_table]:
                        if tbname != self.tb_stream_des_table:
                            self.tdSql.query(f'select start, {self.stb_output_select_str} from {tbname}')
                        else:
                            self.tdSql.query(f'select start, {self.tb_output_select_str} from {tbname}')
                        self.tdSql.checkEqual(self.tdSql.query_row, init_num)

            self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=window_close_ts)
            self.tdCom.insert_rows(tbname=self.tb_name, ts_value=window_close_ts)
            if self.update and i%2 == 0:
                self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=window_close_ts)
                self.tdCom.insert_rows(tbname=self.tb_name, ts_value=window_close_ts)
            # for tbname in [stb_stream_des_table, ctb_stream_des_table, tb_stream_des_table]:
            if i == 0:
                init_num = 2 + i
            else:
                init_num += 1
            if watermark_value is not None:
                expected_value = init_num
            else:
                expected_value = i + 1
            #for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
            for tbname in [self.ctb_name, self.tb_name]:
                if tbname != self.tb_name:
                    self.tdCom.check_stream(f'select start, {self.stb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS start, {self.stb_source_select_str}  from {tbname} session(ts, {self.dataDict["session"]}s)', expected_value, max_delay)
                else:
                    self.tdCom.check_stream(f'select start, {self.tb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS start, {self.tb_source_select_str}  from {tbname} session(ts, {self.dataDict["session"]}s)', expected_value, max_delay)



    # TODO split --------- before is new


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


    def partitionby_interval(self, interval=None, partition_by_elm="tbname"):
        self.case_name = sys._getframe().f_code.co_name
        self.prepare_data(interval=interval)
        self.tdCom.write_latency(self.case_name)
        ctb_name_list = list()
        for i in range(1, self.range_count):
            ctb_name = self.tdCom.get_long_name()
            ctb_name_list.append(ctb_name)
            self.tdCom.create_ctable(stbname=self.stb_name, ctbname=ctb_name)
        if interval is not None:
            source_sql = f'select _wstart AS start, {self.partition_by_stb_source_select_str}  from {self.stb_name} partition by {partition_by_elm} interval({self.dataDict["interval"]}s)'
        else:
            source_sql = f'select {self.stb_filter_des_select_elm} from {self.stb_name} partition by {partition_by_elm}'

        # create stb/ctb/tb stream
        self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=source_sql)
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
                if interval is not None:
                    self.tdCom.check_query_data(f'select `{colname}` from {self.stb_name}{self.des_table_suffix} order by `{colname}`;', f'select {colname}  from {self.stb_name} partition by {partition_by_elm} interval({self.dataDict["interval"]}s) order by `{colname}`;')
                else:
                    self.tdCom.check_query_data(f'select {self.stb_filter_des_select_elm} from {self.stb_name}{self.des_table_suffix} order by c1,c2,c3;', f'select {self.stb_filter_des_select_elm}  from {self.stb_name} partition by {partition_by_elm} order by c1,c2,c3;')

    def max_delay_state_window_order(self, state_window, interation, vgroups=1):
        self.case_name = sys._getframe().f_code.co_name
        # TODO




    


    

    

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

    def create_none_db_stream(self):
        self.case_name = sys._getframe().f_code.co_name
        self.prepare_data()
        self.tdCom.write_latency(self.case_name)
        stream_name = self.tdCom.get_long_name()
        dbname2 = self.tdCom.get_long_name()
        self.tdSql.error(f'create stream if not exists {stream_name} into {dbname2}.stb as select * from {self.dbname}.{self.case_name}_stb')
    
    def create_none_source_tb_stream(self):
        self.case_name = sys._getframe().f_code.co_name
        self.prepare_data()
        self.tdCom.write_latency(self.case_name)
        stream_name = self.tdCom.get_long_name()
        dbname2 = self.tdCom.get_long_name()
        self.tdCom.createDb(dbname2)
        for tbname in ["stb", "ct1", "tb1"]:
            self.tdSql.execute(f'drop table if exists {self.dbname}.{self.case_name}_{tbname}')
            self.tdSql.error(f'create stream if not exists {stream_name}_{tbname} into {dbname2}.{tbname} as select * from {self.dbname}.{self.case_name}_stb')
    
    def create_none_source_tb_tag_stream(self):
        self.case_name = sys._getframe().f_code.co_name
        self.prepare_data()
        self.tdCom.write_latency(self.case_name)
        stream_name = self.tdCom.get_long_name()
        dbname2 = self.tdCom.get_long_name()
        self.tdCom.createDb(dbname2)
        for tbname in ["stb"]:
            self.tdSql.error(f'create stream if not exists {stream_name} into {dbname2}.{tbname} as select ts,t100 from {self.dbname}.{self.case_name}_{tbname}')
    
    def create_none_source_tb_col_stream(self):
        self.case_name = sys._getframe().f_code.co_name
        self.prepare_data()
        self.tdCom.write_latency(self.case_name)
        stream_name = self.tdCom.get_long_name()
        dbname2 = self.tdCom.get_long_name()
        self.tdCom.createDb(dbname2)
        for tbname in ["ct1", "tb1"]:
            self.tdSql.error(f'create stream if not exists {stream_name} into {dbname2}.{tbname} as select ts,c100 from {self.dbname}.{self.case_name}_{tbname}')
    
    def create_error_source_sql_stream(self):
        self.case_name = sys._getframe().f_code.co_name
        self.prepare_data()
        self.tdCom.write_latency(self.case_name)
        stream_name = self.tdCom.get_long_name()
        dbname2 = self.tdCom.get_long_name()
        self.tdCom.createDb(dbname2)
        error_sql_list = [f'select ts,c10%,^ from {self.dbname}.{self.case_name}_stb',
                        f'select ts,c10 from {self.dbname}*.{self.case_name}_stb',
                        f'select ts,t10 from {self.dbname}.{self.case_name}_tb1',
                        f'select c10 from {self.dbname}.{self.case_name}_tb1',
                        f'select c9,c10 from {self.dbname}.{self.case_name}_tb1'
                        ]
        for error_sql in error_sql_list:
            self.tdSql.error(f'create stream if not exists {stream_name} into {dbname2}.target_tb as {error_sql}')
    

    def insert_after_restart(self):
        self.data_filter()
        self.taosd.update_cfg('/tmp', self.taosd_setting, {"supportVnodes": self.cfg["boundary"][-1]}, self.endpoint, True)
         # insert data
        count = self.range_count
        step_count = self.range_count
        for i in range(self.range_count, self.range_count*2):
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


    def insert_after_recreate_source_table(self):
        count = self.data_filter(True)
        new_count = deepcopy(count)
        for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
            self.tdSql.execute(f'drop table if exists {tbname}')
        self.tdCom.create_stable(dbname=self.dbname, stbname=self.stb_name)
        self.tdCom.create_ctable(dbname=self.dbname, stbname=self.stb_name, ctbname=self.ctb_name)
        self.tdCom.create_table(dbname=self.dbname, tbname=self.tb_name)
        for i in range(new_count, self.range_count+new_count):
            self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=f'{self.date_time}+{i}s')
            self.tdCom.insert_rows(tbname=self.tb_name, ts_value=f'{self.date_time}+{i}s')
            if self.update:
                self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=f'{self.date_time}+{i}s')
                self.tdCom.insert_rows(tbname=self.tb_name, ts_value=f'{self.date_time}+{i}s')
            new_count += 1
            # check result
        self.tdCom.check_stream(f'select {self.stb_filter_des_select_elm} from {self.stb_stream_des_table} limit {self.range_count}  offset {new_count-1-count};', f'select {self.filter_source_select_elm} from {self.stb_name} where {self.stb_data_filter_sql};', self.range_count)
        self.tdCom.check_stream(f'select {self.tb_filter_des_select_elm} from {self.ctb_stream_des_table} limit {self.range_count} offset {new_count-1-count};', f'select {self.filter_source_select_elm} from {self.ctb_name} where {self.stb_data_filter_sql};', self.range_count)
        self.tdCom.check_stream(f'select {self.tb_filter_des_select_elm} from {self.tb_stream_des_table} limit {self.range_count} offset {new_count-1-count};', f'select {self.filter_source_select_elm} from {self.tb_name} where {self.tb_data_filter_sql};', self.range_count)

    # TODO refactor
    def query_after_drop_stream_db(self):
        self.case_name = sys._getframe().f_code.co_name
        self.stream_tandem()
        self.tdSql.error(f'drop database if exists target_tandem_db;')
        # self.prepare_data()
        # self.tdCom.write_latency(self.case_name)
        # stream_name = self.tdCom.get_long_name()
        # dbname2 = self.tdCom.get_long_name()
        # self.tdCom.createDb(dbname2)
        # for tbname in ["ct1", "tb1"]:
        #     self.tdSql.error(f'create stream if not exists {stream_name} into {dbname2}.{tbname} as select ts,c100 from {self.dbname}.{self.case_name}_{tbname}')
    

    def run(self):
        # self.create_none_db_stream()
        # self.create_none_source_tb_stream()
        # self.create_none_source_tb_tag_stream()
        # # TODO TD-18111
        # self.create_none_source_tb_col_stream()
        # self.create_error_source_sql_stream()
        # ! TD-18120
        # self.insert_after_restart()
        # ! TD-18123
        # self.insert_after_recreate_source_table()
        # self.query_after_drop_stream_db()


        # self.range_count = 10
        # self.update = True
        self.vgroups = 10
        # # interval=random.randint(10, 15)
        # interval = 15
        # watermark = 17
        # # watermark=random.randint(20, 30)
        # # print(interval, watermark)
        # self.data_filter()
        # self.life_cycle()
        # self.scalar_function()
        # self.stream_tandem()
        # self.udf_interval_order(interval=10)
        # self.at_once_interval(interval=random.randint(10, 15))
        # # self.alter_source_table(interval=random.randint(10, 15))
        # self.at_once_state_window(state_window="c1")
        # self.at_once_session(session=random.randint(10, 15))
        # self.window_close_interval(interval=random.randint(10, 15), watermark=None)
        # ! TD-18176
        self.window_close_interval(interval=random.randint(10, 15), watermark=random.randint(15, 20))
        # self.window_close_interval(interval=interval, watermark=watermark)
        # self.window_close_state_window(state_window="c1")
        # self.watermark_max_delay_interval(interval=random.randint(10, 15), watermark=None, max_delay=f"{random.randint(1, 3)}s")
        # TODO vgroups = 10
        # self.watermark_max_delay_interval(interval=10, watermark=15, max_delay=f"{random.randint(1, 3)}s")
        # # # ! case bug not stable
        # # self.watermark_max_delay_interval(interval=random.randint(10, 15), watermark=random.randint(20, 30), max_delay=f"{random.randint(1, 3)}s")

        # self.watermark_window_close_session(session=random.randint(10, 15), watermark=None)
        # self.watermark_window_close_session(session=random.randint(10, 15), watermark=random.randint(20, 30))
        # ! TD-18180 超级表时不crash了但结果不太对
        # self.watermark_max_delay_session(session=random.randint(10, 15), watermark=None, max_delay=f"{random.randint(1, 3)}s")
        # self.watermark_max_delay_session(session=random.randint(10, 15), watermark=random.randint(20, 30), max_delay=f"{random.randint(1, 3)}s")
        # self.partitionby_interval(interval=None, partition_by_elm="tbname")
        # self.partitionby_interval(interval=10, partition_by_elm="tbname")
        # self.partitionby_interval(interval=None, partition_by_elm="t1")
        # ! TD-18216
        # self.partitionby_interval(interval=10, partition_by_elm="t1")

        # TODO to be supported
        # ! TD-18165
        # self.partitionby_interval(interval=None, partition_by_elm="c1")
        # self.partitionby_interval(interval=10, partition_by_elm="c1")
        # self.partitionby_interval(interval=10, partition_by_elm="abs(t1)")
        # self.partitionby_interval(interval=None, partition_by_elm="abs(t1)")
        # self.partitionby_interval(interval=10, partition_by_elm="abs(c1)")
        # self.partitionby_interval(interval=None, partition_by_elm="abs(c1)")


        
        
        # # self.disorder_data()
        # TODO unfinished
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

