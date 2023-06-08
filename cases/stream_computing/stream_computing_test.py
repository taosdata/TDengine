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
import sys
import os
import random
from taostest.components import TaosD
import time
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

        self.case_name = str()
        self.dbname = "stream_test"
        self.stb_name = str()
        self.ctb_name = str()
        self.tb_name = str()
        self.stb_stream_des_table = str()
        self.ctb_stream_des_table = str()
        self.tb_stream_des_table = str()
        self.record_history_ts = str()

        self.udf1 = "/tmp/libudf1.so"
        self.udf2 = "/tmp/libudf2.so"
        self.offset = 1000
        self.interation = 10
        self.default_interval = 5

        self.range_count = 5
        self.vgroups = 10
        self.vgroups_list = [1, self.vgroups]
        self.des_table_suffix = "_output"
        self.stream_suffix = "_stream"

        self.update = True
        self.disorder = True
        if self.disorder:
            self.update = False

        self.delete = True

        self.subtable = True

        self.subtable_prefix = "prefix_" if self.subtable else ""
        self.subtable_suffix = "_suffix" if self.subtable else ""

        self.stream_case_when_tbname = "tbname"
        self.stream_case_when_column = "c1"
        self.partition_tbname_alias = "ptn_alias" if self.subtable else ""
        self.partition_tag_alias = "ptag_alias" if self.subtable else ""
        self.partition_col_alias = "pcol_alias" if self.subtable else ""
        self.partition_expression_alias = "pexp_alias" if self.subtable else ""

        # ! apercentile(c6, 50) "avg(c7)" "timetruncate(_wstart, 1m)" "timediff(1, 0, 1h)" TD-16878 TD-16877 TD-16876 TD-16869
        self.partition_by_downsampling_function_list = ["min(c1)", "max(c2)", "sum(c3)", "first(c4)", "last(c5)", "count(c8)", "spread(c1)", 
        "stddev(c2)", "hyperloglog(c11)", "min(t1)", "max(t2)", "sum(t3)", "first(t4)", "last(t5)", "count(t8)", "spread(t1)", "stddev(t2)"]
        # self.partition_by_downsampling_function_list = ["min(c1)", "max(c2)", "sum(c3)", "first(c4)", "last(c5)", "count(c8)", "spread(c1)",
        # "stddev(c2)", "hyperloglog(c11)", "min(t1)", "max(t2)", "sum(t3)", "first(t4)", "last(t5)", "count(t8)", "spread(t1)", "stddev(t2)", "hyperloglog(t11)"]
        # ! now() timezone() to_iso8601(now)
        self.downsampling_function_list = ["min(c1)", "max(c2)", "sum(c3)", "first(c4)", "last(c5)", "apercentile(c6, 50)", "avg(c7)", "count(c8)", "spread(c1)", 
        "stddev(c2)", "hyperloglog(c11)", "timediff(1, 0, 1h)", "timezone()", "to_iso8601(1)", 'to_unixtimestamp("1970-01-01T08:00:00+08:00")', "min(t1)", "max(t2)", "sum(t3)",
        "first(t4)", "last(t5)", "apercentile(t6, 50)", "avg(t7)", "count(t8)", "spread(t1)", "stddev(t2)", "hyperloglog(t11)"]
        self.fill_function_list = ["min(c1)", "max(c2)", "sum(c3)", "apercentile(c6, 50)", "avg(c7)", "count(c8)", "spread(c1)", 
        "stddev(c2)", "hyperloglog(c11)", "timediff(1, 0, 1h)", "timezone()", "to_iso8601(1)", 'to_unixtimestamp("1970-01-01T08:00:00+08:00")', "min(t1)", "max(t2)", "sum(t3)",
        "first(t4)", "last(t5)", "apercentile(t6, 50)", "avg(t7)", "count(t8)", "spread(t1)", "stddev(t2)", "hyperloglog(t11)"]
        self.udf_function_list = ["min(udf1(c1))", "max(udf1(c2))", "sum(udf1(c3))", "first(udf1(c4))", "last(udf1(c5))", "apercentile(udf1(c6), 50)", "avg(udf1(c7))", "count(udf1(c8))", "spread(udf1(c1))", 
        "stddev(udf1(c2))", "hyperloglog(udf1(c11))", "timediff(1, 0, 1h)", "to_iso8601(1)", 'to_unixtimestamp("1970-01-01T08:00:00+08:00")', "min(udf1(t1))", "max(udf1(t2))", "sum(udf1(t3))",
        "first(udf1(t4))", "last(udf1(t5))", "apercentile(udf1(t6), 50)", "avg(udf1(t7))", "count(udf1(t8))", "spread(udf1(t1))", "stddev(udf1(t2))", "hyperloglog(udf1(t11))"]
        self.udf_function_list = ["min(udf1(c1))", "max(udf1(c2))", "sum(udf1(c3))", "apercentile(udf1(c6), 50)", "avg(udf1(c7))", "count(udf1(c8))", "spread(udf1(c1))",
        "stddev(udf1(c2))", "hyperloglog(udf1(c11))", "timediff(1, 0, 1h)", "to_iso8601(1)", 'to_unixtimestamp("1970-01-01T08:00:00+08:00")']
        # self.downsampling_function_list = ["min(c1)", "max(c2)", "sum(c1)", "first(c1)", "last(c1)", "apercentile(c1, 50)", "last_row(c1)", "avg(c1)", "count(c1)", "spread(c1)", "stddev(c2)", "hyperloglog(c3)", 
        #     'histogram(c1, "user_input", "[1, 3, 5, 7]", 0)', "now()", "timediff(1, 0, 1h)", "timetruncate(_wstart, 1m)", "timezone()", "today()", "to_iso8601(now)",  'to_unixtimestamp("1970-01-01T08:00:00+08:00")']
        self.stb_output_select_str = ','.join(list(map(lambda x:f'`{x}`', self.downsampling_function_list)))
        self.stb_source_select_str = ','.join(self.downsampling_function_list)
        self.tb_output_select_str = ','.join(list(map(lambda x:f'`{x}`', self.downsampling_function_list[0:15])))
        self.tb_source_select_str = ','.join(self.downsampling_function_list[0:15])
        self.ext_tb_source_select_str = ','.join(self.downsampling_function_list[0:13])

        self.fill_stb_output_select_str = ','.join(list(map(lambda x:f'`{x}`', self.fill_function_list)))
        self.fill_stb_source_select_str = ','.join(self.fill_function_list)
        self.fill_tb_output_select_str = ','.join(list(map(lambda x:f'`{x}`', self.fill_function_list[0:13])))
        self.fill_tb_source_select_str = ','.join(self.fill_function_list[0:13])

        self.partition_by_stb_output_select_str = ','.join(list(map(lambda x:f'`{x}`', self.partition_by_downsampling_function_list)))
        self.partition_by_stb_source_select_str = ','.join(self.partition_by_downsampling_function_list)

        self.udf_stb_output_select_str = ','.join(list(map(lambda x:f'`{x}`', self.udf_function_list)))
        self.udf_stb_source_select_str = ','.join(self.udf_function_list)
        self.udf_tb_output_select_str = ','.join(list(map(lambda x:f'`{x}`', self.udf_function_list[0:15])))
        self.udf_tb_source_select_str = ','.join(self.udf_function_list[0:15])

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
        self.partitial_stb_filter_des_select_elm = ",".join(self.stb_filter_des_select_elm.split(",")[:3])
        self.exchange_stb_filter_des_select_elm = ",".join([self.stb_filter_des_select_elm.split(",")[0], self.stb_filter_des_select_elm.split(",")[2], self.stb_filter_des_select_elm.split(",")[1]])
        self.partitial_ext_tb_source_select_str = ','.join(self.downsampling_function_list[0:2])
        self.tb_filter_des_select_elm = self.stb_filter_des_select_elm.partition(", t1")[0]
        self.tag_filter_des_select_elm = self.stb_filter_des_select_elm.partition("c13, ")[2]
        self.exchange_tag_filter_des_select_elm = ",".join([self.stb_filter_des_select_elm.partition("c13, ")[2].split(",")[0], self.stb_filter_des_select_elm.partition("c13, ")[2].split(",")[2], self.stb_filter_des_select_elm.partition("c13, ")[2].split(",")[1]])
        self.partitial_tag_filter_des_select_elm = ",".join(self.stb_filter_des_select_elm.partition("c13, ")[2].split(",")[:3])
        self.partitial_tag_stb_filter_des_select_elm = "ts, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, t1, t3, t2, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13"
        self.cast_tag_filter_des_select_elm = "t5,t11,t13"
        self.cast_tag_stb_filter_des_select_elm = "ts, t1, t2, t3, t4, cast(t1 as TINYINT UNSIGNED), t6, t7, t8, t9, t10, cast(t2 as varchar(256)), t12, cast(t3 as bool)"
        self.tag_count = len(self.tag_filter_des_select_elm.split(","))

        self.state_window_range = list()

    def update_delete_history_data(self):
        self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=self.record_history_ts)
        self.tdCom.insert_rows(tbname=self.tb_name, ts_value=self.record_history_ts)
        if self.delete:
            self.tdCom.delete_rows(tbname=self.ctb_name, start_ts=self.tdCom.time_cast(self.record_history_ts, "-"))
            self.tdCom.delete_rows(tbname=self.tb_name, start_ts=self.tdCom.time_cast(self.record_history_ts, "-"))


    def build_udf_so(self):
        self._remote.cmd(self._fqdn, [f'gcc -fPIC -shared -o {self.udf1} {self.stream_case_env_root}/udf1.c', f'gcc -fPIC -shared -o {self.udf2} {self.stream_case_env_root}/udf2.c'])
        self._remote.cmd("127.0.0.1", [f'gcc -fPIC -shared -o {self.udf1} {self.stream_case_env_root}/udf1.c', f'gcc -fPIC -shared -o {self.udf2} {self.stream_case_env_root}/udf2.c'])

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
    #     self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.stb_source_select_str}  from {self.stb_name} interval({self.dataDict["interval"]}s)', trigger_mode="at_once")
    #     self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.stb_source_select_str}  from {self.ctb_name} interval({self.dataDict["interval"]}s)', trigger_mode="at_once")
    #     self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.tb_source_select_str}  from {self.tb_name} interval({self.dataDict["interval"]}s)', trigger_mode="at_once")
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
    #             self.tdCom.check_query_data(f'select wstart, {self.stb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS wstart, {self.stb_source_select_str}  from {tbname} interval({self.dataDict["interval"]}s)')
    #         else:
    #             self.tdCom.check_query_data(f'select wstart, {self.tb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS wstart, {self.tb_source_select_str}  from {tbname} interval({self.dataDict["interval"]}s)')
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
            return start_ts + session*self.offset + 1

    def clean_env(self):
        self.tdCom.drop_all_streams()
        self.tdCom.drop_all_db()

    def prepare_data(self, interval=None, watermark=None, session=None, state_window=None, state_window_max=127, interation=3, range_count=None, precision="ms", fill_history_value=0):
        self.clean_env()
        self.dataDict = {
            "stb_name" : f"{self.case_name}_stb",
            "ctb_name" : f"{self.case_name}_ct1",
            "tb_name" : f"{self.case_name}_tb1",
            "ext_stb_name" : f"ext_{self.case_name}_stb",
            "ext_ctb_name" : f"ext_{self.case_name}_ct1",
            "ext_tb_name" : f"ext_{self.case_name}_tb1",
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
        self.ext_stb_name = self.dataDict["ext_stb_name"]
        self.ext_ctb_name = self.dataDict["ext_ctb_name"]
        self.ext_tb_name = self.dataDict["ext_tb_name"]
        self.stb_stream_des_table = f'{self.stb_name}{self.des_table_suffix}'
        self.ctb_stream_des_table = f'{self.ctb_name}{self.des_table_suffix}'
        self.tb_stream_des_table = f'{self.tb_name}{self.des_table_suffix}'
        self.ext_stb_stream_des_table = f'{self.ext_stb_name}{self.des_table_suffix}'
        self.ext_ctb_stream_des_table = f'{self.ext_ctb_name}{self.des_table_suffix}'
        self.ext_tb_stream_des_table = f'{self.ext_tb_name}{self.des_table_suffix}'
        self.date_time = self.tdCom.genTs(precision=self.precision)[0]
        self.tdCom.stream_latency_log = self.run_log_dir + "/latency.log"

        self.tdCom.createDb(dbname=self.dbname, vgroups=self.vgroups, precision=self.precision)
        self.tdCom.create_stable(dbname=self.dbname, stbname=self.stb_name)
        self.tdCom.create_ctable(dbname=self.dbname, stbname=self.stb_name, ctbname=self.ctb_name)
        self.tdCom.create_table(dbname=self.dbname, tbname=self.tb_name)
        self.tdCom.create_stable(dbname=self.dbname, stbname=self.ext_stb_stream_des_table)
        self.tdCom.create_ctable(dbname=self.dbname, stbname=self.ext_stb_stream_des_table, ctbname=self.ext_ctb_stream_des_table)
        self.tdCom.create_table(dbname=self.dbname, tbname=self.ext_tb_stream_des_table)
        if fill_history_value == 1:
            for i in range(self.range_count):
                ts_value = str(self.date_time)+f'-{self.default_interval*(i+1)}s'
                self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=ts_value)
                self.tdCom.insert_rows(tbname=self.tb_name, ts_value=ts_value)
                if i == 1:
                    self.record_history_ts = ts_value

    def data_filter(self, need_return=False, delete=False, fill_history_value=None):
        self.delete = delete
        subtable_value = f'concat(concat("{self.subtable_prefix}", {self.partition_tbname_alias}), "{self.subtable_suffix}")' if self.subtable else None

        self.case_name = sys._getframe().f_code.co_name

        self.prepare_data(fill_history_value=fill_history_value)
        self.tdCom.write_latency(self.case_name)

        # create stb/ctb/tb stream
        self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select {self.filter_source_select_elm} from {self.stb_name} where {self.stb_data_filter_sql} partition by tbname {self.partition_tbname_alias}', trigger_mode="at_once", subtable_value=subtable_value, fill_history_value=fill_history_value)
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select {self.filter_source_select_elm} from {self.ctb_name} where {self.stb_data_filter_sql}', trigger_mode="at_once", fill_history_value=fill_history_value)
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select {self.filter_source_select_elm} from {self.tb_name} where {self.tb_data_filter_sql}', trigger_mode="at_once", fill_history_value=fill_history_value)

        # insert data
        count = 1
        step_count = 1
        for i in range(self.range_count):
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
                for k in range(2):
                    self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=ts_value)
                    self.tdCom.insert_rows(tbname=self.tb_name, ts_value=ts_value)
                    if self.delete:
                        self.tdCom.delete_rows(tbname=self.ctb_name, start_ts=ts_cast_delete_value)
                        self.tdCom.delete_rows(tbname=self.tb_name, start_ts=ts_cast_delete_value)
                count += 1
            # check result
            # self.tdCom.check_stream(f'select {self.stb_filter_des_select_elm} from {self.stb_stream_des_table};', f'select {self.filter_source_select_elm} from {self.stb_name} where {self.stb_data_filter_sql} partition by tbname;', count-1)
            # self.tdCom.check_stream(f'select {self.tb_filter_des_select_elm} from {self.ctb_stream_des_table};', f'select {self.filter_source_select_elm} from {self.ctb_name} where {self.stb_data_filter_sql};', count-1)
            # self.tdCom.check_stream(f'select {self.tb_filter_des_select_elm} from {self.tb_stream_des_table};', f'select {self.filter_source_select_elm} from {self.tb_name} where {self.tb_data_filter_sql};', count-1)
            self.tdCom.check_query_data(f'select {self.stb_filter_des_select_elm} from {self.stb_stream_des_table};', f'select {self.filter_source_select_elm} from {self.stb_name} where {self.stb_data_filter_sql} partition by tbname;')
            self.tdCom.check_query_data(f'select {self.tb_filter_des_select_elm} from {self.ctb_stream_des_table};', f'select {self.filter_source_select_elm} from {self.ctb_name} where {self.stb_data_filter_sql};')
            self.tdCom.check_query_data(f'select {self.tb_filter_des_select_elm} from {self.tb_stream_des_table};', f'select {self.filter_source_select_elm} from {self.tb_name} where {self.tb_data_filter_sql};')

        if fill_history_value:
            self.update_delete_history_data()

        if self.subtable:
            self.tdSql.query(f'select count(*) from {self.subtable_prefix}{self.ctb_name}{self.subtable_suffix};')
            self.tdSql.checkEqual(self.tdSql.query_data[0][0] > 0, True)
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
        stb_source_sql = f'select * from {long_life_cycle_db}.{long_life_cycle_stb} partition by tbname'
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
        self.tdCom.check_stream(f'select {self.stb_filter_des_select_elm} from {stb_stream_target_tbname};', f'select  {self.stb_filter_des_select_elm} from {long_life_cycle_db}.{long_life_cycle_stb}  partition by tbname;', expected_res)
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


        source_stb_source_sql = f'select * from {source_tandem_db}.{source_tandem_stb} partition by tbname'
        source_ctb_source_sql = f'select * from {source_tandem_db}.{source_tandem_ctb}'
        source_tb_source_sql = f'select * from {source_tandem_db}.{source_tandem_tb}'
        target_stb_source_sql = f'select * from {source_stb_stream_target_tbname}  partition by tbname'
        target_ctb_source_sql = f'select * from {source_ctb_stream_target_tbname}  partition by tbname'
        target_tb_source_sql = f'select * from {source_tb_stream_target_tbname}  partition by tbname'

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

    def udf_test(self, udf_size, outputtype, fill_history_value=None):
        self.case_name = sys._getframe().f_code.co_name
        self.prepare_data(fill_history_value=fill_history_value)
        stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", {self.partition_tbname_alias}), "{self.subtable_suffix}")' if self.subtable else None
        ctb_subtable_value = f'concat(concat("{self.ctb_name}_{self.subtable_prefix}", {self.partition_tbname_alias}), "{self.subtable_suffix}")' if self.subtable else None
        tb_subtable_value = f'concat(concat("{self.tb_name}_{self.subtable_prefix}", {self.partition_tbname_alias}), "{self.subtable_suffix}")' if self.subtable else None
        self.tdCom.drop_all_udfs()
        self.tdCom.write_latency(self.case_name)
        udf1 = "udf1"
        self.build_udf_so()
        self.tdCom.create_udf(udf1, self.udf1, udf_size, outputtype)
        # create stb/ctb/tb stream
        self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select ts, udf1(c1), udf1(c2) from {self.stb_name} partition by tbname {self.partition_tbname_alias}', subtable_value=stb_subtable_value, fill_history_value=fill_history_value)
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select ts, udf1(c1), udf1(c2)  from {self.ctb_name} partition by tbname {self.partition_tbname_alias}', subtable_value=ctb_subtable_value, fill_history_value=fill_history_value)
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select ts, udf1(c1), udf1(c2)  from {self.tb_name} partition by tbname {self.partition_tbname_alias}', subtable_value=tb_subtable_value, fill_history_value=fill_history_value)

        # insert data
        count = 1
        step_count = 1
        for i in range(1, self.range_count):
            ctb_name = self.tdCom.get_long_name()
            self.tdCom.create_ctable(stbname=self.stb_name, ctbname=ctb_name)
            if i % 2 == 0:
                step_count += i
                for j in range(count, step_count):
                    self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=f'{self.date_time}+{j}s', need_null=True)
                    self.tdCom.insert_rows(tbname=self.tb_name, ts_value=f'{self.date_time}+{j}s', need_null=True)
                count += i
            else:
                step_count += 1
                for i in range(2):
                    self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=f'{self.date_time}+{count}s', need_null=True)
                    self.tdCom.insert_rows(tbname=self.tb_name, ts_value=f'{self.date_time}+{count}s', need_null=True)
                count += 1
            # check result
            self.tdCom.check_query_data(f'select ts, `udf1(c1)`, `udf1(c2)` from {self.stb_name}{self.des_table_suffix}', f'select ts, udf1(c1), udf1(c2)  from {self.stb_name} partition by tbname')
            self.tdCom.check_query_data(f'select ts, `udf1(c1)`, `udf1(c2)` from {self.ctb_name}{self.des_table_suffix}', f'select ts, udf1(c1), udf1(c2)  from {self.ctb_name} partition by tbname')
            self.tdCom.check_query_data(f'select ts, `udf1(c1)`, `udf1(c2)` from {self.tb_name}{self.des_table_suffix}', f'select ts, udf1(c1), udf1(c2) from {self.tb_name} partition by tbname')

        if fill_history_value:
            self.update_delete_history_data()

        if self.subtable:
            self.tdSql.query(f'select count(*) from {self.stb_name}_{self.subtable_prefix}{self.ctb_name}{self.subtable_suffix};')
            self.tdSql.checkEqual(self.tdSql.query_data[0][0] > 0, True)
            self.tdSql.query(f'select count(*) from {self.ctb_name}_{self.subtable_prefix}{self.ctb_name}{self.subtable_suffix};')
            self.tdSql.checkEqual(self.tdSql.query_data[0][0] > 0, True)
            self.tdSql.query(f'select count(*) from {self.tb_name}_{self.subtable_prefix}{self.tb_name}{self.subtable_suffix};')
            self.tdSql.checkEqual(self.tdSql.query_data[0][0] > 0, True)

    def udaf_test(self, interval, udf_size, outputtype, fill_history_value=None):
        self.case_name = sys._getframe().f_code.co_name
        self.prepare_data(interval=interval, fill_history_value=fill_history_value)
        self.tdCom.drop_all_udfs()
        self.tdCom.write_latency(self.case_name)
        udf2 = "udf2"
        self.build_udf_so()
        self.tdCom.create_udf(udf2, self.udf2, udf_size, outputtype, True)
        # create stb/ctb/tb stream
        # self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select _wstart AS wstart, udf2(c10)  from {self.stb_name} interval({self.dataDict["interval"]}s)')
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstart AS wstart, udf2(c10)  from {self.ctb_name} interval({self.dataDict["interval"]}s)', fill_history_value=fill_history_value)
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstart AS wstart, udf2(c10)  from {self.tb_name} interval({self.dataDict["interval"]}s)', fill_history_value=fill_history_value)

        # insert data
        count = 1
        step_count = 1
        for i in range(1, self.range_count):
            ctb_name = self.tdCom.get_long_name()
            self.tdCom.create_ctable(stbname=self.stb_name, ctbname=ctb_name)
            if i % 2 == 0:
                step_count += i
                for j in range(count, step_count):
                    self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=f'{self.date_time}+{j}s', need_null=False)
                    self.tdCom.insert_rows(tbname=self.tb_name, ts_value=f'{self.date_time}+{j}s', need_null=False)
                count += i
            else:
                step_count += 1
                for i in range(2):
                    self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=f'{self.date_time}+{count}s', need_null=False)
                    self.tdCom.insert_rows(tbname=self.tb_name, ts_value=f'{self.date_time}+{count}s', need_null=False)
                count += 1
            if fill_history_value:
                self.update_delete_history_data()
            # check result
            # self.tdCom.check_query_data(f'select wstart, `udf2(c10)` from {self.stb_name}{self.des_table_suffix}', f'select _wstart AS wstart, udf2(c10) from {self.stb_name} interval({self.dataDict["interval"]}s)')
            self.tdCom.check_query_data(f'select wstart, `udf2(c10)` from {self.ctb_name}{self.des_table_suffix}', f'select _wstart AS wstart, udf2(c10) from {self.ctb_name} interval({self.dataDict["interval"]}s)')
            self.tdCom.check_query_data(f'select wstart, `udf2(c10)` from {self.tb_name}{self.des_table_suffix}', f'select _wstart AS wstart, udf2(c10) from {self.tb_name} interval({self.dataDict["interval"]}s)')

    def at_once_interval(self, interval, partition="tbname", delete=False, fill_value=None, fill_history_value=None, interval_value=None, case_when=None):
        self.delete = delete
        self.case_name = sys._getframe().f_code.co_name
        # if interval_value is None:
        #     interval_value = f'{self.dataDict["interval"]}s'
        self.prepare_data(interval=interval, fill_history_value=fill_history_value)

        if partition == "tbname":
            if case_when:
                stream_case_when_partition = case_when
            else:
                stream_case_when_partition = self.partition_tbname_alias

            partition_elm_alias = self.partition_tbname_alias
        elif partition == "c1":
            if case_when:
                stream_case_when_partition = case_when
            else:
                stream_case_when_partition = self.partition_col_alias
            partition_elm_alias = self.partition_col_alias
        elif partition == "abs(c1)":
            partition_elm_alias = self.partition_expression_alias
        elif partition is None:
            partition_elm_alias = '"no_partition"'
        else:
            partition_elm_alias = self.partition_tag_alias
        if partition == "tbname" or partition is None:
            if case_when:
                stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", {stream_case_when_partition}), "{self.subtable_suffix}")' if self.subtable else None
                ctb_subtable_value = f'concat(concat("{self.ctb_name}_{self.subtable_prefix}", {stream_case_when_partition}), "{self.subtable_suffix}")' if self.subtable else None
                tb_subtable_value = f'concat(concat("{self.tb_name}_{self.subtable_prefix}", {stream_case_when_partition}), "{self.subtable_suffix}")' if self.subtable else None
            else:
                stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", {partition_elm_alias}), "{self.subtable_suffix}")' if self.subtable else None
                ctb_subtable_value = f'concat(concat("{self.ctb_name}_{self.subtable_prefix}", {partition_elm_alias}), "{self.subtable_suffix}")' if self.subtable else None
                tb_subtable_value = f'concat(concat("{self.tb_name}_{self.subtable_prefix}", {partition_elm_alias}), "{self.subtable_suffix}")' if self.subtable else None
        else:
            stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", cast(cast(abs(cast({partition_elm_alias} as int)) as bigint) as varchar(100))), "{self.subtable_suffix}")' if self.subtable else None
            ctb_subtable_value = f'concat(concat("{self.ctb_name}_{self.subtable_prefix}", cast(cast(abs(cast({partition_elm_alias} as int)) as bigint) as varchar(100))), "{self.subtable_suffix}")' if self.subtable else None
            tb_subtable_value = f'concat(concat("{self.tb_name}_{self.subtable_prefix}", cast(cast(abs(cast({partition_elm_alias} as int)) as bigint) as varchar(100))), "{self.subtable_suffix}")' if self.subtable else None
        if partition:
            partition_elm = f'partition by {partition} {partition_elm_alias}'
        else:
            partition_elm = ""
        self.tdCom.write_latency(self.case_name)
        if fill_value:
            if "value" in fill_value.lower():
                fill_value='VALUE,1,2,3,4,5,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11'
        self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.stb_source_select_str}  from {self.stb_name} {partition_elm} interval({self.dataDict["interval"]}s)', trigger_mode="at_once", subtable_value=stb_subtable_value, fill_value=fill_value, fill_history_value=fill_history_value)
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.stb_source_select_str}  from {self.ctb_name} {partition_elm} interval({self.dataDict["interval"]}s)', trigger_mode="at_once", subtable_value=ctb_subtable_value, fill_value=fill_value, fill_history_value=fill_history_value)
        if fill_value:
            if "value" in fill_value.lower():
                fill_value='VALUE,1,2,3,4,5,6,7,8,9,10,11'
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.tb_source_select_str}  from {self.tb_name} {partition_elm} interval({self.dataDict["interval"]}s)', trigger_mode="at_once", subtable_value=tb_subtable_value, fill_value=fill_value, fill_history_value=fill_history_value)
        start_time = self.date_time
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
            if partition:
                partition_elm = f'partition by {partition}'
            else:
                partition_elm = ""

            if not fill_value:
                for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
                    if tbname != self.tb_name:
                        self.tdCom.check_query_data(f'select wstart, {self.stb_output_select_str} from {tbname}{self.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.stb_source_select_str}  from {tbname} {partition_elm} interval({self.dataDict["interval"]}s) order by wstart', sorted=True)
                    else:
                        self.tdCom.check_query_data(f'select wstart, {self.tb_output_select_str} from {tbname}{self.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.tb_source_select_str}  from {tbname} {partition_elm} interval({self.dataDict["interval"]}s) order by wstart', sorted=True)

        if self.subtable:
            # self.tdSql.query(f'select count(*) from {self.stb_name}_{self.subtable_prefix}{self.ctb_name}{self.subtable_suffix};')
            # self.tdSql.checkEqual(self.tdSql.query_data[0][0] > 0, True)
            for tname in [self.stb_name, self.ctb_name]:
                self.tdSql.query(f'select * from {self.ctb_name}')
                ptn_counter = 0
                for c1_value in self.tdSql.query_data:
                    if partition == "c1":
                        self.tdSql.query(f'select count(*) from `{tname}_{self.subtable_prefix}{abs(c1_value[1])}{self.subtable_suffix}`;')
                    elif partition is None:
                        self.tdSql.query(f'select count(*) from `{tname}_{self.subtable_prefix}no_partition{self.subtable_suffix}`;')
                    elif partition == "abs(c1)":
                        abs_c1_value = abs(c1_value[1])
                        self.tdSql.query(f'select count(*) from `{tname}_{self.subtable_prefix}{abs_c1_value}{self.subtable_suffix}`;')
                    elif partition == "tbname" and ptn_counter == 0:
                        self.tdSql.query(f'select count(*) from `{tname}_{self.subtable_prefix}{self.ctb_name}{self.subtable_suffix}`;')
                        ptn_counter += 1
            # self.tdSql.query(f'select count(*) from {self.ctb_name}_{self.subtable_prefix}{self.ctb_name}{self.subtable_suffix};')
                    self.tdSql.checkEqual(self.tdSql.query_data[0][0] > 0, True)

            self.tdSql.query(f'select * from {self.tb_name}')
            ptn_counter = 0
            for c1_value in self.tdSql.query_data:
                if partition == "c1":
                    self.tdSql.query(f'select count(*) from `{self.tb_name}_{self.subtable_prefix}{abs(c1_value[1])}{self.subtable_suffix}`;')
                elif partition is None:
                    self.tdSql.query(f'select count(*) from `{self.tb_name}_{self.subtable_prefix}no_partition{self.subtable_suffix}`;')
                elif partition == "abs(c1)":
                    abs_c1_value = abs(c1_value[1])
                    self.tdSql.query(f'select count(*) from `{self.tb_name}_{self.subtable_prefix}{abs_c1_value}{self.subtable_suffix}`;')
                elif partition == "tbname" and ptn_counter == 0:
                    self.tdSql.query(f'select count(*) from `{self.tb_name}_{self.subtable_prefix}{self.tb_name}{self.subtable_suffix}`;')
                    ptn_counter += 1

                self.tdSql.checkEqual(self.tdSql.query_data[0][0] > 0, True)
            # self.tdSql.query(f'select * from {self.tb_name}')
            # self.tdSql.query(f'select count(*) from {self.tb_name}_{self.subtable_prefix}{self.tb_name}{self.subtable_suffix};')
            # self.tdSql.checkEqual(self.tdSql.query_data[0][0] > 0, True)
        if fill_value:
            end_date_time = self.date_time
            final_range_count = self.range_count
            history_ts = str(start_time)+f'-{self.dataDict["interval"]*(final_range_count+2)}s'
            start_ts = self.tdCom.time_cast(history_ts, "-")
            future_ts = str(end_date_time)+f'+{self.dataDict["interval"]*(final_range_count+2)}s'
            end_ts = self.tdCom.time_cast(future_ts)
            self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=history_ts)
            self.tdCom.insert_rows(tbname=self.tb_name, ts_value=history_ts)
            self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=future_ts)
            self.tdCom.insert_rows(tbname=self.tb_name, ts_value=future_ts)
            self.date_time = start_time
            if self.update:
                history_ts = str(start_time)+f'-{self.dataDict["interval"]*(final_range_count+2)}s'
                start_ts = self.tdCom.time_cast(history_ts, "-")
                future_ts = str(end_date_time)+f'+{self.dataDict["interval"]*(final_range_count+2)}s'
                end_ts = self.tdCom.time_cast(future_ts)
                self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=history_ts)
                self.tdCom.insert_rows(tbname=self.tb_name, ts_value=history_ts)
                self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=future_ts)
                self.tdCom.insert_rows(tbname=self.tb_name, ts_value=future_ts)
                self.date_time = start_time
                for i in range(self.range_count):
                    ts_value = str(self.date_time+self.dataDict["interval"])+f'+{i*10}s'
                    ts_cast_delete_value = self.tdCom.time_cast(ts_value)
                    self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=ts_value)
                    # if self.delete and i%2 != 0:
                    #     self.tdCom.delete_rows(tbname=self.ctb_name, start_ts=ts_cast_delete_value)
                    self.date_time += 1
                    self.tdCom.insert_rows(tbname=self.tb_name, ts_value=ts_value)
                    # if self.delete and i%2 != 0:
                    #     self.tdCom.delete_rows(tbname=self.tb_name, start_ts=ts_cast_delete_value)
                    self.date_time += 1
            if self.delete:
                self.tdCom.delete_rows(tbname=self.ctb_name, start_ts=self.tdCom.time_cast(start_time), end_ts=ts_cast_delete_value)
                self.tdCom.delete_rows(tbname=self.tb_name, start_ts=self.tdCom.time_cast(start_time), end_ts=ts_cast_delete_value)
            # self.tdCom.delete_rows(tbname=self.ctb_name, start_ts=start_ts, end_ts=ts_cast_delete_value)
            for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
                if tbname != self.tb_name:
                    if "value" in fill_value.lower():
                        fill_value='VALUE,1,2,3,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11'
                    if partition == "tbname":
                        self.tdCom.check_query_data(f'select wstart, {self.fill_stb_output_select_str} from {tbname}{self.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.fill_stb_source_select_str}  from {tbname} where ts >= {start_ts} and ts <= {end_ts} partition by {partition} interval({self.dataDict["interval"]}s) fill ({fill_value}) order by wstart', fill_value=fill_value)
                    else:
                        self.tdCom.check_query_data(f'select wstart, {self.fill_stb_output_select_str} from {tbname}{self.des_table_suffix} where `min(c1)` is not Null order by wstart,`min(c1)`', f'select * from (select _wstart AS wstart, {self.fill_stb_source_select_str}  from {tbname} where ts >= {start_ts} and ts <= {end_ts} partition by {partition} interval({self.dataDict["interval"]}s) fill ({fill_value}) order by wstart) where `min(c1)` is not Null order by wstart,`min(c1)`', fill_value=fill_value)
                else:
                    if "value" in fill_value.lower():
                        fill_value='VALUE,1,2,3,6,7,8,9,10,11'
                    if partition == "tbname":
                        self.tdCom.check_query_data(f'select wstart, {self.fill_tb_output_select_str} from {tbname}{self.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.fill_tb_source_select_str}  from {tbname} where ts >= {start_ts} and ts <= {end_ts} partition by {partition} interval({self.dataDict["interval"]}s) fill ({fill_value}) order by wstart', fill_value=fill_value)
                    else:
                        self.tdCom.check_query_data(f'select wstart, {self.fill_tb_output_select_str} from {tbname}{self.des_table_suffix} where `min(c1)` is not Null order by wstart,`min(c1)`', f'select * from (select _wstart AS wstart, {self.fill_tb_source_select_str}  from {tbname} where ts >= {start_ts} and ts <= {end_ts} partition by {partition} interval({self.dataDict["interval"]}s) fill ({fill_value}) order by wstart) where `min(c1)` is not Null order by wstart,`min(c1)`', fill_value=fill_value)

            if self.delete:
                self.tdCom.delete_rows(tbname=self.ctb_name, start_ts=start_ts, end_ts=ts_cast_delete_value)
                self.tdCom.delete_rows(tbname=self.tb_name, start_ts=start_ts, end_ts=ts_cast_delete_value)
                for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
                    if tbname != self.tb_name:
                        if "value" in fill_value.lower():
                            fill_value='VALUE,1,2,3,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11'
                        if partition == "tbname":
                            self.tdCom.check_query_data(f'select wstart, {self.fill_stb_output_select_str} from {tbname}{self.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.fill_stb_source_select_str}  from {tbname} where ts >= {start_ts.replace("-", "+")} and ts <= {end_ts} partition by {partition} interval({self.dataDict["interval"]}s) fill ({fill_value}) order by wstart', fill_value=fill_value)
                        else:
                            # TODO Optimize TD-22963
                            self.tdCom.check_query_data(f'select wstart, {self.fill_stb_output_select_str} from {tbname}{self.des_table_suffix} order by wstart,`min(c1)`', f'select * from (select _wstart AS wstart, {self.fill_stb_source_select_str}  from {tbname} where ts >= {start_ts} and ts <= {end_ts} partition by {partition} interval({self.dataDict["interval"]}s) fill ({fill_value}) order by wstart) where `min(c1)` is not Null order by wstart,`min(c1)`', fill_value=fill_value)

                    else:
                        if "value" in fill_value.lower():
                            fill_value='VALUE,1,2,3,6,7,8,9,10,11'
                        if partition == "tbname":
                            self.tdCom.check_query_data(f'select wstart, {self.fill_tb_output_select_str} from {tbname}{self.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.fill_tb_source_select_str}  from {tbname} where ts >= {start_ts.replace("-", "+")} and ts <= {end_ts} partition by {partition} interval({self.dataDict["interval"]}s) fill ({fill_value}) order by wstart', fill_value=fill_value)
                        else:
                            # TODO Optimize TD-22963
                            self.tdCom.check_query_data(f'select wstart, {self.fill_tb_output_select_str} from {tbname}{self.des_table_suffix} order by wstart,`min(c1)`', f'select * from (select _wstart AS wstart, {self.fill_tb_source_select_str}  from {tbname} where ts >= {start_ts} and ts <= {end_ts} partition by {partition} interval({self.dataDict["interval"]}s) fill ({fill_value}) order by wstart) where `min(c1)` is not Null order by wstart,`min(c1)`', fill_value=fill_value)


            # if fill_value:
            #     history_ts = str(start_time)+f'-{self.dataDict["interval"]*(self.range_count+2)}s'
            # print(history_ts)
            # start_ts = self.tdCom.time_cast(history_ts, "-")
            # future_ts = str(self.date_time)+f'+{self.dataDict["interval"]*(self.range_count+2)}s'
            # end_ts = self.tdCom.time_cast(future_ts)
            # self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=history_ts)
            # self.tdCom.insert_rows(tbname=self.tb_name, ts_value=history_ts)
            # self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=future_ts)
            # self.tdCom.insert_rows(tbname=self.tb_name, ts_value=future_ts)
            # self.date_time = start_time
            # for i in range(self.range_count):

            #     ts_value = str(self.date_time+self.dataDict["interval"])+f'+{i*10}s'
            #     ts_cast_delete_value = self.tdCom.time_cast(ts_value)
            #     self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=ts_value)
            #     # if self.delete and i%2 != 0:
            #     #     self.tdCom.delete_rows(tbname=self.ctb_name, start_ts=ts_cast_delete_value)
            #     self.date_time += 1
            #     self.tdCom.insert_rows(tbname=self.tb_name, ts_value=ts_value)
            #     # if self.delete and i%2 != 0:
            #     #     self.tdCom.delete_rows(tbname=self.tb_name, start_ts=ts_cast_delete_value)
            #     self.date_time += 1
            # print(ts_cast_delete_value)
            # print(start_ts)
            # print(self.tdCom.time_cast(self.date_time))
            # self.tdCom.delete_rows(tbname=self.ctb_name, start_ts=self.tdCom.time_cast(start_time), end_ts=ts_cast_delete_value)

            # for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
            #     if tbname != self.tb_name:
            #         if "value" in fill_value.lower():
            #             fill_value='VALUE,1,2,3,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11'
            #         self.tdCom.check_query_data(f'select wstart, {self.fill_stb_output_select_str} from {tbname}{self.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.fill_stb_source_select_str}  from {tbname} where ts >= {start_ts} and ts <= {end_ts} partition by {partition} interval({self.dataDict["interval"]}s) fill ({fill_value}) order by wstart', sorted=True, fill_value=fill_value)
            #     else:
            #         if "value" in fill_value.lower():
            #             fill_value='VALUE,1,2,3,6,7,8,9,10,11'
            #         self.tdCom.check_query_data(f'select wstart, {self.fill_tb_output_select_str} from {tbname}{self.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.fill_tb_source_select_str}  from {tbname} where ts >= {start_ts} and ts <= {end_ts} partition by {partition} interval({self.dataDict["interval"]}s) fill ({fill_value}) order by wstart', sorted=True, fill_value=fill_value)

            # self.tdCom.delete_rows(tbname=self.ctb_name, start_ts=start_ts, end_ts=ts_cast_delete_value)

            # for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
            #     if tbname != self.tb_name:
            #         if "value" in fill_value.lower():
            #             fill_value='VALUE,1,2,3,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11'
            #         self.tdCom.check_query_data(f'select wstart, {self.fill_stb_output_select_str} from {tbname}{self.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.fill_stb_source_select_str}  from {tbname} where ts >= {start_ts} and ts <= {end_ts} partition by {partition} interval({self.dataDict["interval"]}s) fill ({fill_value}) order by wstart', sorted=True, fill_value=fill_value)
            #     else:
            #         if "value" in fill_value.lower():
            #             fill_value='VALUE,1,2,3,6,7,8,9,10,11'
            #         self.tdCom.check_query_data(f'select wstart, {self.fill_tb_output_select_str} from {tbname}{self.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.fill_tb_source_select_str}  from {tbname} where ts >= {start_ts} and ts <= {end_ts} partition by {partition} interval({self.dataDict["interval"]}s) fill ({fill_value}) order by wstart', sorted=True, fill_value=fill_value)



    def at_once_interval_ext(self, interval, partition="tbname", delete=False, fill_value=None, fill_history_value=None, interval_value=None, subtable=None, case_when=None, stb_field_name_value=None, tag_value=None, use_exist_stb=False, use_except=False):
        if use_except:
            if stb_field_name_value == self.partitial_stb_filter_des_select_elm or stb_field_name_value == self.exchange_stb_filter_des_select_elm or len(stb_field_name_value.split(",")) == len(self.partitial_stb_filter_des_select_elm.split(",")):
                partitial_tb_source_str = self.partitial_ext_tb_source_select_str
            else:
                partitial_tb_source_str = self.ext_tb_source_select_str
        else:
            if stb_field_name_value == self.partitial_stb_filter_des_select_elm or stb_field_name_value == self.exchange_stb_filter_des_select_elm:
                partitial_tb_source_str = self.partitial_ext_tb_source_select_str
            else:
                partitial_tb_source_str = self.ext_tb_source_select_str

        if stb_field_name_value is not None:
            if len(stb_field_name_value) == 0:
                stb_field_name_value = ",".join(self.tb_filter_des_select_elm.split(",")[:5])
            # else:
            #     stb_field_name_value = self.tb_filter_des_select_elm
        self.delete = delete
        self.case_name = sys._getframe().f_code.co_name
        defined_tag_count = len(tag_value.split()) if tag_value is not None else 0
        # if interval_value is None:
        #     interval_value = f'{self.dataDict["interval"]}s'
        self.prepare_data(interval=interval, fill_history_value=fill_history_value)

        if partition == "tbname":
            if case_when:
                stream_case_when_partition = case_when
            else:
                stream_case_when_partition = self.partition_tbname_alias

            partition_elm_alias = self.partition_tbname_alias
        elif partition == "c1":
            if case_when:
                stream_case_when_partition = case_when
            else:
                stream_case_when_partition = self.partition_col_alias
            partition_elm_alias = self.partition_col_alias
        elif partition == "abs(c1)":
            partition_elm_alias = self.partition_expression_alias
        elif partition == "tbname,t1,c1":
            partition_elm_alias = f'{self.partition_tbname_alias},t1,c1'
            partiton_tb = "tbname,c1"
            partition_elm_alias_tb = f'{self.partition_tbname_alias},c1'
        else:
            partition_elm_alias = self.partition_tag_alias
        if subtable:
            if partition == "tbname":
                if case_when:
                    stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", {stream_case_when_partition}), "{self.subtable_suffix}")' if self.subtable else None
                else:
                    stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", {partition_elm_alias}), "{self.subtable_suffix}")' if self.subtable else None
            else:
                if subtable == "constant":
                    # stb_subtable_value = f'"{self.ext_ctb_stream_des_table}"'
                    stb_subtable_value = f'"constant_{self.ext_ctb_stream_des_table}"'
                else:
                    stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", cast(cast(cast({subtable} as int unsigned) as bigint) as varchar(100))), "{self.subtable_suffix}")' if self.subtable else None
        else:
            stb_subtable_value = None
        self.tdCom.write_latency(self.case_name)
        if fill_value:
            if "value" in fill_value.lower():
                fill_value='VALUE,1,2,3,4,5,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11'
        # self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.ext_stb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.ext_tb_source_select_str}  from {self.stb_name} partition by {partition} interval({self.dataDict["interval"]}s)', trigger_mode="at_once", fill_value=fill_value, fill_history_value=fill_history_value, stb_field_name_value=stb_field_name_value, tag_value=tag_value, use_exist_stb=use_exist_stb)
        if partition:
            stream_sql = self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.ext_stb_stream_des_table, subtable_value=stb_subtable_value, source_sql=f'select _wstart AS wstart, {partitial_tb_source_str}  from {self.stb_name} partition by {partition} interval({self.dataDict["interval"]}s)', trigger_mode="at_once", fill_value=fill_value, fill_history_value=fill_history_value, stb_field_name_value=stb_field_name_value, tag_value=tag_value, use_exist_stb=use_exist_stb, use_except=use_except)
        else:
            stream_sql = self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.ext_stb_stream_des_table, subtable_value=stb_subtable_value, source_sql=f'select _wstart AS wstart, {partitial_tb_source_str}  from {self.stb_name} interval({self.dataDict["interval"]}s)', trigger_mode="at_once", fill_value=fill_value, fill_history_value=fill_history_value, stb_field_name_value=stb_field_name_value, tag_value=tag_value, use_exist_stb=use_exist_stb, use_except=use_except)
        if stream_sql:
            self.tdSql.error(stream_sql)
            return
        start_time = self.date_time
        if subtable == "constant":
            range_count = 1
        else:
            range_count = self.range_count

        for i in range(range_count):
            tag_value_list = list()
            ts_value = str(self.date_time+self.dataDict["interval"])+f'+{i*10}s'
            ts_cast_delete_value = self.tdCom.time_cast(ts_value)
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
            if tag_value:
                if subtable == "constant":
                    self.tdSql.query(f'select {tag_value} from constant_{self.ext_ctb_stream_des_table}')
                else:
                    self.tdSql.query(f'select {tag_value} from {self.stb_name}')
                tag_value_list = self.tdSql.query_data
            if not fill_value:
                if stb_field_name_value == self.partitial_stb_filter_des_select_elm:
                    self.tdCom.check_query_data(f'select {self.partitial_stb_filter_des_select_elm } from ext_{self.stb_name}{self.des_table_suffix} order by ts', f'select _wstart AS wstart, {partitial_tb_source_str}  from {self.stb_name} partition by {partition} interval({self.dataDict["interval"]}s) order by wstart', sorted=True)
                elif stb_field_name_value == self.exchange_stb_filter_des_select_elm:
                    self.tdCom.check_query_data(f'select {self.partitial_stb_filter_des_select_elm } from ext_{self.stb_name}{self.des_table_suffix} order by ts', f'select _wstart AS wstart, cast(max(c2) as tinyint), cast(min(c1) as smallint)  from {self.stb_name} partition by {partition} interval({self.dataDict["interval"]}s) order by wstart', sorted=True)
                else:
                    if partition:
                        if tag_value == self.exchange_tag_filter_des_select_elm:
                            self.tdCom.check_query_data(f'select {self.partitial_tag_stb_filter_des_select_elm} from ext_{self.stb_name}{self.des_table_suffix} order by ts', f'select _wstart AS wstart, {self.stb_source_select_str}  from {self.stb_name} partition by {partition} interval({self.dataDict["interval"]}s) order by wstart', defined_tag_count=defined_tag_count, tag_value_list=tag_value_list)
                        elif tag_value == self.cast_tag_filter_des_select_elm:
                            self.tdSql.query(f'select _wstart AS wstart, {self.stb_source_select_str}  from {self.stb_name} partition by {partition} interval({self.dataDict["interval"]}s) order by wstart')
                            limit_row = self.tdSql.query_row
                            self.tdCom.check_query_data(f'select {self.cast_tag_filter_des_select_elm} from ext_{self.stb_name}{self.des_table_suffix} order by ts', f'select cast(t1 as TINYINT UNSIGNED),cast(t2 as varchar(256)),cast(t3 as bool) from {self.stb_name}  order by ts limit {limit_row}')
                            self.tdSql.query(f'select t1,t2,t3,t4,t6,t7,t8,t9,t10,t12 from ext_{self.stb_name}{self.des_table_suffix};')
                            self.tdSql.checkEqual(list(set(self.tdSql.query_data)), [(None, None, None, None, None, None, None, None, None, None)])
                        else:
                            self.tdCom.check_query_data(f'select {self.stb_filter_des_select_elm} from ext_{self.stb_name}{self.des_table_suffix} order by ts', f'select _wstart AS wstart, {self.stb_source_select_str}  from {self.stb_name} partition by {partition} interval({self.dataDict["interval"]}s) order by wstart', defined_tag_count=defined_tag_count, tag_value_list=tag_value_list)
                    else:
                        if use_exist_stb and not tag_value:
                            self.tdCom.check_query_data(f'select {self.stb_filter_des_select_elm} from ext_{self.stb_name}{self.des_table_suffix} order by ts', f'select _wstart AS wstart, {self.stb_source_select_str}  from {self.stb_name} interval({self.dataDict["interval"]}s) order by wstart', defined_tag_count=defined_tag_count, tag_value_list=tag_value_list, partition=partition, use_exist_stb=use_exist_stb)
                        else:
                            self.tdCom.check_query_data(f'select {self.stb_filter_des_select_elm} from ext_{self.stb_name}{self.des_table_suffix} order by ts', f'select _wstart AS wstart, {self.stb_source_select_str}  from {self.stb_name} interval({self.dataDict["interval"]}s) order by wstart', defined_tag_count=defined_tag_count, tag_value_list=tag_value_list, partition=partition, subtable=subtable)

        if subtable:
            for tname in [self.stb_name]:
                self.tdSql.query(f'select * from {self.ctb_name}')
                ptn_counter = 0
                for c1_value in self.tdSql.query_data:
                    if partition == "c1":
                        self.tdSql.query(f'select count(*) from `{tname}_{self.subtable_prefix}{abs(c1_value[1])}{self.subtable_suffix}`;')
                    elif partition == "abs(c1)":
                        abs_c1_value = abs(c1_value[1])
                        self.tdSql.query(f'select count(*) from `{tname}_{self.subtable_prefix}{abs_c1_value}{self.subtable_suffix}`;')
                    elif partition == "tbname" and ptn_counter == 0:
                        self.tdSql.query(f'select count(*) from `{tname}_{self.subtable_prefix}{self.ctb_name}{self.subtable_suffix}`;')
                        ptn_counter += 1
                    else:
                        self.tdSql.query(f'select cast(cast(cast({c1_value[1]} as int unsigned) as bigint) as varchar(100))')
                        subtable_value = self.tdSql.query_data[0][0]
                        if subtable == "constant":
                            return
                        else:
                            self.tdSql.query(f'select count(*) from `{tname}_{self.subtable_prefix}{subtable_value}{self.subtable_suffix}`;')
                    self.tdSql.checkEqual(self.tdSql.query_data[0][0] > 0, True)

        # # ! TD-22500
        # if fill_value:
        #     self.stb_filter_des_select_elm = self.stb_filter_des_select_elm.replace("c4, c5,", "")
        #     end_date_time = self.date_time
        #     final_range_count = self.range_count
        #     history_ts = str(start_time)+f'-{self.dataDict["interval"]*(final_range_count+2)}s'
        #     start_ts = self.tdCom.time_cast(history_ts, "-")
        #     future_ts = str(end_date_time)+f'+{self.dataDict["interval"]*(final_range_count+2)}s'
        #     end_ts = self.tdCom.time_cast(future_ts)
        #     self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=history_ts)
        #     self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=future_ts)
        #     self.date_time = start_time
        #     if self.update:
        #         history_ts = str(start_time)+f'-{self.dataDict["interval"]*(final_range_count+2)}s'
        #         start_ts = self.tdCom.time_cast(history_ts, "-")
        #         future_ts = str(end_date_time)+f'+{self.dataDict["interval"]*(final_range_count+2)}s'
        #         end_ts = self.tdCom.time_cast(future_ts)
        #         self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=history_ts)
        #         self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=future_ts)
        #         self.date_time = start_time
        #         for i in range(self.range_count):
        #             ts_value = str(self.date_time+self.dataDict["interval"])+f'+{i*10}s'
        #             ts_cast_delete_value = self.tdCom.time_cast(ts_value)
        #             self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=ts_value)
        #             # if self.delete and i%2 != 0:
        #             #     self.tdCom.delete_rows(tbname=self.ctb_name, start_ts=ts_cast_delete_value)
        #             self.date_time += 1
        #     if self.delete:
        #         self.tdCom.delete_rows(tbname=self.ctb_name, start_ts=self.tdCom.time_cast(start_time), end_ts=ts_cast_delete_value)
        #     for tbname in [self.stb_name]:
        #         if tbname != self.tb_name:
        #             if "value" in fill_value.lower():
        #                 fill_value='VALUE,1,2,3,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11'
        #             if partition == "tbname":
        #                 self.tdCom.check_query_data(f'select {self.stb_filter_des_select_elm} from ext_{self.stb_name}{self.des_table_suffix} order by ts', f'select _wstart AS wstart, {self.fill_stb_source_select_str}  from {self.stb_name} where ts >= {start_ts} and ts <= {end_ts} partition by {partition} interval({self.dataDict["interval"]}s) fill ({fill_value}) order by wstart', fill_value=fill_value, defined_tag_count=defined_tag_count, tag_value_list=tag_value_list)
        #             else:
        #                 self.tdCom.check_query_data(f'select {self.stb_filter_des_select_elm} from ext_{self.stb_name}{self.des_table_suffix} order by ts,c1', f'select * from (select _wstart AS wstart, {self.fill_stb_source_select_str}  from {self.stb_name} where ts >= {start_ts} and ts <= {end_ts} partition by {partition} interval({self.dataDict["interval"]}s) fill ({fill_value}) order by wstart) where `min(c1)` is not Null order by wstart,`min(c1)`', fill_value=fill_value, defined_tag_count=defined_tag_count, tag_value_list=tag_value_list)

            # if self.delete:
            #     self.tdCom.delete_rows(tbname=self.ctb_name, start_ts=start_ts, end_ts=ts_cast_delete_value)
            #     self.tdCom.delete_rows(tbname=self.tb_name, start_ts=start_ts, end_ts=ts_cast_delete_value)
            #     for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
            #         if tbname != self.tb_name:
            #             if "value" in fill_value.lower():
            #                 fill_value='VALUE,1,2,3,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11'
            #             if partition == "tbname":
            #                 self.tdCom.check_query_data(f'select wstart, {self.fill_stb_output_select_str} from {tbname}{self.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.fill_stb_source_select_str}  from {tbname} where ts >= {start_ts.replace("-", "+")} and ts <= {end_ts} partition by {partition} interval({self.dataDict["interval"]}s) fill ({fill_value}) order by wstart', fill_value=fill_value)
            #             else:
            #                 self.tdCom.check_query_data(f'select wstart, {self.fill_stb_output_select_str} from {tbname}{self.des_table_suffix} order by wstart,`min(c1)`', f'select * from (select _wstart AS wstart, {self.fill_stb_source_select_str}  from {tbname} where ts >= {start_ts} and ts <= {end_ts} partition by {partition} interval({self.dataDict["interval"]}s) fill ({fill_value}) order by wstart) where `min(c1)` is not Null order by wstart,`min(c1)`', fill_value=fill_value)

            #         else:
            #             if "value" in fill_value.lower():
            #                 fill_value='VALUE,1,2,3,6,7,8,9,10,11'
            #             if partition == "tbname":
            #                 self.tdCom.check_query_data(f'select wstart, {self.fill_tb_output_select_str} from {tbname}{self.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.fill_tb_source_select_str}  from {tbname} where ts >= {start_ts.replace("-", "+")} and ts <= {end_ts} partition by {partition} interval({self.dataDict["interval"]}s) fill ({fill_value}) order by wstart', fill_value=fill_value)
            #             else:
            #                 self.tdCom.check_query_data(f'select wstart, {self.fill_tb_output_select_str} from {tbname}{self.des_table_suffix} order by wstart,`min(c1)`', f'select * from (select _wstart AS wstart, {self.fill_tb_source_select_str}  from {tbname} where ts >= {start_ts} and ts <= {end_ts} partition by {partition} interval({self.dataDict["interval"]}s) fill ({fill_value}) order by wstart) where `min(c1)` is not Null order by wstart,`min(c1)`', fill_value=fill_value)


    def at_once_state_window(self, state_window, partition="tbname", delete=False, fill_history_value=None, case_when=None, subtable=True):
        self.delete = delete
        self.case_name = sys._getframe().f_code.co_name
        self.prepare_data(state_window=state_window, fill_history_value=fill_history_value)

        if partition == "tbname":
            partition_elm_alias = self.partition_tbname_alias
        elif partition == "c1" and subtable is not None:
            partition_elm_alias = self.partition_col_alias
        elif partition == "c1" and subtable is None:
            partition_elm_alias = 'constant'
        elif partition == "abs(c1)":
            partition_elm_alias = self.partition_expression_alias
        else:
            partition_elm_alias = self.partition_tag_alias
        if partition == "tbname" or subtable is None:
            if partition == "tbname":
                stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", {partition_elm_alias}), "{self.subtable_suffix}")' if self.subtable else None
                ctb_subtable_value = f'concat(concat("{self.ctb_name}_{self.subtable_prefix}", {partition_elm_alias}), "{self.subtable_suffix}")' if self.subtable else None
                tb_subtable_value = f'concat(concat("{self.tb_name}_{self.subtable_prefix}", {partition_elm_alias}), "{self.subtable_suffix}")' if self.subtable else None
            else:
                stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", "{partition_elm_alias}"), "{self.subtable_suffix}")' if self.subtable else None
                ctb_subtable_value = f'concat(concat("{self.ctb_name}_{self.subtable_prefix}", "{partition_elm_alias}"), "{self.subtable_suffix}")' if self.subtable else None
                tb_subtable_value = f'concat(concat("{self.tb_name}_{self.subtable_prefix}", "{partition_elm_alias}"), "{self.subtable_suffix}")' if self.subtable else None
        else:
            if 'abs' in partition:
                stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", cast(abs(cast({partition_elm_alias} as int)) as binary(20))), "{self.subtable_suffix}")' if self.subtable else None
                ctb_subtable_value = f'concat(concat("{self.ctb_name}_{self.subtable_prefix}", cast(cast(abs(cast({partition_elm_alias} as int)) as bigint) as varchar(20))), "{self.subtable_suffix}")' if self.subtable else None
                tb_subtable_value = f'concat(concat("{self.tb_name}_{self.subtable_prefix}", cast(cast(abs(cast({partition_elm_alias} as int)) as bigint) as varchar(20))), "{self.subtable_suffix}")' if self.subtable else None

            else:
                stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", cast({partition_elm_alias} as binary(20))), "{self.subtable_suffix}")' if self.subtable else None
                ctb_subtable_value = f'concat(concat("{self.ctb_name}_{self.subtable_prefix}", cast(cast({partition_elm_alias} as bigint) as varchar(20))), "{self.subtable_suffix}")' if self.subtable else None
                tb_subtable_value = f'concat(concat("{self.tb_name}_{self.subtable_prefix}", cast(cast({partition_elm_alias} as bigint) as varchar(20))), "{self.subtable_suffix}")' if self.subtable else None
            # stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", cast({partition_elm_alias} as varchar(20))), "{self.subtable_suffix}")' if self.subtable else None
            # ctb_subtable_value = f'concat(concat("{self.ctb_name}_{self.subtable_prefix}", cast({partition_elm_alias} as varchar(20))), "{self.subtable_suffix}")' if self.subtable else None
            # tb_subtable_value = f'concat(concat("{self.tb_name}_{self.subtable_prefix}", cast({partition_elm_alias} as varchar(20))), "{self.subtable_suffix}")' if self.subtable else None

        state_window_col_name = self.dataDict["state_window"]
        if case_when:
            stream_state_window = case_when
        else:
            stream_state_window = state_window_col_name
        self.tdCom.write_latency(self.case_name)
        # self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.stb_source_select_str}  from {self.stb_name} state_window({stream_state_window})', trigger_mode="at_once")
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.stb_source_select_str}  from {self.ctb_name} partition by {partition} {partition_elm_alias} state_window({stream_state_window})', trigger_mode="at_once", subtable_value=ctb_subtable_value, fill_history_value=fill_history_value)
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.tb_source_select_str}  from {self.tb_name} partition by {partition} {partition_elm_alias} state_window({stream_state_window})', trigger_mode="at_once", subtable_value=tb_subtable_value, fill_history_value=fill_history_value)
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
                self.tdCom.check_query_data(f'select wstart, {self.stb_output_select_str} from {tbname}{self.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.stb_source_select_str}  from {tbname} partition by {partition} state_window({state_window_col_name})  order by wstart,{state_window}', sorted=True)
            else:
                self.tdCom.check_query_data(f'select wstart, {self.tb_output_select_str} from {tbname}{self.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.tb_source_select_str}  from {tbname} partition by {partition} state_window({state_window_col_name}) order by wstart,{state_window}', sorted=True)

        if fill_history_value:
            self.update_delete_history_data()

        if self.subtable:
            # self.tdSql.query(f'select count(*) from {self.stb_name}_{self.subtable_prefix}{self.ctb_name}{self.subtable_suffix};')
            # self.tdSql.checkEqual(self.tdSql.query_data[0][0] > 0, True)
            self.tdSql.query(f'select * from {self.ctb_name}')
            ptn_counter = 0
            for c1_value in self.tdSql.query_data:
                if partition == "c1":
                    if subtable:
                        self.tdSql.query(f'select count(*) from `{self.ctb_name}_{self.subtable_prefix}{c1_value[1]}{self.subtable_suffix}`;')
                    else:
                        self.tdSql.query(f'select count(*) from `{self.ctb_name}_{self.subtable_prefix}{partition_elm_alias}{self.subtable_suffix}`;')
                        return
                elif partition == "abs(c1)":
                    abs_c1_value = abs(c1_value[1])
                    self.tdSql.query(f'select count(*) from `{self.ctb_name}_{self.subtable_prefix}{abs_c1_value}{self.subtable_suffix}`;')
                elif partition == "tbname" and ptn_counter == 0:
                    self.tdSql.query(f'select count(*) from `{self.ctb_name}_{self.subtable_prefix}{self.ctb_name}{self.subtable_suffix}`;')
                    ptn_counter += 1
        # self.tdSql.query(f'select count(*) from {self.ctb_name}_{self.subtable_prefix}{self.ctb_name}{self.subtable_suffix};')
                self.tdSql.checkEqual(self.tdSql.query_data[0][0] > 0, True)

            self.tdSql.query(f'select * from {self.tb_name}')
            ptn_counter = 0
            for c1_value in self.tdSql.query_data:
                if partition == "c1":
                    if subtable:
                        self.tdSql.query(f'select count(*) from `{self.tb_name}_{self.subtable_prefix}{c1_value[1]}{self.subtable_suffix}`;')
                    else:
                        self.tdSql.query(f'select count(*) from `{self.tb_name}_{self.subtable_prefix}{partition_elm_alias}{self.subtable_suffix}`;')
                        return
                elif partition == "abs(c1)":
                    abs_c1_value = abs(c1_value[1])
                    self.tdSql.query(f'select count(*) from `{self.tb_name}_{self.subtable_prefix}{abs_c1_value}{self.subtable_suffix}`;')
                elif partition == "tbname" and ptn_counter == 0:
                    self.tdSql.query(f'select count(*) from `{self.tb_name}_{self.subtable_prefix}{self.tb_name}{self.subtable_suffix}`;')
                    ptn_counter += 1

                self.tdSql.checkEqual(self.tdSql.query_data[0][0] > 0, True)

    def at_once_state_window_ext(self, state_window, partition="tbname", delete=False, fill_history_value=None, case_when=None, subtable=None, stb_field_name_value=None, tag_value=None, use_exist_stb=False):
        if not stb_field_name_value:
            stb_field_name_value = self.tb_filter_des_select_elm
        self.delete = delete
        self.case_name = sys._getframe().f_code.co_name
        defined_tag_count = len(tag_value.split())
        self.prepare_data(state_window=state_window, fill_history_value=fill_history_value)

        state_window_col_name = self.dataDict["state_window"]
        if case_when:
            stream_state_window = case_when
        else:
            stream_state_window = state_window_col_name

        if subtable:
            stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", cast(cast(abs(cast({subtable} as int)) as bigint) as varchar(100))), "{self.subtable_suffix}")' if self.subtable else None
        else:
            stb_subtable_value = None
        self.tdCom.write_latency(self.case_name)
        self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.ext_stb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.ext_tb_source_select_str}  from {self.stb_name} partition by {partition} state_window({stream_state_window})', trigger_mode="at_once", subtable_value=stb_subtable_value, fill_history_value=fill_history_value, stb_field_name_value=stb_field_name_value, tag_value=tag_value, use_exist_stb=use_exist_stb)
        range_times = self.range_count
        state_window_max = self.dataDict['state_window_max']
        for i in range(range_times):
            tag_value_list = list()
            state_window_value = random.randint(int((i)*state_window_max/range_times), int((i+1)*state_window_max/range_times))
            for i in range(2, range_times+3):
                self.tdSql.execute(f'insert into {self.ctb_name} (ts, {state_window_col_name}) values ({self.date_time}, {state_window_value})')
                if self.update and i%2 == 0:
                    self.tdSql.execute(f'insert into {self.ctb_name} (ts, {state_window_col_name}) values ({self.date_time}, {state_window_value})')
                if self.delete and i%2 != 0:
                    dt = f'cast({self.date_time-1} as timestamp)'
                    self.tdSql.execute(f'delete from {self.ctb_name} where ts = {dt}')
                self.date_time += 1
            if tag_value:
                self.tdSql.query(f'select {tag_value} from {self.stb_name}')
                tag_value_list = self.tdSql.query_data
            self.tdCom.check_query_data(f'select {self.stb_filter_des_select_elm} from ext_{self.stb_name}{self.des_table_suffix} order by ts', f'select _wstart AS wstart, {self.stb_source_select_str} from {self.stb_name} partition by {partition} state_window({state_window_col_name})  order by wstart,{state_window}', defined_tag_count=defined_tag_count, tag_value_list=tag_value_list, sorted=True)

        if fill_history_value:
            self.update_delete_history_data()

        if self.subtable:
            self.tdSql.query(f'select * from {self.ctb_name}')
            for c1_value in self.tdSql.query_data:
                self.tdSql.query(f'select count(*) from `{self.stb_name}_{self.subtable_prefix}{abs(c1_value[1])}{self.subtable_suffix}`;')
                self.tdSql.checkEqual(self.tdSql.query_data[0][0] > 0, True)

    def subtable_exceed_test(self):
        self.case_name = sys._getframe().f_code.co_name
        self.prepare_data()
        exceed_child_tbname = self.tdCom.get_long_name(self.tdCom.Boundary.CHILD_TBNAME_MAX_LENGTH + 1)
        self.tdSql.execute(f'create stream if not exists subtable_exceed_test_ct1_stream_exceed trigger at_once into subtable_exceed_test_ct1_output_exceed subtable("{exceed_child_tbname}") as select _wstart AS wstart, min(c1),max(c2),sum(c3),first(c4),last(c5),apercentile(c6, 50),avg(c7),count(c8),spread(c1),stddev(c2),hyperloglog(c11),timediff(1, 0, 1h),timezone(),to_iso8601(1),to_unixtimestamp("1970-01-01T08:00:00+08:00"),min(t1),max(t2),sum(t3),first(t4),last(t5),apercentile(t6, 50),avg(t7),count(t8),spread(t1),stddev(t2),hyperloglog(t11) from subtable_exceed_test_ct1 partition by tbname ptn_alias session(ts, 15s);')
        self.tdSql.error(f'create stream if not exists subtable_exceed_test_ct1_stream_error trigger at_once into subtable_exceed_test_ct1_output_error subtable(cast(ptn_alias as nchar(20))) as select _wstart AS wstart, min(c1),max(c2),sum(c3),first(c4),last(c5),apercentile(c6, 50),avg(c7),count(c8),spread(c1),stddev(c2),hyperloglog(c11),timediff(1, 0, 1h),timezone(),to_iso8601(1),to_unixtimestamp("1970-01-01T08:00:00+08:00"),min(t1),max(t2),sum(t3),first(t4),last(t5),apercentile(t6, 50),avg(t7),count(t8),spread(t1),stddev(t2),hyperloglog(t11) from subtable_exceed_test_ct1 partition by tbname ptn_alias session(ts, 15s);')
        ctb_name = self.tdCom.get_long_name()
        self.tdCom.create_ctable(stbname=self.stb_name, ctbname=ctb_name)
        self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=self.date_time, need_null=True)
        self.tdSql.query(f'select * from information_schema.ins_tables where table_name = "{exceed_child_tbname[:-1]}"')
        sleep_step = 0
        while self.tdSql.query_row != 1:
            self.tdSql.query(f'select * from information_schema.ins_tables where table_name = "{exceed_child_tbname[:-1]}"')
            if sleep_step < 5:
                sleep_step += 1
                time.sleep(1)
            else:
                return
        self.tdSql.checkEqual(self.tdSql.query_row, 1)

    def at_once_session(self, session, ignore_expired=None, ignore_update=None, partition="tbname", delete=False, fill_history_value=None, case_when=None, subtable=True):
        self.delete = delete
        self.case_name = sys._getframe().f_code.co_name
        self.prepare_data(session=session, fill_history_value=fill_history_value)
        exceed_child_tbname = self.tdCom.get_long_name(self.tdCom.Boundary.CHILD_TBNAME_MAX_LENGTH + 1)
        if partition == "tbname":
            if case_when:
                stream_case_when_partition = case_when
            else:
                stream_case_when_partition = self.partition_tbname_alias
            partition_elm_alias = self.partition_tbname_alias
        elif partition == "c1":
            partition_elm_alias = self.partition_col_alias
        elif partition == "abs(c1)":
            if subtable:
                partition_elm_alias = self.partition_expression_alias
            else:
                partition_elm_alias = "constant"
        else:
            partition_elm_alias = self.partition_tag_alias
        if partition == "tbname" or subtable is None:
            if case_when:
                stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", {stream_case_when_partition}), "{self.subtable_suffix}")' if self.subtable else None
                ctb_subtable_value = f'concat(concat("{self.ctb_name}_{self.subtable_prefix}", {stream_case_when_partition}), "{self.subtable_suffix}")' if self.subtable else None
                tb_subtable_value = f'concat(concat("{self.tb_name}_{self.subtable_prefix}", {stream_case_when_partition}), "{self.subtable_suffix}")' if self.subtable else None
            else:
                if subtable:
                    stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", {partition_elm_alias}), "{self.subtable_suffix}")' if self.subtable else None
                    ctb_subtable_value = f'concat(concat("{self.ctb_name}_{self.subtable_prefix}", {partition_elm_alias}), "{self.subtable_suffix}")' if self.subtable else None
                    tb_subtable_value = f'concat(concat("{self.tb_name}_{self.subtable_prefix}", {partition_elm_alias}), "{self.subtable_suffix}")' if self.subtable else None
                else:
                    stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", "{partition_elm_alias}"), "{self.subtable_suffix}")' if self.subtable else None
                    ctb_subtable_value = f'concat(concat("{self.ctb_name}_{self.subtable_prefix}", "{partition_elm_alias}"), "{self.subtable_suffix}")' if self.subtable else None
                    tb_subtable_value = f'concat(concat("{self.tb_name}_{self.subtable_prefix}", "{partition_elm_alias}"), "{self.subtable_suffix}")' if self.subtable else None
        else:
            stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", cast({partition_elm_alias} as binary(20))), "{self.subtable_suffix}")' if self.subtable else None
            if 'abs' in partition:
                stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", cast(abs(cast({partition_elm_alias} as int)) as binary(20))), "{self.subtable_suffix}")' if self.subtable else None
                ctb_subtable_value = f'concat(concat("{self.ctb_name}_{self.subtable_prefix}", cast(cast(abs(cast({partition_elm_alias} as int)) as bigint) as varchar(20))), "{self.subtable_suffix}")' if self.subtable else None
                # TODO confirm
                tb_subtable_value = f'concat(concat("{self.tb_name}_{self.subtable_prefix}", cast(cast(abs(cast({partition_elm_alias} as int)) as bigint) as varchar(20))), "{self.subtable_suffix}")' if self.subtable else None

            else:
                stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", cast({partition_elm_alias} as binary(20))), "{self.subtable_suffix}")' if self.subtable else None
                ctb_subtable_value = f'concat(concat("{self.ctb_name}_{self.subtable_prefix}", cast(cast({partition_elm_alias} as bigint) as varchar(20))), "{self.subtable_suffix}")' if self.subtable else None
                # TODO confirm
                tb_subtable_value = f'concat(concat("{self.tb_name}_{self.subtable_prefix}", cast(cast({partition_elm_alias} as bigint) as varchar(20))), "{self.subtable_suffix}")' if self.subtable else None


        self.tdCom.write_latency(self.case_name)
        # create stb/ctb/tb stream
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.stb_source_select_str} from {self.ctb_name} partition by {partition} {partition_elm_alias} session(ts, {self.dataDict["session"]}s)', trigger_mode="at_once", ignore_expired=ignore_expired, ignore_update=ignore_update, subtable_value=ctb_subtable_value, fill_history_value=fill_history_value)
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.tb_source_select_str} from {self.tb_name} partition by {partition} {partition_elm_alias} session(ts, {self.dataDict["session"]}s)', trigger_mode="at_once", ignore_expired=ignore_expired, ignore_update=ignore_update, subtable_value=tb_subtable_value, fill_history_value=fill_history_value)
        for i in range(self.range_count):
            ctb_name = self.tdCom.get_long_name()
            self.tdCom.create_ctable(stbname=self.stb_name, ctbname=ctb_name)

            if i == 0:
                window_close_ts = self.cal_watermark_window_close_session_endts(self.date_time, session=session)
            else:
                self.date_time = window_close_ts + 1
                window_close_ts = self.cal_watermark_window_close_session_endts(self.date_time, session=session)
            if i == 0:
                record_window_close_ts = window_close_ts
            for ts_value in [self.date_time, window_close_ts]:
                # self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=ts_value, need_null=True)
                # if self.update and i%2 == 0:
                #     self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=ts_value, need_null=True)
                # if self.delete and i%2 != 0:
                #     dt = f'cast({self.date_time-1} as timestamp)'
                #     self.tdCom.delete_rows(tbname=self.ctb_name, start_ts=dt)
                # ts_value += 1
                self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=ts_value, need_null=True)
                self.tdCom.insert_rows(tbname=self.tb_name, ts_value=ts_value, need_null=True)
                if self.update and i%2 == 0:
                    self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=ts_value, need_null=True)
                    self.tdCom.insert_rows(tbname=self.tb_name, ts_value=ts_value, need_null=True)
                if self.delete and i%2 != 0:
                    dt = f'cast({self.date_time-1} as timestamp)'
                    self.tdCom.delete_rows(tbname=self.ctb_name, start_ts=dt)
                    self.tdCom.delete_rows(tbname=self.tb_name, start_ts=dt)
                ts_value += 1
                # self.tdCom.insert_rows(tbname=ctb_name, ts_value=ts_value, need_null=True)
                # if self.update and i%2 == 0:
                #     self.tdCom.insert_rows(tbname=ctb_name, ts_value=ts_value, need_null=True)
                # if self.delete and i%2 != 0:
                #     dt = f'cast({self.date_time-1} as timestamp)'
                #     self.tdCom.delete_rows(tbname=ctb_name, start_ts=dt)
                # ts_value += 1

            # check result
            if partition != "tbname":
                for colname in self.partition_by_downsampling_function_list:
                    if "first" not in colname and "last" not in colname:
                        self.tdCom.check_query_data(f'select wstart, {self.tb_output_select_str} from {self.ctb_stream_des_table} order by wstart, `min(c1)`,`max(c2)`,`sum(c3)`;', f'select _wstart AS wstart, {self.tb_source_select_str} from {self.ctb_name} partition by {partition} session(ts, {self.dataDict["session"]}s) order by wstart, `min(c1)`,`max(c2)`,`sum(c3)`;', sorted=True)
                        self.tdCom.check_query_data(f'select wstart, {self.tb_output_select_str} from {self.tb_stream_des_table} order by wstart, `min(c1)`,`max(c2)`,`sum(c3)`;', f'select _wstart AS wstart, {self.tb_source_select_str} from {self.tb_name} partition by {partition} session(ts, {self.dataDict["session"]}s) order by wstart, `min(c1)`,`max(c2)`,`sum(c3)`;')
            else:
                for tbname in [self.tb_name]:
                    if tbname != self.tb_name:
                        self.tdCom.check_query_data(f'select wstart, {self.stb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS wstart, {self.stb_source_select_str}  from {tbname} partition by {partition} session(ts, {self.dataDict["session"]}s)', sorted=True)
                    else:
                        self.tdCom.check_query_data(f'select wstart, {self.tb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS wstart, {self.tb_source_select_str}  from {tbname} partition by {partition} session(ts, {self.dataDict["session"]}s)', sorted=True)

        if self.disorder:
            if ignore_expired:
                for tbname in [self.ctb_name, self.tb_name]:
                    if tbname != self.tb_name:
                        self.tdSql.query(f'select wstart, {self.stb_output_select_str} from {tbname}{self.des_table_suffix}')
                        res2 = self.tdSql.query_data
                        self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=str(self.date_time)+f'-{self.default_interval*(self.range_count+session)}s')
                        self.tdSql.query(f'select _wstart AS wstart, {self.stb_source_select_str}  from {tbname} session(ts, {self.dataDict["session"]}s)')
                        res1 = self.tdSql.query_data
                        self.tdSql.checkNotEqual(res1, res2)
                        self.tdSql.query(f'select wstart, {self.stb_output_select_str} from {tbname}{self.des_table_suffix}')
                        res1 = self.tdSql.query_data
                        self.tdSql.checkEqual(res1, res2)
                    else:
                        self.tdSql.query(f'select wstart, {self.tb_output_select_str} from {tbname}{self.des_table_suffix}')
                        res2 = self.tdSql.query_data
                        self.tdCom.insert_rows(tbname=self.tb_name, ts_value=str(self.date_time)+f'-{self.default_interval*(self.range_count+session)}s')
                        self.tdSql.query(f'select _wstart AS wstart, {self.tb_source_select_str}  from {tbname} session(ts, {self.dataDict["session"]}s)')
                        res1 = self.tdSql.query_data
                        self.tdSql.checkNotEqual(res1, res2)
                        self.tdSql.query(f'select wstart, {self.tb_output_select_str} from {tbname}{self.des_table_suffix}')
                        res1 = self.tdSql.query_data
                        self.tdSql.checkEqual(res1, res2)
            else:
                if ignore_update:
                    for tbname in [self.ctb_name, self.tb_name]:
                        if tbname != self.tb_name:
                            self.tdSql.query(f'select wstart, {self.stb_output_select_str} from {tbname}{self.des_table_suffix}')
                            res2 = self.tdSql.query_data
                            self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=record_window_close_ts)
                            self.tdSql.query(f'select _wstart AS wstart, {self.stb_source_select_str}  from {tbname} session(ts, {self.dataDict["session"]}s)')
                            res1 = self.tdSql.query_data
                            self.tdSql.checkNotEqual(res1, res2)
                            # self.tdSql.query(f'select wstart, {self.stb_output_select_str} from {tbname}{self.des_table_suffix}')
                            # res1 = self.tdSql.query_data
                            # self.tdSql.checkEqual(res1[1][8] - res2[1][8], 1)
                        else:
                            self.tdSql.query(f'select wstart, {self.tb_output_select_str} from {tbname}{self.des_table_suffix}')
                            res2 = self.tdSql.query_data
                            self.tdCom.insert_rows(tbname=self.tb_name, ts_value=record_window_close_ts)
                            self.tdSql.query(f'select _wstart AS wstart, {self.tb_source_select_str}  from {tbname} session(ts, {self.dataDict["session"]}s)')
                            res1 = self.tdSql.query_data
                            self.tdSql.checkNotEqual(res1, res2)
                            # self.tdSql.query(f'select wstart, {self.tb_output_select_str} from {tbname}{self.des_table_suffix}')
                            # res1 = self.tdSql.query_data
                            # self.tdSql.checkEqual(res1[1][8] - res2[1][8], 1)
                else:
                    self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=record_window_close_ts)
                    self.tdCom.insert_rows(tbname=self.tb_name, ts_value=record_window_close_ts)
                    if partition != "tbname":
                        for colname in self.partition_by_downsampling_function_list:
                            if "first" not in colname and "last" not in colname:
                                self.tdCom.check_query_data(f'select wstart, {self.tb_output_select_str} from {self.ctb_stream_des_table} order by wstart, `min(c1)`,`max(c2)`,`sum(c3)`;', f'select _wstart AS wstart, {self.tb_source_select_str} from {self.ctb_name} partition by {partition} session(ts, {self.dataDict["session"]}s) order by wstart, `min(c1)`,`max(c2)`,`sum(c3)`;', sorted=True)
                                self.tdCom.check_query_data(f'select wstart, {self.tb_output_select_str} from {self.tb_stream_des_table} order by wstart, `min(c1)`,`max(c2)`,`sum(c3)`;', f'select _wstart AS wstart, {self.tb_source_select_str} from {self.tb_name} partition by {partition} session(ts, {self.dataDict["session"]}s) order by wstart, `min(c1)`,`max(c2)`,`sum(c3)`;')
                    else:
                        for tbname in [self.tb_name]:
                            if tbname != self.tb_name:
                                self.tdCom.check_query_data(f'select wstart, {self.stb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS wstart, {self.stb_source_select_str}  from {tbname} partition by {partition} session(ts, {self.dataDict["session"]}s)', sorted=True)
                            else:
                                self.tdCom.check_query_data(f'select wstart, {self.tb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS wstart, {self.tb_source_select_str}  from {tbname} partition by {partition} session(ts, {self.dataDict["session"]}s)', sorted=True)

        if fill_history_value:
            self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=self.record_history_ts)
            self.tdCom.insert_rows(tbname=self.tb_name, ts_value=self.record_history_ts)
            if self.delete:
                self.tdCom.delete_rows(tbname=self.ctb_name, start_ts=self.tdCom.time_cast(self.record_history_ts, "-"))
                self.tdCom.delete_rows(tbname=self.tb_name, start_ts=self.tdCom.time_cast(self.record_history_ts, "-"))

        if self.subtable:
            # self.tdSql.query(f'select count(*) from {self.stb_name}_{self.subtable_prefix}{self.ctb_name}{self.subtable_suffix};')
            # self.tdSql.checkEqual(self.tdSql.query_data[0][0] > 0, True)
            self.tdSql.query(f'select * from {self.ctb_name}')
            ptn_counter = 0
            for c1_value in self.tdSql.query_data:
                if c1_value[1] is not None:
                    if partition == "c1":
                        self.tdSql.query(f'select count(*) from `{self.ctb_name}_{self.subtable_prefix}{c1_value[1]}{self.subtable_suffix}`;')
                    elif partition == "abs(c1)":
                        if subtable:
                            abs_c1_value = abs(c1_value[1])
                            self.tdSql.query(f'select count(*) from `{self.ctb_name}_{self.subtable_prefix}{abs_c1_value}{self.subtable_suffix}`;')
                        else:
                            self.tdSql.query(f'select count(*) from `{self.ctb_name}_{self.subtable_prefix}{partition_elm_alias}{self.subtable_suffix}`;')
                    elif partition == "tbname" and ptn_counter == 0:
                        self.tdSql.query(f'select count(*) from `{self.ctb_name}_{self.subtable_prefix}{self.ctb_name}{self.subtable_suffix}`;')
                        ptn_counter += 1
            # self.tdSql.query(f'select count(*) from {self.ctb_name}_{self.subtable_prefix}{self.ctb_name}{self.subtable_suffix};')
                    self.tdSql.checkEqual(self.tdSql.query_data[0][0] > 0, True)

            self.tdSql.query(f'select * from {self.tb_name}')
            ptn_counter = 0
            for c1_value in self.tdSql.query_data:
                if c1_value[1] is not None:
                    if partition == "c1":
                        self.tdSql.query(f'select count(*) from `{self.tb_name}_{self.subtable_prefix}{c1_value[1]}{self.subtable_suffix}`;')
                    elif partition == "abs(c1)":
                        if subtable:
                            abs_c1_value = abs(c1_value[1])
                            self.tdSql.query(f'select count(*) from `{self.tb_name}_{self.subtable_prefix}{abs_c1_value}{self.subtable_suffix}`;')
                        else:
                            self.tdSql.query(f'select count(*) from `{self.tb_name}_{self.subtable_prefix}{partition_elm_alias}{self.subtable_suffix}`;')
                    elif partition == "tbname" and ptn_counter == 0:
                        self.tdSql.query(f'select count(*) from `{self.tb_name}_{self.subtable_prefix}{self.tb_name}{self.subtable_suffix}`;')
                        ptn_counter += 1

                    self.tdSql.checkEqual(self.tdSql.query_data[0][0] > 0, True)

    def at_once_session_ext(self, session, ignore_expired=None, partition="tbname", delete=False, fill_history_value=None, case_when=None, subtable=None, stb_field_name_value=None, tag_value=None, use_exist_stb=False):
        if stb_field_name_value == self.partitial_stb_filter_des_select_elm or stb_field_name_value == self.exchange_stb_filter_des_select_elm:
            partitial_tb_source_str = self.partitial_ext_tb_source_select_str
        else:
            partitial_tb_source_str = self.ext_tb_source_select_str
        if not stb_field_name_value:
            stb_field_name_value = self.tb_filter_des_select_elm
        self.delete = delete
        self.case_name = sys._getframe().f_code.co_name
        defined_tag_count = len(tag_value.split())
        self.prepare_data(session=session, fill_history_value=fill_history_value)
        exceed_child_tbname = self.tdCom.get_long_name(self.tdCom.Boundary.CHILD_TBNAME_MAX_LENGTH + 1)
        if partition == "tbname":
            if case_when:
                stream_case_when_partition = case_when
            else:
                stream_case_when_partition = self.partition_tbname_alias
            partition_elm_alias = self.partition_tbname_alias
        elif partition == "c1":
            partition_elm_alias = self.partition_col_alias
        elif partition == "abs(c1)":
            partition_elm_alias = self.partition_expression_alias
        else:
            partition_elm_alias = self.partition_tag_alias

        if partition == "tbname":
            if case_when:
                stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", {stream_case_when_partition}), "{self.subtable_suffix}")' if self.subtable else None
                ctb_subtable_value = f'concat(concat("{self.ctb_name}_{self.subtable_prefix}", {stream_case_when_partition}), "{self.subtable_suffix}")' if self.subtable else None
                tb_subtable_value = f'concat(concat("{self.tb_name}_{self.subtable_prefix}", {stream_case_when_partition}), "{self.subtable_suffix}")' if self.subtable else None
            else:
                stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", {partition_elm_alias}), "{self.subtable_suffix}")' if self.subtable else None
                ctb_subtable_value = f'concat(concat("{self.ctb_name}_{self.subtable_prefix}", {partition_elm_alias}), "{self.subtable_suffix}")' if self.subtable else None
                tb_subtable_value = f'concat(concat("{self.tb_name}_{self.subtable_prefix}", {partition_elm_alias}), "{self.subtable_suffix}")' if self.subtable else None
        else:
            stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", cast({partition_elm_alias} as binary(20))), "{self.subtable_suffix}")' if self.subtable else None
            if 'abs' in partition:
                stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", cast(abs(cast({partition_elm_alias} as int)) as binary(20))), "{self.subtable_suffix}")' if self.subtable else None
                ctb_subtable_value = f'concat(concat("{self.ctb_name}_{self.subtable_prefix}", cast(cast(abs(cast({partition_elm_alias} as int)) as bigint) as varchar(20))), "{self.subtable_suffix}")' if self.subtable else None
                # TODO confirm
                tb_subtable_value = f'concat(concat("{self.tb_name}_{self.subtable_prefix}", cast(cast(abs(cast({partition_elm_alias} as int)) as bigint) as varchar(20))), "{self.subtable_suffix}")' if self.subtable else None

            else:
                stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", cast({partition_elm_alias} as binary(20))), "{self.subtable_suffix}")' if self.subtable else None
                ctb_subtable_value = f'concat(concat("{self.ctb_name}_{self.subtable_prefix}", cast(cast({partition_elm_alias} as bigint) as varchar(20))), "{self.subtable_suffix}")' if self.subtable else None
                # TODO confirm
                tb_subtable_value = f'concat(concat("{self.tb_name}_{self.subtable_prefix}", cast(cast({partition_elm_alias} as bigint) as varchar(20))), "{self.subtable_suffix}")' if self.subtable else None

        if subtable:
            if partition == "tbname":
                if case_when:
                    stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", {stream_case_when_partition}), "{self.subtable_suffix}")' if self.subtable else None
                else:
                    stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", {partition_elm_alias}), "{self.subtable_suffix}")' if self.subtable else None
            else:
                stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", cast(cast(abs(cast({subtable} as int)) as bigint) as varchar(100))), "{self.subtable_suffix}")' if self.subtable else None
        else:
            stb_subtable_value = None

        self.tdCom.write_latency(self.case_name)
        # create stb/ctb/tb stream
        self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.ext_stb_stream_des_table, source_sql=f'select _wstart AS wstart, {partitial_tb_source_str} from {self.stb_name} partition by {partition} session(ts, {self.dataDict["session"]}s)', trigger_mode="at_once", subtable_value=stb_subtable_value, fill_history_value=fill_history_value, stb_field_name_value=stb_field_name_value, tag_value=tag_value, use_exist_stb=use_exist_stb)
        for i in range(self.range_count):
            tag_value_list = list()
            ctb_name = self.tdCom.get_long_name()
            self.tdCom.create_ctable(stbname=self.stb_name, ctbname=ctb_name)

            if i == 0:
                window_close_ts = self.cal_watermark_window_close_session_endts(self.date_time, session=session)
            else:
                self.date_time = window_close_ts + 1
                window_close_ts = self.cal_watermark_window_close_session_endts(self.date_time, session=session)
            if i == 0:
                record_window_close_ts = window_close_ts
            for ts_value in [self.date_time, window_close_ts]:
                self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=ts_value, need_null=True)
                if self.update and i%2 == 0:
                    self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=ts_value, need_null=True)
                if self.delete and i%2 != 0:
                    dt = f'cast({self.date_time-1} as timestamp)'
                    self.tdCom.delete_rows(tbname=self.ctb_name, start_ts=dt)
                ts_value += 1
            if tag_value:
                self.tdSql.query(f'select {tag_value} from {self.stb_name}')
                tag_value_list = self.tdSql.query_data
            # check result
            for colname in self.partition_by_downsampling_function_list:
                if "first" not in colname and "last" not in colname:
                    self.tdCom.check_query_data(f'select {self.stb_filter_des_select_elm} from ext_{self.stb_name}{self.des_table_suffix} order by ts;', f'select _wstart AS wstart, {self.stb_source_select_str} from {self.stb_name} partition by {partition} session(ts, {self.dataDict["session"]}s) order by wstart;', sorted=True, defined_tag_count=defined_tag_count, tag_value_list=tag_value_list)

        if self.disorder:
            self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=record_window_close_ts)
            if ignore_expired:
                self.tdCom.check_query_data(f'select {self.stb_filter_des_select_elm} from ext_{self.stb_name}{self.des_table_suffix} order by ts;', f'select _wstart AS wstart, {self.stb_source_select_str} from {self.stb_name} partition by {partition} session(ts, {self.dataDict["session"]}s) order by wstart;', sorted=True, defined_tag_count=defined_tag_count, tag_value_list=tag_value_list)
                    # self.tdSql.query(f'select wstart, {self.stb_output_select_str} from {tbname}{self.des_table_suffix}')
                    # res1 = self.tdSql.query_data
                    # self.tdSql.query(f'select _wstart AS wstart, {self.stb_source_select_str}  from {tbname} session(ts, {self.dataDict["session"]}s)')
                    # res2 = self.tdSql.query_data
                    # self.tdSql.checkNotEqual(res1, res2)
            # else:
            #     for tbname in [self.ctb_name, self.tb_name]:
            #         if tbname != self.tb_name:
            #             self.tdCom.check_query_data(f'select wstart, {self.stb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS wstart, {self.stb_source_select_str}  from {tbname} session(ts, {self.dataDict["session"]}s)')
            #         else:
            #             self.tdCom.check_query_data(f'select wstart, {self.tb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS wstart, {self.tb_source_select_str}  from {tbname} session(ts, {self.dataDict["session"]}s)')
        if fill_history_value:
            self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=self.record_history_ts)
            if self.delete:
                self.tdCom.delete_rows(tbname=self.ctb_name, start_ts=self.tdCom.time_cast(self.record_history_ts, "-"))

        if self.subtable:
            self.tdSql.query(f'select * from {self.ctb_name}')
            ptn_counter = 0
            for c1_value in self.tdSql.query_data:
                if c1_value[1] is not None:
                    if partition == "c1":
                        self.tdSql.query(f'select count(*) from `{self.ctb_name}_{self.subtable_prefix}{c1_value[1]}{self.subtable_suffix}`;')
                    elif partition == "abs(c1)":
                        abs_c1_value = abs(c1_value[1])
                        self.tdSql.query(f'select count(*) from `{self.ctb_name}_{self.subtable_prefix}{abs_c1_value}{self.subtable_suffix}`;')
                    elif partition == "tbname" and ptn_counter == 0:
                        self.tdSql.query(f'select count(*) from `{self.ctb_name}_{self.subtable_prefix}{self.ctb_name}{self.subtable_suffix}`;')
                        ptn_counter += 1
                    else:
                        self.tdSql.query(f'select count(*) from `{self.stb_name}_{self.subtable_prefix}{abs(c1_value[1])}{self.subtable_suffix}`;')
                    self.tdSql.checkEqual(self.tdSql.query_data[0][0] > 0, True)

    def window_close_interval(self, interval, watermark=None, ignore_expired=None, partition="tbname", fill_value=None, delete=False):
        self.delete = delete
        self.case_name = sys._getframe().f_code.co_name
        if watermark is not None:
            self.case_name = "watermark" + sys._getframe().f_code.co_name
        self.prepare_data(interval=interval, watermark=watermark)

        if partition == "tbname":
            partition_elm_alias = self.partition_tbname_alias
        elif partition == "c1":
            partition_elm_alias = self.partition_col_alias
        elif partition == "abs(c1)":
            partition_elm_alias = self.partition_expression_alias
        else:
            partition_elm_alias = self.partition_tag_alias
        if partition == "tbname":
            stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", {partition_elm_alias}), "{self.subtable_suffix}")' if self.subtable else None
            ctb_subtable_value = f'concat(concat("{self.ctb_name}_{self.subtable_prefix}", {partition_elm_alias}), "{self.subtable_suffix}")' if self.subtable else None
            tb_subtable_value = f'concat(concat("{self.tb_name}_{self.subtable_prefix}", {partition_elm_alias}), "{self.subtable_suffix}")' if self.subtable else None
        else:
            stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", cast({partition_elm_alias} as varchar(20))), "{self.subtable_suffix}")' if self.subtable else None
            ctb_subtable_value = f'concat(concat("{self.ctb_name}_{self.subtable_prefix}", cast({partition_elm_alias} as varchar(20))), "{self.subtable_suffix}")' if self.subtable else None
            tb_subtable_value = f'concat(concat("{self.tb_name}_{self.subtable_prefix}", cast({partition_elm_alias} as varchar(20))), "{self.subtable_suffix}")' if self.subtable else None


        self.tdCom.write_latency(self.case_name)
        if watermark is not None:
            watermark_value = f'{self.dataDict["watermark"]}s'
        else:
            watermark_value = None
        # create stb/ctb/tb stream
        if fill_value:
            if "value" in fill_value.lower():
                fill_value='VALUE,1,2,3,4,5,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11'
        self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.stb_source_select_str}  from {self.stb_name} partition by {partition} {partition_elm_alias} interval({self.dataDict["interval"]}s)', trigger_mode="window_close", watermark=watermark_value, ignore_expired=ignore_expired, subtable_value=stb_subtable_value, fill_value=fill_value)
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.stb_source_select_str}  from {self.ctb_name} partition by {partition} {partition_elm_alias} interval({self.dataDict["interval"]}s)', trigger_mode="window_close", watermark=watermark_value, ignore_expired=ignore_expired, subtable_value=ctb_subtable_value, fill_value=fill_value)
        if fill_value:
            if "value" in fill_value.lower():
                fill_value='VALUE,1,2,3,4,5,6,7,8,9,10,11'
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.tb_source_select_str}  from {self.tb_name} partition by {partition} {partition_elm_alias} interval({self.dataDict["interval"]}s)', trigger_mode="window_close", watermark=watermark_value, ignore_expired=ignore_expired, subtable_value=tb_subtable_value, fill_value=fill_value)

        start_time = self.date_time
        for i in range(self.range_count):
            if i == 0:
                if watermark is not None:
                    window_close_ts = self.cal_watermark_window_close_interval_endts(self.date_time, self.dataDict['interval'], self.dataDict['watermark'])
                else:
                    window_close_ts = self.cal_watermark_window_close_interval_endts(self.date_time, self.dataDict['interval'])
            else:
                self.date_time = window_close_ts + self.offset
                window_close_ts += self.dataDict['interval']*self.offset
            if i == 0:
                record_window_close_ts = window_close_ts
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
                if not fill_value:
                    for tbname in [self.stb_stream_des_table, self.ctb_stream_des_table, self.tb_stream_des_table]:
                        if tbname != self.tb_stream_des_table:
                            self.tdSql.query(f'select wstart, {self.stb_output_select_str} from {tbname}')
                        else:
                            self.tdSql.query(f'select wstart, {self.tb_output_select_str} from {tbname}')
                        self.tdSql.checkEqual(self.tdSql.query_row, i)

            self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=window_close_ts-1)
            self.tdCom.insert_rows(tbname=self.tb_name, ts_value=window_close_ts-1)
            if self.update and i%2 == 0:
                self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=window_close_ts-1)
                self.tdCom.insert_rows(tbname=self.tb_name, ts_value=window_close_ts-1)
            if not fill_value:
                for tbname in [self.stb_stream_des_table, self.ctb_stream_des_table, self.tb_stream_des_table]:
                    if tbname != self.tb_stream_des_table:
                        self.tdSql.query(f'select wstart, {self.stb_output_select_str} from {tbname}')
                    else:
                        self.tdSql.query(f'select wstart, {self.tb_output_select_str} from {tbname}')

                    self.tdSql.checkEqual(self.tdSql.query_row, i)

            self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=window_close_ts)
            self.tdCom.insert_rows(tbname=self.tb_name, ts_value=window_close_ts)
            if self.update and i%2 == 0:
                self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=window_close_ts)
                self.tdCom.insert_rows(tbname=self.tb_name, ts_value=window_close_ts)

            # for tbname in [stb_stream_des_table, ctb_stream_des_table, tb_stream_des_table]:
            if not fill_value:
                for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
                    if tbname != self.tb_name:
                        self.tdCom.check_stream(f'select wstart, {self.stb_output_select_str} from {tbname}{self.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.stb_source_select_str}  from {tbname}  partition by {partition} interval({self.dataDict["interval"]}s) order by wstart limit {i+1}', i+1)
                    else:
                        self.tdCom.check_stream(f'select wstart, {self.tb_output_select_str} from {tbname}{self.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.tb_source_select_str}  from {tbname}  partition by {partition} interval({self.dataDict["interval"]}s) order by wstart limit {i+1}', i+1)
        if self.disorder and not fill_value:
            self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=record_window_close_ts)
            self.tdCom.insert_rows(tbname=self.tb_name, ts_value=record_window_close_ts)
            if ignore_expired:
                for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
                    if tbname != self.tb_name:
                        self.tdSql.query(f'select wstart, {self.stb_output_select_str} from {tbname}{self.des_table_suffix}')
                        res1 = self.tdSql.query_data
                        self.tdSql.query(f'select _wstart AS wstart, {self.stb_source_select_str}  from {tbname} interval({self.dataDict["interval"]}s) limit {i+1}')
                        res2 = self.tdSql.query_data
                        self.tdSql.checkNotEqual(res1, res2)
                    else:
                        self.tdSql.query(f'select wstart, {self.tb_output_select_str} from {tbname}{self.des_table_suffix}')
                        res1 = self.tdSql.query_data
                        self.tdSql.query(f'select _wstart AS wstart, {self.tb_source_select_str}  from {tbname} interval({self.dataDict["interval"]}s) limit {i+1}')
                        res2 = self.tdSql.query_data
                        self.tdSql.checkNotEqual(res1, res2)
            else:
                for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
                    if tbname != self.tb_name:
                        self.tdCom.check_stream(f'select wstart, {self.stb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS wstart, {self.stb_source_select_str}  from {tbname} interval({self.dataDict["interval"]}s) limit {i+1}', i+1)
                    else:
                        self.tdCom.check_stream(f'select wstart, {self.tb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS wstart, {self.tb_source_select_str}  from {tbname} interval({self.dataDict["interval"]}s) limit {i+1}', i+1)
        if self.subtable:
            # self.tdSql.query(f'select count(*) from {self.stb_name}_{self.subtable_prefix}{self.ctb_name}{self.subtable_suffix};')
            # self.tdSql.checkEqual(self.tdSql.query_data[0][0] > 0, True)
            self.tdSql.query(f'select * from {self.ctb_name}')
            for tname in [self.stb_name, self.ctb_name]:
                ptn_counter = 0
                for c1_value in self.tdSql.query_data:
                    if partition == "c1":
                        self.tdSql.query(f'select count(*) from `{tname}_{self.subtable_prefix}{c1_value[1]}{self.subtable_suffix}`;', count_expected_res=self.range_count)
                    elif partition == "abs(c1)":
                        abs_c1_value = abs(c1_value[1])
                        self.tdSql.query(f'select count(*) from `{tname}_{self.subtable_prefix}{abs_c1_value}{self.subtable_suffix}`;', count_expected_res=self.range_count)
                    elif partition == "tbname" and ptn_counter == 0:
                        self.tdSql.query(f'select count(*) from `{tname}_{self.subtable_prefix}{self.ctb_name}{self.subtable_suffix}`;', count_expected_res=self.range_count)
                        ptn_counter += 1
            # self.tdSql.query(f'select count(*) from {self.ctb_name}_{self.subtable_prefix}{self.ctb_name}{self.subtable_suffix};')
                    
                    self.tdSql.checkEqual(self.tdSql.query_data[0][0] , self.range_count)
                    self.tdSql.checkEqual(self.tdSql.query_data[0][0] > 0, True)

            self.tdSql.query(f'select * from {self.tb_name}')
            ptn_counter = 0
            for c1_value in self.tdSql.query_data:
                if partition == "c1":
                    self.tdSql.query(f'select count(*) from `{self.tb_name}_{self.subtable_prefix}{c1_value[1]}{self.subtable_suffix}`;')
                elif partition == "abs(c1)":
                    abs_c1_value = abs(c1_value[1])
                    self.tdSql.query(f'select count(*) from `{self.tb_name}_{self.subtable_prefix}{abs_c1_value}{self.subtable_suffix}`;')
                elif partition == "tbname" and ptn_counter == 0:
                    self.tdSql.query(f'select count(*) from `{self.tb_name}_{self.subtable_prefix}{self.tb_name}{self.subtable_suffix}`;')
                    ptn_counter += 1

                self.tdSql.checkEqual(self.tdSql.query_data[0][0] > 0, True)

        if fill_value:
            history_ts = str(start_time)+f'-{self.dataDict["interval"]*(self.range_count+2)}s'
            start_ts = self.tdCom.time_cast(history_ts, "-")
            future_ts = str(self.date_time)+f'+{self.dataDict["interval"]*(self.range_count+2)}s'
            end_ts = self.tdCom.time_cast(future_ts)
            self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=history_ts)
            self.tdCom.insert_rows(tbname=self.tb_name, ts_value=history_ts)
            self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=future_ts)
            self.tdCom.insert_rows(tbname=self.tb_name, ts_value=future_ts)
            future_ts_bigint = self.tdCom.str_ts_trans_bigint(future_ts)
            if watermark is not None:
                window_close_ts = self.cal_watermark_window_close_interval_endts(future_ts_bigint, self.dataDict['interval'], self.dataDict['watermark'])
            else:
                window_close_ts = self.cal_watermark_window_close_interval_endts(future_ts_bigint, self.dataDict['interval'])
            self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=window_close_ts)
            self.tdCom.insert_rows(tbname=self.tb_name, ts_value=window_close_ts)


            if self.update:
                for i in range(self.range_count):
                    if i == 0:
                        if watermark is not None:
                            window_close_ts = self.cal_watermark_window_close_interval_endts(self.date_time, self.dataDict['interval'], self.dataDict['watermark'])
                        else:
                            window_close_ts = self.cal_watermark_window_close_interval_endts(self.date_time, self.dataDict['interval'])
                    else:
                        self.date_time = window_close_ts + self.offset
                        window_close_ts += self.dataDict['interval']*self.offset
                    if i == 0:
                        record_window_close_ts = window_close_ts
                    for num in range(int(window_close_ts/self.offset-self.date_time/self.offset)):
                        ts_value=self.date_time+num*self.offset
                        self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=ts_value)
                        self.tdCom.insert_rows(tbname=self.tb_name, ts_value=ts_value)
                    self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=window_close_ts-1)
                    self.tdCom.insert_rows(tbname=self.tb_name, ts_value=window_close_ts-1)
                    self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=window_close_ts)
                    self.tdCom.insert_rows(tbname=self.tb_name, ts_value=window_close_ts)
            if self.delete:
                self.tdCom.delete_rows(tbname=self.ctb_name, start_ts=self.tdCom.time_cast(start_time), end_ts=self.tdCom.time_cast(window_close_ts))
                self.tdCom.delete_rows(tbname=self.tb_name, start_ts=self.tdCom.time_cast(start_time), end_ts=self.tdCom.time_cast(window_close_ts))
            self.date_time = start_time
            for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
                if tbname != self.tb_name:
                    if "value" in fill_value.lower():
                        fill_value='VALUE,1,2,3,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11'
                    if (fill_value == "NULL" or fill_value == "NEXT" or fill_value == "LINEAR") and self.delete:
                        self.tdCom.check_query_data(f'select wstart, {self.fill_stb_output_select_str} from {tbname}{self.des_table_suffix} order by wstart', f'select * from (select _wstart AS wstart, {self.fill_stb_source_select_str}  from {tbname} where ts >= {start_ts} and ts <= {end_ts}  partition by {partition} interval({self.dataDict["interval"]}s) fill ({fill_value}) order by wstart) where `min(c1)` is not Null', fill_value=fill_value)
                    else:
                        if self.delete and (fill_value == "PREV" or "value" in fill_value.lower()):
                            additional_options = f"where ts >= {start_ts}-1s and  ts <= {start_ts}"
                        else:
                            additional_options = f"where ts >= {start_ts} and ts <= {end_ts}"
                        self.tdCom.check_query_data(f'select wstart, {self.fill_stb_output_select_str} from {tbname}{self.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.fill_stb_source_select_str}  from {tbname} {additional_options}  partition by {partition} interval({self.dataDict["interval"]}s) fill ({fill_value}) order by wstart', fill_value=fill_value)
                else:
                    if "value" in fill_value.lower():
                        fill_value='VALUE,1,2,3,6,7,8,9,10,11'
                    if (fill_value == "NULL" or fill_value == "NEXT" or fill_value == "LINEAR") and self.delete:
                        self.tdCom.check_query_data(f'select wstart, {self.fill_tb_output_select_str} from {tbname}{self.des_table_suffix} order by wstart', f'select * from (select _wstart AS wstart, {self.fill_tb_source_select_str}  from {tbname} where ts >= {start_ts} and ts <= {end_ts}  partition by {partition} interval({self.dataDict["interval"]}s) fill ({fill_value}) order by wstart) where `min(c1)` is not Null', fill_value=fill_value)
                    else:
                        if self.delete and (fill_value == "PREV" or "value" in fill_value.lower()):
                            additional_options = f"where ts >= {start_ts}-1s and  ts <= {start_ts}"
                        else:
                            additional_options = f"where ts >= {start_ts} and ts <= {end_ts}"
                        self.tdCom.check_query_data(f'select wstart, {self.fill_tb_output_select_str} from {tbname}{self.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.fill_tb_source_select_str}  from {tbname} {additional_options}  partition by {partition} interval({self.dataDict["interval"]}s) fill ({fill_value}) order by wstart', fill_value=fill_value)



    def window_close_session(self, session):
        self.case_name = sys._getframe().f_code.co_name
        self.prepare_data(session=session)
        self.date_time = self.dataDict["start_ts"]

        self.tdCom.write_latency(self.case_name)
        # create stb/ctb/tb stream
        self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.stb_source_select_str}  from {self.stb_name} session(ts, {self.dataDict["session"]}s)', trigger_mode="window_close")
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.stb_source_select_str}  from {self.ctb_name} session(ts, {self.dataDict["session"]}s)', trigger_mode="window_close")
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.tb_source_select_str}  from {self.tb_name} session(ts, {self.dataDict["session"]}s)', trigger_mode="window_close")
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
                    self.tdCom.check_stream(f'select wstart, {self.stb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS wstart, {self.stb_source_select_str}  from {tbname} session(ts, {self.dataDict["session"]}s) limit {i+1}', i+1)
                else:
                    self.tdCom.check_stream(f'select wstart, {self.tb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS wstart, {self.tb_source_select_str}  from {tbname} session(ts, {self.dataDict["session"]}s) limit {i+1}', i+1)

    def window_close_state_window(self, state_window):
        self.case_name = sys._getframe().f_code.co_name
        self.prepare_data(state_window=state_window)
        state_window_col_name = self.dataDict["state_window"]
        self.tdCom.write_latency(self.case_name)
        # self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.stb_source_select_str}  from {self.stb_name} state_window({state_window_col_name})', trigger_mode="window_close")
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.stb_source_select_str}  from {self.ctb_name} state_window({state_window_col_name})', trigger_mode="window_close")
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.tb_source_select_str}  from {self.tb_name} state_window({state_window_col_name})', trigger_mode="window_close")
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
                    self.tdCom.check_stream(f'select wstart, {self.stb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS wstart, {self.stb_source_select_str}  from {tbname} state_window({state_window_col_name}) limit {i}', i)
                else:
                    self.tdCom.check_stream(f'select wstart, {self.tb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS wstart, {self.tb_source_select_str}  from {tbname} state_window({state_window_col_name}) limit {i}', i)


    def watermark_max_delay_interval_ext(self, interval, max_delay, watermark=None, fill_value=None, partition="tbname", delete=False, fill_history_value=None, interval_value=None, subtable=None, case_when=None, stb_field_name_value=None, tag_value=None, use_exist_stb=False):
        if stb_field_name_value == self.partitial_stb_filter_des_select_elm or stb_field_name_value == self.exchange_stb_filter_des_select_elm:
            partitial_tb_source_str = self.partitial_ext_tb_source_select_str
        else:
            partitial_tb_source_str = self.ext_tb_source_select_str
        if not stb_field_name_value:
            stb_field_name_value = self.tb_filter_des_select_elm
        self.delete = delete
        self.case_name = sys._getframe().f_code.co_name
        defined_tag_count = len(tag_value.split())
        if watermark is not None:
            self.case_name = "watermark" + sys._getframe().f_code.co_name
        self.prepare_data(interval=interval, watermark=watermark)
        if subtable:
            stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", cast(cast(abs(cast({subtable} as int)) as bigint) as varchar(100))), "{self.subtable_suffix}")' if self.subtable else None
        else:
            stb_subtable_value = None
        self.tdCom.write_latency(self.case_name)
        self.date_time = 1658921623245
        if watermark is not None:
            watermark_value = f'{self.dataDict["watermark"]}s'
            fill_watermark_value = watermark_value
        else:
            watermark_value = None
            fill_watermark_value = "0s"

        max_delay_value = f'{self.tdCom.trans_time_to_s(max_delay)}s'
        if fill_value:
            if "value" in fill_value.lower():
                fill_value='VALUE,1,2,3,4,5,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11'
        # create stb/ctb/tb stream
        self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.ext_stb_stream_des_table, subtable_value=stb_subtable_value, source_sql=f'select _wstart AS wstart, {partitial_tb_source_str}  from {self.stb_name} interval({self.dataDict["interval"]}s)', trigger_mode="max_delay", watermark=watermark_value, max_delay=max_delay_value, fill_value=fill_value, fill_history_value=fill_history_value, stb_field_name_value=stb_field_name_value, tag_value=tag_value, use_exist_stb=use_exist_stb)

        init_num = 0
        start_time = self.date_time
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
                if self.update and i%2 == 0:
                    self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=self.date_time+num*self.offset)

            self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=window_close_ts-1)
            if self.update and i%2 == 0:
                self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=window_close_ts-1)

            self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=window_close_ts)
            if self.update and i%2 == 0:
                self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=window_close_ts)

            if i == 0:
                init_num = 2 + i
                if watermark is not None:
                    init_num += 1
            else:
                init_num += 1
            time.sleep(int(max_delay.replace("s", "")))
            if tag_value:
                self.tdSql.query(f'select {tag_value} from {self.stb_name}')
                tag_value_list = self.tdSql.query_data
            if not fill_value:
                self.tdCom.check_query_data(f'select {self.stb_filter_des_select_elm} from ext_{self.stb_name}{self.des_table_suffix} order by ts;', f'select _wstart AS wstart, {self.stb_source_select_str}  from {self.stb_name} interval({self.dataDict["interval"]}s)', defined_tag_count=defined_tag_count, tag_value_list=tag_value_list, partition=partition)

    def watermark_max_delay_interval(self, interval, max_delay, watermark=None, fill_value=None, delete=False):
        self.delete = delete
        self.case_name = sys._getframe().f_code.co_name
        if watermark is not None:
            self.case_name = "watermark" + sys._getframe().f_code.co_name
        self.prepare_data(interval=interval, watermark=watermark)
        self.tdCom.write_latency(self.case_name)
        self.date_time = 1658921623245
        if watermark is not None:
            watermark_value = f'{self.dataDict["watermark"]}s'
            fill_watermark_value = watermark_value
        else:
            watermark_value = None
            fill_watermark_value = "0s"

        max_delay_value = f'{self.tdCom.trans_time_to_s(max_delay)}s'
        if fill_value:
            if "value" in fill_value.lower():
                fill_value='VALUE,1,2,3,4,5,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11'
        # create stb/ctb/tb stream
        self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.stb_source_select_str}  from {self.stb_name} interval({self.dataDict["interval"]}s)', trigger_mode="max_delay", watermark=watermark_value, max_delay=max_delay_value, fill_value=fill_value)
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.stb_source_select_str}  from {self.ctb_name} interval({self.dataDict["interval"]}s)', trigger_mode="max_delay", watermark=watermark_value, max_delay=max_delay_value, fill_value=fill_value)
        if fill_value:
            if "value" in fill_value.lower():
                fill_value='VALUE,1,2,3,4,5,6,7,8,9,10,11'
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.tb_source_select_str}  from {self.tb_name} interval({self.dataDict["interval"]}s)', trigger_mode="max_delay", watermark=watermark_value, max_delay=max_delay_value, fill_value=fill_value)
        init_num = 0
        start_time = self.date_time
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
                if not fill_value:
                    for tbname in [self.stb_stream_des_table, self.ctb_stream_des_table, self.tb_stream_des_table]:
                        if tbname != self.tb_stream_des_table:
                            self.tdSql.query(f'select wstart, {self.stb_output_select_str} from {tbname}')
                        else:
                            self.tdSql.query(f'select wstart, {self.tb_output_select_str} from {tbname}')
                        self.tdSql.checkEqual(self.tdSql.query_row, init_num)

            self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=window_close_ts-1)
            self.tdCom.insert_rows(tbname=self.tb_name, ts_value=window_close_ts-1)
            if self.update and i%2 == 0:
                self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=window_close_ts-1)
                self.tdCom.insert_rows(tbname=self.tb_name, ts_value=window_close_ts-1)
            # if not fill_value:
            #     for tbname in [self.stb_stream_des_table, self.ctb_stream_des_table, self.tb_stream_des_table]:
            #         if tbname != self.tb_stream_des_table:
            #             self.tdSql.query(f'select wstart, {self.stb_output_select_str} from {tbname}')
            #         else:
            #             self.tdSql.query(f'select wstart, {self.tb_output_select_str} from {tbname}')
            #         self.tdSql.checkEqual(self.tdSql.query_row, init_num)

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
            time.sleep(int(max_delay.replace("s", "")))
            if not fill_value:
                for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
                    if tbname != self.tb_name:
                        self.tdCom.check_query_data(f'select wstart, {self.stb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS wstart, {self.stb_source_select_str}  from {tbname} interval({self.dataDict["interval"]}s)')
                        # self.tdCom.check_stream(f'select wstart, {self.stb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS wstart, {self.stb_source_select_str}  from {tbname} interval({self.dataDict["interval"]}s)', init_num, max_delay)
                    else:
                        self.tdCom.check_query_data(f'select wstart, {self.tb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS wstart, {self.tb_source_select_str}  from {tbname} interval({self.dataDict["interval"]}s)')
                        # self.tdCom.check_stream(f'select wstart, {self.tb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS wstart, {self.tb_source_select_str}  from {tbname} interval({self.dataDict["interval"]}s)', init_num, max_delay)
        if fill_value:
            history_ts = str(start_time)+f'-{self.dataDict["interval"]*(self.range_count+2)}s'
            start_ts = self.tdCom.time_cast(history_ts, "-")
            future_ts = str(self.date_time)+f'+{self.dataDict["interval"]*(self.range_count+2)}s'
            end_ts = self.tdCom.time_cast(future_ts)
            self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=history_ts)
            self.tdCom.insert_rows(tbname=self.tb_name, ts_value=history_ts)
            self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=future_ts)
            self.tdCom.insert_rows(tbname=self.tb_name, ts_value=future_ts)
            future_ts_bigint = self.tdCom.str_ts_trans_bigint(future_ts)
            if watermark is not None:
                window_close_ts = self.cal_watermark_window_close_interval_endts(future_ts_bigint, self.dataDict['interval'], self.dataDict['watermark'])
            else:
                window_close_ts = self.cal_watermark_window_close_interval_endts(future_ts_bigint, self.dataDict['interval'])
            self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=window_close_ts)
            self.tdCom.insert_rows(tbname=self.tb_name, ts_value=window_close_ts)

            if self.update:
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

                    self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=window_close_ts-1)
                    self.tdCom.insert_rows(tbname=self.tb_name, ts_value=window_close_ts-1)
                    self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=window_close_ts)
                    self.tdCom.insert_rows(tbname=self.tb_name, ts_value=window_close_ts)
            if self.delete:
                self.tdCom.delete_rows(tbname=self.ctb_name, start_ts=self.tdCom.time_cast(start_time), end_ts=self.tdCom.time_cast(window_close_ts))
                self.tdCom.delete_rows(tbname=self.tb_name, start_ts=self.tdCom.time_cast(start_time), end_ts=self.tdCom.time_cast(window_close_ts))
            time.sleep(int(max_delay.replace("s", "")))
            for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
                if tbname != self.tb_name:
                    if "value" in fill_value.lower():
                        fill_value='VALUE,1,2,3,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11'
                    self.tdCom.check_query_data(f'select wstart, {self.fill_stb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS wstart, {self.fill_stb_source_select_str}  from {tbname}  where ts >= {start_ts} and ts <= {end_ts}+{self.dataDict["interval"]}s+{fill_watermark_value}  interval({self.dataDict["interval"]}s) fill ({fill_value})', fill_value=fill_value)
                else:
                    if "value" in fill_value.lower():
                        fill_value='VALUE,1,2,3,6,7,8,9,10,11'
                    self.tdCom.check_query_data(f'select wstart, {self.fill_tb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS wstart, {self.fill_tb_source_select_str}  from {tbname}  where ts >= {start_ts} and ts <= {end_ts}+{self.dataDict["interval"]}s+{fill_watermark_value}  interval({self.dataDict["interval"]}s) fill ({fill_value})', fill_value=fill_value)


    def watermark_window_close_session(self, session, watermark, fill_history_value=None):
        self.case_name = sys._getframe().f_code.co_name
        if watermark is not None:
            self.case_name = "watermark" + sys._getframe().f_code.co_name
        self.prepare_data(session=session, watermark=watermark, fill_history_value=fill_history_value)
        self.date_time = self.dataDict["start_ts"]
        self.tdCom.write_latency(self.case_name)
        if watermark is not None:
            watermark_value = f'{self.dataDict["watermark"]}s'
        else:
            watermark_value = None
        # create stb/ctb/tb stream
        # self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.stb_source_select_str}  from {self.stb_name} session(ts, {self.dataDict["session"]}s)', trigger_mode="window_close", watermark=watermark_value)
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.stb_source_select_str}  from {self.ctb_name} session(ts, {self.dataDict["session"]}s)', trigger_mode="window_close", watermark=watermark_value, fill_history_value=fill_history_value)
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.tb_source_select_str}  from {self.tb_name} session(ts, {self.dataDict["session"]}s)', trigger_mode="window_close", watermark=watermark_value, fill_history_value=fill_history_value)
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
                            self.tdSql.query(f'select wstart, {self.stb_output_select_str} from {tbname}')
                        else:
                            self.tdSql.query(f'select wstart, {self.tb_output_select_str} from {tbname}')
                        if not fill_history_value:
                            self.tdSql.checkEqual(self.tdSql.query_row, i)
            else:
                expected_value = i
            self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=window_close_ts)
            self.tdCom.insert_rows(tbname=self.tb_name, ts_value=window_close_ts)
            if self.update and i%2 == 0:
                self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=window_close_ts)
                self.tdCom.insert_rows(tbname=self.tb_name, ts_value=window_close_ts)

            if fill_history_value:
                self.update_delete_history_data()

            # for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
            if not fill_history_value:
                for tbname in [self.ctb_name, self.tb_name]:
                    if tbname != self.tb_name:
                        self.tdCom.check_stream(f'select wstart, {self.stb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS wstart, {self.stb_source_select_str}  from {tbname} session(ts, {self.dataDict["session"]}s) limit {expected_value}', expected_value)
                    else:
                        self.tdCom.check_stream(f'select wstart, {self.tb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS wstart, {self.tb_source_select_str}  from {tbname} session(ts, {self.dataDict["session"]}s) limit {expected_value}', expected_value)
            else:
                for tbname in [self.ctb_name, self.tb_name]:
                    if tbname != self.tb_name:
                        self.tdCom.check_query_data(f'select wstart, {self.stb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS wstart, {self.stb_source_select_str}  from {tbname} session(ts, {self.dataDict["session"]}s) limit {expected_value+1}')
                    else:
                        self.tdCom.check_query_data(f'select wstart, {self.tb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS wstart, {self.tb_source_select_str}  from {tbname} session(ts, {self.dataDict["session"]}s) limit {expected_value+1}')

    def watermark_window_close_session_ext(self, session, watermark, fill_history_value=None, partition=None, subtable=None, stb_field_name_value=None, tag_value=None, use_exist_stb=False):
        if stb_field_name_value == self.partitial_stb_filter_des_select_elm or stb_field_name_value == self.exchange_stb_filter_des_select_elm:
            partitial_tb_source_str = self.partitial_ext_tb_source_select_str
        else:
            partitial_tb_source_str = self.ext_tb_source_select_str
        if not stb_field_name_value:
            stb_field_name_value = self.tb_filter_des_select_elm
        self.case_name = sys._getframe().f_code.co_name
        defined_tag_count = len(tag_value.split())
        if watermark is not None:
            self.case_name = "watermark" + sys._getframe().f_code.co_name
        self.prepare_data(session=session, watermark=watermark, fill_history_value=fill_history_value)
        self.date_time = self.dataDict["start_ts"]
        self.tdCom.write_latency(self.case_name)
        if subtable:
            stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", cast(cast(abs(cast({subtable} as int)) as bigint) as varchar(100))), "{self.subtable_suffix}")' if self.subtable else None
        else:
            stb_subtable_value = None
        if watermark is not None:
            watermark_value = f'{self.dataDict["watermark"]}s'
        else:
            watermark_value = None
        # create stb/ctb/tb stream
        self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.ext_stb_stream_des_table, source_sql=f'select _wstart AS wstart, {partitial_tb_source_str} from {self.stb_name} session(ts, {self.dataDict["session"]}s)', trigger_mode="window_close", watermark=watermark_value, subtable_value=stb_subtable_value, fill_history_value=fill_history_value, stb_field_name_value=stb_field_name_value, tag_value=tag_value, use_exist_stb=use_exist_stb)
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
                    if self.update and i%2 == 0:
                        self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=ts_value)
            else:
                expected_value = i
            self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=window_close_ts)
            if self.update and i%2 == 0:
                self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=window_close_ts)

            if fill_history_value:
                self.update_delete_history_data()
            if tag_value:
                self.tdSql.query(f'select {tag_value} from {self.stb_name}')
                tag_value_list = self.tdSql.query_data
            self.tdCom.check_query_data(f'select {self.stb_filter_des_select_elm} from ext_{self.stb_name}{self.des_table_suffix} order by ts', f'select _wstart AS wstart, {self.stb_source_select_str}  from {self.stb_name} session(ts, {self.dataDict["session"]}s) order by wstart limit {expected_value};', sorted=True, defined_tag_count=defined_tag_count, tag_value_list=tag_value_list, partition=partition)

    def watermark_max_delay_session(self, session, watermark, max_delay, fill_history_value=None):
        self.case_name = sys._getframe().f_code.co_name
        if watermark is not None:
            self.case_name = "watermark" + sys._getframe().f_code.co_name
        self.prepare_data(session=session, watermark=watermark, fill_history_value=fill_history_value)
        self.date_time = self.dataDict["start_ts"]

        self.tdCom.write_latency(self.case_name)
        if watermark is not None:
            watermark_value = f'{self.dataDict["watermark"]}s'
        else:
            watermark_value = None
        max_delay_value = f'{self.tdCom.trans_time_to_s(max_delay)}s'
        # create stb/ctb/tb stream
        # self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.stb_source_select_str}  from {self.stb_name} session(ts, {self.dataDict["session"]}s)', trigger_mode="max_delay", watermark=watermark_value, max_delay=max_delay_value)
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.stb_source_select_str}  from {self.ctb_name} session(ts, {self.dataDict["session"]}s)', trigger_mode="max_delay", watermark=watermark_value, max_delay=max_delay_value, fill_history_value=fill_history_value)
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.tb_source_select_str}  from {self.tb_name} session(ts, {self.dataDict["session"]}s)', trigger_mode="max_delay", watermark=watermark_value, max_delay=max_delay_value, fill_history_value=fill_history_value)
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
                            self.tdSql.query(f'select wstart, {self.stb_output_select_str} from {tbname}')
                        else:
                            self.tdSql.query(f'select wstart, {self.tb_output_select_str} from {tbname}')
                        if not fill_history_value:
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
            if not fill_history_value:
                for tbname in [self.ctb_name, self.tb_name]:
                    if tbname != self.tb_name:
                        self.tdCom.check_stream(f'select wstart, {self.stb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS wstart, {self.stb_source_select_str}  from {tbname} session(ts, {self.dataDict["session"]}s)', expected_value, max_delay)
                    else:
                        self.tdCom.check_stream(f'select wstart, {self.tb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS wstart, {self.tb_source_select_str}  from {tbname} session(ts, {self.dataDict["session"]}s)', expected_value, max_delay)
            else:
                self.update_delete_history_data()
                for tbname in [self.ctb_name, self.tb_name]:
                    if tbname != self.tb_name:
                        self.tdCom.check_query_data(f'select wstart, {self.stb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS wstart, {self.stb_source_select_str}  from {tbname} session(ts, {self.dataDict["session"]}s)')
                    else:
                        self.tdCom.check_query_data(f'select wstart, {self.tb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS wstart, {self.tb_source_select_str}  from {tbname} session(ts, {self.dataDict["session"]}s)')

    def json_function(self, partition="tbname", delete=False, fill_history_value=None):
        self.delete = delete
        # self.prepare_stream_data()
        self.case_name = sys._getframe().f_code.co_name
        self.prepare_data()

        self.tdSql.execute('create table if not exists json_stb (ts timestamp, c1 int, c2 double, c3 binary(20), c4 binary(20), c5 nchar(20)) tags (t1 json);')
        self.tdSql.execute('create table json_ct1 using json_stb tags(\'{"abc": 1}\');')

        if fill_history_value is None:
            fill_history = ""
        else:
            fill_history = f'fill_history {fill_history_value}'
            for i in range(self.range_count):
                self.tdSql.execute(f'insert into json_ct1 values ({self.date_time}-{i}s, 100, -100.1, "hebei", Null, "Bigdata");')
        # self.tdSql.error(f'create stream stb_json_stream trigger at_once ignore expired 0 {fill_history} into output_json_stb as select ts, to_json("{{ c2 : 1 }}") from json_stb partition by {partition};')
        self.tdSql.execute(f'create stream stb_json_stream trigger at_once ignore expired 0 {fill_history} into output_json_stb as select ts, tags(to_json(\'{"t1":1}\')) from json_stb partition by {partition};')
        # for tbname in ["json_ct1"]:
        #     self.tdSql.execute(f'insert into {tbname} values ({self.date_time}, 100, 100.1, "beijing", "taos", "Taos");')
        #     self.tdSql.execute(f'insert into {tbname} values ({self.date_time}+1s, -50, -50.1, "tianjin", "taosdata", "Taosdata");')
        #     self.tdSql.execute(f'insert into {tbname} values ({self.date_time}+2s, 0, Null, "hebei", "TDengine", Null);')

    def scalar_function(self, partition="tbname", delete=False, fill_history_value=None):
        self.delete = delete
        # self.prepare_stream_data()
        self.case_name = sys._getframe().f_code.co_name
        self.prepare_data()

        self.tdSql.execute('create table if not exists scalar_stb (ts timestamp, c1 int, c2 double, c3 binary(20), c4 binary(20), c5 nchar(20)) tags (t1 int);')
        self.tdSql.execute('create table scalar_ct1 using scalar_stb tags(10);')
        # self.tdSql.execute(f'create table scalar_ct2 using scalar_stb tags(-20);')
        # self.tdSql.execute(f'create table scalar_ct3 using scalar_stb tags(0);')
        self.tdSql.execute('create table if not exists scalar_tb (ts timestamp, c1 int, c2 double, c3 binary(20), c4 binary(20), c5 nchar(20));')

        if fill_history_value is None:
            fill_history = ""
        else:
            fill_history = f'fill_history {fill_history_value}'
            for i in range(self.range_count):
                self.tdSql.execute(f'insert into scalar_ct1 values ({self.date_time}-{i}s, 100, -100.1, "hebei", Null, "Bigdata");')
                self.tdSql.execute(f'insert into scalar_tb values ({self.date_time}-{i}s, 100, -100.1, "heBei", Null, "Bigdata");')

        # self.tdCom.write_latency(self.case_name)
        math_function_list = ["abs", "acos", "asin", "atan", "ceil", "cos", "floor", "log", "pow", "round", "sin", "sqrt", "tan"]
        string_function_list = ["char_length", "concat", "concat_ws", "length", "lower", "ltrim", "rtrim", "substr", "upper"]
        for math_function in math_function_list:
            if math_function in ["log", "pow"]:
                self.tdSql.execute(f'create stream stb_{math_function}_stream trigger at_once ignore expired 0 ignore update 0 {fill_history} into output_{math_function}_stb as select ts, {math_function}(c1, 2), {math_function}(c2, 2), c3 from scalar_stb partition by {partition};')
                self.tdSql.execute(f'create stream ctb_{math_function}_stream trigger at_once ignore expired 0 ignore update 0  {fill_history} into output_{math_function}_ctb as select ts, {math_function}(c1, 2), {math_function}(c2, 2), c3 from scalar_ct1;')
                self.tdSql.execute(f'create stream tb_{math_function}_stream trigger at_once ignore expired 0 ignore update 0  {fill_history} into output_{math_function}_tb as select ts, {math_function}(c1, 2), {math_function}(c2, 2), c3 from scalar_tb;')
            else:
                self.tdSql.execute(f'create stream stb_{math_function}_stream trigger at_once ignore expired 0 ignore update 0  {fill_history} into output_{math_function}_stb as select ts, {math_function}(c1), {math_function}(c2), c3 from scalar_stb partition by {partition};')
                self.tdSql.execute(f'create stream ctb_{math_function}_stream trigger at_once ignore expired 0 ignore update 0  {fill_history} into output_{math_function}_ctb as select ts, {math_function}(c1), {math_function}(c2), c3 from scalar_ct1;')
                self.tdSql.execute(f'create stream tb_{math_function}_stream trigger at_once ignore expired 0 ignore update 0  {fill_history} into output_{math_function}_tb as select ts, {math_function}(c1), {math_function}(c2), c3 from scalar_tb;')
            self.tdCom.check_stream_field_type(f"describe output_{math_function}_stb", math_function)
            self.tdCom.check_stream_field_type(f"describe output_{math_function}_ctb", math_function)
            self.tdCom.check_stream_field_type(f"describe output_{math_function}_tb", math_function)
            for tbname in ["scalar_ct1", "scalar_tb"]:
                self.tdSql.execute(f'insert into {tbname} values ({self.date_time}, 100, 100.1, "beijing", "taos", "Taos");')
                self.tdSql.execute(f'insert into {tbname} values ({self.date_time}+1s, -50, -50.1, "tianjin", "taosdata", "Taosdata");')
                self.tdSql.execute(f'insert into {tbname} values ({self.date_time}+2s, 0, Null, "hebei", "TDengine", Null);')
            for i in range(self.range_count):
                self.tdSql.execute(f'insert into scalar_ct1 values ({self.date_time}+{i}s, 100, -100.1, "hebei", Null, "Bigdata");')
                self.tdSql.execute(f'insert into scalar_tb values ({self.date_time}+{i}s, 100, -100.1, "heBei", Null, "Bigdata");')
                if self.update and i%2 == 0:
                    self.tdSql.execute(f'insert into scalar_ct1 values ({self.date_time}+{i}s, 50, -50.1, Null, "heBei", "Bigdata1");')
                    self.tdSql.execute(f'insert into scalar_tb values ({self.date_time}+{i}s, 50, -50.1, Null, "heBei", "Bigdata1");')
                if self.delete and i%2 != 0:
                    dt = f'cast({self.date_time-1} as timestamp)'
                    self.tdSql.execute(f'delete from scalar_ct1 where ts = {dt};')
                    self.tdSql.execute(f'delete from scalar_tb where ts = {dt};')

                if fill_history_value:
                    self.tdSql.execute(f'insert into scalar_ct1 values ({self.date_time}-{self.range_count-1}s, 50, -50.1, Null, "heBei", "Bigdata1");')
                    self.tdSql.execute(f'insert into scalar_tb values ({self.date_time}-{self.range_count-1}s, 50, -50.1, Null, "heBei", "Bigdata1");')
                    dt = f'cast({self.date_time-(self.range_count-1)} as timestamp)'
                    self.tdSql.execute(f'delete from scalar_ct1 where ts = {dt};')
                    self.tdSql.execute(f'delete from scalar_tb where ts = {dt};')
            if math_function == "log" or math_function == "pow":
                self.tdCom.check_query_data(f'select `{math_function}(c1, 2)`, `{math_function}(c2, 2)` from output_{math_function}_stb order by ts;', f'select {math_function}(c1, 2), {math_function}(c2, 2) from scalar_stb  partition by {partition} order by ts;')
                self.tdCom.check_query_data(f'select `{math_function}(c1, 2)`, `{math_function}(c2, 2)` from output_{math_function}_ctb;', f'select {math_function}(c1, 2), {math_function}(c2, 2) from scalar_ct1;')
                self.tdCom.check_query_data(f'select `{math_function}(c1, 2)`, `{math_function}(c2, 2)` from output_{math_function}_tb;', f'select {math_function}(c1, 2), {math_function}(c2, 2) from scalar_tb;')
            else:
                self.tdCom.check_query_data(f'select `{math_function}(c1)`, `{math_function}(c2)` from output_{math_function}_stb order by ts;', f'select {math_function}(c1), {math_function}(c2) from scalar_stb  partition by {partition} order by ts;')
                self.tdCom.check_query_data(f'select `{math_function}(c1)`, `{math_function}(c2)` from output_{math_function}_ctb;', f'select {math_function}(c1), {math_function}(c2) from scalar_ct1;')
                self.tdCom.check_query_data(f'select `{math_function}(c1)`, `{math_function}(c2)` from output_{math_function}_tb;', f'select {math_function}(c1), {math_function}(c2) from scalar_tb;')
            self.tdSql.execute(f'drop stream if exists stb_{math_function}_stream')
            self.tdSql.execute(f'drop stream if exists ctb_{math_function}_stream')
            self.tdSql.execute(f'drop stream if exists tb_{math_function}_stream')


        for string_function in string_function_list:
            if string_function == "concat":
                self.tdSql.execute(f'create stream stb_{string_function}_stream trigger at_once ignore expired 0 ignore update 0  {fill_history} into output_{string_function}_stb as select ts, {string_function}(c3, c4), {string_function}(c3, c5), {string_function}(c4, c5), {string_function}(c3, c4, c5) from scalar_stb partition by {partition};')
                self.tdSql.execute(f'create stream ctb_{string_function}_stream trigger at_once ignore expired 0 ignore update 0  {fill_history} into output_{string_function}_ctb as select ts, {string_function}(c3, c4), {string_function}(c3, c5), {string_function}(c4, c5), {string_function}(c3, c4, c5) from scalar_ct1;')
                self.tdSql.execute(f'create stream tb_{string_function}_stream trigger at_once ignore expired 0 ignore update 0  {fill_history} into output_{string_function}_tb as select ts, {string_function}(c3, c4), {string_function}(c3, c5), {string_function}(c4, c5), {string_function}(c3, c4, c5) from scalar_tb;')
            elif string_function == "concat_ws":
                self.tdSql.execute(f'create stream stb_{string_function}_stream trigger at_once ignore expired 0 ignore update 0  {fill_history} into output_{string_function}_stb as select ts, {string_function}("aND", c3, c4), {string_function}("and", c3, c5), {string_function}("And", c4, c5), {string_function}("AND", c3, c4, c5) from scalar_stb partition by {partition};')
                self.tdSql.execute(f'create stream ctb_{string_function}_stream trigger at_once ignore expired 0 ignore update 0  {fill_history} into output_{string_function}_ctb as select ts, {string_function}("aND", c3, c4), {string_function}("and", c3, c5), {string_function}("And", c4, c5), {string_function}("AND", c3, c4, c5) from scalar_ct1;')
                self.tdSql.execute(f'create stream tb_{string_function}_stream trigger at_once ignore expired 0 ignore update 0  {fill_history} into output_{string_function}_tb as select ts, {string_function}("aND", c3, c4), {string_function}("and", c3, c5), {string_function}("And", c4, c5), {string_function}("AND", c3, c4, c5) from scalar_tb;')
            elif string_function == "substr":
                self.tdSql.execute(f'create stream stb_{string_function}_stream trigger at_once ignore expired 0 ignore update 0  {fill_history} into output_{string_function}_stb as select ts, {string_function}(c3, 2), {string_function}(c3, 2, 2), {string_function}(c4, 5, 1), {string_function}(c5, 3, 4) from scalar_stb partition by {partition};')
                self.tdSql.execute(f'create stream ctb_{string_function}_stream trigger at_once ignore expired 0 ignore update 0  {fill_history} into output_{string_function}_ctb as select ts, {string_function}(c3, 2), {string_function}(c3, 2, 2), {string_function}(c4, 5, 1), {string_function}(c5, 3, 4) from scalar_ct1;')
                self.tdSql.execute(f'create stream tb_{string_function}_stream trigger at_once ignore expired 0 ignore update 0  {fill_history} into output_{string_function}_tb as select ts, {string_function}(c3, 2), {string_function}(c3, 2, 2), {string_function}(c4, 5, 1), {string_function}(c5, 3, 4) from scalar_tb;')
            else:
                self.tdSql.execute(f'create stream stb_{string_function}_stream trigger at_once ignore expired 0 ignore update 0  {fill_history} into output_{string_function}_stb as select ts, {string_function}(c3), {string_function}(c4), {string_function}(c5) from scalar_stb partition by {partition};')
                self.tdSql.execute(f'create stream ctb_{string_function}_stream trigger at_once ignore expired 0 ignore update 0  {fill_history} into output_{string_function}_ctb as select ts, {string_function}(c3), {string_function}(c4), {string_function}(c5) from scalar_ct1;')
                self.tdSql.execute(f'create stream tb_{string_function}_stream trigger at_once ignore expired 0 ignore update 0  {fill_history} into output_{string_function}_tb as select ts, {string_function}(c3), {string_function}(c4), {string_function}(c5) from scalar_tb;')
            self.tdCom.check_stream_field_type(f"describe output_{string_function}_stb", string_function)
            self.tdCom.check_stream_field_type(f"describe output_{string_function}_ctb", string_function)
            self.tdCom.check_stream_field_type(f"describe output_{string_function}_tb", string_function)
            for tbname in ["scalar_ct1", "scalar_tb"]:
                self.tdSql.execute(f'insert into {tbname} values ({self.date_time}, 100, 100.1, "beijing", "taos", "Taos");')
                self.tdSql.execute(f'insert into {tbname} values ({self.date_time}+1s, -50, -50.1, "tianjin", "taosdata", "Taosdata");')
                self.tdSql.execute(f'insert into {tbname} values ({self.date_time}+2s, 0, Null, "hebei", "TDengine", Null);')


            for i in range(self.range_count):
                self.tdSql.execute(f'insert into scalar_ct1 values ({self.date_time}+{i}s, 100, -100.1, "hebei", Null, "Bigdata");')
                self.tdSql.execute(f'insert into scalar_tb values ({self.date_time}+{i}s, 100, -100.1, "heBei", Null, "Bigdata");')
                if self.update and i%2 == 0:
                    self.tdSql.execute(f'insert into scalar_ct1 values ({self.date_time}+{i}s, 50, -50.1, Null, "heBei", "Bigdata1");')
                    self.tdSql.execute(f'insert into scalar_tb values ({self.date_time}+{i}s, 50, -50.1, Null, "heBei", "Bigdata1");')
                if self.delete and i%2 != 0:
                    dt = f'cast({self.date_time-1} as timestamp)'
                    self.tdSql.execute(f'delete from scalar_ct1 where ts = {dt};')
                    self.tdSql.execute(f'delete from scalar_tb where ts = {dt};')

                if fill_history_value:
                    self.tdSql.execute(f'insert into scalar_ct1 values ({self.date_time}-{self.range_count-1}s, 50, -50.1, Null, "heBei", "Bigdata1");')
                    self.tdSql.execute(f'insert into scalar_tb values ({self.date_time}-{self.range_count-1}s, 50, -50.1, Null, "heBei", "Bigdata1");')
                    dt = f'cast({self.date_time-(self.range_count-1)} as timestamp)'
                    self.tdSql.execute(f'delete from scalar_ct1 where ts = {dt};')
                    self.tdSql.execute(f'delete from scalar_tb where ts = {dt};')


            if string_function == "concat":
                self.tdCom.check_query_data(f'select `{string_function}(c3, c4)`, `{string_function}(c3, c5)`, `{string_function}(c4, c5)`, `{string_function}(c3, c4, c5)` from output_{string_function}_stb order by ts;', f'select {string_function}(c3, c4), {string_function}(c3, c5), {string_function}(c4, c5), {string_function}(c3, c4, c5) from scalar_stb order by ts;')
                self.tdCom.check_query_data(f'select `{string_function}(c3, c4)`, `{string_function}(c3, c5)`, `{string_function}(c4, c5)`, `{string_function}(c3, c4, c5)` from output_{string_function}_ctb;', f'select {string_function}(c3, c4), {string_function}(c3, c5), {string_function}(c4, c5), {string_function}(c3, c4, c5) from scalar_ct1;')
                self.tdCom.check_query_data(f'select `{string_function}(c3, c4)`, `{string_function}(c3, c5)`, `{string_function}(c4, c5)`, `{string_function}(c3, c4, c5)` from output_{string_function}_tb;', f'select {string_function}(c3, c4), {string_function}(c3, c5), {string_function}(c4, c5), {string_function}(c3, c4, c5) from scalar_tb;')
            elif string_function == "concat_ws":
                self.tdCom.check_query_data(f'select `{string_function}("aND", c3, c4)`, `{string_function}("and", c3, c5)`, `{string_function}("And", c4, c5)`, `{string_function}("AND", c3, c4, c5)` from output_{string_function}_stb order by ts;', f'select {string_function}("aND", c3, c4), {string_function}("and", c3, c5), {string_function}("And", c4, c5), {string_function}("AND", c3, c4, c5) from scalar_stb order by ts;')
                self.tdCom.check_query_data(f'select `{string_function}("aND", c3, c4)`, `{string_function}("and", c3, c5)`, `{string_function}("And", c4, c5)`, `{string_function}("AND", c3, c4, c5)` from output_{string_function}_ctb;', f'select {string_function}("aND", c3, c4), {string_function}("and", c3, c5), {string_function}("And", c4, c5), {string_function}("AND", c3, c4, c5) from scalar_ct1;')
                self.tdCom.check_query_data(f'select `{string_function}("aND", c3, c4)`, `{string_function}("and", c3, c5)`, `{string_function}("And", c4, c5)`, `{string_function}("AND", c3, c4, c5)` from output_{string_function}_tb;', f'select {string_function}("aND", c3, c4), {string_function}("and", c3, c5), {string_function}("And", c4, c5), {string_function}("AND", c3, c4, c5) from scalar_tb;')
            elif string_function == "substr":
                self.tdCom.check_query_data(f'select `{string_function}(c3, 2)`, `{string_function}(c3, 2, 2)`, `{string_function}(c4, 5, 1)`, `{string_function}(c5, 3, 4)` from output_{string_function}_stb order by ts;', f'select {string_function}(c3, 2), {string_function}(c3, 2, 2), {string_function}(c4, 5, 1), {string_function}(c5, 3, 4) from scalar_stb order by ts;')
                self.tdCom.check_query_data(f'select `{string_function}(c3, 2)`, `{string_function}(c3, 2, 2)`, `{string_function}(c4, 5, 1)`, `{string_function}(c5, 3, 4)` from output_{string_function}_ctb;', f'select {string_function}(c3, 2), {string_function}(c3, 2, 2), {string_function}(c4, 5, 1), {string_function}(c5, 3, 4) from scalar_ct1;')
                self.tdCom.check_query_data(f'select `{string_function}(c3, 2)`, `{string_function}(c3, 2, 2)`, `{string_function}(c4, 5, 1)`, `{string_function}(c5, 3, 4)` from output_{string_function}_tb;', f'select {string_function}(c3, 2), {string_function}(c3, 2, 2), {string_function}(c4, 5, 1), {string_function}(c5, 3, 4) from scalar_tb;')
            else:
                self.tdCom.check_query_data(f'select `{string_function}(c3)`, `{string_function}(c4)`, `{string_function}(c5)` from output_{string_function}_stb order by ts;', f'select {string_function}(c3), {string_function}(c4), {string_function}(c5) from scalar_stb order by ts;')
                self.tdCom.check_query_data(f'select `{string_function}(c3)`, `{string_function}(c4)`, `{string_function}(c5)` from output_{string_function}_ctb;', f'select {string_function}(c3), {string_function}(c4), {string_function}(c5) from scalar_ct1;')
                self.tdCom.check_query_data(f'select `{string_function}(c3)`, `{string_function}(c4)`, `{string_function}(c5)` from output_{string_function}_tb;', f'select {string_function}(c3), {string_function}(c4), {string_function}(c5) from scalar_tb;')

            self.tdSql.execute(f'drop stream if exists stb_{string_function}_stream')
            self.tdSql.execute(f'drop stream if exists ctb_{string_function}_stream')
            self.tdSql.execute(f'drop stream if exists tb_{string_function}_stream')

    # def scalar_function(self, partition="tbname", delete=False, fill_history_value=None):
    #     self.delete = delete
    #     # self.prepare_stream_data()
    #     self.case_name = sys._getframe().f_code.co_name
    #     self.prepare_data()

    #     self.tdSql.execute('create table if not exists scalar_stb (ts timestamp, c1 int, c2 double, c3 binary(20), c4 binary(20), c5 nchar(20)) tags (t1 int);')
    #     self.tdSql.execute('create table scalar_ct1 using scalar_stb tags(10);')
    #     # self.tdSql.execute(f'create table scalar_ct2 using scalar_stb tags(-20);')
    #     # self.tdSql.execute(f'create table scalar_ct3 using scalar_stb tags(0);')
    #     self.tdSql.execute('create table if not exists scalar_tb (ts timestamp, c1 int, c2 double, c3 binary(20), c4 binary(20), c5 nchar(20));')

    #     if fill_history_value is None:
    #         fill_history = ""
    #     else:
    #         fill_history = f'fill_history {fill_history_value}'
    #         for i in range(self.range_count):
    #             self.tdSql.execute(f'insert into scalar_ct1 values ({self.date_time}-{i}s, 100, -100.1, "hebei", Null, "Bigdata");')
    #             self.tdSql.execute(f'insert into scalar_tb values ({self.date_time}-{i}s, 100, -100.1, "heBei", Null, "Bigdata");')

    #     # self.tdCom.write_latency(self.case_name)
    #     math_function_list = ["abs", "acos", "asin", "atan", "ceil", "cos", "floor", "log", "pow", "round", "sin", "sqrt", "tan"]
    #     string_function_list = ["char_length", "concat", "concat_ws", "length", "lower", "ltrim", "rtrim", "substr", "upper"]
    #     for math_function in math_function_list:
    #         if math_function in ["log", "pow"]:
    #             self.tdSql.execute(f'create stream stb_{math_function}_stream trigger at_once {fill_history} into output_{math_function}_stb as select ts, {math_function}(c1, 2), {math_function}(c2, 2), c3 from scalar_stb partition by {partition};')
    #             self.tdSql.execute(f'create stream ctb_{math_function}_stream trigger at_once {fill_history} into output_{math_function}_ctb as select ts, {math_function}(c1, 2), {math_function}(c2, 2), c3 from scalar_ct1;')
    #             self.tdSql.execute(f'create stream tb_{math_function}_stream trigger at_once {fill_history} into output_{math_function}_tb as select ts, {math_function}(c1, 2), {math_function}(c2, 2), c3 from scalar_tb;')
    #         else:
    #             self.tdSql.execute(f'create stream stb_{math_function}_stream trigger at_once {fill_history} into output_{math_function}_stb as select ts, {math_function}(c1), {math_function}(c2), c3 from scalar_stb partition by {partition};')
    #             self.tdSql.execute(f'create stream ctb_{math_function}_stream trigger at_once {fill_history} into output_{math_function}_ctb as select ts, {math_function}(c1), {math_function}(c2), c3 from scalar_ct1;')
    #             self.tdSql.execute(f'create stream tb_{math_function}_stream trigger at_once {fill_history} into output_{math_function}_tb as select ts, {math_function}(c1), {math_function}(c2), c3 from scalar_tb;')
    #         self.tdCom.check_stream_field_type(f"describe output_{math_function}_stb", math_function)
    #         self.tdCom.check_stream_field_type(f"describe output_{math_function}_ctb", math_function)
    #         self.tdCom.check_stream_field_type(f"describe output_{math_function}_tb", math_function)
    #     for string_function in string_function_list:
    #         if string_function == "concat":
    #             self.tdSql.execute(f'create stream stb_{string_function}_stream {fill_history} into output_{string_function}_stb as select ts, {string_function}(c3, c4), {string_function}(c3, c5), {string_function}(c4, c5), {string_function}(c3, c4, c5) from scalar_stb partition by {partition};')
    #             self.tdSql.execute(f'create stream ctb_{string_function}_stream {fill_history} into output_{string_function}_ctb as select ts, {string_function}(c3, c4), {string_function}(c3, c5), {string_function}(c4, c5), {string_function}(c3, c4, c5) from scalar_ct1;')
    #             self.tdSql.execute(f'create stream tb_{string_function}_stream {fill_history} into output_{string_function}_tb as select ts, {string_function}(c3, c4), {string_function}(c3, c5), {string_function}(c4, c5), {string_function}(c3, c4, c5) from scalar_tb;')
    #         elif string_function == "concat_ws":
    #             self.tdSql.execute(f'create stream stb_{string_function}_stream {fill_history} into output_{string_function}_stb as select ts, {string_function}("aND", c3, c4), {string_function}("and", c3, c5), {string_function}("And", c4, c5), {string_function}("AND", c3, c4, c5) from scalar_stb partition by {partition};')
    #             self.tdSql.execute(f'create stream ctb_{string_function}_stream {fill_history} into output_{string_function}_ctb as select ts, {string_function}("aND", c3, c4), {string_function}("and", c3, c5), {string_function}("And", c4, c5), {string_function}("AND", c3, c4, c5) from scalar_ct1;')
    #             self.tdSql.execute(f'create stream tb_{string_function}_stream {fill_history} into output_{string_function}_tb as select ts, {string_function}("aND", c3, c4), {string_function}("and", c3, c5), {string_function}("And", c4, c5), {string_function}("AND", c3, c4, c5) from scalar_tb;')
    #         elif string_function == "substr":
    #             self.tdSql.execute(f'create stream stb_{string_function}_stream {fill_history} into output_{string_function}_stb as select ts, {string_function}(c3, 2), {string_function}(c3, 2, 2), {string_function}(c4, 5, 1), {string_function}(c5, 3, 4) from scalar_stb partition by {partition};')
    #             self.tdSql.execute(f'create stream ctb_{string_function}_stream {fill_history} into output_{string_function}_ctb as select ts, {string_function}(c3, 2), {string_function}(c3, 2, 2), {string_function}(c4, 5, 1), {string_function}(c5, 3, 4) from scalar_ct1;')
    #             self.tdSql.execute(f'create stream tb_{string_function}_stream {fill_history} into output_{string_function}_tb as select ts, {string_function}(c3, 2), {string_function}(c3, 2, 2), {string_function}(c4, 5, 1), {string_function}(c5, 3, 4) from scalar_tb;')
    #         else:
    #             self.tdSql.execute(f'create stream stb_{string_function}_stream {fill_history} into output_{string_function}_stb as select ts, {string_function}(c3), {string_function}(c4), {string_function}(c5) from scalar_stb partition by {partition};')
    #             self.tdSql.execute(f'create stream ctb_{string_function}_stream {fill_history} into output_{string_function}_ctb as select ts, {string_function}(c3), {string_function}(c4), {string_function}(c5) from scalar_ct1;')
    #             self.tdSql.execute(f'create stream tb_{string_function}_stream {fill_history} into output_{string_function}_tb as select ts, {string_function}(c3), {string_function}(c4), {string_function}(c5) from scalar_tb;')
    #         self.tdCom.check_stream_field_type(f"describe output_{string_function}_stb", string_function)
    #         self.tdCom.check_stream_field_type(f"describe output_{string_function}_ctb", string_function)
    #         self.tdCom.check_stream_field_type(f"describe output_{string_function}_tb", string_function)

    #     for tbname in ["scalar_ct1", "scalar_tb"]:
    #         self.tdSql.execute(f'insert into {tbname} values ({self.date_time}, 100, 100.1, "beijing", "taos", "Taos");')
    #         self.tdSql.execute(f'insert into {tbname} values ({self.date_time}+1s, -50, -50.1, "tianjin", "taosdata", "Taosdata");')
    #         self.tdSql.execute(f'insert into {tbname} values ({self.date_time}+2s, 0, Null, "hebei", "TDengine", Null);')

    #     count = 1
    #     step_count = 1
    #     for i in range(self.range_count):
    #         self.tdSql.execute(f'insert into scalar_ct1 values ({self.date_time}+{i}s, 100, -100.1, "hebei", Null, "Bigdata");')
    #         self.tdSql.execute(f'insert into scalar_tb values ({self.date_time}+{i}s, 100, -100.1, "heBei", Null, "Bigdata");')
    #         if self.update and i%2 == 0:
    #             self.tdSql.execute(f'insert into scalar_ct1 values ({self.date_time}+{i}s, 50, -50.1, Null, "heBei", "Bigdata1");')
    #             self.tdSql.execute(f'insert into scalar_tb values ({self.date_time}+{i}s, 50, -50.1, Null, "heBei", "Bigdata1");')
    #         if self.delete and i%2 != 0:
    #             dt = f'cast({self.date_time-1} as timestamp)'
    #             self.tdSql.execute(f'delete from scalar_ct1 where ts = {dt};')
    #             self.tdSql.execute(f'delete from scalar_tb where ts = {dt};')
    #         # if i % 2 == 0:
    #         #     step_count += i
    #         #     for j in range(count, step_count):
    #         #         self.tdSql.execute(f'insert into scalar_ct1 values ({self.date_time}+{j}s, 100, -100.1, "hebei", Null, "Bigdata");')
    #         #         self.tdSql.execute(f'insert into scalar_tb values ({self.date_time}+{j}s, 100, -100.1, "heBei", Null, "Bigdata");')
    #         #     count += i
    #         # else:
    #         #     step_count += 1
    #         #     for i in range(2):
    #         #         self.tdSql.execute(f'insert into scalar_ct1 values ({self.date_time}+{count}s, -50, 50.1, "beiJing", "TDengine", "taos");')
    #         #         self.tdSql.execute(f'insert into scalar_tb values ({self.date_time}+{count}s, -50, 50.1, "beiJing", "TDengine", "taos");')
    #         #     count += 1

    #         if fill_history_value:
    #             self.tdSql.execute(f'insert into scalar_ct1 values ({self.date_time}-{self.range_count-1}s, 50, -50.1, Null, "heBei", "Bigdata1");')
    #             self.tdSql.execute(f'insert into scalar_tb values ({self.date_time}-{self.range_count-1}s, 50, -50.1, Null, "heBei", "Bigdata1");')
    #             dt = f'cast({self.date_time-(self.range_count-1)} as timestamp)'
    #             self.tdSql.execute(f'delete from scalar_ct1 where ts = {dt};')
    #             self.tdSql.execute(f'delete from scalar_tb where ts = {dt};')

    #         for math_function in math_function_list:
    #             if math_function == "log" or math_function == "pow":
    #                 self.tdCom.check_query_data(f'select `{math_function}(c1, 2)`, `{math_function}(c2, 2)` from output_{math_function}_stb order by ts;', f'select {math_function}(c1, 2), {math_function}(c2, 2) from scalar_stb  partition by {partition} order by ts;')
    #                 self.tdCom.check_query_data(f'select `{math_function}(c1, 2)`, `{math_function}(c2, 2)` from output_{math_function}_ctb;', f'select {math_function}(c1, 2), {math_function}(c2, 2) from scalar_ct1;')
    #                 self.tdCom.check_query_data(f'select `{math_function}(c1, 2)`, `{math_function}(c2, 2)` from output_{math_function}_tb;', f'select {math_function}(c1, 2), {math_function}(c2, 2) from scalar_tb;')
    #             else:
    #                 self.tdCom.check_query_data(f'select `{math_function}(c1)`, `{math_function}(c2)` from output_{math_function}_stb order by ts;', f'select {math_function}(c1), {math_function}(c2) from scalar_stb  partition by {partition} order by ts;')
    #                 self.tdCom.check_query_data(f'select `{math_function}(c1)`, `{math_function}(c2)` from output_{math_function}_ctb;', f'select {math_function}(c1), {math_function}(c2) from scalar_ct1;')
    #                 self.tdCom.check_query_data(f'select `{math_function}(c1)`, `{math_function}(c2)` from output_{math_function}_tb;', f'select {math_function}(c1), {math_function}(c2) from scalar_tb;')
    #         for string_function in string_function_list:
    #             if string_function == "concat":
    #                 self.tdCom.check_query_data(f'select `{string_function}(c3, c4)`, `{string_function}(c3, c5)`, `{string_function}(c4, c5)`, `{string_function}(c3, c4, c5)` from output_{string_function}_stb order by ts;', f'select {string_function}(c3, c4), {string_function}(c3, c5), {string_function}(c4, c5), {string_function}(c3, c4, c5) from scalar_stb order by ts;')
    #                 self.tdCom.check_query_data(f'select `{string_function}(c3, c4)`, `{string_function}(c3, c5)`, `{string_function}(c4, c5)`, `{string_function}(c3, c4, c5)` from output_{string_function}_ctb;', f'select {string_function}(c3, c4), {string_function}(c3, c5), {string_function}(c4, c5), {string_function}(c3, c4, c5) from scalar_ct1;')
    #                 self.tdCom.check_query_data(f'select `{string_function}(c3, c4)`, `{string_function}(c3, c5)`, `{string_function}(c4, c5)`, `{string_function}(c3, c4, c5)` from output_{string_function}_tb;', f'select {string_function}(c3, c4), {string_function}(c3, c5), {string_function}(c4, c5), {string_function}(c3, c4, c5) from scalar_tb;')
    #             elif string_function == "concat_ws":
    #                 self.tdCom.check_query_data(f'select `{string_function}("aND", c3, c4)`, `{string_function}("and", c3, c5)`, `{string_function}("And", c4, c5)`, `{string_function}("AND", c3, c4, c5)` from output_{string_function}_stb order by ts;', f'select {string_function}("aND", c3, c4), {string_function}("and", c3, c5), {string_function}("And", c4, c5), {string_function}("AND", c3, c4, c5) from scalar_stb order by ts;')
    #                 self.tdCom.check_query_data(f'select `{string_function}("aND", c3, c4)`, `{string_function}("and", c3, c5)`, `{string_function}("And", c4, c5)`, `{string_function}("AND", c3, c4, c5)` from output_{string_function}_ctb;', f'select {string_function}("aND", c3, c4), {string_function}("and", c3, c5), {string_function}("And", c4, c5), {string_function}("AND", c3, c4, c5) from scalar_ct1;')
    #                 self.tdCom.check_query_data(f'select `{string_function}("aND", c3, c4)`, `{string_function}("and", c3, c5)`, `{string_function}("And", c4, c5)`, `{string_function}("AND", c3, c4, c5)` from output_{string_function}_tb;', f'select {string_function}("aND", c3, c4), {string_function}("and", c3, c5), {string_function}("And", c4, c5), {string_function}("AND", c3, c4, c5) from scalar_tb;')
    #             elif string_function == "substr":
    #                 self.tdCom.check_query_data(f'select `{string_function}(c3, 2)`, `{string_function}(c3, 2, 2)`, `{string_function}(c4, 5, 1)`, `{string_function}(c5, 3, 4)` from output_{string_function}_stb order by ts;', f'select {string_function}(c3, 2), {string_function}(c3, 2, 2), {string_function}(c4, 5, 1), {string_function}(c5, 3, 4) from scalar_stb order by ts;')
    #                 self.tdCom.check_query_data(f'select `{string_function}(c3, 2)`, `{string_function}(c3, 2, 2)`, `{string_function}(c4, 5, 1)`, `{string_function}(c5, 3, 4)` from output_{string_function}_ctb;', f'select {string_function}(c3, 2), {string_function}(c3, 2, 2), {string_function}(c4, 5, 1), {string_function}(c5, 3, 4) from scalar_ct1;')
    #                 self.tdCom.check_query_data(f'select `{string_function}(c3, 2)`, `{string_function}(c3, 2, 2)`, `{string_function}(c4, 5, 1)`, `{string_function}(c5, 3, 4)` from output_{string_function}_tb;', f'select {string_function}(c3, 2), {string_function}(c3, 2, 2), {string_function}(c4, 5, 1), {string_function}(c5, 3, 4) from scalar_tb;')
    #             else:
    #                 self.tdCom.check_query_data(f'select `{string_function}(c3)`, `{string_function}(c4)`, `{string_function}(c5)` from output_{string_function}_stb order by ts;', f'select {string_function}(c3), {string_function}(c4), {string_function}(c5) from scalar_stb order by ts;')
    #                 self.tdCom.check_query_data(f'select `{string_function}(c3)`, `{string_function}(c4)`, `{string_function}(c5)` from output_{string_function}_ctb;', f'select {string_function}(c3), {string_function}(c4), {string_function}(c5) from scalar_ct1;')
    #                 self.tdCom.check_query_data(f'select `{string_function}(c3)`, `{string_function}(c4)`, `{string_function}(c5)` from output_{string_function}_tb;', f'select {string_function}(c3), {string_function}(c4), {string_function}(c5) from scalar_tb;')

    #     # count = 1
    #     # step_count = 1
    #     # for i in range(1, 20):
    #     #     if i % 2 == 0:
    #     #         step_count += i
    #     #         for j in range(count, step_count):
    #     #             self.tdSql.execute(f'insert into scalar_ct1 values ({self.date_time}+{j}s, -1, 1, "hebei", Null, "Bigdata");')
    #     #             self.tdSql.execute(f'insert into scalar_tb values ({self.date_time}+{j}s, -1, 1, "hebei", Null, "Bigdata");')
    #     #         count += i
    #     #     else:
    #     #         step_count += 1
    #     #         for i in range(2):
    #     #             self.tdSql.execute(f'insert into scalar_ct1 values ({self.date_time}+{count}s, -1, 1, "hebei", Null, "Bigdata");')
    #     #             self.tdSql.execute(f'insert into scalar_tb values ({self.date_time}+{count}s, -1, 1, "hebei", Null, "Bigdata");')
    #     #         count += 1
    #     #     # check result
    #     #     self.tdCom.check_stream(f'select {select_elm} from output_data_filter_stb where {filter_sql};', f'select {select_elm} from data_filter_stb where {filter_sql};', count-1)
    #     #     self.tdCom.check_stream(f'select {select_elm} from output_data_filter_ctb where {filter_sql};', f'select {select_elm} from data_filter_ct1 where {filter_sql};', count-1)
    #     #     self.tdCom.check_stream(f'select {select_elm} from output_data_filter_tb where {filter_sql};', f'select {select_elm} from data_filter_tb where {filter_sql};', count-1)



    #     # self.tdSql.execute(f'insert into scalar_ct1 values ({self.date_time}+3s, -1, 1, "hebei", Null, "Bigdata");')
    #     # for math_function in math_function_list:
    #     #     if math_function == "log" or math_function == "pow":
    #     #         self.tdCom.check_stream(f'select `{math_function}(c1, 2)`, `{math_function}(c2, 2)` from output_{math_function}_stb;', f'select {math_function}(c1, 2), {math_function}(c2, 2) from scalar_stb;', 4)
    #     #     else:
    #     #         self.tdCom.check_stream(f'select `{math_function}(c1)`, `{math_function}(c2)` from output_{math_function}_stb;', f'select {math_function}(c1), {math_function}(c2) from scalar_stb;', 4)

    #     # for string_function in string_function_list:
    #     #     if string_function == "concat":
    #     #         self.tdCom.check_stream(f'select `{string_function}(c3, c4)`, `{string_function}(c3, c5)`, `{string_function}(c4, c5)`, `{string_function}(c3, c4, c5)` from output_{string_function}_stb;', f'select {string_function}(c3, c4), {string_function}(c3, c5), {string_function}(c4, c5), {string_function}(c3, c4, c5) from scalar_stb;', 4)
    #     #     elif string_function == "concat_ws":
    #     #         self.tdCom.check_stream(f'select `{string_function}("aND", c3, c4)`, `{string_function}("and", c3, c5)`, `{string_function}("And", c4, c5)`, `{string_function}("AND", c3, c4, c5)` from output_{string_function}_stb;', f'select {string_function}("aND", c3, c4), {string_function}("and", c3, c5), {string_function}("And", c4, c5), {string_function}("AND", c3, c4, c5) from scalar_stb;', 4)
    #     #     elif string_function == "substr":
    #     #         self.tdCom.check_stream(f'select `{string_function}(c3, 2)`, `{string_function}(c3, 2, 2)`, `{string_function}(c4, 5, 1)`, `{string_function}(c5, 3, 4)` from output_{string_function}_stb;', f'select {string_function}(c3, 2), {string_function}(c3, 2, 2), {string_function}(c4, 5, 1), {string_function}(c5, 3, 4) from scalar_stb;', 4)
    #     #     else:
    #     #         self.tdCom.check_stream(f'select `{string_function}(c3)`, `{string_function}(c4)`, `{string_function}(c5)` from output_{string_function}_stb;', f'select {string_function}(c3), {string_function}(c4), {string_function}(c5) from scalar_stb;', 4)


    def partitionby_interval(self, interval=None, partition_by_elm="tbname", ignore_expired=None):
        self.case_name = sys._getframe().f_code.co_name
        self.prepare_data(interval=interval)
        self.tdCom.write_latency(self.case_name)
        ctb_name_list = list()
        for i in range(1, self.range_count):
            ctb_name = self.tdCom.get_long_name()
            ctb_name_list.append(ctb_name)
            self.tdCom.create_ctable(stbname=self.stb_name, ctbname=ctb_name)
        if interval is not None:
            source_sql = f'select _wstart AS wstart, {self.partition_by_stb_source_select_str}  from {self.stb_name} partition by {partition_by_elm} interval({self.dataDict["interval"]}s)'
        else:
            source_sql = f'select {self.stb_filter_des_select_elm} from {self.stb_name} partition by {partition_by_elm}'

        # create stb/ctb/tb stream
        self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=source_sql, ignore_expired=ignore_expired)
        # insert data
        count = 1
        step_count = 1
        for i in range(1, self.range_count):
            if i == 1:
                record_window_close_ts = self.date_time - 15 * self.offset
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
                if "first" not in colname and "last" not in colname:
                    if interval is not None:
                        self.tdCom.check_query_data(f'select `{colname}` from {self.stb_name}{self.des_table_suffix} order by `{colname}`;', f'select {colname}  from {self.stb_name} partition by {partition_by_elm} interval({self.dataDict["interval"]}s) order by `{colname}`;')
                    else:
                        self.tdCom.check_query_data(f'select {self.stb_filter_des_select_elm} from {self.stb_name}{self.des_table_suffix} order by c1,c2,c3;', f'select {self.stb_filter_des_select_elm}  from {self.stb_name} partition by {partition_by_elm} order by c1,c2,c3;')

        if self.disorder:
            self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=record_window_close_ts)
            for ctb_name in ctb_name_list:
                self.tdCom.insert_rows(tbname=ctb_name, ts_value=record_window_close_ts)
            if ignore_expired:
                if "first" not in colname and "last" not in colname:
                    for colname in self.partition_by_downsampling_function_list:
                        if interval is not None:
                            self.tdSql.query(f'select `{colname}` from {self.stb_name}{self.des_table_suffix} order by `{colname}`;')
                            res1 = self.tdSql.query_data
                            self.tdSql.query(f'select {colname}  from {self.stb_name} partition by {partition_by_elm} interval({self.dataDict["interval"]}s) order by `{colname}`;')
                            res2 = self.tdSql.query_data
                            self.tdSql.checkNotEqual(res1, res2)
                        else:
                            self.tdCom.check_query_data(f'select {self.stb_filter_des_select_elm} from {self.stb_name}{self.des_table_suffix} order by c1,c2,c3;', f'select {self.stb_filter_des_select_elm}  from {self.stb_name} partition by {partition_by_elm} order by c1,c2,c3;')

            else:
                for colname in self.partition_by_downsampling_function_list:
                    if "first" not in colname and "last" not in colname:
                        if interval is not None:
                            self.tdCom.check_query_data(f'select `{colname}` from {self.stb_name}{self.des_table_suffix} order by `{colname}`;', f'select {colname}  from {self.stb_name} partition by {partition_by_elm} interval({self.dataDict["interval"]}s) order by `{colname}`;')
                        else:
                            self.tdCom.check_query_data(f'select {self.stb_filter_des_select_elm} from {self.stb_name}{self.des_table_suffix} order by c1,c2,c3;', f'select {self.stb_filter_des_select_elm}  from {self.stb_name} partition by {partition_by_elm} order by c1,c2,c3;')

    def partition_tag_by_interval(self, interval=None, partition_by_elm="tag", ignore_expired=None):
        self.case_name = sys._getframe().f_code.co_name
        self.prepare_data(interval=interval)
        self.tdCom.write_latency(self.case_name)
        ctb_name_list = list()
        for i in range(1, self.range_count):
            ctb_name = self.tdCom.get_long_name()
            ctb_name_list.append(ctb_name)
            self.tdCom.create_ctable(stbname=self.stb_name, ctbname=ctb_name)
        if interval is not None:
            stb_source_sql = f'select _wstart AS wstart, {self.partition_by_stb_source_select_str}  from {self.stb_name} partition by {partition_by_elm} interval({self.dataDict["interval"]}s)'
            ctb_source_sql = f'select _wstart AS wstart, {self.partition_by_stb_source_select_str}  from {self.ctb_name} partition by {partition_by_elm} interval({self.dataDict["interval"]}s)'
        else:
            stb_source_sql = f'select {self.stb_filter_des_select_elm} from {self.stb_name} partition by {partition_by_elm}'
            ctb_source_sql = f'select {self.stb_filter_des_select_elm} from {self.ctb_name} partition by {partition_by_elm}'

        # create stb/ctb/tb stream
        self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=stb_source_sql, ignore_expired=ignore_expired)
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=ctb_source_sql, ignore_expired=ignore_expired)
        # insert data
        count = 1
        step_count = 1
        for i in range(1, self.range_count):
            if i == 1:
                record_window_close_ts = self.date_time - 15 * self.offset
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
            for tbname in [self.stb_name, self.ctb_name]:
                for colname in self.partition_by_downsampling_function_list:
                    if interval is not None:
                        self.tdCom.check_query_data(f'select `{colname}` from {tbname}{self.des_table_suffix} order by `{colname}`;', f'select {colname}  from {tbname} partition by {partition_by_elm} interval({self.dataDict["interval"]}s) order by `{colname}`;')
                    else:
                        self.tdCom.check_query_data(f'select {self.stb_filter_des_select_elm} from {tbname}{self.des_table_suffix} order by c1,c2,c3;', f'select {self.stb_filter_des_select_elm}  from {tbname} partition by {partition_by_elm} order by c1,c2,c3;')

        if self.disorder:
            self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=record_window_close_ts)
            for ctb_name in ctb_name_list:
                self.tdCom.insert_rows(tbname=ctb_name, ts_value=record_window_close_ts)
            if ignore_expired:
                for colname in self.partition_by_downsampling_function_list:
                    for tbname in [self.stb_name, self.ctb_name]:
                        if interval is not None:
                            self.tdSql.query(f'select `{colname}` from {tbname}{self.des_table_suffix} order by `{colname}`;')
                            res1 = self.tdSql.query_data
                            self.tdSql.query(f'select {colname}  from {tbname} partition by {partition_by_elm} interval({self.dataDict["interval"]}s) order by `{colname}`;')
                            res2 = self.tdSql.query_data
                            self.tdSql.checkNotEqual(res1, res2)
                        else:
                            self.tdCom.check_query_data(f'select {self.stb_filter_des_select_elm} from {tbname}{self.des_table_suffix} order by c1,c2,c3;', f'select {self.stb_filter_des_select_elm}  from {tbname} partition by {partition_by_elm} order by c1,c2,c3;')

            else:
                for colname in self.partition_by_downsampling_function_list:
                    for tbname in [self.stb_name, self.ctb_name]:
                        if interval is not None:
                            self.tdCom.check_query_data(f'select `{colname}` from {tbname}{self.des_table_suffix} order by `{colname}`;', f'select {colname}  from {tbname} partition by {partition_by_elm} interval({self.dataDict["interval"]}s) order by `{colname}`;')
                        else:
                            self.tdCom.check_query_data(f'select {self.stb_filter_des_select_elm} from {tbname}{self.des_table_suffix} order by c1,c2,c3;', f'select {self.stb_filter_des_select_elm}  from {tbname} partition by {partition_by_elm} order by c1,c2,c3;')


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
        self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.stb_source_select_str}  from {self.stb_name} session(ts, {self.dataDict["session"]}s)', trigger_mode="max_delay", max_delay=max_delay_value)
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.stb_source_select_str}  from {self.ctb_name} session(ts, {self.dataDict["session"]}s)', trigger_mode="max_delay", max_delay=max_delay_value)
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.tb_source_select_str}  from {self.tb_name} session(ts, {self.dataDict["session"]}s)', trigger_mode="max_delay", max_delay=max_delay_value)
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
                    self.tdCom.check_stream(f'select wstart, {self.stb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS wstart, {self.stb_source_select_str}  from {tbname} session(ts, {self.dataDict["session"]}s) limit {i+1}', i+1)
                else:
                    self.tdCom.check_stream(f'select wstart, {self.tb_output_select_str} from {tbname}{self.des_table_suffix}', f'select _wstart AS wstart, {self.tb_source_select_str}  from {tbname} session(ts, {self.dataDict["session"]}s) limit {i+1}', i+1)

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
                        f'select _wstart, count(*) from {self.dbname}.{self.case_name}_stb event_window start with c1 >= 0 end with c1 <= 5;'
                        ]
        for error_sql in error_sql_list:
            self.tdSql.error(f'create stream if not exists {stream_name} into {dbname2}.target_tb as {error_sql}')


    def insert_after_restart(self, delete=False, fill_history_value=None):
        self.data_filter(delete=delete, fill_history_value=fill_history_value)
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
        self.tdCom.check_query_data(f'select {self.stb_filter_des_select_elm} from {self.stb_stream_des_table};', f'select {self.filter_source_select_elm} from {self.stb_name} where {self.stb_data_filter_sql} partition by tbname;')
        self.tdCom.check_query_data(f'select {self.tb_filter_des_select_elm} from {self.ctb_stream_des_table};', f'select {self.filter_source_select_elm} from {self.ctb_name} where {self.stb_data_filter_sql};')
        self.tdCom.check_query_data(f'select {self.tb_filter_des_select_elm} from {self.tb_stream_des_table};', f'select {self.filter_source_select_elm} from {self.tb_name} where {self.tb_data_filter_sql};')

    def insert_after_recreate_source_table(self):
        count = self.data_filter(True)
        new_count = deepcopy(count)
        for tbname in [self.stb_name, self.tb_name]:
            self.tdSql.error(f'drop table if exists {tbname}')
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

    def pause_resume_test(self, interval, partition="tbname", delete=False, fill_history_value=None, pause=True, resume=True, ignore_untreated=False):
        if_exist_value_list = [None, True]
        if_exist = random.choice(if_exist_value_list)
        reverse_check = True if ignore_untreated else False
        tmp_range_count = self.range_count
        range_count = (self.range_count + 3) * 3
        self.delete = delete
        self.case_name = sys._getframe().f_code.co_name
        self.prepare_data(interval=interval, fill_history_value=fill_history_value)

        if partition == "tbname":
            stream_case_when_partition = self.partition_tbname_alias
            partition_elm_alias = self.partition_tbname_alias
        elif partition == "c1":
            stream_case_when_partition = self.partition_col_alias
            partition_elm_alias = self.partition_col_alias
        elif partition == "abs(c1)":
            partition_elm_alias = self.partition_expression_alias
        elif partition is None:
            partition_elm_alias = '"no_partition"'
        else:
            partition_elm_alias = self.partition_tag_alias
        if partition == "tbname" or partition is None:
            stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", {partition_elm_alias}), "{self.subtable_suffix}")' if self.subtable else None
            ctb_subtable_value = f'concat(concat("{self.ctb_name}_{self.subtable_prefix}", {partition_elm_alias}), "{self.subtable_suffix}")' if self.subtable else None
            tb_subtable_value = f'concat(concat("{self.tb_name}_{self.subtable_prefix}", {partition_elm_alias}), "{self.subtable_suffix}")' if self.subtable else None
        else:
            stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", cast(cast(abs(cast({partition_elm_alias} as int)) as bigint) as varchar(100))), "{self.subtable_suffix}")' if self.subtable else None
            ctb_subtable_value = f'concat(concat("{self.ctb_name}_{self.subtable_prefix}", cast(cast(abs(cast({partition_elm_alias} as int)) as bigint) as varchar(100))), "{self.subtable_suffix}")' if self.subtable else None
            tb_subtable_value = f'concat(concat("{self.tb_name}_{self.subtable_prefix}", cast(cast(abs(cast({partition_elm_alias} as int)) as bigint) as varchar(100))), "{self.subtable_suffix}")' if self.subtable else None
        if partition:
            partition_elm = f'partition by {partition} {partition_elm_alias}'
        else:
            partition_elm = ""
        self.tdCom.write_latency(self.case_name)
        self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.stb_source_select_str}  from {self.stb_name} {partition_elm} interval({self.dataDict["interval"]}s)', trigger_mode="at_once", subtable_value=stb_subtable_value, fill_history_value=fill_history_value)
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.stb_source_select_str}  from {self.ctb_name} {partition_elm} interval({self.dataDict["interval"]}s)', trigger_mode="at_once", subtable_value=ctb_subtable_value, fill_history_value=fill_history_value)
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.tb_source_select_str}  from {self.tb_name} {partition_elm} interval({self.dataDict["interval"]}s)', trigger_mode="at_once", subtable_value=tb_subtable_value, fill_history_value=fill_history_value)
        start_time = self.date_time
        for i in range(range_count):
            ts_value = str(self.date_time+self.dataDict["interval"])+f'+{i*10}s'
            ts_cast_delete_value = self.tdCom.time_cast(ts_value)
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
            if partition:
                partition_elm = f'partition by {partition}'
            else:
                partition_elm = ""
            # if i == int(range_count/2):
            if i > 2 and i % 3 == 0:
                for stream_name in [f'{self.stb_name}{self.stream_suffix}', f'{self.ctb_name}{self.stream_suffix}', f'{self.tb_name}{self.stream_suffix}']:
                    if if_exist is not None:
                        self.tdSql.execute(f'pause stream if exists {stream_name}_no_exist')
                    self.tdSql.error(f'pause stream if not exists {stream_name}')
                    self.tdSql.error(f'pause stream {stream_name}_no_exist')
                    self.tdCom.pause_stream(stream_name, if_exist)
                if pause and not resume and range_count-i <= 3:
                    time.sleep(self.default_interval)
                    self.tdSql.query(f'select wstart, {self.stb_output_select_str} from {self.stb_name}{self.des_table_suffix} order by wstart')
                    res_after_pause = self.tdSql.query_data
            if resume:
                if i > 2 and i % 3 != 0:
                    for stream_name in [f'{self.stb_name}{self.stream_suffix}', f'{self.ctb_name}{self.stream_suffix}', f'{self.tb_name}{self.stream_suffix}']:
                        if if_exist is not None:
                            self.tdSql.execute(f'resume stream if exists {stream_name}_no_exist')
                        self.tdSql.error(f'resume stream if not exists {stream_name}')
                        self.tdCom.resume_stream(stream_name, if_exist, None, ignore_untreated)
        if pause and not resume:
            self.tdSql.query(f'select wstart, {self.stb_output_select_str} from {self.stb_name}{self.des_table_suffix} order by wstart')
            res_without_resume = self.tdSql.query_data
            self.tdSql.checkEqual(res_after_pause, res_without_resume)
        else:
            for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
                if tbname != self.tb_name:
                    self.tdCom.check_query_data(f'select wstart, {self.stb_output_select_str} from {tbname}{self.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.stb_source_select_str}  from {tbname} {partition_elm} interval({self.dataDict["interval"]}s) order by wstart', sorted=True, reverse_check=reverse_check)
                else:
                    self.tdCom.check_query_data(f'select wstart, {self.tb_output_select_str} from {tbname}{self.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.tb_source_select_str}  from {tbname} {partition_elm} interval({self.dataDict["interval"]}s) order by wstart', sorted=True, reverse_check=reverse_check)

        if self.subtable:
            for tname in [self.stb_name, self.ctb_name]:
                self.tdSql.query(f'select * from {self.ctb_name}')
                ptn_counter = 0
                for c1_value in self.tdSql.query_data:
                    if partition == "c1":
                        self.tdSql.query(f'select count(*) from `{tname}_{self.subtable_prefix}{abs(c1_value[1])}{self.subtable_suffix}`;')
                    elif partition is None:
                        self.tdSql.query(f'select count(*) from `{tname}_{self.subtable_prefix}no_partition{self.subtable_suffix}`;')
                    elif partition == "abs(c1)":
                        abs_c1_value = abs(c1_value[1])
                        self.tdSql.query(f'select count(*) from `{tname}_{self.subtable_prefix}{abs_c1_value}{self.subtable_suffix}`;')
                    elif partition == "tbname" and ptn_counter == 0:
                        self.tdSql.query(f'select count(*) from `{tname}_{self.subtable_prefix}{self.ctb_name}{self.subtable_suffix}`;')
                        ptn_counter += 1
                    self.tdSql.checkEqual(self.tdSql.query_data[0][0] > 0, True)

            self.tdSql.query(f'select * from {self.tb_name}')
            ptn_counter = 0
            for c1_value in self.tdSql.query_data:
                if partition == "c1":
                    self.tdSql.query(f'select count(*) from `{self.tb_name}_{self.subtable_prefix}{abs(c1_value[1])}{self.subtable_suffix}`;')
                elif partition is None:
                    self.tdSql.query(f'select count(*) from `{self.tb_name}_{self.subtable_prefix}no_partition{self.subtable_suffix}`;')
                elif partition == "abs(c1)":
                    abs_c1_value = abs(c1_value[1])
                    self.tdSql.query(f'select count(*) from `{self.tb_name}_{self.subtable_prefix}{abs_c1_value}{self.subtable_suffix}`;')
                elif partition == "tbname" and ptn_counter == 0:
                    self.tdSql.query(f'select count(*) from `{self.tb_name}_{self.subtable_prefix}{self.tb_name}{self.subtable_suffix}`;')
                    ptn_counter += 1

                self.tdSql.checkEqual(self.tdSql.query_data[0][0] > 0, True)


    def run(self):
        # self.at_once_interval(interval=random.randint(10, 12), partition="c1", fill_value="NULL")
        # self.at_once_interval(interval=random.randint(10, 15), partition="c1", fill_value="NULL", delete=True)
        # self.vgroups = 1
        # self.at_once_interval_ext(interval=random.randint(10, 15), delete=False, fill_history_value=1, partition=None, subtable="constant", stb_field_name_value=self.tb_filter_des_select_elm, tag_value=self.tag_filter_des_select_elm, use_exist_stb=True)
        # for fill_history_value in [1]:
            # ! TD-24620
            # self.watermark_window_close_session(session=random.randint(10, 15), watermark=None, fill_history_value=fill_history_value)
            # self.watermark_window_close_session(session=random.randint(10, 12), watermark=random.randint(20, 25), fill_history_value=fill_history_value)
            # self.watermark_max_delay_session(session=random.randint(10, 15), watermark=None, max_delay=f"{random.randint(1, 3)}s", fill_history_value=fill_history_value)
            # self.watermark_max_delay_session(session=random.randint(10, 15), watermark=random.randint(20, 30), max_delay=f"{random.randint(1, 3)}s", fill_history_value=fill_history_value)
        # for ignore_update in [None, 0, 1]:
        #     self.at_once_session(session=random.randint(10, 15), ignore_update=ignore_update, fill_history_value=1)
        # return
        # ! not stable
        # self.watermark_window_close_session(session=random.randint(10, 15), watermark=random.randint(20, 25), fill_history_value=1)
        # TODO
        # self.watermark_max_delay_interval(interval=random.randint(10, 15), watermark=random.randint(15, 20), max_delay=f"{random.randint(5, 6)}s", fill_value="NULL")
        # self.watermark_max_delay_interval(interval=random.randint(10, 15), watermark=random.randint(15, 20), max_delay=f"{random.randint(5, 6)}s", fill_value="NEXT")
        # self.watermark_max_delay_interval(interval=random.randint(10, 15), watermark=random.randint(15, 20), max_delay=f"{random.randint(5, 6)}s", fill_value="PREV")
        # self.watermark_max_delay_interval(interval=random.randint(10, 15), watermark=random.randint(15, 20), max_delay=f"{random.randint(5, 6)}s", fill_value="LINEAR")
        # self.watermark_max_delay_interval(interval=random.randint(10, 15), watermark=random.randint(15, 20), max_delay=f"{random.randint(5, 6)}s", fill_value="VALUE,1,2,3,4,5,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11")
        # for fill_value in ["NULL", "PREV", "NEXT", "LINEAR", "VALUE,1,2,3,4,5,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11"]:
        # self.at_once_interval(interval=random.randint(10, 15), partition="tbname")
        # self.at_once_interval_ext(interval=random.randint(10, 15), delete=False, fill_history_value=1, partition=None, subtable="constant", stb_field_name_value=self.tb_filter_des_select_elm, tag_value=self.tag_filter_des_select_elm.split(",")[0], use_exist_stb=True)
        # self.at_once_interval_ext(interval=random.randint(10, 15), delete=False, fill_history_value=1, partition=f'tbname,{self.tag_filter_des_select_elm},c1', stb_field_name_value=self.tb_filter_des_select_elm, tag_value=self.tag_filter_des_select_elm, use_exist_stb=True)
        # self.vgroups = 1
        # # for fill_value in ["NULL", "PREV", "NEXT", "VALUE,1,2,3,4,5,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11"]:
        # for fill_value in ["NEXT"]:
        #     for watermark in [None]:
        #         self.window_close_interval(interval=random.randint(10, 12), watermark=watermark, fill_value=fill_value)
        # self.udf_test(8, "int")
        # self.vgroup = 10
        # self.watermark_max_delay_interval(interval=random.randint(10, 15), watermark=None, max_delay=f"{random.randint(5, 6)}s", fill_value="NULL")
        # self.insert_after_recreate_source_table()
        # self.json_function(partition="tbname", delete=True, fill_history_value=1)
        # return
        for vgroups in self.vgroups_list:
            self.vgroups = vgroups
            self.create_none_db_stream()
            self.create_none_source_tb_stream()
            self.create_none_source_tb_tag_stream()
            self.create_none_source_tb_col_stream()
            self.create_error_source_sql_stream()
            ## ! rep3 TD-20280
            self.insert_after_restart()
            # self.insert_after_restart(delete=True, fill_history_value=1)
            ## ! TD-18123
            # # self.insert_after_recreate_source_table()
            self.query_after_drop_stream_db()
            self.data_filter()
            self.data_filter(delete=True)
            self.data_filter(delete=True, fill_history_value=1)
            self.life_cycle()
            self.scalar_function(partition="tbname", delete=True, fill_history_value=1)
            self.scalar_function(partition="tbname,c1", delete=True, fill_history_value=1)
            self.stream_tandem()
            self.udf_test(8, "int")
            self.udf_test(8, "int", 1)
            self.udaf_test(10, 8, "double")
            self.udaf_test(10, 8, "double", 1)

            self.at_once_interval(interval=random.randint(10, 15), partition="tbname")
            self.at_once_interval(interval=random.randint(10, 15), partition="c1")
            self.at_once_interval(interval=random.randint(10, 15), partition="abs(c1)")
            self.at_once_interval(interval=random.randint(10, 15), partition=None)
            self.at_once_interval(interval=random.randint(10, 15), partition="tbname", delete=True)
            self.at_once_interval(interval=random.randint(10, 15), partition="c1", delete=True)
            self.at_once_interval(interval=random.randint(10, 15), partition="abs(c1)", delete=True)
            self.at_once_interval(interval=random.randint(10, 15), partition=None, delete=True)
            self.at_once_session(session=random.randint(10, 15),subtable=None, partition="abs(c1)")
            if self.vgroups == 1:
                self.at_once_interval_ext(interval=random.randint(10, 15), delete=False, fill_history_value=1, partition=None, subtable="constant", stb_field_name_value=self.tb_filter_des_select_elm, tag_value=self.tag_filter_des_select_elm, use_exist_stb=True)
            for ignore_expired in [None, 0, 1]:
                self.at_once_session(session=random.randint(10, 15), ignore_expired=ignore_expired)
            for ignore_update in [None, 0, 1]:
                self.at_once_session(session=random.randint(10, 15), ignore_update=ignore_update, fill_history_value=1)
            ## return
            for fill_history_value in [None, 1]:
                self.at_once_state_window(state_window="c1", partition="tbname", fill_history_value=fill_history_value)
                self.at_once_state_window(state_window="c1", partition="c1", fill_history_value=fill_history_value)
                self.at_once_state_window(state_window="c1", partition="abs(c1)", fill_history_value=fill_history_value)
                self.at_once_state_window(state_window="c1", partition="tbname", delete=True, fill_history_value=fill_history_value)
                self.at_once_state_window(state_window="c1", partition="c1", delete=True, fill_history_value=fill_history_value)
                self.at_once_state_window(state_window="c1", partition="abs(c1)", delete=True, fill_history_value=fill_history_value)
                self.at_once_state_window(state_window="c1", partition="c1", subtable=None, fill_history_value=fill_history_value)
                self.at_once_session(session=random.randint(10, 15), partition="tbname", fill_history_value=fill_history_value)
                self.at_once_session(session=random.randint(10, 15), partition="c1", fill_history_value=fill_history_value)
                self.at_once_session(session=random.randint(10, 15), partition="abs(c1)", fill_history_value=fill_history_value)
                self.at_once_session(session=random.randint(10, 15), partition="tbname", delete=True, fill_history_value=fill_history_value)
                self.at_once_session(session=random.randint(10, 15), partition="c1", delete=True, fill_history_value=fill_history_value)
                self.at_once_session(session=random.randint(10, 15), partition="abs(c1)", delete=True, fill_history_value=fill_history_value)
                self.at_once_session(session=random.randint(10, 15), partition="abs(c1)", delete=True, subtable=None, fill_history_value=fill_history_value)
                self.at_once_session(session=random.randint(10, 15), ignore_expired=1, fill_history_value=fill_history_value)
                self.at_once_session(session=random.randint(10, 15), ignore_update=1, fill_history_value=fill_history_value)
                #! TD-24620
                if fill_history_value != 1:
                    self.watermark_window_close_session(session=random.randint(10, 15), watermark=None, fill_history_value=fill_history_value)
                self.watermark_window_close_session(session=random.randint(10, 12), watermark=random.randint(20, 25), fill_history_value=fill_history_value)
                #! TD-24624	
                if fill_history_value != 1:
                    self.watermark_max_delay_session(session=random.randint(10, 15), watermark=None, max_delay=f"{random.randint(1, 3)}s", fill_history_value=fill_history_value)
                self.watermark_max_delay_session(session=random.randint(10, 15), watermark=random.randint(20, 30), max_delay=f"{random.randint(1, 3)}s", fill_history_value=fill_history_value)
            self.window_close_interval(interval=random.randint(10, 15), watermark=None)
            self.window_close_interval(interval=random.randint(10, 15), watermark=None, ignore_expired=0)
            self.window_close_interval(interval=random.randint(10, 15), watermark=random.randint(15, 20))
            self.window_close_state_window(state_window="c1")
            self.subtable_exceed_test()
            ## TODO not stable
            # self.watermark_max_delay_interval(interval=random.randint(10, 15), watermark=None, max_delay=f"{random.randint(5, 6)}s")
            # * in this case, when vgroups = 10, max_delay must be set upper than 4, root cause not found
            self.watermark_max_delay_interval(interval=random.choice([15]), watermark=random.randint(20, 25), max_delay=f"{random.randint(5, 6)}s")
            self.partitionby_interval(interval=None, partition_by_elm="tbname")
            self.partitionby_interval(interval=None, partition_by_elm="tbname", ignore_expired=0)
            self.partitionby_interval(interval=10, partition_by_elm="tbname")
            self.partitionby_interval(interval=10, partition_by_elm="tbname", ignore_expired=0)
            self.partitionby_interval(interval=10, partition_by_elm="t1")
            self.partition_tag_by_interval(interval=10, partition_by_elm="t1")

            # tmp remove fill_history
            self.at_once_session(session=random.randint(10, 15), partition="abs(c1)")
            self.watermark_window_close_session(session=random.randint(10, 15), watermark=None)
            self.watermark_window_close_session(session=random.randint(10, 12), watermark=random.randint(20, 25))


            # # * FILL
            for fill_value in ["NULL", "PREV", "NEXT", "LINEAR", "VALUE,1,2,3,4,5,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11"]:
                self.at_once_interval(interval=random.randint(10, 15), partition="tbname", fill_value=fill_value)
                self.at_once_interval(interval=random.randint(10, 15), partition="tbname", fill_value=fill_value, delete=True)
                self.watermark_max_delay_interval(interval=random.randint(10, 15), watermark=None, max_delay=f"{random.randint(5, 6)}s", fill_value=fill_value)
                for watermark in [None, random.randint(15, 20)]:
                    self.window_close_interval(interval=random.randint(10, 12), watermark=watermark, fill_value=fill_value)

            self.at_once_interval(interval=random.randint(10, 15), partition="tbname", fill_history_value=1, fill_value="NULL")
            # # TODO optimize，TD-22963 is a right case
            # !!!TD-24631
            # self.at_once_interval(interval=random.randint(10, 12), partition="c1", fill_value="NULL")
            # !!!TD-24631
            # self.at_once_interval(interval=random.randint(10, 15), partition="c1", fill_value="NULL", delete=True)

            self.at_once_state_window(state_window="c2", partition="tbname", case_when="case when c1 < 0 then c1 else c2 end")
            self.at_once_state_window(state_window="c1", partition="tbname", case_when="case when c1 >= 0 then c1 else c2 end")
            self.at_once_interval(interval=random.randint(10, 15), partition=self.stream_case_when_tbname, case_when=f'case when {self.stream_case_when_tbname} = tbname then {self.partition_tbname_alias} else tbname end')
            self.at_once_session(session=random.randint(10, 15), partition=self.stream_case_when_tbname, delete=True, case_when=f'case when {self.stream_case_when_tbname} = tbname then {self.partition_tbname_alias} else tbname end')
            ## for existed stb
            for delete in [True, False]:
                for fill_history_value in [0, 1]:
                    self.at_once_interval_ext(interval=random.randint(10, 15), delete=delete, fill_history_value=fill_history_value, partition=f'tbname,{self.tag_filter_des_select_elm.split(",")[0]},c1', subtable="c1", stb_field_name_value=self.tb_filter_des_select_elm, tag_value=self.tag_filter_des_select_elm.split(",")[0], use_exist_stb=True)
                    self.at_once_interval_ext(interval=random.randint(10, 15), delete=delete, fill_history_value=fill_history_value, partition=f'tbname,{self.tag_filter_des_select_elm},c1', subtable="c1", stb_field_name_value=None, tag_value=self.tag_filter_des_select_elm, use_exist_stb=True)
                    self.at_once_interval_ext(interval=random.randint(10, 15), delete=delete, fill_history_value=fill_history_value, partition=f'tbname,{self.tag_filter_des_select_elm},c1', stb_field_name_value=None, tag_value=self.tag_filter_des_select_elm, use_exist_stb=True)
                    self.at_once_interval_ext(interval=random.randint(10, 15), delete=delete, fill_history_value=fill_history_value, partition=f'tbname,{self.tag_filter_des_select_elm},c1', stb_field_name_value=self.tb_filter_des_select_elm, tag_value=self.tag_filter_des_select_elm, use_exist_stb=True)
                    self.at_once_interval_ext(interval=random.randint(10, 15), delete=delete, fill_history_value=fill_history_value, partition=f'tbname,{self.tag_filter_des_select_elm.split(",")[0]},c1', subtable="c1", stb_field_name_value=self.partitial_stb_filter_des_select_elm, tag_value=self.tag_filter_des_select_elm.split(",")[0], use_exist_stb=True)
                    self.at_once_interval_ext(interval=random.randint(10, 15), delete=delete, fill_history_value=fill_history_value, partition=f'tbname,{self.tag_filter_des_select_elm.split(",")[0]},c1', subtable="c1", stb_field_name_value=self.exchange_stb_filter_des_select_elm, tag_value=self.tag_filter_des_select_elm.split(",")[0], use_exist_stb=True)
                    self.at_once_interval_ext(interval=random.randint(10, 15), delete=delete, fill_history_value=fill_history_value, partition=None, subtable=None, stb_field_name_value=self.tb_filter_des_select_elm, tag_value=self.tag_filter_des_select_elm.split(",")[0], use_exist_stb=True)
                    self.at_once_state_window_ext(state_window="c1", delete=delete, fill_history_value=fill_history_value, partition=f'tbname,{self.tag_filter_des_select_elm},c1', subtable="c1", stb_field_name_value=self.tb_filter_des_select_elm, tag_value=self.tag_filter_des_select_elm.split(",")[0], use_exist_stb=True)
                    self.at_once_state_window_ext(state_window="c1", delete=delete, fill_history_value=fill_history_value, partition=f'tbname,{self.tag_filter_des_select_elm},c1', subtable="c1", stb_field_name_value=None, tag_value=self.tag_filter_des_select_elm, use_exist_stb=True)
                    self.watermark_window_close_session_ext(session=random.randint(10, 12), watermark=random.randint(20, 25), subtable=None, partition=None, stb_field_name_value=self.tb_filter_des_select_elm, tag_value=self.tag_filter_des_select_elm.split(",")[0], use_exist_stb=True)

                    self.at_once_session_ext(session=random.randint(10, 15), delete=False, fill_history_value=fill_history_value, subtable="c1", partition=f'tbname,{self.tag_filter_des_select_elm.split(",")[0]},c1', stb_field_name_value=self.tb_filter_des_select_elm, tag_value=self.tag_filter_des_select_elm.split(",")[0], use_exist_stb=True)
                    self.watermark_max_delay_interval_ext(interval=random.choice([15]), watermark=random.randint(20, 25), max_delay=f"{random.randint(5, 6)}s", delete=delete, fill_history_value=fill_history_value, partition=None, subtable=None, stb_field_name_value=self.tb_filter_des_select_elm, tag_value=self.tag_filter_des_select_elm.split(",")[0], use_exist_stb=True)
                    # self-define tag
                    self.at_once_interval_ext(interval=random.randint(10, 15), delete=delete, fill_history_value=fill_history_value, partition=f'{self.tag_filter_des_select_elm}', subtable=None, stb_field_name_value=None, tag_value=self.tag_filter_des_select_elm, use_exist_stb=True)
                    self.at_once_interval_ext(interval=random.randint(10, 15), delete=delete, fill_history_value=fill_history_value, partition=f'{self.partitial_tag_filter_des_select_elm}', subtable=None, stb_field_name_value=None, tag_value=self.partitial_tag_filter_des_select_elm, use_exist_stb=True)
                    self.at_once_interval_ext(interval=random.randint(10, 15), delete=delete, fill_history_value=fill_history_value, partition=f'{self.partitial_tag_filter_des_select_elm}', subtable=None, stb_field_name_value=None, tag_value=self.exchange_tag_filter_des_select_elm, use_exist_stb=True)
                    self.at_once_interval_ext(interval=random.randint(10, 15), delete=delete, fill_history_value=fill_history_value, partition="t1 as t5,t2 as t11,t3 as t13", subtable=None, stb_field_name_value=None, tag_value="t5,t11,t13", use_exist_stb=True)
                    self.at_once_interval_ext(interval=random.randint(10, 15), delete=delete, fill_history_value=fill_history_value, partition=None, subtable=None, stb_field_name_value=self.tb_filter_des_select_elm, tag_value=None, use_exist_stb=True)
                    self.at_once_interval_ext(interval=random.randint(10, 15), delete=delete, fill_history_value=fill_history_value, partition=None, subtable=None, stb_field_name_value=self.tb_filter_des_select_elm, tag_value="t1", use_exist_stb=True)
            # error cases
            self.at_once_interval_ext(interval=random.randint(10, 15), partition=f'tbname,{self.tag_filter_des_select_elm},c1', stb_field_name_value="", tag_value=self.tag_filter_des_select_elm, use_exist_stb=True, use_except=True)
            self.at_once_interval_ext(interval=random.randint(10, 15), partition=f'tbname,{self.tag_filter_des_select_elm},c1', stb_field_name_value=self.tb_filter_des_select_elm.replace("c1","c19"), tag_value=self.tag_filter_des_select_elm, use_exist_stb=True, use_except=True)
            self.at_once_interval_ext(interval=random.randint(10, 15), partition=f'tbname', subtable="c1", stb_field_name_value=self.tb_filter_des_select_elm, tag_value=self.tag_filter_des_select_elm.split(",")[0], use_exist_stb=True, use_except=True)
            self.at_once_interval_ext(interval=random.randint(10, 15), partition=f'tbname,{self.tag_filter_des_select_elm},c1', subtable="ttt", stb_field_name_value=self.tb_filter_des_select_elm, tag_value=self.tag_filter_des_select_elm.split(",")[0], use_exist_stb=True, use_except=True)
            self.at_once_interval_ext(interval=random.randint(10, 15), partition=f'tbname,{self.tag_filter_des_select_elm},c1', subtable="c1", stb_field_name_value=self.tb_filter_des_select_elm, tag_value=None, use_exist_stb=True, use_except=True)
            self.at_once_interval_ext(interval=random.randint(10, 15), partition=f'tbname,{self.tag_filter_des_select_elm},c1', subtable="c1", stb_field_name_value=self.tb_filter_des_select_elm, tag_value="t15", use_exist_stb=True, use_except=True)
            self.at_once_interval_ext(interval=random.randint(10, 15), partition=f'tbname,{self.tag_filter_des_select_elm},c1', subtable="c1", stb_field_name_value=self.tb_filter_des_select_elm, tag_value="c5", use_exist_stb=True, use_except=True)
            self.at_once_interval_ext(interval=random.randint(10, 15), partition=f'tbname,{self.tag_filter_des_select_elm.split(",")[0]},c1', subtable="c1", stb_field_name_value="ts,c1,c2,c3", tag_value=self.tag_filter_des_select_elm.split(",")[0], use_exist_stb=True, use_except=True)
            self.at_once_interval_ext(interval=random.randint(10, 15), partition=f'tbname,{self.tag_filter_des_select_elm.split(",")[0]},c1', subtable="c1", stb_field_name_value="ts,c1", tag_value=self.tag_filter_des_select_elm.split(",")[0], use_exist_stb=True, use_except=True)
            self.at_once_interval_ext(interval=random.randint(10, 15), partition=f'tbname,{self.tag_filter_des_select_elm.split(",")[0]},c1', subtable="c1", stb_field_name_value="c1,c2,c3", tag_value=self.tag_filter_des_select_elm.split(",")[0], use_exist_stb=True, use_except=True)
            self.at_once_interval_ext(interval=random.randint(10, 15), delete=False, fill_history_value=1, partition="t1 as t5,t2 as t11", subtable=None, stb_field_name_value=self.tb_filter_des_select_elm, tag_value="t5,t11,t13", use_exist_stb=True, use_except=True)
            self.at_once_interval_ext(interval=random.randint(10, 15), delete=False, fill_history_value=1, partition="t1 as t5,t2 as t11,t3 as t14", subtable=None, stb_field_name_value=self.tb_filter_des_select_elm, tag_value="t5,t11,t13", use_exist_stb=True, use_except=True)
            self.at_once_interval_ext(interval=random.randint(10, 15), delete=False, fill_history_value=1, partition="t1 as t5,t2 as t11,t3 as c13", subtable=None, stb_field_name_value=self.tb_filter_des_select_elm, tag_value="t5,t11,c13", use_exist_stb=True, use_except=True)


            # pause/resume
            self.pause_resume_test(interval=random.randint(10, 15), partition="tbname", ignore_untreated=False)
            self.pause_resume_test(interval=random.randint(10, 15), partition="tbname", ignore_untreated=True)
            self.pause_resume_test(interval=random.randint(10, 15), partition="tbname", resume=False)
            # ! TD-23905
            # self.json_function(partition="tbname", delete=True, fill_history_value=1)

            # * FILL
            # self.at_once_interval(interval=random.randint(10, 15), partition="tbname", fill_history_value=1, fill_value="NULL")
            # self.at_once_interval(interval=random.randint(10, 15), partition="c1", fill_value="NULL")
            # self.at_once_interval(interval=random.randint(10, 15), partition="c1", fill_value="NULL", delete=True)



            # self.window_close_interval(interval=random.randint(10, 15), watermark=None, fill_value="NULL")
            # self.window_close_interval(interval=random.randint(10, 15), watermark=None, fill_value="NEXT")
            # self.window_close_interval(interval=random.randint(10, 15), watermark=None, fill_value="PREV")
            # self.window_close_interval(interval=random.randint(10, 15), watermark=None, fill_value="LINEAR")
            # self.window_close_interval(interval=random.randint(10, 12), watermark=None, fill_value="VALUE,1,2,3,4,5,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11")
            # self.window_close_interval(interval=random.randint(10, 15), watermark=random.randint(15, 20), fill_value="NULL")
            # self.window_close_interval(interval=random.randint(10, 15), watermark=random.randint(15, 20), fill_value="NEXT")
            # self.window_close_interval(interval=random.randint(10, 15), watermark=random.randint(15, 20), fill_value="PREV")
            # self.window_close_interval(interval=random.randint(10, 15), watermark=random.randint(15, 20), fill_value="LINEAR")
            # self.window_close_interval(interval=random.randint(10, 12), watermark=random.randint(15, 20), fill_value="VALUE,1,2,3,4,5,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11")
            # self.window_close_interval(interval=random.randint(10, 15), watermark=None, fill_value="NULL", delete=True)
            # self.window_close_interval(interval=random.randint(10, 15), watermark=None, fill_value="NEXT", delete=True)
            # self.window_close_interval(interval=random.randint(10, 15), watermark=None, fill_value="PREV", delete=True)
            # self.window_close_interval(interval=random.randint(10, 15), watermark=None, fill_value="LINEAR", delete=True)
            # self.window_close_interval(interval=random.randint(10, 12), watermark=None, fill_value="VALUE,1,2,3,4,5,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11", delete=True)
            # self.window_close_interval(interval=random.randint(10, 15), watermark=random.randint(15, 20), fill_value="NULL", delete=True)
            # self.window_close_interval(interval=random.randint(10, 15), watermark=random.randint(15, 20), fill_value="NEXT", delete=True)
            # self.window_close_interval(interval=random.randint(10, 15), watermark=random.randint(15, 20), fill_value="PREV", delete=True)
            # self.window_close_interval(interval=random.randint(10, 15), watermark=random.randint(15, 20), fill_value="LINEAR", delete=True)
            # self.window_close_interval(interval=random.randint(10, 12), watermark=random.randint(15, 20), fill_value="VALUE,1,2,3,4,5,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11", delete=True)

            # TODO to be supported
        # self.vgroups = 2
        # self.partitionby_interval(interval=10, partition_by_elm="c1")
        # self.partitionby_interval(interval=10, partition_by_elm="abs(t1)")
        # ! TD-19836
        # self.partitionby_interval(interval=10, partition_by_elm="abs(c1)")

    def cleanup(self):
        pass

    def desc(self):
        case_description = """
            stream computing <jayden>: [TD-16143] : stream computing function test;
            """
        return case_description

    def author(self):
        return "Jayden"

    def tags(self):
        return T.Write.Stream
