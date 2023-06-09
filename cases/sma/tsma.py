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
import sys
import time
class TsmaTest(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.precision = "ms"
        self.case_name = str()
        self.dbname = "sma_test"
        self.stb_name = str()
        self.ctb_name = str()
        self.tb_name = str()
        self.stb_sma_des_table = str()
        self.ctb_sma_des_table = str()
        self.tb_sma_des_table = str()

        self.default_interval = 15
        self.range_count = 5
        self.vgroups = 2
        self.vgroups_list = [1, self.vgroups]
        self.sma_suffix = "_sma"
        self.querySmaOptimize = 1


        # self.tsma_function_list = ["min(c1)", "max(c2)", "sum(c3)", "first(c4)", "last(c5)", "apercentile(c6, 50)", "avg(c7)", "count(c8)", "spread(c1)", "stddev(c2)", "hyperloglog(c11)", 
        #                         "min(t1)", "max(t2)", "sum(t3)", "first(t4)", "last(t5)", "apercentile(t6, 50)", "avg(t7)", "count(t8)", "spread(t1)", "stddev(t2)", "hyperloglog(t11)"]
        self.tsma_function_list = ["min(c1)", "max(c2)", "sum(c3)", "apercentile(c6, 50)", "avg(c7)", "spread(c1)", "stddev(c2)", "hyperloglog(c11)", 
                                "min(t1)", "max(t2)", "sum(t3)", "apercentile(t6, 50)", "avg(t7)", "spread(t1)", "stddev(t2)", "hyperloglog(t11)"]
        self.tsma_function = ','.join(self.tsma_function_list)

        self.date_time = self.tdCom.genTs(precision=self.precision)[0]

    def set_precision_offset(self, precision):
        if precision == "ms":
            self.offset = 1000
        elif precision == "us":
            self.offset = 1000000
        elif precision == "ns":
            self.offset = 1000000000
        else:
            pass

    def clean_env(self):
        self.tdCom.drop_all_streams()
        self.tdCom.drop_all_db()

    def prepare_data(self, interval=None, watermark=None, range_count=None, precision="ms"):
        self.clean_env()
        self.alter_tsma_optimize()
        self.dataDict = {
            "stb_name" : f"{self.case_name}_stb",
            "ctb_name" : f"{self.case_name}_ct1",
            "tb_name" : f"{self.case_name}_tb1",
            "interval" : interval,
            "watermark": watermark,
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
        self.date_time = self.tdCom.genTs(precision=self.precision)[0]
        
        self.tdCom.createDb(dbname=self.dbname, vgroups=self.vgroups, precision=self.precision)
        self.tdCom.create_stable(dbname=self.dbname, stbname=self.stb_name)
        self.tdCom.create_ctable(dbname=self.dbname, stbname=self.stb_name, ctbname=self.ctb_name)
        self.tdCom.create_table(dbname=self.dbname, tbname=self.tb_name)
        for i in range(self.range_count):
            ts_value = str(self.date_time)+f'-{self.default_interval*(i+1)}s'
            self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=ts_value)
            self.tdCom.insert_rows(tbname=self.tb_name, ts_value=ts_value)

    def alter_tsma_optimize(self, querySmaOptimize=0):
        self.tdSql.execute(f'alter local "querySmaOptimize" "{querySmaOptimize}"')

    def insert_update_delete_rows(self):
        count = 1
        step_count = 1
        for i in range(self.range_count):
            if i % 2 == 0:
                step_count += i
                for j in range(count, step_count):
                    self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=f'{self.date_time}+{j}s')
                    self.tdCom.insert_rows(tbname=self.tb_name, ts_value=f'{self.date_time}+{j}s')
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
                    self.tdCom.delete_rows(tbname=self.ctb_name, start_ts=ts_cast_delete_value)
                    self.tdCom.delete_rows(tbname=self.tb_name, start_ts=ts_cast_delete_value)
                count += 1

    def tsma_interval_test(self, interval="1s"):
        self.case_name = sys._getframe().f_code.co_name

        self.prepare_data()
        self.tdCom.create_sma(sma_name=f'{self.stb_name}{self.sma_suffix}', stb_name=self.stb_name, function_value=self.tsma_function, interval_value=interval)
        self.insert_update_delete_rows()
        query_sql = f'select {self.tsma_function} from {self.stb_name} interval({interval}) order by `min(c1)`,`max(c2)`'
        self.tdSql.query(query_sql)
        no_sma_res = self.tdSql.query_data
        self.alter_tsma_optimize(1)
        self.tdCom.check_tsma_res(query_sql, no_sma_res, None)

    def tsma_sliding_test(self, interval="10s", sliding="5s"):
        self.case_name = sys._getframe().f_code.co_name

        self.prepare_data()
        self.tdCom.create_sma(sma_name=f'{self.stb_name}{self.sma_suffix}', stb_name=self.stb_name, function_value=self.tsma_function, interval_value=interval, sliding_value=sliding)
        self.insert_update_delete_rows()
        query_sql = f'select {self.tsma_function} from {self.stb_name} interval({interval}) sliding({sliding}) order by `min(c1)`,`max(c2)`'
        self.tdSql.query(query_sql)
        no_sma_res = self.tdSql.query_data
        self.alter_tsma_optimize(1)
        self.tdCom.check_tsma_res(query_sql, no_sma_res, None)

    def tsma_watermark_max_delay_test(self, interval="10s", sliding="5s", watermark="10s", max_delay="3s"):
        self.case_name = sys._getframe().f_code.co_name
        if interval is None:
            _interval = ""
        else:
            _interval = f'interval({interval})'

        if sliding is None:
            _sliding = ""
        else:
            _sliding = f'sliding({sliding})'

        self.prepare_data()
        self.tdCom.create_sma(sma_name=f'{self.stb_name}{self.sma_suffix}', stb_name=self.stb_name, function_value=self.tsma_function, interval_value=interval, sliding_value=sliding, watermark_value=watermark, max_delay_value=max_delay)
        self.insert_update_delete_rows()

        query_sql = f'select {self.tsma_function} from {self.stb_name} {_interval} {_sliding} order by `min(c1)`,`max(c2)`'
        self.tdSql.query(query_sql)
        time.sleep(int(''.join(list(filter(str.isdigit, max_delay)))))
        no_sma_res = self.tdSql.query_data
        self.alter_tsma_optimize(1)
        self.tdCom.check_tsma_res(query_sql, no_sma_res, None)

    def delete_vnode_test(self, interval="1s"):
        self.case_name = sys._getframe().f_code.co_name

        self.prepare_data()
        self.alter_tsma_optimize(1)
        self.tdCom.create_sma(sma_name=f'{self.stb_name}{self.sma_suffix}', stb_name=self.stb_name, function_value=self.tsma_function, interval_value=interval)
        for i in range(self.range_count*10):
            self.tdCom.insert_rows(tbname=self.ctb_name, ts_value=f'{self.date_time}+{i}s')
        vgroup_id_list = self.tdCom.get_vgroup_id_list(self.dbname)
        # query_sql = f'select {self.tsma_function} from {self.stb_name} interval({interval})'
        # self.tdSql.query(query_sql)
        # no_sma_res = self.tdSql.query_data
        # self.alter_tsma_optimize(1)
        # self.tdCom.check_tsma_res(query_sql, no_sma_res, None)
    
    def recreate_tsma_after_drop(self, interval="1s"):
        self.case_name = sys._getframe().f_code.co_name

        self.prepare_data()
        self.tdCom.create_sma(sma_name=f'{self.stb_name}{self.sma_suffix}', stb_name=self.stb_name, function_value=self.tsma_function, interval_value=interval)
        self.insert_update_delete_rows()
        query_sql = f'select {self.tsma_function} from {self.stb_name} interval({interval}) order by `min(c1)`,`max(c2)`'
        self.tdSql.query(query_sql)
        no_sma_res = self.tdSql.query_data
        self.alter_tsma_optimize(1)
        self.tdCom.check_tsma_res(query_sql, no_sma_res, None)
        self.tdCom.drop_all_smas()
        self.tdCom.create_sma(sma_name=f'{self.stb_name}{self.sma_suffix}', stb_name=self.stb_name, function_value=self.tsma_function, interval_value=interval)
        self.tdCom.check_tsma_res(query_sql, no_sma_res, None)

    def partition_by_unsupported(self):
        self.case_name = sys._getframe().f_code.co_name
        self.prepare_data()
        self.tdSql.error(f'create sma index if not exists {self.stb_name}{self.sma_suffix} on self.stb_name function({self.tsma_function}) partition by tbname interval(1s)')


    def drop_sma_stb(self, interval="1s"):
        self.case_name = sys._getframe().f_code.co_name

        self.prepare_data()
        self.alter_tsma_optimize(1)
        self.tdCom.create_sma(sma_name=f'{self.stb_name}{self.sma_suffix}', stb_name=self.stb_name, function_value=self.tsma_function, interval_value=interval)
        self.insert_update_delete_rows()
        self.tdSql.execute(f'drop stable {self.stb_name}')
        self.tdSql.query('show streams')
        self.tdSql.checkEqual(self.tdSql.query_row, 0)

    def drop_sma_db(self, interval="1s"):
        self.case_name = sys._getframe().f_code.co_name

        self.prepare_data()
        self.alter_tsma_optimize(1)
        self.tdCom.create_sma(sma_name=f'{self.stb_name}{self.sma_suffix}', stb_name=self.stb_name, function_value=self.tsma_function, interval_value=interval)
        self.insert_update_delete_rows()
        self.tdSql.execute(f'drop database {self.dbname}')
        self.tdSql.query('show streams')
        self.tdSql.checkEqual(self.tdSql.query_row, 0)

    def test(self):
        # self.tsma_sliding_test()
        return
    def run(self):
        # self.test()
        # return
        self.tsma_interval_test()
        self.tsma_sliding_test()
        self.tsma_watermark_max_delay_test(sliding=None)
        self.tsma_watermark_max_delay_test(sliding="5s")
        self.delete_vnode_test()
        self.recreate_tsma_after_drop()
        self.partition_by_unsupported()
        self.drop_sma_stb()
        self.drop_sma_db()

    def cleanup(self):
        pass

    def desc(self):
        case_description = """
            tsma <jayden>: [TD-20574] : tsma function test;
            """
        return case_description

    def author(self):
        return "Jayden"

    def tags(self):
        return T.Write
