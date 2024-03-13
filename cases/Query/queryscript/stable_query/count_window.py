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

from taostest import TDCase
from taostest.util.common import TDCom
import time

class TestCountWindow(TDCase):
    def init(self):
        self.firstEP = list()
        self.dbname = "test"
        for env_setting in self.env_setting["settings"]:
            if env_setting["name"].lower() == "taosd":
                self.taosd_setting = env_setting
                self.firstEP.append(
                    self.taosd_setting['spec']['config']['firstEP'])
        self.target_taosd = self.firstEP[-1].split(':')
        print(self.target_taosd[0])
        self.service_host = self.target_taosd[0]
        self.tdCom = TDCom(self.tdSql)
        self.count_window_vol_list = [2, 6]
        self.tbname_check_list = ["stb0", "ctb0", "tb"]
        self.function_list = ["min(c1)", "max(c2)", "sum(c3)", "first(c4)", "last(c5)", "apercentile(c6, 50)", "avg(c7)", "count(c8)", "spread(c1)", 
        "stddev(c2)", "hyperloglog(c11)", "timediff(1, 0, 1h)", "timezone()", "to_iso8601(1)", 'to_unixtimestamp("1970-01-01T08:00:00+08:00")', "min(t1)", "max(t2)", "sum(t3)",
        "first(t4)", "last(t5)", "apercentile(t6, 50)", "avg(t7)", "count(t8)", "spread(t1)", "stddev(t2)", "hyperloglog(t11)"]
        self.nfl_function_list = ["min(c1)", "max(c2)", "sum(c3)", "apercentile(c6, 50)", "avg(c7)", "count(c8)", "spread(c1)", 
        "stddev(c2)", "hyperloglog(c11)", "timediff(1, 0, 1h)", "timezone()", "to_iso8601(1)", 'to_unixtimestamp("1970-01-01T08:00:00+08:00")', "min(t1)", "max(t2)", "sum(t3)",
        "apercentile(t6, 50)", "avg(t7)", "count(t8)", "spread(t1)", "stddev(t2)", "hyperloglog(t11)"]
        self.stb_source_select_str = ','.join(self.function_list)
        self.tb_source_select_str = ','.join(self.function_list[0:15])
        self.nfl_stb_source_select_str = ','.join(self.nfl_function_list)
        self.nfl_tb_source_select_str = ','.join(self.nfl_function_list[0:13])

    def tags(self) -> str:
         
        return ""
    
    def author(self) -> str:
         
        return "Jayden"

    def desc(self) -> str:
        case_description = '''
        ---
        ''' 
        return case_description

    def no_dup_ts_test(self, stable_count, ctable_count, table_count, row_count, custom_col_index, col_value_type):
        self.tdCom.prepare_all_type_data(dbname=self.dbname, stable_count=stable_count, ctable_count=ctable_count, table_count=table_count, row_count=row_count, custom_col_index=custom_col_index, col_value_type=col_value_type)
        for count_window_vol in self.count_window_vol_list:
            for tbname in self.tbname_check_list:
                if "stb" in tbname:
                    # # # No partition
                    # count_window_sql = f'select _wstart, _wend, {self.stb_source_select_str} from {tbname} count_window({count_window_vol})'
                    # self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.stb_source_select_str)
                    # # Partition by tbname
                    # count_window_sql = f'select _wstart, _wend, {self.nfl_stb_source_select_str} from {tbname} partition by tbname count_window({count_window_vol}) order by _wstart'
                    # self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.nfl_stb_source_select_str, "", "tbname")
                    # Partition by column
                    count_window_sql = f'select _wstart, _wend, {self.nfl_stb_source_select_str} from {tbname} partition by c3 count_window({count_window_vol}) order by _wstart'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.nfl_stb_source_select_str, "", "c3")
                else:
                    # # # No partition
                    # count_window_sql = f'select _wstart, _wend, {self.tb_source_select_str} from {tbname} count_window({count_window_vol})'
                    # self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.tb_source_select_str)
                    # # Partition by tbname
                    # count_window_sql = f'select _wstart, _wend, {self.nfl_tb_source_select_str} from {tbname} partition by tbname count_window({count_window_vol}) order by _wstart'
                    # self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.nfl_tb_source_select_str, "", "tbname")
                    # Partition by column
                    count_window_sql = f'select _wstart, _wend, {self.nfl_tb_source_select_str} from {tbname} partition by c3 count_window({count_window_vol}) order by _wstart'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.nfl_tb_source_select_str, "", "c3")
        
        
        
    def dup_ts_test(self):
        self.tdCom.prepare_all_type_data(dbname=self.dbname, stable_count=1, ctable_count=2, table_count=1, row_count=10, custom_col_index=2, col_value_type="Incremental")
        
                                       
                                         
    def run(self)-> bool:
        
        self.no_dup_ts_test(stable_count=1, ctable_count=2, table_count=1, row_count=10, custom_col_index=2, col_value_type="Incremental")
        # startTime = time.time() 
        # self.data_create(self.db)
        return
