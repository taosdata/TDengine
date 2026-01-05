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
import random

class TestCountWindow(TDCase):
    def init(self):
        self.dbname = "test"
        self.alias_name = "m"
        self.tdCom = TDCom(self.tdSql)
        self.count_window_vol_list = [2, 6]
        self.tbname_check_list = ["stb0", "ctb0", "tb"]
        self.function_list = [f"min(c1) {self.alias_name}", "max(c2)", "sum(c3)", "first(c4)", "last(c5)", "apercentile(c6, 50)", "avg(c7)", "count(c8)", "spread(c1)", 
        "stddev(c2)", "hyperloglog(c11)", "timediff(1, 0, 1h)", "timezone()", "to_iso8601(1)", 'to_unixtimestamp("1970-01-01T08:00:00+08:00")', "min(t1)", "max(t2)", "sum(t3)",
        "first(t4)", "last(t5)", "apercentile(t6, 50)", "avg(t7)", "count(t8)", "spread(t1)", "stddev(t2)", "hyperloglog(t11)"]
        self.nfl_function_list = [f"min(c1) {self.alias_name}", "max(c2)", "sum(c3)", "apercentile(c6, 50)", "avg(c7)", "count(c8)", "spread(c1)", 
        "stddev(c2)", "hyperloglog(c11)", "timediff(1, 0, 1h)", "timezone()", "to_iso8601(1)", 'to_unixtimestamp("1970-01-01T08:00:00+08:00")', "min(t1)", "max(t2)", "sum(t3)",
        "apercentile(t6, 50)", "avg(t7)", "count(t8)", "spread(t1)", "stddev(t2)", "hyperloglog(t11)"]
        self.stb_source_select_str = ','.join(self.function_list)
        self.tb_source_select_str = ','.join(self.function_list[0:15])
        self.nfl_stb_source_select_str = ','.join(self.nfl_function_list)
        self.nfl_tb_source_select_str = ','.join(self.nfl_function_list[0:13])
        self.ts_list = list()
        self.disorder = True
        self.delete = True

    def tags(self) -> str:
        return ""

    def author(self) -> str:
        return "Jayden"

    def desc(self) -> str:
        case_description = '''
        ---
        '''
        return case_description

    def get_random_ts(self, tbname):
        self.tdSql.query(f'select * from {self.dbname}.{tbname};')
        self.ts_list = list(map(lambda x:x[0], self.tdSql.query_data))
        return str(random.choice(self.ts_list))

    def no_dup_ts_test(self, stable_count, ctable_count, table_count, row_count, custom_col_index, col_value_type, where_condition=None, having_elm=None, having_condition=None, alias_name=None, sliding=None):
        condition_vol = "" if where_condition is None else f"where {where_condition}"
        # having_condition_vol = "" if having_condition is None else f"having {having_elm} {having_condition}"
        self.tdCom.prepare_all_type_data(dbname=self.dbname, stable_count=stable_count, ctable_count=ctable_count, table_count=table_count, row_count=row_count, custom_col_index=custom_col_index, col_value_type=col_value_type)
        for count_window_vol in self.count_window_vol_list:
            for tbname in self.tbname_check_list:
                if "stb" in tbname:
                    if self.disorder:
                        self.tdCom.insert_rows(dbname=self.dbname, tbname=self.tbname_check_list[1], ts_value=f'"{self.get_random_ts(self.tbname_check_list[1])}"', additional_ts=True)
                    if self.delete:
                        self.tdCom.delete_rows(dbname=self.dbname, tbname=self.tbname_check_list[1], start_ts=f'"{self.get_random_ts(self.tbname_check_list[1])}"')
                    # # No partition
                    count_window_sql = f'select _wstart, _wend, {self.stb_source_select_str} from {tbname} count_window({count_window_vol})'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.stb_source_select_str)
                    # Partition by tbname
                    count_window_sql = f'select _wstart, _wend, {self.nfl_stb_source_select_str} from {tbname} partition by tbname count_window({count_window_vol}) order by _wstart'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.nfl_stb_source_select_str, "", "tbname")
                    ''' 3.4.0.0 do not support duplicte timestamp on count_window 
                    # Partition by no-dup column
                    count_window_sql = f'select _wstart, _wend, {self.nfl_stb_source_select_str} from {tbname} partition by c3 count_window({count_window_vol}) order by _wstart'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.nfl_stb_source_select_str, "", "c3", custom_col_index)
                    # Partition by tag
                    count_window_sql = f'select _wstart, _wend, {self.nfl_stb_source_select_str} from {tbname} partition by t3 count_window({count_window_vol}) order by _wstart'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.nfl_stb_source_select_str, "", "t3", custom_col_index)
                    # Partition by expression
                    partition_vol = "abs(c3)"
                    count_window_sql = f'select _wstart, _wend, {self.nfl_stb_source_select_str} from {tbname} partition by {partition_vol} count_window({count_window_vol}) order by _wstart'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.nfl_stb_source_select_str, "", partition_vol, custom_col_index)
                    '''
                    # Filter
                    count_window_sql = f'select _wstart, _wend, {self.stb_source_select_str} from {tbname} {condition_vol} count_window({count_window_vol})'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.stb_source_select_str, where_condition=condition_vol)
                    # Having
                    # No partition
                    count_window_sql = f'select _wstart, _wend, {self.stb_source_select_str} from {tbname} count_window({count_window_vol})'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.stb_source_select_str, having_elm=having_elm, having_condition=having_condition, alias_name=alias_name)
                    # Partition by tbname
                    count_window_sql = f'select _wstart, _wend, {self.nfl_stb_source_select_str} from {tbname} partition by tbname count_window({count_window_vol}) order by _wstart'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.nfl_stb_source_select_str, "", "tbname", having_elm=having_elm, having_condition=having_condition, alias_name=alias_name)

                    ''' 3.4.0.0 do not support duplicte timestamp on count_window 
                    # # Partition by no-dup column
                    count_window_sql = f'select _wstart, _wend, {self.nfl_stb_source_select_str} from {tbname} partition by c3 count_window({count_window_vol}) order by _wstart'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.nfl_stb_source_select_str, "", "c3", custom_col_index, having_elm=having_elm, having_condition=having_condition, alias_name=alias_name)
                    # # Partition by tag
                    count_window_sql = f'select _wstart, _wend, {self.nfl_stb_source_select_str} from {tbname} partition by t3 count_window({count_window_vol}) order by _wstart'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.nfl_stb_source_select_str, "", "t3", custom_col_index, having_elm=having_elm, having_condition=having_condition, alias_name=alias_name)
                    # # Partition by tag, column
                    count_window_sql = f'select _wstart, _wend, {self.nfl_stb_source_select_str} from {tbname} partition by t3,c3 count_window({count_window_vol}) order by _wstart'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.nfl_stb_source_select_str, "", "t3,c3", custom_col_index, having_elm=having_elm, having_condition=having_condition, alias_name=alias_name)
                    '''

                    # _wcol
                    count_window_sql = f'select _wstart, _wend, _wduration, _qstart, _qend, {self.stb_source_select_str} from {tbname} count_window({count_window_vol})'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.stb_source_select_str, having_elm=having_elm, having_condition=having_condition, alias_name=alias_name)
                    # # sliding
                    count_window_sql = f'select _wstart, _wend, {self.stb_source_select_str} from {tbname} count_window({count_window_vol},{sliding})'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.stb_source_select_str, sliding=sliding)
                    ## union
                    count_window_sql = f'select _wstart, _wend, {self.stb_source_select_str} from {tbname} count_window({count_window_vol})'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.stb_source_select_str, sliding=sliding, union=True)
                else:
                    if self.disorder:
                        self.tdCom.insert_rows(dbname=self.dbname, tbname=tbname, ts_value=f'"{self.get_random_ts(tbname)}"', additional_ts=True)
                    if self.delete:
                        self.tdCom.delete_rows(dbname=self.dbname, tbname=tbname, start_ts=f'"{self.get_random_ts(tbname)}"')
                    # # No partition
                    count_window_sql = f'select _wstart, _wend, {self.tb_source_select_str} from {tbname} count_window({count_window_vol})'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.tb_source_select_str)
                    # Partition by tbname
                    count_window_sql = f'select _wstart, _wend, {self.nfl_tb_source_select_str} from {tbname} partition by tbname count_window({count_window_vol}) order by _wstart'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.nfl_tb_source_select_str, "", "tbname")

                    ''' 3.4.0.0 do not support duplicte timestamp on count_window 
                    # Partition by no-dup column
                    count_window_sql = f'select _wstart, _wend, {self.nfl_tb_source_select_str} from {tbname} partition by c3 count_window({count_window_vol}) order by _wstart'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.nfl_tb_source_select_str, "", "c3", custom_col_index)
                    # Partition by tag
                    if tbname != "tb":
                        count_window_sql = f'select _wstart, _wend, {self.nfl_tb_source_select_str} from {tbname} partition by t3 count_window({count_window_vol}) order by _wstart'
                        self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.nfl_tb_source_select_str, "", "t3", custom_col_index)
                    # Partition by expression
                    partition_vol = "abs(c3)"
                    count_window_sql = f'select _wstart, _wend, {self.nfl_tb_source_select_str} from {tbname} partition by {partition_vol} count_window({count_window_vol}) order by _wstart'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.nfl_tb_source_select_str, "", partition_vol, custom_col_index)
                    '''

                    # Filter
                    count_window_sql = f'select _wstart, _wend, {self.tb_source_select_str} from {tbname} {condition_vol} count_window({count_window_vol})'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.tb_source_select_str, where_condition=condition_vol)
                    # Having
                    # No partition
                    count_window_sql = f'select _wstart, _wend, {self.tb_source_select_str} from {tbname} count_window({count_window_vol})'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.tb_source_select_str, having_elm=having_elm, having_condition=having_condition, alias_name=alias_name)
                    # Partition by tbname
                    count_window_sql = f'select _wstart, _wend, {self.nfl_tb_source_select_str} from {tbname} partition by tbname count_window({count_window_vol}) order by _wstart'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.nfl_tb_source_select_str, "", "tbname", having_elm=having_elm, having_condition=having_condition, alias_name=alias_name)

                    ''' 3.4.0.0 do not support duplicte timestamp on count_window 
                    # # Partition by no-dup column
                    count_window_sql = f'select _wstart, _wend, {self.nfl_tb_source_select_str} from {tbname} partition by c3 count_window({count_window_vol}) order by _wstart'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.nfl_tb_source_select_str, "", "c3", custom_col_index, having_elm=having_elm, having_condition=having_condition, alias_name=alias_name)
                    # Partition by tag
                    if tbname != "tb":
                        count_window_sql = f'select _wstart, _wend, {self.nfl_tb_source_select_str} from {tbname} partition by t3 count_window({count_window_vol}) order by _wstart'
                        self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.nfl_tb_source_select_str, "", "t3", custom_col_index, having_elm=having_elm, having_condition=having_condition, alias_name=alias_name)
                    # # Partition by tag, column
                    if tbname != "tb":
                        count_window_sql = f'select _wstart, _wend, {self.nfl_tb_source_select_str} from {tbname} partition by t3,c3 count_window({count_window_vol}) order by _wstart'
                        self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.nfl_tb_source_select_str, "", "t3,c3", custom_col_index, having_elm=having_elm, having_condition=having_condition, alias_name=alias_name)
                    '''

                    # _wcol
                    count_window_sql = f'select _wstart, _wend, _wduration, _qstart, _qend, {self.tb_source_select_str} from {tbname} count_window({count_window_vol})'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.tb_source_select_str, having_elm=having_elm, having_condition=having_condition, alias_name=alias_name)
                    # # # sliding
                    count_window_sql = f'select _wstart, _wend, {self.tb_source_select_str} from {tbname} count_window({count_window_vol},{sliding})'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.tb_source_select_str, sliding=sliding)

    def dup_col_test(self, stable_count, ctable_count, table_count, row_count, custom_col_index, col_value_type):
        pass
        ''' 3.4.0.0 do not support duplicte timestamp on count_window
        self.tdCom.prepare_all_type_data(dbname=self.dbname, stable_count=stable_count, ctable_count=ctable_count, table_count=table_count, row_count=row_count, custom_col_index=custom_col_index, col_value_type=col_value_type)
        for count_window_vol in self.count_window_vol_list:
            for tbname in self.tbname_check_list:
                if "stb" in tbname:
                    # Partition by dup column
                    count_window_sql = f'select _wstart, _wend, {self.nfl_stb_source_select_str} from {tbname} partition by c3 count_window({count_window_vol}) order by _wstart'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.nfl_stb_source_select_str, "", "c3", custom_col_index)
                else:
                    # # # No partition
                    # Partition by dup column
                    count_window_sql = f'select _wstart, _wend, {self.nfl_tb_source_select_str} from {tbname} partition by c3 count_window({count_window_vol}) order by _wstart'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.nfl_tb_source_select_str, "", "c3", custom_col_index)
        ''' 

    def dup_ts_col_test(self, stable_count, ctable_count, table_count, row_count, custom_col_index, col_value_type, insert_mode="None"):
        pass
        ''' 3.4.0.0 do not support duplicte timestamp on count_window
        self.tdCom.prepare_all_type_data(dbname=self.dbname, stable_count=stable_count, ctable_count=ctable_count, table_count=table_count, row_count=row_count, custom_col_index=custom_col_index, col_value_type=col_value_type, insert_mode=insert_mode)
        for count_window_vol in self.count_window_vol_list:
            for tbname in self.tbname_check_list:
                if "stb" in tbname:
                    # Partition by dup column
                    count_window_sql = f'select _wstart, _wend, {self.nfl_stb_source_select_str} from (select * from {tbname} order by ts,c{custom_col_index+1}) partition by c1 count_window({count_window_vol}) order by _wstart'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.nfl_stb_source_select_str, "", "c1", custom_col_index, insert_mode=insert_mode)
        '''

    def dup_ts_test(self, stable_count, ctable_count, table_count, row_count, custom_col_index, col_value_type, where_condition=None, having_elm=None, having_condition=None, alias_name=None, sliding=None, insert_mode=None):
        self.tdCom.prepare_all_type_data(dbname=self.dbname, stable_count=stable_count, ctable_count=ctable_count, table_count=table_count, row_count=row_count, custom_col_index=custom_col_index, col_value_type=col_value_type, insert_mode=insert_mode)
        condition_vol = "" if where_condition is None else f"where {where_condition}"
        for count_window_vol in self.count_window_vol_list:
            for tbname in self.tbname_check_list:
                if "stb" in tbname:
                    if self.disorder:
                        self.tdCom.insert_rows(dbname=self.dbname, tbname=self.tbname_check_list[1], ts_value=f'"{self.get_random_ts(self.tbname_check_list[1])}"', additional_ts=True)
                    if self.delete:
                        self.tdCom.delete_rows(dbname=self.dbname, tbname=self.tbname_check_list[1], start_ts=f'"{self.get_random_ts(self.tbname_check_list[1])}"')
                    # # No partition
                    count_window_sql = f'select _wstart, _wend, {self.stb_source_select_str} from (select * from {tbname} order by ts,c{custom_col_index+1}) count_window({count_window_vol})'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.stb_source_select_str, insert_mode=insert_mode)
                    # Partition by tbname
                    count_window_sql = f'select _wstart, _wend, {self.nfl_stb_source_select_str} from {tbname} partition by tbname count_window({count_window_vol}) order by _wstart'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.nfl_stb_source_select_str, "", "tbname", insert_mode=insert_mode)

                    ''' 3.4.0.0 do not support duplicte timestamp on count_window
                    # Partition by no-dup column
                    count_window_sql = f'select _wstart, _wend, {self.nfl_stb_source_select_str} from {tbname} partition by c3 count_window({count_window_vol}) order by _wstart'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.nfl_stb_source_select_str, "", "c3", custom_col_index, insert_mode=insert_mode)
                    # Partition by tag
                    count_window_sql = f'select _wstart, _wend, {self.nfl_stb_source_select_str} from {tbname} partition by t3 count_window({count_window_vol}) order by _wstart'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.nfl_stb_source_select_str, "", "t3", custom_col_index, insert_mode=insert_mode)
                    # Partition by expression
                    partition_vol = "abs(c3)"
                    count_window_sql = f'select _wstart, _wend, {self.nfl_stb_source_select_str} from {tbname} partition by {partition_vol} count_window({count_window_vol}) order by _wstart'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.nfl_stb_source_select_str, "", partition_vol, custom_col_index, insert_mode=insert_mode)
                    '''

                    # Filter
                    count_window_sql = f'select _wstart, _wend, {self.stb_source_select_str} from (select * from {tbname} order by ts,c{custom_col_index+1}) {condition_vol} count_window({count_window_vol})'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.stb_source_select_str, where_condition=condition_vol, insert_mode=insert_mode)
                    # Having
                    # No partition
                    count_window_sql = f'select _wstart, _wend, {self.stb_source_select_str} from (select * from {tbname} order by ts,c{custom_col_index+1}) count_window({count_window_vol})'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.stb_source_select_str, having_elm=having_elm, having_condition=having_condition, alias_name=alias_name, insert_mode=insert_mode)
                    # Partition by tbname
                    count_window_sql = f'select _wstart, _wend, {self.nfl_stb_source_select_str} from {tbname} partition by tbname count_window({count_window_vol}) order by _wstart'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.nfl_stb_source_select_str, "", "tbname", having_elm=having_elm, having_condition=having_condition, alias_name=alias_name, insert_mode=insert_mode)

                    ''' 3.4.0.0 do not support duplicte timestamp on count_window
                    # # Partition by no-dup column
                    count_window_sql = f'select _wstart, _wend, {self.nfl_stb_source_select_str} from {tbname} partition by c3 count_window({count_window_vol}) order by _wstart'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.nfl_stb_source_select_str, "", "c3", custom_col_index, having_elm=having_elm, having_condition=having_condition, alias_name=alias_name, insert_mode=insert_mode)
                    # # Partition by tag
                    count_window_sql = f'select _wstart, _wend, {self.nfl_stb_source_select_str} from {tbname} partition by t3 count_window({count_window_vol}) order by _wstart'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.nfl_stb_source_select_str, "", "t3", custom_col_index, having_elm=having_elm, having_condition=having_condition, alias_name=alias_name, insert_mode=insert_mode)
                    # # Partition by tag, column
                    count_window_sql = f'select _wstart, _wend, {self.nfl_stb_source_select_str} from {tbname} partition by t3,c3 count_window({count_window_vol}) order by _wstart'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.nfl_stb_source_select_str, "", "t3,c3", custom_col_index, having_elm=having_elm, having_condition=having_condition, alias_name=alias_name, insert_mode=insert_mode)
                    '''

                    # _wcol not stable and already covered in no_dup_ts_test
                    # count_window_sql = f'select _wstart, _wend, _wduration, _qstart, _qend, {self.stb_source_select_str} from {tbname} count_window({count_window_vol})'
                    # self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.stb_source_select_str, having_elm=having_elm, having_condition=having_condition, alias_name=alias_name, insert_mode=True)
                    # # sliding
                    count_window_sql = f'select _wstart, _wend, {self.stb_source_select_str} from (select * from {tbname} order by ts,c{custom_col_index+1}) count_window({count_window_vol},{sliding})'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.stb_source_select_str, sliding=sliding, insert_mode=insert_mode)
                    # # union
                    count_window_sql = f'select _wstart, _wend, {self.stb_source_select_str} from (select * from {tbname} order by ts,c{custom_col_index+1}) count_window({count_window_vol})'
                    self.tdCom.check_count_window_res(count_window_sql, tbname, count_window_vol, self.stb_source_select_str, sliding=sliding, union=True, insert_mode=insert_mode)

    def error_sql_test(self, stable_count, ctable_count, table_count, row_count):
        self.tdCom.prepare_all_type_data(dbname=self.dbname, stable_count=stable_count, ctable_count=ctable_count, table_count=table_count, row_count=row_count)
        self.tdSql.error(f'select _wstart, _wend, {self.stb_source_select_str} from {self.tbname_check_list[0]} count_window(1)')
        self.tdSql.error(f'select _wstart, _wend, {self.stb_source_select_str} from {self.tbname_check_list[0]} count_window(2147483648)')
        self.tdSql.error(f'select _wstart, _wend, {self.stb_source_select_str} from {self.tbname_check_list[0]} count_window(2, 3)')
        self.tdSql.error(f'select _wstart, _wend, {self.stb_source_select_str} from (select c3 from {self.tbname_check_list[0]} order by ts,c3) count_window(2)')
        self.tdSql.error(f'select _wstart, _wend, {self.stb_source_select_str} from {self.tbname_check_list[0]} count_window(2) interval(2s)')
        self.tdSql.error(f'select _wstart, _wend, {self.stb_source_select_str} from {self.tbname_check_list[0]} count_window(2) session(ts, 1)')
        self.tdSql.error(f'select _wstart, _wend, {self.stb_source_select_str} from {self.tbname_check_list[0]} count_window(2) state_window(c1)')
        self.tdSql.error(f'select _wstart, _wend, {self.stb_source_select_str} from {self.tbname_check_list[0]} count_window(2) event_window start with c1 > 0 end with c2 > 0')
        self.tdSql.error(f'select _wstart, _wend, {self.stb_source_select_str} from {self.tbname_check_list[0]} count_window(2) group by c1')

    def run(self)-> bool:
        self.no_dup_ts_test(stable_count=1, ctable_count=5, table_count=1, row_count=10, custom_col_index=2, col_value_type="Incremental", where_condition="c1 > 0", having_elm="min(c1)", having_condition="< 0", alias_name=self.alias_name, sliding=1)
        self.dup_col_test(stable_count=1, ctable_count=5, table_count=1, row_count=10, custom_col_index=2, col_value_type="Part_equal")
        self.dup_ts_col_test(stable_count=1, ctable_count=5, table_count=1, row_count=10, custom_col_index=2, col_value_type="Incremental", insert_mode="interlace")
        self.dup_ts_test(stable_count=1, ctable_count=5, table_count=1, row_count=10, custom_col_index=2, col_value_type="Incremental", where_condition="c1 > 0", having_elm="min(c1)", having_condition="< 0", alias_name=self.alias_name, sliding=1, insert_mode="interlace")
        self.error_sql_test(stable_count=1, ctable_count=1, table_count=1, row_count=10)


