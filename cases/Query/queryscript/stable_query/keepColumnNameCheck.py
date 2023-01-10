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

class TestKeepColumnName(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.stbname = "stb"
        self.ctbname = "ctb"
        self.tbname = "tb"
        
        self.column_elm_list = [{"type": "int", "count": 2}, {"type": "double", "count": 2}, {"type": "timestamp", "count": 1}]
        self.ts_value = self.tdCom.genTs()[0]

    def keep_column_name_check(self):
        """
        keep column name
        """
        dbname = self.tdCom.get_long_name()
        self.tdCom.createDb(dbname=dbname)
        self.tdCom.create_stable(dbname=dbname, column_elm_list=self.column_elm_list)
        self.tdCom.create_ctable(dbname=dbname)
        self.tdCom.create_table(dbname=dbname, column_elm_list=self.column_elm_list)
        self.tdCom.insert_rows(tbname=self.ctbname, column_ele_list=self.column_elm_list)
        first_res = self.tdCom.column_value_list
        self.tdSql.query(f'select first(*) from {self.stbname}')
        self.tdSql.checkEqual(tuple(first_res[1:]), self.tdSql.query_data[0][1:])
        self.tdSql.checkNotIn("first", str(self.tdSql.query_result.fields))

        self.tdCom.insert_rows(tbname=self.ctbname, column_ele_list=self.column_elm_list)
        last_res = self.tdCom.column_value_list
        self.tdSql.query(f'select last(*) from {self.stbname}')
        self.tdSql.checkEqual(tuple(last_res[1:]), self.tdSql.query_data[0][1:])
        self.tdSql.checkNotIn("last", str(self.tdSql.query_result.fields))
        self.tdSql.query(f'select last_row(*) from {self.stbname}')
        self.tdSql.checkEqual(tuple(last_res[1:]), self.tdSql.query_data[0][1:])
        self.tdSql.checkNotIn("last", str(self.tdSql.query_result.fields))

        self.tdCom.insert_rows(tbname=self.tbname, column_ele_list=self.column_elm_list)
        first_res = self.tdCom.column_value_list
        self.tdSql.query(f'select first(*) from {self.tbname}')
        self.tdSql.checkEqual(tuple(first_res[1:]), self.tdSql.query_data[0][1:])

        self.tdCom.insert_rows(tbname=self.tbname, column_ele_list=self.column_elm_list)
        last_res = self.tdCom.column_value_list
        self.tdSql.query(f'select last(*) from {self.tbname}')
        self.tdSql.checkEqual(tuple(last_res[1:]), self.tdSql.query_data[0][1:])
        self.tdSql.checkNotIn("last", str(self.tdSql.query_result.fields))
        self.tdSql.query(f'select last_row(*) from {self.tbname}')
        self.tdSql.checkEqual(tuple(last_res[1:]), self.tdSql.query_data[0][1:])
        self.tdSql.checkNotIn("last", str(self.tdSql.query_result.fields))

        self.tdSql.error(f'create stream if not exists stream_error into {dbname}.streamtb as select first(c1), last(c1) from {dbname}.stb partition by tbname state_window(c1);')

    def run(self) -> bool:
        self.tdCom.drop_all_db()
        self.keep_column_name_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            keep column name;
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Query