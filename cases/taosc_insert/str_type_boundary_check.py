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

class TestStrBoundary(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.boundary_dict = {
            "binary": self.tdCom.Boundary.BINARY_MAX_LENGTH,
            "varchar": self.tdCom.Boundary.BINARY_MAX_LENGTH,
            "nchar": self.tdCom.Boundary.NCHAR_MAX_LENGTH,
        }
        print(self.tdCom.Boundary.BINARY_MAX_LENGTH)
        print(self.tdCom.Boundary.NCHAR_MAX_LENGTH)
        self.tag_len_value = 1
    def str_type_boundary_check(self):
        """
        binary/varchar/nchar
        """
        for data_type, data_value in self.boundary_dict.items():
            dbname = self.tdCom.get_long_name()
            self.tdCom.createDb(dbname)
            max_length = self.tdCom.get_long_name(data_value)
            exceed_length = self.tdCom.get_long_name(data_value+1)
            self.tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 {data_type}({data_value})) tags (t1 {data_type}({self.tag_len_value}))')
            self.tdSql.execute(f'create table if not exists {dbname}.tb using {dbname}.stb tags ("{self.tag_len_value}")')
            self.tdSql.execute(f'insert into {dbname}.tb values (now, "{max_length}")')
            self.tdSql.query(f'describe {dbname}.stb')
            self.tdSql.checkEqual(self.tdSql.query_data[1][2], data_value)
            self.tdSql.checkEqual(self.tdSql.query_data[2][2], self.tag_len_value)
            self.tdSql.query(f'select t1, c1 from {dbname}.tb')
            self.tdSql.checkEqual(int(self.tdSql.query_data[0][0]), self.tag_len_value)
            self.tdSql.checkEqual(str(self.tdSql.query_data[0][1]), max_length)
            self.tdSql.error(f'create stable if not exists {dbname}.stb_error1 (col_ts timestamp, c1 {data_type}({data_value})) tags (t1 {data_type}({exceed_length}))')
            self.tdSql.error(f'create stable if not exists {dbname}.stb_error2 (col_ts timestamp, c1 {data_type}({exceed_length})) tags (t1 {data_type}({data_value}))')
            self.tdSql.error(f'create table if not exists {dbname}.tb using {dbname}.stb tags (now-2h, "{exceed_length}")')
            self.tdSql.error(f'insert into {dbname}.tb values (now-1h, "{exceed_length}")')

            self.tdSql.execute(f'create table if not exists {dbname}.tb2 (ts timestamp, c1 {data_type}({data_value}))')
            self.tdSql.execute(f'insert into {dbname}.tb2 values (now, "{max_length}")')
            self.tdSql.query(f'describe {dbname}.tb2')
            self.tdSql.checkEqual(self.tdSql.query_data[1][2], data_value)
            self.tdSql.query(f'select c1 from {dbname}.tb2 where c1="{max_length}"')
            self.tdSql.checkEqual(self.tdSql.query_data[0][0], max_length)
            self.tdSql.error(f'insert into {dbname}.tb3 values (now, "{exceed_length}")')
            self.tdSql.execute(f'drop database if exists {dbname}')

    def run(self):
        self.str_type_boundary_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            uint_boundary_check <jayden>: [TD-12748] : str_type_boundary_check;
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Insert.BoundaryTest