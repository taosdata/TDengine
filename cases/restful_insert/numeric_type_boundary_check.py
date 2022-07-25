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
from taostest.util.rest import TDRest
class TestNumericBoundary(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.tdRest = TDRest(env_setting=self.env_setting)
        self.api_type = 'restful'
        self.boundary_dict = {
            "int": self.tdCom.Boundary.INT_BOUNDARY,
            "smallint": self.tdCom.Boundary.SMALLINT_BOUNDARY,
            "tinyint": self.tdCom.Boundary.TINYINT_BOUNDARY,
            "bigint": self.tdCom.Boundary.BIGINT_BOUNDARY,
            "int unsigned": self.tdCom.Boundary.UINT_BOUNDARY,
            "smallint unsigned": self.tdCom.Boundary.USMALLINT_BOUNDARY,
            "tinyint unsigned": self.tdCom.Boundary.UTINYINT_BOUNDARY,
            "bigint unsigned": self.tdCom.Boundary.UBIGINT_BOUNDARY,
        }
    def numeric_boundary_check(self):
        for data_type, data_value in self.boundary_dict.items():
            dbname = self.tdCom.get_long_name()
            self.tdCom.createDb(dbname)
            self.tdRest.request(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 {data_type}) tags (t1 {data_type})')
            self.tdRest.request(f'create table if not exists {dbname}.tb1 using {dbname}.stb tags ({data_value[1]})')
            self.tdRest.request(f'create table if not exists {dbname}.tb2 using {dbname}.stb tags ({data_value[0]})')
            self.tdRest.request(f'insert into {dbname}.tb1 values (now, {data_value[0]})')
            self.tdRest.request(f'insert into {dbname}.tb2 values (now, {data_value[1]})')
            self.tdRest.request(f'select t1, c1 from {dbname}.tb1')
            self.tdSql.checkEqual(int(self.tdRest.resp["data"][0][0]), data_value[1])
            self.tdSql.checkEqual(int(self.tdRest.resp["data"][0][1]), data_value[0])
            self.tdRest.request(f'select t1, c1 from {dbname}.tb2')
            self.tdSql.checkEqual(int(self.tdRest.resp["data"][0][0]), data_value[0])
            self.tdSql.checkEqual(int(self.tdRest.resp["data"][0][1]), data_value[1])
            self.tdRest.error(f'create table if not exists {dbname}.tb using {dbname}.stb tags ({data_value[1]+1})')
            self.tdRest.error(f'create table if not exists {dbname}.tb using {dbname}.stb tags ({data_value[1]-1})')
            self.tdRest.error(f'insert into {dbname}.tb1 values (now-1h, {data_value[1]+1})')
            self.tdRest.error(f'insert into {dbname}.tb2 values (now-1h, {data_value[0]-1})')

            self.tdRest.request(f'create table if not exists {dbname}.tb3 (ts timestamp, c1 {data_type})')
            self.tdRest.request(f'insert into {dbname}.tb3 values (now, {data_value[1]})')
            self.tdRest.request(f'insert into {dbname}.tb3 values (now, {data_value[0]})')
            self.tdRest.request(f'select c1 from {dbname}.tb3 where c1>0')
            self.tdSql.checkEqual(int(self.tdRest.resp["data"][0][0]), data_value[1])
            self.tdRest.request(f'select c1 from {dbname}.tb3 where c1={data_value[0]}')
            self.tdSql.checkEqual(int(self.tdRest.resp["data"][0][0]), data_value[0])
            self.tdRest.error(f'insert into {dbname}.tb3 values (now, {data_value[1]+1})')
            self.tdRest.error(f'insert into {dbname}.tb3 values (now, {data_value[0]-1})')
            self.tdRest.request(f'drop database if exists {dbname}')

        

        pass

    def run(self):
        self.numeric_boundary_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            uint_boundary_check <jayden>: [TD-12748] : numeric_boundary_check;
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Insert.BoundaryTest