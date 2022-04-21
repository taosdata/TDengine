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

class TestBigintBoundary(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)

    def bigint_boundary_check(self):
        """
        max: +- 9223372036854775807
        """
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 bigint) tags (t1 bigint)')
        self.tdSql.execute(f'create table if not exists {dbname}.tb1 using {dbname}.stb tags ({self.tdCom.boundary_config["BIGINT_MAX"]})')
        self.tdSql.execute(f'create table if not exists {dbname}.tb2 using {dbname}.stb tags (-{self.tdCom.boundary_config["BIGINT_MAX"]})')
        self.tdSql.execute(f'insert into {dbname}.tb1 values (now, -{self.tdCom.boundary_config["BIGINT_MAX"]})')
        self.tdSql.execute(f'insert into {dbname}.tb2 values (now, {self.tdCom.boundary_config["BIGINT_MAX"]})')
        self.tdSql.query(f'select t1, c1 from {dbname}.tb1')
        self.tdSql.checkEqual(self.tdSql.query_data[0][0], 9223372036854775807)
        self.tdSql.checkEqual(self.tdSql.query_data[0][1], -9223372036854775807)
        self.tdSql.query(f'select t1, c1 from {dbname}.tb2')
        self.tdSql.checkEqual(self.tdSql.query_data[0][0], -9223372036854775807)
        self.tdSql.checkEqual(self.tdSql.query_data[0][1], 9223372036854775807)
        self.tdSql.error(f'create stable if not exists {dbname}.stb_error1 (col_ts timestamp, c1 {self.tdCom.boundary_config["BIGINT_MAX"]}) tags (t1 {self.tdCom.boundary_config["BIGINT_MAX"]+1})')
        self.tdSql.error(f'create stable if not exists {dbname}.stb_error2 (col_ts timestamp, c1 {self.tdCom.boundary_config["BIGINT_MAX"]+1}) tags (t1 {self.tdCom.boundary_config["BIGINT_MAX"]})')
        self.tdSql.error(f'create stable if not exists {dbname}.stb_error3 (col_ts timestamp, c1 {self.tdCom.boundary_config["BIGINT_MAX"]}) tags (t1 -{self.tdCom.boundary_config["BIGINT_MAX"]+1})')
        self.tdSql.error(f'create stable if not exists {dbname}.stb_error4 (col_ts timestamp, c1 -{self.tdCom.boundary_config["BIGINT_MAX"]+1}) tags (t1 -{self.tdCom.boundary_config["BIGINT_MAX"]})')
        self.tdSql.error(f'create table if not exists {dbname}.tb using {dbname}.stb tags (now-2h, {self.tdCom.boundary_config["BIGINT_MAX"]+1})')
        self.tdSql.error(f'create table if not exists {dbname}.tb using {dbname}.stb tags (now-2h, -{self.tdCom.boundary_config["BIGINT_MAX"]+1})')
        self.tdSql.error(f'insert into {dbname}.tb values (now-1h, {self.tdCom.boundary_config["BIGINT_MAX"]+1})')
        self.tdSql.error(f'insert into {dbname}.tb values (now-1h, -{self.tdCom.boundary_config["BIGINT_MAX"]+1})')
        self.tdSql.execute(f'drop database if exists {dbname}')

    def run(self):
        self.bigint_boundary_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            bigint_boundary_check <jayden>: [TD-12748] : bigint boundary check (max 9223372036854775807);
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Insert.BoundaryTest.Bigint