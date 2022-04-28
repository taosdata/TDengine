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

class TestUnsignedSmallintBoundary(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.tdRest = TDRest(env_setting=self.env_setting)

    def usmallint_boundary_check(self):
        """
        max: 0 - {self.tdCom.boundary_config["USMALLINT_MAX"]}
        """
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 smallint unsigned) tags (t1 smallint unsigned)')
        self.tdSql.execute(f'create table if not exists {dbname}.tb1 using {dbname}.stb tags ({self.tdCom.boundary_config["USMALLINT_MAX"]})')
        self.tdSql.execute(f'create table if not exists {dbname}.tb2 using {dbname}.stb tags (0)')
        self.tdSql.execute(f'insert into {dbname}.tb1 values (now, 0)')
        self.tdSql.execute(f'insert into {dbname}.tb2 values (now, {self.tdCom.boundary_config["USMALLINT_MAX"]})')
        self.tdSql.query(f'select t1, c1 from {dbname}.tb1')
        self.tdSql.checkEqual(self.tdSql.query_data[0][0], self.tdCom.boundary_config["USMALLINT_MAX"])
        self.tdSql.checkEqual(self.tdSql.query_data[0][1], 0)
        self.tdSql.query(f'select t1, c1 from {dbname}.tb2')
        self.tdSql.checkEqual(self.tdSql.query_data[0][0], 0)
        self.tdSql.checkEqual(self.tdSql.query_data[0][1], self.tdCom.boundary_config["USMALLINT_MAX"])
        self.tdSql.error(f'create stable if not exists {dbname}.stb_error1 (col_ts timestamp, c1 {self.tdCom.boundary_config["USMALLINT_MAX"]}) tags (t1 {self.tdCom.boundary_config["USMALLINT_MAX"]+1})')
        self.tdSql.error(f'create stable if not exists {dbname}.stb_error2 (col_ts timestamp, c1 {self.tdCom.boundary_config["USMALLINT_MAX"]+1}) tags (t1 {self.tdCom.boundary_config["USMALLINT_MAX"]})')
        self.tdSql.error(f'create stable if not exists {dbname}.stb_error3 (col_ts timestamp, c1 {self.tdCom.boundary_config["USMALLINT_MAX"]}) tags (t1 -1)')
        self.tdSql.error(f'create stable if not exists {dbname}.stb_error4 (col_ts timestamp, c1 -1) tags (t1 0)')
        self.tdSql.error(f'create table if not exists {dbname}.tb using {dbname}.stb tags (now-2h, {self.tdCom.boundary_config["USMALLINT_MAX"]+1})')
        self.tdSql.error(f'create table if not exists {dbname}.tb using {dbname}.stb tags (now-2h, -1)')
        self.tdSql.error(f'insert into {dbname}.tb values (now-1h, {self.tdCom.boundary_config["USMALLINT_MAX"]+1})')
        self.tdSql.error(f'insert into {dbname}.tb values (now-1h, -1)')
        self.tdSql.execute(f'create table if not exists {dbname}.tb3 (ts timestamp, c1 bigint)')
        self.tdSql.execute(f'insert into {dbname}.tb3 values (now, {self.tdCom.boundary_config["USMALLINT_MAX"]})')
        self.tdSql.execute(f'insert into {dbname}.tb3 values (now, -{self.tdCom.boundary_config["USMALLINT_MAX"]})')
        self.tdSql.query(f'select c1 from {dbname}.tb3 where c1={self.tdCom.boundary_config["USMALLINT_MAX"]}')
        self.tdSql.checkEqual(self.tdSql.query_data[0][0], self.tdCom.boundary_config["USMALLINT_MAX"])
        self.tdSql.query(f'select c1 from {dbname}.tb3 where c1=-{self.tdCom.boundary_config["USMALLINT_MAX"]}')
        self.tdSql.checkEqual(self.tdSql.query_data[0][0], -self.tdCom.boundary_config["USMALLINT_MAX"])
        self.tdSql.error(f'insert into {dbname}.tb3 values (now, {self.tdCom.boundary_config["USMALLINT_MAX"]+1})')
        self.tdSql.error(f'insert into {dbname}.tb3 values (now, -{self.tdCom.boundary_config["USMALLINT_MAX"]+1})')
        self.tdSql.execute(f'drop database if exists {dbname}')

    def run(self):
        self.usmallint_boundary_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            usmallint_boundary_check <jayden>: [TD-12748] : unsigned smallint boundary check (max {self.tdCom.boundary_config["USMALLINT_MAX"]});
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Insert.BoundaryTest.Usmallint