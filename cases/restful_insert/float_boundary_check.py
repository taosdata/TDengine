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

class TestFloatBoundary(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.tdRest = TDRest(env_setting=self.env_setting)

    def float_boundary_check(self):
        """
        max: +- 3.4e+38
        """
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdRest.request(f'create database if not exists {dbname}')
        self.tdRest.request(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 float) tags (t1 float)')
        self.tdRest.request(f'create table if not exists {dbname}.tb1 using {dbname}.stb tags ({self.tdCom.boundary_config["FLOAT_MAX"]})')
        self.tdRest.request(f'create table if not exists {dbname}.tb2 using {dbname}.stb tags (-{self.tdCom.boundary_config["FLOAT_MAX"]})')
        self.tdRest.request(f'insert into {dbname}.tb1 values (now, -{self.tdCom.boundary_config["FLOAT_MAX"]})')
        self.tdRest.request(f'insert into {dbname}.tb2 values (now, {self.tdCom.boundary_config["FLOAT_MAX"]})')
        self.tdRest.request(f'select t1, c1 from {dbname}.tb1')
        self.tdSql.checkEqual(float(self.tdRest.resp["data"][0][0]), self.tdCom.boundary_config["FLOAT_MAX"])
        self.tdSql.checkEqual(float(self.tdRest.resp["data"][0][1]), -self.tdCom.boundary_config["FLOAT_MAX"])
        self.tdRest.request(f'select t1, c1 from {dbname}.tb2')
        self.tdSql.checkEqual(float(self.tdRest.resp["data"][0][0]), -self.tdCom.boundary_config["FLOAT_MAX"])
        self.tdSql.checkEqual(float(self.tdRest.resp["data"][0][1]), self.tdCom.boundary_config["FLOAT_MAX"])
        self.tdRest.error(f'create stable if not exists {dbname}.stb_error1 (col_ts timestamp, c1 {self.tdCom.boundary_config["FLOAT_MAX"]}) tags (t1 {self.tdCom.boundary_config["FLOAT_MAX"]+1})')
        self.tdRest.error(f'create stable if not exists {dbname}.stb_error2 (col_ts timestamp, c1 {self.tdCom.boundary_config["FLOAT_MAX"]+1}) tags (t1 {self.tdCom.boundary_config["FLOAT_MAX"]})')
        self.tdRest.error(f'create stable if not exists {dbname}.stb_error3 (col_ts timestamp, c1 {self.tdCom.boundary_config["FLOAT_MAX"]}) tags (t1 -{self.tdCom.boundary_config["FLOAT_MAX"]-1})')
        self.tdRest.error(f'create stable if not exists {dbname}.stb_error4 (col_ts timestamp, c1 -{self.tdCom.boundary_config["FLOAT_MAX"]-1}) tags (t1 -{self.tdCom.boundary_config["FLOAT_MAX"]})')
        self.tdRest.error(f'create table if not exists {dbname}.tb using {dbname}.stb tags (now-2h, {self.tdCom.boundary_config["FLOAT_MAX"]+1})')
        self.tdRest.error(f'create table if not exists {dbname}.tb using {dbname}.stb tags (now-2h, -{self.tdCom.boundary_config["FLOAT_MAX"]-1})')
        self.tdRest.error(f'insert into {dbname}.tb values (now-1h, {self.tdCom.boundary_config["FLOAT_MAX"]+1})')
        self.tdRest.error(f'insert into {dbname}.tb values (now-1h, -{self.tdCom.boundary_config["FLOAT_MAX"]-1})')
        self.tdRest.request(f'drop database if exists {dbname}')

    def run(self):
        self.float_boundary_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            float_boundary_check <jayden>: [TD-12748] : float boundary check (max {self.tdCom.boundary_config["FLOAT_MAX"]});
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.RestfulSql.Insert.BoundaryTest.Float