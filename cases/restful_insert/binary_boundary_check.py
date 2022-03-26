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
class TestBinaryBoundary(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.tdRest = TDRest()

    def binary_length_check(self):
        """
        max length: 16374
        """
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdRest.request(f'create database if not exists {dbname}')
        str_16374 = self.tdCom.get_long_name(length=16374, mode="letters")
        str_16375 = self.tdCom.get_long_name(length=16375, mode="letters")
        self.tdRest.request(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 binary(16374)) tags (t1 binary(16374))')
        self.tdRest.request(f'create table if not exists {dbname}.tb using {dbname}.stb tags ("{str_16374}")')
        self.tdRest.request(f'insert into {dbname}.tb values (now, "{str_16374}")')
        self.tdRest.request(f'select t1, c1 from {dbname}.tb')
        self.tdSql.checkEqual(str(self.tdRest.resp["data"][0][0]), str_16374)
        self.tdSql.checkEqual(str(self.tdRest.resp["data"][0][1]), str_16374)
        self.tdRest.error(f'create stable if not exists {dbname}.stb_error1 (col_ts timestamp, c1 binary(16374)) tags (t1 binary(16375))')
        self.tdRest.error(f'create stable if not exists {dbname}.stb_error2 (col_ts timestamp, c1 binary(16375)) tags (t1 binary(16374))')
        self.tdRest.error(f'create table if not exists {dbname}.tb using {dbname}.stb tags (now-2h, "{str_16375}")')
        self.tdRest.error(f'insert into {dbname}.tb values (now-1h, "{str_16375}")')
        self.tdRest.request(f'drop database if exists {dbname}')

    def run(self):
        self.binary_length_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            binary_length_check <jayden>: [TD-12748] : binary length check (max 16374);
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.RestfulSql.Insert.BoundaryTest.Binary