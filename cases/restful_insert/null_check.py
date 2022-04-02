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

class TestNull(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.tdRest = TDRest(env_setting=self.env_setting)

    def null_dbname_check(self):
        """
        dbname = "null"
        """
        dbname = "null"
        self.tdRest.error(f'create database if not exists {dbname}')

    def stb_null_check(self):
        """
        stbname/tag/col = "null"
        """
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdRest.request(f'create database if not exists {dbname} precision "ms"')
        stbname = "null"
        self.tdRest.error(f'create stable if not exists {dbname}.{stbname} (col_ts timestamp, c1 int) tags (tag_ts timestamp, t1 int)')
        self.tdRest.error(f'create stable if not exists {dbname}.stb (null timestamp, c1 int) tags (tag_ts timestamp, t1 int)')
        self.tdRest.error(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 int) tags (tag_ts timestamp, null int)')
        self.tdRest.error(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 null) tags (tag_ts timestamp, t1 int)')
        self.tdRest.error(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 int) tags (tag_ts timestamp, t1 null)')
        self.tdRest.request(f'drop database if exists {dbname}')

    def tb_null_check(self):
        """
        tbname/tag/col = "null"
        """
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdRest.request(f'create database if not exists {dbname} precision "ms"')
        self.tdRest.request(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 int) tags (tag_ts timestamp, t1 int)')
        tbname = "null"
        self.tdRest.error(f'create table if not exists {dbname}.{tbname} using {dbname}.stb tags (now, 1)')
        self.tdRest.request(f'create table if not exists {dbname}.tb using {dbname}.stb tags (null, null)')
        self.tdRest.request(f'insert into {dbname}.tb values (now, null)')
        self.tdRest.error(f'insert into {dbname}.tb values (null, 1)')
        self.tdRest.request(f'select tag_ts, t1, c1 from {dbname}.stb')
        self.tdSql.checkEqual(self.tdRest.resp["data"][0][0], None)
        self.tdSql.checkEqual(self.tdRest.resp["data"][0][1], None)
        self.tdSql.checkEqual(self.tdRest.resp["data"][0][2], None)
        self.tdRest.request(f'drop database if exists {dbname}')

    def polling_insert_check(self):
        """
        null and normal poll insert
        """
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdRest.request(f'create database if not exists {dbname}')
        self.tdRest.request(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 tinyint unsigned, c6 smallint unsigned, \
                c7 int unsigned, c8 bigint unsigned, c9 float, c10 double, c11 binary(16), c12 nchar(16), c13 bool) tags (tag_ts timestamp, t1 tinyint, t2 smallint, t3 int, \
                t4 bigint, t5 tinyint unsigned, t6 smallint unsigned, t7 int unsigned, t8 bigint unsigned, t9 float, t10 double, t11 binary(16), t12 nchar(16), t13 bool)')
        self.tdRest.request(f'create table if not exists {dbname}.tb using {dbname}.stb tags (now, 1, 2, 3, 4, 5, 6, 7, 8, 9.9, 10.1, "binary", "nchar", True)')
        self.tdRest.request(f'create table if not exists {dbname}.tb_null using {dbname}.stb tags (now, null, null, null, null, null, null, null, null, null, null, null, null, null)')
        self.tdRest.request(f'insert into {dbname}.tb values (now, 1, 2, 3, 4, 5, 6, 7, 8, 9.9, 10.1, "binary", "nchar", True)')
        self.tdRest.request(f'insert into {dbname}.tb values (now-1h, null, null, null, null, null, null, null, null, null, null, null, null, null)')
        self.tdRest.request(f'insert into {dbname}.tb values (now-2h, 1, 2, 3, 4, 5, 6, 7, 8, 9.9, 10.1, "binary", "nchar", True)')
        self.tdRest.request(f'insert into {dbname}.tb values (now-3h, null, null, null, null, null, null, null, null, null, null, null, null, null)')
        self.tdRest.request(f'insert into {dbname}.tb (col_ts, c3 , c7, c9, c11, c13) values (now+1h, 3, 7, 9.9, "binary", True)')
        self.tdRest.request(f'insert into {dbname}.tb (col_ts, c3 , c7, c9, c11, c13) values (now+2h, null, null, null, null, null)')
        self.tdRest.request(f'insert into {dbname}.tb (col_ts, c3 , c7, c9, c11, c13) values (now+3h, 3, null, 7, null, False)')
        self.tdRest.request(f'select count(*) from {dbname}.stb')
        self.tdSql.checkEqual(int(self.tdRest.resp["data"][0][0]), 7)
        self.tdRest.request(f'drop database if exists {dbname}')

    def run(self):
        self.null_dbname_check()
        self.stb_null_check()
        self.tb_null_check()
        self.polling_insert_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            null_dbname_check <jayden>: [TD-12748] : null dbname check;\n
            stb_null_check <jayden>: [TD-12748] : stb null check;\n
            tb_null_check <jayden>: [TD-12748] : tb null check;\n
            polling_insert_check <jayden>: [TD-12748] : polling insert check;
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.RestfulSql.Insert.NullInsert