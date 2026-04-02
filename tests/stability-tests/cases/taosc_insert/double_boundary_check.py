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

class TestDoubleBoundary(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)

    def double_boundary_check(self):
        """
        max: +- 3.4e+38
        """
        dbname = self.tdCom.get_long_name()
        self.tdCom.createDb(dbname)
        self.tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 double) tags (t1 double)')
        self.tdSql.execute(f'create table if not exists {dbname}.tb1 using {dbname}.stb tags ({self.tdCom.Boundary.DOUBLE_BOUNDARY[1]})')
        self.tdSql.execute(f'create table if not exists {dbname}.tb2 using {dbname}.stb tags ({self.tdCom.Boundary.DOUBLE_BOUNDARY[0]})')
        self.tdSql.execute(f'insert into {dbname}.tb1 values (now+1s, {self.tdCom.Boundary.DOUBLE_BOUNDARY[0]})')
        self.tdSql.execute(f'insert into {dbname}.tb2 values (now+2s, {self.tdCom.Boundary.DOUBLE_BOUNDARY[1]})')
        self.tdSql.query(f'select t1, c1 from {dbname}.tb1')
        self.tdSql.checkEqual(float(self.tdSql.query_data[0][0]), self.tdCom.Boundary.DOUBLE_BOUNDARY[1])
        self.tdSql.checkEqual(float(self.tdSql.query_data[0][1]), self.tdCom.Boundary.DOUBLE_BOUNDARY[0])
        self.tdSql.query(f'select t1, c1 from {dbname}.tb2')
        self.tdSql.checkEqual(float(self.tdSql.query_data[0][0]), self.tdCom.Boundary.DOUBLE_BOUNDARY[0])
        self.tdSql.checkEqual(float(self.tdSql.query_data[0][1]), self.tdCom.Boundary.DOUBLE_BOUNDARY[1])
        self.tdSql.error(f'create table if not exists {dbname}.tb using {dbname}.stb tags (1.797693134862316e308)')
        self.tdSql.error(f'create table if not exists {dbname}.tb using {dbname}.stb tags (-1.797693134862316e308)')
        self.tdSql.error(f'insert into {dbname}.tb1 values (now, 1.797693134862316e308)')
        self.tdSql.error(f'insert into {dbname}.tb1 values (now, -1.797693134862316e308)')

        self.tdSql.execute(f'create table if not exists {dbname}.tb3 (ts timestamp, c1 DOUBLE)')
        self.tdSql.execute(f'insert into {dbname}.tb3 values (now+3s, {self.tdCom.Boundary.DOUBLE_BOUNDARY[1]})')
        self.tdSql.execute(f'insert into {dbname}.tb3 values (now+4s, {self.tdCom.Boundary.DOUBLE_BOUNDARY[0]})')
        self.tdSql.query(f'select c1 from {dbname}.tb3 where c1>0')
        self.tdSql.checkEqual(self.tdSql.query_data[0][0], self.tdCom.Boundary.DOUBLE_BOUNDARY[1])
        self.tdSql.query(f'select c1 from {dbname}.tb3 where c1<0')
        self.tdSql.checkEqual(self.tdSql.query_data[0][0], self.tdCom.Boundary.DOUBLE_BOUNDARY[0])
        self.tdSql.error(f'insert into {dbname}.tb3 values (now, 1.797693134862316e308)')
        self.tdSql.error(f'insert into {dbname}.tb3 values (now, -1.797693134862316e308)')
        self.tdSql.execute(f'drop database if exists {dbname}')

    def run(self):
        self.double_boundary_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            double_boundary_check <jayden>: [TD-12748] : double boundary check (max {self.tdCom.Boundary.DOUBLE_BOUNDARY[1]});
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Insert.BoundaryTest.Double