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
import math

class TestFloatBoundary(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)

    def float_boundary_check(self):
        """
        max: +- 3.4e+38
        """
        dbname = self.tdCom.get_long_name()
        self.tdCom.createDb(dbname)
        self.tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 float) tags (t1 float)')
        self.tdSql.execute(f'create table if not exists {dbname}.tb1 using {dbname}.stb tags ({self.tdCom.Boundary.FLOAT_BOUNDARY[1]})')
        self.tdSql.execute(f'create table if not exists {dbname}.tb2 using {dbname}.stb tags ({self.tdCom.Boundary.FLOAT_BOUNDARY[0]})')
        self.tdSql.execute(f'insert into {dbname}.tb1 values (now, {self.tdCom.Boundary.FLOAT_BOUNDARY[0]})')
        self.tdSql.execute(f'insert into {dbname}.tb2 values (now, {self.tdCom.Boundary.FLOAT_BOUNDARY[1]})')
        self.tdSql.query(f'select t1, c1 from {dbname}.tb1')
        self.tdSql.checkEqual(math.isclose(self.tdCom.Boundary.FLOAT_BOUNDARY[1], self.tdSql.query_data[0][0], rel_tol=0.01), True)
        self.tdSql.checkEqual(math.isclose(self.tdCom.Boundary.FLOAT_BOUNDARY[0], self.tdSql.query_data[0][1], rel_tol=0.01), True)
        self.tdSql.query(f'select t1, c1 from {dbname}.tb2')
        self.tdSql.checkEqual(math.isclose(self.tdCom.Boundary.FLOAT_BOUNDARY[0], self.tdSql.query_data[0][0], rel_tol=0.01), True)
        self.tdSql.checkEqual(math.isclose(self.tdCom.Boundary.FLOAT_BOUNDARY[1], self.tdSql.query_data[0][1], rel_tol=0.01), True)
        self.tdSql.error(f'create table if not exists {dbname}.tb using {dbname}.stb tags (3.4028234664e38)')
        self.tdSql.error(f'create table if not exists {dbname}.tb using {dbname}.stb tags (-3.4028234664e38)')
        self.tdSql.error(f'insert into {dbname}.tb1 values (now-1h, 3.4028234664e38)')
        self.tdSql.error(f'insert into {dbname}.tb2 values (now-1h, -3.4028234664e38)')

        self.tdSql.execute(f'create table if not exists {dbname}.tb3 (ts timestamp, c1 float)')
        self.tdSql.execute(f'insert into {dbname}.tb3 values (now, {self.tdCom.Boundary.FLOAT_BOUNDARY[1]})')
        self.tdSql.execute(f'insert into {dbname}.tb3 values (now, {self.tdCom.Boundary.FLOAT_BOUNDARY[0]})')
        self.tdSql.query(f'select c1 from {dbname}.tb3 where c1>0')
        self.tdSql.checkEqual(abs(float(str(self.tdCom.Boundary.FLOAT_BOUNDARY[1]).replace("e+38",""))-float(str(self.tdSql.query_data[0][0]).replace("e+38",""))) < 0.01, True)
        self.tdSql.query(f'select c1 from {dbname}.tb3 where c1<0')
        self.tdSql.checkEqual(abs(float(str(self.tdCom.Boundary.FLOAT_BOUNDARY[1]).replace("e+38",""))+float(str(self.tdSql.query_data[0][0]).replace("e+38",""))) < 0.01, True)
        self.tdSql.error(f'insert into {dbname}.tb3 values (now, 3.4028234664e38)')
        self.tdSql.error(f'insert into {dbname}.tb3 values (now, -3.4028234664e38)')
        self.tdSql.execute(f'drop database if exists {dbname}')

    def run(self):
        self.float_boundary_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            float_boundary_check <jayden>: [TD-12748] : float boundary check (max {self.tdCom.Boundary.FLOAT_BOUNDARY[1]});
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Insert.BoundaryTest.Float