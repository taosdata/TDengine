###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import sys
import time
import random

import taos
import frame
import frame.etool


from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *


class TDTestCase(TBase):

    def checkGeometry(self):
        tdLog.info(f"check geometry")

        tdSql.execute("create database db_geometry;")
        tdSql.execute("use db_geometry;")
        tdSql.execute("create table t_ge (ts timestamp, id int, c1 GEOMETRY(512));")
        tdSql.execute("insert into t_ge values(1717122943000, 1, 'MULTIPOINT ((0 0), (1 1))');")
        tdSql.execute("insert into t_ge values(1717122944000, 1, 'MULTIPOINT (0 0, 1 1)');")
        tdSql.execute("insert into t_ge values(1717122945000, 2, 'POINT (0 0)');")
        tdSql.execute("insert into t_ge values(1717122946000, 2, 'POINT EMPTY');")
        tdSql.execute("insert into t_ge values(1717122947000, 3, 'LINESTRING (0 0, 0 1, 1 2)');")
        tdSql.execute("insert into t_ge values(1717122948000, 3, 'LINESTRING EMPTY');")
        tdSql.execute("insert into t_ge values(1717122949000, 4, 'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))');")
        tdSql.execute("insert into t_ge values(1717122950000, 4, 'POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1))');")
        tdSql.execute("insert into t_ge values(1717122951000, 4, 'POLYGON EMPTY');")
        tdSql.execute("insert into t_ge values(1717122952000, 5, 'MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))');")
        tdSql.execute("insert into t_ge values(1717122953000, 6, 'MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)), ((4 3, 6 3, 6 1, 4 1, 4 3)))');")
        tdSql.execute("insert into t_ge values(1717122954000, 7, 'GEOMETRYCOLLECTION (MULTIPOINT((0 0), (1 1)), POINT(3 4), LINESTRING(2 3, 3 4))');")
        tdSql.query("select * from t_ge;")
        tdSql.checkRows(12)
        tdSql.query("select * from t_ge where id=1;")
        tdSql.checkRows(2)
        tdSql.query("select * from t_ge where id=2;")
        tdSql.checkRows(2)
        tdSql.query("select * from t_ge where id=3;")
        tdSql.checkRows(2)
        tdSql.query("select * from t_ge where id=4;")
        tdSql.checkRows(3)
        tdSql.query("select * from t_ge where id=5;")
        tdSql.checkRows(1)
        tdSql.query("select * from t_ge where id=6;")
        tdSql.checkRows(1)
        tdSql.query("select * from t_ge where id=7;")
        tdSql.checkRows(1)

    def checkDataType(self):
        tdLog.info(f"check datatype")

        self.checkGeometry()



    # run
    def run(self):
        tdLog.debug(f"start to excute {__file__}")

        # check insert datatype
        self.checkDataType()

        tdLog.success(f"{__file__} successfully executed")



tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())