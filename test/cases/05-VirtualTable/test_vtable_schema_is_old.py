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
from new_test_framework.utils import tdLog, tdSql, etool, tdCom
import time


class testVtableSchemaIsOld:

    def test_unorderd_vtable_column_and_origin_table_column(self):
        tdLog.info(f"prepare org tables.")

        tdSql.execute("drop database if exists test_vtable_schema_is_old_origin;")
        tdSql.execute("create database test_vtable_schema_is_old_origin;")
        tdSql.execute("use test_vtable_schema_is_old_origin;")

        tdSql.execute("create stable meters(ts timestamp, current float, voltage int, phase float) tags (groupid int, location varchar(24));")
        tdSql.execute("create table d0 using meters tags (1, 'chengdu');")
        tdSql.execute("insert into d0 values (now, 0, 0, 0);")

        tdLog.info(f"prepare virtual tables.")
        tdSql.execute("drop database if exists test_vtable_schema_is_old_vtb;")
        tdSql.execute("create database test_vtable_schema_is_old_vtb;")
        tdSql.execute("use test_vtable_schema_is_old_vtb;")

        tdSql.execute("create stable meters_vtb(ts timestamp, voltage int, current float, phase float) tags (groupid int, location varchar(24)) virtual 1;")
        tdSql.execute("create vtable d0_vtb("
                      "voltage from test_vtable_schema_is_old_origin.d0.voltage, "
                      "current from test_vtable_schema_is_old_origin.d0.current,"
                      "phase from test_vtable_schema_is_old_origin.d0.phase)"
                      "USING meters_vtb "
                      "TAGS (1, 'v_chegndu');")

        tdSql.query("select * from meters_vtb;")
        tdSql.checkRows(1)
        tdSql.query("select * from d0_vtb;")
        tdSql.checkRows(1)

    def test_vtable_multi_columns_use_ts_column(self):
        tdLog.info(f"prepare org tables.")

        tdSql.execute("drop database if exists test_vtable_schema_is_old_origin_1;")
        tdSql.execute("create database test_vtable_schema_is_old_origin_1;")
        tdSql.execute("use test_vtable_schema_is_old_origin_1;")

        tdSql.execute("create stable meters(ts timestamp, current float, voltage int, phase float) tags (groupid int, location varchar(24));")
        tdSql.execute("create table d0 using meters tags (1, 'chengdu');")
        tdSql.execute("insert into d0 values ('2020-10-10 11:11:11', 0, 0, 0);")

        tdSql.execute("drop database if exists test_vtable_schema_is_old_vtb_2;")
        tdSql.execute("create database test_vtable_schema_is_old_vtb_2;")
        tdSql.execute("use test_vtable_schema_is_old_vtb_2;")

        tdSql.execute("create vtable d0_vtb("
                      "ts timestamp,"
                      "ts1 timestamp from test_vtable_schema_is_old_origin_1.d0.ts, "
                      "ts2 timestamp from test_vtable_schema_is_old_origin_1.d0.ts,"
                      "ts3 timestamp from test_vtable_schema_is_old_origin_1.d0.ts);")

        tdSql.query("select * from d0_vtb;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2020-10-10 11:11:11')
        tdSql.checkData(0, 1, '2020-10-10 11:11:11')
        tdSql.checkData(0, 2, '2020-10-10 11:11:11')
        tdSql.checkData(0, 3, '2020-10-10 11:11:11')
