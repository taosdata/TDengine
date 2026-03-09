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
from new_test_framework.utils import tdLog, tdSql, tdCom
import os


class TestVTablePlanPushdownTsSubquery:
    def setup_class(cls):
        tdLog.info("prepare origin tables for vtable ts pushdown test.")

        tdSql.execute("drop database if exists test_vtable_ts_pushdown_origin;")
        tdSql.execute("create database test_vtable_ts_pushdown_origin;")
        tdSql.execute("use test_vtable_ts_pushdown_origin;")

        tdSql.execute("create table ntb_0(event_time timestamp, value_col int);")
        tdSql.execute("insert into ntb_0 values "
                      "('2020-10-10 10:00:00', 1) "
                      "('2020-10-10 10:00:01', 2) "
                      "('2020-10-10 10:00:02', 3) "
                      "('2020-10-10 10:00:03', 4);")

        tdSql.execute("create stable stb_0(event_time timestamp, value_col int) tags (group_id int);")
        tdSql.execute("create table ctb_0 using stb_0 tags (1);")
        tdSql.execute("insert into ctb_0 values "
                      "('2020-10-10 10:00:00', 11) "
                      "('2020-10-10 10:00:01', 12) "
                      "('2020-10-10 10:00:02', 13) "
                      "('2020-10-10 10:00:03', 14);")

        tdSql.execute(
            "create table bound_t("
            "ts timestamp, "
            "lower_ts timestamp, "
            "upper_ts timestamp, "
            "exact_ts timestamp, "
            "mid_ts timestamp);"
        )
        tdSql.execute("insert into bound_t values "
                      "('2020-10-10 09:59:59', "
                      "'2020-10-10 10:00:01', "
                      "'2020-10-10 10:00:03', "
                      "'2020-10-10 10:00:02', "
                      "'2020-10-10 10:00:02');")

        tdLog.info("prepare virtual tables for vtable ts pushdown test.")
        tdSql.execute("drop database if exists test_vtable_ts_pushdown_vtb;")
        tdSql.execute("create database test_vtable_ts_pushdown_vtb;")
        tdSql.execute("use test_vtable_ts_pushdown_vtb;")

        tdSql.execute("create vtable ntb_0_vtb("
                      "ts timestamp, "
                      "value_col int from test_vtable_ts_pushdown_origin.ntb_0.value_col);")

        tdSql.execute("create stable vstb_0(ts timestamp, value_col int) tags (group_id int) virtual 1;")
        tdSql.execute("create vtable ctb_0_vtb("
                      "value_col from test_vtable_ts_pushdown_origin.ctb_0.value_col) "
                      "using vstb_0 tags (1);")

    def run_normal_query(self, test_case):
        tdLog.info(f"test case : {test_case}.")
        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", f"{test_case}.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", f"{test_case}.ans")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, test_case)

    def test_vtable_plan_ts_subquery(self):
        """Query: virtual table ts subquery explain plan

        1. validate explain plan with ts scalar-subquery range conditions

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-3-9 Jing Sima Added

        """

        self.run_normal_query("test_vtable_plan_ts_subquery")
