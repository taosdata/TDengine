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
from decimal import Decimal

from new_test_framework.utils import tdLog, tdSql, tdDnodes


DB_NAME = "test_vtable_decimal"


class TestVTableDecimal:

    @staticmethod
    def find_row(field_name):
        for row in range(tdSql.getRows()):
            if tdSql.getData(row, 0) == field_name:
                return row
        raise AssertionError(f"field {field_name} not found in result set")

    @staticmethod
    def check_decimal(row, col, expect):
        value = tdSql.getData(row, col)
        assert Decimal(str(value)) == Decimal(expect), f"expect {expect}, got {value}"

    @staticmethod
    def check_decimal_close(row, col, expect, tolerance="0.000001"):
        value = Decimal(str(tdSql.getData(row, col)))
        delta = abs(value - Decimal(expect))
        assert delta <= Decimal(tolerance), f"expect {expect} +/- {tolerance}, got {value}"

    @staticmethod
    def check_column_meta(table_name, col_name, precision, scale, source):
        tdSql.query(
            "select col_precision, col_scale, col_source "
            "from information_schema.ins_columns "
            f"where db_name='{DB_NAME}' and table_name='{table_name}' and col_name='{col_name}'"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, precision)
        tdSql.checkData(0, 1, scale)
        tdSql.checkData(0, 2, source)

    @staticmethod
    def check_non_decimal_meta(table_name, col_name, source):
        tdSql.query(
            "select col_precision, col_scale, col_source "
            "from information_schema.ins_columns "
            f"where db_name='{DB_NAME}' and table_name='{table_name}' and col_name='{col_name}'"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, source)

    @staticmethod
    def check_no_column_meta(table_name, col_name):
        tdSql.query(
            "select col_name from information_schema.ins_columns "
            f"where db_name='{DB_NAME}' and table_name='{table_name}' and col_name='{col_name}'"
        )
        tdSql.checkRows(0)

    @staticmethod
    def check_projected_rows(expect_rows):
        tdSql.checkRows(len(expect_rows))
        for row_idx, (ts_val, dec64_val, dec128_val, metric) in enumerate(expect_rows):
            tdSql.checkData(row_idx, 0, ts_val)
            TestVTableDecimal.check_decimal(row_idx, 1, dec64_val)
            TestVTableDecimal.check_decimal(row_idx, 2, dec128_val)
            tdSql.checkData(row_idx, 3, metric)

    @classmethod
    def setup_class(cls):
        tdLog.info("prepare decimal origin tables.")
        tdSql.execute(f"drop database if exists {DB_NAME}")
        tdSql.execute(f"create database {DB_NAME}")
        tdSql.execute(f"use {DB_NAME}")

        tdSql.execute(
            "create table org_ntb_0 ("
            "ts timestamp, "
            "dec64_col decimal(18,2), "
            "dec128_col decimal(20,4), "
            "dec64_max decimal(18,18), "
            "dec128_max decimal(38,10), "
            "metric int)"
        )
        tdSql.execute(
            "create table org_ntb_1 ("
            "ts timestamp, "
            "dec64_col decimal(10,3), "
            "dec128_col decimal(30,8), "
            "metric float)"
        )
        tdSql.execute(
            "create stable org_stb ("
            "ts timestamp, "
            "dec64_col decimal(18,2), "
            "dec128_col decimal(20,4), "
            "metric int"
            ") tags (group_id int)"
        )
        tdSql.execute("create table org_ctb_0 using org_stb tags (0)")
        tdSql.execute("create table org_ctb_1 using org_stb tags (1)")

        org_ntb_0_rows = [
            (1700000000000, "1.25", "100.1234", "0.123456789012345678", "12345678901234567890.1234567890", 10),
            (1700000001000, "2.50", "200.5678", "0.000000000000000001", "99999999999999999999.9999999999", 20),
            (1700000002000, "3.75", "300.0001", "0.999999999999999999", "0.0000000001", 30),
        ]
        for ts_val, dec64_val, dec128_val, dec64_max, dec128_max, metric in org_ntb_0_rows:
            tdSql.execute(
                "insert into org_ntb_0 values "
                f"({ts_val}, {dec64_val}, {dec128_val}, {dec64_max}, {dec128_max}, {metric})"
            )

        org_ntb_1_rows = [
            (1700000000000, "1.234", "12345678901234567890.12345678", "1.5"),
            (1700000001000, "9.876", "99999999999999999999.99999999", "2.5"),
        ]
        for ts_val, dec64_val, dec128_val, metric in org_ntb_1_rows:
            tdSql.execute(
                f"insert into org_ntb_1 values ({ts_val}, {dec64_val}, {dec128_val}, {metric})"
            )

        org_ctb_0_rows = [
            (1700000000000, "1.25", "100.1234", 10),
            (1700000001000, "2.50", "200.5678", 20),
            (1700000002000, "3.75", "300.0001", 30),
        ]
        for ts_val, dec64_val, dec128_val, metric in org_ctb_0_rows:
            tdSql.execute(
                f"insert into org_ctb_0 values ({ts_val}, {dec64_val}, {dec128_val}, {metric})"
            )

        org_ctb_1_rows = [
            (1700000000000, "10.00", "1000.0000", 100),
            (1700000001000, "20.00", "2000.0000", 200),
        ]
        for ts_val, dec64_val, dec128_val, metric in org_ctb_1_rows:
            tdSql.execute(
                f"insert into org_ctb_1 values ({ts_val}, {dec64_val}, {dec128_val}, {metric})"
            )

        tdLog.info("prepare decimal virtual tables.")
        tdSql.execute(
            "create stable vstb_decimal ("
            "ts timestamp, "
            "dec64_col decimal(18,2), "
            "dec128_col decimal(20,4), "
            "metric int"
            ") tags (tag_plain int) virtual 1"
        )
        tdSql.execute(
            "create vtable vntb_ref_ntb ("
            "ts timestamp, "
            "dec64_col decimal(18,2) from org_ntb_0.dec64_col, "
            "dec128_col decimal(20,4) from org_ntb_0.dec128_col, "
            "metric int from org_ntb_0.metric)"
        )
        tdSql.execute(
            "create vtable vntb_ref_ctb ("
            "ts timestamp, "
            "dec64_col decimal(18,2) from org_ctb_0.dec64_col, "
            "dec128_col decimal(20,4) from org_ctb_0.dec128_col, "
            "metric int from org_ctb_0.metric)"
        )
        tdSql.execute(
            "create vtable vntb_multi_src ("
            "ts timestamp, "
            "dec64_from_ctb decimal(18,2) from org_ctb_0.dec64_col, "
            "dec128_from_ntb decimal(20,4) from org_ntb_0.dec128_col, "
            "metric_from_ctb int from org_ctb_0.metric)"
        )
        tdSql.execute(
            "create vtable vntb_local_dec ("
            "ts timestamp, "
            "dec64_col decimal(18,2), "
            "dec128_col decimal(38,10))"
        )
        tdSql.execute(
            "create vtable vntb_mixed_dec ("
            "ts timestamp, "
            "dec64_ref decimal(18,2) from org_ctb_0.dec64_col, "
            "dec128_local decimal(30,8), "
            "metric int from org_ctb_0.metric)"
        )
        tdSql.execute(
            "create vtable vntb_precision_decimal ("
            "ts timestamp, "
            "dec64_max decimal(18,18) from org_ntb_0.dec64_max, "
            "dec128_max decimal(38,10) from org_ntb_0.dec128_max)"
        )
        tdSql.execute(
            "create vtable vctb_full ("
            "dec64_col from org_ctb_0.dec64_col, "
            "dec128_col from org_ntb_0.dec128_col, "
            "metric from org_ctb_0.metric)"
            " using vstb_decimal tags (1)"
        )
        tdSql.execute(
            "create vtable vctb_partial ("
            "dec64_col from org_ctb_0.dec64_col)"
            " using vstb_decimal tags (2)"
        )
        tdSql.execute("create vtable vctb_empty using vstb_decimal tags (3)")
        tdSql.execute(
            "create vtable vctb_src0 ("
            "dec64_col from org_ctb_0.dec64_col, "
            "dec128_col from org_ctb_0.dec128_col, "
            "metric from org_ctb_0.metric)"
            " using vstb_decimal tags (10)"
        )
        tdSql.execute(
            "create vtable vctb_src1 ("
            "dec64_col from org_ctb_1.dec64_col, "
            "dec128_col from org_ctb_1.dec128_col, "
            "metric from org_ctb_1.metric)"
            " using vstb_decimal tags (20)"
        )

    def test_decimal_create_and_ref_validation(self):
        """Create: decimal create boundaries and reference validation

        1. validate virtual stable decimal precision and scale boundaries
        2. validate virtual normal table decimal reference matching
        3. validate virtual child table decimal reference matching
        4. verify decimal precision and scale metadata are persisted on creation

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, decimal, create

        Jira: None

        History:
            - 2026-4-1 Joey Sima Expanded by test plan

        """
        tdSql.execute(f"use {DB_NAME}")

        tdSql.execute("create stable vstb_p1s0 (ts timestamp, c1 decimal(1,0)) tags (t1 int) virtual 1")
        tdSql.execute("create stable vstb_p18s18 (ts timestamp, c1 decimal(18,18)) tags (t1 int) virtual 1")
        tdSql.execute("create stable vstb_p18s0 (ts timestamp, c1 decimal(18,0)) tags (t1 int) virtual 1")
        tdSql.execute("create stable vstb_p19s0 (ts timestamp, c1 decimal(19,0)) tags (t1 int) virtual 1")
        tdSql.execute("create stable vstb_p38s0 (ts timestamp, c1 decimal(38,0)) tags (t1 int) virtual 1")
        tdSql.execute("create stable vstb_p38s38 (ts timestamp, c1 decimal(38,38)) tags (t1 int) virtual 1")

        tdSql.error("create stable vstb_bad_p0 (ts timestamp, c1 decimal(0,0)) tags (t1 int) virtual 1")
        tdSql.error("create stable vstb_bad_p39 (ts timestamp, c1 decimal(39,0)) tags (t1 int) virtual 1")
        tdSql.error("create stable vstb_bad_s19 (ts timestamp, c1 decimal(18,19)) tags (t1 int) virtual 1")
        tdSql.error("create stable vstb_bad_s39 (ts timestamp, c1 decimal(38,39)) tags (t1 int) virtual 1")

        tdSql.query(f"show create stable {DB_NAME}.vstb_p18s18")
        tdSql.checkRows(1)
        assert "DECIMAL(18,18)" in tdSql.getData(0, 1)

        tdSql.query(f"show create stable {DB_NAME}.vstb_p38s38")
        tdSql.checkRows(1)
        assert "DECIMAL(38,38)" in tdSql.getData(0, 1)

        tdSql.query(f"describe {DB_NAME}.vstb_p19s0")
        tdSql.checkCols(7)
        tdSql.checkRows(3)
        dec19_row = self.find_row("c1")
        tdSql.checkData(dec19_row, 1, "DECIMAL(19, 0)")

        tdSql.query(f"describe {DB_NAME}.vstb_p38s38")
        tdSql.checkCols(7)
        tdSql.checkRows(3)
        dec38_row = self.find_row("c1")
        tdSql.checkData(dec38_row, 1, "DECIMAL(38, 38)")

        tdSql.error(
            "create stable vstb_dec_tag_bad ("
            "ts timestamp, metric int"
            ") tags (group_id int, tag_dec64 decimal(18,2), tag_dec128 decimal(30,6)) virtual 1"
        )
        tdSql.error(
            "create stable vstb_dec_both_bad ("
            "ts timestamp, dec64_col decimal(18,2), dec128_col decimal(38,10)"
            ") tags (tag_dec decimal(10,2)) virtual 1"
        )
        tdSql.error(
            "create stable vstb_dec_tag_ref_bad ("
            "ts timestamp, metric int"
            ") tags (group_id int, tag_dec decimal(10,2)) virtual 1"
        )
        tdSql.error(
            "create stable stb_dec_tag_bad ("
            "ts timestamp, metric int"
            ") tags (tag_dec decimal(10,2))"
        )
        tdSql.error(
            "create stable vstb_dec_tag_bad2 ("
            "ts timestamp, metric int"
            ") tags (tag_dec decimal(10,2)) virtual 1"
        )

        tdSql.error(
            "create vtable vntb_bad_type ("
            "ts timestamp, "
            "dec64_col decimal(18,2) from org_ntb_0.dec128_col)"
        )
        tdSql.error(
            "create vtable vntb_bad_prec ("
            "ts timestamp, "
            "dec64_col decimal(10,2) from org_ntb_0.dec64_col)"
        )
        tdSql.error(
            "create vtable vntb_bad_scale ("
            "ts timestamp, "
            "dec64_col decimal(18,3) from org_ntb_0.dec64_col)"
        )
        tdSql.error(
            "create vtable vntb_bad_dec_to_int ("
            "ts timestamp, "
            "dec64_col decimal(18,2) from org_ntb_0.metric)"
        )
        tdSql.error(
            "create vtable vntb_bad_int_to_dec ("
            "ts timestamp, "
            "metric int from org_ntb_0.dec64_col)"
        )
        tdSql.error(
            "create vtable vctb_bad_ref ("
            "dec64_col from org_ntb_1.dec64_col, "
            "dec128_col from org_ntb_0.dec128_col, "
            "metric from org_ntb_0.metric)"
            " using vstb_decimal tags (99)"
        )

        self.check_column_meta("vntb_ref_ntb", "dec64_col", 18, 2, f"{DB_NAME}.org_ntb_0.dec64_col")
        self.check_column_meta("vntb_ref_ntb", "dec128_col", 20, 4, f"{DB_NAME}.org_ntb_0.dec128_col")
        self.check_non_decimal_meta("vntb_ref_ntb", "metric", f"{DB_NAME}.org_ntb_0.metric")
        self.check_column_meta("vntb_precision_decimal", "dec64_max", 18, 18, f"{DB_NAME}.org_ntb_0.dec64_max")
        self.check_column_meta("vntb_precision_decimal", "dec128_max", 38, 10, f"{DB_NAME}.org_ntb_0.dec128_max")
        self.check_column_meta("vctb_full", "dec64_col", 18, 2, f"{DB_NAME}.org_ctb_0.dec64_col")
        self.check_column_meta("vctb_full", "dec128_col", 20, 4, f"{DB_NAME}.org_ntb_0.dec128_col")
        self.check_column_meta("vctb_partial", "dec64_col", 18, 2, f"{DB_NAME}.org_ctb_0.dec64_col")

    def test_decimal_alter_paths(self):
        """Alter: decimal add, drop, and set reference

        1. validate add column on virtual normal tables with and without references
        2. validate alter set keeps decimal precision and scale rules
        3. validate drop column removes decimal metadata
        4. validate virtual stable add and drop decimal columns propagate to child tables

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, decimal, alter

        Jira: None

        History:
            - 2026-4-1 Joey Sima Expanded by test plan

        """
        tdSql.execute(f"use {DB_NAME}")

        tdSql.execute(
            "create vtable vntb_decimal_alter ("
            "ts timestamp, "
            "dec64_col decimal(18,2) from org_ctb_0.dec64_col, "
            "dec128_local decimal(30,8), "
            "metric int from org_ctb_0.metric)"
        )
        tdSql.execute("alter vtable vntb_decimal_alter add column extra_dec decimal(38,18)")
        tdSql.execute(
            "alter vtable vntb_decimal_alter add column extra_dec128 decimal(20,4) from org_ntb_0.dec128_col"
        )
        tdSql.error(
            "alter vtable vntb_decimal_alter add column bad_dec_scale decimal(10,3) from org_ntb_0.dec64_col"
        )
        tdSql.error(
            "alter vtable vntb_decimal_alter add column bad_dec_type decimal(18,2) from org_ntb_0.dec128_col"
        )
        tdSql.execute(
            "alter vtable vntb_decimal_alter alter column dec128_local set org_ntb_1.dec128_col"
        )
        tdSql.error(
            "alter vtable vntb_decimal_alter alter column dec64_col set org_ntb_1.dec64_col"
        )

        self.check_column_meta("vntb_decimal_alter", "extra_dec", 38, 18, None)
        self.check_column_meta(
            "vntb_decimal_alter", "extra_dec128", 20, 4, f"{DB_NAME}.org_ntb_0.dec128_col"
        )
        self.check_column_meta(
            "vntb_decimal_alter", "dec128_local", 30, 8, f"{DB_NAME}.org_ntb_1.dec128_col"
        )

        tdSql.query(f"describe {DB_NAME}.vntb_decimal_alter")
        tdSql.checkCols(5)
        extra_dec_row = self.find_row("extra_dec")
        tdSql.checkData(extra_dec_row, 1, "DECIMAL(38, 18)")

        tdSql.execute("alter vtable vntb_decimal_alter drop column extra_dec")
        self.check_no_column_meta("vntb_decimal_alter", "extra_dec")

        tdSql.execute(
            "create stable vstb_decimal_alter ("
            "ts timestamp, "
            "dec64_col decimal(18,2), "
            "dec128_col decimal(20,4), "
            "metric int"
            ") tags (tag_plain int) virtual 1"
        )
        tdSql.execute(
            "create vtable vctb_decimal_alter ("
            "dec64_col from org_ctb_0.dec64_col, "
            "dec128_col from org_ctb_0.dec128_col, "
            "metric from org_ctb_0.metric)"
            " using vstb_decimal_alter tags (100)"
        )
        tdSql.execute("alter stable vstb_decimal_alter add column new_dec decimal(38,10)")

        self.check_column_meta("vctb_decimal_alter", "new_dec", 38, 10, None)

        tdSql.query(f"describe {DB_NAME}.vstb_decimal_alter")
        tdSql.checkCols(7)
        stable_new_dec_row = self.find_row("new_dec")
        tdSql.checkData(stable_new_dec_row, 1, "DECIMAL(38, 10)")

        tdSql.query(f"describe {DB_NAME}.vctb_decimal_alter")
        tdSql.checkCols(5)
        new_dec_row = self.find_row("new_dec")
        tdSql.checkData(new_dec_row, 1, "DECIMAL(38, 10)")

        tdSql.error("alter stable vstb_decimal add tag new_dec_tag decimal(18,6)")

        tdSql.query(f"select dec64_col, new_dec from {DB_NAME}.vctb_decimal_alter order by ts")
        tdSql.checkRows(3)
        for row_idx in range(3):
            self.check_decimal(row_idx, 0, ["1.25", "2.50", "3.75"][row_idx])
            tdSql.checkData(row_idx, 1, None)

        tdSql.execute("alter stable vstb_decimal_alter drop column new_dec")
        self.check_no_column_meta("vstb_decimal_alter", "new_dec")
        self.check_no_column_meta("vctb_decimal_alter", "new_dec")

    def test_decimal_describe_show_create_and_information_schema(self):
        """Meta: describe, show create, and ins_columns for decimal virtual tables

        1. verify describe keeps the current decimal virtual-table result shape
        2. verify show create renders DECIMAL with references
        3. verify information_schema reports precision and scale for decimal columns
        4. verify non-decimal columns keep null precision and scale

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, decimal, metadata

        Jira: None

        History:
            - 2026-4-1 Joey Sima Expanded by test plan

        """
        tdSql.execute(f"use {DB_NAME}")

        tdSql.query(f"describe {DB_NAME}.vntb_ref_ntb")
        tdSql.checkCols(5)
        tdSql.checkRows(4)
        dec64_row = self.find_row("dec64_col")
        tdSql.checkData(dec64_row, 1, "DECIMAL(18, 2)")
        dec128_row = self.find_row("dec128_col")
        tdSql.checkData(dec128_row, 1, "DECIMAL(20, 4)")

        tdSql.query(f"describe {DB_NAME}.vctb_full")
        tdSql.checkCols(5)
        tdSql.checkRows(5)
        child_dec64_row = self.find_row("dec64_col")
        tdSql.checkData(child_dec64_row, 1, "DECIMAL(18, 2)")
        child_dec128_row = self.find_row("dec128_col")
        tdSql.checkData(child_dec128_row, 1, "DECIMAL(20, 4)")
        tag_row = self.find_row("tag_plain")
        tdSql.checkData(tag_row, 3, "TAG")

        tdSql.query(f"describe {DB_NAME}.vstb_decimal")
        tdSql.checkCols(7)
        tdSql.checkRows(5)
        stb_dec128_row = self.find_row("dec128_col")
        tdSql.checkData(stb_dec128_row, 1, "DECIMAL(20, 4)")

        tdSql.query(f"show create vtable {DB_NAME}.vntb_ref_ntb")
        tdSql.checkRows(1)
        create_sql = tdSql.getData(0, 1)
        assert "DECIMAL(18,2)" in create_sql
        assert "DECIMAL(20,4)" in create_sql
        assert "FROM `test_vtable_decimal`.`org_ntb_0`.`dec64_col`" in create_sql
        assert "FROM `test_vtable_decimal`.`org_ntb_0`.`dec128_col`" in create_sql

        tdSql.query(f"show create stable {DB_NAME}.vstb_decimal")
        tdSql.checkRows(1)
        create_sql = tdSql.getData(0, 1)
        assert "`dec64_col` DECIMAL(18,2)" in create_sql
        assert "`dec128_col` DECIMAL(20,4)" in create_sql

        tdSql.query(f"show create vtable {DB_NAME}.vctb_full")
        tdSql.checkRows(1)
        create_sql = tdSql.getData(0, 1)
        assert "FROM `test_vtable_decimal`.`org_ctb_0`.`dec64_col`" in create_sql
        assert "FROM `test_vtable_decimal`.`org_ntb_0`.`dec128_col`" in create_sql

        self.check_column_meta("vntb_ref_ntb", "dec64_col", 18, 2, f"{DB_NAME}.org_ntb_0.dec64_col")
        self.check_column_meta("vntb_ref_ntb", "dec128_col", 20, 4, f"{DB_NAME}.org_ntb_0.dec128_col")
        self.check_column_meta("vctb_full", "dec64_col", 18, 2, f"{DB_NAME}.org_ctb_0.dec64_col")
        self.check_column_meta("vctb_full", "dec128_col", 20, 4, f"{DB_NAME}.org_ntb_0.dec128_col")
        self.check_non_decimal_meta("vntb_ref_ntb", "metric", f"{DB_NAME}.org_ntb_0.metric")

        tdSql.query(
            "select stable_name, columns, `tags` "
            "from information_schema.ins_stables "
            f"where db_name='{DB_NAME}' and stable_name='vstb_decimal'"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "vstb_decimal")
        tdSql.checkData(0, 1, 4)
        tdSql.checkData(0, 2, 1)

    def test_decimal_query_paths(self):
        """Query: decimal projection, merge, and null-path behavior

        1. validate select * on virtual normal and child tables with decimal columns
        2. validate multi-source decimal projection on virtual normal tables
        3. validate virtual stable merge across multiple child mappings
        4. validate local decimal columns remain null or empty without data sources

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, decimal, query

        Jira: None

        History:
            - 2026-4-1 Joey Sima Expanded by test plan

        """
        tdSql.execute(f"use {DB_NAME}")

        expect_rows = [
            (1700000000000, "1.25", "100.1234", 10),
            (1700000001000, "2.50", "200.5678", 20),
            (1700000002000, "3.75", "300.0001", 30),
        ]

        tdSql.query(f"select * from {DB_NAME}.vntb_ref_ntb order by ts")
        tdSql.checkCols(4)
        self.check_projected_rows(expect_rows)

        tdSql.query(f"select * from {DB_NAME}.vctb_full order by ts")
        tdSql.checkCols(4)
        self.check_projected_rows(expect_rows)

        tdSql.query(f"select ts, dec64_col, dec128_col, metric from {DB_NAME}.vctb_partial order by ts")
        tdSql.checkRows(3)
        for row_idx, (ts_val, dec64_val, _, _) in enumerate(expect_rows):
            tdSql.checkData(row_idx, 0, ts_val)
            self.check_decimal(row_idx, 1, dec64_val)
            tdSql.checkData(row_idx, 2, None)
            tdSql.checkData(row_idx, 3, None)

        tdSql.query(f"describe {DB_NAME}.vctb_empty")
        tdSql.checkCols(5)
        tdSql.checkRows(5)
        empty_dec64_row = self.find_row("dec64_col")
        empty_dec128_row = self.find_row("dec128_col")
        empty_metric_row = self.find_row("metric")
        tdSql.checkData(empty_dec64_row, 1, "DECIMAL(18, 2)")
        tdSql.checkData(empty_dec128_row, 1, "DECIMAL(20, 4)")
        tdSql.checkData(empty_metric_row, 1, "INT")

        self.check_column_meta("vctb_empty", "dec64_col", 18, 2, None)
        self.check_column_meta("vctb_empty", "dec128_col", 20, 4, None)
        self.check_non_decimal_meta("vctb_empty", "metric", None)

        tdSql.query(f"select ts, dec64_col, dec128_col, metric from {DB_NAME}.vctb_empty")
        if tdSql.getRows() == 0:
            tdSql.checkRows(0)
        else:
            for row_idx in range(tdSql.getRows()):
                tdSql.checkData(row_idx, 0, None)
                tdSql.checkData(row_idx, 1, None)
                tdSql.checkData(row_idx, 2, None)
                tdSql.checkData(row_idx, 3, None)

        tdSql.query(
            "select ts, dec64_from_ctb, dec128_from_ntb, metric_from_ctb "
            f"from {DB_NAME}.vntb_multi_src order by ts"
        )
        tdSql.checkCols(4)
        self.check_projected_rows(expect_rows)

        tdSql.query(
            "select ts, dec64_ref, dec128_local, metric "
            f"from {DB_NAME}.vntb_mixed_dec order by ts"
        )
        tdSql.checkRows(3)
        for row_idx, (ts_val, dec64_val, _, metric) in enumerate(expect_rows):
            tdSql.checkData(row_idx, 0, ts_val)
            self.check_decimal(row_idx, 1, dec64_val)
            tdSql.checkData(row_idx, 2, None)
            tdSql.checkData(row_idx, 3, metric)

        tdSql.query(f"select * from {DB_NAME}.vntb_local_dec")
        tdSql.checkRows(0)

        tdSql.query(
            "select tbname, tag_plain, ts, dec64_col, dec128_col, metric "
            f"from {DB_NAME}.vstb_decimal where tag_plain in (10, 20) order by tag_plain, ts"
        )
        tdSql.checkRows(5)
        stable_expect_rows = [
            ("vctb_src0", 10, 1700000000000, "1.25", "100.1234", 10),
            ("vctb_src0", 10, 1700000001000, "2.50", "200.5678", 20),
            ("vctb_src0", 10, 1700000002000, "3.75", "300.0001", 30),
            ("vctb_src1", 20, 1700000000000, "10.00", "1000.0000", 100),
            ("vctb_src1", 20, 1700000001000, "20.00", "2000.0000", 200),
        ]
        for row_idx, (tbname, tag_plain, ts_val, dec64_val, dec128_val, metric) in enumerate(stable_expect_rows):
            tdSql.checkData(row_idx, 0, tbname)
            tdSql.checkData(row_idx, 1, tag_plain)
            tdSql.checkData(row_idx, 2, ts_val)
            self.check_decimal(row_idx, 3, dec64_val)
            self.check_decimal(row_idx, 4, dec128_val)
            tdSql.checkData(row_idx, 5, metric)

    def test_decimal_filter_aggregate_and_advanced_query(self):
        """Query: decimal filters, aggregates, and advanced operators

        1. validate decimal equality, range, and mixed-type filters
        2. validate aggregate, first, and last on decimal columns
        3. validate stable-level group aggregates across child mappings
        4. validate limit, order by, distinct, subquery, union all, and arithmetic expressions

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, decimal, aggregate

        Jira: None

        History:
            - 2026-4-1 Joey Sima Expanded by test plan

        """
        tdSql.execute(f"use {DB_NAME}")

        tdSql.query(f"select count(*) from {DB_NAME}.vntb_multi_src where dec64_from_ctb = 1.25")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select count(*) from {DB_NAME}.vntb_multi_src where dec64_from_ctb >= 2.50")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

        tdSql.query(
            f"select count(*) from {DB_NAME}.vntb_multi_src "
            "where dec128_from_ntb between 100.0000 and 250.0000"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

        tdSql.query(f"select count(*) from {DB_NAME}.vntb_multi_src where dec64_from_ctb > 2")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

        tdSql.query(
            f"select count(*) from {DB_NAME}.vntb_multi_src "
            "where dec64_from_ctb >= 2.50 and dec128_from_ntb < 300.0000"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query(
            "select count(*), sum(dec64_from_ctb), avg(dec64_from_ctb), "
            "min(dec64_from_ctb), max(dec64_from_ctb), "
            f"min(dec128_from_ntb), max(dec128_from_ntb) from {DB_NAME}.vntb_multi_src"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)
        self.check_decimal(0, 1, "7.50")
        self.check_decimal(0, 2, "2.50")
        self.check_decimal(0, 3, "1.25")
        self.check_decimal(0, 4, "3.75")
        self.check_decimal(0, 5, "100.1234")
        self.check_decimal(0, 6, "300.0001")

        tdSql.query(
            "select first(dec64_from_ctb), last(dec64_from_ctb), "
            f"first(dec128_from_ntb), last(dec128_from_ntb) from {DB_NAME}.vntb_multi_src"
        )
        tdSql.checkRows(1)
        self.check_decimal(0, 0, "1.25")
        self.check_decimal(0, 1, "3.75")
        self.check_decimal(0, 2, "100.1234")
        self.check_decimal(0, 3, "300.0001")

        tdSql.error(
            f"select stddev(dec64_col), variance(dec64_col) from {DB_NAME}.vntb_ref_ntb"
        )

        tdSql.query(
            f"select sum(dec64_col) from {DB_NAME}.vntb_ref_ntb where dec64_col > 1.25"
        )
        tdSql.checkRows(1)
        self.check_decimal(0, 0, "6.25")

        tdSql.query(
            f"select avg(dec128_col) from {DB_NAME}.vstb_decimal "
            "where tag_plain = 20 and metric > 15"
        )
        tdSql.checkRows(1)
        self.check_decimal_close(0, 0, "1500.0000", "0.000001")

        tdSql.query(
            "select tag_plain, count(*), sum(dec64_col) "
            f"from {DB_NAME}.vstb_decimal where tag_plain in (10, 20) "
            "group by tag_plain order by tag_plain"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 10)
        tdSql.checkData(0, 1, 3)
        self.check_decimal(0, 2, "7.50")
        tdSql.checkData(1, 0, 20)
        tdSql.checkData(1, 1, 2)
        self.check_decimal(1, 2, "30.00")

        tdSql.query(
            f"select dec64_from_ctb, dec128_from_ntb from {DB_NAME}.vntb_multi_src "
            "order by ts limit 2 offset 1"
        )
        tdSql.checkRows(2)
        self.check_decimal(0, 0, "2.50")
        self.check_decimal(0, 1, "200.5678")
        self.check_decimal(1, 0, "3.75")
        self.check_decimal(1, 1, "300.0001")

        tdSql.query(f"select dec64_from_ctb from {DB_NAME}.vntb_multi_src order by dec64_from_ctb desc")
        tdSql.checkRows(3)
        self.check_decimal(0, 0, "3.75")
        self.check_decimal(2, 0, "1.25")

        tdSql.query(
            f"select distinct dec64_from_ctb from {DB_NAME}.vntb_multi_src order by dec64_from_ctb"
        )
        tdSql.checkRows(3)
        self.check_decimal(0, 0, "1.25")
        self.check_decimal(1, 0, "2.50")
        self.check_decimal(2, 0, "3.75")

        tdSql.query(
            "select * from ("
            f"select dec64_from_ctb, dec128_from_ntb from {DB_NAME}.vntb_multi_src"
            ") order by dec64_from_ctb"
        )
        tdSql.checkRows(3)
        self.check_decimal(0, 0, "1.25")
        self.check_decimal(0, 1, "100.1234")

        tdSql.query(
            f"select dec64_from_ctb from {DB_NAME}.vntb_multi_src "
            f"union all select dec64_col from {DB_NAME}.vctb_full"
        )
        tdSql.checkRows(6)

        tdSql.query(
            "select dec64_from_ctb + 1.00, dec64_from_ctb * 2, dec128_from_ntb - 50.0000 "
            f"from {DB_NAME}.vntb_multi_src order by ts"
        )
        tdSql.checkRows(3)
        self.check_decimal_close(0, 0, "2.25")
        self.check_decimal_close(0, 1, "2.50")
        self.check_decimal_close(0, 2, "50.1234")
        self.check_decimal_close(2, 0, "4.75")
        self.check_decimal_close(2, 1, "7.50")
        self.check_decimal_close(2, 2, "250.0001")

    def test_decimal_precision_and_restart(self):
        """Meta: decimal precision values and restart persistence

        1. validate extreme precision and scale decimal values query correctly
        2. validate decimal metadata survives taosd restart
        3. validate decimal queries still return exact values after restart

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, decimal, restart

        Jira: None

        History:
            - 2026-4-1 Joey Sima Expanded by test plan

        """
        tdSql.execute(f"use {DB_NAME}")

        tdSql.query(f"select dec64_max, dec128_max from {DB_NAME}.vntb_precision_decimal order by ts")
        tdSql.checkRows(3)
        self.check_decimal(0, 0, "0.123456789012345678")
        self.check_decimal(0, 1, "12345678901234567890.1234567890")
        self.check_decimal(1, 0, "0.000000000000000001")
        self.check_decimal(1, 1, "99999999999999999999.9999999999")
        self.check_decimal(2, 0, "0.999999999999999999")
        self.check_decimal(2, 1, "0.0000000001")

        tdSql.query(f"select count(*) from {DB_NAME}.vntb_precision_decimal where dec64_max > 0.5")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select max(dec128_max) from {DB_NAME}.vntb_precision_decimal")
        tdSql.checkRows(1)
        self.check_decimal(0, 0, "99999999999999999999.9999999999")

        tdSql.query(f"select sum(dec64_max) from {DB_NAME}.vntb_precision_decimal")
        tdSql.checkRows(1)
        self.check_decimal(0, 0, "1.123456789012345678")

        tdSql.query(f"select min(dec128_max), max(dec128_max) from {DB_NAME}.vntb_precision_decimal")
        tdSql.checkRows(1)
        self.check_decimal(0, 0, "0.0000000001")
        self.check_decimal(0, 1, "99999999999999999999.9999999999")

        tdDnodes.stoptaosd(1)
        tdDnodes.starttaosd(1)

        tdSql.execute(f"use {DB_NAME}")

        tdSql.query(f"describe {DB_NAME}.vctb_full")
        tdSql.checkCols(5)
        dec64_row = self.find_row("dec64_col")
        tdSql.checkData(dec64_row, 1, "DECIMAL(18, 2)")

        tdSql.query(f"show create vtable {DB_NAME}.vctb_full")
        tdSql.checkRows(1)
        create_sql = tdSql.getData(0, 1)
        assert "FROM `test_vtable_decimal`.`org_ctb_0`.`dec64_col`" in create_sql
        assert "FROM `test_vtable_decimal`.`org_ntb_0`.`dec128_col`" in create_sql

        self.check_column_meta("vntb_precision_decimal", "dec64_max", 18, 18, f"{DB_NAME}.org_ntb_0.dec64_max")
        self.check_column_meta("vntb_precision_decimal", "dec128_max", 38, 10, f"{DB_NAME}.org_ntb_0.dec128_max")

        tdSql.query(f"select sum(dec64_col), max(dec128_col) from {DB_NAME}.vntb_ref_ntb")
        tdSql.checkRows(1)
        self.check_decimal(0, 0, "7.50")
        self.check_decimal(0, 1, "300.0001")

        tdSql.query(
            "select tag_plain, count(*), sum(dec64_col) "
            f"from {DB_NAME}.vstb_decimal where tag_plain in (10, 20) "
            "group by tag_plain order by tag_plain"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 10)
        self.check_decimal(0, 2, "7.50")
        tdSql.checkData(1, 0, 20)
        self.check_decimal(1, 2, "30.00")

        tdSql.execute("alter vtable vntb_ref_ntb add column post_restart_dec decimal(15,5)")
        self.check_column_meta("vntb_ref_ntb", "post_restart_dec", 15, 5, None)

        tdSql.query(f"describe {DB_NAME}.vntb_ref_ntb")
        tdSql.checkCols(5)
        post_restart_row = self.find_row("post_restart_dec")
        tdSql.checkData(post_restart_row, 1, "DECIMAL(15, 5)")
