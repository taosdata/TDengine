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
from new_test_framework.utils import tdLog, tdSql


class TestFunGreatestLeast:
    """Automation of the base-feature test cases (GTL-G / GTL-L / GTL-N /
    GTL-T / GTL-CFG / GTL-COL / GTL-ERR) defined in
    ``Func-GreatestLeast-TS.md`` (TSDB v3.4.2).

    The orthogonal ``ignoreNullInGreatest`` cases (GTL-IGN-001..010) live
    in ``test_fun_sca_greatest_least_ignorenull.py`` and are excluded here.
    """

    db = "gtl_base"

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.execute(f"drop database if exists {cls.db}")
        tdSql.execute(f"create database {cls.db}")
        tdSql.execute(f"use {cls.db}")

        # m(ts, temperature, humidity, pressure) for GTL-COL-001
        tdSql.execute(
            "create table m (ts timestamp, temperature int, "
            "humidity int, pressure int)"
        )
        tdSql.execute("insert into m values (1700000000000, 30, 60, 1010)")
        tdSql.execute("insert into m values (1700000001000, 25, 80, 1005)")
        tdSql.execute("insert into m values (1700000002000, 40, 55, 1020)")

        # t1(ts, v, a, b) for GTL-COL-002 / GTL-COL-004
        tdSql.execute(
            "create table t1 (ts timestamp, v int, a int, b int)"
        )
        tdSql.execute("insert into t1 values (1700000000000, -3, 5, 9)")
        tdSql.execute("insert into t1 values (1700000001000,  4, 7, 2)")
        tdSql.execute("insert into t1 values (1700000002000,  0, 1, 1)")

        # metrics(ts, cpu_usr, cpu_sys, cpu_io) for GTL-COL-003
        tdSql.execute(
            "create table metrics (ts timestamp, cpu_usr int, "
            "cpu_sys int, cpu_io int)"
        )
        tdSql.execute(
            "insert into metrics values (1700000000000, 50, 20, 10)"
        )
        tdSql.execute(
            "insert into metrics values (1700000001000, 90, 10,  5)"
        )
        tdSql.execute(
            "insert into metrics values (1700000002000, 30, 85,  5)"
        )
        tdSql.execute(
            "insert into metrics values (1700000003000, 30, 30, 95)"
        )

        # JSON tag stable for GTL-ERR-004
        tdSql.execute(
            "create stable st_json (ts timestamp, v int) tags (j json)"
        )
        tdSql.execute(
            "create table t_json using st_json tags ('{\"k\":1}')"
        )
        tdSql.execute("insert into t_json values (1700000000000, 1)")

    def teardown_class(cls):
        try:
            tdSql.execute("alter local 'compareAsStrInGreatest' '1'")
            tdSql.execute("alter local 'ignoreNullInGreatest' '0'")
        except Exception:
            pass
        tdSql.execute(f"drop database if exists {cls.db}")

    # ------------------------------------------------------------------
    # config helpers
    # ------------------------------------------------------------------
    def _set_compare_as_str(self, value):
        tdSql.execute(f"alter local 'compareAsStrInGreatest' '{value}'")

    # ==================================================================
    # 1. GREATEST basic cases
    # ==================================================================

    # GTL-G-001: numeric greatest
    def case_g_001(self):
        tdSql.query("select greatest(3,12,34,8,25)")
        tdSql.checkData(0, 0, 34)

    # GTL-G-002: two-argument greatest
    def case_g_002(self):
        tdSql.query("select greatest(1, 2)")
        tdSql.checkData(0, 0, 2)

    # GTL-G-003: float/int promotion -> DOUBLE result, value 3
    def case_g_003(self):
        tdSql.query("select greatest(1, 2.5, 3)")
        tdSql.checkData(0, 0, 3)
        assert tdSql.checkDataType(0, 0, "DOUBLE")

    # GTL-G-004: negatives
    def case_g_004(self):
        tdSql.query("select greatest(-5, -1, -10)")
        tdSql.checkData(0, 0, -1)

    # GTL-G-005: string lexicographic
    def case_g_005(self):
        tdSql.query("select greatest('apple','banana','cherry')")
        tdSql.checkData(0, 0, "cherry")

    # GTL-G-006: string lexicographic, different ordering of inputs
    def case_g_006(self):
        tdSql.query("select greatest('cherry','apple','banana')")
        tdSql.checkData(0, 0, "cherry")

    # ==================================================================
    # 2. LEAST basic cases
    # ==================================================================

    # GTL-L-001: numeric least
    def case_l_001(self):
        tdSql.query("select least(3,12,34,8,25)")
        tdSql.checkData(0, 0, 3)

    # GTL-L-002: float least
    def case_l_002(self):
        tdSql.query("select least(1.5, 2, 0.5)")
        tdSql.checkData(0, 0, 0.5)

    # GTL-L-003: string lexicographic
    def case_l_003(self):
        tdSql.query("select least('banana','apple','cherry')")
        tdSql.checkData(0, 0, "apple")

    # GTL-L-004: all negatives
    def case_l_004(self):
        tdSql.query("select least(-1, -2, -3)")
        tdSql.checkData(0, 0, -3)

    # ==================================================================
    # 3. NULL handling (with default ignoreNullInGreatest=0)
    # ==================================================================

    # GTL-N-001: literal NULL among inputs -> NULL
    def case_n_001(self):
        tdSql.query("select greatest(1, NULL, 5)")
        tdSql.checkData(0, 0, None)

    # GTL-N-002: literal NULL in LEAST -> NULL
    def case_n_002(self):
        tdSql.query("select least(1, NULL, 5)")
        tdSql.checkData(0, 0, None)

    # GTL-N-003: all-NULL inputs -> NULL
    def case_n_003(self):
        tdSql.query("select greatest(NULL, NULL)")
        tdSql.checkData(0, 0, None)

    # GTL-N-004: row-level NULL propagation on column inputs
    def case_n_004(self):
        # Use a fresh table to isolate NULL row semantics
        tdSql.execute(
            f"create table {self.db}.tn (ts timestamp, v1 int, v2 int)"
        )
        tdSql.execute(
            f"insert into {self.db}.tn values (1700000000000, NULL, 5)"
        )
        tdSql.execute(
            f"insert into {self.db}.tn values (1700000001000, 3, 7)"
        )
        tdSql.execute(
            f"insert into {self.db}.tn values (1700000002000, 8, 2)"
        )
        tdSql.query(
            f"select greatest(v1, v2) from {self.db}.tn order by ts"
        )
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 7)
        tdSql.checkData(2, 0, 8)

    # ==================================================================
    # 4. Type derivation
    # ==================================================================

    # GTL-T-001: INT/BIGINT -> BIGINT
    def case_t_001(self):
        tdSql.query(
            "select greatest(cast(1 as int), cast(2 as bigint))"
        )
        assert tdSql.checkDataType(0, 0, "BIGINT")

    # GTL-T-002: INT/DOUBLE -> DOUBLE (compareAsStrInGreatest does not
    # apply, both are numeric)
    def case_t_002(self):
        tdSql.query("select greatest(1, 2.0)")
        assert tdSql.checkDataType(0, 0, "DOUBLE")

    # GTL-T-003: VARCHAR length must accommodate the longest input
    def case_t_003(self):
        tdSql.query("select greatest('a', 'abcdef')")
        assert tdSql.checkDataType(0, 0, "VARCHAR")
        # description tuple: (name, type, display_size, ...) — index 2 is
        # the byte size; for VARCHAR this is the declared length.
        size = tdSql.cursor.description[0][2]
        assert size is None or size >= 6, f"VARCHAR length {size} < 6"

    # GTL-T-004: VARCHAR/NCHAR mix derives a comparable string type and
    # returns the lexicographic max
    def case_t_004(self):
        tdSql.query("select greatest(cast('a' as nchar(10)), 'b')")
        tdSql.checkData(0, 0, "b")

    # ==================================================================
    # 5. compareAsStrInGreatest config
    # ==================================================================

    # GTL-CFG-001: default=1 -> '2' wins lexicographically
    def case_cfg_001(self):
        self._set_compare_as_str(1)
        tdSql.query("select greatest(2, '10')")
        tdSql.checkData(0, 0, "2")

    # GTL-CFG-002: default LEAST -> '10' lexicographic min
    def case_cfg_002(self):
        self._set_compare_as_str(1)
        tdSql.query("select least(2, '10')")
        tdSql.checkData(0, 0, "10")

    # GTL-CFG-003: switch to numeric mode -> 10 wins
    def case_cfg_003(self):
        self._set_compare_as_str(0)
        tdSql.query("select greatest(2, '10')")
        tdSql.checkData(0, 0, 10)

    # GTL-CFG-004: numeric mode preserves DOUBLE width when present.
    # vectorGetConvertType picks BIGINT for INT+VARCHAR, which would
    # truncate '2.5' to 2; using a DOUBLE input is the documented way
    # to surface the non-integer string value at full precision.
    def case_cfg_004(self):
        self._set_compare_as_str(0)
        tdSql.query("select greatest(cast(1 as double), '2.5')")
        assert tdSql.checkDataType(0, 0, "DOUBLE")
        tdSql.checkData(0, 0, 2.5)

    # GTL-CFG-005: switch back to default -> behavior restored
    def case_cfg_005(self):
        self._set_compare_as_str(1)
        tdSql.query("select greatest(2, '10')")
        tdSql.checkData(0, 0, "2")
        tdSql.query("select least(2, '10')")
        tdSql.checkData(0, 0, "10")

    # ==================================================================
    # 6. Column / table queries
    # ==================================================================

    # GTL-COL-001: per-row max across multiple columns
    def case_col_001(self):
        tdSql.query(
            f"select greatest(temperature, humidity, pressure) "
            f"from {self.db}.m order by ts"
        )
        tdSql.checkData(0, 0, 1010)
        tdSql.checkData(1, 0, 1005)
        tdSql.checkData(2, 0, 1020)

    # GTL-COL-002: column + scalar broadcast
    def case_col_002(self):
        tdSql.query(
            f"select greatest(v, 0) from {self.db}.t1 order by ts"
        )
        tdSql.checkData(0, 0, 0)   # v=-3 -> 0
        tdSql.checkData(1, 0, 4)   # v= 4 -> 4
        tdSql.checkData(2, 0, 0)   # v= 0 -> 0

    # GTL-COL-003: GREATEST in WHERE — only rows with any cpu* > 80
    def case_col_003(self):
        tdSql.query(
            f"select ts from {self.db}.metrics "
            "where greatest(cpu_usr, cpu_sys, cpu_io) > 80 order by ts"
        )
        tdSql.checkRows(3)  # rows 2,3,4 (90, 85, 95)

    # GTL-COL-004: GREATEST and LEAST projected together; hi >= lo
    def case_col_004(self):
        tdSql.query(
            f"select greatest(a,b) as hi, least(a,b) as lo "
            f"from {self.db}.t1 order by ts"
        )
        for r in range(tdSql.queryRows):
            hi = tdSql.queryResult[r][0]
            lo = tdSql.queryResult[r][1]
            assert hi >= lo, f"row {r}: hi={hi} < lo={lo}"

    # ==================================================================
    # 7. Error / unsupported
    # ==================================================================

    # GTL-ERR-001: GREATEST(1) -> error (minParamNum=2)
    def case_err_001(self):
        tdSql.error("select greatest(1)")

    # GTL-ERR-002: GREATEST() -> parse / param error
    def case_err_002(self):
        tdSql.error("select greatest()")

    # GTL-ERR-003: LEAST(1) -> error
    def case_err_003(self):
        tdSql.error("select least(1)")

    # GTL-ERR-004: JSON tag is not a comparable type
    def case_err_004(self):
        tdSql.error(f"select greatest(j, 1) from {self.db}.t_json")

    # ==================================================================
    # main
    # ==================================================================
    def test_fun_sca_greatest_least(self):
        """Fun: greatest()/least() base feature coverage

        1. GREATEST/LEAST numeric and string semantics
        2. NULL propagation with default ignoreNullInGreatest=0
        3. Type derivation across numeric and string mixtures
        4. compareAsStrInGreatest dynamic switch (1/0/back to 1)
        5. Column references, broadcast, and projection patterns
        6. Argument-count and unsupported-type error paths

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None
        """
        try:
            # GREATEST
            self.case_g_001()
            self.case_g_002()
            self.case_g_003()
            self.case_g_004()
            self.case_g_005()
            self.case_g_006()
            # LEAST
            self.case_l_001()
            self.case_l_002()
            self.case_l_003()
            self.case_l_004()
            # NULL
            self.case_n_001()
            self.case_n_002()
            self.case_n_003()
            self.case_n_004()
            # Type derivation
            self.case_t_001()
            self.case_t_002()
            self.case_t_003()
            self.case_t_004()
            # compareAsStrInGreatest config
            self.case_cfg_001()
            self.case_cfg_002()
            self.case_cfg_003()
            self.case_cfg_004()
            self.case_cfg_005()
            # Column queries
            self.case_col_001()
            self.case_col_002()
            self.case_col_003()
            self.case_col_004()
            # Error paths
            self.case_err_001()
            self.case_err_002()
            self.case_err_003()
            self.case_err_004()
        finally:
            self._set_compare_as_str(1)
