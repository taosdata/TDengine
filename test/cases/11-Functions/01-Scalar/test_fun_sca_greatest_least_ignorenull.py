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


class TestFunGreatestLeastIgnoreNull:
    """Coverage for the ``ignoreNullInGreatest`` client-scope config that
    governs how ``GREATEST`` / ``LEAST`` treat NULL arguments.

    Default (``0``) preserves the MySQL-compatible behavior: any NULL input
    forces a NULL result.  When set to ``1`` NULL inputs are skipped, and
    NULL is only returned when *every* argument is NULL.

    The cases mirror GTL-IGN-001 .. GTL-IGN-009 of
    ``Func-GreatestLeast-TS.md`` (TSDB v3.4.2).
    """

    db = "gtl_ignnull"

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.execute(f"drop database if exists {cls.db}")
        tdSql.execute(f"create database {cls.db}")
        tdSql.execute(f"use {cls.db}")
        tdSql.execute(
            "create table t1 (ts timestamp, col1 int, col2 int, col3 int, "
            "s1 varchar(16), s2 varchar(16), s3 varchar(16))"
        )
        # row 0: mixed NULL among numeric columns; full-NULL row for strings
        tdSql.execute(
            "insert into t1 values (1700000000000, 3, NULL, 7, "
            "'apple', NULL, 'cherry')"
        )
        # row 1: all numeric NULL, partial string NULL
        tdSql.execute(
            "insert into t1 values (1700000001000, NULL, NULL, NULL, "
            "'banana', NULL, 'cherry')"
        )

    def teardown_class(cls):
        # Restore both configs to their documented defaults so subsequent
        # tests in the same run see a clean client state.
        try:
            tdSql.execute("alter local 'ignoreNullInGreatest' '0'")
            tdSql.execute("alter local 'compareAsStrInGreatest' '1'")
        except Exception:
            pass
        tdSql.execute(f"drop database if exists {cls.db}")

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    def _set_ignore_null(self, value):
        tdSql.execute(f"alter local 'ignoreNullInGreatest' '{value}'")

    def _set_compare_as_str(self, value):
        tdSql.execute(f"alter local 'compareAsStrInGreatest' '{value}'")

    # ------------------------------------------------------------------
    # GTL-IGN-001 default value preserves MySQL-compatible NULL semantics
    # ------------------------------------------------------------------
    def case_default_null_propagates(self):
        self._set_ignore_null(0)
        tdSql.query("select greatest(1, NULL, 5)")
        tdSql.checkData(0, 0, None)
        tdSql.query("select least(1, NULL, 5)")
        tdSql.checkData(0, 0, None)

    # ------------------------------------------------------------------
    # GTL-IGN-002 ignoreNullInGreatest=1 skips constant NULL in GREATEST
    # ------------------------------------------------------------------
    def case_ignore_const_null_greatest(self):
        self._set_ignore_null(1)
        tdSql.query("select greatest(1, NULL, 5)")
        tdSql.checkData(0, 0, 5)

    # ------------------------------------------------------------------
    # GTL-IGN-003 ignoreNullInGreatest=1 skips constant NULL in LEAST
    # ------------------------------------------------------------------
    def case_ignore_const_null_least(self):
        self._set_ignore_null(1)
        tdSql.query("select least(1, NULL, 5)")
        tdSql.checkData(0, 0, 1)
        tdSql.query("select least(NULL, 7, 5)")
        tdSql.checkData(0, 0, 5)

    # ------------------------------------------------------------------
    # GTL-IGN-004 ignoreNullInGreatest=1 skips NULL stored in columns
    # ------------------------------------------------------------------
    def case_ignore_column_null(self):
        self._set_ignore_null(1)
        tdSql.query(
            f"select greatest(col1, col2, col3) from {self.db}.t1 "
            "where ts = 1700000000000"
        )
        tdSql.checkData(0, 0, 7)
        tdSql.query(
            f"select least(col1, col2, col3) from {self.db}.t1 "
            "where ts = 1700000000000"
        )
        tdSql.checkData(0, 0, 3)

    # ------------------------------------------------------------------
    # GTL-IGN-005 mix of column NULL and constant NULL with literals
    # ------------------------------------------------------------------
    def case_ignore_mixed_null(self):
        self._set_ignore_null(1)
        tdSql.query(
            f"select greatest(col1, NULL, 10) from {self.db}.t1 "
            "where ts = 1700000000000"
        )
        tdSql.checkData(0, 0, 10)
        tdSql.query(
            f"select least(NULL, col3, 5) from {self.db}.t1 "
            "where ts = 1700000000000"
        )
        tdSql.checkData(0, 0, 5)

    # ------------------------------------------------------------------
    # GTL-IGN-006 all-NULL inputs always return NULL regardless of config
    # ------------------------------------------------------------------
    def case_all_null_independent_of_config(self):
        for v in (0, 1):
            self._set_ignore_null(v)
            tdSql.query("select greatest(NULL, NULL)")
            tdSql.checkData(0, 0, None)
            tdSql.query("select least(NULL, NULL)")
            tdSql.checkData(0, 0, None)
            # row 1 has all numeric columns NULL
            tdSql.query(
                f"select greatest(col1, col2, col3) from {self.db}.t1 "
                "where ts = 1700000001000"
            )
            tdSql.checkData(0, 0, None)
            tdSql.query(
                f"select least(col1, col2, col3) from {self.db}.t1 "
                "where ts = 1700000001000"
            )
            tdSql.checkData(0, 0, None)

    # ------------------------------------------------------------------
    # GTL-IGN-007 ignoreNullInGreatest is orthogonal to compareAsStrInGreatest
    # ------------------------------------------------------------------
    def case_orthogonal_with_compare_as_str(self):
        self._set_ignore_null(1)
        # Numeric path: with compareAsStrInGreatest=0 the translator converts
        # the VARCHAR '10' to numeric (vectorGetConvertType picks BIGINT for
        # INT+VARCHAR), then compares numerically: greatest(2, 10) -> 10.
        # See translateGreatestleast in
        # community/source/libs/function/src/builtins.c (the IS_NULL_TYPE
        # short-circuit must be skipped for constant NULL when
        # ignoreNullInGreatest=1, otherwise this case returns NULL).
        self._set_compare_as_str(0)
        tdSql.query("select greatest(2, '10', NULL)")
        tdSql.checkData(0, 0, 10)
        # String path: with compareAsStrInGreatest=1 numeric+string promotes
        # to VARCHAR, so lexicographic max over {'2','10'} is '2'.
        self._set_compare_as_str(1)
        tdSql.query("select greatest(2, '10', NULL)")
        tdSql.checkData(0, 0, "2")

    # ------------------------------------------------------------------
    # GTL-IGN-008 NULL is skipped for string-typed inputs as well
    # ------------------------------------------------------------------
    def case_ignore_null_string_inputs(self):
        self._set_ignore_null(1)
        self._set_compare_as_str(1)
        tdSql.query("select greatest('apple', NULL, 'cherry')")
        tdSql.checkData(0, 0, "cherry")
        tdSql.query("select least('banana', NULL, 'cherry')")
        tdSql.checkData(0, 0, "banana")
        # Same behavior over column data
        tdSql.query(
            f"select greatest(s1, s2, s3) from {self.db}.t1 "
            "where ts = 1700000000000"
        )
        tdSql.checkData(0, 0, "cherry")
        tdSql.query(
            f"select least(s1, s2, s3) from {self.db}.t1 "
            "where ts = 1700000000000"
        )
        tdSql.checkData(0, 0, "apple")

    # ------------------------------------------------------------------
    # GTL-IGN-009 boundary: exactly one non-NULL argument among many
    # NULLs returns that single value when ignoreNullInGreatest=1.
    # ------------------------------------------------------------------
    def case_single_non_null(self):
        self._set_ignore_null(1)
        tdSql.query("select greatest(NULL, NULL, 5)")
        tdSql.checkData(0, 0, 5)
        tdSql.query("select least(NULL, 7, NULL)")
        tdSql.checkData(0, 0, 7)

    # ------------------------------------------------------------------
    # GTL-IGN-010 effectiveNum=1 with row-level NULL on the surviving
    # column: when only one input survives the translator-level NULL drop
    # and that column itself is NULL on a given row, the row must still
    # return NULL even with ignoreNullInGreatest=1.  Exercises the
    # single-column path through vectorCompareAndSelect.
    # ------------------------------------------------------------------
    def case_effectivenum_one_with_row_null(self):
        self._set_ignore_null(1)
        # row 1: col1 IS NULL -> result NULL
        tdSql.query(
            f"select greatest(NULL, col1) from {self.db}.t1 "
            "where ts = 1700000001000"
        )
        tdSql.checkData(0, 0, None)
        # row 0: col1 = 3 -> result 3
        tdSql.query(
            f"select greatest(NULL, col1) from {self.db}.t1 "
            "where ts = 1700000000000"
        )
        tdSql.checkData(0, 0, 3)

    # ------------------------------------------------------------------
    # GTL-IGN-011 typed constant NULL with multi-row column input.
    # ``CAST(NULL AS INT)`` is a runtime constant param (numOfRows=1)
    # rather than a TSDB_DATA_TYPE_NULL literal, so it is NOT filtered
    # by the translator and reaches vectorCompareAndSelect with
    # numOfRows=1.  Reading the null bitmap with the outer-loop row
    # index ``i`` instead of broadcast index ``0`` would walk past the
    # constant's bitmap allocation; this case guards against that
    # out-of-bounds read on both ignoreNullInGreatest=0 and =1 paths.
    # ------------------------------------------------------------------
    def case_typed_const_null_broadcast(self):
        # Build a small multi-row table so the outer row loop iterates
        # past index 0, which is what would surface the OOB read.
        tdSql.execute(
            f"create table if not exists {self.db}.tbcast "
            "(ts timestamp, v int)"
        )
        tdSql.execute(f"delete from {self.db}.tbcast")
        tdSql.execute(
            f"insert into {self.db}.tbcast values "
            "(1700000010000, 5), (1700000011000, 3), (1700000012000, 7)"
        )

        # Default config: typed constant NULL participates in the row
        # comparison and must NULL out every row.
        self._set_ignore_null(0)
        tdSql.query(
            f"select greatest(cast(NULL as int), v) from {self.db}.tbcast "
            "order by ts"
        )
        tdSql.checkRows(3)
        for r in range(3):
            tdSql.checkData(r, 0, None)

        # ignoreNullInGreatest=1: typed constant NULL is skipped at
        # runtime (it is not a TSDB_DATA_TYPE_NULL literal so the
        # translator keeps it), leaving only ``v`` to drive each row.
        self._set_ignore_null(1)
        tdSql.query(
            f"select greatest(cast(NULL as int), v) from {self.db}.tbcast "
            "order by ts"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(2, 0, 7)

    # ------------------------------------------------------------------
    # GTL-IGN-012 NULL literal + multi-row column with default config.
    # When ``ignoreNullInGreatest=0`` and a NULL literal is present,
    # the translator pins the function output type to TSDB_DATA_TYPE_NULL.
    # The runtime IsNullType short-circuit must still mark *every* row
    # in the output column NULL — not just row 0 — since the caller
    # allocated the output for the full driver row count.
    # ------------------------------------------------------------------
    def case_null_literal_multirow_default(self):
        tdSql.execute(
            f"create table if not exists {self.db}.tnlit "
            "(ts timestamp, v int)"
        )
        tdSql.execute(f"delete from {self.db}.tnlit")
        tdSql.execute(
            f"insert into {self.db}.tnlit values "
            "(1700000020000, 1), (1700000021000, 2), (1700000022000, 3)"
        )
        self._set_ignore_null(0)
        tdSql.query(
            f"select greatest(v, NULL) from {self.db}.tnlit order by ts"
        )
        tdSql.checkRows(3)
        for r in range(3):
            tdSql.checkData(r, 0, None)
        tdSql.query(
            f"select least(v, NULL) from {self.db}.tnlit order by ts"
        )
        tdSql.checkRows(3)
        for r in range(3):
            tdSql.checkData(r, 0, None)

    # ------------------------------------------------------------------
    # main
    # ------------------------------------------------------------------
    def test_fun_sca_greatest_least_ignorenull(self):
        """Fun: greatest()/least() ignoreNullInGreatest

        1. Default config preserves MySQL-compatible NULL propagation
        2. ignoreNullInGreatest=1 skips constant NULL inputs
        3. ignoreNullInGreatest=1 skips column NULL values per row
        4. All-NULL input still returns NULL regardless of config
        5. Orthogonal behavior with compareAsStrInGreatest
        6. NULL skipping applies to string-typed inputs
        7. Single non-NULL argument among multiple NULLs returns that value
        8. Typed constant NULL (CAST) broadcasts safely across rows

        Since: v3.4.2.0

        Labels: common,ci

        Jira: None
        """
        try:
            self.case_default_null_propagates()
            self.case_ignore_const_null_greatest()
            self.case_ignore_const_null_least()
            self.case_ignore_column_null()
            self.case_ignore_mixed_null()
            self.case_all_null_independent_of_config()
            self.case_orthogonal_with_compare_as_str()
            self.case_ignore_null_string_inputs()
            self.case_single_non_null()
            self.case_effectivenum_one_with_row_null()
            self.case_typed_const_null_broadcast()
            self.case_null_literal_multirow_default()
        finally:
            self._set_ignore_null(0)
            self._set_compare_as_str(1)
