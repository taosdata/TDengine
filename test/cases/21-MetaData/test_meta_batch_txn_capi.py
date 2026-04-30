###################################################################           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

"""
Integration tests for C API: taos_txn_begin / taos_txn_commit / taos_txn_rollback.

Tests verify the wrapper layer (libtaos.so) correctly proxies to the native
implementation and that the pTxnTableMeta cleanup bug is fixed.
"""

import ctypes
from ctypes import c_void_p, c_int, c_char_p, c_uint16, cast
from new_test_framework.utils import tdLog, tdSql, tdCom
import time

# Load libtaos and set up C function signatures
_lib = ctypes.CDLL("libtaos.so")

_lib.taos_connect.restype = c_void_p
_lib.taos_connect.argtypes = [c_char_p, c_char_p, c_char_p, c_char_p, c_uint16]

_lib.taos_close.argtypes = [c_void_p]
_lib.taos_close.restype = None

_lib.taos_txn_begin.argtypes = [c_void_p]
_lib.taos_txn_begin.restype = c_int

_lib.taos_txn_commit.argtypes = [c_void_p]
_lib.taos_txn_commit.restype = c_int

_lib.taos_txn_rollback.argtypes = [c_void_p]
_lib.taos_txn_rollback.restype = c_int

_lib.taos_query.argtypes = [c_void_p, c_char_p]
_lib.taos_query.restype = c_void_p

_lib.taos_free_result.argtypes = [c_void_p]
_lib.taos_free_result.restype = None

_lib.taos_errno.argtypes = [c_void_p]
_lib.taos_errno.restype = c_int

_lib.taos_errstr.argtypes = [c_void_p]
_lib.taos_errstr.restype = c_char_p


def _capi_connect():
    """Create a raw C API connection."""
    conn = _lib.taos_connect(None, b"root", b"taosdata", None, 0)
    assert conn, "taos_connect returned NULL"
    return conn


def _capi_query(conn, sql):
    """Execute a SQL via C API, return (code, res)."""
    res = _lib.taos_query(conn, sql.encode("utf-8"))
    code = _lib.taos_errno(res)
    return code, res


def _capi_exec(conn, sql):
    """Execute SQL, free result, assert success."""
    code, res = _capi_query(conn, sql)
    _lib.taos_free_result(res)
    assert code == 0, f"SQL failed [{hex(code)}]: {sql}"


def _capi_error(conn, sql, expected_code=None):
    """Execute SQL expecting failure."""
    code, res = _capi_query(conn, sql)
    _lib.taos_free_result(res)
    assert code != 0, f"Expected error but got success: {sql}"
    if expected_code is not None:
        assert code == expected_code, f"Expected {hex(expected_code)} but got {hex(code)}: {sql}"
    return code


class TestBatchMetaTxnCApi:

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)

    def s0_reset_env(self):
        tdSql.execute("drop database if exists capi_db")
        tdSql.execute("create database capi_db vgroups 2")
        tdSql.execute("use capi_db")

    # =========================================================================
    # s1: Basic begin + commit via C API
    # =========================================================================
    def s1_begin_commit(self):
        self.s0_reset_env()
        tdLog.info("======== s1_begin_commit (C API)")

        conn = _capi_connect()
        _capi_exec(conn, "use capi_db")
        _capi_exec(conn, "create table stb (ts timestamp, v int) tags (t1 int)")

        # Begin transaction via C API
        code = _lib.taos_txn_begin(conn)
        tdLog.info(f"taos_txn_begin returned: {code}")
        assert code == 0, f"taos_txn_begin failed: {hex(code)}"

        # Create tables via SQL on the same connection
        _capi_exec(conn, "create table ct1 using stb tags(1)")
        _capi_exec(conn, "create table ct2 using stb tags(2)")

        # Commit via C API
        code = _lib.taos_txn_commit(conn)
        tdLog.info(f"taos_txn_commit returned: {code}")
        assert code == 0, f"taos_txn_commit failed: {hex(code)}"

        # Verify via test framework connection
        tdSql.query("select count(*) from capi_db.stb")
        tdSql.checkRows(1)

        tdSql.query("show capi_db.tables")
        tdSql.checkRows(2)

        _lib.taos_close(conn)

    # =========================================================================
    # s2: Basic begin + rollback via C API
    # =========================================================================
    def s2_begin_rollback(self):
        self.s0_reset_env()
        tdLog.info("======== s2_begin_rollback (C API)")

        conn = _capi_connect()
        _capi_exec(conn, "use capi_db")
        _capi_exec(conn, "create table stb (ts timestamp, v int) tags (t1 int)")

        code = _lib.taos_txn_begin(conn)
        assert code == 0

        _capi_exec(conn, "create table ct1 using stb tags(1)")
        _capi_exec(conn, "create table ct2 using stb tags(2)")

        # Rollback via C API
        code = _lib.taos_txn_rollback(conn)
        tdLog.info(f"taos_txn_rollback returned: {code}")
        assert code == 0, f"taos_txn_rollback failed: {hex(code)}"

        # Tables should not exist
        tdSql.query("show capi_db.tables")
        tdSql.checkRows(0)

        _lib.taos_close(conn)

    # =========================================================================
    # s3: Double begin should fail
    # =========================================================================
    def s3_double_begin(self):
        self.s0_reset_env()
        tdLog.info("======== s3_double_begin (C API)")

        conn = _capi_connect()
        _capi_exec(conn, "use capi_db")

        code = _lib.taos_txn_begin(conn)
        assert code == 0

        # Second BEGIN should fail
        code2 = _lib.taos_txn_begin(conn)
        tdLog.info(f"second taos_txn_begin returned: {hex(code2)}")
        assert code2 != 0, "Double BEGIN should fail"

        # Cleanup: rollback the first txn
        _lib.taos_txn_rollback(conn)
        _lib.taos_close(conn)

    # =========================================================================
    # s4: Commit without active txn should fail
    # =========================================================================
    def s4_commit_no_txn(self):
        self.s0_reset_env()
        tdLog.info("======== s4_commit_no_txn (C API)")

        conn = _capi_connect()
        _capi_exec(conn, "use capi_db")

        code = _lib.taos_txn_commit(conn)
        tdLog.info(f"taos_txn_commit (no txn) returned: {hex(code)}")
        assert code != 0, "COMMIT without txn should fail"

        _lib.taos_close(conn)

    # =========================================================================
    # s5: Rollback without active txn should fail
    # =========================================================================
    def s5_rollback_no_txn(self):
        self.s0_reset_env()
        tdLog.info("======== s5_rollback_no_txn (C API)")

        conn = _capi_connect()
        _capi_exec(conn, "use capi_db")

        code = _lib.taos_txn_rollback(conn)
        tdLog.info(f"taos_txn_rollback (no txn) returned: {hex(code)}")
        assert code != 0, "ROLLBACK without txn should fail"

        _lib.taos_close(conn)

    # =========================================================================
    # s6: C API begin + SQL commit (mixed usage)
    # =========================================================================
    def s6_capi_begin_sql_commit(self):
        self.s0_reset_env()
        tdLog.info("======== s6_capi_begin_sql_commit (C API)")

        conn = _capi_connect()
        _capi_exec(conn, "use capi_db")
        _capi_exec(conn, "create table stb (ts timestamp, v int) tags (t1 int)")

        # Begin via C API
        code = _lib.taos_txn_begin(conn)
        assert code == 0

        _capi_exec(conn, "create table ct1 using stb tags(1)")

        # Commit via SQL (on same raw connection)
        _capi_exec(conn, "COMMIT")

        tdSql.query("show capi_db.tables")
        tdSql.checkRows(1)

        _lib.taos_close(conn)

    # =========================================================================
    # s7: SQL begin + C API commit (mixed usage)
    # =========================================================================
    def s7_sql_begin_capi_commit(self):
        self.s0_reset_env()
        tdLog.info("======== s7_sql_begin_capi_commit (C API)")

        conn = _capi_connect()
        _capi_exec(conn, "use capi_db")
        _capi_exec(conn, "create table stb (ts timestamp, v int) tags (t1 int)")

        # Begin via SQL
        _capi_exec(conn, "BEGIN")

        _capi_exec(conn, "create table ct1 using stb tags(1)")

        # Commit via C API
        code = _lib.taos_txn_commit(conn)
        assert code == 0, f"taos_txn_commit failed: {hex(code)}"

        tdSql.query("show capi_db.tables")
        tdSql.checkRows(1)

        _lib.taos_close(conn)

    # =========================================================================
    # s8: Sequential transactions via C API
    # =========================================================================
    def s8_sequential_txns(self):
        self.s0_reset_env()
        tdLog.info("======== s8_sequential_txns (C API)")

        conn = _capi_connect()
        _capi_exec(conn, "use capi_db")
        _capi_exec(conn, "create table stb (ts timestamp, v int) tags (t1 int)")

        # First txn: create + commit
        assert _lib.taos_txn_begin(conn) == 0
        _capi_exec(conn, "create table ct1 using stb tags(1)")
        assert _lib.taos_txn_commit(conn) == 0

        # Second txn: create + rollback
        assert _lib.taos_txn_begin(conn) == 0
        _capi_exec(conn, "create table ct2 using stb tags(2)")
        assert _lib.taos_txn_rollback(conn) == 0

        # Third txn: create + commit
        assert _lib.taos_txn_begin(conn) == 0
        _capi_exec(conn, "create table ct3 using stb tags(3)")
        assert _lib.taos_txn_commit(conn) == 0

        # ct1 and ct3 should exist, ct2 should not
        tdSql.query("show capi_db.tables")
        tdSql.checkRows(2)

        _lib.taos_close(conn)

    # =========================================================================
    # s9: NULL taos handle
    # =========================================================================
    def s9_null_handle(self):
        tdLog.info("======== s9_null_handle (C API)")

        null = c_void_p(None)
        assert _lib.taos_txn_begin(null) != 0
        assert _lib.taos_txn_commit(null) != 0
        assert _lib.taos_txn_rollback(null) != 0

    # =========================================================================
    # s10: C API commit cleans pTxnTableMeta (no stale catalog)
    # =========================================================================
    def s10_commit_cleans_meta_cache(self):
        self.s0_reset_env()
        tdLog.info("======== s10_commit_cleans_meta_cache (C API)")

        conn = _capi_connect()
        _capi_exec(conn, "use capi_db")
        _capi_exec(conn, "create table stb (ts timestamp, v int) tags (t1 int)")

        # Transaction: create table + alter + commit via C API
        assert _lib.taos_txn_begin(conn) == 0
        _capi_exec(conn, "create table nt1 (ts timestamp, a int)")
        _capi_exec(conn, "alter table nt1 add column b float")
        assert _lib.taos_txn_commit(conn) == 0

        # After commit, DESC should return the latest schema (with column b)
        # Use test framework connection (different session) to verify no stale cache
        tdSql.query("describe capi_db.nt1")
        # Should have: ts, a, b = 3 columns
        tdSql.checkRows(3)

        _lib.taos_close(conn)

    # =========================================================================
    # s11: C API rollback cleans pTxnTableMeta (no stale catalog)
    # =========================================================================
    def s11_rollback_cleans_meta_cache(self):
        self.s0_reset_env()
        tdLog.info("======== s11_rollback_cleans_meta_cache (C API)")

        conn = _capi_connect()
        _capi_exec(conn, "use capi_db")

        # Pre-create a table
        _capi_exec(conn, "create table nt1 (ts timestamp, a int)")

        # Transaction: alter + rollback via C API
        assert _lib.taos_txn_begin(conn) == 0
        _capi_exec(conn, "alter table nt1 add column b float")
        assert _lib.taos_txn_rollback(conn) == 0

        # After rollback, DESC should return original schema (no column b)
        tdSql.query("describe capi_db.nt1")
        tdSql.checkRows(2)  # ts, a only

        _lib.taos_close(conn)

    # =========================================================================
    # s12: Drop table via C API txn
    # =========================================================================
    def s12_drop_table_commit(self):
        self.s0_reset_env()
        tdLog.info("======== s12_drop_table_commit (C API)")

        conn = _capi_connect()
        _capi_exec(conn, "use capi_db")
        _capi_exec(conn, "create table stb (ts timestamp, v int) tags (t1 int)")
        _capi_exec(conn, "create table ct1 using stb tags(1)")
        _capi_exec(conn, "create table ct2 using stb tags(2)")

        # Transaction: drop ct1 + commit
        assert _lib.taos_txn_begin(conn) == 0
        _capi_exec(conn, "drop table ct1")
        assert _lib.taos_txn_commit(conn) == 0

        # ct1 should be gone, ct2 remains
        tdSql.query("show capi_db.tables")
        tdSql.checkRows(1)

        _lib.taos_close(conn)

    # =========================================================================
    # Run all tests
    # =========================================================================
    def test_meta_batch_txn_capi(self):
        self.s1_begin_commit()
        self.s2_begin_rollback()
        self.s3_double_begin()
        self.s4_commit_no_txn()
        self.s5_rollback_no_txn()
        self.s6_capi_begin_sql_commit()
        self.s7_sql_begin_capi_commit()
        self.s8_sequential_txns()
        self.s9_null_handle()
        self.s10_commit_cleans_meta_cache()
        self.s11_rollback_cleans_meta_cache()
        self.s12_drop_table_commit()
