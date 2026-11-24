# -*- coding: utf-8 -*-

# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

"""
Integration tests for C API: taos_txn_begin / taos_txn_commit / taos_txn_rollback.

Tests verify the wrapper layer (libtaos.so) correctly proxies to the native
implementation and that the pTxnTableMeta cleanup bug is fixed.
"""

import ctypes
import os
import platform
from ctypes import c_void_p, c_int, c_char_p, c_uint16, cast, POINTER
from new_test_framework.utils import tdLog, tdSql, tdCom
import threading
import time

# Load the native client from the current build on Windows; Linux keeps the
# conventional loader lookup.
if platform.system().lower() == "windows":
    _lib = ctypes.CDLL(os.path.join(tdCom.getBuildPath(), "build", "bin", "taos.dll"))
else:
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

_lib.taos_schemaless_insert.argtypes = [c_void_p, POINTER(c_char_p), c_int, c_int, c_int]
_lib.taos_schemaless_insert.restype = c_void_p

TSDB_SML_LINE_PROTOCOL = 1
TSDB_SML_TIMESTAMP_MILLI_SECONDS = 4  # NOT_CONFIGURED = 0, HOURS, MINUTES, SECONDS, MILLI_SECONDS


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


def _capi_sml(conn, line):
    """Schemaless insert of one InfluxDB line, return the error code (0 = success)."""
    lines = (c_char_p * 1)(line.encode("utf-8"))
    res = _lib.taos_schemaless_insert(conn, lines, 1, TSDB_SML_LINE_PROTOCOL, TSDB_SML_TIMESTAMP_MILLI_SECONDS)
    code = _lib.taos_errno(res)
    _lib.taos_free_result(res)
    return code


class TestBatchMetaTxnCApi:

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)

    # =========================================================================
    # s0: Reset test database
    # =========================================================================
    def s0_reset_env(self):
        tdSql.execute("drop database if exists capi_db")
        tdSql.execute("create database capi_db vgroups 2 keep 36500")
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
    # s13: statements of a txn run one at a time on their connection
    # =========================================================================
    TXN_CONCURRENT_STMT_CODE16 = 0x3321

    def _run_in_thread(self, conn, sql, out):
        """Run sql on conn in a background thread (ctypes drops the GIL inside taos_query)."""
        def worker():
            code, res = _capi_query(conn, sql)
            _lib.taos_free_result(res)
            out.append(code)
        t = threading.Thread(target=worker)
        t.start()
        return t

    def s13_concurrent_stmt_in_txn(self):
        self.s0_reset_env()
        tdLog.info("======== s13_concurrent_stmt_in_txn (C API)")

        conn = _capi_connect()
        _capi_exec(conn, "use capi_db")
        _capi_exec(conn, "create table stb (ts timestamp, v int) tags (t1 int)")

        # Outside a txn the connection keeps accepting concurrent statements.
        slow = []
        t = self._run_in_thread(conn, "select sleep(4)", slow)
        time.sleep(1)
        _capi_exec(conn, "show databases")
        t.join(timeout=30)
        assert slow == [0], f"slow statement outside txn failed: {slow}"

        assert _lib.taos_txn_begin(conn) == 0

        # Inside a txn: while one statement of the txn is still executing, a second one on the same connection
        # (DDL, COMMIT, ROLLBACK alike) is rejected up front and changes nothing.
        slow = []
        t = self._run_in_thread(conn, "select sleep(4)", slow)
        time.sleep(1)
        for sql in ["create table ct_busy using stb tags(1)", "COMMIT", "ROLLBACK"]:
            code = _capi_error(conn, sql)
            assert code & 0xFFFF == self.TXN_CONCURRENT_STMT_CODE16, f"{sql}: expected 0x3321, got {hex(code)}"
        code = _lib.taos_txn_commit(conn)
        assert code & 0xFFFF == self.TXN_CONCURRENT_STMT_CODE16, f"taos_txn_commit: expected 0x3321, got {hex(code)}"
        t.join(timeout=30)
        assert slow == [0], f"slow statement inside txn failed: {slow}"

        # Once it is done the txn goes on as usual: the rejected statements left no trace.
        _capi_exec(conn, "create table ct_ok using stb tags(2)")
        assert _lib.taos_txn_commit(conn) == 0
        tdSql.query("select table_name from information_schema.ins_tables where db_name = 'capi_db' order by 1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "ct_ok")

        _lib.taos_close(conn)

    # =========================================================================
    # s14: the bypass paths (schemaless here; STMT / STMT2 / raw block alike) are statements of the txn too
    # =========================================================================
    def s14_bypass_path_is_serialised(self):
        self.s0_reset_env()
        tdLog.info("======== s14_bypass_path_is_serialised (C API)")

        conn = _capi_connect()
        _capi_exec(conn, "use capi_db")
        # the schemaless super / child table are created before the txn: inside it only rows are written
        assert _capi_sml(conn, "sml_stb,t1=a v=1i 1700000000000") == 0

        # ALLOW_NONTRANSACTIONAL lets statements the txn cannot cover (row writes) through, executed outside it.
        _capi_exec(conn, "BEGIN ALLOW_NONTRANSACTIONAL")

        slow = []
        t = self._run_in_thread(conn, "select sleep(4)", slow)
        time.sleep(1)
        # a schemaless write that arrives while a statement of the txn runs is turned away like any other statement,
        # and so are COMMIT / ROLLBACK: the connection's txn-local state is borrowed by the running statement
        code = _capi_sml(conn, "sml_stb,t1=a v=2i 1700000000001")
        assert code & 0xFFFF == self.TXN_CONCURRENT_STMT_CODE16, f"sml while busy: expected 0x3321, got {hex(code)}"
        code = _lib.taos_txn_commit(conn)
        assert code & 0xFFFF == self.TXN_CONCURRENT_STMT_CODE16, f"commit while busy: expected 0x3321, got {hex(code)}"
        t.join(timeout=30)
        assert slow == [0], f"slow statement inside txn failed: {slow}"

        # the rejected write left no trace and the slot is free again: the same write now goes through
        tdSql.query("select count(*) from capi_db.sml_stb")
        tdSql.checkData(0, 0, 1)
        assert _capi_sml(conn, "sml_stb,t1=a v=2i 1700000000001") == 0, "sml after the slow statement"
        tdSql.query("select count(*) from capi_db.sml_stb")
        tdSql.checkData(0, 0, 2)

        # the slot is not leaked by the bypass path: a plain statement and the COMMIT still run
        _capi_exec(conn, "show databases")
        assert _lib.taos_txn_commit(conn) == 0
        _lib.taos_close(conn)

    # =========================================================================
    # s15: the next txn starts clean after COMMIT / ROLLBACK, whatever the previous one left in flight
    # =========================================================================
    def s15_next_txn_starts_clean(self):
        self.s0_reset_env()
        tdLog.info("======== s15_next_txn_starts_clean (C API)")

        conn = _capi_connect()
        _capi_exec(conn, "use capi_db")
        _capi_exec(conn, "create table stb (ts timestamp, v int) tags (t1 int)")

        for rnd in range(3):
            # txn A creates objects, then ends (alternately by COMMIT / ROLLBACK)
            assert _lib.taos_txn_begin(conn) == 0
            _capi_exec(conn, f"create table a_ct{rnd} using stb tags({rnd})")
            _capi_exec(conn, f"create table a_nt{rnd} (ts timestamp, v int)")
            if rnd == 0:
                _capi_exec(conn, "alter stable stb add column c1 int")
            if rnd % 2 == 0:
                assert _lib.taos_txn_commit(conn) == 0
            else:
                assert _lib.taos_txn_rollback(conn) == 0

            # txn B right after: it must not see anything of A as its own txn-local state. Its own statements
            # work against the committed (or restored) state, and what it creates is its alone.
            assert _lib.taos_txn_begin(conn) == 0
            _capi_exec(conn, f"create table b_ct{rnd} using stb tags({100 + rnd})")
            _capi_exec(conn, f"describe b_ct{rnd}")
            if rnd % 2 == 0:  # A was committed: its table is there; A was rolled back: it is not
                _capi_exec(conn, f"describe a_ct{rnd}")
            else:
                _capi_error(conn, f"describe a_ct{rnd}")
            assert _lib.taos_txn_rollback(conn) == 0

            # after B's rollback nothing of B is left, for this connection and for another one
            _capi_error(conn, f"describe b_ct{rnd}")
            tdSql.error(f"describe capi_db.b_ct{rnd}")

        tdSql.query("select table_name from information_schema.ins_tables where db_name = 'capi_db' "
                    "and table_name like 'b_%'")
        tdSql.checkRows(0)
        _lib.taos_close(conn)

    # =========================================================================
    # Run all tests
    # =========================================================================
    def test_meta_batch_txn_capi(self):
        """C API integration tests for taos_txn_begin / commit / rollback

        1. begin + commit via C API
        2. begin + rollback via C API
        3. double begin should fail
        4. commit without active txn should fail
        5. rollback without active txn should fail
        6. C API begin + SQL commit (mixed usage)
        7. SQL begin + C API commit (mixed usage)
        8. sequential transactions via C API
        9. NULL taos handle
        10. commit cleans pTxnTableMeta (no stale catalog)
        11. rollback cleans pTxnTableMeta (no stale catalog)
        12. drop table via C API txn
        13. statements of a txn run one at a time: a concurrent one on the same connection gets 0x3321
        14. the bypass paths (schemaless) are statements of the txn too: serialised, slot not leaked
        15. the next txn starts clean after COMMIT / ROLLBACK

        Since: v3.3.6.0

        Labels: common,ci

        Jira: TD-XXXXX

        History:
            - 2026-04-01 Created — §36 C API wrapper tests
            - 2026-11-19 Added s13: concurrent statements of one txn on a connection are rejected
            - 2026-11-21 Added s14 / s15: bypass paths take the statement slot; the next txn starts clean
        """
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
        self.s13_concurrent_stmt_in_txn()
        self.s14_bypass_path_is_serialised()
        self.s15_next_txn_starts_clean()
