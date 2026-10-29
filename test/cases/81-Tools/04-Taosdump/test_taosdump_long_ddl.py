# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
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
from new_test_framework.utils import tdLog, tdSql, etool
import os


class TestTaosdumpLongDdl:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def _clean_dir(self, path):
        if os.path.exists(path):
            os.system("rm -rf %s" % path)
        os.makedirs(path)

    def test_taosdump_long_ddl(self):
        """taosdump dumps and restores DDL longer than 65535 bytes completely

        1. Create a normal table, a super table, a virtual super table, a virtual
           normal table and a virtual child table whose DDLs exceed 65535 bytes
        2. Dump the database with taosdump
        3. Verify the dumped CREATE statements are complete (not truncated, COMMENT
           kept; the virtual child table is stored as skeleton without TAGS, its
           values living in vtags/)
        4. Drop the database and restore with taosdump
        5. Verify SHOW CREATE output is identical to the original and data is restored

        Catalog:
            - Tools:Taosdump

        Since: v3.3.6.x

        Labels: common,ci,taosdump,blob

        Jira: None

        History:
            - 2026-07-29 Created for show create table BLOB change
            - 2026-09-22 Add >65535-byte VTABLE (virtual normal/child) coverage:
              the virtual child TAGS skeleton search in backupMeta.c must stay
              bounded on non-NUL-terminated BLOB payloads

        """
        db = "dump_long_ddl_db"
        ntb = "bigtb"
        stb = "bigstb"
        ctb = "ctb0"
        vstb = "bigvstb"
        vctb = "bigvctb"
        vntb = "bigvntb"
        tmpdir = "/tmp/taosdump_long_ddl"

        # 1200 columns with 55-char names make the DDL text ~76KB (> 65535)
        num_cols = 1200
        col_names = [f"c{str(i).zfill(4)}_{'x' * 50}" for i in range(num_cols)]
        col_defs = ", ".join([f"`{name}` INT" for name in col_names])

        expected_ntb_ddl = (
            f"CREATE TABLE `{ntb}` (`ts` TIMESTAMP, {col_defs}) COMMENT 'bigtable_comment'"
        )
        expected_stb_ddl = (
            f"CREATE STABLE `{stb}` (`ts` TIMESTAMP, {col_defs}) "
            f"TAGS (`loc` INT) SECURITY_LEVEL 0 SECURE_DELETE 0"
        )
        # explicit runtime guard, not assert: python -O strips asserts, and a setup
        # regression that shrank the DDL below the boundary must fail the test here
        # instead of letting it silently "pass" without covering the >64KB path
        if len(expected_ntb_ddl) <= 65535:
            tdLog.exit(f"test setup broken: expected DDL must exceed 65535 bytes, got {len(expected_ntb_ddl)}")

        try:
            tdLog.info("===============create tables with DDL longer than 65535 bytes")
            tdSql.execute(f"drop database if exists {db}")
            tdSql.execute(f"create database {db}")
            tdSql.execute(f"create table {db}.{ntb} (ts timestamp, {col_defs}) comment 'bigtable_comment'")
            tdSql.execute(f"create table {db}.{stb} (ts timestamp, {col_defs}) tags (loc int)")
            tdSql.execute(f"create table {db}.{ctb} using {db}.{stb} tags (1)")
            c0 = col_names[0]
            tdSql.execute(f"insert into {db}.{ntb} (ts, `{c0}`) values (now, 1)")
            tdSql.execute(f"insert into {db}.{ctb} (ts, `{c0}`) values (now, 2)")

            tdLog.info("===============create virtual tables with DDL longer than 65535 bytes")
            # The virtual objects reference the wide normal table's columns, so
            # their rendered DDLs carry one `db`.`ntb`.`col` ref per column
            col_refs = ", ".join([f"`{name}` FROM `{db}`.`{ntb}`.`{name}`" for name in col_names])
            col_defs_from = ", ".join([f"`{name}` INT FROM `{db}`.`{ntb}`.`{name}`" for name in col_names])
            tdSql.execute(f"create stable {db}.{vstb} (ts timestamp, {col_defs}) tags (loc int) virtual 1")
            tdSql.execute(f"create vtable {db}.{vctb} ({col_refs}) using {db}.{vstb} tags (7)")
            tdSql.execute(f"create vtable {db}.{vntb} (ts timestamp, {col_defs_from})")

            # Capture the original SHOW CREATE output as the restore reference; the
            # child DDL must carry a TAGS clause so the skeleton split is exercised
            tdSql.query(f"show create table {db}.{vstb} FULL")
            expected_vstb_ddl = tdSql.queryResult[0][1]
            tdSql.query(f"show create vtable {db}.{vctb} FULL")
            expected_vctb_ddl = tdSql.queryResult[0][1]
            tdSql.query(f"show create vtable {db}.{vntb} FULL")
            expected_vntb_ddl = tdSql.queryResult[0][1]
            vctb_tags_idx = expected_vctb_ddl.rfind(" TAGS (")
            if vctb_tags_idx < 0:
                tdLog.exit(f"test setup broken: virtual child DDL has no TAGS clause: {expected_vctb_ddl[-200:]!r}")
            expected_vctb_skeleton = expected_vctb_ddl[:vctb_tags_idx]
            for name, ddl in ((vstb, expected_vstb_ddl), (vctb, expected_vctb_ddl), (vntb, expected_vntb_ddl)):
                if len(ddl) <= 65535:
                    tdLog.exit(f"test setup broken: {name} DDL must exceed 65535 bytes, got {len(ddl)}")

            tdLog.info("===============dump database")
            self._clean_dir(tmpdir)
            # -M all: virtual table DDL (vtb.sql) and vtags live in the ext-meta
            # stage, skipped under the default basic content mode
            etool.taosdump(f"-M all -o {tmpdir} {db}")

            tdLog.info("===============check dumped CREATE statements are complete")

            def excerpt(text, n=200):
                # head/tail snapshot for assertion messages: distinguishes "truncated" from
                # "formatting drift" (whitespace/newline differences) at a glance
                return f"len={len(text)} head=[{text[:n]!r}] tail=[{text[-n:]!r}]"

            def diff_hint(expected, actual):
                # longest prefix of expected present in actual → where the dumped text
                # starts to diverge; pinpoints the exact column instead of leaving an
                # ~76KB expected string to eyeball
                lo, hi = 0, len(expected)
                while lo < hi:
                    mid = (lo + hi + 1) // 2
                    if expected[:mid] in actual:
                        lo = mid
                    else:
                        hi = mid - 1
                frag = ""
                if lo > 0:
                    start = actual.find(expected[:lo]) + lo
                    frag = f", actual[{start}:{start + 80}]={actual[start:start + 80]!r}"
                return (f"first divergence at expected[{lo}] of {len(expected)}: "
                        f"expected[{lo}:{lo + 80}]={expected[lo:lo + 80]!r}{frag}")

            with open(os.path.join(tmpdir, db, "ntb.sql")) as f:
                ntb_sql = f.read()
            assert expected_ntb_ddl in ntb_sql, (
                f"dumped normal table DDL is truncated or lost COMMENT: "
                f"{diff_hint(expected_ntb_ddl, ntb_sql)}; {excerpt(ntb_sql)}")
            with open(os.path.join(tmpdir, db, "stb.sql")) as f:
                stb_sql = f.read()
            assert expected_stb_ddl in stb_sql, (
                f"dumped super table DDL is truncated or lost TAGS clause: "
                f"{diff_hint(expected_stb_ddl, stb_sql)}; {excerpt(stb_sql)}")
            # the virtual super table DDL is also in stb.sql (embedded newlines are
            # space-normalized there, so check content, not verbatim text)
            assert col_names[-1] in stb_sql, (
                f"dumped virtual super table DDL is truncated (last column missing); "
                f"{excerpt(stb_sql)}")

            # vtb.sql: the virtual normal table is stored verbatim; the virtual
            # child is stored as its skeleton (USING clause, no TAGS) -- a
            # truncated/unbounded TAGS search would break both the byte-exact
            # skeleton match and the child list
            with open(os.path.join(tmpdir, db, "vtb.sql")) as f:
                vtb_sql = f.read()
            assert expected_vntb_ddl in vtb_sql, (
                f"dumped virtual normal table DDL is truncated: "
                f"{diff_hint(expected_vntb_ddl, vtb_sql)}; {excerpt(vtb_sql)}")
            vctb_lines = [line for line in vtb_sql.splitlines() if f"`{vctb}`" in line]
            assert len(vctb_lines) == 1, (
                f"vtb.sql must hold exactly one line for virtual child {vctb}, got {len(vctb_lines)}; "
                f"{excerpt(vtb_sql)}")
            assert vctb_lines[0] == expected_vctb_skeleton, (
                f"dumped virtual child skeleton mismatch (TAGS stripped at wrong offset?): "
                f"{diff_hint(expected_vctb_skeleton, vctb_lines[0])}; {excerpt(vctb_lines[0])}")

            tdLog.info("===============drop and restore")
            tdSql.execute(f"drop database {db}")
            etool.taosdump(f"-M all -i {tmpdir}")

            tdLog.info("===============verify restored schema and data")
            # Explicit-value assertions with diff_hint/excerpt: checkData only
            # reports whole-string inequality, which cannot locate the divergence
            # on ~76KB strings; this matches the diagnosability of the dump-text
            # verification above
            tdSql.query(f"show create table {db}.{ntb} FULL")
            tdSql.checkRows(1)
            restored_ntb_ddl = tdSql.queryResult[0][1]
            assert restored_ntb_ddl == expected_ntb_ddl, (
                f"restored NTB SHOW CREATE mismatch: "
                f"{diff_hint(expected_ntb_ddl, restored_ntb_ddl)}; {excerpt(restored_ntb_ddl)}")

            tdSql.query(f"show create stable {db}.{stb} FULL")
            tdSql.checkRows(1)
            restored_stb_ddl = tdSql.queryResult[0][1]
            assert restored_stb_ddl == expected_stb_ddl, (
                f"restored STB SHOW CREATE mismatch: "
                f"{diff_hint(expected_stb_ddl, restored_stb_ddl)}; {excerpt(restored_stb_ddl)}")

            # virtual objects: SHOW CREATE must be byte-identical after restore;
            # the child is rebuilt from the vtb.sql skeleton plus its vtags values
            tdSql.query(f"show create table {db}.{vstb} FULL")
            tdSql.checkRows(1)
            restored_vstb_ddl = tdSql.queryResult[0][1]
            assert restored_vstb_ddl == expected_vstb_ddl, (
                f"restored virtual STB SHOW CREATE mismatch: "
                f"{diff_hint(expected_vstb_ddl, restored_vstb_ddl)}; {excerpt(restored_vstb_ddl)}")

            tdSql.query(f"show create vtable {db}.{vctb} FULL")
            tdSql.checkRows(1)
            restored_vctb_ddl = tdSql.queryResult[0][1]
            assert restored_vctb_ddl == expected_vctb_ddl, (
                f"restored virtual child SHOW CREATE mismatch (skeleton/vtags reassembly broke): "
                f"{diff_hint(expected_vctb_ddl, restored_vctb_ddl)}; {excerpt(restored_vctb_ddl)}")

            tdSql.query(f"show create vtable {db}.{vntb} FULL")
            tdSql.checkRows(1)
            restored_vntb_ddl = tdSql.queryResult[0][1]
            assert restored_vntb_ddl == expected_vntb_ddl, (
                f"restored virtual normal SHOW CREATE mismatch: "
                f"{diff_hint(expected_vntb_ddl, restored_vntb_ddl)}; {excerpt(restored_vntb_ddl)}")

            tdSql.query(f"select `{c0}` from {db}.{ntb}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)
            tdSql.query(f"select `{c0}` from {db}.{ctb}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 2)
            # the restored virtual child must actually resolve its source column
            tdSql.query(f"select `{c0}` from {db}.{vctb}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)
        finally:
            # cleanup must run even when an assertion aborts mid-test, so neither the
            # database nor the dump directory leaks into the environment; guard it so a
            # cleanup failure never masks the real assertion error
            try:
                tdSql.execute(f"drop database if exists {db}")
            except Exception as ex:
                tdLog.notice(f"cleanup drop database {db} ignored: {ex}")
            os.system("rm -rf %s" % tmpdir)
