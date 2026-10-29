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


from new_test_framework.utils import tdLog, tdSql
from taos.constants import FieldType


class TestShowCreateTableBlob:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def teardown_class(cls):
        # backstop cleanup: a failed assertion skips the drop at the end of a test
        # method, and leftover 1200-column tables would pollute later cases sharing
        # the environment (and collide by name, e.g. srctb across vtable cases)
        for db in (
            "show_create_blob_db",
            "show_create_varchar_db",
            "show_create_tagval_db",
            "show_create_vtb_blob_db",
            "show_create_vtb_small_db",
            "show_create_thr_db",
            "show_create_nofull_db",
            "show_create_vstc_db",
            "show_create_vbase_db",
            "show_create_vthr_db",
        ):
            tdSql.execute(f"drop database if exists {db}")

    def test_show_create_table_long_ddl(self):
        """Show create table with DDL longer than 65535 bytes

        1. Create a normal table whose CREATE TABLE statement exceeds 65535 bytes
        2. Execute SHOW CREATE TABLE and verify the DDL text is not truncated
        3. Verify the returned value is decoded to str
        4. Replay the returned DDL and verify SHOW CREATE TABLE output stays identical

        Catalog:
            - ShowCommands

        Since: v3.3.6.x

        Labels: common,ci,show,blob

        Jira: None

        History:
            - 2026-07-29 Created for show create table BLOB change

        """
        db = "show_create_blob_db"
        tb = "bigtb"
        # 1200 columns with 55-char names make the DDL text ~74KB (> 65535)
        num_cols = 1200
        col_names = [f"c{str(i).zfill(4)}_{'x' * 50}" for i in range(num_cols)]

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")

        col_defs = ", ".join([f"`{name}` INT" for name in col_names])
        create_sql = f"create table {tb} (ts timestamp, {col_defs})"
        tdSql.execute(create_sql)

        expected_ddl = (
            f"CREATE TABLE `{tb}` (`ts` TIMESTAMP, "
            + ", ".join([f"`{name}` INT" for name in col_names])
            + ")"
        )
        assert len(expected_ddl) > 65535, f"expected DDL should exceed 65535 bytes, got {len(expected_ddl)}"

        tdLog.info("===============check show create table output is not truncated")
        tdSql.query(f"show create table {tb} FULL")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, tb)
        # DDL exceeds the varstr cap -> the result column is BLOB (4-byte length header)
        assert tdSql.cursor.description[1][1] == FieldType.C_BLOB, (
            f"large DDL should use BLOB, got type {tdSql.cursor.description[1][1]}")
        ddl = tdSql.getData(0, 1)
        assert isinstance(ddl, str), f"expected str after BLOB decode, got {type(ddl)}"
        tdSql.checkData(0, 1, expected_ddl)

        tdLog.info("===============replay the returned DDL")
        tdSql.execute(f"drop table {tb}")
        tdSql.execute(ddl)
        tdSql.query(f"show create table {tb} FULL")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, expected_ddl)

        tdSql.execute(f"drop database {db}")

    def test_show_create_table_small_ddl_varchar(self):
        """A small DDL keeps the historical VARCHAR result column type.

        Verify that a table whose DDL stays under the 65535-byte varstr cap keeps
        the historical VARCHAR column type and returns complete content, so
        small-table output and old-client compatibility are unchanged.

        Catalog:
            - ShowCommands

        Since: v3.3.6.x

        Labels: common,ci,show,blob

        Jira: None

        History:
            - 2026-07-29 Created for show create table BLOB change

        """
        db = "show_create_varchar_db"
        tb = "smalltb"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {tb} (ts timestamp, c1 int, c2 varchar(8))")

        tdSql.query(f"show create table {tb}")
        tdSql.checkRows(1)
        # DDL fits in a varstr -> the result column stays VARCHAR/BINARY (type code 8)
        assert tdSql.cursor.description[1][1] == FieldType.C_BINARY, (
            f"small DDL should use VARCHAR/BINARY, got type {tdSql.cursor.description[1][1]}")
        ddl = tdSql.getData(0, 1)
        assert isinstance(ddl, str), f"expected str, got {type(ddl)}"
        assert ddl.startswith(f"CREATE TABLE `{tb}`"), f"unexpected DDL: {ddl}"

        tdSql.execute(f"drop database {db}")

    def test_show_create_child_table_large_tag_value(self):
        """Child table with large live tag values picks the BLOB result column.

        1. Create a stable with a wide VARCHAR tag and a child table carrying a ~16KB
           tag value (TSDB_MAX_TAGS_LEN caps the total tag payload at 16384)
        2. SHOW CREATE TABLE FULL renders the live tag values in TAGS (...); the result
           column is BLOB even though the schema definition itself is tiny
        3. Verify the returned DDL is complete (tag value not truncated)

        Catalog:
            - ShowCommands

        Since: v3.3.6.x

        Labels: common,ci,show,blob

        Jira: None

        History:
            - 2026-08-10 Created for child-table tag-value estimate fix

        """
        db = "show_create_tagval_db"
        stb = "stb"
        tb = "ctb1"
        tag_val = "x" * 16000

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create stable {stb} (ts timestamp, c1 int) tags (t1 varchar(16300))")
        tdSql.execute(f"create table {tb} using {stb} tags ('{tag_val}')")

        tdSql.query(f"show create table {tb} FULL")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, tb)
        # FULL + live tag values -> BLOB result column
        assert tdSql.cursor.description[1][1] == FieldType.C_BLOB, (
            f"child-table DDL with large tag value should use BLOB, got type {tdSql.cursor.description[1][1]}")
        ddl = tdSql.getData(0, 1)
        assert isinstance(ddl, str), f"expected str after BLOB decode, got {type(ddl)}"
        assert len(ddl) > len(tag_val), f"DDL should contain the full tag value, got {len(ddl)} bytes"
        assert tag_val in ddl, "tag value should not be truncated in the returned DDL"

        tdSql.execute(f"drop database {db}")

    def test_show_create_vtable_long_ddl(self):
        """Virtual (normal) table whose DDL exceeds 65535 bytes picks the BLOB column.

        1. Create a source normal table with many columns, then a VTABLE referencing
           each of them (one SERIES per column reference) so the rendered DDL — which
           appends a "SERIES ... AS src.db.measure" clause per reference — exceeds 65535
        2. SHOW CREATE VTABLE must pick BLOB (the FULL suffix selects) and return the full DDL without truncation
        3. Replay the returned DDL and verify SHOW CREATE output is byte-identical
           (round-trip stability, the real proof of "not truncated")

        Catalog:
            - ShowCommands

        Since: v3.3.6.x

        Labels: common,ci,show,blob

        Jira: None

        History:
            - 2026-08-13 Added VTABLE BLOB coverage (numOfSeries estimate path)

        """
        db = "show_create_vtb_blob_db"
        src = "srctb"
        tb = "vbigtb"
        # Each column reference renders "`vNNNN` INT FROM `db`.`srctb`.`cNNNN`" (~45
        # bytes); 1600 references make the DDL exceed 65535, forcing the BLOB column.
        num_cols = 1600
        src_cols = [f"c{str(i).zfill(4)}" for i in range(num_cols)]
        v_cols = [f"v{str(i).zfill(4)}" for i in range(num_cols)]

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        # source normal table: num_cols columns (TSDB_MAX_COLUMNS_NON_VIRTUAL = 4096)
        src_defs = ", ".join([f"`{c}` INT" for c in src_cols])
        tdSql.execute(f"create table {src} (ts timestamp, {src_defs})")
        # vtable: one FROM reference per column
        v_refs = ", ".join([f"`{v}` INT FROM {db}.{src}.`{c}`" for v, c in zip(v_cols, src_cols)])
        tdSql.execute(f"create vtable {tb} (ts timestamp, {v_refs})")

        tdLog.info("===============check show create vtable output is not truncated")
        tdSql.query(f"show create vtable {tb} FULL")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, tb)
        # with FULL, a DDL that exceeds 65535 selects BLOB (4-byte length header)
        assert tdSql.cursor.description[1][1] == FieldType.C_BLOB, (
            f"large VTABLE DDL should use BLOB, got type {tdSql.cursor.description[1][1]}")
        ddl = tdSql.getData(0, 1)
        assert isinstance(ddl, str), f"expected str after BLOB decode, got {type(ddl)}"
        # the full DDL must be present, not clamped at 65535
        assert len(ddl) > 65535, f"VTABLE DDL should exceed 65535 bytes, got {len(ddl)} (truncated?)"
        assert ddl.startswith(f"CREATE VTABLE `{tb}`"), f"unexpected DDL start: {ddl[:60]}"
        # every column reference must render its own FROM clause
        from_count = ddl.count(" FROM ")
        assert from_count == num_cols, (
            f"expected {num_cols} FROM clauses, got {from_count} (some truncated?)")

        tdLog.info("===============replay the returned VTABLE DDL (round-trip)")
        tdSql.execute(f"drop table {tb}")
        tdSql.execute(ddl)
        tdSql.query(f"show create vtable {tb} FULL")
        tdSql.checkRows(1)
        # replay must reproduce the exact DDL — proves the original was complete
        tdSql.checkData(0, 1, ddl)

        tdSql.execute(f"drop database {db}")

    def test_show_create_vtable_small_ddl(self):
        """A small VTABLE DDL keeps the historical VARCHAR/BINARY result column type.

        Verify that a virtual table whose DDL stays under the 65535-byte varstr
        cap keeps the historical VARCHAR column type and returns complete
        content, matching the normal-table behavior for small DDLs.

        Catalog:
            - ShowCommands

        Since: v3.3.6.x

        Labels: common,ci,show,blob

        Jira: None

        History:
            - 2026-08-13 Added VTABLE small-DDL coverage

        """
        db = "show_create_vtb_small_db"
        src = "srctb"
        tb = "vsmalltb"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {src} (ts timestamp, c1 int, c2 int)")
        tdSql.execute(f"create vtable {tb} (ts timestamp, v1 int from {db}.{src}.c1, v2 int from {db}.{src}.c2)")

        tdSql.query(f"show create vtable {tb}")
        tdSql.checkRows(1)
        # DDL fits in a varstr -> the result column stays VARCHAR/BINARY (type code 8)
        assert tdSql.cursor.description[1][1] == FieldType.C_BINARY, (
            f"small VTABLE DDL should use VARCHAR/BINARY, got type {tdSql.cursor.description[1][1]}")
        ddl = tdSql.getData(0, 1)
        assert isinstance(ddl, str), f"expected str, got {type(ddl)}"
        assert ddl.startswith(f"CREATE VTABLE `{tb}`"), f"unexpected DDL: {ddl}"

        tdSql.execute(f"drop database {db}")

    def test_show_create_varchar_blob_threshold(self):
        """The FULL result form carries DDLs at the 65535 boundary completely.

        The FULL suffix selects the BLOB result form at any DDL length. Pin
        the DDL to the exact boundary by tuning column-name padding (each pad
        byte in a column name is one byte in the rendered DDL) and verify:
          - at exactly 65535 bytes: BLOB result, content complete
          - at 65536 bytes: BLOB result, content complete
        Both sides replay to identical SHOW CREATE output. Byte-exactness here
        guards the BLOB length header (4-byte) and the payload move at the
        boundary lengths.

        Catalog:
            - ShowCommands

        Since: v3.3.6.x

        Labels: common,ci,show,blob

        Jira: None

        History:
            - 2026-09-30 Created for review round: pin the switch boundary

        """
        db = "show_create_thr_db"
        tb = "thrtb"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")

        # Step 1: measure the baseline DDL length with short column names.
        # Rendering is deterministic, and each extra byte in a column name adds
        # exactly one byte to the rendered DDL, so the baseline measurement
        # solves byte-exactly for the padding needed to hit the target.
        n_cols = 2000
        cols = ", ".join(f"c{i:04d} double" for i in range(n_cols))
        tdSql.execute(f"create table {tb} (ts timestamp, {cols})")
        tdSql.query(f"show create table {tb} FULL")
        base = len(tdSql.getData(0, 1))
        tdSql.execute(f"drop table {tb}")

        def build_with_extra(total_extra):
            # Distribute `total_extra` bytes over the first columns' names
            # (column names cap at 64 visible bytes; pad blocks of 55 keep a margin).
            per = 55
            names = []
            left = total_extra
            for i in range(n_cols):
                if left <= 0:
                    names.append(f"c{i:04d}")
                    continue
                take = min(per, left)
                left -= take
                names.append(f"c{i:04d}" + "x" * take)
            cols_sql = ", ".join(f"`{n}` double" for n in names)
            tdSql.execute(f"create table {tb} (ts timestamp, {cols_sql})")

        # Step 2: pad to exactly the varstr cap. Under FULL the content must be
        # complete (not silently truncated); one byte over must be BLOB. Without
        # FULL the result stays VARCHAR at any length (65535-clamped) -- covered
        # by test_show_create_default_clamps instead.
        build_with_extra(65535 - base)
        tdSql.query(f"show create table {tb} FULL")
        ddl = tdSql.getData(0, 1)
        assert len(ddl) == 65535, f"expected DDL of exactly 65535 bytes, got {len(ddl)}"
        col_type = tdSql.cursor.description[1][1]
        assert col_type == FieldType.C_BLOB, (
            f"FULL at the varstr cap must return BLOB, got type {col_type}")
        tdSql.execute(f"drop table {tb}")
        tdSql.execute(ddl)
        tdSql.query(f"show create table {tb} FULL")
        assert tdSql.getData(0, 1) == ddl, "replay at the boundary changed the DDL"
        tdSql.execute(f"drop table {tb}")

        # Step 3: one byte over the cap -- BLOB form takes over.
        build_with_extra(65536 - base)
        tdSql.query(f"show create table {tb} FULL")
        ddl = tdSql.getData(0, 1)
        assert len(ddl) == 65536, f"expected DDL of exactly 65536 bytes, got {len(ddl)}"
        assert tdSql.cursor.description[1][1] == FieldType.C_BLOB, (
            f"DDL one byte over the varstr cap should use BLOB, got type "
            f"{tdSql.cursor.description[1][1]}")
        tdSql.execute(f"drop table {tb}")
        tdSql.execute(ddl)
        tdSql.query(f"show create table {tb} FULL")
        assert tdSql.getData(0, 1) == ddl, "replay one byte over the cap changed the DDL"

        tdSql.execute(f"drop database if exists {db}")

    def test_show_create_vst_and_vchild_blob_direct(self):
        """Virtual super/child tables with >65535 DDL pick BLOB (direct assertions).

        The taosdump long-DDL case exercises these table forms end-to-end through
        dump/restore, but nothing asserts the SHOW CREATE result column type
        directly. Pin it here, mirroring the virtual-normal coverage:
          - a virtual super table (VIRTUAL 1) whose column list references a wide
            local source table
          - a virtual child table under it (USING ... TAGS) whose reference list
            likewise exceeds the varstr cap
        Both must return the BLOB column with complete, replayable DDL.

        Catalog:
            - ShowCommands

        Since: v3.3.6.x

        Labels: common,ci,show,blob

        Jira: None

        History:
            - 2026-10-08 Added direct column-type coverage for vst/vchild

        """
        db = "show_create_vstc_db"
        src = "srctb"
        vstb = "vstb"
        vctb = "vctb"
        num_cols = 1600
        col_names = [f"c{i:04d}" for i in range(num_cols)]
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        col_defs = ", ".join(f"`{c}` int" for c in col_names)
        tdSql.execute(f"create table {src} (ts timestamp, {col_defs})")
        # A virtual SUPER table's columns cannot carry FROM references (only
        # vtable/vchild lists can); its long DDL comes from wide ordinary columns
        # with padded names -- same shape as the taosdump long-DDL case.
        vstb_defs = ", ".join(f"`{c}{'x' * 40}` int" for c in col_names)
        tdSql.execute(f"create stable {vstb} (ts timestamp, {vstb_defs}) tags (loc int) virtual 1")
        # the child's column list references the SOURCE table; membership comes
        # from USING the virtual super table
        col_refs = ", ".join(f"`{c}` int from {db}.{src}.`{c}`" for c in col_names)
        tdSql.execute(f"create vtable {vctb} ({col_refs}) using {vstb} tags (7)")

        for form, table in (("show create table", vstb), ("show create vtable", vctb)):
            tdSql.query(f"{form} {table} FULL")
            tdSql.checkRows(1)
            assert tdSql.cursor.description[1][1] == FieldType.C_BLOB, (
                f"{form} on {table} should use BLOB for a >65535 DDL, got type "
                f"{tdSql.cursor.description[1][1]}")
            ddl = tdSql.getData(0, 1)
            assert len(ddl) > 65535, f"{table} DDL should exceed 65535 bytes, got {len(ddl)}"

        # replay the virtual super table DDL byte-identically
        tdSql.query(f"show create table {vstb} FULL")
        ddl = tdSql.getData(0, 1)
        tdSql.execute(f"drop table {vctb}")
        tdSql.execute(f"drop stable {vstb}")
        tdSql.execute(ddl)
        tdSql.query(f"show create table {vstb} FULL")
        assert tdSql.getData(0, 1) == ddl, "vstb replay changed the DDL"

        tdSql.execute(f"drop database if exists {db}")

    def test_show_create_vst_base_on_long_ddl(self):
        """A BASE ON inheritance chain renders completely in the BLOB result.

        The SHOW CREATE output of an inheriting virtual super table must carry
        its full "BASE ON" clause plus every referenced column; a clamp would
        drop parents or trailing references. Build a two-parent chain over wide
        reference lists so the DDL exceeds the varstr cap and verify BLOB form,
        clause presence, and byte-identical replay.

        Catalog:
            - ShowCommands

        Since: v3.3.6.x

        Labels: common,ci,show,blob

        Jira: None

        History:
            - 2026-10-08 Added BASE ON long-DDL coverage

        """
        db = "show_create_vbase_db"
        src = "srctb"
        num_cols = 900
        col_names = [f"c{i:04d}" for i in range(num_cols)]
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        # Virtual super tables take ordinary columns only (no FROM refs), so the
        # long DDL comes from padded column names on the parents and the leaf.
        col_defs = ", ".join(f"`{c}{'x' * 40}` int" for c in col_names)
        # distinct names per parent: VST inheritance rejects duplicate column names
        p2_defs = ", ".join(f"`d{i:04d}{'x' * 40}` int" for i in range(num_cols))
        tdSql.execute(
            f"create stable p1 (ts timestamp, {col_defs}) tags (loc int) virtual 1")
        tdSql.execute(
            f"create stable p2 (ts timestamp, {p2_defs}) tags (site int) virtual 1")
        # the leaf's rendered DDL carries only its OWN columns (parents appear
        # as a BASE ON name list), so the own list alone must exceed the cap
        own = ", ".join(f"`o{i:04d}{'x' * 45}` int" for i in range(1500))
        tdSql.execute(
            f"create stable leaf (ts timestamp, {own}) tags (extra int) "
            f"base on {db}.p1, {db}.p2 virtual 1"
        )

        tdSql.query(f"show create table leaf FULL")
        tdSql.checkRows(1)
        assert tdSql.cursor.description[1][1] == FieldType.C_BLOB, (
            f"BASE ON vst DDL should use BLOB, got type {tdSql.cursor.description[1][1]}")
        ddl = tdSql.getData(0, 1)
        assert len(ddl) > 65535, f"leaf DDL should exceed 65535 bytes, got {len(ddl)}"
        assert "BASE ON" in ddl, "BASE ON clause missing from the rendered DDL"
        for parent in ("p1", "p2"):
            assert f"`{parent}`" in ddl, f"parent {parent} missing from BASE ON clause"

        tdSql.execute(f"drop stable leaf")
        tdSql.execute(ddl)
        tdSql.query(f"show create table leaf FULL")
        assert tdSql.getData(0, 1) == ddl, "BASE ON replay changed the DDL"

        tdSql.execute(f"drop database if exists {db}")

    def test_show_create_vtable_threshold(self):
        """The VARCHAR/BLOB switch for VTABLE lands at the 65535-byte boundary.

        Virtual-table twin of test_show_create_varchar_blob_threshold: tune the
        rendered VTABLE DDL byte-exactly via column-name padding on the FROM
        references and verify completeness and replay on both sides of the cap.
        At exactly 65535 the result is BLOB (FULL decides by syntax, not
        length); one byte over must be BLOB as well.

        Catalog:
            - ShowCommands

        Since: v3.3.6.x

        Labels: common,ci,show,blob

        Jira: None

        History:
            - 2026-10-08 Added VTABLE threshold coverage

        """
        db = "show_create_vthr_db"
        src = "srctb"
        tb = "vthrtb"
        # ~56 bytes per reference: 1000 columns keep the baseline DDL at ~56KB,
        # leaving a ~9.5KB tuning window below the cap (padding max 55B/column)
        n_cols = 1000
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")

        short = [f"c{i:04d}" for i in range(n_cols)]
        tdSql.execute(f"create table {src} (ts timestamp, {', '.join(f'`{c}` int' for c in short)})")

        def build_vddl(total_extra):
            # pad the VTABLE's own column names (the source names stay short);
            # every pad byte is one byte of rendered DDL
            per = 55
            takes, left = [], total_extra
            for _base in short:
                if left <= 0:
                    takes.append(0)
                    continue
                take = min(per, left)
                left -= take
                takes.append(take)
            refs = ", ".join(
                f"`v{i:04d}{'x' * take}` int from {db}.{src}.`{n}`"
                for i, (n, take) in enumerate(zip(short, takes)))
            tdSql.execute(f"create vtable {tb} (ts timestamp, {refs})")

        tdSql.execute(f"create vtable {tb} (ts timestamp, {', '.join(f'`v{i:04d}` int from {db}.{src}.`{c}`' for i, c in enumerate(short))})")
        tdSql.query(f"show create vtable {tb} FULL")
        base = len(tdSql.getData(0, 1))
        tdSql.execute(f"drop table {tb}")

        for extra, must_blob in ((65535 - base, False), (65536 - base, True)):
            build_vddl(extra)
            tdSql.query(f"show create vtable {tb} FULL")
            ddl = tdSql.getData(0, 1)
            assert len(ddl) == base + extra, f"expected DDL of {base + extra} bytes, got {len(ddl)}"
            col_type = tdSql.cursor.description[1][1]
            if must_blob:
                assert col_type == FieldType.C_BLOB, f"one byte over the cap should be BLOB, got {col_type}"
            else:
                assert col_type == FieldType.C_BLOB, f"at the cap FULL must return BLOB, got {col_type}"
            tdSql.execute(f"drop table {tb}")
            tdSql.execute(ddl)
            tdSql.query(f"show create vtable {tb} FULL")
            assert tdSql.getData(0, 1) == ddl, "VTABLE replay at the boundary changed the DDL"
            tdSql.execute(f"drop table {tb}")

        tdSql.execute(f"drop database if exists {db}")

    def test_show_create_default_clamps(self):
        """Without FULL the historical VARCHAR result (65535 clamp) is preserved.

        Old consumers (connectors reading 2-byte varstr headers, tools keyed to
        the type-8 column) must keep seeing the exact pre-BLOB behavior unless
        they opt in with the FULL suffix: an over-cap DDL is silently clamped
        to exactly 65535 bytes in a VARCHAR column, and the statement still
        succeeds.

        Catalog:
            - ShowCommands

        Since: v3.4.3.0

        Labels: common,ci,show,blob

        Jira: None

        History:
            - 2026-10-08 Added with the FULL-suffix opt-in redesign

        """
        db = "show_create_nofull_db"
        tb = "clamptb"
        num_cols = 1500
        col_names = [f"c{i:05d}_name_padding_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" for i in range(num_cols)]
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        col_defs = ", ".join(f"`{c}` double" for c in col_names)
        tdSql.execute(f"create table {tb} (ts timestamp, {col_defs})")

        tdSql.query(f"show create table {tb}")
        tdSql.checkRows(1)
        assert tdSql.cursor.description[1][1] == FieldType.C_BINARY, (
            f"without FULL the result must stay VARCHAR/BINARY, got type "
            f"{tdSql.cursor.description[1][1]}")
        ddl = tdSql.getData(0, 1)
        assert isinstance(ddl, str) and len(ddl) == 65535, (
            f"over-cap DDL without FULL must clamp to exactly 65535 bytes, got {len(ddl)}")
        assert ddl.startswith(f"CREATE TABLE `{tb}`"), f"unexpected DDL start: {ddl[:50]}"

        # the same table with FULL returns the complete DDL as BLOB
        tdSql.query(f"show create table {tb} FULL")
        assert tdSql.cursor.description[1][1] == FieldType.C_BLOB, (
            f"FULL should return BLOB, got type {tdSql.cursor.description[1][1]}")
        full_ddl = tdSql.getData(0, 1)
        assert len(full_ddl) > 65535 and full_ddl.startswith(ddl[:1000]), (
            f"FULL DDL should be the complete superset, got {len(full_ddl)} bytes")

        tdSql.execute(f"drop database if exists {db}")
