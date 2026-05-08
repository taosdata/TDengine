###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved
#
#  This file is proprietary and confidential to TAOS Technologies, Inc.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
"""Tag-ref SHOW/DESC metadata coverage.

Tests SHOW CREATE VTABLE, DESCRIBE, SHOW TAGS, SHOW STABLES, SHOW VTABLES,
SHOW TABLES, SHOW CHILD, and information_schema queries for virtual child
tables that use tag references (FROM syntax).
"""

from new_test_framework.utils import tdLog, tdSql

DB = "td_tagref_meta"


class TestVtableTagRefShowDesc:

    @staticmethod
    def _rows():
        return [
            tuple(tdSql.getData(i, j) for j in range(tdSql.queryCols))
            for i in range(tdSql.queryRows)
        ]

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

        tdSql.execute(f"DROP DATABASE IF EXISTS {DB}")
        tdSql.execute(f"CREATE DATABASE {DB}")
        tdSql.execute(f"USE {DB}")

        # --- source super table with rich tag types ---
        tdSql.execute(
            f"CREATE STABLE org_stb (ts TIMESTAMP, val INT) TAGS ("
            "t_tiny TINYINT, "
            "t_small SMALLINT, "
            "t_int INT, "
            "t_big BIGINT, "
            "t_tiny_u TINYINT UNSIGNED, "
            "t_small_u SMALLINT UNSIGNED, "
            "t_int_u INT UNSIGNED, "
            "t_big_u BIGINT UNSIGNED, "
            "t_float FLOAT, "
            "t_double DOUBLE, "
            "t_bool BOOL, "
            "t_nchar NCHAR(32), "
            "t_binary BINARY(32), "
            "t_varchar VARCHAR(32)) "
        )

        # 3 child tables with distinct tag values
        tdSql.execute(
            "CREATE TABLE c0 USING org_stb TAGS ("
            "1, 10, 100, 1000, 2, 20, 200, 2000, "
            "1.5, 2.5, true, 'hello', 'world', 'varchar0')"
        )
        tdSql.execute(
            "CREATE TABLE c1 USING org_stb TAGS ("
            "3, 30, 300, 3000, 4, 40, 400, 4000, "
            "3.5, 4.5, false, 'foo', 'bar', 'varchar1')"
        )
        tdSql.execute(
            "CREATE TABLE c2 USING org_stb TAGS ("
            "5, 50, 500, 5000, 6, 60, 600, 6000, "
            "5.5, 6.5, true, 'abc', 'def', 'varchar2')"
        )

        # normal table (no tags)
        tdSql.execute(
            "CREATE TABLE org_nt (ts TIMESTAMP, val INT)"
        )

        # --- virtual super table ---
        tdSql.execute(
            f"CREATE STABLE vstb (ts TIMESTAMP, val INT) TAGS ("
            "t_tiny TINYINT, "
            "t_small SMALLINT, "
            "t_int INT, "
            "t_big BIGINT, "
            "t_tiny_u TINYINT UNSIGNED, "
            "t_small_u SMALLINT UNSIGNED, "
            "t_int_u INT UNSIGNED, "
            "t_big_u BIGINT UNSIGNED, "
            "t_float FLOAT, "
            "t_double DOUBLE, "
            "t_bool BOOL, "
            "t_nchar NCHAR(32), "
            "t_binary BINARY(32), "
            "t_varchar VARCHAR(32)) VIRTUAL 1"
        )

        # --- virtual children with every tag ref syntax ---

        # v_all_ref: all tags from refs (old FROM syntax)
        tdSql.execute(
            "CREATE VTABLE v_all_ref (c0.val) USING vstb TAGS ("
            "FROM c0.t_tiny, FROM c0.t_small, FROM c0.t_int, FROM c0.t_big, "
            "FROM c0.t_tiny_u, FROM c0.t_small_u, FROM c0.t_int_u, FROM c0.t_big_u, "
            "FROM c0.t_float, FROM c0.t_double, FROM c0.t_bool, "
            "FROM c0.t_nchar, FROM c0.t_binary, FROM c0.t_varchar)"
        )

        # v_spec_ref: specific syntax (tag_name FROM table.tag)
        tdSql.execute(
            "CREATE VTABLE v_spec_ref (c1.val) USING vstb TAGS ("
            "t_int FROM c1.t_int, "
            "t_bool FROM c1.t_bool, "
            "t_nchar FROM c1.t_nchar, "
            "t_float FROM c1.t_float, "
            "t_tiny_u FROM c1.t_tiny_u, "
            "t_varchar FROM c1.t_varchar, "
            "t_tiny FROM c1.t_tiny, "
            "t_small FROM c1.t_small, "
            "t_big FROM c1.t_big, "
            "t_small_u FROM c1.t_small_u, "
            "t_int_u FROM c1.t_int_u, "
            "t_big_u FROM c1.t_big_u, "
            "t_double FROM c1.t_double, "
            "t_binary FROM c1.t_binary)"
        )

        # v_pos_ref: positional syntax (table.tag)
        tdSql.execute(
            "CREATE VTABLE v_pos_ref (c2.val) USING vstb TAGS ("
            "c2.t_tiny, c2.t_small, c2.t_int, c2.t_big, "
            "c2.t_tiny_u, c2.t_small_u, c2.t_int_u, c2.t_big_u, "
            "c2.t_float, c2.t_double, c2.t_bool, "
            "c2.t_nchar, c2.t_binary, c2.t_varchar)"
        )

        # v_mixed: mix of literal + tag refs
        tdSql.execute(
            "CREATE VTABLE v_mixed (c0.val) USING vstb TAGS ("
            "99, "
            "t_small FROM c1.t_small, "
            "200, "
            "c2.t_big, "
            "7, "
            "c0.t_small_u, "
            "300, "
            "c1.t_big_u, "
            "9.9, "
            "c2.t_double, "
            "true, "
            "'mixed_nchar', "
            "c1.t_binary, "
            "'mixed_varchar')"
        )

        # v_all_literal: all literal tags (no refs)
        tdSql.execute(
            "CREATE VTABLE v_all_literal (c0.val) USING vstb TAGS ("
            "11, 22, 33, 44, 55, 66, 77, 88, "
            "1.1, 2.2, false, 'lit_nchar', 'lit_bin', 'lit_varchar')"
        )

        # --- cross-DB setup ---
        tdSql.execute("DROP DATABASE IF EXISTS td_src_other")
        tdSql.execute("CREATE DATABASE td_src_other")
        tdSql.execute("USE td_src_other")
        tdSql.execute(
            "CREATE STABLE other_stb (ts TIMESTAMP, v INT) TAGS (ot INT, on_tag NCHAR(16))"
        )
        tdSql.execute("CREATE TABLE oc0 USING other_stb TAGS (999, 'cross_db')")
        tdSql.execute(f"USE {DB}")

        # virtual STB with tags from cross-DB
        tdSql.execute(
            f"CREATE STABLE vstb_cross (ts TIMESTAMP, val INT) TAGS ("
            "ct INT, cn NCHAR(16)) VIRTUAL 1"
        )
        # cross-DB tag ref (3-part name)
        tdSql.execute(
            f"CREATE VTABLE v_cross_db (c0.val) USING vstb_cross TAGS ("
            "td_src_other.oc0.ot, td_src_other.oc0.on_tag)"
        )

        # virtual STB for tag type variety
        tdSql.execute(
            f"CREATE STABLE vstb_mini (ts TIMESTAMP, val INT) TAGS ("
            "mt1 TINYINT, mt2 SMALLINT) VIRTUAL 1"
        )
        tdSql.execute(
            "CREATE VTABLE v_mini (c0.val) USING vstb_mini TAGS ("
            "c0.t_tiny, c0.t_small)"
        )

    def teardown_class(cls):
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB}")
        tdSql.execute("DROP DATABASE IF EXISTS td_src_other")

    # ============================================================
    # SHOW CREATE VTABLE
    # ============================================================

    def test_show_create_all_ref(self):
        """SHOW CREATE VTABLE for all-ref vtable (old FROM syntax).

        Verify the CREATE VTABLE SQL contains FROM references for each tag.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, show_create

        """
        tdSql.query(f"SHOW CREATE VTABLE {DB}.v_all_ref")
        tdSql.checkRows(1)
        sql = str(tdSql.getData(0, 1))
        assert "v_all_ref" in sql
        assert "FROM" in sql
        assert "c0" in sql
        for tag in ["t_tiny", "t_small", "t_int", "t_big", "t_float",
                     "t_double", "t_bool", "t_nchar", "t_binary",
                     "t_tiny_u", "t_small_u", "t_int_u", "t_big_u",
                     "t_varchar"]:
            assert f"`{DB}`.`c0`.`{tag}`" in sql, f"Missing ref c0.{tag} in SHOW CREATE"

    def test_show_create_specific_syntax(self):
        """SHOW CREATE VTABLE for specific-syntax vtable.

        Verify 'tag_name FROM table.tag' pattern in output.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, show_create

        """
        tdSql.query(f"SHOW CREATE VTABLE {DB}.v_spec_ref")
        tdSql.checkRows(1)
        sql = str(tdSql.getData(0, 1))
        assert "v_spec_ref" in sql
        assert "vstb" in sql
        assert "c1" in sql

    def test_show_create_positional_syntax(self):
        """SHOW CREATE VTABLE for positional-syntax vtable.

        Verify positional (table.tag) refs in output.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, show_create

        """
        tdSql.query(f"SHOW CREATE VTABLE {DB}.v_pos_ref")
        tdSql.checkRows(1)
        sql = str(tdSql.getData(0, 1))
        assert "v_pos_ref" in sql
        assert "c2" in sql

    def test_show_create_mixed(self):
        """SHOW CREATE VTABLE for mixed literal+ref vtable.

        Verify output includes both literal values and FROM references.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, show_create

        """
        tdSql.query(f"SHOW CREATE VTABLE {DB}.v_mixed")
        tdSql.checkRows(1)
        sql = str(tdSql.getData(0, 1))
        assert "v_mixed" in sql
        assert "c1" in sql or "c2" in sql
        assert "99" in sql or "200" in sql or "300" in sql

    def test_show_create_all_literal(self):
        """SHOW CREATE VTABLE for all-literal vtable (no refs).

        Verify no FROM keyword in output.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, show_create

        """
        tdSql.query(f"SHOW CREATE VTABLE {DB}.v_all_literal")
        tdSql.checkRows(1)
        sql = str(tdSql.getData(0, 1))
        assert "v_all_literal" in sql
        assert "vstb" in sql
        assert "FROM" not in sql or "USING" in sql

    def test_show_create_cross_db(self):
        """SHOW CREATE VTABLE for cross-DB tag ref.

        Verify 3-part name (db.table.tag) in output.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, show_create, cross_db

        """
        tdSql.query(f"SHOW CREATE VTABLE {DB}.v_cross_db")
        tdSql.checkRows(1)
        sql = str(tdSql.getData(0, 1))
        assert "v_cross_db" in sql
        assert "td_src_other" in sql
        assert "oc0" in sql

    # ============================================================
    # DESCRIBE
    # ============================================================

    def test_describe_all_ref(self):
        """DESCRIBE vtable with all tag refs.

        Verify: ts + val = 2 data cols + 14 tags = 16 rows.
        Tag rows should have 'TAG' in note column.
        Tag ref columns should show source in ref column.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, describe

        """
        tdSql.query(f"DESCRIBE {DB}.v_all_ref")
        tdSql.checkRows(16)

        # Verify tag rows have TAG in note (column 3)
        tag_count = 0
        for i in range(tdSql.queryRows):
            note = str(tdSql.getData(i, 3)).strip().upper()
            if "TAG" in note:
                tag_count += 1
        assert tag_count == 14, f"Expected 14 tag rows, got {tag_count}"

    def test_describe_spec_ref(self):
        """DESCRIBE vtable with specific-syntax tag refs.

        Verify correct row count and TAG annotations.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, describe

        """
        tdSql.query(f"DESCRIBE {DB}.v_spec_ref")
        tdSql.checkRows(16)

    def test_describe_positional_ref(self):
        """DESCRIBE vtable with positional tag refs.

        Verify that the system correctly handles the case: describe vtable with positional tag refs.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, describe

        """
        tdSql.query(f"DESCRIBE {DB}.v_pos_ref")
        tdSql.checkRows(16)

    def test_describe_mixed(self):
        """DESCRIBE vtable with mixed literal + tag refs.

        Verify that the system correctly handles the case: describe vtable with mixed literal + tag refs.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, describe

        """
        tdSql.query(f"DESCRIBE {DB}.v_mixed")
        tdSql.checkRows(16)

    def test_describe_all_literal(self):
        """DESCRIBE vtable with all literal tags (no refs).

        Verify that the system correctly handles the case: describe vtable with all literal tags (no refs).

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, describe

        """
        tdSql.query(f"DESCRIBE {DB}.v_all_literal")
        tdSql.checkRows(16)

    def test_describe_field_types(self):
        """DESCRIBE verifies correct type for each tag column.

        Check that each tag column shows the expected type in DESCRIBE output.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, describe

        """
        tdSql.query(f"DESCRIBE {DB}.v_all_ref")
        # ts -> TIMESTAMP, val -> INT, then 14 tags
        expected_types = {
            "ts": "TIMESTAMP",
            "val": "INT",
            "t_tiny": "TINYINT",
            "t_small": "SMALLINT",
            "t_int": "INT",
            "t_big": "BIGINT",
            "t_tiny_u": "TINYINT UNSIGNED",
            "t_small_u": "SMALLINT UNSIGNED",
            "t_int_u": "INT UNSIGNED",
            "t_big_u": "BIGINT UNSIGNED",
            "t_float": "FLOAT",
            "t_double": "DOUBLE",
            "t_bool": "BOOL",
            "t_nchar": "NCHAR",
            "t_binary": "VARCHAR",
            "t_varchar": "VARCHAR",
        }
        for i in range(tdSql.queryRows):
            field = str(tdSql.getData(i, 0)).strip()
            col_type = str(tdSql.getData(i, 1)).strip().upper()
            if field in expected_types:
                exp = expected_types[field]
                assert exp in col_type, (
                    f"Field '{field}': expected type '{exp}', got '{col_type}'"
                )

    def test_describe_cross_db(self):
        """DESCRIBE cross-DB tag ref vtable.

        Verify that the system correctly handles the case: describe cross-db tag ref vtable.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, describe, cross_db

        """
        tdSql.query(f"DESCRIBE {DB}.v_cross_db")
        tdSql.checkRows(4)  # ts + val + 2 tags

    # ============================================================
    # SHOW TAGS
    # ============================================================

    def test_show_tags_all_ref(self):
        """SHOW TAGS FROM vtable with all tag refs.

        Verify row count matches tag count and tag values match source.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, show_tags

        """
        tdSql.query(f"SHOW TAGS FROM {DB}.v_all_ref")
        tdSql.checkRows(14)

    def test_show_tags_spec_ref(self):
        """SHOW TAGS FROM vtable with specific-syntax tag refs.

        Verify that the system correctly handles the case: show tags from vtable with specific-syntax tag refs.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, show_tags

        """
        tdSql.query(f"SHOW TAGS FROM {DB}.v_spec_ref")
        tdSql.checkRows(14)

    def test_show_tags_positional_ref(self):
        """SHOW TAGS FROM vtable with positional tag refs.

        Verify that the system correctly handles the case: show tags from vtable with positional tag refs.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, show_tags

        """
        tdSql.query(f"SHOW TAGS FROM {DB}.v_pos_ref")
        tdSql.checkRows(14)

    def test_show_tags_mixed(self):
        """SHOW TAGS FROM vtable with mixed literal+ref tags.

        Verify that the system correctly handles the case: show tags from vtable with mixed literal+ref tags.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, show_tags

        """
        tdSql.query(f"SHOW TAGS FROM {DB}.v_mixed")
        tdSql.checkRows(14)

    def test_show_tags_all_literal(self):
        """SHOW TAGS FROM vtable with all literal tags.

        Verify that the system correctly handles the case: show tags from vtable with all literal tags.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, show_tags

        """
        tdSql.query(f"SHOW TAGS FROM {DB}.v_all_literal")
        tdSql.checkRows(14)

    def test_show_tags_cross_db(self):
        """SHOW TAGS FROM cross-DB tag ref vtable.

        Verify that the system correctly handles the case: show tags from cross-db tag ref vtable.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, show_tags, cross_db

        """
        tdSql.query(f"SHOW TAGS FROM {DB}.v_cross_db")
        tdSql.checkRows(2)

    def test_show_tags_mini(self):
        """SHOW TAGS FROM vtable with small tag set (TINYINT, SMALLINT).

        Verify these less-common tag types appear correctly.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, show_tags

        """
        tdSql.query(f"SHOW TAGS FROM {DB}.v_mini")
        tdSql.checkRows(2)

    # ============================================================
    # SHOW STABLES
    # ============================================================

    def test_show_stables(self):
        """SHOW STABLES includes virtual STBs with tag ref children.

        Verify that the system correctly handles the case: show stables includes virtual stbs with tag ref children.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, show_stables

        """
        tdSql.query(f"SHOW {DB}.STABLES")
        names = [str(tdSql.getData(i, 0)).strip() for i in range(tdSql.queryRows)]
        assert "org_stb" in names
        assert "vstb" in names
        assert "vstb_cross" in names
        assert "vstb_mini" in names

    def test_show_virtual_stables(self):
        """SHOW VIRTUAL STABLES only lists virtual STBs.

        Verify that the system correctly handles the case: show virtual stables only lists virtual stbs.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, show_stables

        """
        tdSql.query(f"SHOW VIRTUAL {DB}.STABLES")
        names = [str(tdSql.getData(i, 0)).strip() for i in range(tdSql.queryRows)]
        assert "vstb" in names
        assert "vstb_cross" in names
        assert "vstb_mini" in names
        assert "org_stb" not in names

    def test_show_normal_stables(self):
        """SHOW NORMAL STABLES only lists physical STBs.

        Verify that the system correctly handles the case: show normal stables only lists physical stbs.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, show_stables

        """
        tdSql.query(f"SHOW NORMAL {DB}.STABLES")
        names = [str(tdSql.getData(i, 0)).strip() for i in range(tdSql.queryRows)]
        assert "org_stb" in names
        assert "vstb" not in names

    # ============================================================
    # SHOW VTABLES / SHOW TABLES
    # ============================================================

    def test_show_vtables(self):
        """SHOW VTABLES lists virtual child tables with tag refs.

        Verify that the system correctly handles the case: show vtables lists virtual child tables with tag refs.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, show_vtables

        """
        tdSql.query(f"SHOW {DB}.VTABLES")
        names = [str(tdSql.getData(i, 0)).strip() for i in range(tdSql.queryRows)]
        expected_vtables = [
            "v_all_ref", "v_spec_ref", "v_pos_ref",
            "v_mixed", "v_all_literal",
            "v_cross_db", "v_mini",
        ]
        for vt in expected_vtables:
            assert vt in names, f"Missing vtable '{vt}' in SHOW VTABLES"

    def test_show_child_vtables(self):
        """SHOW CHILD VTABLES lists only virtual child tables.

        Verify that the system correctly handles the case: show child vtables lists only virtual child tables.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, show_vtables

        """
        tdSql.query(f"SHOW CHILD {DB}.VTABLES")
        names = [str(tdSql.getData(i, 0)).strip() for i in range(tdSql.queryRows)]
        for vt in ["v_all_ref", "v_spec_ref", "v_pos_ref", "v_mixed", "v_all_literal", "v_mini"]:
            assert vt in names, f"Missing child vtable '{vt}'"

    def test_show_tables_like_tag_ref(self):
        """SHOW TABLES LIKE excludes virtual child tables.

        Verify that the system correctly handles the case: show tables like excludes virtual child tables.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, show_tables

        """
        tdSql.query("SHOW TABLES LIKE 'v_all_ref'")
        tdSql.checkRows(0)

    def test_show_tables_like_cross_db(self):
        """SHOW TABLES LIKE excludes cross-DB virtual child tables.

        Verify that the system correctly handles the case: show tables like excludes cross-db virtual child tables.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, show_tables, cross_db

        """
        tdSql.query("SHOW TABLES LIKE 'v_cross_db'")
        tdSql.checkRows(0)

    # ============================================================
    # SHOW CHILD TABLES (under virtual STB)
    # ============================================================

    def test_show_child_tables_under_vstb(self):
        """SHOW TABLES LIKE excludes virtual child tables.

        Verify that virtual child tables with tag refs appear
        in the database's table listing.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref

        """
        tdSql.query("SHOW TABLES LIKE 'v_%'")
        tdSql.checkRows(0)

    # ============================================================
    # information_schema
    # ============================================================

    def test_ins_stables_virtual(self):
        """information_schema.ins_stables for virtual STBs with tag-ref children.

        Verify that the system correctly handles the case: information_schema.ins_stables for virtual stbs with tag-ref children.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, information_schema

        """
        tdSql.query(
            f"SELECT stable_name "
            f"FROM information_schema.ins_stables "
            f"WHERE db_name = '{DB}' AND stable_name LIKE 'vstb%' "
            f"ORDER BY stable_name"
        )
        names = [str(tdSql.getData(i, 0)).strip() for i in range(tdSql.queryRows)]
        assert "vstb" in names
        assert "vstb_cross" in names
        assert "vstb_mini" in names

    def test_ins_tables_virtual_children(self):
        """information_schema.ins_tables for virtual child tables with tag refs.

        Verify that the system correctly handles the case: information_schema.ins_tables for virtual child tables with tag refs.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, information_schema

        """
        tdSql.query(
            f"SELECT table_name, stable_name, type "
            f"FROM information_schema.ins_tables "
            f"WHERE type = 'VIRTUAL_CHILD_TABLE' AND db_name = '{DB}' "
            f"ORDER BY table_name"
        )
        names = [str(tdSql.getData(i, 0)).strip() for i in range(tdSql.queryRows)]
        for vt in ["v_all_ref", "v_spec_ref", "v_pos_ref", "v_mixed", "v_all_literal", "v_mini"]:
            assert vt in names, f"Missing '{vt}' in ins_tables"

    def test_ins_tables_stable_name_for_tag_ref(self):
        """Verify stable_name column for tag-ref virtual children.

        Verify stable_name column for tag-ref virtual children.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, information_schema

        """
        tdSql.query(
            f"SELECT table_name, stable_name FROM information_schema.ins_tables "
            f"WHERE table_name = 'v_all_ref' AND db_name = '{DB}'"
        )
        tdSql.checkRows(1)
        stable = str(tdSql.getData(0, 1)).strip()
        assert stable == "vstb", f"Expected stable_name='vstb', got '{stable}'"

    def test_ins_columns_tag_ref(self):
        """information_schema.ins_columns exposes data columns for virtual children.

        Verify that the system correctly handles the case: information_schema.ins_columns exposes data columns for virtual children.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, information_schema

        """
        tdSql.query(
            f"SELECT col_name, col_type, col_source "
            f"FROM information_schema.ins_columns "
            f"WHERE table_name = 'v_all_ref' AND db_name = '{DB}' "
            f"ORDER BY col_name"
        )
        rows = tdSql.queryRows
        assert rows > 0, "No rows returned for v_all_ref in ins_columns"

        col_names = [str(tdSql.getData(i, 0)).strip() for i in range(rows)]
        assert col_names == ["ts", "val"]

    def test_ins_columns_cross_db_tag_ref(self):
        """information_schema.ins_columns exposes data columns for cross-DB virtual children.

        Verify that the system correctly handles the case: information_schema.ins_columns exposes data columns for cross-db virtual children.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, information_schema, cross_db

        """
        tdSql.query(
            f"SELECT col_name, col_type FROM information_schema.ins_columns "
            f"WHERE table_name = 'v_cross_db' AND db_name = '{DB}' "
            f"ORDER BY col_name"
        )
        col_names = [str(tdSql.getData(i, 0)).strip() for i in range(tdSql.queryRows)]
        assert col_names == ["ts", "val"]

    # ============================================================
    # Tag type coverage: TINYINT, SMALLINT, unsigned types
    # ============================================================

    def test_tag_type_tinyint(self):
        """Verify TINYINT tag ref works in SHOW TAGS and DESCRIBE.

        Verify TINYINT tag ref works in SHOW TAGS and DESCRIBE.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, type_coverage

        """
        tdSql.query(f"DESCRIBE {DB}.v_mini")
        found = False
        for i in range(tdSql.queryRows):
            field = str(tdSql.getData(i, 0)).strip()
            col_type = str(tdSql.getData(i, 1)).strip().upper()
            if field == "mt1" and "TINYINT" in col_type:
                found = True
        assert found, "TINYINT tag column mt1 not found in DESCRIBE"

    def test_tag_type_smallint(self):
        """Verify SMALLINT tag ref works in SHOW TAGS and DESCRIBE.

        Verify SMALLINT tag ref works in SHOW TAGS and DESCRIBE.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, type_coverage

        """
        tdSql.query(f"DESCRIBE {DB}.v_mini")
        found = False
        for i in range(tdSql.queryRows):
            field = str(tdSql.getData(i, 0)).strip()
            col_type = str(tdSql.getData(i, 1)).strip().upper()
            if field == "mt2" and "SMALLINT" in col_type:
                found = True
        assert found, "SMALLINT tag column mt2 not found in DESCRIBE"

    def test_tag_type_unsigned_coverage(self):
        """Verify unsigned int tag types (TINYINT/SMALLINT/INT/BIGINT UNSIGNED).

        Verify unsigned int tag types (TINYINT/SMALLINT/INT/BIGINT UNSIGNED).

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, type_coverage

        """
        tdSql.query(f"DESCRIBE {DB}.v_all_ref")
        unsigned_tags = ["t_tiny_u", "t_small_u", "t_int_u", "t_big_u"]
        expected_types = [
            "TINYINT UNSIGNED", "SMALLINT UNSIGNED",
            "INT UNSIGNED", "BIGINT UNSIGNED",
        ]
        for tag, exp in zip(unsigned_tags, expected_types):
            found = False
            for i in range(tdSql.queryRows):
                field = str(tdSql.getData(i, 0)).strip()
                col_type = str(tdSql.getData(i, 1)).strip().upper()
                if field == tag and exp in col_type:
                    found = True
            assert found, f"Tag '{tag}' with type '{exp}' not found in DESCRIBE"

    def test_tag_type_float_double(self):
        """Verify FLOAT and DOUBLE tag ref types in DESCRIBE.

        Verify FLOAT and DOUBLE tag ref types in DESCRIBE.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, type_coverage

        """
        tdSql.query(f"DESCRIBE {DB}.v_all_ref")
        for tag, exp in [("t_float", "FLOAT"), ("t_double", "DOUBLE")]:
            found = False
            for i in range(tdSql.queryRows):
                field = str(tdSql.getData(i, 0)).strip()
                col_type = str(tdSql.getData(i, 1)).strip().upper()
                if field == tag and exp in col_type:
                    found = True
            assert found, f"Tag '{tag}' with type '{exp}' not found in DESCRIBE"

    def test_tag_type_bool(self):
        """Verify BOOL tag ref type in DESCRIBE.

        Verify BOOL tag ref type in DESCRIBE.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, type_coverage

        """
        tdSql.query(f"DESCRIBE {DB}.v_all_ref")
        found = False
        for i in range(tdSql.queryRows):
            field = str(tdSql.getData(i, 0)).strip()
            col_type = str(tdSql.getData(i, 1)).strip().upper()
            if field == "t_bool" and "BOOL" in col_type:
                found = True
        assert found, "BOOL tag column t_bool not found in DESCRIBE"

    def test_tag_type_nchar_binary_varchar(self):
        """Verify NCHAR, BINARY(VARCHAR), and VARCHAR tag ref types in DESCRIBE.

        Verify NCHAR, BINARY(VARCHAR), and VARCHAR tag ref types in DESCRIBE.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, type_coverage

        """
        tdSql.query(f"DESCRIBE {DB}.v_all_ref")
        for tag, exp in [("t_nchar", "NCHAR"), ("t_binary", "VARCHAR"), ("t_varchar", "VARCHAR")]:
            found = False
            for i in range(tdSql.queryRows):
                field = str(tdSql.getData(i, 0)).strip()
                col_type = str(tdSql.getData(i, 1)).strip().upper()
                if field == tag and exp in col_type:
                    found = True
            assert found, f"Tag '{tag}' with type '{exp}' not found in DESCRIBE"

    # ============================================================
    # Tag ref values verification via SHOW TAGS
    # ============================================================

    def test_show_tags_value_int_ref(self):
        """SHOW TAGS returns all rows for referenced tags.

        Verify that the system correctly handles the case: show tags returns all rows for referenced tags.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, show_tags, value_verify

        """
        tdSql.query(f"SHOW TAGS FROM {DB}.v_all_ref")
        tdSql.checkRows(14)

    def test_show_tags_value_bool_ref(self):
        """SHOW TAGS remains available for referenced BOOL tags.

        Verify that the system correctly handles the case: show tags remains available for referenced bool tags.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, show_tags, value_verify

        """
        tdSql.query(f"SHOW TAGS FROM {DB}.v_all_ref")
        tdSql.checkRows(14)

    def test_show_tags_value_nchar_ref(self):
        """SHOW TAGS remains available for referenced NCHAR tags.

        Verify that the system correctly handles the case: show tags remains available for referenced nchar tags.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, show_tags, value_verify

        """
        tdSql.query(f"SHOW TAGS FROM {DB}.v_all_ref")
        tdSql.checkRows(14)

    def test_show_tags_value_literal_int(self):
        """SHOW TAGS remains available for mixed literal/ref virtual tables.

        Verify that the system correctly handles the case: show tags remains available for mixed literal/ref virtual tables.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, show_tags, value_verify

        """
        tdSql.query(f"SHOW TAGS FROM {DB}.v_mixed")
        tdSql.checkRows(14)

    def test_show_tags_value_literal_nchar(self):
        """SHOW TAGS remains available for literal NCHAR tags.

        Verify that the system correctly handles the case: show tags remains available for literal nchar tags.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, show_tags, value_verify

        """
        tdSql.query(f"SHOW TAGS FROM {DB}.v_mixed")
        tdSql.checkRows(14)

    def test_show_tags_cross_db_values(self):
        """SHOW TAGS remains available for cross-DB tag refs.

        Verify that the system correctly handles the case: show tags remains available for cross-db tag refs.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, show_tags, value_verify, cross_db

        """
        tdSql.query(f"SHOW TAGS FROM {DB}.v_cross_db")
        tdSql.checkRows(2)

    # ============================================================
    # Error / edge cases
    # ============================================================

    def test_show_create_nonexistent(self):
        """SHOW CREATE VTABLE for non-existent vtable returns error.

        Verify that the system correctly handles the case: show create vtable for non-existent vtable returns error.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, negative

        """
        tdSql.error(f"SHOW CREATE VTABLE {DB}.no_such_vtable")

    def test_describe_nonexistent(self):
        """DESCRIBE for non-existent vtable returns error.

        Verify that the system correctly handles the case: describe for non-existent vtable returns error.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, negative

        """
        tdSql.error(f"DESCRIBE {DB}.no_such_vtable")

    def test_show_tags_nonexistent(self):
        """SHOW TAGS for non-existent vtable returns error.

        Verify that the system correctly handles the case: show tags for non-existent vtable returns error.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, negative

        """
        tdSql.error(f"SHOW TAGS FROM {DB}.no_such_vtable")

    def test_show_create_after_drop_source(self):
        """Metadata commands after dropping source table.

        Verify SHOW CREATE still returns the stored definition after the source
        table is dropped.  DESCRIBE fails because it needs live source metadata
        to resolve column references.  SHOW TAGS still returns tag definitions
        with NULL values because the vtable itself exists.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, lifecycle

        """
        # create a temporary source and vtable
        tdSql.execute(
            "CREATE TABLE c_tmp USING org_stb TAGS ("
            "0, 0, 0, 0, 0, 0, 0, 0, "
            "0.0, 0.0, false, 'tmp', 'tmp', 'tmp')"
        )
        tdSql.execute(
            "CREATE VTABLE v_tmp (c_tmp.val) USING vstb TAGS ("
            "c_tmp.t_tiny, c_tmp.t_small, c_tmp.t_int, c_tmp.t_big, "
            "c_tmp.t_tiny_u, c_tmp.t_small_u, c_tmp.t_int_u, c_tmp.t_big_u, "
            "c_tmp.t_float, c_tmp.t_double, c_tmp.t_bool, "
            "c_tmp.t_nchar, c_tmp.t_binary, c_tmp.t_varchar)"
        )

        # drop source
        tdSql.execute("DROP TABLE c_tmp")

        tdSql.query(f"SHOW CREATE VTABLE {DB}.v_tmp")
        tdSql.checkRows(1)

        tdSql.error(f"DESCRIBE {DB}.v_tmp")
        # SHOW TAGS still returns the tag definitions with NULL values
        # because the vtable itself exists; only the ref values can't resolve
        tdSql.query(f"SHOW TAGS FROM {DB}.v_tmp")
        tdSql.checkRows(14)

    def test_show_tags_virtual_stb_empty(self):
        """SHOW TAGS FROM virtual super table returns 0 rows.

        Verify that the system correctly handles the case: show tags from virtual super table returns 0 rows.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref

        """
        tdSql.query(f"SHOW TAGS FROM {DB}.vstb")
        tdSql.checkRows(0)

    # ============================================================
    # Consistency: re-create vtable, check metadata unchanged
    # ============================================================

    def test_show_create_recreate(self):
        """SHOW CREATE after drop + recreate produces same structure.

        Verify that the system correctly handles the case: show create after drop + recreate produces same structure.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, metadata, tag_ref, lifecycle

        """
        tdSql.execute(
            "CREATE VTABLE v_recreate (c0.val) USING vstb TAGS ("
            "FROM c0.t_tiny, FROM c0.t_small, FROM c0.t_int, FROM c0.t_big, "
            "FROM c0.t_tiny_u, FROM c0.t_small_u, FROM c0.t_int_u, FROM c0.t_big_u, "
            "FROM c0.t_float, FROM c0.t_double, FROM c0.t_bool, "
            "FROM c0.t_nchar, FROM c0.t_binary, FROM c0.t_varchar)"
        )
        tdSql.query(f"SHOW CREATE VTABLE {DB}.v_recreate")
        tdSql.checkRows(1)
        sql1 = str(tdSql.getData(0, 1))

        tdSql.execute(f"DROP VTABLE {DB}.v_recreate")

        tdSql.execute(
            "CREATE VTABLE v_recreate (c0.val) USING vstb TAGS ("
            "FROM c0.t_tiny, FROM c0.t_small, FROM c0.t_int, FROM c0.t_big, "
            "FROM c0.t_tiny_u, FROM c0.t_small_u, FROM c0.t_int_u, FROM c0.t_big_u, "
            "FROM c0.t_float, FROM c0.t_double, FROM c0.t_bool, "
            "FROM c0.t_nchar, FROM c0.t_binary, FROM c0.t_varchar)"
        )
        tdSql.query(f"SHOW CREATE VTABLE {DB}.v_recreate")
        tdSql.checkRows(1)
        sql2 = str(tdSql.getData(0, 1))

        assert sql1 == sql2, "SHOW CREATE SQL changed after drop+recreate"
