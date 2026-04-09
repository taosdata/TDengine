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
"""ALTER interaction coverage for virtual tables that use tag references."""

from new_test_framework.utils import tdLog, tdSql

DB = "td_tagref_alter"


class TestVtableTagRefAlter:

    @staticmethod
    def _prepare():
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB}")
        tdSql.execute(f"CREATE DATABASE {DB}")
        tdSql.execute(f"USE {DB}")

        tdSql.execute(
            "CREATE STABLE src_stb (ts TIMESTAMP, val INT) "
            "TAGS (city NCHAR(20), code INT)"
        )
        tdSql.execute("CREATE TABLE src0 USING src_stb TAGS ('beijing', 100)")
        tdSql.execute("CREATE TABLE src1 USING src_stb TAGS ('shanghai', 200)")

        for offset, value in enumerate([1, 2, 3]):
            tdSql.execute(
                f"INSERT INTO src0 VALUES ({1700000000000 + offset * 1000}, {value})"
            )
        for offset, value in enumerate([10, 11]):
            tdSql.execute(
                f"INSERT INTO src1 VALUES ({1700000005000 + offset * 1000}, {value})"
            )

        tdSql.execute(
            "CREATE STABLE vstb (ts TIMESTAMP, val INT) "
            "TAGS (local_tag NCHAR(20), ref_city NCHAR(20), ref_code INT) VIRTUAL 1"
        )
        tdSql.execute(
            "CREATE VTABLE v0 (val FROM src0.val) USING vstb TAGS ("
            "'local0', "
            "ref_city FROM src0.city, "
            "ref_code FROM src1.code)"
        )
        tdSql.execute(
            "CREATE VTABLE v1 (val FROM src1.val) USING vstb TAGS ("
            "'local1', "
            "ref_city FROM src1.city, "
            "ref_code FROM src0.code)"
        )

    @staticmethod
    def _distinct_values(sql):
        tdSql.query(sql)
        return sorted(
            tuple(str(tdSql.getData(i, j)) for j in range(tdSql.queryCols))
            for i in range(tdSql.queryRows)
        )

    def setup_method(self, method):
        tdLog.debug(f"start to execute {__file__}::{method.__name__}")
        self._prepare()

    def teardown_method(self, method):
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB}")

    def test_alter_source_child_set_tag_updates_referenced_values(self):
        """ALTER TABLE child SET TAG propagates to referenced tag values."""
        tdSql.execute(f"USE {DB}")
        before = self._distinct_values("SELECT DISTINCT ref_city, ref_code FROM v0")
        assert before == [("beijing", "200")]

        tdSql.execute("ALTER TABLE src0 SET TAG city='beijing_new'")
        tdSql.execute("ALTER TABLE src1 SET TAG code=250")

        after = self._distinct_values("SELECT DISTINCT ref_city, ref_code FROM v0")
        assert after == [("beijing_new", "250")]

    def test_alter_source_stable_add_tag_preserves_existing_refs(self):
        """ALTER STABLE source ADD TAG does not break existing tag refs."""
        tdSql.execute(f"USE {DB}")
        tdSql.execute("ALTER STABLE src_stb ADD TAG region NCHAR(20)")
        tdSql.execute("ALTER TABLE src0 SET TAG region='north'")
        tdSql.execute("ALTER TABLE src1 SET TAG region='east'")

        values = self._distinct_values(
            "SELECT DISTINCT tbname, ref_city, ref_code FROM vstb"
        )
        assert values == [
            ("v0", "beijing", "200"),
            ("v1", "shanghai", "100"),
        ]

    def test_alter_virtual_stable_add_and_drop_local_tag(self):
        """ALTER STABLE on the virtual stable updates child tag metadata."""
        tdSql.execute(f"USE {DB}")
        tdSql.query(
            f"SELECT tag_name FROM information_schema.ins_tags "
            f"WHERE db_name='{DB}' AND table_name='v0' ORDER BY tag_name"
        )
        tdSql.checkRows(3)

        tdSql.execute("ALTER STABLE vstb ADD TAG extra_tag INT")
        tdSql.query(
            f"SELECT tag_name FROM information_schema.ins_tags "
            f"WHERE db_name='{DB}' AND table_name='v0' ORDER BY tag_name"
        )
        tdSql.checkRows(4)

        tdSql.execute("ALTER STABLE vstb DROP TAG extra_tag")
        tdSql.query(
            f"SELECT tag_name FROM information_schema.ins_tags "
            f"WHERE db_name='{DB}' AND table_name='v0' ORDER BY tag_name"
        )
        tdSql.checkRows(3)

    def test_alter_virtual_stable_rename_and_modify_local_tag(self):
        """ALTER STABLE rename/modify tag is reflected in child metadata."""
        tdSql.execute(f"USE {DB}")
        tdSql.execute("ALTER STABLE vstb RENAME TAG local_tag local_label")
        tdSql.execute("ALTER STABLE vstb MODIFY TAG local_label NCHAR(64)")

        tdSql.query(
            f"SELECT tag_type FROM information_schema.ins_tags "
            f"WHERE db_name='{DB}' AND table_name='v0' AND tag_name='local_label'"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "NCHAR(64)")

        tdSql.query(
            f"SELECT tag_name FROM information_schema.ins_tags "
            f"WHERE db_name='{DB}' AND table_name='v0' AND tag_name='local_tag'"
        )
        tdSql.checkRows(0)

    def test_alter_tag_ref_vtable_set_local_tag(self):
        """Direct SET TAG updates a literal tag on a tag-ref vtable."""
        tdSql.execute(f"USE {DB}")
        tdSql.execute("ALTER VTABLE v0 SET TAG local_tag='local0_updated'")
        tag_ref_values = self._distinct_values(
            "SELECT DISTINCT local_tag, ref_city, ref_code FROM v0"
        )
        assert tag_ref_values == [("local0_updated", "beijing", "200")]

    def test_alter_tag_ref_vtable_set_ref_tag_to_literal_is_rejected(self):
        """Direct SET TAG cannot rewrite a tag-ref field to a literal value."""
        tdSql.execute(f"USE {DB}")
        tdSql.error("ALTER VTABLE v0 SET TAG ref_city='literal_city'")
        tag_ref_values = self._distinct_values(
            "SELECT DISTINCT local_tag, ref_city, ref_code FROM v0"
        )
        assert tag_ref_values == [("local0", "beijing", "200")]

    def test_alter_vtable_using_stable_can_update_local_tag(self):
        """Batch SET TAG through the virtual stable can update non-ref local tags."""
        tdSql.execute(f"USE {DB}")
        tdSql.execute("ALTER VTABLE USING vstb SET TAG local_tag='local0_batch' WHERE tbname='v0'")
        tag_ref_values = self._distinct_values(
            "SELECT DISTINCT local_tag, ref_city, ref_code FROM v0"
        )
        assert tag_ref_values == [("local0_batch", "beijing", "200")]

    def test_alter_vtable_using_stable_cannot_override_tag_ref_with_literal(self):
        """Batch SET TAG does not rewrite a tag-ref field to a literal value."""
        tdSql.execute(f"USE {DB}")
        tdSql.execute("ALTER VTABLE USING vstb SET TAG ref_city='literal_city' WHERE tbname='v0'")
        tag_ref_values = self._distinct_values(
            "SELECT DISTINCT local_tag, ref_city, ref_code FROM v0"
        )
        assert tag_ref_values == [("local0", "beijing", "200")]

    def test_alter_vtable_using_stable_cannot_set_tag_ref_to_another_reference(self):
        """ALTER ... SET TAG only accepts literal values, not tag-reference syntax."""
        tdSql.execute(f"USE {DB}")
        tdSql.error("ALTER VTABLE USING vstb SET TAG ref_city=src1.city WHERE tbname='v0'")
