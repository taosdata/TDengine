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
"""ALTER TAG REF tests: tag-ref ↔ static value conversion for virtual child tables."""

from new_test_framework.utils import tdLog, tdSql

DB = "td_alter_tag_ref"


class TestVtableAlterTagRef:

    @staticmethod
    def _prepare():
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB}")
        tdSql.execute(f"CREATE DATABASE {DB}")
        tdSql.execute(f"USE {DB}")

        # Source tables
        tdSql.execute(
            "CREATE STABLE src_stb (ts TIMESTAMP, val INT) "
            "TAGS (city NCHAR(20), code INT)"
        )
        tdSql.execute("CREATE TABLE src0 USING src_stb TAGS ('beijing', 100)")
        tdSql.execute("CREATE TABLE src1 USING src_stb TAGS ('shanghai', 200)")

        for i in range(3):
            tdSql.execute(
                f"INSERT INTO src0 VALUES ({1700000000000 + i * 1000}, {i + 1})"
            )
        for i in range(2):
            tdSql.execute(
                f"INSERT INTO src1 VALUES ({1700000005000 + i * 1000}, {i + 10})"
            )

        # Virtual super table and child tables
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

    # -------------------------------------------------------
    # 1. local tag → tag-ref
    # -------------------------------------------------------
    def test_local_tag_to_tag_ref(self):
        """Convert a local (static) tag to a tag-ref.

        Verify that ALTER VTABLE v0 SET TAG local_tag = src0.city
        establishes a tag-ref and subsequent queries resolve dynamically.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {DB}")

        # Before: local_tag is 'local0'
        vals = self._distinct_values("SELECT DISTINCT local_tag FROM v0")
        assert vals == [("local0",)]

        # Set local_tag to reference src0.city
        tdSql.execute("ALTER VTABLE v0 SET TAG local_tag = src0.city")

        # After: local_tag should resolve to 'beijing'
        vals = self._distinct_values("SELECT DISTINCT local_tag FROM v0")
        assert vals == [("beijing",)]

        # Dynamic: modify source, verify reflected
        tdSql.execute("ALTER TABLE src0 SET TAG city='nanjing'")
        vals = self._distinct_values("SELECT DISTINCT local_tag FROM v0")
        assert vals == [("nanjing",)]

    # -------------------------------------------------------
    # 2. tag-ref → static value
    # -------------------------------------------------------
    def test_tag_ref_to_static_value(self):
        """Convert a tag-ref back to a static literal value.

        Verify that ALTER VTABLE v0 SET TAG ref_city='static_city'
        clears the tag-ref and sets a static value.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {DB}")

        # Before: ref_city references src0.city → 'beijing'
        vals = self._distinct_values("SELECT DISTINCT ref_city FROM v0")
        assert vals == [("beijing",)]

        # Set ref_city to a static value
        tdSql.execute("ALTER VTABLE v0 SET TAG ref_city='static_city'")

        # After: ref_city should be 'static_city'
        vals = self._distinct_values("SELECT DISTINCT ref_city FROM v0")
        assert vals == [("static_city",)]

        # Changing source should NOT affect the now-static tag
        tdSql.execute("ALTER TABLE src0 SET TAG city='nanjing'")
        vals = self._distinct_values("SELECT DISTINCT ref_city FROM v0")
        assert vals == [("static_city",)]

    # -------------------------------------------------------
    # 3. tag-ref → different tag-ref
    # -------------------------------------------------------
    def test_tag_ref_to_different_ref(self):
        """Change a tag-ref to reference a different source.

        Verify that ALTER VTABLE v0 SET TAG ref_city = src1.city
        switches the tag-ref source.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {DB}")

        # Before: ref_city references src0.city → 'beijing'
        vals = self._distinct_values("SELECT DISTINCT ref_city FROM v0")
        assert vals == [("beijing",)]

        # Switch to reference src1.city
        tdSql.execute("ALTER VTABLE v0 SET TAG ref_city = src1.city")

        # After: should resolve to 'shanghai'
        vals = self._distinct_values("SELECT DISTINCT ref_city FROM v0")
        assert vals == [("shanghai",)]

    # -------------------------------------------------------
    # 4. Error cases
    # -------------------------------------------------------
    def test_type_mismatch_rejected(self):
        """Setting tag-ref with type mismatch is rejected.

        Verify that SET TAG = column_ref fails when the tag type differs from the source column type.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {DB}")
        # ref_code is INT, src0.city is NCHAR → type mismatch
        tdSql.error("ALTER VTABLE v0 SET TAG ref_code = src0.city")

    def test_source_table_not_exist(self):
        """Setting tag-ref to non-existent source table is rejected.

        Verify that SET TAG = column_ref fails when the referenced source table does not exist.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {DB}")
        tdSql.error("ALTER VTABLE v0 SET TAG ref_city = nonexist_table.city")

    def test_source_col_not_tag(self):
        """Setting tag-ref to a regular column (not tag) is rejected.

        Verify that SET TAG = column_ref fails when the source column is a data column, not a tag.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {DB}")
        # 'val' is a data column, not a tag
        tdSql.error("ALTER VTABLE v0 SET TAG ref_code = src0.val")

    def test_non_virtual_table_rejected(self):
        """ALTER TABLE SET TAG = column_ref on a non-virtual table is rejected.

        Verify that ALTER VTABLE SET TAG with column_ref fails on physical child tables.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {DB}")
        tdSql.error("ALTER VTABLE src0 SET TAG city = src1.city")

    def test_super_table_rejected(self):
        """ALTER super table SET TAG = column_ref is rejected.

        Verify that ALTER STABLE SET TAG with column_ref fails on super tables.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {DB}")
        tdSql.error("ALTER STABLE vstb SET TAG ref_city = src0.city")

    def test_nonexistent_tag_name(self):
        """Setting tag-ref on a non-existent tag name is rejected.

        Verify that SET TAG = column_ref fails when the tag name does not exist on the virtual table.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {DB}")
        tdSql.error("ALTER VTABLE v0 SET TAG no_such_tag = src0.city")

    # -------------------------------------------------------
    # 5. Dynamic consistency after conversions
    # -------------------------------------------------------
    def test_source_tag_change_after_ref_established(self):
        """After establishing tag-ref, source tag changes are reflected.

        Verify that modifying the source table's tag value is immediately visible through the tag-ref.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {DB}")

        # Establish a new tag-ref: local_tag → src1.city
        tdSql.execute("ALTER VTABLE v0 SET TAG local_tag = src1.city")
        vals = self._distinct_values("SELECT DISTINCT local_tag FROM v0")
        assert vals == [("shanghai",)]

        # Modify source
        tdSql.execute("ALTER TABLE src1 SET TAG city='hangzhou'")
        vals = self._distinct_values("SELECT DISTINCT local_tag FROM v0")
        assert vals == [("hangzhou",)]

    def test_round_trip_local_ref_local(self):
        """Full round-trip: local → ref → local preserves correct values.

        Verify converting a tag from literal to ref and back to literal preserves correct query results.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {DB}")

        # Start: local_tag = 'local0'
        vals = self._distinct_values("SELECT DISTINCT local_tag FROM v0")
        assert vals == [("local0",)]

        # Convert to ref
        tdSql.execute("ALTER VTABLE v0 SET TAG local_tag = src0.city")
        vals = self._distinct_values("SELECT DISTINCT local_tag FROM v0")
        assert vals == [("beijing",)]

        # Convert back to static
        tdSql.execute("ALTER VTABLE v0 SET TAG local_tag='final_value'")
        vals = self._distinct_values("SELECT DISTINCT local_tag FROM v0")
        assert vals == [("final_value",)]

        # Source changes should NOT affect the now-static tag
        tdSql.execute("ALTER TABLE src0 SET TAG city='nanjing'")
        vals = self._distinct_values("SELECT DISTINCT local_tag FROM v0")
        assert vals == [("final_value",)]
