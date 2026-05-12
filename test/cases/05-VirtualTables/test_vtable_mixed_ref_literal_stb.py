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
"""Tests for mixed ref/literal tag values at STB level.

Verifies that when some virtual children have tag-refs and others have
literal tag values for the same tag column, STB-level queries correctly
return values for ALL children (not NULL for literal children).
"""

import time
from new_test_framework.utils import tdLog, tdSql


DB = "td_mixed_ref_lit"


class TestVtableMixedRefLiteralStb:

    def setup_method(self):
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB}")
        tdSql.execute(f"CREATE DATABASE {DB}")
        tdSql.execute(f"USE {DB}")

    def teardown_method(self):
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB}")

    def _cache_reset(self):
        time.sleep(1)
        tdSql.execute("RESET QUERY CACHE")
        time.sleep(0.5)

    def _setup_basic(self, num_src=3, num_vc=5):
        """Create source tables and virtual STB with ref tags."""
        tdSql.execute(
            "CREATE STABLE src_stb (ts TIMESTAMP, val INT) "
            "TAGS (city NCHAR(32), code INT)"
        )
        for i in range(num_src):
            tdSql.execute(
                f"CREATE TABLE src_{i} USING src_stb "
                f"TAGS ('city_{i}', {100 + i})"
            )
            tdSql.execute(
                f"INSERT INTO src_{i} VALUES (1700000000000, {i * 10})"
            )

        tdSql.execute(
            "CREATE STABLE vstb (ts TIMESTAMP, val INT) "
            "TAGS (name NCHAR(32), ref_city NCHAR(32), ref_code INT) VIRTUAL 1"
        )
        for i in range(num_vc):
            src = i % num_src
            tdSql.execute(
                f"CREATE VTABLE vc_{i} (val FROM src_{src}.val) USING vstb "
                f"TAGS ('name_{i}', ref_city FROM src_{src}.city, "
                f"ref_code FROM src_{src}.code)"
            )

    def test_mixed_ref_literal_stb_query(self):
        """STB query with some children ref, some literal - core bug test.

        Verify that STB-level queries return correct tag values when some children
        have tag-refs and others have literal tag values.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        self._setup_basic(num_src=3, num_vc=5)

        # Verify all-ref baseline
        self._cache_reset()
        tdSql.query("SELECT name, ref_city, ref_code FROM vstb ORDER BY name")
        assert tdSql.queryRows == 5
        for i in range(5):
            row = tdSql.queryResult[i]
            assert row[0] == f"name_{i}", f"row {i} name mismatch: {row[0]}"
            assert row[1] is not None, f"row {i} ref_city is NULL"
            assert row[2] is not None, f"row {i} ref_code is NULL"

        # Clear refs for vc_1 and vc_3 (convert to literal)
        tdSql.execute("ALTER VTABLE vc_1 SET TAG ref_city = 'LIT_CITY_1'")
        tdSql.execute("ALTER VTABLE vc_1 SET TAG ref_code = 999")
        tdSql.execute("ALTER VTABLE vc_3 SET TAG ref_city = 'LIT_CITY_3'")
        tdSql.execute("ALTER VTABLE vc_3 SET TAG ref_code = 888")

        # STB query should return correct values for ALL children
        self._cache_reset()
        tdSql.query("SELECT name, ref_city, ref_code FROM vstb ORDER BY name")
        assert tdSql.queryRows == 5

        expected = {
            "name_0": ("city_0", 100),   # ref to src_0
            "name_1": ("LIT_CITY_1", 999),  # literal
            "name_2": ("city_2", 102),   # ref to src_2
            "name_3": ("LIT_CITY_3", 888),  # literal
            "name_4": ("city_1", 101),   # ref to src_1
        }
        for i in range(5):
            row = tdSql.queryResult[i]
            name = row[0]
            assert name in expected, f"unexpected name: {name}"
            exp_city, exp_code = expected[name]
            assert row[1] == exp_city, (
                f"{name}: ref_city={row[1]}, expected={exp_city}"
            )
            assert row[2] == exp_code, (
                f"{name}: ref_code={row[2]}, expected={exp_code}"
            )

    def test_mixed_partial_ref_clear(self):
        """Clear only ONE ref tag column, keep the other as ref.

        Verify that clearing a single tag-ref column to literal preserves the
        other tag-ref column's dynamic resolution.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        self._setup_basic(num_src=2, num_vc=3)

        # Clear only ref_city for vc_0, keep ref_code as ref
        tdSql.execute("ALTER VTABLE vc_0 SET TAG ref_city = 'STATIC_CITY'")

        self._cache_reset()
        tdSql.query("SELECT name, ref_city, ref_code FROM vstb ORDER BY name")
        assert tdSql.queryRows == 3

        # vc_0: ref_city=literal, ref_code=ref(src_0)=100
        row0 = [r for r in tdSql.queryResult if r[0] == "name_0"][0]
        assert row0[1] == "STATIC_CITY", f"vc_0 ref_city={row0[1]}"
        assert row0[2] == 100, f"vc_0 ref_code={row0[2]}"

        # vc_1: both still ref(src_1)
        row1 = [r for r in tdSql.queryResult if r[0] == "name_1"][0]
        assert row1[1] == "city_1", f"vc_1 ref_city={row1[1]}"
        assert row1[2] == 101, f"vc_1 ref_code={row1[2]}"

    def test_mixed_with_data_query(self):
        """STB query including data columns alongside mixed ref/literal tags.

        Verify that data columns resolve correctly alongside mixed tag values
        in STB-level queries.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        self._setup_basic(num_src=2, num_vc=4)

        # Clear refs for vc_2
        tdSql.execute("ALTER VTABLE vc_2 SET TAG ref_city = 'FIXED'")
        tdSql.execute("ALTER VTABLE vc_2 SET TAG ref_code = 777")

        self._cache_reset()
        tdSql.query(
            "SELECT name, ref_city, ref_code, val FROM vstb ORDER BY name"
        )
        assert tdSql.queryRows == 4

        # vc_2 refs src_0 for data, literal for tags
        row2 = [r for r in tdSql.queryResult if r[0] == "name_2"][0]
        assert row2[1] == "FIXED", f"vc_2 ref_city={row2[1]}"
        assert row2[2] == 777, f"vc_2 ref_code={row2[2]}"
        assert row2[3] is not None, "vc_2 val should not be NULL"

    def test_mixed_filter_on_literal_tag(self):
        """WHERE filter on a tag column works for literal children.

        Verify that WHERE predicates on tag columns correctly filter children
        with literal tag values in mixed ref/literal STB queries.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        self._setup_basic(num_src=3, num_vc=6)

        # Clear refs for vc_0 and vc_3
        tdSql.execute("ALTER VTABLE vc_0 SET TAG ref_code = 9999")
        tdSql.execute("ALTER VTABLE vc_3 SET TAG ref_code = 9999")

        self._cache_reset()
        tdSql.query(
            "SELECT name, ref_code FROM vstb WHERE ref_code = 9999 ORDER BY name"
        )
        assert tdSql.queryRows == 2
        assert tdSql.queryResult[0][0] == "name_0"
        assert tdSql.queryResult[1][0] == "name_3"

    def test_mixed_aggregate_stb(self):
        """Aggregate query on STB with mixed ref/literal tags.

        Verify that GROUP BY on tag columns produces correct aggregation
        when children have a mix of tag-ref and literal tag values.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        self._setup_basic(num_src=2, num_vc=4)

        # Clear refs for vc_1 and vc_3
        tdSql.execute("ALTER VTABLE vc_1 SET TAG ref_code = 500")
        tdSql.execute("ALTER VTABLE vc_3 SET TAG ref_code = 500")

        self._cache_reset()
        tdSql.query(
            "SELECT ref_code, COUNT(*) as cnt FROM vstb "
            "GROUP BY ref_code ORDER BY ref_code"
        )
        # ref_code values: src_0=100 (vc_0,vc_2), 500 (vc_1,vc_3), src_1=101 - wait
        # vc_0 -> src_0 (code=100), vc_1 -> literal(500), vc_2 -> src_0 (code=100), vc_3 -> literal(500)
        assert tdSql.queryRows == 2
        results = {r[0]: r[1] for r in tdSql.queryResult}
        assert results.get(100) == 2, f"code=100 count={results.get(100)}"
        assert results.get(500) == 2, f"code=500 count={results.get(500)}"

    def test_all_children_mixed_then_all_literal(self):
        """Mixed first, then convert all to literal, verify STB query.

        Verify that converting all children from tag-ref to literal values
        produces correct STB query results with no residual ref resolution.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        self._setup_basic(num_src=2, num_vc=3)

        # Make vc_0 literal
        tdSql.execute("ALTER VTABLE vc_0 SET TAG ref_city = 'A'")
        tdSql.execute("ALTER VTABLE vc_0 SET TAG ref_code = 1")

        # Mixed query
        self._cache_reset()
        tdSql.query("SELECT name, ref_city, ref_code FROM vstb ORDER BY name")
        assert tdSql.queryRows == 3
        row0 = [r for r in tdSql.queryResult if r[0] == "name_0"][0]
        assert row0[1] == "A"
        assert row0[2] == 1

        # Now convert all to literal
        tdSql.execute("ALTER VTABLE vc_1 SET TAG ref_city = 'B'")
        tdSql.execute("ALTER VTABLE vc_1 SET TAG ref_code = 2")
        tdSql.execute("ALTER VTABLE vc_2 SET TAG ref_city = 'C'")
        tdSql.execute("ALTER VTABLE vc_2 SET TAG ref_code = 3")

        self._cache_reset()
        tdSql.query("SELECT name, ref_city, ref_code FROM vstb ORDER BY name")
        assert tdSql.queryRows == 3
        expected = {"name_0": ("A", 1), "name_1": ("B", 2), "name_2": ("C", 3)}
        for r in tdSql.queryResult:
            exp = expected[r[0]]
            assert r[1] == exp[0], f"{r[0]} city={r[1]} exp={exp[0]}"
            assert r[2] == exp[1], f"{r[0]} code={r[2]} exp={exp[1]}"
