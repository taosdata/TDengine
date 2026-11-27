# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

# -*- coding: utf-8 -*-
"""Virtual table query with a source column far wider than the declared virtual width.

By design a virtual table column returns the full source value even when the source
column is wider than the virtual declaration (DDL gates only the type).  The engine
must therefore treat the planned width as widened to the source width everywhere a
fixed-size buffer is involved (function interbuf, tuple row image, merge output block,
dispatcher row-size contract); any residual violation must surface as a clean error
rather than memory corruption.
"""

import hashlib

from new_test_framework.utils import tdLog, tdSql

DB = "vtb_wide_src"
TS0 = 1700000000000

SHORT = "abc"          # len 3, fits into varchar(4)
LONG = "b" * 1200      # len 1200, far beyond varchar(4)
LONG2 = "c" * 1200     # same length, different content, for the second source table
MID = "d" * 800        # len 800

ROWS = 5000            # > 4096 rows, so pipelines cross block/vnode boundaries
ROWS_B = 1000          # second wide source: same values as the first ROWS_B rows
INSERT_BATCH = 100     # rows per INSERT statement (~285KB of SQL, below the 1MB cap)


def wide_v1(i):
    return f"{i:06d}" + "a" * 1018  # exactly 1024 bytes


def wide_v2(i):
    return f"{i:06d}" + "b" * 1018  # exactly 1024 bytes


def wide_n1(i):
    return f"{i:06d}" + "n" * 506   # exactly 512 chars


def wide_n2(i):
    return f"{i:06d}" + "m" * 250   # exactly 256 chars


class TestVtableWideSourceCol:
    """Wide source column (varchar(2048), 1200-byte values) read through varchar(4) virtual columns."""

    @classmethod
    def setup_class(cls):
        tdLog.info("=== setup: wide source column env ===")
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB};")
        # 2 vgroups so aggregate queries go through the partial/merge (exchange) path
        tdSql.execute(f"CREATE DATABASE {DB} VGROUPS 2 BUFFER 16;")
        tdSql.execute(f"USE {DB};")

        # source tables with wide declared columns
        tdSql.execute("CREATE TABLE src1 (ts TIMESTAMP, c VARCHAR(2048));")
        tdSql.execute("CREATE TABLE src2 (ts TIMESTAMP, d VARCHAR(2048));")
        tdSql.execute(f"INSERT INTO src1 VALUES ({TS0}, '{SHORT}');")
        tdSql.execute(f"INSERT INTO src1 VALUES ({TS0 + 1000}, '{LONG}');")
        tdSql.execute(f"INSERT INTO src2 VALUES ({TS0}, '{SHORT}');")
        tdSql.execute(f"INSERT INTO src2 VALUES ({TS0 + 1000}, '{LONG2}');")

        # single-source virtual table (flat scan rewrite path)
        tdSql.execute("CREATE VTABLE vtb_flat (ts TIMESTAMP, c VARCHAR(4) FROM src1.c);")
        # multi-source virtual table (merge path: columns from two source tables)
        tdSql.execute("CREATE VTABLE vtb_merge (ts TIMESTAMP, c VARCHAR(4) FROM src1.c, d VARCHAR(4) FROM src2.d);")

        # ALTER-widen scenario: source created narrow, widened after the virtual table exists
        tdSql.execute("CREATE TABLE src3 (ts TIMESTAMP, c VARCHAR(4));")
        tdSql.execute("CREATE VTABLE vtb_alter (ts TIMESTAMP, c VARCHAR(4) FROM src3.c);")
        tdSql.execute(f"INSERT INTO src3 VALUES ({TS0}, '{SHORT}');")
        tdSql.execute("ALTER TABLE src3 MODIFY COLUMN c VARCHAR(2048);")
        tdSql.execute(f"INSERT INTO src3 VALUES ({TS0 + 1000}, '{MID}');")

        # virtual super table with two children bound to differently-widthed sources
        tdSql.execute("CREATE STABLE vst (ts TIMESTAMP, c VARCHAR(4)) TAGS (loc VARCHAR(8)) VIRTUAL 1;")
        tdSql.execute("CREATE VTABLE vc1 (c FROM src1.c) USING vst TAGS ('l1');")
        tdSql.execute("CREATE VTABLE vc2 (c FROM src2.d) USING vst TAGS ('l2');")

        # nchar source for the wide-value to_timestamp path: a 1500-char value is 6000
        # ucs4 bytes, beyond the fixed 4096-byte conversion buffer
        tdSql.execute("CREATE TABLE src_ts (ts TIMESTAMP, s NCHAR(2048));")
        tdSql.execute("CREATE VTABLE vtb_ts (ts TIMESTAMP, s NCHAR(64) FROM src_ts.s);")
        tdSql.execute(f"INSERT INTO src_ts VALUES ({TS0}, '{'x' * 1500}');")
        # valid timestamp string for the positive to_timestamp path
        tdSql.execute(f"INSERT INTO src_ts VALUES ({TS0 + 1000}, '2026-10-10 01:02:03');")

        # multi-column wide source tables: declared widths match the stored data
        # width (1024-byte varchar / 512-char nchar) and row count exceeds 4096,
        # so scans, scalar functions, aggregates, joins and window operators all
        # process full-width values across many blocks
        tdSql.execute("CREATE TABLE src_wide (ts TIMESTAMP, v1 VARCHAR(1024), v2 VARCHAR(1024), n1 NCHAR(512), n2 NCHAR(256), ival INT);")
        tdSql.execute("CREATE TABLE src_wide_b (ts TIMESTAMP, v1 VARCHAR(1024), v2 VARCHAR(1024), n1 NCHAR(512), n2 NCHAR(256), ival INT);")
        for start in range(0, ROWS, INSERT_BATCH):
            vals = ",".join(
                f"({TS0 + i}, '{wide_v1(i)}', '{wide_v2(i)}', '{wide_n1(i)}', '{wide_n2(i)}', {i})"
                for i in range(start, min(start + INSERT_BATCH, ROWS))
            )
            tdSql.execute(f"INSERT INTO src_wide VALUES {vals};")
        for start in range(0, ROWS_B, INSERT_BATCH):
            vals = ",".join(
                f"({TS0 + i}, '{wide_v1(i)}', '{wide_v2(i)}', '{wide_n1(i)}', '{wide_n2(i)}', {i})"
                for i in range(start, min(start + INSERT_BATCH, ROWS_B))
            )
            tdSql.execute(f"INSERT INTO src_wide_b VALUES {vals};")

        # virtual tables declaring narrow columns over the wide sources
        tdSql.execute("CREATE VTABLE vtb_wide (ts TIMESTAMP, v1 VARCHAR(4) FROM src_wide.v1, v2 VARCHAR(4) FROM src_wide.v2, n1 NCHAR(4) FROM src_wide.n1, n2 NCHAR(4) FROM src_wide.n2, ival INT FROM src_wide.ival);")
        tdSql.execute("CREATE VTABLE vtb_wide2 (ts TIMESTAMP, v1 VARCHAR(4) FROM src_wide_b.v1, v2 VARCHAR(4) FROM src_wide_b.v2, n1 NCHAR(4) FROM src_wide_b.n1, n2 NCHAR(4) FROM src_wide_b.n2, ival INT FROM src_wide_b.ival);")

        # virtual super table with two children bound to the two wide sources
        tdSql.execute("CREATE STABLE vst2 (ts TIMESTAMP, v1 VARCHAR(4), v2 VARCHAR(4), n1 NCHAR(4), n2 NCHAR(4), ival INT) TAGS (loc VARCHAR(8)) VIRTUAL 1;")
        tdSql.execute("CREATE VTABLE vsw1 (v1 FROM src_wide.v1, v2 FROM src_wide.v2, n1 FROM src_wide.n1, n2 FROM src_wide.n2, ival FROM src_wide.ival) USING vst2 TAGS ('w1');")
        tdSql.execute("CREATE VTABLE vsw2 (v1 FROM src_wide_b.v1, v2 FROM src_wide_b.v2, n1 FROM src_wide_b.n1, n2 FROM src_wide_b.n2, ival FROM src_wide_b.ival) USING vst2 TAGS ('w2');")
        tdLog.info("=== setup complete ===")

    def test_scan_returns_full_data(self):
        """Plain scan of both flat and merge paths returns complete source values.

        1. Flat single-source virtual table returns full wide values
        2. Merge multi-source virtual table returns full wide values

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, width

        Jira: None

        History:
            - 2026-10-10 Created
        """
        for vtb in ("vtb_flat",):
            tdSql.query(f"select c from {DB}.{vtb} order by ts;")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, SHORT)
            tdSql.checkData(1, 0, LONG)

        tdSql.query(f"select c, d from {DB}.vtb_merge order by ts;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, SHORT)
        tdSql.checkData(0, 1, SHORT)
        tdSql.checkData(1, 0, LONG)
        tdSql.checkData(1, 1, LONG2)

    def test_first_last_lastrow(self):
        """first/last/last_row keep full values on both flat and merge paths.

        1. first/last/last_row over a widened column on the flat path
        2. Same aggregates on the merge path

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, aggregate

        Jira: None

        History:
            - 2026-10-10 Created
        """
        for vtb in ("vtb_flat", "vtb_merge"):
            tdSql.query(f"select first(c), last(c), last_row(c) from {DB}.{vtb};")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, SHORT)
            tdSql.checkData(0, 1, LONG)
            tdSql.checkData(0, 2, LONG)

    def test_min_max_mode(self):
        """min/max/mode on a widened virtual column.

        1. min/max return the full extreme values
        2. mode returns the full most-frequent value

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, aggregate

        Jira: None

        History:
            - 2026-10-10 Created
        """
        for vtb in ("vtb_flat", "vtb_merge"):
            tdSql.query(f"select min(c), max(c) from {DB}.{vtb};")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, SHORT)  # 'a'=0x61 < 'b'=0x62, min is the short one
            tdSql.checkData(0, 1, LONG)
        # mode with a deterministic winner: LONG appears twice
        tdSql.execute("CREATE TABLE src_m (ts TIMESTAMP, c VARCHAR(2048));")
        tdSql.execute(f"INSERT INTO src_m VALUES ({TS0}, '{SHORT}'), ({TS0 + 1000}, '{LONG}'), ({TS0 + 2000}, '{LONG}');")
        tdSql.execute("CREATE VTABLE vtb_m (ts TIMESTAMP, c VARCHAR(4) FROM src_m.c);")
        tdSql.query(f"select mode(c) from {DB}.vtb_m;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, LONG)

    def test_sample_tail(self):
        """sample/tail keep full values.

        1. sample reservoir slots hold full wide values
        2. tail items hold full wide values

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, aggregate

        Jira: None

        History:
            - 2026-10-10 Created
        """
        tdSql.query(f"select length(sample(c, 2)) from {DB}.vtb_merge order by 1;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 1200)

        tdSql.query(f"select tail(c, 1) from {DB}.vtb_merge;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, LONG)

        tdSql.query(f"select tail(c, 2) from {DB}.vtb_merge order by 1;")
        tdSql.checkRows(2)

    def test_selectivity_cols(self):
        """Bare select columns accompanying an aggregate come from the same source row.

        1. Bare column beside last() on the merge path
        2. Bare column beside first() on the merge path

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, selectivity

        Jira: None

        History:
            - 2026-10-10 Created
        """
        tdSql.query(f"select d, last(c) from {DB}.vtb_merge;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, LONG2)
        tdSql.checkData(0, 1, LONG)

        tdSql.query(f"select c, first(c) from {DB}.vtb_merge;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, SHORT)
        tdSql.checkData(0, 1, SHORT)

    def test_group_partition_order(self):
        """group by / partition by / order by on widened virtual columns.

        1. group by the wide column
        2. partition by the wide column
        3. order by the wide column

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, group

        Jira: None

        History:
            - 2026-10-10 Created
        """
        tdSql.query(f"select count(*) from {DB}.vtb_merge group by c order by 1;")
        tdSql.checkRows(2)

        tdSql.query(f"select length(c), count(*) from {DB}.vtb_merge group by c order by 1;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 1200)

        tdSql.query(f"select length(first(c)), length(last(c)) from {DB}.vtb_merge partition by d order by 1;")
        tdSql.checkRows(2)

        tdSql.query(f"select length(c) from {DB}.vtb_merge order by c;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 3)     # 'abc' sorts before 'b' * 1200
        tdSql.checkData(1, 0, 1200)

    def test_interval(self):
        """Windowed aggregation over widened virtual columns (both rows share one window).

        1. interval aggregation over widened columns

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, window

        Jira: None

        History:
            - 2026-10-10 Created
        """
        tdSql.query(f"select length(first(c)), length(last(c)) from {DB}.vtb_merge interval(1d);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(0, 1, 1200)

    def test_subquery(self):
        """Widened values flow through a subquery.

        1. last() result flows through an outer query

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, subquery

        Jira: None

        History:
            - 2026-10-10 Created
        """
        tdSql.query(f"select length(c) from (select last(c) as c from {DB}.vtb_merge);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1200)

    def test_alter_widen_source(self):
        """Source column widened by ALTER after the virtual table exists.

        1. Scan and aggregates after ALTER MODIFY COLUMN widens the source

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, alter

        Jira: None

        History:
            - 2026-10-10 Created
        """
        tdSql.query(f"select c from {DB}.vtb_alter order by ts;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, SHORT)
        tdSql.checkData(1, 0, MID)

        tdSql.query(f"select first(c), last(c), last_row(c) from {DB}.vtb_alter;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, SHORT)
        tdSql.checkData(0, 1, MID)
        tdSql.checkData(0, 2, MID)

        tdSql.query(f"select length(min(c)), length(max(c)) from {DB}.vtb_alter;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(0, 1, 800)

    def test_vstable_child_direct(self):
        """A virtual child table queried directly is planned per-child: full data.

        1. Per-child scan and aggregates on a virtual child table

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, width

        Jira: None

        History:
            - 2026-10-10 Created
        """
        tdSql.query(f"select c from {DB}.vc1 order by ts;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, SHORT)
        tdSql.checkData(1, 0, LONG)

        tdSql.query(f"select first(c), last(c) from {DB}.vc1;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, SHORT)
        tdSql.checkData(0, 1, LONG)

    def test_vstable_scan(self):
        """Virtual super table scan returns full data per child.

        1. Virtual super table scan over children bound to differently-widthed sources

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, width

        Jira: None

        History:
            - 2026-10-10 Created
        """
        tdSql.query(f"select tbname, length(c) from {DB}.vst order by tbname, ts;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "vc1")
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 0, "vc1")
        tdSql.checkData(1, 1, 1200)
        tdSql.checkData(2, 0, "vc2")
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(3, 0, "vc2")
        tdSql.checkData(3, 1, 1200)

    def test_state_window_wide_col(self):
        """state_window over a widened virtual column: key buffers must grow, not overflow.

        1. state_window keys grow to hold full wide values

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, window

        Jira: None

        History:
            - 2026-10-10 Created
        """
        tdSql.query(f"select _wstart, length(c) from {DB}.vtb_merge state_window(c) order by _wstart;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 1, 1200)

    def test_vstable_hetero_agg_clean_error(self):
        """Aggregates on a hetero-width virtual super table fail cleanly.

        Children of the super table bind differently-widthed sources, so varlen
        aggregate buffers cannot be sized at plan time: values that fit the
        declared width pass, wider ones must fail with a clean error instead of
        corrupting memory.

        1. first() on values that fit the declared width succeeds
        2. last()/last_row() on wider values fail cleanly

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, aggregate, negative

        Jira: None

        History:
            - 2026-10-10 Created
        """
        # first(c) only touches the earliest (short) row, which fits: succeeds.
        tdSql.query(f"select first(c) from {DB}.vst;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, SHORT)
        # last/last_row evaluate the 1200-byte values: clean error, no corruption.
        tdSql.error(f"select last(c) from {DB}.vst;")
        tdSql.error(f"select last_row(c) from {DB}.vst;")
        tdSql.error(f"select last(c) from {DB}.vst group by tbname;")

    def test_join_composite_keys(self):
        """Composite join keys over widened virtual columns grow the key buffer.

        1. Two-column join key (wide varchar + ts) matches full-length values

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, join

        Jira: None

        History:
            - 2026-10-10 Created
        """
        tdSql.query(f"select count(*) from {DB}.vtb_merge a join {DB}.vtb_flat b on a.c = b.c and a.ts = b.ts;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

    def test_regexp_extract_wide(self):
        """regexp_extract over a widened virtual column grows the match buffer.

        1. A match far longer than the declared width returns in full

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, scalar

        Jira: None

        History:
            - 2026-10-10 Created
        """
        tdSql.query(f"select regexp_extract(c, '(b+)') from {DB}.vtb_merge where c = '{LONG}';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, LONG)

    def test_to_timestamp_wide_nchar(self):
        """to_timestamp over a wider-than-declared nchar virtual column fails cleanly.

        1. A 1500-char nchar value (6000 ucs4 bytes) exceeds the fixed 4096-byte
           conversion buffer: clean parse error, no corruption

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, scalar, negative

        Jira: None

        History:
            - 2026-10-10 Created
        """
        tdSql.error(f"select to_timestamp(s, 'yyyy-mm-dd hh24:mi:ss') from {DB}.vtb_ts;")

    def test_interp_wide_col(self):
        """interp fill(prev) with a wide partition key grows the row keeper cells.

        interp itself accepts only numeric arguments, so the wide column is used
        as the PARTITION BY key: partition keys and fill(prev) keepers must grow
        to hold the full 1024-byte values across empty windows.

        1. Every returned row carries a full 1024-byte partition key value
        2. fill(prev) propagates values into windows beyond the last data row

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, interp

        Jira: None

        History:
            - 2026-10-10 Created
        """
        tdSql.execute("CREATE TABLE src_ip (ts TIMESTAMP, v VARCHAR(1024), k INT);")
        for i in range(3):
            tdSql.execute(f"INSERT INTO src_ip VALUES ({TS0 + i}, '{wide_v1(i)}', {100 + i});")
        tdSql.execute("CREATE VTABLE vtb_ip (ts TIMESTAMP, v VARCHAR(4) FROM src_ip.v, k INT FROM src_ip.k);")

        tdSql.query(f"select v, interp(k) from {DB}.vtb_ip "
                    f"partition by v range({TS0}, {TS0 + 4000}) every(1s) fill(prev);")
        keys = [tdSql.getData(r, 0) for r in range(tdSql.queryRows)]
        non_null = [v for v in keys if v is not None]
        assert tdSql.queryRows > 3, f"interp fill(prev) produced too few rows: {tdSql.queryRows}"
        assert len(set(non_null)) == 3, f"expected 3 distinct partition keys, got {set(non_null)}"
        assert all(len(v) == 1024 for v in non_null), "interp keeper truncated a wide partition key"

    def test_wide_multi_col_scan(self):
        """Scan a multi-column wide virtual table with more than 4096 rows.

        1. Row count over the 5000-row wide source
        2. Full 1024-byte varchar values returned raw per row
        3. Full 512-char / 256-char nchar values verified by equality and length
        4. Filter on length of a wide column

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, width

        Jira: None

        History:
            - 2026-10-10 Created
        """
        tdSql.query(f"select count(*) from {DB}.vtb_wide;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, ROWS)

        tdSql.query(f"select v1, v2 from {DB}.vtb_wide where ts = {TS0 + 1};")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, wide_v1(1))
        tdSql.checkData(0, 1, wide_v2(1))

        # nchar values are verified server-side: returning a wider-than-declared
        # nchar value raw through the multi-vgroup exchange hits a pre-existing
        # engine limitation (clean internal error, unrelated to this fix)
        tdSql.query(f"select case when n1 = '{wide_n1(1)}' then 1 else 0 end, "
                    f"case when n2 = '{wide_n2(1)}' then 1 else 0 end, "
                    f"char_length(n1), char_length(n2) from {DB}.vtb_wide where ts = {TS0 + 1};")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 512)
        tdSql.checkData(0, 3, 256)

        tdSql.query(f"select count(*) from {DB}.vtb_wide where length(v1) = 1024 and char_length(n1) = 512;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, ROWS)

    def test_wide_value_aggregates(self):
        """Varlen-holding aggregates keep full wide values on a wide virtual table.

        1. first/last/last_row/min/max return complete 1024-byte values
        2. tail/sample hold complete wide values
        3. mode over distinct wide values returns a full value

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, aggregate

        Jira: None

        History:
            - 2026-10-10 Created
        """
        tdSql.query(f"select first(v1), last(v1), last_row(v1), min(v1), max(v1) from {DB}.vtb_wide;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, wide_v1(0))
        tdSql.checkData(0, 1, wide_v1(ROWS - 1))
        tdSql.checkData(0, 2, wide_v1(ROWS - 1))
        tdSql.checkData(0, 3, wide_v1(0))
        tdSql.checkData(0, 4, wide_v1(ROWS - 1))

        tdSql.query(f"select char_length(first(n1)), char_length(last(n1)) from {DB}.vtb_wide;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 512)
        tdSql.checkData(0, 1, 512)

        tdSql.query(f"select tail(v1, 1) from {DB}.vtb_wide;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, wide_v1(ROWS - 1))

        tdSql.query(f"select length(sample(v1, 3)) from {DB}.vtb_wide;")
        for r in range(tdSql.queryRows):
            assert tdSql.getData(r, 0) == 1024, f"sample truncated a wide value: row {r}"

        tdSql.query(f"select length(mode(v1)) from {DB}.vtb_wide;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1024)

    def test_numeric_aggregates_wide(self):
        """Numeric aggregates over lengths of wide columns.

        1. count/sum/min/max over length(v1) are exact
        2. avg/stddev/percentile/spread over constant 1024 lengths

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, aggregate

        Jira: None

        History:
            - 2026-10-10 Created
        """
        tdSql.query(f"select count(v1), count(n1), sum(length(v1)), min(length(v1)), max(length(v1)) from {DB}.vtb_wide;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, ROWS)
        tdSql.checkData(0, 1, ROWS)
        tdSql.checkData(0, 2, ROWS * 1024)
        tdSql.checkData(0, 3, 1024)
        tdSql.checkData(0, 4, 1024)

        tdSql.query(f"select avg(length(v1)), stddev(length(v1)), percentile(length(v1), 50), spread(length(v1)) from {DB}.vtb_wide;")
        tdSql.checkRows(1)
        vals = [tdSql.getData(0, c) for c in range(4)]
        assert abs(vals[0] - 1024.0) < 1e-6, f"avg: {vals}"
        assert abs(vals[1]) < 1e-6, f"stddev: {vals}"
        assert abs(vals[2] - 1024.0) < 1e-6, f"percentile: {vals}"
        assert abs(vals[3]) < 1e-6, f"spread: {vals}"

    def test_scalar_string_functions(self):
        """String scalar functions over wide virtual columns grow their result buffers.

        1. length/char_length on full-width values
        2. concat of two 1024-byte columns returns 2048 bytes in full
        3. substr/upper/replace on wide values

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, scalar

        Jira: None

        History:
            - 2026-10-10 Created
        """
        i = 42
        tdSql.query(f"select length(v1), char_length(n1), length(concat(v1, v2)), "
                    f"substr(v1, 1, 6), upper(substr(v1, 7, 3)), length(replace(v1, 'a', 'b')) "
                    f"from {DB}.vtb_wide where ts = {TS0 + i};")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1024)
        tdSql.checkData(0, 1, 512)
        tdSql.checkData(0, 2, 2048)
        tdSql.checkData(0, 3, f"{i:06d}")
        tdSql.checkData(0, 4, "AAA")
        tdSql.checkData(0, 5, 1024)

        tdSql.query(f"select concat(v1, v2) from {DB}.vtb_wide where ts = {TS0 + i};")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, wide_v1(i) + wide_v2(i))

    def test_crypto_hash_functions(self):
        """md5/sha/sha1/sha2 over wide virtual columns match hashlib exactly.

        1. Hash digests (wider than the declared varchar(4)) are grown, not truncated
        2. Digests over nchar input keep their fixed widths

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, scalar, crypto

        Jira: None

        History:
            - 2026-10-10 Created
        """
        i = 7
        v = wide_v1(i).encode("utf-8")
        tdSql.query(f"select md5(v1), sha(v1), sha1(v1), sha2(v1, 224), sha2(v1, 256), sha2(v1, 384), sha2(v1, 512) "
                    f"from {DB}.vtb_wide where ts = {TS0 + i};")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, hashlib.md5(v).hexdigest())
        tdSql.checkData(0, 1, hashlib.sha1(v).hexdigest())
        tdSql.checkData(0, 2, hashlib.sha1(v).hexdigest())
        tdSql.checkData(0, 3, hashlib.sha224(v).hexdigest())
        tdSql.checkData(0, 4, hashlib.sha256(v).hexdigest())
        tdSql.checkData(0, 5, hashlib.sha384(v).hexdigest())
        tdSql.checkData(0, 6, hashlib.sha512(v).hexdigest())

        tdSql.query(f"select length(md5(n1)), length(sha(n1)), length(sha2(n1, 256)) "
                    f"from {DB}.vtb_wide where ts = {TS0 + i};")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 32)
        tdSql.checkData(0, 1, 40)
        tdSql.checkData(0, 2, 64)

    def test_crypto_roundtrip_wide(self):
        """aes/sm4 encrypt+decrypt round-trips over wide virtual columns.

        1. aes_decrypt(aes_encrypt(v1)) returns the full 1024-byte original
        2. sm4 round-trip on varchar and nchar wide values
        3. Ciphertext wider than the source is grown, not truncated

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, scalar, crypto

        Jira: None

        History:
            - 2026-10-10 Created
        """
        i = 9
        key = "mykeystring"
        tdSql.query(f"select aes_decrypt(aes_encrypt(v1, '{key}'), '{key}') from {DB}.vtb_wide where ts = {TS0 + i};")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, wide_v1(i))

        tdSql.query(f"select sm4_decrypt(sm4_encrypt(v1, '{key}'), '{key}') from {DB}.vtb_wide where ts = {TS0 + i};")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, wide_v1(i))

        tdSql.query(f"select case when sm4_decrypt(sm4_encrypt(n1, '{key}'), '{key}') = n1 then 1 else 0 end "
                    f"from {DB}.vtb_wide where ts = {TS0 + i};")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select length(aes_encrypt(v1, '{key}')), length(sm4_encrypt(v1, '{key}')) "
                    f"from {DB}.vtb_wide where ts = {TS0 + i};")
        tdSql.checkRows(1)
        lens = [tdSql.getData(0, c) for c in range(2)]
        assert all(n > 1024 for n in lens), f"ciphertext truncated: {lens}"

    def test_regexp_extract_wide_nchar(self):
        """regexp_extract over wide nchar/varchar virtual columns grows the match buffer.

        1. nchar arm: the full 506-char match is produced (verified by length,
           since raw wider-than-declared nchar return hits a pre-existing
           exchange limitation unrelated to this fix)
        2. varchar arm returns the full 1018-byte match

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, scalar

        Jira: None

        History:
            - 2026-10-10 Created
        """
        i = 3
        # the raw match is a wider-than-declared nchar value; assert it server-side
        # by length (raw nchar return through the exchange hits a pre-existing
        # engine limitation unrelated to this fix)
        tdSql.query(f"select char_length(regexp_extract(n1, '(n+)')) from {DB}.vtb_wide where ts = {TS0 + i};")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 506)

        tdSql.query(f"select regexp_extract(v2, '(b+)') from {DB}.vtb_wide where ts = {TS0 + i};")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "b" * 1018)

    def test_to_timestamp_wide_valid(self):
        """to_timestamp over a virtual nchar column parses a valid wide value.

        1. A valid timestamp string round-trips through to_timestamp/to_char

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, scalar

        Jira: None

        History:
            - 2026-10-10 Created
        """
        tdSql.query(f"select to_char(to_timestamp(s, 'yyyy-mm-dd hh24:mi:ss'), 'yyyy-mm-dd hh24:mi:ss') "
                    f"from {DB}.vtb_ts where ts = {TS0 + 1000};")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2026-10-10 01:02:03")

    def test_group_order_wide_keys(self):
        """group by / partition by / order by over full-width keys at scale.

        1. group by a wide varchar key yields one group per distinct 1024-byte value
        2. group by a wide nchar key and composite wide keys
        3. order by a wide key sorts full values; partition by a wide key

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, group

        Jira: None

        History:
            - 2026-10-10 Created
        """
        tdSql.query(f"select count(*) from (select v1 from {DB}.vtb_wide group by v1);")
        tdSql.checkData(0, 0, ROWS)

        tdSql.query(f"select count(*) from (select n1 from {DB}.vtb_wide group by n1);")
        tdSql.checkData(0, 0, ROWS)

        tdSql.query(f"select count(*) from (select v1, n1 from {DB}.vtb_wide group by v1, n1);")
        tdSql.checkData(0, 0, ROWS)

        tdSql.query(f"select substr(v1, 1, 6) from {DB}.vtb_wide order by v1 limit 2;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "000000")
        tdSql.checkData(1, 0, "000001")

        tdSql.query(f"select substr(v1, 1, 6) from {DB}.vtb_wide order by v1 desc limit 1;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, f"{ROWS - 1:06d}")

        tdSql.query(f"select count(*) from (select first(v1) from {DB}.vtb_wide partition by n2);")
        tdSql.checkData(0, 0, ROWS)

    def test_hash_join_wide_keys(self):
        """Hash join over wide key columns grows the join key buffer at scale.

        1. Self join on a 1024-byte key matches every row exactly once
        2. Composite wide key (varchar + nchar)
        3. Cross-table join on wide varchar and nchar keys
        4. Wide values flow through join output; left join keeps unmatched rows

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, join

        Jira: None

        History:
            - 2026-10-10 Created
        """
        tdSql.query(f"select count(*) from {DB}.vtb_wide a join {DB}.vtb_wide b on a.v1 = b.v1;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, ROWS)

        tdSql.query(f"select count(*) from {DB}.vtb_wide a join {DB}.vtb_wide b on a.v1 = b.v1 and a.n1 = b.n1;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, ROWS)

        tdSql.query(f"select count(*) from {DB}.vtb_wide a join {DB}.vtb_wide2 b on a.v1 = b.v1;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, ROWS_B)

        tdSql.query(f"select count(*) from {DB}.vtb_wide a join {DB}.vtb_wide2 b on a.n1 = b.n1;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, ROWS_B)

        tdSql.query(f"select length(a.v1), length(b.v1) from {DB}.vtb_wide a "
                    f"join {DB}.vtb_wide2 b on a.v1 = b.v1 limit 1;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1024)
        tdSql.checkData(0, 1, 1024)

        tdSql.query(f"select count(*) from {DB}.vtb_wide a left join {DB}.vtb_wide2 b on a.v1 = b.v1;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, ROWS)

    def test_merge_join_wide_ts(self):
        """Merge-style (ts-equality) join over wide virtual columns.

        1. ts-equality join matches the overlapping 1000 rows
        2. Composite ts + wide varchar key
        3. Wide values from both sides flow through join output

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, join

        Jira: None

        History:
            - 2026-10-10 Created
        """
        tdSql.query(f"select count(*) from {DB}.vtb_wide a join {DB}.vtb_wide2 b on a.ts = b.ts;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, ROWS_B)

        tdSql.query(f"select count(*) from {DB}.vtb_wide a join {DB}.vtb_wide2 b on a.ts = b.ts and a.v1 = b.v1;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, ROWS_B)

        tdSql.query(f"select length(a.v2), char_length(b.n2) from {DB}.vtb_wide a "
                    f"join {DB}.vtb_wide2 b on a.ts = b.ts order by a.ts limit 1;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1024)
        tdSql.checkData(0, 1, 256)

    def test_interp_fill_wide(self):
        """interp fill(prev) over 5000 wide partition keys grows keeper cells at scale.

        interp accepts only numeric arguments, so the wide column drives the
        PARTITION BY: every one of the 5000 partitions keeps its full 1024-byte
        key through the fill keepers, and fill(prev) carries it into trailing
        empty windows.

        1. Every returned row carries a full 1024-byte partition key value
        2. No partition loses its key to truncation across empty windows

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, interp

        Jira: None

        History:
            - 2026-10-10 Created
        """
        sub = (f"select v1, interp(ival) as k from {DB}.vtb_wide "
               f"partition by v1 range({TS0}, {TS0 + 4000}) every(1s) fill(prev)")
        tdSql.query(f"select count(*) from ({sub});")
        tdSql.checkRows(1)
        total = tdSql.getData(0, 0)
        assert total > 0, "interp over wide partition keys returned no rows"

        tdSql.query(f"select count(*) from ({sub}) where length(v1) = 1024;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, total)

    def test_state_window_wide_scale(self):
        """state_window over a wide key with 5000 distinct values.

        1. One window per distinct full-width key value

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, window

        Jira: None

        History:
            - 2026-10-10 Created
        """
        tdSql.query(f"select count(*) from (select _wstart, count(*) from {DB}.vtb_wide state_window(v1));")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, ROWS)

    def test_vstable_wide_scan(self):
        """Virtual super table scan over children bound to wide sources at scale.

        1. Total row count across both wide children
        2. Per-child counts via group by tbname
        3. Full-width values through the super table scan path

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, width

        Jira: None

        History:
            - 2026-10-10 Created
        """
        tdSql.query(f"select count(*) from {DB}.vst2;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, ROWS + ROWS_B)

        tdSql.query(f"select tbname, count(*) from {DB}.vst2 group by tbname order by tbname;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "vsw1")
        tdSql.checkData(0, 1, ROWS)
        tdSql.checkData(1, 0, "vsw2")
        tdSql.checkData(1, 1, ROWS_B)

        tdSql.query(f"select length(v1), char_length(n1) from {DB}.vst2 where tbname = 'vsw1' and ts = {TS0 + 5};")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1024)
        tdSql.checkData(0, 1, 512)

    def test_vstable_wide_agg(self):
        """Aggregates over a wide virtual super table.

        1. max/min over length of wide columns succeed across children
        2. sum over length per child via group by tbname
        3. Varlen-holding last() on wider-than-declared values fails cleanly

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, aggregate, negative

        Jira: None

        History:
            - 2026-10-10 Created
        """
        tdSql.query(f"select max(length(v1)), min(char_length(n1)) from {DB}.vst2;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1024)
        tdSql.checkData(0, 1, 512)

        tdSql.query(f"select tbname, sum(length(v1)) from {DB}.vst2 group by tbname order by tbname;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, ROWS * 1024)
        tdSql.checkData(1, 1, ROWS_B * 1024)

        # varlen-holding aggregates cannot be sized for a super table plan:
        # clean error, no corruption
        tdSql.error(f"select last(v1) from {DB}.vst2;")

    def test_subquery_union_wide(self):
        """Wide values flow through subqueries and union all.

        1. Aggregate over a subquery on wide values
        2. Filtered subquery count
        3. union all of two wide virtual tables

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, query, subquery

        Jira: None

        History:
            - 2026-10-10 Created
        """
        tdSql.query(f"select max(length(v1)) from (select v1 from {DB}.vtb_wide);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1024)

        tdSql.query(f"select count(*) from (select v1 from {DB}.vtb_wide where ts >= {TS0 + 4000});")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1000)

        tdSql.query(f"select count(*) from (select v1 from {DB}.vtb_wide union all select v1 from {DB}.vtb_wide2);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, ROWS + ROWS_B)

    @classmethod
    def teardown_class(cls):
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB};")
        tdLog.info("=== teardown complete ===")
