# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
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

from new_test_framework.utils import tdSql, tdLog, tdStream, StreamItem
from new_test_framework.utils.eutil import findTaosdLog

class TestStreamTagCache:
    def setup_class(cls):
        tdLog.info(f"start to execute {__file__}")

    def test_out_table(self):
        """summary: test out table schema change

        description:
            - check_all_types_basic:
                test tag cache with all data types in basic scenario
            - check_all_types_alter:
                test tag cache with all data types in alter table tag scenario
            - check_null_tag_value:
                test tag cache with null tag values
            - check_create_drop_ctable:
                test tag cache with create and drop child table operations
            - check fallback_to_normal_cache:
                test stable tag filter cache fallback to normal tag filter cache
            - todo(Tony Zhang): check virtual super table

        Since: v3.3.8.8

        Labels: outTable,schemaChange,stream,integration,functional

        Catalog:
            - StreamProcessing:Others

        History:
            - 2025-12-23 Pengrk: created this test

        """
        tdStream.createSnode()
        self.check_out_table_schema_change()

    def test_out_table_partial_columns_tags(self):
        """summary: test partial column and tag mapping into pre-created output stable

        description:
            - check_partial_columns:
                pre-create output stable, stream writes only specified columns
            - check_partial_tags:
                pre-create output stable, stream writes only specified tags
            - check_partial_columns_and_tags:
                pre-create output stable, stream writes partial columns and tags

        Since: v3.3.8.8

        Labels: outTable,partialColumns,partialTags,stream,integration,functional

        Catalog:
            - StreamProcessing:Others

        History:
            - 2026-07-03 Pengrk: created this test

        """
        tdStream.ensureSnode()
        self.check_partial_columns()
        self.check_partial_tags()
        self.check_partial_columns_and_tags()

    def test_out_table_partial_columns_after_alter(self):
        """summary: test partial column write after drop/add column on pre-created output stable

        description:
            - check_partial_columns_after_alter:
                pre-create output stable, alter drop one column and add one column,
                then stream writes only the newly added column partially

        Since: v3.3.8.8

        Labels: outTable,partialColumns,schemaChange,stream,integration,functional

        Catalog:
            - StreamProcessing:Others

        History:
            - 2026-07-03 Pengrk: created this test

        """
        tdStream.ensureSnode()
        self.check_partial_columns_after_alter()

    def test_out_table_partial_tags_after_alter(self):
        """summary: test partial tag write after drop/add tag on pre-created output stable

        description:
            - check_partial_tags_after_alter:
                pre-create output stable, alter drop one tag and add one tag,
                then stream writes only the newly added tag partially

        Since: v3.3.8.8

        Labels: outTable,partialTags,schemaChange,stream,integration,functional

        Catalog:
            - StreamProcessing:Others

        History:
            - 2026-07-03 Pengrk: created this test

        """
        tdStream.ensureSnode()
        self.check_partial_tags_after_alter()

    def test_out_table_into_column_order(self):
        """summary: reject out-of-order or duplicate INTO column lists on pre-created output stable

        description:
            - check_out_table_invalid_column_order:
                CREATE STREAM must fail when INTO columns are not in ascending schema colId order,
                including partial reverse order, full reverse order, and duplicate column names
            - check_out_table_invalid_first_column:
                CREATE STREAM must fail when the first INTO column is not the primary timestamp column

        Since: v3.3.8.8

        Labels: outTable,partialColumns,stream,integration,functional

        Catalog:
            - StreamProcessing:Others

        History:
            - 2026-07-14 Pengrk: created this test

        """
        tdStream.ensureSnode()
        self.check_out_table_invalid_column_order()
        self.check_out_table_invalid_first_column()

    def test_out_normal_table_partial_columns(self):
        """summary: test partial column mapping into pre-created output normal table

        description:
            - check_normal_table_partial_columns:
                pre-create output normal table, stream writes only specified columns

        Since: v3.3.8.8

        Labels: outTable,partialColumns,normalTable,stream,integration,functional

        Catalog:
            - StreamProcessing:Others

        History:
            - 2026-07-14 Pengrk: created this test

        """
        tdStream.ensureSnode()
        self.check_normal_table_partial_columns()

    def test_out_normal_table_partial_columns_after_alter(self):
        """summary: test partial column write after drop/add column on pre-created output normal table

        description:
            - check_normal_table_partial_columns_after_alter:
                pre-create output normal table, alter drop one column and add one column,
                then stream writes only the newly added column partially

        Since: v3.3.8.8

        Labels: outTable,partialColumns,normalTable,schemaChange,stream,integration,functional

        Catalog:
            - StreamProcessing:Others

        History:
            - 2026-07-14 Pengrk: created this test

        """
        tdStream.ensureSnode()
        self.check_normal_table_partial_columns_after_alter()

    def check_partial_columns(self):
        db_name = "test_partial_cols"
        tdSql.execute(f"drop database if exists {db_name}", show=1)
        tdSql.execute(f"create database {db_name} vgroups 1 keep 3650", show=1)
        tdSql.execute(f"use {db_name}")
        tdSql.execute(
            "create stable stb_src (ts timestamp, c0 int, c1 int, c2 int, c3 int) "
            "tags (gid1 int, gid2 int, gid3 int);",
            show=1,
        )
        tdSql.execute(
            "create stable out_partial_cols (ts timestamp, c1 int, c2 int, c3 int) tags (gid1 int, gid2 int, gid3 int);",
            show=1,
        )
        sql = """create stream s_partial_cols interval(1s) sliding (1s)
            from stb_src partition by tbname, gid1, gid2, gid3
            into out_partial_cols (ts, c2) tags (gid1 int as gid1, gid2 int as gid2, gid3 int as gid3)
            as select _twstart ts, last(c2) c2 from %%tbname
            where _c0 >= _twstart and _c0 < _twend group by gid1, gid2, gid3;"""
        tdSql.execute(sql)
        tdStream.checkStreamStatus("s_partial_cols")

        tdSql.execute("create table ct1 using stb_src tags (1, 2, 3);", show=1)
        tdSql.execute(
            "insert into ct1 (ts, c1, c2, c3) values "
            "('2025-01-01 00:00:00', 10, 100, 1000), "
            "('2025-01-01 00:00:01', 11, 101, 1001),"
            "('2025-01-01 00:00:02', 12, 102, 1002),"
            "('2025-01-01 00:00:03', 13, 103, 1003),"
            "('2025-01-01 00:00:04', 14, 104, 1004);",
            show=1,
        )

        tdSql.checkResultsByFunc(
            sql=f"select gid1, gid2, gid3, ts, c1, c2, c3 from {db_name}.out_partial_cols",
            func=lambda: tdSql.getRows() == 4
            and tdSql.compareData(0, 0, 1)
            and tdSql.compareData(0, 1, 2)
            and tdSql.compareData(0, 2, 3)
            and tdSql.compareData(0, 3, "2025-01-01 00:00:00")
            and tdSql.compareData(0, 4, None)
            and tdSql.compareData(0, 5, 100)
            and tdSql.compareData(0, 6, None)
            and tdSql.compareData(1, 0, 1)
            and tdSql.compareData(1, 1, 2)
            and tdSql.compareData(1, 2, 3)
            and tdSql.compareData(1, 3, "2025-01-01 00:00:01")
            and tdSql.compareData(1, 4, None)
            and tdSql.compareData(1, 5, 101)
            and tdSql.compareData(1, 6, None)
            and tdSql.compareData(2, 0, 1)
            and tdSql.compareData(2, 1, 2)
            and tdSql.compareData(2, 2, 3)
            and tdSql.compareData(2, 3, "2025-01-01 00:00:02")
            and tdSql.compareData(2, 4, None)
            and tdSql.compareData(2, 5, 102)
            and tdSql.compareData(2, 6, None)
            and tdSql.compareData(3, 0, 1)
            and tdSql.compareData(3, 1, 2)
            and tdSql.compareData(3, 2, 3)
            and tdSql.compareData(3, 3, "2025-01-01 00:00:03")
            and tdSql.compareData(3, 4, None)
            and tdSql.compareData(3, 5, 103)
            and tdSql.compareData(3, 6, None),
            retry=60,
        )

    def check_normal_table_partial_columns(self):
        db_name = "test_ntb_partial_cols"
        tdSql.execute(f"drop database if exists {db_name}", show=1)
        tdSql.execute(f"create database {db_name} vgroups 1 keep 3650", show=1)
        tdSql.execute(f"use {db_name}")
        tdSql.execute(
            "create table tb_src (ts timestamp, c0 int, c1 int, c2 int, c3 int);",
            show=1,
        )
        tdSql.execute(
            "create table out_ntb_partial (ts timestamp, c1 int, c2 int, c3 int);",
            show=1,
        )
        sql = """create stream s_ntb_partial interval(1s) sliding (1s)
            from tb_src
            into out_ntb_partial (ts, c2)
            as select _twstart ts, last(c2) c2 from tb_src
            where _c0 >= _twstart and _c0 < _twend;"""
        tdSql.execute(sql)
        tdStream.checkStreamStatus("s_ntb_partial")

        tdSql.execute(
            "insert into tb_src (ts, c1, c2, c3) values "
            "('2025-01-01 00:00:00', 10, 100, 1000), "
            "('2025-01-01 00:00:01', 11, 101, 1001),"
            "('2025-01-01 00:00:02', 12, 102, 1002),"
            "('2025-01-01 00:00:03', 13, 103, 1003),"
            "('2025-01-01 00:00:04', 14, 104, 1004);",
            show=1,
        )

        tdSql.checkResultsByFunc(
            sql=f"select ts, c1, c2, c3 from {db_name}.out_ntb_partial",
            func=lambda: tdSql.getRows() == 4
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
            and tdSql.compareData(0, 1, None)
            and tdSql.compareData(0, 2, 100)
            and tdSql.compareData(0, 3, None)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:01")
            and tdSql.compareData(1, 1, None)
            and tdSql.compareData(1, 2, 101)
            and tdSql.compareData(1, 3, None)
            and tdSql.compareData(2, 0, "2025-01-01 00:00:02")
            and tdSql.compareData(2, 1, None)
            and tdSql.compareData(2, 2, 102)
            and tdSql.compareData(2, 3, None)
            and tdSql.compareData(3, 0, "2025-01-01 00:00:03")
            and tdSql.compareData(3, 1, None)
            and tdSql.compareData(3, 2, 103)
            and tdSql.compareData(3, 3, None),
            retry=60,
        )

    def check_normal_table_partial_columns_after_alter(self):
        db_name = "test_ntb_partial_alter"
        out_ntb = "out_ntb_alter_partial"
        tdSql.execute(f"drop database if exists {db_name}", show=1)
        tdSql.execute(f"create database {db_name} vgroups 1 keep 3650", show=1)
        tdSql.execute(f"use {db_name}")
        tdSql.execute(
            "create table tb_src (ts timestamp, c0 int, c1 int, c2 int, c3 int);",
            show=1,
        )
        tdSql.execute(
            f"create table {out_ntb} (ts timestamp, c1 int, c2 int, c3 int);",
            show=1,
        )
        tdSql.execute(f"alter table {out_ntb} drop column c3;", show=1)
        tdSql.execute(f"alter table {out_ntb} add column c4 int;", show=1)
        sql = f"""create stream s_ntb_alter_partial interval(1s) sliding (1s)
            from tb_src
            into {out_ntb} (ts, c4)
            as select _twstart ts, last(c3) c4 from tb_src
            where _c0 >= _twstart and _c0 < _twend;"""
        tdSql.execute(sql)
        tdStream.checkStreamStatus("s_ntb_alter_partial")

        tdSql.execute(
            "insert into tb_src (ts, c1, c2, c3) values "
            "('2025-01-01 00:00:00', 10, 100, 1000), "
            "('2025-01-01 00:00:01', 11, 101, 1001), "
            "('2025-01-01 00:00:02', 12, 102, 1002), "
            "('2025-01-01 00:00:03', 13, 103, 1003);",
            show=1,
        )

        tdSql.checkResultsByFunc(
            sql=f"select ts, c1, c2, c4 from {db_name}.{out_ntb}",
            func=lambda: tdSql.getRows() == 3
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
            and tdSql.compareData(0, 1, None)
            and tdSql.compareData(0, 2, None)
            and tdSql.compareData(0, 3, 1000)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:01")
            and tdSql.compareData(1, 1, None)
            and tdSql.compareData(1, 2, None)
            and tdSql.compareData(1, 3, 1001)
            and tdSql.compareData(2, 0, "2025-01-01 00:00:02")
            and tdSql.compareData(2, 1, None)
            and tdSql.compareData(2, 2, None)
            and tdSql.compareData(2, 3, 1002),
            retry=60,
        )

    def check_partial_tags(self):
        db_name = "test_partial_tags"
        tdSql.execute(f"drop database if exists {db_name}", show=1)
        tdSql.execute(f"create database {db_name} vgroups 1 keep 3650", show=1)
        tdSql.execute(f"use {db_name}")
        tdSql.execute(
            "create stable stb_src (ts timestamp, c0 int, c1 int, c2 int, c3 int) tags (gid1 int, gid2 int, gid3 int);",
            show=1,
        )
        tdSql.execute(
            "create stable out_partial_tags (ts timestamp, c1 int, c2 int, c3 int) "
            "tags (gid1 int, gid2 int, gid3 int);",
            show=1,
        )
        sql = """create stream s_partial_tags interval(1s) sliding (1s)
            from stb_src partition by tbname, gid2
            into out_partial_tags tags (gid2 int as gid2)
            as select _twstart ts, last(c1) c1, last(c2) c2, last(c3) c3 from %%tbname
            where _c0 >= _twstart and _c0 < _twend group by gid2;"""
        tdSql.execute(sql)
        tdStream.checkStreamStatus("s_partial_tags")

        tdSql.execute("create table ct1 using stb_src tags (1, 2, 3);", show=1)
        tdSql.execute(
            "insert into ct1 (ts, c1, c2, c3) values "
            "('2025-01-01 00:00:00', 10, 100, 1000), "
            "('2025-01-01 00:00:01', 11, 101, 1001);",
            show=1,
        )

        tdSql.checkResultsByFunc(
            sql=f"select gid1, gid2, gid3, ts, c1, c2, c3 from {db_name}.out_partial_tags",
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, None)
            and tdSql.compareData(0, 1, 2)
            and tdSql.compareData(0, 2, None)
            and tdSql.compareData(0, 3, "2025-01-01 00:00:00")
            and tdSql.compareData(0, 4, 10)
            and tdSql.compareData(0, 5, 100)
            and tdSql.compareData(0, 6, 1000),
            retry=60,
        )

    def check_partial_columns_and_tags(self):
        db_name = "test_partial_both"
        tdSql.execute(f"drop database if exists {db_name}", show=1)
        tdSql.execute(f"create database {db_name} vgroups 1 keep 3650", show=1)
        tdSql.execute(f"use {db_name}")
        tdSql.execute(
            "create stable stb_src (ts timestamp, c0 int, c1 int, c2 int, c3 int) tags (gid1 int, gid2 int, gid3 int);",
            show=1,
        )
        tdSql.execute(
            "create stable out_partial_both (ts timestamp, c1 int, c2 int, c3 int) tags (gid1 int, gid2 int, gid3 int);",
            show=1,
        )
        sql = """create stream s_partial_both interval(1s) sliding (1s)
            from stb_src partition by tbname, gid1, gid2, gid3
            into out_partial_both (ts, c1) tags (gid1 int as gid1, gid2 int as gid2)
            as select _twstart ts, last(c1) c1 from %%tbname
            where _c0 >= _twstart and _c0 < _twend group by gid1, gid2, gid3;"""
        tdSql.execute(sql)
        tdStream.checkStreamStatus("s_partial_both")

        tdSql.execute("create table ct1 using stb_src tags (1, 2, 3);", show=1)
        tdSql.execute(
            "insert into ct1 (ts, c1, c2, c3) values "
            "('2025-01-01 00:00:00', 10, 100, 1000), "
            "('2025-01-01 00:00:01', 11, 101, 1001);",
            show=1,
        )

        tdSql.checkResultsByFunc(
            sql=f"select gid1, gid2, gid3, ts, c1, c2, c3 from {db_name}.out_partial_both",
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, 1)
            and tdSql.compareData(0, 1, 2)
            and tdSql.compareData(0, 2, None)
            and tdSql.compareData(0, 3, "2025-01-01 00:00:00")
            and tdSql.compareData(0, 4, 10)
            and tdSql.compareData(0, 5, None)
            and tdSql.compareData(0, 6, None),
            retry=60,
        )

    def check_partial_columns_after_alter(self):
        db_name = "test_partial_alter"
        out_stb = "out_alter_partial"
        tdSql.execute(f"drop database if exists {db_name}", show=1)
        tdSql.execute(f"create database {db_name} vgroups 1 keep 3650", show=1)
        tdSql.execute(f"use {db_name}")
        tdSql.execute(
            "create stable stb_src (ts timestamp, c0 int, c1 int, c2 int, c3 int) "
            "tags (gid1 int, gid2 int);",
            show=1,
        )
        tdSql.execute(
            f"create stable {out_stb} (ts timestamp, c1 int, c2 int, c3 int) "
            "tags (gid1 int, gid2 int);",
            show=1,
        )
        tdSql.execute(f"alter table {out_stb} drop column c3;", show=1)
        tdSql.execute(f"alter table {out_stb} add column c4 int;", show=1)
        sql = f"""create stream s_alter_partial interval(1s) sliding (1s)
            from stb_src partition by tbname, gid1, gid2
            into {out_stb} (ts, c4) tags (gid1 int as gid1, gid2 int as gid2)
            as select _twstart ts, last(c3) c4 from %%tbname
            where _c0 >= _twstart and _c0 < _twend group by gid1, gid2;"""
        tdSql.execute(sql)
        tdStream.checkStreamStatus("s_alter_partial")

        tdSql.execute("create table ct1 using stb_src tags (1, 2);", show=1)
        tdSql.execute(
            "insert into ct1 (ts, c1, c2, c3) values "
            "('2025-01-01 00:00:00', 10, 100, 1000), "
            "('2025-01-01 00:00:01', 11, 101, 1001), "
            "('2025-01-01 00:00:02', 12, 102, 1002), "
            "('2025-01-01 00:00:03', 13, 103, 1003);",
            show=1,
        )

        tdSql.checkResultsByFunc(
            sql=f"select gid1, gid2, ts, c1, c2, c4 from {db_name}.{out_stb}",
            func=lambda: tdSql.getRows() == 3
            and tdSql.compareData(0, 0, 1)
            and tdSql.compareData(0, 1, 2)
            and tdSql.compareData(0, 2, "2025-01-01 00:00:00")
            and tdSql.compareData(0, 3, None)
            and tdSql.compareData(0, 4, None)
            and tdSql.compareData(0, 5, 1000)
            and tdSql.compareData(1, 0, 1)
            and tdSql.compareData(1, 1, 2)
            and tdSql.compareData(1, 2, "2025-01-01 00:00:01")
            and tdSql.compareData(1, 3, None)
            and tdSql.compareData(1, 4, None)
            and tdSql.compareData(1, 5, 1001)
            and tdSql.compareData(2, 0, 1)
            and tdSql.compareData(2, 1, 2)
            and tdSql.compareData(2, 2, "2025-01-01 00:00:02")
            and tdSql.compareData(2, 3, None)
            and tdSql.compareData(2, 4, None)
            and tdSql.compareData(2, 5, 1002),
            retry=60,
        )

    def check_out_table_invalid_column_order(self):
        db_name = "test_col_order_invalid"
        tdSql.execute(f"drop database if exists {db_name}", show=1)
        tdSql.execute(f"create database {db_name} vgroups 1 keep 3650", show=1)
        tdSql.execute(f"use {db_name}")
        tdSql.execute(
            "create stable stb_src (ts timestamp, c0 int, c1 int, c2 int, c3 int) "
            "tags (gid1 int, gid2 int, gid3 int);",
            show=1,
        )
        tdSql.execute(
            "create stable out_col_order (ts timestamp, c1 int, c2 int, c3 int) "
            "tags (gid1 int, gid2 int, gid3 int);",
            show=1,
        )

        err_msg = "Out table cols must be specified in ascending column id order"
        invalid_streams = [
            (
                "s_col_order_partial_reverse",
                "(ts, c3, c2)",
                "last(c3) c3, last(c2) c2",
            ),
            (
                "s_col_order_skip_and_reverse",
                "(ts, c3, c1)",
                "last(c3) c3, last(c1) c1",
            ),
            (
                "s_col_order_full_reverse",
                "(ts, c3, c2, c1)",
                "last(c3) c3, last(c2) c2, last(c1) c1",
            ),
            (
                "s_col_order_duplicate",
                "(ts, c2, c2)",
                "last(c2) c2, last(c3) c3",
            ),
        ]

        for stream_name, into_cols, select_cols in invalid_streams:
            sql = f"""create stream {stream_name} interval(1s) sliding (1s)
                from stb_src partition by tbname, gid1, gid2, gid3
                into out_col_order {into_cols} tags (gid1 int as gid1, gid2 int as gid2, gid3 int as gid3)
                as select _twstart ts, {select_cols} from %%tbname
                where _c0 >= _twstart and _c0 < _twend group by gid1, gid2, gid3;"""
            tdSql.error(sql, expectErrInfo=err_msg, fullMatched=False)

        # ascending colId order with skipped columns remains valid
        sql_ok = """create stream s_col_order_valid interval(1s) sliding (1s)
            from stb_src partition by tbname, gid1, gid2, gid3
            into out_col_order (ts, c2) tags (gid1 int as gid1, gid2 int as gid2, gid3 int as gid3)
            as select _twstart ts, last(c2) c2 from %%tbname
            where _c0 >= _twstart and _c0 < _twend group by gid1, gid2, gid3;"""
        tdSql.execute(sql_ok)
        tdStream.checkStreamStatus("s_col_order_valid")

    def check_out_table_invalid_first_column(self):
        db_name = "test_col_first_invalid"
        tdSql.execute(f"drop database if exists {db_name}", show=1)
        tdSql.execute(f"create database {db_name} vgroups 1 keep 3650", show=1)
        tdSql.execute(f"use {db_name}")
        tdSql.execute(
            "create stable stb_src (ts timestamp, c0 int, c1 int, c2 int, c3 int) "
            "tags (gid1 int, gid2 int, gid3 int);",
            show=1,
        )
        tdSql.execute(
            "create stable out_col_first (ts timestamp, c1 int, c2 int) "
            "tags (gid1 int, gid2 int, gid3 int);",
            show=1,
        )
        tdSql.execute("alter table out_col_first add column ts2 timestamp;", show=1)

        err_msg = "Out table first col must be primary timestamp column"
        sql_invalid = """create stream s_col_first_invalid interval(1s) sliding (1s)
            from stb_src partition by tbname, gid1, gid2, gid3
            into out_col_first (ts2, c2) tags (gid1 int as gid1, gid2 int as gid2, gid3 int as gid3)
            as select _twstart ts, last(c2) c2 from %%tbname
            where _c0 >= _twstart and _c0 < _twend group by gid1, gid2, gid3;"""
        tdSql.error(sql_invalid, expectErrInfo=err_msg, fullMatched=False)

        sql_ok = """create stream s_col_first_valid interval(1s) sliding (1s)
            from stb_src partition by tbname, gid1, gid2, gid3
            into out_col_first (ts, c2) tags (gid1 int as gid1, gid2 int as gid2, gid3 int as gid3)
            as select _twstart ts, last(c2) c2 from %%tbname
            where _c0 >= _twstart and _c0 < _twend group by gid1, gid2, gid3;"""
        tdSql.execute(sql_ok)
        tdStream.checkStreamStatus("s_col_first_valid")

    def check_partial_tags_after_alter(self):
        db_name = "test_partial_tag_alter"
        out_stb = "out_alter_partial_tag"
        tdSql.execute(f"drop database if exists {db_name}", show=1)
        tdSql.execute(f"create database {db_name} vgroups 1 keep 3650", show=1)
        tdSql.execute(f"use {db_name}")
        tdSql.execute(
            "create stable stb_src (ts timestamp, c0 int, c1 int, c2 int, c3 int) "
            "tags (gid1 int, gid2 int, gid3 int);",
            show=1,
        )
        tdSql.execute(
            f"create stable {out_stb} (ts timestamp, c1 int, c2 int, c3 int) "
            "tags (gid1 int, gid2 int, gid3 int);",
            show=1,
        )
        tdSql.execute(f"alter table {out_stb} drop tag gid3;", show=1)
        tdSql.execute(f"alter table {out_stb} add tag gid4 int;", show=1)
        sql = f"""create stream s_alter_partial_tag interval(1s) sliding (1s)
            from stb_src partition by tbname, gid2
            into {out_stb} tags (gid4 int as gid2)
            as select _twstart ts, last(c1) c1, last(c2) c2, last(c3) c3 from %%tbname
            where _c0 >= _twstart and _c0 < _twend group by gid2;"""
        tdSql.execute(sql)
        tdStream.checkStreamStatus("s_alter_partial_tag")

        tdSql.execute("create table ct1 using stb_src tags (1, 2, 3);", show=1)
        tdSql.execute(
            "insert into ct1 (ts, c1, c2, c3) values "
            "('2025-01-01 00:00:00', 10, 100, 1000), "
            "('2025-01-01 00:00:01', 11, 101, 1001);",
            show=1,
        )

        tdSql.checkResultsByFunc(
            sql=f"select gid1, gid2, gid4, ts, c1, c2, c3 from {db_name}.{out_stb}",
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, None)
            and tdSql.compareData(0, 1, None)
            and tdSql.compareData(0, 2, 2)
            and tdSql.compareData(0, 3, "2025-01-01 00:00:00")
            and tdSql.compareData(0, 4, 10)
            and tdSql.compareData(0, 5, 100)
            and tdSql.compareData(0, 6, 1000),
            retry=60,
        )

    def check_out_table_schema_change(self):
        db_name = "test"
        stb_name = "stb"
        out_exist_tbname = "out_table"
        out_not_exist_tbname = "out_table_not_exist"
        out_more_cols = "out_table_more_cols"
        tdSql.execute(f"create database if not exists {db_name} vgroups 1 keep 3650", show=1)
        tdSql.execute(f"use {db_name}")
        tdSql.execute(f"create stable {out_exist_tbname} (ts timestamp, c1 int) tags (gid1 int, gid2 int);", show=1)
        tdSql.execute(f"alter table {out_exist_tbname} add column c2 int;", show=1)
        tdSql.execute(f"create stable {out_more_cols} (ts timestamp, c1 int, c2 int, c3 int) tags (gid1 int, gid2 int);", show=1)
        tdSql.execute(f"alter table {out_more_cols} drop column c3;", show=1)
        tdSql.execute(f"create stable {stb_name} (ts timestamp, c0 int, c1 int, c2 int, c3 int) tags (gid1 int, gid2 int);", show=1)



        sql1 =f"""create stream s1 interval(1s) sliding (1s) from {stb_name} partition by tbname,
            gid1, gid2 into {out_exist_tbname} tags (gid1 int as gid1, gid2 int as gid2)
            as select _twstart ts, last(c1) c1, last(c2) c2 from %%tbname where _c0 >= _twstart and _c0 < _twend group by gid1, gid2;"""
        sql2 =f"""create stream s2 interval(1s) sliding (1s) from {stb_name} partition by tbname,
            gid1, gid2 into {out_not_exist_tbname} tags (gid1 int as gid1, gid2 int as gid2)
            as select _twstart ts, last(c1) c1, last(c2) c2 from %%tbname where _c0 >= _twstart and _c0 < _twend group by gid1, gid2;"""
        sql3 =f"""create stream s3 interval(1s) sliding (1s) from {stb_name} partition by tbname,
            gid1, gid2 into {out_not_exist_tbname} tags (gid1 int as gid1, gid2 int as gid2)
            as select _twstart ts, last(c1) c1, last(c2) c2 from %%tbname where _c0 >= _twstart and _c0 < _twend group by gid1, gid2;"""
        streams = [
            self.StreamItem(sql1, self.checks1),
            self.StreamItem(sql2, self.checks2),
            self.StreamItem(sql3, self.checks3)
        ]
        for stream in streams:
            tdSql.execute(stream.sql)
        tdStream.checkStreamStatus()

        tdSql.execute(f"create table ct1 using {stb_name} tags (1, 2);", show=1)
        tdSql.execute(f"insert into ct1 (ts, c1) values ('2025-01-01 00:00:00', 0), ('2025-01-01 00:00:01', 1);", show=1)

        tdSql.query(f"select gid1, gid2, c1, c2 from {out_exist_tbname}")


    def checks1(self):
        db_name = "test"
        out_exist_tbname = "out_table"
        result_sql = f"select gid1, gid2, c1, c2 from {db_name}.{out_exist_tbname}"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, 1)
            and tdSql.compareData(0, 1, 2)
            and tdSql.compareData(0, 2, 1)
            and tdSql.compareData(0, 3, "NULL")
        )

    def checks2(self):
        db_name = "test"
        out_not_exist_tbname = "out_table_not_exist"
        result_sql = f"select gid1, gid2, c1, c2 from {db_name}.{out_not_exist_tbname}"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, 1)
            and tdSql.compareData(0, 1, 2)
            and tdSql.compareData(0, 2, 1)
            and tdSql.compareData(0, 3, "NULL")
        )
    def checks3(self):
        db_name = "test"
        out_more_cols = "out_table_more_cols"
        result_sql = f"select gid1, gid2, c0, c1, c2 from {db_name}.{out_more_cols}"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, 1)
            and tdSql.compareData(0, 1, 2)
            and tdSql.compareData(0, 2, "NULL")
            and tdSql.compareData(0, 3, 1)
            and tdSql.compareData(0, 4, "NULL")
        )


    class StreamItem:
        def __init__(self, sql, checkfunc):
            self.sql = sql
            self.checkfunc = checkfunc

        def check(self):
            self.checkfunc()
