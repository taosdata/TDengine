# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
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


DB = "db_blob_func"


class TestBlobCastFunc:
    def setup_class(cls):
        cls.replicaVar = 1
        tdLog.debug(f"start to execute {__file__}")

    def _create_blob_table(self, table="t_blob"):
        tdSql.execute(f"create table if not exists {DB}.{table} (ts timestamp, id int, b_data blob, label varchar(50))")

    def _prepare_db(self):
        tdSql.execute(f"drop database if exists {DB}")
        tdSql.execute(f"create database {DB}")
        tdSql.execute(f"use {DB}")

    def cast_varchar_to_blob(self):
        tdLog.info("cast varchar column to blob")
        tdSql.execute(f"create table {DB}.tstr (ts timestamp, id int, vc varchar(100))")
        tdSql.execute(f"insert into {DB}.tstr values ('2025-01-01 00:00:01', 1, 'hello world')")
        tdSql.execute(f"insert into {DB}.tstr values ('2025-01-01 00:00:02', 2, 'TDengine')")
        tdSql.execute(f"insert into {DB}.tstr values ('2025-01-01 00:00:03', 3, '')")

        tdSql.query(f"select id, cast(vc as blob) from {DB}.tstr where id=1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, b'hello world')

        tdSql.query(f"select id, cast(vc as blob) from {DB}.tstr where id=2")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, b'TDengine')

        tdSql.query(f"select id, cast(vc as blob) from {DB}.tstr where id=3")
        tdSql.checkRows(1)

    def cast_varchar_literal_to_blob(self):
        tdLog.info("cast varchar literal to blob")
        tdSql.query("select cast('literal test' as blob)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, b'literal test')

        tdSql.query("select cast('hello' as blob)")
        tdSql.checkData(0, 0, b'hello')

        tdSql.query("select cast('' as blob)")
        tdSql.checkRows(1)

    def cast_blob_to_varchar(self):
        tdLog.info("cast blob to varchar")
        self._create_blob_table("tb2v")
        tdSql.execute(f"insert into {DB}.tb2v values ('2025-01-01 00:00:01', 1, 'hello world', 'a')")
        tdSql.execute(f"insert into {DB}.tb2v values ('2025-01-01 00:00:02', 2, 'TDengine', 'b')")
        tdSql.execute(f"insert into {DB}.tb2v values ('2025-01-01 00:00:03', 3, '1234567890', 'c')")

        tdSql.query(f"select id, cast(b_data as varchar(64)) from {DB}.tb2v where id=1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "hello world")

        # truncation
        tdSql.query(f"select id, cast(b_data as varchar(5)) from {DB}.tb2v where id=1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "hello")

        # multiple rows
        tdSql.query(f"select id, cast(b_data as varchar(64)) from {DB}.tb2v order by id")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, "hello world")
        tdSql.checkData(1, 1, "TDengine")
        tdSql.checkData(2, 1, "1234567890")

    def cast_blob_to_blob(self):
        tdLog.info("cast blob to blob (identity)")
        self._create_blob_table("tb2b")
        tdSql.execute(f"insert into {DB}.tb2b values ('2025-01-01 00:00:01', 1, 'hello world', 'a')")

        tdSql.query(f"select id, cast(b_data as blob) from {DB}.tb2b where id=1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, b'hello world')

    def cast_blob_to_int(self):
        tdLog.info("cast blob to int types")
        self._create_blob_table("tb2i")
        tdSql.execute(f"insert into {DB}.tb2i values ('2025-01-01 00:00:01', 1, '42', 'int')")
        tdSql.execute(f"insert into {DB}.tb2i values ('2025-01-01 00:00:02', 2, '255', 'u8')")

        tdSql.query(f"select cast(b_data as int) from {DB}.tb2i where id=1")
        tdSql.checkData(0, 0, 42)

        tdSql.query(f"select cast(b_data as bigint) from {DB}.tb2i where id=1")
        tdSql.checkData(0, 0, 42)

        tdSql.query(f"select cast(b_data as tinyint) from {DB}.tb2i where id=1")
        tdSql.checkData(0, 0, 42)

        tdSql.query(f"select cast(b_data as smallint) from {DB}.tb2i where id=1")
        tdSql.checkData(0, 0, 42)

        tdSql.query(f"select cast(b_data as tinyint unsigned) from {DB}.tb2i where id=2")
        tdSql.checkData(0, 0, 255)

        tdSql.query(f"select cast(b_data as smallint unsigned) from {DB}.tb2i where id=2")
        tdSql.checkData(0, 0, 255)

        tdSql.query(f"select cast(b_data as int unsigned) from {DB}.tb2i where id=2")
        tdSql.checkData(0, 0, 255)

        tdSql.query(f"select cast(b_data as bigint unsigned) from {DB}.tb2i where id=2")
        tdSql.checkData(0, 0, 255)

    def cast_blob_to_float(self):
        tdLog.info("cast blob to float/double")
        self._create_blob_table("tb2f")
        tdSql.execute(f"insert into {DB}.tb2f values ('2025-01-01 00:00:01', 1, '3.14', 'f')")

        tdSql.query(f"select cast(b_data as float) from {DB}.tb2f where id=1")
        val = tdSql.getData(0, 0)
        if abs(val - 3.14) > 0.01:
            tdLog.exit(f"cast blob to float: expected ~3.14, got {val}")

        tdSql.query(f"select cast(b_data as double) from {DB}.tb2f where id=1")
        val = tdSql.getData(0, 0)
        if abs(val - 3.14) > 0.001:
            tdLog.exit(f"cast blob to double: expected ~3.14, got {val}")

    def cast_blob_to_bool(self):
        tdLog.info("cast blob to bool")
        self._create_blob_table("tb2bool")
        tdSql.execute(f"insert into {DB}.tb2bool values ('2025-01-01 00:00:01', 1, '1', 'true')")
        tdSql.execute(f"insert into {DB}.tb2bool values ('2025-01-01 00:00:02', 2, '0', 'false')")

        tdSql.query(f"select cast(b_data as bool) from {DB}.tb2bool where id=1")
        tdSql.checkData(0, 0, True)

        tdSql.query(f"select cast(b_data as bool) from {DB}.tb2bool where id=2")
        tdSql.checkData(0, 0, False)

    def cast_numeric_to_blob(self):
        tdLog.info("cast numeric to blob")
        tdSql.execute(f"create table {DB}.tnum (ts timestamp, id int, v_int int, v_big bigint, v_float float, v_bool bool)")
        tdSql.execute(f"insert into {DB}.tnum values ('2025-01-01 00:00:01', 1, 42, 100, 3.14, true)")

        tdSql.query(f"select cast(v_int as blob) from {DB}.tnum where id=1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, b'42')

        tdSql.query(f"select cast(v_big as blob) from {DB}.tnum where id=1")
        tdSql.checkData(0, 0, b'100')

    def cast_decimal_to_blob(self):
        tdSql.query("select cast(cast(1.50 as decimal(10,2)) as blob)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, b"1.50")

        cases = [
            ("decimal(10,2)", "1.50", b"1.50"),
            ("decimal(10,2)", "-1234.56", b"-1234.56"),
            ("decimal(10,2)", "0.00", b"0"),
            ("decimal(18,0)", "999999999999999999", b"999999999999999999"),
            ("decimal(18,18)", "-0.000000000000000001", b"-0.000000000000000001"),
            (
                "decimal(38,18)",
                "12345678901234567890.123456789012345678",
                b"12345678901234567890.123456789012345678",
            ),
            (
                "decimal(38,18)",
                "-12345678901234567890.123456789012345678",
                b"-12345678901234567890.123456789012345678",
            ),
            ("decimal(38,18)", "0.000000000000000000", b"0"),
        ]
        for index, (data_type, value, expected) in enumerate(cases):
            table = f"{DB}.t_decimal_{index}"
            tdSql.execute(f"create table {table} (ts timestamp, v {data_type})")
            tdSql.execute(
                f"insert into {table} values (1735689600000, '{value}') "
                "(1735689600001, NULL)"
            )
            for target in ("blob", "varchar(128)", "varchar(3)"):
                want = expected if target == "blob" else expected.decode("ascii")
                if target == "varchar(3)":
                    want = want[:3]
                tdSql.query(f"select cast(cast('{value}' as {data_type}) as {target})")
                tdSql.checkRows(1)
                tdSql.checkData(0, 0, want)
                tdSql.query(f"select cast(v as {target}) from {table} order by ts")
                tdSql.checkRows(2)
                tdSql.checkData(0, 0, want)
                tdSql.checkData(1, 0, None)
        tdLog.info("decimal to blob and varchar ................ [ passed ]")


    def cast_blob_to_nchar(self):
        tdLog.info("cast blob to nchar")
        self._create_blob_table("tb2nc")
        tdSql.execute(f"insert into {DB}.tb2nc values ('2025-01-01 00:00:01', 1, 'hello', 'a')")

        tdSql.query(f"select cast(b_data as nchar(32)) from {DB}.tb2nc where id=1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "hello")

    def cast_blob_to_binary(self):
        tdLog.info("cast blob to binary")
        self._create_blob_table("tb2bin")
        tdSql.execute(f"insert into {DB}.tb2bin values ('2025-01-01 00:00:01', 1, 'binary data', 'a')")

        tdSql.query(f"select cast(b_data as binary(64)) from {DB}.tb2bin where id=1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "binary data")

        tdSql.query(f"select cast(b_data as binary(6)) from {DB}.tb2bin where id=1")
        tdSql.checkData(0, 0, "binary")

    def cast_null_blob(self):
        tdLog.info("cast on null blob values")
        self._create_blob_table("tnull")
        tdSql.execute(f"insert into {DB}.tnull values ('2025-01-01 00:00:01', 1, NULL, 'null_test')")

        tdSql.query(f"select cast(b_data as varchar(64)) from {DB}.tnull where id=1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select cast(b_data as int) from {DB}.tnull where id=1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select cast(b_data as bigint) from {DB}.tnull where id=1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select cast(b_data as float) from {DB}.tnull where id=1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select cast(b_data as double) from {DB}.tnull where id=1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select cast(b_data as bool) from {DB}.tnull where id=1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select cast(b_data as blob) from {DB}.tnull where id=1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select cast(b_data as nchar(32)) from {DB}.tnull where id=1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select cast(b_data as binary(32)) from {DB}.tnull where id=1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

    def substr_blob_positive(self):
        tdLog.info("substr on blob with positive position")
        self._create_blob_table("tsub_pos")
        tdSql.execute(f"insert into {DB}.tsub_pos values ('2025-01-01 00:00:01', 1, 'hello world', 'a')")
        tdSql.execute(f"insert into {DB}.tsub_pos values ('2025-01-01 00:00:02', 2, 'TDengine test', 'b')")

        tdSql.query(f"select substr(b_data, 1, 5) from {DB}.tsub_pos where id=1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, b'hello')

        tdSql.query(f"select substr(b_data, 7, 5) from {DB}.tsub_pos where id=1")
        tdSql.checkData(0, 0, b'world')

        tdSql.query(f"select substr(b_data, 1, 8) from {DB}.tsub_pos where id=2")
        tdSql.checkData(0, 0, b'TDengine')

    def substr_blob_negative(self):
        tdLog.info("substr on blob with negative position")
        self._create_blob_table("tsub_neg")
        tdSql.execute(f"insert into {DB}.tsub_neg values ('2025-01-01 00:00:01', 1, 'hello world', 'a')")

        tdSql.query(f"select substr(b_data, -5) from {DB}.tsub_neg where id=1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, b'world')

        tdSql.query(f"select substr(b_data, -11) from {DB}.tsub_neg where id=1")
        tdSql.checkData(0, 0, b'hello world')

    def substr_blob_null(self):
        tdLog.info("substr on null blob")
        self._create_blob_table("tsub_null")
        tdSql.execute(f"insert into {DB}.tsub_null values ('2025-01-01 00:00:01', 1, NULL, 'x')")

        tdSql.query(f"select substr(b_data, 1, 5) from {DB}.tsub_null where id=1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

    def substr_blob_empty(self):
        tdLog.info("substr on empty blob")
        self._create_blob_table("tsub_empty")
        tdSql.execute(f"insert into {DB}.tsub_empty values ('2025-01-01 00:00:01', 1, '', 'x')")

        tdSql.query(f"select substr(b_data, 1, 5) from {DB}.tsub_empty where id=1")
        tdSql.checkRows(1)

    def substr_blob_boundary(self):
        tdLog.info("substr boundary conditions")
        self._create_blob_table("tsub_bnd")
        tdSql.execute(f"insert into {DB}.tsub_bnd values ('2025-01-01 00:00:01', 1, 'hello world', 'a')")

        tdSql.query(f"select substr(b_data, 0, 5) from {DB}.tsub_bnd where id=1")
        tdSql.checkRows(1)

        tdSql.query(f"select substr(b_data, 100) from {DB}.tsub_bnd where id=1")
        tdSql.checkRows(1)

        tdSql.query(f"select substr(b_data, 1, 10000) from {DB}.tsub_bnd where id=1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, b'hello world')

        tdSql.query(f"select substr(b_data, 1, 0) from {DB}.tsub_bnd where id=1")
        tdSql.checkRows(1)

    def length_blob(self):
        tdLog.info("length on blob")
        self._create_blob_table("tlen")
        tdSql.execute(f"insert into {DB}.tlen values ('2025-01-01 00:00:01', 1, 'hello world', 'a')")
        tdSql.execute(f"insert into {DB}.tlen values ('2025-01-01 00:00:02', 2, '', 'b')")
        tdSql.execute(f"insert into {DB}.tlen values ('2025-01-01 00:00:03', 3, NULL, 'c')")
        tdSql.execute(f"insert into {DB}.tlen values ('2025-01-01 00:00:04', 4, 'a', 'd')")

        tdSql.query(f"select length(b_data) from {DB}.tlen where id=1")
        tdSql.checkData(0, 0, 11)

        tdSql.query(f"select length(b_data) from {DB}.tlen where id=2")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select length(b_data) from {DB}.tlen where id=3")
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select length(b_data) from {DB}.tlen where id=4")
        tdSql.checkData(0, 0, 1)

    def blob_where_with_cast(self):
        tdLog.info("blob in WHERE clause with cast")
        self._create_blob_table("twhere")
        tdSql.execute(f"insert into {DB}.twhere values ('2025-01-01 00:00:01', 1, 'hello world', 'a')")
        tdSql.execute(f"insert into {DB}.twhere values ('2025-01-01 00:00:02', 2, 'TDengine', 'b')")

        tdSql.query(f"select id from {DB}.twhere where cast(b_data as varchar(64)) = 'hello world'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select id from {DB}.twhere where cast(b_data as varchar(64)) = 'TDengine'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

    def blob_aggregate(self):
        tdLog.info("blob with aggregate functions")
        self._create_blob_table("tagg")
        tdSql.execute(f"insert into {DB}.tagg values ('2025-01-01 00:00:01', 1, 'hello', 'a')")
        tdSql.execute(f"insert into {DB}.tagg values ('2025-01-01 00:00:02', 2, 'TDengine', 'b')")
        tdSql.execute(f"insert into {DB}.tagg values ('2025-01-01 00:00:03', 3, '1234567890', 'c')")

        tdSql.query(f"select count(*) from {DB}.tagg")
        tdSql.checkData(0, 0, 3)

        tdSql.query(f"select max(length(b_data)) from {DB}.tagg")
        tdSql.checkData(0, 0, 10)

        tdSql.query(f"select min(length(b_data)) from {DB}.tagg")
        tdSql.checkData(0, 0, 5)

    def blob_group_by_error(self):
        tdLog.info("blob group by restrictions")
        self._create_blob_table("tgrp")
        tdSql.execute(f"insert into {DB}.tgrp values ('2025-01-01 00:00:01', 1, 'hello', 'a')")
        tdSql.execute(f"insert into {DB}.tgrp values ('2025-01-01 00:00:02', 2, 'world', 'b')")

        tdSql.error(f"select substr(b_data, 1, 3), count(*) from {DB}.tgrp group by substr(b_data, 1, 3)")
        tdSql.error(f"select cast(b_data as varchar(10)), count(*) from {DB}.tgrp group by cast(b_data as varchar(10))")

    def test_blob_cast_func(self):
        """Fun: blob cast/substr/length

        Test BLOB type support in cast, substr, and length scalar functions.

        1. CAST VARCHAR/literal to BLOB
        2. CAST BLOB to VARCHAR with truncation
        3. CAST BLOB to BLOB (identity)
        4. CAST BLOB to INT/BIGINT/TINYINT/SMALLINT/unsigned variants
        5. CAST BLOB to FLOAT/DOUBLE
        6. CAST BLOB to BOOL
        7. CAST numeric types to BLOB
        8. CAST BLOB to NCHAR
        9. CAST BLOB to BINARY with truncation
        10. CAST on NULL BLOB to all types
        11. SUBSTR on BLOB: positive/negative position
        12. SUBSTR on NULL/empty BLOB
        13. SUBSTR boundary: position 0, out-of-range, huge length
        14. LENGTH on BLOB: normal/empty/NULL/single-byte
        15. BLOB in WHERE clause with CAST
        16. Aggregate functions with LENGTH(BLOB)
        17. GROUP BY with BLOB substr/cast (expect error)
        18. CAST DECIMAL constants and columns to BLOB/VARCHAR with NULL and boundary values

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-03-20 Created BLOB cast/substr/length function tests
            - 2026-10-21 Cover DECIMAL formatting with different output capacities

        """
        self._prepare_db()
        self.cast_varchar_to_blob()
        self.cast_varchar_literal_to_blob()
        self.cast_blob_to_varchar()
        self.cast_blob_to_blob()
        self.cast_blob_to_int()
        self.cast_blob_to_float()
        self.cast_blob_to_bool()
        self.cast_numeric_to_blob()
        self.cast_decimal_to_blob()
        self.cast_blob_to_nchar()
        self.cast_blob_to_binary()
        self.cast_null_blob()
        self.substr_blob_positive()
        self.substr_blob_negative()
        self.substr_blob_null()
        self.substr_blob_empty()
        self.substr_blob_boundary()
        self.length_blob()
        self.blob_where_with_cast()
        self.blob_aggregate()
        self.blob_group_by_error()

        tdSql.execute(f"drop database if exists {DB}")
