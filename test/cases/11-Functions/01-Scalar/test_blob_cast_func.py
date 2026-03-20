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
        tdSql.checkData(0, 1, "hello world")

        tdSql.query(f"select id, cast(vc as blob) from {DB}.tstr where id=2")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "TDengine")

        tdSql.query(f"select id, cast(vc as blob) from {DB}.tstr where id=3")
        tdSql.checkRows(1)

    def cast_varchar_literal_to_blob(self):
        tdLog.info("cast varchar literal to blob")
        tdSql.query("select cast('literal test' as blob)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "literal test")

        tdSql.query("select cast('hello' as blob)")
        tdSql.checkData(0, 0, "hello")

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
        tdSql.checkData(0, 1, "hello world")

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
        tdSql.checkData(0, 0, "42")

        tdSql.query(f"select cast(v_big as blob) from {DB}.tnum where id=1")
        tdSql.checkData(0, 0, "100")

        tdSql.query(f"select cast(v_bool as blob) from {DB}.tnum where id=1")
        tdSql.checkData(0, 0, "true")

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
        tdSql.checkData(0, 0, "hello")

        tdSql.query(f"select substr(b_data, 7, 5) from {DB}.tsub_pos where id=1")
        tdSql.checkData(0, 0, "world")

        tdSql.query(f"select substr(b_data, 1, 8) from {DB}.tsub_pos where id=2")
        tdSql.checkData(0, 0, "TDengine")

    def substr_blob_negative(self):
        tdLog.info("substr on blob with negative position")
        self._create_blob_table("tsub_neg")
        tdSql.execute(f"insert into {DB}.tsub_neg values ('2025-01-01 00:00:01', 1, 'hello world', 'a')")

        tdSql.query(f"select substr(b_data, -5) from {DB}.tsub_neg where id=1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "world")

        tdSql.query(f"select substr(b_data, -11) from {DB}.tsub_neg where id=1")
        tdSql.checkData(0, 0, "hello world")

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
        tdSql.checkData(0, 0, "hello world")

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

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-20 Created BLOB cast/substr/length function tests

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
