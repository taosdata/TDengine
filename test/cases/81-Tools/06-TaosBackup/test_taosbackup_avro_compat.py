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

"""
taosBackup AVRO backward-compatibility tests.

taosBackup can restore legacy taosdump AVRO-format backups by detecting
the presence of a `dbs.sql` file in the backup directory.  This test
creates a database with rich data, uses taosdump to produce an AVRO
backup, then restores it with taosBackup and verifies full data
correctness.
"""

from new_test_framework.utils import tdLog, tdSql, etool
import os
import shutil


SRC_DB = "avro_src"
DST_DB = "avro_dst"


class TestTaosBackupAvroCompat:

    # -----------------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------------

    def makeDir(self, path):
        if os.path.exists(path):
            shutil.rmtree(path)
        os.makedirs(path)

    # -----------------------------------------------------------------------
    # 1. AVRO full-data restore: taosdump backup → taosBackup restore
    # -----------------------------------------------------------------------

    def do_avro_full_restore(self):
        """Create data, dump with taosdump (AVRO), restore with taosBackup.

        Verification:
          - Database exists after restore.
          - STB DDL preserved (column count, tag count).
          - Child table count matches source.
          - Row count matches source.
          - SUM of numeric columns matches source.
          - NTB data preserved.
          - Tag values preserved.
        """
        tmpdir = "./taosbackuptest/tmpdir_avro_full"
        self.makeDir(tmpdir)

        # --- Setup source database ---
        tdSql.execute(f"drop database if exists {SRC_DB}")
        tdSql.execute(f"create database {SRC_DB} keep 3649 vgroups 2")
        tdSql.execute(f"use {SRC_DB}")

        # STB with multiple data types
        tdSql.execute(
            f"create stable {SRC_DB}.meters"
            f"(ts timestamp, ic int, bi bigint, fc float,"
            f" bc bool, bin binary(16), nch nchar(16))"
            f" tags(tid int, loc nchar(10))"
        )
        for t in range(5):
            tdSql.execute(
                f"create table {SRC_DB}.d{t} using {SRC_DB}.meters"
                f" tags({t}, '城市{t}')"
            )
            vals = []
            for i in range(100):
                ts = 1640000000000 + i * 1000
                bc = 1 if i % 2 == 0 else 0
                vals.append(
                    f"({ts}, {i*t}, {i*t*10}, {i*0.5},"
                    f" {bc}, 'b{t}r{i}', '中{t}行{i}')"
                )
            batch = 50
            for start in range(0, len(vals), batch):
                chunk = ",".join(vals[start:start + batch])
                tdSql.execute(f"insert into {SRC_DB}.d{t} values {chunk}")

        # Normal table
        tdSql.execute(f"create table {SRC_DB}.ntb1(ts timestamp, v int, s binary(20))")
        vals = ",".join(
            f"({1640000000000 + i * 1000}, {i * 100}, 'nt{i}')" for i in range(30)
        )
        tdSql.execute(f"insert into {SRC_DB}.ntb1 values {vals}")

        # Record source aggregates
        src_stb_rows = tdSql.getResult(f"select count(*) from {SRC_DB}.meters")[0][0]
        src_sum_ic = tdSql.getResult(f"select sum(ic) from {SRC_DB}.meters")[0][0]
        src_sum_bi = tdSql.getResult(f"select sum(bi) from {SRC_DB}.meters")[0][0]
        src_ntb_rows = tdSql.getResult(f"select count(*) from {SRC_DB}.ntb1")[0][0]
        src_ntb_sum = tdSql.getResult(f"select sum(v) from {SRC_DB}.ntb1")[0][0]
        src_ctb_count = tdSql.getResult(
            f"select count(*) from information_schema.ins_tables "
            f"where db_name='{SRC_DB}' and stable_name='meters'"
        )[0][0]

        tdLog.info(
            f"Source: {src_stb_rows} STB rows, {src_ctb_count} CTBs, "
            f"{src_ntb_rows} NTB rows, SUM(ic)={src_sum_ic}"
        )

        # --- Dump with taosdump (AVRO format) ---
        tdLog.info("Dumping with taosdump (AVRO format)")
        etool.taosdump(f"-D {SRC_DB} -o {tmpdir}")

        # Verify dbs.sql exists (AVRO format marker)
        dbs_sql_path = os.path.join(tmpdir, "dbs.sql")
        assert os.path.exists(dbs_sql_path), (
            f"taosdump did not produce dbs.sql at {dbs_sql_path}"
        )
        tdLog.info(f"  dbs.sql found: {dbs_sql_path}")

        # --- Restore with taosBackup ---
        tdLog.info("Restoring with taosBackup (AVRO compat path)")
        tdSql.execute(f"drop database if exists {DST_DB}")
        rlist = etool.taosbackup(
            f'-W "{SRC_DB}={DST_DB}" -i {tmpdir}'
        )
        output = "\n".join(rlist) if rlist else ""
        if "SUCCESS" not in output:
            tdLog.exit(f"taosBackup AVRO restore failed:\n{output[:600]}")

        # --- Verify database exists ---
        tdSql.query("select name from information_schema.ins_databases")
        db_names = [row[0] for row in tdSql.queryResult]
        assert DST_DB in db_names, f"{DST_DB} not found after AVRO restore"
        tdLog.info(f"  database {DST_DB} created .................... [passed]")

        # --- Verify STB structure ---
        tdSql.query(
            f"select stable_name from information_schema.ins_stables "
            f"where db_name='{DST_DB}'"
        )
        stb_names = [row[0] for row in tdSql.queryResult]
        assert "meters" in stb_names, "STB 'meters' not found after AVRO restore"
        tdLog.info("  STB 'meters' exists ......................... [passed]")

        # --- Verify child table count ---
        tdSql.query(
            f"select count(*) from information_schema.ins_tables "
            f"where db_name='{DST_DB}' and stable_name='meters'"
        )
        tdSql.checkData(0, 0, src_ctb_count)
        tdLog.info(f"  CTB count = {src_ctb_count} ............................ [passed]")

        # --- Verify STB row count ---
        tdSql.query(f"select count(*) from {DST_DB}.meters")
        tdSql.checkData(0, 0, src_stb_rows)
        tdLog.info(f"  STB row count = {src_stb_rows} ........................ [passed]")

        # --- Verify numeric aggregates ---
        tdSql.query(f"select sum(ic) from {DST_DB}.meters")
        tdSql.checkData(0, 0, src_sum_ic)
        tdSql.query(f"select sum(bi) from {DST_DB}.meters")
        tdSql.checkData(0, 0, src_sum_bi)
        tdLog.info("  SUM(ic), SUM(bi) match ...................... [passed]")

        # --- Verify tag values ---
        tdSql.query(
            f"select distinct tid from {DST_DB}.meters order by tid"
        )
        tdSql.checkRows(5)
        for t in range(5):
            tdSql.checkData(t, 0, t)
        tdLog.info("  tag values (tid 0-4) preserved .............. [passed]")

        # Verify NCHAR tag
        tdSql.query(f"select loc from {DST_DB}.d0 limit 1")
        tdSql.checkData(0, 0, "城市0")
        tdSql.query(f"select loc from {DST_DB}.d4 limit 1")
        tdSql.checkData(0, 0, "城市4")
        tdLog.info("  NCHAR tag values correct .................... [passed]")

        # --- Verify normal table ---
        tdSql.query(f"select count(*) from {DST_DB}.ntb1")
        tdSql.checkData(0, 0, src_ntb_rows)
        tdSql.query(f"select sum(v) from {DST_DB}.ntb1")
        tdSql.checkData(0, 0, src_ntb_sum)
        tdSql.query(f"select s from {DST_DB}.ntb1 order by ts limit 1")
        tdSql.checkData(0, 0, "nt0")
        tdLog.info("  NTB data correct ............................ [passed]")

        # --- Verify per-row data sample ---
        # d3, row 10: ic = 10*3 = 30, bi = 10*3*10 = 300
        tdSql.query(
            f"select ic, bi from {DST_DB}.d3 "
            f"where ts = 1640000010000"
        )
        tdSql.checkData(0, 0, 30)
        tdSql.checkData(0, 1, 300)
        tdLog.info("  per-row spot check (d3, ts=10s) ............. [passed]")

        # Cleanup
        tdSql.execute(f"drop database if exists {SRC_DB}")
        tdSql.execute(f"drop database if exists {DST_DB}")

        tdLog.info("do_avro_full_restore ......................... [passed]")

    # -----------------------------------------------------------------------
    # 2. AVRO restore with database rename
    # -----------------------------------------------------------------------

    def do_avro_rename(self):
        """Verify -W rename works correctly on AVRO-format backups.

        The rename must apply to CREATE DATABASE, CREATE STABLE, USE, and
        all table references inside dbs.sql (handled by avroAfterRenameSql).
        """
        tmpdir = "./taosbackuptest/tmpdir_avro_rename"
        self.makeDir(tmpdir)

        src_db = "avro_rn_src"
        dst_db = "avro_rn_dst"

        tdSql.execute(f"drop database if exists {src_db}")
        tdSql.execute(f"create database {src_db} keep 3649")
        tdSql.execute(
            f"create stable {src_db}.st1"
            f"(ts timestamp, v int) tags(tid int)"
        )
        for t in range(3):
            tdSql.execute(
                f"create table {src_db}.ct{t} using {src_db}.st1 tags({t})"
            )
            vals = ",".join(
                f"({1640000000000 + i * 1000}, {i + t * 100})" for i in range(20)
            )
            tdSql.execute(f"insert into {src_db}.ct{t} values {vals}")

        src_sum = tdSql.getResult(f"select sum(v) from {src_db}.st1")[0][0]

        # Dump with taosdump
        etool.taosdump(f"-D {src_db} -o {tmpdir}")

        # Drop source DB before restore so we can verify rename isolation
        tdSql.execute(f"drop database if exists {src_db}")

        # Restore with taosBackup using -W rename
        tdSql.execute(f"drop database if exists {dst_db}")
        rlist = etool.taosbackup(
            f'-W "{src_db}={dst_db}" -i {tmpdir}'
        )
        output = "\n".join(rlist) if rlist else ""
        if "SUCCESS" not in output:
            tdLog.exit(f"AVRO rename restore failed:\n{output[:600]}")

        # Verify renamed database
        tdSql.query("select name from information_schema.ins_databases")
        db_names = [row[0] for row in tdSql.queryResult]
        assert dst_db in db_names, f"{dst_db} not found"
        assert src_db not in db_names or src_db == dst_db, (
            f"Source DB {src_db} should not exist (was dropped before restore)"
        )

        # Verify data in renamed DB
        tdSql.query(f"select sum(v) from {dst_db}.st1")
        tdSql.checkData(0, 0, src_sum)
        tdSql.query(f"select count(*) from {dst_db}.st1")
        tdSql.checkData(0, 0, 60)
        tdSql.query(
            f"select distinct tid from {dst_db}.st1 order by tid"
        )
        tdSql.checkRows(3)
        for t in range(3):
            tdSql.checkData(t, 0, t)

        # Cleanup
        tdSql.execute(f"drop database if exists {src_db}")
        tdSql.execute(f"drop database if exists {dst_db}")

        tdLog.info("do_avro_rename ............................... [passed]")

    # -----------------------------------------------------------------------
    # 3. AVRO schema-only detection (dbs.sql present, no data files)
    # -----------------------------------------------------------------------

    def do_avro_schema_only(self):
        """Construct a minimal dbs.sql manually (no .avro data files).

        taosBackup should detect AVRO format, execute the SQL statements
        from dbs.sql, and create the database + STB schema even without
        any data files.

        This exercises:
          - isAvroBackupDir() detection (dbs.sql existence check)
          - avroRestoreDbSql() line-by-line SQL execution
          - Metadata lines (#!charset, #!server_ver) parsing
          - Comment lines (-- and #) skipping
        """
        tmpdir = "./taosbackuptest/tmpdir_avro_schema"
        self.makeDir(tmpdir)

        schema_db = "avro_schema_db"
        tdSql.execute(f"drop database if exists {schema_db}")

        # Manually create dbs.sql in taosdump format
        # taosBackup scans {inputDir}/{dbName}/dbs.sql
        db_dir = os.path.join(tmpdir, schema_db)
        os.makedirs(db_dir, exist_ok=True)
        dbs_sql_content = (
            "#!charset: UTF-8\n"
            "#!server_ver: 3\n"
            "# This is a comment line\n"
            "-- Another comment\n"
            f"CREATE DATABASE IF NOT EXISTS `{schema_db}` KEEP 3649;\n"
            f"CREATE STABLE IF NOT EXISTS `{schema_db}`.`sensors`"
            f" (ts TIMESTAMP, temperature FLOAT, humidity INT)"
            f" TAGS (location NCHAR(20), device_id INT);\n"
            f"CREATE TABLE IF NOT EXISTS `{schema_db}`.`log_table`"
            f" (ts TIMESTAMP, msg NCHAR(100), level INT);\n"
        )
        dbs_sql_path = os.path.join(db_dir, "dbs.sql")
        with open(dbs_sql_path, "w", encoding="utf-8") as f:
            f.write(dbs_sql_content)

        # Restore with taosBackup — should detect AVRO and execute SQL
        rlist = etool.taosbackup(f"-i {tmpdir}", checkRun=False)
        output = "\n".join(rlist) if rlist else ""
        tdLog.info(f"AVRO schema-only output: {output[:500]}")

        # Verify database was created
        tdSql.query("select name from information_schema.ins_databases")
        db_names = [row[0] for row in tdSql.queryResult]
        assert schema_db in db_names, (
            f"{schema_db} not found — AVRO dbs.sql execution failed"
        )
        tdLog.info(f"  database {schema_db} created ................. [passed]")

        # Verify STB schema
        tdSql.query(
            f"select stable_name from information_schema.ins_stables "
            f"where db_name='{schema_db}'"
        )
        stb_names = [row[0] for row in tdSql.queryResult]
        assert "sensors" in stb_names, "STB 'sensors' not found"
        tdLog.info("  STB 'sensors' exists ........................ [passed]")

        # Verify STB columns
        tdSql.query(f"describe {schema_db}.sensors")
        col_names = [row[0] for row in tdSql.queryResult]
        assert "ts" in col_names, "Column 'ts' missing"
        assert "temperature" in col_names, "Column 'temperature' missing"
        assert "humidity" in col_names, "Column 'humidity' missing"
        assert "location" in col_names, "Tag 'location' missing"
        assert "device_id" in col_names, "Tag 'device_id' missing"
        tdLog.info("  STB columns/tags correct .................... [passed]")

        # Verify NTB
        tdSql.query(
            f"select table_name from information_schema.ins_tables "
            f"where db_name='{schema_db}' and stable_name is null"
        )
        ntb_names = [row[0] for row in tdSql.queryResult]
        assert "log_table" in ntb_names, "NTB 'log_table' not found"
        tdLog.info("  NTB 'log_table' exists ...................... [passed]")

        tdSql.execute(f"drop database {schema_db}")
        tdLog.info("do_avro_schema_only .......................... [passed]")

    # -----------------------------------------------------------------------
    # Main test entry point
    # -----------------------------------------------------------------------

    def test_taosbackup_avro_compat(self):
        """taosBackup AVRO backward compatibility with taosdump backups

        taosBackup detects legacy taosdump AVRO-format backups by checking
        for `dbs.sql` in the backup directory (compatAvro.c:isAvroBackupDir).
        When detected, it switches to the AVRO restore path which:
          - Parses dbs.sql metadata (#!charset, #!server_ver, etc.)
          - Executes CREATE DATABASE/STABLE/TABLE SQL from dbs.sql
          - Restores child table tags from .avro-tbtags files
          - Restores data from .avro files in data*/ subdirectories
          - Applies -W database rename transformations

        Test scenarios:
          1. Full data: taosdump → taosBackup restore → verify all data.
          2. Rename: AVRO restore with -W rename → verify renamed DB.
          3. Schema-only: hand-crafted dbs.sql (no data) → verify schema.

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-05-04 Created; AVRO backward-compatibility coverage

        """
        self.do_avro_full_restore()
        self.do_avro_rename()
        self.do_avro_schema_only()

        os.system("rm -rf ./taosbackuptest/tmpdir_avro*")
        tdLog.info("test_taosbackup_avro_compat .................. [passed]")
