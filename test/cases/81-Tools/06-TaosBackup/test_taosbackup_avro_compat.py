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
uses pre-generated taosdump AVRO backup fixtures (produced once with the
legacy `old_taosdump` binary at /root/TDinternal/debug/build/bin/old_taosdump
and stored under data/) and verifies that taosBackup can fully restore
them with correct data and schema.  No live taosdump invocation is needed.
"""

from new_test_framework.utils import tdLog, tdSql, etool
import os
import shutil

# Destination database names used during restore
DST_DB = "avro_dst"

# DB names as recorded inside the pre-generated AVRO fixtures
_AVRO_FULL_ORIG_DB = "avro_compat_full"
_AVRO_RENAME_ORIG_DB = "avro_compat_rn"

# Absolute path to the fixture data directories (next to this test file)
_HERE = os.path.dirname(os.path.abspath(__file__))
_AVRO_FULL_DIR = os.path.join(_HERE, "data", "avro_full")
_AVRO_RENAME_DIR = os.path.join(_HERE, "data", "avro_rename")

# Expected aggregate values derived from the fixture data:
#   meters STB: 5 child tables (d0-d4), 100 rows each
#     ic = i*t, bi = i*t*10 for t in 0..4, i in 0..99
#   ntb1: 30 rows, v = i*100 for i in 0..29
_FULL_STB_ROWS = 500
_FULL_CTB_COUNT = 5
_FULL_SUM_IC = 49500        # sum(t=0..4) t * sum(i=0..99) i = 10 * 4950
_FULL_SUM_BI = 495000       # 10 * _FULL_SUM_IC
_FULL_NTB_ROWS = 30
_FULL_NTB_SUM = 43500       # 100 * sum(i=0..29) i = 100 * 435

# Expected values for the rename fixture:
#   st1 STB: 3 child tables (ct0-ct2), 20 rows each
#     v = i + t*100 for t in 0..2, i in 0..19
_RENAME_TOTAL_ROWS = 60
_RENAME_SUM_V = 6570        # sum(t=0..2) [190 + 2000*t] = 190+2190+4190


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
        """Restore a pre-generated taosdump AVRO backup with taosBackup.

        The AVRO fixture was produced once with the legacy old_taosdump binary
        and is stored under data/avro_full/.  This test skips data generation
        and starts directly from the pre-backed-up AVRO data.

        Verification:
          - Database exists after restore.
          - STB DDL preserved (column count, tag count).
          - Child table count matches expected fixture value.
          - Row count matches expected fixture value.
          - SUM of numeric columns matches expected fixture value.
          - NTB data preserved.
          - Tag values preserved.
        """
        # Verify that the fixture directory exists before proceeding
        assert os.path.isdir(_AVRO_FULL_DIR), (
            f"AVRO full fixture directory not found: {_AVRO_FULL_DIR}"
        )
        assert os.path.exists(os.path.join(_AVRO_FULL_DIR, "dbs.sql")), (
            f"dbs.sql missing from fixture: {_AVRO_FULL_DIR}"
        )

        # --- Restore with taosBackup ---
        tdLog.info(f"Restoring AVRO fixture from {_AVRO_FULL_DIR}")
        tdSql.execute(f"drop database if exists {DST_DB}")
        rlist = etool.taosdump(
            f'-W "{_AVRO_FULL_ORIG_DB}={DST_DB}" -i {_AVRO_FULL_DIR}'
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
        tdSql.checkData(0, 0, _FULL_CTB_COUNT)
        tdLog.info(f"  CTB count = {_FULL_CTB_COUNT} ............................ [passed]")

        # --- Verify STB row count ---
        tdSql.query(f"select count(*) from {DST_DB}.meters")
        tdSql.checkData(0, 0, _FULL_STB_ROWS)
        tdLog.info(f"  STB row count = {_FULL_STB_ROWS} ........................ [passed]")

        # --- Verify numeric aggregates ---
        tdSql.query(f"select sum(ic) from {DST_DB}.meters")
        tdSql.checkData(0, 0, _FULL_SUM_IC)
        tdSql.query(f"select sum(bi) from {DST_DB}.meters")
        tdSql.checkData(0, 0, _FULL_SUM_BI)
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
        tdSql.checkData(0, 0, _FULL_NTB_ROWS)
        tdSql.query(f"select sum(v) from {DST_DB}.ntb1")
        tdSql.checkData(0, 0, _FULL_NTB_SUM)
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
        tdSql.execute(f"drop database if exists {DST_DB}")

        tdLog.info("do_avro_full_restore ......................... [passed]")

    # -----------------------------------------------------------------------
    # 2. AVRO restore with database rename
    # -----------------------------------------------------------------------

    def do_avro_rename(self):
        """Verify -W rename works correctly on AVRO-format backups.

        The AVRO fixture was produced once with the legacy old_taosdump binary
        and is stored under data/avro_rename/.  This test skips data generation
        and starts directly from the pre-backed-up AVRO data.

        The rename must apply to CREATE DATABASE, CREATE STABLE, USE, and
        all table references inside dbs.sql (handled by avroAfterRenameSql).
        """
        dst_db = "avro_rn_dst"

        # Verify fixture exists
        assert os.path.isdir(_AVRO_RENAME_DIR), (
            f"AVRO rename fixture directory not found: {_AVRO_RENAME_DIR}"
        )

        # Restore with taosBackup using -W rename
        tdSql.execute(f"drop database if exists {dst_db}")
        tdSql.execute(f"drop database if exists {_AVRO_RENAME_ORIG_DB}")
        rlist = etool.taosdump(
            f'-W "{_AVRO_RENAME_ORIG_DB}={dst_db}" -i {_AVRO_RENAME_DIR}'
        )
        output = "\n".join(rlist) if rlist else ""
        if "SUCCESS" not in output:
            tdLog.exit(f"AVRO rename restore failed:\n{output[:600]}")

        # Verify renamed database exists; original DB name must NOT appear
        tdSql.query("select name from information_schema.ins_databases")
        db_names = [row[0] for row in tdSql.queryResult]
        assert dst_db in db_names, f"{dst_db} not found"
        assert _AVRO_RENAME_ORIG_DB not in db_names, (
            f"Original DB {_AVRO_RENAME_ORIG_DB} should not exist after rename restore"
        )

        # Verify data in renamed DB
        tdSql.query(f"select sum(v) from {dst_db}.st1")
        tdSql.checkData(0, 0, _RENAME_SUM_V)
        tdSql.query(f"select count(*) from {dst_db}.st1")
        tdSql.checkData(0, 0, _RENAME_TOTAL_ROWS)
        tdSql.query(
            f"select distinct tid from {dst_db}.st1 order by tid"
        )
        tdSql.checkRows(3)
        for t in range(3):
            tdSql.checkData(t, 0, t)

        # Cleanup
        tdSql.execute(f"drop database if exists {dst_db}")

        tdLog.info("do_avro_rename ............................... [passed]")

    # -----------------------------------------------------------------------
    # 3. AVRO + --content=ext-meta alone must be rejected (no stage1 run)
    # -----------------------------------------------------------------------

    def do_avro_extmeta_only_rejected(self):
        """--content=ext-meta alone against an AVRO backup must fail loudly.

        AVRO-format backups have no separate vtb.sql/stream.sql/topic.sql —
        everything (including virtual tables) is restored inside the AVRO
        stage-1 path.  If the user asks for --content=ext-meta alone (stage1
        never runs for this db), restoreDatabaseExtMetaOne() must reject the
        request instead of silently doing nothing (restore.c).
        """
        dst_db = "avro_extmeta_only_dst"
        tdSql.execute(f"drop database if exists {dst_db}")
        tdSql.execute(f"drop database if exists {_AVRO_FULL_ORIG_DB}")

        rlist = etool.taosdump(
            f'--content=ext-meta -W "{_AVRO_FULL_ORIG_DB}={dst_db}" -i {_AVRO_FULL_DIR}',
            checkRun=False,
        )
        output = "\n".join(rlist) if rlist else ""
        assert "cannot restore it standalone" in output, (
            f"expected AVRO ext-meta-only rejection message not found:\n{output[:600]}"
        )
        tdLog.info("  rejection message present .................... [passed]")

        # Must NOT have created the destination database
        tdSql.query("select name from information_schema.ins_databases")
        db_names = [row[0] for row in tdSql.queryResult]
        assert dst_db not in db_names, (
            f"{dst_db} should not exist after rejected ext-meta-only AVRO restore"
        )
        tdLog.info("  destination database not created ............. [passed]")

        tdLog.info("do_avro_extmeta_only_rejected ................ [passed]")

    # -----------------------------------------------------------------------
    # 4. AVRO schema-only detection (dbs.sql present, no data files)
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
        rlist = etool.taosdump(f"-i {tmpdir}", checkRun=False)
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

        Fixture data (under data/avro_full/ and data/avro_rename/) was
        generated once with the legacy old_taosdump binary.  No live
        taosdump invocation is performed during this test.

        Test scenarios:
          1. Full data: pre-generated AVRO backup → taosBackup restore → verify all data.
          2. Rename: AVRO restore with -W rename → verify renamed DB.
          3. --content=ext-meta alone against an AVRO backup must be rejected
             (stage1 never ran, so there's nothing to attach ext-meta to).
          4. Schema-only: hand-crafted dbs.sql (no data) → verify schema.

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-05-04 Created; AVRO backward-compatibility coverage
            - 2026-05-10 Refactored; use pre-generated AVRO fixtures instead of
                         live old_taosdump invocation (old_taosdump no longer
                         compiled in CI)
            - 2026-08-12 Added ext-meta-only rejection coverage for
                         restoreDatabaseExtMetaOne() (restore.c)

        """
        self.do_avro_full_restore()
        self.do_avro_rename()
        self.do_avro_extmeta_only_rejected()
        self.do_avro_schema_only()

        tdLog.info("test_taosbackup_avro_compat .................. [passed]")
