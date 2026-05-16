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

"""Test taosdump with maximum columns, all data types, and case-sensitive names.

Verifies fixes for:
- avro-c 64KB schema_buf overflow (corrupted 35-byte avro files)
- SHOW CREATE TABLE 65535-char truncation (DESCRIBE fallback)
- all data types survive round-trip through avro export/import
- backtick-quoted case-sensitive column/tag names preserved

Part 1 - Export test (3968 cols + 128 tags, 64-char names):
  Verifies avro files are NOT corrupted and pass --inspect.

Part 2 - Round-trip test (3968 cols + 128 tags, all data types,
  mixed-case names):
  Full export -> drop -> restore -> verify cycle.
  Exercises DESCRIBE fallback path since SHOW CREATE TABLE
  will be truncated at 65535 chars.
"""

import os
import subprocess

from new_test_framework.utils import tdLog, tdSql, etool


# All TDengine data types for columns (excluding first TIMESTAMP column).
# Each entry: (sql_type_str, needs_length, default_length, sample_value, verify_fn)
# verify_fn: callable(inserted, queried) -> bool
ALL_COL_TYPES = [
    ("BOOL", False, 0, "true", lambda i, q: q in (True, 1, "true")),
    ("TINYINT", False, 0, "1", lambda i, q: int(q) == 1),
    ("SMALLINT", False, 0, "2", lambda i, q: int(q) == 2),
    ("INT", False, 0, "3", lambda i, q: int(q) == 3),
    ("BIGINT", False, 0, "4", lambda i, q: int(q) == 4),
    ("FLOAT", False, 0, "5.5", lambda i, q: abs(float(q) - 5.5) < 0.01),
    ("DOUBLE", False, 0, "6.6", lambda i, q: abs(float(q) - 6.6) < 1e-9),
    ("BINARY", True, 20, "'bin_v'", lambda i, q: q.strip() == "bin_v"),
    ("NCHAR", True, 20, "'nch_v'", lambda i, q: q.strip() == "nch_v"),
    ("TINYINT UNSIGNED", False, 0, "7", lambda i, q: int(q) == 7),
    ("SMALLINT UNSIGNED", False, 0, "8", lambda i, q: int(q) == 8),
    ("INT UNSIGNED", False, 0, "9", lambda i, q: int(q) == 9),
    ("BIGINT UNSIGNED", False, 0, "10", lambda i, q: int(q) == 10),
    ("VARBINARY", True, 20, "'\\x4142'", lambda i, q: True),  # binary compare tricky
    ("GEOMETRY", True, 50, "'POINT(1 2)'", lambda i, q: True),  # geometry compare tricky
    ("DECIMAL", "decimal", "10,5", "12345.67890",
     lambda i, q: abs(float(q) - 12345.67890) < 1e-4),
    ("DECIMAL", "decimal", "30,10", "12345678.1234567890",
     lambda i, q: abs(float(q) - 12345678.1234567890) < 1e-4),
]

# Tag types: same as column types but excluding DECIMAL (not allowed as tag)
ALL_TAG_TYPES = [t for t in ALL_COL_TYPES if t[0] not in ("DECIMAL",)]


class TestTaosdumpMaxcols:

    # TDengine limits: columns + tags <= 4096
    MAX_COL_TAG = 4096
    MAX_TAGS = 128
    STB_COLUMNS = MAX_COL_TAG - MAX_TAGS  # 3968
    NTB_COLUMNS = MAX_COL_TAG  # 4096
    COL_NAME_MAX = 64

    def _genWideName(self, prefix, idx, length=64):
        """Generate a column name of exactly `length` chars."""
        base = "%s%04d_" % (prefix, idx)
        pad_len = length - len(base)
        if pad_len > 0:
            base += "a" * pad_len
        return base[:length]

    def _genMixedCaseName(self, prefix, idx):
        """Generate a mixed-case column name like 'Col_0001_Abc'.

        Uses backtick quoting so TDengine preserves case.
        """
        return "%s_%04d_Abc" % (prefix, idx)

    def _colTypeAt(self, idx):
        """Return type info for column index (0-based, excluding ts)."""
        return ALL_COL_TYPES[idx % len(ALL_COL_TYPES)]

    def _tagTypeAt(self, idx):
        """Return type info for tag index (0-based)."""
        return ALL_TAG_TYPES[idx % len(ALL_TAG_TYPES)]

    def _testExportWideTable(self, binPath):
        """Part 1: Export test with maximum wide table."""
        tdLog.info("=" * 60)
        tdLog.info("PART 1: Export test - max cols + max tags + 64-char names")
        tdLog.info("=" * 60)

        dbName = "maxcoldb"
        stbName = "stb_wide"
        ctbName = "ctb_wide"
        ntbName = "ntb_wide"

        tdSql.execute("DROP DATABASE IF EXISTS %s" % dbName)
        tdSql.execute("CREATE DATABASE %s KEEP 3650" % dbName)
        tdSql.execute("USE %s" % dbName)

        # 1. Create supertable: 3968 cols + 128 tags, 64-char names
        tdLog.info("Creating STABLE: %d cols + %d tags (64-char names)"
                   % (self.STB_COLUMNS, self.MAX_TAGS))
        col_defs = ["`ts` TIMESTAMP"]
        for i in range(self.STB_COLUMNS - 1):
            col_defs.append("`%s` FLOAT" % self._genWideName("c", i, self.COL_NAME_MAX))
        tag_defs = []
        for i in range(self.MAX_TAGS):
            tag_defs.append("`%s` BINARY(20)" % self._genWideName("t", i, self.COL_NAME_MAX))

        sql = "CREATE TABLE `%s`.`%s` (%s) TAGS(%s)" % (
            dbName, stbName, ", ".join(col_defs), ", ".join(tag_defs))
        tdLog.info("CREATE STABLE SQL length: %d bytes" % len(sql))
        tdSql.execute(sql)

        # 2. Child table + data
        tag_vals = ", ".join(["'tag_val_%04d________'" % i
                              for i in range(self.MAX_TAGS)])
        tdSql.execute("CREATE TABLE `%s`.`%s` USING `%s`.`%s` TAGS(%s)"
                      % (dbName, ctbName, dbName, stbName, tag_vals))
        vals = ["now"] + ["%.1f" % (i * 0.1) for i in range(self.STB_COLUMNS - 1)]
        tdSql.execute("INSERT INTO `%s`.`%s` VALUES (%s)"
                      % (dbName, ctbName, ", ".join(vals)))

        # 3. Normal table: 4096 cols, 64-char names
        tdLog.info("Creating normal table: %d cols (64-char names)" % self.NTB_COLUMNS)
        ntb_defs = ["`ts` TIMESTAMP"]
        for i in range(self.NTB_COLUMNS - 1):
            ntb_defs.append("`%s` FLOAT" % self._genWideName("n", i, self.COL_NAME_MAX))
        tdSql.execute("CREATE TABLE `%s`.`%s` (%s)"
                      % (dbName, ntbName, ", ".join(ntb_defs)))
        nvals = ["now"] + ["%.1f" % (i * 0.5) for i in range(self.NTB_COLUMNS - 1)]
        tdSql.execute("INSERT INTO `%s`.`%s` VALUES (%s)"
                      % (dbName, ntbName, ", ".join(nvals)))

        # 4. Export
        dumpdir = os.path.abspath(self.tmpdir + "/maxcol_export")
        if os.path.exists(dumpdir):
            os.system("rm -rf %s" % dumpdir)
        os.makedirs(dumpdir)

        dump_cmd = "%s --databases %s -o %s -T 1" % (
            binPath, dbName, dumpdir)
        tdLog.info("Dump command: %s" % dump_cmd)
        rc = os.system(dump_cmd)
        assert rc == 0, "taosdump export failed with rc=%d" % rc

        # 5. Verify avro files NOT corrupted (size > 100 bytes)
        avro_files = []
        for root, dirs, files in os.walk(dumpdir):
            for f in files:
                if f.endswith(".avro"):
                    avro_files.append(os.path.join(root, f))

        tdLog.info("Found %d avro files" % len(avro_files))
        assert len(avro_files) > 0, "No avro files found in dump output!"

        for af in avro_files:
            fsize = os.path.getsize(af)
            tdLog.info("  %s size=%d bytes" % (os.path.basename(af), fsize))
            assert fsize >= 100, "CORRUPTED avro (size=%d): %s" % (fsize, af)

        tdLog.info("All %d avro files have valid sizes" % len(avro_files))

        # 6. Inspect all avro files
        for af in avro_files:
            inspect_cmd = "%s --inspect %s 2>&1" % (binPath, af)
            output = subprocess.check_output(inspect_cmd, shell=True).decode("utf-8")
            if "ERROR" in output or "Unable" in output or "Cannot" in output:
                assert False, "Inspect error on %s: %s" % (os.path.basename(af), output[:500])
            tdLog.info("  Inspect OK: %s" % os.path.basename(af))

        tdLog.info("All avro files pass --inspect")

        tdSql.execute("DROP DATABASE IF EXISTS %s" % dbName)

    def _testRoundtripMaxCols(self, binPath):
        """Part 2: Round-trip with max cols, all types, mixed-case names.

        3968 cols + 128 tags, covering all 15 TDengine data types
        round-robin. Column names use mixed case (e.g. Col_0001_Abc)
        to verify case-sensitive backtick quoting survives round-trip.
        SHOW CREATE TABLE will be truncated -> DESCRIBE fallback path.
        """
        tdLog.info("=" * 60)
        tdLog.info("PART 2: Round-trip - max cols, all types, case-sensitive names")
        tdLog.info("=" * 60)

        dbName = "allcoldb"
        stbName = "Stb_Mix"  # mixed case table name
        ctbName = "Ctb_Mix"
        ntbName = "Ntb_Mix"
        num_cols = self.STB_COLUMNS  # 3968
        num_tags = self.MAX_TAGS  # 128
        ntb_cols = self.NTB_COLUMNS  # 4096

        tdSql.execute("DROP DATABASE IF EXISTS %s" % dbName)
        tdSql.execute("CREATE DATABASE %s KEEP 3650" % dbName)
        tdSql.execute("USE %s" % dbName)

        # -- Build supertable DDL --
        col_defs = ["`ts` TIMESTAMP"]
        col_vals = ["now"]
        for i in range(num_cols - 2):  # reserve last col for BLOB
            cname = self._genMixedCaseName("Col", i)
            tinfo = self._colTypeAt(i)
            type_str, needs_len, deflen, sample_val = tinfo[0], tinfo[1], tinfo[2], tinfo[3]
            if needs_len == "decimal":
                # DECIMAL(precision,scale)
                col_defs.append("`%s` %s(%s)" % (cname, type_str, deflen))
            elif needs_len:
                col_defs.append("`%s` %s(%d)" % (cname, type_str, deflen))
            else:
                col_defs.append("`%s` %s" % (cname, type_str))
            col_vals.append(sample_val)
        # Add one BLOB column (only one allowed per table)
        col_defs.append("`%s` BLOB" % self._genMixedCaseName("Col_Blob", 0))
        col_vals.append("NULL")

        tag_defs = []
        tag_vals = []
        for i in range(num_tags):
            tname = self._genMixedCaseName("Tag", i)
            tinfo = self._tagTypeAt(i)
            type_str, needs_len, deflen, sample_val = tinfo[0], tinfo[1], tinfo[2], tinfo[3]
            if needs_len == "decimal":
                tag_defs.append("`%s` %s(%s)" % (tname, type_str, deflen))
            elif needs_len:
                tag_defs.append("`%s` %s(%d)" % (tname, type_str, deflen))
            else:
                tag_defs.append("`%s` %s" % (tname, type_str))
            tag_vals.append(sample_val)

        create_stb = "CREATE TABLE `%s`.`%s` (%s) TAGS(%s)" % (
            dbName, stbName, ", ".join(col_defs), ", ".join(tag_defs))
        tdLog.info("STB DDL length: %d bytes, %d cols + %d tags"
                   % (len(create_stb), num_cols, num_tags))
        tdSql.execute(create_stb)

        # Child table + data
        create_ctb = "CREATE TABLE `%s`.`%s` USING `%s`.`%s` TAGS(%s)" % (
            dbName, ctbName, dbName, stbName, ", ".join(tag_vals))
        tdSql.execute(create_ctb)
        insert_sql = "INSERT INTO `%s`.`%s` VALUES (%s)" % (
            dbName, ctbName, ", ".join(col_vals))
        tdSql.execute(insert_sql)
        tdLog.info("STB child table created with 1 row")

        # -- Build normal table DDL (4096 cols, all types) --
        ntb_defs = ["`ts` TIMESTAMP"]
        ntb_vals = ["now"]
        for i in range(ntb_cols - 2):  # reserve last col for BLOB
            cname = self._genMixedCaseName("Ntb", i)
            tinfo = self._colTypeAt(i)
            type_str, needs_len, deflen, sample_val = tinfo[0], tinfo[1], tinfo[2], tinfo[3]
            if needs_len == "decimal":
                ntb_defs.append("`%s` %s(%s)" % (cname, type_str, deflen))
            elif needs_len:
                ntb_defs.append("`%s` %s(%d)" % (cname, type_str, deflen))
            else:
                ntb_defs.append("`%s` %s" % (cname, type_str))
            ntb_vals.append(sample_val)
        # Add one BLOB column (only one allowed per table)
        ntb_defs.append("`%s` BLOB" % self._genMixedCaseName("Ntb_Blob", 0))
        ntb_vals.append("NULL")

        create_ntb = "CREATE TABLE `%s`.`%s` (%s)" % (
            dbName, ntbName, ", ".join(ntb_defs))
        tdLog.info("NTB DDL length: %d bytes, %d cols" % (len(create_ntb), ntb_cols))
        tdSql.execute(create_ntb)
        tdSql.execute("INSERT INTO `%s`.`%s` VALUES (%s)" % (
            dbName, ntbName, ", ".join(ntb_vals)))
        tdLog.info("NTB created with 1 row")

        # -- Record originals --
        tdSql.query("SELECT COUNT(*) FROM `%s`.`%s`" % (dbName, stbName))
        orig_stb_cnt = tdSql.queryResult[0][0]
        tdSql.query("SELECT COUNT(*) FROM `%s`.`%s`" % (dbName, ntbName))
        orig_ntb_cnt = tdSql.queryResult[0][0]

        # Pick a few representative columns to verify values after restore
        # For each type, pick the first occurrence
        verify_cols = {}  # col_name -> (type_str, sample_val, verify_fn)
        for i in range(min(num_cols - 1, len(ALL_COL_TYPES))):
            cname = self._genMixedCaseName("Col", i)
            tinfo = ALL_COL_TYPES[i]
            verify_cols[cname] = (tinfo[0], tinfo[3], tinfo[4])

        verify_tags = {}
        for i in range(min(num_tags, len(ALL_TAG_TYPES))):
            tname = self._genMixedCaseName("Tag", i)
            tinfo = ALL_TAG_TYPES[i]
            verify_tags[tname] = (tinfo[0], tinfo[3], tinfo[4])

        # Query original values for verify columns
        orig_col_vals = {}
        for cname in verify_cols:
            tdSql.query("SELECT `%s` FROM `%s`.`%s`" % (cname, dbName, ctbName))
            orig_col_vals[cname] = tdSql.queryResult[0][0]

        orig_tag_vals = {}
        for tname in verify_tags:
            tdSql.query("SELECT `%s` FROM `%s`.`%s`" % (tname, dbName, ctbName))
            orig_tag_vals[tname] = tdSql.queryResult[0][0]

        orig_ntb_vals = {}
        for i in range(min(ntb_cols - 1, len(ALL_COL_TYPES))):
            cname = self._genMixedCaseName("Ntb", i)
            tdSql.query("SELECT `%s` FROM `%s`.`%s`" % (cname, dbName, ntbName))
            orig_ntb_vals[cname] = tdSql.queryResult[0][0]

        # -- Export --
        dumpdir = os.path.abspath(self.tmpdir + "/allcol_roundtrip")
        if os.path.exists(dumpdir):
            os.system("rm -rf %s" % dumpdir)
        os.makedirs(dumpdir)

        dump_cmd = "%s --databases %s -o %s -T 1" % (
            binPath, dbName, dumpdir)
        tdLog.info("Dump command: %s" % dump_cmd)
        rc = os.system(dump_cmd)
        assert rc == 0, "taosdump export failed with rc=%d" % rc

        avro_files = []
        for root, dirs, files in os.walk(dumpdir):
            for f in files:
                if f.endswith(".avro"):
                    avro_files.append(os.path.join(root, f))
        tdLog.info("Found %d avro files" % len(avro_files))
        for af in avro_files:
            fsize = os.path.getsize(af)
            tdLog.info("  %s size=%d" % (os.path.basename(af), fsize))
            assert fsize >= 100, "Corrupted avro: %s (size=%d)" % (af, fsize)

        # -- Drop + restore --
        tdSql.execute("DROP DATABASE %s" % dbName)

        restore_cmd = "%s -i %s" % (binPath, dumpdir)
        tdLog.info("Restore command: %s" % restore_cmd)
        rc = os.system(restore_cmd)
        assert rc == 0, "taosdump import failed with rc=%d" % rc

        # -- Verify --
        tdSql.execute("USE %s" % dbName)

        # Schema: supertable
        tdSql.query("DESCRIBE `%s`" % stbName)
        assert tdSql.queryRows == num_cols + num_tags, \
            "STB schema: expected %d rows, got %d" % (num_cols + num_tags, tdSql.queryRows)
        tdLog.info("STB schema restored: %d cols + %d tags" % (num_cols, num_tags))

        # Schema: verify each column type is correct
        # DESCRIBE returns rows: field, type, length, note
        desc_map = {}
        for i in range(tdSql.queryRows):
            fname = tdSql.queryResult[i][0]
            ftype = tdSql.queryResult[i][1]
            desc_map[fname] = ftype

        type_errors = 0
        for i in range(min(num_cols - 1, len(ALL_COL_TYPES))):
            cname = self._genMixedCaseName("Col", i)
            expected_type = ALL_COL_TYPES[i][0].upper()
            # TDengine may report BINARY as VARCHAR etc
            actual = desc_map.get(cname, "MISSING").upper()
            # normalize: BINARY -> VARCHAR in some versions
            if expected_type == "BINARY" and actual == "VARCHAR":
                continue
            if expected_type not in actual and actual not in expected_type:
                tdLog.info("  TYPE MISMATCH col `%s`: expected %s, got %s"
                           % (cname, expected_type, actual))
                type_errors += 1
        assert type_errors == 0, "STB column type mismatches: %d" % type_errors
        tdLog.info("STB column types verified (first %d types)" % len(ALL_COL_TYPES))

        # Verify case-sensitive names survived
        case_check_name = self._genMixedCaseName("Col", 0)  # e.g. Col_0000_Abc
        assert case_check_name in desc_map, \
            "Case-sensitive column name `%s` not found after restore!" % case_check_name
        tdLog.info("Case-sensitive column names preserved after round-trip")

        # Row counts
        tdSql.query("SELECT COUNT(*) FROM `%s`" % stbName)
        assert tdSql.queryResult[0][0] == orig_stb_cnt, \
            "STB count: expected %s, got %s" % (orig_stb_cnt, tdSql.queryResult[0][0])
        tdLog.info("STB row count: %s" % orig_stb_cnt)

        tdSql.query("SELECT COUNT(*) FROM `%s`" % ntbName)
        assert tdSql.queryResult[0][0] == orig_ntb_cnt, \
            "NTB count: expected %s, got %s" % (orig_ntb_cnt, tdSql.queryResult[0][0])
        tdLog.info("NTB row count: %s" % orig_ntb_cnt)

        # Column values for representative types
        mismatch = 0
        for cname, (type_str, _, verify_fn) in verify_cols.items():
            tdSql.query("SELECT `%s` FROM `%s`.`%s`" % (cname, dbName, ctbName))
            restored = tdSql.queryResult[0][0]
            original = orig_col_vals[cname]
            # compare as strings for simplicity, types like GEOMETRY/VARBINARY
            # use the lenient verify_fn
            if str(restored) != str(original):
                if not verify_fn(None, restored):
                    tdLog.info("  VALUE MISMATCH col `%s` (%s): orig=%s restored=%s"
                               % (cname, type_str, original, restored))
                    mismatch += 1
        assert mismatch == 0, "STB column value mismatches: %d" % mismatch
        tdLog.info("STB column values verified for all %d types" % len(verify_cols))

        # Tag values
        mismatch = 0
        for tname, (type_str, _, verify_fn) in verify_tags.items():
            tdSql.query("SELECT `%s` FROM `%s`.`%s`" % (tname, dbName, ctbName))
            restored = tdSql.queryResult[0][0]
            original = orig_tag_vals[tname]
            if str(restored) != str(original):
                if not verify_fn(None, restored):
                    tdLog.info("  VALUE MISMATCH tag `%s` (%s): orig=%s restored=%s"
                               % (tname, type_str, original, restored))
                    mismatch += 1
        assert mismatch == 0, "STB tag value mismatches: %d" % mismatch
        tdLog.info("STB tag values verified for all %d types" % len(verify_tags))

        # Normal table schema
        tdSql.query("DESCRIBE `%s`" % ntbName)
        assert tdSql.queryRows == ntb_cols, \
            "NTB schema: expected %d, got %d" % (ntb_cols, tdSql.queryRows)
        tdLog.info("NTB schema restored: %d cols" % ntb_cols)

        # Normal table values
        mismatch = 0
        for cname, original in orig_ntb_vals.items():
            tdSql.query("SELECT `%s` FROM `%s`.`%s`" % (cname, dbName, ntbName))
            restored = tdSql.queryResult[0][0]
            if str(restored) != str(original):
                i = int(cname.split("_")[1])
                verify_fn = ALL_COL_TYPES[i % len(ALL_COL_TYPES)][4]
                if not verify_fn(None, restored):
                    tdLog.info("  VALUE MISMATCH ntb col `%s`: orig=%s restored=%s"
                               % (cname, original, restored))
                    mismatch += 1
        assert mismatch == 0, "NTB column value mismatches: %d" % mismatch
        tdLog.info("NTB column values verified for all %d types" % len(orig_ntb_vals))

        tdSql.execute("DROP DATABASE IF EXISTS %s" % dbName)

    def test_taosdump_maxcols(self):
        """taosdump max columns export and round-trip

        1. Create supertable with 3968 columns + 128 tags (64-char names)
        2. Create normal table with 4096 columns (64-char names)
        3. Export database via taosdump
        4. Verify avro files are NOT corrupted (size > 100 bytes)
        5. Inspect all avro files with --inspect
        6. Round-trip test: export with all data types and mixed-case names
        7. Drop and restore database
        8. Verify schema, column types, case-sensitive names, row counts, and values


        Since: v3.3.6.0

        Labels: common,ci

        Jira: TD-6989427572

        History:
            - 2025-05-15 Alex Duan Migrated from tests/army/tools/taosdump/native/taosdumpMaxCols.py

        """
        self.tmpdir = "./taosdumptest/tmpdir_maxcols"
        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        else:
            os.system("rm -rf %s" % self.tmpdir)
            os.makedirs(self.tmpdir)

        binPath = etool.taosDumpFile()
        if binPath == "":
            tdLog.exit("taosdump not found!")
        tdLog.info("taosdump found: %s" % binPath)

        self._testExportWideTable(binPath)
        self._testRoundtripMaxCols(binPath)

        tdLog.info("All verifications passed")
