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
test_taosbackup_spectable.py

taosdump supports positional table arguments — "taosdump dbname tb1 tb2 ..." —
to select specific tables within one database, on BOTH the backup side (-o)
and the restore side (-i). This file covers each side independently:

- do_backup_spectable: the backup only writes the tables named on the command
  line, everything else is excluded from the backup directory (this already
  worked; verified here as a baseline).
- do_restore_spectable: given a backup directory that has EVERY table for a
  database, restoring with "taosdump dbname tb1 tb2 ... -i outpath" must
  create ONLY the named tables in the target database — child tables, tags,
  normal tables, and their data.  Before the two-stage / --content work in
  this area, the restore side ignored the positional table list entirely and
  restored every table present in the backup directory regardless of what was
  asked for on the command line.
- do_restore_whole_stable: naming the STB itself keeps every one of its
  children, mirroring the backup side's "whole STB requested" rule.

Verification method: the source database is never dropped. Every restore
targets a RENAMED destination database via -W SRC=DST, so after each restore
we can diff the destination against the still-live source: the exact set of
tables present, and a full row-by-row content comparison for every table that
is supposed to exist. This catches both "restored a table that should have
been excluded" and "row data does not match" — not just presence checks.
"""

import os
import shutil
import tempfile

from new_test_framework.utils import tdLog, tdSql, etool

RESULT_SUCCESS = "Result       : SUCCESS"
RESULT_FAILED = "Result       : FAILED"


class TestTaosbackupSpecTable:
    def dump(self, args):
        """Run taosdump via etool.taosdump() and return its output lines.

        checkRun=False: a nonzero exit must not raise — callers decide
        success/failure by checking the "Result :" line via checkManyString(),
        same convention as test_taosbackup_commandline.py /
        test_taosbackup_schema_change.py. retFail=True merges stderr into the
        returned lines so error-path messages are matchable too.
        """
        return etool.taosdump(args, checkRun=False, retFail=True)

    def checkManyString(self, rlist, expected_list):
        """Check that all expected strings appear in output."""
        output = "\n".join(rlist)
        for expected in expected_list:
            if expected not in output:
                tdLog.exit(
                    f"Expected string '{expected}' not found in output:\n{output}"
                )

    def makeDir(self, path):
        if os.path.exists(path):
            shutil.rmtree(path)
        os.makedirs(path)

    def tableNames(self, dbName):
        tdSql.query(
            f"select table_name from information_schema.ins_tables "
            f"where db_name='{dbName}' order by table_name"
        )
        return [row[0] for row in tdSql.queryResult]

    def stableNames(self, dbName):
        tdSql.query(
            f"select stable_name from information_schema.ins_stables "
            f"where db_name='{dbName}' order by stable_name"
        )
        return [row[0] for row in tdSql.queryResult]

    def checkSameResult(self, sql1, sql2):
        """Run both SQL statements and compare their results row-by-row."""
        res1 = tdSql.getResult(sql1)
        res2 = tdSql.getResult(sql2)
        if res1 == res2:
            tdLog.info(f"  results match: {sql2!r} ({len(res2)} row(s))")
        else:
            tdLog.exit(
                f"Results differ!\n  sql1={sql1!r} -> {res1}\n  sql2={sql2!r} -> {res2}"
            )

    def compareTableData(self, srcDb, dstDb, tbName, orderCol="ts"):
        """Full row-by-row comparison of one table between srcDb and dstDb."""
        self.checkSameResult(
            f"select * from {srcDb}.{tbName} order by {orderCol}",
            f"select * from {dstDb}.{tbName} order by {orderCol}",
        )

    def verifyTableSchema(self, srcDb, dstDb, tbName):
        """Verify schema is identical: column names and types."""
        src_cols = tdSql.getResult(
            f"describe {srcDb}.{tbName}"
        )
        dst_cols = tdSql.getResult(
            f"describe {dstDb}.{tbName}"
        )
        if src_cols != dst_cols:
            tdLog.exit(
                f"Schema mismatch for {tbName}!\n"
                f"  source: {src_cols}\n"
                f"  restored: {dst_cols}"
            )
        tdLog.info(f"  schema match: {tbName}")

    def verifyTableRowCount(self, srcDb, dstDb, tbName):
        """Verify row count is identical."""
        tdSql.query(f"select count(*) from {srcDb}.{tbName}")
        src_count = tdSql.getRows()
        src_rows = tdSql.queryResult[0][0] if src_count > 0 else 0

        tdSql.query(f"select count(*) from {dstDb}.{tbName}")
        dst_count = tdSql.getRows()
        dst_rows = tdSql.queryResult[0][0] if dst_count > 0 else 0

        if src_rows != dst_rows:
            tdLog.exit(
                f"Row count mismatch for {tbName}: "
                f"source={src_rows}, restored={dst_rows}"
            )
        tdLog.info(f"  row count match: {tbName} ({src_rows} rows)")

    def verifyTableDataDetailed(self, srcDb, dstDb, tbName, orderCol="ts"):
        """Comprehensive verification: schema, row count, and full data."""
        self.verifyTableSchema(srcDb, dstDb, tbName)
        self.verifyTableRowCount(srcDb, dstDb, tbName)
        self.compareTableData(srcDb, dstDb, tbName, orderCol)

    #
    # ------------------- shared source fixture ----------------
    #

    DB = "spt_db"

    def prepareSource(self):
        """One db, two STBs (3 + 2 children), two NTBs. Distinct data per table.
        The source database is created once and kept alive for the whole test:
        every restore below targets a renamed destination so the source stays
        available as the ground truth for comparison.
        """
        tdSql.execute(f"drop database if exists {self.DB}")
        tdSql.execute(f"create database {self.DB}")

        tdSql.execute(
            f"create table {self.DB}.st1 (ts timestamp, v int) tags(t1 int)"
        )
        tdSql.execute(f"create table {self.DB}.c1 using {self.DB}.st1 tags(1)")
        tdSql.execute(f"create table {self.DB}.c2 using {self.DB}.st1 tags(2)")
        tdSql.execute(f"create table {self.DB}.c3 using {self.DB}.st1 tags(3)")
        tdSql.execute(
            f"insert into {self.DB}.c1 values(1700000000000,10)(1700000001000,11)"
        )
        tdSql.execute(
            f"insert into {self.DB}.c2 values(1700000000000,20)(1700000001000,21)"
        )
        tdSql.execute(
            f"insert into {self.DB}.c3 values(1700000000000,30)(1700000001000,31)"
        )

        tdSql.execute(
            f"create table {self.DB}.st2 (ts timestamp, f float) tags(area int)"
        )
        tdSql.execute(f"create table {self.DB}.d1 using {self.DB}.st2 tags(100)")
        tdSql.execute(f"create table {self.DB}.d2 using {self.DB}.st2 tags(200)")
        tdSql.execute(f"insert into {self.DB}.d1 values(1700000000000,1.5)")
        tdSql.execute(f"insert into {self.DB}.d2 values(1700000000000,2.5)")

        tdSql.execute(f"create table {self.DB}.nt1 (ts timestamp, v int)")
        tdSql.execute(f"create table {self.DB}.nt2 (ts timestamp, v int)")
        tdSql.execute(f"insert into {self.DB}.nt1 values(1700000000000,100)")
        tdSql.execute(f"insert into {self.DB}.nt2 values(1700000000000,200)")

    #
    # ------------------- backup-side spec-table ----------------
    #

    def do_backup_spectable(self, tmpdir):
        """taosdump dbname tb1 tb2 ... -o outpath backs up ONLY the named tables.

        Restores into a RENAMED destination (spt_db -> spt_bck) so the
        (untouched) source database can be used as the comparison baseline.
        """
        dstDb = "spt_bck"
        tdSql.execute(f"drop database if exists {dstDb}")

        backdir = os.path.join(tmpdir, "backup_side")
        self.makeDir(backdir)
        # request one CTB from each STB, plus one NTB — c2 and d2 must be excluded
        rlist = self.dump(f"{self.DB} c1 d1 nt1 -o {backdir}")
        self.checkManyString(rlist, [RESULT_SUCCESS])

        # both STB DDLs are preserved (c1's parent st1, d1's parent st2)
        assert os.path.exists(os.path.join(backdir, self.DB, "stb.sql")), \
            "stb.sql missing from backup"
        with open(os.path.join(backdir, self.DB, "stb.sql")) as f:
            stbContent = f.read()
        assert "st1" in stbContent and "st2" in stbContent, \
            f"both parent STB DDLs must be preserved: {stbContent!r}"

        # data files exist only for the requested tables
        for stb, tb in (("st1", "c1"), ("st2", "d1")):
            found = os.path.exists(os.path.join(backdir, self.DB, f"{stb}_data0", f"{tb}.dat"))
            assert found, f"expected data file for {tb} under {stb}_data0"
        for stb, tb in (("st1", "c2"), ("st1", "c3"), ("st2", "d2")):
            found = os.path.exists(os.path.join(backdir, self.DB, f"{stb}_data0", f"{tb}.dat"))
            assert not found, f"{tb} must NOT be backed up (not in spec list)"
        assert os.path.exists(os.path.join(backdir, self.DB, "_ntb_data0", "nt1.dat")), \
            "expected data file for nt1"
        assert not os.path.exists(os.path.join(backdir, self.DB, "_ntb_data0", "nt2.dat")), \
            "nt2 must NOT be backed up (not in spec list)"

        # restore the (already-filtered) backup into a renamed db and diff
        # against the still-live source
        rlist = self.dump(f'-i {backdir} -W "{self.DB}={dstDb}"')
        self.checkManyString(rlist, [RESULT_SUCCESS])

        # exact table set — nothing more, nothing less than what was backed up
        assert self.tableNames(dstDb) == ["c1", "d1", "nt1"], \
            f"unexpected tables after restore: {self.tableNames(dstDb)}"
        assert self.stableNames(dstDb) == ["st1", "st2"], \
            f"both parent STBs should exist: {self.stableNames(dstDb)}"

        # full row-by-row comparison against source for every restored table
        self.verifyTableDataDetailed(self.DB, dstDb, "c1")
        self.verifyTableDataDetailed(self.DB, dstDb, "d1")
        self.verifyTableDataDetailed(self.DB, dstDb, "nt1")
        # tags of the restored child tables are correct
        self.checkSameResult(
            f"select t1 from {self.DB}.c1", f"select t1 from {dstDb}.c1"
        )
        self.checkSameResult(
            f"select area from {self.DB}.d1", f"select area from {dstDb}.d1"
        )

        tdSql.execute(f"drop database if exists {dstDb}")
        tdLog.info("backup-side positional table filter ...... [passed]")

    #
    # ------------------- restore-side spec-table ----------------
    #

    def do_restore_spectable(self, tmpdir):
        """taosdump dbname tb1 tb2 ... -i inpath restores ONLY the named tables,
        even when the backup directory contains every table for the database.

        Restores into a RENAMED destination (spt_db -> spt_rst) so the
        (untouched) source database can be used as the comparison baseline.
        """
        dstDb = "spt_rst"
        tdSql.execute(f"drop database if exists {dstDb}")

        # full backup: no positional filter, everything goes into the backup dir
        backdir = os.path.join(tmpdir, "restore_side")
        self.makeDir(backdir)
        rlist = self.dump(f"-D {self.DB} -o {backdir}")
        self.checkManyString(rlist, [RESULT_SUCCESS])

        # sanity: the backup directory really has every table
        for stb, tb in (("st1", "c1"), ("st1", "c2"), ("st1", "c3"),
                        ("st2", "d1"), ("st2", "d2")):
            assert os.path.exists(os.path.join(backdir, self.DB, f"{stb}_data0", f"{tb}.dat")), \
                f"backup dir should contain {tb}"
        for tb in ("nt1", "nt2"):
            assert os.path.exists(os.path.join(backdir, self.DB, "_ntb_data0", f"{tb}.dat")), \
                f"backup dir should contain {tb}"

        # restore into a renamed db, asking for c1 (from st1), d2 (from st2),
        # and nt1 only
        rlist = self.dump(f'{self.DB} c1 d2 nt1 -i {backdir} -W "{self.DB}={dstDb}"')
        self.checkManyString(rlist, [RESULT_SUCCESS])

        # exactly the requested tables were created — not c2, c3, d1, nt2
        assert self.tableNames(dstDb) == sorted(["c1", "d2", "nt1"]), \
            f"unexpected tables after filtered restore: {self.tableNames(dstDb)}"

        # both parent STBs still exist (needed to hold c1 / d2), but with only
        # their requested child, not their siblings
        assert self.stableNames(dstDb) == ["st1", "st2"], \
            f"both parent STBs should exist: {self.stableNames(dstDb)}"
        tdSql.query(f"select count(*) from {dstDb}.st1")
        tdSql.checkData(0, 0, 2)   # only c1's 2 rows — c2/c3 excluded
        tdSql.query(f"select count(*) from {dstDb}.st2")
        tdSql.checkData(0, 0, 1)   # only d2's 1 row — d1 excluded

        # full row-by-row comparison against source for every restored table
        self.verifyTableDataDetailed(self.DB, dstDb, "c1")
        self.verifyTableDataDetailed(self.DB, dstDb, "d2")
        self.verifyTableDataDetailed(self.DB, dstDb, "nt1")
        # tags of the restored child tables are correct
        self.checkSameResult(
            f"select t1 from {self.DB}.c1", f"select t1 from {dstDb}.c1"
        )
        self.checkSameResult(
            f"select area from {self.DB}.d2", f"select area from {dstDb}.d2"
        )

        tdSql.execute(f"drop database if exists {dstDb}")
        tdLog.info("restore-side positional table filter ..... [passed]")

    def do_restore_whole_stable(self, tmpdir):
        """Naming the STB itself (not a child) on restore keeps ALL its children,
        matching the backup side's "whole STB requested" rule.

        Restores into a RENAMED destination (spt_db -> spt_stb) so the
        (untouched) source database can be used as the comparison baseline.
        """
        dstDb = "spt_stb"
        tdSql.execute(f"drop database if exists {dstDb}")

        backdir = os.path.join(tmpdir, "restore_whole_stb")
        self.makeDir(backdir)
        rlist = self.dump(f"-D {self.DB} -o {backdir}")
        self.checkManyString(rlist, [RESULT_SUCCESS])

        # "st1" names the super table itself -> every child of st1 must come
        # back, and NOTHING from st2 or the normal tables
        rlist = self.dump(f'{self.DB} st1 -i {backdir} -W "{self.DB}={dstDb}"')
        self.checkManyString(rlist, [RESULT_SUCCESS])

        assert self.tableNames(dstDb) == ["c1", "c2", "c3"], \
            f"whole-STB restore should keep every child and nothing else, " \
            f"got {self.tableNames(dstDb)}"
        assert self.stableNames(dstDb) == ["st1"], \
            f"only st1 should exist, got {self.stableNames(dstDb)}"

        # full row-by-row comparison against source for every child of st1
        self.verifyTableDataDetailed(self.DB, dstDb, "c1")
        self.verifyTableDataDetailed(self.DB, dstDb, "c2")
        self.verifyTableDataDetailed(self.DB, dstDb, "c3")
        self.checkSameResult(
            f"select t1 from {self.DB}.c1 order by t1",
            f"select t1 from {dstDb}.c1 order by t1",
        )
        tdSql.query(f"select count(*) from {dstDb}.st1")
        tdSql.checkData(0, 0, 6)   # 2 rows each for c1, c2, c3

        tdSql.execute(f"drop database if exists {dstDb}")
        tdLog.info("restore-side whole-STB positional arg ..... [passed]")

    #
    # ------------------- fail-fast on nonexistent spec db/table ----------------
    #

    def do_backup_table_not_found(self, tmpdir):
        """Backup: a positional table name that doesn't exist in the source
        database must fail fast (nonzero exit, no partial backup directory),
        instead of silently backing up nothing for that name.
        """
        backdir = os.path.join(tmpdir, "backup_missing_table")
        self.makeDir(backdir)

        rlist = self.dump(f"{self.DB} no_such_table_zzz -o {backdir}")
        self.checkManyString(rlist, [RESULT_FAILED])
        assert not os.path.exists(os.path.join(backdir, self.DB, "backup_complete.flag")), \
            "a failed spec-table validation must not produce a completed backup"
        tdLog.info("backup-side nonexistent table fails fast ..... [passed]")

    def do_backup_partial_match_fails(self, tmpdir):
        """Backup: multiple positional table names, some existing and some not.
        The whole command must fail — no partial backup of only the tables
        that do exist.
        """
        backdir = os.path.join(tmpdir, "backup_partial_match")
        self.makeDir(backdir)

        # c1 exists, no_such_table_zzz does not
        rlist = self.dump(f"{self.DB} c1 no_such_table_zzz -o {backdir}")
        self.checkManyString(rlist, [RESULT_FAILED])
        # nothing for c1 should have been written either
        dataFile = os.path.join(backdir, self.DB, "st1_data0", "c1.dat")
        assert not os.path.exists(dataFile), \
            f"c1 must not be partially backed up when the command overall failed: {dataFile}"
        tdLog.info("backup-side partial table match fails whole command ..... [passed]")

    def do_backup_whole_stable_name_ok(self, tmpdir):
        """Backup: a positional name that is a super table itself (not a
        physical table row) must still validate successfully — DESCRIBE
        resolves super table names just as it does child/normal tables.
        """
        backdir = os.path.join(tmpdir, "backup_whole_stb_name")
        self.makeDir(backdir)

        rlist = self.dump(f"{self.DB} st1 -o {backdir}")
        self.checkManyString(rlist, [RESULT_SUCCESS])
        assert os.path.exists(os.path.join(backdir, self.DB, "backup_complete.flag")), \
            "a valid whole-STB backup must complete"
        tdLog.info("backup-side whole-STB name validates via DESCRIBE ..... [passed]")

    def do_backup_nonexistent_db_other_error(self, tmpdir):
        """Backup: DESCRIBE failing for a reason OTHER than "table does not
        exist" (e.g. the database itself doesn't exist) must be reported and
        the command must fail immediately with that real error — not
        misreported as "specified table(s) do not exist" the way a
        genuinely missing table is (backup.c: validateSpecTablesForBackup).
        """
        backdir = os.path.join(tmpdir, "backup_nonexistent_db")
        self.makeDir(backdir)

        noSuchDb = "spt_no_such_db_zzz"
        rlist = self.dump(f"{noSuchDb} sometable -o {backdir}")
        self.checkManyString(rlist, [RESULT_FAILED])
        output = "\n".join(rlist)
        assert "specified table(s) do not exist" not in output, (
            "a nonexistent database must not be misreported via the "
            "'missing table' path:\n" + output
        )
        assert "DESCRIBE" in output and "failed" in output, (
            "expected the transient/other-error DESCRIBE-failure message:\n" + output
        )
        tdLog.info(
            "backup-side nonexistent db reports the real error, "
            "not 'table not found' ..... [passed]"
        )

    def do_backup_many_missing_tables_not_truncated(self, tmpdir):
        """Backup: many missing positional table names must ALL be reported
        in the error message. Before the dynamic missingBuf allocation fix,
        a fixed-size buffer silently truncated the list once enough missing
        names were requested (backup.c: validateSpecTablesForBackup).
        """
        backdir = os.path.join(tmpdir, "backup_many_missing")
        self.makeDir(backdir)

        names = [f"missing_tbl_{i:04d}" for i in range(300)]
        rlist = self.dump(f"{self.DB} {' '.join(names)} -o {backdir}")
        self.checkManyString(rlist, [RESULT_FAILED])
        output = "\n".join(rlist)
        for name in names:
            assert f"'{name}'" in output, (
                f"missing table {name!r} dropped from the error message "
                f"(missingBuf truncation regression)"
            )
        tdLog.info(
            "backup-side many-missing-tables list is not truncated ..... [passed]"
        )

    def do_restore_table_not_found(self, tmpdir):
        """Restore: a positional table name that isn't present anywhere in the
        backup directory (not in stb.sql, ntb.sql, or any tag file) must fail
        fast, instead of silently restoring zero tables and reporting success.
        """
        dstDb = "spt_missing"
        tdSql.execute(f"drop database if exists {dstDb}")

        backdir = os.path.join(tmpdir, "restore_missing_table")
        self.makeDir(backdir)
        rlist = self.dump(f"-D {self.DB} -o {backdir}")
        self.checkManyString(rlist, [RESULT_SUCCESS])

        rlist = self.dump(
            f'{self.DB} no_such_table_zzz -i {backdir} -W "{self.DB}={dstDb}"'
        )
        self.checkManyString(rlist, [RESULT_FAILED])
        assert self.tableNames(dstDb) == [], \
            f"a failed spec-table validation must not restore any table, got {self.tableNames(dstDb)}"

        tdSql.execute(f"drop database if exists {dstDb}")
        tdLog.info("restore-side nonexistent table fails fast ..... [passed]")

    def do_restore_partial_match_fails(self, tmpdir):
        """Restore: multiple positional table names, some present in the
        backup and some not.  The whole restore must fail — no partial
        restore of only the tables that are present.
        """
        dstDb = "spt_partial"
        tdSql.execute(f"drop database if exists {dstDb}")

        backdir = os.path.join(tmpdir, "restore_partial_match")
        self.makeDir(backdir)
        rlist = self.dump(f"-D {self.DB} -o {backdir}")
        self.checkManyString(rlist, [RESULT_SUCCESS])

        # c1 is present in the backup, no_such_table_zzz is not
        rlist = self.dump(
            f'{self.DB} c1 no_such_table_zzz -i {backdir} -W "{self.DB}={dstDb}"'
        )
        self.checkManyString(rlist, [RESULT_FAILED])
        assert self.tableNames(dstDb) == [], \
            f"c1 must not be partially restored when the command overall failed, got {self.tableNames(dstDb)}"

        tdSql.execute(f"drop database if exists {dstDb}")
        tdLog.info("restore-side partial table match fails whole command ..... [passed]")

    def do_restore_whole_stable_name_ok(self, tmpdir):
        """Restore: a positional name that is a super table itself must still
        validate successfully against the backup's stb.sql / tag files.
        """
        dstDb = "spt_stb_ok"
        tdSql.execute(f"drop database if exists {dstDb}")

        backdir = os.path.join(tmpdir, "restore_whole_stb_name")
        self.makeDir(backdir)
        rlist = self.dump(f"-D {self.DB} -o {backdir}")
        self.checkManyString(rlist, [RESULT_SUCCESS])

        rlist = self.dump(f'{self.DB} st1 -i {backdir} -W "{self.DB}={dstDb}"')
        self.checkManyString(rlist, [RESULT_SUCCESS])
        assert self.tableNames(dstDb) == ["c1", "c2", "c3"], \
            f"whole-STB restore should bring back every child, got {self.tableNames(dstDb)}"

        tdSql.execute(f"drop database if exists {dstDb}")
        tdLog.info("restore-side whole-STB name validates ..... [passed]")

    def do_conflict_D_and_positional(self, tmpdir):
        """Using both -D/--databases AND a positional dbname at the same time
        must be rejected.  argsInit() returns -1 before any backup/restore
        work begins, so "Result : FAILED" is never printed — we check for
        the specific arg-error message instead.

        Covers both backup (-o) and restore (-i) modes, and both short (-D)
        and long (--databases) option forms.
        """
        ERR = "cannot specify both"

        backdir = os.path.join(tmpdir, "conflict_D_pos")
        self.makeDir(backdir)

        for opts, mode in (
            (f"-D {self.DB} {self.DB} c1 -o {backdir}",       "backup -D"),
            (f"--databases {self.DB} {self.DB} c1 -o {backdir}","backup --databases"),
            (f"-D {self.DB} {self.DB} c1 -i {backdir}",        "restore -D"),
            (f"--databases {self.DB} {self.DB} c1 -i {backdir}","restore --databases"),
        ):
            rlist = self.dump(opts)
            self.checkManyString(rlist, [ERR])
            tdLog.info(f"  {mode}: conflict detected")

        tdLog.info("-D + positional dbname conflict rejected ..... [passed]")

    #
    # ------------------- main ----------------
    #

    def test_taosbackup_spectable(self):
        """Positional table-name filtering on both the backup and restore side

        "taosdump dbname tb1 tb2 ..." selects which tables to operate on. The
        backup side (-o) has always filtered correctly. This verifies that the
        SAME command form on the restore side (-i) also filters, and that the
        restored tables' data, tags, and parent STB schema are all correct —
        verified by a full row-by-row comparison against the still-live
        source database (every restore targets a renamed destination via -W).

        1. Backup-side filter: only named tables are written to the backup
           directory (baseline)
        2. Restore-side filter: given a full backup, restoring with a table
           list creates ONLY those tables, with correct data/tags
        3. Restore-side whole-STB: naming the STB itself keeps every child
           and nothing else
        4. Backup/restore fail fast (nonzero exit, no partial output) when a
           positional table name does not exist — including when the list is
           a mix of existing and nonexistent names — and a whole-STB name
           still validates successfully (backup: DESCRIBE; restore: stb.sql)
        5. Backup-side DESCRIBE errors that are NOT "table does not exist"
           (e.g. the database itself is missing) are reported as the real
           error, not misreported as a missing-table list; and a large
           missing-table list is reported in full, not silently truncated
           by a fixed-size buffer

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-08-08 Added to cover restore-side positional table filtering
            - 2026-08-10 Rewritten to verify restores against the live source
              database via -W rename, instead of isolated data spot-checks
            - 2026-08-10 Added fail-fast coverage for nonexistent positional
              db/table names on both backup and restore
            - 2026-08-12 Added coverage for the transient-error-vs-missing-table
              distinction and the dynamic missingBuf sizing fix in
              validateSpecTablesForBackup (backup.c)

        """
        tmpdir = tempfile.mkdtemp(prefix="taosbackup_spectable_")

        self.prepareSource()
        try:
            self.do_backup_spectable(tmpdir)
            self.do_restore_spectable(tmpdir)
            self.do_restore_whole_stable(tmpdir)
            self.do_backup_table_not_found(tmpdir)
            self.do_backup_partial_match_fails(tmpdir)
            self.do_backup_whole_stable_name_ok(tmpdir)
            self.do_backup_nonexistent_db_other_error(tmpdir)
            self.do_backup_many_missing_tables_not_truncated(tmpdir)
            self.do_restore_table_not_found(tmpdir)
            self.do_restore_partial_match_fails(tmpdir)
            self.do_restore_whole_stable_name_ok(tmpdir)
            self.do_conflict_D_and_positional(tmpdir)
        finally:
            tdSql.execute(f"drop database if exists {self.DB}")
