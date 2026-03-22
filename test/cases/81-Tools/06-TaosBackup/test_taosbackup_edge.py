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

from new_test_framework.utils import tdLog, tdSql, etool
import os
import shutil
import subprocess
import time


class TestTaosBackupEdge:
    """
    Edge-case and operational scenarios: long/special identifiers, output-path
    boundaries, debug mode, non-root user authentication, and hot backup
    (concurrent inserts during backup).
    """

    # -----------------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------------

    def makeDir(self, path):
        if os.path.exists(path):
            shutil.rmtree(path)
        os.makedirs(path)

    def taosbackup(self, cmd, checkRun=True):
        return etool.taosbackup(cmd, checkRun=checkRun)

    def isDbFound(self, dbname):
        tdSql.query("select name from information_schema.ins_databases")
        for row in tdSql.queryResult:
            if row[0] == dbname:
                return True
        return False

    # -----------------------------------------------------------------------
    # 1. Long and unusual identifier names
    # -----------------------------------------------------------------------

    def do_long_name(self):
        """Backup/restore DBs and tables with maximum-length identifiers.

        TDengine limits:
          DB name  : 64 characters
          Table name: 192 characters
        """
        tmpdir = "./taosbackuptest/tmpdir_names"
        self.makeDir(tmpdir)

        # --- Case A: 63-character database name (just within the 64-char limit) ---
        long_db = "a" * 63
        tdSql.execute(f"drop database if exists {long_db}")
        tdSql.execute(f"create database {long_db} keep 3649")
        tdSql.execute(f"create table {long_db}.nt(ts timestamp, v int)")
        tdSql.execute(
            f"insert into {long_db}.nt values(1640000000000, 42)"
        )
        tdSql.execute(
            f"insert into {long_db}.nt values(1640000001000, 88)"
        )

        # --- Case B: 180-character table name (well within 192-char limit) ---
        short_db  = "namedb"
        long_tbl  = "t" + "b" * 179  # 180 chars total
        tdSql.execute(f"drop database if exists {short_db}")
        tdSql.execute(f"create database {short_db} keep 3649")
        tdSql.execute(f"create table {short_db}.{long_tbl}(ts timestamp, v int)")
        tdSql.execute(
            f"insert into {short_db}.{long_tbl} values(1640000000000, 7)"
        )

        # --- Case C: table name starting with a digit (requires backticks) ---
        digit_db  = "digitdb"
        digit_tbl = "`3sensors`"
        tdSql.execute(f"drop database if exists {digit_db}")
        tdSql.execute(f"create database {digit_db} keep 3649")
        tdSql.execute(f"create table {digit_db}.{digit_tbl}(ts timestamp, v int)")
        tdSql.execute(
            f"insert into {digit_db}.{digit_tbl} values(1640000000000, 99)"
        )

        bin = etool.taosBackupFile()
        os.system(f"{bin} --databases {long_db},{short_db},{digit_db} -o {tmpdir}")

        for db in (long_db, short_db, digit_db):
            tdSql.execute(f"drop database {db}")

        os.system(f"{bin} -i {tmpdir}")

        # Case A: long DB name
        assert self.isDbFound(long_db), f"Long DB name '{long_db}' not found after restore"
        tdSql.query(f"select count(*) from {long_db}.nt")
        tdSql.checkData(0, 0, 2)
        tdLog.info("  long DB name (63 chars) ..................... [passed]")

        # Case B: long table name
        assert self.isDbFound(short_db), f"DB '{short_db}' not found after restore"
        tdSql.query(f"select count(*) from {short_db}.{long_tbl}")
        tdSql.checkData(0, 0, 1)
        tdSql.query(f"select v from {short_db}.{long_tbl} where ts=1640000000000")
        tdSql.checkData(0, 0, 7)
        tdLog.info("  long table name (180 chars) ................. [passed]")

        # Case C: digit-prefix table name
        assert self.isDbFound(digit_db), f"DB '{digit_db}' not found after restore"
        tdSql.query(f"select count(*) from {digit_db}.{digit_tbl}")
        tdSql.checkData(0, 0, 1)
        tdSql.query(f"select v from {digit_db}.{digit_tbl} where ts=1640000000000")
        tdSql.checkData(0, 0, 99)
        tdLog.info("  digit-prefix table name ..................... [passed]")

        tdLog.info("do_long_name ................................. [passed]")

    # -----------------------------------------------------------------------
    # 2. Output path edge cases
    # -----------------------------------------------------------------------

    def do_outpath_edges(self):
        """Output path edge cases: spaces in directory name, absolute paths."""
        tdSql.execute("drop database if exists pathdb")
        tdSql.execute("create database pathdb keep 3649")
        tdSql.execute("create table pathdb.nt(ts timestamp, v int)")
        tdSql.execute("insert into pathdb.nt values(1640000000000, 1)")
        tdSql.execute("insert into pathdb.nt values(1640000001000, 2)")

        bin = etool.taosBackupFile()

        # Case A: directory name containing spaces  (must be pre-created;
        # taosBackup does not auto-create output directories)
        space_path = "./taosbackuptest/path with spaces"
        self.makeDir(space_path)
        os.system(f'{bin} -D pathdb -o "{space_path}" -T 1')
        tdSql.execute("drop database pathdb")
        os.system(f'{bin} -i "{space_path}" -T 1')
        tdSql.query("select count(*) from pathdb.nt")
        tdSql.checkData(0, 0, 2)
        tdLog.info("  outpath with spaces ......................... [passed]")

        # Case B: absolute output path
        import tempfile
        abs_path = os.path.join(tempfile.gettempdir(), "taosbackup_abs_test")
        self.makeDir(abs_path)
        tdSql.execute("insert into pathdb.nt values(1640000002000, 3)")
        os.system(f"{bin} -D pathdb -o {abs_path} -T 1")
        tdSql.execute("drop database pathdb")
        os.system(f"{bin} -i {abs_path} -T 1")
        tdSql.query("select count(*) from pathdb.nt")
        tdSql.checkData(0, 0, 3)
        os.system(f"rm -rf {abs_path}")
        tdLog.info("  absolute output path ........................ [passed]")

        tdLog.info("do_outpath_edges ............................. [passed]")

    # -----------------------------------------------------------------------
    # 3. Debug mode (-g)
    # -----------------------------------------------------------------------

    def do_debug_mode(self):
        """Verify -g debug mode: produces more log lines than non-debug; data still correct."""
        tmpdir_norm  = "./taosbackuptest/tmpdir_debug_norm"
        tmpdir_debug = "./taosbackuptest/tmpdir_debug_g"
        self.makeDir(tmpdir_norm)
        self.makeDir(tmpdir_debug)

        tdSql.execute("drop database if exists debugdb")
        tdSql.execute("create database debugdb keep 3649")
        tdSql.execute("use debugdb")
        tdSql.execute(
            "create table st(ts timestamp, c1 int) tags(t1 int)"
        )
        for i in range(3):
            tdSql.execute(f"create table t{i} using st tags({i})")
            tdSql.execute(
                f"insert into t{i} values"
                f"(1640000000000, {i * 10})"
                f"(1640000001000, {i * 10 + 1})"
            )
        tdSql.execute("create table nt(ts timestamp, v int)")
        tdSql.execute("insert into nt values(1640000000000, 100)")

        bin = etool.taosBackupFile()

        # Backup without -g (normal mode)
        rlist_norm = etool.taosbackup(f"-D debugdb -o {tmpdir_norm} -T 1")
        count_norm = len(rlist_norm) if rlist_norm else 0

        self.makeDir(tmpdir_debug)
        # Backup with -g (debug mode)
        rlist_debug = etool.taosbackup(f"-g -D debugdb -o {tmpdir_debug} -T 1")
        count_debug = len(rlist_debug) if rlist_debug else 0

        assert count_debug > count_norm, (
            f"Debug mode (-g) should produce more output than normal mode "
            f"(debug={count_debug} lines, normal={count_norm} lines)"
        )
        tdLog.info(
            f"  debug lines={count_debug} > normal lines={count_norm} ...... [passed]"
        )

        # Restore with -g and verify data is intact
        tdSql.execute("drop database debugdb")
        etool.taosbackup(f"-g -i {tmpdir_debug} -T 1")

        tdSql.query("select count(*) from debugdb.st")
        tdSql.checkData(0, 0, 6)
        tdSql.query("select count(*) from debugdb.nt")
        tdSql.checkData(0, 0, 1)
        tdLog.info("  debug-mode restore: data correct ........... [passed]")

        tdLog.info("do_debug_mode ................................ [passed]")

    # -----------------------------------------------------------------------
    # 4. Non-root user authentication
    # -----------------------------------------------------------------------

    def do_auth_user(self):
        """User authentication scenarios for backup.

        Tests:
        - Correct credentials (root) succeed.
        - A custom user with correct credentials can backup.

        Note: Wrong-password tests are excluded because taosBackup defers
        authentication until it has filled its connection pool, causing
        hung processes instead of fast failure.  GRANT/ACL is Enterprise-only.
        """
        tmpdir = "./taosbackuptest/tmpdir_auth"
        self.makeDir(tmpdir)

        # Prepare test database
        tdSql.execute("drop database if exists authdb")
        tdSql.execute("create database authdb keep 3649")
        tdSql.execute("use authdb")
        tdSql.execute("create table nt(ts timestamp, v int)")
        for i in range(5):
            tdSql.execute(
                f"insert into authdb.nt values({1640000000000 + i * 1000}, {i})"
            )

        # --- Scenario A: root with correct password succeeds ---
        self.makeDir(tmpdir)
        rlist = etool.taosbackup(
            f"-D authdb -o {tmpdir}",
            checkRun=True,
        )
        output = "\n".join(rlist) if rlist else ""
        assert "SUCCESS" in output, \
            f"Root user should be able to backup.\nOutput: {output[:400]}"
        tdLog.info("  root user can backup ........................ [passed]")

        # --- Scenario B: custom user with correct credentials ---
        tdSql.execute("drop user if exists baktest")
        tdSql.execute("create user baktest pass 'Bak@2026'")

        # Grant read access so baktest can SHOW STABLES / query authdb.
        # On Community edition GRANT may be unsupported — catch and skip gracefully.
        try:
            tdSql.execute("grant use,show on database authdb to baktest")
            tdSql.execute("grant select on authdb.* to baktest")
        except Exception as e:
            tdLog.info(f"  GRANT not supported (Community edition?): {e} — skipping grant")

        self.makeDir(tmpdir)
        rlist2 = etool.taosbackup(
            f"-u baktest -p Bak@2026 -D authdb -o {tmpdir}",
            checkRun=True,
        )
        output2 = "\n".join(rlist2) if rlist2 else ""
        assert "SUCCESS" in output2, \
            f"Custom user should be able to backup.\nOutput: {output2[:400]}"
        tdLog.info("  custom user can backup ...................... [passed]")

        tdSql.execute("drop user baktest")
        tdSql.execute("drop database authdb")

        tdLog.info("do_auth_user ................................. [passed]")

    # -----------------------------------------------------------------------
    # 5. Hot backup: concurrent inserts during backup
    # -----------------------------------------------------------------------

    def do_hot_backup(self):
        """Run backup while a background process continuously inserts rows.

        Assertion: the restored row count must be >= the number of rows that
        existed BEFORE the backup process started (consistency lower bound).
        """
        tmpdir = "./taosbackuptest/tmpdir_hot"
        self.makeDir(tmpdir)

        tdSql.execute("drop database if exists hotdb")
        tdSql.execute("create database hotdb keep 3649")
        tdSql.execute("create table hotdb.nt(ts timestamp, v int)")

        # Pre-insert a known baseline of rows before backup starts
        pre_rows = 300
        sql = "insert into hotdb.nt values"
        for i in range(pre_rows):
            sql += f"({1640000000000 + i * 1000}, {i})"
        tdSql.execute(sql)

        bin = etool.taosBackupFile()

        # Build a writer script: inserts rows using the taos CLI in a tight loop.
        # Each row uses now() for the timestamp to avoid collisions with pre-rows.
        writer_script = (
            "for i in $(seq 1 500); do "
            "taos -s 'insert into hotdb.nt values(now(), '\"$i\"');' "
            ">/dev/null 2>&1; "
            "sleep 0.02; "
            "done"
        )
        writer_proc = subprocess.Popen(writer_script, shell=True)

        # Give the writer a brief head-start before backup begins
        time.sleep(0.3)

        # Run the backup while the writer is active
        os.system(f"{bin} -D hotdb -o {tmpdir} -T 2")

        # Stop the writer (if still running)
        writer_proc.terminate()
        try:
            writer_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            writer_proc.kill()

        # Record the total rows in the source now (used only for logging)
        tdSql.query("select count(*) from hotdb.nt")
        total_in_source = tdSql.getData(0, 0)
        tdLog.info(
            f"  hotdb total rows in source at backup time: {total_in_source}"
        )

        # Restore and verify the consistency lower bound
        tdSql.execute("drop database hotdb")
        os.system(f"{bin} -i {tmpdir} -T 2")

        tdSql.query("select count(*) from hotdb.nt")
        restored_rows = tdSql.getData(0, 0)
        tdLog.info(f"  hotdb restored rows: {restored_rows} (baseline >= {pre_rows})")
        assert int(str(restored_rows)) >= pre_rows, (
            f"Hot backup lost pre-existing data: "
            f"restored={restored_rows}, expected >= {pre_rows}"
        )

        tdLog.info("do_hot_backup ................................ [passed]")

    # -----------------------------------------------------------------------
    # Main test entry point
    # -----------------------------------------------------------------------

    def test_taosbackup_edge(self):
        """taosBackup edge cases: long names, output paths, debug mode, auth, hot backup

        1. Long identifier names:
           - 63-character database name (near the 64-char limit)
           - 180-character table name (near the 192-char limit)
           - Table name starting with a digit (backtick-escaped)
        2. Output path edge cases:
           - Directory name containing spaces (backup and restore with shell quoting)
           - Absolute output path (not relative to cwd)
        3. Debug mode (-g):
           - Backup with -g produces more log output than without -g
           - Restored data with debug mode is still correct
        4. User authentication (-u / -p):
           - Correct credentials (root) succeed
           - Custom user with correct credentials can backup
           Note: Wrong-password tests excluded (taosBackup pools connections
           before auth failure, causing hangs not fast exits).
           GRANT/ACL is Enterprise-only.
        5. Hot backup (concurrent inserts):
           - Background writer inserts rows during backup
           - Restored row count is >= the pre-backup baseline

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-06 Created to fill gap coverage identified in analysis

        """
        self.do_long_name()
        self.do_outpath_edges()
        self.do_debug_mode()
        self.do_auth_user()
        self.do_hot_backup()

        os.system("rm -rf ./taosbackuptest/")
        tdLog.info("test_taosbackup_edge ......................... [passed]")
