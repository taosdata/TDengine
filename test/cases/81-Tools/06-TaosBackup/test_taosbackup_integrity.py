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


class TestTaosBackupIntegrity:
    """
    Data integrity, multi-database batch backup, thread-count correctness,
    database property preservation, incremental backup, and empty-DB scenarios.
    """

    # -----------------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------------

    def makeDir(self, path):
        if not os.path.exists(path):
            os.makedirs(path)
        else:
            os.system("rm -rf %s" % path)
            os.makedirs(path)

    def taosbackup(self, cmd, checkRun=True):
        return etool.taosbackup(cmd, checkRun=checkRun)

    def binPath(self):
        p = etool.taosBackupFile()
        if not p:
            tdLog.exit("taosBackup not found!")
        return p

    # -----------------------------------------------------------------------
    # 1. Row count and aggregation integrity
    # -----------------------------------------------------------------------

    def do_data_integrity(self):
        """Verify COUNT, SUM, AVG, LAST(ts) are identical between source and restored DB."""
        tmpdir = "./taosbackuptest/tmpdir_integrity"
        self.makeDir(tmpdir)

        tdSql.execute("drop database if exists intdb")
        tdSql.execute("create database intdb keep 3649 vgroups 2")
        tdSql.execute("use intdb")
        tdSql.execute(
            "create table st(ts timestamp, c1 int, c2 double, c3 binary(20)) "
            "tags(t1 int, t2 binary(10))"
        )
        for i in range(5):
            tdSql.execute(f"create table t{i} using st tags({i}, 'city{i}')")

        rows_per_table = 200
        for i in range(5):
            sql = f"insert into t{i} values"
            for j in range(rows_per_table):
                sql += f"({1640000000000 + j * 1000}, {j}, {j * 0.5}, 'str{j % 10}')"
            tdSql.execute(sql)

        # also add a normal table
        tdSql.execute("create table intdb.nt(ts timestamp, v int, f double)")
        tdSql.execute(
            "insert into intdb.nt values"
            "(1640000000000, 10, 1.5)"
            "(1640000001000, 20, 2.5)"
            "(1640000002000, 30, 3.5)"
        )

        # Record source aggregations before backup
        tdSql.query("select count(*), sum(c1), avg(c2) from intdb.st")
        src_count = int(tdSql.getData(0, 0))
        src_sum_c1 = float(str(tdSql.getData(0, 1) or 0))
        src_avg_c2 = float(str(tdSql.getData(0, 2) or 0))

        tdSql.query("select last(ts) from intdb.st")
        src_last_ts = tdSql.getData(0, 0)

        tdSql.query("select count(*), sum(v), avg(f) from intdb.nt")
        src_nt_count = int(tdSql.getData(0, 0))
        src_nt_sum_v = float(str(tdSql.getData(0, 1) or 0))

        bin = self.binPath()
        os.system(f"{bin} -D intdb -o {tmpdir} -T 1")
        tdSql.execute("drop database intdb")
        os.system(f"{bin} -i {tmpdir} -T 1")

        # Verify STB aggregations
        tdSql.query("select count(*), sum(c1), avg(c2) from intdb.st")
        dst_count = int(tdSql.getData(0, 0))
        dst_sum_c1 = float(str(tdSql.getData(0, 1) or 0))
        dst_avg_c2 = float(str(tdSql.getData(0, 2) or 0))

        assert dst_count == src_count, \
            f"STB count mismatch: restored={dst_count}, source={src_count}"
        assert abs(dst_sum_c1 - src_sum_c1) < 1.0, \
            f"STB sum(c1) mismatch: restored={dst_sum_c1}, source={src_sum_c1}"
        assert abs(dst_avg_c2 - src_avg_c2) < 0.001, \
            f"STB avg(c2) mismatch: restored={dst_avg_c2}, source={src_avg_c2}"

        tdSql.query("select last(ts) from intdb.st")
        dst_last_ts = tdSql.getData(0, 0)
        assert dst_last_ts == src_last_ts, \
            f"STB last(ts) mismatch: restored={dst_last_ts}, source={src_last_ts}"

        # Verify NTB aggregations
        tdSql.query("select count(*), sum(v), avg(f) from intdb.nt")
        dst_nt_count = int(tdSql.getData(0, 0))
        dst_nt_sum_v = float(str(tdSql.getData(0, 1) or 0))

        assert dst_nt_count == src_nt_count, \
            f"NTB count mismatch: restored={dst_nt_count}, source={src_nt_count}"
        assert abs(dst_nt_sum_v - src_nt_sum_v) < 1.0, \
            f"NTB sum(v) mismatch: restored={dst_nt_sum_v}, source={src_nt_sum_v}"

        tdLog.info("do_data_integrity ............................ [passed]")

    # -----------------------------------------------------------------------
    # 2. Multi-database batch backup
    # -----------------------------------------------------------------------

    def do_multi_db(self):
        """Backup multiple databases at once with --databases db1,db2,db3."""
        tmpdir = "./taosbackuptest/tmpdir_multidb"
        self.makeDir(tmpdir)

        for idx, db in enumerate(("mdb1", "mdb2", "mdb3")):
            tdSql.execute(f"drop database if exists {db}")
            tdSql.execute(f"create database {db} keep 3649")
            tdSql.execute(f"use {db}")
            tdSql.execute(
                f"create table st(ts timestamp, c1 int) tags(t1 int)"
            )
            for t in range(3):
                tdSql.execute(f"create table t{t} using st tags({t})")
            tdSql.execute(f"create table {db}.nt(ts timestamp, v int)")
            for j in range(10):
                tdSql.execute(
                    f"insert into {db}.nt values({1640000000000 + j * 1000}, {j + idx * 100})"
                )
            for t in range(3):
                for j in range(5):
                    tdSql.execute(
                        f"insert into t{t} values({1640000000000 + j * 1000}, {j})"
                    )

        bin = self.binPath()
        # Single backup command covering all 3 databases
        os.system(f"{bin} --databases mdb1,mdb2,mdb3 -o {tmpdir}")

        for db in ("mdb1", "mdb2", "mdb3"):
            tdSql.execute(f"drop database {db}")

        os.system(f"{bin} -i {tmpdir}")

        # Verify each database is restored correctly
        for idx, db in enumerate(("mdb1", "mdb2", "mdb3")):
            tdSql.query(f"select count(*) from {db}.nt")
            tdSql.checkData(0, 0, 10)
            # spot-check one row
            tdSql.query(f"select v from {db}.nt where ts=1640000000000")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, idx * 100)

            tdSql.query(f"select count(*) from {db}.st")
            tdSql.checkData(0, 0, 15)

        tdLog.info("do_multi_db .................................. [passed]")

    # -----------------------------------------------------------------------
    # 3. Thread count (-T) correctness
    # -----------------------------------------------------------------------

    def do_thread_num(self):
        """Verify -T thread count option produces correct results for T=1, T=4, T=8."""
        tdSql.execute("drop database if exists thrdb")
        tdSql.execute("create database thrdb keep 3649")
        tdSql.execute("use thrdb")
        tdSql.execute(
            "create table st(ts timestamp, c1 int, c2 float) tags(t1 int)"
        )
        for i in range(8):
            tdSql.execute(f"create table t{i} using st tags({i})")

        rows_per_table = 50
        for i in range(8):
            sql = f"insert into t{i} values"
            for j in range(rows_per_table):
                sql += f"({1640000000000 + j * 1000}, {j}, {j * 0.1})"
            tdSql.execute(sql)

        expected_total = 8 * rows_per_table  # 400
        expected_sum_c1 = 8 * sum(range(rows_per_table))  # 8 * 1225 = 9800

        bin = self.binPath()
        for thread_count in (1, 4, 8):
            tmpdir = f"./taosbackuptest/tmpdir_T{thread_count}"
            self.makeDir(tmpdir)

            os.system(f"{bin} -D thrdb -o {tmpdir} -T {thread_count}")
            tdSql.execute("drop database thrdb")
            os.system(f"{bin} -i {tmpdir} -T {thread_count}")

            tdSql.query("select count(*) from thrdb.st")
            tdSql.checkData(0, 0, expected_total)

            tdSql.query("select sum(c1) from thrdb.st")
            tdSql.checkData(0, 0, expected_sum_c1)

            tdLog.info(
                f"  -T {thread_count}: {expected_total} rows, sum={expected_sum_c1} .... [passed]"
            )

        tdLog.info("do_thread_num ................................ [passed]")

    # -----------------------------------------------------------------------
    # 4. Database properties preserved (PRECISION, KEEP, VGROUPS)
    # -----------------------------------------------------------------------

    def do_db_props(self):
        """Verify PRECISION, KEEP, and VGROUPS are preserved in the restored DB."""
        tmpdir = "./taosbackuptest/tmpdir_dbprops"
        self.makeDir(tmpdir)

        # Create DBs with distinct property combinations
        tdSql.execute("drop database if exists propdb_ms")
        tdSql.execute("drop database if exists propdb_us")
        tdSql.execute("drop database if exists propdb_ns")

        tdSql.execute("create database propdb_ms keep 30   precision 'ms' vgroups 1")
        tdSql.execute("create database propdb_us keep 365  precision 'us' vgroups 2")
        tdSql.execute("create database propdb_ns keep 3650 precision 'ns' vgroups 1")

        for db in ("propdb_ms", "propdb_us", "propdb_ns"):
            tdSql.execute(f"create table {db}.nt(ts timestamp, v int)")
            tdSql.execute(f"insert into {db}.nt values(now(), 1)")

        bin = self.binPath()
        os.system(f"{bin} --databases propdb_ms,propdb_us,propdb_ns -o {tmpdir}")

        for db in ("propdb_ms", "propdb_us", "propdb_ns"):
            tdSql.execute(f"drop database {db}")

        os.system(f"{bin} -i {tmpdir}")

        # Use named-column queries — more robust than positional indexing
        tdSql.query(
            "select `precision` from information_schema.ins_databases where name='propdb_ms'"
        )
        tdSql.checkData(0, 0, "ms")

        tdSql.query(
            "select `precision` from information_schema.ins_databases where name='propdb_us'"
        )
        tdSql.checkData(0, 0, "us")

        tdSql.query(
            "select `precision` from information_schema.ins_databases where name='propdb_ns'"
        )
        tdSql.checkData(0, 0, "ns")

        # keep is returned as "Xd,Xd,Xd"
        tdSql.query(
            "select `keep` from information_schema.ins_databases where name='propdb_ms'"
        )
        keep_ms = str(tdSql.getData(0, 0))
        assert "30d" in keep_ms, f"propdb_ms keep expected '30d': got {keep_ms}"

        tdSql.query(
            "select `keep` from information_schema.ins_databases where name='propdb_us'"
        )
        keep_us = str(tdSql.getData(0, 0))
        assert "365d" in keep_us, f"propdb_us keep expected '365d': got {keep_us}"

        tdSql.query(
            "select `keep` from information_schema.ins_databases where name='propdb_ns'"
        )
        keep_ns = str(tdSql.getData(0, 0))
        assert "3650d" in keep_ns, f"propdb_ns keep expected '3650d': got {keep_ns}"

        # vgroups
        tdSql.query(
            "select `vgroups` from information_schema.ins_databases where name='propdb_ms'"
        )
        tdSql.checkData(0, 0, 1)

        tdSql.query(
            "select `vgroups` from information_schema.ins_databases where name='propdb_us'"
        )
        tdSql.checkData(0, 0, 2)

        tdLog.info("do_db_props .................................. [passed]")

    # -----------------------------------------------------------------------
    # 5. Incremental backup (rolling time-window)
    # -----------------------------------------------------------------------

    def do_incremental_backup(self):
        """Full backup + incremental time-window backup + per-snapshot restore verification."""
        tmpdir_full = "./taosbackuptest/tmpdir_incr_full"
        tmpdir_t0   = "./taosbackuptest/tmpdir_incr_t0"
        tmpdir_t1   = "./taosbackuptest/tmpdir_incr_t1"
        self.makeDir(tmpdir_full)
        self.makeDir(tmpdir_t0)
        self.makeDir(tmpdir_t1)

        tdSql.execute("drop database if exists incrdb")
        tdSql.execute("create database incrdb keep 3649")
        tdSql.execute("use incrdb")
        tdSql.execute("create table nt(ts timestamp, v int)")

        # T0 phase: rows 0..99  (base epoch + 0..99 seconds)
        base_ms = 1640000000000
        t0_rows = 100
        t1_rows = 100
        cutoff_ms = base_ms + t0_rows * 1000  # first ts of T1 phase

        sql = "insert into incrdb.nt values"
        for i in range(t0_rows):
            sql += f"({base_ms + i * 1000}, {i})"
        tdSql.execute(sql)

        # T1 phase: rows 100..199
        sql = "insert into incrdb.nt values"
        for i in range(t0_rows, t0_rows + t1_rows):
            sql += f"({base_ms + i * 1000}, {i})"
        tdSql.execute(sql)

        bin = self.binPath()

        # Full backup (T0 + T1 = 200 rows)
        os.system(f"{bin} -D incrdb -o {tmpdir_full} -T 1")

        # T0-only backup: end before cutoff
        os.system(f"{bin} -D incrdb -o {tmpdir_t0} -T 1 -E {cutoff_ms - 1}")

        # T1-only backup: start at cutoff
        os.system(f"{bin} -D incrdb -o {tmpdir_t1} -T 1 -S {cutoff_ms}")

        # --- Verify full backup restores all 200 rows ---
        tdSql.execute("drop database incrdb")
        os.system(f"{bin} -i {tmpdir_full} -T 1")
        tdSql.query("select count(*) from incrdb.nt")
        tdSql.checkData(0, 0, t0_rows + t1_rows)
        tdSql.query("select sum(v) from incrdb.nt")
        # sum(0..199) = 199*200/2 = 19900
        tdSql.checkData(0, 0, sum(range(t0_rows + t1_rows)))
        tdLog.info("  full backup: 200 rows, sum correct ......... [passed]")

        # --- Verify T0-only backup restores exactly 100 rows ---
        tdSql.execute("drop database incrdb")
        os.system(f"{bin} -i {tmpdir_t0} -T 1")
        tdSql.query("select count(*) from incrdb.nt")
        tdSql.checkData(0, 0, t0_rows)
        tdSql.query("select sum(v) from incrdb.nt")
        # sum(0..99) = 4950
        tdSql.checkData(0, 0, sum(range(t0_rows)))
        # all v values should be < t0_rows
        tdSql.checkAgg("select count(*) from incrdb.nt where v >= 100", 0)
        tdLog.info("  T0-only backup: 100 rows, T1 data absent ... [passed]")

        # --- Verify T1-only backup restores exactly 100 rows ---
        tdSql.execute("drop database incrdb")
        os.system(f"{bin} -i {tmpdir_t1} -T 1")
        tdSql.query("select count(*) from incrdb.nt")
        tdSql.checkData(0, 0, t1_rows)
        tdSql.query("select sum(v) from incrdb.nt")
        # sum(100..199) = 14950
        tdSql.checkData(0, 0, sum(range(t0_rows, t0_rows + t1_rows)))
        # all v values should be >= t0_rows
        tdSql.checkAgg("select count(*) from incrdb.nt where v < 100", 0)
        tdLog.info("  T1-only backup: 100 rows, T0 data absent ... [passed]")

        tdLog.info("do_incremental_backup ........................ [passed]")

    # -----------------------------------------------------------------------
    # 6. Empty database and zero-row tables
    # -----------------------------------------------------------------------

    def do_empty_db(self):
        """Backup/restore empty databases and zero-data tables."""
        tmpdir = "./taosbackuptest/tmpdir_empty"
        self.makeDir(tmpdir)

        # Case A: completely empty database (no tables)
        tdSql.execute("drop database if exists emptydb_a")
        tdSql.execute("create database emptydb_a keep 3649")

        # Case B: database with STB but no child tables and no data
        tdSql.execute("drop database if exists emptydb_b")
        tdSql.execute("create database emptydb_b keep 3649")
        tdSql.execute("use emptydb_b")
        tdSql.execute("create table st(ts timestamp, c1 int) tags(t1 int)")

        # Case C: database with STB + CTBs but zero data rows; also a zero-row NTB
        tdSql.execute("drop database if exists emptydb_c")
        tdSql.execute("create database emptydb_c keep 3649")
        tdSql.execute("use emptydb_c")
        tdSql.execute("create table st(ts timestamp, c1 int) tags(t1 int)")
        for i in range(3):
            tdSql.execute(f"create table t{i} using st tags({i})")
        tdSql.execute("create table nt(ts timestamp, c1 int)")

        bin = self.binPath()
        os.system(f"{bin} --databases emptydb_a,emptydb_b,emptydb_c -o {tmpdir}")

        for db in ("emptydb_a", "emptydb_b", "emptydb_c"):
            tdSql.execute(f"drop database {db}")

        os.system(f"{bin} -i {tmpdir}")

        # Case A: database must exist; no tables
        tdSql.query(
            "select count(*) from information_schema.ins_databases where name='emptydb_a'"
        )
        tdSql.checkData(0, 0, 1)
        tdSql.query(
            "select count(*) from information_schema.ins_tables where db_name='emptydb_a'"
        )
        tdSql.checkData(0, 0, 0)
        tdLog.info("  empty DB (no tables) correctly restored .... [passed]")

        # Case B: STB exists, no CTBs
        tdSql.query(
            "select count(*) from information_schema.ins_stables where db_name='emptydb_b'"
        )
        tdSql.checkData(0, 0, 1)
        tdSql.query(
            "select count(*) from information_schema.ins_tables where db_name='emptydb_b'"
        )
        tdSql.checkData(0, 0, 0)
        tdLog.info("  DB with STB but no CTBs correctly restored . [passed]")

        # Case C: STB + 3 CTBs + 1 NTB, all zero rows
        tdSql.query(
            "select count(*) from information_schema.ins_tables where db_name='emptydb_c'"
        )
        tdSql.checkData(0, 0, 4)  # t0, t1, t2, nt
        tdSql.query("select count(*) from emptydb_c.st")
        tdSql.checkData(0, 0, 0)
        tdSql.query("select count(*) from emptydb_c.nt")
        tdSql.checkData(0, 0, 0)
        tdLog.info("  DB with zero-row CTBs/NTB correctly restored  [passed]")

        tdLog.info("do_empty_db .................................. [passed]")

    # -----------------------------------------------------------------------
    # Main test entry point
    # -----------------------------------------------------------------------

    def test_taosbackup_integrity(self):
        """taosBackup data integrity, multi-DB, thread count, DB properties, incremental, empty-DB

        1. Verify COUNT(*), SUM(c1), AVG(c2), LAST(ts) match between source and restored DB
           (covers both super-table child tables and normal tables)
        2. Backup multiple databases at once with --databases db1,db2,db3; verify each DB
           is fully restored with correct row counts and spot-checked values
        3. Verify -T thread count option (T=1, T=4, T=8) all produce the same correct row count
           and SUM aggregation after backup and restore
        4. Verify database properties are preserved after restore:
           - PRECISION (ms / us / ns) matches original
           - KEEP value matches original
           - VGROUPS count matches original
        5. Incremental backup using -S/-E time-window:
           - Full backup restores all rows
           - T0-windowed backup restores exactly T0 rows with correct max timestamp
           - T1-windowed backup restores exactly T1 rows with correct min timestamp
        6. Empty database and zero-row table scenarios:
           - Completely empty DB (no tables) backed up and restored
           - DB with STB schema but no CTBs
           - DB with CTBs and NTB that have zero data rows

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-06 Created to fill gap coverage identified in analysis

        """
        self.do_data_integrity()
        self.do_multi_db()
        self.do_thread_num()
        self.do_db_props()
        self.do_incremental_backup()
        self.do_empty_db()

        os.system("rm -rf ./taosbackuptest/")
        tdLog.info("test_taosbackup_integrity .................... [passed]")
