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
import string
import random
import subprocess


class TestTaosBackupBasic:

    def exec(self, command):
        """Run a shell command, stream output, and fail the test on non-zero exit code."""
        tdLog.info(command)
        result = subprocess.run(
            command, shell=True, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
        if result.stdout:
            for line in result.stdout.splitlines():
                tdLog.info(line)
        if result.returncode != 0:
            tdLog.exit(f"Command failed (rc={result.returncode}): {command}")
        return result.returncode

    def taosbackup(self, command):
        """Run taosBackup command and return output list."""
        return etool.taosbackup(command)

    def makeDir(self, path):
        if not os.path.exists(path):
            os.makedirs(path)
        else:
            os.system("rm -rf %s" % path)
            os.makedirs(path)

    def dbFound(self, dbname):
        tdSql.query("show databases")
        dbresult = tdSql.queryResult
        for row in dbresult:
            if row[0] == dbname:
                return True
        return False

    #
    # ------------------- do_taosbackup_basic ----------------
    #
    def do_taosbackup_basic(self):
        """Basic STB/CTB/NTB backup restore.

        Since: v3.0.0.0

        Labels: common

        Jira: None

        History:

            - 2026-03-04 Alex Duan Created

        """
        tmpdir = "./taosbackuptest/tmpdir_basic"

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db keep 3649")
        tdSql.execute("use db")
        # All data types including NCHAR are tested here.
        # NCHAR columns are used in both data (via STMT2) and tags.
        tdSql.execute(
            "create table st(ts timestamp, c1 INT, c2 BOOL, c3 TINYINT, c4 SMALLINT, c5 BIGINT, "
            "c6 FLOAT, c7 DOUBLE, c8 TIMESTAMP, c9 BINARY(10), c10 NCHAR(10), "
            "c11 TINYINT UNSIGNED, c12 SMALLINT UNSIGNED, c13 INT UNSIGNED, c14 BIGINT UNSIGNED) "
            "tags(n1 INT, w2 BOOL, t3 TINYINT, t4 SMALLINT, t5 BIGINT, t6 FLOAT, t7 DOUBLE, "
            "t8 TIMESTAMP, t9 BINARY(10), t10 NCHAR(10), t11 TINYINT UNSIGNED, t12 SMALLINT UNSIGNED, "
            "t13 INT UNSIGNED, t14 BIGINT UNSIGNED)"
        )
        tdSql.execute(
            "create table t1 using st tags(1, true, 1, 1, 1, 1.0, 1.0, 1, '1', 'nc1', 1, 1, 1, 1)"
        )
        tdSql.execute(
            "insert into t1 values(1640000000000, 1, true, 1, 1, 1, 1.0, 1.0, 1, '1', 'nc1', 1, 1, 1, 1)"
        )
        tdSql.execute(
            "create table t2 using st tags(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
        )
        tdSql.execute(
            "insert into t2 values(1640000000000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
        )
        tdSql.execute(
            "create table db.nt1 (ts timestamp, c1 INT, c2 BOOL, c3 TINYINT, c4 SMALLINT, c5 BIGINT, "
            "c6 FLOAT, c7 DOUBLE, c8 TIMESTAMP, c9 BINARY(10), c10 NCHAR(10), "
            "c11 TINYINT UNSIGNED, c12 SMALLINT UNSIGNED, c13 INT UNSIGNED, c14 BIGINT UNSIGNED)"
        )
        tdSql.execute(
            "insert into nt1 values(1640000000000, 1, true, 1, 1, 1, 1.0, 1.0, 1, '1', 'nc1', 1, 1, 1, 1)"
        )
        tdSql.execute(
            "insert into nt1 values(1640000000001, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
        )

        binPath = etool.taosBackupFile()
        if binPath == "":
            tdLog.exit("taosBackup not found!")
        else:
            tdLog.info("taosBackup found in %s" % binPath)

        self.makeDir(tmpdir)

        self.exec("%s -D db -o %s -T 1" % (binPath, tmpdir))

        tdSql.execute("drop database db")

        self.exec("%s -i %s -T 1" % (binPath, tmpdir))

        assert self.dbFound("db"), "database 'db' not found after restore"

        tdSql.execute("use db")
        tdSql.query("show stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "st")

        tdSql.query("show tables")
        tdSql.checkRows(3)

        tdSql.query("select * from db.t1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)

        tdSql.query("select * from db.t2")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)

        tdSql.query("select count(*) from db.nt1")
        tdSql.checkData(0, 0, 2)

        tdLog.info("do_taosbackup_basic ......................... [passed]")

    #
    # ------------------- do_taosbackup_rename_db ----------------
    #
    def do_taosbackup_rename_db(self):
        """Rename database on restore.

        Since: v3.0.0.0

        Labels: common

        Jira: None

        History:

            - 2026-03-04 Alex Duan Created

        """
        ts = 1538548685000
        tmpdir1 = "./taosbackuptest/tmp1"
        tmpdir2 = "./taosbackuptest/tmp2"

        self.makeDir(tmpdir1)
        self.makeDir(tmpdir2)

        tdSql.execute("drop database if exists db")
        tdSql.execute("drop database if exists db1")
        tdSql.execute("drop database if exists newdb")
        tdSql.execute("drop database if exists newdb1")
        tdSql.execute("create database db duration 11 keep 3649")
        tdSql.execute("create database db1 duration 12 keep 3640")
        tdSql.execute("use db")
        tdSql.execute(
            "create table st(ts timestamp, c1 int, c2 nchar(10)) tags(t1 int, t2 binary(10))"
        )
        tdSql.execute("create table t1 using st tags(1, 'beijing')")
        sql = "insert into t1 values"
        currts = ts
        for i in range(100):
            sql += "(%d, %d, 'nchar%d')" % (currts + i, i % 100, i % 100)
        tdSql.execute(sql)
        tdSql.execute("create table t2 using st tags(2, 'shanghai')")
        sql = "insert into t2 values"
        currts = ts
        for i in range(100):
            sql += "(%d, %d, 'nchar%d')" % (currts + i, i % 100, i % 100)
        tdSql.execute(sql)

        binPath = etool.taosBackupFile()
        if binPath == "":
            tdLog.exit("taosBackup not found!")

        self.exec("%s --databases db -o %s" % (binPath, tmpdir1))
        self.exec("%s -D db1 -o %s" % (binPath, tmpdir2))

        tdSql.execute("drop database db")
        tdSql.execute("drop database db1")

        # restore with renamed databases
        self.exec('%s -W "db=newdb" -i %s' % (binPath, tmpdir1))
        self.exec('%s -W "db1=newdb1" -i %s' % (binPath, tmpdir2))

        tdSql.query("select * from information_schema.ins_databases")
        dbresult = tdSql.queryResult
        dbnames = [row[0] for row in dbresult]
        assert "newdb" in dbnames, "newdb not found after restore"
        assert "newdb1" in dbnames, "newdb1 not found after restore"

        # check db properties preserved
        for row in dbresult:
            if row[0] == "newdb":
                assert row[6] == "11d", f"duration expected 11d, got {row[6]}"
                assert row[7] == "3649d,3649d,3649d", f"keep expected 3649d, got {row[7]}"
            if row[0] == "newdb1":
                assert row[6] == "12d", f"duration expected 12d, got {row[6]}"
                assert row[7] == "3640d,3640d,3640d", f"keep expected 3640d, got {row[7]}"

        tdSql.execute("use newdb")
        tdSql.query("show stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "st")

        tdSql.query("show tables")
        tdSql.checkRows(2)

        tdSql.query("select * from t1")
        tdSql.checkRows(100)
        for i in range(100):
            tdSql.checkData(i, 1, i)
            tdSql.checkData(i, 2, "nchar%d" % i)

        # cleanup
        tdSql.execute("drop database newdb")
        tdSql.execute("drop database newdb1")
        os.system("rm -rf %s" % tmpdir1)
        os.system("rm -rf %s" % tmpdir2)

        tdLog.info("do_taosbackup_rename_db ..................... [passed]")

    #
    # ------------------- do_taosbackup_schemaonly ----------------
    #
    def do_taosbackup_schemaonly(self):
        """Schema-only backup no data.

        Since: v3.0.0.0

        Labels: common

        Jira: None

        History:

            - 2026-03-04 Alex Duan Created

        """
        tmpdir = "./taosbackuptest/tmpdir_schema"

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db keep 3649")
        tdSql.execute("use db")
        tdSql.execute(
            "create table st(ts timestamp, c1 int, c2 binary(16)) tags(t1 int)"
        )
        tdSql.execute("create table t1 using st tags(1)")
        tdSql.execute("insert into t1 values(1640000000000, 1, 'hello')")
        tdSql.execute("insert into t1 values(1640000000001, 2, 'world')")
        tdSql.execute("create table nt1(ts timestamp, c1 int)")
        tdSql.execute("insert into nt1 values(1640000000000, 100)")

        binPath = etool.taosBackupFile()
        if binPath == "":
            tdLog.exit("taosBackup not found!")

        self.makeDir(tmpdir)

        # backup schema only
        self.exec("%s -s -D db -o %s -T 1" % (binPath, tmpdir))

        tdSql.execute("drop database db")

        self.exec("%s -i %s -T 1" % (binPath, tmpdir))

        assert self.dbFound("db"), "database 'db' not found after schema-only restore"

        tdSql.execute("use db")
        tdSql.query("show stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "st")

        tdSql.query("show tables")
        tdSql.checkRows(2)

        # schema only: no data should be restored
        tdSql.query("select count(*) from db.st")
        tdSql.checkData(0, 0, 0)

        tdSql.query("select count(*) from db.nt1")
        tdSql.checkData(0, 0, 0)

        tdLog.info("do_taosbackup_schemaonly .................... [passed]")

    #
    # ------------------- do_taosbackup_db_ntb ----------------
    #
    def do_taosbackup_db_ntb(self):
        """Backup specific child table.

        Since: v3.0.0.0

        Labels: common

        Jira: None

        History:

            - 2026-03-04 Alex Duan Created

        """
        tmpdir = "./taosbackuptest/tmpdir_ntb"

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db keep 3649")
        tdSql.execute("use db")
        tdSql.execute(
            "create table st(ts timestamp, c1 INT) tags(n1 INT)"
        )
        tdSql.execute("create table t1 using st tags(1)")
        tdSql.execute("insert into t1 values(1640000000000, 1)")
        tdSql.execute("create table t2 using st tags(2)")
        tdSql.execute("insert into t2 values(1640000000000, 2)")
        tdSql.execute("create table db.nt1 (ts timestamp, c1 INT)")
        tdSql.execute("insert into nt1 values(1640000000000, 100)")

        binPath = etool.taosBackupFile()
        if binPath == "":
            tdLog.exit("taosBackup not found!")

        self.makeDir(tmpdir)

        # dump only t1 (ctable) from db
        self.exec("%s db t1 -o %s -T 1" % (binPath, tmpdir))

        tdSql.execute("drop database db")

        self.exec('%s -i %s -T 1 -W "db=newdb"' % (binPath, tmpdir))

        assert self.dbFound("newdb"), "newdb not found after restore"

        tdSql.execute("use newdb")
        tdSql.query("show stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "st")

        # only t1 should be restored, not t2 or nt1
        tdSql.query("show tables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "t1")

        tdSql.execute("drop database newdb")
        tdLog.info("do_taosbackup_db_ntb ........................ [passed]")

    #
    # ------------------- do_taosbackup_db_stb ----------------
    #
    def do_taosbackup_db_stb(self):
        """Backup specific super table.

        Since: v3.0.0.0

        Labels: common

        Jira: None

        History:

            - 2026-03-04 Alex Duan Created

        """
        tmpdir = "./taosbackuptest/tmpdir_stb"

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db keep 3649")
        tdSql.execute("use db")
        tdSql.execute(
            "create table st(ts timestamp, c1 INT) tags(n1 INT)"
        )
        tdSql.execute("create table t1 using st tags(1)")
        tdSql.execute("insert into t1 values(1640000000000, 1)")
        tdSql.execute("create table t2 using st tags(2)")
        tdSql.execute("insert into t2 values(1640000000000, 2)")
        tdSql.execute(
            "create table st2(ts timestamp, c1 INT) tags(n1 INT)"
        )
        tdSql.execute("create table t3 using st2 tags(3)")
        tdSql.execute("insert into t3 values(1640000000000, 3)")

        binPath = etool.taosBackupFile()
        if binPath == "":
            tdLog.exit("taosBackup not found!")

        self.makeDir(tmpdir)

        # dump only super table st (plus its child tables)
        self.exec("%s db st -o %s -T 1" % (binPath, tmpdir))

        tdSql.execute("drop database db")

        self.exec('%s -i %s -T 1 -W "db=newdb"' % (binPath, tmpdir))

        assert self.dbFound("newdb"), "newdb not found after restore"

        tdSql.execute("use newdb")
        tdSql.query("show stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "st")

        # t1 and t2 should be restored (children of st), not t3
        tdSql.query("show tables")
        tdSql.checkRows(2)

        tdSql.execute("drop database newdb")
        tdLog.info("do_taosbackup_db_stb ........................ [passed]")

    #
    # ------------------- do_taosbackup_format_parquet ----------------
    #
    def do_taosbackup_format_parquet(self):
        """Parquet format backup restore.

        Since: v3.0.0.0

        Labels: common

        Jira: None

        History:

            - 2026-03-04 Alex Duan Created

        """
        tmpdir = "./taosbackuptest/tmpdir_parquet"

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db keep 3649")
        tdSql.execute("use db")
        tdSql.execute(
            "create table st(ts timestamp, c1 int, c2 float, c3 binary(20)) tags(t1 int)"
        )
        tdSql.execute("create table t1 using st tags(1)")
        for i in range(50):
            tdSql.execute(
                "insert into t1 values(%d, %d, %f, 'val%d')"
                % (1640000000000 + i, i, i * 1.0, i)
            )
        tdSql.execute("create table t2 using st tags(2)")
        for i in range(50):
            tdSql.execute(
                "insert into t2 values(%d, %d, %f, 'val%d')"
                % (1640000000000 + i, i + 50, (i + 50) * 1.0, i + 50)
            )

        binPath = etool.taosBackupFile()
        if binPath == "":
            tdLog.exit("taosBackup not found!")

        self.makeDir(tmpdir)

        # backup with parquet format
        self.exec("%s -F parquet -D db -o %s -T 1" % (binPath, tmpdir))

        tdSql.execute("drop database db")

        # restore with parquet format
        self.exec("%s -F parquet -i %s -T 1" % (binPath, tmpdir))

        assert self.dbFound("db"), "database 'db' not found after parquet restore"

        tdSql.execute("use db")
        tdSql.query("show stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "st")

        tdSql.query("select count(*) from db.st")
        tdSql.checkData(0, 0, 100)

        tdSql.query("select sum(c1) from db.st")
        tdSql.checkData(0, 0, sum(range(100)))

        tdLog.info("do_taosbackup_format_parquet ................ [passed]")

    #
    # ------------------- do_taosbackup_many_cols ----------------
    #
    def generateString(self, length):
        chars = string.ascii_uppercase + string.ascii_lowercase
        return "".join(random.choice(chars) for _ in range(length))

    def do_taosbackup_test_large_data(self):
        """Large data backup restore.

        Since: v3.0.0.0

        Labels: common

        Jira: None

        History:

            - 2026-03-04 Alex Duan Created

        """
        tmpdir = "./taosbackuptest/tmpdir_large"
        ts = 1601481600000
        numberOfRecords = 150

        self.makeDir(tmpdir)

        tdSql.prepare()

        tdSql.execute(
            "create table st(ts timestamp, c1 timestamp, c2 int, c3 bigint, c4 float, "
            "c5 double, c6 binary(8), c7 smallint, c8 tinyint, c9 bool, c10 nchar(8)) tags(t1 int)"
        )
        tdSql.execute("create table t1 using st tags(0)")
        currts = ts
        finish = 0
        while finish < numberOfRecords:
            sql = "insert into t1 values"
            for i in range(finish, numberOfRecords):
                sql += (
                    "(%d, 1019774612, 29931, 1442173978, 165092.468750, 1128.643179, "
                    "'MOCq1pTu', 18405, 82, 0, 'g0A6S0Fu')" % (currts + i)
                )
                finish = i + 1
                if (1048576 - len(sql)) < 65519:
                    break
            tdSql.execute(sql)

        binPath = etool.taosBackupFile()
        if binPath == "":
            tdLog.exit("taosBackup not found!")

        self.exec("%s --databases db -o %s" % (binPath, tmpdir))

        tdSql.execute("drop database db")

        self.exec("%s -i %s" % (binPath, tmpdir))

        assert self.dbFound("db"), "database 'db' not found after restore"

        tdSql.execute("use db")
        tdSql.query("show stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "st")

        tdSql.query("select count(*) from t1")
        tdSql.checkData(0, 0, numberOfRecords)

        # Wide binary column test (TS-1225)
        tdSql.execute("drop database if exists test")
        tdSql.execute("create database test")
        tdSql.execute("use test")
        tdSql.execute(
            "create table stb(ts timestamp, c1 binary(16374), c2 binary(16374), c3 binary(16374)) "
            "tags(t1 nchar(256))"
        )
        tdSql.execute(
            "insert into t1 using stb tags('t1') values(now, '%s', '%s', '%s')"
            % (
                self.generateString(16374),
                self.generateString(16374),
                self.generateString(16374),
            )
        )

        os.system("rm -rf %s" % tmpdir)
        os.makedirs(tmpdir)
        self.exec("%s -D test -o %s" % (binPath, tmpdir))

        tdSql.execute("drop database test")

        self.exec("%s -i %s" % (binPath, tmpdir))

        tdSql.execute("use test")
        tdSql.query("show stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "stb")

        tdSql.query("select * from stb")
        tdSql.checkRows(1)

        tdLog.info("do_taosbackup_test_large_data ............... [passed]")

    #
    # ------------------- do_taosbackup_many_cols ----------------
    #
    def do_taosbackup_many_cols(self):
        """Many columns backup restore.

        Since: v3.0.0.0

        Labels: common

        Jira: None

        History:

            - 2026-03-04 Alex Duan Created

        """
        tmpdir = "./taosbackuptest/tmpdir_many_cols"

        tdSql.prepare()
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db keep 3649")
        tdSql.execute("use db")

        maxCol = 300
        stb_sql = "create stable stb(ts timestamp"
        for index in range(maxCol):
            stb_sql += ", col%d INT" % (index + 1)
        stb_sql += ") tags(tag0 INT"
        for index in range(127):
            stb_sql += ", tag%d INT" % (index + 1)
        stb_sql += ")"
        tdSql.execute(stb_sql)

        tb_sql = "create table tb using stb tags(0"
        for index in range(127):
            tb_sql += ",%d" % (index + 1)
        tb_sql += ")"
        tdSql.execute(tb_sql)

        for record in range(100):
            ins_sql = "insert into tb values(%d" % (1640000000000 + record)
            for index in range(maxCol):
                ins_sql += ",%d" % index
            ins_sql += ")"
            tdSql.execute(ins_sql)

        binPath = etool.taosBackupFile()
        if binPath == "":
            tdLog.exit("taosBackup not found!")

        self.makeDir(tmpdir)

        self.exec("%s db -o %s -T 1" % (binPath, tmpdir))

        tdSql.execute("drop database db")

        self.exec("%s -i %s -T 1" % (binPath, tmpdir))

        assert self.dbFound("db"), "database 'db' not found after restore"

        tdSql.execute("use db")
        tdSql.query("show stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "stb")

        tdSql.query("show tables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "tb")

        tdSql.query("select count(*) from db.stb")
        tdSql.checkData(0, 0, 100)

        tdLog.info("do_taosbackup_many_cols ..................... [passed]")

    #
    # ------------------- do_taosbackup_tag_threads ----------------
    #
    def do_taosbackup_tag_threads(self):
        """Tag thread backup option.

        Since: v3.0.0.0

        Labels: common

        Jira: None

        History:

            - 2026-03-04 Alex Duan Created

        """
        tmpdir = "./taosbackuptest/tmpdir_tag_threads"

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db keep 3649")
        tdSql.execute("use db")
        tdSql.execute(
            "create table st(ts timestamp, c1 int) tags(t1 int, t2 binary(16))"
        )
        for i in range(20):
            tdSql.execute(
                "create table t%d using st tags(%d, 'tag%d')" % (i, i, i)
            )
            tdSql.execute(
                "insert into t%d values(1640000000000, %d)" % (i, i)
            )

        binPath = etool.taosBackupFile()
        if binPath == "":
            tdLog.exit("taosBackup not found!")

        self.makeDir(tmpdir)

        # backup with 2 tag threads
        self.exec("%s -m 2 -T 2 -D db -o %s" % (binPath, tmpdir))

        tdSql.execute("drop database db")

        self.exec("%s -T 2 -i %s" % (binPath, tmpdir))

        assert self.dbFound("db"), "database 'db' not found after restore"

        tdSql.execute("use db")
        tdSql.query("show tables")
        tdSql.checkRows(20)

        tdSql.query("select count(*) from db.st")
        tdSql.checkData(0, 0, 20)

        tdLog.info("do_taosbackup_tag_threads ................... [passed]")

    #
    # ------------------- do_taosbackup_stmt_version ----------------
    #
    def do_taosbackup_stmt_version(self):
        """STMT version restore option.

        Since: v3.0.0.0

        Labels: common

        Jira: None

        History:

            - 2026-03-04 Alex Duan Created

        """
        tmpdir = "./taosbackuptest/tmpdir_stmt_ver"

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db keep 3649")
        tdSql.execute("use db")
        tdSql.execute(
            "create table st(ts timestamp, c1 int, c2 binary(32)) tags(t1 int)"
        )
        tdSql.execute("create table t1 using st tags(1)")
        for i in range(50):
            tdSql.execute(
                "insert into t1 values(%d, %d, 'val%d')"
                % (1640000000000 + i, i, i)
            )

        binPath = etool.taosBackupFile()
        if binPath == "":
            tdLog.exit("taosBackup not found!")

        self.makeDir(tmpdir)

        # backup
        self.exec("%s -D db -o %s -T 1" % (binPath, tmpdir))

        # restore with stmt version 1
        tdSql.execute("drop database db")
        self.exec("%s -v 1 -i %s -T 1" % (binPath, tmpdir))

        assert self.dbFound("db"), "database 'db' not found after STMT v1 restore"

        tdSql.execute("use db")
        tdSql.query("select count(*) from db.st")
        tdSql.checkData(0, 0, 50)

        # restore with stmt version 2 (default)
        tdSql.execute("drop database db")
        self.exec("%s -v 2 -i %s -T 1" % (binPath, tmpdir))

        assert self.dbFound("db"), "database 'db' not found after STMT v2 restore"

        tdSql.execute("use db")
        tdSql.query("select count(*) from db.st")
        tdSql.checkData(0, 0, 50)

        tdLog.info("do_taosbackup_stmt_version .................. [passed]")

    #
    # ------------------- do_taosbackup_special_types ----------------
    #
    def do_taosbackup_special_types(self):
        """Special column types backup.

        Since: v3.0.0.0

        Labels: common

        Jira: None

        History:

            - 2026-03-04 Alex Duan Created

        """
        tmpdir = "./taosbackuptest/tmpdir_special_types"

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db keep 3649")
        tdSql.execute("use db")

        # Super table: VARBINARY, GEOMETRY, DECIMAL, BLOB columns; VARBINARY tag
        tdSql.execute(
            "create table st("
            "  ts timestamp,"
            "  c1 varbinary(32),"
            "  c2 geometry(128),"
            "  c3 decimal(10,3),"
            "  c4 blob"
            ") tags(t1 int, t2 varbinary(16))"
        )
        tdSql.execute("create table ct1 using st tags(1, '\\x4142')")
        # row with real data
        tdSql.execute(
            "insert into ct1 values("
            "  1640000000000,"
            "  '\\x48454C4C4F',"
            "  'POINT (1.000000 2.000000)',"
            "  123.456,"
            "  '\\x424c4f42'"
            ")"
        )
        # row with NULLs
        tdSql.execute(
            "insert into ct1 values(1640000000001, NULL, NULL, NULL, NULL)"
        )

        # Normal table: same columns
        tdSql.execute(
            "create table nt1("
            "  ts timestamp,"
            "  c1 varbinary(32),"
            "  c2 geometry(128),"
            "  c3 decimal(10,3),"
            "  c4 blob"
            ")"
        )
        tdSql.execute(
            "insert into nt1 values("
            "  1640000000000,"
            "  '\\x48454C4C4F',"
            "  'POINT (1.000000 2.000000)',"
            "  123.456,"
            "  '\\x424c4f42'"
            ")"
        )
        tdSql.execute(
            "insert into nt1 values(1640000000001, NULL, NULL, NULL, NULL)"
        )

        binPath = etool.taosBackupFile()
        if binPath == "":
            tdLog.exit("taosBackup not found!")

        self.makeDir(tmpdir)
        self.exec("%s -D db -o %s -T 1" % (binPath, tmpdir))

        tdSql.execute("drop database db")
        self.exec("%s -i %s -T 1" % (binPath, tmpdir))

        assert self.dbFound("db"), "database 'db' not found after restore"
        tdSql.execute("use db")

        # Verify STB row count
        tdSql.query("select count(*) from db.ct1")
        tdSql.checkData(0, 0, 2)

        # Verify NTB row count
        tdSql.query("select count(*) from db.nt1")
        tdSql.checkData(0, 0, 2)

        # Verify STB data row (real values)
        tdSql.query("select c1, c2, c3, c4 from db.ct1 where ts=1640000000000")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None if tdSql.queryResult[0][0] is None else tdSql.queryResult[0][0])  # not NULL
        assert tdSql.queryResult[0][0] is not None, "ct1.c1 (varbinary) should not be NULL after restore"
        assert tdSql.queryResult[0][1] is not None, "ct1.c2 (geometry) should not be NULL after restore"
        # DECIMAL value check
        c3_val = float(str(tdSql.queryResult[0][2]))
        assert abs(c3_val - 123.456) < 0.001, f"ct1.c3 (decimal) expected ~123.456, got {c3_val}"
        assert tdSql.queryResult[0][3] is not None, "ct1.c4 (blob) should not be NULL after restore"

        # Verify STB NULL row
        tdSql.query("select c1, c2, c3, c4 from db.ct1 where ts=1640000000001")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(0, 3, None)

        # Verify NTB data row
        tdSql.query("select c1, c2, c3, c4 from db.nt1 where ts=1640000000000")
        tdSql.checkRows(1)
        assert tdSql.queryResult[0][0] is not None, "nt1.c1 (varbinary) should not be NULL after restore"
        assert tdSql.queryResult[0][1] is not None, "nt1.c2 (geometry) should not be NULL after restore"
        c3_nt = float(str(tdSql.queryResult[0][2]))
        assert abs(c3_nt - 123.456) < 0.001, f"nt1.c3 (decimal) expected ~123.456, got {c3_nt}"
        assert tdSql.queryResult[0][3] is not None, "nt1.c4 (blob) should not be NULL after restore"

        # Verify NTB NULL row
        tdSql.query("select c1, c2, c3, c4 from db.nt1 where ts=1640000000001")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdLog.info("do_taosbackup_special_types ................. [passed]")

    #
    # ------------------- do_taosbackup_skip_empty_ctb ----------------
    #
    def do_taosbackup_skip_empty_ctb(self):
        """Skip empty child tables.

        Since: v3.0.0.0

        Labels: common

        Jira: None

        History:

            - 2026-03-04 Alex Duan Created

        """
        tmpdir = "./taosbackuptest/tmpdir_skip_empty_ctb"

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db keep 3649")
        tdSql.execute("use db")
        tdSql.execute("create table st(ts timestamp, c1 int) tags(t1 int)")
        tdSql.execute("create table t1 using st tags(1)")
        tdSql.execute("insert into t1 values(1640000000000, 100)")  # has data
        tdSql.execute("create table t2 using st tags(2)")           # empty

        binPath = etool.taosBackupFile()
        if binPath == "":
            tdLog.exit("taosBackup not found!")

        self.makeDir(tmpdir)
        self.exec("%s -D db -o %s -T 1" % (binPath, tmpdir))

        # t1.dat must exist; t2.dat must NOT exist (empty table → skipped)
        dat_t1 = "%s/db/st_data0/t1.dat" % tmpdir
        dat_t2 = "%s/db/st_data0/t2.dat" % tmpdir
        assert os.path.exists(dat_t1), "t1.dat should exist for non-empty child table"
        assert not os.path.exists(dat_t2), "t2.dat must NOT exist for empty child table"

        tdSql.execute("drop database db")
        self.exec("%s -i %s -T 1" % (binPath, tmpdir))

        assert self.dbFound("db"), "database 'db' not found after restore"
        tdSql.execute("use db")
        # Schema for both CTBs must be present
        tdSql.query("show tables")
        tdSql.checkRows(2)
        # Only t1 carries data
        tdSql.query("select count(*) from db.t1")
        tdSql.checkData(0, 0, 1)
        tdSql.query("select count(*) from db.t2")
        tdSql.checkData(0, 0, 0)

        tdLog.info("do_taosbackup_skip_empty_ctb ................ [passed]")

    #
    # ------------------- do_taosbackup_skip_empty_ntb ----------------
    #
    def do_taosbackup_skip_empty_ntb(self):
        """Skip empty normal tables.

        Since: v3.0.0.0

        Labels: common

        Jira: None

        History:

            - 2026-03-04 Alex Duan Created

        """
        tmpdir = "./taosbackuptest/tmpdir_skip_empty_ntb"

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db keep 3649")
        tdSql.execute("use db")
        tdSql.execute("create table nt1(ts timestamp, c1 int)")
        tdSql.execute("insert into nt1 values(1640000000000, 42)")  # has data
        tdSql.execute("create table nt2(ts timestamp, c1 int)")     # empty

        binPath = etool.taosBackupFile()
        if binPath == "":
            tdLog.exit("taosBackup not found!")

        self.makeDir(tmpdir)
        self.exec("%s -D db -o %s -T 1" % (binPath, tmpdir))

        # nt1.dat must exist; nt2.dat must NOT exist
        dat_nt1 = "%s/db/_ntb_data0/nt1.dat" % tmpdir
        dat_nt2 = "%s/db/_ntb_data0/nt2.dat" % tmpdir
        assert os.path.exists(dat_nt1), "nt1.dat should exist for non-empty normal table"
        assert not os.path.exists(dat_nt2), "nt2.dat must NOT exist for empty normal table"

        tdSql.execute("drop database db")
        self.exec("%s -i %s -T 1" % (binPath, tmpdir))

        assert self.dbFound("db"), "database 'db' not found after restore"
        tdSql.execute("use db")
        tdSql.query("show tables")
        tdSql.checkRows(2)
        tdSql.query("select count(*) from db.nt1")
        tdSql.checkData(0, 0, 1)
        tdSql.query("select count(*) from db.nt2")
        tdSql.checkData(0, 0, 0)

        tdLog.info("do_taosbackup_skip_empty_ntb ................ [passed]")

    #
    # ------------------- do_taosbackup_skip_empty_time_range ----------------
    #
    def do_taosbackup_skip_empty_time_range(self):
        """Skip out-of-range CTBs.

        Since: v3.0.0.0

        Labels: common

        Jira: None

        History:

            - 2026-03-04 Alex Duan Created

        """
        tmpdir = "./taosbackuptest/tmpdir_skip_time_range"

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db keep 3649")
        tdSql.execute("use db")
        tdSql.execute("create table st(ts timestamp, c1 int) tags(t1 int)")
        tdSql.execute("create table t1 using st tags(1)")
        # t1: data in 2022 — inside the backup range
        tdSql.execute("insert into t1 values(1641000000000, 10)")
        tdSql.execute("create table t2 using st tags(2)")
        # t2: data in 2021 — outside the backup range
        tdSql.execute("insert into t2 values(1609459200000, 20)")

        binPath = etool.taosBackupFile()
        if binPath == "":
            tdLog.exit("taosBackup not found!")

        self.makeDir(tmpdir)
        # Backup only 2022 data
        self.exec(
            "%s -D db -o %s -T 1 -S '2022-01-01 00:00:00.000' -E '2022-12-31 23:59:59.999'"
            % (binPath, tmpdir)
        )

        # t1.dat must exist; t2.dat must NOT exist (0 rows in range)
        dat_t1 = "%s/db/st_data0/t1.dat" % tmpdir
        dat_t2 = "%s/db/st_data0/t2.dat" % tmpdir
        assert os.path.exists(dat_t1), "t1.dat should exist: t1 has data in time range"
        assert not os.path.exists(dat_t2), "t2.dat must NOT exist: t2 has no data in time range"

        tdSql.execute("drop database db")
        self.exec("%s -i %s -T 1" % (binPath, tmpdir))

        assert self.dbFound("db"), "database 'db' not found after restore"
        tdSql.execute("use db")
        # Schema for both CTBs must be present
        tdSql.query("show tables")
        tdSql.checkRows(2)
        # Only in-range row (t1) must be restored
        tdSql.query("select count(*) from db.t1")
        tdSql.checkData(0, 0, 1)
        tdSql.query("select count(*) from db.t2")
        tdSql.checkData(0, 0, 0)

        tdLog.info("do_taosbackup_skip_empty_time_range ......... [passed]")

    #
    # ------------------- do_taosbackup_complete_flag ----------------
    #
    def do_taosbackup_complete_flag(self):
        """Backup complete flag lifecycle.

        Since: v3.0.0.0

        Labels: common

        Jira: None

        History:

            - 2026-03-04 Alex Duan Created

        """
        tmpdir = "./taosbackuptest/tmpdir_complete_flag"

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db keep 3649")
        tdSql.execute("use db")
        tdSql.execute("create table st(ts timestamp, c1 int) tags(t1 int)")
        tdSql.execute("create table t1 using st tags(1)")
        tdSql.execute("insert into t1 values(1640000000000, 1)")

        binPath = etool.taosBackupFile()
        if binPath == "":
            tdLog.exit("taosBackup not found!")

        self.makeDir(tmpdir)

        # First backup — flag must be created on completion
        self.exec("%s -D db -o %s -T 1" % (binPath, tmpdir))
        flag_path = "%s/db/backup_complete.flag" % tmpdir
        assert os.path.exists(flag_path), "backup_complete.flag must exist after successful backup"

        # Second backup — flag is detected, deleted at start (fresh run), then recreated
        self.exec("%s -D db -o %s -T 1" % (binPath, tmpdir))
        assert os.path.exists(flag_path), "backup_complete.flag must exist after second backup"

        # Restore and verify correctness after two successive backups
        tdSql.execute("drop database db")
        self.exec("%s -i %s -T 1" % (binPath, tmpdir))

        assert self.dbFound("db"), "database 'db' not found after restore"
        tdSql.execute("use db")
        tdSql.query("select count(*) from db.st")
        tdSql.checkData(0, 0, 1)

        tdLog.info("do_taosbackup_complete_flag ................. [passed]")

    #
    # ------------------- do_taosbackup_prefilter_empty_ctb ----------------
    #
    def do_taosbackup_prefilter_empty_ctb(self):
        """Pre-filter empty CTBs.

        Since: v3.0.0.0

        Labels: common

        Jira: None

        History:
            - 2026-03-04 Alex Duan Created

        """
        tmpdir = "./taosbackuptest/tmpdir_prefilter_notf"

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db keep 3649")
        tdSql.execute("use db")
        tdSql.execute("create table st(ts timestamp, c1 int) tags(t1 int)")

        # t1, t2, t3: each has 10 rows
        for i in range(1, 4):
            tdSql.execute("create table t%d using st tags(%d)" % (i, i))
            for j in range(10):
                tdSql.execute(
                    "insert into t%d values(%d, %d)" % (i, 1640000000000 + j, j)
                )
        # t4, t5: empty
        tdSql.execute("create table t4 using st tags(4)")
        tdSql.execute("create table t5 using st tags(5)")

        binPath = etool.taosBackupFile()
        if binPath == "":
            tdLog.exit("taosBackup not found!")

        self.makeDir(tmpdir)
        self.exec("%s -D db -o %s -T 2" % (binPath, tmpdir))

        # .dat files exist only for non-empty CTBs
        for i in range(1, 4):
            dat = "%s/db/st_data0/t%d.dat" % (tmpdir, i)
            assert os.path.exists(dat), "t%d.dat must exist (non-empty CTB)" % i
        for i in range(4, 6):
            dat = "%s/db/st_data0/t%d.dat" % (tmpdir, i)
            assert not os.path.exists(dat), "t%d.dat must NOT exist (empty CTB)" % i

        tdSql.execute("drop database db")
        self.exec("%s -i %s -T 2" % (binPath, tmpdir))

        assert self.dbFound("db"), "database 'db' not found after restore"
        tdSql.execute("use db")
        # All 5 CTB schemas must be present
        tdSql.query("show tables")
        tdSql.checkRows(5)
        # Non-empty CTBs: all 10 rows intact (correctness)
        for i in range(1, 4):
            tdSql.query("select count(*) from db.t%d" % i)
            tdSql.checkData(0, 0, 10)
        # Empty CTBs: schema present, 0 rows
        for i in range(4, 6):
            tdSql.query("select count(*) from db.t%d" % i)
            tdSql.checkData(0, 0, 0)

        tdLog.info("do_taosbackup_prefilter_empty_ctb ........... [passed]")

    #
    # ------------------- do_taosbackup_prefilter_empty_ctb_time_filter ----------------
    #
    def do_taosbackup_prefilter_empty_ctb_time_filter(self):
        """Pre-filter CTBs time filter.

        Since: v3.0.0.0

        Labels: common

        Jira: None

        History:

            - 2026-03-04 Alex Duan Created

        """
        tmpdir = "./taosbackuptest/tmpdir_prefilter_tf"

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db keep 3649")
        tdSql.execute("use db")
        tdSql.execute("create table st(ts timestamp, c1 int) tags(t1 int)")

        # t1: 5 rows entirely inside range (2022)
        tdSql.execute("create table t1 using st tags(1)")
        for j in range(5):
            tdSql.execute("insert into t1 values(%d, %d)" % (1641000000000 + j, j))

        # t2: 3 rows entirely outside range (2021)
        tdSql.execute("create table t2 using st tags(2)")
        for j in range(3):
            tdSql.execute("insert into t2 values(%d, %d)" % (1609459200000 + j, j))

        # t3: 4 rows outside range (2021) + 6 rows inside range (2022)
        tdSql.execute("create table t3 using st tags(3)")
        for j in range(4):
            tdSql.execute(
                "insert into t3 values(%d, %d)" % (1609459200000 + j, j)
            )
        for j in range(6):
            tdSql.execute(
                "insert into t3 values(%d, %d)" % (1641000000000 + j, j + 10)
            )

        # t4: empty
        tdSql.execute("create table t4 using st tags(4)")

        binPath = etool.taosBackupFile()
        if binPath == "":
            tdLog.exit("taosBackup not found!")

        self.makeDir(tmpdir)
        self.exec(
            "%s -D db -o %s -T 2 -S '2022-01-01 00:00:00.000' -E '2022-12-31 23:59:59.999'"
            % (binPath, tmpdir)
        )

        # t1: has in-range data → must have .dat
        assert os.path.exists("%s/db/st_data0/t1.dat" % tmpdir), \
            "t1.dat must exist (has in-range data)"
        # t2: no in-range data → must NOT have .dat
        assert not os.path.exists("%s/db/st_data0/t2.dat" % tmpdir), \
            "t2.dat must NOT exist (all data outside range)"
        # t3: has in-range data → must have .dat
        assert os.path.exists("%s/db/st_data0/t3.dat" % tmpdir), \
            "t3.dat must exist (has in-range data)"
        # t4: empty → must NOT have .dat
        assert not os.path.exists("%s/db/st_data0/t4.dat" % tmpdir), \
            "t4.dat must NOT exist (empty)"

        tdSql.execute("drop database db")
        self.exec("%s -i %s -T 2" % (binPath, tmpdir))

        assert self.dbFound("db"), "database 'db' not found after restore"
        tdSql.execute("use db")
        # All 4 CTB schemas restored
        tdSql.query("show tables")
        tdSql.checkRows(4)
        # t1: all 5 in-range rows
        tdSql.query("select count(*) from db.t1")
        tdSql.checkData(0, 0, 5)
        # t2: 0 rows (no in-range data backed up)
        tdSql.query("select count(*) from db.t2")
        tdSql.checkData(0, 0, 0)
        # t3: only the 6 in-range rows (out-of-range rows not backed up)
        tdSql.query("select count(*) from db.t3")
        tdSql.checkData(0, 0, 6)
        # t4: 0 rows
        tdSql.query("select count(*) from db.t4")
        tdSql.checkData(0, 0, 0)

        tdLog.info("do_taosbackup_prefilter_empty_ctb_time_filter [passed]")

    #
    # ------------------- do_taosbackup_prefilter_spec_tables_time_filter ----------------
    #
    def do_taosbackup_prefilter_spec_tables_time_filter(self):
        """Pre-filter spec-tables time filter.

        Since: v3.0.0.0

        Labels: common

        Jira: None

        History:
            - 2026-03-04 Alex Duan Created

        """
        tmpdir = "./taosbackuptest/tmpdir_prefilter_spec_tf"

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db keep 3649")
        tdSql.execute("use db")
        tdSql.execute("create table st(ts timestamp, c1 int) tags(t1 int)")

        # t1 (spec): 8 rows inside range (2022)
        tdSql.execute("create table t1 using st tags(1)")
        for j in range(8):
            tdSql.execute("insert into t1 values(%d, %d)" % (1641000000000 + j, j))

        # t2 (spec): 5 rows entirely outside range (2021) — no .dat expected
        tdSql.execute("create table t2 using st tags(2)")
        for j in range(5):
            tdSql.execute("insert into t2 values(%d, %d)" % (1609459200000 + j, j))

        # t3 (NOT in spec): 5 in-range rows — must not appear at all
        tdSql.execute("create table t3 using st tags(3)")
        for j in range(5):
            tdSql.execute(
                "insert into t3 values(%d, %d)" % (1641000000000 + j, j + 100)
            )

        binPath = etool.taosBackupFile()
        if binPath == "":
            tdLog.exit("taosBackup not found!")

        self.makeDir(tmpdir)
        # Specify t1 and t2 explicitly (spec-tables path), plus time filter
        self.exec(
            "%s db t1 t2 -o %s -T 1 -S '2022-01-01 00:00:00.000' -E '2022-12-31 23:59:59.999'"
            % (binPath, tmpdir)
        )

        # t1: spec + in-range → .dat must exist
        assert os.path.exists("%s/db/st_data0/t1.dat" % tmpdir), \
            "t1.dat must exist (spec, has in-range data)"
        # t2: spec but no in-range data → .dat must NOT exist
        assert not os.path.exists("%s/db/st_data0/t2.dat" % tmpdir), \
            "t2.dat must NOT exist (spec, all data outside range)"
        # t3: not in spec → .dat must NOT exist
        assert not os.path.exists("%s/db/st_data0/t3.dat" % tmpdir), \
            "t3.dat must NOT exist (not in spec)"

        tdSql.execute("drop database db")
        self.exec('%s -i %s -T 1 -W "db=newdb"' % (binPath, tmpdir))

        assert self.dbFound("newdb"), "newdb not found after restore"
        tdSql.execute("use newdb")
        # Only t1 and t2 schemas restored (spec-tables)
        tdSql.query("show tables")
        tdSql.checkRows(2)
        # t1: all 8 in-range rows correctly restored
        tdSql.query("select count(*) from newdb.t1")
        tdSql.checkData(0, 0, 8)
        # t2: 0 rows (time filter excluded all its data)
        tdSql.query("select count(*) from newdb.t2")
        tdSql.checkData(0, 0, 0)

        tdSql.execute("drop database newdb")
        tdLog.info("do_taosbackup_prefilter_spec_tables_time_filter [passed]")

    #
    # ------------------- main test method ----------------
    #
    def test_taosbackup_basic(self):
        """taosBackup basic functionality

        1. Backup/restore database with super table, child tables and normal tables
        2. Test -W rename database option during restore
        3. Test -s/--schemaonly: backup schemas only, verify no data restored
        4. Test backup/restore of a specific normal child table (db tb -o)
        5. Test backup/restore of a specific super table (db st -o)
        6. Test -F parquet format: backup and restore using Parquet format
        7. Test backup/restore with large number of records and wide columns
        8. Test backup/restore with maximum number of columns (300 cols, 128 tags)
        9. Test -m tag-thread-num for multi-threaded tag backup
        10. Test -v stmt-version option for restore (v1 legacy and v2 default)
        11. Test backup/restore with VARBINARY, GEOMETRY, DECIMAL, BLOB columns
        12. Test skip-empty-ctb: empty child tables produce no .dat file
        13. Test skip-empty-ntb: empty normal tables produce no .dat file
        14. Test skip-empty-time-range: CTBs with no in-range rows get no .dat file
        15. Test backup_complete.flag lifecycle (created / deleted on fresh run)
        16. Pre-filter empty CTBs via last(ts): mixed CTBs, no time filter; all non-empty rows intact
        17. Pre-filter empty CTBs via last_row(ts): time filter; in-range rows correct, out-of-range skipped
        18. Pre-filter empty CTBs with spec-tables + time filter; -S/-E applies to named CTBs

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-04 Alex Duan Created

        """
        self.do_taosbackup_basic()
        self.do_taosbackup_rename_db()
        self.do_taosbackup_schemaonly()
        self.do_taosbackup_db_ntb()
        self.do_taosbackup_db_stb()
        self.do_taosbackup_format_parquet()
        self.do_taosbackup_test_large_data()
        self.do_taosbackup_many_cols()
        self.do_taosbackup_tag_threads()
        self.do_taosbackup_stmt_version()
        self.do_taosbackup_special_types()
        self.do_taosbackup_skip_empty_ctb()
        self.do_taosbackup_skip_empty_ntb()
        self.do_taosbackup_skip_empty_time_range()
        self.do_taosbackup_complete_flag()
        self.do_taosbackup_prefilter_empty_ctb()
        self.do_taosbackup_prefilter_empty_ctb_time_filter()
        self.do_taosbackup_prefilter_spec_tables_time_filter()
