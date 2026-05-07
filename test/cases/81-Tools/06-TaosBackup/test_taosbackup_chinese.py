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
Tests for taosBackup with Chinese and special-character identifiers.

Coverage:
  1. Chinese DB / STB / CTB / NTB names + Chinese column / tag names
     + Chinese NCHAR tag values + Chinese NCHAR data content
  2. Mixed Chinese + ASCII identifiers: hyphens, underscores, digits
  3. ASCII identifiers that are problematic as OS path components:
       - hyphens (valid everywhere)
       - spaces (valid on Linux/macOS, valid on Windows NTFS)
       - '@' '#' '(' ')' '-' (valid in Windows filenames)
     Characters that are INVALID in Windows filenames (\\  :  *  ?  "  <  >  |)
     are REJECTED by TDengine itself (e.g. '.' → error 0x80002617), so no
     separate Windows-only guard is needed for those.
  4. Same Chinese DB backed up / restored with Parquet format (-F parquet).

Windows notes
─────────────
taosBackup uses the database name as a sub-directory and table names as
file-name stems inside that directory.  Characters that are illegal in
Windows filenames would therefore break the backup on that platform even
if TDengine accepted them.

Characters ILLEGAL in Windows filenames:  \\ / : * ? " < > |
• dot (.)   → rejected by TDengine via error 0x80002617 (contains '.')
• slash (/) → accepted by TDengine but would silently create a nested
              directory on Linux and fail with "invalid path" on Windows
              → taosBackup MUST encode such names or reject them clearly
• space ( ) → legal on both Linux and Windows NTFS; test included
• hyphen    → legal everywhere; test included
• @  #  (  ) → legal on both platforms; test included

Any path-encoding logic (or explicit test for that slash edge-case) should
be added to test_taosbackup_edge.py once the feature is implemented.
"""

import os
import shutil
import sys
import subprocess

from new_test_framework.utils import tdLog, tdSql, etool


class TestTaosBackupChinese:

    # ──────────────────────────── helpers ────────────────────────────

    def exec(self, command):
        """Run a shell command, stream output, and fail on non-zero exit code."""
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

    def makeDir(self, path):
        if os.path.exists(path):
            shutil.rmtree(path)
        os.makedirs(path)

    def dbFound(self, dbname):
        tdSql.query("select name from information_schema.ins_databases")
        return any(row[0] == dbname for row in tdSql.queryResult)

    def binPath(self):
        p = etool.taosBackupFile()
        if not p:
            tdLog.exit("taosBackup binary not found")
        return p

    # ─────────────────────── Test 1: Chinese identifiers ─────────────────────

    def do_chinese_stb(self):
        """
        Full Chinese identifier chain:
          DB name     : 设备监控
          STB name    : 传感器数据
          Column names: 温度  湿度  状态描述   (FLOAT / DOUBLE / NCHAR)
          Tag names   : 区域  设备编号  安装位置  (NCHAR / INT / NCHAR)
          CTB names   : 北京机房_001  上海中心_02  深圳03号
          Tag values  : Chinese NCHAR strings
          Data values : Chinese NCHAR strings
          NTB name    : 普通日志
          NTB columns : 描述  级别  (NCHAR / INT)
        """
        tmpdir = "./taosbackuptest/tmpdir_chinese_stb"
        db = "设备监控"

        tdSql.execute(f"drop database if exists `{db}`")
        tdSql.execute(f"create database `{db}` keep 3649")

        # STB with Chinese column + tag names
        tdSql.execute(
            f"create table `{db}`.`传感器数据`("
            f"ts timestamp, `温度` float, `湿度` double, `状态描述` nchar(50)"
            f") tags(`区域` nchar(10), `设备编号` int, `安装位置` nchar(20))"
        )

        # CTB 1 — 华北
        tdSql.execute(
            f"create table `{db}`.`北京机房_001` using `{db}`.`传感器数据`"
            f" tags('华北区', 1, '北京数据中心')"
        )
        tdSql.execute(
            f"insert into `{db}`.`北京机房_001` values"
            f"(1640000000000, 25.6, 60.5, '运行正常')"
            f"(1640000001000, 26.1, 58.2, '温度偏高')"
            f"(1640000002000, 24.8, 61.0, '故障告警')"
        )

        # CTB 2 — 华东
        tdSql.execute(
            f"create table `{db}`.`上海中心_02` using `{db}`.`传感器数据`"
            f" tags('华东区', 2, '上海金融中心')"
        )
        tdSql.execute(
            f"insert into `{db}`.`上海中心_02` values"
            f"(1640000000000, 22.3, 55.0, '正常运行')"
        )

        # CTB 3 — name starts with Chinese digit character + trailing digit
        tdSql.execute(
            f"create table `{db}`.`深圳03号` using `{db}`.`传感器数据`"
            f" tags('华南区', 3, '深圳科技园')"
        )
        tdSql.execute(
            f"insert into `{db}`.`深圳03号` values"
            f"(1640000000000, 28.9, 70.0, '维护中')"
        )

        # NTB with Chinese name and Chinese column names
        tdSql.execute(
            f"create table `{db}`.`普通日志`("
            f"ts timestamp, `描述` nchar(100), `级别` int)"
        )
        tdSql.execute(
            f"insert into `{db}`.`普通日志` values"
            f"(1640000000000, '系统启动完成', 1)"
            f"(1640000001000, '检测到异常温度，请检查设备', 3)"
            f"(1640000002000, '备份任务已完成', 2)"
        )

        bin_ = self.binPath()
        self.makeDir(tmpdir)

        # ── backup ──
        self.exec(f"{bin_} -D \"{db}\" -o {tmpdir} -T 1")

        # ── drop and restore ──
        tdSql.execute(f"drop database `{db}`")
        assert not self.dbFound(db), f"db `{db}` should be gone before restore"

        self.exec(f"{bin_} -i {tmpdir} -T 1")

        assert self.dbFound(db), f"db `{db}` not found after restore"

        # ── verify STB structure ──
        tdSql.query(f"show `{db}`.stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "传感器数据")

        # ── verify CTB count ──
        tdSql.query(f"show `{db}`.tables")
        tdSql.checkRows(4)  # 3 CTBs + 1 NTB

        # ── verify tag values (Chinese tags correctly restored) ──
        tdSql.query(
            f"select distinct `区域`, `安装位置` from `{db}`.`传感器数据`"
            f" where tbname = '北京机房_001'"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "华北区")
        tdSql.checkData(0, 1, "北京数据中心")

        tdSql.query(
            f"select distinct `区域`, `设备编号` from `{db}`.`传感器数据`"
            f" where tbname = '上海中心_02'"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "华东区")
        tdSql.checkData(0, 1, 2)

        # ── verify Chinese data content ──
        tdSql.query(
            f"select `温度`, `状态描述` from `{db}`.`北京机房_001` order by ts"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 25.6)
        tdSql.checkData(0, 1, "运行正常")
        tdSql.checkData(1, 1, "温度偏高")
        tdSql.checkData(2, 1, "故障告警")

        tdSql.query(
            f"select `温度`, `状态描述` from `{db}`.`深圳03号`"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "维护中")

        # ── verify NTB data ──
        tdSql.query(
            f"select `描述`, `级别` from `{db}`.`普通日志` order by ts"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "系统启动完成")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, "检测到异常温度，请检查设备")
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(2, 0, "备份任务已完成")
        tdSql.checkData(2, 1, 2)

        # ── total row count ──
        tdSql.query(f"select count(*) from `{db}`.`传感器数据`")
        tdSql.checkData(0, 0, 5)  # 3 + 1 + 1

        tdLog.info("do_chinese_stb ................................ [passed]")

    # ──────────────── Test 2: Chinese + Parquet format ────────────────────────

    def do_chinese_parquet(self):
        """
        Same Chinese DB, backed up then restored with Parquet format (-F parquet).
        Verifies that UTF-8 column/tag names and NCHAR data survive
        the Arrow/Parquet round-trip.
        """
        tmpdir = "./taosbackuptest/tmpdir_chinese_parquet"
        db = "设备监控"

        # Re-use data created by do_chinese_stb (called before this method).
        bin_ = self.binPath()
        self.makeDir(tmpdir)

        # backup with parquet format
        self.exec(f"{bin_} -F parquet -D \"{db}\" -o {tmpdir} -T 1")

        tdSql.execute(f"drop database `{db}`")
        assert not self.dbFound(db)

        self.exec(f"{bin_} -i {tmpdir} -T 1")

        assert self.dbFound(db), f"db `{db}` not found after parquet restore"

        tdSql.query(f"select count(*) from `{db}`.`传感器数据`")
        tdSql.checkData(0, 0, 5)

        tdSql.query(
            f"select `状态描述` from `{db}`.`北京机房_001` order by ts"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "运行正常")
        tdSql.checkData(2, 0, "故障告警")

        tdSql.query(
            f"select `描述` from `{db}`.`普通日志` order by ts limit 1"
        )
        tdSql.checkData(0, 0, "系统启动完成")

        # Clean up — next sub-test creates its own databases from scratch.
        tdSql.execute(f"drop database if exists `{db}`")

        tdLog.info("do_chinese_parquet ............................ [passed]")

    # ─────────── Test 3: Mixed Chinese + ASCII with hyphens ──────────────────

    def do_chinese_mixed_ascii(self):
        """
        Identifiers that mix Chinese and ASCII characters, including hyphens
        and underscores — all legal on Linux and Windows filesystems.

        DB    : db-中文-monitor
        STB   : stb_传感器-v2
        Columns: col-温度  col-湿度
        Tags  : tag-区域  tag-id
        CTB   : device-北京-001  device-上海-02
        NTB   : ntb-日志_v1
        """
        tmpdir = "./taosbackuptest/tmpdir_mixed_ascii"
        db = "db-中文-monitor"

        tdSql.execute(f"drop database if exists `{db}`")
        tdSql.execute(f"create database `{db}` keep 3649")

        tdSql.execute(
            f"create table `{db}`.`stb_传感器-v2`("
            f"ts timestamp, `col-温度` float, `col-湿度` double"
            f") tags(`tag-区域` nchar(10), `tag-id` int)"
        )

        tdSql.execute(
            f"create table `{db}`.`device-北京-001`"
            f" using `{db}`.`stb_传感器-v2` tags('华北', 1)"
        )
        tdSql.execute(
            f"insert into `{db}`.`device-北京-001` values"
            f"(1640000000000, 22.5, 55.1)"
            f"(1640000001000, 23.0, 54.8)"
        )

        tdSql.execute(
            f"create table `{db}`.`device-上海-02`"
            f" using `{db}`.`stb_传感器-v2` tags('华东', 2)"
        )
        tdSql.execute(
            f"insert into `{db}`.`device-上海-02` values"
            f"(1640000000000, 20.1, 60.0)"
        )

        # NTB with hyphen + Chinese
        tdSql.execute(
            f"create table `{db}`.`ntb-日志_v1`("
            f"ts timestamp, `msg-内容` nchar(60), level int)"
        )
        tdSql.execute(
            f"insert into `{db}`.`ntb-日志_v1` values"
            f"(1640000000000, '中英文混合 mixed content 123', 0)"
            f"(1640000001000, '特殊符号测试 @ # ( )', 1)"
        )

        bin_ = self.binPath()
        self.makeDir(tmpdir)

        self.exec(f"{bin_} -D \"{db}\" -o {tmpdir} -T 1")

        tdSql.execute(f"drop database `{db}`")
        assert not self.dbFound(db)

        self.exec(f"{bin_} -i {tmpdir} -T 1")

        assert self.dbFound(db), f"db `{db}` not found after restore"

        # Verify
        tdSql.query(f"select count(*) from `{db}`.`stb_传感器-v2`")
        tdSql.checkData(0, 0, 3)

        tdSql.query(
            f"select `col-温度`, `tag-区域` from `{db}`.`stb_传感器-v2`"
            f" where tbname = 'device-北京-001' order by ts"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 22.5)
        tdSql.checkData(0, 1, "华北")

        tdSql.query(
            f"select `msg-内容`, level from `{db}`.`ntb-日志_v1` order by ts"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "中英文混合 mixed content 123")
        tdSql.checkData(1, 0, "特殊符号测试 @ # ( )")

        # Clean up — next sub-test creates its own databases from scratch.
        tdSql.execute(f"drop database if exists `{db}`")

        tdLog.info("do_chinese_mixed_ascii ........................ [passed]")

    # ──── Test 4: Windows-problematic ASCII special chars in identifiers ──────

    def do_special_chars_path(self):
        """
        ASCII characters that are legal in TDengine identifiers (with backtick
        quoting) but could cause problems when used as directory / file names
        on Windows or on Linux.

        Characters tested:
          space  — valid on Linux + Windows NTFS; many shell scripts break
          @      — valid on both platforms
          #      — valid on both platforms
          ( )    — valid on both platforms
          -      — already covered in do_chinese_mixed_ascii

        Characters NOT tested here (they are REJECTED by TDengine itself):
          .   → TDengine error 0x80002617 "name cannot contain '.'"
          /   → accepted by TDengine but taosBackup would create a nested
                directory on Linux (db/slash → sub-path) and fail outright
                on Windows.  Filed separately as a known limitation.
          \\ : * ? " < > |  → not accepted by TDengine's SQL parser.

        On Windows, this test verifies (via sys.platform check) that the
        backup directory created for a DB whose name contains a space or
        @ sign is correctly named — Windows NTFS supports these characters
        in directory names.
        """
        tmpdir = "./taosbackuptest/tmpdir_special_chars"

        # ── Sub-case A: @ in db, table and column names ──
        db_at = "db@monitor"
        tdSql.execute(f"drop database if exists `{db_at}`")
        tdSql.execute(f"create database `{db_at}` keep 3649")
        tdSql.execute(
            f"create table `{db_at}`.`tbl@sensor`("
            f"ts timestamp, `val@1` int, `tag@cn` nchar(10)"
            f")"
        )
        tdSql.execute(
            f"insert into `{db_at}`.`tbl@sensor` values"
            f"(1640000000000, 100, '中文@值')"
            f"(1640000001000, 200, '正常@状态')"
        )

        # ── Sub-case B: # ( ) in names ──
        db_sym = "db#test"
        tdSql.execute(f"drop database if exists `{db_sym}`")
        tdSql.execute(f"create database `{db_sym}` keep 3649")
        tdSql.execute(
            f"create table `{db_sym}`.`tbl(v1)_传感器`("
            f"ts timestamp, `c#1` int, `描述` nchar(20)"
            f") tags(`t#region` nchar(8))"
        )
        tdSql.execute(
            f"create table `{db_sym}`.`ctb(北京)#01`"
            f" using `{db_sym}`.`tbl(v1)_传感器` tags('华北')"
        )
        tdSql.execute(
            f"insert into `{db_sym}`.`ctb(北京)#01` values"
            f"(1640000000000, 42, '括号与井号测试')"
        )

        # ── Sub-case C: space in db name and table name ──
        # (Valid on both Linux ext4/xfs and Windows NTFS)
        db_sp = "db monitor"
        tdSql.execute(f"drop database if exists `{db_sp}`")
        tdSql.execute(f"create database `{db_sp}` keep 3649")
        tdSql.execute(
            f"create table `{db_sp}`.`table name`("
            f"ts timestamp, `col name` nchar(20), `区域 代码` int"
            f")"
        )
        tdSql.execute(
            f"insert into `{db_sp}`.`table name` values"
            f"(1640000000000, '空格名称测试', 1)"
            f"(1640000001000, 'space name test', 2)"
        )

        bin_ = self.binPath()
        self.makeDir(tmpdir)

        # ── Backup all three databases ──
        for db in (db_at, db_sym, db_sp):
            self.exec(f"{bin_} -D \"{db}\" -o {tmpdir} -T 1")

        # On Windows, assert the directory names are created correctly.
        # Chars @  #  (  )  space are all legal Windows path characters.
        if sys.platform.startswith("win"):
            for db in (db_at, db_sym, db_sp):
                assert os.path.isdir(os.path.join(tmpdir, db)), (
                    f"Backup directory for `{db}` not created on Windows"
                )

        # ── Drop and restore ──
        for db in (db_at, db_sym, db_sp):
            tdSql.execute(f"drop database `{db}`")
        for db in (db_at, db_sym, db_sp):
            assert not self.dbFound(db), f"`{db}` should be gone before restore"

        self.exec(f"{bin_} -i {tmpdir} -T 1")

        for db in (db_at, db_sym, db_sp):
            assert self.dbFound(db), f"db `{db}` not found after restore"

        # ── Verify Sub-case A ──
        tdSql.query(f"select `val@1`, `tag@cn` from `{db_at}`.`tbl@sensor` order by ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 100)
        tdSql.checkData(0, 1, "中文@值")
        tdSql.checkData(1, 1, "正常@状态")

        # ── Verify Sub-case B ──
        tdSql.query(
            f"select `c#1`, `描述` from `{db_sym}`.`tbl(v1)_传感器`"
            f" where tbname = 'ctb(北京)#01'"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 42)
        tdSql.checkData(0, 1, "括号与井号测试")

        tdSql.query(
            f"select `t#region` from `{db_sym}`.`tbl(v1)_传感器`"
            f" where tbname = 'ctb(北京)#01'"
        )
        tdSql.checkData(0, 0, "华北")

        # ── Verify Sub-case C ──
        tdSql.query(
            f"select `col name`, `区域 代码` from `{db_sp}`.`table name` order by ts"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "空格名称测试")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, "space name test")

        tdLog.info("do_special_chars_path ......................... [passed]")

    # ──────────── Test 5: TDengine identifier rejection / acceptance ─────────

    def do_rejected_identifiers(self):
        """
        Verify TDengine's actual acceptance/rejection rules for special chars.

        ACTUALLY REJECTED by TDengine (with backtick quoting):
          • dot (.)  → error 0x80002617 "name cannot contain '.'"

        ACCEPTED by TDengine (despite being Windows-illegal as filenames):
          • colon (:)   — accepted; taosBackup creates dirs like "db:name/" which
                          would fail on Windows NTFS.  Windows handling is a known
                          limitation: such databases should not be created on Windows.
          • asterisk (*), question mark (?), pipe (|), angle brackets (< >) —
                          also accepted by TDengine but Windows-illegal as filenames.

        This test only asserts the one character TDengine actually rejects (dot),
        so the assertion is reliable across all TDengine versions.
        """
        def expect_error(sql, description):
            try:
                tdSql.execute(sql)
                tdLog.exit(
                    f"Expected TDengine to reject `{description}` but it succeeded"
                )
            except Exception:
                pass  # expected

        # dot is the only character rejected by TDengine's SQL parser
        expect_error(
            "create database `db.dot` keep 3649",
            "db name with dot"
        )

        tdLog.info("do_rejected_identifiers ...................... [passed]")

    # ──────────────────────────── entry point ────────────────────────────────

    def test_taosbackup_chinese(self):
        """taosBackup: Chinese and special-character identifier support

        1. Chinese DB / STB / CTB / NTB names with Chinese column and tag
           names; Chinese NCHAR tag values and data values.
        2. Same Chinese database backed up and restored in Parquet format.
        3. Mixed Chinese + ASCII identifiers (hyphens, underscores in names).
        4. ASCII special characters valid on both Linux and Windows file
           systems: '@', '#', '()', space.
        5. Verify TDengine rejects '.' and other Windows-illegal chars so
           taosBackup is never exposed to them.

        Windows notes: taosBackup uses DB/table names as directory/file names.
        Characters illegal in Windows paths (\\  :  *  ?  "  <  >) are
        rejected by TDengine's parser, so backup is protected on that side.
        Characters that are legal on Windows (space, '@', '#', '()', '-')
        are explicitly tested end-to-end.

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-12 Created for Chinese / Unicode / special-char coverage

        """
        self.do_chinese_stb()
        self.do_chinese_parquet()
        self.do_chinese_mixed_ascii()
        self.do_special_chars_path()
        self.do_rejected_identifiers()

        # cleanup
        for db in (
            "设备监控",
            "db-中文-monitor",
            "db@monitor",
            "db#test",
            "db monitor",
        ):
            tdSql.execute(f"drop database if exists `{db}`")
        os.system("rm -rf ./taosbackuptest/")

        tdLog.info("test_taosbackup_chinese ...................... [passed]")
