# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
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

# ─────────────────────────────────────────────────────────────────────────────
# Cross-platform fixture: a backup produced by a FIXED Windows taosBackup and
# committed under data/.  The restore here runs on whatever platform the CI
# worker is on (typically Linux); it must succeed and yield byte-identical
# Chinese / special-char identifiers and NCHAR content.  This guards the two
# historical Windows bugs:
#   1. Windows LZ4-only compression: the engine maps zlib/zstd/xz slots to LZ4,
#      so the .dat columns must be labelled LZ4 or Linux fails to decompress.
#   2. bckToUtf8 double-encoding: DDL / identifiers used to be re-encoded from
#      UTF-8 as if they were ANSI(GBK), corrupting every Chinese name.
# The fixture dir layout is exactly what `taosdump -i <dir>` expects (one
# sub-directory per database).
_HERE = os.path.dirname(os.path.abspath(__file__))
_WIN_BCK_DIR = os.path.join(_HERE, "data", "windows_backup_cn")

# Expected content of the Windows-produced fixture (verified once on Linux):
_WIN_BCK_DBS = ["nchar_big", "db monitor", "db@monitor", "db#test"]
#   nchar_big: 1 NTB `t1`(ts, remark NCHAR(50)), 5000 Chinese NCHAR rows.
#     Row ts=1700000001000 -> remark '数据管道在边缘节点完成预处理并上报云端时序数据库'
_NCHAR_BIG_ROWS = 5000
_NCHAR_BIG_SAMPLE_TS = 1700000001000
_NCHAR_BIG_SAMPLE_REMARK = "数据管道在边缘节点完成预处理并上报云端时序数据库"
#   db monitor: NTB `table name`(ts, `col name` NCHAR(20), `区域 代码` INT), 2 rows.
_DB_MONITOR_ROWS = 2
#   db@monitor: NTB `tbl@sensor`(ts, `val@1` INT, `tag@cn` NCHAR(10)), 2 rows.
_DB_AT_ROWS = 2
_DB_AT_ROW0_VAL = 100
_DB_AT_ROW0_TAG = "cn-val"
#   db#test: STB `tbl(v1)_传感器`(ts, `c#1` INT, `描述` NCHAR(20)) TAGS(`t#region` NCHAR(8))
_DB_HASH_STB = "tbl(v1)_传感器"


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

    def execArgs(self, argv):
        """Run a command as an argv list WITHOUT a shell.

        Windows cmd.exe cannot transport identifiers that contain cmd
        metacharacters (`< > | & % "`).  POSIX shlex.quote() single-quotes
        are not understood by cmd.exe, so `-D '<db>'` is re-parsed as
        redirection/pipes and the command never reaches taosdump
        (`> was unexpected at this time.`).  Passing the arguments as a
        list makes Python build the command line itself (CreateProcess on
        Windows, execvp on POSIX), so such names arrive verbatim in the
        child's argv on every platform.
        """
        tdLog.info(" ".join(argv))
        result = subprocess.run(
            argv, shell=False, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
        if result.stdout:
            for line in result.stdout.splitlines():
                tdLog.info(line)
        if result.returncode != 0:
            tdLog.exit(f"Command failed (rc={result.returncode}): {' '.join(argv)}")
        return result.returncode

    def makeDir(self, path):
        if os.path.exists(path):
            shutil.rmtree(path)
        os.makedirs(path)

    def _encNameLen(self, name):
        """Length of bckNameEncode(name) as taosdump writes it on disk.

        Mirrors bckNameCharUnsafe() in bckName.c: only these ASCII chars are
        percent-encoded to '%XX' (3 bytes each) - '<' '>' ':' '"' '/' '\\' '|'
        '?' '*' '%' and control chars (0x00-0x1F).  Everything else (letters,
        digits, space, '.', '-', '_', '@', '#', '(', ')', multibyte UTF-8) is
        passed through unchanged.
        """
        unsafe = set('<>:"/\\|?*%')
        n = 0
        for ch in name:
            o = ord(ch)
            if o < 0x20 or ch in unsafe:
                n += 3
            else:
                n += 1
        return n

    def _boundaryNames(self, tmpdir):
        """Pick max-length DB / table names that fit the OS path ceiling.

        taosdump writes data files as:
            {abspath(tmpdir)}/<encoded-DB>/_ntb_data0/<encoded-TB>.dat.tmp
        Windows caps the full path at MAX_PATH=260 (taosdump has no
        long-path manifest), whereas Linux/macOS allow up to PATH_MAX.
        Sizing the table name from the *real absolute* tmpdir keeps this
        test green on every platform: the DB is kept at its 63-char max and
        the table name is shortened only as much as the platform needs.

        Returns (db, tb) where tb contains one '\\' so encoding is exercised.
        """
        db = "L" * 55 + ":" + "D" * 7        # 63 chars: engine max is 63
        enc_db = self._encNameLen(db)        # ':' -> %3A
        # Fixed path scaffolding around the table file:
        #   <abs>/<db>/_ntb_data0/<tb>.dat.tmp
        prefix = (len(os.path.abspath(tmpdir)) + 1 + enc_db + 1
                  + len("_ntb_data0") + 1 + len(".dat.tmp"))
        if sys.platform.startswith("win"):
            ceiling = 260
        else:
            ceiling = 4096
        # How many encoded bytes the table-name segment may occupy.
        enc_tb_room = ceiling - prefix
        # Keep a safety margin (drive letter, trailing NUL, any separator).
        enc_tb_room -= 8
        if enc_tb_room < 16:
            tdLog.exit(f"too little path room for boundary table name: {enc_tb_room}")
        # tb = T*.. + '\\' + U*.. ; encoding makes '\\' 1 byte -> 3 bytes.
        # enc_len = (len(tb)-1) + 3.  We target enc_len == enc_tb_room (max).
        target_enc = min(enc_tb_room, 193)   # engine max encodable buffer
        tbl_len = target_enc - 2             # backslash adds 2 encoded bytes
        if tbl_len > 192:
            tbl_len = 192                    # engine max legal table name
        if tbl_len < 8:
            tdLog.exit(f"too little room for boundary table name: {tbl_len}")
        # Split so '\\' sits near the middle: T.. \ U..
        half = (tbl_len - 1) // 2
        tb = "T" * half + "\\" + "U" * (tbl_len - 1 - half)
        tdLog.info(f"boundary names: db={len(db)} tb={len(tb)} "
                   f"(encoded {self._encNameLen(tb)}) path-room={enc_tb_room}")
        return db, tb

    def dbFound(self, dbname):
        tdSql.query("select name from information_schema.ins_databases")
        return any(row[0] == dbname for row in tdSql.queryResult)

    def binPath(self):
        p = etool.taosDumpFile()
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
        # dot is the only character rejected by TDengine's SQL parser
        tdSql.error(
            "create database `db.dot` keep 3649",
            expectedErrno=0x80002617,
        )
        tdLog.info("do_rejected_identifiers ...................... [passed]")

    # ──── Test 6: all Windows-illegal filter chars in identifiers ──────

    def do_special_chars_illegal(self):
        """
        Every ASCII character that is illegal in Windows file names, but
        accepted by TDengine when backtick-quoted, is embedded in the
        DB / STB / CTB / NTB names.  taosBackup percent-encodes these name
        components when building file/directory names, so a backup must
        round-trip with no data loss on every platform.

        Filtered characters (all 10):  < > : " / \\ | ? * %
        """
        # all 10 chars TDengine accepts (backtick-quoted) but Windows forbids
        sc = "<>" + ":" + '"' + "/" + "\\" + "|" + "?" + "*" + "%"

        db  = "sp" + sc + "db"
        stb = "st" + sc + "tb"
        ct1 = "ct" + sc + "1"
        ct2 = "ct" + sc + "2"
        nt  = "nt" + sc + "b"
        tmpdir = "./taosbackuptest/tmpdir_special_illegal"

        tdSql.execute(f"drop database if exists `{db}`")
        tdSql.execute(f"create database `{db}` keep 3649")

        tdSql.execute(
            f"create table `{db}`.`{stb}`("
            f"ts timestamp, v int, n nchar(64)) tags(region nchar(20))"
        )
        tdSql.execute(f"create table `{db}`.`{ct1}` using `{db}`.`{stb}` tags('华北')")
        tdSql.execute(
            f"insert into `{db}`.`{ct1}` values"
            f"(1640000000000, 1, 'row-1 特殊')"
            f"(1640000001000, 2, 'row-2')"
        )
        tdSql.execute(f"create table `{db}`.`{ct2}` using `{db}`.`{stb}` tags('华东')")
        tdSql.execute(
            f"insert into `{db}`.`{ct2}` values(1640000000000, 10, 'row-10')"
        )
        tdSql.execute(f"create table `{db}`.`{nt}`(ts timestamp, v int)")
        tdSql.execute(
            f"insert into `{db}`.`{nt}` values"
            f"(1640000000000, 100)(1640000001000, 200)"
        )

        bin_ = self.binPath()
        self.makeDir(tmpdir)

        # The db name contains cmd.exe metacharacters (`< > : " / \ | ? * %`),
        # so it must be passed as an argv list (no shell): shlex.quote() emits
        # POSIX single quotes that Windows cmd.exe does not understand.
        self.execArgs([bin_, "-D", db, "-o", tmpdir, "-T", "1"])

        tdSql.execute(f"drop database `{db}`")
        assert not self.dbFound(db), f"db `{db}` should be gone before restore"

        self.execArgs([bin_, "-i", tmpdir, "-T", "1"])

        assert self.dbFound(db), f"db `{db}` not found after restore"

        # STB: 2 CTBs x (2 + 1) rows = 3
        tdSql.query(f"select count(*) from `{db}`.`{stb}`")
        tdSql.checkData(0, 0, 3)

        # CTB1 name round-tripped (db.ct1 resolves) and data intact
        tdSql.query(f"select v, n from `{db}`.`{ct1}` order by ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, "row-1 特殊")
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(1, 1, "row-2")

        # tags restored for both CTBs
        tdSql.query(f"select distinct region from `{db}`.`{stb}`")
        tdSql.checkRows(2)

        # NTB data intact
        tdSql.query(f"select count(*) from `{db}`.`{nt}`")
        tdSql.checkData(0, 0, 2)
        tdSql.query(f"select v from `{db}`.`{nt}` order by ts")
        tdSql.checkData(0, 0, 100)
        tdSql.checkData(1, 0, 200)

        tdSql.execute(f"drop database if exists `{db}`")
        tdLog.info("do_special_chars_illegal ..................... [passed]")

    # ──────────── Test 7: boundary and encoding edge identifiers ────────────

    def do_boundary(self):
        """Max-length DB / table names and literal '%HH' round-trip."""
        tmpdir = "./taosbackuptest/tmpdir_boundary"

        # max-length names sized from the real absolute path (see _boundaryNames)
        long_db, long_tbl = self._boundaryNames(tmpdir)
        tdSql.execute(f"drop database if exists `{long_db}`")
        tdSql.execute(f"create database `{long_db}` vgroups 1")
        tdSql.execute(f"create table `{long_db}`.`{long_tbl}`(ts timestamp, v int)")
        tdSql.execute(f"insert into `{long_db}`.`{long_tbl}` values(1640000000000, 42)")

        # literal '%2A' in a name exercises the '%' -> '%25' escaping round-trip
        pct_tbl = "a%2Ab"
        tdSql.execute(f"create table `{long_db}`.`{pct_tbl}`(ts timestamp, v int)")
        tdSql.execute(f"insert into `{long_db}`.`{pct_tbl}` values(1640000000000, 43)")

        bin_ = self.binPath()
        self.makeDir(tmpdir)

        # argv list (no shell): long_db contains ':' and long_tbl contains '\'
        self.execArgs([bin_, "-D", long_db, "-o", tmpdir, "-T", "1"])

        tdSql.execute(f"drop database `{long_db}`")
        assert not self.dbFound(long_db), "long_db should be gone before restore"

        self.execArgs([bin_, "-i", tmpdir, "-T", "1"])

        assert self.dbFound(long_db), "long_db not found after restore"

        tdSql.query(f"select v from `{long_db}`.`{long_tbl}`")
        tdSql.checkData(0, 0, 42)
        tdSql.query(f"select v from `{long_db}`.`{pct_tbl}`")
        tdSql.checkData(0, 0, 43)

        tdSql.execute(f"drop database if exists `{long_db}`")
        tdLog.info("do_boundary .................................. [passed]")

    # ──────── Test 7b: Windows reserved device names as identifiers ────────

    def do_reserved_devs(self):
        """Windows reserved device names (CON/PRN/... any case) as identifiers."""
        tmpdir = "./taosbackuptest/tmpdir_reserved_devs"

        reserved_devs = [
            "CON", "PRN", "AUX", "NUL",
            "COM1", "COM2", "COM3", "COM4", "COM5",
            "COM6", "COM7", "COM8", "COM9",
            "LPT1", "LPT2", "LPT3", "LPT4", "LPT5",
            "LPT6", "LPT7", "LPT8", "LPT9",
            "con", "nul", "cOm3", "lPt9",
        ]

        # vgroups are limited: first name is the DB, the rest are table names
        db = reserved_devs[0]
        tbls = reserved_devs[1:]

        tdSql.execute(f"drop database if exists `{db}`")
        tdSql.execute(f"create database `{db}` vgroups 1")
        for i, tb in enumerate(tbls):
            tdSql.execute(f"create table `{db}`.`{tb}`(ts timestamp, v int)")
            tdSql.execute(f"insert into `{db}`.`{tb}` values(1640000000000, {i + 1})")

        bin_ = self.binPath()
        self.makeDir(tmpdir)

        self.execArgs([bin_, "-D", db, "-o", tmpdir, "-T", "1"])

        tdSql.execute(f"drop database `{db}`")
        assert not self.dbFound(db), f"db `{db}` should be gone before restore"

        self.execArgs([bin_, "-i", tmpdir, "-T", "1"])

        assert self.dbFound(db), f"db `{db}` not found after restore"

        for i, tb in enumerate(tbls):
            tdSql.query(f"select v from `{db}`.`{tb}`")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, i + 1)

        tdSql.execute(f"drop database if exists `{db}`")
        tdLog.info("do_reserved_devs .............................. [passed]")

    # ─────────── Test 8: restore Windows-produced binary fixture ────────────

    def do_windows_backup_cn_restore(self):
        """
        Restore a BINARY (.dat) backup that was produced by a FIXED Windows
        taosBackup and committed under data/windows_backup_cn/.

        Rationale / history:
          On Windows the engine is built WITHOUT zstd/zlib/xz, so its
          compressL2Dict substitutes LZ4 for those slots.  Older taosdump
          wrote the *schema-declared* algorithm (e.g. zstd for NCHAR) into the
          .dat while actually compressing with LZ4.  A Linux restore then ran
          the real zstd decoder on an LZ4 stream and failed with 0x80000120.
          The fix normalises the recorded algorithm to what the platform really
          runs (LZ4 on Windows), so this fixture's NCHAR columns are labelled
          LZ4 and must decompress cleanly on Linux/macOS.

          A second historical bug (bckToUtf8) double-encoded UTF-8 identifiers
          as if they were ANSI/GBK, corrupting Chinese table/column/file names.
          It was removed, so every identifier in this fixture is clean UTF-8.

          This test therefore does NOT run a backup — it only restores the
          committed fixture and asserts the full content round-tripped:
            - all 4 databases exist
            - nchar_big  : 5000 Chinese NCHAR(50) rows (LZ4-compressed block)
            - db monitor : NTB with Chinese column name `区域 代码`
            - db@monitor : NTB with '@' special chars in table/column names
            - db#test    : STB with Chinese name `tbl(v1)_传感器` + column
                           `描述` NCHAR(20) + tag `t#region` NCHAR(8)
        """
        # Fixture directory must exist (data/ is committed to the repo)
        assert os.path.isdir(_WIN_BCK_DIR), (
            f"Windows backup fixture dir not found: {_WIN_BCK_DIR}"
        )

        # Drop any stale databases first so restore starts from a clean slate
        for db in _WIN_BCK_DBS:
            tdSql.execute(f"drop database if exists `{db}`")

        tdLog.info(f"Restoring Windows-produced fixture from {_WIN_BCK_DIR}")
        bin_ = self.binPath()
        restore_proc = subprocess.run(
            [bin_, "-i", _WIN_BCK_DIR],
            text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            encoding='utf-8', errors='replace'
        )
        output = restore_proc.stdout or ""
        for line in output.splitlines():
            tdLog.info(line)
        tdLog.info(f"taosdump restore rc={restore_proc.returncode}")
        if restore_proc.returncode != 0 or "SUCCESS" not in output:
            tdLog.exit(
                f"Windows fixture restore failed rc={restore_proc.returncode}; "
                f"see full taosdump output above (lines={len(output.splitlines())})"
            )

        # --- all 4 databases exist ---
        for db in _WIN_BCK_DBS:
            assert self.dbFound(db), f"db `{db}` not found after Windows fixture restore"
        tdLog.info("  4 databases created ........................ [passed]")

        # --- nchar_big: 5000 Chinese NCHAR(50) rows, sample content exact ---
        tdSql.query(f"select count(*) from nchar_big.t1")
        tdSql.checkData(0, 0, _NCHAR_BIG_ROWS)
        tdSql.query(
            f"select remark from nchar_big.t1 where ts = {_NCHAR_BIG_SAMPLE_TS}"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, _NCHAR_BIG_SAMPLE_REMARK)
        tdLog.info(f"  nchar_big: {_NCHAR_BIG_ROWS} rows + sample ............ [passed]")

        # --- db monitor: Chinese column name `区域 代码` restored ---
        tdSql.query(f"select count(*) from `db monitor`.`table name`")
        tdSql.checkData(0, 0, _DB_MONITOR_ROWS)
        tdSql.query(
            f"select `col name` from `db monitor`.`table name` "
            f"where ts = 1640000000000"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "space name")
        # Chinese column name is addressable after restore
        tdSql.query(
            f"select `区域 代码` from `db monitor`.`table name` limit 1"
        )
        tdSql.checkRows(1)
        tdLog.info("  db monitor: NTB + `区域 代码` ................. [passed]")

        # --- db@monitor: '@' in db/table/column names ---
        tdSql.query(f"select count(*) from `db@monitor`.`tbl@sensor`")
        tdSql.checkData(0, 0, _DB_AT_ROWS)
        tdSql.query(
            f"select `val@1`, `tag@cn` from `db@monitor`.`tbl@sensor` "
            f"order by ts limit 1"
        )
        tdSql.checkData(0, 0, _DB_AT_ROW0_VAL)
        tdSql.checkData(0, 1, _DB_AT_ROW0_TAG)
        tdLog.info("  db@monitor: '@' names + data .................. [passed]")

        # --- db#test: Chinese STB / column / tag names ---
        tdSql.query("select stable_name from information_schema.ins_stables "
                    "where db_name='db#test'")
        stb_names = [row[0] for row in tdSql.queryResult]
        assert _DB_HASH_STB in stb_names, (
            f"STB `{_DB_HASH_STB}` missing after Windows fixture restore"
        )
        # Chinese data-column names survive in the catalog (`描述` NCHAR col)
        tdSql.query(
            "select col_name from information_schema.ins_columns "
            "where db_name='db#test' and table_name='%s' and table_type='SUPER_TABLE'"
            % _DB_HASH_STB
        )
        col_names = [row[0] for row in tdSql.queryResult]
        assert "ts" in col_names, "`ts` col missing"
        assert "c#1" in col_names, "`c#1` col missing"
        assert "描述" in col_names, "Chinese col `描述` missing"
        # Chinese tag name (+ its value) survived in ins_tags
        tdSql.query(
            "select tag_name, tag_value from information_schema.ins_tags "
            "where db_name='db#test' and stable_name='%s'"
            % _DB_HASH_STB
        )
        tag_rows = tdSql.queryResult
        assert len(tag_rows) >= 1, "tag rows missing for db#test"
        tag_map = {row[0]: row[1] for row in tag_rows}
        assert "t#region" in tag_map, "Chinese tag `t#region` missing"
        # Chinese column is usable in a real query (projection must parse cleanly)
        tdSql.query(
            f"select `描述` from `db#test`.`{_DB_HASH_STB}` limit 1"
        )
        tdSql.checkRows(0)
        tdLog.info("  db#test: Chinese STB/col/tag names .......... [passed]")

        # Cleanup
        for db in _WIN_BCK_DBS:
            tdSql.execute(f"drop database if exists `{db}`")

        tdLog.info("do_windows_backup_cn_restore ................. [passed]")

    # ──────────────────────────── entry point ────────────────────────────────

    def test_taosbackup_chinese(self):
        """taosBackup: Chinese and special-character identifier support

        1. Chinese DB / STB / CTB / NTB names with Chinese column and tag
           names; Chinese NCHAR tag values and data values.
        2. Same Chinese database backed up and restored in Parquet format.
        3. Mixed Chinese + ASCII identifiers (hyphens, underscores in names).
        4. ASCII special characters valid on both Linux and Windows file
           systems: '@', '#', '()', space.
        5. ALL Windows-illegal ASCII filter characters (< > : " / \\ | ? * %)
           embedded in DB / STB / CTB / NTB names, percent-encoded by
           taosBackup, backed up and restored with data verified intact.
        6. Boundary cases: max-length names with special chars, literal
           '%HH' sequences; Windows reserved device names as DB + table names.
        7. Verify TDengine rejects '.' (the only character its parser rejects).
        8. Restore a BINARY (.dat) backup produced by a fixed Windows
           taosBackup (committed under data/windows_backup_cn/) and verify all
           Chinese / special-char identifiers + NCHAR content round-trip.

        Windows notes: taosBackup uses DB/table names as directory/file names.
        Characters illegal in Windows paths (< > : " / \\ | ? * %) are
        percent-encoded by taosBackup, so they round-trip on Windows as well.
        Control characters (0x00-0x1F) are also encoded by the same logic but
        cannot be reliably created through the SQL transport (the CLI /
        terminal layer swallows or reinterprets them), so they are covered by
        the bckNameTests unit test instead.

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-12 AlexDuan Created for Chinese / Unicode / special-char coverage
            - 2026-09-03 AlexDuan Added Windows-produced binary fixture restore
              (do_windows_backup_cn_restore, data/windows_backup_cn)

        """
        self.do_chinese_stb()
        self.do_chinese_parquet()
        self.do_chinese_mixed_ascii()
        self.do_special_chars_path()
        self.do_special_chars_illegal()
        self.do_boundary()
        self.do_reserved_devs()
        self.do_rejected_identifiers()
        self.do_windows_backup_cn_restore()

        # cleanup
        for db in (
            "设备监控",
            "db-中文-monitor",
            "db@monitor",
            "db#test",
            "db monitor",
        ) + tuple(_WIN_BCK_DBS):
            tdSql.execute(f"drop database if exists `{db}`")
        os.system("rm -rf ./taosbackuptest/")

        tdLog.info("test_taosbackup_chinese ...................... [passed]")
