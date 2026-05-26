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

import os
import shutil
import subprocess
import time

from new_test_framework.utils import tdLog, tdSql, etool, tdStream

SRC_DB = "tc_stream_src"
DST_DB = "tc_stream_dst"

STREAM_BASIC     = "s_basic"
STREAM_PARTITION = "s_partition"
STREAM_SLIDING   = "s_sliding"
STREAM_MULTI     = "s_multi"

ALL_STREAMS = [STREAM_BASIC, STREAM_PARTITION, STREAM_SLIDING, STREAM_MULTI]

# WebSocket connection flags for taosdump
WS_FLAGS = "-Z WebSocket -X http://127.0.0.1:6041"


class TestTaosBackupStream:

    # -----------------------------------------------------------------------
    # Shell / filesystem helpers
    # -----------------------------------------------------------------------

    def exec(self, command: str) -> str:
        tdLog.info(command)
        env = os.environ.copy()
        env.pop("LD_PRELOAD", None)
        result = subprocess.run(
            command, shell=True, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            env=env,
        )
        if result.stdout:
            for line in result.stdout.splitlines():
                tdLog.info(line)
        if result.returncode != 0:
            tdLog.exit(
                f"Command failed (exit {result.returncode}): {command}\n"
                f"{result.stdout}"
            )
        return result.stdout or ""

    def mkdir(self, path: str):
        if os.path.exists(path):
            shutil.rmtree(path)
        os.makedirs(path)

    def rmdir(self, path: str):
        if os.path.exists(path):
            shutil.rmtree(path)

    def dump_out(self, binPath: str, db: str, outdir: str, extra: str = "", ws: str = ""):
        """Run taosdump backup with optional WebSocket flags."""
        self.exec(f"{binPath} -D {db} -o {outdir} -T 1 {extra} {ws}".strip())

    def dump_in(self, binPath: str, indir: str, extra: str = "", ws: str = ""):
        """Run taosdump restore with optional WebSocket flags."""
        self.exec(f"{binPath} -i {indir} -T 1 {extra} {ws}".strip())

    # -----------------------------------------------------------------------
    # Stream helpers
    # -----------------------------------------------------------------------

    def ensure_snode(self):
        """Ensure at least one snode exists (required for stream creation)."""
        tdSql.query("SHOW SNODES")
        if tdSql.getRows() == 0:
            tdStream.createSnode()

    def get_stream_names(self, db: str) -> list:
        """Return sorted list of stream names in the given db."""
        tdSql.query(
            f"SELECT stream_name FROM information_schema.ins_streams "
            f"WHERE db_name='{db}' ORDER BY stream_name"
        )
        names = []
        for r in range(tdSql.getRows()):
            names.append(tdSql.getData(r, 0))
        return names

    def wait_streams_ready(self, db: str, expected_count: int, timeout: int = 30):
        """Wait until expected_count streams appear in ins_streams."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            names = self.get_stream_names(db)
            if len(names) >= expected_count:
                return names
            time.sleep(1)
        tdLog.exit(
            f"Timeout waiting for {expected_count} streams in {db}, "
            f"got {len(self.get_stream_names(db))}"
        )

    # -----------------------------------------------------------------------
    # Source DB setup
    # -----------------------------------------------------------------------

    def create_source_db(self):
        tdSql.execute(f"DROP DATABASE IF EXISTS {SRC_DB}")
        tdSql.execute(f"CREATE DATABASE {SRC_DB} VGROUPS 2 PRECISION 'ms'")
        tdSql.execute(f"USE {SRC_DB}")

        # physical super table + child tables
        tdSql.execute(
            f"CREATE STABLE stb (ts TIMESTAMP, c1 INT, c2 FLOAT, c3 VARCHAR(32)) "
            f"TAGS (t1 INT, t2 VARCHAR(16))"
        )
        tdSql.execute(f"CREATE TABLE ctb1 USING stb TAGS(1, 'beijing')")
        tdSql.execute(f"CREATE TABLE ctb2 USING stb TAGS(2, 'shanghai')")

        # insert data
        ts = 1700000000000
        for i in range(20):
            tdSql.execute(
                f"INSERT INTO ctb1 VALUES({ts + i * 1000}, {i}, {i * 1.1}, 'row{i}')"
            )
            tdSql.execute(
                f"INSERT INTO ctb2 VALUES({ts + i * 1000}, {i + 100}, {i * 2.2}, 'val{i}')"
            )

        # normal table
        tdSql.execute(
            f"CREATE TABLE ntb (ts TIMESTAMP, v1 INT, v2 DOUBLE)"
        )
        for i in range(10):
            tdSql.execute(
                f"INSERT INTO ntb VALUES({ts + i * 1000}, {i * 10}, {i * 3.3})"
            )

        tdLog.info(f"Source DB {SRC_DB} created with tables and data")

    def create_streams(self):
        """Create various stream types to exercise backup coverage."""
        self.ensure_snode()
        tdSql.execute(f"USE {SRC_DB}")

        # 1. Basic stream: simple interval aggregation
        tdSql.execute(
            f"CREATE STREAM {STREAM_BASIC} INTERVAL(5s) SLIDING(5s) "
            f"FROM {SRC_DB}.stb INTO {SRC_DB}.out_basic "
            f"AS SELECT _twstart + 0s AS ts, count(*) AS cnt, sum(c1) AS sum_c1 FROM %%trows"
        )

        # 2. Stream with PARTITION BY
        tdSql.execute(
            f"CREATE STREAM {STREAM_PARTITION} INTERVAL(5s) SLIDING(5s) "
            f"FROM {SRC_DB}.stb PARTITION BY tbname "
            f"INTO {SRC_DB}.out_partition "
            f"AS SELECT _twstart + 0s AS ts, count(*) AS cnt, avg(c2) AS avg_c2 FROM %%trows"
        )

        # 3. Stream with sliding window (sliding != interval)
        tdSql.execute(
            f"CREATE STREAM {STREAM_SLIDING} INTERVAL(10s) SLIDING(5s) "
            f"FROM {SRC_DB}.stb INTO {SRC_DB}.out_sliding "
            f"AS SELECT _twstart + 0s AS ts, min(c1) AS min_c1, max(c1) AS max_c1 FROM %%trows"
        )

        # 4. Multi: stream on normal table
        tdSql.execute(
            f"CREATE STREAM {STREAM_MULTI} INTERVAL(5s) SLIDING(5s) "
            f"FROM {SRC_DB}.ntb INTO {SRC_DB}.out_multi "
            f"AS SELECT _twstart + 0s AS ts, sum(v1) AS sum_v1, avg(v2) AS avg_v2 FROM %%trows"
        )

        self.wait_streams_ready(SRC_DB, len(ALL_STREAMS))
        tdLog.info(f"Created {len(ALL_STREAMS)} streams in {SRC_DB}")

    # -----------------------------------------------------------------------
    # Test 1: Full backup/restore of streams
    # -----------------------------------------------------------------------

    def do_stream_backup_restore(self, ws: str = ""):
        """End-to-end: create -> backup -> verify artifacts -> restore -> verify streams."""
        binPath = etool.taosDumpFile()
        if not binPath:
            tdLog.exit("taosBackup binary not found")

        outdir = "./taosbackuptest/stream_basic"
        self.mkdir(outdir)

        try:
            # Step 1: Build source
            self.create_source_db()
            self.create_streams()

            # Step 2: Verify source streams
            src_streams = self.get_stream_names(SRC_DB)
            tdLog.info(f"Source streams: {src_streams}")
            assert len(src_streams) == len(ALL_STREAMS), \
                f"Expected {len(ALL_STREAMS)} streams, got {len(src_streams)}"

            # Step 3: Full backup
            tdLog.info(f"Backup {SRC_DB} to {outdir}")
            self.dump_out(binPath, SRC_DB, outdir, ws=ws)

            # Step 4: Verify stream.sql exists and has content for each stream
            stream_sql_path = os.path.join(outdir, SRC_DB, "stream.sql")
            if not os.path.exists(stream_sql_path):
                tdLog.exit(f"stream.sql not found: {stream_sql_path}")
            tdLog.info(f"[OK] stream.sql exists: {stream_sql_path}")

            with open(stream_sql_path, "r") as f:
                stream_sql_content = f.read()

            # Each stream DDL should appear as one line
            lines = [ln.strip() for ln in stream_sql_content.splitlines() if ln.strip()]
            tdLog.info(f"stream.sql has {len(lines)} DDL line(s)")
            if len(lines) != len(ALL_STREAMS):
                tdLog.exit(
                    f"Expected {len(ALL_STREAMS)} lines in stream.sql, "
                    f"got {len(lines)}"
                )

            # Verify each stream name appears in the file
            for sname in ALL_STREAMS:
                found = any(sname in ln.lower() for ln in lines)
                if not found:
                    tdLog.exit(
                        f"Stream '{sname}' DDL not found in stream.sql"
                    )
                tdLog.info(f"[OK] Stream '{sname}' DDL found in stream.sql")

            # Verify no embedded newlines (each line should be a complete statement)
            for ln in lines:
                if '\n' in ln or '\r' in ln:
                    tdLog.exit(
                        f"Embedded newline in stream.sql line: {ln[:80]}..."
                    )
            tdLog.info("[OK] No embedded newlines in stream.sql")

            # Step 5: Drop source and restore
            tdSql.execute(f"DROP DATABASE IF EXISTS {SRC_DB}")

            tdLog.info(f"Restoring from {outdir}")
            self.dump_in(binPath, outdir, ws=ws)

            # Step 6: Verify streams recreated
            restored_streams = self.get_stream_names(SRC_DB)
            tdLog.info(f"Restored streams: {restored_streams}")
            if len(restored_streams) != len(ALL_STREAMS):
                tdLog.exit(
                    f"Expected {len(ALL_STREAMS)} streams after restore, "
                    f"got {len(restored_streams)}"
                )

            for sname in ALL_STREAMS:
                if sname not in restored_streams:
                    tdLog.exit(
                        f"Stream '{sname}' not found after restore"
                    )
                tdLog.info(f"[OK] Stream '{sname}' restored successfully")

            tdLog.info("Stream backup/restore basic test PASSED")

        finally:
            self.rmdir(outdir)

    # -----------------------------------------------------------------------
    # Test 2: Backup with no streams (backward compatibility)
    # -----------------------------------------------------------------------

    def do_no_stream_backup(self, ws: str = ""):
        """DB without streams: stream.sql should not be created."""
        binPath = etool.taosDumpFile()
        if not binPath:
            tdLog.exit("taosBackup binary not found")

        outdir = "./taosbackuptest/stream_nostream"
        self.mkdir(outdir)
        db = "tc_stream_empty"

        try:
            tdSql.execute(f"DROP DATABASE IF EXISTS {db}")
            tdSql.execute(f"CREATE DATABASE {db}")
            tdSql.execute(f"USE {db}")
            tdSql.execute(f"CREATE TABLE ntb (ts TIMESTAMP, v1 INT)")
            tdSql.execute(f"INSERT INTO ntb VALUES(NOW, 1)")

            self.dump_out(binPath, db, outdir, ws=ws)

            stream_sql_path = os.path.join(outdir, db, "stream.sql")
            if os.path.exists(stream_sql_path):
                with open(stream_sql_path, "r") as f:
                    content = f.read().strip()
                if content:
                    tdLog.exit(
                        f"stream.sql should be empty for DB without streams, "
                        f"but has content: {content[:120]}"
                    )
            tdLog.info("[OK] No stream.sql (or empty) for DB without streams")

            # Restore should still work (no stream.sql to process)
            tdSql.execute(f"DROP DATABASE IF EXISTS {db}")
            self.dump_in(binPath, outdir, ws=ws)

            # Verify DB restored
            tdSql.query(f"SELECT * FROM information_schema.ins_databases WHERE name='{db}'")
            if tdSql.getRows() == 0:
                tdLog.exit(f"Database '{db}' not found after restore")
            tdLog.info("[OK] DB without streams restored successfully")

            tdSql.execute(f"DROP DATABASE IF EXISTS {db}")
            tdLog.info("No-stream backup/restore test PASSED")

        finally:
            self.rmdir(outdir)

    # -----------------------------------------------------------------------
    # Test 3: Restore with stream already exists (idempotency)
    # -----------------------------------------------------------------------

    def do_stream_already_exists(self, ws: str = ""):
        """Restore when streams already exist should warn but not fail."""
        binPath = etool.taosDumpFile()
        if not binPath:
            tdLog.exit("taosBackup binary not found")

        outdir = "./taosbackuptest/stream_exists"
        self.mkdir(outdir)
        db = "tc_stream_dup"

        try:
            self.ensure_snode()
            tdSql.execute(f"DROP DATABASE IF EXISTS {db}")
            tdSql.execute(f"CREATE DATABASE {db} VGROUPS 1")
            tdSql.execute(f"USE {db}")
            tdSql.execute(
                f"CREATE TABLE stb (ts TIMESTAMP, c1 INT) TAGS(t1 INT)"
            )
            tdSql.execute(f"CREATE TABLE ct1 USING stb TAGS(1)")
            tdSql.execute(f"INSERT INTO ct1 VALUES(NOW, 100)")

            tdSql.execute(
                f"CREATE STREAM s_dup INTERVAL(5s) SLIDING(5s) "
                f"FROM {db}.stb INTO {db}.out_dup "
                f"AS SELECT _twstart + 0s AS ts, count(*) AS cnt FROM %%trows"
            )
            self.wait_streams_ready(db, 1)

            # Backup
            self.dump_out(binPath, db, outdir, ws=ws)

            # Do NOT drop the database — streams still exist
            # Restore again: streams already exist, should not fail
            tdLog.info("Restoring with existing streams (expect warning, not failure)")
            self.dump_in(binPath, outdir, ws=ws)

            # Verify stream still exists
            streams = self.get_stream_names(db)
            assert "s_dup" in streams, f"Stream 's_dup' lost after re-restore"
            tdLog.info("[OK] Stream survives duplicate restore")

            tdSql.execute(f"DROP DATABASE IF EXISTS {db}")
            tdLog.info("Stream already-exists test PASSED")

        finally:
            self.rmdir(outdir)

    # -----------------------------------------------------------------------
    # Test 4: Backup/restore with database rename (-W)
    # -----------------------------------------------------------------------

    def do_stream_rename_db(self, ws: str = ""):
        """Backup then restore with -W rename: streams should still work."""
        binPath = etool.taosDumpFile()
        if not binPath:
            tdLog.exit("taosBackup binary not found")

        outdir = "./taosbackuptest/stream_rename"
        self.mkdir(outdir)

        try:
            # Create source
            self.create_source_db()
            self.create_streams()

            src_streams = self.get_stream_names(SRC_DB)
            assert len(src_streams) == len(ALL_STREAMS)

            # Backup
            self.dump_out(binPath, SRC_DB, outdir, ws=ws)

            # Restore into DST_DB
            tdSql.execute(f"DROP DATABASE IF EXISTS {DST_DB}")
            tdSql.execute(f"DROP DATABASE IF EXISTS {SRC_DB}")
            tdLog.info(f"Restoring to {DST_DB}")
            self.dump_in(binPath, outdir, extra=f"-W {SRC_DB}={DST_DB}", ws=ws)

            # Verify streams in DST_DB
            dst_streams = self.get_stream_names(DST_DB)
            tdLog.info(f"Restored streams in {DST_DB}: {dst_streams}")

            if len(dst_streams) != len(ALL_STREAMS):
                tdLog.exit(
                    f"Expected {len(ALL_STREAMS)} streams in {DST_DB}, "
                    f"got {len(dst_streams)}"
                )

            for sname in ALL_STREAMS:
                if sname not in dst_streams:
                    tdLog.exit(f"Stream '{sname}' not found in {DST_DB}")
                tdLog.info(f"[OK] Stream '{sname}' exists in {DST_DB}")

            tdSql.execute(f"DROP DATABASE IF EXISTS {DST_DB}")
            tdLog.info("Stream rename-db test PASSED")

        finally:
            self.rmdir(outdir)

    # -----------------------------------------------------------------------
    # Test 5: Schema-only backup includes streams
    # -----------------------------------------------------------------------

    def do_stream_schema_only(self, ws: str = ""):
        """Schema-only backup (-s) should still include streams."""
        binPath = etool.taosDumpFile()
        if not binPath:
            tdLog.exit("taosBackup binary not found")

        outdir = "./taosbackuptest/stream_schema"
        self.mkdir(outdir)
        db = "tc_stream_schema"

        try:
            self.ensure_snode()
            tdSql.execute(f"DROP DATABASE IF EXISTS {db}")
            tdSql.execute(f"CREATE DATABASE {db} VGROUPS 1")
            tdSql.execute(f"USE {db}")
            tdSql.execute(
                f"CREATE TABLE stb (ts TIMESTAMP, c1 INT) TAGS(t1 INT)"
            )
            tdSql.execute(f"CREATE TABLE ct1 USING stb TAGS(1)")
            tdSql.execute(f"INSERT INTO ct1 VALUES(NOW, 42)")

            tdSql.execute(
                f"CREATE STREAM s_schema INTERVAL(5s) SLIDING(5s) "
                f"FROM {db}.stb INTO {db}.out_schema "
                f"AS SELECT _twstart + 0s AS ts, count(*) AS cnt FROM %%trows"
            )
            self.wait_streams_ready(db, 1)

            # Schema-only backup
            self.dump_out(binPath, db, outdir, extra="-s", ws=ws)

            # stream.sql should exist
            stream_sql_path = os.path.join(outdir, db, "stream.sql")
            if not os.path.exists(stream_sql_path):
                tdLog.exit(f"stream.sql not found in schema-only backup")
            tdLog.info("[OK] stream.sql present in schema-only backup")

            # Restore
            tdSql.execute(f"DROP DATABASE IF EXISTS {db}")
            self.dump_in(binPath, outdir, ws=ws)

            streams = self.get_stream_names(db)
            assert "s_schema" in streams, \
                f"Stream 's_schema' not found after schema-only restore"
            tdLog.info("[OK] Stream restored from schema-only backup")

            tdSql.execute(f"DROP DATABASE IF EXISTS {db}")
            tdLog.info("Schema-only stream test PASSED")

        finally:
            self.rmdir(outdir)

    # -----------------------------------------------------------------------
    # Test 6: Backtick-quoted, case-sensitive stream names
    # -----------------------------------------------------------------------

    def do_stream_backtick_names(self, ws: str = ""):
        """Streams with backtick-quoted, case-sensitive names should survive backup/restore."""
        binPath = etool.taosDumpFile()
        if not binPath:
            tdLog.exit("taosBackup binary not found")

        outdir = "./taosbackuptest/stream_backtick"
        self.mkdir(outdir)
        db = "tc_stream_bt"

        try:
            self.ensure_snode()
            tdSql.execute(f"DROP DATABASE IF EXISTS {db}")
            tdSql.execute(f"CREATE DATABASE {db} VGROUPS 1")
            tdSql.execute(f"USE {db}")
            tdSql.execute(
                f"CREATE STABLE stb (ts TIMESTAMP, c1 INT) TAGS(t1 INT)"
            )
            tdSql.execute(f"CREATE TABLE ct1 USING stb TAGS(1)")
            ts = 1700000000000
            for i in range(10):
                tdSql.execute(
                    f"INSERT INTO ct1 VALUES({ts + i * 1000}, {i})"
                )

            # Create streams with backtick-quoted, mixed-case names
            bt_streams = ["MyStream", "Stream_Mix_123", "UPPER_STREAM"]
            for sname in bt_streams:
                tdSql.execute(
                    f"CREATE STREAM `{sname}` INTERVAL(5s) SLIDING(5s) "
                    f"FROM {db}.stb INTO {db}.`out_{sname}` "
                    f"AS SELECT _twstart + 0s AS ts, count(*) AS cnt FROM %%trows"
                )
            self.wait_streams_ready(db, len(bt_streams))

            # Verify source streams (ins_streams stores names lowercase-or-original?)
            src_streams = self.get_stream_names(db)
            tdLog.info(f"Backtick source streams: {src_streams}")
            assert len(src_streams) == len(bt_streams), \
                f"Expected {len(bt_streams)} streams, got {len(src_streams)}"

            # Backup
            self.dump_out(binPath, db, outdir, ws=ws)

            # Verify stream.sql content has all stream names
            stream_sql_path = os.path.join(outdir, db, "stream.sql")
            if not os.path.exists(stream_sql_path):
                tdLog.exit(f"stream.sql not found: {stream_sql_path}")
            with open(stream_sql_path, "r") as f:
                sql_content = f.read()
            tdLog.info(f"stream.sql content:\n{sql_content}")

            lines = [ln.strip() for ln in sql_content.splitlines() if ln.strip()]
            assert len(lines) == len(bt_streams), \
                f"Expected {len(bt_streams)} DDL lines, got {len(lines)}"

            # Verify backtick-quoted names appear in DDL
            for sname in bt_streams:
                found = any(sname in ln for ln in lines)
                if not found:
                    tdLog.exit(f"Backtick stream '{sname}' DDL not found in stream.sql")
                tdLog.info(f"[OK] Backtick stream '{sname}' found in stream.sql")

            # Drop and restore
            tdSql.execute(f"DROP DATABASE IF EXISTS {db}")
            self.dump_in(binPath, outdir, ws=ws)

            # Verify all streams restored
            restored = self.get_stream_names(db)
            tdLog.info(f"Restored backtick streams: {restored}")
            assert len(restored) == len(bt_streams), \
                f"Expected {len(bt_streams)} streams after restore, got {len(restored)}"
            tdLog.info("[OK] All backtick-quoted streams restored")

            tdSql.execute(f"DROP DATABASE IF EXISTS {db}")
            tdLog.info("Backtick stream name test PASSED")

        finally:
            self.rmdir(outdir)

    # -----------------------------------------------------------------------
    # Test 7: Backtick-quoted stream names with -W rename
    # -----------------------------------------------------------------------

    def do_stream_backtick_rename(self, ws: str = ""):
        """Backtick-quoted streams with -W rename: db refs must be rewritten correctly."""
        binPath = etool.taosDumpFile()
        if not binPath:
            tdLog.exit("taosBackup binary not found")

        outdir = "./taosbackuptest/stream_bt_rename"
        self.mkdir(outdir)
        db_src = "tc_bt_src"
        db_dst = "tc_bt_dst"

        try:
            self.ensure_snode()
            tdSql.execute(f"DROP DATABASE IF EXISTS {db_src}")
            tdSql.execute(f"DROP DATABASE IF EXISTS {db_dst}")
            tdSql.execute(f"CREATE DATABASE {db_src} VGROUPS 1")
            tdSql.execute(f"USE {db_src}")
            tdSql.execute(
                f"CREATE STABLE stb (ts TIMESTAMP, c1 INT) TAGS(t1 INT)"
            )
            tdSql.execute(f"CREATE TABLE ct1 USING stb TAGS(1)")
            ts = 1700000000000
            for i in range(10):
                tdSql.execute(
                    f"INSERT INTO ct1 VALUES({ts + i * 1000}, {i})"
                )

            # Create stream with backtick-quoted, mixed-case name
            tdSql.execute(
                f"CREATE STREAM `CaseSensitive_Rename` INTERVAL(5s) SLIDING(5s) "
                f"FROM {db_src}.stb INTO {db_src}.`out_CsRename` "
                f"AS SELECT _twstart + 0s AS ts, count(*) AS cnt FROM %%trows"
            )
            self.wait_streams_ready(db_src, 1)

            # Backup
            self.dump_out(binPath, db_src, outdir, ws=ws)

            # Inspect stream.sql: DDL should contain db_src references (possibly backtick-quoted)
            stream_sql_path = os.path.join(outdir, db_src, "stream.sql")
            with open(stream_sql_path, "r") as f:
                sql_content = f.read()
            tdLog.info(f"stream.sql before rename:\n{sql_content}")

            # Restore with rename
            tdSql.execute(f"DROP DATABASE IF EXISTS {db_src}")
            self.dump_in(binPath, outdir, extra=f"-W {db_src}={db_dst}", ws=ws)

            # Verify stream exists in dst db
            dst_streams = self.get_stream_names(db_dst)
            tdLog.info(f"Renamed backtick streams in {db_dst}: {dst_streams}")
            assert len(dst_streams) == 1, \
                f"Expected 1 stream in {db_dst}, got {len(dst_streams)}"
            tdLog.info(f"[OK] Backtick stream renamed to {db_dst}")

            tdSql.execute(f"DROP DATABASE IF EXISTS {db_dst}")
            tdLog.info("Backtick stream rename test PASSED")

        finally:
            self.rmdir(outdir)

    # -----------------------------------------------------------------------
    # Run all sub-tests with a given connection mode
    # -----------------------------------------------------------------------

    def run_all_subtests(self, mode: str, ws: str = ""):
        """Run all 5 sub-tests with the given connection mode flags."""
        tdLog.info(f"========== Stream tests [{mode}] ==========")
        self.do_stream_backup_restore(ws=ws)
        self.do_no_stream_backup(ws=ws)
        self.do_stream_already_exists(ws=ws)
        self.do_stream_rename_db(ws=ws)
        self.do_stream_schema_only(ws=ws)
        self.do_stream_backtick_names(ws=ws)
        self.do_stream_backtick_rename(ws=ws)
        tdLog.info(f"========== Stream tests [{mode}] PASS ==========")

    # -----------------------------------------------------------------------
    # Test entry point
    # -----------------------------------------------------------------------

    def test_taosbackup_stream(self):
        """Backup/restore stream objects via Native and WebSocket.

        Sub-tests (run twice: Native + WebSocket):
        1. Full backup/restore with 4 stream types (interval, partition, sliding, normal-table).
        2. DB without streams: stream.sql absent, restore still works.
        3. Restore when stream already exists: warn and skip, not fail.
        4. Backup then restore with -W rename-db.
        5. Schema-only backup (-s) includes streams.
        6. Backtick-quoted, case-sensitive stream names survive backup/restore.
        7. Backtick-quoted stream names with -W rename-db.

        Since: v3.3.6.0

        Jira: None

        Labels: common

        History:
            - 2026-05-12 Alex Duan Created
            - 2026-05-13 Alex Duan Added WebSocket mode, removed USE db

        """
        tdLog.info("=== test_taosbackup_stream: START ===")

        # Round 1: Native mode
        self.run_all_subtests("Native")

        # Round 2: WebSocket mode (only when taosAdapter is running)
        self.run_all_subtests("WebSocket", ws=WS_FLAGS)

        tdLog.info("=== test_taosbackup_stream: PASS ===")
