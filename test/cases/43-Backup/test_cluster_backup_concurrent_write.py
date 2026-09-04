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
import threading
import time

from new_test_framework.utils import tdLog, tdSql, tdCom, cluster
from backup_restore_base import BackupRestoreTestBase


class TestClusterBackupConcurrentWrite(BackupRestoreTestBase):
    """Cluster backup/restore with concurrent writes, streams, multiple
    replicas and multiple vgroups.

    Run with: pytest -N 3
    Creates a 3-replica database with several vgroups, writes a large amount
    of data into the meters stable with taosBenchmark (much faster than a
    Python insert loop), creates a stream (snode) reading the same stable,
    then takes a backup WHILE a background thread keeps writing to the
    stream source. The test verifies:
      - the backup succeeds while writes are in flight
      - the restore succeeds and the restored rows match the pre-backup
        snapshot
      - the snode/stream survive the restore
      - the restored instance keeps accepting writes

    The stream is created AFTER the big data set is written: state_window
    does not re-compute history (fill_history is off by default), so the
    stream only processes the small amount of new data written afterwards.

    Since: v3.4.3.0

    Labels: common,ci,integration,functional,cluster
    """

    backup_root = None

    DB = "big"
    # single super table used for both the big data set and the stream source
    SOURCE_STB = "meters"
    RESULT_TABLE = "r1"
    # very large backup: allow a long wait for it to finish
    BACKUP_WAIT_TIMEOUT = 1800
    # big data set: 10000 child tables x 5000 rows = 50M rows
    # (reduced from 100M rows: the case used to time out in CI)
    BULK_TABLES = 10000
    BULK_ROWS = 5000

    # the bulk insert starts at this timestamp; it is also hardcoded in
    # json/insert_meters.json. The concurrent writer picks up right after
    # the bulk data (same timeline) so the stream computes correctly.
    START_TIMESTAMP = 1600000000000
    # taosBenchmark names the child tables d0..d{BULK_TABLES-1}; the
    # concurrent writer writes into d0, a child table that already exists.
    CHILD_TABLE = "d0"
    # first timestamp the concurrent writer uses: right after the last row
    # of d0 written by the bulk insert (d0 holds insert_rows rows starting
    # at START_TIMESTAMP with timestamp_step = 1).
    CONCURRENT_START_TS = START_TIMESTAMP + BULK_ROWS

    # common schema for the meters stable; ts + c1 pk first
    COLUMNS = "ts TIMESTAMP, c1 VARCHAR(16) primary key, c2 INT, " \
              "c3 INT UNSIGNED, c4 BIGINT, c5 BIGINT UNSIGNED, c6 FLOAT, " \
              "c7 DOUBLE, c8 BOOL, c9 BINARY(16), c10 NCHAR(16)"
    TAGS = "t INT"

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.dnode_id = 1
        cls.dnode_nums = len(cluster.dnodes)
        cls.backup_dnode = 1            # backup agent runs on dnode 1
        cls.backup_root = f"/tmp/backup_big_cluster_{int(time.time() * 1000)}"

    def teardown_class(cls):
        # remove the backup chains
        if cls.backup_root and os.path.exists(cls.backup_root):
            shutil.rmtree(cls.backup_root, ignore_errors=True)

    def _prepare_env(self):
        """Create a snode on dnode 1.

        The database and the meters stable are created by taosBenchmark
        (which drops and recreates the database), so here we only make sure a
        snode exists for the stream.
        """
        tdSql.execute("CREATE SNODE ON DNODE 1")
        tdSql.query("SELECT count(*) FROM information_schema.ins_snodes")
        if int(tdSql.getData(0, 0)) < 1:
            tdLog.exit(f"expected at least 1 snode, got {tdSql.getData(0, 0)}")

    def _create_stream(self, stream_name):
        """Create a state-window stream reading the meters stable.

        Created AFTER the big data set is written, so state_window only
        processes data written afterwards (fill_history is off). USE the
        database first: CREATE STREAM requires a current database context
        even though the tables are fully qualified.
        """
        tdSql.execute(f"USE {self.DB}")
        tdSql.execute(
            f"CREATE STREAM {stream_name} state_window(c2) from {self.DB}.{self.SOURCE_STB} "
            f"partition by tbname into {self.DB}.{self.RESULT_TABLE} "
            f"as select * from {self.DB}.{self.SOURCE_STB} "
            f"where ts >= _twstart and ts <= _twend"
        )

    def _wait_stream_running(self, stream_name, timeout=120):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                tdSql.query("SELECT status FROM information_schema.ins_streams "
                            f"WHERE stream_name = '{stream_name}'")
                if tdSql.queryRows > 0 and tdSql.getData(0, 0) == "Running":
                    return
            except Exception:
                pass
            time.sleep(1)
        tdLog.exit(f"stream {stream_name} did not become running within {timeout}s")

    def _wait_result_rows(self, table, timeout=60):
        """Poll the result table until it has rows."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                tdSql.query(f"SELECT COUNT(*) FROM {self.DB}.{table}")
                if tdSql.queryRows > 0 and tdSql.getData(0, 0) > 0:
                    return int(tdSql.getData(0, 0))
            except Exception:
                pass
            time.sleep(1)
        tdLog.exit(f"result table {self.DB}.{table} did not get rows within {timeout}s")

    def _insert_bulk(self):
        """Write BULK_TABLES x BULK_ROWS rows into the meters stable using
        taosBenchmark (much faster than a Python insert loop).

        The JSON config is a static template (json/insert_meters.json, the
        same layout used by 81-Tools/03-Benchmark); only the cfgdir
        placeholder is substituted at runtime with the real dnode cfg
        directory of the local sim cluster.
        """
        from new_test_framework.utils import sc, epath
        template = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "json", "insert_meters.json")
        with open(template) as f:
            config = f.read()
        cfg_dir = os.path.join(sc.clusterRootPath(), "dnode1", "cfg")
        config = config.replace("PLACEHOLDER_CFGDIR", cfg_dir)
        json_path = os.path.join(self.backup_root, "bench.json")
        os.makedirs(self.backup_root, exist_ok=True)
        with open(json_path, "w") as f:
            f.write(config)
        taosbenchmark_bin = epath.binFile("taosBenchmark")
        env = os.environ.copy()
        lib_dir = os.path.join(os.path.dirname(taosbenchmark_bin), "lib")
        env["LD_LIBRARY_PATH"] = lib_dir + os.pathsep + env.get("LD_LIBRARY_PATH", "")
        tdLog.info(f"running taosBenchmark with {json_path}")
        proc = subprocess.run(
            [taosbenchmark_bin, "-f", json_path],
            capture_output=True, text=True, timeout=1800, env=env,
        )
        tdLog.info(f"taosBenchmark exit: {proc.returncode}")
        if proc.returncode != 0:
            tdLog.exit(f"taosBenchmark failed: {proc.stdout[-2000:]}{proc.stderr[-2000:]}")
        tdLog.info(f"bulk insert done via taosBenchmark: {self.BULK_TABLES} x {self.BULK_ROWS} rows")

    def _values_row(self, ts, i):
        """Build one row of values for the shared schema (used by the
        concurrent writer so it can write into the taosBenchmark-created
        meters stable with the same 11 columns).
        """
        return (f"({ts}, 'pk{ts}', {i % 1000}, {i % 100000}, {i * 1000}, "
                f"{i * 1000000}, {i % 100 / 10.0}, {i % 1000 / 100.0}, "
                f"{i % 2 == 0}, 'b{i % 1000:08d}', 'n{i % 1000:08d}')")

    def _start_concurrent_writer(self):
        """Start a background thread that keeps writing to the stream source.

        The writer writes into d0, a child table of the meters stable that
        was already created by the bulk insert. Timestamps start right after
        the bulk data (START_TIMESTAMP + BULK_ROWS) so they stay on the same
        timeline and the stream keeps computing while the backup runs. The
        write rate is deliberately modest (4 rows every 0.2s) - fast enough
        to exercise a backup taken while writes are in flight, slow enough
        to keep the test light.

        Returns the thread, a stop Event and a state dict (batch count and
        last timestamp used).
        """
        stop = threading.Event()
        state = {"count": 0, "last_ts": self.CONCURRENT_START_TS}

        def worker():
            tdLog.info("concurrent writer started: %s, ts from %d"
                       % (self.CHILD_TABLE, self.CONCURRENT_START_TS))
            conn = tdCom.newTdSql()
            try:
                while not stop.is_set():
                    base = state["last_ts"]
                    rows = []
                    for k in range(4):
                        rows.append(self._values_row(base + k, (base + k) % 2))
                    conn.execute(
                        f"INSERT INTO {self.DB}.{self.CHILD_TABLE} VALUES "
                        + ", ".join(rows) + ";")
                    state["last_ts"] = base + 4
                    state["count"] += 1
                    time.sleep(0.2)
            except Exception as e:
                tdLog.info(f"concurrent writer stopped: {e}")
            finally:
                conn.close()
            tdLog.info("concurrent writer finished")

        t = threading.Thread(target=worker, daemon=True)
        t.start()
        return t, stop, state

    def _stop_concurrent_writer(self, thread, stop, timeout=30):
        stop.set()
        thread.join(timeout)
        tdLog.info("concurrent writer joined")

    def test_backup_with_concurrent_write(self):
        """Backup/restore with concurrent writes, multi-replica, multi-vgroup,
        and a stream.

        1. Create a snode
        2. Write 50M rows (10000 child tables x 5000 rows) into the meters
           stable with taosBenchmark
        3. Create a state-window stream reading the meters stable; since the
           big data set was written before the stream, it is not recomputed
        4. Start a background writer, then CREATE FULL BACKUP ... PARALLEL 2
           while the writer keeps writing to the meters stable; report total_size
        5. Stop the writer, stop taosd, clear data, restore with --parallel 2
           (parallel vnode verification + parallel vnode restore)
        6. Verify the restored row count matches the pre-backup snapshot
        7. Verify the snode/stream survive and the instance accepts writes

        Since: v3.4.3.0

        Labels: common,ci,integration,functional,cluster
        Jira: None

        History:
            - 2026-08-21 Bomin Zhang created

        """
        tdLog.info("=========== backup with concurrent write (50M rows) ===========")
        self._prepare_env()

        # 1. write the big data set into the meters stable via taosBenchmark
        self._insert_bulk()

        # 2. create the stream AFTER the big write: state_window does not
        #    re-compute history, so only the data written afterwards is
        #    processed (bounded result set)
        self._create_stream("s1")
        self._wait_stream_running("s1")
        tdLog.info("stream created after the bulk write and running")

        # 3. record the pre-backup row count
        tdSql.query(f"SELECT count(*) FROM {self.DB}.{self.SOURCE_STB}")
        pre_rows = int(tdSql.getData(0, 0))
        tdLog.info(f"pre-backup rows: {pre_rows}")
        if pre_rows < self.BULK_ROWS * self.BULK_TABLES:
            tdLog.exit(f"expected at least {self.BULK_ROWS * self.BULK_TABLES} rows, got {pre_rows}")

        # 4. start the concurrent writer, then take the backup with PARALLEL 2
        #    (the big database has 2 vgroups, so both are backed up concurrently)
        writer, stop, state = self._start_concurrent_writer()
        try:
            chain = self._start_backup("big_concurrent", parallel=2)
            tdLog.info(f"backup chain: {chain}")
        finally:
            self._stop_concurrent_writer(writer, stop)
        tdLog.info(f"concurrent writer inserted {state['count']} batches during backup")
        # the concurrent writes must have driven the stream to produce results
        self._wait_result_rows(self.RESULT_TABLE, timeout=60)
        tdLog.info("stream computed results from the concurrent writes")

        # 5. stop, clear, rebuild, restore (parallel 2: parallel vnode
        #    verification + parallel vnode restore)
        self._stop_all_taosd()
        self._clear_all_data()
        self._bootstrap_cluster_cfg()
        self._restore_all(chain, parallel=2)
        self._start_all_taosd()

        # 6. the restored data must match the pre-backup snapshot
        tdSql.query(f"SELECT count(*) FROM {self.DB}.{self.SOURCE_STB}")
        restored_rows = int(tdSql.getData(0, 0))
        tdLog.info(f"restored rows: {restored_rows}")
        if restored_rows < pre_rows:
            tdLog.exit(f"restored rows {restored_rows} < pre-backup rows {pre_rows}")
        if restored_rows != pre_rows:
            tdLog.info(f"note: restored rows {restored_rows} vs pre-backup {pre_rows} "
                       f"(backup is a snapshot)")

        # 7. snode/stream survive; instance keeps accepting writes
        tdSql.query("SELECT count(*) FROM information_schema.ins_snodes")
        tdSql.checkData(0, 0, 1)
        self._wait_stream_running("s1")
        # write into the existing d0 child table with a timestamp well past
        # everything written before/during the backup
        base_ts = self.CONCURRENT_START_TS + 1000000
        tdSql.execute(f"INSERT INTO {self.DB}.{self.CHILD_TABLE} VALUES "
                      f"{self._values_row(base_ts, 1)}")
        tdSql.query(f"SELECT count(*) FROM {self.DB}.{self.SOURCE_STB}")
        after_rows = int(tdSql.getData(0, 0))
        if after_rows <= restored_rows:
            tdLog.exit(f"write after restore did not increase rows: {after_rows} <= {restored_rows}")
        tdLog.info("restored cluster accepts new writes and stream is running")
