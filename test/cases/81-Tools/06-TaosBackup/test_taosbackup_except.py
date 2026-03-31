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

from new_test_framework.utils import tdLog, tdSql, etool, sc
import copy
import json
import os
import resource
import shutil
import signal
import subprocess
import time
from threading import Event, Thread


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def _killTask(stopEvent, taosadapter, presleep, sleep, count):
    """Background task that repeatedly kills and restarts taosadapter."""
    tdLog.info(f"kill task: pre-sleep {presleep}s")
    time.sleep(presleep)
    stopcmd = "kill -9 $(pidof taosadapter)"
    startcmd = (
        f"nohup {taosadapter} --logLevel=error --opentsdb_telnet.enable=true "
        f"> ~/taosa.log 2>&1 &"
    )
    for i in range(count):
        tdLog.info(f"  i={i}: killing taosadapter, then sleeping {sleep}s")
        os.system(stopcmd)
        time.sleep(sleep)
        tdLog.info(f"  starting taosadapter")
        os.system(startcmd)
        if stopEvent.is_set():
            tdLog.info("  received stop event, exiting killTask")
            break
        time.sleep(sleep)
    tdLog.info("killTask exited.")


def _pauseTaosdTask(stopEvent, presleep, pausetime):
    """Background task: stop taosd after *presleep* s to simulate an
    unresponsive server, hold for *pausetime* s, then start it again.

    Uses sc.dnodeStop(1) / sc.dnodeStart(1) for proper framework-level
    node control instead of raw SIGSTOP/SIGCONT signals.
    sc.dnodeStart is called unconditionally via try/finally so taosd is
    never left stopped even if the test thread exits early.
    """
    tdLog.info(f"pauseTaosdTask: pre-sleep {presleep}s before stopping taosd")
    slept = 0
    while slept < presleep:
        if stopEvent.is_set():
            tdLog.info("pauseTaosdTask: stop event during pre-sleep – cancelled")
            return
        time.sleep(0.5)
        slept += 0.5

    tdLog.info("pauseTaosdTask: sc.dnodeStop(1)")
    sc.dnodeStop(1)

    try:
        tdLog.info(f"pauseTaosdTask: taosd stopped, holding {pausetime}s")
        time.sleep(pausetime)
    finally:
        tdLog.info("pauseTaosdTask: sc.dnodeStart(1)")
        sc.dnodeStart(1)
        tdLog.info("pauseTaosdTask: taosd restarted")


# ---------------------------------------------------------------------------
# Single test class – all exception / retry scenarios in one place so the
# test framework runs a single deploy/destroy lifecycle for the whole file.
# ---------------------------------------------------------------------------

class TestTaosBackupRetry:
    """taosBackup exception handling tests.

    Covers:
      1. taosadapter kill/restart during backup   (native + WebSocket)
      2. taosd stop/start during backup       (connection-pool backoff)
      3. taosd stop/start during restore       (connection-pool backoff)
      4. taosadapter kill/restart during restore  (adapter retry)
      5. pthread_create failure safety            (no hang/crash/use-after-free)
    """

    # ── constants for server-pause tests ─────────────────────────────────
    _SRV_DB_SRC        = "srv_src"
    _SRV_DB_DST        = "srv_dst"
    _SRV_STB           = "meters"
    _SRV_CHILD_TABLES  = 20
    _SRV_INSERT_ROWS   = 50000      # 1 M rows – keeps backup busy
    _SRV_PRESLEEP_BCK  = 3          # seconds after backup starts → sc.dnodeStop
    _SRV_PRESLEEP_RST  = 3          # seconds after restore starts → sc.dnodeStop
    _SRV_PAUSETIME     = 6          # seconds taosd is held stopped

    # ── constants for restore-retry test ─────────────────────────────────
    _RR_DB_SRC = "exdb"      # must match except_small.json dbinfo.name
    _RR_DB_DST = "exdb_dst"
    _RR_STB    = "meters"

    # ── constants for thread-fail test ───────────────────────────────────
    _TF_DB_SRC  = "tf_src"
    _TF_THREADS = 8

    # =========================================================================
    # Helpers shared by the original retry test
    # =========================================================================

    def exec(self, command):
        tdLog.info(command)
        return os.system(command)

    def findPrograme(self):
        taosbackup = etool.taosBackupFile()
        if taosbackup == "":
            tdLog.exit("taosBackup not found!")
        else:
            tdLog.info("taosBackup found at %s" % taosbackup)

        benchmark = etool.benchMarkFile()
        if benchmark == "":
            tdLog.exit("taosBenchmark not found!")
        else:
            tdLog.info("taosBenchmark found at %s" % benchmark)

        taosadapter = etool.taosAdapterFile()
        if taosadapter == "":
            tdLog.exit("taosadapter not found!")
        else:
            tdLog.info("taosadapter found at %s" % taosadapter)

        tmpdir = "./tmp"
        if not os.path.exists(tmpdir):
            os.makedirs(tmpdir)
        else:
            tdLog.info(f"{tmpdir} exists, clearing data.")
            os.system("rm -rf %s/*" % tmpdir)

        return taosbackup, benchmark, taosadapter, tmpdir

    def checkCorrectWithJson(self, jsonFile, newdb=None, checkInterval=False):
        with open(jsonFile, "r") as f:
            data = json.load(f)
        db = newdb if newdb else data["databases"][0]["dbinfo"]["name"]
        stb = data["databases"][0]["super_tables"][0]["name"]
        child_count = data["databases"][0]["super_tables"][0]["childtable_count"]
        insert_rows = data["databases"][0]["super_tables"][0]["insert_rows"]
        timestamp_step = data["databases"][0]["super_tables"][0]["timestamp_step"]

        tdLog.info(
            f"check json: db={db} stb={stb} child_count={child_count} insert_rows={insert_rows}"
        )
        sql = f"select * from {db}.{stb}"
        tdSql.query(sql)
        tdSql.checkRows(child_count * insert_rows)

        if checkInterval:
            sql = (
                f"select * from (select diff(ts) as dif from {db}.{stb} partition by tbname) "
                f"where dif != {timestamp_step};"
            )
            tdSql.query(sql)
            tdSql.checkRows(0)

    def insertData(self, benchmark, jsonFile, db):
        self.exec(f"{benchmark} -f {jsonFile}")

    def dumpOut(self, taosbackup, db, outdir, websocket=False):
        command = f"{taosbackup} -T 2 -k 2 -z 800 -D {db} -o {outdir}"
        if websocket:
            command += " -Z WebSocket -X http://127.0.0.1:6041"
        self.exec(command)

    def dumpIn(self, taosbackup, db, newdb, indir):
        self.exec(f'{taosbackup} -T 10 -W "{db}={newdb}" -i {indir}')

    def checkAggSame(self, db, newdb, stb, aggfun):
        sql = f"select {aggfun} from {db}.{stb}"
        tdSql.query(sql)
        sum1 = tdSql.getData(0, 0)
        sql = f"select {aggfun} from {newdb}.{stb}"
        tdSql.query(sql)
        sum2 = tdSql.getData(0, 0)
        if sum1 == sum2:
            tdLog.info(f"{aggfun} source:{sum1} import:{sum2} equal.")
        else:
            tdLog.exit(f"{aggfun} source:{sum1} import:{sum2} NOT equal.")

    def checkProjSame(self, db, newdb, stb, row, col, where="where tbname='d0'"):
        sql = f"select * from {db}.{stb} {where} limit {row + 1}"
        tdSql.query(sql)
        val1 = copy.deepcopy(tdSql.getData(row, col))
        sql = f"select * from {newdb}.{stb} {where} limit {row + 1}"
        tdSql.query(sql)
        val2 = copy.deepcopy(tdSql.getData(row, col))
        if val1 == val2:
            tdLog.info(f"{stb}[{row},{col}] source:{val1} import:{val2} equal.")
        else:
            tdLog.exit(f"{stb}[{row},{col}] source:{val1} import:{val2} NOT equal.")

    def verifyResult(self, db, newdb, jsonFile):
        self.checkCorrectWithJson(jsonFile, newdb)
        stb = "meters"
        self.checkAggSame(db, newdb, stb, "sum(ic)")
        self.checkAggSame(db, newdb, stb, "sum(usi)")
        self.checkProjSame(db, newdb, stb, 0, 3)
        self.checkProjSame(db, newdb, stb, 0, 4)
        self.checkProjSame(db, newdb, stb, 0, 6)
        self.checkProjSame(db, newdb, stb, 8, 3)
        self.checkProjSame(db, newdb, stb, 8, 4)
        self.checkProjSame(db, newdb, stb, 8, 6)

    def startKillThread(self, taosadapter, presleep, sleep, count):
        tdLog.info("startKillThread called")
        self.stopEvent = Event()
        self.thread = Thread(
            target=_killTask,
            args=(self.stopEvent, taosadapter, presleep, sleep, count),
        )
        self.thread.start()

    def stopKillThread(self):
        tdLog.info("stopKillThread called")
        self.stopEvent.set()
        self.thread.join()
        tdLog.info("stopKillThread done")

    def _run_retry_test(self, newdb, websocket=False):
        db = "redb"

        taosbackup, benchmark, taosadapter, tmpdir = self.findPrograme()
        jsonFile = f"{os.path.dirname(os.path.abspath(__file__))}/json/retry.json"

        self.insertData(benchmark, jsonFile, db)
        self.startKillThread(taosadapter, presleep=2, sleep=5, count=3)
        self.dumpOut(taosbackup, db, tmpdir, websocket=websocket)
        self.stopKillThread()
        self.dumpIn(taosbackup, db, newdb, tmpdir)
        self.verifyResult(db, newdb, jsonFile)

    # =========================================================================
    # Helpers for server-pause tests
    # =========================================================================

    def _srv_find_programs(self):
        taosbackup = etool.taosBackupFile()
        if not taosbackup:
            tdLog.exit("taosBackup not found!")
        benchmark = etool.benchMarkFile()
        if not benchmark:
            tdLog.exit("taosBenchmark not found!")
        tmpdir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "tmp_srvrestart"
        )
        if os.path.exists(tmpdir):
            os.system(f"rm -rf {tmpdir}/*")
        else:
            os.makedirs(tmpdir)
        return taosbackup, benchmark, tmpdir

    def _srv_insert_data(self, benchmark):
        cmd = (
            f"{benchmark} -d {self._SRV_DB_SRC}"
            f" -t {self._SRV_CHILD_TABLES} -n {self._SRV_INSERT_ROWS} -y"
        )
        tdLog.info(f"insert data: {cmd}")
        ret = os.system(cmd)
        if ret != 0:
            tdLog.exit(f"taosBenchmark failed (ret={ret})")

    def _srv_get_agg(self, db):
        results = {}
        tdSql.query(f"SELECT count(*) FROM {db}.{self._SRV_STB}")
        results["count"] = tdSql.getData(0, 0)
        tdSql.query(f"SELECT sum(voltage) FROM {db}.{self._SRV_STB}")
        results["sum_voltage"] = tdSql.getData(0, 0)
        tdSql.query(f"SELECT avg(current) FROM {db}.{self._SRV_STB}")
        results["avg_current"] = tdSql.getData(0, 0)
        tdLog.info(
            f"[{db}] count={results['count']} sum_voltage={results['sum_voltage']}"
            f" avg_current={results['avg_current']}"
        )
        return results

    def _srv_verify(self, src_agg, dst_db):
        dst_agg = self._srv_get_agg(dst_db)
        ok = True
        for key in ("count", "sum_voltage"):
            if src_agg[key] != dst_agg[key]:
                tdLog.error(f"mismatch [{key}]: src={src_agg[key]} dst={dst_agg[key]}")
                ok = False
            else:
                tdLog.info(f"  ok [{key}]: {src_agg[key]}")
        key = "avg_current"
        sv, dv = src_agg[key], dst_agg[key]
        if sv is None or dv is None or abs(sv - dv) / (abs(sv) + 1e-15) > 1e-6:
            tdLog.error(f"mismatch [{key}]: src={sv} dst={dv}")
            ok = False
        else:
            tdLog.info(f"  ok [{key}]: {sv}")
        if not ok:
            tdLog.exit(f"data verification FAILED for restored db: {dst_db}")
        tdLog.info(f"data verification PASSED for restored db: {dst_db}")

    def _start_pause_thread(self, presleep):
        self._pause_stop_evt = Event()
        self._pause_thread = Thread(
            target=_pauseTaosdTask,
            args=(self._pause_stop_evt, presleep, self._SRV_PAUSETIME),
            daemon=True,
        )
        self._pause_thread.start()

    def _stop_pause_thread(self):
        """Cancel pre-sleep (if still waiting) and join.  The try/finally in
        _pauseTaosdTask guarantees sc.dnodeStart is called before the thread exits."""
        self._pause_stop_evt.set()
        self._pause_thread.join(timeout=self._SRV_PAUSETIME + 10)
        if self._pause_thread.is_alive():
            tdLog.info("WARNING: pause thread did not finish in time")

    def teardown_method(self, method):
        """Safety net: ensure taosd and taosadapter are up after each test;
        also remove tmp_* directories created by this test."""
        import glob
        if hasattr(self, "_pause_thread") and self._pause_thread.is_alive():
            self._pause_stop_evt.set()
            self._pause_thread.join(timeout=self._SRV_PAUSETIME + 10)
        # Ensure taosd is running (sc.dnodeStart is a no-op if already up).
        sc.dnodeStart(1)
        # Restart taosadapter if kill-tests left it dead, so the next
        # test class starts with a healthy adapter.
        try:
            out = subprocess.check_output(
                ["pidof", "taosadapter"], stderr=subprocess.DEVNULL
            )
            if out.strip():
                return  # adapter is alive
        except (subprocess.CalledProcessError, FileNotFoundError):
            pass
        taosadapter = etool.taosAdapterFile()
        if taosadapter:
            tdLog.info("teardown: taosadapter not running, restarting it")
            os.system(
                f"nohup {taosadapter} --logLevel=error"
                f" > ~/taosa_teardown.log 2>&1 &"
            )
            time.sleep(3)  # wait for adapter to bind port 6041 before next class
        # Clean up tmp directories
        test_dir = os.path.dirname(os.path.abspath(__file__))
        for d in glob.glob(os.path.join(test_dir, "tmp_*")):
            shutil.rmtree(d, ignore_errors=True)
        # Also clean the relative ./tmp used by findPrograme()
        tmp_rel = os.path.join(test_dir, "tmp")
        if os.path.isdir(tmp_rel):
            shutil.rmtree(tmp_rel, ignore_errors=True)

    # =========================================================================
    # Helpers for restore-retry test
    # =========================================================================

    def _rr_find_programs(self):
        taosbackup = etool.taosBackupFile()
        if not taosbackup:
            tdLog.exit("taosBackup not found!")
        benchmark = etool.benchMarkFile()
        if not benchmark:
            tdLog.exit("taosBenchmark not found!")
        taosadapter = etool.taosAdapterFile()
        if not taosadapter:
            tdLog.exit("taosadapter not found!")
        tmpdir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "tmp_rr"
        )
        if os.path.exists(tmpdir):
            os.system(f"rm -rf {tmpdir}/*")
        else:
            os.makedirs(tmpdir)
        return taosbackup, benchmark, taosadapter, tmpdir

    def _rr_insert_data(self, benchmark):
        jsonFile = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "json", "except_small.json"
        )
        tdLog.info(f"insert data via json: {jsonFile}")
        ret = os.system(f"{benchmark} -f {jsonFile}")
        if ret != 0:
            tdLog.exit(f"taosBenchmark failed (ret={ret})")
        return jsonFile

    def _start_kill_thread(self, taosadapter, presleep, sleep, count):
        self._kill_stop_evt = Event()
        self._kill_thread = Thread(
            target=_killTask,
            args=(self._kill_stop_evt, taosadapter, presleep, sleep, count),
            daemon=True,
        )
        self._kill_thread.start()

    def _stop_kill_thread(self):
        self._kill_stop_evt.set()
        self._kill_thread.join()

    # =========================================================================
    # Helpers for thread-fail test
    # =========================================================================

    def _tf_find_programs(self):
        taosbackup = etool.taosBackupFile()
        if not taosbackup:
            tdLog.exit("taosBackup not found!")
        benchmark = etool.benchMarkFile()
        if not benchmark:
            tdLog.exit("taosBenchmark not found!")
        tmpdir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "tmp_threadfail"
        )
        if os.path.exists(tmpdir):
            os.system(f"rm -rf {tmpdir}/*")
        else:
            os.makedirs(tmpdir)
        return taosbackup, benchmark, tmpdir

    def _tf_insert_small_data(self, benchmark):
        cmd = f"{benchmark} -d {self._TF_DB_SRC} -t 4 -n 1000 -y"
        tdLog.info(f"insert data: {cmd}")
        ret = os.system(cmd)
        if ret != 0:
            tdLog.exit(f"taosBenchmark failed (ret={ret})")

    # =========================================================================
    # Test methods
    # =========================================================================

    def test_taosbackup_retry(self):
        """taosBackup exception/retry

        1. taosBenchmark prepares data with super table and normal table
        2. taosBackup starts dump-out with retry options (-k 2 -z 800)
        3. Simulates exception by kill -9 taosadapter during dump-out
        4. taosadapter is restarted automatically
        5. taosBackup completes dump-out (retried successfully)
        6. taosBackup dumps in the exported data
        7. Verify data correctness with aggregation comparison
        8. Verify data correctness with projection comparison
        All tested in both Native and WebSocket connection modes.

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-04 Migrated and adapted from 04-Taosdump/test_taosdump_except.py

        """
        # Native mode
        self._run_retry_test(newdb="nredb", websocket=False)
        # WebSocket mode
        self._run_retry_test(newdb="nwredb", websocket=True)

    def test_taosbackup_server_restart_backup(self):
        """taosBackup: taosd unresponsive (sc.dnodeStop) during backup

        1. taosBenchmark inserts _SRV_CHILD_TABLES × _SRV_INSERT_ROWS rows.
        2. Record reference aggregations (count, sum(voltage), avg(current)).
        3. Start backup with retry headroom (-k 5 -z 2000).
           A background thread stops taosd after _SRV_PRESLEEP_BCK s,
           holds it for _SRV_PAUSETIME s, then restarts it.
        4. Backup must complete successfully despite the outage.
        5. Restore the backup to _SRV_DB_DST.
        6. Verify aggregations match the reference values.

        Exercises the exponential back-off in bckPool.c:
        getConnection() retries 1s→2s→4s→… until taosd responds again.

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-19 Alex Duan created

        """
        taosbackup, benchmark, tmpdir = self._srv_find_programs()

        tdLog.info("=== step 1: insert data ===")
        self._srv_insert_data(benchmark)

        tdLog.info("=== step 2: record reference aggregations ===")
        src_agg = self._srv_get_agg(self._SRV_DB_SRC)
        if src_agg["count"] == 0:
            tdLog.exit("source table is empty – taosBenchmark may have failed")

        tdLog.info("=== step 3: backup with taosd stopped mid-flight ===")
        backup_cmd = (
            f"{taosbackup} -T 2 -k 5 -z 2000"
            f" -D {self._SRV_DB_SRC} -o {tmpdir}"
        )
        self._start_pause_thread(presleep=self._SRV_PRESLEEP_BCK)
        try:
            tdLog.info(f"  exec: {backup_cmd}")
            ret = os.system(backup_cmd)
        finally:
            self._stop_pause_thread()
        if ret != 0:
            tdLog.exit(f"backup FAILED (ret={ret}) – backoff did not recover")

        # Ensure taosd is fully up before restore regardless of whether the
        # pause thread's join timed out or dnodeStart is still initializing.
        sc.dnodeStart(1)
        time.sleep(2)

        tdLog.info("=== step 4: restore ===")
        restore_cmd = (
            f"{taosbackup} -T 4"
            f" -W \"{self._SRV_DB_SRC}={self._SRV_DB_DST}\" -i {tmpdir}"
        )
        tdLog.info(f"  exec: {restore_cmd}")
        ret = os.system(restore_cmd)
        if ret != 0:
            tdLog.exit(f"restore FAILED (ret={ret})")

        tdLog.info("=== step 5: verify data correctness ===")
        self._srv_verify(src_agg, self._SRV_DB_DST)
        tdLog.info("test_taosbackup_server_restart_backup PASSED")

    def test_taosbackup_server_restart_restore(self):
        """taosBackup: taosd unresponsive (sc.dnodeStop) during restore

        1. taosBenchmark inserts data into _SRV_DB_SRC.
        2. Record reference aggregations.
        3. Backup _SRV_DB_SRC to disk (no fault injection during backup).
        4. Start restore to _SRV_DB_DST with retry headroom (-k 5 -z 2000).
           A background thread stops taosd after _SRV_PRESLEEP_RST s,
           holds it for _SRV_PAUSETIME s, then restarts it.
        5. Restore must complete successfully despite the outage.
        6. Verify aggregations match the reference values.

        Exercises the retry loop in restoreData.c:
        releaseConnectionBad() + getConnection() on retriable errors.

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-19 Alex Duan created

        """
        taosbackup, benchmark, tmpdir = self._srv_find_programs()

        tdLog.info("=== step 1: insert data ===")
        self._srv_insert_data(benchmark)

        tdLog.info("=== step 2: record reference aggregations ===")
        src_agg = self._srv_get_agg(self._SRV_DB_SRC)
        if src_agg["count"] == 0:
            tdLog.exit("source table is empty – taosBenchmark may have failed")

        tdLog.info("=== step 3: backup (no fault injection) ===")
        backup_cmd = f"{taosbackup} -T 4 -D {self._SRV_DB_SRC} -o {tmpdir}"
        tdLog.info(f"  exec: {backup_cmd}")
        ret = os.system(backup_cmd)
        if ret != 0:
            tdLog.exit(f"backup FAILED (ret={ret})")

        tdLog.info("=== step 4: restore with taosd stopped mid-flight ===")
        restore_cmd = (
            f"{taosbackup} -T 2 -k 5 -z 2000"
            f" -W \"{self._SRV_DB_SRC}={self._SRV_DB_DST}\" -i {tmpdir}"
        )
        self._start_pause_thread(presleep=self._SRV_PRESLEEP_RST)
        try:
            tdLog.info(f"  exec: {restore_cmd}")
            ret = os.system(restore_cmd)
        finally:
            self._stop_pause_thread()
        if ret != 0:
            tdLog.exit(f"restore FAILED (ret={ret}) – backoff did not recover")

        tdLog.info("=== step 5: verify data correctness ===")
        self._srv_verify(src_agg, self._SRV_DB_DST)
        tdLog.info("test_taosbackup_server_restart_restore PASSED")

    def test_taosbackup_restore_retry(self):
        """taosBackup restore retry: taosadapter kill/restart during dump-in

        1. taosBenchmark inserts data (except_small.json).
        2. Record reference aggregations (count, sum(ic)).
        3. Backup to disk (no fault injection during backup).
        4. Start restore with retry flags (-k 3 -z 1000).
           A background thread kills and restarts taosadapter 3 times,
           every 5 s, starting 2 s after the restore begins.
        5. Restore must complete successfully.
        6. Verify count(*) and sum(ic) match the source.

        Exercises the retry loop in restoreData.c on network errors.

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-19 Alex Duan created

        """
        taosbackup, benchmark, taosadapter, tmpdir = self._rr_find_programs()
        src_db = self._RR_DB_SRC

        tdLog.info("=== step 1: insert data ===")
        self._rr_insert_data(benchmark)

        tdLog.info("=== step 2: record reference aggregations ===")
        tdSql.query(f"SELECT count(*) FROM {src_db}.{self._RR_STB}")
        src_count = tdSql.getData(0, 0)
        tdSql.query(f"SELECT sum(ic) FROM {src_db}.{self._RR_STB}")
        src_sum_ic = tdSql.getData(0, 0)
        tdLog.info(f"source: count={src_count}  sum_ic={src_sum_ic}")
        if src_count == 0:
            tdLog.exit("source table empty – taosBenchmark may have failed")

        tdLog.info("=== step 3: backup (no fault injection) ===")
        backup_cmd = f"{taosbackup} -T 4 -D {src_db} -o {tmpdir}"
        tdLog.info(f"  exec: {backup_cmd}")
        ret = os.system(backup_cmd)
        if ret != 0:
            tdLog.exit(f"backup FAILED (ret={ret})")

        tdLog.info("=== step 4: restore with taosadapter kill/restart ===")
        # -k 15 -z 2000: up to 30 s of retries so adapter has time to restart
        restore_cmd = (
            f"{taosbackup} -T 2 -k 15 -z 2000"
            f" -W \"{src_db}={self._RR_DB_DST}\" -i {tmpdir}"
        )
        self._start_kill_thread(taosadapter, presleep=2, sleep=5, count=3)
        try:
            tdLog.info(f"  exec: {restore_cmd}")
            ret = os.system(restore_cmd)
        finally:
            self._stop_kill_thread()
        if ret != 0:
            tdLog.exit(f"restore FAILED (ret={ret}) – adapter retry did not recover")

        tdLog.info("=== step 5: verify data correctness ===")
        tdSql.query(f"SELECT count(*) FROM {self._RR_DB_DST}.{self._RR_STB}")
        dst_count = tdSql.getData(0, 0)
        tdSql.query(f"SELECT sum(ic) FROM {self._RR_DB_DST}.{self._RR_STB}")
        dst_sum_ic = tdSql.getData(0, 0)
        tdLog.info(f"restored: count={dst_count}  sum_ic={dst_sum_ic}")
        if src_count != dst_count:
            tdLog.exit(f"count mismatch: src={src_count} dst={dst_count}")
        if src_sum_ic != dst_sum_ic:
            tdLog.exit(f"sum(ic) mismatch: src={src_sum_ic} dst={dst_sum_ic}")
        tdLog.info("test_taosbackup_restore_retry PASSED")

    def test_taosbackup_thread_creation_failure(self):
        """taosBackup: clean exit when pthread_create fails mid-launch

        1. Check we are not running as root (root ignores RLIMIT_NPROC).
        2. Insert a small dataset.
        3. Lower RLIMIT_NPROC so taosBackup cannot spawn all -T 8 threads.
        4. Run taosBackup backup with -T 8.  Expect non-zero exit (thread
           creation failed), but no hang, crash, or use-after-free.
        5. Restore the original limit.
        6. Confirm the process terminated within 60 s (no hang).

        Since: v3.0.0.0

        Labels: common

        Jira: None

        History:
            - 2026-03-19 Alex Duan created

        """
        import pwd

        # Root bypasses RLIMIT_NPROC – skip gracefully
        if os.getuid() == 0:
            tdLog.info("running as root – skipping RLIMIT_NPROC test")
            return

        taosbackup, benchmark, tmpdir = self._tf_find_programs()

        tdLog.info("=== step 1: insert small dataset ===")
        self._tf_insert_small_data(benchmark)

        username = pwd.getpwuid(os.getuid()).pw_name
        proc_count_raw = subprocess.check_output(
            f"ps -u {username} --no-headers | wc -l", shell=True
        )
        current_procs = int(proc_count_raw.strip())
        tdLog.info(f"current user processes: {current_procs}")

        soft_orig, hard_orig = resource.getrlimit(resource.RLIMIT_NPROC)
        tdLog.info(f"original RLIMIT_NPROC: soft={soft_orig} hard={hard_orig}")

        new_soft = current_procs + 4
        if hard_orig != resource.RLIM_INFINITY and new_soft > hard_orig:
            tdLog.info(f"hard limit ({hard_orig}) too low – skipping")
            return

        try:
            resource.setrlimit(resource.RLIMIT_NPROC, (new_soft, hard_orig))
            tdLog.info(f"RLIMIT_NPROC lowered to soft={new_soft}")

            tdLog.info("=== step 2: run backup expecting thread-creation failure ===")
            cmd = f"{taosbackup} -T {self._TF_THREADS} -D {self._TF_DB_SRC} -o {tmpdir}"
            tdLog.info(f"  exec: {cmd}")

            proc = subprocess.Popen(cmd, shell=True, preexec_fn=os.setsid)
            try:
                proc.wait(timeout=60)
            except subprocess.TimeoutExpired:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                proc.wait()
                tdLog.exit(
                    "taosBackup HUNG (>60 s) when thread creation failed – "
                    "likely a deadlock or missing join"
                )

            rc = proc.returncode
            tdLog.info(f"taosBackup exited with code {rc}")
            if rc < -1:
                tdLog.exit(
                    f"taosBackup terminated by signal {-rc} – possible crash"
                )
        finally:
            resource.setrlimit(resource.RLIMIT_NPROC, (soft_orig, hard_orig))
            tdLog.info(f"RLIMIT_NPROC restored to soft={soft_orig}")

        tdLog.info("test_taosbackup_thread_creation_failure PASSED")
