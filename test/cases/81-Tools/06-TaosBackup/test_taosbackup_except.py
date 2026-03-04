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
import copy
import json
import os
import time
from threading import Event, Thread


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


class TestTaosBackupRetry:
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
        # Use -k / -z retry options to handle adapter restarts
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

        # insert test data
        self.insertData(benchmark, jsonFile, db)

        # start background kill/restart thread that simulates connection failures
        self.startKillThread(taosadapter, presleep=2, sleep=5, count=3)

        # dump out (with retry options -k 2 -z 800)
        self.dumpOut(taosbackup, db, tmpdir, websocket=websocket)

        # stop kill thread
        self.stopKillThread()

        # dump in
        self.dumpIn(taosbackup, db, newdb, tmpdir)

        # verify correctness
        self.verifyResult(db, newdb, jsonFile)

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
