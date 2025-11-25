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
import json
import time
from threading import Thread
from threading import Event
import copy

#
#  kill taosadapter task
#
def killTask(stopEvent, taosadapter, presleep, sleep, count):
    tdLog.info(f"kill task pre sleep {presleep}s\n")
    time.sleep(presleep)
    stopcmd  = "kill -9 $(pidof taosadapter)"
    startcmd = f"nohup {taosadapter} --logLevel=error --opentsdb_telnet.enable=true > ~/taosa.log 2>&1 &"
    for i in range(count):
        tdLog.info(f" i={i} cmd:{stopcmd} sleep {sleep}s\n")
        os.system(stopcmd)
        time.sleep(sleep)
        tdLog.info(f" start cmd:{startcmd}\n")
        os.system(startcmd)
        if stopEvent.is_set():
            tdLog.info(" recv stop event and exit killTask ...\n")
            break

        time.sleep(sleep)

    tdLog.info("kill task exited.\n")


class TestTaosdumpRetry:
    def exec(self, command):
        tdLog.info(command)
        return os.system(command)

    def findPrograme(self):
        # taosdump
        taosdump = etool.taosDumpFile()
        if taosdump == "":
            tdLog.exit("taosdump not found!")
        else:
            tdLog.info("taosdump found in %s" % taosdump)

        # taosBenchmark
        benchmark = etool.benchMarkFile()
        if benchmark == "":
            tdLog.exit("benchmark not found!")
        else:
            tdLog.info("benchmark found in %s" % benchmark)

        # taosadapter
        taosadapter = etool.taosAdapterFile()
        if taosadapter == "":
            tdLog.exit("taosadapter not found!")
        else:
            tdLog.info("taosadapter found in %s" % taosadapter)

        # tmp dir
        tmpdir = "./tmp"
        if not os.path.exists(tmpdir):
            os.makedirs(tmpdir)
        else:
            print(f"{tmpdir} directory exists, clear data.")
            os.system("rm -rf %s/*" % tmpdir)

        return taosdump, benchmark, taosadapter, tmpdir

    def checkCorrectWithJson(self, jsonFile, newdb = None, checkInterval=False):
        #
        # check insert result
        #
        with open(jsonFile, "r") as file:
            data = json.load(file)

        # db come from arguments
        if newdb is None:
            db = data["databases"][0]["dbinfo"]["name"]
        else:
            db = newdb

        stb = data["databases"][0]["super_tables"][0]["name"]
        child_count = data["databases"][0]["super_tables"][0]["childtable_count"]
        insert_rows = data["databases"][0]["super_tables"][0]["insert_rows"]
        timestamp_step = data["databases"][0]["super_tables"][0]["timestamp_step"]

        tdLog.info(f"get json: db={db} stb={stb} child_count={child_count} insert_rows={insert_rows} \n")

        # all count insert_rows * child_table_count
        sql = f"select * from {db}.{stb}"
        tdSql.query(sql)
        tdSql.checkRows(child_count * insert_rows)

        # timestamp step
        if checkInterval:
            sql = f"select * from (select diff(ts) as dif from {db}.{stb} partition by tbname) where dif != {timestamp_step};"
            tdSql.query(sql)
            tdSql.checkRows(0)

    def testBenchmarkJson(self, benchmark, jsonFile, options="", checkInterval=False):
        # exe insert
        cmd = f"{benchmark} {options} -f {jsonFile}"
        self.exec(cmd)

    def insertData(self, benchmark, json, db):
        # insert super table
        self.testBenchmarkJson(benchmark, json)


    def dumpOut(self, taosdump, db , outdir, websocket:bool=False):
        # dump out
        command = f"{taosdump} -T 2 -k 2 -z 800 -D {db} -o {outdir}"
        if websocket:
            command += " -Z WebSocket"
        self.exec(command)

    def dumpIn(self, taosdump, db, newdb, indir):
        # dump in
        self.exec(f'{taosdump} -T 10 -W "{db}={newdb}" -i {indir}')

    def checkAggSame(self, db, newdb, stb, aggfun):
        # sum pk db
        sql = f"select {aggfun} from {db}.{stb}"
        tdSql.query(sql)
        sum1 = tdSql.getData(0,0)
        # sum pk newdb
        sql = f"select {aggfun} from {newdb}.{stb}"
        tdSql.query(sql)
        sum2 = tdSql.getData(0,0)

        if sum1 == sum2:
            tdLog.info(f"{aggfun} source db:{sum1} import db:{sum2} both equal.")
        else:
            tdLog.exit(f"{aggfun} source db:{sum1} import db:{sum2} not equal.")

    def checkProjSame(self, db, newdb, stb , row, col, where = "where tbname='d0'"):
        # sum pk db
        sql = f"select * from {db}.{stb} {where} limit {row+1}"
        tdSql.query(sql)
        val1 = copy.deepcopy(tdSql.getData(row, col))
        # sum pk newdb
        sql = f"select * from {newdb}.{stb} {where} limit {row+1}"
        tdSql.query(sql)
        val2 = copy.deepcopy(tdSql.getData(row, col))

        if val1 == val2:
            tdLog.info(f"{db}.{stb} {row},{col} source db:{val1} import db:{val2} both equal.")
        else:
            tdLog.exit(f"{db}.{stb} {row},{col} source db:{val1} len={len(val1)} import db:{val2} len={len(val2)} not equal.")


    def verifyResult(self, db, newdb, json):
        # compare with insert json
        self.checkCorrectWithJson(json, newdb)

        #  compare sum(pk)
        stb = "meters"
        self.checkAggSame(db, newdb, stb, "sum(ic)")
        self.checkAggSame(db, newdb, stb, "sum(usi)")
        self.checkProjSame(db, newdb, stb, 0, 3)
        self.checkProjSame(db, newdb, stb, 0, 4)
        self.checkProjSame(db, newdb, stb, 0, 6) # tag

        self.checkProjSame(db, newdb, stb, 8, 3)
        self.checkProjSame(db, newdb, stb, 8, 4)
        self.checkProjSame(db, newdb, stb, 8, 6) # tag

    # start kill
    def startKillThread(self, taosadapter, presleep, sleep, count):
        tdLog.info("call startKillThread ...\n")
        self.stopEvent = Event()
        self.thread = Thread(target=killTask, args=(self.stopEvent, taosadapter, presleep, sleep, count))
        self.thread.start()

    # stop kill
    def stopKillThread(self):
        tdLog.info("call stopKillThread begin...\n")
        self.stopEvent.set()
        self.thread.join()
        tdLog.info("call stopKillThread end\n")

    def _run_retry_test(self, newdb, websocket):
        # database
        db = "redb"

        # find
        taosdump, benchmark, taosadapter, tmpdir = self.findPrograme()
        json = f"{os.path.dirname(os.path.abspath(__file__))}/json/retry.json"

        # insert data with taosBenchmark
        self.insertData(benchmark, json, db)

        # start kill thread
        self.startKillThread(taosadapter, 2, 5, 3)

        # dump out
        self.dumpOut(taosdump, db, tmpdir, websocket=websocket)

        # stop kill
        self.stopKillThread()

        # dump in
        self.dumpIn(taosdump, db, newdb, tmpdir)

        # verify db
        self.verifyResult(db, newdb, json)

    def test_taosdump_retry(self):
        """taosdump except

        1. taosBenchmark prepare data with super table and normal table
        2. taosdump start dump out database
        3. Create except with kill -9 taosadapter during dump out
        4. Start taosadapter again
        5. taosdump dump in database
        6. Verify data correctness with sum aggregation
        7. Verify data correctness with row by row comparison on some data


        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-30 Alex Duan Migrated from uncatalog/army/tools/taosdump/ws/test_taosdump_retry.py

        """
        self._run_retry_test(newdb="nredb", websocket=False)
        self._run_retry_test(newdb="nwredb", websocket=True)
        tdLog.success("%s successfully executed" % __file__)
