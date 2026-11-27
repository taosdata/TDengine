
# Copyright (c) 2023 TAOS Data, Inc. <jhtao@taosdata.com>
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

import sys
import time
import json
import subprocess
import tempfile
from taos.tmq import Consumer
import platform
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from new_test_framework.utils import etool, tdLog, tdSql, tdCom, tmqCom
from new_test_framework.utils.server.dnodes import tdDnodes

class TestCase:
    updatecfgDict = {'debugFlag': 135}    
    
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")
        cls.vgroups    = 1
        cls.ctbNum     = 10
        cls.rowsPerTbl = 10
        cls.tmqMaxTopicNum = 20
        cls.tmqMaxGroups = 100

    def prepareTestEnv(self):
        tdLog.printNoPrefix("======== prepare test env include database, stable, ctables, and insert data: ")
        paraDict = {'dbName':     'dbt',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    1,
                    'stbName':    'stb',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1},{'type': 'TIMESTAMP', 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbStartIdx': 0,
                    'ctbNum':     10,
                    'rowsPerTbl': 10,
                    'batchNum':   10,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  10,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   1}

        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl

        tmqCom.initConsumerTable()
        tdCom.create_database(tdSql, paraDict["dbName"],paraDict["dropFlag"], vgroups=paraDict["vgroups"],replica=1)
        tdSql.execute("alter database %s wal_retention_period 360000" % (paraDict['dbName']))
        tdLog.info("create stb")
        tmqCom.create_stable(tdSql, dbName=paraDict["dbName"],stbName=paraDict["stbName"])
        tdLog.info("create ctb")
        tmqCom.create_ctable(tdSql, dbName=paraDict["dbName"],stbName=paraDict["stbName"],ctbPrefix=paraDict['ctbPrefix'],
                             ctbNum=paraDict["ctbNum"],ctbStartIdx=paraDict['ctbStartIdx'])
        tdLog.info("insert data")
        tmqCom.insert_data_interlaceByMultiTbl(tsql=tdSql,dbName=paraDict["dbName"],ctbPrefix=paraDict["ctbPrefix"],
                                               ctbNum=paraDict["ctbNum"],rowsPerTbl=paraDict["rowsPerTbl"],batchNum=paraDict["batchNum"],
                                               startTs=paraDict["startTs"],ctbStartIdx=paraDict['ctbStartIdx'])

        tdLog.info("restart taosd to ensure that the data falls into the disk")
        # tdDnodes.stop(1)
        # tdDnodes.start(1)
        tdSql.query("flush database %s"%(paraDict['dbName']))
        return

    def tmqSubscribe(self, topicName, newGroupId, expectResult):
        # create new connector for new tdSql instance in my thread
        # newTdSql = tdCom.newTdSql()
        # topicName = inputDict['topic_name']
        # group_id = inputDict['group_id']
        
        consumer_dict = {
            "group.id": newGroupId,
            "client.id": "client",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.commit.interval.ms": "1000",
            "enable.auto.commit": "true",
            "auto.offset.reset": "earliest",
            "experimental.snapshot.enable": "false",
            "msg.with.table.name": "false"
        }
        
        ret = 'success'
        consumer = Consumer(consumer_dict)
        # print("======%s"%(inputDict['topic_name']))
        try:
            consumer.subscribe([topicName])
        except Exception as e:
            tdLog.info("consumer.subscribe() fail ")
            tdLog.info("%s"%(e))
            if (expectResult == "fail"):
                consumer.close()
                return 'success'
            else: 
                consumer.close()
                return 'fail'
    
        tdLog.info("consumer.subscribe() success ")
        if (expectResult == "success"):
            consumer.close()
            return 'success'
        else: 
            consumer.close()
            return 'fail'

    def waitForRows(self, statement, expected, process, timeout=180):
        deadline = time.monotonic() + timeout
        while True:
            tdSql.query(statement, queryTimes=1)
            actual = tdSql.queryRows
            tdLog.info(f"{statement}: count={actual}, expected={expected}")
            returncode = process.poll()
            if returncode is not None:
                raise AssertionError(
                    f"taosBenchmark exited with status {returncode}; "
                    f"{statement}: count={actual}, expected={expected}"
                )
            if actual == expected:
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AssertionError(
                    f"Timed out after {timeout}s: {statement}: "
                    f"count={actual}, expected={expected}"
                )
            time.sleep(min(1, remaining))

    def dumpDiagnostics(self, process, writer, benchmarkLog):
        tdLog.info(
            f"taosBenchmark pid={process.pid if process else None}, "
            f"returncode={process.poll() if process else None}, "
            f"writer_alive={writer.is_alive()}, benchmark_log={benchmarkLog}"
        )
        for statement in ("show consumers", "show subscriptions"):
            try:
                tdSql.query(statement, queryTimes=1)
                tdLog.info(f"{statement}: {tdSql.queryResult!r}")
            except Exception as error:
                tdLog.info(f"Diagnostic query {statement} failed: {error}")
        try:
            command = (["tasklist"] if platform.system().lower() == "windows"
                       else ["ps", "-eo", "pid,ppid,stat,etime,comm"])
            result = subprocess.run(command, capture_output=True, text=True, timeout=5)
            tdLog.info(f"Process state:\n{result.stdout}")
        except Exception as error:
            tdLog.info(f"Process diagnostics failed: {error}")
        logFiles = [benchmarkLog]
        for node in [tdDnodes.sim, *tdDnodes.dnodes]:
            logDir = getattr(node, "logDir", None)
            if logDir and os.path.isdir(logDir):
                logFiles.extend(os.path.join(logDir, name) for name in os.listdir(logDir)
                                if name.startswith(("taosdlog", "taoslog")))
        for logFile in logFiles:
            try:
                with open(logFile, "rb") as log:
                    log.seek(0, os.SEEK_END)
                    log.seek(max(0, log.tell() - 16384))
                    tdLog.info(f"Log tail {logFile}:\n{log.read().decode(errors='replace')}")
            except OSError as error:
                tdLog.info(f"Cannot read {logFile}: {error}")

    def tmqCase1(self):
        tdLog.printNoPrefix("======== test case 1: ")
        paraDict = {'dbName':     'dbt',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    1,
                    'stbName':    'stb',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1},{'type': 'TIMESTAMP', 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbStartIdx': 0,
                    'ctbNum':     10,
                    'rowsPerTbl': 100000000,
                    'batchNum':   10,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  3,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   1}

        topicNameList = ['dbtstb_0001']
        tdLog.info("create topics from stb")
        queryString = "select * from %s.%s"%(paraDict['dbName'], paraDict['stbName'])
        for i in range(len(topicNameList)):
            sqlString = "create topic %s as %s" %(topicNameList[i], queryString)
            tdLog.info("create topic sql: %s"%sqlString)
            tdSql.execute(sqlString)

        # tdSql.query('show topics;')
        # topicNum = tdSql.queryRows
        # tdLog.info(" topic count: %d"%(topicNum))
        # if topicNum != len(topicNameList):
        #     tdLog.exit("show topics %d not equal expect num: %d"%(topicNum, len(topicNameList)))
        
        jsonfile = os.path.join(os.path.dirname(__file__), "tmqMaxGroupIds.json")
        with open(jsonfile, encoding="utf-8") as configFile:
            config = json.load(configFile)
        tmqInfo = config["tmq_info"]
        assert tmqInfo["concurrent"] == self.tmqMaxGroups - 1
        assert tmqInfo["group_mode"] == "independent"
        assert tmqInfo["group.id"] == ""
        expected = tmqInfo["concurrent"]

        os.makedirs("sim", exist_ok=True)
        logDir = tempfile.mkdtemp(prefix="tmq-max-groups-", dir="sim")
        benchmarkLog = os.path.abspath(os.path.join(logDir, "taosBenchmark.log"))
        tmqCom.g_end_insert_flag = 0
        pThread = tmqCom.asyncInsertDataByInterlace(paraDict)
        process = None
        try:
            # Keep the process handle and output: startProcess discards both on Linux.
            with open(benchmarkLog, "w", encoding="utf-8") as output:
                process = subprocess.Popen(
                    [etool.benchMarkFile(), "-f", jsonfile],
                    stdout=output, stderr=subprocess.STDOUT,
                )
                tdSql.query("show topics")
                tdSql.checkRows(1)
                self.waitForRows("show consumers", expected, process)
                self.waitForRows("show subscriptions", expected, process)

                res = self.tmqSubscribe(topicNameList[0], "newGroupId_001", "success")
                assert res == "success", "100th consumer group must subscribe"
                res = self.tmqSubscribe(topicNameList[0], "newGroupId_002", "fail")
                assert res == "success", "101st consumer group must be rejected"
        except BaseException:
            try:
                self.dumpDiagnostics(process, pThread, benchmarkLog)
            except Exception as error:
                tdLog.info(f"Failed to collect TMQ diagnostics: {error}")
            raise
        finally:
            tmqCom.g_end_insert_flag = 1
            try:
                if process is not None and process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=10)
            finally:
                pThread.join(timeout=30)
                if pThread.is_alive():
                    raise AssertionError("Insert thread did not stop within 30s")

        tdLog.printNoPrefix("======== test case 1 end ...... ")

    def test_tmq_max_groupids(self):
        """Basic: max groups limit
        
        1. Create database and topic
        2. Create 100 consumer groups (maximum allowed)
        3. Use a 180s polling deadline for consumers and subscriptions; dump diagnostics on failure
        4. Try creating 101st group and verify limit
        
        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-12-23 Alex Duan Migrated from uncatalog/system-test/7-tmq/test_tmqMaxGroupIds.py

        """
        self.prepareTestEnv()
        self.tmqCase1()



