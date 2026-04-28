import os
import platform
import subprocess
import sys
import threading
import time

from taos.cinterface import *
from taos.error import *
from taos.tmq import Consumer

from new_test_framework.utils import tdCom, tdLog, tdSql

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# taosBenchmark write params: 10 child tables x 100,000 rows = 1,000,000 rows total
_TABLE_COUNT   = 10
_ROWS_PER_TBL  = 100_000
_TOTAL_ROWS    = _TABLE_COUNT * _ROWS_PER_TBL   # 1 million
_ALTER_TRIGGER = 300_000                         # alter replica after consuming 300,000 rows


class TestCase:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.dbName    = "db_bench_consume_replica"
        cls.stbName   = "meters"          # default super table created by taosBenchmark
        cls.topicName = "topic_bench_consume_replica"

    # ------------------------------------------------------------------
    # Locate the taosBenchmark binary
    # ------------------------------------------------------------------
    def getPath(self, tool="taosBenchmark"):
        if platform.system().lower() == "windows":
            tool = tool + ".exe"
        selfPath = os.path.dirname(os.path.realpath(__file__))
        projPath = (
            selfPath[: selfPath.find("community")]
            if "community" in selfPath
            else selfPath[: selfPath.find("test")]
        )
        paths = []
        for root, dirs, files in os.walk(projPath):
            if tool in files:
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if "packaging" not in rootRealPath:
                    paths.append(os.path.join(root, tool))
                    break
        if not paths:
            tdLog.exit("taosBenchmark not found!")
        else:
            tdLog.info("taosBenchmark found in %s" % paths[0])
            return paths[0]

    # ------------------------------------------------------------------
    # Start taosBenchmark as a background process and return the Popen object.
    # taosBenchmark creates the database automatically (-y auto-confirms).
    # -v 1 : 1 vgroup  -a 1 : replica=1  -B 1 : batch size 1
    # ------------------------------------------------------------------
    def _startBenchmark(self, benchmarkPath):
        cmd = (
            f"{benchmarkPath} -y -d {self.dbName}"
            f" -t {_TABLE_COUNT} -n {_ROWS_PER_TBL} -v 1 -a 1 -B 1 -k 1000 -z 1000"
        )
        tdLog.info(f"starting taosBenchmark: {cmd}")
        kwargs = dict(shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if platform.system().lower() != "windows":
            kwargs["preexec_fn"] = os.setsid
        return subprocess.Popen(cmd, **kwargs)

    # ------------------------------------------------------------------
    # Consumer thread: poll continuously, signal alter at 300K rows,
    # exit when 1M rows have been consumed or no data for 60 seconds.
    # Logs consumed row count every 1,000 rows.
    # ------------------------------------------------------------------
    def _consumerThread(self, alterTriggerEvent, consumeDoneEvent,
                        consumedCounter, errorHolder):
        consumer = None
        try:
            consumer = Consumer({
                "group.id":                     "cgrp_bench_replica",
                "client.id":                    "client1",
                "td.connect.user":              "root",
                "td.connect.pass":              "taosdata",
                "enable.auto.commit":           "true",
                "auto.commit.interval.ms":      "1000",
                "auto.offset.reset":            "earliest",
                "experimental.snapshot.enable": "false",
            })
            consumer.subscribe([self.topicName])
            tdLog.info("consumer subscribed to topic, start polling")

            lastLogged = 0
            noDataCount = 0
            while True:
                res = consumer.poll(2)
                if not res:
                    err = taos_errno(None)
                    if err == 0:
                        noDataCount += 1
                        if noDataCount >= 30:
                            tdLog.info("no new data for 60 seconds, consumer exits")
                            break
                        continue
                    else:
                        tdLog.info(f"poll error: {taos_errstr(None)}")
                        time.sleep(2)
                        continue

                noDataCount = 0
                err = res.error()
                if err is not None:
                    errorHolder.append(str(err))
                    break

                val = res.value()
                if val:
                    for block in val:
                        consumedCounter[0] += len(block.fetchall())

                # print progress every 10,000 rows
                if consumedCounter[0] // 10000 > lastLogged // 10000:
                    tdLog.info(f"consumed rows: {consumedCounter[0]}")
                    lastLogged = consumedCounter[0]

                # trigger alter replica after consuming 300,000 rows
                if (not alterTriggerEvent.is_set()
                        and consumedCounter[0] >= _ALTER_TRIGGER):
                    tdLog.info(
                        f"consumed {consumedCounter[0]} rows, "
                        f"triggering alter replica 3"
                    )
                    alterTriggerEvent.set()

                # exit after consuming all 1 million rows
                if consumedCounter[0] >= _TOTAL_ROWS:
                    tdLog.info(
                        f"consumed {consumedCounter[0]} rows, target reached, exiting"
                    )
                    break

        except Exception as e:
            errorHolder.append(str(e))
            tdLog.info(f"consumer thread exception: {e}")
        finally:
            if consumer:
                consumer.close()
            consumeDoneEvent.set()

    # ------------------------------------------------------------------
    # Main test logic
    # ------------------------------------------------------------------
    def _runCase(self):
        tdSql.execute(f"alter all dnodes 'tqdebugflag 135'")
        tdSql.execute(f"alter all dnodes 'wdebugflag 135'")
        tdSql.execute(f"alter all dnodes 'qdebugflag 135'")

        tdLog.printNoPrefix(
            "======== test: concurrent write + consume + alter replica ========"
        )

        benchmarkPath = self.getPath()

        # 1. Start taosBenchmark in background; it creates the DB automatically
        benchProc = self._startBenchmark(benchmarkPath)
        tdLog.info("taosBenchmark started, waiting for table creation to complete")
        # Wait for taosBenchmark to finish creating meters stb + d0~d9 child tables
        time.sleep(10)

        # 2. Create topic with earliest offset to cover all written data
        tdSql.execute(f"drop topic if exists {self.topicName}")
        tdSql.execute(
            f"create topic {self.topicName} "
            f"as select * from {self.dbName}.{self.stbName}"
        )
        tdLog.info(f"topic {self.topicName} created")

        # 3. Shared state between threads
        consumedCounter   = [0]               # mutable row counter
        alterTriggerEvent = threading.Event() # fired when 10M rows consumed
        consumeDoneEvent  = threading.Event() # fired when consumer exits
        errorHolder       = []                # consumer thread errors

        # 4. Start consumer thread
        ct = threading.Thread(
            target=self._consumerThread,
            args=(alterTriggerEvent, consumeDoneEvent,
                  consumedCounter, errorHolder),
            daemon=True,
        )
        ct.start()
        tdLog.info("consumer thread started")

        # 5. Wait until 10M rows consumed, then alter replica 1 -> 3
        tdLog.info("waiting for consumer to reach 300,000 rows...")
        alterTriggerEvent.wait(timeout=3600)

        if alterTriggerEvent.is_set():
            tdLog.info("executing alter database replica 3")
            # Use a dedicated connection to avoid sharing cursor with consumer thread
            alterSql = tdCom.newTdSql()
            try:
                alterSql.execute(f"flush database {self.dbName}")
                alterSql.execute(f"alter database {self.dbName} replica 3")
                tdLog.info("alter database replica 3 submitted, waiting for transaction")
                tdCom.waitTransactionZeroWithTdsql(alterSql)
                tdLog.info("alter replica 3 completed")
            except Exception as e:
                tdLog.info(f"alter replica exception (cluster may have insufficient nodes): {e}")
        else:
            tdLog.info("WARNING: timed out waiting for alter replica trigger")

        # 6. Wait for taosBenchmark to finish writing
        tdLog.info("waiting for taosBenchmark to finish writing...")
        try:
            benchProc.wait(timeout=7200)
            tdLog.info("taosBenchmark write finished")
        except subprocess.TimeoutExpired:
            tdLog.info("taosBenchmark timed out, terminating")
            benchProc.terminate()

        # 7. Wait for consumer to finish consuming all data
        tdLog.info("waiting for consumer to finish consuming all 1 million rows...")
        consumeDoneEvent.wait(timeout=3600)
        ct.join(timeout=60)

        # 8. Validate
        if errorHolder:
            tdLog.exit(f"consumer thread errors: {errorHolder}")

        tdSql.query(f"select count(*) from {self.dbName}.{self.stbName}")
        totalInserted = tdSql.getData(0, 0) or 0
        consumedRows  = consumedCounter[0]

        tdLog.info(
            f"total inserted: {totalInserted}, "
            f"total consumed: {consumedRows}, "
            f"expected: {_TOTAL_ROWS}"
        )

        if totalInserted != _TOTAL_ROWS:
            tdLog.exit(
                f"inserted rows {totalInserted} != expected {_TOTAL_ROWS}"
            )

        if consumedRows < _TOTAL_ROWS:
            tdLog.exit(
                f"consumed rows {consumedRows} < expected {_TOTAL_ROWS}"
            )

        tdLog.info("validation passed: consumed rows match inserted rows (1 million)")

        # 9. Cleanup
        tdSql.execute(f"drop topic if exists {self.topicName}")
        tdLog.printNoPrefix("======== test finished ========")

    # ------------------------------------------------------------------
    # pytest entry point
    # ------------------------------------------------------------------
    def test_tmq_write_consume_alter_replica(self):
        """Concurrent: write, consume and alter replica

        While taosBenchmark continuously writes 1 million rows, a TMQ consumer
        reads from the earliest offset. After consuming 300,000 rows the
        database replica is altered from 1 to 3. Finally the test verifies that
        the total consumed row count equals 1 million.

        Steps:
        1. Start taosBenchmark to write 1 million rows (10 tables x 100K rows,
           replica=1, vgroups=1). taosBenchmark creates the database automatically.
        2. Wait for taosBenchmark to finish creating tables, then create TMQ topic
           with auto.offset.reset=earliest.
        3. Start Python TMQ consumer thread to poll and count rows in real time.
        4. When consumed rows reach 300,000, alter database replica from 1 to 3
           using a dedicated connection.
        5. Wait for taosBenchmark to finish all 1 million writes.
        6. Wait for consumer to finish consuming all data.
        7. Verify total consumed rows == 1 million.

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-16 wangmm0220 Created

        """
        self._runCase()


event = threading.Event()
