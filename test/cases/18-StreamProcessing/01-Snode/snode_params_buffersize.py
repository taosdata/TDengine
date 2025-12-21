import time
import math
import random
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    streamUtil,
    StreamTableType,
    StreamTable,
    cluster,
)
from random import randint
import os
import subprocess
import json


class TestStreamParametersMemoryUsage:
    caseName = "streambuffersize verify"
    currentDir = os.path.dirname(os.path.abspath(__file__))
    runAll = False
    dbname = "test1"
    stbname = "stba"
    stName = ""
    resultIdx = ""
    sliding = 1
    subTblNum = 3
    tblRowNum = 10
    tableList = []

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_params_memory_usage(self):
        """Parameter: check memory usage

        1. Create snode
        2. Create stream
        3. Modify streambuffersize
        4. Check memory usage

        Catalog:
            - Streams:Snode

        Since: v3.3.3.7

        Labels: common,ci,skip

        Jira: None

        History:
            - 2025-7-8 lvze Created

        """

        tdStream.dropAllStreamsAndDbs()

        self.data()
        self.createSnodeTest()
        self.createStream()
        self.checkStreamRunning()
        self.checkstreamBufferSize()

    def data(self):
        random.seed(42)
        tdSql.execute("create database test1 vgroups 6;")
        tdSql.execute(
            "CREATE STABLE test1.`stba` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `cts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `cint` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium', `i1` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium') TAGS (`tint` INT, `tdouble` DOUBLE, `tvar` VARCHAR(100), `tnchar` NCHAR(100), `tts` TIMESTAMP, `tbool` BOOL)"
        )
        tdSql.execute(
            'CREATE TABLE test1.`a0` USING test1.`stba` (`tint`, `tdouble`, `tvar`, `tnchar`, `tts`, `tbool`) TAGS (44, 3.757254e+01, "klPqiAWzV1F6hSPMjm80YOOZEcSCF", "xOYc37COtmFYhKEUkL8hKVUmJmorOS30uOcmIC12OtNT4hE", 1943455971, true)'
        )
        tdSql.execute(
            'CREATE TABLE test1.`a1` USING test1.`stba` (`tint`, `tdouble`, `tvar`, `tnchar`, `tts`, `tbool`) TAGS (19, 6.525488e+01, "jMGdGyha8Q7WZxFBv6XO", "GvDFs3DREMcgidLGjJBZFmM2RbmLY", 439606400, false)'
        )
        tdSql.execute(
            'CREATE TABLE test1.`a2` USING test1.`stba` (`tint`, `tdouble`, `tvar`, `tnchar`, `tts`, `tbool`) TAGS (9, 4.416963e+01, "lE0hSUOVxfVkGrORvnnLiOJp", "TMxs9A8VS4", 1291130063, true)'
        )

        tables = ["a0", "a1", "a2"]
        base_ts = (
            int(time.mktime(time.strptime("2025-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")))
            * 1000
        )
        interval_ms = 600 * 1000
        total_rows = 10000

        for i in range(total_rows):
            ts = base_ts + i * interval_ms
            c1 = random.randint(0, 1000)
            c2 = random.randint(1000, 2000)
            for tb in tables:
                sql = "INSERT INTO test1.%s VALUES (%d,%d, %d, %d)" % (
                    tb,
                    ts,
                    ts,
                    c1,
                    c2,
                )
                tdSql.execute(sql)

    def data2(self):
        random.seed(42)
        # tdSql.execute("create database test1 vgroups 6;")
        # tdSql.execute("CREATE STABLE test1.`stba` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `cts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `cint` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium', `i1` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium') TAGS (`tint` INT, `tdouble` DOUBLE, `tvar` VARCHAR(100), `tnchar` NCHAR(100), `tts` TIMESTAMP, `tbool` BOOL)")
        # tdSql.execute('CREATE TABLE test1.`a0` USING test1.`stba` (`tint`, `tdouble`, `tvar`, `tnchar`, `tts`, `tbool`) TAGS (44, 3.757254e+01, "klPqiAWzV1F6hSPMjm80YOOZEcSCF", "xOYc37COtmFYhKEUkL8hKVUmJmorOS30uOcmIC12OtNT4hE", 1943455971, true)')
        # tdSql.execute('CREATE TABLE test1.`a1` USING test1.`stba` (`tint`, `tdouble`, `tvar`, `tnchar`, `tts`, `tbool`) TAGS (19, 6.525488e+01, "jMGdGyha8Q7WZxFBv6XO", "GvDFs3DREMcgidLGjJBZFmM2RbmLY", 439606400, false)')
        # tdSql.execute('CREATE TABLE test1.`a2` USING test1.`stba` (`tint`, `tdouble`, `tvar`, `tnchar`, `tts`, `tbool`) TAGS (9, 4.416963e+01, "lE0hSUOVxfVkGrORvnnLiOJp", "TMxs9A8VS4", 1291130063, true)')

        tables = ["a0", "a1", "a2"]
        base_ts = (
            int(time.mktime(time.strptime("2025-01-05 00:00:00", "%Y-%m-%d %H:%M:%S")))
            * 1000
        )
        interval_ms = 30 * 1000
        total_rows = 10000

        for i in range(total_rows):
            ts = base_ts + i * interval_ms
            c1 = random.randint(0, 1000)
            c2 = random.randint(1000, 2000)
            for tb in tables:
                sql = "INSERT INTO test1.%s VALUES (%d,%d, %d, %d)" % (
                    tb,
                    ts,
                    ts,
                    c1,
                    c2,
                )
                tdSql.execute(sql)

    def prepareData(self):
        tdLog.info(f"prepare data")

        tdStream.dropAllStreamsAndDbs()
        # wait all dnode ready
        time.sleep(5)
        tdStream.init_database(self.dbname)

        st1 = StreamTable(self.dbname, "st1", StreamTableType.TYPE_SUP_TABLE)
        st1.createTable(3)
        st1.append_data(0, self.tblRowNum)

        self.tableList.append("st1")
        for i in range(0, self.subTblNum + 1):
            self.tableList.append(f"st1_{i}")

        ntb = StreamTable(self.dbname, "ntb1", StreamTableType.TYPE_NORMAL_TABLE)
        ntb.createTable()
        ntb.append_data(0, self.tblRowNum)
        self.tableList.append(f"ntb1")

    def dataIn(self):
        tdLog.info(f"insert more data:")
        config = {
            "filetype": "insert",
            "cfgdir": "/etc/taos",
            "host": "localhost",
            "port": 6030,
            "user": "root",
            "password": "taosdata",
            "thread_count": 16,
            "thread_count_create_tbl": 8,
            "result_file": "./insert.txt",
            "confirm_parameter_prompt": "no",
            "insert_interval": 0,
            "num_of_records_per_req": 1000,
            "max_sql_len": 1048576,
            "databases": [
                {
                    "dbinfo": {
                        "name": "test1",
                        "drop": "no",
                        "replica": 3,
                        "days": 10,
                        "precision": "ms",
                        "keep": 36500,
                        "minRows": 100,
                        "maxRows": 4096,
                    },
                    "super_tables": [
                        {
                            "name": "stba",
                            "child_table_exists": "no",
                            "childtable_count": 3,
                            "childtable_prefix": "a",
                            "auto_create_table": "no",
                            "batch_create_tbl_num": 10,
                            "data_source": "rand",
                            "insert_mode": "taosc",
                            "insert_rows": 5000,
                            "childtable_limit": 100000000,
                            "childtable_offset": 0,
                            "interlace_rows": 0,
                            "insert_interval": 0,
                            "max_sql_len": 1048576,
                            "disorder_ratio": 0,
                            "disorder_range": 1000,
                            "timestamp_step": 30000,
                            "start_timestamp": "2025-01-01 00:00:00.000",
                            "sample_format": "",
                            "sample_file": "",
                            "tags_file": "",
                            "columns": [
                                {
                                    "type": "timestamp",
                                    "name": "cts",
                                    "count": 1,
                                    "start": "2025-02-01 00:00:00.000",
                                },
                                {"type": "int", "name": "cint", "max": 100, "min": -1},
                                {"type": "int", "name": "i1", "max": 100, "min": -1},
                            ],
                            "tags": [
                                {"type": "int", "name": "tint", "max": 100, "min": -1},
                                {
                                    "type": "double",
                                    "name": "tdouble",
                                    "max": 100,
                                    "min": 0,
                                },
                                {
                                    "type": "varchar",
                                    "name": "tvar",
                                    "len": 100,
                                    "count": 1,
                                },
                                {
                                    "type": "nchar",
                                    "name": "tnchar",
                                    "len": 100,
                                    "count": 1,
                                },
                                {"type": "timestamp", "name": "tts"},
                                {"type": "bool", "name": "tbool"},
                            ],
                        }
                    ],
                }
            ],
        }

        with open("insert_config.json", "w") as f:
            json.dump(config, f, indent=4)
        tdLog.info("config file ready")
        cmd = f"taosBenchmark -f insert_config.json "
        # output = subprocess.check_output(cmd, shell=True).decode().strip()
        ret = os.system(cmd)
        if ret != 0:
            raise Exception("taosBenchmark run failed")
        time.sleep(5)
        tdLog.info(f"Insert data:taosBenchmark -f insert_config.json")

    def checkResultRows(self, expectedRows):
        tdSql.checkResultsByFunc(
            f"select * from information_schema.ins_snodes order by id;",
            lambda: tdSql.getRows() == expectedRows,
            delay=0.5,
            retry=2,
        )

    def get_memory_usage_mb(self, pid):
        status_file = "/proc/{}/status".format(pid)
        try:
            with open(status_file, "r") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        parts = line.split()
                        if len(parts) >= 2:
                            # kB -> MB
                            tdLog.info(f"taosd memory: {int(parts[1]) / 1024.0}")
                            return int(parts[1]) / 1024.0

        except IOError:
            print("Cannot open status file for pid {}".format(pid))
        return 0.0

    def get_pid_by_cmdline(self, pattern):
        try:
            cmd = "unset LD_PRELOAD;ps -eo pid,cmd | grep '{}' | grep -v grep | grep -v SCREEN".format(
                pattern
            )
            output = subprocess.check_output(cmd, shell=True).decode().strip()
            # 可多行，默认取第一行
            lines = output.split("\n")
            if lines:
                pid = int(lines[0].strip().split()[0])
                return pid
        except subprocess.CalledProcessError:
            return None

    def checkstreamBufferSize(self):
        tdLog.info(f"check streamBufferSize")
        tdSql.query(f"show dnode 1 variables like 'streamBufferSize';")
        result = tdSql.getData(0, 2)
        tdLog.info(f"streamBufferSize is {result}")

        pid = self.get_pid_by_cmdline("taosd -c")

        for i in range(15):
            mem = self.get_memory_usage_mb(pid)
            time.sleep(2)
            if mem > float(result):
                raise Exception(f"ERROR:taosd memory large than streamBufferSize!")
            i = i + 1

    def createSnodeTest(self):
        tdLog.info(f"create snode test")
        tdSql.query("select * from information_schema.ins_dnodes order by id;")
        numOfNodes = tdSql.getRows()
        tdLog.info(f"numOfNodes: {numOfNodes}")

        for i in range(1, numOfNodes + 1):
            tdSql.execute(f"create snode on dnode {i}")
            tdLog.info(f"create snode on dnode {i} success")
        self.checkResultRows(numOfNodes)

        tdSql.checkResultsByFunc(
            f"show snodes;", lambda: tdSql.getRows() == numOfNodes, delay=0.5, retry=2
        )

    def createStream(self):
        tdLog.info(f"create stream ")
        for i in range(2):
            tdSql.execute(
                f"create stream {self.dbname}.`s{i}` interval(1s) sliding(1s) from {self.dbname}.{self.stbname} stream_options(fill_history('2025-01-01 00:00:00')) into {self.dbname}.`s{i}out` as select _wstart, sum(cint) from %%trows interval(10a)  order by _wstart;"
            )
            tdLog.info(f"create stream s{i} success!")

    def getMemoryMB(self):
        cmd = "unset LD_PRELOAD;free -m | grep  Mem | awk '{print $2}'"
        output = subprocess.check_output(cmd, shell=True).decode().strip()
        tdLog.info(f"total memory is {output} MB")
        return int(output)  # 单位：MB

    def checkStreamRunning(self):
        tdLog.info(f"check stream running status:")

        timeout = 60
        start_time = time.time()

        while True:
            if time.time() - start_time > timeout:
                tdLog.error("Timeout waiting for all streams to be running.")
                tdLog.error(f"Final stream running status: {streamRunning}")
                raise TimeoutError(
                    f"Stream status did not reach 'Running' within {timeout}s timeout."
                )

            tdSql.query(
                f"select status from information_schema.ins_streams order by stream_name;"
            )
            streamRunning = tdSql.getColData(0)

            if all(status == "Running" for status in streamRunning):
                tdLog.info("All Stream running!")
                tdLog.info(f"stream running status: {streamRunning}")
                return
            else:
                tdLog.info("Stream not running! Wait stream running ...")
                tdLog.info(f"stream running status: {streamRunning}")
                time.sleep(1)
