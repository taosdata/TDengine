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

from new_test_framework.utils import tdLog, tdSql, etool, eos
import os
import json


class TestTaosBackupCommandline:

    def exec(self, command):
        tdLog.info(command)
        return os.system(command)

    def taosbackup(self, command):
        """Run taosBackup and return list of output lines."""
        return etool.taosbackup(command)

    def clearPath(self, path):
        os.system("rm -rf %s/*" % path)

    def findPrograme(self):
        # taosBackup
        taosbackup = etool.taosBackupFile()
        if taosbackup == "":
            tdLog.exit("taosBackup not found!")
        else:
            tdLog.info("taosBackup found in %s" % taosbackup)

        # taosBenchmark
        benchmark = etool.benchMarkFile()
        if benchmark == "":
            tdLog.exit("benchmark not found!")
        else:
            tdLog.info("benchmark found in %s" % benchmark)

        # tmp dir
        tmpdir = "./tmp"
        if not os.path.exists(tmpdir):
            os.makedirs(tmpdir)
        else:
            self.clearPath(tmpdir)

        return taosbackup, benchmark, tmpdir

    def checkCorrectWithJson(self, jsonFile, newdb=None, checkInterval=True):
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

    def insertBenchJson(self, jsonFile):
        benchmark = etool.benchMarkFile()
        self.exec(f"{benchmark} -f {jsonFile}")
        with open(jsonFile, "r") as f:
            data = json.load(f)
        db = data["databases"][0]["dbinfo"]["name"]
        stb = data["databases"][0]["super_tables"][0]["name"]
        child_count = data["databases"][0]["super_tables"][0]["childtable_count"]
        insert_rows = data["databases"][0]["super_tables"][0]["insert_rows"]
        return db, stb, child_count, insert_rows

    def insertData(self, jsonFile):
        db, stb, child_count, insert_rows = self.insertBenchJson(jsonFile)

        # normal table
        sqls = [
            f"create table {db}.ntb(st timestamp, c1 int, c2 binary(32))",
            f"insert into {db}.ntb values('2025-01-01 10:00:01', 1, 'abc1')",
            f"insert into {db}.ntb values('2025-01-01 10:00:02', 2, 'abc2')",
            f"insert into {db}.ntb values('2025-01-01 10:00:03', 3, 'abc3')",
            f"insert into {db}.ntb values('2025-01-01 10:00:04', 4, 'abc4')",
            f"insert into {db}.ntb values('2025-01-01 10:00:05', 5, 'abc5')",
        ]
        for sql in sqls:
            tdSql.execute(sql)

        return db, stb, child_count, insert_rows

    def checkManyString(self, rlist, expected_list):
        """Check that all expected strings appear in output."""
        output = "\n".join(rlist)
        for expected in expected_list:
            if expected not in output:
                tdLog.exit(
                    f"Expected string '{expected}' not found in output:\n{output}"
                )
            else:
                tdLog.info(f"  Found: '{expected}'")

    def checkListString(self, rlist, expected):
        """Check that a string appears in output list."""
        output = "\n".join(rlist)
        if expected not in output:
            tdLog.exit(f"Expected string '{expected}' not found in output:\n{output}")

    def check_same(self, db, newdb, stb, aggfun):
        sql = f"select {aggfun} from {db}.{stb}"
        tdSql.query(sql)
        sum1 = tdSql.getData(0, 0)
        sql = f"select {aggfun} from {newdb}.{stb}"
        tdSql.query(sql)
        sum2 = tdSql.getData(0, 0)
        if sum1 == sum2:
            tdLog.info(f"{aggfun} source:{sum1} import:{sum2} equal.")
        else:
            tdLog.exit(f"{aggfun} source:{sum1} import:{sum2} not equal.")

    def verifyResult(self, db, newdb, jsonFile):
        self.checkCorrectWithJson(jsonFile, newdb)
        stb = "meters"
        self.check_same(db, newdb, stb, "sum(fc)")
        self.check_same(db, newdb, stb, "sum(ti)")
        self.check_same(db, newdb, stb, "sum(si)")
        self.check_same(db, newdb, stb, "sum(ic)")
        self.check_same(db, newdb, stb, "avg(bi)")
        self.check_same(db, newdb, stb, "sum(uti)")
        self.check_same(db, newdb, stb, "sum(usi)")
        self.check_same(db, newdb, stb, "sum(ui)")
        self.check_same(db, newdb, stb, "avg(ubi)")
        self.check_same(db, newdb, "ntb", "sum(c1)")

    def dumpInOutMode(self, mode, db, jsonFile, tmpdir):
        """Test backup/restore with a specific connection mode."""
        self.clearPath(tmpdir)
        self.taosbackup(f"{mode} -D {db} -o {tmpdir}")

        newdb = "new" + db
        self.taosbackup(f'{mode} -W "{db}={newdb}" -i {tmpdir}')

        self.verifyResult(db, newdb, jsonFile)
        tdSql.execute(f"drop database if exists {newdb}")

    def checkExcept(self, command):
        """Check that a command fails with non-zero exit code."""
        try:
            code = eos.exe(command, show=True)
            if code == 0:
                tdLog.exit(f"Expected failure but command succeeded: {command}")
            else:
                tdLog.info(f"Passed: expected error code={code} for cmd: {command}")
        except Exception:
            tdLog.info(f"Passed: exception caught for command: {command}")

    def basicCommandLine(self, taosbackup, db, tmpdir):
        """Test basic commandline arguments."""
        checkItems = [
            # version info
            [f"-V", ["version:"]],
            # help
            [f"--help", ["Report bugs to"]],
            # schema only
            [f"-s -D {db} -o {tmpdir}", ["OK: Database"]],
            # thread count
            [f"-T 2 -D {db} -o {tmpdir}", ["OK: Database"]],
            # tag thread count (new option)
            [f"-m 2 -T 2 -D {db} -o {tmpdir}", ["OK: Database"]],
            # time range
            [
                f"-S '2022-10-01 00:00:50.000' -E '2022-10-01 00:00:60.000' {db} meters -o {tmpdir}",
                ["OK: Database"],
            ],
            # native connection
            [
                f"-Z native -D {db} -o {tmpdir}",
                ["OK: Database"],
            ],
            # websocket connection
            [
                f"-Z websocket -X http://127.0.0.1:6041 -D {db} -o {tmpdir}",
                ["OK: Database"],
            ],
            # parquet format backup
            [f"-F parquet -D {db} -o {tmpdir}", ["OK: Database"]],
            # binary format backup (default)
            [f"-F binary -D {db} -o {tmpdir}", ["OK: Database"]],
        ]

        for item in checkItems:
            self.clearPath(tmpdir)
            command = item[0]
            results = item[1]
            rlist = self.taosbackup(command)
            self.checkManyString(rlist, results)

    def exceptCommandLine(self, taosbackup, db, tmpdir):
        """Test commandline arguments that should fail."""
        # invalid port
        self.checkExcept(taosbackup + f" -P 65536 -D {db} -o {tmpdir}")
        # invalid driver
        self.checkExcept(taosbackup + f" -Z invalid -D {db} -o {tmpdir}")
        # invalid format
        self.checkExcept(taosbackup + f" -F unknownfmt -D {db} -o {tmpdir}")
        # invalid stmt version
        self.checkExcept(taosbackup + f" -v 99 -D {db} -o {tmpdir}")
        # missing output path
        self.checkExcept(taosbackup + f" -D {db}")
        # invalid DSN (unreachable)
        self.checkExcept(
            taosbackup
            + f" -Z 1 -X https://gw.cloud.taosdata.com?token=invalid -D {db} -o {tmpdir}"
        )
        # output path that doesn't exist
        self.checkExcept(taosbackup + f" -D {db} -o ./nonexistent/path/")

    def checkConnMode(self, db, tmpdir):
        """Test connection mode priority: cmd option > env variable."""
        taosbackup = etool.taosBackupFile()
        results = ["OK: Database"]

        # env=invalid port 6043, cmd=valid 6041 -> should use cmd
        os.environ["TDENGINE_CLOUD_DSN"] = "http://127.0.0.1:6043"
        self.clearPath(tmpdir)
        rlist = self.taosbackup(f"-X http://127.0.0.1:6041 -D {db} -o {tmpdir}")
        self.checkManyString(rlist, results)

        # env=valid 6041, no cmd -> should use env
        os.environ["TDENGINE_CLOUD_DSN"] = "http://127.0.0.1:6041"
        self.clearPath(tmpdir)
        rlist = self.taosbackup(f"-D {db} -o {tmpdir}")
        self.checkManyString(rlist, results)

        # no env, cmd=valid 6041
        os.environ["TDENGINE_CLOUD_DSN"] = ""
        self.clearPath(tmpdir)
        rlist = self.taosbackup(f"-X http://127.0.0.1:6041 -D {db} -o {tmpdir}")
        self.checkManyString(rlist, results)

        # cleanup env
        os.environ["TDENGINE_CLOUD_DSN"] = ""

    def checkVersion(self):
        """Check -V version output format."""
        rlist = self.taosbackup("-V")
        output = "\n".join(rlist)
        assert "version:" in output, f"'version:' not in output: {output}"
        tdLog.info("checkVersion passed.")

    def test_taosbackup_commandline(self):
        """taosBackup commandline

        1. Insert data with taosBenchmark (full type)
        2. Test backup/restore with Native, WebSocket, DSN modes
        3. Test basic commandline arguments:
           - -V version
           - --help
           - -s schemaonly
           - -T/-m thread count options
           - -S/-E time range
           - -Z native/websocket driver
           - -F binary/parquet format
        4. Test invalid commandline arguments (should fail):
           - Invalid port (-P 65536)
           - Invalid driver (-Z invalid)
           - Invalid format (-F unknown)
           - Invalid stmt version (-v 99)
           - Missing output path
           - Invalid DSN
           - Non-existent output directory
        5. Test connection mode priority: cmd > env variable (TDENGINE_CLOUD_DSN)

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-04 Migrated and adapted from 04-Taosdump/test_taosdump_commandline.py

        """
        taosbackup, benchmark, tmpdir = self.findPrograme()
        jsonFile = f"{os.path.dirname(os.path.abspath(__file__))}/json/insertFullType.json"

        # insert data
        db, stb, childCount, insertRows = self.insertData(jsonFile)

        # 1. check version
        self.checkVersion()
        tdLog.info("1. check version ...................................... [Passed]")

        # 2. test dump in/out with different modes
        modes = ["-Z native", "-Z websocket -X http://localhost:6041", "--dsn=http://localhost:6041"]
        for mode in modes:
            self.dumpInOutMode(mode, db, jsonFile, tmpdir)
        tdLog.info("2. native/websocket/dsn dump in/out ................ [Passed]")

        # 3. basic commandline
        self.basicCommandLine(taosbackup, db, tmpdir)
        tdLog.info("3. basic commandline arguments ...................... [Passed]")

        # 4. except commandline (expected failures)
        self.exceptCommandLine(taosbackup, db, tmpdir)
        tdLog.info("4. except commandline arguments ..................... [Passed]")

        # 5. check conn mode priority
        self.checkConnMode(db, tmpdir)
        tdLog.info("5. check conn mode priority ......................... [Passed]")
