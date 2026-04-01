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

RESULT_SUCCESS = "Result       : SUCCESS"
class TestTaosBackupCommandline:

    def exec(self, command):
        tdLog.info(command)
        return os.system(command)

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
        newdb = "new" + db
        # Drop newdb before backup so it is not accidentally included in the backup package.
        tdSql.execute(f"drop database if exists {newdb}")
        self.clearPath(tmpdir)
        etool.taosbackup(f"{mode} -D {db} -o {tmpdir}")

        etool.taosbackup(f'{mode} -W "{db}={newdb}" -i {tmpdir}')

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
            [f"--help", ["Give this help list."]],
            # schema only
            [f"-s -D {db} -o {tmpdir}", [RESULT_SUCCESS]],
            # thread count
            [f"-T 2 -D {db} -o {tmpdir}", [RESULT_SUCCESS]],
            # tag thread count (new option)
            [f"-m 2 -T 2 -D {db} -o {tmpdir}", [RESULT_SUCCESS]],
            # time range
            [
                f"-S '2022-10-01 00:00:50.000' -E '2022-10-01 00:00:60.000' {db} meters -o {tmpdir}",
                [RESULT_SUCCESS],
            ],
            # native connection
            [
                f"-Z native -D {db} -o {tmpdir}",
                [RESULT_SUCCESS],
            ],
            # websocket connection
            [
                f"-Z websocket -X http://127.0.0.1:6041 -D {db} -o {tmpdir}",
                [RESULT_SUCCESS],
            ],
            # parquet format backup
            [f"-F parquet -D {db} -o {tmpdir}", [RESULT_SUCCESS]],
            # binary format backup (default)
            [f"-F binary -D {db} -o {tmpdir}", [RESULT_SUCCESS]],
            # debug mode (-g): backup still succeeds with richer output
            [f"-g -D {db} -o {tmpdir}", [RESULT_SUCCESS]],
            # config-dir (-c): use default /etc/taos explicitly
            [f"-c /etc/taos -D {db} -o {tmpdir}", [RESULT_SUCCESS, "Config Dir   : /etc/taos"]],
            # config-dir (long form)
            [f"--config-dir=/etc/taos -D {db} -o {tmpdir}", [RESULT_SUCCESS, "Config Dir   : /etc/taos"]],
        ]

        for item in checkItems:
            self.clearPath(tmpdir)
            command = item[0]
            results = item[1]
            rlist = etool.taosbackup(command)
            self.checkManyString(rlist, results)

    def exceptCommandLine(self, taosbackup, db, tmpdir):
        """Test commandline arguments that should fail (exit non-zero quickly).

        All validation below happens at argument-parse time (before any network
        connection), so these cases exit immediately with a non-zero code.
        """
        # invalid driver
        self.checkExcept(taosbackup + f" -Z invalid -D {db} -o {tmpdir}")
        # invalid format
        self.checkExcept(taosbackup + f" -F unknownfmt -D {db} -o {tmpdir}")
        # invalid stmt-version value
        self.checkExcept(taosbackup + f" -v 99 -D {db} -o {tmpdir}")
        # missing output path
        self.checkExcept(taosbackup + f" -D {db}")

        # --- restore-only options must be rejected in backup mode ---
        # -B (data-batch) is restore-only
        self.checkExcept(taosbackup + f" -B 20000 -D {db} -o {tmpdir}")
        # -v (stmt-version) is restore-only
        self.checkExcept(taosbackup + f" -v 1 -D {db} -o {tmpdir}")
        self.checkExcept(taosbackup + f" -v 2 -D {db} -o {tmpdir}")
        # -W (rename) is restore-only
        self.checkExcept(taosbackup + f' -W "{db}=newdb" -D {db} -o {tmpdir}')

        # --- backup-only options must be rejected in restore mode ---
        # -S / -E (time range) are backup-only
        self.checkExcept(taosbackup + f" -S 2024-01-01T00:00:00 -D {db} -i {tmpdir}")
        self.checkExcept(taosbackup + f" -E 2024-01-01T00:00:00 -D {db} -i {tmpdir}")

        # --- -B range validation (all at parse time; -i used so action=RESTORE) ---
        # below minimum: 0
        self.checkExcept(taosbackup + f" -B 0 -D {db} -i {tmpdir}")
        # below minimum: negative (use long-form to avoid shell flag confusion)
        self.checkExcept(taosbackup + f" --data-batch=-1 -D {db} -i {tmpdir}")
        # above global max (>100000), caught at parse time regardless of -v
        self.checkExcept(taosbackup + f" -B 100001 -D {db} -i {tmpdir}")
        # above STMT1 max explicitly: -v 1, B=100001
        self.checkExcept(taosbackup + f" -B 100001 -v 1 -D {db} -i {tmpdir}")
        # exceeds STMT2 max (16384) when -v 2 is explicit: boundary value 16385
        self.checkExcept(taosbackup + f" -B 16385 -v 2 -D {db} -i {tmpdir}")
        # exceeds STMT2 max (16384) when -v 2 is the default (no explicit -v)
        self.checkExcept(taosbackup + f" -B 16385 -D {db} -i {tmpdir}")
        # mid-range value valid for STMT1 but exceeds STMT2 max: 50000 with -v 2
        self.checkExcept(taosbackup + f" -B 50000 -v 2 -D {db} -i {tmpdir}")
        # same with default STMT2 (no -v): 50000 should also fail
        self.checkExcept(taosbackup + f" -B 50000 -D {db} -i {tmpdir}")

        # --- -T / -m thread counts must be >= 1 ---
        self.checkExcept(taosbackup + f" -T 0 -D {db} -o {tmpdir}")
        self.checkExcept(taosbackup + f" -m 0 -D {db} -o {tmpdir}")

        # --- -P port must be in [1, 65535] (parse-time check, exits immediately) ---
        self.checkExcept(taosbackup + f" -P 99999 -D {db} -o {tmpdir}")
        self.checkExcept(taosbackup + f" -P 65536 -D {db} -o {tmpdir}")
        self.checkExcept(taosbackup + f" -P 0 -D {db} -o {tmpdir}")

    def checkDataBatch(self, db, jsonFile, tmpdir):
        """Validate -B (data-batch) and -v (stmt-version) boundary values for restore.

        STMT2 (default): range [1, 16384], default 10000
        STMT1 (-v 1):    range [1, 100000], default 60000

        Backup is performed once; all restore cases reuse the same backup dir.
        Each restore uses a unique renamed db which is verified then dropped.
        """
        # backup once, reuse for all restore tests
        self.clearPath(tmpdir)
        etool.taosbackup(f"-D {db} -o {tmpdir}")

        cases = [
            # (stmt_ver, batch_size, label)
            (2,      1, "STMT2 min=1"),
            (2,  10000, "STMT2 default=10000"),
            (2,  16384, "STMT2 max=16384"),
            (1,      1, "STMT1 min=1"),
            (1,  60000, "STMT1 default=60000"),
            (1, 100000, "STMT1 max=100000"),
        ]

        for (stmt_ver, batch, label) in cases:
            newdb = f"bckb{stmt_ver}x{batch}"
            tdSql.execute(f"drop database if exists {newdb}")
            rlist = etool.taosbackup(
                f'-v {stmt_ver} -B {batch} -W "{db}={newdb}" -i {tmpdir}'
            )
            self.checkManyString(rlist, [RESULT_SUCCESS])
            self.verifyResult(db, newdb, jsonFile)
            tdSql.execute(f"drop database if exists {newdb}")
            tdLog.info(f"  {label}: B={batch} v={stmt_ver} ............... [OK]")

    def checkConfigDir(self, db, tmpdir):
        """Test -c / --config-dir parameter.

        Verifies that:
        1. -c <dir> is accepted and shown in startup summary (Config Dir line).
        2. --config-dir=<dir> long form works identically.
        3. A custom (non-default) directory path is reflected in the summary.
        """
        results_default = [RESULT_SUCCESS, "Config Dir   : /etc/taos"]
        results_custom  = [RESULT_SUCCESS, "Config Dir   : /tmp/taos_cfg_test"]

        # short form with default dir
        self.clearPath(tmpdir)
        rlist = etool.taosbackup(f"-c /etc/taos -D {db} -o {tmpdir}")
        self.checkManyString(rlist, results_default)

        # long form with default dir
        self.clearPath(tmpdir)
        rlist = etool.taosbackup(f"--config-dir=/etc/taos -D {db} -o {tmpdir}")
        self.checkManyString(rlist, results_default)

        # custom path (dir need not exist for the summary check — taos_options
        # accepts any path string; connection still uses the real /etc/taos config)
        self.clearPath(tmpdir)
        rlist = etool.taosbackup(f"-c /tmp/taos_cfg_test -D {db} -o {tmpdir}", retFail=False)
        self.checkManyString(rlist, results_custom)

    def checkConnMode(self, db, tmpdir):
        """Test connection mode priority: cmd option > env variable."""
        taosbackup = etool.taosBackupFile()
        results = [RESULT_SUCCESS]

        # env=invalid port 6043, cmd=valid 6041 -> should use cmd
        os.environ["TDENGINE_CLOUD_DSN"] = "http://127.0.0.1:6043"
        self.clearPath(tmpdir)
        rlist = etool.taosbackup(f"-X http://127.0.0.1:6041 -D {db} -o {tmpdir}")
        self.checkManyString(rlist, results)

        # env=valid 6041, no cmd -> should use env
        os.environ["TDENGINE_CLOUD_DSN"] = "http://127.0.0.1:6041"
        self.clearPath(tmpdir)
        rlist = etool.taosbackup(f"-D {db} -o {tmpdir}")
        self.checkManyString(rlist, results)

        # no env, cmd=valid 6041
        os.environ["TDENGINE_CLOUD_DSN"] = ""
        self.clearPath(tmpdir)
        rlist = etool.taosbackup(f"-X http://127.0.0.1:6041 -D {db} -o {tmpdir}")
        self.checkManyString(rlist, results)

        # cleanup env
        os.environ["TDENGINE_CLOUD_DSN"] = ""

    def checkVersion(self):
        """Check -V version output format."""
        rlist = etool.taosbackup("-V")
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
           - -g debug mode (backup succeeds with richer output)
        4. Test invalid commandline arguments (all errors occur at parse time):
           - Invalid driver (-Z invalid)
           - Invalid format (-F unknown)
           - Invalid stmt-version value (-v 99)
           - Missing output path
           - Restore-only options used with -o: -B, -v, -W
           - Backup-only options used with -i: -S, -E
           - -B below minimum (0, -1) and above global max (100001)
           - -B exceeds STMT2 max (16385 with -v 2 or default)
           - -T 0, -m 0 (thread count must be >= 1)
           - -P out of range (0, 65536, 99999) — parse-time check
        5. Test connection mode priority: cmd > env variable (TDENGINE_CLOUD_DSN)
        6. Test -B/-v boundary values for restore:
           - STMT2: B=1 (min), B=10000 (default), B=16384 (max)
           - STMT1: B=1 (min), B=60000 (default), B=100000 (max)
        7. Test -c / --config-dir parameter:
           - short form (-c /etc/taos) accepted, shown in Config Dir summary line
           - long form (--config-dir=/etc/taos) works identically
           - custom path reflected in summary output

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-04 Migrated and adapted from 04-Taosdump/test_taosdump_commandline.py
            - 2026-03-06 Added -g debug mode to basicCommandLine checks
            - 2026-03-16 Added -B/-v boundary tests and extended error validation
            - 2026-03-31 Added -c/--config-dir parameter tests (step 7)

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

        # 6. -B/-v boundary values for restore (STMT2 and STMT1)
        self.checkDataBatch(db, jsonFile, tmpdir)
        tdLog.info("6. data-batch -B/-v boundary restore tests ......... [Passed]")

        # 7. -c / --config-dir parameter
        self.checkConfigDir(db, tmpdir)
        tdLog.info("7. config-dir -c/--config-dir tests ................. [Passed]")

    def test_taosbackup_all_databases(self):
        """taosBackup backup without -D to exercise getAllDatabases() in backup.c

        When no -D flag is provided, backupMain() calls getAllDatabases() which
        issues SHOW DATABASES and iterates all non-system databases.  This
        covers ~45 lines in backup.c that are never reached by -D tests,
        including the capacity-doubling realloc path.

        Steps:
          1. Insert a small dataset into cmd_alldb.
          2. Backup with no -D (native mode, -T 2).
          3. Verify SUCCESS in output.
          4. Restore cmd_alldb into cmd_alldb_r and verify row count.

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-24 Alex Duan Created to cover backup.c getAllDatabases()

        """
        taosbackup, benchmark, tmpdir = self.findPrograme()
        db     = "cmd_alldb"
        dst_db = "cmd_alldb_r"

        tdLog.info("=== step 1: insert small dataset ===")
        ret = self.exec(f"{benchmark} -d {db} -t 4 -n 500 -y")
        if ret != 0:
            tdLog.exit(f"taosBenchmark failed (ret={ret})")

        tdLog.info("=== step 2: backup ALL databases (no -D) ===")
        self.clearPath(tmpdir)
        # retFail=False, checkRun=False: allow partial failures from stale DBs in the environment;
        # correctness is verified by a successful restore of cmd_alldb in step 3/4.
        rlist = etool.taosbackup(f"-Z native -T 2 -o {tmpdir}", checkRun=False, retFail=False)
        tdLog.info("backup all-dbs done (partial failures tolerated)")

        tdLog.info("=== step 3: restore cmd_alldb ===")
        tdSql.execute(f"drop database if exists {dst_db}")
        rlist = etool.taosbackup(f'-Z native -W "{db}={dst_db}" -i {tmpdir}')
        self.checkListString(rlist, RESULT_SUCCESS)

        tdLog.info("=== step 4: verify row count ===")
        tdSql.query(f"SELECT count(*) FROM {dst_db}.meters")
        count = tdSql.getData(0, 0)
        if count == 0:
            tdLog.exit("restored table is empty")
        tdSql.execute(f"drop database if exists {dst_db}")
        tdLog.info(f"test_taosbackup_all_databases PASSED (rows={count})")

    def test_taosbackup_rename_separator(self):
        """taosBackup: -W rename supports both '=' and '->' separators, and mixed

        Verifies that the -W (rename) option accepts three separator styles:
          1. '=' separator:  -W "src=dst"
          2. '->' separator: -W "src->dst"
          3. Mixed in multi-db rename: -W "src1=dst1|src2->dst2"

        Each case backs up source database(s), restores with rename, and
        verifies row count and sum(c1) match.

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-31 Alex Duan created

        """
        taosbackup, benchmark, tmpdir = self.findPrograme()

        src = "rn_sep_src"
        rows = 500
        tables = 4

        tdLog.info("=== step 1: insert source data ===")
        ret = self.exec(f"{benchmark} -d {src} -t {tables} -n {rows} -y")
        if ret != 0:
            tdLog.exit(f"taosBenchmark failed (ret={ret})")
        tdSql.query(f"SELECT sum(voltage) FROM {src}.meters")
        src_sum = tdSql.getData(0, 0)
        tdSql.query(f"SELECT count(*) FROM {src}.meters")
        src_count = tdSql.getData(0, 0)
        tdLog.info(f"source: count={src_count} sum(voltage)={src_sum}")

        tdLog.info("=== step 2: backup ===")
        self.clearPath(tmpdir)
        rlist = etool.taosbackup(f"-Z native -D {src} -T 2 -o {tmpdir}")
        self.checkListString(rlist, RESULT_SUCCESS)

        # --- case 1: '=' separator ---
        dst_eq = "rn_sep_eq"
        tdLog.info(f"=== case 1: '=' separator: {src}={dst_eq} ===")
        tdSql.execute(f"drop database if exists {dst_eq}")
        rlist = etool.taosbackup(f'-Z native -W "{src}={dst_eq}" -i {tmpdir}')
        self.checkManyString(rlist, [RESULT_SUCCESS, f"rename database: {src} -> {dst_eq}"])
        tdSql.query(f"SELECT count(*) FROM {dst_eq}.meters")
        tdSql.checkData(0, 0, src_count)
        tdSql.query(f"SELECT sum(voltage) FROM {dst_eq}.meters")
        tdSql.checkData(0, 0, src_sum)
        tdSql.execute(f"drop database if exists {dst_eq}")
        tdLog.info("  case 1 '=' separator ............................ [OK]")

        # --- case 2: '->' separator ---
        dst_arrow = "rn_sep_arrow"
        tdLog.info(f"=== case 2: '->' separator: {src}->{dst_arrow} ===")
        tdSql.execute(f"drop database if exists {dst_arrow}")
        rlist = etool.taosbackup(f'-Z native -W "{src}->{dst_arrow}" -i {tmpdir}')
        self.checkManyString(rlist, [RESULT_SUCCESS, f"rename database: {src} -> {dst_arrow}"])
        tdSql.query(f"SELECT count(*) FROM {dst_arrow}.meters")
        tdSql.checkData(0, 0, src_count)
        tdSql.query(f"SELECT sum(voltage) FROM {dst_arrow}.meters")
        tdSql.checkData(0, 0, src_sum)
        tdSql.execute(f"drop database if exists {dst_arrow}")
        tdLog.info("  case 2 '->' separator ........................... [OK]")

        # --- case 3: mixed '=' and '->' in multi-db rename ---
        # Need a second source database
        src2 = "rn_sep_src2"
        ret = self.exec(f"{benchmark} -d {src2} -t {tables} -n {rows} -y")
        if ret != 0:
            tdLog.exit(f"taosBenchmark failed for {src2} (ret={ret})")
        tdSql.query(f"SELECT sum(voltage) FROM {src2}.meters")
        src2_sum = tdSql.getData(0, 0)
        tdSql.query(f"SELECT count(*) FROM {src2}.meters")
        src2_count = tdSql.getData(0, 0)

        # backup both sources
        self.clearPath(tmpdir)
        rlist = etool.taosbackup(f"-Z native -D {src},{src2} -T 2 -o {tmpdir}")
        self.checkListString(rlist, RESULT_SUCCESS)

        dst_mix1 = "rn_sep_mix1"
        dst_mix2 = "rn_sep_mix2"
        tdLog.info(f"=== case 3: mixed: {src}={dst_mix1}|{src2}->{dst_mix2} ===")
        tdSql.execute(f"drop database if exists {dst_mix1}")
        tdSql.execute(f"drop database if exists {dst_mix2}")
        rlist = etool.taosbackup(
            f'-Z native -W "{src}={dst_mix1}|{src2}->{dst_mix2}" -i {tmpdir}'
        )
        self.checkManyString(rlist, [
            RESULT_SUCCESS,
            f"rename database: {src} -> {dst_mix1}",
            f"rename database: {src2} -> {dst_mix2}",
        ])
        # verify first db
        tdSql.query(f"SELECT count(*) FROM {dst_mix1}.meters")
        tdSql.checkData(0, 0, src_count)
        tdSql.query(f"SELECT sum(voltage) FROM {dst_mix1}.meters")
        tdSql.checkData(0, 0, src_sum)
        # verify second db
        tdSql.query(f"SELECT count(*) FROM {dst_mix2}.meters")
        tdSql.checkData(0, 0, src2_count)
        tdSql.query(f"SELECT sum(voltage) FROM {dst_mix2}.meters")
        tdSql.checkData(0, 0, src2_sum)
        tdSql.execute(f"drop database if exists {dst_mix1}")
        tdSql.execute(f"drop database if exists {dst_mix2}")
        tdLog.info("  case 3 mixed '=' and '->' separator ............. [OK]")

        # cleanup source databases
        tdSql.execute(f"drop database if exists {src}")
        tdSql.execute(f"drop database if exists {src2}")
        tdLog.info("test_taosbackup_rename_separator PASSED")
