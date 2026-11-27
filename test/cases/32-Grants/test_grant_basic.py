# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
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

from new_test_framework.utils import tdLog, tdSql, tdCom, tdDnodes
from new_test_framework.utils.sqlset import TDSetSql
from new_test_framework.utils.pathFinding import find_proj_root
from decimal import Decimal
import os
import random
import re
import string
import subprocess
import taos
import threading
import time
import shutil
import tempfile

class TestCase:
    _proj_root = find_proj_root(os.path.dirname(os.path.realpath(__file__)))
    dnode1Path = os.path.join(_proj_root, "sim", "dnode1")
    configFile = os.path.join(dnode1Path, "cfg", "taos.cfg")
    hostPath = os.path.join(dnode1Path, "multi")
    localSSPath = os.path.join(_proj_root, "sim", "localSS") # shared storage path for local test
    clientCfgDict = {'debugFlag': 135}
    encryptConfig = {
        "svrKey": "1234567890",
        "dbKey": "1234567890",
        "dataKey": "1234567890",
        "generateConfig": True,
        "generateMeta": True,
        "generateData": True
    }
        
    def setup_cls(cls):
        tdLog.debug("start to execute %s" % __file__)
        cls.setsql = TDSetSql()

    def s0_prepare_test_data(self):
        tdLog.info("======== s0_prepare_test_data: ")
        tdSql.query("select * from information_schema.ins_dnodes;")
        tdSql.execute("create database if not exists audit is_audit 1 allow_drop 1 wal_level 2 vgroups 1 ENCRYPT_ALGORITHM 'SM4-CBC' keep 36500d");
        tdSql.execute("use audit");
        # creating an is_audit database already creates the built-in audit.operations
        # super table (tag: cluster_id varchar), so only the child table is needed here
        tdSql.execute("create table t_operations_abc using operations tags('abc')");
        tdSql.execute("drop database if exists db")
        # grantTest 1 expects 4 vnodes in total: the is_audit db above always has 1 vgroup, so db needs 3
        tdSql.execute("create database if not exists db replica 1 vgroups 3 keep 36500d")
        tdSql.execute("use db")
        tdSql.execute("create table stb0(ts timestamp, c0 int primary key,c1 bigint,c2 int,c3 float,c4 double) tags(t0 bigint unsigned)");
        tdSql.execute("create table ctb0 using stb0 tags(0)");
        tdSql.execute("create snode on dnode 1;")
        tdSql.execute("create stream db.streams1 count_window(1,c0) from db.stb0 partition by tbname into db.result3 as select * from db.stb0");
        tdSql.execute("create topic topic_stb_column as select ts, c3 from stb0");
        tdSql.execute("create topic topic_stb_all as select ts, c1, c2, c3 from stb0");
        tdSql.execute("create topic topic_stb_function as select ts, abs(c1), sin(c2) from stb0");
        tdSql.execute("create view view1 as select * from stb0");
        tdSql.execute("flush database db")
        tdLog.info("prepare test data done")
    
    def getShowGrantsTimeSeries(self, maxRetry=15):
        for nRetry in range(maxRetry):
            tdSql.query("show grants")
            timeseries = tdSql.queryResult[0][5]
            tdSql.query("show grants full")
            full_timeseries = tdSql.queryResult[1][3]
            if timeseries == full_timeseries:
                tdLog.info(f"timeseries: {timeseries}, == full_timeseries: {full_timeseries}, retried: {nRetry}")
                return int(timeseries.split('/')[0])
            else:
                tdLog.info(f"timeseries: {timeseries}, != full_timeseries: {full_timeseries}, retry: {nRetry}") 
                time.sleep(1)
        raise Exception(f"Timeseries not equal within {maxRetry} seconds")

    def getTablesTimeSeries(self, ):
        tdSql.query(f"select cast(sum(columns-1) as int) as tss from information_schema.ins_tables where db_name not in ('information_schema', 'performance_schema', 'audit') and type not like '%VIRTUAL%'")
        return int(tdSql.queryResult[0][0])

    def checkGrantsTimeSeries(self, prompt="", nExpectedTimeSeries=0, strictMode=False, maxRetry=15):
        for nRetry in range(maxRetry):
            tss_grant = self.getShowGrantsTimeSeries()
            if tss_grant == nExpectedTimeSeries:
                if not strictMode:
                    tdLog.info(f"{prompt}: tss_grant: {tss_grant} == nExpectedTimeSeries: {nExpectedTimeSeries}, retry: {nRetry}")
                    return
                tss_table = self.getTablesTimeSeries()
                if tss_grant == tss_table:
                    tdLog.info(f"{prompt}: tss_grant: {tss_grant} == tss_table: {tss_table}")
                    return
                else:
                    raise Exception(f"{prompt}: tss_grant: {tss_grant} != tss_table: {tss_table}")
            tdLog.info(f"{prompt}: tss_grant: {tss_grant} != nExpectedTimeSeries: {nExpectedTimeSeries}, retry: {nRetry}")
            time.sleep(1)
        raise Exception(f"{prompt}: tss_grant: {tss_grant} != nExpectedTimeSeries: {nExpectedTimeSeries}")

    def clearEnv(self):
        if hasattr(self, 'infoPath') and self.infoPath and os.path.exists(self.infoPath):
            os.remove(self.infoPath)

    def s1_check_timeseries(self):
        tdLog.info("======== test timeseries: ")
        tss_grant = 5
        for i in range(0, 3):
            tdLog.printNoPrefix(f"======== test timeseries: loop{i}")
            self.checkGrantsTimeSeries("initial check", tss_grant)
            tdSql.execute("create database if not exists db100 keep 36500d")
            tdSql.execute("create table db100.stb100(ts timestamp, c0 int,c1 bigint,c2 int,c3 float,c4 double) tags(t0 bigint unsigned)")
            tdSql.execute("create table db100.ctb100 using db100.stb100 tags(100)")
            tdSql.execute("create table db100.ctb101 using db100.stb100 tags(101)")
            tdSql.execute("create table db100.ntb100 (ts timestamp, c0 int,c1 bigint,c2 int,c3 float,c4 double)")
            tdSql.execute("create table db100.ntb101 (ts timestamp, c0 int,c1 bigint,c2 int,c3 float,c4 double)")
            tss_grant += 20
            self.checkGrantsTimeSeries("create tables and check", tss_grant)
            tdSql.execute("create vtable db100.vntb100(ts timestamp, v0_0 int from db100.ntb100.c0, v0_1 int from db100.ntb101.c0)")
            self.checkGrantsTimeSeries("create virtual normal tables and check", tss_grant)
            tdSql.execute("create stable db100.vstb100(ts timestamp, c0 int, c1 int) tags(t0 int, t1 varchar(20)) virtual 1")
            tdSql.execute("create vtable db100.vctb100(c0 from db100.ntb100.c0, c1 from db100.ntb101.c0) using db100.vstb100 tags(0, '0')")
            self.checkGrantsTimeSeries("create virtual stb/ctb and check", tss_grant)
            tdSql.execute("alter table db100.stb100 add column c5 int")
            tdSql.execute("alter stable db100.stb100 add column c6 int")
            tdSql.execute("alter table db100.stb100 add tag t1 int")
            tss_grant += 4
            self.checkGrantsTimeSeries("add stable column and check", tss_grant)
            tdSql.execute("alter table db100.vstb100 add column c5 int")
            self.checkGrantsTimeSeries("add virtual stb/ctb column and check", tss_grant)
            tdSql.execute("create table db100.ctb102 using db100.stb100 tags(102, 102)")
            tdSql.execute("alter table db100.ctb100 set tag t0=1000")
            tdSql.execute("alter table db100.ntb100 add column c5 int")
            tss_grant += 8
            self.checkGrantsTimeSeries("add ntable column and check", tss_grant)
            tdSql.execute("alter table db100.vntb100 add column c5 int")
            self.checkGrantsTimeSeries("add virtual ntable column and check", tss_grant)
            tdSql.execute("alter table db100.stb100 drop column c5")
            tdSql.execute("alter table db100.stb100 drop tag t1")
            tdSql.execute("alter table db100.ntb100 drop column c0")
            tdSql.execute("alter table db100.stb100 drop column c0")
            tss_grant -= 7
            self.checkGrantsTimeSeries("drop stb/ntb column and check", tss_grant)
            tdSql.execute("alter table db100.vstb100 drop column c5")
            tdSql.execute("alter table db100.vntb100 drop column c5")
            self.checkGrantsTimeSeries("drop virtual stb/ntb column and check", tss_grant)
            tdSql.execute("drop table db100.ctb100")
            tdSql.execute("drop table db100.ntb100")
            tss_grant -= 10
            self.checkGrantsTimeSeries("drop ctb/ntb and check", tss_grant)
            tdSql.execute("drop table db100.stb100")
            tss_grant -= 10
            self.checkGrantsTimeSeries("drop stb and check", tss_grant)
            tdSql.execute("drop table db100.vctb100")
            tdSql.execute("drop table db100.vntb100")
            self.checkGrantsTimeSeries("drop virtual ctb/ntb and check", tss_grant)
            tdSql.execute("drop table db100.vstb100")
            self.checkGrantsTimeSeries("drop virtual stb and check", tss_grant)
            tdSql.execute("drop database db100")
            tss_grant -= 5
            self.checkGrantsTimeSeries("drop database and check", tss_grant)

    @staticmethod
    def _toEpoch(val):
        # ins_cluster timestamp columns may come back as datetime or as string,
        # depending on the connector; normalize both to epoch seconds.
        if hasattr(val, "timestamp"):
            return val.timestamp()
        return time.mktime(time.strptime(str(val).split('.')[0], "%Y-%m-%d %H:%M:%S"))

    def s1_0_check_default_grant_expire(self):
        tdLog.printNoPrefix("======== test default grant expire time: ")
        # The default (ungranted) license expires 10 days after cluster creation.
        # Allow generous redundancy (7~30 days) to tolerate rounding/service delays,
        # but reject clearly wrong values such as ~1 year.
        DAY = 86400
        tdSql.query("select create_time, expire_time from information_schema.ins_cluster;")
        tdSql.checkRows(1)
        createEpoch = self._toEpoch(tdSql.queryResult[0][0])
        expireEpoch = self._toEpoch(tdSql.queryResult[0][1])
        deltaDays = (expireEpoch - createEpoch) / DAY
        tdLog.info(f"create_time: {tdSql.queryResult[0][0]}, expire_time: {tdSql.queryResult[0][1]}, "
                   f"delta: {deltaDays:.3f} days")
        if not (7 <= deltaDays <= 30):
            raise Exception(f"default grant expire delta {deltaDays:.3f} days out of expected 7~30 days "
                            f"(cluster create_time + ~10 days)")
        # Also confirm show grants agrees: expire_time == service_time + 10 days.
        tdSql.query("show grants;")
        tdSql.checkRows(1)
        expireTime = time.mktime(time.strptime(tdSql.queryResult[0][1], "%Y-%m-%d %H:%M:%S"))
        serviceTime = time.mktime(time.strptime(tdSql.queryResult[0][2], "%Y-%m-%d %H:%M:%S"))
        tdSql.checkEqual(True, abs(expireTime - serviceTime - 10 * DAY) < 15)
        tdLog.info("default grant expire time check passed (~10 days from cluster creation)")

    def genClusterInfo(self, check=False):
        tdLog.info("======== genClusterInfo: ")
        self.infoPath = os.path.join(self.workPath, ".clusterInfo")
        infoFile = open(self.infoPath, "w")
        try:
            tdSql.query(f'select create_time,expire_time,version from information_schema.ins_cluster;')
            tdSql.checkEqual(len(tdSql.queryResult), 1)
            infoFile.write(";".join(map(str, tdSql.queryResult[0])) + "\n")
            tdSql.query(f'show cluster machines;')
            tdSql.checkEqual(len(tdSql.queryResult), 1)
            infoFile.write(";".join(map(str,tdSql.queryResult[0])) + "\n")
            tdSql.query(f'show grants;')
            tdSql.checkEqual(len(tdSql.queryResult), 1)
            infoFile.write(";".join(map(str,tdSql.queryResult[0])) + "\n")
            if check:
                expireTimeStr=tdSql.queryResult[0][1]
                serviceTimeStr=tdSql.queryResult[0][2]
                tdLog.info(f"expireTimeStr: {expireTimeStr}, serviceTimeStr: {serviceTimeStr}")
                expireTime = time.mktime(time.strptime(expireTimeStr, "%Y-%m-%d %H:%M:%S"))
                serviceTime = time.mktime(time.strptime(serviceTimeStr, "%Y-%m-%d %H:%M:%S"))
                tdLog.info(f"expireTime: {expireTime}, serviceTime: {serviceTime}")
                tdSql.checkEqual(True, abs(expireTime - serviceTime - 864000) < 15)
                tdSql.query(f'show grants full;')
                nGrantItems = 52
                tdSql.checkRows(nGrantItems)
                tdSql.checkEqual(tdSql.queryResult[0][2], serviceTimeStr)
                for i in range(1, nGrantItems):
                    tdSql.checkEqual(tdSql.queryResult[i][2], expireTimeStr)
            if infoFile:
                infoFile.flush()

        except Exception as e:
            raise Exception(repr(e))
        finally:
            if infoFile:
                infoFile.close()

    def _run_grant_test(self, *args):
        command = [os.path.join(self.workPath, "grantTest"), *args]
        if os.name != "nt":
            return subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # The current Windows stub locates .clusterInfo by truncating cwd at
        # "tsdb" and appending debug/build/bin. Give it a private matching
        # layout while keeping the actual cluster and its generated metadata.
        with tempfile.TemporaryDirectory(prefix="td-grant-") as temporary:
            legacy_root = os.path.join(temporary, "tsdb")
            legacy_bin = os.path.join(legacy_root, "debug", "build", "bin")
            os.makedirs(legacy_bin)
            shutil.copy2(self.infoPath, os.path.join(legacy_bin, ".clusterInfo"))
            return subprocess.run(command, cwd=legacy_root,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def s2_check_show_grants_ungranted(self):
        tdLog.printNoPrefix("======== test show grants ungranted: ")
        self.infoPath = os.path.join(self.workPath, ".clusterInfo")
        try:
            self.genClusterInfo(check=True)
            files_and_dirs = os.listdir(f'{self.workPath}')
            print(f"files_and_dirs: {files_and_dirs}")
            process = self._run_grant_test()
            output, error = process.stdout, process.stderr
            output = output.decode(encoding="utf-8")
            error = error.decode(encoding="utf-8")
            print(f"code: {process.returncode}")
            print(f"error:\n{error}")
            tdSql.checkEqual(process.returncode, 0)
            tdSql.checkEqual(error, "")
            lines = output.splitlines()
            for line in lines:
                if line.startswith("code:"):
                    fields  = line.split(":")
                    tdSql.error(f"{fields[2]}", int(fields[1]), fields[3])
        except Exception as e:
            self.clearEnv()
            raise Exception(repr(e))

    def s3_check_show_grants_granted(self):
        tdLog.printNoPrefix("======== test show grants granted: ")
        try:
            process = self._run_grant_test('1')
            output, error = process.stdout, process.stderr
            output = output.decode(encoding="utf-8")
            error = error.decode(encoding="utf-8")
            print(f"code: {process.returncode}")
            print(f"error:\n{error}")
            print(f"output:\n{output}")
            tdSql.checkEqual(process.returncode, 0)
        except Exception as e:
            self.clearEnv()
            raise Exception(repr(e))

    def _prepare_dual_replica_drop_target(self):
        statement = "select `id`, `role`, `status` from information_schema.ins_mnodes"
        rows = tdSql.query(statement, row_tag=True)
        if not any(row[0] == 3 and row[1] == "leader" for row in rows):
            return
        expected_ids = {row[0] for row in rows}
        if len(expected_ids) < 3:
            raise RuntimeError(f"Cannot prepare grant stub drop dnode 3 with this mnode topology: {rows!r}")

        def wait_for_topology(predicate):
            deadline = time.monotonic() + 60
            observed = []
            while time.monotonic() < deadline:
                observed = tdSql.query(statement, row_tag=True, queryTimes=1, exit=False) or []
                if predicate(observed):
                    return
                time.sleep(1)
            raise TimeoutError(f"Grant stub drop dnode 3 topology did not become ready: {observed!r}")

        tdLog.info("Grant dual-replica stub drops dnode 3; moving mnode leadership before invoking it")
        try:
            tdDnodes.stop(3)
            wait_for_topology(lambda current: any(
                row[0] != 3 and row[1] == "leader" and row[2] == "ready" for row in current))
        finally:
            tdDnodes.startWithoutSleep(3)
        wait_for_topology(lambda current: (
            {row[0] for row in current} == expected_ids
            and sum(row[1] == "leader" for row in current) == 1
            and all(row[1] in ("leader", "follower") and row[2] == "ready" for row in current)
            and any(row[0] == 3 and row[1] == "follower" for row in current)))

    def s4_ts6191_check_dual_replica(self):
        tdLog.printNoPrefix("======== test dual replica: ")
        try:
            tdSql.query("select * from information_schema.ins_dnodes;")
            tdSql.checkRows(5)
            for i in range(4, 6):
                tdSql.execute(f"drop dnode {i}")
            tdSql.query("select * from information_schema.ins_dnodes;")
            tdSql.checkRows(3)

            self._prepare_dual_replica_drop_target()
            self.genClusterInfo(check=False)
            process = self._run_grant_test('2')
            output, error = process.stdout, process.stderr
            output = output.decode(encoding="utf-8")
            error = error.decode(encoding="utf-8")
            print(f"code: {process.returncode}")
            print(f"error:\n{error}")
            print(f"output:\n{output}")
            tdSql.checkEqual(process.returncode, 0)
        except Exception as e:
            self.clearEnv()
            raise Exception(repr(e))

    def s6_check_free_edition(self):
        tdLog.printNoPrefix("======== test free license: ")
        # Driven by grantTest 3. The lic-free- prefix match lives on the CLS response path and
        # is not reachable from a test binary, so the stub sets the license-tier flag directly
        # the way the CLS handler would once the prefix matched, and asserts the behaviour that
        # follows from it: "free" shows up in all three tier-visible outputs (ins_cluster.version,
        # SHOW CLUSTER, SHOW GRANTS) and stays there after expiry, the granted time-series figure
        # is echoed back rather than replaced by taosd, neither table creation nor column addition
        # is rejected above that figure, and applying the licence on an already-over-limit cluster
        # succeeds where a tier-0 licence is refused.
        #
        # Runs last on purpose: it leaves the cluster on an expired license, which would break
        # the timeseries and expire-time checks in the earlier steps.

        # registerEmail defaults to empty. Asserted here rather than inside the stub because it is
        # a once-per-cluster observation: the config framework refuses an empty value (see the
        # stub), so once any address has been written the default can never be observed again, and
        # only this side knows the cluster has not been touched yet.
        tdSql.query("select value from information_schema.ins_dnode_variables "
                    "where name = 'registerEmail' and dnode_id = 1")
        tdSql.checkRows(1)
        tdSql.checkEqual(tdSql.queryResult[0][0], "")
        tdLog.info("registerEmail defaults to empty")

        # grantTest 3 pins the licensed time-series figure to 1 and expects a tier-0 licence to be
        # refused because the cluster already uses more than that. s3 and s5 leave no normal table
        # behind (usage 0), so create one here: 1 ntb = 5 time series.
        tdSql.execute("drop database if exists free_ts_db")
        base = self.getShowGrantsTimeSeries()
        tdSql.execute("create database free_ts_db keep 36500d")
        tdSql.execute("create table free_ts_db.ntb(ts timestamp, c0 int, c1 bigint, c2 int, c3 float, c4 double)")
        self.checkGrantsTimeSeries("free edition prerequisite usage", base + 5)

        try:
            process = self._run_grant_test('3')
            output, error = process.stdout, process.stderr
            output = output.decode(encoding="utf-8")
            error = error.decode(encoding="utf-8")
            print(f"code: {process.returncode}")
            print(f"error:\n{error}")
            print(f"output:\n{output}")
            tdSql.checkEqual(process.returncode, 0)
        except Exception as e:
            self.clearEnv()
            raise Exception(repr(e))

    def s5_run_ext_steps(self):
        """Run the optional extra steps shipped with the enterprise sources, skipped when absent."""
        import importlib.util
        rel = os.path.join("test", "cases", "32-Grants", "grant_ext_steps.py")
        here = os.path.dirname(os.path.realpath(__file__))
        for root in (os.path.join(here, "..", "..", "..", "..", "taos-internal"),
                     os.path.join(self._proj_root, "source", "taos-internal")):
            path = os.path.join(root, rel)
            if os.path.isfile(path):
                spec = importlib.util.spec_from_file_location("grant_ext_steps", path)
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                mod.run(self)
                return
        tdLog.info("no extra steps found, skip")

    def test_grant(self):
        """
        summary: Verify grant-related timeseries statistics and system table exclusion logic

        description:
            This test case mainly verifies the timeseries statistics function related to TDengine grant, focusing on the following aspects:
            1. Whether timeseries statistics are accurate for normal tables, supertables, and virtual tables.
            2. Optional extra steps shipped with the enterprise sources (s5_run_ext_steps).
            4. Consistency of statistics for grant-related commands (show grants, show grants full).
            5. The impact of adding, deleting, and modifying various tables on timeseries statistics.
            6. Prepare dnode3 as a follower before the external dual-replica stub drops it, preserving the original mnode count and grant assertions.

        Since: 2026-03-13

        Labels: audit,grant,log,regression,system-table,timeseries,integration,functional,security

        Jira: 6672169603

        Catalog:
            - Feature: Grant and timeseries statistics
            - Scenario: System table exclusion, grant statistics consistency

        History:
            - 2026-03-13: Initial migration, covering main grant and timeseries statistics process from Cary Xu.
            - 2026-03-13: Added system table exclusion and is_audit database scenarios from Cary Xu.
            - 2026-11-13: replaced the source-parsing exclusion case with optional extra steps (s5_run_ext_steps).
            - 2026-07: Added query_restricted grant item (show grants full now 50 rows) and default grant expire-time check.
            - 2026-09-23: Re-enabled s2/s3/s4 (previously deferred) and added s7 free-edition coverage.
        """
        # keep the order of following steps
        self.workPath = os.path.join(tdCom.getBuildPath(), "build", "bin")
        tdLog.info(self.workPath)
        self.s0_prepare_test_data()
        self.s1_check_timeseries()
        self.s1_0_check_default_grant_expire()
        self.s2_check_show_grants_ungranted()
        self.s3_check_show_grants_granted()
        self.s4_ts6191_check_dual_replica()
        self.s5_run_ext_steps()
        self.s6_check_free_edition()
        self.clearEnv()


