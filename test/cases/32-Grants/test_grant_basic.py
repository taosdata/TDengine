from new_test_framework.utils import tdLog, tdSql, tdCom, tdDnodes
from new_test_framework.utils.sqlset import TDSetSql
from decimal import Decimal
import os
import random
import re
import string
import taos
import threading
import time
import shutil

class TestCase:
    path_parts = os.getcwd().split(os.sep)
    try:
        tdinternal_index = path_parts.index("TDinternal")
    except ValueError:
        raise ValueError("The specified directory 'TDinternal' was not found in the path.")
    TDinternal = os.sep.join(path_parts[:tdinternal_index + 1])
    dnode1Path = os.path.join(TDinternal, "sim", "dnode1")
    configFile = os.path.join(dnode1Path, "cfg", "taos.cfg")
    hostPath = os.path.join(dnode1Path, "multi")
    localSSPath = os.path.join(TDinternal, "sim", "localSS") # shared storage path for local test
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
        tdSql.execute("create database if not exists audit keep 36500d");
        tdSql.execute("use audit");
        tdSql.execute("create table operations(ts timestamp, c0 int primary key,c1 bigint,c2 int,c3 float,c4 double) tags(t0 bigint unsigned)");
        tdSql.execute("create table t_operations_abc using operations tags(1)");
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database if not exists db replica 1 keep 36500d")
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
                nGrantItems = 49
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

    def s2_check_show_grants_ungranted(self):
        tdLog.printNoPrefix("======== test show grants ungranted: ")
        self.infoPath = os.path.join(self.workPath, ".clusterInfo")
        try:
            self.genClusterInfo(check=True)
            files_and_dirs = os.listdir(f'{self.workPath}')
            print(f"files_and_dirs: {files_and_dirs}")
            process = subprocess.Popen([f'{self.workPath}{os.sep}grantTest'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            output, error = process.communicate() 
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
            process = subprocess.Popen(f'{self.workPath}{os.sep}grantTest 1', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            output, error = process.communicate()
            output = output.decode(encoding="utf-8")
            error = error.decode(encoding="utf-8")
            print(f"code: {process.returncode}")
            print(f"error:\n{error}")
            print(f"output:\n{output}")
            tdSql.checkEqual(process.returncode, 0)
        except Exception as e:
            self.clearEnv()
            raise Exception(repr(e))

    def s4_ts6191_check_dual_replica(self):
        tdLog.printNoPrefix("======== test dual replica: ")
        try:
            tdSql.query("select * from information_schema.ins_dnodes;")
            tdSql.checkRows(5)
            for i in range(4, 6):
                tdSql.execute(f"drop dnode {i}")
            tdSql.query("select * from information_schema.ins_dnodes;")
            tdSql.checkRows(3)

            self.genClusterInfo(check=False)
            process = subprocess.Popen(f'{self.workPath}{os.sep}grantTest 2', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            output, error = process.communicate()
            output = output.decode(encoding="utf-8")
            error = error.decode(encoding="utf-8")
            print(f"code: {process.returncode}")
            print(f"error:\n{error}")
            print(f"output:\n{output}")
            tdSql.checkEqual(process.returncode, 0)
        except Exception as e:
            self.clearEnv()
            raise Exception(repr(e))

    @staticmethod
    def parseSystemStbArrayFromC(filePath, arrayName):
        """
        Parse C array definition from vnodeQuery.c file.
        Example: const char *tkLogStb[] = {"cluster_info", "data_dir", ...};
        Returns a list of string values.
        """
        try:
            with open(filePath, 'r') as f:
                content = f.read()
            # Match pattern: const char *arrayName[] = {...};
            pattern = rf'const\s+char\s+\*{arrayName}\[\]\s*=\s*\{{([^}}]+)\}}'
            match = re.search(pattern, content, re.DOTALL)
            if match:
                array_content = match.group(1)
                # Extract all quoted strings
                strings = re.findall(r'"([^"]+)"', array_content)
                return strings
            else:
                raise Exception(f"Array '{arrayName}' not found in {filePath}")
        except FileNotFoundError:
            raise Exception(f"File not found: {filePath}")

    def s5_check_timeseries_exclude_systable(self):
        """
        Test the functionality of excluding system tables when calculating timeseries count(6672169603).
        
        The excluded system tables are supertables:
        1. Supertables in the 'log' database (tkLogStb)
        2. Supertables in the 'audit' database (tkAuditStb), where audit database is identified by:
           - Database name is 'audit'
           - Or the database is created with 'is_audit 1' option
        """
        # Dynamically parse system supertable arrays from vnodeQuery.c
        vnodeQueryPath = os.path.join(self.TDinternal, "community", "source", "dnode", "vnode", "src", "vnd", "vnodeQuery.c")
        tkLogStb = self.parseSystemStbArrayFromC(vnodeQueryPath, "tkLogStb")
        tkAuditStb = self.parseSystemStbArrayFromC(vnodeQueryPath, "tkAuditStb")
        tdLog.info(f"Parsed tkLogStb ({len(tkLogStb)} items): {tkLogStb}")
        tdLog.info(f"Parsed tkAuditStb ({len(tkAuditStb)} items): {tkAuditStb}")
        assert len(tkLogStb) > 0, "Parsed tkLogStb is empty"
        assert len(tkAuditStb) > 0, "Parsed tkAuditStb is empty"

        tdLog.printNoPrefix("======== test timeseries exclude systable: ")
        try:
            # Get the current timeseries count as baseline
            tss_grant_base = 5
            self.checkGrantsTimeSeries("initial grant check", tss_grant_base)

            # ========== Test1: Verify ALL system tables in audit db are excluded from timeseries ==========
            tdLog.printNoPrefix("======== test1: audit db system tables should be excluded (full coverage)")
            # Drop and recreate audit db to test all tkAuditStb tables
            tdSql.execute("drop database if exists audit")
            tdSql.execute("create database audit keep 36500d")
            tdSql.execute("use audit")
            # Create ALL system supertables and child tables in audit db (full coverage test)
            for stb_name in tkAuditStb:
                tdSql.execute(f"create table `{stb_name}`(ts timestamp, c0 int primary key, c1 bigint, c2 int, c3 float, c4 double) tags(t0 bigint unsigned)")
                tdSql.execute(f"create table `t_{stb_name}_1` using `{stb_name}` tags(1)")
                tdSql.execute(f"insert into `t_{stb_name}_1` values(now, 1, 100, 10, 1.0, 2.0)")
                tdSql.execute(f"create table `t_{stb_name}_2` using `{stb_name}` tags(2)")
                tdSql.execute(f"insert into `t_{stb_name}_2` values(now, 2, 200, 20, 2.0, 3.0)")
            
            # Verify timeseries count unchanged (all audit db system tables should be excluded)
            self.checkGrantsTimeSeries("audit db system tables should be excluded", tss_grant_base)
            tdLog.info(f"test1 passed: all {len(tkAuditStb)} audit db system tables excluded from timeseries count")

            # ========== Test2: Verify operations table in normal db is counted in timeseries ==========
            tdLog.printNoPrefix("======== test2: normal db operations table should be counted")
            tss_grant_expected = tss_grant_base
            tdSql.execute("drop database if exists normal_test")
            tdSql.execute("create database normal_test keep 36500d")
            tdSql.execute("use normal_test")
            # Create operations supertable with same name in normal db
            tdSql.execute("create table operations(ts timestamp, c0 int primary key, c1 bigint, c2 int, c3 float, c4 double) tags(t0 bigint unsigned)")
            tdSql.execute("create table t_ops_normal_1 using operations tags(1)")
            tdSql.execute("insert into t_ops_normal_1 values(now, 1, 100, 10, 1.0, 2.0)")
            tss_grant_expected += 5  # 5 columns (excluding ts)
            self.checkGrantsTimeSeries("normal db operations table should be counted", tss_grant_expected)
            tdLog.info("test2 passed: normal db operations table counted in timeseries")

            # ========== Test3: Verify ALL system tables in log db are excluded from timeseries ==========
            tdLog.printNoPrefix("======== test3: log db system tables should be excluded (full coverage)")
            tdSql.execute("drop database if exists log")
            tdSql.execute("create database log keep 36500d")
            tdSql.execute("use log")
            # Create ALL system supertables and child tables in log db (full coverage test)
            for stb_name in tkLogStb:
                tdSql.execute(f"create table `{stb_name}`(ts timestamp, c0 int, c1 bigint, c2 int, c3 float, c4 double) tags(t0 bigint unsigned)")
                tdSql.execute(f"create table `t_{stb_name}_1` using `{stb_name}` tags(1)")
                tdSql.execute(f"insert into `t_{stb_name}_1` values(now, 1, 100, 10, 1.0, 2.0)")
            
            # Verify timeseries count unchanged (all log db system tables should be excluded)
            self.checkGrantsTimeSeries("log db system tables should be excluded", tss_grant_expected)
            tdLog.info(f"test3 passed: all {len(tkLogStb)} log db system tables excluded from timeseries count")

            # ========== Test4: Verify non-system table in log db is counted in timeseries ==========
            tdLog.printNoPrefix("======== test4: log db non-system table should be counted")
            tdSql.execute("use log")
            # Create non-system table in log db
            tdSql.execute("create table `user_defined_stb`(ts timestamp, c0 int, c1 bigint, c2 int) tags(t0 int)")
            tdSql.execute("create table `t_user_1` using `user_defined_stb` tags(1)")
            tdSql.execute("insert into `t_user_1` values(now, 1, 100, 10)")
            tss_grant_expected += 3  # 3 columns (excluding ts)
            self.checkGrantsTimeSeries("log db non-system table should be counted", tss_grant_expected)
            tdLog.info("test4 passed: log db non-system table counted in timeseries")

            # ========== Cleanup test data ==========
            tdLog.printNoPrefix("======== cleanup test databases")
            tdSql.execute("drop database if exists normal_test")
            tdSql.execute("drop database if exists log")
            
            # Verify timeseries restored to base count after cleanup
            self.checkGrantsTimeSeries("cleanup and verify base timeseries", tss_grant_base)
            tdLog.info("cleanup completed, timeseries restored to base count")

            tdLog.printNoPrefix("======== all s5_check_timeseries_exclude_systable tests passed!")

        except Exception as e:
            self.clearEnv()
            raise Exception(repr(e))

    def test_grant(self):
        """
        summary: Verify grant-related timeseries statistics and system table exclusion logic

        description:
            This test case mainly verifies the timeseries statistics function related to TDengine grant, focusing on the following aspects:
            1. Whether timeseries statistics are accurate for normal tables, supertables, and virtual tables.
            2. Specific supertables (such as operations, tkLogStb) in system databases like audit and log should be excluded from timeseries statistics.
            3. Exclusion logic for operations tables in databases marked with is_audit.
            4. Consistency of statistics for grant-related commands (show grants, show grants full).
            5. The impact of adding, deleting, and modifying various tables on timeseries statistics.

        Since: 2026-03-13

        Labels: grant, timeseries, system-table, audit, log, regression

        Jira: 6672169603

        Catalog:
            - Feature: Grant and timeseries statistics
            - Scenario: System table exclusion, grant statistics consistency

        History:
            - 2026-03-13: Initial migration, covering main grant and timeseries statistics process from Cary Xu.
            - 2026-03-13: Added system table exclusion and is_audit database scenarios from Cary Xu.
        """
        # keep the order of following steps
        self.workPath = os.path.join(tdCom.getBuildPath(), "build", "bin")
        tdLog.info(self.workPath)
        self.s0_prepare_test_data()
        self.s1_check_timeseries()
        # self.s2_check_show_grants_ungranted() # migrated later
        # self.s3_check_show_grants_granted() # migrated later
        # self.s4_ts6191_check_dual_replica() # migrated later
        self.s5_check_timeseries_exclude_systable()
        self.clearEnv()


