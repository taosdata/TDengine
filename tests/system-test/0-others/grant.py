from ssl import ALERT_DESCRIPTION_CERTIFICATE_UNOBTAINABLE
import taos
import sys
import time
import os

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.dnodes import TDDnodes
from util.dnodes import TDDnode
import time
import socket
import subprocess

class MyDnodes(TDDnodes):
    def __init__(self ,dnodes_lists):
        super(MyDnodes,self).__init__()
        self.dnodes = dnodes_lists  # dnode must be TDDnode instance
        if platform.system().lower() == 'windows':
            self.simDeployed = True
        else:
            self.simDeployed = False

class TDTestCase:
    noConn = True
    def getTDinternalPath():
        path_parts = os.getcwd().split(os.sep)
        try:
            tdinternal_index = path_parts.index("TDinternal")
        except ValueError:
            raise ValueError("The specified directory 'TDinternal' was not found in the path.")
        return os.sep.join(path_parts[:tdinternal_index + 1])
        
    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug(f"start to excute {__file__}")
        self.TDDnodes = None
        self.depoly_cluster(5)
        self.master_dnode = self.TDDnodes.dnodes[0]
        self.host=self.master_dnode.cfgDict["fqdn"]
        conn1 = taos.connect(self.master_dnode.cfgDict["fqdn"] , config=self.master_dnode.cfgDir)
        tdSql.init(conn1.cursor(), True)
        self.TDinternal = TDTestCase.getTDinternalPath()
        self.workPath = os.path.join(self.TDinternal, "debug", "build", "bin")
        tdLog.info(self.workPath)

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files or "taosd.exe" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/build/bin")]
                    break
        return buildPath

    def depoly_cluster(self ,dnodes_nums):

        testCluster = False
        valgrind = 0
        hostname = socket.gethostname()
        dnodes = []
        start_port = 6030
        for num in range(1, dnodes_nums+1):
            dnode = TDDnode(num)
            dnode.addExtraCfg("firstEp", f"{hostname}:{start_port}")
            dnode.addExtraCfg("fqdn", f"{hostname}")
            dnode.addExtraCfg("serverPort", f"{start_port + (num-1)*100}")
            dnode.addExtraCfg("monitorFqdn", hostname)
            dnode.addExtraCfg("monitorPort", 7043)
            dnodes.append(dnode)

        self.TDDnodes = MyDnodes(dnodes)
        self.TDDnodes.init("")
        self.TDDnodes.setTestCluster(testCluster)
        self.TDDnodes.setValgrind(valgrind)

        self.TDDnodes.setAsan(tdDnodes.getAsan())
        self.TDDnodes.stopAll()
        for dnode in self.TDDnodes.dnodes:
            self.TDDnodes.deploy(dnode.index,{})

        for dnode in self.TDDnodes.dnodes:
            self.TDDnodes.starttaosd(dnode.index)

        # create cluster
        for dnode in self.TDDnodes.dnodes[1:]:
            # print(dnode.cfgDict)
            dnode_id = dnode.cfgDict["fqdn"] +  ":" +dnode.cfgDict["serverPort"]
            dnode_first_host = dnode.cfgDict["firstEp"].split(":")[0]
            dnode_first_port = dnode.cfgDict["firstEp"].split(":")[-1]
            cmd = f"{self.getBuildPath()}/build/bin/taos -h {dnode_first_host} -P {dnode_first_port} -s \"create dnode \\\"{dnode_id}\\\"\""
            print(cmd)
            os.system(cmd)

        time.sleep(2)
        tdLog.info(" create cluster done! ")

    def s0_five_dnode_one_mnode(self):
        tdSql.query("select * from information_schema.ins_dnodes;")
        tdSql.checkData(0,1,'%s:6030'%self.host)
        tdSql.checkData(4,1,'%s:6430'%self.host)
        tdSql.checkData(0,4,'ready')
        tdSql.checkData(4,4,'ready')
        tdSql.query("select * from information_schema.ins_mnodes;")
        tdSql.checkData(0,1,'%s:6030'%self.host)
        tdSql.checkData(0,2,'leader')
        tdSql.checkData(0,3,'ready')
        tdSql.error("create mnode on dnode 1;")
        tdSql.error("drop mnode on dnode 1;")
        tdSql.execute("create database if not exists audit keep 36500d");
        tdSql.execute("use audit");
        tdSql.execute("create table operations(ts timestamp, c0 int primary key,c1 bigint,c2 int,c3 float,c4 double) tags(t0 bigint unsigned)");
        tdSql.execute("create table t_operations_abc using operations tags(1)");
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database if not exists db replica 1 keep 36500d")
        tdSql.execute("use db")
        tdSql.execute("create table stb0(ts timestamp, c0 int primary key,c1 bigint,c2 int,c3 float,c4 double) tags(t0 bigint unsigned)");
        tdSql.execute("create table ctb0 using stb0 tags(0)");
        tdSql.execute("create stream streams1 trigger at_once IGNORE EXPIRED 0 IGNORE UPDATE 0  into streamt as select _wstart, count(*) c1, count(c2) c2 , sum(c3) c3 , max(c4) c4 from stb0 interval(10s)");
        tdSql.execute("create topic topic_stb_column as select ts, c3 from stb0");
        tdSql.execute("create topic topic_stb_all as select ts, c1, c2, c3 from stb0");
        tdSql.execute("create topic topic_stb_function as select ts, abs(c1), sin(c2) from stb0");
        tdSql.execute("create view view1 as select * from stb0");
        for r in range(0, 50000, 50):
            tdLog.info(f"insert data {r} into stb0")
            tdSql.query(f"insert into db.ctb0 values(now + %ds, %d, %d, %d, %f, %f)(now + %ds, %d, %d, %d, %f, %f)(now + %ds, %d, %d, %d, %f, %f)(now + %ds, %d, %d, %d, %f, %f)(now + %ds, %d, %d, %d, %f, %f)(now + %ds, %d, %d, %d, %f, %f)(now + %ds, %d, %d, %d, %f, %f)(now + %ds, %d, %d, %d, %f, %f)(now + %ds, %d, %d, %d, %f, %f)(now + %ds, %d, %d, %d, %f, %f)  "%(r, r*2, r*4, r*3, float(r)/39, float(r)/23,r+1, (r+1)*2, (r+1)*4, (r+1)*3, float(r)/139, float(r)/123,r+2, (r+2)*2, (r+2)*4, (r+2)*3, float(r)/239, float(r)/223,r+3, (r+3)*2, (r+3)*4, (r+3)*3, float(r)/339, float(r)/323,r+4, (r+4)*2, (r+4)*4, (r+4)*3, float(r)/439, float(r)/423,r+5, r+5*2, r+5*4, r+5*3, float(r)/539, float(r)/523,r+6, r+6*2, r+6*4, r+6*3, float(r)/639, float(r)/623,r+7, r+7*2, r+7*4, r+7*3, float(r)/739, float(r)/723,r+8, r+8*2, r+8*4, r+8*3, float(r)/839, float(r)/823,r+9, r+9*2, r+9*4, r*3, float(r)/939, float(r)/923))
        tdSql.execute("flush database db")
        
    def getConnection(self, dnode):
        host = dnode.cfgDict["fqdn"]
        port = dnode.cfgDict["serverPort"]
        config_dir = dnode.cfgDir
        return taos.connect(host=host, port=int(port), config=config_dir)
    
    def getShowGrantsTimeSeries(self, maxRetry=10):
        for nRetry in range(maxRetry):
            tdSql.query("show grants")
            timeseries = tdSql.queryResult[0][5]
            tdSql.query("show grants full")
            full_timeseries = tdSql.queryResult[1][3]
            if timeseries == full_timeseries:
                return int(timeseries.split('/')[0])
            else:
                tdLog.info(f"timeseries: {timeseries}, != full_timeseries: {full_timeseries}, retry: {nRetry}") 
                time.sleep(1)
        raise Exception("Timeseries not equal within {maxRetry} seconds")

    def getTablesTimeSeries(self):
        tdSql.query(f"select cast(sum(columns-1) as int) as tss from information_schema.ins_tables where db_name not in ('information_schema', 'performance_schema', 'audit') and type not like '%VIRTUAL%'")
        return int(tdSql.queryResult[0][0])

    def checkGrantsTimeSeries(self, prompt="", nExpectedTimeSeries=0, maxRetry=10):
        for nRetry in range(maxRetry):
            tss_grant = self.getShowGrantsTimeSeries()
            if tss_grant == nExpectedTimeSeries:
                tss_table = self.getTablesTimeSeries()
                if tss_grant == tss_table:
                    tdLog.info(f"{prompt}: tss_grant: {tss_grant} == tss_table: {tss_table}")
                    return
                else:
                    raise Exception(f"{prompt}: tss_grant: {tss_grant} != tss_table: {tss_table}")
            time.sleep(1)
        raise Exception(f"{prompt}: tss_grant: {tss_grant} != nExpectedTimeSeries: {nExpectedTimeSeries}")

    def clearEnv(self):
        if os.path.exists(self.infoPath):
            os.remove(self.infoPath)

    def s1_check_timeseries(self):
        # check cluster alive
        tdLog.printNoPrefix("======== test cluster alive: ")
        tdSql.checkDataLoop(0, 0, 1, "show cluster alive;", 20, 0.5)

        tdSql.query("show db.alive;")
        tdSql.checkData(0, 0, 1)

        # check timeseries
        tss_grant = 9
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
                tdSql.checkEqual(len(tdSql.queryResult), nGrantItems)
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
            process = subprocess.Popen(f'{self.workPath}{os.sep}grantTest', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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

    def s5_check_timeseries_exclude_systable(self):
        """
        Test the functionality of excluding system tables when calculating timeseries count(6672169603).

        The excluded system tables are supertables:
        1. Supertables in the 'log' database (tkLogStb)
        2. Supertables in the 'audit' database (tkAuditStb), where audit database is identified by:
           - Database name is 'audit'
           - Or the database is created with 'is_audit 1' option
        """
        # List of system supertables in the 'log' database
        tkLogStb = [
            "cluster_info", "data_dir", "dnodes_info", "d_info", "grants_info",
            "keeper_monitor", "logs", "log_dir", "log_summary", "m_info",
            "taosadapter_restful_http_request_fail", "taosadapter_restful_http_request_in_flight",
            "taosadapter_restful_http_request_summary_milliseconds", "taosadapter_restful_http_request_total",
            "taosadapter_system_cpu_percent", "taosadapter_system_mem_percent", "temp_dir", "vgroups_info",
            "vnodes_role", "taosd_dnodes_status", "adapter_conn_pool", "taosd_vnodes_info", "taosd_dnodes_metrics",
            "taosd_vgroups_info", "taos_sql_req", "taosd_mnodes_info", "adapter_c_interface", "taosd_cluster_info",
            "taosd_sql_req", "taosd_dnodes_info", "adapter_requests", "taosd_write_metrics", "adapter_status",
            "taos_slow_sql", "taos_slow_sql_detail", "taosd_cluster_basic", "taosd_dnodes_data_dirs",
            "taosd_dnodes_log_dirs", "xnode_agent_activities", "xnode_task_activities", "xnode_task_metrics",
            "taosx_task_csv", "taosx_task_progress", "taosx_task_kinghist", "taosx_task_tdengine2",
            "taosx_task_tdengine3", "taosx_task_opc_da", "taosx_task_opc_ua", "taosx_task_kafka",
            "taosx_task_influxdb", "taosx_task_mqtt", "taosx_task_avevahistorian", "taosx_task_opentsdb",
            "taosx_task_mysql", "taosx_task_postgres", "taosx_task_oracle", "taosx_task_mssql",
            "taosx_task_mongodb", "taosx_task_sparkplugb", "taosx_task_orc", "taosx_task_pulsar", "taosx_task_pspace"
        ]

        # List of system supertables in the 'audit' database
        tkAuditStb = ["operations"]

        tdLog.printNoPrefix("======== test timeseries exclude systable: ")
        try:
            # Get the current timeseries count as baseline
            tss_grant_base = 11 # table + stream result table
            self.checkGrantsTimeSeries("initial grant check", tss_grant_base)

            # ========== Test1: Verify operations table in audit db is excluded from timeseries ==========
            tdLog.printNoPrefix("======== test1: audit db operations table should be excluded")
            # audit db was created in s0_prepare_test_data with operations supertable
            # Verify that operations child tables in audit db are not counted in timeseries
            tdSql.execute("use audit")
            tdSql.execute("insert into t_operations_abc values(now, 1, 100, 10, 1.0, 2.0)")
            tdSql.execute("create table t_operations_def using operations tags(2)")
            tdSql.execute("insert into t_operations_def values(now, 2, 200, 20, 2.0, 3.0)")

            # Verify timeseries count unchanged (audit db operations table should be excluded)
            self.checkGrantsTimeSeries("audit db operations should be excluded", tss_grant_base)
            tdLog.info("test1 passed: audit db operations table excluded from timeseries count")

            # ========== Test2: Verify operations table in is_audit db is excluded from timeseries ==========
            tdLog.printNoPrefix("======== test2: is_audit db operations table should be excluded")
            tdSql.execute("drop database if exists audit_test")
            tdSql.error("create database `log` is_audit 1 wal_level 2 ENCRYPT_ALGORITHM 'SM4-CBC' keep 36500d", expectErrInfo="Invalid database name", fullMatched=False)
            tdSql.execute("create database audit_test is_audit 1 wal_level 2 ENCRYPT_ALGORITHM 'SM4-CBC' keep 36500d")
            tdSql.execute("use audit_test")
            # Create operations supertable and child tables in is_audit db
            tdSql.execute("create table `operations`(ts timestamp, c0 int primary key, c1 bigint, c2 int, c3 float, c4 double) tags(t0 bigint unsigned)")
            tdSql.execute("create table `t_ops_1` using `operations` tags(1)")
            tdSql.execute("insert into `t_ops_1` values(now, 1, 100, 10, 1.0, 2.0)")
            tdSql.execute("create table `t_ops_2` using `operations` tags(2)")
            tdSql.execute("insert into `t_ops_2` values(now, 2, 200, 20, 2.0, 3.0)")

            # Verify timeseries count unchanged (is_audit db operations table should be excluded)
            self.checkGrantsTimeSeries("is_audit db operations should be excluded", tss_grant_base)
            tdLog.info("test2 passed: is_audit db operations table excluded from timeseries count")

            # ========== Test3: Verify non-operations table in is_audit db is counted in timeseries ==========
            tdLog.printNoPrefix("======== test3: is_audit db non-operations table should be counted")
            tdSql.execute("use audit_test")
            # Create non-operations table in is_audit db
            tdSql.execute("create table normal_stb(ts timestamp, c0 int, c1 bigint, c2 int) tags(t0 int)")
            tdSql.execute("create table t_normal_1 using normal_stb tags(1)")
            tdSql.execute("insert into t_normal_1 values(now, 1, 100, 10)")
            tss_grant_expected = tss_grant_base + 3  # 3 columns (excluding ts)
            self.checkGrantsTimeSeries("is_audit db non-operations table should be counted", tss_grant_expected)
            tdLog.info("test3 passed: is_audit db non-operations table counted in timeseries")

            # ========== Test4: Verify operations table in normal db is counted in timeseries ==========
            tdLog.printNoPrefix("======== test4: normal db operations table should be counted")
            tdSql.execute("drop database if exists normal_test")
            tdSql.execute("create database normal_test keep 36500d")
            tdSql.execute("use normal_test")
            # Create operations supertable with same name in normal db
            tdSql.execute("create table operations(ts timestamp, c0 int primary key, c1 bigint, c2 int, c3 float, c4 double) tags(t0 bigint unsigned)")
            tdSql.execute("create table t_ops_normal_1 using operations tags(1)")
            tdSql.execute("insert into t_ops_normal_1 values(now, 1, 100, 10, 1.0, 2.0)")
            tss_grant_expected += 5  # 5 columns (excluding ts)
            self.checkGrantsTimeSeries("normal db operations table should be counted", tss_grant_expected)
            tdLog.info("test4 passed: normal db operations table counted in timeseries")

            # ========== Test5: Verify ALL system tables in log db are excluded from timeseries ==========
            tdLog.printNoPrefix("======== test5: log db system tables should be excluded (full coverage)")
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
            tdLog.info(f"test5 passed: all {len(tkLogStb)} log db system tables excluded from timeseries count")

            # ========== Test6: Verify non-system table in log db is counted in timeseries ==========
            tdLog.printNoPrefix("======== test6: log db non-system table should be counted")
            tdSql.execute("use log")
            # Create non-system table in log db
            tdSql.execute("create table `user_defined_stb`(ts timestamp, c0 int, c1 bigint, c2 int) tags(t0 int)")
            tdSql.execute("create table `t_user_1` using `user_defined_stb` tags(1)")
            tdSql.execute("insert into `t_user_1` values(now, 1, 100, 10)")
            tss_grant_expected += 3  # 3 columns (excluding ts)
            self.checkGrantsTimeSeries("log db non-system table should be counted", tss_grant_expected)
            tdLog.info("test6 passed: log db non-system table counted in timeseries")

            # ========== Cleanup test data ==========
            tdLog.printNoPrefix("======== cleanup test databases")
            tdSql.execute("alter database `audit_test` allow_drop 1")
            tdSql.execute("drop database if exists audit_test")
            tdSql.execute("drop database if exists normal_test")
            tdSql.execute("drop database if exists log")

            # Verify timeseries restored to base count after cleanup
            self.checkGrantsTimeSeries("cleanup and verify base timeseries", tss_grant_base)
            tdLog.info("cleanup completed, timeseries restored to base count")

            tdLog.printNoPrefix("======== all s5_check_timeseries_exclude_systable tests passed!")

        except Exception as e:
            self.clearEnv()
            raise Exception(repr(e))

    def run(self):
        # print(self.master_dnode.cfgDict)
        # keep the order of following steps
        self.s0_five_dnode_one_mnode()
        self.s1_check_timeseries()
        self.s2_check_show_grants_ungranted()
        self.s3_check_show_grants_granted()
        self.s4_ts6191_check_dual_replica()
        self.s5_check_timeseries_exclude_systable()

    def stop(self):
        self.clearEnv()
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
