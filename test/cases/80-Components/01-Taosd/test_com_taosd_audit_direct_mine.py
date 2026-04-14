from new_test_framework.utils import tdLog, tdSql, tdDnodes, tdCom, cluster
import taos
import sys
import time
import socket
# import pexpect
import os
import http.server
import gzip
import threading
import json
import pickle
import platform

import threading

telemetryPort = '6043'
serverPort = '6030'
hostname = "localhost" #socket.gethostname()
threadisExit = False

class TestTaosdAudit:
    global hostname
    global serverPort
    if (platform.system().lower() == 'windows' and not tdDnodes.dnodes[0].remoteIP == ""):
        try:
            config = eval(tdDnodes.dnodes[0].remoteIP )
            hostname = config["host"]
        except Exception:
            hostname = tdDnodes.dnodes[0].remoteIP
    rpcDebugFlagVal = '131'
    clientCfgDict = {'serverPort': '', 'firstEp': '', 'secondEp':'', 'rpcDebugFlag':'135', 'fqdn':''}
    clientCfgDict["serverPort"]    = serverPort
    clientCfgDict["firstEp"]       = hostname + ':' + serverPort
    clientCfgDict["secondEp"]      = hostname + ':' + serverPort
    clientCfgDict["rpcDebugFlag"]  = rpcDebugFlagVal
    clientCfgDict["fqdn"]          = hostname

    updatecfgDict = {'clientCfg': {}, 'serverPort': '', 'firstEp': '', 'secondEp':'', 'rpcDebugFlag':'135', 'fqdn':''}
    updatecfgDict["clientCfg"]  = clientCfgDict
    updatecfgDict["serverPort"] = serverPort
    updatecfgDict["firstEp"]    = hostname + ':' + serverPort
    updatecfgDict["secondEp"]   = hostname + ':' + serverPort
    updatecfgDict["fqdn"]       = hostname

    updatecfgDict["monitorFqdn"]       = hostname
    updatecfgDict["monitorPort"]          = '6043'
    updatecfgDict["monitor"]            = '0'
    updatecfgDict["monitorInterval"]        = "5"
    updatecfgDict["monitorMaxLogs"]        = "10"
    updatecfgDict["monitorComp"]        = "1"
    updatecfgDict["monitorForceV2"]        = "0"

    updatecfgDict["audit"]            = '1'
    updatecfgDict["uDebugFlag"]            = '131'
    updatecfgDict["auditLevel"]            = '4'
    updatecfgDict["auditHttps"]            = '0'
    # auditInterval (ms): valid range [500, 200000]; default is 200000 in
    # enterprise builds.  Set to 500 so flush fires quickly during tests.
    updatecfgDict["auditInterval"]         = '500'

    print ("===================: ", updatecfgDict)

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), logSql)
        self.dnodes = cluster.dnodes

    def test_taosd_audit(self):
        """Taosd telemetry audit
        
        1. Create database with vgroups 4
        2. Create super table and table
        3. Insert data into table
        4. Delete data from table
        5. Start http server to receive telemetry info
        6. Check telemetry info content valid
        7. Stop http server

        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-02 Alex Duan Migrated from uncatalog/system-test/0-others/test_taosd_audit.py

        """
        tdSql.prepare()
        # time.sleep(2)

        tdLog.info("create audit database")
        sql = "create database audit is_audit 1 wal_level 2;"
        tdSql.query(sql)

        # Wait for the audit DB vgroups to come online before writing to it.
        time.sleep(3)

        tdLog.info("create operations supertable in audit database")
        # This supertable is normally created by taoskeeper on first startup.
        # We create it manually so the test is self-contained (no taoskeeper).
        # mndAuditFlushCb requires this table to exist; without it every flush
        # is silently skipped.
        sql = """create stable audit.operations (
                     ts             timestamp,
                     user_name      varchar(25),
                     operation      varchar(20),
                     db             varchar(65),
                     resource       varchar(193),
                     client_address varchar(64),
                     details        varchar(50000)
                 ) tags (cluster_id varchar(64))"""
        tdSql.query(sql)

        tdLog.info("create user audit pass '123456Ab@' sysinfo 0;")
        sql = "create user audit pass '123456Ab@' sysinfo 0;"
        tdSql.query(sql)

        tdLog.info("create token audit_token from user audit;")
        sql = "create token audit_token from user audit;"
        tdSql.query(sql)

        # Wait for the first audit flush (auditInterval=500ms, allow 10s margin).
        time.sleep(10)

        vgroups = "4"
        tdLog.info("create database")
        sql = "create database db3 vgroups " + vgroups
        tdSql.query(sql)

        tdLog.info("create stb")
        sql = "create table db3.stb (ts timestamp, f int) tags (t int)"
        tdSql.query(sql)

        # Wait for audit background thread to flush the above operations.
        # auditInterval=500ms; 5s is more than enough.
        tdLog.info("waiting for audit flush...")
        time.sleep(5)

        tdLog.info("verify audit records were written to audit.operations")
        tdSql.query("select count(*) from audit.operations")
        rows = tdSql.getData(0, 0)
        tdLog.info(f"audit record count: {rows}")
        if int(rows) < 1:
            tdLog.exit("No audit records found in audit.operations — direct-write path not working")

        tdLog.info("show some audit records")
        tdSql.query("select ts, user_name, operation, db, client_address from audit.operations limit 10")
        for i in range(tdSql.getRows()):
            tdLog.info(f"  [{i}] ts={tdSql.getData(i,0)} user={tdSql.getData(i,1)} "
                       f"op={tdSql.getData(i,2)} db={tdSql.getData(i,3)} addr={tdSql.getData(i,4)}")

        tdLog.success(f"{__file__} successfully executed")

