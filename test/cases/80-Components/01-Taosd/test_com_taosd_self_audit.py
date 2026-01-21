from new_test_framework.utils import tdLog, tdSql, tdDnodes, cluster
import time
import platform

serverPort = '6030'
hostname = "localhost" #socket.gethostname()

class TestTaosdAudit:
    global hostname
    global serverPort
    if (platform.system().lower() == 'windows' and not tdDnodes.dnodes[0].remoteIP == ""):
        try:
            config = eval(tdDnodes.dnodes[0].remoteIP )
            hostname = config["host"]
        except Exception:
            hostname = tdDnodes.dnodes[0].remoteIP
    clientCfgDict = {'serverPort': '', 'firstEp': '', 'secondEp':'', 'rpcDebugFlag':'135', 'fqdn':''}
    clientCfgDict["serverPort"]    = serverPort
    clientCfgDict["firstEp"]       = hostname + ':' + serverPort
    clientCfgDict["secondEp"]      = hostname + ':' + serverPort
    clientCfgDict["fqdn"]          = hostname

    updatecfgDict = {'clientCfg': {}, 'serverPort': '', 'firstEp': '', 'secondEp':'', 'debugFlag':'131', 'fqdn':''}
    updatecfgDict["clientCfg"]  = clientCfgDict
    updatecfgDict["serverPort"] = serverPort
    updatecfgDict["firstEp"]    = hostname + ':' + serverPort
    updatecfgDict["secondEp"]   = hostname + ':' + serverPort
    updatecfgDict["fqdn"]       = hostname

    updatecfgDict["debugFlag"]            = '131'
    updatecfgDict["vdebugFlag"]            = '143'
    updatecfgDict["ddebugFlag"]            = '131'
    updatecfgDict["mdebugFlag"]            = '131'
    updatecfgDict["rpcdebugFlag"]            = '131'
    updatecfgDict["qDebugFlag"]            = '131'
    updatecfgDict["smaDebugFlag"]            = '131'
    updatecfgDict["stDebugFlag"]            = '131'

    updatecfgDict["audit"]            = '1'
    updatecfgDict["monitorFqdn"]       = hostname
    updatecfgDict["auditLevel"]            = '5'
    updatecfgDict["auditHttps"]            = '0'
    updatecfgDict["auditSaveInSelf"]            = '1'
    updatecfgDict["auditUseToken"]            = '0'
    

    encryptConfig = {
        "svrKey": "sdfsadfasdfasfas",
        "dbKey": "sdfsadfasdfasfas",
        "dataKey": "sdfsadfasdfasfas",
        "generateConfig": True,
        "generateMeta": True,
        "generateData": True

    }

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
        5. Check info content valid
  
        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-1-15 dmchen init

        """
        tdSql.prepare()
        # time.sleep(2)

        tdLog.info("create audit database")
        sql = "create database audit is_audit 1 wal_level 2 vgroups 1 ENCRYPT_ALGORITHM 'SM4-CBC' PRECISION 'ns';"
        tdSql.query(sql)

        tdLog.info("create user audit pass '123456Ab@' sysinfo 0;")
        sql = "create user audit pass '123456Ab@' sysinfo 0;"
        tdSql.query(sql)

        tdLog.info("create token audit_token from user audit;")
        sql = "create token audit_token from user audit;"
        tdSql.query(sql)

        time.sleep(3)

        # 1
        tdLog.info("create user")
        sql = "create user test pass '123456Ab@' sysinfo 0;"
        tdSql.query(sql)

        tdLog.info("check create user")
        sql = "select * from audit.operations where operation = 'createUser' and resource = 'test';"
        tdSql.query(sql)
        tdSql.checkRows(1)

        # 2
        tdLog.info("create stb")
        sql = "create table db.stb (ts timestamp, f int) tags (t int)"
        tdSql.execute(sql, queryTimes = 1)

        tdLog.info("check create stb")
        sql = "select * from audit.operations where operation = 'createStb' and resource = 'stb';"
        tdSql.query(sql)
        tdSql.checkRows(1)

        # 3
        tdLog.info("create tb")
        sql = "create table db.tb using db.stb tags (1)"
        tdSql.execute(sql, queryTimes = 1)

        time.sleep(2)

        tdLog.info("check create tb")
        sql = "select * from audit.operations where operation = 'createTable';"
        tdSql.query(sql)
        tdSql.checkRows(1)

        # 4
        tdLog.info("insert tb")
        sql = "INSERT INTO db.tb USING db.stb TAGS (1) VALUES (NOW, 2);"
        tdSql.execute(sql, queryTimes = 1)

        time.sleep(5)

        tdLog.info("check insert tb")
        sql = "select * from audit.operations where operation = 'insert' and resource = 'tb';"
        tdSql.query(sql)
        tdSql.checkRows(1)

        # 5
        tdLog.info("select stb")
        sql = "SELECT * FROM db.stb;"
        tdSql.query(sql)

        time.sleep(5)

        tdLog.info("check select stb")
        sql = "select * from audit.operations where operation = 'select' and resource = 'stb';"
        tdSql.query(sql)
        tdSql.checkRows(1)

        # 6
        tdLog.info("delete tb")
        sql = "delete from db.tb"
        tdSql.execute(sql, queryTimes = 1)

        time.sleep(5)

        tdLog.info("check delete tb")
        sql = "select * from audit.operations where operation = 'delete' and resource = 'tb';"
        tdSql.query(sql)
        tdSql.checkRows(1)

        tdLog.success(f"{__file__} successfully executed")

