
from new_test_framework.utils import tdLog, tdSql, tdDnodes, tdCom, tdDnodes, tdCom
import taos
import sys
import time
import socket
import platform
if platform.system().lower() == 'windows':
    import wexpect as taosExpect
else:
    import pexpect as taosExpect
import os


def taos_command (buildPath, key, value, expectString, cfgDir, sqlString='', key1='', value1=''):
    if len(key) == 0:
        tdLog.exit("taos test key is null!")

    if platform.system().lower() == 'windows':
        taosCmd = buildPath + '\\build\\bin\\taos.exe '
        taosCmd = taosCmd.replace('\\','\\\\')
    else:
        taosCmd = buildPath + '/build/bin/taos '
    if len(cfgDir) != 0:
        taosCmd = taosCmd + '-c ' + cfgDir

    taosCmd = taosCmd + ' -' + key
    if len(value) != 0:
        if key == 'p':
            taosCmd = taosCmd + value
        else:
            taosCmd = taosCmd + ' ' + value

    if len(key1) != 0:
        taosCmd = taosCmd + ' -' + key1
        if key1 == 'p':
            taosCmd = taosCmd + value1
        else:
            if len(value1) != 0:
                taosCmd = taosCmd + ' ' + value1

    tdLog.info ("taos cmd: %s" % taosCmd)

    # 修改：Windows下合并stdout和stderr
    if sys.platform.startswith("win"):
        import subprocess
        result = subprocess.run(
            taosCmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            shell=True
        )
        retResult = result.stdout + result.stderr
        tdLog.info("cmd return result:\n%s\n" % retResult)
        tdLog.info("expect string: %s\n" % expectString)
        # 判断期望字符串
        if len(expectString) != 0 and expectString in retResult:
            i = 0
        else:
            i = 1
    else:
        child = taosExpect.spawn(taosCmd, timeout=3)
        #output = child.readline()
        #print (output.decode())
        if len(expectString) != 0:
            i = child.expect([expectString, taosExpect.TIMEOUT, taosExpect.EOF], timeout=40)
        else:
            i = child.expect([taosExpect.TIMEOUT, taosExpect.EOF], timeout=40)
        retResult = child.before.decode()
        print("cmd return result:\n%s\n"%retResult)
    # print(child.after.decode())
    if i == 0:
        print ('taos login success! Here can run sql, taos> ')
        if len(sqlString) != 0:
            child.sendline (sqlString)
            w = child.expect(["Query OK", taosExpect.TIMEOUT, taosExpect.EOF], timeout=20)
            if platform.system().lower() == 'windows':
                retResult = child.before
            else:
                retResult = child.before.decode()
            if w == 0:
                return "TAOS_OK", retResult
            else:
                return "TAOS_FAIL", retResult
        else:
            if key == 'A' or key1 == 'A' or key == 'C' or key1 == 'C' or key == 'V' or key1 == 'V':
                return "TAOS_OK", retResult
            else:
                return  "TAOS_OK", retResult
    else:
        if key == 'A' or key1 == 'A' or key == 'C' or key1 == 'C' or key == 'V' or key1 == 'V':
            return "TAOS_OK", retResult
        else:
            return "TAOS_FAIL", retResult

class TestTaosShellError:
    #updatecfgDict = {'clientCfg': {'serverPort': 7080, 'firstEp': 'trd02:7080', 'secondEp':'trd02:7080'},\
    #                 'serverPort': 7080, 'firstEp': 'trd02:7080'}
    hostname = socket.gethostname()
    if (platform.system().lower() == 'windows' and not tdDnodes.dnodes[0].remoteIP == ""):
        try:
            config = eval(tdDnodes.dnodes[0].remoteIP)
            hostname = config["host"]
        except Exception:
            hostname = tdDnodes.dnodes[0].remoteIP
    serverPort = '7080'
    rpcDebugFlagVal = '143'
    clientCfgDict = {'serverPort': '', 'firstEp': '', 'secondEp':'', 'rpcDebugFlag':'135', 'fqdn':''}
    clientCfgDict["serverPort"]    = serverPort
    clientCfgDict["firstEp"]       = hostname + ':' + serverPort
    clientCfgDict["secondEp"]      = hostname + ':' + serverPort
    clientCfgDict["rpcDebugFlag"]  = rpcDebugFlagVal
    clientCfgDict["fqdn"] = hostname

    updatecfgDict = {'clientCfg': {}, 'serverPort': '', 'firstEp': '', 'secondEp':'', 'rpcDebugFlag':'135', 'fqdn':''}
    updatecfgDict["clientCfg"]  = clientCfgDict
    updatecfgDict["serverPort"] = serverPort
    updatecfgDict["firstEp"]    = hostname + ':' + serverPort
    updatecfgDict["secondEp"]   = hostname + ':' + serverPort
    updatecfgDict["fqdn"] = hostname

    print ("===================: ", updatecfgDict)

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

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

    def test_taos_shell_error(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx
        """
        tdSql.prepare()
        # time.sleep(2)
        tdSql.query("create user testpy pass 'testpy243#@'")

        #hostname = socket.gethostname()
        #tdLog.info ("hostname: %s" % hostname)

        buildPath = tdCom.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        if platform.system().lower() == 'windows':
            cfgPath = buildPath + "\..\sim\psim\cfg"
            cfgPath = cfgPath.replace('\\','\\\\')
        else:
            cfgPath = buildPath + "/../sim/psim/cfg"
        tdLog.info("cfgPath: %s" % cfgPath)

        checkNetworkStatus = ['0: unavailable', '1: network ok', '2: service ok', '3: service degraded', '4: exiting']
        netrole            = ['client', 'server']

        keyDict = {'h':'', 'P':'6030', 'p':'testpy243#@', 'u':'testpy', 'a':'', 'A':'', 'c':'', 'C':'', 's':'', 'r':'', 'f':'', \
                   'k':'', 't':'', 'n':'', 'l':'1024', 'N':'100', 'V':'', 'd':'db', 'w':'30', '-help':'', '-usage':'', '?':''}

        keyDict['h'] = self.hostname
        keyDict['c'] = cfgPath
        keyDict['P'] = self.serverPort

        tdLog.printNoPrefix("================================ parameter: -h wiht error value")
        #newDbName="dbh"
        #sqlString = 'create database ' + newDbName + ';'
        keyDict['h'] = 'abc'
        retCode, retVal = taos_command(buildPath, "h", keyDict['h'], "taos>", keyDict['c'], '')
        if (retCode == "TAOS_FAIL") and ("Unable to establish connection" in retVal):
            tdLog.info("taos -h %s test success"%keyDict['h'])
        else:
            tdLog.exit("taos -h %s fail"%keyDict['h'])

        keyDict['h'] = '\'abc\''
        retCode, retVal = taos_command(buildPath, "h", keyDict['h'], "taos>", keyDict['c'], '')
        if (retCode == "TAOS_FAIL") and ("Unable to establish connection" in retVal):
            tdLog.info("taos -h %s test success"%keyDict['h'])
        else:
            tdLog.exit("taos -h %s fail"%keyDict['h'])

        keyDict['h'] = '3'
        retCode, retVal = taos_command(buildPath, "h", keyDict['h'], "taos>", keyDict['c'], '')
        if (retCode == "TAOS_FAIL") and ("Unable to establish connection" in retVal):
            tdLog.info("taos -h %s test success"%keyDict['h'])
        else:
            tdLog.exit("taos -h %s fail"%keyDict['h'])

        keyDict['h'] = '\'3\''
        retCode, retVal = taos_command(buildPath, "h", keyDict['h'], "taos>", keyDict['c'], '')
        if (retCode == "TAOS_FAIL") and ("Unable to establish connection" in retVal):
            tdLog.info("taos -h %s test success"%keyDict['h'])
        else:
            tdLog.exit("taos -h %s fail"%keyDict['h'])

        tdLog.printNoPrefix("================================ parameter: -P wiht error value")
        #newDbName="dbh"
        #sqlString = 'create database ' + newDbName + ';'
        keyDict['P'] = 'abc'
        retCode, retVal = taos_command(buildPath, "P", keyDict['P'], "taos>", keyDict['c'], '')
        if (retCode == "TAOS_FAIL") and ("Invalid port" in retVal):
            tdLog.info("taos -P %s test success"%keyDict['P'])
        else:
            tdLog.exit("taos -P %s fail"%keyDict['P'])

        keyDict['P'] = '\'abc\''
        retCode, retVal = taos_command(buildPath, "P", keyDict['P'], "taos>", keyDict['c'], '')
        if (retCode == "TAOS_FAIL") and ("Invalid port" in retVal):
            tdLog.info("taos -P %s test success"%keyDict['P'])
        else:
            tdLog.exit("taos -P %s fail"%keyDict['P'])

        keyDict['P'] = '3'
        retCode, retVal = taos_command(buildPath, "P", keyDict['P'], "taos>", keyDict['c'], '')
        if (retCode == "TAOS_FAIL") and ("Unable to establish connection" in retVal):
            tdLog.info("taos -P %s test success"%keyDict['P'])
        else:
            tdLog.exit("taos -P %s fail"%keyDict['P'])

        keyDict['P'] = "3"
        retCode, retVal = taos_command(buildPath, "P", keyDict['P'], "taos>", keyDict['c'], '')
        if (retCode == "TAOS_FAIL") and ("Unable to establish connection" in retVal):
            tdLog.info("taos -P %s test success"%keyDict['P'])
        else:
            tdLog.exit("taos -P %s fail"%keyDict['P'])

        keyDict['P'] = '12ab'
        retCode, retVal = taos_command(buildPath, "P", keyDict['P'], "taos>", keyDict['c'], '')
        if (retCode == "TAOS_FAIL") and ("Unable to establish connection" in retVal):
            tdLog.info("taos -P %s test success"%keyDict['P'])
        else:
            tdLog.exit("taos -P %s fail"%keyDict['P'])

        keyDict['P'] = "12ab"
        retCode, retVal = taos_command(buildPath, "P", keyDict['P'], "taos>", keyDict['c'], '')
        if (retCode == "TAOS_FAIL") and ("Unable to establish connection" in retVal):
            tdLog.info("taos -P %s test success"%keyDict['P'])
        else:
            tdLog.exit("taos -P %s fail"%keyDict['P'])

        tdLog.printNoPrefix("================================ parameter: -f with error sql ")
        pwd=os.getcwd()
        tdLog.info("pwd: %s"%pwd)
        newDbName="dbf"
        sqlFile = f"{os.path.dirname(os.path.realpath(__file__))}/sql.txt"
        sql1 = "echo create database " + newDbName + " > " + sqlFile
        sql2 = "echo use " + newDbName + " >> " + sqlFile
        if platform.system().lower() == 'windows':
            sql3 = "echo create table ntbf (ts timestamp, c binary(40)) no this item >> " + sqlFile
            sql4 = "echo insert into ntbf values (\"2021-04-01 08:00:00.000\", \"test taos -f1\")(\"2021-04-01 08:00:01.000\", \"test taos -f2\") >> " + sqlFile
        else:
            sql3 = "echo 'create table ntbf (ts timestamp, c binary(40)) no this item' >> " + sqlFile
            sql4 = "echo 'insert into ntbf values (\"2021-04-01 08:00:00.000\", \"test taos -f1\")(\"2021-04-01 08:00:01.000\", \"test taos -f2\")' >> " + sqlFile
        sql5 = "echo show databases >> " + sqlFile
        os.system(sql1)
        os.system(sql2)
        os.system(sql3)
        os.system(sql4)
        os.system(sql5)

        keyDict['f'] = f"{os.path.dirname(os.path.realpath(__file__))}/sql.txt"
        retCode, retVal = taos_command(buildPath, "f", keyDict['f'], 'performance_schema', keyDict['c'], '', '', '')
        #print("============ ret code: ", retCode)
        if retCode != "TAOS_OK":
            tdLog.exit("taos -f fail")

        print ("========== check new db ==========")
        tdSql.query("select * from information_schema.ins_databases")
        for i in range(tdSql.queryRows):
            #print ("dbseq: %d, dbname: %s"%(i, tdSql.getData(i, 0)))
            if tdSql.getData(i, 0) == newDbName:
                break
        else:
            tdLog.exit("create db fail after taos -f fail")

        sqlString = "select * from " + newDbName + ".ntbf"
        tdSql.error(sqlString)

        shellCmd = "rm -f " + sqlFile
        os.system(shellCmd)

        keyDict['f'] = f"{os.path.dirname(os.path.realpath(__file__))}/noexistfile.txt"
        retCode, retVal = taos_command(buildPath, "f", keyDict['f'], 'failed to open file', keyDict['c'], '', '', '')
        #print("============ ret code: ", retCode)
        if retCode != "TAOS_OK":
            tdLog.exit("taos -f fail")

        tdSql.query('drop database %s'%newDbName)

        tdLog.printNoPrefix("================================ parameter: -a with error value")
        #newDbName="dba"
        errorPassword = 'errorPassword'
        sqlString = 'create database ' + newDbName + ';'
        retCode, retVal = taos_command(buildPath, "u", keyDict['u'], "taos>", keyDict['c'], sqlString, 'a', errorPassword)
        if retCode != "TAOS_FAIL":
            tdLog.exit("taos -u %s -a %s"%(keyDict['u'], errorPassword))

        tdLog.printNoPrefix("================================ parameter: -p with error value")
        #newDbName="dba"
        keyDict['p'] = 'errorPassword'
        retCode, retVal = taos_command(buildPath, "u", keyDict['u'], "taos>", keyDict['c'], sqlString, 'p', keyDict['p'])
        if retCode == "TAOS_FAIL" and "Authentication failure" in retVal:
            tdLog.info("taos -p %s test success"%keyDict['p'])
        else:
            tdLog.exit("taos -u %s -p %s"%(keyDict['u'], keyDict['p']))


        tdLog.success(f"{__file__} successfully executed")

