import sys
import taos
import os
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql
from util.dnodes import tdDnodes

class TDTestCase:
    updatecfgDict = {
        "supportVnodes":"1000",
        "tmqMaxTopicNum":"30",
        "maxShellConns":"1000",
        "monitorFqdn":"localhost:9033",
        "tmqRowSize":"1000",
        "ttlChangeOnWrite":"1",
        "compressor":"1",
        "statusInterval":"4",
    }

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
    
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        self.index = 1
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def cli_get_param_value(self, config_name):
        tdSql.query("show dnode 1 variables;")
        for row in tdSql.queryResult:
            if config_name == row[1]:
                tdLog.debug("Found variable '{}'".format(row[0]))
                return row[2]
            
    def cfg(self, option, value):
        cmd = "echo '%s %s' >> %s" % (option, value, self.cfgPath)
        if os.system(cmd) != 0 :
            tdLog.exit(cmd)
            
    def run(self):  
        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)

        cfgPath = buildPath + "/../sim/dnode%d/cfg" % (self.index)
        self.cfgPath = cfgPath

        dataPath = buildPath + "/../sim/dnode%d/data" % (self.index)
        self.dataPath = dataPath

        logPath = buildPath + "/../sim/dnode%d/log" % (self.index)
        self.logPath = logPath

        tdLog.info("start to check cfg value load from cfg file {0}".format(cfgPath))

        for name,expValue in self.updatecfgDict.items():
            actValue = self.cli_get_param_value(name)
            tdLog.debug("Get {} value: {} Expect value: {}".format(name, actValue,expValue))
            assert str(actValue) == str(expValue)

        tdLog.info("rm -rf {0}".format(cfgPath))
        os.system("rm -rf {0}/*".format(cfgPath))

        tdLog.info("rebuilt cfg file {0}".format(cfgPath))
        cfgName = cfgPath+"/taos.cfg"
        os.system("touch {0}".format(cfgName))
        os.system("echo 'fqdn localhost' >> {0}".format(cfgName))
        os.system("echo 'dataDir {0}' >> {1}".format(dataPath, cfgName))
        os.system("echo 'logDir {0}' >> {1}".format(logPath, cfgName))

        tdDnodes.stop(1)
        tdLog.info("restart taosd")
        tdDnodes.start(1)

        tdLog.info("start to check cfg value load from mnd sdb")

        for name,expValue in self.updatecfgDict.items():
            actValue = self.cli_get_param_value(name)
            tdLog.debug("Get {} value: {} Expect value: {}".format(name, actValue,expValue))
            assert str(actValue) == str(expValue)
    
    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())

