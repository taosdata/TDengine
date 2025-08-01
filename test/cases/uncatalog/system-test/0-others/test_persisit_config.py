from new_test_framework.utils import tdLog, tdSql, tdCom, tdDnodes
import sys
import taos
import os

class TestPersisitConfig:
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
    
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")
        cls.index = 1

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
            
    def test_persisit_config(self):
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
        buildPath = tdCom.getBuildPath()
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
    
        tdLog.success(f"{__file__} successfully executed")


