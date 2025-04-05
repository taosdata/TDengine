"""
Manage test environment.
管理测试环境.
"""
import os
from logging import Logger
from ..errors import *

try:
    import taos
except:
    pass
from ..components import TaosPy, TaosD, TaosAdapter, TaosBenchmark, PrometheusServer, Container, Agent, CreateTableTool, TaosKeeper

from ..util.remote import Remote


class EnvManager:
    def __init__(self, logger: Logger, run_log_dir: str, opts=None):
        self.env_setting: dict = None
        self._logger: Logger = logger
        self._run_log_dir: str = run_log_dir
        self._tmp_dir: str = os.path.join(self._run_log_dir, "tmp")
        self._remote: Remote = Remote(logger)
        self._opts = opts
        if self._opts is not None:
            self.taosd = TaosD(self._remote, self._run_log_dir, self._opts)
            self.fast_write_param_list = self._opts.fast_write_param_list
            # self.adapter = TaosAdapter(self._remote, self._run_log_dir, self._opts)
        else:
            self.taosd = TaosD(self._remote)
        self.adapter = TaosAdapter(self._remote)
        self.taoskeeper = TaosKeeper(self._remote)
        self.taospy = TaosPy(self._remote)
        self.benchmark = TaosBenchmark(self._remote)
        self.prometheus = PrometheusServer(self._remote)
        self.docker_container = Container(self._remote)
        self.agent = Agent(self._remote)
        self.create_table_tool = CreateTableTool(self._remote)

    def set_initial_env(self, env_setting):
        self.env_setting = env_setting

    def resetNode(self, nodeDict):
        name = nodeDict["name"]
        self._logger.debug("reset %s", name)
        if name == "taospy":
            pass
        elif name == "taosd":
            self.taosd.reset(nodeDict)
        elif name == "taosAdapter":
            self.adapter.reset(nodeDict)
        elif name == "taosBenchmark":
            self.benchmark.reset(nodeDict)
        else:
            pass

    def destroyNode(self, nodeDict):
        name = nodeDict["name"]
        self._logger.debug("destroy %s", name)
        if name == "taospy":
            self.taospy.destroy(nodeDict)
        elif name == "taosd":
            self.taosd.destroy(nodeDict)
        elif name == "taosAdapter":
            self.adapter.destroy(nodeDict)
        elif name == "taosBenchmark":
            self.benchmark.destroy(nodeDict)
        elif name == "prometheus":
            self.prometheus.destroy(nodeDict)
        elif name == "taoskeeper":
            self.taoskeeper.destroy(nodeDict)
        else:
            pass

    def collectNodeData(self, nodeDict, logDir):
        name = nodeDict["name"]
        self._logger.debug("collect %s", name)
        if name == "taospy":
            pass
        elif name == "taosd":
            self.taosd.collect(nodeDict, logDir)
        elif name == "taosAdapter":
            pass
        elif name == "taosBenchmark":
            pass
        elif name == "prometheus":
            pass
        else:
            pass

    def initNode(self, nodeDict):
        name = nodeDict["name"]
        self._logger.debug("init %s", name)
        if name == "taospy":
            pass
        elif name == "taosd":
            self.taosd.init_node(nodeDict)
        elif name == "taosAdapter":
            pass
        elif name == "taosBenchmark":
            pass
        elif name == "prometheus":
            pass
        elif name == "agent":
            pass
        elif name == "create_table_tool":
            pass
        else:
            pass

    def setupNode(self, nodeDict):
        name = nodeDict["name"]
        self._logger.debug("setup %s", name)
        if name == "taospy":
            if "config_dir" in nodeDict["spec"]:
                config_dir = os.path.join(
                    self._run_log_dir, nodeDict["spec"]["config_dir"])
                self.taospy.setup(config_dir, nodeDict)
            else:
                self.taospy.setup(self._run_log_dir, nodeDict)
        elif name == "taosd":
            self.taosd.setup(self._tmp_dir, nodeDict)
        elif name == "taosAdapter":
            self.adapter.setup(self._tmp_dir, nodeDict)
        elif name == "taoskeeper":
            self.taoskeeper.setup(self._tmp_dir, nodeDict)
        elif name == "taosBenchmark":
            self.benchmark.setup(self._tmp_dir, nodeDict)
        elif name == "prometheus":
            self.prometheus.setup(self._tmp_dir, nodeDict)
        elif name == "agent":
            self.agent.setup(nodeDict)
        elif name == "create_table_tool":
            self.create_table_tool.setup(nodeDict)
        else:
            pass

    def launchNode(self, nodeDict):
        name = nodeDict["name"]
        self._logger.debug("launch %s", name)
        if name == "taosd":
            self.taosd.launch(self._tmp_dir, nodeDict)
        elif name == "taosAdapter":
            self.adapter.launch(self._tmp_dir, nodeDict)
        elif name == "taoskeeper":
            self.taoskeeper.launch(self._tmp_dir, nodeDict)
        else:
            pass

    def updateCfg(self, cfg_dict, component, node='all', restart=0):
        self._logger.debug("update config of %s", component)
        cfgOrderDict = {}
        settingList = self.env_setting["settings"]
        for i in range(len(self.env_setting["settings"])):
            cfgOrderDict[self.env_setting["settings"][i]["name"]] = i
            # TODO when self.env_setting["settings"] has mutil-same name (like 2 taosd)
            # cfgOrderDict["taosd"] will as be last taosd index
        tmp_dict = settingList[cfgOrderDict[component]]
        del settingList[cfgOrderDict[component]]
        if component == "taospy":
            self.taospy.update_cfg(self._run_log_dir, tmp_dict, cfg_dict)
        elif component == "taosd":
            self.taosd.update_cfg(self._tmp_dir, tmp_dict,
                                  cfg_dict, node, restart)
        elif component == "taosAdapter":
            self.adapter.update_cfg(tmp_dict, cfg_dict, node, restart)
        elif component == "taosBenchmark":
            self.updateCfg(self._tmp_dir, tmp_dict, cfg_dict)
        settingList.append(tmp_dict)
        self.env_setting["settings"] = settingList

    def getCfg(self, node, component, cfg_dict={}):
        """
        仅支持获取某一个或者全部参数的值
        cfgDict:传入想获取值的字典,只有taosAdapter必须包含完成路径,eg.
        想获取taosd 的logDir,cfgDict={"logDir":""}
        想获取taosAdapter的logDir,cfgDict={"adapter_config":{"log":{"path":""}}}
        """
        self._logger.debug("get config of %s", component)
        if len(cfg_dict.keys()) > 1:
            raise TypeError()
        cfgOrderDict = {}
        settingList = self.env_setting["settings"]
        for i in range(len(self.env_setting["settings"])):
            cfgOrderDict[self.env_setting["settings"][i]["name"]] = i
        try:
            tmp_dict = settingList[cfgOrderDict[component]]
        except:
            return None
        if component == "taospy":
            return self.taospy.get_cfg(tmp_dict, cfg_dict)
        elif component == "taosd":
            return self.taosd.get_cfg(tmp_dict, cfg_dict, node)
        elif component == "taosAdapter":
            return self.adapter.get_cfg(tmp_dict, cfg_dict)
        elif component == "taosBenchmark":
            return self.benchmark.get_cfg(tmp_dict, cfg_dict)
        else:
            return None

    def configure_extra_dnodes(self, dnodeDict, component):
        """
        add an extra dnode for a spec component
        """
        # update env-setting
        Flag = False
        for index, component_tmp in enumerate(self.env_setting["settings"]):

            if component == component_tmp:
                # update
                self.env_setting["settings"][index]['spec']['dnodes'].append(
                    dnodeDict)
                Flag = True
                break
        if not Flag:
            raise ComponentNotFoundException(
                "component %s not found in env_setting" % component['name'])

        self.taosd.configure_extra_dnodes(self._tmp_dir, dnodeDict, component)

    def addDnode(self, endPoint):
        cfgPath = self.getCfg(endPoint, "taosd", {"config_dir": ""})[
            "config_dir"]
        node, _ = endPoint.split(":")
        startCmd = "screen -L -d -m taosd -c {}  ".format(cfgPath)
        self._remote.cmd(
            node, [startCmd])
        createDnode = "create dnode '{0}'".format(endPoint)
        self._remote.cmd(node,
                         ["sleep 0.5", "taos -c {0} -s \"{1}\";".format(cfgPath, createDnode)])

    def dropDnode(self, endPoint):
        """
        this function support drop dnode
        """
        cfgPath = self.getCfg(endPoint, "taosd", {"config_dir": ""})[
            "config_dir"]
        node, _ = endPoint.split(":")
        dropDnode = "drop dnode '{0}'".format(endPoint)
        self._remote.cmd(node,
                         ["sleep 5", "taos -c {0} -s \"{1}\";".format(cfgPath, dropDnode)])

    def startDnode(self, endPoint):
        cfgPath = self.getCfg(endPoint, "taosd", {"config_dir": ""})[
            "config_dir"]
        node, _ = endPoint.split(":")
        startCmd = "screen -L -d -m taosd -c {}  ".format(cfgPath)
        self._remote.cmd(
            node, [startCmd])

    def stopDnode(self, endPoint):
        self._logger.debug("stop dnode %s", endPoint)
        cfgPath = self.getCfg(endPoint, "taosd", {"config_dir": ""})[
            "config_dir"]
        node, _ = endPoint.split(":")
        killCmd = "ps -ef|grep -wi %s | grep -v grep | awk '{print $2}' | xargs kill -9 > /dev/null 2>&1" % cfgPath
        self._remote.cmd(node, [killCmd])

    def kill_process(self, host, process_name):
        killCmd = [
            "ps -ef|grep -wi %s| grep -v grep | awk '{print $2}' | xargs kill -9 > /dev/null 2>&1" % process_name]
        self._remote.cmd(host, killCmd)
