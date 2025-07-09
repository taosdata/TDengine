import os
import re

from ..util.file import dict2file
from ..util.remote import Remote
from ..util.common import TDCom

class TaosBenchmark:
    def __init__(self, remote: Remote):
        self._remote = remote

    def install(self, host, version):
        installFlag = False
        result = self._remote.cmd(host, ["which taosBenchmark"])
        if result == "":
            installFlag = True
        else:
            result = self._remote.cmd(host, ["taosBenchmark --version"])
            if not result is None:
                result = result.strip()
                if re.match(f'^{version}$', result) == None:
                    installFlag = True
            else:
                installFlag = True
        if installFlag == True:
            TDCom.download_pkg(version, self._remote, host)
        return

    def install_pkg(self, host, pkg):
        TDCom.install_with_pkg(self._remote, host, pkg)

    def configure(self, tmp_dir, nodeDict):
        config_dir = nodeDict["spec"]["config_dir"]
        dict2file(tmp_dir, "taos.cfg", nodeDict["spec"]["taos_config"])
        for i in nodeDict["fqdn"]:
            self._remote.cmd(i, ["mkdir -p {}".format(config_dir)])
            self._remote.put(i, os.path.join(tmp_dir, "taos.cfg"), config_dir)

    def uninstall(self):
        pass

    def reset(self, nodeDict):
        tmpDict = nodeDict["spec"]
        removeLog = "for i in `find . -name 'taos*'`; do cat /dev/null >$i; done"
        for i in nodeDict["fqdn"]:
            cmdList = []
            for dir in (tmpDict["taos_config"]["logDir"]):
                cmdList.append("cd {};{}".format(dir, removeLog))
            self._remote.cmd(i, cmdList)

    def destroy(self, nodeDict):
        tmpDict = nodeDict["spec"]
        for i in nodeDict["fqdn"]:
            killCmd = [
                "ps -ef|grep -wi %s| grep -v grep | awk '{print $2}' | xargs kill -9 > /dev/null 2>&1" % nodeDict["name"]]
            self._remote.cmd(i, killCmd)
            cmdList = []
            for dir in (tmpDict["config_dir"], tmpDict["taos_config"]["logDir"]):
                cmdList.append("rm -rf {}".format(dir))
            self._remote.cmd(i, cmdList)

    def setup(self, tmp_dir, nodeDict):
        hosts = nodeDict["fqdn"]
        version = nodeDict["spec"]["taos_version"]
        pkg = nodeDict["server_pkg"]
        for host in hosts:
            if pkg is None:
                self.install(host, version)
            else:
                # if package specified, install the package without checking version
                self.install_pkg(host, pkg)
        self.configure(tmp_dir, nodeDict)

    def update_cfg(self, tmp_dir, tmp_dict, cfg_dict, node):
        tmp_dict["spec"]["taos_config"].update(cfg_dict)
        confPath = tmp_dict["spec"]["config_dir"]
        dict2file(tmp_dir, "taos.cfg", tmp_dict["spec"]["taos_config"])
        for i in tmp_dict["fqdn"]:
            if i == node or node == 'all':
                self._remote.put(i, os.path.join(tmp_dir, "taos.cfg"), confPath)

    def get_cfg(self, tmp_dict, cfg_dict):
        if cfg_dict:
            for i in cfg_dict.keys():
                for j in tmp_dict["spec"].keys():
                    if i == j:
                        cfg_dict[i] = tmp_dict["spec"][i]
                        continue
                for j in tmp_dict["spec"]["taos_config"].keys():
                    if i == j:
                        cfg_dict[i] = tmp_dict["spec"]["taos_config"][i]
                        continue
            return cfg_dict
        else:
            return tmp_dict["spec"]
