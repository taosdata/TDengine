import os
import re
import winrm
from threading import Thread
import platform
import subprocess
from ..util.file import dict2toml, dict2file
from ..util.remote import Remote
from ..util.common import TDCom

class TaosAdapter:
    def __init__(self, remote: Remote):
        self._remote: Remote = remote
        self._tmp_dir = "/tmp"
        self.logger = remote._logger
        self._local_host = platform.node()
    def install(self, host, version):
        installFlag = False
        result = self._remote.cmd(host, ["which taosadapter"])
        if result == "":
            installFlag = True
        else:
            result = self._remote.cmd(host, ["taosadapter --version"])
            if not result is None:
                result = result.strip()
                if re.match(f'.* v{version}-', result) == None:
                    installFlag = True
            else:
                installFlag = True
        if installFlag == True:
            TDCom.download_pkg(version, self._remote, host)
        return

    def install_pkg(self, host, pkg):
        TDCom.install_with_pkg(self._remote, host, pkg)

    def _configure_and_start_windows(self, node, tmp_dir, nodeDict, config_dir, config_file):
        win_adapter=winrm.Session(f'http://{node}:5985/wsman',auth=('administrator','tbase125!'))
        config_dir_win = config_dir.replace('/','\\')
        win_adapter.run_cmd(f"md {config_dir_win}")
        self._remote.cmd(self._local_host,f'scp {os.path.join(tmp_dir, config_file)} administrator@{node}:/{config_dir}/')
        # os.system(f'scp {os.path.join(tmp_dir, config_file)} administrator@{node}:/{config_dir}/')
        win_adapter.run_cmd('sc create taosadapter binpath= C:/TDengine/taosadapter.exe type= own start= auto displayname= taosadapter')
        win_adapter.run_cmd(f'net start taosadapter')

    def _configure_and_start(self, node, tmp_dir, nodeDict, config_dir, config_file):
        self._remote.cmd(node, ["mkdir -p {}".format(config_dir)])
        self._remote.put(node, os.path.join(tmp_dir, config_file), config_dir)
        taosadapter_path = nodeDict["spec"]["taosadapterPath"] if "taosadapterPath" in nodeDict["spec"] else "/usr/bin/taosadapter"
        self._remote.cmd(node, [f"screen -d -m {taosadapter_path} -c {nodeDict['spec']['config_file']}", "sleep 5s"])

    def configure_and_start(self, tmp_dir, nodeDict):
        config_dir, config_file = os.path.split(nodeDict["spec"]["config_file"])
        
        dict2toml(tmp_dir, config_file, nodeDict["spec"]["adapter_config"])
        self.logger.debug(f"nodeDict['spec']['adapter_config']: {nodeDict['spec']['adapter_config']}")
        self.logger.debug(f"nodeDict['spec']: {nodeDict['spec']}")
        # dict2file(tmp_dir, "taos.cfg", nodeDict["spec"]["taos_config"])
        threads = []
        for i in nodeDict["fqdn"]:
            if "system" in nodeDict["spec"] and nodeDict["spec"]["system"].lower() == "windows":
                t = Thread(target = self._configure_and_start_windows, args = (i, tmp_dir, nodeDict, config_dir, config_file))
                pass
            else:
                t = Thread(target = self._configure_and_start, args = (i, tmp_dir, nodeDict, config_dir, config_file))
            t.start()
            threads.append(t)
        for thread in threads:
            thread.join()

    def uninstall(self):
        pass

    def reset(self, nodeDict):
        tmpDict = nodeDict["spec"]
        removeLog = "for i in `find . -name 'taos*'`; do cat /dev/null >$i; done"
        for i in nodeDict["fqdn"]:
            cmdList = []
            for dir in (tmpDict["adapter_config"]["log"]["path"], tmpDict["taos_config"]["logDir"]):
                cmdList.append("cd {};{}".format(dir, removeLog))
            self._remote.cmd(i, cmdList)

    def destroy(self, nodeDict):
        tmpDict = nodeDict["spec"]
        for i in nodeDict["fqdn"]:
            if 'system' in nodeDict['spec'].keys() and nodeDict['spec']['system'].lower() == 'windows':
                win_taosadapter=winrm.Session(f'http://{i}:5985/wsman',auth=('administrator','tbase125!'))
                win_taosadapter.run_cmd("taskkill -f /im taosadapter.exe")
                win_taosadapter.run_cmd("sc delete 'taosdadapter'")
                for dir in (tmpDict["config_file"], tmpDict["adapter_config"]["taosConfigDir"],
                            tmpDict["adapter_config"]["log"]["path"], tmpDict["taos_config"]["logDir"]):
                    dir_win = dir.replace('/','\\')
                    win_taosadapter.run_cmd(f"rd /S /Q {dir_win}")
            else:
                if "asanDir" in tmpDict:
                    if i == "localhost":
                        killCmd = ["ps -ef | grep -w taosadapter | grep -v grep | awk '{print $2}' | xargs kill "]
                        env = os.environ.copy()
                        env.pop('LD_PRELOAD', None)
                        try:
                            subprocess.run(killCmd, shell=True, text=True, env=env)
                        except Exception as e:
                            print(e)
                    else:
                        killCmd = ["ps -ef | grep -w %s | grep -v grep | awk '{print $2}' | xargs kill " % nodeDict["name"]]
                        self._remote.cmd(i, killCmd)
                else:
                    killCmd = [
                        "ps -ef|grep -wi %s| grep -v grep | awk '{print $2}' | xargs kill -9 > /dev/null 2>&1 || true" % nodeDict[
                            "name"]]
                    self._remote.cmd(i, killCmd)
                #cmdList = []
                #for dir in (tmpDict["config_file"], tmpDict["adapter_config"]["taosConfigDir"],
                #            tmpDict["adapter_config"]["log"]["path"], tmpDict["taos_config"]["logDir"]):
                #    cmdList.append("rm -rf {}".format(dir))
                #self._remote.cmd(i, cmdList)

    def _install(self, host, version, pkg):
        if pkg is None:
            self.install(host, version)
        else:
            # if package specified, install the package without checking version
            self.install_pkg(host, pkg)

    def setup(self, tmp_dir, nodeDict):
        hosts = nodeDict["fqdn"]
        version = nodeDict["spec"]["version"]
        pkg = nodeDict["server_pkg"]
        threads = []
        for host in hosts:
            t = Thread(target = self._install, args = (host, version, pkg))
            t.start()
            threads.append(t)
        for thread in threads:
            thread.join()

    def launch(self, tmp_dir, nodeDict):
        self.configure_and_start(tmp_dir, nodeDict)

    def update_cfg(self, tmp_dict, cfg_dict, node, restart):
        config_dir, config_file = os.path.split(tmp_dict["spec"]["config_file"])
        for i in cfg_dict.keys():
            for j in cfg_dict[i].keys():
                if type(cfg_dict[i][j]) == dict:
                    tmp_dict["spec"][i][j].update(cfg_dict[i][j])
                else:
                    tmp_dict["spec"][i][j] = cfg_dict[i][j]
            if i == 'adapter_config':
                dict2toml(self._tmp_dir, config_file,
                          tmp_dict["spec"]["adapter_config"])
            else:
                dict2file(self._tmp_dir, "taos.cfg",
                          tmp_dict["spec"]["taos_config"])
        adapterCfgPath = os.path.join(self._tmp_dir, config_file)
        taosCfgPath = os.path.join(self._tmp_dir, "taos.cfg")
        
        for i in tmp_dict["fqdn"]:
            if i == node or node == 'all':
                self._remote.put(
                    i, adapterCfgPath, config_dir)
                self._remote.put(i, taosCfgPath, config_dir)
                if restart:
                    killCmd = "ps -ef | grep -wi taosAdapter | grep -v grep | awk '{print $2}' | xargs kill -9 > /dev/null 2>&1"
                    self._remote.cmd(i, [killCmd, "screen -d -m taosadapter -c {}  ".format(tmp_dict["spec"]["config_file"]), "sleep 5s"])

    def update_taosa_toml(self, tmp_dict, node, restart):
        config_dir, config_file = os.path.split(tmp_dict["spec"]["config_file"])
        dict2toml(self._tmp_dir, config_file, tmp_dict["spec"]["adapter_config"])
        adapterCfgPath = os.path.join(self._tmp_dir, config_file)
        taosCfgPath = os.path.join(self._tmp_dir, "taos.cfg")
        for i in tmp_dict["fqdn"]:
            if i == node or node == 'all':
                self._remote.put(
                    i, adapterCfgPath, config_dir)
                self._remote.put(i, taosCfgPath, config_dir)
                if restart:
                    killCmd = "ps -ef | grep -wi taosAdapter | grep -v grep | awk '{print $2}' | xargs kill -9 > /dev/null 2>&1"
                    self._remote.cmd(i, [killCmd, "screen -d -m taosadapter -c {}  ".format(tmp_dict["spec"]["config_file"]), "sleep 5s"])

    def get_cfg(self, tmp_dict, cfg_dict):
        if cfg_dict:
            for i, j in cfg_dict.items():
                if type(j) == dict:
                    for k, l in j.items():
                        if type(l) == dict:
                            for m, n in l.items():
                                cfg_dict[i][k][m] = tmp_dict['spec'][i][k][m]
                        else:
                            cfg_dict[i][k] = tmp_dict['spec'][i][k]
                else:
                    cfg_dict[i] = tmp_dict['spec'][i]
            return cfg_dict
        else:
            return tmp_dict["spec"]
