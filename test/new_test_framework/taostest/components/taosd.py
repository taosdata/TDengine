from ..util.file import dict2file
from ..util.remote import Remote
from ..util.common import TDCom
from ..frame import *
from threading import Thread
import winrm
import ctypes
import psutil
import os
import time
import re
import platform
import subprocess
try:
    import taos
except:
    pass


class TaosD:
    
    def __init__(self, remote: Remote, run_log_dir=None, opts=None):
        self._remote = remote
        self._run_log_dir = run_log_dir
        self._opts = opts
        self.taosd_valgrind = False
        self.taosc_valgrind = False
        self.reserve_dnode_list = None
        self.record_dnode = None
        self.logger = remote._logger
        if self._opts is not None:
            if self._opts.taosd_valgrind:
                self.taosd_valgrind = True
            if self._opts.taosc_valgrind:
                self.taosc_valgrind = True
        self.run_time = time.strftime("%Y%m%d%H%M%S", time.localtime(time.time()))
        self._local_host = platform.node()
    def check_status(self):
        pass

    def install(self, host, version):
        """
        1. check whether installed correct version.
        2. if not, install new one.
        3. else uninstall old one and install new one.
        """
        installFlag = False
        result = self._remote.cmd(host, ["which taosd"])
        if result == "":
            installFlag = True
        else:
            result = self._remote.cmd(host, ["taos --version"])
            if not result is None:
                result = result.strip()
                if re.match(f'.* {version}$', result) == None:
                    installFlag = True
            else:
                installFlag = True
        if installFlag == True:
            TDCom.download_pkg(version, self._remote, host)
        return

    def install_pkg(self, host, pkg):
        TDCom.install_with_pkg(self._remote, host, pkg)

    def _configure_and_start_windows(self, tmp_dir, dnode, common_cfg):
        cfg = common_cfg.copy()
        cfg["fqdn"], cfg["serverPort"] = dnode["endpoint"].split(":")
        cfg.update(dnode["config"])
        win=winrm.Session(f'http://{cfg["fqdn"]}:5985/wsman',auth=('administrator','tbase125!'))
        tmp_dir=os.path.join(tmp_dir, cfg["fqdn"] + "_" + cfg["serverPort"])
        dict2file(tmp_dir, "taos.cfg", cfg)
        config_dir_win = dnode['config_dir'].replace('/','\\')
        win.run_cmd(f"md {config_dir_win}")
        if "dataDir" in cfg:
            data_dir_win = cfg['dataDir'].replace('/','\\')
            win.run_cmd(f"md {data_dir_win}")
        if "logDir" in cfg:
            log_dir_win = cfg['logDir'].replace('/','\\')
            win.run_cmd(f"md {log_dir_win}")
        self._remote.cmd(self._local_host,f'scp {os.path.join(tmp_dir, "taos.cfg")} administrator@{cfg["fqdn"]}:/{dnode["config_dir"]}/')
        win.run_cmd('sc create taosd binpath= C:/TDengine/taosd.exe type= own start= auto displayname= taosd')
        win.run_cmd('net start taosd')
        
    def _configure_and_start(self, tmp_dir, dnode, common_cfg, index):
        cfg = common_cfg.copy()
        cfg["fqdn"], cfg["serverPort"] = dnode["endpoint"].split(":")
        cfg.update(dnode["config"])
        tmp_dir=os.path.join(tmp_dir, cfg["fqdn"] + "_" + cfg["serverPort"])
        dict2file(tmp_dir, "taos.cfg", cfg)
        self._remote.mkdir(cfg["fqdn"], dnode["config_dir"])
        if self.taosd_valgrind:
            self._remote.mkdir(cfg["fqdn"], "/var/log/valgrind")
        taosd_path = dnode["taosdPath"] if "taosdPath" in dnode else "/usr/bin/taosd"
        error_output = dnode["asanDir"] if "asanDir" in dnode else None
        if "dataDir" in cfg:
            if type(cfg["dataDir"]) == list:
                for dataDir_i in cfg["dataDir"]:
                    self._remote.mkdir(cfg["fqdn"], dataDir_i.split(" ")[0])
            else:
                self._remote.mkdir(cfg["fqdn"], cfg["dataDir"])
        if "logDir" in cfg:
            self._remote.mkdir(cfg["fqdn"], cfg["logDir"])
            self._run_log_dir = cfg["logDir"]
        cfgPath = os.path.join(tmp_dir, "taos.cfg")
        self._remote.put(cfg["fqdn"], cfgPath, dnode["config_dir"])
        createDnode = "show dnodes"
    
        
        # valgrind_cmdline = f"valgrind --log-file=/var/log/valgrind/valgrind_{self.run_time}/valgrind.log --tool=memcheck --leak-check=full --show-reachable=no --track-origins=yes --show-leak-kinds=all -v --workaround-gcc296-bugs=yes"
        valgrind_cmdline = f"valgrind --log-file={self._run_log_dir}/valgrind_taosd.log --tool=memcheck --leak-check=full --show-reachable=no --track-origins=yes --show-leak-kinds=all -v --workaround-gcc296-bugs=yes"
        if dnode["endpoint"] != cfg["firstEP"]:
            createDnode = "create dnode '{0}'".format(dnode["endpoint"])
        if self.taosd_valgrind:
            start_cmd = f"screen -L -d -m {valgrind_cmdline} {taosd_path} -c {dnode['config_dir']}  "
        else:
            if error_output:
                # use ASAN options abort_on_error=1 to generate core dump when error occurs
                asan_options = [
                    "detect_odr_violation=0",
                    "abort_on_error=1",
                ]
                cmds = [
                    'export LD_PRELOAD="$(realpath $(gcc -print-file-name=libasan.so)) '
                    '$(realpath $(gcc -print-file-name=libstdc++.so))"',
                    f'export ASAN_OPTIONS="{":".join(asan_options)}"',
                    f'{taosd_path} -c {dnode["config_dir"]} 2>{error_output}',
                ]
                run_cmd = " && ".join(cmds)
                start_cmd = f"screen -d -m bash -c '{run_cmd}'"
                self._remote.cmd(cfg["fqdn"], ["ulimit -n 1048576", start_cmd])
            else:
                if platform.system().lower() == "windows":
                    start_cmd = f"mintty -h never {taosd_path} -c {dnode['config_dir']}"
                    self._remote.cmd_windows(cfg["fqdn"], [start_cmd])
                else:
                    start_cmd = f"screen -L -d -m {taosd_path} -c {dnode['config_dir']}  "
                    self._remote.cmd(cfg["fqdn"], ["ulimit -n 1048576", start_cmd])
        
        if self.taosd_valgrind == 0:
            time.sleep(0.1)
            if index == 0:
                key = 'from offline to online'
                bkey = bytes(key, encoding="utf8")
                logFile = os.path.join(self._run_log_dir, "taosdlog.0")
                i = 0
                while not os.path.exists(logFile):
                    time.sleep(0.1)
                    i += 1
                    if i > 50:
                        break
                with open(logFile) as f:
                    timeout = time.time() + 10 * 2
                    while True:
                        line = f.readline().encode('utf-8')
                        if bkey in line:
                            break
                        if time.time() > timeout:
                            self.logger.error('wait too long for taosd start')
                            break
                    self.logger.debug("the dnode:%d has been started." % (index))
        else:
            self.logger.debug(
                "wait 10 seconds for the dnode:%d to start." %(index))
            time.sleep(10)

    def configure_and_start(self, tmp_dir, nodeDict):
        threads = []

        # 调试信息，检查 nodeDict["spec"]["dnodes"] 的内容
        self.logger.debug(f"nodeDict['spec']['dnodes']: {nodeDict['spec']['dnodes']}")

        for index, dnode in enumerate(nodeDict["spec"]["dnodes"]):
            common_cfg: dict = nodeDict["spec"]["config"] if 'config' in nodeDict['spec'] else {
            }
            if "system" in dnode.keys() and dnode["system"].lower() == "windows":
                t = Thread(target = self._configure_and_start_windows, args = (tmp_dir, dnode, common_cfg))
                pass
            else:
                #t = Thread(target = self._configure_and_start, args = (tmp_dir, dnode, common_cfg))
                self._configure_and_start(tmp_dir, dnode, common_cfg, index)
            #t.start()
            #threads.append(t)
        #for thread in threads:
            #thread.join()

    def update_taosd(self, nodeDict):
        for dnode in nodeDict["spec"]["dnodes"]:
            if "update_branch" in dnode and "code_dir" in dnode:
                self._remote.cmd(dnode["endpoint"].split(":")[0],
                                [f'cd {dnode["code_dir"]}',
                                "git reset --hard FETCH_HEAD",
                                f'git checkout {dnode["update_branch"]}',
                                f'git pull origin {dnode["update_branch"]}',
                                "mkdir -p debug",
                                "cd debug",
                                "rm -rf *", "cmake .. && make -j 16 && make install"])

    def configure_and_start_reserve_dnode(self, tmp_dir, nodeDict):
        common_cfg: dict = nodeDict["spec"]["config"] if 'config' in nodeDict['spec'] else {}
        self.reserve_dnode_list = nodeDict["spec"]["reserve_dnodes"]
        self.record_dnode = self.reserve_dnode_list[0]
        self.reserve_dnode_list.pop(0)

        self._configure_and_start(tmp_dir, self.record_dnode, common_cfg)

    def configure_and_start_specified_dnode(self, tmp_dir, nodeDict, specified_dnode):
        common_cfg: dict = nodeDict["spec"]["config"] if 'config' in nodeDict['spec'] else {}
        self._configure_and_start(tmp_dir, specified_dnode, common_cfg)

    def init_node(self, nodeDict):
        # read /proc/sys/kernel/core_pattern and create dir
        # mkdir -p $(( cat /proc/sys/kernel/core_pattern ))
        # rm -rf coredump files
        nodeDictList = [nodeDict["spec"]["dnodes"], nodeDict["spec"]["reserve_dnodes"]] if "reserve_dnodes" in nodeDict["spec"] else [nodeDict["spec"]["dnodes"]]
        for dnodeList in nodeDictList:
            for dnode in dnodeList:
                system_platform = dnode["system"] if "system" in dnode.keys() else "linux"
                if system_platform.lower() == "linux":
                    host, port = dnode["endpoint"].split(":")
                    corePattern = self._remote.cmd(host, ["cat /proc/sys/kernel/core_pattern"])
                    if not corePattern is None:
                        dirName = os.path.dirname(corePattern)
                        if dirName.startswith("/"):
                            self._remote.cmd(host, ["mkdir -p {} ".format(dirName)])

    def collect(self, nodeDict, logDir):
        nodeDictList = [nodeDict["spec"]["dnodes"], nodeDict["spec"]["reserve_dnodes"]] if "reserve_dnodes" in nodeDict["spec"] else [nodeDict["spec"]["dnodes"]]
        for dnodeList in nodeDictList:
            for dnode in dnodeList:
                host, port = dnode["endpoint"].split(":")
                if "system" in dnode.keys() and dnode["system"].lower() == "windows":
                    host, port = dnode["endpoint"].split(":")
                    win=winrm.Session(f'http://{host}:5985/wsman',auth=('administrator','tbase125!'))
                    core_dir = f"{logDir}/data/{host}/coredump"
                    os.system(f"mkdir -p {core_dir}")
                    # because the Windows environment executes test cases concurrently, taosd is shared, so when an error occurs, taosd cannot be stopped;  The log files will be occupied by the taosd process when collecting(copy)  logs, so taos and taosd log can't be collected at the same time;
                    # self._remote.cmd_windows(host, ["taskkill -f /im taosd.exe"])         
                    corePattern = self._remote.cmd_windows(host,["ls /C:/TDengine/taosd.dmp"])
                    print(corePattern)
                    if corePattern == "cmd failed code:2":                           
                        self._remote.cmd_windows(host, f"scp -r /C:/TDengine/taosd.dmp root@{self._local_host}:{core_dir}/")
                    # log_dir = f"{logDir}/data/{host}/log"
                    # os.system(f"mkdir -p {log_dir}")
                    # data_dir = f"{logDir}/data/{host}/data"
                    # os.system(f"mkdir -p {data_dir}")
                    # self._remote.cmd_windows(host, f"scp -r /C:/TDengine/log/ root@{self._local_host}:{log_dir}/")
                    # # winrm session that are too fast will failed, so sleep 2s
                    # time.sleep(2)
                    # self._remote.cmd_windows(host, f"scp -r /C:/TDengine/data/ root@{self._local_host}:{data_dir}/")
                else:
                    cfg: dict = dnode["config"] if 'config' in dnode else {}
                    # cat /proc/sys/kernel/core_pattern
                    # scp coredump files to logDir/data/{fqdn}/coredump
                    system_platform = dnode["system"] if "system" in dnode.keys() else "linux"
                    if system_platform.lower() == "linux":
                        coreDir = "{}/data/{}/coredump".format(logDir, host)
                        os.system("mkdir -p {}".format(coreDir))
                        corePattern = self._remote.cmd(host, ["cat /proc/sys/kernel/core_pattern"])
                        if not corePattern is None:
                            dirName = os.path.dirname(corePattern)
                            if dirName.startswith("/"):
                                self._remote.get(host, dirName, coreDir)
                    # default data dir & log dir
                    remoteDataDir = "/var/lib/taos"
                    remoteLogDir = "/var/log/taos"
                    if "dataDir" in cfg:
                        remoteDataDir = cfg["dataDir"]
                    if "logDir" in cfg:
                        remoteLogDir = cfg["logDir"]
                    localLogDir = "{}/data/{}/log".format(logDir, host)
                    os.system("mkdir -p {}".format(localLogDir))
                    localDataDir = "{}/data/{}/data".format(logDir, host)
                    os.system("mkdir -p {}".format(localDataDir))
                    self._remote.cmd(host, ["tar -czf /tmp/log.tar.gz {}".format(remoteLogDir)])
                    self._remote.get(host, "/tmp/log.tar.gz", localLogDir)
                    if self._opts is None:
                        self._remote.cmd(host, ["tar -czf /tmp/data.tar.gz {}".format(remoteDataDir)])
                        self._remote.get(host, "/tmp/data.tar.gz", localDataDir)
                    else:
                        if not self._opts.disable_data_collection:
                            self._remote.cmd(host, ["tar -czf /tmp/data.tar.gz {}".format(remoteDataDir)])
                            self._remote.get(host, "/tmp/data.tar.gz", localDataDir)

    def configure_extra_dnodes(self, tmp_dir, dnodeDict, component):
        '''
        this function support add dnode for a spec component
        '''
        dnode = dnodeDict
        cfg = {}
        cfg["firstEP"] = component['spec']['config']["firstEP"]
        cfg["fqdn"], cfg["serverPort"] = dnode["endpoint"].split(":")
        cfg.update(dnode["config"])
        dict2file(tmp_dir, "taos.cfg", cfg)
        # clean new run_dir of this dnode
        self._remote.delete(cfg["fqdn"], dnode["config_dir"])
        self._remote.mkdir(cfg["fqdn"], dnode["config_dir"])
        if "dataDir" in cfg:
            self._remote.delete(cfg["fqdn"], cfg["dataDir"])
            self._remote.mkdir(cfg["fqdn"], cfg["dataDir"])
        if "logDir" in cfg:
            self._remote.delete(cfg["fqdn"], cfg["logDir"])
            self._remote.mkdir(cfg["fqdn"], cfg["logDir"])
        cfgPath = os.path.join(tmp_dir, "taos.cfg")
        self._remote.put(cfg["fqdn"], cfgPath, dnode["config_dir"])

    def uninstall(self):
        pass

    def reset(self, nodeDict):
        """
        reset log and data
        """
        first_ep = nodeDict["spec"]["config"]["firstEP"]
        host, port = first_ep.split(":")
        conn = taos.connect(host=host, port=int(port))
        self.clean_data(conn)
        for i in range(len(nodeDict["spec"]["dnodes"])):
            sql = f"alter dnode {i + 1} resetlog"
            conn.execute(sql)
        conn.close()

    def clean_data(self, conn):
        result = conn.query("show databases;")
        for row in result:
            dbname = row[0]
            if dbname == 'log':
                continue
            sql = f"drop database {dbname}"
            conn.execute(sql)

    def destroy(self, nodeDict):
        """
        stop taosd, clean data and log.
        TODO: 当前实现会把一个机器上的所有taosd都杀掉,这是不对的,应该只销毁配置文件中指定的那个。
        """

        nodeDictList = [nodeDict["spec"]["dnodes"]]
        if "reserve_dnodes" in nodeDict["spec"]:
            nodeDictList = [nodeDict["spec"]["dnodes"], nodeDict["spec"]["reserve_dnodes"]]
        nodeDictList = [nodeDict["spec"]["dnodes"], nodeDict["spec"]["reserve_dnodes"]] if "reserve_dnodes" in nodeDict["spec"] else [nodeDict["spec"]["dnodes"]]
        for dnodeList in nodeDictList:
            for i in dnodeList:
                fqdn, _ = i["endpoint"].split(":")
                if platform.system().lower() == "windows":
                    if "asanDir" in i:
                        self.logger.info("Windows not support asanDir yet")
                    else:
                        self.logger.debug("destroy taosd on windows")
                        pid = None
                        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                            if ('mintty' in  proc.info['name']
                                and proc.info['cmdline']  # 确保 cmdline 非空
                                and any('taosd' in arg for arg in proc.info['cmdline'])
                            ):
                                self.logger.debug(proc.info)
                                self.logger.debug("Found taosd.exe process with PID: %s", proc.info['pid'])
                                pid = proc.info['pid']
                                #kernel32 = ctypes.windll.kernel32
                                #kernel32.GenerateConsoleCtrlEvent(0, pid)
                                killCmd = f"taskkill /PID {pid} /T /F"
                                #killCmd = "for /f %%a in ('wmic process where \"name='taosd.exe'\" get processId ^| xargs echo ^| awk ^'{print $2}^' ^&^& echo aa') do @(ps | grep %%a | awk '{print $1}' | xargs)"
                                self._remote.cmd_windows(fqdn, [killCmd])

                else:
                    if "asanDir" in i:
                        if fqdn == "localhost":
                            killCmd = ["ps -efww | grep -w %s | grep -v grep | awk '{print $2}' | xargs kill " % nodeDict["name"]]
                            env = os.environ.copy()
                            env.pop('LD_PRELOAD', None)
                            subprocess.run(killCmd, shell=True, text=True, env=env)
                        else:
                            killCmd = ["ps -efww | grep -w %s | grep -v grep | awk '{print $2}' | xargs kill " % nodeDict["name"]]
                            self._remote.cmd(fqdn, killCmd)
                    else:
                        check_cmd = f"ps -efww | grep -wi {nodeDict['name']} | grep {i['config_dir']} | grep -v grep | wc -l"
                        result = self._remote.cmd(fqdn, [check_cmd])
                        if int(result[0].strip()) > 0:
                            killCmd = [
                                f"ps -efww | grep -wi {nodeDict['name']} | grep {i['config_dir']} | grep -v grep | awk '{{print $2}}' | xargs kill -9 > /dev/null 2>&1"]
                            self._remote.cmd(fqdn, killCmd)

                    # if "system" in i.keys() and i["system"].lower() == "darwin":
                    #     stop_service_cmd = "launchctl unload /Library/LaunchDaemons/com.taosdata.taosd.plist"
                    # else:
                    #     stop_service_cmd = "systemctl is-active taosd && systemctl stop taosd || true"
                    # self.logger.debug(f"Executing stop command on {fqdn}: {stop_service_cmd}")
                    # self._remote.cmd(fqdn, [stop_service_cmd])

                    # get_and_kill_cmd = (
                    #     "ps -efww | grep '[t]aosd' | grep -v grep | awk '{print $2}' | "
                    #     "while read pid; do "
                    #     "if kill -0 $pid 2>/dev/null; then "
                    #     "echo Killing taosd process with PID: $pid; "
                    #     "kill -9 $pid; "
                    #     "fi; "
                    #     "done"
                    # )
                    # self.logger.debug(f"Executing get and kill command on {fqdn}: {get_and_kill_cmd}")
                    # output=self._remote.cmd(fqdn, [get_and_kill_cmd])
                    # self.logger.info(f"Kill log on {fqdn}:{output}")
                    
                    if self.taosd_valgrind and not self.taosc_valgrind:
                        killCmd = [
                            "ps -ef|grep -wi valgrind.bin | grep -v grep | awk '{print $2}' | xargs kill -9 > /dev/null 2>&1"]
                    if self.taosc_valgrind:
                        killCmd = [
                            "ps -ef|grep -wi valgrind.bin | grep -v grep | grep -v taostest | awk '{print $2}' | xargs kill -9 > /dev/null 2>&1"]
                        self._remote.cmd(fqdn, killCmd)
                    if self.taosd_valgrind:
                        self._remote.cmd(fqdn, [f'mkdir -p /var/log/valgrind/valgrind_{self.run_time} 2>/dev/null', f'cp -rf {i["config"]["logDir"]}/* /var/log/valgrind/valgrind_{self.run_time} 2>/dev/null'])
                    #cmdList = []
                    #for dir in (i["config_dir"], i["config"]["dataDir"], i["config"]["logDir"]):
                    #    cmdList.append("rm -rf {}".format(dir))
                    #self._remote.cmd(fqdn, cmdList)


    def kill_and_start(self, nodeDict, sleep_seconds=1):
        """
        kill 配置中的第一个 dnode, sleep, 重启
        """
        dnode = nodeDict["spec"]["dnodes"][0]
        taosd_path = dnode["taosdPath"] if "taosdPath" in dnode else "/usr/bin/taosd"
        fqdn, _ = dnode["endpoint"].split(":")
        config_dir = dnode["config_dir"]
        killCmd = "ps ef | grep %s | grep -v grep | awk '{print $1}' | xargs kill -9 " % config_dir
        sleepCmd = "sleep %s" % sleep_seconds
        ulimitCmd = "ulimit -n 1048576"
        startCmd = f"screen -L -d -m {taosd_path} -c {config_dir}"
        self._remote.cmd(fqdn, [killCmd, sleepCmd, ulimitCmd, startCmd])

    def restart(self, dnode, sleep_seconds=1):
        """
        kill taosd instance by dnode end_point ,run remote cmds
        """
        fqdn, _ = dnode["endpoint"].split(":")
        taosd_path = dnode["taosdPath"] if "taosdPath" in dnode else "/usr/bin/taosd"
        config_dir = dnode["config_dir"]
        killCmd = "ps ef | grep %s | grep -v grep | awk '{print $1}' | xargs kill -9 " % config_dir
        sleepCmd = "sleep %s" % sleep_seconds
        ulimitCmd = "ulimit -n 1048576"
        startCmd = f"screen -L -d -m {taosd_path} -c {config_dir}"
        self._remote.cmd(fqdn, [killCmd, sleepCmd, ulimitCmd, startCmd])

    def kill_by_config_dir(self, dnode):
        """
        kill taosd instance by dnode config_dir
        """
        fqdn, _ = dnode["endpoint"].split(":")
        config_dir = dnode["config_dir"]
        killCmd = "ps ef | grep %s | grep -v grep | awk '{print $1}' | xargs kill " % config_dir
        self._remote.cmd(fqdn, [killCmd])

    def kill_by_port(self, endpoint):
        """
        kill taosd instance by port
        """
        fqdn, port = endpoint.split(":")
        killCmd = "netstat -ntlp | grep %s | grep -v grep | awk '{print $7}' | cut -d '/' -f 1 | xargs kill " % port
        self._remote.cmd(fqdn, [killCmd])
    
    def start(self, dnode):
        """
        start taosd instance by dnode end_point ,run remote cmds
        """
        fqdn, _ = dnode["endpoint"].split(":")
        taosd_path = dnode["taosdPath"] if "taosdPath" in dnode else "/usr/bin/taosd"
        config_dir = dnode["config_dir"]
        ulimitCmd = "ulimit -n 1048576"
        startCmd = f"screen -L -d -m {taosd_path} -c {config_dir}"
        self._remote.cmd(fqdn, [ulimitCmd, startCmd])

    def _install(self, host, version, pkg):
        if pkg is None:
            #self.install(host, version)
            self.logger.info("_install ignore ")
        else:
            # if package specified, install the package without checking version
            self.install_pkg(host, pkg)

    def setup(self, tmp_dir, nodeDict):
        hosts = nodeDict["fqdn"]
        version = nodeDict["spec"]["version"]
        pkg = nodeDict["server_pkg"]
        nodeDictList = nodeDict["spec"]["dnodes"]
        if "reserve_dnodes" in nodeDict["spec"]:
            nodeDictList = [nodeDict["spec"]["dnodes"], nodeDict["spec"]["reserve_dnodes"]]
        nodeDictList = [nodeDict["spec"]["dnodes"], nodeDict["spec"]["reserve_dnodes"]] if "reserve_dnodes" in nodeDict["spec"] else [nodeDict["spec"]["dnodes"]]
        for dnodeList in nodeDictList:
            for i in dnodeList:
                fqdn, _ = i["endpoint"].split(":")
                if "system" in i.keys() and i["system"].lower() == "windows":
                    self._remote._logger.debug(f"windows system can't install taosd")
                else :
                    threads = []
                    for host in hosts:
                        t = Thread(target = self._install, args = (host, version, pkg))
                        t.start()
                        threads.append(t)
                    for thread in threads:
                        thread.join()

    def launch(self, tmp_dir, nodeDict):
        self.configure_and_start(tmp_dir, nodeDict)

    def update_cfg(self, tmp_dir, tmp_dict, cfg_dict, node, restart):
        dnode_index = 0
        dnode_list = tmp_dict["spec"]["dnodes"]
        dnode_list_loop = list(dnode_list)
        updateDnodeList = []
        common_cfg: dict = tmp_dict["spec"]["config"] if 'config' in tmp_dict['spec'] else {
        }
        for dnode in dnode_list_loop:
            cfg = common_cfg.copy()
            if dnode["endpoint"] == node or node == 'all':
                dnode["config"].update(cfg_dict)
                updateDnodeList.append(dnode)
                del dnode_list[dnode_index]
                dnode_index -= 1
                cfg["fqdn"], cfg["serverPort"] = dnode["endpoint"].split(":")
                cfg.update(dnode["config"])
                dict2file(tmp_dir, "taos.cfg", cfg)
                cfgPath = os.path.join(tmp_dir, "taos.cfg")
                self._remote.put(cfg["fqdn"],
                                 cfgPath, dnode["config_dir"])
                taosd_path = dnode["taosdPath"] if "taosdPath" in dnode else "/usr/bin/taosd"
                if restart:
                    killCmd = "ps -ef|grep -wi %s | grep -v grep | awk '{print $2}' | xargs kill -9 > /dev/null 2>&1" % (
                        dnode["config_dir"])
                    self._remote.cmd(cfg["fqdn"], [killCmd])
                    self._remote.cmd(
                        cfg["fqdn"], ["ulimit -n 1048576", f"screen -L -d -m {taosd_path} -c {dnode['config_dir']}"])
                    taosd_process_count = self._remote.cmd(cfg["fqdn"], [f"ps -ef | grep taosd | grep -v grep | grep -v sudo | grep -v defunct | wc -l"])
                    if int(taosd_process_count) > 0:
                        ready_count = self._remote.cmd(cfg["fqdn"], [f'taos -s "show dnodes" | grep {cfg["firstEP"]} | grep ready | wc -l'])
                        ready_flag = 0
                        if int(ready_count) == 1:
                            self._remote._logger.info(f'restart {cfg["firstEP"]} success')
                        while int(ready_count) != 1:
                            taosd_process_count = self._remote.cmd(cfg["fqdn"], [f"ps -ef | grep taosd | grep -v grep | grep -v sudo | grep -v defunct | wc -l"])
                            if ready_flag < 10 and int(taosd_process_count) > 0:
                                ready_flag += 0.5
                            else:
                                self._remote._logger.info(f'restart {cfg["firstEP"]} failed')
                                return
                            time.sleep(0.5)
                            ready_count = self._remote.cmd(cfg["fqdn"], [f'taos -s "show dnodes" | grep {cfg["firstEP"]} | grep ready | wc -l'])
                            if int(ready_count) == 1:
                                self._remote._logger.info(f'restart {cfg["firstEP"]} success')

            dnode_index += 1
        tmp_dict["spec"]["dnodes"] = dnode_list + updateDnodeList

    def get_cfg(self, tmp_dict, cfg_dict, node):
        if cfg_dict:
            for i in cfg_dict.keys():
                for j in tmp_dict["spec"].keys():
                    if i == j:
                        cfg_dict[i] = tmp_dict["spec"][i]
                        break
                for j in tmp_dict["spec"]["dnodes"]:
                    if j['endpoint'] == node:
                        for k in j.keys():
                            if i == k:
                                cfg_dict[i] = j[k]
                                break
                        for k in j['config'].keys():
                            if i == k:
                                cfg_dict[i] = j['config'][k]
                                break
            return cfg_dict
        else:
            return tmp_dict["spec"]