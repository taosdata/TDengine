import requests
import toml
import os
from fabric2 import Connection
from .log import *
from .common import *


class TaosKeeper:
    def __init__(self):
        self.running = 0
        self.deployed = 0
        self.remoteIP = ""
        self.taoskeeper_cfg_dict = {
            "tdengine":{
                "host": "localhost",
                "port": 6041,
                "username": "root",
                "password": "taosdata",
                },
                "port": 6043,
                "taosConfigDir": "",
                "log":{
                    "path": "",
                    "level": "info",
                    "RotationInterval": "15s",
                    "keepDays": 30,
                    "rotationSize": "1GB",
                    "rotationCount": 30
                        },
                "metrics":{
                    "prefix": "taos",
                },
                "metrics.database":{
                    "name": "log",
                },
                "metrics.database.options":{
                    "vgroups": 1,
                    "buffer": 64,
                    "keep": 90,
                    "cachemodel": "both",
                },
                "enviornment":{
                    "incgroup": "false",
                } 
        }
    def init(self, path, remoteIP=""):
        self.path = path
        self.remoteIP = remoteIP
        binPath = get_path() + "/../../../"
        binPath = os.path.realpath(binPath)

        if path == "":
            self.path = os.path.abspath(binPath + "../../")
        else:
            self.path = os.path.realpath(path)

        if self.remoteIP:
            try:
                self.config = eval(remoteIP)
                self.remote_conn = Connection(host=self.config["host"], port=self.config["port"], user=self.config["user"], connect_kwargs={'password':self.config["password"]})
            except Exception as e:
                tdLog.notice(e)
    def update_cfg(self, update_dict :dict):
        '''
        update taoskeeper cfg file
        
        Args:
            update_dict: dict, update dict
                example: {"log": {"path": "/var/log/taos"}}
                
        Returns:
            None
            
        Raises:
            None
        '''
        if not isinstance(update_dict, dict):
            return
        if ("log" in update_dict and "path" not in update_dict["log"]) or "log" not in update_dict:
            self.taoskeeper_cfg_dict["log"]["path"] = os.path.join(self.path,"sim","dnode1","log")
        # if "log" in update_dict and "path" in update_dict["log"]:
        #     del update_dict["log"]["path"]
        for key, value in update_dict.items():
            if key in ["tdengine","log","metrics","metrics.database","metrics.database.options","enviornment"]:
                if isinstance(value, dict):
                    for k, v in value.items():
                        self.taoskeeper_cfg_dict[key][k] = v
            else:
                self.taoskeeper_cfg_dict[key] = value
        try:
            with open(self.cfg_path, 'r') as f:
                existing_data = toml.load(f)
        except FileNotFoundError:
            print(f"文件 {self.cfg_path} 不存在，将创建新文件。")
            existing_data = {}
        existing_data.update(self.taoskeeper_cfg_dict)
        with open(self.cfg_path, 'w') as f:
            toml.dump(existing_data, f)
            print(f"TOML 文件已成功更新：{self.cfg_path}")   
    def cfg(self, option, value):
        '''
        add param option and value to cfg file
        
        Args:
            option: str, param name
            value: str, param value

        Returns:
            None

        Raises:
            None
        '''
        cmd = f"echo {option} = {value} >> {self.cfg_path}"
        if os.system(cmd) != 0:
            tdLog.exit(cmd)
    def remote_exec(self, updateCfgDict, execCmd):
        remoteCfgDict = copy.deepcopy(updateCfgDict)
        if "log" in remoteCfgDict and "path" in remoteCfgDict["log"]:
            del remoteCfgDict["log"]["path"]

        remoteCfgDictStr = base64.b64encode(toml.dumps(remoteCfgDict).encode()).decode()
        execCmdStr = base64.b64encode(execCmd.encode()).decode()
        with self.remote_conn.cd((self.config["path"]+sys.path[0].replace(self.path, '')).replace('\\','/')):
            self.remote_conn.run(f"python3 ./test.py  -D {remoteCfgDictStr} -e {execCmdStr}" )
    def check_taoskeeper(self):
        if get_path(tool="taoskeeper"):
            return False
        else:
            return True
    def deploy(self, *update_cfg_dict):
        
        self.log_dir = os.path.join(self.path,"sim","dnode1","log")
        self.cfg_dir = os.path.join(self.path,"sim","dnode1","cfg")
        self.cfg_path = os.path.join(self.cfg_dir,"taoskeeper.toml")

        cmd = f"touch {self.cfg_path}"
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

        self.taoskeeper_cfg_dict["log"]["path"] = self.log_dir


        if (self.remoteIP == ""):
            dict2toml(self.taoskeeper_cfg_dict, self.cfg_path)
        else:
            self.remote_exec(self.taoskeeper_cfg_dict, "taoskeeper.deploy(update_cfg_dict)")

        self.deployed = 1

        tdLog.debug(f"taoskeeper is deployed and configured by {self.cfg_path}")
    
    def start(self):
        """
        start taoskeeper process.

        Args:
            None

        Returns:
            None

        Raises:
            None
        """
        bin_path = get_path(tool="taoskeeper")

        if (bin_path == ""):
            tdLog.exit("taoskeeper not found!")
        else:
            tdLog.info(f"taoskeeper found: {bin_path}")

        if platform.system().lower() == 'windows':
            cmd = f"mintty -h never {bin_path} -c {self.cfg_path}"
        else:
            cmd = f"nohup {bin_path} -c {self.cfg_path} > /dev/null & "

        if  self.remoteIP:
            self.remote_exec(self.taoskeeper_cfg_dict, f"taoskeeper.deployed=1\ntaoskeeper.log_dir={self.log_dir}\ntaoskeeper.cfg_dir={self.cfg_dir}\ntaoskeeper.start()")
            self.running = 1
        else:
            os.system(f"rm -rf {self.log_dir}{os.sep}taoskeeper*")
            if os.system(cmd) != 0:
                tdLog.exit(cmd)
            self.running = 1
            tdLog.debug(f"taoskeeper is running with {cmd} " )

            time.sleep(0.1)

            taoskeeper_port = self.taoskeeper_cfg_dict["port"]
            
            ip = 'localhost'
            if self.remoteIP != "":
                ip = self.remoteIP
            url = f'http://{ip}:{taoskeeper_port}/-/ping'
            try:
                r = requests.get(url)
                if r.status_code == 200:
                    tdLog.info(f"the taoskeeper has been started, using port:{taoskeeper_port}")
            except Exception:
                    tdLog.info(f"the taoskeeper do not started!!!")
                    time.sleep(1)
                    
    def start_taoskeeper(self):
        bin_path = get_path(tool="taoskeeper")

        if (bin_path == ""):
            tdLog.exit("taoskeeper not found!")
        else:
            tdLog.info(f"taoskeeper found: {bin_path}")

        if self.deployed == 0:
            tdLog.exit("taoskeeper is not deployed")

        if platform.system().lower() == 'windows':
            cmd = f"mintty -h never {bin_path} -c {self.cfg_dir}"
        else:
            cmd = f"nohup {bin_path} -c {self.cfg_path} > /dev/null & "

        if  self.remoteIP:
            self.remote_exec(self.taoskeeper_cfg_dict, f"taoskeeper.deployed=1\ntaoskeeper.log_dir={self.log_dir}\ntaoskeeper.cfg_dir={self.cfg_dir}\ntaoskeeper.start()")
            self.running = 1
        else:
            if os.system(cmd) != 0:
                tdLog.exit(cmd)
            self.running = 1
            tdLog.debug(f"taoskeeper is running with {cmd} " )

            time.sleep(0.1)
            
    def stop(self, force_kill=False):
        """
        stop taoskeeper process.

        Args:
            force_kill: bool, whether to force kill the process
                default: False
                if True, use kill -9
                if False, use kill -15

        Returns:
            None

        Raises:
            None
        """
        signal = "-9" if force_kill else "-15"
        if  self.remoteIP:
            self.remote_exec(self.taoskeeper_cfg_dict, "taoskeeper.running=1\ntaoskeeper.stop()")
            tdLog.info("stop taosadapter")
            return
        toBeKilled = "taoskeeper"
        if platform.system().lower() == 'windows':
            psCmd = f"ps -efww |grep -w {toBeKilled}| grep -v grep | awk '{{print $2}}'"
            processID = subprocess.check_output(psCmd, shell=True).decode("utf-8").strip()
            while(processID):
                killCmd = "kill %s %s > nul 2>&1" % (signal, processID)
                os.system(killCmd)
                time.sleep(1)
                processID = subprocess.check_output(psCmd, shell=True).decode("utf-8").strip()
            self.running = 0
            tdLog.debug(f"taoskeeper is stopped by kill {signal}")

        else:
            if self.running != 0:
                psCmd = f"ps -efww |grep -w {toBeKilled}| grep -v grep | awk '{{print $2}}'"
                processID = subprocess.check_output(psCmd, shell=True).decode("utf-8").strip()
                while(processID):
                    killCmd = "kill %s %s > /dev/null 2>&1" % (signal, processID)
                    os.system(killCmd)
                    time.sleep(1)
                    processID = subprocess.check_output(psCmd, shell=True).decode("utf-8").strip()
                port = 6041
                fuserCmd = f"fuser -k -n tcp {port} > /dev/null"
                os.system(fuserCmd)
                self.running = 0
                tdLog.debug(f"taoskeeper is stopped by kill {signal}")

taoskeeper = TaosKeeper()