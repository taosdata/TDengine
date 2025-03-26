import requests
from fabric2 import Connection
from .util.log import *
from .util.common import *


class TaosKeeper:
    def __init__(self):
        self.running = 0
        self.deployed = 0
        self.remoteIP = ""
        self.taoskeeper_cfg_dict = {
            "instanceID": 64,
            "port": 6043,
            "gopoolsize": 50000,
            "RotationInterval": "15s",
            "host": "127.0.0.1",
            "username": "root",
            "password": "taosdata",
            "usessl": False,
            "level": "info",
            "rotationcount": 30,
            "keepDays": 30,
            "compress": False,
            "reservedDiskSize": "1GB" 
        }
    # TODO: add taosadapter env:
    # 1. init cfg.toml.dict ：OK
    # 2. dump dict to toml ： OK
    # 3. update cfg.toml.dict ：OK
    # 4. check adapter exists ： OK
    # 5. deploy adapter cfg ： OK
    # 6. adapter start ：   OK
    # 7. adapter stop
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
        """
            use this method, must deploy taoskeeper first
        """
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
        signal = "-9" if force_kill else "-15"
        if  self.remoteIP:
            self.remote_exec(self.taoskeeper_cfg_dict, "taoskeeper.running=1\ntaoskeeper.stop()")
            tdLog.info("stop taosadapter")
            return
        toBeKilled = "taoskeeper"
        if platform.system().lower() == 'windows':
            psCmd = f"ps -ef|grep -w {toBeKilled}| grep -v grep | awk '{{print $2}}'"
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
                psCmd = f"ps -ef|grep -w {toBeKilled}| grep -v grep | awk '{{print $2}}'"
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