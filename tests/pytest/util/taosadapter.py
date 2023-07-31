import requests
from fabric2 import Connection
from util.log import *
from util.common import *


class TAdapter:
    def __init__(self):
        self.running = 0
        self.deployed = 0
        self.remoteIP = ""
        self.taosadapter_cfg_dict = {
            "debug"         : True,
            "taosConfigDir" : "",
            "port"          : 6041,
            "logLevel"      : "error",
            "cors"          : {
                "allowAllOrigins" : True,
            },
            "pool"          : {
                "maxConnect"    : 4000,
                "maxIdle"       : 4000,
                "idleTimeout"   : "1h"
            },
            "ssl"           : {
                "enable"        : False,
                "certFile"      : "",
                "keyFile"       : "",
            },
            "log"           : {
                "path"                  : "",
                "rotationCount"         : 30,
                "rotationTime"          : "24h",
                "rotationSize"          : "1GB",
                "enableRecordHttpSql"   : True,
                "sqlRotationCount"      : 2,
                "sqlRotationTime"       : "24h",
                "sqlRotationSize"       : "1GB",
            },
            "monitor"       : {
                "collectDuration"           : "3s",
                "incgroup"                  : False,
                "pauseQueryMemoryThreshold" : 70,
                "pauseAllMemoryThreshold"   : 80,
                "identity"                  : "",
                "writeToTD"                 : True,
                "user"                      : "root",
                "password"                  : "taosdata",
                "writeInterval"             : "30s"
            },
            "opentsdb"      : {
                "enable"        : True
            },
            "influxdb"      : {
                "enable"        : True
            },
            "statsd"      : {
                "enable"        : True
            },
            "collectd"      : {
                "enable"        : True
            },
            "opentsdb_telnet"      : {
                "enable"        : True
            },
            "node_exporter"      : {
                "enable"        : True
            },
            "prometheus"      : {
                "enable"        : True
            },
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

    def update_cfg(self, update_dict :dict):
        if not isinstance(update_dict, dict):
            return
        if "log" in update_dict and "path" in update_dict["log"]:
            del update_dict["log"]["path"]
        for key, value in update_dict.items():
            if key in ["cors", "pool", "ssl", "log", "monitor", "opentsdb", "influxdb", "statsd", "collectd", "opentsdb_telnet", "node_exporter", "prometheus"]:
                if  isinstance(value, dict):
                    for k, v in value.items():
                        self.taosadapter_cfg_dict[key][k] = v
            else:
                self.taosadapter_cfg_dict[key] = value

    def check_adapter(self):
        if get_path(tool="taosadapter"):
            return False
        else:
            return True

    def remote_exec(self, updateCfgDict, execCmd):
        remoteCfgDict = copy.deepcopy(updateCfgDict)
        if "log" in remoteCfgDict and "path" in remoteCfgDict["log"]:
            del remoteCfgDict["log"]["path"]

        remoteCfgDictStr = base64.b64encode(toml.dumps(remoteCfgDict).encode()).decode()
        execCmdStr = base64.b64encode(execCmd.encode()).decode()
        with self.remote_conn.cd((self.config["path"]+sys.path[0].replace(self.path, '')).replace('\\','/')):
            self.remote_conn.run(f"python3 ./test.py  -D {remoteCfgDictStr} -e {execCmdStr}" )

    def cfg(self, option, value):
        cmd = f"echo {option} = {value} >> {self.cfg_path}"
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

    def deploy(self, *update_cfg_dict):
        self.log_dir = os.path.join(self.path,"sim","dnode1","log")
        self.cfg_dir = os.path.join(self.path,"sim","dnode1","cfg")
        self.cfg_path = os.path.join(self.cfg_dir,"taosadapter.toml")

        cmd = f"touch {self.cfg_path}"
        if os.system(cmd) != 0:
            tdLog.exit(cmd)

        self.taosadapter_cfg_dict["log"]["path"] = self.log_dir
        if bool(update_cfg_dict):
            self.update_cfg(update_dict=update_cfg_dict)

        if (self.remoteIP == ""):
            dict2toml(self.taosadapter_cfg_dict, self.cfg_path)
        else:
            self.remote_exec(self.taosadapter_cfg_dict, "tAdapter.deploy(update_cfg_dict)")

        self.deployed = 1

        tdLog.debug(f"taosadapter is deployed and configured by {self.cfg_path}")

    def start(self):
        bin_path = get_path(tool="taosadapter")

        if (bin_path == ""):
            tdLog.exit("taosadapter not found!")
        else:
            tdLog.info(f"taosadapter found: {bin_path}")

        if platform.system().lower() == 'windows':
            cmd = f"mintty -h never {bin_path} -c {self.cfg_path}"
        else:
            cmd = f"nohup {bin_path} -c {self.cfg_path} > /dev/null & "

        if  self.remoteIP:
            self.remote_exec(self.taosadapter_cfg_dict, f"tAdapter.deployed=1\ntAdapter.log_dir={self.log_dir}\ntAdapter.cfg_dir={self.cfg_dir}\ntAdapter.start()")
            self.running = 1
        else:
            os.system(f"rm -rf {self.log_dir}{os.sep}taosadapter*")
            if os.system(cmd) != 0:
                tdLog.exit(cmd)
            self.running = 1
            tdLog.debug(f"taosadapter is running with {cmd} " )

            time.sleep(0.1)

            taosadapter_port = self.taosadapter_cfg_dict["port"]
            for i in range(5):
                ip = 'localhost'
                if self.remoteIP != "":
                    ip = self.remoteIP
                url = f'http://{ip}:{taosadapter_port}/-/ping'
                try:
                    r = requests.get(url)
                    if r.status_code == 200:
                        tdLog.info(f"the taosadapter has been started, using port:{taosadapter_port}")
                        break
                except Exception:
                        tdLog.info(f"the taosadapter do not started!!!")
                        time.sleep(1)

    def start_taosadapter(self):
        """
            use this method, must deploy taosadapter
        """
        bin_path = get_path(tool="taosadapter")

        if (bin_path == ""):
            tdLog.exit("taosadapter not found!")
        else:
            tdLog.info(f"taosadapter found: {bin_path}")

        if self.deployed == 0:
            tdLog.exit("taosadapter is not deployed")

        if platform.system().lower() == 'windows':
            cmd = f"mintty -h never {bin_path} -c {self.cfg_dir}"
        else:
            cmd = f"nohup {bin_path} -c {self.cfg_path} > /dev/null & "

        if  self.remoteIP:
            self.remote_exec(self.taosadapter_cfg_dict, f"tAdapter.deployed=1\ntAdapter.log_dir={self.log_dir}\ntAdapter.cfg_dir={self.cfg_dir}\ntAdapter.start()")
            self.running = 1
        else:
            if os.system(cmd) != 0:
                tdLog.exit(cmd)
            self.running = 1
            tdLog.debug(f"taosadapter is running with {cmd} " )

            time.sleep(0.1)

    def stop(self, force_kill=False):
        signal = "-9" if force_kill else "-15"
        if  self.remoteIP:
            self.remote_exec(self.taosadapter_cfg_dict, "tAdapter.running=1\ntAdapter.stop()")
            tdLog.info("stop taosadapter")
            return
        toBeKilled = "taosadapter"
        if platform.system().lower() == 'windows':
            psCmd = f"ps -ef|grep -w {toBeKilled}| grep -v grep | awk '{{print $2}}'"
            processID = subprocess.check_output(psCmd, shell=True).decode("utf-8").strip()
            while(processID):
                killCmd = "kill %s %s > nul 2>&1" % (signal, processID)
                os.system(killCmd)
                time.sleep(1)
                processID = subprocess.check_output(psCmd, shell=True).decode("utf-8").strip()
            self.running = 0
            tdLog.debug(f"taosadapter is stopped by kill {signal}")

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
                tdLog.debug(f"taosadapter is stopped by kill {signal}")



tAdapter = TAdapter()
