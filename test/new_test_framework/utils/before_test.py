import subprocess
import sys
import os
import copy
from new_test_framework import taostest

import taos
import taosrest
import taosws
import yaml
import socket
import configparser
import shutil
from .sql import tdSql
from .log import tdLog
from .server.cluster import cluster, clusterDnodes
from .server.dnodes import tdDnodes
from .taosadapter import tAdapter
from .common import tdCom
from .taoskeeper import taoskeeper

def load_yaml_config(filename):
    config_path = os.path.join(os.path.dirname(__file__), filename)
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

class BeforeTest:
    def __init__(self, request):
        self.request = request
        self.root_dir = os.path.abspath(request.config.rootdir)
        #self.yaml_file = request.config.getoption("--yaml_file")
        #self.yaml_file_path = os.path.join(self.root_dir, 'env', self.yaml_file)
        temp_config = configparser.ConfigParser()
        temp_config.read(f"{os.path.dirname(__file__)}/config.ini")
        self.config = {}
        for section in temp_config.sections():
            self.config[section] = dict(temp_config.items(section))
        tdLog.debug(f"config_: {self.config}")
        self.log_level = request.config.getoption("--log-level").lower() if request.config.getoption("--log-level") else "info"
    def install_taos(self):
        pass

    def deploy_taos(self, yaml_file, mnodes_num=1, clean=False):
        """
        get env directory from request;
        use yaml file for taostest run;
        """
        # 初始化目录
        # 获取根目录
        #env_vars = os.environ.copy()
        #env_vars['TEST_ROOT'] = os.path.abspath(self.root_dir) # 将新路径添加到 PATH 的前面
        # 定义需要检查的目录
        required_dirs = [os.path.join(self.root_dir, 'cases'), 
                        os.path.join(self.root_dir, 'env'), 
                        os.path.join(self.root_dir, 'run')]
        
        # 检查目录是否存在
        if not all(os.path.exists(d) for d in required_dirs):
            tdLog.debug("Required directories do not exist. Initializing...")
            # 调用初始化命令
            try:
                #process = subprocess.Popen([sys.executable, "taostest", "--init"],
                #    stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, shell=True, env=env_vars)
                # 向子进程发送输入
                #process.stdin.write("\n")
                #process.stdin.flush()  # 确保输入被发送到子进程
                # 关闭子进程的stdin，防止它无限期等待更多输入
                #process.stdin.close()
                # 读取标准输出和标准错误输出
                
                # 等待子进程结束
                #process.wait()
                result = taostest.main({"test_root": self.root_dir, "init": True})
                if result != 0:
                    tdLog.error(f"Error run taostest --init: {result}")
            except Exception as e:
                tdLog.exit(f"Error run taostest --init: {e}")

        tdLog.debug(f"Deploying environment with config: {yaml_file}")
        setup_params = {
            "test_root": self.root_dir,
            "setup": yaml_file,
            "mnode_count": mnodes_num,
            "log_level": self.log_level
        }
        if clean:
            setup_params["clean"] = ''

        try:
            #subprocess.run([sys.executable, f"taostest --setup {yaml_file} --mnode-count {mnodes_num}"], check=True, text=True, shell=True, env=env_vars)
            result = taostest.main(setup_params)
            if result != 0:
                tdLog.error(f"Error run taostest --setup {yaml_file} --mnode-count {mnodes_num}: {result}")
        except Exception as e:
            tdLog.error(f"Exception run taostest --setup {yaml_file} --mnode-count {mnodes_num}: {e}")

    def configure_test(self, yaml_file):

        # 获取部署信息
        # 读取 YAML 文件内容
        with open(yaml_file, 'r') as file:
            deploy_data = yaml.safe_load(file)  # 使用 safe_load 读取内容
        firstEP = deploy_data['settings'][0]['spec']['config']['firstEP']   # 获取 firstEP 字段
        host = firstEP.split(':')[0]
        port = int(firstEP.split(':')[1])
       
        request.session.host = host
        request.session.port = port
        request.session.user = "root"
        request.session.password = "taosdata"
        request.session.cfg_path = deploy_data['settings'][0]['spec']['dnodes'][0]['config_dir']

        self.host = host
        self.port = port
        self.user = "root"
        self.password = "taosdata"
        
        denodes = []
        for dnode in deploy_data['settings'][0]['spec']['dnodes']:
            dnode_info = {}
            dnode_info['host'] = dnode['endpoint'].split(':')[0]
            dnode_info['port'] = int(dnode['endpoint'].split(':')[1])
            dnode_info['dataDir'] = dnode['config']['dataDir']
            dnode_info['logDir'] = dnode['config']['logDir']
            dnode_info['config_dir'] = dnode['config_dir']
            denodes.append(dnode_info)
        tdLog.debug(f"[BeforeTest.configure_test] dnodes: {denodes}")
        request.session.denodes = denodes
        self.denodes = denodes


    def create_database(self, request, db_name, host, port):
        '''
        创建module级别的数据库
        '''
        tdLog.debug(f"[BeforeTest.create_database] Creating database: {db_name}")
        try:
            conn = taos.connect(host=host, port=port)
            tdSql = self.get_tdsql(conn)
            tdSql.create_database(db_name, drop=False)
        except Exception as e:
            tdLog.error(f"[BeforeTest.create_database] Error create database: {e}")
        finally:
            conn.close()


    def get_taos_conn(self, request):
        if request.session.restful:
            return taosrest.connect(url=f"http://{request.session.host}:6041", timezone="utc")
        else:
            return taos.connect(host=request.session.host, port=request.session.port, config=tdDnodes.sim.cfgPath)

    def get_tdsql(self, conn):
        tdSql.init(conn.cursor())
        return tdSql

    def destroy(self, yaml_file):
        #env_vars = os.environ.copy()
        #env_vars['TEST_ROOT'] = os.path.abspath(self.root_dir) # 将新路径添加到 PATH 的前面
        try:
            #subprocess.run([sys.executable, f"taostest --destroy {yaml_file}"], check=True, text=True, shell=True, env=env_vars)
            result = taostest.main({"test_root": self.root_dir, "destroy": yaml_file})
            if result != 0:
                tdLog.error(f"[BeforeTest.destroy] Error run taostest --destroy: {result}")
        except Exception as e:
            tdLog.error(f"[BeforeTest.destroy] Error run taostest --destroy: {e}")
        
    def get_config_from_param(self, request):
        work_dir = request.session.work_dir
        #if "TDinternal" in self.root_dir:
        #    ci_path = os.path.join(os.path.dirname(os.path.dirname(self.root_dir)), 'sim')
        #else:
        #    ci_path = os.path.join(os.path.dirname(self.root_dir), 'sim')
        cfg_path = os.path.join(work_dir, 'cfg')
        #bin_path = os.path.dirname(self.getPath("taosd"))
        lib_path = os.path.join(os.path.dirname(request.session.taos_bin_path), 'lib')
        tdLog.debug(f"lib_path: {lib_path}")
        #os.environ["LD_LIBRARY_PATH"] = f"{lib_path}:{os.environ['LD_LIBRARY_PATH']}"
        #if os.path.exists(f"{lib_path}/libtaos.so") and lib_path != "/usr/lib":
        #    libso_file = os.path.basename(subprocess.check_output([f"ls -l {lib_path}/libtaos.so.3.3* | awk '{{print $9}}'"], shell=True).splitlines()[0].decode())
        #    tdLog.debug(f"libso_file: {libso_file}")
        #    subprocess.run([f"rm -rf /usr/lib/libtaos.so && ln -s {lib_path}/libtaos.so /usr/lib/libtaos.so"], check=True, text=True, shell=True)
        #    subprocess.run([f"rm -rf /usr/lib/libtaos.so.1 && ln -s {lib_path}/{libso_file} /usr/lib/libtaos.so.1"], check=True, text=True, shell=True)
        # os.environ["LIB_PATH"]= f"{lib_path}:{os.environ['LIB_PATH']}"
        #os.environ["LIBRARY_PATH"]= f"{lib_path}:{os.environ['LIBRARY_PATH']}"
        #tdLog.info(f"os.environ: {os.environ}")
        yaml_data = {
            "settings": [{
                "name": "taosd",
                "fqdn": ["localhost"],
                "spec": {
                    "version": "2.4.0.0",
                    "config": {
                        "firstEP": "localhost:6030"
                    },
                    "dnodes": []
                }
            }]
        }
        dnode_config_template = load_yaml_config(os.path.join(self.root_dir, 'env', 'taos_config.yaml'))
        adapter_config_template = load_yaml_config(os.path.join(self.root_dir, 'env', 'taosadapter_config.yaml'))
        taoskeeper_config_template = load_yaml_config(os.path.join(self.root_dir, 'env', 'taoskeeper_config.yaml'))
        servers = []
        port_base = dnode_config_template["port"] if "port" in dnode_config_template else 6030
        mqttport_base = dnode_config_template["mqttPort"] if "mqttPort" in dnode_config_template else 6083
        for i in range(request.session.denodes_num):
            dnode_cfg_path = os.path.join(work_dir, f"dnode{i+1}", "cfg")
            log_path = os.path.join(work_dir, f"dnode{i+1}", "log")
            if request.session.level > 1 or request.session.disk > 1:
                data_path = []
                primary = 1
                for l in range(request.session.level):
                    for d in range(request.session.disk):
                        eDir = os.path.join(work_dir, f"dnode{i+1}", f"data{l}{d}")
                        data_path.append(f"{eDir} {l} {primary}")
                        if primary == 1:
                            primary = 0
            else:
                data_path = os.path.join(work_dir, f"dnode{i+1}", "data")
            dnode_config = copy.deepcopy(dnode_config_template)
            dnode_config.pop("port", None)
            dnode_config["mqttPort"] = mqttport_base + i * 100
            dnode_config["dataDir"] = data_path
            dnode_config["logDir"] = log_path
            dnode = {
                "endpoint": f"localhost:{port_base + i * 100}",
                "config_dir": dnode_cfg_path,
                "taosdPath": os.path.join(request.session.taos_bin_path, "taosd"),
                "system": sys.platform,
                "config": dnode_config,
                "mqttPort": dnode_config["mqttPort"],
            }
            tdLog.debug(f"[BeforeTest.ci_init_config] dnode: {dnode}")
            if request.session.query_policy > 1:
                dnode["config"]["queryPolicy"] = request.session.query_policy
            if request.session.independentMnode and i < request.session.mnodes_num:
                dnode["config"]["supportVnodes"] = 0
            if request.session.asan:
                dnode["asanDir"] = os.path.join(work_dir, "asan", f"dnode{i+1}.asan")
                os.makedirs(os.path.dirname(dnode["asanDir"]), exist_ok=True)
                request.session.asan_dir = dnode["asanDir"]
            yaml_data["settings"][0]["spec"]["dnodes"].append(dnode)
            server = {
                "host": dnode["endpoint"].split(":")[0],
                "port": int(dnode["endpoint"].split(":")[1]),
                "cfg_path": dnode["config_dir"],
                "endpoint": dnode["endpoint"],
                "log_dir": log_path,
                "data_dir": data_path,
                "config": dnode["config"],
                "mqttPort": dnode["mqttPort"],
            }
            servers.append(server)
        request.session.servers = servers
        if request.session.restful:
            # TODO: 增加taosAdapter的配置
            adapter_config_dir = os.path.join(work_dir, "dnode1", "cfg")
            adapter_config_file = os.path.join(adapter_config_dir, "taosadapter.toml")
            taos_config_file = os.path.join(work_dir, "dnode1", "cfg", "taos.cfg")
            adapter_log_dir = os.path.join(work_dir, "dnode1", "log")
            taos_log_dir = os.path.join(work_dir, "dnode1", "log")
            
            adapter_config = copy.deepcopy(adapter_config_template)
            adapter_config["taosConfigDir"] = taos_config_file
            adapter_config["log"]["path"] = adapter_log_dir
            restful_dict = {
                "name": "taosAdapter",
                "fqdn": ["localhost"],
                "spec": {
                    "version": "2.4.0.0",
                    "config_file": adapter_config_file,
                    "adapter_config": adapter_config,
                    "taos_config": {
                        "firstEP": "localhost:6030",
                        "logDir": taos_log_dir
                    },
                    "taosadapterPath": os.path.join(request.session.taos_bin_path, "taosadapter")
                }
            }
            if request.session.asan:
                restful_dict["spec"]["asanDir"] = os.path.join(work_dir, "asan", f"taosadapter.asan")
            yaml_data["settings"].append(restful_dict)
            adapter = {}
            adapter["host"] = "localhost"
            adapter["cfg_dir"] = adapter_config_dir
            adapter["config_file"] = adapter_config_file
            adapter["port"] = 6041
            adapter["logLevel"] = "info"
            adapter["log_path"] = adapter_log_dir
            adapter["taos_firstEP"] = "localhost:6030"
            adapter["taos_logDir"] = taos_log_dir
            request.session.adapter = adapter
        if request.session.set_taoskeeper:
            # TODO:增加taoskeeper配置
            taoskeeper_config_dir = os.path.join(work_dir, "dnode1", "cfg")
            taoskeeper_config_file = os.path.join(taoskeeper_config_dir, "taoskeeper.toml")
            taos_config_file = os.path.join(work_dir, "dnode1", "cfg", "taos.cfg")
            taoskeeper_log_dir = os.path.join(work_dir, "dnode1", "log")
            taos_log_dir = os.path.join(work_dir, "dnode1", "log")
            
            taoskeeper_config = copy.deepcopy(taoskeeper_config_template)
            taoskeeper_config["log"]["path"] = taoskeeper_log_dir
            taoskeeper_dict = {
                "name": "taoskeeper",
                "fqdn": ["localhost"],
                "spec": {
                    "version": "2.4.0.0",
                    "config_file": taoskeeper_config_file,
                    "taoskeeper_config": taoskeeper_config,
                    "taos_config": {
                        "firstEP": "localhost:6030",
                        "logDir": taos_log_dir
                    },
                    "taoskeeperPath": os.path.join(request.session.taos_bin_path, "taoskeeper")
            }
                
            }
            yaml_data["settings"].append(taoskeeper_dict)
            taoskeeper = {}
            taoskeeper["host"] = "localhost"
            taoskeeper["port"] = 6043
            taoskeeper["username"] = "root"
            taoskeeper["password"] = "taosdata"
            taoskeeper["path"] = taoskeeper_config_dir
            taoskeeper["RotationInterval"] = "15s"
            taoskeeper["level"] = "info"
            taoskeeper["rotationSize"] = "1GB"
            taoskeeper["rotationCount"] = 30
            taoskeeper["keepDays"] = 30
            taoskeeper["reservedDiskSize"] = "0"
            taoskeeper["log_path"] = taoskeeper_log_dir
            taoskeeper["config_file"] = taoskeeper_config_file
            taoskeeper["taos_logDir"] = taos_log_dir
            taoskeeper["cfg_dir"] = taoskeeper_config_dir
            request.session.taoskeeper = taoskeeper
        request.session.yaml_data = yaml_data
        request.session.yaml_file = 'ci_default.yaml'
        if not os.path.exists(os.path.join(self.root_dir, 'env')):
            os.makedirs(os.path.join(self.root_dir, 'env'))
        with open(os.path.join(self.root_dir, 'env', request.session.yaml_file), 'w') as file:
            yaml.dump(yaml_data, file)
        request.session.host = servers[0]["host"]
        request.session.port = servers[0]["port"]
        request.session.user = "root"
        request.session.password = "taosdata"
        request.session.cfg_path = servers[0]["cfg_path"]
        #request.session.bin_path = bin_path
        request.session.lib_path = lib_path
        request.session.tsim_path = os.path.join(request.session.taos_bin_path, "tsim")
        #request.session.ci_workdir = ci_path
    

    def get_config_from_yaml(self, request, yaml_file_path):
        with open(yaml_file_path, "r") as f:
            yaml_data = yaml.safe_load(f)

        # 解析settings中name=taosd的配置
        servers = []
        request.session.restful = False
        request.session.set_taoskeeper = False
        for setting in yaml_data.get("settings", []):
            if setting.get("name") == "taosd":
                for dnode in setting["spec"]["dnodes"]:
                    endpoint = dnode["endpoint"]
                    host, port = endpoint.split(":")
                    server = {
                        "host": host,
                        "port": int(port),
                        "cfg_path": dnode["config_dir"],
                        "endpoint": endpoint,
                        "log_dir": dnode["config"]["logDir"],
                        "data_dir": dnode["config"]["dataDir"],
                        "config": dnode["config"],
                        "taosd_path": dnode["taosdPath"] if "taosdPath" in dnode else None
                    }
                    servers.append(server)
            if setting.get("name") == "taosAdapter":
                # TODO: 解析taosAdapter的配置
                request.session.restful = True
                adapter = {}
                adapter["host"] = setting["fqdn"][0]
                adapter["cfg_dir"] = os.path.dirname(setting["spec"]["config_file"])
                adapter["config_file"] = setting["spec"]["config_file"]
                adapter["port"] = setting["spec"]["adapter_config"]["port"]
                adapter["logLevel"] = setting["spec"]["adapter_config"]["logLevel"]
                adapter["log_path"] = setting["spec"]["adapter_config"]["log"]["path"]
                adapter["taos_firstEP"] = setting["spec"]["taos_config"]["firstEP"]
                adapter["taos_logDir"] = setting["spec"]["taos_config"]["logDir"]
            if setting.get("name") == "taoskeeper":
                # TODO:解析taoskeeper的配置
                request.session.set_taoskeeper = True
                taoskeeper = {}
                taoskeeper["host"] = setting["fqdn"][0]
                taoskeeper["port"] = setting["spec"]["port"]
                taoskeeper["username"] = setting["spec"]["taoskeeper_config"]["username"]
                taoskeeper["password"] = setting["spec"]["taoskeeper_config"]["password"]
                taoskeeper["path"] = os.path.dirname(setting["spec"]["config_file"])
                taoskeeper["RotationInterval"] = setting["spec"]["taoskeeper_config"]["RotationInterval"]
                taoskeeper["level"] = setting["spec"]["taoskeeper_config"]["level"]
                taoskeeper["rotationSize"] = setting["spec"]["taoskeeper_config"]["rotationSize"]
                taoskeeper["rotationCount"] = setting["spec"]["taoskeeper_config"]["rotationCount"]
                taoskeeper["keepDays"] = setting["spec"]["taoskeeper_config"]["keepDays"]
                taoskeeper["reservedDiskSize"] = setting["spec"]["taoskeeper_config"]["reservedDiskSize"]
                
        request.session.host = servers[0]["host"]
        request.session.port = servers[0]["port"]
        request.session.user = "root"
        request.session.password = "taosdata"
        request.session.cfg_path = servers[0]["cfg_path"]
        request.session.servers = servers
        request.session.level = 1
        request.session.disk = 1
        request.session.denodes_num = len(servers)
        request.session.create_dnode_num = len(servers)
        request.session.query_policy = 1
        request.session.yaml_data = yaml_data
        if setting.get("name") == "taosAdapter":
            request.session.adapter = adapter
        if setting.get("name") == "taoskeeper":
            request.session.taoskeeper = taoskeeper

        if servers[0]["taosd_path"] is not None:
            request.session.taos_bin_path = servers[0]["taosd_path"]
        else:
            request.session.taos_bin_path = self.get_taos_bin_path()
        request.session.lib_path = os.path.join(os.path.dirname(request.session.taos_bin_path), 'lib')


    def init_dnode_cluster(self, request, dnode_nums, mnode_nums, independentMnode=True, level=1, disk=1):
        global tdDnodes, clusterDnodes, cluster
        host = socket.gethostname()
        if request.session.host == host or request.session.host == "localhost":
            master_ip = ""
        else:
            master_ip = request.session.host
        #logger.info(f"tdDnodes_pytest in init_dnode_cluster: {tdDnodes_pytest}")
        if dnode_nums > 1:
            dnodes_list = cluster.configure_cluster(dnodeNums=dnode_nums, mnodeNums=mnode_nums, independentMnode=independentMnode)
            #clusterDnodes.init(dnodes_list, request.session.work_dir, os.path.join(request.session.taos_bin_path, "taosd"), master_ip)
            #clusterDnodes.setTestCluster(False)
            #clusterDnodes.setValgrind(0)
            #clusterDnodes.setAsan(request.session.asan)
            tdDnodes.dnodes = dnodes_list #clusterDnodes.dnodes
            tdDnodes.init(request.session.work_dir, os.path.join(request.session.taos_bin_path, "taosd"), master_ip)
        else:
            tdDnodes.init(request.session.work_dir, os.path.join(request.session.taos_bin_path, "taosd"), master_ip)
            tdDnodes.setKillValgrind(1)
            tdDnodes.setTestCluster(False)
            tdDnodes.setValgrind(0)
            tdDnodes.setAsan(request.session.asan)
        tdDnodes.sim.setTestCluster(False)
        tdDnodes.sim.logDir = os.path.join(tdDnodes.sim.path,"psim","log")
        tdDnodes.sim.cfgDir = os.path.join(tdDnodes.sim.path,"psim","cfg")
        tdDnodes.sim.cfgPath = os.path.join(tdDnodes.sim.path,"psim","cfg","taos.cfg")
        tdDnodes.sim.deploy(request.cls.updatecfgDict if hasattr(request.cls, "updatecfgDict") else {})
        tdDnodes.simDeployed = True
        tdDnodes.setLevelDisk(request.session.level, request.session.disk)
        for i in range(dnode_nums):
            tdDnodes.dnodes[i].setTestCluster(False)
            tdDnodes.dnodes[i].setValgrind(0)
            tdDnodes.dnodes[i].setAsan(request.session.asan)
            tdDnodes.dnodes[i].logDir = request.session.servers[i]["log_dir"]
            tdDnodes.dnodes[i].dataDir = request.session.servers[i]["data_dir"]
            tdDnodes.dnodes[i].cfgDir = request.session.servers[i]["cfg_path"]
            tdDnodes.dnodes[i].cfgPath = os.path.join(request.session.servers[i]["cfg_path"],"taos.cfg")
            tdDnodes.dnodes[i].cfgDict["dataDir"] = tdDnodes.dnodes[i].dataDir
            tdDnodes.dnodes[i].cfgDict["logDir"] = tdDnodes.dnodes[i].logDir
            tdDnodes.dnodes[i].deployed = 1
            tdDnodes.dnodes[i].running = 1
        
        if request.session.restful:
            tAdapter.init(request.session.work_dir, master_ip)
            tAdapter.log_dir = request.session.adapter["log_path"]
            tAdapter.cfg_dir = request.session.adapter["cfg_dir"]
            tAdapter.cfg_path = request.session.adapter["config_file"]
            tAdapter.taosadapter_cfg_dict["log"]["path"] = request.session.adapter["log_path"]
            tAdapter.deployed = 1
            tAdapter.running = 1

    # TODO: 增加taoskeeper实例化
        if request.session.set_taoskeeper:
            taoskeeper.init("", master_ip)
            taoskeeper.log_dir = request.session.taoskeeper["log_path"]
            taoskeeper.cfg_dir = request.session.taoskeeper["cfg_dir"]
            taoskeeper.cfg_path = request.session.taoskeeper["config_file"]
            taoskeeper.deployed = 1
            taoskeeper.running = 1

        # 实例化 tdCommon
        tdCom.init(request.session.taos_bin_path, request.session.cfg_path, request.session.work_dir)
       
    def update_cfg(self, updatecfgDict):
        for key, value in updatecfgDict.items():
            if key == "clientCfg":
                continue
            for dnode in self.request.session.yaml_data["settings"][0]["spec"]["dnodes"]:
                dnode["config"][key] = value
        with open(os.path.join(self.root_dir, 'env', 'ci_default.yaml'), 'w') as file:
            yaml.dump(self.request.session.yaml_data, file)


    def getPath(self, binary="taosd"):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("test")]

        paths = []
        debug_path = os.path.join(projPath, "debug", "build", "bin")
        for root, dirs, files in os.walk(debug_path):
            if (binary in files or (f"{binary}.exe") in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    paths.append(os.path.join(root, binary))
                    break
        if (len(paths) == 0):
            if sys.platform == "win32":
                return f"C:\\TDengine\\bin\\{binary}.exe"
            elif sys.platform == "darwin":
                if os.path.exists(f"/usr/local/bin/{binary}"):
                    return f"/usr/local/bin/{binary}"
                else:
                    tdLog.error(f"taosd binary not found in /usr/local/bin/{binary}")
                    return None
            else:
                tdLog.debug(f"taos_bin_path: {os.path.exists('/usr/bin/{binary}')}")
                if os.path.exists(f"/usr/bin/{binary}"):
                    return f"/usr/bin/{binary}"
                else:
                    tdLog.error(f"taosd binary not found in /usr/bin/{binary}")
                    return None
        return paths[0]

    def get_taos_bin_path(self, taos_bin_path):
        if taos_bin_path is not None and os.path.exists(os.path.join(taos_bin_path, "taosd")):
            return taos_bin_path
        bin_path = self.getPath()
        if bin_path is not None:
            return os.path.dirname(bin_path)
        raise Exception("taosd binary not found in TAOS_BIN_PATH")

    def get_and_mkdir_workdir(self, workdir):
        if workdir is None:
            selfPath = os.path.dirname(os.path.realpath(__file__))
            projPath = None
            if ("community" in selfPath):
                projPath = selfPath[:selfPath.find("community")]
            elif ("TDengine" in selfPath):
                projPath = selfPath[:selfPath.find("test")]
            if projPath is not None:
                workdir = os.path.join(projPath, "sim")
            else:
                workdir = os.path.join(selfPath, "sim")
        tdLog.debug(f"workdir: {workdir}")
        #if os.path.exists(workdir):
        #    shutil.rmtree(workdir)
        os.makedirs(workdir, exist_ok=True)
        return workdir