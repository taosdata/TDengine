import subprocess
import sys
import os
import taostest

from utils.sql import TDSql
import taos
import taosrest
import taosws
import yaml
import logging
import configparser
from utils.pytest.util.cluster import cluster as cluster_pytest, ClusterDnodes as ClusterDnodes_pytest
from utils.pytest.util.dnodes import tdDnodes as tdDnodes_pytest

logger = logging.getLogger(__name__)
logger.info(f"tdDnodes_pytest: {tdDnodes_pytest}")

class BeforeTest:
    def __init__(self, request):
        self.request = request
        self.root_dir = request.config.rootdir
        #self.yaml_file = request.config.getoption("--yaml_file")
        #self.yaml_file_path = os.path.join(self.root_dir, 'env', self.yaml_file)
        temp_config = configparser.ConfigParser()
        temp_config.read(f"{os.path.dirname(__file__)}/config.ini")
        self.config = {}
        for section in temp_config.sections():
            self.config[section] = dict(temp_config.items(section))
        logger.debug(f"config_: {self.config}")
    def install_taos(self):
        pass

    def deploy_taos(self, yaml_file, mnodes_num=1):
        """
        get env directory from request;
        use yaml file for tostest run;
        """
        # 初始化目录
        # 获取根目录
        env_vars = os.environ.copy()
        env_vars['TEST_ROOT'] = self.root_dir  # 将新路径添加到 PATH 的前面
        # 定义需要检查的目录
        required_dirs = [os.path.join(self.root_dir, 'cases'), 
                        os.path.join(self.root_dir, 'env'), 
                        os.path.join(self.root_dir, 'run')]
        
        # 检查目录是否存在
        if not all(os.path.exists(d) for d in required_dirs):
            logger.debug("Required directories do not exist. Initializing...")
            # 调用初始化命令
            try:
                process = subprocess.Popen(["taostest", "--init"],
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, env=env_vars)
                # 向子进程发送输入
                process.stdin.write("\n")
                process.stdin.flush()  # 确保输入被发送到子进程
                # 关闭子进程的stdin，防止它无限期等待更多输入
                process.stdin.close()
                # 等待子进程结束
                process.wait()
            except Exception as e:
                logger.exit(f"Error run taostest --init: {e}")

        logger.debug(f"Deploying environment with config: {yaml_file}")
        try:
            subprocess.run(["taostest", "--setup", yaml_file, "--mnode-count", f"{mnodes_num}"], check=True)
        except Exception as e:
            logger.error(f"Error run taostest --setup {yaml_file} --mnode-count {mnodes_num}: {e}")

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
        logger.debug(f"[BeforeTest.configure_test] dnodes: {denodes}")
        request.session.denodes = denodes
        self.denodes = denodes


    def create_database(self, request, db_name, host, port):
        '''
        创建module级别的数据库
        '''
        logger.debug(f"[BeforeTest.create_database] Creating database: {db_name}")
        try:
            conn = taos.connect(host=host, port=port)
            tdSql = self.get_tdsql(conn)
            tdSql.create_database(db_name, drop=False)
        except Exception as e:
            logger.error(f"[BeforeTest.create_database] Error create database: {e}")
        finally:
            conn.close()


    def get_taos_conn(self, host, port):
        return taos.connect(host=host, port=port)

    def get_tdsql(self, conn):
        tdSql = TDSql()
        tdSql.init(conn.cursor())
        return tdSql

    #def destroy(self):
    #    try:
    #        subprocess.run(["taostest", "--destroy", self.yaml_file], check=True)
    #    except Exception as e:
    #        logger.error(f"[BeforeTest.destroy] Error run taostest --destroy: {e}")
        
    def ci_init_config(self, denodes_num, query_policy, restful):
        ci_path = os.path.join(os.path.dirname(self.root_dir), 'sim')
        cfg_path = os.path.join(ci_path, 'cfg')
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
        for i in range(denodes_num):
            dnode_cfg_path = os.path.join(ci_path, f"dnode{i+1}", "cfg")
            log_path = os.path.join(ci_path, f"dnode{i+1}", "log")
            data_path = os.path.join(ci_path, f"dnode{i+1}", "data")
            dnode = {
                "endpoint": f"localhost:{6030 + i * 100}",
                "config_dir": dnode_cfg_path,
                "config": {
                    "dataDir": data_path,
                    "logDir": log_path
                }
            }
            logger.debug(f"[BeforeTest.ci_init_config] dnode: {dnode}")
            if query_policy > 1:
                dnode["spec"]["config"]["queryPolicy"] = query_policy
            yaml_data["settings"][0]["spec"]["dnodes"].append(dnode)
        if restful:
            config_file = os.path.join(ci_path, "dnode1", "cfg", "taosadapter.toml")
            taosConfigDir = os.path.join(ci_path, "dnode1", "cfg", "taos.cfg")
            log_dir = os.path.join(ci_path, "dnode1", "log")
            taos_log_dir = os.path.join(ci_path, "dnode1", "log")
            restful_dict = {
                "name": "taosAdapter",
                "fqdn": ["localhost"],
                "spec": {
                    "version": "2.4.0.0",
                    "config_file": config_file,
                    "adapter_config": {
                        "logLevel": "info",
                        "port": 6041,
                        "taosConfigDir": taosConfigDir,
                        "log": {"path": log_dir}
                    },
                    "taos_config": {
                        "firstEP": "localhost:6030",
                        "logDir": taos_log_dir
                    }
                }
            }
            yaml_data["settings"].append(restful_dict)
        with open(os.path.join(self.root_dir, 'env', 'ci_default.yaml'), 'w') as file:
            yaml.dump(yaml_data, file)
    

    def init_dnode_cluster(self, request, dnode_nums, mnode_nums, independentMnode=True):
        global tdDnodes_pytest
        #logger.info(f"tdDnodes_pytest in init_dnode_cluster: {tdDnodes_pytest}")
        if dnode_nums > 1:
            dnodeslist = cluster_pytest.configure_cluster(dnodeNums=dnode_nums, mnodeNums=mnode_nums, independentMnode=independentMnode)
            tdDnodes_pytest = ClusterDnodes_pytest(dnodeslist)
            tdDnodes_pytest.init("", "")
            tdDnodes_pytest.setTestCluster(False)
            tdDnodes_pytest.setValgrind(0)
            tdDnodes_pytest.setAsan(0)
        else:
            tdDnodes_pytest.init("", "")
            tdDnodes_pytest.setKillValgrind(1)
            tdDnodes_pytest.setTestCluster(False)
            tdDnodes_pytest.setValgrind(0)
            tdDnodes_pytest.setAsan(0)
        tdDnodes_pytest.sim.setTestCluster(False)
        tdDnodes_pytest.sim.logDir = os.path.join(tdDnodes_pytest.sim.path,"sim","psim","log")
        tdDnodes_pytest.sim.cfgDir = os.path.join(tdDnodes_pytest.sim.path,"sim","psim","cfg")
        tdDnodes_pytest.sim.cfgPath = os.path.join(tdDnodes_pytest.sim.path,"sim","psim","cfg","taos.cfg")
        tdDnodes_pytest.simDeployed = True
        for i in range(dnode_nums):
            tdDnodes_pytest.dnodes[i].setTestCluster(False)
            tdDnodes_pytest.dnodes[i].setValgrind(0)
            tdDnodes_pytest.dnodes[i].setAsan(0)
            tdDnodes_pytest.dnodes[i].logDir = os.path.join(tdDnodes_pytest.dnodes[i].path,"sim","dnode%d" % (i + 1), "log")
            tdDnodes_pytest.dnodes[i].dataDir = os.path.join(tdDnodes_pytest.dnodes[i].path,"sim","dnode%d" % (i + 1), "data")
            tdDnodes_pytest.dnodes[i].cfgDir = os.path.join(tdDnodes_pytest.dnodes[i].path,"sim","dnode%d" % (i + 1), "cfg")
            tdDnodes_pytest.dnodes[i].cfgPath = os.path.join(tdDnodes_pytest.dnodes[i].path,"sim","dnode%d" % (i + 1), "cfg","taos.cfg")
            tdDnodes_pytest.dnodes[i].cfgDict["dataDir"] = tdDnodes_pytest.dnodes[i].dataDir
            tdDnodes_pytest.dnodes[i].cfgDict["logDir"] = tdDnodes_pytest.dnodes[i].logDir
            tdDnodes_pytest.dnodes[i].deployed = 1
            tdDnodes_pytest.dnodes[i].running = 1
