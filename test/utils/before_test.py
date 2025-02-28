import subprocess
import sys
import os
import taostest

from utils.sql import TDSql
import taos
import taosrest
import taosws
import yaml


class BeforeTest:
    def __init__(self, request):
        self.request = request
        self.root_dir = request.config.rootdir
        self.yaml_file = request.config.getoption("--yaml_file")
        self.yaml_file_path = os.path.join(self.root_dir, 'env', self.yaml_file)
        self.tdLog = request.session.tdLog
    def install_taos(self):
        pass

    def deploy_taos(self, request):
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
            self.tdLog.debug("Required directories do not exist. Initializing...")
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
                self.tdLog.exit(f"Error run taostest --init: {e}")

        self.tdLog.debug(f"Deploying environment with config: {self.yaml_file}")
        try:
            subprocess.run(["taostest", "--setup", self.yaml_file], check=True)
        except Exception as e:
            self.tdLog.exit("Error run taostest --setup")

    def configure_test(self, request):

        # 获取部署信息
        # 读取 YAML 文件内容
        with open(self.yaml_file_path, 'r') as file:
            deploy_data = yaml.safe_load(file)  # 使用 safe_load 读取内容
        firstEP = deploy_data['settings'][0]['spec']['config']['firstEP']   # 获取 firstEP 字段
        host = firstEP.split(':')[0]
        port = int(firstEP.split(':')[1])
       
        request.session.host = host
        request.session.port = port
        request.session.user = "root"
        request.session.password = "taosdata"

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
        self.tdLog.debug(f"[BeforeTest.configure_test] dnodes: {denodes}")
        request.session.denodes = denodes
        self.denodes = denodes


    def create_database(self, request, db_name):
        '''
        创建module级别的数据库
        '''
        self.tdLog.debug(f"[BeforeTest.create_database] Creating database: {db_name}")
        try:
            conn = taos.connect(host=self.host, port=self.port)
            tdSql = self.get_tdsql(conn)
            tdSql.create_database(db_name, drop=False)
        except Exception as e:
            self.tdLog.exit(f"[BeforeTest.create_database] Error create database: {e}")
        finally:
            conn.close()


    def get_taos_conn(self):
        return taos.connect(host=self.host, port=self.port)

    def get_tdsql(self, conn):
        tdSql = TDSql()
        tdSql.init(conn.cursor())
        return tdSql

    def destroy(self):
        try:
            subprocess.run(["taostest", "--destroy", self.yaml_file], check=True)
        except Exception as e:
            self.tdLog.exit(f"[BeforeTest.destroy] Error run taostest --destroy: {e}")
        