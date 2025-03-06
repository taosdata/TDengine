import pytest
import subprocess
import sys
import os
import taostest
from utils.log import TDLog
from utils.sql import tdSql
from utils.before_test import BeforeTest
from utils.pytest.util.cluster import cluster as cluster_pytest, ClusterDnodes as ClusterDnodes_pytest
from utils.pytest.util.dnodes import tdDnodes as tdDnodes_pytest
from utils.pytest.util.sql import tdSql as tdSql_pytest
import taos
import taosrest
import taosws
import yaml
import logging

def pytest_addoption(parser):
    parser.addoption("--yaml_file", action="store",
                    help="config YAML file in env directory")
    parser.addoption("-N", action="store", 
                    help="start dnodes numbers in clusters")
    parser.addoption("-M", action="store",
                    help="create mnode numbers in clusters")
    parser.addoption("-R", action="store_true",
                    help="restful realization form")
    parser.addoption("-Q", action="store",
                    help="set queryPolicy in one dnode")
    parser.addoption("--replica", action="store",
                    help="the number of replicas")
    parser.addoption("--skip_test", action="store_true",
                    help="Only do deploy or install without running test")
    parser.addoption("--debug_log", action="store_true",
                    help="Enable debug log output.")
    parser.addoption("--setup_all", action="store_true",
                    help="Setup environment once before all tests running")


def pytest_configure(config):
    log_level = logging.DEBUG if config.getoption("--debug_log") else logging.INFO
    logging.basicConfig(level=log_level, format='%(asctime)s [%(levelname)s]: %(message)s')


@pytest.fixture(scope="session", autouse=True)
def before_test_session(request):
    before_test = BeforeTest(request)
    request.session.before_test = before_test
    # 获取yaml文件，缓存到servers变量中，供cls使用
    if request.config.getoption("--yaml_file"):
        root_dir = request.config.rootdir
        yaml_file = request.config.getoption("--yaml_file")
        # 构建 env 目录下的 yaml_file 路径
        yaml_file_path = os.path.join(root_dir, 'env', yaml_file)

        # 检查文件是否存在
        if not os.path.isfile(yaml_file_path):
            raise pytest.UsageError(f"YAML file '{yaml_file_path}' does not exist.")

        with open(yaml_file_path, "r") as f:
            config_data = yaml.safe_load(f)

        # 解析settings中name=taosd的配置
        servers = []
        for setting in config_data.get("settings", []):
            if setting.get("name") == "taosd":
                endpoint = setting["spec"]["dnodes"][0]["endpoint"]
                host, port = endpoint.split(":")
                servers.append({
                    "host": host,
                    "port": int(port),
                    "config": setting["spec"]["dnodes"][0]["config"]  # 完整的配置信息
                })
        request.session.host = servers[0]["host"]
        request.session.port = servers[0]["port"]
        request.session.user = "root"
        request.session.password = "taosdata"
        request.session.cfg_path = servers[0]["config"]["config_dir"]

    # 解析入参，存入session变量
    else:
        if request.config.getoption("-N"):
            request.session.denodes_num = int(request.config.getoption("-N"))
        else:
            request.session.denodes_num = 1
        
        if request.config.getoption("-R"):
            request.session.restful = True
        else:
            request.session.restful = False
        if request.config.getoption("-Q"):   
            request.session.query_policy = int(request.config.getoption("-Q"))
        else:
            request.session.query_policy = 1
        request.session.host = "localhost"
        request.session.port = 6030
        yaml_file = "ci_default.yaml"
    if request.config.getoption("-M"):
        request.session.mnodes_num = int(request.config.getoption("-M"))
    else:
        request.session.mnodes_num = 1
    if request.config.getoption("--replica"):
        request.session.replicaVar = int(request.config.getoption("--replica"))
    else:
        request.session.replicaVar = 1
    if not request.config.getoption("--setup_all"):
        request.session.setup_all = False
    else:
        request.session.setup_all = True
    

    request.session.root_dir = request.config.rootdir
    request.session.yaml_file = yaml_file


@pytest.fixture(scope="class", autouse=True)
def before_test_class(request):
    '''
    获取session中的配置，建立连接
    断开连接
    '''
    logger = logging.getLogger(__name__)
    logger.debug(f"Current class name: {request.cls.__name__}")

    if request.config.getoption("--yaml_file"):
        request.cls.yaml_file = request.session.yaml_file
        request.cls.host = request.session.host
        request.cls.port = request.session.port
        request.cls.cfg_path = request.session.cfg_path
    else:
        request.session.before_test.ci_init_config(request.session.denodes_num, request.session.query_policy, request.session.restful)
        request.cls.yaml_file = 'ci_default.yaml'
        request.cls.host = "localhost"
        request.cls.port = 6030
        request.cls.cfg_path = os.path.join(os.path.dirname(request.session.root_dir), 'sim', 'dnode0', 'cfg')
    if not request.session.setup_all:
        request.session.before_test.deploy_taos(request.cls.yaml_file, request.session.mnodes_num)
    
    request.cls.dnode_nums = request.session.denodes_num
    request.cls.mnode_nums = request.session.mnodes_num
    request.cls.restful = request.session.restful
    request.cls.query_policy = request.session.query_policy
    request.cls.conn = request.session.before_test.get_taos_conn(request.cls.host, request.cls.port)
    request.cls.tdSql = request.session.before_test.get_tdsql(request.cls.conn)
    request.cls.replicaVar = request.session.replicaVar
    # 为老用例兼容，初始化部分实例
    if request.cls.dnode_nums > 1:
        dnodeslist = cluster_pytest.configure_cluster(dnodeNums=request.cls.dnode_nums, mnodeNums=request.cls.mnode_nums, independentMnode=True)
        tdDnodes_pytest = ClusterDnodes_pytest(dnodeslist)
        tdDnodes_pytest.init("", "")
        tdDnodes_pytest.setValgrind(0)


    tdSql_pytest.init(request.cls.conn.cursor())


    yield

    request.cls.tdSql.close()
    request.cls.conn.close()
