import pytest
import subprocess
import sys
import os
import taostest
from utils.log import TDLog
from utils.sql import tdSql
from utils.before_test import BeforeTest
import taos
import taosrest
import taosws
import yaml


def pytest_addoption(parser):
    parser.addoption("--yaml_file", action="store", default="default.yaml",
                    help="config YAML file in env directory")
    parser.addoption("--deploy", action="store_true",
                    help="Whether to deploy environment before running test")
    parser.addoption("--install", action="store_true",
                    help="Install TDengine before deploy")
    parser.addoption("--skip_test", action="store_true",
                    help="Only do deploy or install without running test")
    parser.addoption("--debug_log", action="store_true",
                    help="Enable debug output.")
    parser.addoption("--destroy", action="store_true",
                    help="Destroy environment after running test")


@pytest.fixture(scope="session", autouse=True)
def before_test_session(request):

    # 检查 --yaml_file 参数是否被提供

    if request.config.getoption("--yaml_file") is None:
        raise pytest.UsageError("--yaml_file is a required parameter.")
    
    # 获取根目录
    root_dir = request.config.rootdir
    yaml_file = request.config.getoption("--yaml_file")
    # 构建 env 目录下的 yaml_file 路径
    yaml_file_path = os.path.join(root_dir, 'env', yaml_file)
    
    # 检查文件是否存在
    if not os.path.isfile(yaml_file_path):
        raise pytest.UsageError(f"YAML file '{yaml_file_path}' does not exist.")

    if request.config.getoption("--debug_log"):
        tdLog = TDLog(debug_log=True)
    else:
        tdLog = TDLog()
    request.session.tdLog = tdLog
    before_test = BeforeTest(request)
    if request.config.getoption("--install"):
        before_test.install_taos(request)
    if request.config.getoption("--deploy"):
        before_test.deploy_taos(request)
    if request.config.getoption("--skip_test"):
        pytest.skip("Skipping tests as per --skip_test option.")
    else:
        before_test.configure_test(request)
        request.session.before_test = before_test
    
    yield

    if request.config.getoption("--destroy"):
        before_test.destroy()


#@pytest.fixture(scope="package", autouse=True)
#def before_test_package(request):
#    '''
#    获取session中的配置，建立连接
#    创建package级别的数据库
#    断开连接
#    '''
#    module_name = request.module.__name__
#    tdLog = request.session.tdLog
#    tdLog.debug(f"Current module name: {module_name}")
#    db_name = module_name.split('.')[-1]
#    request.session.before_test.create_database(request, db_name)
#    request.module.db_name = db_name

#    host = request.session.host
#    port = request.session.port
#    user = request.session.user
#    password = request.session.password
    
@pytest.fixture(scope="class", autouse=True)
def before_test_class(request):
    '''
    获取session中的配置，建立连接
    断开连接
    '''
    tdLog = request.session.tdLog
    tdLog.debug(f"[conftest.py.before_test_class] Current class name: {request.cls.__name__}")
    request.cls.DEPLOY_HOST = request.session.host
    request.cls.DEPLOY_PORT = request.session.port
    request.cls.DEPLOY_USER = request.session.user
    request.cls.DEPLOY_PASSWORD = request.session.password
    request.cls.conn = request.session.before_test.get_taos_conn()
    request.cls.tdSql = request.session.before_test.get_tdsql(request.cls.conn)
    request.cls.db_name = os.path.dirname(request.fspath).split(os.sep)[-1]  # 获取最后一层目录名
    tdLog.debug(f"[conftest.py.before_test_class] Current db_name: {request.cls.db_name}")
    request.cls.tdLog = tdLog

    yield

    request.cls.conn.close()


#@pytest.fixture(scope="class", autouse=True)
#def deploy_environment(request):
#
#    # 获取用例文件的目录和文件名
#    # 以下获取路径方法只适合scope=class的运行，如果scope为module或者session，获取的路径会在用例上层，需要修改
#    test_file_path = request.node.fspath
#    tdLog.info(f"test_file_path: {test_file_path}")
#    test_dir = os.path.dirname(test_file_path)
#    tdLog.info(f"test_dir: {test_dir}")
#    test_file_name = os.path.splitext(os.path.basename(test_file_path))[0]
#    tdLog.info(f"test_file_name: {test_file_name}")
#    env_path = test_dir.split('test')[0] + 'test'
#    tdLog.info(f"env_path: {env_path}")
    
#    # 检查同名的 YAML 文件是否存在
#    yaml_file_path = os.path.join(env_path, "env")
#    yaml_file_case = f"{test_file_name}.yaml"
#    if os.path.exists(yaml_file_case):
#        deploy_file = yaml_file_case  # 使用同名的 YAML 文件
#    else:
#        deploy_file = request.config.getoption("--deploy")  # 使用默认的 deploy

#    # 初始化框架工作目录
#    if not deploy_file:
#        #taostest_cmd = ["taostest", "--env_init", "--init"]
#        deploy_file = "default.yaml"
    #else:
    #    taostest_cmd = ["taostest", "--init"]
#    try:
#        env_vars = os.environ.copy()
#        env_vars['TEST_ROOT'] = test_file_path  # 将新路径添加到 PATH 的前面
#        process = subprocess.Popen(["taostest", "--init"],
#                               stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, env=env_vars)
        # 向子进程发送输入
#        process.stdin.write("\n")
#        process.stdin.flush()  # 确保输入被发送到子进程
        # 关闭子进程的stdin，防止它无限期等待更多输入
#        process.stdin.close()
        # 等待子进程结束
#        process.wait()
        # subprocess.run(["taostest", "--init"], check=True, env=env_vars)
#    except Exception as e:
#        tdLog.exit(f"Error run taostest --init: {e}")

    # 调用环境部署代码
#    print(f"Deploying environment with config: {deploy_file}")

    # 假设 deploy.py 中有一个 deploy_environment 函数
#    try:
#        subprocess.run(["taostest", "--setup", deploy_file], check=True)
#    except Exception as e:
#        tdLog.exit("Error run taostest --setup")

    # 获取部署信息
    # 读取 YAML 文件内容
#    with open(os.path.join(yaml_file_path, f"{deploy_file}"), 'r') as file:
#        deploy_data = yaml.safe_load(file)  # 使用 safe_load 读取内容
#    firstEP = deploy_data['settings'][0]['spec']['config']['firstEP']   # 获取 firstEP 字段
#    host = firstEP.split(':')[0]
#    port = int(firstEP.split(':')[1])
    # 判断是否存在 name 为 taosAdapter 的部分
#    taos_adapter_exists = any(item.get('name') == 'taosAdapter' for item in deploy_data['settings'])
#    rest_url = f"http://{host}:6041"
#    ws_url = f"ws://{host}:6041"

#    request.cls.DEPLOY_HOST = host
#    request.cls.DEPLOY_PORT = port
#    request.cls.DEPLOY_USER = "root"
#    request.cls.DEPLOY_PASSWORD = "taosdata"
#    try:
#        request.cls.DEPLOY_TAOS_CONN = taos.connect(host=host, port=port)
#        if taos_adapter_exists:
#            request.cls.DEPLOY_TAOS_REST_CONN = taosrest.connect(url=rest_url)
#            request.cls.DEPLOY_TAOS_WS_CONN = taosws.connect(url=ws_url)
#    except Exception as e:
#        tdLog.info(f"Error connect to taos: {e}")
#    denodes = []
#    for dnode in deploy_data['settings'][0]['spec']['dnodes']:
#        tdLog.info(f"dnode: {dnode}")
#        dnode_info = {}
#        dnode_info['host'] = dnode['endpoint'].split(':')[0]
#        dnode_info['port'] = int(dnode['endpoint'].split(':')[1])
#        dnode_info['dataDir'] = dnode['config']['dataDir']
#        dnode_info['logDir'] = dnode['config']['logDir']
#        dnode_info['config_dir'] = dnode['config_dir']
#        denodes.append(dnode_info)
#    request.cls.DEPLOY_DNODES = denodes
#    tdSql.init(request.cls.DEPLOY_TAOS_CONN.cursor())
#    request.cls.tdSql = tdSql
#    yield  # 测试执行前的环境部署

    # 可选：在测试完成后进行清理
#    print("Cleaning up environment...")
#    if taos_adapter_exists:
#        request.cls.DEPLOY_TAOS_WS_CONN.close()
#        request.cls.DEPLOY_TAOS_REST_CONN.close()
#    request.cls.DEPLOY_TAOS_CONN.close()
#    subprocess.run(["taostest", "--destroy", deploy_file], check=False)