import pytest
import subprocess
import sys
import os
import taostest


def pytest_addoption(parser):
    parser.addoption("--deploy", action="store", default="",
                     help="Path to the environment config YAML file")
    parser.addoption("--env", action="store", default="production", help="Environment to deploy")


@pytest.fixture(scope="session", autouse=True)
def deploy_environment(request):

    # 获取用例文件的目录和文件名
    test_file_path = request.node.fspath
    test_dir = os.path.dirname(test_file_path)
    test_file_name = os.path.splitext(os.path.basename(test_file_path))[0]
    
    # 检查同名的 YAML 文件是否存在
    yaml_file_path = os.path.join(test_dir, f"{test_file_name}.yaml")
    if os.path.exists(yaml_file_path):
        deploy_file = yaml_file_path  # 使用同名的 YAML 文件
    else:
        deploy_file = request.config.getoption("--deploy")  # 使用默认的 deploy

    #env = request.config.getoption("--env")
    #config_file = request.config.getoption("--config")
    #env = request.config.getoption("--env")

    # 调用环境部署代码
    print(f"Deploying environment with config: {deploy_file}")

    # 假设 deploy.py 中有一个 deploy_environment 函数
    subprocess.run(["taostest", "--setup", deploy_file], check=True)

    yield  # 测试执行前的环境部署

    # 可选：在测试完成后进行清理
    print("Cleaning up environment...")
    #subprocess.run(["python", "deploy.py", "--cleanup"], check=True)