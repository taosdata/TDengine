
import pytest
import os
@pytest.fixture(scope="package", autouse=True)
def before_test_package(request):
    '''
    获取session中的配置，建立连接
    创建package级别的数据库
    断开连接
    '''
    # 获取当前测试文件的路径
    test_file_path = request.fspath
    # 获取包名
    package = os.path.dirname(test_file_path).split(os.sep)[-1]  # 获取最后一层目录名
    tdLog = request.session.tdLog
    tdLog.debug(f"[package.conftest.py.before_test_package] Current package name: {package}")
    db_name = package
    request.session.before_test.create_database(request, db_name)
    #request.module.db_name = db_name

    host = request.session.host
    port = request.session.port
    user = request.session.user
    password = request.session.password


    