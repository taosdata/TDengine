import logging
import time

import pytest

from testng_taosx.constant import EnvType, TaskType, TAOSX_LOG_DIR, TaskStatus
from testng_taosx.env import ENV
from testng_taosx.task import Task
from testng_taosx.util import TaosAdapter
from testng_taosx.util import Util

mssql_test_logger = logging.getLogger(__name__)
task_type = TaskType.MSSQL
logfile = f"{TAOSX_LOG_DIR}/mysql.log"
mssql_invalid_conf_status = TaskStatus.INTERRUPTED.value


@pytest.fixture(scope="module")
def input_data():
    mssql_test_logger.info("before test...")
    env_data = Util.get_env_data()
    case_data = Util.get_case_data_from_yaml(
        "mssql/test-mssql-sanity-basic.yaml", task_type
    )
    TaosAdapter.create_db(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    yield env_data, case_data
    mssql_test_logger.info("after test...")
    TaosAdapter.drop_db(ENV.taosd_host, case_data["to"]["target_dbname"])


@pytest.mark.sanity
def test_case_base(input_data):
    """
    用例概述: sql server 用例, 基本用例
    数据源信息: 192.168.1.66:3433, 数据库:ci_test, 用户名密码:test/tbase125!
    用例步骤：
    1. 使用 agent
    2. 查询模版: select * from ci_test.dbo.TestTable where dDateTimeOffset > ${start} and dDateTimeOffset < ${end}
    3. 数据起始时间: '2024-07-05 08:00:00+08:00' - '2024-07-06 08:00:00+08:00'

    验证点：
    1. 1000条数据正常写入
    """
    mssql_test_logger.info("start test...sanity case: basic")
    env_data, case_data = input_data
    mssql_payload = Util.read_yaml("mssql/payload-basic.yaml")["parser"]
    # 全数据类型字段数据迁移测试
    # 在目标 taosd 中创建超级表,其中主键列名称为ts
    case_data["parser"] = mssql_payload
    Util.create_stable(case_data, mssql_payload)
    payload = Util.get_task_payload(case_data, env_data, EnvType.LOCAL)
    task = Task(env_data, case_data)
    task_info = task.create_task(payload)
    # 等待任务结束
    while True:
        task_status = task.get_task_status(task_info["id"])
        if task_status["status"] == "completed":
            break
        else:
            time.sleep(5)
    rows_count = TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    assert rows_count == 1000, mssql_test_logger.error(
        "test case failed: target count should be same as source"
    )
