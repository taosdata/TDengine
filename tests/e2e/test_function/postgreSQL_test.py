import time
from testng_taosx.env import ENV
from testng_taosx.constant import EnvType, TaskType, TAOSX_LOG_DIR, TaskStatus
from testng_taosx.task import Task
from testng_taosx.util import TaosAdapter
from testng_taosx.util import Util
import logging
import pytest
import copy

psql_test_logger = logging.getLogger(__name__)
task_type = TaskType.POSTGRESQL
logfile = f"{TAOSX_LOG_DIR}/pg.log"


@pytest.fixture(scope="module")
def input_data():
    psql_test_logger.info("before test...")
    env_data = Util.get_env_data()
    case_data = Util.get_case_data_from_yaml(
        "postgreSQL/test-postgres-sanity-basic.yaml", TaskType
    )

    yield env_data, case_data
    psql_test_logger.info("after test...")


@pytest.mark.sanity
def test_case_base(input_data):
    """
    用例概述: postgresql 用例, 基本用例，只提供必填字段
    数据源信息: 192.168.1.45:5432, 数据库:test, 用户名密码:postgres/tbase125!
    用例步骤：
    1. 任务使用 agent
    2. SQL 模版: select * from public.pg_ci where ttimezone >= ${start} and ttimezone < ${end}
    3. 数据起始时间: '2024-05-07 00:00:00+08:00' - '2024-05-08 00:00:00+08:00'
    数据源信息：

    验证点：
    1. 1000条数据正常写入
    """
    psql_test_logger.info("start test...sanity case: basic")
    env_data, case_data_orig = input_data
    case_data = copy.deepcopy(case_data_orig)
    pg_payload = Util.read_yaml("postgreSQL/payload-basic.yaml")["parser"]
    case_data["parser"] = pg_payload

    Util.create_stable(case_data, pg_payload["parser"])
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
    assert rows_count == 1000, psql_test_logger.error(
        "test case failed: target count should be same as source"
    )
    TaosAdapter.drop_db(ENV.taosd_host, case_data["to"]["target_dbname"])


@pytest.mark.sanity
def test_case_base_2_sharding(input_data):
    """
    用例概述: postgresql 用例, 分库分表
    数据源信息: 192.168.1.45:5432, 数据库:test, 用户名密码:postgres/tbase125!
    用例步骤：
    1. 任务使用 agent
    2. SQL 模版: select sint ,concat('${F}', 'T', ttnozone, '+08:00') as ts from public.pg_ci_${Ymd} where ttnozone >=${start_time} and ttnozone <${end_time}
    3. 数据起始时间: '2024-04-22 10:00:00+08:00' - '2024-04-30 18:00:00+08:00'

    验证点：
    1. 13条数据正常写入
    """
    psql_test_logger.info("start test...sanity case: basic")
    env_data, case_data_orig = input_data
    case_data = copy.deepcopy(case_data_orig)
    pg_payload = Util.read_yaml("postgreSQL/payload-basic-case2.yaml")["parser"]
    case_data["parser"] = pg_payload
    case_data["from"][
        "sql"
    ] = "select sint ,concat('${F}', 'T', ttnozone, '+08:00') as ts from public.pg_ci_${Ymd} where ttnozone >=${start_time} and ttnozone <${end_time}"
    case_data["from"]["start"] = "2024-04-22T10:00:00+08:00"
    case_data["from"]["end"] = "2024-04-30T18:00:00+08:00"

    Util.create_stable(case_data, pg_payload["parser"])
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
    assert rows_count == 13, psql_test_logger.error(
        "test case failed: target count should be same as source"
    )
    TaosAdapter.drop_db(ENV.taosd_host, case_data["to"]["target_dbname"])


@pytest.mark.sanity
@pytest.mark.parametrize("with_agent", [True, False])
def test_case_base_subtable(with_agent, input_data):
    """
    用例概述: postgresql 用例, 测试子表字段
    数据源信息: 192.168.1.45:5432, 数据库:test, 用户名密码:postgres/tbase125!
    用例步骤：
    1. 依据 with_agent 的值决定是否使用 agent
    2. 配置子表字段: select distinct sint,cchar from public.pg_ci
    3. SQL 模版: select * from public.pg_ci where ttimezone >= ${start} and ttimezone < ${end} and ${sint} and ${cchar} order by ttimezone
    4. 数据起始时间: '2024-05-07 00:00:00+08:00' - '2024-05-08 00:00:00+08:00'

    验证点：
    1. 1000条数据正常写入
    """
    psql_test_logger.info("start test...sanity case: subtable_fields")
    env_data, case_data_orig = input_data
    case_data = copy.deepcopy(case_data_orig)
    pg_payload = Util.read_yaml("postgreSQL/payload-basic.yaml")["parser"]
    case_data["parser"] = pg_payload
    case_data["from"][
        "sql"
    ] = "select * from public.pg_ci where ttimezone >= ${start} and ttimezone < ${end} and ${sint} and ${cchar} order by ttimezone"
    case_data["from"][
        "subtable_fields"
    ] = "select distinct sint,cchar from public.pg_ci"
    case_data["from"]["read_concurrency"] = 10
    if not with_agent:
        case_data.pop("via")

    Util.create_stable(case_data, pg_payload["parser"])
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
    assert rows_count == 1000, psql_test_logger.error(
        "test case failed: target count should be same as source"
    )
    TaosAdapter.drop_db(ENV.taosd_host, case_data["to"]["target_dbname"])
