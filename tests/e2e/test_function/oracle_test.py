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
task_type = TaskType.ORACLE
logfile = f"{TAOSX_LOG_DIR}/oracle.log"


@pytest.fixture(scope="module")
def input_data():
    psql_test_logger.info("before test...")
    env_data = Util.get_env_data()
    case_data = Util.get_case_data_from_yaml(
        "oracle/test-oracle-sanity-basic.yaml", TaskType
    )

    yield env_data, case_data
    psql_test_logger.info("after test...")


@pytest.mark.sanity
def test_case_base(input_data):
    """
    用例概述: oracle 用例, 基本用例
    用例步骤：
    1. 使用 agent
    2. 查询模版: select * from taosx_test_ci where t_time >= ${start} and t_time < ${end}
    3. 数据起始时间: '2024-05-28 00:00:00+08:00' - '2024-05-31 00:00:00+08:00'

    验证点：
    1. 1000条数据正常写入
    """
    psql_test_logger.info("start test...sanity case: basic")
    env_data, case_data_orig = input_data
    case_data = copy.deepcopy(case_data_orig)
    pg_payload = Util.read_yaml("oracle/payload-basic.yaml")["parser"]
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
    assert rows_count == 1000, "入库的数据量应等于 1000"
    TaosAdapter.drop_db(ENV.taosd_host, case_data["to"]["target_dbname"])


@pytest.mark.sanity
def test_case_base_2_sharding(input_data):
    """
    用例概述: oracle 用例, 分库分表, 占位符使用${Ymd}
    用例步骤：
    1. 使用 agent
    2. 查询模版: select * from taosx_test_ci_${Ymd} where t_time>${start} and t_time<${end}
    3. 数据起始时间: '2024-05-25 00:00:00+08:00' - '2024-05-30 18:00:00+08:00'

    验证点：
    1. 60条数据正常写入
    """
    psql_test_logger.info("start test...sanity case: basic")
    env_data, case_data_orig = input_data
    case_data = copy.deepcopy(case_data_orig)
    pg_payload = Util.read_yaml("oracle/payload-basic.yaml")["parser"]
    case_data["parser"] = pg_payload
    case_data["from"][
        "sql"
    ] = "select * from taosx_test_ci_${Ymd} where t_time>${start} and t_time<${end}"
    case_data["from"]["start"] = "2024-05-25T00:00:00+08:00"
    case_data["from"]["end"] = "2024-05-30T18:00:00+08:00"

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
    assert rows_count == 60, "入库的数据量应等于 60"
    TaosAdapter.drop_db(ENV.taosd_host, case_data["to"]["target_dbname"])
