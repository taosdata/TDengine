import time
from testng_taosx.env import ENV
from testng_taosx.constant import EnvType, TaskType, TAOSX_LOG_DIR, TaskStatus
from testng_taosx.task import Task
from testng_taosx.util import TaosAdapter
from testng_taosx.util import Util
import logging
import pytest
import copy

mongodb_test_logger = logging.getLogger(__name__)
task_type = TaskType.MONGODB
logfile = f"{TAOSX_LOG_DIR}/mongodb.log"


@pytest.fixture(scope="module")
def input_data():
    mongodb_test_logger.info("before test...")
    env_data = Util.get_env_data()
    case_data = Util.get_case_data_from_yaml(
        "mongodb/test-mongodb-sanity-basic.yaml", TaskType
    )

    yield env_data, case_data
    mongodb_test_logger.info("after test...")


@pytest.mark.sanity
def test_case_base(input_data):
    """
    用例概述: mongodb 用例, 测试子表字段
    数据源信息: 192.168.1.45:27017, 用户名密码:admin/tbase125!
    用例步骤：
    1. 使用 agent
    2. 配置数据库名： test_ci, 表名： ci_7_1
    3. 查询模版: {"createtime":{"$gte":${start_datetime},"$lt":${end_datetime}}}
    4. 数据起始时间: '2024-07-01 00:00:00+08:00' - '2024-07-31 00:00:00+08:00'

    验证点：
    1. 860条数据正常写入
    """
    mongodb_test_logger.info("start test...sanity case: basic")
    env_data, case_data_orig = input_data
    case_data = copy.deepcopy(case_data_orig)
    case_data["from"]["database"] = "test_ci"
    case_data["from"]["collection"] = "ci_7_1"
    case_data["from"][
        "sql"
    ] = '{"createtime":{"$gte":${start_datetime},"$lt":${end_datetime}}}'
    case_data["from"]["start"] = "2024-07-01T00:00:00+08:00"
    case_data["from"]["end"] = "2024-07-31T00:00:00+08:00"
    mongodb_payload = Util.read_yaml("mongodb/payload-basic.yaml")["parser"]
    case_data["parser"] = mongodb_payload

    Util.create_stable(case_data, mongodb_payload["parser"])
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
    assert rows_count == 860, "入库的数据量应等于 860"
    TaosAdapter.drop_db(ENV.taosd_host, case_data["to"]["target_dbname"])


@pytest.mark.sanity
@pytest.mark.parametrize(
    "database, collection, start, end, expected",
    [
        (
            "test_db1_${Y}",
            "tb_${M}",
            "2020-01-01T00:00:00+08:00",
            "2024-07-31T00:00:00+08:00",
            360,
        ),
        (
            "test_db2_${Y}",
            "tb_${M}_${D}",
            "2020-01-01T00:00:00+08:00",
            "2024-07-31T00:00:00+08:00",
            1793,
        ),
        (
            "test_db4_${Y}",
            "tb_${m}_${d}",
            "2021-12-01T00:00:00+08:00",
            "2023-05-01T00:00:00+08:00",
            810,
        ),
        (
            "test_db5_${y}",
            "tb_${j}",
            "2020-01-01T00:00:00+08:00",
            "2020-03-01T00:00:00+08:00",
            24,
        ),
        (
            "test_db5_${y}",
            "tb_${J}",
            "2020-01-02T00:00:00+08:00",
            "2020-03-01T00:00:00+08:00",
            21,
        ),
    ],
)
def test_case_base_0(database, collection, start, end, expected, input_data):
    """
    用例概述: mongodb 用例, 测试分库分表
    数据源信息: 192.168.1.45:27017, 用户名密码:admin/tbase125!
    用例步骤：
    1. 使用 agent
    2. 配置数据库名,表名,数据起始时间
    3. 查询模版: {"createtime":{"$gte":${start_datetime},"$lt":${end_datetime}}}

    验证点：
    1. 数据正常写入
    """
    mongodb_test_logger.info("start test...sanity case: basic")
    env_data, case_data_orig = input_data
    case_data = copy.deepcopy(case_data_orig)
    case_data["from"]["database"] = database
    case_data["from"]["collection"] = collection
    case_data["from"][
        "sql"
    ] = '{"createtime":{"$gte":${start_datetime},"$lt":${end_datetime}}}'
    case_data["from"]["start"] = start
    case_data["from"]["end"] = end
    mongodb_payload = Util.read_yaml("mongodb/payload-basic.yaml")["parser"]
    case_data["parser"] = mongodb_payload

    Util.create_stable(case_data, mongodb_payload["parser"])
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
    assert rows_count == expected, f"入库的数据量应等于 {expected}"
    TaosAdapter.drop_db(ENV.taosd_host, case_data["to"]["target_dbname"])


@pytest.mark.sanity
@pytest.mark.parametrize("with_agent", [True, False])
def test_case_subtable(with_agent, input_data):
    """
    用例概述: mongodb 用例, 测试子表字段
    数据源信息: 192.168.1.45:27017, 用户名密码:admin/tbase125!
    用例步骤：
    1. 依据 with_agent 的值决定是否使用 agent
    2. 配置数据库名： test_ci, 表名： ci_0920, 子表字段: sn1, sn2
    3. 查询模版: {"createtime":{"$gte":${start_datetime},"$lt":${end_datetime}},${sn1},${sn2}}
    4. 查询排序：{"createtime":1
    5. 数据起始时间: '2024-09-01 00:00:00+08:00' - '2024-09-30 00:00:00+08:00'

    验证点：
    1. 1089条数据正常写入
    2. 总共 100 张子表
    """
    mongodb_test_logger.info("start test...sanity case: 子表字段")
    env_data, case_data_orig = input_data
    case_data = copy.deepcopy(case_data_orig)
    case_data["from"]["database"] = "test_ci"
    case_data["from"]["collection"] = "ci_0920"
    case_data["from"]["subtable_fields"] = "sn1,sn2"
    case_data["from"][
        "sql"
    ] = '{"createtime":{"$gte":${start_datetime},"$lt":${end_datetime}},${sn1},${sn2}}'
    case_data["from"]["sort"] = '{"createtime":1}'
    case_data["from"]["start"] = "2024-09-01T00:00:00+08:00"
    case_data["from"]["end"] = "2024-09-30T00:00:00+08:00"
    case_data["from"]["read_concurrency"] = 10
    mongodb_payload = Util.read_yaml("mongodb/payload-basic.yaml")["parser"]
    mongodb_payload["parser"]["model"]["name"] = "tb_${name}_${sn1}_${sn2}"
    mongodb_payload["parser"]["model"]["tags"] = ["name", "sn1", "sn2"]
    mongodb_payload["parser"]["mutate"][0]["map"]["sn1"] = {
        "cast": "sn1",
        "as": "VARCHAR(128)",
    }
    mongodb_payload["parser"]["mutate"][0]["map"]["sn2"] = {
        "cast": "sn2",
        "as": "VARCHAR(128)",
    }
    case_data["parser"] = mongodb_payload
    if not with_agent:
        case_data.pop("via")

    Util.create_stable(case_data, mongodb_payload["parser"])
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
    assert rows_count == 1089, "入库的数据量应等于 1089"
    sqlresult = TaosAdapter.run_sql(
        env_data["taosadapter_host"],
        f"SELECT count(*) from (select DISTINCT TBNAME from `{case_data['to']['target_dbname']}`.`mongostb`)",
    )
    assert sqlresult["data"][0][0] == 100, "子表数量应等于 100"
    TaosAdapter.drop_db(ENV.taosd_host, case_data["to"]["target_dbname"])
