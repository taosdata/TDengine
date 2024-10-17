import logging
import time

import pytest

from testng_taosx.task import Task
from testng_taosx.util import TaosAdapter
from testng_taosx.util import Util
from testng_taosx.constant import TaskType

influxdb_test_logger = logging.getLogger(__name__)
task_type = TaskType.INFLUXDB


@pytest.fixture(scope="function")
def input_data():
    influxdb_test_logger.info("before influxdb test...")
    env_data = Util.get_env_data()

    yield env_data

    influxdb_test_logger.info("after influxdb test...")
    TaosAdapter.drop_db(env_data["taosadapter_host"], "testinflux")


@pytest.mark.sanity
def test_sanity_basic(input_data):
    env_data = input_data
    case_data = Util.get_case_data_from_yaml("influxdb/test_influxdb.yaml", task_type)

    taosadapter_addr = env_data["taosadapter_host"]
    target_dbname = case_data["to"]["target_dbname"]
    task = Task(env_data, case_data)
    metrics = task.sanity_test(additional_params={"precision": "ns"})
    metrics_records = metrics["current"]["processed_rows"]
    inserted_rows = TaosAdapter.check_db_count(taosadapter_addr, target_dbname)
    source_rows_count = case_data["source_bucket"]["record_number"]
    assert inserted_rows > 0
    assert metrics_records > 0


@pytest.mark.sanity
@pytest.mark.xfail(
    reason="不稳定，单个执行可能存在偶尔失败的情况，还未找到原因，暂时标记为xfail"
)
def test_sanity_1_8(input_data):
    """
    测试用例 2：测试 1.8 influxdb 数据迁移到 taosd
    版本设置为 1.8
    起止时间为：2023-06-01T00:00:00+08:00 - 2024-04-01T00:00:00+08:00
    bucket 为 zqsong
    """
    env_data = input_data
    case_data = Util.get_case_data_from_yaml(
        "influxdb/test_influxdb_1_8.yaml", task_type
    )
    taosadapter_addr = env_data["taosadapter_host"]
    target_dbname = case_data["to"]["target_dbname"]
    task = Task(env_data, case_data)
    metrics = task.sanity_test(additional_params={"precision": "ns"})
    metrics_records = metrics["current"]["processed_rows"]
    inserted_rows = TaosAdapter.check_db_count(taosadapter_addr, target_dbname)
    assert inserted_rows > 0
    assert metrics_records > 0


@pytest.mark.performance
def test_case_performance_scenario1(input_data):
    env_data = input_data
    case_data = Util.get_case_data_from_yaml("influxdb/test_influxdb.yaml", task_type)
    case_data["from"]["bucket"] = "1k_subtable_10w_12column_20240115"
    case_data["from"]["beginTime"] = "2024-01-15T00:00:00+08:00"
    case_data["from"]["endTime"] = "2024-01-16T00:00:00+08:00"
    case_data["to"]["target_dbname"] = "perf_influxdb_s1"
    case_data["to"]["column_count"] = 13
    case_data["task_exec_time"] = 60 * 10

    TaosAdapter.create_db(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"], "ns"
    )
    task = Task(env_data, case_data)
    payload = Util.get_task_payload(case_data, env_data)
    task.perf_test(
        payload,
        1,
        "1 task,1 stable,1000 subtables,ts column+4 columns(int)+4 columns(double) + 4column(string)",
        True,
    )
