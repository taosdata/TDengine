import copy
import logging
from typing import Dict

import pytest

from testng_taosx.constant import TaskType
from testng_taosx.env import ENV, Env
from testng_taosx.file import File
from testng_taosx.task import Task
from testng_taosx.util import Util, DataSetRequest, TaosAdapter

opentsdb_test_logger = logging.getLogger(__name__)
task_type = TaskType.OPENTSDB


@pytest.fixture(scope="function")
def input_data():
    opentsdb_test_logger.info("before opentsdb test...")
    env_data = Util.get_env_data()

    yield env_data

    opentsdb_test_logger.info("after opentsdb test...")
    TaosAdapter.drop_db(env_data["taosadapter_host"], "ci_opentsdb")


def opentsdb_sanity_save(
    input_data, case_data, files_to_upload: Dict = None, case_data_modify: Dict = None
):
    env_data = input_data
    file = File(env_data, task_type)
    additional_params = None
    if files_to_upload:
        additional_params = {}
        for key, value in files_to_upload.items():
            additional_params[key] = file.upload(value)
    if case_data_modify:
        for key, value in case_data_modify.items():
            case_data["from"][key] = value
    task = Task(env_data, case_data)
    metrics = task.sanity_test(additional_params=additional_params)
    # 1.入库数据应与 Metrics 应匹配
    rows_count = TaosAdapter.check_db_count(
        ENV.taosadapter_host, case_data["to"]["target_dbname"]
    )
    assert rows_count > 0
    assert metrics["current"]["written_rows"] > 0
    return case_data


# opentsdb simple save
# 使用的配置文件只包含必填列
# 使用的参数只包含必填参数
@pytest.mark.sanity
def test_opentsdb_sanity_simple_save(input_data):
    opentsdb_test_logger.info("start test_sanity_simple_save...")
    case_data = Util.get_case_data_from_yaml(
        "opentsdb/test_opentsdb_simple_save.yaml", task_type
    )
    opentsdb_sanity_save(input_data, case_data)


# 测试获取 OpenTSDB 的 metrics
def opentsdb_get_metrics(input_data):
    opentsdb_test_logger.info("start test_opentsdb_get_metrics...")
    case_data = Util.get_case_data_from_yaml(
        "opentsdb/test_opentsdb_simple_save.yaml", task_type
    )
    from_dsn = Util.get_task_payload(case_data, input_data)
    data_set_request = DataSetRequest(
        from_dsn["from"], via=from_dsn["via"], pattern="api", categories=["nodes"]
    )
    data_set = Util.get_data_set(data_set_request)
    assert data_set != "", opentsdb_test_logger.error(
        "opentsdb dataset shoudn't be empty"
    )
    # 返回示例：[{"id": "[\"test\",\"test2\",\"test3\",\"test4\",\"zqsong0921\",\"zqsong1026_1kw\"]\n"} ]
    # 注意：这个格式不够通用，这里解析之后可能后期会有修改
    return data_set[0]["id"]


@pytest.mark.sanity
def test_opentsdb_sanity_simple_save_with_specific_metrics(input_data):
    opentsdb_test_logger.info(
        "start test_opentsdb_sanity_simple_save_with_specific_metrics..."
    )
    case_data = Util.get_case_data_from_yaml(
        "opentsdb/test_opentsdb_simple_save.yaml", task_type
    )
    case_data["from"]["metrics"] = opentsdb_get_metrics(input_data)[:-2][1:].replace(
        '"', ""
    )
    opentsdb_sanity_save(input_data, case_data)


# 使用的参数包含所有参数
@pytest.mark.sanity
def test_opentsdb_complicated_save_with_max_value(input_data):
    opentsdb_test_logger.info("start test_opentsdb_complicated_save_with_max_value...")
    env_data = input_data
    # 1.任务正常创建
    case_data = Util.get_case_data_from_yaml(
        "opentsdb/test_opentsdb_complicated_save.yaml", task_type
    )
    case_data["from"]["readWindow"] = 60
    case_data["from"]["delay"] = 30
    #    case_data["from"]["maxThread"] = 150
    #    case_data["from"]["queueSizeT"] = 2000
    #    case_data["from"]["queueSizeD"] = 500000
    #    case_data["from"]["limitSpeed"] = 500000
    case_data = opentsdb_sanity_save(input_data, case_data)


@pytest.mark.sanity
def test_opentsdb_complicated_save_with_min_value(input_data):
    opentsdb_test_logger.info("start test_sanity_complicated_save...")
    env_data = input_data
    # 1.任务正常创建
    case_data = Util.get_case_data_from_yaml(
        "opentsdb/test_opentsdb_complicated_save.yaml", task_type
    )
    case_data["from"]["readWindow"] = 60  # 这里时间不能设置过小，否则短时间内任务获取不到 metrics
    case_data["from"]["delay"] = 1
    #    case_data["from"]["maxThread"] = 1
    #    case_data["from"]["queueSizeT"] = 10
    #    case_data["from"]["queueSizeD"] = 10000
    #    case_data["from"]["limitSpeed"] = 5000
    opentsdb_sanity_save(input_data, case_data)


@pytest.mark.performance
def test_case_performance_scenario1(input_data):
    opentsdb_test_logger.info("start opentsdb performance test...")
    env_data = input_data
    case_data = Util.get_case_data_from_yaml(
        "opentsdb/test_opentsdb_performance.yaml", task_type
    )
    case_data["to"]["target_dbname"] = "perf_opentsdb_s1"
    case_data["to"]["column_count"] = 2
    case_data["task_exec_time"] = 10 * 60

    TaosAdapter.create_db(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    task = Task(env_data, case_data)
    payload = Util.get_task_payload(case_data, env_data)
    task.perf_test(
        payload,
        1,
        "1 task to subscribe 1 metric,1 stable,10w subtables, 1000 per subtable, ts column+1 columns(double)",
        True,
    )
