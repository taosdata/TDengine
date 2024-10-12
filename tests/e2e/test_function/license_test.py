import json
import logging
import time

import allure
import pytest

from testng_taosx.constant import TaskType
from testng_taosx.task import Task
from testng_taosx.util import Util

license_test_logger = logging.getLogger(__name__)
task_type = TaskType.INFLUXDB


@pytest.fixture(scope="function")
def input_data():
    license_test_logger.info("before license test...")
    env_data = Util.get_env_data()
    # activate the license that only 1 connector num for influxdb
    Util.activate_data_in(env_data["taosd_host"], ["InfluxDB,un,1,un"])

    yield env_data

    license_test_logger.info("after license test...")
    # activate the license that unlimited connector num for influxdb
    Util.activate_data_in(env_data["taosd_host"], ["InfluxDB,un,un,un"])


@allure.link("https://jira.taosdata.com:18080/browse/TD-31628")
def test_connector_num(input_data):
    """
    用例概述：当授权的数据源连接数为 1 时，仅能对一个数据源（ip 和 port 均相同）创建任务
    用例步骤：
    1. 授权连接数为 1
    2. 创建 2 个任务，且这两个任务使用同一个 InfluxDB 2.7 数据源
    3. 创建第 3 个任务，这个任务使用 InfluxDB 1.8 数据源，ip+port 与前两个任务不同
    4. 授权连接数改为 unlimited
    验证点：
    1. 前两个任务都可以成功运行，第三个任务提交的时候会报错：License error
    """

    env_data = input_data
    case_data = Util.get_case_data_from_yaml("influxdb/test_influxdb.yaml", task_type)

    task1 = Task(env_data, case_data)
    task2 = Task(env_data, case_data)
    task1_info = task1.sanity_test_create_task(additional_params={"precision": "ns"})
    task2_info = task2.sanity_test_create_task(additional_params={"precision": "ns"})
    task1_id = task1_info["id"]
    task2_id = task2_info["id"]

    time.sleep(5)

    metrics1 = task1.get_task_metrics(task1_id)
    metrics2 = task2.get_task_metrics(task2_id)
    metrics1_records = metrics1["current"]["processed_rows"]
    metrics2_records = metrics2["current"]["processed_rows"]

    assert metrics1_records > 0 and metrics2_records > 0, license_test_logger.error(
        f"task1 and task2 should run successfully,"
        f"metrics1_records: {metrics1_records}, metrics2_records: {metrics2_records}"
    )

    case_data = Util.get_case_data_from_yaml(
        "influxdb/test_influxdb_1_8.yaml", task_type
    )
    task = Task(env_data, case_data)
    payload = Util.get_task_payload(case_data=case_data, env_data=env_data)
    r = task.create_task_raw(payload)
    status_code = r.status_code
    error_message = json.loads(r.text)["message"]
    assert (
        status_code == 500 and "license error" in error_message.lower()
    ), license_test_logger.error(
        f"License error should be returned when creating task with different data source,"
        f"status_code: {status_code}, error_message: {error_message}"
    )

    task1.stop_task_with_retry(task1_id)
    task2.stop_task_with_retry(task2_id)
    task1.delete_task(task1_id)
    task2.delete_task(task2_id)
