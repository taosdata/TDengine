import csv
import inspect
import json
import logging
import time

import pytest

from testng_taosx.constant import *
from testng_taosx.env import ENV
from testng_taosx.file import File, TaskType
from testng_taosx.task import Task
from testng_taosx.util import TaosAdapter
from testng_taosx.util import Util

pi_test_logger = logging.getLogger(__name__)
task_type = TaskType.PI


@pytest.fixture(scope="function")
def input_data():
    pi_test_logger.info("before pi test...")
    env_data = Util.get_env_data()

    yield env_data
    pi_test_logger.info("after pi test...")
    

def pi_sanity(env_data, case_data, task, file, files_dir, param):
    TaosAdapter.create_db(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    payload_str = Util.get_task_payload(case_data, env_data)
    file_dir = file.upload(files_dir)
    payload = File.add_file_param(payload_str, param, file_dir)
    task_info = task.create_task(payload)
    pi_test_logger.info(task_info)
    task_id = task_info["id"]
    time.sleep(1)
    task_status = task.get_task_status(task_id)
    assert task_status["status"] == TaskStatus.RUNNING.value, pi_test_logger.error(
        f"task status should be running after created, task status response: {task_status}"
    )
    # waiting for insert
    time.sleep(case_data["task_exec_time"])
    # stop task
    task.stop_task(task_id)
    task_status = task.get_task_status(task_id)
    task.stop_task_with_retry(task_id)
    return task_id


@pytest.mark.sanity
def test_multicol_template(input_data):
    pi_test_logger.info(f"running test case...{inspect.currentframe().f_code.co_name}")
    env_data = input_data
    case_data = Util.get_case_data_from_yaml("pi/test_multicol.yaml", task_type)
    files_dir = "pi/pi_multicol_template.csv"
    param = "transform_config_file"
    task = Task(env_data, case_data)
    file = File(env_data, task_type)
    task_id = pi_sanity(env_data, case_data, task, file, files_dir, param)
    metrics = task.get_task_metrics(task_id)
    stable_count = TaosAdapter.run_sql(
        ENV.taosd_host, f"""show {case_data["to"]["target_dbname"]}.stables"""
    )
    row_count = TaosAdapter.check_db_count(
        ENV.taosd_host, case_data["to"]["target_dbname"]
    )
    assert (
        stable_count["rows"] == metrics["current"]["created_stables"]
    ), f"test case {inspect.currentframe().f_code.co_name} failed,stable count in TDengine is wrong!"
    assert (
        row_count == metrics["current"]["written_rows"]
    ), f"test case {inspect.currentframe().f_code.co_name} failed, insert rows wrong!"
    TaosAdapter.drop_db(ENV.taosd_host, case_data["to"]["target_dbname"])

@pytest.mark.sanity
def test_singlecol_template(input_data):
    pi_test_logger.info(f"running test case...{inspect.currentframe().f_code.co_name}")
    env_data = input_data
    case_data = Util.get_case_data_from_yaml(
        "pi/test_singlecol_template.yaml", task_type
    )
    files_dir = "pi/pi_singlecol_template.csv"
    param = "transform_config_file"
    task = Task(env_data, case_data)
    file = File(env_data, task_type)
    task_id = pi_sanity(env_data, case_data, task, file, files_dir, param)
    metrics = task.get_task_metrics(task_id)
    row_count = TaosAdapter.check_db_count(
        ENV.taosd_host, case_data["to"]["target_dbname"]
    )
    assert (
        row_count == metrics["current"]["written_rows"]
    ), f"test case {inspect.currentframe().f_code.co_name} failed, insert rows wrong!"
    TaosAdapter.drop_db(ENV.taosd_host, case_data["to"]["target_dbname"])

@pytest.mark.sanity
def test_singlecol_point(input_data):
    pi_test_logger.info(f"running test case...{inspect.currentframe().f_code.co_name}")
    env_data = input_data
    case_data = Util.get_case_data_from_yaml("pi/test_singlecol_point.yaml", task_type)
    files_dir = "pi/pi_singlecol_point.csv"
    param = "transform_config_file"
    task = Task(env_data, case_data)
    file = File(env_data, task_type)
    task_id = pi_sanity(env_data, case_data, task, file, files_dir, param)
    metrics = task.get_task_metrics(task_id)
    row_count = TaosAdapter.check_db_count(
        ENV.taosd_host, case_data["to"]["target_dbname"]
    )
    assert (
        row_count == metrics["current"]["written_rows"]
    ), f"test case {inspect.currentframe().f_code.co_name} failed, insert rows wrong!"
    TaosAdapter.drop_db(ENV.taosd_host, case_data["to"]["target_dbname"])

