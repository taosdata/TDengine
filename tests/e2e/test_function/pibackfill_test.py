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
task_type = TaskType.PIBACKFILL


@pytest.fixture(scope="function")
def input_data():
    pi_test_logger.info("before pibackfill test...")
    env_data = Util.get_env_data()
    yield env_data
    pi_test_logger.info("after pibackfill test...")


def pibackfill_sanity(env_data, case_data, task, file, files_dir, param):
    TaosAdapter.create_db(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    payload_str = Util.get_task_payload(case_data, env_data)
    file_dir = file.upload(files_dir)
    payload = File.add_file_param(payload_str, param, file_dir)
    task_info = task.create_task(payload)
    while True:
        task_status = task.get_task_status(task_info["id"])
        if task_status["status"] == "completed":
            break
        else:
            time.sleep(10)
    return task_info["id"]


@pytest.mark.sanity
def test_multicol_template(input_data):
    pi_test_logger.info(f"running test case...{inspect.currentframe().f_code.co_name}")
    env_data = input_data
    case_data = Util.get_case_data_from_yaml(
        "pi/test_backfill_multicol.yaml", task_type
    )
    files_dir = "pi/pi_multicol_template.csv"
    param = "transform_config_file"

    task = Task(env_data, case_data)
    file = File(env_data, task_type)
    task_id = pibackfill_sanity(env_data, case_data, task, file, files_dir, param)
    # get metrics
    metrics = task.get_task_metrics(task_id)
    # get row count
    rows_count = TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    assert (
        metrics["current"]["written_rows"] == rows_count
    ), f"test case {inspect.currentframe().f_code.co_name} failed, insert rows wrong!"


@pytest.mark.sanity
def test_singlecol_template(input_data):
    pi_test_logger.info(f"running test case...{inspect.currentframe().f_code.co_name}")
    env_data = input_data
    case_data = Util.get_case_data_from_yaml(
        "pi/test_backfill_singlecol_template.yaml", task_type
    )
    files_dir = "pi/pi_singlecol_template.csv"
    param = "transform_config_file"
    task = Task(env_data, case_data)
    file = File(env_data, task_type)
    task_id = pibackfill_sanity(env_data, case_data, task, file, files_dir, param)
    metrics = task.get_task_metrics(task_id)
    row_count = TaosAdapter.check_db_count(
        ENV.taosd_host, case_data["to"]["target_dbname"]
    )
    assert (
        row_count == metrics["current"]["written_rows"]
    ), f"test case {inspect.currentframe().f_code.co_name} failed, insert rows wrong!"


@pytest.mark.sanity
def test_singlecol_point(input_data):
    pi_test_logger.info(f"running test case...{inspect.currentframe().f_code.co_name}")
    env_data = input_data
    case_data = Util.get_case_data_from_yaml(
        "pi/test_backfill_singlecol_point.yaml", task_type
    )
    files_dir = "pi/pi_singlecol_point.csv"
    param = "transform_config_file"
    task = Task(env_data, case_data)
    file = File(env_data, task_type)
    task_id = pibackfill_sanity(env_data, case_data, task, file, files_dir, param)
    metrics = task.get_task_metrics(task_id)
    row_count = TaosAdapter.check_db_count(
        ENV.taosd_host, case_data["to"]["target_dbname"]
    )
    assert (
        row_count == metrics["current"]["written_rows"]
    ), f"test case {inspect.currentframe().f_code.co_name} failed, insert rows wrong!"


@pytest.mark.skip
def test_TemplateForAFElementFile_performance_s1():
    pi_test_logger.info(f"running test case...{inspect.currentframe().f_code.co_name}")
    env_data = Util.get_env_data()
    case_data = Util.get_case_data_from_yaml("pi/test_PiBackfillFile.yaml", task_type)
    case_data["to"]["target_dbname"] = "perf_pibackfill_s1"
    case_data["from"]["BackfillStartTime"] = "2024-03-28 00:00:00"
    case_data["from"]["BackfillEndTime"] = "2024-03-28 01:00:00"
    case_data["from"]["batch_size"] = "10000"
    case_data["to"]["column_count"] = 5
    case_data["task_exec_time"] = 10 * 60
    files_dir = "pi/perf/cargill_3.csv"
    param = "template_for_af_element_file"
    task = Task(env_data, case_data)
    file = File(env_data, task_type)

    TaosAdapter.create_db(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    # TaosAdapter.delete_db(env_data["taosadapter_host"], case_data["to"]["target_dbname"])
    payload_str = Util.get_task_payload(case_data, env_data)
    file_dir = file.upload(files_dir)
    payload = File.add_file_param(payload_str, param, file_dir)
    task.perf_test(payload, 1, "test_PiBackfill_performance", True)


@pytest.mark.skip
def test_TemplateForAFElementFile_performance():
    pi_test_logger.info(f"running test case...{inspect.currentframe().f_code.co_name}")
    env_data = Util.get_env_data()
    case_data = Util.get_case_data_from_yaml(
        "pi/test_PiBackfill_performance.yaml", task_type
    )
    files_dir = "pi/perf/perf_Template.csv"
    param = "template_for_pi_point_file"
    task = Task(env_data, case_data)
    file = File(env_data, task_type)
    pibackfill_sanity(
        env_data, case_data, task, file, files_dir, param, "TemplateForAFElementFile"
    )
