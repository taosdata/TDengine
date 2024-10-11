import copy
import logging
import random
import time

import pytest

from testng_taosx.constant import TaskType, TAOSX_LOG_DIR, TaskStatus
from testng_taosx.file import File
from testng_taosx.task import Task
from testng_taosx.util import TaosAdapter
from testng_taosx.util import Util

historian_test_logger = logging.getLogger(__name__)

task_type = TaskType.HISTORIAN


@pytest.fixture(scope="module")
def input_data():
    historian_test_logger.info("before historian test...")
    env_data = Util.get_env_data()

    yield env_data
    historian_test_logger.info("after historian test...")


@pytest.mark.sanity
def test_check_connectivity(input_data):
    env_data = input_data
    case_data = Util.get_case_data_from_yaml(
        "historian/historian_basic.yaml", task_type
    )
    dsn = Util.get_task_payload(case_data, input_data)["from"]
    json_result = Util.check_connectivity(input_data, dsn)
    assert json_result[
        "valid"
    ], f"aveva Historian 连通性校验失败，result: {json_result} dsn: {dsn}"
