import json
import logging
from time import sleep

import pytest

from testng_taosx.constant import TaskType, EnvType
from testng_taosx.task import Task
from testng_taosx.util import Util, TaosAdapter

backup_test_logger = logging.getLogger(__name__)
task_type = TaskType.BACKUP


@pytest.fixture(scope="module", autouse=True)
def case_setup():
    backup_test_logger.info("before all backup cases...")
    env_data = Util.get_env_data()
    TaosAdapter.create_db(env_data["taosadapter_host"], "ci_backup")
    yield env_data
    backup_test_logger.info("after all backup cases...")
    TaosAdapter.drop_topic(env_data["taosadapter_host"], "ci_backup")
    TaosAdapter.drop_db(env_data["taosadapter_host"], "ci_backup")


@pytest.mark.sanity
def test_sanity_backup(case_setup):
    backup_test_logger.info("start backup test...")
    env_data = case_setup
    case_data = Util.get_case_data_from_yaml("backup/test_backup.yaml", task_type)

    task = Task(env_data, case_data)
    payload = Util.get_task_payload(case_data, env_data, env_type=EnvType.LOCAL)

    task_info = task.create_task(payload)
    task_id = task_info["id"]

    # wait for the status to change from created to queued
    sleep(3)

    r = task.get_task_status(task_id)
    assert r["status"] == "queued"

    r = task.delete_task(task_id)
    assert json.loads(r.text)["message"] == "Task is in scheduler, please stop it first"

    r = task.stop_task(task_id)
    assert r.status_code == 200

    r = task.delete_task(task_id)
    assert r.status_code == 200
