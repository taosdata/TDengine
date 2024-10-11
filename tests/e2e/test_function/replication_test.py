import json
import logging
import subprocess
import time
import taosws

import allure
import pytest

from testng_taosx.constant import *
from testng_taosx.file import TaskType
from testng_taosx.task import Task
from testng_taosx.util import TaosAdapter
from testng_taosx.util import Util

replication_test_logger = logging.getLogger(__name__)
task_type = TaskType.REPLICATION


@pytest.fixture(scope="function", autouse=True)
def input_data():
    replication_test_logger.info("before all replication cases...")
    env_data = Util.get_env_data()
    case_data = Util.get_case_data_from_yaml(
        "replication/test_replication.yaml", task_type
    )
    # split_result = case_data["from"]["fromhost"].split("@")
    # dsn = None
    # if split_result.__len__() == 1:
    #     dsn = f"ws://root:taosdata@{env_data['data_source']['replication'][0]}"
    # elif split_result[0].split("//").__len__ == 1:
    #     raise Exception(f"用例配置错误，case_data: {case_data}")
    # else:
    #     dsn = f"ws://{split_result[0].split('//')[1]}@{env_data['data_source']['replication'][0]}"
    # 修改用例使用的 topic 名称和目标库的库名为随机字符串，这样方便之后删除
    case_data["source"]["source_dbname"] = f"{Util.get_long_name(10)}_replication"
    case_data["to"]["target_dbname"] = f"{Util.get_long_name(10)}_replication_target"
    case_data["from"][
        "fromhost"
    ] = f"{case_data['from']['fromhost']}{case_data['source']['source_dbname']}"
    case_data["to"][
        "target_host"
    ] = f"{case_data['to']['target_host']}{case_data['to']['target_dbname']}"

    # TaosAdapter.run_sql(
    #     env_data["taosadapter_host"],
    #     f"drop topic {case_data['source']['source_dbname']}",
    #     ignore_result=True,
    # )

    yield env_data, case_data
    replication_test_logger.info("after all replication cases...")
    # TaosAdapter.run_sql(
    #     env_data["taosadapter_host"],
    #     f"drop topic {case_data['source']['source_dbname']}",
    # )
    # TaosAdapter.drop_db(env_data["taosadapter_host"], case_data["to"]["target_dbname"])


def run_command_local_or_remote(ip, command):
    if ip == "localhost" or ip == "127.0.0.1":
        subprocess.run(command, shell=True)
    else:
        Util.ssh_run(ip, command)


@pytest.mark.sanity
def test_sanity_replication(input_data):
    env_data, case_data = input_data
    task = Task(env_data, case_data)
    command = f"taosBenchmark -t {case_data['source']['subtable_number']}    \
                                                    -n {case_data['source']['record_number_per_subtable']} \
                                                    -y -d {case_data['source']['source_dbname']}"
    run_command_local_or_remote(env_data["taosadapter_host"], command)
    payload = Util.get_task_payload(case_data, env_data, env_type=EnvType.LOCAL)
    TaosAdapter.create_db(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    task_info = task.create_task(payload)
    time.sleep(case_data["task_exec_time"])
    task.stop_task_with_retry(task_info["id"])
    task.delete_task(task_info["id"])
    # time.sleep(
    #     10
    # )  # 这里 sleep 10s 是为了能够成功删除 topic，否则会一直报 topic subscribed cannot be dropped
    # result = TaosAdapter.run_sql(
    #     env_data["taosadapter_host"],
    #     f"drop topic {case_data['source']['source_dbname']}",
    # )
    row_count_source = int(case_data["source"]["record_number_per_subtable"]) * int(
        case_data["source"]["subtable_number"]
    )
    row_count_target = TaosAdapter.check_db_count(
        env_data["taosadapter_host"],
        case_data["to"]["target_dbname"],
        case_data["source"]["source_stbname"],
    )
    assert row_count_source == row_count_target, replication_test_logger.error(
        f"row count in target should be same as source"
    )


@pytest.mark.negative
@pytest.mark.xfail(
    reason="负向用例的判断逻辑是任务状态，但是任务状态不一定是失败，可能是中断，也可能是运行中，且这个任务状态的切换无法固定由时间决定"
)
def test_replicate_with_wrong_precision(input_data):
    env_data, case_data = input_data
    task = Task(env_data, case_data)
    payload = Util.get_task_payload(case_data, env_data, env_type=EnvType.LOCAL)
    TaosAdapter.create_db(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    for precision in ["us", "ns"]:
        TaosAdapter.create_db(
            env_data["taosadapter_host"],
            case_data["source"]["source_dbname"],
            precision,
        )
        task_info = task.create_task(payload)
        time.sleep(20)
        task_status = task.get_task_status(task_info["id"])
        response = task.get_activities(task_info["id"])
        assert (
            response.text.find(f'"status":"interrupted"') != -1
        ), "使用错误参数在最近的任务活动日志中应有 interrupted"
        task.delete_task(task_info["id"])


# dsn错误
@pytest.mark.negative
@allure.link("https://jira.taosdata.com:18080/browse/TD-31731")
def test_wrong_dsn(input_data):
    """
    用例概述：针对创建数据同步任务时，目标 DSN 的异常场景进行测试
    用例步骤：
    1. 目标 DSN 中用户名密码错误
    2. 目标 DSN 中指定的数据库不存在
    3. 目标 DSN 中未设置数据库
    验证点：
    1. 创建数据同步任务的 API 返回 status code 500
    """
    env_data, case_data = input_data
    task = Task(env_data, case_data)
    payload = Util.get_task_payload(case_data, env_data, env_type=EnvType.LOCAL)

    # 用户名密码错误
    TaosAdapter.create_db(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    wrong_user_pwd = "root:tbase125"
    payload[
        "to"
    ] = f"taos+http://{wrong_user_pwd}@{env_data['taosadapter_host']}:6041/{case_data['to']['target_dbname']}"
    task_info = task.create_task_raw(payload)
    assert task_info.status_code == 500, replication_test_logger.error(
        "task should be failure when username or password is wrong"
    )
    TaosAdapter.drop_db(env_data["taosadapter_host"], case_data["to"]["target_dbname"])

    # 目标库不存在
    non_existent_dbname = "non_existent_db"
    payload[
        "to"
    ] = f"taos+http://{env_data['taosadapter_host']}:6041/{non_existent_dbname}"
    task_result = task.create_task_raw(payload)
    assert task_result.status_code == 500, replication_test_logger.error(
        "task should be failure when target not exists"
    )

    # Test task can NOT be created if target DB is not set in target DSN
    payload["to"] = f"taos+ws://{env_data['taosadapter_host']}:6041/"
    task_result = task.create_task_raw(payload)
    assert task_result.status_code == 500, replication_test_logger.error(
        "Task should fail with status code 500 when target DB is not set"
    )
    assert (
        json.loads(task_result.text)["message"] == "Sink database must be set"
    ), replication_test_logger.error("Task can't be created if target DB is not set")


# 创建任务后追加写入数据
@pytest.mark.sanity
def test_replication_add_data(input_data):
    env_data, case_data = input_data
    task = Task(env_data, case_data)
    payload = Util.get_task_payload(case_data, env_data, env_type=EnvType.LOCAL)
    TaosAdapter.create_db(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    # 建库建表，不写数据
    command = f"taosBenchmark -t {case_data['source']['subtable_number']}    \
                                                    -n  {case_data['source']['record_number_per_subtable']}\
                                                    -y -d {case_data['source']['source_dbname']}"
    run_command_local_or_remote(env_data["taosadapter_host"], command)

    task_info = task.create_task(payload)
    time.sleep(5)
    task_status = task.get_task_status(task_info["id"])
    assert (
        task_status["status"] == TaskStatus.RUNNING.value
    ), replication_test_logger.error("task status should be running after created")
    time.sleep(case_data["task_exec_time"])
    # 追加写入新数据
    command = f"taosBenchmark -t {case_data['source']['subtable_number']}    \
                                                    -n {case_data['source']['record_number_per_subtable']} \
                                                       -y -d {case_data['source']['source_dbname']} -U -s 1704038400000"
    run_command_local_or_remote(env_data["taosadapter_host"], command)
    time.sleep(case_data["task_exec_time"])
    task.stop_task_with_retry(task_info["id"])
    row_count_source = (
        2
        * int(case_data["source"]["record_number_per_subtable"])
        * int(case_data["source"]["subtable_number"])
    )
    row_count_target = TaosAdapter.check_db_count(
        env_data["taosadapter_host"],
        case_data["source"]["source_dbname"],
        case_data["source"]["source_stbname"],
    )
    assert row_count_source == row_count_target, replication_test_logger.error(
        f"row count in target should be same as source"
    )
    task.delete_task(task_info["id"])
    # TaosAdapter.drop_topic_with_retry(
    #     env_data["taosadapter_host"], case_data["source"]["source_dbname"]
    # )


# 同步完成后，删除部分源库数据
@pytest.mark.sanity
def test_replication_delete_data(input_data):
    env_data, case_data = input_data
    task = Task(env_data, case_data)
    payload = Util.get_task_payload(case_data, env_data, env_type=EnvType.LOCAL)
    TaosAdapter.create_db(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    # 写入源库数据
    command = f"taosBenchmark -t {case_data['source']['subtable_number']}    \
                                                    -n {case_data['source']['record_number_per_subtable']} \
                                                    -y -d {case_data['source']['source_dbname']} -s 1704038400 -M"
    if env_data["taosadapter_host"] == "localhost":
        subprocess.call(command, shell=True)
    else:
        Util.ssh_run(env_data["taosadapter_host"], command)
    task_info = task.create_task(payload)
    time.sleep(5)
    task_status = task.get_task_status(task_info["id"])
    assert (
        task_status["status"] == TaskStatus.RUNNING.value
    ), replication_test_logger.error("task status should be running after created")
    time.sleep(case_data["task_exec_time"])
    # 删除源库中的所有数据
    TaosAdapter.run_sql(
        env_data["taosadapter_host"],
        f"delete from {case_data['source']['source_dbname']}.meters",
    )

    # 目标库中数据也被清空
    result = TaosAdapter.run_sql(
        env_data["taosadapter_host"],
        f"select count(*) from {case_data['source']['source_dbname']}.meters",
    )
    assert result["data"][0][0] == 0, replication_test_logger.error(
        "data in target database should be deleted "
    )
    task.stop_task_with_retry(task_info["id"])
    task.delete_task(task_info["id"])
    # TaosAdapter.drop_topic_with_retry(
    #     env_data["taosadapter_host"], case_data["source"]["source_dbname"]
    # )
