import copy
import logging
import random
import time

import pytest

from testng_taosx.constant import EnvType, TaskType, TAOSX_LOG_DIR, TaskStatus
from testng_taosx.file import File
from testng_taosx.mqttPub import MQTTPub
from testng_taosx.task import Task
from testng_taosx.util import TaosAdapter
from testng_taosx.util import Util
from testng_taosx.env import *

mqtt_test_logger = logging.getLogger(__name__)
task_type = TaskType.MQTT
mqtt_invalid_conf_status = TaskStatus.INTERRUPTED.value


@pytest.fixture(scope="module")
def input_data():
    mqtt_test_logger.info("before test...")
    env_data = Util.get_env_data()
    case_data = Util.get_case_data_from_yaml(
        "mqtt/test-mqtt-sanity-basic.yaml", task_type
    )
    mqtt_payload = Util.read_yaml("mqtt/payload-basic-transformer-json.yaml")
    case_data["parser"] = mqtt_payload["parser"]

    yield env_data, case_data

    mqtt_test_logger.info("after test...")


def mqtt_check_status(task, taskid, retry_count=10):
    retry = 0
    while True:
        task_status = task.get_task_status(taskid)
        if task_status["status"] == mqtt_invalid_conf_status or retry == retry_count:
            return task_status
        else:
            time.sleep(2)
        retry += 1


def uploadSSL(
    ca_file: str, client_cert_file: str, client_key_file: str, env_data, case_data
):
    file = File(env_data, task_type)
    ca_dir = file.upload(ca_file)
    client_cert_dir = file.upload(client_cert_file)
    client_key_dir = file.upload(client_key_file)

    case_data["from"]["fromhost"] = f"mqtt://{env_data['data_source']['mqtt'][2]}"
    case_data["from"]["ca"] = f"@{ca_dir}"
    case_data["from"]["cert"] = f"@{client_cert_dir}"
    case_data["from"]["cert_key"] = f"@{client_key_dir}"


def mqtt_sanity_test(env_data, case_data, mqttconfigfile):
    Util.create_stable(case_data, case_data["parser"]["parser"], "ns")
    payload = Util.get_task_payload(case_data, env_data, EnvType.LOCAL)
    task = Task(env_data, case_data)
    task_info = task.create_task(payload)
    task_id = task_info["id"]

    time.sleep(1)

    # get and check status of task
    r = task.get_task_status(task_id)
    assert (
        r["status"] == "running"
    ), f"task status should be running after creation, but got {r['status']}, reason: {r['reason']}"

    mqtt1 = MQTTPub(mqttconfigfile)
    mqtt1.start()
    time.sleep(case_data["task_exec_time"])
    mqtt1.terminate()

    task.check_task_metrics(task_id, ENV.retryCheckMetrics)
    task.stop_task_with_retry(task_id=task_id, use_assert=False)
    time.sleep(1)
    metrics = task.get_task_metrics(task_id)
    return metrics


@pytest.mark.sanity
@pytest.mark.parametrize("with_agent", [True, False])
def test_case_base_transformer(with_agent, input_data):
    """
    用例概述: mqtt 用例, 基本用例
    用例步骤：
    1. 依据 with_agent 的值决定是否使用 agent
    2. 订阅 topic : testmqtt/1::2

    验证点：
    1. 数据正常写入
    """
    mqtt_test_logger.info("start test...sanity case: transformer(json)")
    env_data, case_data_orig = input_data
    case_data = copy.deepcopy(case_data_orig)
    case_data["from"]["client_id"] = "client" + str(random.randint(1, 10000))
    if not with_agent:
        case_data.pop("via")

    metrics = mqtt_sanity_test(env_data, case_data, "mqtt/mqtt.yaml")
    rows_count = TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    assert rows_count > 0, "入库的数据量应大于 0"
    assert (
        rows_count == metrics["current"]["written_rows"]
    ), f"rows({rows_count}) inserted should be equal to metrics({metrics['current']['written_rows']})"
    # check column value
    sqlresult = TaosAdapter.run_sql(
        env_data["taosadapter_host"],
        f"select * from `{case_data['to']['target_dbname']}`.`mqttstb` limit 1",
    )
    for item in sqlresult["data"][0]:
        assert (
            item != None
        ), f'the value should not be None. but the result is {sqlresult["data"][0]}'
    TaosAdapter.drop_db(env_data["taosadapter_host"], case_data["to"]["target_dbname"])


@pytest.mark.sanity
def test_case_topics(input_data):
    """
    用例概述: mqtt 用例, 基本用例
    用例步骤：
    1. 任务使用 agent
    2. 订阅 多个topic : testmqtt/2/+::2,testmqtt/3/#::2

    验证点：
    1. 数据正常写入
    """
    mqtt_test_logger.info("start test...sanity case: topics")
    env_data, case_data_orig = input_data
    case_data = copy.deepcopy(case_data_orig)
    case_data["from"]["topics"] = "testmqtt/2/+::2,testmqtt/3/#::2"
    case_data["from"]["version"] = "5.0"
    case_data["from"]["client_id"] = "client" + str(random.randint(1, 10000))

    metrics = mqtt_sanity_test(env_data, case_data, "mqtt/mqtt.yaml")
    rows_count = TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    assert metrics["current"]["written_rows"] > 0, "任务 metrics 中的 written_rows 应大于 0"
    assert rows_count > 0, "入库的数据量应大于 0"
    TaosAdapter.drop_db(env_data["taosadapter_host"], case_data["to"]["target_dbname"])


@pytest.mark.sanity
def test_case_auth(input_data):
    """
    用例概述: mqtt 用例
    用例步骤：
    1. 任务使用 agent
    2. 认证方式使用用户名密码

    验证点：
    1. 数据正常写入
    """
    mqtt_test_logger.info("start test...sanity case: auth")
    env_data, case_data_orig = input_data
    case_data = copy.deepcopy(case_data_orig)
    case_data["from"]["fromhost"] = f"mqtt://{env_data['data_source']['mqtt'][1]}"
    case_data["from"]["log_level"] = "debug"
    case_data["from"]["version"] = "3.1.1"
    case_data["from"]["client_id"] = "client" + str(random.randint(1, 10000))

    metrics = mqtt_sanity_test(env_data, case_data, "mqtt/mqtt-auth.yaml")
    rows_count = TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    assert rows_count > 0, "入库的数据量应大于 0"
    assert metrics["current"]["written_rows"] > 0, "任务 metrics 中的 written_rows 应大于 0"
    TaosAdapter.drop_db(env_data["taosadapter_host"], case_data["to"]["target_dbname"])


@pytest.mark.sanity
def test_case_ssl(input_data):
    """
    用例概述: mqtt 用例
    用例步骤：
    1. 任务使用 agent
    2. 使用 ssl 证书

    验证点：
    1. 数据正常写入
    """
    mqtt_test_logger.info("start test...sanity case: ssl")
    env_data, case_data_orig = input_data
    case_data = copy.deepcopy(case_data_orig)
    case_data["from"]["client_id"] = "client" + str(random.randint(1, 10000))

    ca_file = "mqtt/ssl/ca.crt"
    client_cert_file = "mqtt/ssl/client.crt"
    client_key_file = "mqtt/ssl/client.key"
    uploadSSL(ca_file, client_cert_file, client_key_file, env_data, case_data)

    metrics = mqtt_sanity_test(env_data, case_data, "mqtt/mqtt.yaml")
    rows_count = TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    assert rows_count > 0, "入库的数据量应大于 0"
    assert metrics["current"]["written_rows"] > 0, "任务 metrics 中的 written_rows 应大于 0"
    TaosAdapter.drop_db(env_data["taosadapter_host"], case_data["to"]["target_dbname"])


@pytest.mark.negative
def test_invalid_dsn(input_data):
    mqtt_test_logger.info("start test...avaliablity case: invalid dsn")
    env_data, case_data_orig = input_data
    case_data = copy.deepcopy(case_data_orig)
    case_data["from"]["fromhost"] = "mqtt://xx.xx.xx.xx:1234"
    payload_new = Util.get_task_payload(case_data, env_data)

    Util.create_stable(case_data, case_data["parser"]["parser"])
    task = Task(env_data, case_data)
    task_info = task.create_task(payload_new)
    time.sleep(1)
    task_id = task_info["id"]
    task_status = mqtt_check_status(task, task_id)
    assert (
        task_status["status"] == mqtt_invalid_conf_status
    ), f"When dsn error, the error should be reported and task status should be failed"
    task.delete_task(task_id)
    TaosAdapter.drop_db(env_data["taosadapter_host"], case_data["to"]["target_dbname"])


@pytest.mark.negative
def test_invalid_userpw(input_data):
    mqtt_test_logger.info("start test...availablity case: invalid username or passwd")
    env_data, case_data_orig = input_data
    case_data = copy.deepcopy(case_data_orig)
    case_data["from"]["fromhost"] = f"mqtt://{env_data['data_source']['mqtt'][1]}"
    task = Task(env_data, case_data)

    payload_dict = Util.get_task_payload(case_data, env_data)
    paramIndex = payload_dict["from"].index("@")
    param = payload_dict["from"][paramIndex:]

    payload_dict["from"] = f"mqtt://abc:abc{param}"

    Util.create_stable(case_data, case_data["parser"]["parser"])
    task_info = task.create_task(payload_dict)
    time.sleep(2)
    task_id = task_info["id"]
    mqtt_check_status(task, task_info["id"])
    task_status = mqtt_check_status(task, task_id)
    assert (
        task_status["status"] == mqtt_invalid_conf_status
    ), "When user or passwd error, the error should be reported and task status should be failed: not Authorized"
    task.delete_task(task_id)
    TaosAdapter.drop_db(env_data["taosadapter_host"], case_data["to"]["target_dbname"])


@pytest.mark.negative
def test_invalid_topic(input_data):
    mqtt_test_logger.info("start test...availablity case: invalid topic or Qos config")
    env_data, case_data_orig = input_data
    case_data = copy.deepcopy(case_data_orig)
    task = Task(env_data, case_data)
    Util.create_stable(case_data, case_data["parser"]["parser"])

    case_data["from"]["topics"] = "testmqtt:2"
    payload_new = Util.get_task_payload(case_data, env_data)
    mqtt_test_logger.debug(f"new payload:{payload_new}")
    task_info = task.create_task(payload_new)
    time.sleep(1)
    task_id = task_info["id"]
    mqtt_check_status(task, task_info["id"])
    task_status = mqtt_check_status(task, task_id)
    assert (
        task_status["status"] == mqtt_invalid_conf_status
    ), "When topic config error, task status should be failed"
    task.delete_task(task_id)

    case_data["from"]["topics"] = "testmqtt::3"
    payload_new = Util.get_task_payload(case_data, env_data)
    mqtt_test_logger.debug(f"new payload:{payload_new}")
    task_info = task.create_task(payload_new)
    time.sleep(1)
    task_id = task_info["id"]
    task_status = mqtt_check_status(task, task_id)
    assert (
        task_status["status"] == mqtt_invalid_conf_status
    ), "When Qos config error, task status should be failed"
    task.delete_task(task_id)

    TaosAdapter.drop_db(env_data["taosadapter_host"], case_data["to"]["target_dbname"])


@pytest.mark.negative
def test_invalid_ssl(input_data):
    mqtt_test_logger.info("start test...sanity case: invalid ssl")
    env_data, case_data_orig = input_data
    case_data = copy.deepcopy(case_data_orig)
    Util.create_stable(case_data, case_data["parser"]["parser"])

    ca_file = "mqtt/ssl/ca.crt"
    client_cert_file = "mqtt/ssl/client.crt"
    client_key_file = "mqtt/ssl/client.key.invalid"
    uploadSSL(ca_file, client_cert_file, client_key_file, env_data, case_data)
    payload = Util.get_task_payload(case_data, env_data)
    task = Task(env_data, case_data)
    task_info = task.create_task(payload)
    time.sleep(1)
    task_id = task_info["id"]
    task_status = mqtt_check_status(task, task_id)
    assert (
        task_status["status"] == mqtt_invalid_conf_status
    ), "When TLS info error, task status should be failed. Private key does not match public key"
    task.delete_task(task_id)
    TaosAdapter.drop_db(env_data["taosadapter_host"], case_data["to"]["target_dbname"])


@pytest.mark.sanity
def test_connectivity_check(input_data):
    """
    用例概述: mqtt 用例, 连通性校验
    用例步骤：
    1. 匿名登录方式连通性校验
    2. 用户名密码方式连通性校验

    验证点：
    1. 连通性校验通过
    """
    mqtt_test_logger.info("start test...sanity case: connectivity check")
    env_data, case_data_orig = input_data
    case_data = copy.deepcopy(case_data_orig)
    dsn = Util.get_task_payload(case_data, env_data)["from"]
    result = Util.check_connectivity(env_data, dsn)
    assert result["valid"] == True, f"mqtt 连通性校验失败 dsn={dsn}, error:{result['message']}"

    case_data["from"]["fromhost"] = f"mqtt://{env_data['data_source']['mqtt'][1]}"
    dsn = Util.get_task_payload(case_data, env_data)["from"]
    result = Util.check_connectivity(env_data, dsn)
    assert result["valid"] == True, f"mqtt 连通性校验失败 dsn={dsn}, error:{result['message']}"


@pytest.mark.performance
def test_case_performance_scenario1(input_data):
    mqtt_test_logger.info("start mqtt performance test...")
    env_data, case_data_orig = input_data
    case_data = Util.get_case_data_from_yaml(
        "mqtt/test-mqtt-performance.yaml", task_type
    )
    mqtt_payload = Util.read_yaml("mqtt/payload-performance-s1.yaml")
    case_data["parser"] = mqtt_payload["parser"]
    case_data["to"]["target_dbname"] = "perf_mqtt_s1"
    case_data["from"]["topics"] = "testperf/1::2"
    case_data["to"]["column_count"] = 5
    case_data["task_exec_time"] = 10 * 60
    case_data["from"]["client_id"] = "mqttclient" + str(random.randint(1, 10000))

    sqlstr = f"CREATE STABLE {case_data['to']['target_dbname']}.mqtt_meters (ts TIMESTAMP,id INT, \
        current DOUBLE,phase DOUBLE,voltage DOUBLE) TAGS (groupid INT, location VARCHAR(64));"
    TaosAdapter.create_db(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"], "ns"
    )
    r = TaosAdapter.run_sql(env_data["taosadapter_host"], sqlstr)
    assert r["code"] == 0, f"fail to create database: {r['desc']}"

    jmeter_cmd = "E:\\cyjia\\apache-jmeter-5.6.2\\bin\\jmeter.bat -n -R 192.168.1.40,192.168.1.42 -t E:\\nminhui\\mqtt.jmx"
    cmd = f"powershell Invoke-WmiMethod -Class win32_process -Name create -ArgumentList '{jmeter_cmd}'"
    Util.wincmd_run(case_data["jmeter_host"], cmd)
    time.sleep(5)

    task = Task(env_data, case_data)
    payload = Util.get_task_payload(case_data, env_data)
    task.perf_test(
        payload, 1, "1 task ,1 stable,10w subtables, 5 columns(int, double)", True
    )
    # stop jmeter test
    jmeter_stop_cmd = "E:\\cyjia\\apache-jmeter-5.6.2\\bin\\stoptest.cmd"
    stop_cmd = f"powershell Invoke-WmiMethod -Class win32_process -Name create -ArgumentList '{jmeter_stop_cmd}'"
    Util.wincmd_run(case_data["jmeter_host"], stop_cmd)
