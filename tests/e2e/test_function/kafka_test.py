import logging
import pytest
import copy
import time
import random

from testng_taosx.constant import TaskType, EnvType
from testng_taosx.task import Task
from testng_taosx.util import TaosAdapter
from testng_taosx.util import Util
from testng_taosx.kafkaPub import Producer
from testng_taosx.file import File

kafka_test_logger = logging.getLogger(__name__)
task_type = TaskType.KAFKA


@pytest.fixture(scope="module")
def input_data():
    kafka_test_logger.info("before test...")
    env_data = Util.get_env_data()
    case_data = Util.get_case_data_from_yaml(
        "kafka/test-kafka-sanity-basic.yaml", task_type
    )
    case_data["from"]["client_id"] = "taosx_client" + str(random.randint(1, 10000))
    kafka_payload = Util.read_yaml("kafka/payload-basic.yaml")
    case_data["parser"] = kafka_payload["parser"]

    yield env_data, case_data

    kafka_test_logger.info("after test...")


@pytest.mark.sanity
def test_case_base_transformer_json(input_data):
    """
    用例概述: kafka 基本用例, 输入数据为json格式
    用例步骤：
    1. 任务使用 agent
    2. 订阅 topic: test_taosx
       数据： {"mytime":1726141225712961792,"id":1534,"current":10.77,"phase":0.77,"voltage":220,"description":"hello taosx","groupid":1,"location":"Beijing"}
    3. offset: earliest
    4. transformer 解析： json
    5. transformer 过滤规则： id >= 1
    6. transformer 映射配置操作有： mapping, expr(replace, sub_string, to_upper, truncate, append), format, sum

    验证点：
    1. 数据正常写入
    2. 验证 transformer 映射配置的规则生效
    """
    kafka_test_logger.info("start test...sanity case: transformer(json)")
    kafka1 = Producer("kafka/kafka.yaml")
    kafka1.start()
    time.sleep(5)
    kafka1.terminate()
    env_data, case_data_orig = input_data
    case_data = copy.deepcopy(case_data_orig)
    kafka_payload = Util.read_yaml("kafka/payload-basic-transformer-json.yaml")
    case_data["parser"] = kafka_payload["parser"]
    case_data["from"]["group"] = "taosx_group1" + str(random.randint(1, 10000))
    task = Task(env_data, case_data)
    Util.create_stable(case_data, kafka_payload["parser"]["parser"], "ns")
    payload = Util.get_task_payload(case_data, env_data, EnvType.LOCAL)
    task_id = task.run_task(payload)
    task.stop_task_with_retry(task_id)
    metrics = task.get_task_metrics(task_id)
    rows_count = TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    assert rows_count > 0, f"入库的数据量应大于 0"
    assert metrics["current"]["written_rows"] > 0, f"任务 metrics 中的 written_rows 应大于 0"
    # check column value
    sqlresult = TaosAdapter.run_sql(
        env_data["taosadapter_host"],
        f"select id, current, phase, voltage, description, col1, col2, col3, col4, col5, col6, \
                                        col7 from `{case_data['to']['target_dbname']}`.`kafkastb` limit 1",
    )
    assert sqlresult["data"][0][5] == int(
        sqlresult["data"][0][1] + sqlresult["data"][0][2]
    ), f"col1 should be equal to {int(sqlresult['data'][0][1] + sqlresult['data'][0][2])}"
    assert (
        sqlresult["data"][0][6]
        == f"{sqlresult['data'][0][0]}-{sqlresult['data'][0][4]}"
    ), f"col2 should be equal to '{sqlresult['data'][0][0]}-{sqlresult['data'][0][4]}'"
    assert (
        sqlresult["data"][0][7] == f"{sqlresult['data'][0][4]} !OK"
    ), f"col3 should be equal to '{sqlresult['data'][0][4]} !OK'"
    assert (
        sqlresult["data"][0][8] == str(sqlresult["data"][0][4])[:5]
    ), f"col4 should be equal to '{str(sqlresult['data'][0][4])[:5]}'"
    assert (
        sqlresult["data"][0][9] == str(sqlresult["data"][0][4]).upper()
    ), f"col5 should be equal to '{str(sqlresult['data'][0][4]).upper()}'"
    assert (
        sqlresult["data"][0][10] == str(sqlresult["data"][0][4])[5:]
    ), f"col6 should be equal to '{str(sqlresult['data'][0][4])[5:]}'"
    assert sqlresult["data"][0][11] == str(sqlresult["data"][0][4]).replace(
        "hello", "hi"
    ), f"col7 should be equal to '{str(sqlresult['data'][0][4]).replace('hello','hi')}'"
    TaosAdapter.drop_db(env_data["taosadapter_host"], case_data["to"]["target_dbname"])


@pytest.mark.sanity
def test_case_base_transformer_json_split(input_data):
    """
    用例概述: kafka 基本用例, 输入数据为json格式
    用例步骤：
    1. 任务使用 agent
    2. 订阅 topic: test_taosx1
       数据： {"mytime":1701938756205263616,"id": "1","message": "10.77-0.77-220","groupid": 1,"location": "California.SanDiego"}
    3. offset: earliest
    4. transformer 解析: json
    5. split 字段: message
    6. transformer 过滤规则： id >= 1
    7. transformer 映射配置操作有： mapping, join, value

    验证点：
    1. 数据正常写入
    2. 验证 transformer 映射配置的规则生效
    """
    kafka_test_logger.info("start test...sanity case: transformer(split)")
    kafka1 = Producer("kafka/kafka.yaml")
    kafka1.start()
    time.sleep(5)
    kafka1.terminate()
    env_data, case_data_orig = input_data
    case_data = copy.deepcopy(case_data_orig)
    kafka_payload = Util.read_yaml("kafka/payload-basic-transformer-json-split.yaml")
    case_data["parser"] = kafka_payload["parser"]
    case_data["from"]["topics"] = "test_taosx1"
    case_data["from"]["group"] = "taosx_group2" + str(random.randint(1, 10000))
    task = Task(env_data, case_data)

    Util.create_stable(case_data, kafka_payload["parser"]["parser"], "ns")
    payload = Util.get_task_payload(case_data, env_data, EnvType.LOCAL)
    task_id = task.run_task(payload)
    task.stop_task_with_retry(task_id)
    metrics = task.get_task_metrics(task_id)
    rows_count = TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    assert rows_count > 0, f"入库的数据量应大于 0"
    assert metrics["current"]["written_rows"] > 0, f"任务 metrics 中的 written_rows 应大于 0"
    # check column value
    sqlresult = TaosAdapter.run_sql(
        env_data["taosadapter_host"],
        f"select * from `{case_data['to']['target_dbname']}`.`kafkastb` limit 1",
    )
    for item in sqlresult["data"][0]:
        assert (
            item != None
        ), f'the value should not be None. but the result is {sqlresult["data"][0]}'
    # check column value
    sqlresult = TaosAdapter.run_sql(
        env_data["taosadapter_host"],
        f"select col1, col2, id, location from `{case_data['to']['target_dbname']}`.`kafkastb` limit 1",
    )
    assert (
        sqlresult["data"][0][0] == 123
    ), f"col1 should be equal to 123. but it is {sqlresult['data'][0][0]}"
    assert (
        sqlresult["data"][0][1]
        == f"{sqlresult['data'][0][2]}-{sqlresult['data'][0][3]}"
    ), f"col2 should be equal to {sqlresult['data'][0][2]}-{sqlresult['data'][0][3]}. but it is sqlresult['data'][0][1]"
    TaosAdapter.drop_db(env_data["taosadapter_host"], case_data["to"]["target_dbname"])


@pytest.mark.sanity
def test_case_base_transformer_regex(input_data):
    """
    用例概述: kafka 基本用例, 输入数据为文本格式
    用例步骤：
    1. 任务使用 agent
    2. 订阅 topic: test_taosx2
       数据： mytime:1701927703909401600,id:1,current:10.3,voltage:219,phase:0.32,description:Beijing,location:California.SanFrancisco,groupid:3
    3. offset: earliest
    4. transformer 解析: regex
    5. transformer 过滤规则： id.parse_int()>=200 && description.contains("jing") && description.starts_with("Bei")
    6. transformer 映射配置操作有： mapping

    验证点：
    1. 过滤规则生效
    2. 数据正常写入
    """
    kafka_test_logger.info("start test...sanity case: transformer(regex)")
    kafka1 = Producer("kafka/kafka.yaml")
    kafka1.start()
    time.sleep(5)
    kafka1.terminate()
    env_data, case_data_orig = input_data
    case_data = copy.deepcopy(case_data_orig)
    kafka_payload = Util.read_yaml("kafka/payload-basic-transformer-regex.yaml")
    case_data["parser"] = kafka_payload["parser"]
    case_data["from"]["topics"] = "test_taosx2"
    case_data["from"]["group"] = "taosx_group3" + str(random.randint(1, 10000))
    task = Task(env_data, case_data)
    Util.create_stable(case_data, kafka_payload["parser"]["parser"], "ns")
    payload = Util.get_task_payload(case_data, env_data, EnvType.LOCAL)
    task_id = task.run_task(payload)
    task.stop_task_with_retry(task_id)
    metrics = task.get_task_metrics(task_id)
    rows_count = TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    assert rows_count >= 0, f"入库的数据量应大于 0"
    assert metrics["current"]["written_rows"] >= 0, f"任务 metrics 中的 written_rows 应大于 0"
    # check column value
    sqlresult = TaosAdapter.run_sql(
        env_data["taosadapter_host"],
        f"select count(*) from `{case_data['to']['target_dbname']}`.`kafkastb` where id<200",
    )
    assert sqlresult["data"][0][0] == 0, f"id should be not less than 200"
    sqlresult = TaosAdapter.run_sql(
        env_data["taosadapter_host"],
        f"select count(*) from `{case_data['to']['target_dbname']}`.`kafkastb` where description LIKE \"Bei%jing%\"",
    )
    assert sqlresult["data"][0][0] == rows_count, f"description shoud be like Bei%jing%"
    TaosAdapter.drop_db(env_data["taosadapter_host"], case_data["to"]["target_dbname"])


@pytest.mark.sanity
def test_case_base_ssl(input_data):
    """
    用例概述: kafka 用例, 测试SSL证书
    用例步骤：
    1. 任务使用 agent
    2. broker: 192.168.1.45:9093, topic: test_taosx, partition: 1
       数据： {"mytime":myts, "id": myid,"current": 10.77,"phase": 0.77,"voltage": 220, "description": "hello taosx", "groupid": 1,"location":"Beijing"}
    3. offset: earliest

    验证点：
    1. 数据正常写入
    """
    kafka_test_logger.info("start test...sanity case: ssl")
    kafka1 = Producer("kafka/kafka.yaml")
    kafka1.start()
    time.sleep(5)
    kafka1.terminate()
    env_data, case_data_orig = input_data
    case_data = copy.deepcopy(case_data_orig)
    kafka_payload = Util.read_yaml("kafka/payload-basic-transformer-json.yaml")
    case_data["parser"] = kafka_payload["parser"]

    # ssl 认证
    file = File(env_data, task_type)
    ca_dir = file.upload("kafka/ssl/ca-cert")
    client_cert_dir = file.upload("kafka/ssl/clientclient.pem")
    client_key_dir = file.upload("kafka/ssl/clientclient.key")
    case_data["from"]["fromhost"] = f"kafka://{env_data['data_source']['kafka'][1]}"
    case_data["from"]["ca_password"] = "taosdata"
    case_data["from"]["ca"] = f"@{ca_dir}"
    case_data["from"]["cert"] = f"@{client_cert_dir}"
    case_data["from"]["cert_key"] = f"@{client_key_dir}"
    case_data["from"]["group"] = "taosx_group4" + str(random.randint(1, 10000))

    task = Task(env_data, case_data)
    Util.create_stable(case_data, kafka_payload["parser"]["parser"], "ns")
    payload = Util.get_task_payload(case_data, env_data, EnvType.LOCAL)
    task_id = task.run_task(payload)
    task.stop_task_with_retry(task_id)
    metrics = task.get_task_metrics(task_id)
    rows_count = TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    assert rows_count > 0, f"入库的数据量应大于 0"


@pytest.mark.sanity
@pytest.mark.xfail(reason="ubuntu编译出的taosx使用gssapi认证不通过")
def test_case_base_sasl_gssapi(input_data):
    """
    用例概述: kafka 用例, 测试 GSSAPI
    用例步骤：
    1. 任务使用 agent
    2. broker: 192.168.1.45:9094, topic: test_taosx, partition: 1
       数据： {"mytime":1701938756205263616, "id": 1,"current": 10.77,"phase": 0.77,"voltage": 220, "description": "hello taosx", "groupid": 1,"location":"Beijing"}
    3. offset: earliest

    验证点：
    1. 数据正常写入
    """
    kafka_test_logger.info("start test...sanity case: sasl gssapi")
    kafka1 = Producer("kafka/kafka.yaml")
    kafka1.start()
    time.sleep(5)
    kafka1.terminate()
    env_data, case_data_orig = input_data
    case_data = copy.deepcopy(case_data_orig)
    kafka_payload = Util.read_yaml("kafka/payload-basic-transformer-json.yaml")
    case_data["parser"] = kafka_payload["parser"]

    # sasl gssapi 认证
    file = File(env_data, task_type)
    kerberos_keytab = file.upload("kafka/sasl/kafka-client.keytab")
    case_data["from"]["fromhost"] = f"kafka://{env_data['data_source']['kafka'][2]}"
    case_data["from"]["sasl_mechanism"] = "GSSAPI"
    case_data["from"]["sasl_kerberos_service_name"] = "kafka-server"
    case_data["from"]["sasl_kerberos_principal"] = "kafka-client@KERBEROS"
    case_data["from"]["sasl_kerberos_keytab"] = f"@{kerberos_keytab}"
    case_data["from"]["group"] = "taosx_group5" + str(random.randint(1, 10000))

    task = Task(env_data, case_data)
    Util.create_stable(case_data, kafka_payload["parser"]["parser"], "ns")
    payload = Util.get_task_payload(case_data, env_data, EnvType.LOCAL)
    task_id = task.run_task(payload)
    task.stop_task_with_retry(task_id)
    metrics = task.get_task_metrics(task_id)
    rows_count = TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    assert rows_count > 0, f"入库的数据量应大于 0"


@pytest.mark.sanity
@pytest.mark.parametrize("with_agent", [True, False])
def test_case_base_sasl_plain(with_agent, input_data):
    """
    用例概述: kafka 用例, 测试SASL PLAIN认证机制
    用例步骤：
    1. 依据 with_agent 的值决定是否使用 agent
    2. broker: 192.168.1.45:19094, username/passwd: nick/nick-sec, topic: test_taosx, partition: 6
       数据： {"mytime":myts, "id": myid,"current": 10.77,"phase": 0.77,"voltage": 210, "description": "hello taosx", "groupid": 1,"location":"Beijing"}
    3. offset: earliest

    验证点：
    1. 数据正常写入
    2. 验证 metrics 中kafka_consumed_messages, kafka_consumers, kafka_consuming_partitions 和 kafka_total_partitions
    """
    kafka_test_logger.info("start test...sanity case: sasl plain")
    kafka1 = Producer("kafka/kafka1.yaml")
    kafka1.start()
    time.sleep(5)
    kafka1.terminate()

    env_data, case_data_orig = input_data
    case_data = copy.deepcopy(case_data_orig)
    kafka_payload = Util.read_yaml("kafka/payload-basic-transformer-json.yaml")
    case_data["parser"] = kafka_payload["parser"]
    if not with_agent:
        case_data.pop("via")

    # sasl plain 认证
    case_data["from"]["fromhost"] = f"kafka://{env_data['data_source']['kafka'][3]}"
    case_data["from"]["sasl_mechanism"] = "PLAIN"
    case_data["from"]["sasl_username"] = "nick"
    case_data["from"]["sasl_password"] = "nick-sec"
    case_data["from"]["group"] = "taosx_group6" + str(random.randint(1, 10000))
    case_data["from"]["read_concurrency"] = 2

    task = Task(env_data, case_data)
    Util.create_stable(case_data, kafka_payload["parser"]["parser"], "ns")
    payload = Util.get_task_payload(case_data, env_data, EnvType.LOCAL)
    task_id = task.run_task(payload)
    metrics1 = task.get_task_metrics(task_id)
    assert (
        metrics1["current"]["kafka_consumers"] == 2
    ), f"任务运行时 metrics 中的 kafka_consumers 应等于 2"
    assert (
        metrics1["current"]["kafka_consuming_partitions"] == 6
    ), f"任务运行时 metrics 中的 kafka_consuming_partitions 应等于 6"
    assert (
        metrics1["current"]["kafka_total_partitions"] == 6
    ), f"任务运行时 metrics 中的 kafka_total_partitions 应等于 6"
    task.stop_task_with_retry(task_id)
    metrics2 = task.get_task_metrics(task_id)
    rows_count = TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    assert rows_count > 0, f"入库的数据量应大于 0"
    assert (
        metrics2["current"]["kafka_consumed_messages"] > 0
    ), f"任务 metrics 中的 kafka_consumed_messages 应大于 0"
    assert (
        metrics2["current"]["kafka_consumers"] == 0
    ), f"任务停止后 metrics 中的 kafka_consumers 应等于 0"
    assert (
        metrics2["current"]["kafka_consuming_partitions"] == 0
    ), f"任务停止后 metrics 中的 kafka_consuming_partitions 应等于 0"
    assert (
        metrics2["current"]["kafka_total_partitions"] == 6
    ), f"任务停止后 metrics 中的 kafka_total_partitions 应等于 6"


@pytest.mark.sanity
def test_case_base_sasl_scram_sha_256(input_data):
    """
    用例概述: kafka 用例, 测试SASL SCRAM-SHA-256认证机制
    用例步骤：
    1. 任务使用 agent
    2. broker: 192.168.1.45:29094, username/passwd: admin/admin-sec, topic: test_taosx, partition: 1
       数据： {"mytime":myts, "id": myid,"current": 10.77,"phase": 0.77,"voltage": 200, "description": "hello taosx", "groupid": 1,"location":"Beijing"}
    3. offset: earliest

    验证点：
    1. 数据正常写入
    """
    kafka_test_logger.info("start test...sanity case: sasl scram_sha_256")
    kafka1 = Producer("kafka/kafka2.yaml")
    kafka1.start()
    time.sleep(5)
    kafka1.terminate()

    env_data, case_data_orig = input_data
    case_data = copy.deepcopy(case_data_orig)
    kafka_payload = Util.read_yaml("kafka/payload-basic-transformer-json.yaml")
    case_data["parser"] = kafka_payload["parser"]

    # sasl scram_sha_256 认证
    case_data["from"]["fromhost"] = f"kafka://{env_data['data_source']['kafka'][4]}"
    case_data["from"]["sasl_mechanism"] = "SCRAM-SHA-256"
    case_data["from"]["sasl_username"] = "admin"
    case_data["from"]["sasl_password"] = "admin-sec"
    case_data["from"]["group"] = "taosx_group7" + str(random.randint(1, 10000))

    task = Task(env_data, case_data)
    Util.create_stable(case_data, kafka_payload["parser"]["parser"], "ns")
    payload = Util.get_task_payload(case_data, env_data, EnvType.LOCAL)
    task_id = task.run_task(payload)
    task.stop_task_with_retry(task_id)
    metrics = task.get_task_metrics(task_id)
    rows_count = TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    assert rows_count > 0, f"入库的数据量应大于 0"


@pytest.mark.performance
def test_case_performance_scenario1(input_data):
    kafka_test_logger.info("start test...sanity case: performance, scenarion1")
    env_data, case_data_orig = input_data
    case_data = copy.deepcopy(case_data_orig)
    kafka_value = Util.read_yaml("kafka/payload-performance-10col-int.yaml")
    case_data["parser"] = kafka_value["parser"]
    case_data["from"]["topics"] = "test_10col_int_10w_1partition"
    case_data["from"]["fallback_offset"] = "Earliest"
    case_data["to"]["target_dbname"] = "perf_kafka_s1"
    case_data["to"]["column_count"] = 11
    case_data["task_exec_time"] = 10 * 60
    case_data["from"]["group"] = "taosx_group8" + str(random.randint(1, 10000))

    sqlstr = f"CREATE STABLE {case_data['to']['target_dbname']}.kafkastb (ts TIMESTAMP,col1 INT, \
        col2 INT,col3 INT,col4 INT,col5 INT,col6 INT,col7 INT,col8 INT,col9 INT,col10 INT) TAGS (groupid INT);"
    TaosAdapter.create_db(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"], "ns"
    )
    r = TaosAdapter.run_sql(env_data["taosadapter_host"], sqlstr)
    assert r["code"] == 0, f"fail to create database: {r['desc']}"

    task = Task(env_data, case_data)
    payload = Util.get_task_payload(case_data, env_data)
    task.perf_test(
        payload,
        1,
        "1 task to subscribe 1 topic with 1 partition,1 stable,10w subtables,ts column+10 columns(int)",
        True,
    )


@pytest.mark.performance
def test_case_performance_scenario2(input_data):
    kafka_test_logger.info("start test...sanity case: performance, scenarion2")
    env_data, case_data_orig = input_data
    case_data = copy.deepcopy(case_data_orig)
    kafka_value = Util.read_yaml("kafka/payload-performance-10col-int.yaml")
    case_data["parser"] = kafka_value["parser"]
    case_data["from"]["topics"] = "test_10col_int_10w"
    case_data["from"]["fallback_offset"] = "Earliest"
    case_data["to"]["target_dbname"] = "perf_kafka_s2"
    case_data["to"]["column_count"] = 11
    case_data["task_exec_time"] = 10 * 60
    case_data["from"]["group"] = "taosx_group9" + str(random.randint(1, 10000))

    sqlstr = f"CREATE STABLE {case_data['to']['target_dbname']}.kafkastb (ts TIMESTAMP,col1 INT, \
        col2 INT,col3 INT,col4 INT,col5 INT,col6 INT,col7 INT,col8 INT,col9 INT,col10 INT) TAGS (groupid INT);"
    TaosAdapter.create_db(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"], "ns"
    )
    r = TaosAdapter.run_sql(env_data["taosadapter_host"], sqlstr)
    assert r["code"] == 0, f"fail to create database: {r['desc']}"

    task = Task(env_data, case_data)
    payload = Util.get_task_payload(case_data, env_data)
    task.perf_test(
        payload,
        2,
        "1 task to subscribe 1 topic with 10 partition,1 stable,10w subtables,ts column+10 columns(int)",
        True,
    )
