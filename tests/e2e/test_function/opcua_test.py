"""
OPC UA 测试用例
sanity 用例设计：
1.安全模式 None，认证方式选用匿名，只上传 CSV 配置文件，CSV 文件只包含必填列（明确指定超级表，子表），其他全使用默认参数
2.安全模式 Sign，安全策略使用 Basic256，上传安全通信证书即安全通信私钥，连接超时设置为最小值，认证方式选用“用户名”，上传的 CSV 配置文件包含全部列（超级表及子表使用占位符，transform 列为空，ts 在 received_ts 左侧，不包含 quality 列），包含两列 tag 列，指定采集模式为 observe（不修改采集间隔）这里需要判断采集间隔的生效方式，不使用 agent，日志级别调整为 debug。开启原始数据保存（最大保留天数和原始数据存储目录使用默认值）。
3.安全模式 SignAndEncrypt，安全策略使用 Basic256Sha256，上传安全通信证书和安全通信私钥，连接超时设置为最大值，认证方式选用“证书访问”，上传的 CSV 配置文件包含全部列（超级表及子表使用占位符，填写 transform 规则，received_ts 在 ts 左侧，包含 quality 列），包含两列 tag 。开启原始数据保存，修改最大保留天数为 180，修改原始数据存储目录（目录可以先试用 /data/ci_rawdata/）。采集模式设置为 observe，采集间隔设置为 1。
4.安全模式 SignAndEncrypt，安全策略使用 Aes128_Sha256_RsaOaep，上传安全通信证书和安全通信私钥，下载数据点位，下载时不选择过滤条件，同时上传直接使用该文件。采集模式设置为 observe，采集间隔设置为 60。采集超时设置为 60。
5.安全模式为 None，下载数据点位，过滤条件同时设置三个条件。连通性校验。这里需要进行连通性校验，获取命名空间。
6.选择数据点位，不填写过滤条件，其他使用默认值。不使用 agent。
7.选择数据点位，同时设置三个过滤条件，主键列修改为 received_ts。表名称设置为 meter_{ns}_{id}。采集模式模式设置为 Observe，采集间隔设置为 1。
8.下载 CSV 模板（英文和中文）
9.连通性检查
10.宽表模式
11.批量操作任务
"""
import copy
import logging
import os
import allure
import json
from pathlib import Path
from time import sleep, time

import pytest

from testng_taosx.constant import TaskType, TAOSX_LOG_DIR, TaskStatus, CUSTOM_SQLS
from testng_taosx.file import File
from testng_taosx.task import Task
from testng_taosx.util import TaosAdapter
from testng_taosx.util import Util
from testng_taosx.env import ENV
from testng_taosx.constant import *

opcua_test_logger = logging.getLogger(__name__)
task_type = TaskType.OPCUA
OPCUA_CI_DBNAME = "ci_opcua"


@pytest.fixture(scope="function")
def input_data():
    opcua_test_logger.info("before all opc ua cases...")
    env_data = Util.get_env_data()
    TaosAdapter.create_db(env_data["taosadapter_host"], OPCUA_CI_DBNAME)
    yield env_data
    opcua_test_logger.info("after all opc ua cases...")
    TaosAdapter.drop_db(env_data["taosadapter_host"], OPCUA_CI_DBNAME)


@pytest.mark.sanity
def test_sanity_1(input_data):
    """
    1.安全模式 None，认证方式选用匿名，只上传 CSV 配置文件，CSV 文件只包含必填列（明确指定超级表，子表），其他全使用默认参数
    验证点：1.数据写入正常 2.表 Schema 符合预期
    """
    env_data = input_data
    case_data = Util.get_case_data_from_yaml(
        "opcua/test_opcua_csv_config_base.yaml", task_type
    )
    task = Task(env_data, case_data)
    file = File(env_data, task_type)
    file_path = file.upload("opcua/opcua_sanity_1.csv")
    additional_params = {"csv_config_file": file_path}

    metrics = task.sanity_test(additional_params=additional_params)

    rows_count = TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    assert rows_count > 0
    assert metrics["current"]["written_rows"] > 0


@pytest.mark.sanity
def test_sanity_2(input_data):
    """
    2.安全模式 Sign，安全策略使用 Basic256，需要上传安全通信证书即安全通信私钥，连接超时设置为最小值，认证方式选用“用户名”，
    上传的 CSV 配置文件包含全部列（超级表及子表使用占位符，transform 列为空，ts 在 received_ts 左侧，不包含 quality 列），包含 1 列 tag，
    指定采集模式为 observe（不修改采集间隔）这里需要判断采集间隔的生效方式，不使用 agent，
    日志级别调整为 debug。开启原始数据保存（最大保留天数和原始数据存储目录使用默认值）。
    验证点：
        1.数据写入正常
        2.表 Schema 符合预期
        3.数据采集频率符合预期
        4.日志级别符合预期
        5.原始数据保存成功
    """
    env_data = input_data
    case_data = Util.get_case_data_from_yaml(
        "opcua/test_opcua_csv_config_base.yaml", task_type
    )
    case_data["from"][
        "fromhost"
    ] = "opcua://test1:tbase125%21@{0}/OPCUA/SimulationServer"
    case_data["from"]["security_mode"] = "Sign"
    case_data["from"]["security_policy"] = "Basic128Rsa15"
    case_data["from"]["connect_timeout"] = 1
    case_data["from"]["collect_mode"] = "observe"
    case_data["from"]["request_timeout"] = "1"
    case_data["from"]["log_level"] = "debug"
    case_data["from"]["keep_raw_data"] = "true"
    case_data["task_exec_time"] = 25
    task = Task(env_data, case_data)
    file = File(env_data, task_type)
    additional_params = {
        "csv_config_file": file.upload("opcua/opcua_sanity_2.csv"),
        "certificate": file.upload("opcua/certificate.crt"),
        "private_key": file.upload("opcua/private_key.pem"),
    }

    metrics = task.sanity_test(additional_params=additional_params)

    rows_count = TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    assert rows_count > 0
    assert metrics["current"]["written_rows"] > 0


@pytest.mark.sanity
def test_sanity_3(input_data):
    """
    3.安全模式 SignAndEncrypt，安全策略使用 Basic256Sha256，上传安全通信证书和安全通信私钥，连接超时设置为最大值，
    认证方式选用“证书访问”，上传的 CSV 配置文件包含全部列（超级表及子表使用占位符，填写 transform 规则，received_ts 在 ts 左侧，包含），
    采集模式设置为 observe，采集间隔设置为 1。
    包含两列 tag 。开启原始数据保存，修改最大保留天数为 180，修改原始数据存储目录（目录可以先试用 /data/ci_rawdata/）。
    验证点：
        1.任务创建成功，数据写入正常
        2.表 Schema 符合预期（表名及主键列），tag 列
        3.transform 规则不大好验证，需要有一个参照点
    """
    env_data = input_data
    case_data = Util.get_case_data_from_yaml(
        "opcua/test_opcua_csv_config_base.yaml", task_type
    )
    case_data["from"]["security_mode"] = "SignAndEncrypt"
    case_data["from"]["security_policy"] = "Basic256Sha256"
    case_data["from"]["connect_timeout"] = 60
    case_data["from"]["collect_mode"] = "observe"
    case_data["from"]["interval"] = 1  # 采集间隔设置为 1
    case_data["from"]["request_timeout"] = "60"
    case_data["from"]["log_level"] = "info"
    case_data["from"]["keep_raw_data"] = "true"
    case_data["from"]["keep_raw_data_days"] = "180"
    case_data["from"][
        "keep_raw_data_dir"
    ] = f"%2Fdata%2Fci_file_raw%2F"  # /data/ci_file_raw/

    task = Task(env_data, case_data)
    file = File(env_data, task_type)
    additional_params = {
        "csv_config_file": file.upload("opcua/opcua_sanity_3.csv"),
        "certificate": file.upload("opcua/certificate.crt"),
        "private_key": file.upload("opcua/private_key.pem"),
        "auth_certificate": file.upload("opcua/certificate.crt"),
        "auth_private_key": file.upload("opcua/private_key.pem"),
    }
    metrics = task.sanity_test(additional_params=additional_params)

    rows_count = TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    assert rows_count > 0
    assert metrics["current"]["written_rows"] > 0


@pytest.mark.sanity
def test_sanity_4(input_data):
    """
    4.安全模式 SignAndEncrypt，安全策略使用 Aes128_Sha256_RsaOaep，上传安全通信证书和安全通信私钥，
    下载数据点位，下载时不选择过滤条件，同时上传直接使用该文件。
    采集模式设置为 observe，采集间隔设置为 60。采集超时设置为 60。
    验证点：
        1.文件可以正常下载
        2.文件上传之后数据可以正常写入
    """
    env_data = input_data
    case_data = Util.get_case_data_from_yaml(
        "opcua/test_opcua_csv_config_base.yaml", task_type
    )
    case_data["from"]["security_mode"] = "SignAndEncrypt"
    case_data["from"]["security_policy"] = "Aes128_Sha256_RsaOaep"
    case_data["from"]["connect_timeout"] = 60
    case_data["from"]["collect_mode"] = "observe"
    case_data["from"]["interval"] = 1  # 采集间隔设置为 1

    # 下载数据点位文件
    file = File(env_data, task_type)
    certificate_path = file.upload("opcua/certificate.crt")
    private_key_path = file.upload("opcua/private_key.pem")
    auth_certificate_path = file.upload("opcua/certificate.crt")
    auth_private_key_path = file.upload("opcua/private_key.pem")
    case_data["from"]["certificate"] = f"@{certificate_path}"
    case_data["from"]["private_key"] = f"@{private_key_path}"
    case_data["from"]["auth_certificate"] = f"@{auth_certificate_path}"
    case_data["from"]["auth_private_key"] = f"@{auth_private_key_path}"
    case_data["task_exec_time"] = 20
    # case_data["from"]["namespaces"] = f"3"
    payload = Util.get_task_payload(case_data, env_data)
    filename, save_path = File.download_opc_data_points_file(
        task_type, env_data, payload["from"]
    )
    assert os.path.getsize(save_path) > 0, "OPC UA 数据点位下载失败，大小为 0"
    # 直接使用下载的点位文件创建任务
    additional_params = {
        "csv_config_file": file.upload(f"{task_type.value}/{filename}")
    }
    os.remove(save_path)
    task = Task(env_data, case_data)
    metrics = task.sanity_test(additional_params=additional_params)

    rows_count = TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    assert rows_count > 0
    assert metrics["current"]["written_rows"] > 0


@pytest.mark.sanity
def test_sanity_5(input_data):
    """
    5.安全模式为 None，下载数据点位，过滤条件同时设置三个条件。
    """
    env_data = input_data
    case_data = Util.get_case_data_from_yaml(
        "opcua/test_opcua_csv_config_base.yaml", task_type
    )
    # 设置过滤条件
    case_data["from"]["namespaces"] = "3"
    case_data["from"]["root"] = "i=85"
    case_data["from"]["pattern"] = "C"
    case_data["task_exec_time"] = 20
    payload = Util.get_task_payload(case_data, env_data)
    filename, save_path = File.download_opc_data_points_file(
        task_type, env_data, payload["from"]
    )
    assert os.path.getsize(save_path) > 0, "OPC UA 过滤条件数据点位下载失败，大小为 0"
    file = File(env_data, task_type)
    additional_params = {
        "csv_config_file": file.upload(f"{task_type.value}/{filename}")
    }
    os.remove(save_path)
    task = Task(env_data, case_data)
    metrics = task.sanity_test(additional_params=additional_params)

    rows_count = TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    assert rows_count > 0
    assert metrics["current"]["written_rows"] > 0


@pytest.mark.sanity
def test_sanity_6(input_data):
    """
    6.选择数据点位，不填写过滤条件，其他使用默认值。
    不使用 agent。
    主键使用默认的 orginal_ts，表名称使用默认的 t_{ns}_{id}
    数据采集使用默认的 subscribe
    验证点：
        1.任务创建成功，数据写入正常
        2.表 schema 符合预期
    """
    env_data = input_data
    case_data = Util.get_case_data_from_yaml(
        "opcua/test_opcua_csv_config_base.yaml", task_type
    )
    case_data.pop("via")
    case_data["from"]["table_primary_key"] = "original_ts"
    case_data["from"]["child_table_expression"] = "t_%7Bns%7D_%7Bid%7D"
    case_data["from"]["select_all_points"] = "true"
    case_data["task_exec_time"] = 15

    task = Task(env_data, case_data)
    metrics = task.sanity_test()

    rows_count = TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    assert rows_count > 0
    assert metrics["current"]["written_rows"] > 0


@pytest.mark.sanity
def test_sanity_7(input_data):
    """
    7.选择数据点位，不填写过滤条件，其他使用默认值。不使用 agent。
    主键使用 received_ts，表名称使用修改的 meter_{ns}_{id}_t
    数据采集使用 observe 模式，采集间隔为 1s。
    验证点：
        1.任务创建成功，数据写入正常
        2.表 schema 符合预期
    """
    env_data = input_data
    case_data = Util.get_case_data_from_yaml(
        "opcua/test_opcua_csv_config_base.yaml", task_type
    )
    case_data["from"]["collect_mode"] = "observe"
    case_data["from"]["request_timeout"] = "1"
    case_data["from"]["interval"] = "1"

    case_data["from"]["root"] = "i%3D85"
    # 这里使用多个 namespace
    case_data["from"]["namespaces"] = "3,5"
    case_data["from"]["pattern"] = "C"

    case_data["from"]["table_primary_key"] = "received_ts"
    case_data["from"]["child_table_expression"] = "meter_%7Bns%7D_%7Bid%7D_t"
    case_data["from"]["select_all_points"] = "true"
    case_data["task_exec_time"] = 20

    task = Task(env_data, case_data)
    metrics = task.sanity_test()

    rows_count = TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    assert rows_count > 0
    assert metrics["current"]["written_rows"] > 0


@pytest.mark.sanity
def test_download_opcua_template_8(input_data):
    """
    8.下载 OPC CSV 模板（英文和中文）
    """
    env_data = input_data
    file = File(env_data, TaskType.OPCUA)
    filename = file.download_opc_template(TaskType.OPCUA, "zh")
    assert os.path.getsize(filename) > 0, "OPC UA 中文模板下载失败"
    os.remove(filename)
    filename = file.download_opc_template(TaskType.OPCUA, "en")
    assert os.path.getsize(filename) > 0, "OPC UA 英文模板下载失败"
    os.remove(filename)


@pytest.mark.sanity
def test_check_connectivity_9(input_data):
    """
    9.OPC UA 连通性测试
    测试用例包含两个：
        1.安全模式为 None，不使用 Agent
        2.安全模式为 Sign，使用 Linux Agent
        3.安全模式为 SignAndEncrypt，使用 Windows Agent（OPC UA 兼容 Windows）
    """
    case_data = Util.get_case_data_from_yaml(
        "opcua/test_opcua_csv_config_base.yaml", task_type
    )
    dsn = Util.get_task_payload(case_data, input_data)["from"]
    json_result = Util.check_connectivity(input_data, dsn)
    assert json_result["valid"], f"OPC UA 连通性校验失败，result: {json_result} dsn: {dsn}"
    # 证书相关文件上传
    file = File(input_data, task_type)
    certificate_path = file.upload("opcua/certificate.crt")
    private_key_path = file.upload("opcua/private_key.pem")
    auth_certificate_path = file.upload("opcua/certificate.crt")
    auth_private_key_path = file.upload("opcua/private_key.pem")

    case_data["from"]["certificate"] = f"@{certificate_path}"
    case_data["from"]["private_key"] = f"@{private_key_path}"
    case_data["from"]["auth_certificate"] = f"@{auth_certificate_path}"
    case_data["from"]["auth_private_key"] = f"@{auth_private_key_path}"
    case_data["from"]["security_mode"] = "Sign"
    case_data["from"]["security_policy"] = "Aes256_Sha256_RsaPss"
    dsn = Util.get_task_payload(case_data, input_data)["from"]
    # 这里通过 linux 部署的 agent 校验 OPC UA Server 的连通性
    linux_agent = ENV.choose_platform_agent(task_type)
    json_result = Util.check_connectivity(input_data, dsn, linux_agent)
    assert json_result["valid"], f"OPC UA 连通性校验失败，result: {json_result} dsn: {dsn}"

    case_data["from"]["certificate"] = f"@{certificate_path}"
    case_data["from"]["private_key"] = f"@{private_key_path}"
    case_data["from"]["auth_certificate"] = f"@{auth_certificate_path}"
    case_data["from"]["auth_private_key"] = f"@{auth_private_key_path}"
    case_data["from"]["security_mode"] = "SignAndEncrypt"
    case_data["from"]["security_policy"] = "Basic256"
    dsn = Util.get_task_payload(case_data, input_data)["from"]
    # 这里通过 windows 部署的 agent 校验 OPC UA Server 的连通性
    windows_agent = ENV.choose_platform_agent(TaskType.OPCDA)
    json_result = Util.check_connectivity(input_data, dsn, windows_agent)
    assert json_result["valid"], f"OPC UA 连通性校验失败，result: {json_result} dsn: {dsn}"


@pytest.mark.sanity
def test_wide_table_10():
    """
    10.OPC UA 数据写入宽表（多数据列）
        注意：写入宽表的前提是提前创建好超级表和子表
        校验：
            1.数据写入正常
    """
    env_data = Util.get_env_data()
    case_data = Util.get_case_data_from_yaml(
        "opcua/test_opcua_csv_config_base.yaml", task_type
    )
    # 需要事先创建好子表和超级表
    """
        表的 schema 为：
        point_id,stable,tbname,value_col,type
        ns=3;i=1001,stb1,stb1_tb1,val1,double
        ns=3;i=1002,stb1,stb1_tb1,val2,int
        ns=3;i=1003,stb1,stb1_tb1,val3,double
        ns=3;i=1010,stb2,stb2_tb2,val4,boolean
        ns=3;i=1014,stb2,stb2_tb2,val5,varchar(256)
    """
    dbname = case_data["to"]["target_dbname"]
    task = Task(env_data, case_data)
    file = File(env_data, task_type)
    file_path = file.upload("opcua/opcua_sanity_10.csv")
    additional_params = {"csv_config_file": file_path}
    additional_params[CUSTOM_SQLS] = [
        f"CREATE STABLE {dbname}.`stb1` (`ts` TIMESTAMP , `val1` DOUBLE, `val2` INT, `val3` DOUBLE) TAGS (`groupid` VARCHAR(256))",
        f'CREATE TABLE {dbname}.`stb1_tb1` USING {dbname}.`stb1` (`groupid`) TAGS ("group1")',
        f"CREATE STABLE {dbname}.`stb2` (`ts` TIMESTAMP , `val4` BOOL, `val5` varchar(256)) TAGS (`groupid` VARCHAR(256))",
        f'CREATE TABLE `{dbname}`.`stb2_tb2` USING `{dbname}`.`stb2` (`groupid`) TAGS ("group2")',
    ]

    metrics = task.sanity_test(additional_params=additional_params)

    rows_count = TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    assert rows_count > 0
    assert metrics["current"]["written_rows"] > 0


@allure.link("https://jira.taosdata.com:18080/browse/TS-5209")
# @pytest.xfail(reason="反复测试过程中发现任务的状态有些不稳定，可以先随 CI 跑一段时间，见：https://jira.taosdata.com:18080/browse/TD-32106")
@pytest.mark.sanity
def test_tasks_batch_operations_11(input_data):
    """
    11.批量操作任务测试
    批量操作操作包括启动、停止、删除任务
    用例设计：
        1.创建 3 个 OPC UA 的任务，其中一个任务使用 agent，三个用例分别使用相同的数据库，相同的参数
        2.批量停止任务，此时任务状态应全部为停止状态
        3.删除其中任务使用的数据库，批量启动任务，此时被删除数据库的任务状态应是停止中，数据库被删除启动失败，其他两个状态应为运行中
        4.批量停止任务，此时任务状态应全部为停止状态
        4.批量删除任务，任务应能删除成功
    """
    env_data = input_data
    case_data = Util.get_case_data_from_yaml(
        "opcua/test_opcua_csv_config_base.yaml", task_type
    )
    payload2_db = "ci_opcua2"
    case_data2 = copy.deepcopy(case_data)
    case_data2["to"]["target_dbname"] = payload2_db
    TaosAdapter.create_db(env_data["taosadapter_host"], payload2_db)
    task = Task(env_data, case_data)
    file = File(env_data, task_type)
    file_path = file.upload("opcua/opcua_sanity_1.csv")
    # upload_file_path = file.upload("opcua/opcua_10.csv")
    payload = Util.get_task_payload(case_data, env_data)

    payload = File.add_file_param(payload, File.file_key["opcua"][0], file_path)
    task_info1 = task.create_task(payload)
    payload2 = File.add_file_param(
        Util.get_task_payload(case_data2, env_data),
        File.file_key["opcua"][0],
        file_path,
    )
    task_info2 = task.create_task(payload2)
    task_info3 = task.create_task(payload)
    sleep(10)
    task_ids = [task_info1["id"], task_info2["id"], task_info3["id"]]
    stop_response = task.batch_stop_tasks(task_ids)
    assert (
        stop_response.status_code == 200
    ), f"batch stop tasks failed: {stop_response.text}"
    sleep(10)
    TaosAdapter.drop_db(env_data["taosadapter_host"], payload2_db)
    task.batch_start_tasks(task_ids)
    sleep(10)
    task1_status = task.get_task_status(task_info1["id"])
    assert (
        task1_status["status"] == TaskStatus.RUNNING.value
    ), f"task1 status: {task1_status} 应为 RUNNING"
    task2_status = task.get_task_status(task_info2["id"])
    assert (
        task2_status["status"] == TaskStatus.STOPPED.value
    ), f"task2 status: {task2_status} 应为 STOPPED"
    task3_status = task.get_task_status(task_info3["id"])
    assert (
        task3_status["status"] == TaskStatus.RUNNING.value
    ), f"task3 status: {task3_status} 应为 RUNNING"
    task.batch_stop_tasks(task_ids)
    sleep(10)
    delete_response = task.batch_delete_tasks(task_ids)
    assert (
        delete_response.status_code == 200
    ), f"batch delete tasks failed: {delete_response.text}"


from testng_taosx.requests_wrapper import http
from testng_taosx.opc_point import *
from dateutil import parser


@allure.link("https://jira.taosdata.com:18080/browse/TS-5209")
@pytest.mark.sanity
@pytest.mark.parametrize("with_agent", [True, False])
def test_task_add_points_12(with_agent, input_data):
    """
    12.使用 CSV 配置的任务运行过程中添加数据点位
        1.创建一个使用 CSV 配置的 OPC 任务，CSV 配置包含全部列（使用下载得到的 CSV 文件格式），这里直接复用 opcua_sanity_12.csv，其它参数使用默认参数
        2.待任务正常运行之后调用添加数据点位接口，添加一个数据点位 A，数据点位的配置与 CSV 中的配置一致（超级表、子表使用占位符）
        3.添加点位 B，超级表子表均不存在的，指定 ts_transform 和 rts_transform，不指定 value_transform（暂时没有固定的校验方式）
        4.添加点位 C，enable 为 0
        5.依据 with_agent 的值决定是否使用 agent
    验证点：
        1.使用占位符的点位添加之后数据写入正常
        2.使用新的超级表和子表的点位配置添加之后数据写入正常
        3.添加点位之后，通过“查看点位列表”获取的点位数据包含新增的数据点位
        4.enable 为 0 的点位的数据不会采集，子表不会创建
        5.点位 B 的 ts 和 rts 与北京时间相比有设置的毫秒差值
    """
    env_data = input_data
    case_data = Util.get_case_data_from_yaml(
        "opcua/test_opcua_csv_config_base.yaml", task_type
    )
    via = case_data["via"]
    if not with_agent:
        # 删除 agent 信息，即创建任务不通过 agent
        case_data.pop("via")
        via = None
    task = Task(env_data, case_data)
    file = File(env_data, task_type)
    payload = Util.get_task_payload(case_data, env_data)
    payload = File.add_file_param(
        payload, File.file_key["opcua"][0], file.upload("opcua/opcua_sanity_12.csv")
    )
    task_info = task.create_task(payload)
    sleep(15)
    rows_count = TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    metrics = task.get_task_metrics(task_info["id"])
    assert rows_count > 0, f"库{OPCUA_CI_DBNAME}中的数据行数应大于 0"
    assert metrics["current"]["written_rows"] > 0, f"任务 metrics 中的 written_rows 应大于 0"
    # 接口调用获取要填的参数，这里只做调用不实际使用
    # 接口地址：GET /ds/in/opc/csv/points/header?task_id=169
    response = http.request(
        "GET",
        f"{env_data['taos_explorer_root_endpoint']}{TAOSX_BASE_URL}/ds/in/opc/csv/points/header?task_id={task_info['id']}",
    )
    assert response.status_code == 200, f"获取任务 ID {task_info['id']} 失败: {response.text}"
    # 添加点位 A
    point_a_point_id = "ns=3;i=1003"
    point_a_tbname = "t_3_1003"
    point_a_tag_list = [
        OPCTagConfig(name="name", value=point_a_point_id, tag_type="varchar(200)"),
        OPCTagConfig(name="groupid", value="1003", tag_type="int"),
    ]
    OPCPointUtil.add_opc_point(
        task_id=task_info["id"],
        opc_type=TaskType.OPCUA,
        number_value="12",
        point_id_or_tag_name=point_a_point_id,
        enabled="1",
        stable="opc_{type}",
        tbname="t_{ns}_{id}",
        value_col="val",
        value_transform=None,
        point_type="double",
        quality_col="quality",
        ts_col="ts",
        received_ts_col="rts",
        ts_transform=None,
        received_ts_transform=None,
        tag_list=point_a_tag_list,
        via=via,
    )
    # 添加点位 B
    point_b_point_id = "ns=3;i=1004"
    point_b_stable = "ci_stb1"
    point_b_tbname = "ci_tb1"
    ts_transform = "ts + 8 * 3600 * 1000"
    rts_transform = "rts + 8 * 3600 * 1000"
    point_b_tag_list = [
        OPCTagConfig(name="name", value=point_a_point_id, tag_type="varchar(200)"),
        OPCTagConfig(name="groupid", value="1004", tag_type="int"),
    ]
    OPCPointUtil.add_opc_point(
        task_id=task_info["id"],
        opc_type=TaskType.OPCUA,
        number_value="13",
        point_id_or_tag_name=point_b_point_id,
        enabled="1",
        stable=point_b_stable,
        tbname=point_b_tbname,
        value_col="val",
        value_transform=None,
        point_type=None,
        quality_col="quality",
        ts_col="ts",
        ts_transform=ts_transform,
        received_ts_transform=rts_transform,
        received_ts_col="rts",
        tag_list=point_b_tag_list,
        via=via,
    )
    # 添加点位 C enable 为 0
    point_c_point_id = "ns=3;i=1005"
    point_c_stable = "opc_{type}"
    point_c_tbname = "t_{ns}_{id}"
    point_c_tag_list = [
        OPCTagConfig(name="name", value=point_c_point_id, tag_type="varchar(200)"),
        OPCTagConfig(name="groupid", value="1005", tag_type="int"),
    ]
    OPCPointUtil.add_opc_point(
        task_id=task_info["id"],
        opc_type=TaskType.OPCUA,
        number_value="14",
        point_id_or_tag_name=point_c_point_id,
        enabled="0",
        stable=point_c_stable,
        tbname=point_c_tbname,
        value_col="val",
        value_transform=None,
        point_type=None,
        quality_col="quality",
        ts_col="ts",
        ts_transform=None,
        received_ts_transform=None,
        received_ts_col="rts",
        tag_list=point_c_tag_list,
        via=via,
    )
    # 调用查看点位接口，新增的点位应当包含在内
    ticket = File.download_opc_data_points_file(
        task_type, env_data, payload["from"], exec_download=False
    )
    response = http.request(
        "GET",
        f"{env_data['taos_explorer_root_endpoint']}{TAOSX_BASE_URL}/ds/in/point/data/page?ticket={ticket}&page=1&page_size=20",
    )
    assert response.status_code == 200, f"查询点位失败: {response.text}"
    point_data = json.loads(response.text)
    assert point_data["code"] == 0, f"查询点位失败: {point_data}"
    assert (
        point_a_point_id in response.text
    ), f"点位 A {point_a_point_id} 未找到: {response.text}"
    assert (
        point_b_point_id in response.text
    ), f"点位 B {point_b_point_id} 未找到: {response.text}"
    assert (
        point_c_point_id in response.text
    ), f"点位 C {point_c_point_id} 未找到: {response.text}"
    # 等待新增的点位生效
    sleep(60)
    # 验证正确生效
    TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    response = TaosAdapter.run_sql(
        env_data["taosadapter_host"],
        f"select count(*) from {OPCUA_CI_DBNAME}.`{point_a_tbname}`",
    )
    assert (
        response["data"][0][0] > 0
    ), f"点位 A 数据写入失败:  {OPCUA_CI_DBNAME}.`{point_a_tbname}` {response}"
    now_resp = TaosAdapter.run_sql(env_data["taosadapter_host"], f"select now()")
    now = int(parser.parse(now_resp["data"][0][0]).timestamp())
    sleep(5)
    response = TaosAdapter.run_sql(
        env_data["taosadapter_host"],
        f"select LAST_ROW(ts), LAST_ROW(rts) from {OPCUA_CI_DBNAME}.`{point_b_tbname}`",
    )
    # ts 及 rts 与北京时间相比有设置的毫秒差值
    last_row_ts = int(parser.parse(response["data"][0][0]).timestamp())
    last_row_rts = int(parser.parse(response["data"][0][1]).timestamp())
    assert (
        last_row_ts - now >= 8 * 3600
    ), f"点位 B ts_transform 没有生效:  last_row_ts: {last_row_ts} now: {now}"
    assert (
        last_row_rts - now >= 8 * 3600
    ), f"点位 B rts_transform 没有生效:  last_row_rts: {last_row_rts} now: {now}"
    table_list = TaosAdapter.get_table_list(
        env_data["taosadapter_host"], OPCUA_CI_DBNAME
    )
    print(table_list)
    assert (
        point_c_tbname not in table_list
    ), f"点位 C 数据 enable 0 失效: {OPCUA_CI_DBNAME}.`{point_c_tbname}` 不应存在于 {table_response}"
    # 停止及删除任务
    task.stop_task_with_retry(task_info["id"])
    task.delete_task(task_info["id"])


@pytest.mark.parametrize(
    "files",
    [
        # ("opcua/opcua_10.csv", "opcua/opcua_15.csv"),
        # ("opcua/opcua_15.csv", "opcua/opcua_10.csv"),
        # ("opcua/opcua_10.csv", "opcua/opcua_add_remove.csv"),
        ("opcua/opcua_10.csv", "opcua/opcua_0.csv")
    ],
)
def test_change_points(input_data, files):
    env_data = input_data
    case_data = Util.get_case_data_from_yaml("opcua/test_opcua_csv.yaml", task_type)

    uploaded_files = files

    task = Task(env_data, case_data)

    file = File(env_data, TaskType.OPCUA)
    upload_file_path1 = file.upload(uploaded_files[0])
    upload_file_path2 = file.upload(uploaded_files[1])

    metrics = task.sanity_test_edit(
        case_data,
        case_data,
        File.file_key["opcua"][0],
        upload_file_path1,
        upload_file_path2,
    )

    rows_count = TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    assert rows_count == metrics["current"]["written_rows"]


@pytest.mark.parametrize("files", [(1, 1), (2, 2)])
def test_aaa(input_data, files):
    env_data = input_data
    uploaded_files = files
    print(uploaded_files[0], uploaded_files[1])
    TaosAdapter.drop_stable(env_data["taosadapter_host"], "test", "meters")
    r1 = TaosAdapter.check_db_count(env_data["taosadapter_host"], "opcua1")
    r2 = TaosAdapter.check_db_count(env_data["taosadapter_host"], "opcua", "stb_int")
    print(r1, r2)


def test_load_10k(input_data):
    env_data = input_data
    case_data = Util.get_case_data_from_yaml("opcua/test_opcua_csv.yaml", task_type)

    case_data["task_exec_time"] = 300
    task = Task(env_data, case_data)
    file = File(env_data, TaskType.OPCUA)
    upload_file_path = file.upload("opcua/opcua_10k.csv")

    file_config = {File.file_key["opcua"][0]: upload_file_path}
    metrics = task.sanity_test(additional_params=file_config)

    rows_count = TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    assert rows_count == metrics["current"]["written_rows"]


def test_multiple_tasks(input_data):
    env_data = input_data
    TaosAdapter.create_db(env_data["taosadapter_host"], "opcua2")
    case_data = Util.get_case_data_from_yaml("opcua/test_opcua_csv.yaml", task_type)

    case_data2 = copy.deepcopy(case_data)
    case_data2["to"]["target_dbname"] = "opcua2"

    task = Task(env_data, case_data)
    task2 = Task(env_data, case_data2)

    file = File(env_data, TaskType.OPCUA)
    upload_file_path = file.upload("opcua/opcua_10.csv")
    file_config = {File.file_key["opcua"][0]: upload_file_path}

    metrics = task.sanity_test(additional_params=file_config)
    metrics2 = task2.sanity_test(additional_params=file_config)

    rows_count = TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    assert rows_count == metrics["current"]["written_rows"]

    rows_count = TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data2["to"]["target_dbname"]
    )
    assert rows_count == metrics["current"]["written_rows"]


@pytest.mark.xfail
def test_invalid_source(input_data):
    env_data = input_data
    case_data = Util.get_case_data_from_yaml("opcua/test_opcua_csv.yaml", task_type)

    case_data["from"]["fromhost"] = "opcua://192.168.2.100:8080/OPCUA/SimulationServer"
    task = Task(env_data, case_data)

    file = File(env_data, TaskType.OPCUA)
    upload_file_path = file.upload("opcua/opcua_10.csv")

    payload = Util.get_task_payload(case_data, env_data)
    payload = File.add_file_param(payload, File.file_key["opcua"][0], upload_file_path)

    task_info = task.create_task(payload)
    task_id = task_info["id"]
    sleep(10)

    r = task.get_task_status(task_id)
    # 错误信息在 OPC 连接器重构后修改为了 panic "new opc client error" error=dial tcp 192.168.2.100:8080: i/o timeout model=collect panic: (*logrus.Entry) 0xc000308070
    # assert r["status"] == TaskStatus.INTERRUPTED.value and "connect error" in r["reason"]
    assert r["status"] == TaskStatus.INTERRUPTED.value


@pytest.mark.skip
def test_opcua_performance(input_data):
    """
    创建 OPC UA 任务的性能测试任务，使用的点位配置为每个包含 10000 的 CSV 配置文件
    config/opcua/performance/ 每个文件都会对应创建一个任务
    """
    env_data = input_data
    current_path = os.getcwd()
    path = Path("/config/opcua/performance/")
    print(path)
    index = 0
    for file_path in path.rglob("*"):
        # index += 1
        # if index >= 2:
        #    break
        if file_path.is_file():
            file_path_replaced = file_path._str.replace("config/", "")
            case_data = Util.get_case_data_from_yaml(
                "opcua/test_opcua_performance_save.yaml", task_type
            )
            payload = Util.get_task_payload(case_data=case_data, env_data=env_data)
            if "observe" in file_path._str:
                payload["from"] += f"&collect_mode=observe"
                payload["name"] = f"{case_data['task_name']}-observe"
            if "subscribe" in file_path._str:
                payload["from"] += f"&collect_mode=subscribe"
                payload["name"] = f"{case_data['task_name']}-subscribe"
            file = File(env_data, task_type)
            file_path_for_url = file.upload(file_path_replaced)
            payload = File.add_file_param(payload, "csv_config_file", file_path_for_url)
            task = Task(env_data, case_data)
            # task.delete_all_tasks()
            # break
            # 只创建任务，不校验状态，任务状态的校验交给 sanity 以及前期的测试
            task.create_task(payload)


@pytest.mark.performance
def test_opcua_performance_s1(input_data):
    """
    创建 OPC UA 任务的性能测试任务，使用的点位配置为每个包含 10000 的 CSV 配置文件
    """
    env_data = input_data
    file_path = "opcua/performance/opcua-double-0-observe-data-0.csv"
    case_data = Util.get_case_data_from_yaml(
        "opcua/test_opcua_performance_save.yaml", task_type
    )
    case_data["to"]["target_dbname"] = "perf_opcua_s1"
    payload = Util.get_task_payload(case_data=case_data, env_data=env_data)
    payload["from"] += f"&collect_mode=observe"
    payload["name"] = f"{case_data['task_name']}-observe"

    file = File(env_data, task_type)
    file_path_for_url = file.upload(file_path)
    payload = File.add_file_param(payload, "csv_config_file", file_path_for_url)

    task = Task(env_data, case_data)
    case_data["task_exec_time"] = 10 * 60
    case_data["to"]["column_count"] = 2
    TaosAdapter.create_db(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    task.perf_test(
        payload,
        1,
        "1 task, 1w points, 1w subtable, observe mode, production rate is 1w/s",
        True,
    )


# import queue
import itertools


# @pytest.mark.performance
@pytest.mark.skip
def test_opcua_performance_multi(input_data):
    """
    OPC UA 性能测试
    创建的测试用例是所有涉及性能的变量的一个组合
    例如：
        param_matrix:
            use_agent: [true, false]
            collect_mode: [subscribe, observe]
        则会的组合有
            {'use_agent': True, 'collect_mode': 'subscribe'}
            {'use_agent': True, 'collect_mode': 'observe'}
            {'use_agent': False, 'collect_mode': 'subscribe'}
            {'use_agent': False, 'collect_mode': 'observe'}
    """
    env_data = input_data
    # file_path = "opcua/performance/opcua-double-0-observe-data-0.csv"
    yaml_data = Util.get_case_data_from_yaml(
        "opcua/performance/opc_ua_performace_matrix.yaml", task_type
    )
    yaml_data["to"]["target_dbname"] = "p_opcua"

    file = File(env_data, task_type)
    performance_param_matrix = yaml_data[PERFORMANCE_PARAM_MATRIX_KEY]
    keys = list(performance_param_matrix.keys())
    # 获取参数的笛卡尔积
    combinations = list(
        itertools.product(*[performance_param_matrix[key] for key in keys])
    )
    scenario_id = 1
    for combination in combinations:
        params = dict(zip(keys, combination))
        print(f"task params: {params}")
        case_data = yaml_data.copy()
        case_data["task_exec_time"] = 10 * 60
        case_data["to"]["column_count"] = 2
        files = {}
        for key, value in params.items():
            if key == PERFORMANCE_AGENT_KEY:
                if not bool(value):
                    case_data.pop("via")
            elif is_file_param(key):
                files[key] = value
            else:
                case_data["from"][key] = value

        payload = Util.get_task_payload(case_data=case_data, env_data=env_data)
        for key, value in files.items():
            file_path_for_url = file.upload(value)
            payload = File.add_file_param(payload, key, file_path_for_url)
        task = Task(env_data, case_data)
        TaosAdapter.create_db(
            env_data["taosadapter_host"], case_data["to"]["target_dbname"]
        )
        task.perf_test(
            payload,
            scenario_id,
            f"1 task, 1w points, 1w subtable, observe mode, production rate is 1w/s",
            True,
        )
        scenario_id += 1
        break


def is_file_param(key):
    return key in TASK_FILE_PARAM
