import copy
import logging
import os
from typing import Dict
from pathlib import Path
import pytest
import json
from time import sleep, time

from testng_taosx.constant import TaskType
from testng_taosx.env import ENV, Env
from testng_taosx.file import File
from testng_taosx.task import Task
from testng_taosx.util import TaosAdapter
from testng_taosx.util import Util

opcda_test_logger = logging.getLogger(__name__)
task_type = TaskType.OPCDA
OPCDA_CI_DBNAME = "ci_opcda"

# OPC DA sanity 用例设计：
# 1.使用只包含必填列的 CSV 配置文件上传，配置文件，其他全使用默认参数
# 2.使用包含所有列的 CSV 配置文件（tag 使用多列），连接超时设置为最大 60s，请求超时设置为最大 60s，采集间隔使用默认值 1s，日志级别指定为 debug，保存原始数据打开，保留天数和原始数据存储目录使用默认值
# 3.使用配置了 transform 的 CSV 配置文件，值类型不同的话使用的 transform 规则也不同，保存原始数据，修改保留天数为 180，修改原始数据存储目录为 E:\opcda-data
# 4.使用下载点位获取的 CSV 文件直接上传创建任务
# 5.使用选择数据点位不填写过滤条件创建任务，主键列为 orignial_ts，表名称为 t_{TagName}
# 6.使用选择数据点位填写过滤条件创建任务，主键列为 received_ts，表名称为 meter_t_{TagName}


@pytest.fixture(scope="function")
def input_data():
    opcda_test_logger.info("before opc da test...")
    env_data = Util.get_env_data()
    TaosAdapter.create_db(env_data["taosadapter_host"], "ci_opcda")
    yield env_data

    opcda_test_logger.info("after opc da test...")
    TaosAdapter.drop_db(env_data["taosadapter_host"], "ci_opcda")


@pytest.mark.sanity
def test_sanity_1(input_data):
    """
    1.使用只包含必填列的 CSV 配置文件上传，配置文件，其他全使用默认参数
    验证点：
        1.数据写入正常
        2.表 Schema 符合预期
    """
    files_to_upload = {"csv_config_file": "opcda/opcda_sanity_1.csv"}
    case_data = opcda_sanity_save(
        input_data, "opcda/test_opcda_csv_config_base.yaml", files_to_upload
    )
    stables = TaosAdapter.show_stables_or_tables(case_data["to"]["target_dbname"], True)
    assert len(stables) > 0, "stables should be created"
    stable1 = stables[0]
    column_data_with_meta = TaosAdapter.desc_table_or_stable(
        case_data["to"]["target_dbname"], stable1
    )
    assert "ts::TIMESTAMP(8)::" == column_data_with_meta[0], "ts 列应当是主键列"
    assert "quality::INT(4)::" not in column_data_with_meta, "quality 列不应该存在"


@pytest.mark.sanity
def test_sanity_2(input_data):
    """
    2.使用包含所有列的 CSV 配置文件（tag 使用 2 列），包含不存在的点位
    连接超时设置为最大 60s，请求超时设置为最大 60s，采集间隔使用默认值 1s，
    日志级别指定为 debug，保存原始数据打开，保留天数和原始数据存储目录使用默认值，
    ts 在左，rts 在右
    验证点：
        1.数据写入正常
        2.表 Schema 符合预期
    """
    files_to_upload = {"csv_config_file": "opcda/opcda_sanity_2.csv"}
    case_data_modify = {}
    case_data_modify["connect_timeout"] = 60
    case_data_modify["request_timeout"] = 60
    case_data_modify["log_level"] = "debug"
    case_data_modify["keep_raw_data"] = "true"
    case_data = opcda_sanity_save(
        input_data, "opcda/test_opcda_csv_config_base.yaml", files_to_upload
    )
    stables = TaosAdapter.show_stables_or_tables(case_data["to"]["target_dbname"], True)
    assert len(stables) > 0, f"库 {case_data['to']['target_dbname']} 中的 stable 不应为空"
    stable1 = stables[0]
    column_data_with_meta = TaosAdapter.desc_table_or_stable(
        case_data["to"]["target_dbname"], stable1
    )
    assert "ts::TIMESTAMP(8)::" == column_data_with_meta[0], "ts 列应当是主键列"
    assert "rts::TIMESTAMP(8)::" in column_data_with_meta, "rts 列应当存在"
    assert "quality::INT(4)::" in column_data_with_meta, "quality 列应当存在"


@pytest.mark.sanity
def test_sanity_3(input_data):
    """
    3.使用包含所有列的 CSV 配置文件
    received_ts_col 在左，ts_col 在右
    配置 value_transform，配置ts_transform 和 received_ts_transform
    修改 quality 列名为 quality11
    超级表表名和子表名使用占位符
    验证点：
        1.数据写入正常
        2.表 Schema 符合预期
    """
    files_to_upload = {"csv_config_file": "opcda/opcda_sanity_3.csv"}
    case_data_modify = {}
    case_data_modify["connect_timeout"] = 1
    case_data_modify["request_timeout"] = 1
    case_data_modify["log_level"] = "debug"
    case_data_modify["keep_raw_data"] = "true"
    case_data_modify["keep_raw_days"] = 180
    case_data_modify["keep_raw_data"] = "true"
    case_data = opcda_sanity_save(
        input_data, "opcda/test_opcda_csv_config_base.yaml", files_to_upload
    )
    stables = TaosAdapter.show_stables_or_tables(case_data["to"]["target_dbname"], True)
    assert len(stables) > 0, "stables should be created"
    stable1 = stables[0]
    column_data_with_meta = TaosAdapter.desc_table_or_stable(
        case_data["to"]["target_dbname"], stable1
    )
    assert "rts::TIMESTAMP(8)::" == column_data_with_meta[0], "rts 列应当是主键列"
    assert "original_ts::TIMESTAMP(8)::" in column_data_with_meta, "original_ts 列应当存在"
    assert "quality11::INT(4)::" in column_data_with_meta, "quality11 列应当存在"


@pytest.mark.sanity
def test_sanity_4(input_data):
    """
    4.使用下载点位获取的 CSV 文件直接上传创建任务
    rts 在左，ts 在右
    验证点：
        1.数据写入正常
        2.表 Schema 符合预期
    """
    # 下载数据点位文件
    env_data = input_data
    file = File(env_data, task_type)
    case_data = Util.get_case_data_from_yaml(
        "opcda/test_opcda_csv_config_base.yaml", task_type
    )
    # case_data["from"]["root"] = "Configured Aliases.device0"
    case_data["via"] = ENV.choose_platform_agent(task_type)
    payload = Util.get_task_payload(case_data, env_data)
    filename, save_path = File.download_opc_data_points_file(
        task_type, env_data, payload["from"], via=case_data["via"]
    )
    assert os.path.getsize(save_path) > 0, "OPC DA 数据点位下载失败，大小为 0"
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
    5.使用选择数据点位填写过滤条件创建任务，主键列为 original_ts t_{TagName}
    验证点：
        1.数据写入正常
        2.表 Schema 符合预期
    """
    env_data = input_data
    case_data = Util.get_case_data_from_yaml(
        "opcda/test_opcda_csv_config_base.yaml", task_type
    )
    case_data["from"]["table_primary_key"] = "original_ts"
    case_data["from"]["table_primary_key_alias"] = "original_ts"
    case_data["from"]["child_table_expression"] = "t_%7BTagName%7D"
    case_data["from"]["select_all_points"] = "true"
    case_data["task_exec_time"] = 15

    task = Task(env_data, case_data)
    metrics = task.sanity_test()

    rows_count = TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    assert rows_count > 0
    assert metrics["current"]["written_rows"] > 0
    stables = TaosAdapter.show_stables_or_tables(case_data["to"]["target_dbname"], True)
    assert len(stables) > 0, "stables should be created"
    stable1 = stables[0]
    column_data_with_meta = TaosAdapter.desc_table_or_stable(
        case_data["to"]["target_dbname"], stable1
    )
    assert (
        "original_ts::TIMESTAMP(8)::" == column_data_with_meta[0]
    ), "original_ts 列应当是主键列"
    # assert "received_ts::TIMESTAMP(8)::" in column_data_with_meta, "received_ts 列应当存在"
    assert "quality::INT(4)::" in column_data_with_meta, "quality 列应当存在"


@pytest.mark.sanity
def test_sanity_6(input_data):
    """
    6.使用选择数据点位不填写过滤条件创建任务，主键列为 received_ts，表名称为 meters_t_{TagName}
    验证点：
        1.数据写入正常
        2.表 Schema 符合预期
    """
    env_data = input_data
    case_data = Util.get_case_data_from_yaml(
        "opcda/test_opcda_csv_config_base.yaml", task_type
    )
    case_data["from"]["table_primary_key"] = "received_ts"
    case_data["from"]["table_primary_key_alias"] = "received_ts"
    case_data["from"]["child_table_expression"] = "meters_t_%7BTagName%7D"
    case_data["from"]["select_all_points"] = "true"
    case_data["task_exec_time"] = 15

    # 过滤条件
    case_data["from"]["root"] = "Configured%20Aliases%2edevice0"
    case_data["from"]["pattern"] = "device0"

    task = Task(env_data, case_data)
    metrics = task.sanity_test()

    rows_count = TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    assert rows_count > 0
    assert metrics["current"]["written_rows"] > 0
    stables = TaosAdapter.show_stables_or_tables(case_data["to"]["target_dbname"], True)
    assert len(stables) > 0, "stables should be created"
    stable1 = stables[0]
    column_data_with_meta = TaosAdapter.desc_table_or_stable(
        case_data["to"]["target_dbname"], stable1
    )
    assert (
        "received_ts::TIMESTAMP(8)::" == column_data_with_meta[0]
    ), "received_ts 列应当是主键列"
    assert "quality::INT(4)::" in column_data_with_meta, "quality 列应当存在"


def opcda_sanity_save(
    input_data,
    yaml_config_path,
    files_to_upload: Dict = None,
    case_data_modify: Dict = None,
):
    env_data = input_data
    case_data = Util.get_case_data_from_yaml(yaml_config_path, task_type)
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
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    assert rows_count > 0
    assert metrics["current"]["written_rows"] > 0
    return case_data


@pytest.mark.sanity
def test_check_connectivity(input_data):
    """
    7.OPC DA 连通性测试
    """
    case_data = Util.get_case_data_from_yaml(
        "opcda/test_opcda_csv_config_base.yaml", task_type
    )
    dsn = Util.get_task_payload(case_data, input_data)["from"]
    json_result = Util.check_connectivity(
        input_data, dsn, ENV.choose_platform_agent(task_type)
    )
    assert json_result["valid"], f"OPC DA 连通性校验失败，result: {json_result} dsn: {dsn}"


from testng_taosx.requests_wrapper import http
from testng_taosx.opc_point import *
from dateutil import parser


@pytest.mark.sanity
def test_task_add_points_8(input_data):
    """
    用例概述：测试使用 CSV 配置的 OPC DA 任务运行过程中添加点位
    用例使用：
        1.起始用例点位包含 device0.tagd0_0，device0.tagd0_1
        2.之后添加点位 device0.tagd0_2（A），device0.tagd0_3（B），device0.tagd0_4（C），
        其中点位 A 和 B enable 为 1，点位 C enable 为 0
        点位 A 和 C 的 stable 为 opc_{type} 子表为 t_{tag_name}，点位 B 的 stable 为 ci_stb1，子表为 ci_tb1
    验证点：
        1.任务成功创建之后，数据写入正常
        2.三个点位添加正常
        3.点位添加之后等待一分钟，A 和 B 的点位数据采集正常，C 的点位数据不会采集
    """
    env_data = input_data
    case_data = Util.get_case_data_from_yaml(
        "opcda/test_opcda_csv_config_base.yaml", task_type
    )
    via = case_data["via"]
    task = Task(env_data, case_data)
    file = File(env_data, task_type)
    payload = Util.get_task_payload(case_data, env_data)
    payload = File.add_file_param(
        payload, File.file_key["opcda"][0], file.upload("opcda/opcda_sanity_8.csv")
    )
    task_info = task.create_task(payload)
    sleep(15)
    rows_count = TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    metrics = task.get_task_metrics(task_info["id"])
    assert rows_count > 0, f"库{OPCDA_CI_DBNAME}中的数据行数应大于 0, 实际为 {rows_count}"
    assert (
        metrics["current"]["written_rows"] > 0
    ), f"任务 metrics 中的 written_rows 应大于 0, 实际为 {metrics}"
    # 接口调用获取要填的参数，这里只做调用不实际使用
    # 接口地址：GET /ds/in/opc/csv/points/header?task_id=169
    response = http.request(
        "GET",
        f"{env_data['taos_explorer_root_endpoint']}{TAOSX_BASE_URL}/ds/in/opc/csv/points/header?task_id={task_info['id']}",
    )
    assert response.status_code == 200, f"获取任务 ID {task_info['id']} 失败: {response.text}"
    # 添加点位 A
    point_a_tag = "device0.tagd0_2"
    point_a_tbname = "t_tagd0_2"
    point_a_tag_list = [
        OPCTagConfig(name="name", value=point_a_tag, tag_type="varchar(200)"),
        OPCTagConfig(name="groupid", value="2", tag_type="int"),
    ]
    OPCPointUtil.add_opc_point(
        task_id=task_info["id"],
        opc_type=TaskType.OPCDA,
        number_value="3",
        point_id_or_tag_name=point_a_tag,
        enabled="1",
        stable="opc_{type}",
        tbname="t_{tag_name}",
        value_col="val",
        value_transform=None,
        point_type="varchar(200)",
        quality_col="quality",
        ts_col="ts",
        received_ts_col="rts",
        ts_transform=None,
        received_ts_transform=None,
        tag_list=point_a_tag_list,
        via=via,
    )
    # 添加点位 B
    point_b_tag = "device0.tagd0_3"
    point_b_stable = "ci_stb1"
    point_b_tbname = "ci_tb1"
    ts_transform = "ts + 8 * 3600 * 1000"
    rts_transform = "rts + 8 * 3600 * 1000"
    point_b_tag_list = [
        OPCTagConfig(name="name", value=point_b_tag, tag_type="varchar(200)"),
        OPCTagConfig(name="groupid", value="3", tag_type="int"),
    ]
    OPCPointUtil.add_opc_point(
        task_id=task_info["id"],
        opc_type=TaskType.OPCDA,
        number_value="4",
        point_id_or_tag_name=point_b_tag,
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
    point_c_tag = "device0.tagd0_4"
    point_c_stable = "opc_{type}"
    point_c_tbname = "t_{tag_name}"
    point_c_tag_list = [
        OPCTagConfig(name="name", value=point_c_tag, tag_type="varchar(200)"),
        OPCTagConfig(name="groupid", value="4", tag_type="int"),
    ]
    OPCPointUtil.add_opc_point(
        task_id=task_info["id"],
        opc_type=TaskType.OPCDA,
        number_value="5",
        point_id_or_tag_name=point_c_tag,
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
    assert point_a_tag in response.text, f"点位 A {point_a_tag} 未找到: {response.text}"
    assert point_b_tag in response.text, f"点位 B {point_b_tag} 未找到: {response.text}"
    assert point_c_tag in response.text, f"点位 C {point_c_tag} 未找到: {response.text}"
    # 等待新增的点位生效
    sleep(60)
    # 验证正确生效
    TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    response = TaosAdapter.run_sql(
        env_data["taosadapter_host"],
        f"select count(*) from {OPCDA_CI_DBNAME}.`{point_a_tbname}`",
    )
    assert (
        response["data"][0][0] > 0
    ), f"点位 A 数据写入失败:  {OPCDA_CI_DBNAME}.`{point_a_tbname}` {response}"
    now_resp = TaosAdapter.run_sql(env_data["taosadapter_host"], f"select now()")
    now = int(parser.parse(now_resp["data"][0][0]).timestamp())
    sleep(5)  # 实际测试中发现 OPC DA 数据源的时间戳比北京时间可能稍微慢，所以这里采用休眠较长的时间来确保 ts_transform 之后的验证是通过的
    response = TaosAdapter.run_sql(
        env_data["taosadapter_host"],
        f"select LAST_ROW(ts), LAST_ROW(rts) from {OPCDA_CI_DBNAME}.`{point_b_tbname}`",
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
        env_data["taosadapter_host"], OPCDA_CI_DBNAME
    )
    assert (
        point_c_tbname not in table_list
    ), f"点位 C 数据 enable 0 失效: {OPCDA_CI_DBNAME}.`{point_c_tbname}` 不应存在于 {table_response}"
    # 停止及删除任务
    task.stop_task_with_retry(task_info["id"])
    task.delete_task(task_info["id"])


# 使用的配置文件包含可选列，tag 列包含多个
# 使用的参数包含所有参数
@pytest.mark.sanity
@pytest.mark.skip
def test_opcda_sanity_complicated_save(input_data):
    opcda_test_logger.info("start test_sanity_complicated_save...")
    env_data = input_data
    files_to_upload = {"csv_config_file": "opcda/opc_da_point_config_complicated.csv"}
    # 1.任务正常创建
    case_data = opcda_sanity_save(
        input_data, "opcda/test_opcda_complicated_save.yaml", files_to_upload
    )
    # 2.指定的超级表应存在
    database_name = case_data["to"]["target_dbname"]
    stables = TaosAdapter.show_stables_or_tables(database_name)
    # show_stables = TaosAdapter.run_sql(env_data["taosadapter_host"], f"show {database_name}.stables")
    # stables = []
    # for stable_name in show_stables["data"]:
    # stables.append(stable_name[0])
    assert "stb_int" in stables, opcda_test_logger.error(
        "stable stb_int should be created"
    )
    assert "stb_double" in stables, opcda_test_logger.error(
        "stable stb_double should be created"
    )
    assert "stb_varchar" in stables, opcda_test_logger.error(
        "stable stb_varchar should be created"
    )
    # 3.子表名应匹配
    tables = TaosAdapter.show_stables_or_tables(database_name, False)
    # show_tables_res = TaosAdapter.run_sql(env_data["taosadapter_host"], f"show `{database_name}`.tables")
    # tables = []
    # for table_name in show_tables_res["data"]:
    # tables.append(table_name[0])
    assert "meter_tagd0-0" in tables, opcda_test_logger.error(
        "stable meter_tagd0-0 should be created"
    )
    assert "meter_tagd0-1" in tables, opcda_test_logger.error(
        "stable meter_tagd0-1 should be created"
    )
    assert "meter_tagd0-2" in tables, opcda_test_logger.error(
        "stable meter_tagd0-2 should be created"
    )
    # 4.字段类型及字段长度应匹配
    for stable in stables:
        column_data_with_meta = []  # device_name::varchar(256)::TAG
        stable_desc = TaosAdapter.run_sql(
            env_data["taosadapter_host"], f"desc `{database_name}`.`{stable}`"
        )
        # 返回结果：{"code":0,"column_meta":[["field","VARCHAR",64],["type","VARCHAR",20],["length","INT",4],["note","VARCHAR",8]],"data":[["ts","TIMESTAMP",8,""],["val","INT",4,""],["device_name","VARCHAR",256,"TAG"]],"rows":3}
        for column_data in stable_desc["data"]:
            column_data_with_meta.append(
                f"{column_data[0]}::{column_data[1]}({column_data[2]})::{column_data[3]}"
            )

        # 4.1 TAG 应存在
        assert (
            "name::VARCHAR(40)::TAG" in column_data_with_meta
        ), opcda_test_logger.error("tag name should be exists")
        assert (
            "device_name::VARCHAR(234)::TAG" in column_data_with_meta
        ), "tag device_name should be exists"
        # 4.2 value 列名及列类型应匹配
        if "stb_int" == stable:
            assert (
                "valuevalue::INT(4)::" in column_data_with_meta
            ), "value column not match"
        elif "stb_boolean" == stable:
            assert (
                "valuevalue::BOOL(1)::" in column_data_with_meta
            ), "value column not match"
        elif "stb_varchar" == stable:
            assert (
                "valuevalue::VARCHAR(256)::" in column_data_with_meta
            ), "value column not match"
    # 5.1 日志的 debug 应开启
    grep_res = Util.winps_run(
        Env.get_agent_host(task_type),
        f"Get-Content -Path 'C:\\TDengine\\log\\opc.log' | Select-String -Pattern 'debug' | Select-Object -First 10",
    )
    assert grep_res.status_code == 0, opcda_test_logger.error(
        f"winps run error: {grep_res.std_err.decode()}"
    )
    assert grep_res != "", opcda_test_logger.error(
        "case debug log should be en2023-11-14 00:00:00abled"
    )
    # 5.2 删除 opc.log 文件（注意此时测试环境不应有多个 opc 连接器启动
    # remove_res = Util.winps_run(Env.get_agent_host(task_type),
    # "Remove-Item -Path 'C:\\Tdengine\\logs\\opc\\*' -Force")
    # assert remove_res.status_code == 0
    # 6.1 原始数据的保存应生效
    ls_result = Util.winps_run(Env.get_agent_host(task_type), "dir E:\\opcda-data")
    assert ls_result.status_code == 0, opcda_test_logger.error(
        f"winps run error: {ls_result.std_err.decode()}"
    )
    assert ls_result != "", opcda_test_logger.error("original data should be saved")
    # 6.2 删除保留的原始数据
    remove_original_data_res = Util.winps_run(
        Env.get_agent_host(task_type), "Remove-Item -Path 'E:\\opcda-data\\*' -Force"
    )
    assert remove_original_data_res.status_code == 0


import time
import multiprocessing


@pytest.mark.skip
def test_start_process():
    p = multiprocessing.Process(target=run_remote_command)
    p.start()
    time.sleep(10)
    p.terminate()


def run_remote_command():
    result = Util.winps_run(
        Env.get_agent_host(task_type),
        "Start-Process -FilePath 'C:\\TDengine\\bin\\taosx-agent.exe' -NoNewWindow",
    )


# 选择所有点位
# primary_key 设置为 received_ts
@pytest.mark.sanity
@pytest.mark.skip
def test_opcda_select_all_points(input_data):
    env_data = input_data
    opcda_test_logger.info("start test_opcda_select_all_points...")
    case_data = opcda_sanity_save(input_data, "opcda/test_opcda_select_all_points.yaml")
    # 表字段应符合
    database_name = case_data["to"]["target_dbname"]
    stables = TaosAdapter.show_stables_or_tables(database_name)
    for stable in stables:
        if stable.startswith("ctb_"):
            stable_desc = TaosAdapter.desc_table_or_stable(database_name, stable)
            assert "received_ts::TIMESTAMP(8)::" in stable_desc
            assert "original_ts::TIMESTAMP(8)::" in stable_desc


@pytest.mark.skip
def test_opcda_performance(input_data):
    """
    创建 OPC DA 任务的性能测试任务，使用的点位配置为每个包含 4000 的 CSV 配置文件
    config/opcda/performance/ 每个文件都会对应创建一个任务
    """
    env_data = input_data
    current_path = os.getcwd()
    path = Path("config/opcda/performance/")
    for file_path in path.rglob("*"):
        if file_path.is_file():
            file_path_replaced = file_path._str.replace("config/", "")
            case_data = Util.get_case_data_from_yaml(
                "opcda/test_opcda_performance_save.yaml", task_type
            )
            payload = Util.get_task_payload(case_data=case_data, env_data=env_data)
            file = File(env_data, task_type)
            file_path_for_url = file.upload(file_path_replaced)
            payload = File.add_file_param(payload, "csv_config_file", file_path_for_url)
            task = Task(env_data, case_data)
            # task.delete_all_tasks()
            # break
            # 只创建任务，不校验状态，任务状态的校验交给 sanity 以及前期的测试
            task.create_task(payload)


@pytest.mark.skip
def test_opcda_performance_scenario1(input_data):
    env_data = input_data
    file_path = "opcda/performance/opc_da_point_config_d0-0-4000.csv"
    case_data = Util.get_case_data_from_yaml(
        "opcda/test_opcda_performance_save.yaml", task_type
    )
    case_data["task_exec_time"] = 2 * 60
    case_data["to"]["target_dbname"] = "perf_opcda_s1"
    case_data["to"]["column_count"] = 3
    payload = Util.get_task_payload(case_data=case_data, env_data=env_data)

    file = File(env_data, task_type)
    file_path_for_url = file.upload(file_path)
    print(file_path_for_url)
    payload = File.add_file_param(payload, "csv_config_file", file_path_for_url)

    task = Task(env_data, case_data)
    TaosAdapter.create_db(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    task.perf_test(payload, 1, "1 task, 4000 points, production rate is 4000/s", True)
