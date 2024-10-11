import logging
import time

import allure
import pytest

from testng_taosx.constant import TaskType, CUSTOM_SQLS
from testng_taosx.file import File
from testng_taosx.task import Task
from testng_taosx.util import TaosAdapter
from testng_taosx.util import Util

csv_test_logger = logging.getLogger(__name__)
task_type = TaskType.CSV


@pytest.fixture(scope="module", autouse=True)
def module_setup():
    csv_test_logger.info("before all csv cases...")
    yield
    csv_test_logger.info("after all csv cases...")


@pytest.fixture(scope="function")
def env_data():
    csv_test_logger.info("before csv test...")
    env_data = Util.get_env_data()
    # TaosAdapter.drop_stable(env_data["taosadapter_host"], "ci_csv")

    yield env_data

    csv_test_logger.info("after csv test...")
    TaosAdapter.drop_stable(env_data["taosadapter_host"], "ci_csv")


@pytest.mark.sanity
@allure.link("https://jira.taosdata.com:18080/browse/TS-5208")
@allure.link("https://jira.taosdata.com:18080/browse/TD-32457")
def test_sanity_csv(env_data):
    """
    用例概述：测试 taosX  的文件导入功能
    用例步骤：
    1. 在 DB 中创建超级表 csv_meters
    2. 创建任务，导入 CSV 文件，写入超级表 csv_meters
    4. 验证数据写入成功

    验证点：
    1. 导入 DB 中数据的条数与 metrics 一致
    2. 使用 expr: int(parse_float(current)) 能将 current 字段转换为 int 类型
    3. 能够正确导入包含双引号的数据
    """
    csv_test_logger.info("start csv test...")
    env_data = env_data
    case_data = Util.get_case_data_from_yaml("csv/test_csv.yaml", task_type)
    parser_data = Util.get_case_data_from_yaml("csv/test_csv_parser.yaml", task_type)
    case_data["parser"] = parser_data["parser"]

    task = Task(env_data, case_data)
    file = File(env_data, TaskType.OPCUA)
    upload_file_path = file.upload("csv/d0-15.csv")

    additional_params = {}
    additional_params["csv"] = upload_file_path
    additional_params[
        "createStb"
    ] = f"""
    CREATE STABLE
    `{case_data["to"]["target_dbname"]}`.`csv_meters` (`ts` TIMESTAMP, `current` INT, `voltage` INT, `phase` DOUBLE, `desc` BINARY(64))
    TAGS (`id` INT);
    """

    metrics = task.sanity_test(additional_params=additional_params)

    rows_count = TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    assert rows_count == metrics["current"]["written_rows"]

    sqlresult = TaosAdapter.run_sql(
        env_data["taosadapter_host"],
        f"select * from `{case_data['to']['target_dbname']}`.`csv_meters`",
    )
    assert sqlresult["data"][0][1] == 10, csv_test_logger.info(
        "TD-32457: value in stable should be converted into int"
    )
    assert sqlresult["data"][0][4] == '"hello,world!"', csv_test_logger.info(
        "TS-5208: value in stable should contain double quotes"
    )


@pytest.mark.negative
@allure.link("https://jira.taosdata.com:18080/browse/TD-30828")
def test_subtables_conflict(env_data):
    """
    用例概述：导入 CSV 文件时，由于子表 d1 在其它超级表已存在，导致部分数据写入失败，活动日志中报错
    用例步骤：
    1. 在 DB 中创建超级表 csv_s1 子表 d1
    2. 创建任务，导入 CSV 文件，写入超级表 csv_meters 下的子表 d1, d2
    3. d1 由于子表名在不同的超级表中冲突，导致部分数据写入失败

    验证点：
    1. 在任务的活动日志中，应能够查看到错误消息：Table already exists in other stables
    """
    env_data = env_data
    case_data = Util.get_case_data_from_yaml("csv/test_csv.yaml", task_type)
    parser_data = Util.get_case_data_from_yaml("csv/test_csv_parser.yaml", task_type)
    case_data["parser"] = parser_data["parser"]

    task = Task(env_data, case_data)
    file = File(env_data, TaskType.OPCUA)
    upload_file_path = file.upload("csv/d0-15.csv")

    additional_params = {
        "csv": upload_file_path,
        "createStb": f"""
            CREATE STABLE
            `{case_data["to"]["target_dbname"]}`.`csv_meters`
            (`ts` TIMESTAMP, `current` DOUBLE, `voltage` INT, `phase` DOUBLE, `desc` BINARY(64))
            TAGS (`id` INT);
        """,
        CUSTOM_SQLS: [
            f"CREATE STABLE `{case_data['to']['target_dbname']}`.`csv_s1` "
            f"(`ts` TIMESTAMP, `current` DOUBLE, `voltage` INT, `phase` DOUBLE, `desc` BINARY(64)) "
            f"TAGS (`id` INT);",
            f"CREATE TABLE `{case_data['to']['target_dbname']}`.`d1` "
            f"USING `{case_data['to']['target_dbname']}`.`csv_s1` TAGS(1);",
        ],
    }

    task_info = task.sanity_test_create_task(additional_params=additional_params)
    task_id = task_info["id"]

    time.sleep(3)

    task_status = task.get_task_status(task_id).get("status", "").strip().lower()
    assert task_status == "completed", csv_test_logger.error(
        "Task status should be completed"
    )

    task_activities = task.get_activities(task_id)
    assert (
        "Internal error: `Table already exists in other stables`"
        in task_activities.text
    ), csv_test_logger.error(
        "Table already exists error message should be in task activities"
    )


@pytest.mark.performance
def test_case_performance_scenario1(env_data):
    csv_test_logger.info("start csv performance test...")
    env_data = env_data
    case_data = Util.get_case_data_from_yaml("csv/test_csv_performance.yaml", task_type)
    parser_data = Util.get_case_data_from_yaml(
        "csv/test_csv_parser_performance.yaml", task_type
    )
    case_data["parser"] = parser_data["parser"]
    case_data["from"]["fromhost"] = "csv:/mnt/share/csv_perf/scenario1"
    case_data["to"]["target_dbname"] = "perf_csv_s1"
    case_data["to"]["column_count"] = 5
    case_data["task_exec_time"] = 10 * 60
    case_data["from"]["batch_size"] = 1000

    sqlstr = f"CREATE STABLE {case_data['to']['target_dbname']}.csv_meters (ts TIMESTAMP,id INT, \
        current DOUBLE,phase DOUBLE,voltage DOUBLE) TAGS (groupid INT, location VARCHAR(64));"
    TaosAdapter.create_db(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"], "ns"
    )
    r = TaosAdapter.run_sql(env_data["taosadapter_host"], sqlstr)
    assert r["code"] == 0, f"fail to create database: {r['desc']}"

    task = Task(env_data, case_data)
    payload = Util.get_task_payload(case_data, env_data)
    task.perf_test(
        payload, 1, "1 task ,1 stable,10w subtables, 5 columns(int, double)", True
    )
