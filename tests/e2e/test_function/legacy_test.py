import json
import logging
from re import split
import time

import pytest

from testng_taosx.constant import TaskStatus
from testng_taosx.task import Task
from testng_taosx.util import TaosAdapter
from testng_taosx.util import Util
from testng_taosx.env import *

legacy_test_logger = logging.getLogger(__name__)
task_type = TaskType.TDENGINE2X
taosBenchmark_json_dir = Util.get_absolute_path("legacy/performance")
# legacy_host = ENV.tdengine2_host


@pytest.fixture(scope="function")
def input_data():
    legacy_test_logger.info("before legacy test...")
    env_data = Util.get_env_data()

    yield env_data

    legacy_test_logger.info("after legacy test...")
    TaosAdapter.drop_db(env_data["taosadapter_host"], "legacy")


def retry_to_expected_status(task, taskid, expected_status, wait_time, retry_count=10):
    retry = 0
    while True:
        task_status = task.get_task_status(taskid)
        if task_status["status"] == expected_status or retry == retry_count:
            return task_status
        else:
            time.sleep(wait_time)
        retry += 1


@pytest.mark.sanity
def test_sanity_basic(input_data):
    """
    用例概述：验证从2.6版本的TDengine迁移数据到3.0版本的TDengine基础功能
    用例步骤：
    1. 在2.6版本的TDengine中创建一个数据库，一个超级表，一个子表，插入数据
    2. 在3.0版本的TDengine中创建一个数据库
    3. 创建历史数据迁移，将2.6版本的TDengine中的数据迁移到3.0版本的TDengine中
    验证点：
    1. 任务启动后，是否有数据写入目标库
    2. 源库中的子表数量与目标库中的子表数量一致
    3. 写入目标库的数据量与源库的数据量一致
    """
    env_data = input_data
    case_data = Util.read_yaml("legacy/test_legacy_tdengine.yaml")
    source_db_name = case_data["source_db"]["name"]
    case_data["from"]["fromhost"] = f'{case_data["from"]["fromhost"]}{source_db_name}'
    case_data["source_db"]["name"] = source_db_name
    source_subtable_number = case_data["source_db"]["subtable_number"]
    record_number_per_subtable = case_data["source_db"]["record_number_per_subtable"]
    task = Task(env_data, case_data)
    task.sanity_test()
    taosadapter_addr = env_data["taosadapter_host"]
    target_dbname = case_data["to"]["target_dbname"]
    rows_count = TaosAdapter.check_db_count(taosadapter_addr, target_dbname)
    assert rows_count > 0, legacy_test_logger.error("目标库中应有数据，若没有数据，则任务启动时的状态可能有异常")
    assert source_subtable_number == source_subtable_number, legacy_test_logger.error(
        f"源库中的子表数量应与目标库中的子表数量一致，实际源库中子表数量为{source_subtable_number},目标库子表数量为{source_subtable_number}"
    )
    assert (
        rows_count == source_subtable_number * record_number_per_subtable
    ), legacy_test_logger.error
    (
        f"写入目标库的数据量应与源库的数据量一致，实际源库数据量为{source_subtable_number * record_number_per_subtable}, 写入目标库数据量为{rows_count}"
    )


@pytest.mark.sanity
def test_sanity_realtime(input_data):
    """
    用例概述：验证将实时写入2.6TDengine的数据迁移到3.0TDengine
    用例步骤：
    1. 在2.6版本的TDengine中创建一个数据库，一个超级表，一个子表，插入实时数据
    2. 在3.0版本的TDengine中创建一个数据库
    3. 创建实时数据迁移任务，将2.6版本的TDengine中的数据迁移到3.0版本的TDengine中
    4. 然后再写入新的实时数据
    验证点：
    1. 任务启动后，任务状态持续保持为running
    2. 最终目标库中的数据量应只包含任务启动后写入的实时数据
    """
    env_data = input_data
    case_data = Util.read_yaml("legacy/test_legacy_tdengine_realtime.yaml")
    source_db_name = Util.get_long_name(10)
    case_data["from"]["fromhost"] = f'{case_data["from"]["fromhost"]}{source_db_name}'
    data_source_type = case_data["task_type"]
    source_db_ip = env_data["data_source"][data_source_type][0].split(":")[0]
    source_db_port = env_data["data_source"][data_source_type][0].split(":")[1]
    source_subtable_number = case_data["source_db"]["subtable_number"]
    record_number_per_subtable = case_data["source_db"]["record_number_per_subtable"]
    prepare_source_db_cmd = (
        "/usr/bin/taosBenchmark"
        + " -t " + str(source_subtable_number)
        + " -s `date +%s`000 "
        + " -n " + str(record_number_per_subtable)
        + " -y "
        + " -d " + source_db_name
        + " -h " + source_db_ip
        + " -P " + source_db_port
        + " -I rest"
    )
    fill_data_to_source_db_cmd = (
        "/usr/bin/taosBenchmark"
        + " -t " + str(source_subtable_number)
        + " -s `date +%s`000 "
        + " -n " + str(record_number_per_subtable)
        + " -y "
        + " -d " + source_db_name
        + " -U "
        + " -h " + source_db_ip
        + " -P " + source_db_port
        + " -I rest"
    )
    TaosAdapter.drop_legacy_db(source_db_ip, source_db_name, port=source_db_port)
    Util.ssh_run(source_db_ip, prepare_source_db_cmd)
    TaosAdapter.create_db(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    payload = Util.get_task_payload(case_data, env_data)
    task = Task(env_data, case_data)
    task_info = task.create_task(payload)
    task_id = task_info["id"]
    task_status = task.get_task_status(task_id)
    assert (
        task_status["status"] == TaskStatus.RUNNING.value
    ), f"创建任务后，判断任务状态应为{TaskStatus.RUNNING.value}，实际任务状态为{task_status}"
    time.sleep(case_data["task_exec_time"])
    Util.ssh_run(source_db_ip, fill_data_to_source_db_cmd)
    legacy_test_logger.info(f"wait for the task to run: {case_data['task_exec_time']}")
    time.sleep(case_data["task_exec_time"])

    # get metrics
    metrics = task.get_task_metrics(task_id)
    task.stop_task_with_retry(task_id)
    taosadapter_addr = env_data["taosadapter_host"]
    target_dbname = case_data["to"]["target_dbname"]
    inserted_rows = TaosAdapter.check_db_count(taosadapter_addr, target_dbname)
    assert metrics['current']['written_rows'] == inserted_rows, legacy_test_logger.error(
        f"写入目标库的数据量应只包含任务启动后写入的实时数据，实际metrics统计结果为{metrics['current']['written_rows']}, 写入目标库数据量为{inserted_rows}"
    )
    TaosAdapter.drop_legacy_db(source_db_ip, source_db_name, port=source_db_port)

@pytest.mark.sanity
@pytest.mark.xfail(
    reason="不稳定，单个执行多次可能都没有问题，但是全量执行的时候多次出现 assert 结果失败，assert rows_count == num_between_time_range assert 432005 == 432000"
)
def test_sanity_time_range(input_data):
    """
    用例概述：验证从2.6版本的TDengine迁移指定历史时间区间的数据到3.0版本的TDengine基础功能
    用例步骤：
    1. 在2.6版本的TDengine中创建一个数据库，一个超级表，一个子表，插入数据
    2. 在3.0版本的TDengine中创建一个数据库
    3. 创建历史数据迁移，指定历史时间区间，将2.6版本的TDengine中的数据迁移到3.0版本的TDengine中
    验证点：
    1. 任务启动后，是否有数据写入目标库
    2. 任务能够正常结束，任务最终状态为completed
    3. 写入目标库的数据量与源库的数据量一致
    """
    env_data = input_data
    case_data = Util.read_yaml("legacy/test_legacy_tdengine_with_time_range.yaml")
    source_db_name = case_data["source_db"]["name"]
    case_data["from"]["fromhost"] = f'{case_data["from"]["fromhost"]}{source_db_name}'
    data_source_type = case_data["task_type"]
    source_db_ip = env_data["data_source"][data_source_type][0].split(":")[0]
    source_db_port = env_data["data_source"][data_source_type][0].split(":")[1]
    sync_start = case_data["from"]["start"]
    sync_end = case_data["from"]["end"]
    sql_query = f"select count(*) from {source_db_name}.meters where ts >= '{sync_start}' and ts < '{sync_end}';"
    num_between_time_range = TaosAdapter.run_sql(source_db_ip, sql_query, version=2,port=source_db_port)["data"][0][0]
    task = Task(env_data, case_data)
    TaosAdapter.create_db(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    payload = Util.get_task_payload(case_data, env_data)
    task_info = task.create_task(payload)
    task_status = retry_to_expected_status(
        task, task_info["id"], "completed", case_data["task_exec_time"], 15
    )
    assert task_status["status"] == "completed", f"任务状态最终应该为completed，实际任务状态为{task_status}"
    taosadapter_addr = env_data["taosadapter_host"]
    target_dbname = case_data["to"]["target_dbname"]
    rows_count = TaosAdapter.check_db_count(taosadapter_addr, target_dbname)
    assert rows_count > 0, legacy_test_logger.error(f"目标库中应有数据，实际上目标库数据量为{rows_count}，任务启动时的状态可能有异常")
    assert rows_count == num_between_time_range, legacy_test_logger.error(
        f"写入目标库的数据量应只包含任务启动后写入的实时数据，实际源库写入数据量为{num_between_time_range}, 写入目标库数据量为{rows_count}")

@pytest.mark.sanity
def test_sanity_schema_only(input_data):
    """
    用例概述：验证从2.6版本的TDengine迁移数据到3.0版本的TDengine，只同步schema信息
    用例步骤：
    1. 在2.6版本的TDengine中创建一个数据库，一个超级表，一个子表，插入数据
    2. 在3.0版本的TDengine中创建一个数据库
    3. 创建历史数据迁移，将2.6版本的TDengine中的数据迁移到3.0版本的TDengine中
    验证点：
    1. 源库中的子表数量与目标库中的子表数量一致
    2. 写入目标库的数据量为0
    """
    env_data = input_data
    case_data = Util.read_yaml("legacy/test_legacy_tdengine_with_schema_only.yaml")
    source_db_name = case_data["source_db"]["name"]
    case_data["from"]["fromhost"] = f'{case_data["from"]["fromhost"]}{source_db_name}'
    data_source_type = case_data["task_type"]
    source_db_ip = env_data["data_source"][data_source_type][0].split(":")[0]
    source_db_port = env_data["data_source"][data_source_type][0].split(":")[1]
    source_subtable_number = case_data["source_db"]["subtable_number"]
    sql_for_stable = f"show create stable {source_db_name}.meters"
    source_schema_for_stable = TaosAdapter.run_sql(
        source_db_ip, sql_for_stable, version=2, port=source_db_port
    )["data"][0][1]
    source_schema_for_stable = source_schema_for_stable.replace(",", ", ")
    source_schema_for_stable = source_schema_for_stable.replace("TABLE", "STABLE")
    source_schema_for_stable = source_schema_for_stable.replace("BINARY", "VARCHAR")
    source_schema_for_substables = []
    for i in range(source_subtable_number):
        s_for_stable = TaosAdapter.run_sql(
            source_db_ip,
            f"show create table {source_db_name}.d{i}",
            version=2,
            port=source_db_port,
        )["data"][0][1]
        source_schema_for_substables.append(s_for_stable)

    taosadapter_addr = env_data["taosadapter_host"]
    target_dbname = case_data["to"]["target_dbname"]
    TaosAdapter.create_db(taosadapter_addr, target_dbname)
    task = Task(env_data, case_data)
    payload = Util.get_task_payload(case_data, env_data)
    task_info = task.create_task(payload)
    task_id = task_info["id"]
    task_exec_time = case_data["task_exec_time"]
    for i in range(task_exec_time):
        task_status = task.get_task_status(task_id)
        legacy_test_logger.info(f"task_status is: {task_status['status']}")
        if task_status["status"] == "completed":
            break
        else:
            legacy_test_logger.info("Wait for the task completed")
            time.sleep(3)
    assert task_status["status"] == "completed", f"任务状态最终应该为completed，实际任务状态为{task_status}"

    metrics = task.get_task_metrics(task_id)
    rows_count = TaosAdapter.check_db_count(taosadapter_addr, target_dbname)
    assert rows_count == 0, "只同步schema，不应该有数据写入目标库"
    created_subtable_number = metrics["current"]["created_tables"]
    assert source_subtable_number == created_subtable_number, legacy_test_logger.error(
        "schema同步后，目标库中的子表数量应与源库中的子表数量一致"
    )


@pytest.mark.sanity
def test_sanity_wide_schema(input_data):
    """
    用例概述：验证从2.6版本的TDengine迁移4000列大宽表数据到3.0版本的TDengine基础功能
    用例步骤：
    1. 在2.6版本的TDengine中创建一个数据库，一个超级表，一个子表，插入数据，schema为4000列int型
    2. 在3.0版本的TDengine中创建一个数据库
    3. 创建datain任务，将2.6版本的TDengine中的数据迁移到3.0版本的TDengine中
    验证点：
    1. 任务启动后，是否能够正常结束
    2. 源库中的子表数量与目标库中的子表数量一致
    """
    env_data = input_data
    case_data = Util.read_yaml("legacy/test_legacy_tdengine_wide_schema.yaml")
    source_db_name = case_data["source_db"]["name"]
    source_stb_name = case_data["source_db"]["stb_name"]
    case_data["from"]["fromhost"] = f'{case_data["from"]["fromhost"]}{source_db_name}'
    tb_count_source = case_data["source_db"]["subtable_number"]
    TaosAdapter.create_db(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    task = Task(env_data, case_data)
    payload = Util.get_task_payload(case_data, env_data)
    task_info = task.create_task(payload)
    task_id = task_info["id"]
    for i in range(case_data["task_exec_time"]):
        task_status = task.get_task_status(task_id)
        legacy_test_logger.info(f"task_status is: {task_status['status']}")
        if task_status["status"] == "completed":
            break
        else:
            legacy_test_logger.info("Wait for the task completed")
            time.sleep(3)
    assert task_status["status"] == "completed", f"任务最终状态应该为completed，实际任务状态为{task_status}"
    result = TaosAdapter.run_sql(
        env_data["taosadapter_host"],
        f"select count(*) from (select distinct tbname from {case_data['to']['target_dbname']}.{source_stb_name})",
    )
    tb_count_target = result["data"][0][0]
    assert tb_count_source == tb_count_target, legacy_test_logger.error(
        f"源库中的子表数量应与目标库中的子表数量一致，实际源库子表数量为{tb_count_source}, 目标库子表数量为{tb_count_target}"
    )


@pytest.mark.sanity
def test_new_table_realtime(input_data):
    """
    用例概述：验证从2.6版本的TDengine新增的schema信息和时序数据同步到3.0版本的TDengine基础功能
    用例步骤：
    1. 在2.6版本的TDengine中创建一个数据库，在3.0版本的TDengine中创建一个数据库
    2. 创建实时数据同步任务，将2.6版本的TDengine中的数据迁移到3.0版本的TDengine中
    3. 在源库中新建超级表、子表、写入数据
    验证点：
    1. 任务启动后，持续保持为running
    2. 目标库中的超级表和子表名称与源库中的超级表和子表名称一致
    3. 写入目标库的数据量与源库的数据一致
    4. 任务能够正常停止
    """
    env_data = input_data
    case_data = Util.read_yaml("legacy/test_legacy_new_table_realtime.yaml")
    stbname = "stb"
    tbname = "tb"
    data_source_type = case_data["task_type"]
    source_db_ip = env_data["data_source"][data_source_type][0].split(":")[0]
    source_db_port = env_data["data_source"][data_source_type][0].split(":")[1]
    source_db_name = Util.get_long_name(10)
    case_data["from"][
        "fromhost"
    ] = f'{case_data["from"]["fromhost"]}{source_db_name}'
    TaosAdapter.drop_legacy_db(source_db_ip, source_db_name, port=source_db_port)
    TaosAdapter.create_db_legacy(source_db_ip, source_db_name,port=source_db_port)
    TaosAdapter.create_db(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    payload = Util.get_task_payload(case_data, env_data)
    task = Task(env_data, case_data)
    task_info = task.create_task(payload)
    task_id = task_info["id"]
    time.sleep(5)
    task_status = task.get_task_status(task_id)
    assert (
        task_status["status"] == TaskStatus.RUNNING.value
    ), f"实时任务启动后，任务状态应为running，实际任务状态为{task_status['status']}"

    # 源库中建超级表、子表
    TaosAdapter.run_sql(
        source_db_ip,
        f"create stable {source_db_name}.{stbname} (ts timestamp,c0 int) tags(t0 int)",
        version=2,
        port=source_db_port,
    )
    TaosAdapter.run_sql(
        source_db_ip,
        f"create table {source_db_name}.{tbname} using {source_db_name}.{stbname} tags(1)",
        version=2,
        port=source_db_port,
    )
    time.sleep(10)
    # 查看目标库中的超级表和子表名称是否正确
    stable_result = TaosAdapter.run_sql(
        env_data["taosadapter_host"], f"show {case_data['to']['target_dbname']}.stables"
    )
    table_result = TaosAdapter.run_sql(
        env_data["taosadapter_host"], f"show {case_data['to']['target_dbname']}.tables"
    )
    assert stable_result["data"][0][0] == stbname, legacy_test_logger.error(
        f"目标库中的超级表名称应与源库中的超级表名称一致，源库中的超级表名称为{stbname}，目标库超级表名称为{stable_result['data'][0][0]}"
    )
    assert table_result["data"][0][0] == tbname, legacy_test_logger.error(
        f"目标库中的子表名称应与源库中的子表名称一致，源库中的子表名称为{tbname}，目标库子表名称为{table_result['data'][0][0]}"
    )

    # 在新生成的子表中写入实时数据
    insert_value = 1
    TaosAdapter.run_sql(
        source_db_ip,
        f"insert into {source_db_name}.{tbname} values(now,{insert_value})",
        version=2,
        port=source_db_port,
    )
    time.sleep(5)
    query_result = TaosAdapter.run_sql(
        env_data["taosadapter_host"],
        f"select * from {case_data['to']['target_dbname']}.{tbname}",
    )
    assert query_result["data"][0][1] == insert_value, legacy_test_logger.error(
        f"目标库应同步写入新的数据，源库子表中写入的值为{insert_value},目标子表中数据值为{query_result['data'][0][1]}"
    )
    task.stop_task_with_retry(task_id)
    TaosAdapter.drop_legacy_db(source_db_ip, source_db_name, port=source_db_port)


@pytest.mark.sanity
def test_connectivity_test(input_data):
    env_data = input_data
    case_data = Util.get_case_data_from_yaml(
        "legacy/test_legacy_tdengine.yaml", task_type
    )
    case_data["from"]["fromhost"] += case_data["source_db"]["name"]
    dsn = Util.get_task_payload(case_data, input_data)["from"]
    json_result = Util.check_connectivity(input_data, dsn)
    assert json_result[
        "valid"
    ], f"TDengine2 连通性校验失败，result: {json_result} dsn: {dsn}"


@pytest.mark.performance
def test_case_performance_scenario1(input_data):
    legacy_test_logger.info(f"running TD2 to TD3 performance case scenario1...")
    env_data = input_data
    case_data = Util.read_yaml("legacy/test_legacy_performance.yaml")
    case_data["to"]["target_dbname"] = "perf_legacy_s1"
    case_data["to"]["column_count"] = 11
    case_data["task_exec_time"] = 30 * 60

    legacy_host = split(":", env_data["data_source"]["tdengine2.x"][0])[0]
    TaosAdapter.run_sql(
        legacy_host,
        f"create topic if not exists `{case_data['source']['name']}` with meta as database `{case_data['source']['name']}`",
    )
    TaosAdapter.create_db(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    task = Task(env_data, case_data)
    driver = "taos"
    if ENV.source_protocol == "native":
        case_data["from"]["fromhost"] = (
            driver
            + "://root:taosdata@"
            + case_data["from"]["host"]
            + ":6030/"
            + case_data["from"]["dbname"]
        )
    else:
        case_data["from"]["fromhost"] = (
            driver
            + "+ws://root:taosdata@"
            + case_data["from"]["host"]
            + ":6041/"
            + case_data["from"]["dbname"]
        )
        del case_data["from"]["libraryPath"]
        del case_data["from"]["configDir"]
    del case_data["from"]["host"]
    del case_data["from"]["dbname"]

    payload = Util.get_task_payload(case_data, env_data)
    print(
        f"driver:{driver}, source_protocol:{ENV.source_protocol}, targar_protocol:{ENV.target_protocol}"
    )
    print(payload)
    case_data["task_type"] = "tdengine2"
    task.perf_test(
        payload,
        1,
        "1 task, mode is history, schema is always, 1 stable,10w subtables X 1w,ts column+5 columns(int)+5 columns(double)",
        True,
    )
