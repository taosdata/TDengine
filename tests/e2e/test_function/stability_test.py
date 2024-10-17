import json
import logging
import time

import pytest

from testng_taosx.env import ENV
from testng_taosx.file import File, TaskType
from testng_taosx.task import Task
from testng_taosx.util import TaosAdapter
from testng_taosx.util import Util

stability_test_logger = logging.getLogger(__name__)

taosBenchmark_json_dir = Util.get_absolute_path("stability")
taosBenchmark_json_tar = "/tmp/"


@pytest.fixture(scope="function")
def input_data():
    stability_test_logger.info("before stability test...")
    env_data = Util.get_env_data()

    yield env_data
    stability_test_logger.info("after stability test...")


def test_stability(input_data):
    env_data = input_data
    # 构建legacy任务配置并创建任务
    case_data = Util.read_yaml("legacy/test_legacy_tdengine.yaml")
    case_data["from"]["fromhost"] = (
        case_data["from"]["fromhost"].rpartition("/")[0] + "/stability_1"
    )
    case_data["from"]["interval"] = "10s"
    case_data["from"]["mode"] = "history"
    case_data["from"]["unit"] = "5s"
    case_data["to"]["target_dbname"] = "legacy_stable"
    payload = Util.get_task_payload(case_data, env_data)
    task = Task(env_data, case_data)
    legacy_task_info = task.create_task(payload)
    legacy_task_id = legacy_task_info["id"]

    # 添加 PI 任务配置并创建任务
    task_type = TaskType.PI
    case_data = Util.get_case_data_from_yaml("pi/test_PiFile.yaml", task_type)
    case_data["to"]["target_dbname"] = "pi_stable"

    param = "template_for_pi_point_file"
    file = File(env_data, task_type)
    files_dir = "pi/Stability.csv"
    file_dir = file.upload(files_dir)
    payload_str = Util.get_task_payload(case_data, env_data)
    payload = File.add_file_param(payload_str, param, file_dir)
    task = Task(env_data, case_data)
    pi_task_info = task.create_task(payload)
    pi_task_id = pi_task_info["id"]
    # 添加TMQ 任务配置并创建任务
    task_type = TaskType.TMQ
    case_data = Util.get_case_data_from_yaml("tmq/test_tmq_sanity.yaml", task_type)
    case_data["to"]["target_dbname"] = "tmq_stable"

    # 每添加一个数据源任务，需要在task_list列表中增加新的task_id
    task_list = [legacy_task_id]
    # task_list = [legacy_task_id,pi_task_id]
    error_massage_dict = {}
    while True:
        for task_id in task_list:
            task_activities_response = task.get_activities(task_id)
            task_activities = json.loads(task_activities_response.text)
            for activities in task_activities:
                if activities["level"] not in ["ERROR", "error", "warn", "WARN"]:
                    continue
                if f'{task_id},{activities["at"]}' in error_massage_dict.keys():
                    break
                else:
                    message = f""" task {task_id} is {activities["level"]}, {activities["level"]} massage is {activities["activity"]} ,
                                    happened at {activities["at"]}"""
                    error_massage_dict[f'{task_id},{activities["at"]}'] = message
                    Util.send_message_to_feishu(message)
                # 为避免频繁上报，暂时只保留将 ERROR 信息上报
                # if activities["status"] is not "running":
                #     message = activities["status"]
                #     Util.send_message_to_feishu(token,headers,message)
        time.sleep(10)


@pytest.mark.parametrize(
    "sub_table_num, records_per_sub_table, deleted_sub_table_num",
    [(100_000, 10_000, 1000)],
)
def test_stability_replication(
    input_data, sub_table_num, records_per_sub_table, deleted_sub_table_num
):
    """
    stability test for TDengine 3
    :param input_data:
    :param sub_table_num: sub table number during init
    :param records_per_sub_table: records in each sub table during init
    :param deleted_sub_table_num: deleted sub table number each time
    """

    first_time = int(time.time())
    data = Util.read_jsonfile(f"{taosBenchmark_json_dir}/basic.json")
    data["host"] = ENV.taosadapter_host
    data["databases"][0]["super_tables"][0]["childtable_prefix"] = f"t_{first_time}"
    data["databases"][0]["super_tables"][0]["childtable_count"] = sub_table_num
    data["databases"][0]["super_tables"][0]["insert_rows"] = records_per_sub_table
    data["databases"][0]["super_tables"][0]["start_timestamp"] = first_time * 1000
    Util.write_jsonfile(f"{taosBenchmark_json_dir}/insert.json", data)

    Util.scp(f"{taosBenchmark_json_dir}/insert.json", ENV.taosBenchmark_host)
    Util.ssh_run(
        ENV.taosBenchmark_host, f"taosBenchmark -f {taosBenchmark_json_tar}/insert.json"
    )
    TaosAdapter.run_sql(ENV.taosadapter_host, "flush database stability_test_db1")

    # 每个循环进行一次数据写入，删除最早创建的1000个子表，监控任务状态
    while True:
        # 获取库中创建时间最早的1000个子表
        table_list = []
        table_list_result = TaosAdapter.run_sql(
            ENV.taosadapter_host,
            f"""
                            select table_name from information_schema.ins_tables
                            where db_name = 'stability_test_db1' order by create_time limit 1000""",
        )["data"]
        for table in table_list_result:
            table_list.append(table[0])

        # 本轮建表，写入数据
        timestamp = int(time.time())
        data = Util.read_jsonfile(f"{taosBenchmark_json_dir}/basic.json")
        data["host"] = ENV.taosadapter_host
        data["databases"][0]["super_tables"][0]["childtable_prefix"] = f"t_{timestamp}"
        data["databases"][0]["super_tables"][0][
            "childtable_count"
        ] = deleted_sub_table_num
        data["databases"][0]["super_tables"][0]["insert_rows"] = records_per_sub_table
        data["databases"][0]["super_tables"][0]["start_timestamp"] = timestamp * 1000
        Util.write_jsonfile(f"{taosBenchmark_json_dir}/insert.json", data)

        Util.scp(f"{taosBenchmark_json_dir}/insert.json", ENV.taosBenchmark_host)
        Util.ssh_run(
            ENV.taosBenchmark_host,
            f"taosBenchmark -f {taosBenchmark_json_tar}/insert.json",
        )
        TaosAdapter.run_sql(ENV.taosadapter_host, "flush database stability_test_db1")

        # 遍历删除子表
        for table_name in table_list:
            TaosAdapter.drop_tables("stability_test_db1", table_name)


def test_monitor(input_data):
    env_data = input_data
    tasks = Task(env_data, None)
    # 获取task id列表
    task_list = tasks.get_all_task_ids()
    error_massage_dict = {}
    while True:
        for task_id in task_list:
            task_activities_response = tasks.get_activities(task_id)
            task_activities = json.loads(task_activities_response.text)
            for activities in task_activities:
                if activities["level"] not in ["ERROR", "error", "warn", "WARN"]:
                    continue
                if f'{task_id},{activities["at"]}' in error_massage_dict.keys():
                    # alert has already been sent
                    break
                else:
                    message = f""" task {task_id} is {activities["level"]}, {activities["level"]} massage is {activities["activity"]} ,
                                happened at {activities["at"]}"""
                    error_massage_dict[f'{task_id},{activities["at"]}'] = message
                    Util.send_message_to_feishu(message)
        time.sleep(10)
