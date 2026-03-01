import logging
import os
import time

import allure
import pytest

from testng_taosx.constant import TaskType, CUSTOM_SQLS
from testng_taosx.file import File
from testng_taosx.task import Task
from testng_taosx.util import TaosAdapter
from testng_taosx.util import Util
from packaging import version

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
    env_data = env_data
    # Skip test if TDengine version >= 3.4
    if version.parse(env_data["db_version"][:5]) >= version.parse("3.4"):
        return

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
    assert rows_count == metrics["current"]["written_rows"], csv_test_logger.info(
        "TD-32457: data should be imported successfully"
    )

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
    env_data = env_data
    # Skip test if TDengine version >= 3.4
    if version.parse(env_data["db_version"][:5]) >= version.parse("3.4"):
        return

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
        "TD-30828: Task status should be completed"
    )

    task_activities = task.get_activities(task_id)
    assert (
        "Internal error: `Table already exists in other stables`"
        in task_activities.text
    ), csv_test_logger.error(
        "TD-30828: Table already exists error message should be in task activities"
    )


@pytest.mark.performance
def test_case_performance_scenario1(env_data):
    env_data = env_data
    # Skip test if TDengine version >= 3.4
    if version.parse(env_data["db_version"][:5]) >= version.parse("3.4"):
        return

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


@pytest.mark.sanity
@allure.link("https://jira.taosdata.com:18080/browse/TD-32573")
def test_sanity_csv_td32573_01(env_data):
    env_data = env_data
    # Skip test if TDengine version >= 3.4
    if version.parse(env_data["db_version"][:5]) >= version.parse("3.4"):
        return

    """
    用例概述：验证上传单个文件且“保留已完成的文件”可以正确工作

    用例步骤：
    1. 在 DB 中创建超级表 csv_meters
    2. 创建任务，导入单个 CSV 文件，配置 keep_processed_files=true
    3. 等待任务完成

    验证点：

    1. 任务可以成功创建
    2. 数据库中可以成功入库 CSV 文件的内容
    3. 文件仍然保留在 taosX data_dir 中
    """
    csv_test_logger.info("start test_sanity_csv_td32573_01...")
    env_data = env_data
    case_data = Util.get_case_data_from_yaml("csv/test_csv.yaml", task_type)
    parser_data = Util.get_case_data_from_yaml("csv/test_csv_parser.yaml", task_type)
    case_data["parser"] = parser_data["parser"]

    # set keep_processed_files to true
    case_data["from"]["keep_processed_files"] = "true"

    task = Task(env_data, case_data)
    file = File(env_data, TaskType.CSV)
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

    # 创建任务
    task_info = task.sanity_test_create_task(additional_params=additional_params)

    # 等待 10s 或任务结束
    for _ in range(2):
        task_status = task.get_task_status(task_info["id"])
        if task_status["status"] == "completed":
            break
        else:
            time.sleep(5)

    # 获取运行指标
    metrics = task.get_task_metrics(task_info["id"])

    # check db count
    rows_count = TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    assert rows_count == metrics["current"]["written_rows"], csv_test_logger.info(
        "TD-32573: data should be imported successfully"
    )
    print(rows_count)

    # check file is exists
    file_absoulte_path = os.path.join(
        os.path.dirname("/var/lib/taos/taosx/"), upload_file_path
    )
    assert os.path.exists(file_absoulte_path), csv_test_logger.info(
        "TD-32573: file should be exists"
    )
    print(file_absoulte_path)


@pytest.mark.sanity
@allure.link("https://jira.taosdata.com:18080/browse/TD-32573")
def test_sanity_csv_td32573_02(env_data):
    env_data = env_data
    # Skip test if TDengine version >= 3.4
    if version.parse(env_data["db_version"][:5]) >= version.parse("3.4"):
        return

    """
    用例概述：验证上传多个文件且“不保留已完成的文件”可以正确工作

    用例步骤：

    1. 在 DB 中创建超级表 csv_meters
    2. 创建任务，导入多个 CSV 文件，配置 keep_processed_files=false
    3. 等待任务完成

    验证点：

    1. 任务可以成功创建
    2. 数据库中可以成功入库 CSV 文件的内容
    3. 文件已从 taosX data_dir 中删除
    """
    csv_test_logger.info("start test_sanity_csv_td32573_02...")
    env_data = env_data
    case_data = Util.get_case_data_from_yaml("csv/test_csv.yaml", task_type)
    parser_data = Util.get_case_data_from_yaml("csv/test_csv_parser.yaml", task_type)
    case_data["parser"] = parser_data["parser"]

    # set keep_processed_files to false
    case_data["from"]["keep_processed_files"] = "false"

    task = Task(env_data, case_data)
    file = File(env_data, TaskType.CSV)
    upload_file_path1 = file.upload("csv/d0-15.csv")
    upload_file_path2 = file.upload("csv/d0-16.csv")
    upload_file_path = upload_file_path1 + "," + upload_file_path2

    additional_params = {}
    additional_params["csv"] = upload_file_path
    additional_params[
        "createStb"
    ] = f"""
    CREATE STABLE
    `{case_data["to"]["target_dbname"]}`.`csv_meters` (`ts` TIMESTAMP, `current` INT, `voltage` INT, `phase` DOUBLE, `desc` BINARY(64))
    TAGS (`id` INT);
    """

    # 创建任务
    task_info = task.sanity_test_create_task(additional_params=additional_params)

    # 等待 10s 或任务结束
    for _ in range(2):
        task_status = task.get_task_status(task_info["id"])
        if task_status["status"] == "completed":
            break
        else:
            time.sleep(5)

    # 获取运行指标
    metrics = task.get_task_metrics(task_info["id"])

    # check db count
    rows_count = TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    assert rows_count == metrics["current"]["written_rows"], csv_test_logger.info(
        "TD-32573: data should be imported successfully"
    )
    print(rows_count)

    # check file1 is exists
    file_absoulte_path1 = os.path.join(
        os.path.dirname("/var/lib/taos/taosx/"), upload_file_path1
    )
    assert not os.path.exists(file_absoulte_path1), csv_test_logger.info(
        "TD-32573: file should be removed"
    )
    print(file_absoulte_path1)

    # check file2 is exists
    file_absoulte_path2 = os.path.join(
        os.path.dirname("/var/lib/taos/taosx/"), upload_file_path2
    )
    assert not os.path.exists(file_absoulte_path2), csv_test_logger.info(
        "TD-32573: file should be removed"
    )
    print(file_absoulte_path2)


@pytest.mark.sanity
@allure.link("https://jira.taosdata.com:18080/browse/TD-32573")
def test_sanity_csv_td32573_03(env_data):
    env_data = env_data
    # Skip test if TDengine version >= 3.4
    if version.parse(env_data["db_version"][:5]) >= version.parse("3.4"):
        return

    """
    用例概述：验证上传多个文件且“保留已完成的文件”可以正确工作
    用例步骤：
    1. 在 DB 中创建超级表 csv_meters
    2. 创建任务，导入多个 CSV 文件，配置 keep_processed_files=true
    3. 等待任务完成

    验证点：
    1. 任务可以成功创建
    2. 数据库中可以成功入库 CSV 文件的内容
    3. 文件仍然保留在 taosX data_dir 中
    """
    csv_test_logger.info("start test_sanity_csv_td32573_03...")
    env_data = env_data
    case_data = Util.get_case_data_from_yaml("csv/test_csv.yaml", task_type)
    parser_data = Util.get_case_data_from_yaml("csv/test_csv_parser.yaml", task_type)
    case_data["parser"] = parser_data["parser"]

    # set keep_processed_files to true
    case_data["from"]["keep_processed_files"] = "true"

    task = Task(env_data, case_data)
    file = File(env_data, TaskType.CSV)
    upload_file_path1 = file.upload("csv/d0-15.csv")
    upload_file_path2 = file.upload("csv/d0-16.csv")
    upload_file_path = upload_file_path1 + "," + upload_file_path2

    additional_params = {}
    additional_params["csv"] = upload_file_path
    additional_params[
        "createStb"
    ] = f"""
    CREATE STABLE
    `{case_data["to"]["target_dbname"]}`.`csv_meters` (`ts` TIMESTAMP, `current` INT, `voltage` INT, `phase` DOUBLE, `desc` BINARY(64))
    TAGS (`id` INT);
    """

    # 创建任务
    task_info = task.sanity_test_create_task(additional_params=additional_params)

    # 等待 10s 或任务结束
    for _ in range(2):
        task_status = task.get_task_status(task_info["id"])
        if task_status["status"] == "completed":
            break
        else:
            time.sleep(5)

    # 获取运行指标
    metrics = task.get_task_metrics(task_info["id"])

    # check db count
    rows_count = TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    assert rows_count == metrics["current"]["written_rows"], csv_test_logger.info(
        "TD-32573: data should be imported successfully"
    )
    print(rows_count)

    # check file1 is exists
    file_absoulte_path1 = os.path.join(
        os.path.dirname("/var/lib/taos/taosx/"), upload_file_path1
    )
    assert os.path.exists(file_absoulte_path1), csv_test_logger.info(
        "TD-32573: file should be exists"
    )
    print(file_absoulte_path1)

    # check file2 is exists
    file_absoulte_path2 = os.path.join(
        os.path.dirname("/var/lib/taos/taosx/"), upload_file_path2
    )
    assert os.path.exists(file_absoulte_path2), csv_test_logger.info(
        "TD-32573: file should be exists"
    )
    print(file_absoulte_path2)


@pytest.mark.sanity
@allure.link("https://jira.taosdata.com:18080/browse/TD-32576")
def test_sanity_csv_td32576_01(env_data):
    env_data = env_data
    # Skip test if TDengine version >= 3.4
    if version.parse(env_data["db_version"][:5]) >= version.parse("3.4"):
        return

    """
    用例概述：验证“不监听新文件”，文件名排序“降序”的目录配置可以工作
    用例步骤：
    1. 在 DB 中创建超级表 csv_meters
    2. 在 taosX 服务器上准备一个包含 CSV 文件的目录
    3. 创建任务，配置数据源为服务器目录，配置 new_file_notify=false, file_sort=2
    4. 启动任务后，继续向目录中增加新的 CSV 文件
    5. 等待任务完成

    验证点：
    1. 任务可以成功创建，并且任务完成以后状态为 Completed
    2. 数据库中可以成功入库 CSV 文件的内容，文件处理的顺序是按照降序来处理的
    3. 观察新增文件应该不被继续处理，并且任务始终保持 Completed
    """
    csv_test_logger.info("start test_sanity_csv_td32576_01...")
    env_data = env_data
    case_data = Util.get_case_data_from_yaml("csv/test_csv.yaml", task_type)
    parser_data = Util.get_case_data_from_yaml("csv/test_csv_parser.yaml", task_type)
    case_data["parser"] = parser_data["parser"]

    # set new_file_notify to false, file_sort to 2 and read_concurrency to 1
    case_data["from"]["new_file_notify"] = "false"
    case_data["from"]["sort"] = 2
    case_data["from"]["read_concurrency"] = 1

    # set data source to a directory
    os.makedirs("/data/test-csv", exist_ok=True)
    os.chdir(os.getcwd())
    os.system(f"cp config/csv/d0-15.csv /data/test-csv/d0-15.csv")
    os.system(f"cp config/csv/d0-16.csv /data/test-csv/d0-16.csv")
    case_data["from"]["fromhost"] = "csv:/data/test-csv"

    task = Task(env_data, case_data)

    additional_params = {}
    additional_params[
        "createStb"
    ] = f"""
    CREATE STABLE
    `{case_data["to"]["target_dbname"]}`.`csv_meters` (`ts` TIMESTAMP, `current` INT, `voltage` INT, `phase` DOUBLE, `desc` BINARY(64))
    TAGS (`id` INT);
    """

    # 创建任务
    task_info = task.sanity_test_create_task(additional_params=additional_params)

    # 5s 后继续添加文件
    time.sleep(5)
    os.system(f"cp config/csv/d0-15.csv /data/test-csv/d0-17.csv")

    # 等待 20s 或任务结束
    for _ in range(4):
        task_status = task.get_task_status(task_info["id"])
        if task_status["status"] == "completed":
            break
        else:
            time.sleep(5)

    # 清空目录
    os.system(f"rm -rf /data/test-csv")

    # 获取运行指标
    metrics = task.get_task_metrics(task_info["id"])

    # check db count
    rows_count = TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    assert rows_count == metrics["current"]["written_rows"], csv_test_logger.info(
        "TD-32576: data should be imported successfully"
    )
    print(rows_count)

    # check the processing order
    result = TaosAdapter.run_sql(
        env_data["taosadapter_host"],
        f"select * from `{case_data['to']['target_dbname']}`.`csv_meters` order by ts limit 1",
    )
    assert result["data"][0][5] == 6, csv_test_logger.info(
        "TD-32576: the processing order should be descending"
    )

    # check task status
    assert task_status["status"] == "completed", csv_test_logger.info(
        "TD-32576: task status should be completed"
    )

    # check total_task_file
    assert metrics["total"]["total_csv_files"] == 2, csv_test_logger.info(
        "TD-32576: total_task_file should be 2"
    )


@pytest.mark.sanity
@pytest.mark.skip
@allure.link("https://jira.taosdata.com:18080/browse/TD-32576")
def test_sanity_csv_td32576_02(env_data):
    env_data = env_data
    # Skip test if TDengine version >= 3.4
    if version.parse(env_data["db_version"][:5]) >= version.parse("3.4"):
        return

    """
    用例概述：验证“监听新文件”，文件名排序“升序”的目录配置可以工作
    用例步骤：
    1. 在 DB 中创建超级表 csv_meters
    2. 在 taosX 服务器上准备一个包含 CSV 文件的目录
    3. 创建任务，配置数据源为服务器目录，配置 new_file_notify=true, file_sort=1
    4. 启动任务后，继续向目录中增加新的 CSV 文件：mv/cp/scp 到目录与子目录中
    5. 等待一段时间后观察任务持续运行

    验证点：
    1. 任务可以成功创建，并且任务状态持续为 Running
    2. 数据库中可以成功入库 CSV 文件的内容，文件处理的顺序是按照升序来处理的
    3. 观察新增文件应该被继续处理
    """
    csv_test_logger.info("start test_sanity_csv_td32576_02...")
    env_data = env_data
    case_data = Util.get_case_data_from_yaml("csv/test_csv.yaml", task_type)
    parser_data = Util.get_case_data_from_yaml("csv/test_csv_parser.yaml", task_type)
    case_data["parser"] = parser_data["parser"]

    # set new_file_notify to true, notify_duration to 5s, file_sort to 1 and read_concurrency to 1
    case_data["from"]["new_file_notify"] = "true"
    case_data["from"]["notify_interval"] = "5s"
    case_data["from"]["sort"] = 1
    case_data["from"]["read_concurrency"] = 1

    # set data source to a directory
    os.makedirs("/data/test-csv", exist_ok=True)
    os.chdir(os.getcwd())
    os.system(f"cp config/csv/d0-15.csv /data/test-csv/d0-15.csv")
    os.system(f"cp config/csv/d0-16.csv /data/test-csv/d0-16.csv")
    case_data["from"]["fromhost"] = "csv:/data/test-csv"

    task = Task(env_data, case_data)

    additional_params = {}
    additional_params[
        "createStb"
    ] = f"""
    CREATE STABLE
    `{case_data["to"]["target_dbname"]}`.`csv_meters` (`ts` TIMESTAMP, `current` INT, `voltage` INT, `phase` DOUBLE, `desc` BINARY(64))
    TAGS (`id` INT);
    """

    # 创建任务
    task_info = task.sanity_test_create_task(additional_params=additional_params)

    # 等待 10s 或任务结束
    for _ in range(2):
        task_status = task.get_task_status(task_info["id"])
        if task_status["status"] == "completed":
            break
        else:
            time.sleep(5)

    # check the processing order
    result = TaosAdapter.run_sql(
        env_data["taosadapter_host"],
        f"select * from `{case_data['to']['target_dbname']}`.`csv_meters` order by ts limit 1",
    )
    assert result["data"][0][5] == 1, csv_test_logger.info(
        "TD-32576: the processing order should be ascending"
    )

    # 继续添加文件
    os.system(f"cp config/csv/d0-15.csv /data/test-csv/d0-17.csv")
    os.makedirs("/data/test-csv/subdir", exist_ok=True)
    os.system(
        f"cp config/csv/d0-15.csv config/csv/d0-18.csv & mv config/csv/d0-18.csv /data/test-csv/subdir/d0-18.csv"
    )
    os.system(
        f"sshpass -p 'Tbaseapp2!' scp root@192.168.0.201:/app2/test-csv/d0-15.csv /data/test-csv/subdir/d0-19.csv"
    )

    # 等待 30s 或任务结束
    for _ in range(6):
        task_status = task.get_task_status(task_info["id"])
        if task_status["status"] == "completed":
            break
        else:
            time.sleep(5)

    # 清空目录
    os.system(f"rm -rf /data/test-csv")

    # 获取运行指标
    metrics = task.get_task_metrics(task_info["id"])

    # check db count
    rows_count = TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    assert rows_count == metrics["current"]["written_rows"], csv_test_logger.info(
        "TD-32576: data should be imported successfully"
    )
    print(rows_count)

    # check task status
    assert task_status["status"] == "running", csv_test_logger.info(
        "TD-32576: task status should be running"
    )

    # check total_task_file
    assert metrics["total"]["total_csv_files"] == 5, csv_test_logger.info(
        "TD-32576: total_task_file should be 5"
    )


@pytest.mark.sanity
@allure.link("https://jira.taosdata.com:18080/browse/TD-32576")
def test_sanity_csv_td32576_03(env_data):
    env_data = env_data
    # Skip test if TDengine version >= 3.4
    if version.parse(env_data["db_version"][:5]) >= version.parse("3.4"):
        return

    """
    用例概述：验证“匹配模式”可以对文件进行过滤筛选
    用例步骤：
    1. 在 DB 中创建超级表 csv_meters
    2. 在 taosX 服务器上准备一个包含 CSV 文件的目录，文件名包含多种组合的规则
    3. 创建任务，配置数据源为服务器目录，配置 new_file_notify=false, file_pattern="^\\?\\-\\*\\-\\[\\-\\]\\-[ab]\\-[^ef]\\-.\\-.*\\.csv$"
    4. 等待一段时间后，观察被处理的文件列表是否符合预期

    验证点：
    1. 任务可以成功创建
    2. 数据库中可以成功入库 CSV 文件的内容，被处理的文件列表符合预期
    """
    csv_test_logger.info("start test_sanity_csv_td32576_03...")
    env_data = env_data
    case_data = Util.get_case_data_from_yaml("csv/test_csv.yaml", task_type)
    parser_data = Util.get_case_data_from_yaml("csv/test_csv_parser.yaml", task_type)
    case_data["parser"] = parser_data["parser"]

    # set new_file_notify to false and file_pattern="^\\?\\-\\*\\-\\[\\-\\]\\-[ab]\\-[^ef]\\-.\\-.*\\.csv$"
    case_data["from"]["new_file_notify"] = "false"
    case_data["from"][
        "file_pattern"
    ] = "^\\?\\-\\*\\-\\[\\-\\]\\-[ab]\\-[^ef]\\-.\\-.*\\.csv$"

    # set data source to a directory
    os.makedirs("/data/test-csv", exist_ok=True)
    os.chdir(os.getcwd())
    # 符合通配符的文件名
    os.system(f"cp config/csv/d0-15.csv /data/test-csv/?-*-[-]-a-c-1-123.csv")
    # 不符合通配符的文件名：不符合 [?]
    os.system(f"cp config/csv/d0-15.csv /data/test-csv/x-*-[-]-a-c-1-123.csv")
    # 不符合通配符的文件名：不符合 [*]
    os.system(f"cp config/csv/d0-15.csv /data/test-csv/?-x-[-]-a-c-1-123.csv")
    # 不符合通配符的文件名：不符合 [[]
    os.system(f"cp config/csv/d0-15.csv /data/test-csv/?-*-x-]-a-c-1-123.csv")
    # 不符合通配符的文件名：不符合 []]
    os.system(f"cp config/csv/d0-15.csv /data/test-csv/?-*-[-x-a-c-1-123.csv")
    # 不符合通配符的文件名：不符合 [ab]
    os.system(f"cp config/csv/d0-15.csv /data/test-csv/?-*-[-]-x-c-1-123.csv")
    # 不符合通配符的文件名：不符合 [!ef]
    os.system(f"cp config/csv/d0-15.csv /data/test-csv/?-*-[-]-a-e-1-123.csv")
    # 不符合通配符的文件名：不符合 ?
    os.system(f"cp config/csv/d0-15.csv /data/test-csv/?-*-[-]-a-c-12-123.csv")
    # 不符合通配符的文件名：前缀
    os.system(f"cp config/csv/d0-15.csv /data/test-csv/xxxx?-*-[-]-a-c-1-123.csv")
    # 不符合通配符的文件名：后缀
    os.system(f"cp config/csv/d0-15.csv /data/test-csv/?-*-[-]-a-c-1-123.csvxxxx")

    case_data["from"]["fromhost"] = "csv:/data/test-csv"

    task = Task(env_data, case_data)

    additional_params = {}
    additional_params[
        "createStb"
    ] = f"""
    CREATE STABLE
    `{case_data["to"]["target_dbname"]}`.`csv_meters` (`ts` TIMESTAMP, `current` INT, `voltage` INT, `phase` DOUBLE, `desc` BINARY(64))
    TAGS (`id` INT);
    """

    # 创建任务
    task_info = task.sanity_test_create_task(additional_params=additional_params)

    # 等待 10s 或任务结束
    for _ in range(2):
        task_status = task.get_task_status(task_info["id"])
        if task_status["status"] == "completed":
            break
        else:
            time.sleep(5)

    # 清空目录
    os.system(f"rm -rf /data/test-csv")

    # 获取运行指标
    metrics = task.get_task_metrics(task_info["id"])

    # check db count
    rows_count = TaosAdapter.check_db_count(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    assert rows_count == metrics["current"]["written_rows"], csv_test_logger.info(
        "TD-32576: data should be imported successfully"
    )
    print(rows_count)

    # check total_task_file
    assert metrics["total"]["total_csv_files"] == 1, csv_test_logger.info(
        "TD-32576: total_task_file should be 1"
    )


@pytest.mark.sanity
@allure.link("https://jira.taosdata.com:18080/browse/TD-32578")
def test_sanity_csv_td32578_01(env_data):
    env_data = env_data
    # Skip test if TDengine version >= 3.4
    if version.parse(env_data["db_version"][:5]) >= version.parse("3.4"):
        return

    """
    用例概述：验证上传多个文件的本次运行指标（csv_files, csv_files_completed, csv_files_completed_rows）计数正确
    用例步骤：
    1. 在 DB 中创建超级表 csv_meters
    2. 创建任务，导入多个 CSV 文件
    3. 等待任务完成，查看运行指标

    验证点：
    1. 任务可以成功创建
    2. 本次运行指标（csv_files, csv_files_completed, csv_files_completed_rows）计数正确
    """
    csv_test_logger.info("start test_sanity_csv_td32578_01...")
    env_data = env_data
    case_data = Util.get_case_data_from_yaml("csv/test_csv.yaml", task_type)
    parser_data = Util.get_case_data_from_yaml("csv/test_csv_parser.yaml", task_type)
    case_data["parser"] = parser_data["parser"]

    task = Task(env_data, case_data)
    file = File(env_data, TaskType.CSV)
    upload_file_path1 = file.upload("csv/d0-15.csv")
    upload_file_path2 = file.upload("csv/d0-16.csv")
    upload_file_path = upload_file_path1 + "," + upload_file_path2

    additional_params = {}
    additional_params["csv"] = upload_file_path
    additional_params[
        "createStb"
    ] = f"""
    CREATE STABLE
    `{case_data["to"]["target_dbname"]}`.`csv_meters` (`ts` TIMESTAMP, `current` INT, `voltage` INT, `phase` DOUBLE, `desc` BINARY(64))
    TAGS (`id` INT);
    """

    # 创建任务
    task_info = task.sanity_test_create_task(additional_params=additional_params)

    # 等待 10s 或任务结束
    for _ in range(2):
        task_status = task.get_task_status(task_info["id"])
        if task_status["status"] == "completed":
            break
        else:
            time.sleep(5)

    # 获取运行指标
    metrics = task.get_task_metrics(task_info["id"])
    print(metrics)

    # check csv_files
    assert metrics["current"]["csv_files"] == 2, csv_test_logger.info(
        "TD-32578: csv_files should be 2"
    )

    # check csv_files_completed
    assert metrics["current"]["csv_files_completed"] == 2, csv_test_logger.info(
        "TD-32578: csv_files_completed should be 2"
    )

    # check csv_files_completed_rows
    assert metrics["current"]["csv_files_completed_rows"] == 10, csv_test_logger.info(
        "TD-32578: csv_files_completed_rows should be 10"
    )


@pytest.mark.sanity
@allure.link("https://jira.taosdata.com:18080/browse/TD-32578")
def test_sanity_csv_td32578_02(env_data):
    env_data = env_data
    # Skip test if TDengine version >= 3.4
    if version.parse(env_data["db_version"][:5]) >= version.parse("3.4"):
        return

    """
    用例概述：验证配置目录的本次运行指标（csv_files, csv_files_completed, csv_files_completed_rows）计数正确
    用例步骤：
    1. 在 DB 中创建超级表 csv_meters
    2. 在 taosX 服务器上准备一个包含 CSV 文件的目录
    3. 创建任务，配置数据源为服务器目录，配置 new_file_notify=false
    4. 等待任务完成，查看运行指标

    验证点：
    1. 任务可以成功创建
    2. 本次运行指标（csv_files, csv_files_completed, csv_files_completed_rows）计数正确
    """
    csv_test_logger.info("start test_sanity_csv_td32578_02...")
    env_data = env_data
    case_data = Util.get_case_data_from_yaml("csv/test_csv.yaml", task_type)
    parser_data = Util.get_case_data_from_yaml("csv/test_csv_parser.yaml", task_type)
    case_data["parser"] = parser_data["parser"]

    # set new_file_notify to false
    case_data["from"]["new_file_notify"] = "false"

    # set data source to a directory
    os.makedirs("/data/test-csv", exist_ok=True)
    os.chdir(os.getcwd())
    os.system(f"cp config/csv/d0-15.csv /data/test-csv/d0-15.csv")
    os.system(f"cp config/csv/d0-16.csv /data/test-csv/d0-16.csv")
    case_data["from"]["fromhost"] = "csv:/data/test-csv"

    task = Task(env_data, case_data)

    additional_params = {}
    additional_params[
        "createStb"
    ] = f"""
    CREATE STABLE
    `{case_data["to"]["target_dbname"]}`.`csv_meters` (`ts` TIMESTAMP, `current` INT, `voltage` INT, `phase` DOUBLE, `desc` BINARY(64))
    TAGS (`id` INT);
    """

    # 创建任务
    task_info = task.sanity_test_create_task(additional_params=additional_params)

    # 等待 10s 或任务结束
    for _ in range(2):
        task_status = task.get_task_status(task_info["id"])
        if task_status["status"] == "completed":
            break
        else:
            time.sleep(5)

    # 清空目录
    os.system(f"rm -rf /data/test-csv")

    # 获取运行指标
    metrics = task.get_task_metrics(task_info["id"])
    print(metrics)

    # check csv_files
    assert metrics["current"]["csv_files"] == 2, csv_test_logger.info(
        "TD-32578: csv_files should be 2"
    )

    # check csv_files_completed
    assert metrics["current"]["csv_files_completed"] == 2, csv_test_logger.info(
        "TD-32578: csv_files_completed should be 2"
    )

    # check csv_files_completed_rows
    assert metrics["current"]["csv_files_completed_rows"] == 10, csv_test_logger.info(
        "TD-32578: csv_files_completed_rows should be 10"
    )


@pytest.mark.sanity
@allure.link("https://jira.taosdata.com:18080/browse/TD-32578")
def test_sanity_csv_td32578_03(env_data):
    env_data = env_data
    # Skip test if TDengine version >= 3.4
    if version.parse(env_data["db_version"][:5]) >= version.parse("3.4"):
        return

    """
    用例概述：验证上传多个文件的累计指标（total_csv_files, total_csv_files_completed, total_csv_files_completed_rows）计数正确
    用例步骤：
    1. 在 DB 中创建超级表 csv_meters
    2. 创建任务，导入多个 CSV 文件
    3. 等待任务完成
    4. 重新启动任务，查看运行指标

    验证点：
    1. 任务可以成功创建
    2. 累计指标（total_csv_files, total_csv_files_completed, total_csv_files_completed_rows）计数正确
    """
    csv_test_logger.info("start test_sanity_csv_td32578_03...")
    env_data = env_data
    case_data = Util.get_case_data_from_yaml("csv/test_csv.yaml", task_type)
    parser_data = Util.get_case_data_from_yaml("csv/test_csv_parser.yaml", task_type)
    case_data["parser"] = parser_data["parser"]

    task = Task(env_data, case_data)
    file = File(env_data, TaskType.CSV)
    upload_file_path1 = file.upload("csv/d0-15.csv")
    upload_file_path2 = file.upload("csv/d0-16.csv")
    upload_file_path = upload_file_path1 + "," + upload_file_path2

    additional_params = {}
    additional_params["csv"] = upload_file_path
    additional_params[
        "createStb"
    ] = f"""
    CREATE STABLE
    `{case_data["to"]["target_dbname"]}`.`csv_meters` (`ts` TIMESTAMP, `current` INT, `voltage` INT, `phase` DOUBLE, `desc` BINARY(64))
    TAGS (`id` INT);
    """

    # 创建任务
    task_info = task.sanity_test_create_task(additional_params=additional_params)

    # 等待 10s 或任务结束
    for _ in range(2):
        task_status = task.get_task_status(task_info["id"])
        if task_status["status"] == "completed":
            break
        else:
            time.sleep(5)

    # 重新启动任务
    task.start_task(task_info["id"])
    time.sleep(2)

    # 获取运行指标
    metrics = task.get_task_metrics(task_info["id"])
    print(metrics)

    # check total_csv_files
    assert metrics["total"]["total_csv_files"] == 2, csv_test_logger.info(
        "TD-32578: total_csv_files should be 2"
    )

    # check total_csv_files_completed
    assert metrics["total"]["total_csv_files_completed"] == 2, csv_test_logger.info(
        "TD-32578: total_csv_files_completed should be 2"
    )

    # check total_csv_files_completed_rows
    assert (
        metrics["total"]["total_csv_files_completed_rows"] == 10
    ), csv_test_logger.info("TD-32578: total_csv_files_completed_rows should be 10")


@pytest.mark.sanity
@allure.link("https://jira.taosdata.com:18080/browse/TD-32578")
def test_sanity_csv_td32578_04(env_data):
    env_data = env_data
    # Skip test if TDengine version >= 3.4
    if version.parse(env_data["db_version"][:5]) >= version.parse("3.4"):
        return

    """
    用例概述：验证配置目录的累计指标（total_csv_files, total_csv_files_completed, total_csv_files_completed_rows）计数正确
    用例步骤：
    1. 在 DB 中创建超级表 csv_meters
    2. 在 taosX 服务器上准备一个包含 CSV 文件的目录
    3. 创建任务，配置数据源为服务器目录，配置 new_file_notify=false
    4. 等待任务完成
    5. 重新启动任务，查看运行指标

    验证点：
    1. 任务可以成功创建
    2. 累计指标（total_csv_files, total_csv_files_completed, total_csv_files_completed_rows）计数正确
    """
    csv_test_logger.info("start test_sanity_csv_td32578_04...")
    env_data = env_data
    case_data = Util.get_case_data_from_yaml("csv/test_csv.yaml", task_type)
    parser_data = Util.get_case_data_from_yaml("csv/test_csv_parser.yaml", task_type)
    case_data["parser"] = parser_data["parser"]

    # set new_file_notify to false
    case_data["from"]["new_file_notify"] = "false"

    # set data source to a directory
    os.makedirs("/data/test-csv", exist_ok=True)
    os.chdir(os.getcwd())
    os.system(f"cp config/csv/d0-15.csv /data/test-csv/d0-15.csv")
    os.system(f"cp config/csv/d0-16.csv /data/test-csv/d0-16.csv")
    case_data["from"]["fromhost"] = "csv:/data/test-csv"

    task = Task(env_data, case_data)

    additional_params = {}
    additional_params[
        "createStb"
    ] = f"""
    CREATE STABLE
    `{case_data["to"]["target_dbname"]}`.`csv_meters` (`ts` TIMESTAMP, `current` INT, `voltage` INT, `phase` DOUBLE, `desc` BINARY(64))
    TAGS (`id` INT);
    """

    # 创建任务
    task_info = task.sanity_test_create_task(additional_params=additional_params)

    # 等待 10s 或任务结束
    for _ in range(2):
        task_status = task.get_task_status(task_info["id"])
        if task_status["status"] == "completed":
            break
        else:
            time.sleep(5)

    # 重新启动任务
    task.start_task(task_info["id"])
    time.sleep(2)

    # 清空目录
    os.system(f"rm -rf /data/test-csv")

    # 获取运行指标
    metrics = task.get_task_metrics(task_info["id"])
    print(metrics)

    # check total_csv_files
    assert metrics["total"]["total_csv_files"] == 2, csv_test_logger.info(
        "TD-32578: total_csv_files should be 2"
    )

    # check total_csv_files_completed
    assert metrics["total"]["total_csv_files_completed"] == 2, csv_test_logger.info(
        "TD-32578: total_csv_files_completed should be 2"
    )

    # check total_csv_files_completed_rows
    assert (
        metrics["total"]["total_csv_files_completed_rows"] == 10
    ), csv_test_logger.info("TD-32578: total_csv_files_completed_rows should be 10")


@pytest.mark.sanity
@allure.link("https://jira.taosdata.com:18080/browse/TD-32577")
def test_sanity_csv_td32577_01(env_data):
    env_data = env_data
    # Skip test if TDengine version >= 3.4
    if version.parse(env_data["db_version"][:5]) >= version.parse("3.4"):
        return

    """
    用例概述：验证上传文件的任务，断点功能可以正确工作
    用例步骤：
    1. 在 DB 中创建超级表 csv_meters
    2. 创建任务，导入多个 CSV 文件
    3. 等待任务完成
    4. 重新启动任务，查看运行指标

    验证点：
    1. 任务可以成功创建
    2. 本次运行指标（csv_files, csv_files_completed, csv_files_completed_rows）计数为零
    """
    csv_test_logger.info("start test_sanity_csv_td32577_01...")
    env_data = env_data
    case_data = Util.get_case_data_from_yaml("csv/test_csv.yaml", task_type)
    parser_data = Util.get_case_data_from_yaml("csv/test_csv_parser.yaml", task_type)
    case_data["parser"] = parser_data["parser"]

    task = Task(env_data, case_data)
    file = File(env_data, TaskType.CSV)
    upload_file_path1 = file.upload("csv/d0-15.csv")
    upload_file_path2 = file.upload("csv/d0-16.csv")
    upload_file_path = upload_file_path1 + "," + upload_file_path2

    additional_params = {}
    additional_params["csv"] = upload_file_path
    additional_params[
        "createStb"
    ] = f"""
    CREATE STABLE
    `{case_data["to"]["target_dbname"]}`.`csv_meters` (`ts` TIMESTAMP, `current` INT, `voltage` INT, `phase` DOUBLE, `desc` BINARY(64))
    TAGS (`id` INT);
    """

    # 创建任务
    task_info = task.sanity_test_create_task(additional_params=additional_params)

    # 等待 10s 或任务结束
    for _ in range(2):
        task_status = task.get_task_status(task_info["id"])
        if task_status["status"] == "completed":
            break
        else:
            time.sleep(5)

    # 重新启动任务
    task.start_task(task_info["id"])
    time.sleep(5)

    # 获取运行指标
    metrics = task.get_task_metrics(task_info["id"])
    print(metrics)

    # check csv_files
    assert metrics["current"]["csv_files"] == 2, csv_test_logger.info(
        "TD-32578: csv_files should be 2"
    )

    # check csv_files_completed
    assert metrics["current"]["csv_files_completed"] == 0, csv_test_logger.info(
        "TD-32578: csv_files_completed should be 0"
    )

    # check csv_files_completed_rows
    assert metrics["current"]["csv_files_completed_rows"] == 0, csv_test_logger.info(
        "TD-32578: csv_files_completed_rows should be 0"
    )


@pytest.mark.sanity
@allure.link("https://jira.taosdata.com:18080/browse/TD-32578")
def test_sanity_csv_td32577_02(env_data):
    env_data = env_data
    # Skip test if TDengine version >= 3.4
    if version.parse(env_data["db_version"][:5]) >= version.parse("3.4"):
        return

    """
    用例概述：验证启用动态监听配置目录的任务，断点功能可以正确工作
    用例步骤：
    1. 在 DB 中创建超级表 csv_meters
    2. 在 taosX 服务器上准备一个包含 CSV 文件的目录
    3. 创建任务，配置数据源为服务器目录，配置 new_file_notify=true
    4. 任务执行一段时间后，停止并重新启动任务
    5. 继续向目录中增加新的 CSV 文件
    6. 等待任务完成，查看运行指标

    验证点：
    1. 任务可以成功创建
    2. 任务重启后仅处理新增文件
    3. 累计指标（total_csv_files, total_csv_files_completed, total_csv_files_completed_rows）计数正确
    4. 本次运行指标（csv_files, csv_files_completed, csv_files_completed_rows）计数正确
    """
    csv_test_logger.info("start test_sanity_csv_td32577_02...")
    env_data = env_data
    case_data = Util.get_case_data_from_yaml("csv/test_csv.yaml", task_type)
    parser_data = Util.get_case_data_from_yaml("csv/test_csv_parser.yaml", task_type)
    case_data["parser"] = parser_data["parser"]

    # set new_file_notify to true
    case_data["from"]["new_file_notify"] = "true"
    case_data["from"]["notify_interval"] = "5s"

    # set data source to a directory
    os.makedirs("/data/test-csv", exist_ok=True)
    os.chdir(os.getcwd())
    os.system(f"cp config/csv/d0-15.csv /data/test-csv/d0-15.csv")
    os.system(f"cp config/csv/d0-16.csv /data/test-csv/d0-16.csv")
    case_data["from"]["fromhost"] = "csv:/data/test-csv"

    task = Task(env_data, case_data)

    additional_params = {}
    additional_params[
        "createStb"
    ] = f"""
    CREATE STABLE
    `{case_data["to"]["target_dbname"]}`.`csv_meters` (`ts` TIMESTAMP, `current` INT, `voltage` INT, `phase` DOUBLE, `desc` BINARY(64))
    TAGS (`id` INT);
    """

    # 创建任务
    task_info = task.sanity_test_create_task(additional_params=additional_params)

    # 等待 10s 或任务结束
    for _ in range(2):
        task_status = task.get_task_status(task_info["id"])
        if task_status["status"] == "completed":
            break
        else:
            time.sleep(5)

    # 停止并重新启动任务
    task.stop_task(task_info["id"])
    time.sleep(2)
    task.start_task(task_info["id"])
    time.sleep(2)

    # 继续添加文件
    os.system(f"cp config/csv/d0-15.csv /data/test-csv/d0-17.csv")

    # 等待 20s 或任务结束
    for _ in range(4):
        task_status = task.get_task_status(task_info["id"])
        if task_status["status"] == "completed":
            break
        else:
            time.sleep(5)

    # 清空目录
    os.system(f"rm -rf /data/test-csv")

    # 获取运行指标
    metrics = task.get_task_metrics(task_info["id"])
    print(metrics)

    # check total_csv_files
    assert metrics["total"]["total_csv_files"] == 3, csv_test_logger.info(
        "TD-32578: total_csv_files should be 3"
    )

    # check total_csv_files_completed
    assert metrics["total"]["total_csv_files_completed"] == 3, csv_test_logger.info(
        "TD-32578: total_csv_files_completed should be 3"
    )

    # check total_csv_files_completed_rows
    assert (
        metrics["total"]["total_csv_files_completed_rows"] == 15
    ), csv_test_logger.info("TD-32578: total_csv_files_completed_rows should be 15")

    # check csv_files
    assert metrics["current"]["csv_files"] == 3, csv_test_logger.info(
        "TD-32578: csv_files should be 3"
    )

    # check csv_files_completed
    assert metrics["current"]["csv_files_completed"] == 1, csv_test_logger.info(
        "TD-32578: csv_files_completed should be 1"
    )

    # check csv_files_completed_rows
    assert metrics["current"]["csv_files_completed_rows"] == 5, csv_test_logger.info(
        "TD-32578: csv_files_completed_rows should be 5"
    )
