import logging
import os
import time
import random
import semver
import allure
import pytest
import taosws
from dateutil import parser

from testng_taosx.constant import *
from testng_taosx.file import TaskType
from testng_taosx.task import Task
from testng_taosx.util import TaosAdapter
from testng_taosx.util import Util
from testng_taosx.env import ENV

tmq_test_logger = logging.getLogger(__name__)
task_type = TaskType.TMQ
taosBenchmark_json_dir = Util.get_absolute_path("tmq")
taosBenchmark_json_tar = "/tmp/"


def drop_ci_resource(dsn: str):
    """
    删除创建时间是一天之前的 CI 开头的 topic 和数据库
    :param dsn: python 连接器支持的有效的 dsn
    """
    conn = None
    try:
        conn = taosws.connect(dsn)
        yesterday_query = conn.query("select now() - 1d")
        yesterday = None
        for row in yesterday_query:
            yesterday = row[0]
            break
        yesterday_timestamp = parser.parse(yesterday).timestamp() * 1000
        topic_query = conn.query(
            f"select topic_name from information_schema.ins_topics where topic_name like 'ci_%' and create_time <= {yesterday_timestamp}"
        )
        for topic in topic_query:
            topic_name = topic[0]
            conn.query(f"drop topic `{topic_name}`")

        database_query = conn.query(
            f"select name from information_schema.ins_databases where name like 'ci_%' and create_time <= {yesterday_timestamp}"
        )
        for database in database_query:
            database_name = database[0]
            conn.query(f"drop database `{database_name}`")
    except Exception as err:
        tmq_test_logger.error(f"连接建立失败: {err}, dsn : {dsn}")
        raise err
    finally:
        if conn:
            conn.close()


@pytest.fixture(scope="function")
def input_data():
    tmq_test_logger.info("before tmq test...")
    env_data = Util.get_env_data()
    case_data = Util.read_yaml("tmq/test_tmq_sanity.yaml")
    drop_ci_resource(f"ws://root:taosdata@{ENV.taosd_source_host}:6041")
    yield env_data, case_data
    tmq_test_logger.info("after tmq test...")


@pytest.mark.sanity
@pytest.mark.skipif(Util.lt_version_3_3(), reason="只支持 3.3 版本及以上")
def test_sanity(input_data):
    env_data, case_data = input_data
    case_data["from"]["group.id"] = Util.get_long_name(10)
    source_db_name = f"{Util.get_long_name(10)}"
    data = Util.read_jsonfile(f"{taosBenchmark_json_dir}/basic.json")
    data["databases"][0]["dbinfo"]["name"] = source_db_name
    Util.write_jsonfile(f"{taosBenchmark_json_dir}/basic-1.json", data)
    case_data["source"]["name"] = source_db_name
    case_data["from"]["fromhost"] = f'{case_data["from"]["fromhost"]}{source_db_name}'
    task = Task(env_data, case_data)
    os.system(f"taosBenchmark -f {taosBenchmark_json_dir}/basic-1.json")
    TaosAdapter.run_sql(
        ENV.taosd_source_host,
        f"drop topic if exists {case_data['source']['name']}",
        ignore_result=True,
    )
    source_sum = TaosAdapter.run_sql(
        ENV.taosd_source_host,
        f"select sum(current) from {case_data['source']['name']}.stb_source",
    )["data"][0][0]
    source_count = TaosAdapter.run_sql(
        ENV.taosd_source_host,
        f"select count(*) from {case_data['source']['name']}.stb_source",
    )["data"][0][0]
    TaosAdapter.create_db(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )

    payload = Util.get_task_payload(case_data, env_data)
    task_info = task.create_task(payload)
    while True:
        task_status = task.get_task_status(task_info["id"])
        if task_status["status"] == "completed":
            break
        else:
            time.sleep(10)
    # metrics = task.get_task_metrics(task_info["id"])
    target_count = TaosAdapter.run_sql(
        ENV.taosadapter_host,
        f"select count(*) from {case_data['to']['target_dbname']}.stb_source",
    )["data"][0][0]
    target_sum = TaosAdapter.run_sql(
        ENV.taosadapter_host,
        f"select sum(current) from {case_data['to']['target_dbname']}.stb_source",
    )["data"][0][0]
    # TD-29465 tmq 任务以 raw data 的方式写入时取消 written_rows 等 metrics
    # assert target_count == metrics["current"]["written_rows"] ,tmq_test_logger.error(f"test case failed: insert rows should be same as metrics")
    assert source_sum == target_sum, tmq_test_logger.error(
        f"test case failed: target sum should be same as source"
    )
    assert source_count == target_count, tmq_test_logger.error(
        f"test case failed: target count should be same as source"
    )
    TaosAdapter.drop_db(env_data["taosadapter_host"], case_data["to"]["target_dbname"])
    # TaosAdapter.run_sql(
    #     ENV.taosd_source_host, f"drop topic {case_data['source']['name']}"
    # )
    # TaosAdapter.run_sql(
    #     ENV.taosd_source_host, f"drop database {case_data['source']['name']}"
    # )


@pytest.mark.sanity
@pytest.mark.skipif(Util.lt_version_3_3(), reason="只支持 3.3 版本及以上")
def test_update(input_data):
    """用例描述：
    tmq同步任务对已有数据更新的实时同步
    """
    env_data, case_data = input_data
    case_data["from"]["group.id"] = Util.get_long_name(10)
    source_db_name = f"{Util.get_long_name(10)}"
    data = Util.read_jsonfile(f"{taosBenchmark_json_dir}/basic.json")
    data["databases"][0]["dbinfo"]["name"] = source_db_name
    Util.write_jsonfile(f"{taosBenchmark_json_dir}/basic-1.json", data)
    case_data["source"]["name"] = source_db_name
    case_data["from"]["fromhost"] = f'{case_data["from"]["fromhost"]}{source_db_name}'
    os.system(f"taosBenchmark -f {taosBenchmark_json_dir}/basic-1.json")
    task = Task(env_data, case_data)
    TaosAdapter.run_sql(
        ENV.taosd_source_host,
        f"drop topic {case_data['source']['name']}",
        ignore_result=True,
    )
    TaosAdapter.create_db(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    payload = Util.get_task_payload(case_data, env_data)
    task_info = task.create_task(payload)
    data = Util.read_jsonfile(f"{taosBenchmark_json_dir}/update.json")
    data["databases"][0]["dbinfo"]["name"] = source_db_name
    Util.write_jsonfile(f"{taosBenchmark_json_dir}/update-1.json", data)
    os.system(f"taosBenchmark -f {taosBenchmark_json_dir}/update-1.json")
    time.sleep(10)
    while True:
        task_status = task.get_task_status(task_info["id"])
        print(f"task_status:{task_status}")
        if task_status["status"] == "completed":
            break
        else:
            time.sleep(10)
    source_sum = TaosAdapter.run_sql(
        ENV.taosd_source_host,
        f"select sum(current) from {case_data['source']['name']}.stb_source",
    )["data"][0][0]
    source_count = TaosAdapter.run_sql(
        ENV.taosd_source_host,
        f"select count(*) from {case_data['source']['name']}.stb_source",
    )["data"][0][0]
    target_count = TaosAdapter.run_sql(
        ENV.taosadapter_host,
        f"select count(*) from {case_data['to']['target_dbname']}.stb_source",
    )["data"][0][0]
    target_sum = TaosAdapter.run_sql(
        ENV.taosadapter_host,
        f"select sum(current) from {case_data['to']['target_dbname']}.stb_source",
    )["data"][0][0]
    assert source_sum == target_sum, tmq_test_logger.error(
        f"test case failed: target sum should be same as source"
    )
    assert source_count == target_count, tmq_test_logger.error(
        f"test case failed: target count should be same as source"
    )
    TaosAdapter.drop_db(env_data["taosadapter_host"], case_data["to"]["target_dbname"])
    # TaosAdapter.run_sql(
    #     ENV.taosd_source_host, f"drop topic {case_data['source']['name']}"
    # )
    # TaosAdapter.run_sql(
    #     ENV.taosd_source_host, f"drop database {case_data['source']['name']}"
    # )


@pytest.mark.skip
def test_check_connectivity(input_data):
    env_data, case_data = input_data
    TaosAdapter.run_sql(
        ENV.taosd_source_host, f"drop topic {case_data['source']['name']}"
    )
    Util.ssh_run(
        ENV.taosBenchmark_host, f"taosBenchmark -f {taosBenchmark_json_tar}/basic.json"
    )
    Util.check_connectivity(env_data, case_data["from"]["fromhost"])


@pytest.mark.skip
def test_wrong_dsn(input_data):
    env_data, case_data = input_data
    case_data["from"]["fromhost"] = "tmq+ws:///db1"
    task = Task(env_data, case_data)
    payload = Util.get_task_payload(case_data, env_data)
    task_result = task.create_task_raw(payload)
    assert task_result.status_code == 500, tmq_test_logger.error(
        f"test case failed: When dsn is wrong, the response code should be 500"
    )


@pytest.mark.sanity
@pytest.mark.skipif(Util.lt_version_3_3(), reason="只支持 3.3 版本及以上")
def test_drop_table_while_replication(input_data):
    """
    用例描述：
    tmq 同步过程中，如果删除了目标库的某些子表，taosx在订阅到该表新数据时，会再次自动创建子表并写入新增的数据
    """
    env_data, case_data = input_data
    case_data["from"]["timeout"] = "never"
    case_data["from"]["group.id"] = Util.get_long_name(10)
    source_db_name = f"{Util.get_long_name(10)}"
    data = Util.read_jsonfile(f"{taosBenchmark_json_dir}/basic.json")
    data["databases"][0]["dbinfo"]["name"] = source_db_name
    Util.write_jsonfile(f"{taosBenchmark_json_dir}/basic-1.json", data)
    case_data["source"]["name"] = source_db_name
    case_data["from"]["fromhost"] = f'{case_data["from"]["fromhost"]}{source_db_name}'
    task = Task(env_data, case_data)
    TaosAdapter.run_sql(
        ENV.taosd_source_host,
        f"drop topic {case_data['source']['name']}",
        ignore_result=True,
    )
    os.system(f"taosBenchmark -f {taosBenchmark_json_dir}/basic-1.json")
    TaosAdapter.create_db(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    payload = Util.get_task_payload(case_data, env_data)
    task_info = task.create_task(payload)
    time.sleep(case_data["task_exec_time"])
    TaosAdapter.run_sql(
        ENV.taosd_host,
        f"drop table {case_data['to']['target_dbname']}.tb0",
        ignore_result=True,
    )
    time.sleep(1)
    TaosAdapter.run_sql(
        ENV.taosd_source_host,
        f"insert into {case_data['source']['name']}.tb0 values (now,1)",
    )
    time.sleep(1)
    retry_time = 0
    while retry_time < 10:
        result = TaosAdapter.run_sql(
            ENV.taosd_host,
            f"select count(*) from {case_data['to']['target_dbname']}.tb0",
            True,
        )
        if "data" not in result:
            time.sleep(1)
            retry_time += 1
            continue
        else:
            target_count = TaosAdapter.run_sql(
                ENV.taosd_host,
                f"select count(*) from {case_data['to']['target_dbname']}.tb0",
            )["data"][0][0]
            assert target_count == 1, tmq_test_logger.error(
                f"test case failed: If target table has been dropped before insert,taosx should create a new one"
            )
            break
    task.stop_task(task_info["id"])
    TaosAdapter.drop_db(env_data["taosadapter_host"], case_data["to"]["target_dbname"])
    # TaosAdapter.run_sql(
    #     ENV.taosd_source_host, f"drop topic {case_data['source']['name']}", True
    # )
    # TaosAdapter.run_sql(
    #     ENV.taosd_source_host, f"drop database {case_data['source']['name']}", True
    # )


@pytest.mark.xfail(reason="可能会出现 assert 200 == 291 的情况")
@pytest.mark.sanity
@pytest.mark.skipif(Util.lt_version_3_3(), reason="只支持 3.3 版本及以上")
def test_multi_topic_sub_db(input_data):
    """
    用例描述：
        同步2个topic,每个topic订阅一个db,同步至相同db中。
    """
    env_data, case_data = input_data
    first_source_dbname = f"{Util.get_long_name(10)}"
    first_source_stbname = "source_stb1"
    second_source_dbname = f"{Util.get_long_name(10)}"
    second_source_stbname = "source_stb2"
    TaosAdapter.run_sql(
        ENV.taosd_source_host, f"drop topic  `{first_source_dbname}`", True
    )
    TaosAdapter.run_sql(
        ENV.taosd_source_host, f"drop topic  `{second_source_dbname}`", True
    )
    TaosAdapter.run_sql(
        ENV.taosadapter_host, f"drop datebase  {case_data['to']['target_dbname']}"
    )
    # 获取basic.json中的内容并修改
    # 修改数据源1 的配置并建模
    data = Util.read_jsonfile(f"{taosBenchmark_json_dir}/basic.json")
    data["databases"][0]["dbinfo"]["name"] = first_source_dbname
    data["databases"][0]["super_tables"][0]["name"] = first_source_stbname
    data["databases"][0]["super_tables"][0]["childtable_prefix"] = "tb_1_"
    Util.write_jsonfile(f"{taosBenchmark_json_dir}/basic-1.json", data)
    os.system(f"taosBenchmark -f {taosBenchmark_json_dir}/basic-1.json")

    # 修改数据源2 的配置并建模
    data["databases"][0]["dbinfo"]["name"] = second_source_dbname
    data["databases"][0]["super_tables"][0]["name"] = second_source_stbname
    data["databases"][0]["super_tables"][0]["childtable_prefix"] = "tb_2_"
    Util.write_jsonfile(f"{taosBenchmark_json_dir}/basic-1.json", data)
    os.system(f"taosBenchmark -f {taosBenchmark_json_dir}/basic-1.json")
    source_count = (
        data["databases"][0]["super_tables"][0]["childtable_count"]
        * data["databases"][0]["super_tables"][0]["insert_rows"]
        * 2
    )
    TaosAdapter.run_sql(
        ENV.taosadapter_host,
        f"create database if not exists {case_data['to']['target_dbname']}",
    )
    # 创建两个topic，分别订阅两个db
    TaosAdapter.run_sql(
        ENV.taosd_source_host,
        f"create topic if not exists `{first_source_dbname}` with meta as database `{first_source_dbname}`",
    )
    TaosAdapter.run_sql(
        ENV.taosd_source_host,
        f"create topic if not exists `{second_source_dbname}` with meta as database `{second_source_dbname}`",
    )

    case_data["from"]["fromhost"] = (
        case_data["from"]["fromhost"].rpartition("/")[0]
        + f"/{first_source_dbname},{second_source_dbname}"
    )
    payload = Util.get_task_payload(case_data, env_data)
    task = Task(env_data, case_data)
    task_info = task.create_task(payload)
    while True:
        task_status = task.get_task_status(task_info["id"])
        if task_status["status"] == "completed":
            break
        else:
            time.sleep(10)
    target_count = TaosAdapter.check_db_count(
        ENV.taosadapter_host, case_data["to"]["target_dbname"]
    )
    assert source_count == target_count, tmq_test_logger.error(
        "test case failed,target count should be same as source"
    )
    # TaosAdapter.run_sql(ENV.taosd_source_host, f"drop topic  {first_source_dbname}")
    # TaosAdapter.run_sql(ENV.taosd_source_host, f"drop database {first_source_dbname}")
    # TaosAdapter.run_sql(ENV.taosd_source_host, f"drop topic  {second_source_dbname}")
    # TaosAdapter.run_sql(ENV.taosd_source_host, f"drop database {second_source_dbname}")


@allure.link("https://jira.taosdata.com:18080/browse/TS-5466")
def test_add_col(input_data):
    """
    用例描述：
        订阅数据源中存在schema变更，且普通列字段逐渐增加至64个以上，taosx同步历史数据时，任务可以正确完成，且数据完整。
        1. 创建一个db中，其中有一个stb，stb中有1个ts列和1个普通列
        2. 逐渐给stb中增加普通列，直至64个以上
        3. 创建topic，通过taosx同步数据,此过程中源和目标的taosd服务均正常工作
    """
    env_data, case_data = input_data
    case_data["source"]["name"] = Util.get_long_name(10)
    case_data["from"][
        "fromhost"
    ] = f'{case_data["from"]["fromhost"]}{case_data["source"]["name"]}'
    payload = Util.get_task_payload(case_data, env_data)
    TaosAdapter.run_sql(
        ENV.taosd_source_host,
        f"""create database if not exists {case_data["source"]["name"]}""",
    )
    TaosAdapter.run_sql(
        ENV.taosadapter_host,
        f"""create database if not exists {case_data["to"]["target_dbname"]} """,
    )
    TaosAdapter.run_sql(
        ENV.taosd_source_host,
        f"""create stable if not exists {case_data["source"]["name"]}.s5466 (ts timestamp, c1 int, c2 int) tags (t binary(32))""",
    )
    TaosAdapter.run_sql(
        ENV.taosd_source_host,
        f"""insert into {case_data["source"]["name"]}.t1 using {case_data["source"]["name"]}.s5466 tags('devicid') values(1669092069068, 0, 1)""",
    )
    for i in range(3, 80):
        TaosAdapter.run_sql(
            ENV.taosd_source_host,
            f"""alter table {case_data["source"]["name"]}.s5466 add column c{i} int""",
        )
    TaosAdapter.run_sql(
        ENV.taosd_source_host,
        f"""insert into {case_data["source"]["name"]}.t1(ts, c1, c2) values(1669092069067, 0, 1)""",
    )
    TaosAdapter.run_sql(
        ENV.taosd_source_host, f"""flush database {case_data["source"]["name"]}"""
    )
    task = Task(env_data, case_data)
    task_info = task.create_task(payload)
    while True:
        task_status = task.get_task_status(task_info["id"])
        if task_status["status"] == "completed":
            break
        else:
            time.sleep(10)
    TaosAdapter.run_sql(
        ENV.taosd_source_host, f"""select * from  {case_data["source"]["name"]}.t1"""
    )
    TaosAdapter.run_sql(
        ENV.taosadapter_host,
        f"""select * from  {case_data['to']['target_dbname']}.t1""",
    )


@allure.link("https://jira.taosdata.com:18080/browse/TS-5466")
def test_add_tag(input_data):
    """
    用例描述：
        订阅数据源中存在schema变更，且普通列字段逐渐增加至64个以上，taosx同步历史数据时，任务可以正确完成，且数据完整。
        1. 创建一个db中，其中有一个stb，stb中有1个ts列和1个普通列
        2. 逐渐给stb中增加tag列，直至64个以上
        3. 创建topic，通过taosx同步数据,此过程中源和目标的taosd服务均正常工作
    """
    env_data, case_data = input_data
    case_data["source"]["name"] = Util.get_long_name(10)
    case_data["from"][
        "fromhost"
    ] = f'{case_data["from"]["fromhost"]}{case_data["source"]["name"]}'
    payload = Util.get_task_payload(case_data, env_data)
    TaosAdapter.run_sql(
        ENV.taosd_source_host,
        f"""create database if not exists {case_data["source"]["name"]}""",
    )
    TaosAdapter.run_sql(
        ENV.taosadapter_host,
        f"""create database if not exists {case_data["to"]["target_dbname"]} """,
    )
    TaosAdapter.run_sql(
        ENV.taosd_source_host,
        f"""create stable if not exists {case_data["source"]["name"]}.s5466 (ts timestamp, c1 int, c2 int) tags (t binary(32))""",
    )
    TaosAdapter.run_sql(
        ENV.taosd_source_host,
        f"""insert into {case_data["source"]["name"]}.t1 using {case_data["source"]["name"]}.s5466 tags('devicid') values(1669092069068, 0, 1)""",
    )
    for i in range(3, 80):
        TaosAdapter.run_sql(
            ENV.taosd_source_host,
            f"""alter table {case_data["source"]["name"]}.s5466 add tag t{i} int""",
        )
    TaosAdapter.run_sql(
        ENV.taosd_source_host,
        f"""insert into {case_data["source"]["name"]}.t1(ts, c1, c2) values(1669092069067, 0, 1)""",
    )
    TaosAdapter.run_sql(
        ENV.taosd_source_host, f"""flush database {case_data["source"]["name"]}"""
    )
    task = Task(env_data, case_data)
    task_info = task.create_task(payload)
    while True:
        task_status = task.get_task_status(task_info["id"])
        if task_status["status"] == "completed":
            break
        else:
            time.sleep(10)
    TaosAdapter.run_sql(
        ENV.taosd_source_host, f"""select * from  {case_data["source"]["name"]}.t1"""
    )
    TaosAdapter.run_sql(
        ENV.taosadapter_host,
        f"""select * from  {case_data['to']['target_dbname']}.t1""",
    )


def test_multi_topic_sub_stb(input_data):
    """
    用例描述：
        同步多个topic,每个topic订阅一个stb,同步至相同db中。
    """


def test_topic_as_select(input_data):
    """
    用例描述：
        同步topic，topic为一个select语句且不带with meta
    """

@pytest.mark.performance
def test_case_performance_scenario1(input_data):
    tmq_test_logger.info(f"running TD3 to TD3 performance case scenario1...")
    env_data, case_data = input_data
    case_data = Util.read_yaml("tmq/test_tmq_performance.yaml")
    case_data["to"]["target_dbname"] = "perf_tmq_s1"
    case_data["to"]["column_count"] = 11
    case_data["task_exec_time"] = 60 * 60

    TaosAdapter.run_sql(
        ENV.taosd_source_host, f"drop topic if exists `{case_data['source']['name']}`"
    )
    TaosAdapter.run_sql(
        ENV.taosd_source_host,
        f"create topic if not exists `{case_data['source']['name']}` with meta as database `{case_data['source']['name']}`",
    )
    TaosAdapter.create_db(
        env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    )
    # TaosAdapter.delete_db(
    #     env_data["taosadapter_host"], case_data["to"]["target_dbname"]
    # )
    task = Task(env_data, case_data)
    driver = ENV.driver or "tmq"
    if ENV.source_protocol == "native":
        case_data["from"]["fromhost"] = (
            driver
            + "://root:taosdata@"
            + case_data["from"]["host"]
            + ":6030/"
            + case_data["from"]["dbname"]
        )
        case_data["task_exec_time"] = 30 * 60
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
    if driver == "tmq":
        case_data["from"]["group.id"] = "taosx" + str(random.randint(1, 10000))

    payload = Util.get_task_payload(case_data, env_data)
    print(
        f"driver:{driver}, source_protocol:{ENV.source_protocol}, targar_protocol:{ENV.target_protocol}"
    )
    print(payload)
    task.perf_test(
        payload,
        1,
        "1 task to subscribe 1 topic,1 stable,10w subtables X 1w,ts column+5 columns(int)+5 columns(double)",
        True,
    )


def test_case_performance_scenario2():
    """数据同步性能测试
    用例描述：
        1. 在源IP上用taosBenchmark写入数据，100万子表，meters表，每个子表1000行数据，写入模式stmt+interlace=1
        2. 用taosx同步数据到目标库
    
    """

    env_data = Util.get_env_data()
