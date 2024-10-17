import json
import logging
from dataclasses import dataclass
from urllib.parse import quote

import pytest

from testng_taosx.constant import *
from testng_taosx.env import ENV
from testng_taosx.requests_wrapper import http
from testng_taosx.util import Util

connectivity_test_logger = logging.getLogger(__name__)

env_data = Util.get_env_data()
CHECK_CONNECTIVITY_URL = (
    f"{env_data['taos_explorer_root_endpoint']}{DATA_IN_URL}/validate"
)


@dataclass
class CheckConnnectivityRequest:
    dsn: str
    via: int
    timeout: int


def request_connectivity(request_data: CheckConnnectivityRequest):
    dsn = quote(request_data.dsn)
    via_param = ""
    if request_data.via:
        via_param = f"&via={request_data.via}"
    request_url = f"{CHECK_CONNECTIVITY_URL}?dsn={dsn}{via_param}"
    response = http.request("GET", request_url)
    assert (
        response.status_code == 200
    ), f"request {CHECK_CONNECTIVITY_URL} result is {response.text}"
    # 连通性检验应通过
    json_resp = json.loads(response.text)
    assert (
        json_resp["valid"] is True
    ), f"check dsn {request_data.dsn} connectivity response {json_resp} via: {via_param}"
    assert (
        json_resp["support"] is True
    ), f"check dsn {request_data.dsn} connectivity response {json_resp} via: {via_param}"


@pytest.fixture(scope="module", autouse=True)
def get_datasource_dsn():
    connectivity_test_logger.info("connectivity get dsn ...")
    dsn_yaml = Util.read_yaml("connectivity.yaml")
    env_data = Util.get_env_data()
    yield dsn_yaml["dsn"], env_data


def check_connectivity_by_type(get_datasource_dsn, task_type: TaskType):
    dsns, env_data = get_datasource_dsn
    # 这里有一个问题 env.yaml 中部分数据源的声明携带了特殊的内容
    # 如果 data_source 中的 key 值和 TaskType 中的不匹配的话，
    # 就需要 connectivity.yaml 对应的 key 包含完整的 dsn
    data_source_config = env_data["data_source"].get(task_type.value)
    for dsn in dsns[task_type.value]:
        if data_source_config:
            dsn = dsn.format(*data_source_config)
        request = CheckConnnectivityRequest(
            dsn, ENV.choose_platform_agent(task_type), timeout=None
        )
        request_connectivity(request)


@pytest.mark.sanity
def test_TMQ_connectivity(get_datasource_dsn):
    check_connectivity_by_type(get_datasource_dsn, TaskType.TMQ)


@pytest.mark.sanity
def test_td2x_connectivity(get_datasource_dsn):
    check_connectivity_by_type(get_datasource_dsn, TaskType.TDENGINE2X)


@pytest.mark.sanity
def test_pi_connectivity(get_datasource_dsn):
    check_connectivity_by_type(get_datasource_dsn, TaskType.PI)


@pytest.mark.sanity
@pytest.mark.skip(reason="连通性测试需要转移到各个数据源专用的文件中")
def test_opcua_connectivity(get_datasource_dsn):
    check_connectivity_by_type(get_datasource_dsn, TaskType.OPCUA)


@pytest.mark.sanity
@pytest.mark.skip(reason="连通性测试需要转移到各个数据源专用的文件中")
def test_opcda_connectivity(get_datasource_dsn):
    check_connectivity_by_type(get_datasource_dsn, TaskType.OPCDA)


@pytest.mark.sanity
def test_influxdb_connectivity(get_datasource_dsn):
    check_connectivity_by_type(get_datasource_dsn, TaskType.INFLUXDB)


@pytest.mark.sanity
def test_opentsdb_connectivity(get_datasource_dsn):
    check_connectivity_by_type(get_datasource_dsn, TaskType.OPENTSDB)


@pytest.mark.sanity
@pytest.mark.skip(reason="连通性测试需要转移到各个数据源专用的文件中")
def test_mqtt_connectivity(get_datasource_dsn):
    check_connectivity_by_type(get_datasource_dsn, TaskType.MQTT)


@pytest.mark.sanity
def test_kafka_connectivity(get_datasource_dsn):
    check_connectivity_by_type(get_datasource_dsn, TaskType.KAFKA)
