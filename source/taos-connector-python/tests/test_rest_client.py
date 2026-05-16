import datetime
import os
import taos
import utils

from dotenv import load_dotenv
from decorators import check_env
from taosrest.restclient import RestClient
from taos.utils import gen_req_id


load_dotenv()


@check_env
def test_auth():
    url = os.environ["TDENGINE_URL"]
    client = RestClient(url, user=utils.test_username(), password=utils.test_password())
    resp = client.sql("select server_version()")
    print("\n", resp)


@check_env
def test_show_database():
    url = os.environ["TDENGINE_URL"]
    client = RestClient(url)
    resp = client.sql("show databases")
    print("\n", resp)


@check_env
def test_show_database_with_req_id():
    url = os.environ["TDENGINE_URL"]
    client = RestClient(url)
    resp = client.sql("show databases", req_id=gen_req_id())
    print("\n", resp)


@check_env
def test_insert_data():
    url = os.environ["TDENGINE_URL"]
    c = RestClient(url, password=utils.test_password(), database="test")
    c.sql("drop database if exists test")
    c.sql("create database test")
    resp = c.sql("create table tb2 (ts timestamp, c1 int, c2 double, c3 timestamp)")
    print("\n=====================create table resp================")
    print(resp)
    # {'status': 'succ', 'head': ['affected_rows'], 'column_meta': [['affected_rows', 4, 4]], 'rows': 1, 'data': [[0]]}
    resp = c.sql("insert into tb2 values (now, -100, -200.3, now+1m) (now+10s, -101, -340.2423424, now+2m)")
    print("==============insert resp==============")
    print(resp)
    #  {'status': 'succ', 'head': ['affected_rows'], 'column_meta': [['affected_rows', 4, 4]], 'rows': 1, 'data': [[2]]}
    assert resp["rows"] == 1
    if taos.IS_V3:
        assert resp["column_meta"] == [["affected_rows", "INT", 4]]
    else:
        assert resp["column_meta"] == [["affected_rows", 4, 4]]


@check_env
def test_insert_data_with_req_id():
    url = os.environ["TDENGINE_URL"]
    c = RestClient(url, password=utils.test_password(), database="test")
    c.sql("drop database if exists test", req_id=gen_req_id())
    c.sql("create database test", req_id=gen_req_id())
    resp = c.sql("create table tb2 (ts timestamp, c1 int, c2 double, c3 timestamp)", req_id=gen_req_id())
    print("\n=====================create table resp================")
    print(resp)
    # {'status': 'succ', 'head': ['affected_rows'], 'column_meta': [['affected_rows', 4, 4]], 'rows': 1, 'data': [[0]]}
    resp = c.sql(
        "insert into tb2 values (now, -100, -200.3, now+1m) (now+10s, -101, -340.2423424, now+2m)", req_id=gen_req_id()
    )
    print("==============insert resp==============")
    print(resp)
    #  {'status': 'succ', 'head': ['affected_rows'], 'column_meta': [['affected_rows', 4, 4]], 'rows': 1, 'data': [[2]]}
    assert resp["rows"] == 1
    if taos.IS_V3:
        assert resp["column_meta"] == [["affected_rows", "INT", 4]]
    else:
        assert resp["column_meta"] == [["affected_rows", 4, 4]]


@check_env
def test_describe_table():
    url = os.environ["TDENGINE_URL"]
    c = RestClient(url)
    try:
        c.sql("describe test.noexits")
        assert False
    except Exception as e:
        print(e)


@check_env
def test_select_data_with_timestamp_type():
    url = os.environ["TDENGINE_URL"]
    c = RestClient(url)
    resp = c.sql("select * from test.tb2")
    print("\n", resp)
    data = resp["data"]
    assert isinstance(data[0][0], datetime.datetime) and data[0][0].tzinfo is None
    assert isinstance(data[0][3], datetime.datetime) and data[0][3].tzinfo is None


@check_env
def test_select_data_with_timestamp_type_with_req_id():
    url = os.environ["TDENGINE_URL"]
    c = RestClient(url)
    resp = c.sql("select * from test.tb2", req_id=gen_req_id())
    print("\n", resp)
    data = resp["data"]
    assert isinstance(data[0][0], datetime.datetime) and data[0][0].tzinfo is None
    assert isinstance(data[0][3], datetime.datetime) and data[0][3].tzinfo is None


@check_env
def test_use_str_timestamp():
    url = os.environ["TDENGINE_URL"]
    c = RestClient(url, convert_timestamp=False)
    resp = c.sql("select * from test.tb2")
    data = resp["data"]
    print(data[0][0], data[0][3])
    assert isinstance(data[0][0], str) and isinstance(data[0][3], str)


@check_env
def test_use_str_timestamp_with_req_id():
    url = os.environ["TDENGINE_URL"]
    c = RestClient(url, convert_timestamp=False)
    resp = c.sql("select * from test.tb2", req_id=gen_req_id())
    data = resp["data"]
    print(data[0][0], data[0][3])
    assert isinstance(data[0][0], str) and isinstance(data[0][3], str)


def teardown_module(module):
    url = os.environ["TDENGINE_URL"]
    c = RestClient(url)
    c.sql("drop database if exists test")


if __name__ == "__main__":
    test_show_database()
