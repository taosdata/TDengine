import datetime
import taosrest
import os
import utils
from decorators import check_env
from dotenv import load_dotenv
from taos.utils import gen_req_id
from taosrest import HTTPError, ConnectError


load_dotenv()


@check_env
def test_fetch_all():
    url = os.environ["TDENGINE_URL"]
    conn = taosrest.connect(url=url, password=utils.test_password())
    cursor = conn.cursor()

    cursor.execute("show databases")
    results: list[tuple] = cursor.fetchall()
    for row in results:
        print(row)
    print(cursor.description)


@check_env
def test_fetch_all_with_req_id():
    url = os.environ["TDENGINE_URL"]
    conn = taosrest.connect(url=url, password=utils.test_password())
    cursor = conn.cursor()

    cursor.execute("show databases", req_id=gen_req_id())
    results: list[tuple] = cursor.fetchall()
    for row in results:
        print(row)
    print(cursor.description)


@check_env
def test_fetch_one():
    url = os.environ["TDENGINE_URL"]

    conn = taosrest.connect(url=url, user=utils.test_username(), password=utils.test_password())
    c = conn.cursor()
    c.execute("drop database if exists test")
    c.executemany("create database test")
    c.execute("create table test.tb (ts timestamp, c1 int, c2 double)")
    c.execute("insert into test.tb values (now, -100, -200.3) (now+10s, -101, -340.2423424)")
    assert c.rowcount == 2
    assert c.affected_rows == 2
    c.execute("select * from test.tb")
    assert c.rowcount == 2
    assert c.affected_rows is None
    print()
    row = c.fetchone()
    while row is not None:
        print(row)
        row = c.fetchone()


@check_env
def test_fetch_one_with_req_id():
    url = os.environ["TDENGINE_URL"]

    conn = taosrest.connect(url=url, user=utils.test_username(), password=utils.test_password())
    c = conn.cursor()
    c.execute("drop database if exists test", req_id=gen_req_id())
    c.executemany("create database test", req_id=gen_req_id())
    c.execute("create table test.tb (ts timestamp, c1 int, c2 double)", req_id=gen_req_id())
    c.execute("insert into test.tb values (now, -100, -200.3) (now+10s, -101, -340.2423424)", req_id=gen_req_id())
    assert c.rowcount == 2
    assert c.affected_rows == 2
    c.execute("select * from test.tb", req_id=gen_req_id())
    assert c.rowcount == 2
    assert c.affected_rows is None
    print()
    row = c.fetchone()
    while row is not None:
        print(row)
        row = c.fetchone()


@check_env
def test_row_count():
    url = os.environ["TDENGINE_URL"]
    conn = taosrest.connect(url=url, user=utils.test_username(), password=utils.test_password())
    cursor = conn.cursor()
    cursor.execute("select * from test.tb")
    assert cursor.rowcount == 2


@check_env
def test_row_count_with_req_id():
    url = os.environ["TDENGINE_URL"]
    conn = taosrest.connect(url=url, user=utils.test_username(), password=utils.test_password())
    cursor = conn.cursor()
    cursor.execute("select * from test.tb", req_id=gen_req_id())
    assert cursor.rowcount == 2


@check_env
def test_get_server_info():
    url = os.environ["TDENGINE_URL"]
    conn = taosrest.connect(url=url, user=utils.test_username(), password=utils.test_password())

    version: str = conn.server_info
    # 3.0.5.0 or 3.0.6.0.alpha or 3.4.0.0.alpha.community
    assert len(version.split(".")) == 4 or len(version.split(".")) == 5 or len(version.split(".")) == 6


@check_env
def test_execute():
    url = os.environ["TDENGINE_URL"]
    c = taosrest.connect(url=url)
    c.execute("drop database if exists test")
    c.execute("create database test")
    c.execute("create table test.tb (ts timestamp, c1 int, c2 double)")
    affected_rows = c.execute("insert into test.tb values (now, -100, -200.3) (now+10s, -101, -340.2423424)")
    assert affected_rows == 2
    affected_rows = c.execute("select * from test.tb")
    assert affected_rows is None


@check_env
def test_execute_with_req_id():
    url = os.environ["TDENGINE_URL"]
    c = taosrest.connect(url=url)
    c.execute("drop database if exists test", req_id=gen_req_id())
    c.execute("create database test", req_id=gen_req_id())
    c.execute("create table test.tb (ts timestamp, c1 int, c2 double)", req_id=gen_req_id())
    affected_rows = c.execute(
        "insert into test.tb values (now, -100, -200.3) (now+10s, -101, -340.2423424)", req_id=gen_req_id()
    )
    assert affected_rows == 2
    affected_rows = c.execute("select * from test.tb", req_id=gen_req_id())
    assert affected_rows is None


@check_env
def test_query():
    """
    Note: run it immediately after `test_execute`
    """
    url = os.environ["TDENGINE_URL"]
    c = taosrest.connect(url=url)
    r = c.query("select * from test.tb")
    assert r.rows == 2


@check_env
def test_query_with_req_id():
    """
    Note: run it immediately after `test_execute`
    """
    url = os.environ["TDENGINE_URL"]
    c = taosrest.connect(url=url)
    r = c.query("select * from test.tb", req_id=gen_req_id())
    assert r.rows == 2


@check_env
def test_default_database():
    """
    Note: run it immediately after `test_query`
    """
    url = os.environ["TDENGINE_URL"]
    c = taosrest.connect(url=url, database="test")
    r = c.query("select * from tb")
    assert r.rows == 2


@check_env
def test_default_database_with_req_id():
    """
    Note: run it immediately after `test_query`
    """
    url = os.environ["TDENGINE_URL"]
    c = taosrest.connect(url=url, database="test")
    r = c.query("select * from tb", req_id=gen_req_id())
    assert r.rows == 2


@check_env
def test_no_timezone():
    url = os.environ["TDENGINE_URL"]
    c = taosrest.connect(url=url)
    r = c.query("select * from test.tb")
    for row in r:
        print(row)  # [datetime.datetime(2022, 7, 26, 5, 56, 58, 746000), -100, -200.3]


@check_env
def test_no_timezone_with_req_id():
    url = os.environ["TDENGINE_URL"]
    c = taosrest.connect(url=url)
    r = c.query("select * from test.tb", req_id=gen_req_id())
    for row in r:
        print(row)  # [datetime.datetime(2022, 7, 26, 5, 56, 58, 746000), -100, -200.3]


@check_env
def test_str_timezone():
    url = os.environ["TDENGINE_URL"]
    c = taosrest.connect(url=url, timezone="Asia/Shanghai")
    r = c.query("select * from test.tb")
    for row in r:
        print(row)
        # [datetime.datetime(2022, 7, 26, 13, 56, 58, 746000,
        # tzinfo=<DstTzInfo 'Asia/Shanghai' CST+8:00:00 STD>), -100, -200.3]


@check_env
def test_str_timezone_with_req_id():
    url = os.environ["TDENGINE_URL"]
    c = taosrest.connect(url=url, timezone="Asia/Shanghai")
    r = c.query("select * from test.tb", req_id=gen_req_id())
    for row in r:
        print(row)
        # [datetime.datetime(2022, 7, 26, 13, 56, 58, 746000,
        # tzinfo=<DstTzInfo 'Asia/Shanghai' CST+8:00:00 STD>), -100, -200.3]


@check_env
def test_tzinfo_timezone():
    url = os.environ["TDENGINE_URL"]
    tz = datetime.datetime.now().astimezone().tzinfo
    c = taosrest.connect(url=url, timezone=tz)
    r = c.query("select * from test.tb")
    for row in r:
        print(row)
        # [datetime.datetime(2022, 7, 26, 13, 56, 58, 746000,
        # tzinfo=datetime.timezone(datetime.timedelta(seconds=28800), 'CST')), -100, -200.3]


@check_env
def test_tzinfo_timezone_with_req_id():
    url = os.environ["TDENGINE_URL"]
    tz = datetime.datetime.now().astimezone().tzinfo
    c = taosrest.connect(url=url, timezone=tz)
    r = c.query("select * from test.tb", req_id=gen_req_id())
    for row in r:
        print(row)
        # [datetime.datetime(2022, 7, 26, 13, 56, 58, 746000,
        # tzinfo=datetime.timezone(datetime.timedelta(seconds=28800), 'CST')), -100, -200.3]


def test_wrong_token():
    try:
        conn = taosrest.connect(url="https://gw.us-east.azure.cloud.tdengine.com", token="wrong_token")
        print(conn.server_info)
    except HTTPError as e:
        print(e)
        assert e.status_code == 401


def test_token():
    conn = taosrest.connect(
        url="https://gw.us-west-2.aws.cloud.tdengine.com", token="f158c322e165e82156f50ba4aa0f3e01081b38d7"
    )
    print(conn.server_info)


@check_env
def test_user():
    url = os.environ["TDENGINE_URL"]
    try:
        root = taosrest.connect(url=url)
        root.execute("CREATE USER test_user PASS 'Ab1!@#$%^&*()-_+=[]{}'")
    except ConnectError as e:
        print(f"Failed to create user: {e}")
        assert e.errno == 0x0350


@check_env
def test_special_characters():
    try:
        url = os.environ["TDENGINE_URL"]
        user1 = taosrest.connect(url=url, user="test_user", password="Ab1!@#$%^&*()-_+=[]{}")
        print("conn server info: %s" % user1.server_info)
    except ConnectError as e:
        print(e)


def teardown_module(module):
    url = os.environ["TDENGINE_URL"]
    c = taosrest.connect(url=url)
    c.execute("drop database if exists test")
