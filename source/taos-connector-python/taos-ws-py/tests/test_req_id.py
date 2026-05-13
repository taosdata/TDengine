import pytest
import taosws
import datetime
import utils


config = [
    {
        "db_protocol": "taosws",
        "db_user": utils.test_username(),
        "db_pass": utils.test_password(),
        "db_host": "localhost",
        "db_port": 6041,
        "db_name": "t_ws",
    }
]


@pytest.fixture(params=config)
def ctx(request):
    db_protocol = request.param["db_protocol"]
    db_user = request.param["db_user"]
    db_pass = request.param["db_pass"]
    db_host = request.param["db_host"]
    db_port = request.param["db_port"]

    db_url = f"{db_protocol}://{db_user}:{db_pass}@{db_host}:{db_port}"

    db_name = request.param["db_name"]

    conn = taosws.connect(db_url)

    yield conn, db_name

    conn.execute("DROP DATABASE IF EXISTS %s" % db_name)
    conn.close()


def test_query_simple(ctx):
    conn, db = ctx
    res = conn.query("show dnodes")
    print(f"res: {res}")


def test_execute_with_req_id_simple(ctx):
    conn, db = ctx
    res = conn.execute_with_req_id("show dnodes", 1)
    print(f"res: {res}")


def test_cursor_execute_with_req_id_simple(ctx):
    conn, db = ctx
    cur = conn.cursor()
    res = cur.execute_with_req_id("show dnodes", 1)
    print(f"res: {res}")


def test_cursor_execute_many_with_req_id(ctx):
    conn, db = ctx
    ws = conn
    cur = ws.cursor()
    cur.execute("drop database if exists {}", db)
    cur.execute("create database {}", db)
    cur.execute("use {name}", name=db)
    cur.execute("create stable stb (ts timestamp, v1 int) tags(t1 int)")

    data = [
        {
            "name": "tb1",
            "t1": 1,
        },
        {
            "name": "tb2",
            "t1": 2,
        },
        {
            "name": "tb3",
            "t1": 3,
        },
    ]

    res = cur.execute_many_with_req_id(
        "create table {name} using stb tags({t1})",
        data,
        1,
    )

    print(f"res: {res}")


def test_execute(ctx):
    conn, db = ctx
    ws = conn
    cur = ws.cursor()
    res = cur.execute("show dnodes", 1)
    print(f"res: {res}")
    cur.execute("drop database if exists {}", db)
    cur.execute("create database {}", db)
    cur.execute("use {name}", name=db)
    cur.execute("create stable stb (ts timestamp, v1 int) tags(t1 int)")

    data = [
        {
            "name": "tb1",
            "t1": 1,
        },
        {
            "name": "tb2",
            "t1": 2,
        },
        {
            "name": "tb3",
            "t1": 3,
        },
    ]

    res = cur.execute_many("create table {name} using stb tags({t1})", data)
    print(f"res: {res}")

    ts = datetime.datetime.now().astimezone()
    data = [
        ("tb1", ts, 1),
        ("tb2", ts, 2),
        ("tb3", ts, 3),
    ]
    cur.execute_many(
        "insert into {} values('{}', {})",
        data,
    )
    cur.execute("select * from stb")
    row = cur.fetchone()
    print(f"row: {row}")
    assert row is not None


def test_execute_with_req_id(ctx):
    conn, db = ctx
    ws = conn
    cur = ws.cursor()
    res = cur.execute_with_req_id("show dnodes", 10)
    print(f"res: {res}")
    cur.execute_with_req_id(f"drop database if exists {db}", req_id=11)
    cur.execute_with_req_id(f"create database {db}", req_id=12)
    cur.execute_with_req_id("use {name}", name=db, req_id=13)
    cur.execute_with_req_id("create stable stb (ts timestamp, v1 int) tags(t1 int)", req_id=14)

    data = [
        {
            "name": "tb1",
            "t1": 1,
        },
        {
            "name": "tb2",
            "t1": 2,
        },
        {
            "name": "tb3",
            "t1": 3,
        },
    ]

    res = cur.execute_many_with_req_id(
        "create table {name} using stb tags({t1})",
        data,
        1,
    )
    print(f"res: {res}")

    ts = datetime.datetime.now().astimezone()
    data = [
        ("tb1", ts, 1),
        ("tb2", ts, 2),
        ("tb3", ts, 3),
    ]
    cur.execute_many_with_req_id(
        "insert into {} values('{}', {})",
        data,
        1,
    )
    cur.execute_with_req_id("select * from stb", 1)
    row = cur.fetchone()
    print(f"row: {row}")
    assert row is not None
