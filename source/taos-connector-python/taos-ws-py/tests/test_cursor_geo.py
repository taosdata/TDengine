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
        "db_name": "t_ws1",
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


def test_execute(ctx):
    conn, db = ctx
    ws = conn
    cur = ws.cursor()
    res = cur.execute("show dnodes", 1)
    print(f"res: {res}")
    db = "test11"
    cur.execute("drop database if exists {}", db)
    cur.execute("create database {}", db)
    cur.execute("use {name}", name=db)
    cur.execute("create stable stb (ts timestamp, v1 int, geo geometry(512), vbinary varbinary(32)) tags(t1 int)")

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
        ("tb1", ts, 1, "POINT (4.0 8.0)", "0x7661726332"),
        ("tb2", ts, 2, "POINT (4.0 8.0)", "0x7661726332"),
        ("tb3", ts, 3, "POINT (4.0 8.0)", "0x7661726332"),
    ]
    cur.execute_many(
        "insert into {} values('{}', {}, '{}', '{}')",
        data,
    )
    geo = bytes(
        [
            0x01,
            0x01,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x10,
            0x40,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x20,
            0x40,
        ]
    )
    varbinary = b"0x7661726332"

    cur.execute("select * from stb")
    while True:
        row = cur.fetchone()
        if row:
            print(f"row: {row}")
            assert row[2] == geo
            assert row[3] == varbinary
        else:
            break
