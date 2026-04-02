import taosws
import datetime

env = {
    "db_protocol": "taosws",
    "db_user": "root",
    "db_pass": "taosdata",
    "db_host": "localhost",
    "db_port": 6041,
    "db_name": "t_ws",
}


def make_context(config):
    db_protocol = config["db_protocol"]
    db_user = config["db_user"]
    db_pass = config["db_pass"]
    db_host = config["db_host"]
    db_port = config["db_port"]

    db_url = f"{db_protocol}://{db_user}:{db_pass}@{db_host}:{db_port}"

    db_name = config["db_name"]

    conn = taosws.connect(db_url)

    return conn, db_name


def cursor_execute_with_req_id():
    conn, db = make_context(env)
    ws = conn
    cur = ws.cursor()
    res = cur.execute_with_req_id("show dnodes", 1)
    print(f"res: {res}")
    cur.execute_with_req_id(f"drop database if exists {db}", req_id=1)
    cur.execute_with_req_id(f"create database {db}", req_id=1)
    cur.execute_with_req_id("use {name}", name=db, req_id=1)
    cur.execute_with_req_id("create stable stb (ts timestamp, v1 int) tags(t1 int)", req_id=1)

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


if __name__ == "__main__":
    cursor_execute_with_req_id()
