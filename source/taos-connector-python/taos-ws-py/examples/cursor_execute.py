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


def cursor_execute():
    conn, db = make_context(env)
    cur = conn.cursor()
    res = cur.execute("show dnodes", 1)
    print(f"res: {res}")
    cur.execute(f"drop database if exists {db}")
    cur.execute(f"create database {db}")
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

    res = cur.execute_many(
        "create table {name} using stb tags({t1})",
        data,
    )
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
    cur.close()
    conn.close()


if __name__ == "__main__":
    cursor_execute()
