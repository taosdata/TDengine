import taos

env = {
    "user": "root",
    "password": "taosdata",
    "host": "localhost",
    "port": 6030,
}


def make_context(config):
    db_protocol = config.get("db_protocol", "taos")
    db_user = config["user"]
    db_pass = config["password"]
    db_host = config["host"]
    db_port = config["port"]

    db_url = f"{db_protocol}://{db_user}:{db_pass}@{db_host}:{db_port}"
    print("dsn: ", db_url)

    conn = taos.connect(**config)

    db_name = config.get("database", "c_cursor")

    return conn, db_name


def test_cursor():
    conn, db_name = make_context(env)

    cur = conn.cursor()

    cur.execute(f"DROP DATABASE IF EXISTS {db_name}")
    cur.execute(f"CREATE DATABASE {db_name}")
    cur.execute(f"USE {db_name}")

    cur.execute("create stable stb (ts timestamp, v1 int) tags(t1 int)")

    create_table_data = [
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
        create_table_data,
    )
    print(f"affected_rows: {res}")
    assert res == 0

    data = [
        ("2018-10-03 14:38:05.100", 219),
        ("2018-10-03 14:38:15.300", 218),
        ("2018-10-03 14:38:16.800", 221),
    ]

    for table in create_table_data:
        table_name = table["name"]

        res = cur.execute_many(
            f"insert into {table_name} values",
            data,
        )
        print(f"affected_rows: {res}")
        assert res == 3

    cur.execute("select * from stb")

    data = cur.fetchall()
    column_names = [meta[0] for meta in cur.description]
    print(column_names)
    for r in data:
        print(r)

    cur.close()
    conn.close()
