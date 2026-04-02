import taos
import utils
from utils import tear_down_database, PORT


env = {
    "user": utils.test_username(),
    "password": utils.test_password(),
    "host": "localhost",
    "port": PORT,
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

    cursor = conn.cursor()

    cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
    cursor.execute(f"CREATE DATABASE {db_name}")
    cursor.execute(f"USE {db_name}")

    cursor.execute("CREATE STABLE weather(ts TIMESTAMP, temperature FLOAT) TAGS (location INT)")

    cursor.execute(f"INSERT INTO t1 USING weather TAGS(1) VALUES (now, 23.5) (now+100a, 23.5)")

    assert cursor.rowcount == 2

    cursor.execute("SELECT tbname, ts, temperature, location FROM weather")
    # rowcount can only get correct value after fetching all data
    all_data = cursor.fetchall()
    print(f"{'*' * 20} row count: {cursor.rowcount} {'*' * 20}")
    for row in all_data:
        print(row)
    assert cursor.rowcount == 2
    tear_down_database(cursor, db_name)
    cursor.close()
    conn.close()
