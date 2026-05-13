import taosrest
import os


def connect_to_cloud_use_token(url, token):
    conn = taosrest.connect(url=url, token=token)

    print(conn.server_info)
    cursor = conn.cursor()
    cursor.execute("drop database if exists pytest")
    cursor.execute("create database pytest precision 'ns' keep 365")
    cursor.execute("create table pytest.temperature(ts timestamp, temp int)")
    cursor.execute("insert into pytest.temperature values(now, 1) (now+10b, 2)")
    cursor.execute("select * from pytest.temperature")
    rows = cursor.fetchall()
    print(rows)


if __name__ == "__main__":
    url = os.environ["TDENGINE_CLOUD_URL"]
    token = os.environ["TDENGINE_CLOUD_TOKEN"]
    connect_to_cloud_use_token(url, token)
