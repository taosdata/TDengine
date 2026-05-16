from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import pandas as pd
from dateutil import parser

# InfluxDB 连接参数
url = "http://192.168.1.53:8086"
token = "DGBtxI4gWuLaH9CLYkzYREV80tWNUnJQcnQ62D0ux15cGBdDqJfbZYYI-fawBNU7zdyBVap11hHNWvqHNA7dLw=="
org = "taosdata"
bucket = "stb"

client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)


def write_from_csv():
    write_api = client.write_api(write_options=SYNCHRONOUS)
    source_csv = "/root/m2.csv"
    df = pd.read_csv(source_csv, header=None)
    od = df.iloc[1:, 0]
    nd = list()
    for ts in od:
        d0 = ts.split(".")[0].replace("'", "")
        d1 = ts.split(".")[1].replace("'", "")
        h_d1 = f"{d1}".lstrip("0")
        d1 = 0 if h_d1 == "" else int(f"{d1}".lstrip("0"))
        ms_ts = int(parser.parse(d0).timestamp()) * 10**3 + d1
        nd.append(ms_ts)
    df.iloc[1:, 0] = nd
    for index, row in df.iterrows():
        if index == 0:
            field1_name = row[1]
            field2_name = row[2]
            tagname = row[3]
        else:
            point = (
                Point("influx_stb")
                .tag(tagname, row[3])
                .field(field1_name, row[1])
                .field(field2_name, row[2])
                .time(row[0])
            )
    write_api.write(bucket=bucket, org="taosdata", record=point)


def write():
    write_api = client.write_api(write_options=SYNCHRONOUS)
    for value in range(5):
        point = (
            Point("measurement1")
            .tag("tagname1", "tagvalue1")
            .field("field1", value)
        )
    write_api.write(bucket=bucket, org="taosdata", record=point)


def query_all():
    query_api = client.query_api()
    query = """
            from(bucket: "stb")
            |> range(start: 0)"""
    tables = query_api.query(query, org="taosdata")
    for table in tables:
        for record in table.records:
            print(record)
def query_all_cq():
    query_api = client.query_api()
    query = """
            from(bucket: "stream_bk1")
            |> range(start: 0)"""
    tables = query_api.query(query, org="taosdata")
    for table in tables:
        for record in table.records:
            print(record)
def query():
    query_api = client.query_api()

    query = """from(bucket: "stb")
            |> range(start: -100d)
            |> filter(fn: (r) => r._measurement == "influx_stb")"""
    tables = query_api.query(query, org="taosdata")

    for table in tables:
        for record in table.records:
            print(record)

def agg_query():
    query_api = client.query_api()

    query = """from(bucket: "stb")
            |> range(start: -100d)
            |> filter(fn: (r) => r._measurement == "influx_stb")
            |> filter(fn: (r) => r._field == "c1")
            |> aggregateWindow(every: 10ms, fn: count, createEmpty: false)"""
            # |> count()"""
    tables = query_api.query(query, org="taosdata")

    for table in tables:
        for record in table.records:
            print(record)

def get_database():
    buckets = client.buckets_api().find_buckets()
    print(buckets)

def continue_query():
    # client = influxdb_client.InfluxDBClient(url=url, token="48gdnqiAUYx4aHr-NicF-pTmzpecODvm4fxycZQ-2e41Itf7n0tW3-L6zUrTQP_4GjeLCottya--z4eYWycttA==", org=org)
    query_api = client.query_api()
    query = '''
            option task = {
                name: "continuous_query",
                every: 5m
            }

            from(bucket: "stb")
                |> range(start: 0)
                |> filter(fn: (r) => r._measurement == "influx_stb")
                |> filter(fn: (r) => r._field == "c1")
                |> aggregateWindow(every: 5m, fn: count, createEmpty: false)
                |> to(bucket: "stream_bk1", org: "taosdata")
            '''

    # Execute the Flux query
    result = query_api.query(org="taosdata", query=query)
    # for table in result:
    #     for record in table.records:
    #         print(record)
# write_from_csv()
# query()
# agg_query()
# query_all()
# get_database()
continue_query()
query_all_cq()

# influx export --org "taosdata" --bucket "stb" --file "/root/influxdb_test.csv" --format csv --header
# influx query --org "taosdata" --token "DGBtxI4gWuLaH9CLYkzYREV80tWNUnJQcnQ62D0ux15cGBdDqJfbZYYI-fawBNU7zdyBVap11hHNWvqHNA7dLw==" --bucket "stb" --format csv --raw 'SELECT * FROM measurement_name' | csvtool namedcol
# # 创建 InfluxDB 客户端
# client = InfluxDBClient(url=url, token=token, org=org)

# # 创建写入 API
# write_api = client.write_api(write_options=SYNCHRONOUS)

# # 构建数据点
# data_point = {
#     "measurement": "stb",
#     "tags": {
#         "location": "room1"
#     },
#     "fields": {
#         "value": 25.5
#     }
# }

# # 写入数据点
# write_api.write(bucket=bucket, org=org, record=data_point)