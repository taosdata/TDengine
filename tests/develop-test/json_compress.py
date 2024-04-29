import csv
import json
import os
import random
import string
import time

from faker import Faker
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import fastavro
import psycopg2
from psycopg2.extras import Json


def get_dir_size(start_path='.'):
    total = 0
    for dirpath, dirs, files in os.walk(start_path):
        for f in files:
            fp = os.path.join(dirpath, f)
            # 获取文件大小并累加到total上
            total += os.path.getsize(fp)
    return total


def to_avro_record(obj):
    return {key: value for key, value in obj.items()}


def generate_random_string(length):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def generate_random_values(t):
    if t == 0:
        return random.randint(-255, 256)
    elif t == 1:
        return random.randint(-2100000000, 2100000000)
    elif t == 2:
        return random.uniform(-10000.0, 10000.0)
    elif t == 3:
        return generate_random_string(10)
    elif t == 4:
        return random.choice([True, False])


def generate_json_object(key_set, value_set):
    values = [generate_random_values(t) for t in value_set]
    return dict(zip(key_set, values))


def generate_json_array(keys, values, array_length):
    return [generate_json_object(keys, values) for _ in range(array_length)]


def write_parquet_file(parquet_file, json_array):
    df = pd.DataFrame(json_array)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_file + ".parquet")


def write_json_file(json_file, json_array):
    with open(json_file + ".json", 'w') as f:
        json.dump(json_array, f, separators=(',', ':'))


def generate_avro_schema(k, t):
    if t == 0:
        return {"name": k, "type": "int", "logicalType": "int"}
    elif t == 1:
        return {"name": k, "type": "int", "logicalType": "int"}
    elif t == 2:
        return {"name": k, "type": "float"}
    elif t == 3:
        return {"name": k, "type": "string"}
    elif t == 4:
        return {"name": k, "type": "boolean"}


def write_avro_file(avro_file, json_array, keys, values):
    k = list(json_array[0].keys())

    if keys != k:
        raise ValueError("keys and values should have the same length")

    avro_schema = {
        "type": "record",
        "name": "MyRecord",
        "fields": [generate_avro_schema(k, v) for k, v in dict(zip(keys, values)).items()]
    }

    avro_records = [to_avro_record(obj) for obj in json_array]
    with open(avro_file + ".avro", 'wb') as f:
        fastavro.writer(f, avro_schema, avro_records, codec='xz')


def generate_avro_schema_double(k, t):
    if t == 0:
        return {"name": k, "type": "int", "logicalType": "int"}
    elif t == 1:
        return {"name": k, "type": "int", "logicalType": "int"}
    elif t == 2:
        return {"name": k, "type": "double"}
    elif t == 3:
        return {"name": k, "type": "string"}
    elif t == 4:
        return {"name": k, "type": "boolean"}

def write_avro_file_double(avro_file, json_array, keys, values):
    k = list(json_array[0].keys())

    if keys != k:
        raise ValueError("keys and values should have the same length")

    avro_schema = {
        "type": "record",
        "name": "MyRecord",
        "fields": [generate_avro_schema_double(k, v) for k, v in dict(zip(keys, values)).items()]
    }

    avro_records = [to_avro_record(obj) for obj in json_array]
    with open(avro_file + "_double.avro", 'wb') as f:
        fastavro.writer(f, avro_schema, avro_records, codec='xz')


def write_pg_file(json_array):
    conn_str = "dbname=mydatabase user=myuser host=localhost"
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()

    cur.execute("drop table if exists my_table")
    conn.commit()

    # 创建表（如果不存在）
    cur.execute("""
    CREATE TABLE IF NOT EXISTS my_table (
        id SERIAL PRIMARY KEY,
        json_data JSONB
    );
    """)
    conn.commit()

    # 执行SQL查询
    cur.execute("SELECT count(*) FROM my_table")
    # 获取查询结果
    rows = cur.fetchall()
    # 打印查询结果
    for row in rows:
        print("rows before:", row[0])

    # 插入数据
    for idx, json_obj in enumerate(json_array):
        # print(json.dumps(json_obj))
        cur.execute("INSERT INTO my_table (json_data) VALUES (%s)", (json.dumps(json_obj),))

    conn.commit()  # 提交事务

    # 执行SQL查询
    cur.execute("SELECT count(*) FROM my_table")
    # 获取查询结果
    rows = cur.fetchall()
    # 打印查询结果
    for row in rows:
        print("rows after:", row[0])

    # # 执行SQL查询
    # cur.execute("SELECT pg_relation_size('my_table')")
    # # 获取查询结果
    # rows = cur.fetchall()
    # # 打印查询结果
    # size = 0
    # for row in rows:
    #     size = row[0]
    #     print("table size:", row[0])

    # 关闭游标和连接
    cur.close()
    conn.close()

def read_parquet_file(parquet_file):
    table = pq.read_table(parquet_file + ".parquet")
    df = table.to_pandas()
    print(df)


def read_avro_file(avg_file):
    with open(avg_file + ".avro", 'rb') as f:
        reader = fastavro.reader(f)

        for record in reader:
            print(record)


def read_json_file(csv_file):
    with open(csv_file + ".json", 'r') as f:
        data = json.load(f)
        print(data)


def main():
    key_length = 7
    key_sizes = 4000
    row_sizes = 1000
    file_name = "output"

    # cases = [(0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (0, 4)]
    # cases = [(2, 2), (3, 3), (0, 4)]
    cases = [(2, 2)]

    for data in cases:
        begin, end = data
        print(f"执行类型：{begin}-{end}")

        N = 2
        for _ in range(N):

            t0 = time.time()

            keys = [generate_random_string(key_length) for _ in range(key_sizes)]
            values = [random.randint(begin, end) for _ in range(key_sizes)]
            # 生成JSON数组
            json_array = generate_json_array(keys, values, row_sizes)

            t1 = time.time()

            write_json_file(file_name, json_array)

            t2 = time.time()

            # write_parquet_file(file_name, json_array)

            t3 = time.time()

            write_avro_file(file_name, json_array, keys, values)
            write_avro_file_double(file_name, json_array, keys, values)

            t4 = time.time()

            # size = write_pg_file(json_array)

            t5 = time.time()

            print("生成json 速度：", t2 - t0, "文件大小：", os.path.getsize(file_name + ".json"))
            # print("parquet 速度：", t3 - t2, "文件大小：", os.path.getsize(file_name + ".parquet"))
            print("avro    速度：", t4 - t3, "文件大小：", os.path.getsize(file_name + ".avro"))
            print("avro double 速度：", t4 - t3, "文件大小：", os.path.getsize(file_name + "_double.avro"))
            # print("pg json 速度：", t5 - t4, "文件大小：", get_dir_size("/opt/homebrew/var/postgresql@14/base/16385") - 8 * 1024 * 1024)

            # read_json_file(file_name)
            # read_parquet_file(file_name)
            # read_avro_file(file_name)
        print(f"\n---------------\n")

if __name__ == "__main__":
    main()