# encoding:UTF-8
import taos

conn = taos.connect()
conn.execute("drop database if exists py_test_json_type")
conn.execute("create database if not exists py_test_json_type")
conn.execute("use py_test_json_type")

conn.execute("create stable s1 (ts timestamp, v1 int) tags (info json)")
conn.execute('create table s1_1 using s1 tags (\'{"k1": "v1"}\')')
tags = conn.query("select info, tbname from s1").fetch_all_into_dict()
print(tags)

k1 = conn.query("select info->'k1' as k1 from s1").fetch_all_into_dict()
print(k1)

conn.execute("drop database py_test_json_type")

conn.close()
