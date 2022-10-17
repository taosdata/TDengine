import taos
from taos.tmq import *

conn = taos.connect()

print("init")
conn.execute("drop database if exists py_tmq")
conn.execute("create database if not exists py_tmq vgroups 2")
conn.select_db("py_tmq")
conn.execute("create stable if not exists stb1 (ts timestamp, c1 int, c2 float, c3 binary(10)) tags(t1 int)")
conn.execute("create table if not exists tb1 using stb1 tags(1)")
conn.execute("create table if not exists tb2 using stb1 tags(2)")
conn.execute("create table if not exists tb3 using stb1 tags(3)")

print("create topic")
conn.execute("drop topic if exists topic_ctb_column")
conn.execute("create topic if not exists topic_ctb_column as select ts, c1, c2, c3 from stb1")

print("build consumer")
conf = TaosTmqConf()
conf.set("group.id", "tg2")
conf.set("td.connect.user", "root")
conf.set("td.connect.pass", "taosdata")
conf.set("enable.auto.commit", "true")
conf.set("msg.with.table.name", "true")

def tmq_commit_cb_print(tmq, resp, offset, param=None):
    print(f"commit: {resp}, tmq: {tmq}, offset: {offset}, param: {param}")
    
conf.set_auto_commit_cb(tmq_commit_cb_print, None)
tmq = conf.new_consumer()

print("build topic list")

topic_list = TaosTmqList()
topic_list.append("topic_ctb_column")

print("basic consume loop")
tmq.subscribe(topic_list)

sub_list = tmq.subscription()

print("subscribed topics: ",sub_list)

while 1:
    res = tmq.poll(1000)
    if res:
        topic = res.get_topic_name()
        vg = res.get_vgroup_id()
        db = res.get_db_name()
        print(f"topic: {topic}\nvgroup id: {vg}\ndb: {db}")
        for row in res:
            print(row)
            tb = res.get_table_name()
            print(f"from table: {tb}")
    


