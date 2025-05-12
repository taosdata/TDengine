import taos
import datetime
import random
import multiprocessing

def taos_excute(table, connect_host):
    conn = taos.connect(host=connect_host, user="root", password="taosdata", config="/etc/taos", database='test')
    cursor = conn.cursor()
    for i in range(1000000):
        pk = random.randint(100001, 300000)
        time_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        col1 = random.randint(1, 10000)
        col2 = random.randint(1, 10000)
        col3 = random.randint(1, 10000)
        col4 = random.randint(1, 10000)
        col5 = random.randint(1, 10000)
        col6 = random.randint(1, 10000)
        sql = f"INSERT INTO {table}_{pk} USING {table} TAGS ({pk}) VALUES ('{time_now}', {col1}, {col2}, {col3}, {col4}, {col5}, {col6})"
        cursor.execute(sql)
    cursor.close()
    conn.close()

def taos_init(table, connect_host, pk):
    conn = taos.connect(host=connect_host, user="root", password="taosdata", config="/etc/taos", database='test')
    cursor = conn.cursor()
    sql = f"CREATE TABLE {table}_{pk} USING {table} TAGS ({pk})"
    cursor.execute(sql)
    cursor.close()
    conn.close()

print("init time:", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

connect_list = ["node1", "node2", "node3", "node4", "node5"]
pool = multiprocessing.Pool(processes=108)

for pk in range(100001, 300000):
    pool.apply_async(func=taos_init, args=("test", connect_list[pk % 5], pk, ))

print("start time:", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

for i in range(10000):
    pool.apply_async(func=taos_excute, args=("test", connect_list[i % 5],))

pool.close()
pool.join()

print("end time:", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))