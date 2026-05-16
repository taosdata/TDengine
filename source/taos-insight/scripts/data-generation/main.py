# coding:utf-8

import time
import random
import taos
import re
from metrics import *

host = "tdengine"
conn = taos.connect(host=host)
dbname = "log"
write_interval = 3


def select_db():
    conn.execute("create database IF NOT EXISTS " + dbname)
    conn.select_db(dbname)


def clear_stable():
    for stable in all_metrics:
        stname = stable["stable_name"]
        print(f"drop stable {stname}")
        conn.execute("drop stable IF EXISTS " + stname)

def rand_data(v):
    try:
        num, suffix = re.match(r"(\d+|\.\d+|\d+\.\d+)([a-z]\d+)", v).groups()
        if suffix in ["u8", "u16", "u32", "u64", "i8", "i16", "i32", "i64"]:
            num = int(num)
            num = random.randint(0, num * 2)
            v = f"{num}{suffix}"
        elif suffix in ["f32", "f64"]:
            num = float(num)
            num = random.uniform(0, num * 2)
            v = f"{num:.0f}{suffix}"
    except:
        print(f"indivisible v={v}")
    return v


def task():
    lines = []
    for stable in all_metrics:
        stname = stable["stable_name"]
        tags = stable["tags"]
        metrics = stable["metrics"]

        tags_list = []
        for tag in tags:
            tags_list.append(f'{tag["name"]}={tag["value"]}')

        metrics_list = []
        for metric in metrics:
            k = metric["name"]
            v = metric["value"]
            v = rand_data(v)
            metrics_list.append(f"{k}={v}")

        line = f"{stname},{','.join(tags_list)} {','.join(metrics_list)} {int(time.time() * 1000)}"
        lines.append(line)
        print(line)

    conn.schemaless_insert(
        lines, taos.SmlProtocol.LINE_PROTOCOL, taos.SmlPrecision.MILLI_SECONDS
    )


def main():
    select_db()
    clear_stable()
    while True:
        task()
        time.sleep(write_interval)


if __name__ == "__main__":
    main()
