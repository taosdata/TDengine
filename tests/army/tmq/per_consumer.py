import os
import taos
import time
from datetime import datetime
from frame.log import *
import subprocess
from multiprocessing import Process
import threading
from taos.tmq import Consumer
import click

# TDDO
# 1. using tmq common class to replace the function, file drop_lost_consumers.py has the same function

try:
    conn = taos.connect()
except Exception as e:
    tdLog.info(str(e))


@click.command()
@click.option(
    "-c",
    "--consumer-groups-num",
    "consumer_group_num",
    default=1,
    help="Number of consumer group.",
)
@click.option(
    "-s",
    "--session-timeout-ms",
    "session_timeout_ms",
    default=60000,
    help="session timeout:ms",
)
@click.option(
    "-p",
    "--max-poll-interval-ms",
    "max_poll_interval_ms",
    default=180000,
    help="max poll interval timeout:ms",
)
def test_timeout_sub(consumer_group_num, session_timeout_ms, max_poll_interval_ms):
    threads = []
    tdLog.info(consumer_group_num, session_timeout_ms, max_poll_interval_ms)
    for id in range(consumer_group_num):
        conf = set_conf(
            group_id=id,
            session_timeout_ms=session_timeout_ms,
            max_poll_interval_ms=max_poll_interval_ms,
        )
        tdLog.info(conf)
        threads.append(threading.Thread(target=taosc_consumer, args=(conf,)))
    for tr in threads:
        tr.start()
    for tr in threads:
        tr.join()


def sub_consumer(consumer, group_id):
    group_id = int(group_id)
    if group_id < 100:
        try:
            consumer.subscribe(["select_d1"])
        except Exception as e:
            tdLog.info(f"subscribe error")
            exit(1)

    nrows = 0
    while True:
        start = datetime.now()
        tdLog.info(f"time:{start},consumer:{group_id}, start to consume")
        message = consumer.poll(timeout=10.0)

        if message:
            id = message.offset()
            topic = message.topic()
            database = message.database()

            for block in message:
                addrows = block.nrows()
                nrows += block.nrows()
                ncols = block.ncols()
                values = block.fetchall
            end = datetime.now()
            elapsed_time = end - start
            tdLog.info(
                f"time:{end},consumer:{group_id}, elapsed time:{elapsed_time},consumer_nrows:{nrows},consumer_addrows:{addrows}, consumer_ncols:{ncols},offset:{id}"
            )
        consumer.commit()
        tdLog.info(f"consumer:{group_id},consumer_nrows:{nrows}")
        # consumer.unsubscribe()
        # consumer.close()


def sub_consumer_once(consumer, group_id):
    group_id = int(group_id)
    if group_id < 100:
        consumer.subscribe(["select_d1"])
    nrows = 0
    consumer_nrows = 0
    while True:
        start = datetime.now()
        tdLog.info(f"time:{start},consumer:{group_id}, start to consume")
        # start = datetime.now()
        # tdLog.info(f"time:{start},consumer:{group_id}, start to consume")
        tdLog.info(f"consumer_nrows:{consumer_nrows}")
        if consumer_nrows < 1000000:
            message = consumer.poll(timeout=10.0)
        else:
            tdLog.info(" stop consumer when consumer all rows")

        if message:
            id = message.offset()
            topic = message.topic()
            database = message.database()

            for block in message:
                addrows = block.nrows()
                nrows += block.nrows()
                ncols = block.ncols()
                values = block.fetchall
            end = datetime.now()
            elapsed_time = end - start
            # tdLog.info(f"time:{end},consumer:{group_id}, elapsed time:{elapsed_time},consumer_nrows:{nrows},consumer_addrows:{addrows}, consumer_ncols:{ncols},offset:{id}")
        consumer.commit()
        # tdLog.info(f"consumer:{group_id},consumer_nrows:{nrows}")
        consumer_nrows = nrows
        # consumer.unsubscribe()
        # consumer.close()
        # break


def set_conf(
    td_connect_ip="localhost",
    group_id=1,
    client_id="test_consumer_py",
    enable_auto_commit="false",
    auto_commit_interval_ms="1000",
    auto_offset_reset="earliest",
    msg_with_table_name="true",
    session_timeout_ms=10000,
    max_poll_interval_ms=20000,
    experimental_snapshot_enable="false",
):
    conf = {
        # auth options
        # consume options
        "td.connect.ip": f"{td_connect_ip}",
        "group.id": f"{group_id}",
        "client.id": f"{client_id}",
        "enable.auto.commit": f"{enable_auto_commit}",
        "auto.commit.interval.ms": f"{auto_commit_interval_ms}",
        "auto.offset.reset": f"{auto_offset_reset}",
        "msg.with.table.name": f"{msg_with_table_name}",
        "session.timeout.ms": f"{session_timeout_ms}",
        "max.poll.interval.ms": f"{max_poll_interval_ms}",
        "experimental.snapshot.enable": f"{experimental_snapshot_enable}",
    }
    return conf


def taosc_consumer(conf):
    consumer = Consumer(conf)
    group_id = int(conf["group.id"])
    tdLog.info(f"{consumer},{group_id}")
    try:
        sub_consumer_once(consumer, group_id)
    except Exception as e:
        tdLog.info(str(e))


if __name__ == "__main__":
    test_timeout_sub()
