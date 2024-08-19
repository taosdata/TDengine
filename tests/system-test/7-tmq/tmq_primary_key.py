import taos
import sys
import time
import socket
import os
import threading

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *
from taos.tmq import *
from util.dnodes import *
from util.cluster import *
import datetime

sys.path.append("./7-tmq")
from tmqCommon import *


class TDTestCase:
    clientCfgDict = {'debugFlag': 135}
    updatecfgDict = {'debugFlag': 135, 'asynclog': 0, 'tmqRowSize':1}
    updatecfgDict["clientCfg"] = clientCfgDict

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def primaryKeyTestIntQuery(self):
        print("==============Case 1: primary key test int for query")
        tdSql.execute(f'create database if not exists db_pk_query vgroups 1 wal_retention_period 3600;')
        tdSql.execute(f'use db_pk_query;')
        tdSql.execute(f'create table if not exists pk (ts timestamp, c1 int primary key, c2 int);')
        tdSql.execute(f'insert into pk values(1669092069068, 0, 1);')
        tdSql.execute(f'insert into pk values(1669092069068, 6, 1);')
        tdSql.execute(f'flush database db_pk_query')

        tdSql.execute(f'insert into pk values(1669092069069, 0, 1) (1669092069069, 1, 1);')
        tdSql.execute(f'insert into pk values(1669092069069, 2, 1) (1669092069069, 3, 1);')
        tdSql.execute(f'insert into pk values(1669092069068, 10, 1) (1669092069068, 16, 1);')

        tdSql.execute(f'create topic topic_pk_query as select * from pk')

        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false",
            "experimental.snapshot.enable": "true",
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe(["topic_pk_query"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        try:
            while True:
                res = consumer.poll(1)
                if not res:
                    continue
                val = res.value()
                if val is None:
                    continue
                for block in val:
                    data = block.fetchall()
                    for element in data:
                        print(element)
                    if len(data) != 2:
                        tdLog.exit(f"fetchall len != 2")
                    if data != [(datetime(2022, 11, 22, 12, 41, 9, 68000), 0, 1),
                                (datetime(2022, 11, 22, 12, 41, 9, 68000), 6, 1)]:
                        tdLog.exit(f"data error")

                consumer.commit(res)
                break
        finally:
            consumer.close()

        time.sleep(4)  # wait for heart beat

        tdSql.query(f'show subscriptions;')
        sub = tdSql.getData(0, 6);
        print(sub)
        if not sub.startswith("tsdb"):
            tdLog.exit(f"show subscriptions error")

        tdSql.execute(f'use db_pk_query;')
        tdSql.execute(f'insert into pk values(1669092069069, 10, 1);')
        tdSql.execute(f'insert into pk values(1669092069069, 5, 1);')
        tdSql.execute(f'insert into pk values(1669092069069, 12, 1);')
        tdSql.execute(f'insert into pk values(1669092069069, 7, 1);')

        tdSql.execute(f'flush database db_pk_query')

        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe(["topic_pk_query"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        index = 0
        try:
            while True:
                res = consumer.poll(1)
                if not res:
                    break
                val = res.value()
                if val is None:
                    continue
                for block in val:
                    data = block.fetchall()
                    for element in data:
                        print(element)
                    if index == 0:
                        if len(data) != 6:
                            tdLog.exit(f"fetchall len != 6")
                        if data != [(datetime(2022, 11, 22, 12, 41, 9, 68000), 10, 1),
                                    (datetime(2022, 11, 22, 12, 41, 9, 68000), 16, 1),
                                    (datetime(2022, 11, 22, 12, 41, 9, 69000), 0, 1),
                                    (datetime(2022, 11, 22, 12, 41, 9, 69000), 1, 1),
                                    (datetime(2022, 11, 22, 12, 41, 9, 69000), 2, 1),
                                    (datetime(2022, 11, 22, 12, 41, 9, 69000), 3, 1)]:
                            tdLog.exit(f"data error")
                    if index >= 1:
                        if data != [(datetime(2022, 11, 22, 12, 41, 9, 69000), 10, 1)] \
                                and data != [(datetime(2022, 11, 22, 12, 41, 9, 69000), 5, 1)] \
                                and data != [(datetime(2022, 11, 22, 12, 41, 9, 69000), 12, 1)] \
                                and data != [(datetime(2022, 11, 22, 12, 41, 9, 69000), 7, 1)]:
                            tdLog.exit(f"data error")
                index += 1
                print("index:" + str(index))
        finally:
            consumer.close()
            tdSql.execute(f'drop topic topic_pk_query;')

    def primaryKeyTestIntStable(self):
        print("==============Case 2: primary key test int for stable")
        tdSql.execute(f'create database if not exists db_pk_stable vgroups 1 wal_retention_period 3600;')
        tdSql.execute(f'use db_pk_stable;')
        tdSql.execute(f'create table if not exists pks (ts timestamp, c1 int primary key, c2 int) tags (t int);')
        tdSql.execute(f'create table if not exists pk using pks tags(1);')
        tdSql.execute(f'insert into pk values(1669092069068, 0, 1);')
        tdSql.execute(f'insert into pk values(1669092069068, 6, 1);')
        tdSql.execute(f'flush database db_pk_stable')

        tdSql.execute(f'insert into pk values(1669092069069, 0, 1) (1669092069069, 1, 1);')
        tdSql.execute(f'insert into pk values(1669092069069, 2, 1) (1669092069069, 3, 1);')
        tdSql.execute(f'insert into pk values(1669092069068, 10, 1) (1669092069068, 16, 1);')

        tdSql.execute(f'create topic topic_pk_stable as stable pks')

        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false",
            "experimental.snapshot.enable": "true",
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe(["topic_pk_stable"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        try:
            while True:
                res = consumer.poll(1)
                if not res:
                    continue
                val = res.value()
                if val is None:
                    continue
                for block in val:
                    data = block.fetchall()
                    for element in data:
                        print(element)
                    if len(data) != 2:
                        tdLog.exit(f"fetchall len != 2")
                    if data != [(datetime(2022, 11, 22, 12, 41, 9, 68000), 0, 1),
                                (datetime(2022, 11, 22, 12, 41, 9, 68000), 6, 1)]:
                        tdLog.exit(f"data error")

                consumer.commit(res)
                break
        finally:
            consumer.close()

        time.sleep(4)  # wait for heart beat
        tdSql.query(f'show subscriptions;')
        sub = tdSql.getData(0, 6);
        print(sub)
        if not sub.startswith("tsdb"):
            tdLog.exit(f"show subscriptions error")

        tdSql.execute(f'use db_pk_stable;')
        tdSql.execute(f'insert into pk values(1669092069069, 10, 1);')
        tdSql.execute(f'insert into pk values(1669092069069, 5, 1);')
        tdSql.execute(f'insert into pk values(1669092069069, 12, 1);')
        tdSql.execute(f'insert into pk values(1669092069069, 7, 1);')

        tdSql.execute(f'flush database db_pk_stable')

        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe(["topic_pk_stable"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        index = 0
        try:
            while True:
                res = consumer.poll(1)
                if not res:
                    break
                val = res.value()
                if val is None:
                    continue
                for block in val:
                    data = block.fetchall()
                    for element in data:
                        print(element)
                    if index == 0:
                        if len(data) != 6:
                            tdLog.exit(f"fetchall len != 6")
                        if data != [(datetime(2022, 11, 22, 12, 41, 9, 68000), 10, 1),
                                    (datetime(2022, 11, 22, 12, 41, 9, 68000), 16, 1),
                                    (datetime(2022, 11, 22, 12, 41, 9, 69000), 0, 1),
                                    (datetime(2022, 11, 22, 12, 41, 9, 69000), 1, 1),
                                    (datetime(2022, 11, 22, 12, 41, 9, 69000), 2, 1),
                                    (datetime(2022, 11, 22, 12, 41, 9, 69000), 3, 1)]:
                            tdLog.exit(f"data error")
                    if index >= 1:
                        if data != [(datetime(2022, 11, 22, 12, 41, 9, 69000), 10, 1)] \
                                and data != [(datetime(2022, 11, 22, 12, 41, 9, 69000), 5, 1)] \
                                and data != [(datetime(2022, 11, 22, 12, 41, 9, 69000), 12, 1)] \
                                and data != [(datetime(2022, 11, 22, 12, 41, 9, 69000), 7, 1)]:
                            tdLog.exit(f"data error")
                index += 1
                print("index:" + str(index))
        finally:
            consumer.close()
            tdSql.execute(f'drop topic topic_pk_stable;')

    def primaryKeyTestInt(self):
        print("==============Case 3: primary key test int for db")
        tdSql.execute(f'create database if not exists abc1 vgroups 1 wal_retention_period 3600;')
        tdSql.execute(f'use abc1;')
        tdSql.execute(f'create table if not exists pk (ts timestamp, c1 int primary key, c2 int);')
        tdSql.execute(f'insert into pk values(1669092069068, 0, 1);')
        tdSql.execute(f'insert into pk values(1669092069068, 6, 1);')
        tdSql.execute(f'flush database abc1')

        tdSql.execute(f'insert into pk values(1669092069069, 0, 1) (1669092069069, 1, 1);')
        tdSql.execute(f'insert into pk values(1669092069069, 2, 1) (1669092069069, 3, 1);')
        tdSql.execute(f'insert into pk values(1669092069068, 10, 1) (1669092069068, 16, 1);')

        tdSql.execute(f'create topic topic_in with meta as database abc1')

        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false",
            "experimental.snapshot.enable": "true",
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe(["topic_in"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        try:
            while True:
                res = consumer.poll(1)
                if not res:
                    continue
                val = res.value()
                if val is None:
                    continue
                for block in val:
                    data = block.fetchall()
                    for element in data:
                        print(element)
                    if len(data) != 2:
                        tdLog.exit(f"fetchall len != 2")
                    if data != [(datetime(2022, 11, 22, 12, 41, 9, 68000), 0, 1),
                                (datetime(2022, 11, 22, 12, 41, 9, 68000), 6, 1)]:
                        tdLog.exit(f"data error")

                consumer.commit(res)
                break
        finally:
            consumer.close()

        time.sleep(4)  # wait for heart beat
        tdSql.query(f'show subscriptions;')
        sub = tdSql.getData(0, 6);
        print(sub)
        if not sub.startswith("tsdb"):
            tdLog.exit(f"show subscriptions error")

        tdSql.execute(f'use abc1;')
        tdSql.execute(f'insert into pk values(1669092069069, 10, 1);')
        tdSql.execute(f'insert into pk values(1669092069069, 5, 1);')
        tdSql.execute(f'insert into pk values(1669092069069, 12, 1);')
        tdSql.execute(f'insert into pk values(1669092069069, 7, 1);')

        tdSql.execute(f'flush database abc1')

        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe(["topic_in"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        index = 0
        try:
            while True:
                res = consumer.poll(1)
                if not res:
                    break
                val = res.value()
                if val is None:
                    continue
                for block in val:
                    data = block.fetchall()
                    for element in data:
                        print(element)
                    if index == 0:
                        if len(data) != 6:
                            tdLog.exit(f"fetchall len != 6")
                        if data != [(datetime(2022, 11, 22, 12, 41, 9, 68000), 10, 1),
                                    (datetime(2022, 11, 22, 12, 41, 9, 68000), 16, 1),
                                    (datetime(2022, 11, 22, 12, 41, 9, 69000), 0, 1),
                                    (datetime(2022, 11, 22, 12, 41, 9, 69000), 1, 1),
                                    (datetime(2022, 11, 22, 12, 41, 9, 69000), 2, 1),
                                    (datetime(2022, 11, 22, 12, 41, 9, 69000), 3, 1)]:
                            tdLog.exit(f"data error")
                    if index >= 1:
                        if data != [(datetime(2022, 11, 22, 12, 41, 9, 69000), 10, 1)] \
                                and data != [(datetime(2022, 11, 22, 12, 41, 9, 69000), 5, 1)] \
                                and data != [(datetime(2022, 11, 22, 12, 41, 9, 69000), 12, 1)] \
                                and data != [(datetime(2022, 11, 22, 12, 41, 9, 69000), 7, 1)]:
                            tdLog.exit(f"data error")
                index += 1
                print("index:" + str(index))
        finally:
            consumer.close()
            tdSql.execute(f'drop topic topic_in;')

    def primaryKeyTestString(self):
        print("==============Case 4: primary key test string for db")
        tdSql.execute(f'create database if not exists db_pk_string vgroups 1 wal_retention_period 3600;')
        tdSql.execute(f'use db_pk_string;')
        tdSql.execute(f'create table if not exists pk (ts timestamp, c1 varchar(64) primary key, c2 int);')
        tdSql.execute(f'insert into pk values(1669092069068, "ahello", 1);')
        tdSql.execute(f'insert into pk values(1669092069068, "aworld", 1);')
        tdSql.execute(f'flush database db_pk_string')

        tdSql.execute(f'insert into pk values(1669092069069, "him", 1) (1669092069069, "value", 1);')
        tdSql.execute(f'insert into pk values(1669092069069, "she", 1) (1669092069069, "like", 1);')
        tdSql.execute(f'insert into pk values(1669092069068, "from", 1) (1669092069068, "it", 1);')

        tdSql.execute(f'create topic topic_pk_string with meta as database db_pk_string')

        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false",
            "experimental.snapshot.enable": "true",
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe(["topic_pk_string"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        try:
            while True:
                res = consumer.poll(1)
                if not res:
                    continue
                val = res.value()
                if val is None:
                    continue
                for block in val:
                    data = block.fetchall()
                    for element in data:
                        print(element)
                    if len(data) != 2:
                        tdLog.exit(f"fetchall len != 2")
                    if data != [(datetime(2022, 11, 22, 12, 41, 9, 68000), 'ahello', 1),
                                (datetime(2022, 11, 22, 12, 41, 9, 68000), 'aworld', 1)]:
                        tdLog.exit(f"data error")

                consumer.commit(res)
                break
        finally:
            consumer.close()

        time.sleep(4)  # wait for heart beat
        tdSql.query(f'show subscriptions;')
        sub = tdSql.getData(0, 6);
        print(sub)
        if not sub.startswith("tsdb"):
            tdLog.exit(f"show subscriptions error")

        tdDnodes.stop(1)
        time.sleep(2)
        tdDnodes.start(1)

        tdSql.execute(f'use db_pk_string;')
        tdSql.execute(f'insert into pk values(1669092069069, "10", 1);')
        tdSql.execute(f'insert into pk values(1669092069069, "5", 1);')
        tdSql.execute(f'insert into pk values(1669092069069, "12", 1);')
        tdSql.execute(f'insert into pk values(1669092069069, "7", 1);')

        tdSql.execute(f'flush database db_pk_string')

        consumer = Consumer(consumer_dict)
        try:
            consumer.subscribe(["topic_pk_string"])
        except TmqError:
            tdLog.exit(f"subscribe error")
        index = 0
        try:
            while True:
                res = consumer.poll(1)
                if not res:
                    break
                val = res.value()
                if val is None:
                    continue
                for block in val:
                    data = block.fetchall()
                    for element in data:
                        print(element)
                    if index == 0:
                        if len(data) != 6:
                            tdLog.exit(f"fetchall len != 6")
                        if data != [(datetime(2022, 11, 22, 12, 41, 9, 68000), 'from', 1),
                                    (datetime(2022, 11, 22, 12, 41, 9, 68000), 'it', 1),
                                    (datetime(2022, 11, 22, 12, 41, 9, 69000), 'him', 1),
                                    (datetime(2022, 11, 22, 12, 41, 9, 69000), 'like', 1),
                                    (datetime(2022, 11, 22, 12, 41, 9, 69000), 'she', 1),
                                    (datetime(2022, 11, 22, 12, 41, 9, 69000), 'value', 1)]:
                            tdLog.exit(f"data error")
                    if index >= 1:
                        if data != [(datetime(2022, 11, 22, 12, 41, 9, 69000), "10", 1)] \
                                and data != [(datetime(2022, 11, 22, 12, 41, 9, 69000), "5", 1)] \
                                and data != [(datetime(2022, 11, 22, 12, 41, 9, 69000), "12", 1)] \
                                and data != [(datetime(2022, 11, 22, 12, 41, 9, 69000), "7", 1)]:
                            tdLog.exit(f"data error")

                index += 1
                print("index:" + str(index))
        finally:
            consumer.close()
            tdSql.execute(f'drop topic topic_pk_string;')

    def primaryKeyTestTD_30755(self):
        print("==============Case 5: primary key test td-30755 for query")
        tdSql.execute(f'create database if not exists db_pk_query_30755 vgroups 1 wal_retention_period 3600;')
        tdSql.execute(f'use db_pk_query_30755;')
        tdSql.execute(f'create table if not exists pk (ts timestamp, c1 int primary key, c2 int);')
        for i in range(0, 100000):
            tdSql.execute(f'insert into pk values(1669092069068, {i}, 1);')
        tdSql.execute(f'flush database db_pk_query_30755')

        tdSql.execute(f'create topic topic_pk_query_30755 as select * from pk')

        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false",
            "experimental.snapshot.enable": "true",
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe(["topic_pk_query_30755"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        firstConsume = 0
        try:
            while firstConsume < 50000:
                res = consumer.poll(1)
                if not res:
                    continue
                val = res.value()
                if val is None:
                    continue
                for block in val:
                    data = block.fetchall()
                    firstConsume += len(data)
                consumer.commit(res)
        finally:
            consumer.close()

        tdSql.query(f'show subscriptions;')
        sub = tdSql.getData(0, 6);
        print(sub)
        if not sub.startswith("tsdb"):
            tdLog.exit(f"show subscriptions error")

        tdDnodes.stop(1)
        time.sleep(2)
        tdDnodes.start(1)

        consumer = Consumer(consumer_dict)

        tdSql.execute(f'use db_pk_query_30755;')
        try:
            consumer.subscribe(["topic_pk_query_30755"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        secondConsume = 0
        try:
            while firstConsume + secondConsume < 100000:
                res = consumer.poll(1)
                if not res:
                    continue
                val = res.value()
                if val is None:
                    continue
                for block in val:
                    data = block.fetchall()
                    secondConsume += len(data)
                consumer.commit(res)
        finally:
            consumer.close()
            tdSql.execute(f'drop topic topic_pk_query_30755;')
    def run(self):
        self.primaryKeyTestIntQuery()
        self.primaryKeyTestIntStable()
        self.primaryKeyTestInt()
        self.primaryKeyTestString()
        self.primaryKeyTestTD_30755()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())

