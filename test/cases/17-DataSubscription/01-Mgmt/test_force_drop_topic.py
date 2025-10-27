from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck
from taos.tmq import Consumer
import threading
import time
import random

class TestTmqForceDropTopic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_force_drop_topic(self):
        """Tmq: force drop topic

        1. Create db
        2. Create supper table and sub table
        3. Insert data into sub table
        3. Create topic and consume data from sub table
        4. Force drop topic, check the consumer status in another topic, it will be still active

        Catalog:
            - Tmq

        Since: v3.0.0.0

        Labels: common,ci

        Jira: TD-35287

        History:
            - 2025-7-21 Ethan liu adds test for force drop topic

        """

        tdLog.info(f"========== start force drop topic test")
        self.init_data()
        self.prepare_db_topic()
        self.start_consumer()
        self.wait_complete()
        self.check_result()
        tdLog.info(f"========== end force drop topic test successfully")

    def prepare_db_topic(self):
        # create database and use it
        tdLog.info(f"Start prepare database and topic")
        tdSql.execute(f"drop topic if exists force {self.topic} ")
        tdSql.execute(f"drop topic if exists force {self.drop_topic}")

        tdSql.execute(f"drop database if exists {self.db}")
        tdSql.execute(f"create database {self.db} vgroups 1")
        tdSql.execute(f"use {self.db}")

        # create super table and sub table
        tdSql.execute(f"create table {self.super_table} (ts timestamp, flag int) tags (t1 VARCHAR(10))")
        tdSql.execute(f"create table {self.sub_table} using {self.super_table} tags('t1')")

        # create a topic
        tdSql.execute(f"create topic if not exists {self.topic} as database {self.db}")
        tdSql.execute(f"create topic if not exists {self.drop_topic} as database {self.db}")
        tdSql.query(f"show topics")
        tdSql.checkRows(2)

        # insert data into sub table
        start = 1537146000000
        c = 0
        while c < 10000:
            tdLog.info(f"Inserting data into {self.sub_table}, remaining count: {c}")
            ts = start + c * 1000
            tdSql.execute(f"insert into {self.sub_table} values ({ts},{c})")
            c = c + 1

        tdLog.info(f"Prepare database and topic successfully")

    def start_consumer(self):
        tdLog.info(f"Start all threads for inserting and consuming data")
        self.consume_thread_normal1.start()
        self.consume_drop_thread1.start()
        tdLog.info(f"Start all threads for inserting and consuming data successfully")

    def wait_complete(self):
        """Wait for the consumer to complete."""
        tdLog.info(f"Start waiting for threads to complete")
        self.consume_thread_normal1.join()
        self.consume_drop_thread1.join()
        tdLog.info(f"waiting for threads to complete successfully")

    def consume_topic(self, topic_name, consumer_group, client_id, force_drop):
        """Consume data from the specified topic."""
        consumer = Consumer(
            {
                "group.id": consumer_group,
                "client.id": client_id,
                "td.connect.user": "root",
                "td.connect.pass": "taosdata",
                "enable.auto.commit": "true",
                "auto.commit.interval.ms": "1000",
                "auto.offset.reset": "earliest",
                "td.connect.ip": "localhost",
                "td.connect.port": "6030",
                "fetch.max.wait.ms": "1000",
            }
        )

        consumer.subscribe([topic_name])

        tdLog.info(f"Start consuming data from topic {topic_name} with consumer group {consumer_group}")
        while self.consume_data:
            message = consumer.poll(10)
            if message is None:
                tdLog.info(f"No message consumed from {topic_name}, stop consume")
                break
            err = message.error()
            if err is not None:
                tdLog.info(f"stop since get error {err}")
                break

            if force_drop:
                tdLog.info(f"Force dropping topic {topic_name}")
                tdSql.execute(f"drop topic force {topic_name} ")
                break

        consumer.close()

    def check_result(self):
        """Check the result of the test."""
        tdLog.info(f"Checking the result of the test")
        # Check if the topic is dropped
        tdSql.query(f"show topics")
        tdSql.checkRows(1)

        # Check if the consumer is still active
        tdSql.query(f"show subscriptions")
        tdSql.checkAssert(tdSql.getData(0, 1) == "test_consumer_group_normal")
        tdSql.checkAssert(tdSql.getData(0, 3) is None)

    def init_data(self):
        """Initialize the data for the test."""
        self.insert_data = True
        self.consume_data = True
        self.db = "test_tmq_drop_db"
        self.super_table = "super_t"
        self.sub_table = "sub_t0"
        self.topic = "topic_tmq"
        self.drop_topic = "topic_tmq_drop"
        self.consumer_group_normal = "test_consumer_group_normal"
        self.consumer_group_drop = "test_consumer_group_drop"

        self.consume_thread_normal1 = threading.Thread(
            target=self.consume_topic, args=(self.topic, self.consumer_group_normal, "normal_client1", False)
        )

        self.consume_drop_thread1 = threading.Thread(
            target=self.consume_topic, args=(self.drop_topic, self.consumer_group_drop, "drop_client1", True)
        )