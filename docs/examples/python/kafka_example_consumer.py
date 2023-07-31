#! encoding = utf-8
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, Future
from json import JSONDecodeError
from typing import Callable

import taos
from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord

import kafka_example_common as common


class Consumer(object):
    DEFAULT_CONFIGS = {
        'kafka_brokers': 'localhost:9092',  # kafka broker
        'kafka_topic': 'tdengine_kafka_practices',
        'kafka_group_id': 'taos',
        'taos_host': 'localhost',  # TDengine host
        'taos_port': 6030,  # TDengine port
        'taos_user': 'root',  # TDengine user name
        'taos_password': 'taosdata',  # TDengine password
        'taos_database': 'power',  # TDengine database
        'message_type': 'json',  # message format, 'json' or 'line'
        'clean_after_testing': False,  # if drop database after testing
        'max_poll': 1000,  # poll size for batch mode
        'workers': 10,  # thread count for multi-threading
        'testing': False
    }

    INSERT_SQL_HEADER = "insert into "
    INSERT_PART_SQL = '{} values (\'{}\', {}, {}, {})'

    def __init__(self, **configs):
        self.config = self.DEFAULT_CONFIGS
        self.config.update(configs)

        self.consumer = None
        if not self.config.get('testing'):
            self.consumer = KafkaConsumer(
                self.config.get('kafka_topic'),
                bootstrap_servers=self.config.get('kafka_brokers'),
                group_id=self.config.get('kafka_group_id'),
            )

        self.conns = taos.connect(
            host=self.config.get('taos_host'),
            port=self.config.get('taos_port'),
            user=self.config.get('taos_user'),
            password=self.config.get('taos_password'),
            db=self.config.get('taos_database'),
        )
        if self.config.get('workers') > 1:
            self.pool = ThreadPoolExecutor(max_workers=self.config.get('workers'))
            self.tasks = []
        # tags and table mapping # key: {location}_{groupId} value:

    def consume(self):
        """

        consume data from kafka and deal. Base on `message_type`, `bath_consume`, `insert_by_table`,
        there are several deal function.
        :return:
        """
        self.conns.execute(common.USE_DATABASE_SQL.format(self.config.get('taos_database')))
        try:
            if self.config.get('message_type') == 'line':  # line
                self._run(self._line_to_taos)
            if self.config.get('message_type') == 'json':  # json
                self._run(self._json_to_taos)
        except KeyboardInterrupt:
            logging.warning("## caught keyboard interrupt, stopping")
        finally:
            self.stop()

    def stop(self):
        """

        stop consuming
        :return:
        """
        # close consumer
        if self.consumer is not None:
            self.consumer.commit()
            self.consumer.close()

        # multi thread
        if self.config.get('workers') > 1:
            if self.pool is not None:
                self.pool.shutdown()
            for task in self.tasks:
                while not task.done():
                    time.sleep(0.01)

        # clean data
        if self.config.get('clean_after_testing'):
            self.conns.execute(common.DROP_TABLE_SQL)
            self.conns.execute(common.DROP_DATABASE_SQL.format(self.config.get('taos_database')))
        # close taos
        if self.conns is not None:
            self.conns.close()

    def _run(self, f):
        """

        run in batch consuming mode
        :param f:
        :return:
        """
        i = 0  # just for test.
        while True:
            messages = self.consumer.poll(timeout_ms=100, max_records=self.config.get('max_poll'))
            if messages:
                if self.config.get('workers') > 1:
                    self.pool.submit(f, messages.values())
                else:
                    f(list(messages.values()))
            if not messages:
                i += 1  # just for test.
                time.sleep(0.1)
            if i > 3:  # just for test.
                logging.warning('## test over.')  # just for test.
                return  # just for test.

    def _json_to_taos(self, messages):
        """

        convert a batch of json data to sql, and insert into TDengine
        :param messages:
        :return:
        """
        sql = self._build_sql_from_json(messages=messages)
        self.conns.execute(sql=sql)

    def _line_to_taos(self, messages):
        """

        convert a batch of lines data to sql, and insert into TDengine
        :param messages:
        :return:
        """
        lines = []
        for partition_messages in messages:
            for message in partition_messages:
                lines.append(message.value.decode())
        sql = self.INSERT_SQL_HEADER + ' '.join(lines)
        self.conns.execute(sql=sql)

    def _build_single_sql_from_json(self, msg_value):
        try:
            data = json.loads(msg_value)
        except JSONDecodeError as e:
            logging.error('## decode message [%s] error ', msg_value, e)
            return ''
        # location = data.get('location')
        # group_id = data.get('groupId')
        ts = data.get('ts')
        current = data.get('current')
        voltage = data.get('voltage')
        phase = data.get('phase')
        table_name = data.get('table_name')

        return self.INSERT_PART_SQL.format(table_name, ts, current, voltage, phase)

    def _build_sql_from_json(self, messages):
        sql_list = []
        for partition_messages in messages:
            for message in partition_messages:
                sql_list.append(self._build_single_sql_from_json(message.value))
        return self.INSERT_SQL_HEADER + ' '.join(sql_list)


def test_json_to_taos(consumer: Consumer):
    records = [
        [
            ConsumerRecord(checksum=None, headers=None, offset=1, key=None,
                           value=json.dumps({'table_name': 'd0',
                                             'ts': '2022-12-06 15:13:38.643',
                                             'current': 3.41,
                                             'voltage': 105,
                                             'phase': 0.02027, }),
                           partition=1, topic='test', serialized_key_size=None, serialized_header_size=None,
                           serialized_value_size=None, timestamp=time.time(), timestamp_type=None),
            ConsumerRecord(checksum=None, headers=None, offset=1, key=None,
                           value=json.dumps({'table_name': 'd1',
                                             'ts': '2022-12-06 15:13:39.643',
                                             'current': 3.41,
                                             'voltage': 102,
                                             'phase': 0.02027, }),
                           partition=1, topic='test', serialized_key_size=None, serialized_header_size=None,
                           serialized_value_size=None, timestamp=time.time(), timestamp_type=None),
        ]
    ]

    consumer._json_to_taos(messages=records)


def test_line_to_taos(consumer: Consumer):
    records = [
        [
            ConsumerRecord(checksum=None, headers=None, offset=1, key=None,
                           value="d0 values('2023-01-01 00:00:00.001', 3.49, 109, 0.02737)".encode('utf-8'),
                           partition=1, topic='test', serialized_key_size=None, serialized_header_size=None,
                           serialized_value_size=None, timestamp=time.time(), timestamp_type=None),
            ConsumerRecord(checksum=None, headers=None, offset=1, key=None,
                           value="d1 values('2023-01-01 00:00:00.002', 6.19, 112, 0.09171)".encode('utf-8'),
                           partition=1, topic='test', serialized_key_size=None, serialized_header_size=None,
                           serialized_value_size=None, timestamp=time.time(), timestamp_type=None),
        ]
    ]
    consumer._line_to_taos(messages=records)


def consume(kafka_brokers, kafka_topic, kafka_group_id, taos_host, taos_port, taos_user,
            taos_password, taos_database, message_type, max_poll, workers):
    c = Consumer(kafka_brokers=kafka_brokers, kafka_topic=kafka_topic, kafka_group_id=kafka_group_id,
                 taos_host=taos_host, taos_port=taos_port, taos_user=taos_user, taos_password=taos_password,
                 taos_database=taos_database, message_type=message_type, max_poll=max_poll, workers=workers)
    c.consume()


if __name__ == '__main__':
    consumer = Consumer(testing=True)
    common.create_database_and_tables(host='localhost', port=6030, user='root', password='taosdata', db='py_kafka_test',
                                      table_count=10)
    consumer.conns.execute(common.USE_DATABASE_SQL.format('py_kafka_test'))
    test_json_to_taos(consumer)
    test_line_to_taos(consumer)
    common.clean(host='localhost', port=6030, user='root', password='taosdata', db='py_kafka_test')
