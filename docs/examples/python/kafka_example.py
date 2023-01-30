#! encoding = utf-8
import json
import time
from json import JSONDecodeError
from typing import Callable
import logging
from concurrent.futures import ThreadPoolExecutor, Future

import taos
from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord


class Consumer(object):
    DEFAULT_CONFIGS = {
        'kafka_brokers': 'localhost:9092',
        'kafka_topic': 'python_kafka',
        'kafka_group_id': 'taos',
        'taos_host': 'localhost',
        'taos_user': 'root',
        'taos_password': 'taosdata',
        'taos_database': 'power',
        'taos_port': 6030,
        'timezone': None,
        'clean_after_testing': False,
        'bath_consume': True,
        'batch_size': 1000,
        'async_model': True,
        'workers': 10,
        'testing': False
    }

    LOCATIONS = ['California.SanFrancisco', 'California.LosAngles', 'California.SanDiego', 'California.SanJose',
                 'California.PaloAlto', 'California.Campbell', 'California.MountainView', 'California.Sunnyvale',
                 'California.SantaClara', 'California.Cupertino']

    CREATE_DATABASE_SQL = 'create database if not exists {} keep 365 duration 10 buffer 16 wal_level 1'
    USE_DATABASE_SQL = 'use {}'
    DROP_TABLE_SQL = 'drop table if exists meters'
    DROP_DATABASE_SQL = 'drop database if exists {}'
    CREATE_STABLE_SQL = 'create stable meters (ts timestamp, current float, voltage int, phase float) ' \
                        'tags (location binary(64), groupId int)'
    CREATE_TABLE_SQL = 'create table if not exists {} using meters tags (\'{}\', {})'
    INSERT_SQL_HEADER = "insert into "
    INSERT_PART_SQL = 'power.{} values (\'{}\', {}, {}, {})'

    def __init__(self, **configs):
        self.config: dict = self.DEFAULT_CONFIGS
        self.config.update(configs)
        if not self.config.get('testing'):
            self.consumer = KafkaConsumer(
                self.config.get('kafka_topic'),  # topic
                bootstrap_servers=self.config.get('kafka_brokers'),
                group_id=self.config.get('kafka_group_id'),
            )
        self.taos = taos.connect(
            host=self.config.get('taos_host'),
            user=self.config.get('taos_user'),
            password=self.config.get('taos_password'),
            port=self.config.get('taos_port'),
            timezone=self.config.get('timezone'),
        )
        if self.config.get('async_model'):
            self.pool = ThreadPoolExecutor(max_workers=self.config.get('workers'))
            self.tasks = []
        # tags and table mapping # key: {location}_{groupId} value:
        self.tag_table_mapping = {}
        i = 0
        for location in self.LOCATIONS:
            for j in range(1, 11):
                table_name = 'd{}'.format(i)
                self._cache_table(location=location, group_id=j, table_name=table_name)
                i += 1

    def init_env(self):
        # create database and table
        self.taos.execute(self.DROP_DATABASE_SQL.format(self.config.get('taos_database')))
        self.taos.execute(self.CREATE_DATABASE_SQL.format(self.config.get('taos_database')))
        self.taos.execute(self.USE_DATABASE_SQL.format(self.config.get('taos_database')))
        self.taos.execute(self.DROP_TABLE_SQL)
        self.taos.execute(self.CREATE_STABLE_SQL)
        for tags, table_name in self.tag_table_mapping.items():
            location, group_id = _get_location_and_group(tags)
            self.taos.execute(self.CREATE_TABLE_SQL.format(table_name, location, group_id))

    def consume(self):
        logging.warning('## start consumer topic-[%s]', self.config.get('kafka_topic'))
        try:
            if self.config.get('bath_consume'):
                self._run_batch(self._to_taos_batch)
            else:
                self._run(self._to_taos)
        except KeyboardInterrupt:
            logging.warning("## caught keyboard interrupt, stopping")
        finally:
            self.stop()

    def stop(self):
        # close consumer
        if self.consumer is not None:
            self.consumer.commit()
            self.consumer.close()

        # multi thread
        if self.config.get('async_model'):
            for task in self.tasks:
                while not task.done():
                    pass
                if self.pool is not None:
                    self.pool.shutdown()

        # clean data
        if self.config.get('clean_after_testing'):
            self.taos.execute(self.DROP_TABLE_SQL)
            self.taos.execute(self.DROP_DATABASE_SQL.format(self.config.get('taos_database')))
        # close taos
        if self.taos is not None:
            self.taos.close()

    def _run(self, f):
        for message in self.consumer:
            if self.config.get('async_model'):
                self.pool.submit(f(message))
            else:
                f(message)

    def _run_batch(self, f):
        while True:
            messages = self.consumer.poll(timeout_ms=500, max_records=self.config.get('batch_size'))
            if messages:
                if self.config.get('async_model'):
                    self.pool.submit(f, messages.values())
                else:
                    f(list(messages.values()))
            if not messages:
                time.sleep(0.1)

    def _to_taos(self, message: ConsumerRecord) -> bool:
        sql = self.INSERT_SQL_HEADER + self._build_sql(message.value)
        if len(sql) == 0:  # decode error, skip
            return True
        logging.info('## insert sql %s', sql)
        return self.taos.execute(sql=sql) == 1

    def _to_taos_batch(self, messages):
        sql = self._build_sql_batch(messages=messages)
        if len(sql) == 0:  # decode error, skip
            return
        self.taos.execute(sql=sql)

    def _build_sql(self, msg_value: str) -> str:
        try:
            data = json.loads(msg_value)
        except JSONDecodeError as e:
            logging.error('## decode message [%s] error ', msg_value, e)
            return ''
        location = data.get('location')
        group_id = data.get('groupId')
        ts = data.get('ts')
        current = data.get('current')
        voltage = data.get('voltage')
        phase = data.get('phase')

        table_name = self._get_table_name(location=location, group_id=group_id)
        return self.INSERT_PART_SQL.format(table_name, ts, current, voltage, phase)

    def _build_sql_batch(self, messages) -> str:
        sql_list = []
        for partition_messages in messages:
            for message in partition_messages:
                sql_list.append(self._build_sql(message.value))

        return self.INSERT_SQL_HEADER + ' '.join(sql_list)

    def _cache_table(self, location: str, group_id: int, table_name: str):
        self.tag_table_mapping[_tag_table_mapping_key(location=location, group_id=group_id)] = table_name

    def _get_table_name(self, location: str, group_id: int) -> str:
        return self.tag_table_mapping.get(_tag_table_mapping_key(location=location, group_id=group_id))


def _tag_table_mapping_key(location: str, group_id: int):
    return '{}_{}'.format(location, group_id)


def _get_location_and_group(key: str) -> (str, int):
    fields = key.split('_')
    return fields[0], fields[1]


def test_to_taos(consumer: Consumer):
    msg = {
        'location': 'California.SanFrancisco',
        'groupId': 1,
        'ts': '2022-12-06 15:13:38.643',
        'current': 3.41,
        'voltage': 105,
        'phase': 0.02027,
    }
    record = ConsumerRecord(checksum=None, headers=None, offset=1, key=None, value=json.dumps(msg), partition=1,
                            topic='test', serialized_key_size=None, serialized_header_size=None,
                            serialized_value_size=None, timestamp=time.time(), timestamp_type=None)
    assert consumer._to_taos(message=record)


def test_to_taos_batch(consumer: Consumer):
    records = [
        [
            ConsumerRecord(checksum=None, headers=None, offset=1, key=None,
                           value=json.dumps({'location': 'California.SanFrancisco',
                                             'groupId': 1,
                                             'ts': '2022-12-06 15:13:38.643',
                                             'current': 3.41,
                                             'voltage': 105,
                                             'phase': 0.02027, }),
                           partition=1, topic='test', serialized_key_size=None, serialized_header_size=None,
                           serialized_value_size=None, timestamp=time.time(), timestamp_type=None),
            ConsumerRecord(checksum=None, headers=None, offset=1, key=None,
                           value=json.dumps({'location': 'California.LosAngles',
                                             'groupId': 2,
                                             'ts': '2022-12-06 15:13:39.643',
                                             'current': 3.41,
                                             'voltage': 102,
                                             'phase': 0.02027, }),
                           partition=1, topic='test', serialized_key_size=None, serialized_header_size=None,
                           serialized_value_size=None, timestamp=time.time(), timestamp_type=None),
        ]
    ]

    consumer._to_taos_batch(messages=records)


if __name__ == '__main__':
    consumer = Consumer(async_model=True, testing=True)
    # init env
    consumer.init_env()
    # consumer.consume()
    # test build sql
    # test build sql batch
    test_to_taos(consumer)
    test_to_taos_batch(consumer)
    