#! encoding = utf-8

# Copyright (c) 2023 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import json
import random
import threading
from concurrent.futures import ThreadPoolExecutor, Future
from datetime import datetime

from kafka import KafkaProducer

locations = ['California.SanFrancisco', 'California.LosAngles', 'California.SanDiego', 'California.SanJose',
             'California.PaloAlto', 'California.Campbell', 'California.MountainView', 'California.Sunnyvale',
             'California.SantaClara', 'California.Cupertino']

producers: list[KafkaProducer] = []

lock = threading.Lock()
start = 1640966400


def produce_total(workers, broker, topic, message_type, total, table_count):
    if len(producers) == 0:
        lock.acquire()
        if len(producers) == 0:
            _init_kafka_producers(broker=broker, count=10)
        lock.release()
    pool = ThreadPoolExecutor(max_workers=workers)
    futures = []
    for _ in range(0, workers):
        futures.append(pool.submit(_produce_total, topic, message_type, int(total / workers), table_count))
    pool.shutdown()
    for f in futures:
        f.result()
    _close_kafka_producers()


def _produce_total(topic, message_type, total, table_count):
    producer = _get_kafka_producer()
    for _ in range(total):
        message = _get_fake_date(message_type=message_type, table_count=table_count)
        producer.send(topic=topic, value=message.encode(encoding='utf-8'))


def _init_kafka_producers(broker, count):
    for _ in range(count):
        p = KafkaProducer(bootstrap_servers=broker, batch_size=64 * 1024, linger_ms=300, acks=0)
        producers.append(p)


def _close_kafka_producers():
    for p in producers:
        p.close()


def _get_kafka_producer():
    return producers[random.randint(0, len(producers) - 1)]


def _get_fake_date(table_count, message_type='json'):
    if message_type == 'json':
        return _get_json_message(table_count=table_count)
    if message_type == 'line':
        return _get_line_message(table_count=table_count)
    return ''


def _get_json_message(table_count):
    return json.dumps({
        'ts': _get_timestamp(),
        'current': random.randint(0, 1000) / 100,
        'voltage': random.randint(105, 115),
        'phase': random.randint(0, 32000) / 100000,
        'location': random.choice(locations),
        'groupId': random.randint(1, 10),
        'table_name': _random_table_name(table_count)
    })


def _get_line_message(table_count):
    return "{} values('{}', {}, {}, {})".format(
        _random_table_name(table_count),  # table
        _get_timestamp(),  # ts
        random.randint(0, 1000) / 100,  # current
        random.randint(105, 115),  # voltage
        random.randint(0, 32000) / 100000,  # phase
    )


def _random_table_name(table_count):
    return 'd{}'.format(random.randint(0, table_count - 1))


def _get_timestamp():
    global start
    lock.acquire(blocking=True)
    start += 0.001
    lock.release()
    return datetime.fromtimestamp(start).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
