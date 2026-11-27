# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
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

# -*- coding: utf-8 -*-

#
#  The option for wal_retetion_period and wal_retention_size is work well
#

import taos
from taos.tmq import Consumer

import os
import sys
import threading
import json
import time
import random
from datetime import date
from datetime import datetime
from datetime import timedelta
from os       import path


topicName = "topic"
topicNum  = 100

# consume topic 
def consume_topic(topic_name, group,consume_cnt, index, wait):
    consumer = Consumer(
        {
            "group.id": group,
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "enable.auto.commit": "true",
        }
    )
    
    print(f"start consumer topic:{topic_name} group={group} index={index} ...")
    consumer.subscribe([topic_name])
    cnt = 0
    try:
        while True and cnt < consume_cnt:
            res = consumer.poll(1)
            if not res:
                if wait:
                    continue
                else:
                    break
            err = res.error()
            if err is not None:
                raise err
            val = res.value()
            cnt += 1
            print(f" consume {cnt} ")
            for block in val:
                datas = block.fetchall()
                data = datas[0][:50]

                print(f"  {topic_name}_{group}_{index} {cnt} {data}")

    finally:
        consumer.unsubscribe()
        consumer.close()

def consumerThread(index):
    global topicName, topicNum
    print(f' thread {index} start...')
    while True:
        idx = random.randint(0, topicNum - 1)
        name = f"{topicName}{idx}"
        group = f"group_{index}_{idx}"
        consume_topic(name, group, 100, index, True)
    


if __name__ == "__main__":
    print(sys.argv)
    threadCnt = 10

    if len(sys.argv) == 1:
       threadCnt = int(sys.argv[1])

    
    threads = []
    print(f'consumer with {threadCnt} threads...')
    for i in range(threadCnt):
        x = threading.Thread(target=consumerThread, args=(i,))
        x.start()
        threads.append(x)

    # wait
    for i, thread in enumerate(threads):
        thread.join()
        print(f'join thread {i} end.')

