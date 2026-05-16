###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

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
from datetime import date
from datetime import datetime
from datetime import timedelta
from os       import path


# consume topic 
def consume_topic(topic_name, consume_cnt, wait):
    print("start consume...")
    consumer = Consumer(
        {
            "group.id": "tg2",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "enable.auto.commit": "true",
        }
    )
    print("start subscrite...")
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
                print(block.fetchall())
    finally:
        consumer.unsubscribe()
        consumer.close()


if __name__ == "__main__":
    print(sys.argv)
    if len(sys.argv) < 2:

        print(" please input topic name for consume . -c for wait")
    else:
        wait = False
        if "-c" == sys.argv[1]:
           wait = True
           topic = sys.argv[2]
        else:
           topic = sys.argv[1]

        print(f' wait={wait} topic={topic}')        
        consume_topic(topic, 10000000, wait)