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

import sys
import requests, json
import threading
import string
import random

class RestfulInsert:
    def init(self):
        self.header = {'Authorization': 'Basic cm9vdDp0YW9zZGF0YQ=='}
        self.url = "http://127.0.0.1:6041/rest/sql"
        self.ts = 1104508800000
        self.numOfThreads = 50

    def get_random_string(self, length):
        letters = string.ascii_lowercase
        result_str = ''.join(random.choice(letters) for i in range(length))
        return result_str

    def insertData(self, threadID):
        print("thread %d started" % threadID)
        data = "create table test.tb%d(ts timestamp, name nchar(20))" % threadID
        requests.post(self.url, data, headers = self.header)              
        name = self.get_random_string(10)
        start = self.ts
        while True:
            start += 1
            data = "insert into test.tb%d values(%d, '%s')" % (threadID, start, name)
            requests.post(self.url, data, headers = self.header)

    def run(self):
        data = "drop database if exists test"
        requests.post(self.url, data, headers = self.header)
        data = "create database test keep 7300"
        requests.post(self.url, data, headers = self.header)

        threads = []        
        for i in range(self.numOfThreads):
            thread = threading.Thread(target=self.insertData, args=(i,))
            thread.start()
            threads.append(thread)            
        
        for i in range(self.numOfThreads):
            threads[i].join()

ri = RestfulInsert()
ri.init()
ri.run()