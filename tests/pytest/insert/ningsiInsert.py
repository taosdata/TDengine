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
import time

class RestfulInsert:
    def init(self):
        self.header = {'Authorization': 'Basic cm9vdDp0YW9zZGF0YQ=='}
        self.url = "http://ningsi60:6041/rest/sql"
        self.ts = 1104508800000
        self.numOfThreads = 10
        self.numOfTables = 3000
        self.dbName = 'netmonitortaos'
        self.stbName = 'devinfomt'
        self.prefix = 'dev'

    def get_random_string(self, length):
        letters = string.ascii_lowercase
        result_str = ''.join(random.choice(letters) for i in range(length))
        return result_str
    
    def createTables(self, threadID):
        print("create table: thread %d started" % threadID)
        tablesPerThread = int (self.numOfTables / self.numOfThreads)
        for i in range(tablesPerThread):
            data = "create table '%s'.dev_%d using '%s'.'%s' tags('%s', '%s')" % (self.dbName, i + threadID * tablesPerThread, self.dbName, self.stbName, self.get_random_string(25), self.get_random_string(25))
            response = requests.post(self.url, data, headers = self.header)
            if response.status_code != 200:
                print(response.content)

    def insertData(self, threadID):
        print("insert data: thread %d started" % threadID)
        tablesPerThread = int (self.numOfTables / self.numOfThreads)
        base_ts = self.ts
        while True:
            i = 0
            for i in range(tablesPerThread):        
                data = "insert into %s.dev_%d values(%d, '%s', '%s', %d, %d, %d)" % (self.dbName, i + threadID * tablesPerThread, base_ts, self.get_random_string(25), self.get_random_string(30), random.randint(1, 10000), random.randint(1, 10000), random.randint(1, 10000))                
                response = requests.post(self.url, data, headers = self.header)
                if response.status_code != 200:
                    print(response.content)

            time.sleep(30)
            base_ts = base_ts + 1

    def run(self):    
        data = "create database if not exists %s keep 7300" % self.dbName
        requests.post(self.url, data, headers = self.header)

        data = "create table '%s'.'%s' (timeid  timestamp, devdesc binary(50), devname binary(50), cpu bigint, temp bigint, ram bigint) tags(devid binary(50), modelid binary(50))" % (self.dbName, self.stbName)
        requests.post(self.url, data, headers = self.header)

        threads = []        
        for i in range(self.numOfThreads):
            thread = threading.Thread(target=self.createTables, args=(i,))
            thread.start()
            threads.append(thread)            
        
        for i in range(self.numOfThreads):
            threads[i].join()

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
