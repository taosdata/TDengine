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

import requests
import threading
import random
import time

class RestfulInsert:
    def init(self):
        self.header = {'Authorization': 'Basic cm9vdDp0YW9zZGF0YQ=='}
        self.url = "http://127.0.0.1:6041/rest/sql"
        self.ts = 1500000000000
        self.numOfThreads = 20
        self.numOfTables = 10000
        self.recordsPerTable = 10000
        self.batchSize = 1000
        self.tableNamePerfix = 't'
    
    def createTable(self, threadID):
        tablesPerThread = int (self.numOfTables / self.numOfThreads)
        print("create table %d to %d" % (tablesPerThread * threadID, tablesPerThread * (threadID + 1) - 1))
        for i in range(tablesPerThread):
            tableID = threadID * tablesPerThread
            name = 'beijing' if tableID % 2 == 0 else 'shanghai'
            data = "create table test.%s%d using test.meters tags(%d, '%s')" % (self.tableNamePerfix, tableID + i, tableID + i, name)            
            requests.post(self.url, data, headers = self.header)

    def insertData(self, threadID):        
        print("thread %d started" % threadID)
        tablesPerThread = int (self.numOfTables / self.numOfThreads)        
        for i in range(tablesPerThread):
            tableID = i + threadID * tablesPerThread
            start = self.ts
            for j in range(int(self.recordsPerTable / self.batchSize)):
                data = "insert into test.%s%d values" % (self.tableNamePerfix, tableID)
                for k in range(self.batchSize):
                    data += "(%d, %d, %d, %d)" %  (start + j * self.batchSize + k, random.randint(1, 100), random.randint(1, 100), random.randint(1, 100))                
                requests.post(self.url, data, headers = self.header)

    def run(self):
        data = "drop database if exists test"
        requests.post(self.url, data, headers = self.header)
        data = "create database test"
        requests.post(self.url, data, headers = self.header)
        data = "create table test.meters(ts timestamp, f1 int, f2 int, f3 int) tags(id int, loc nchar(20))"
        requests.post(self.url, data, headers = self.header)

        threads = []
        startTime = time.time()    
        for i in range(self.numOfThreads):
            thread = threading.Thread(target=self.createTable, args=(i,))
            thread.start()
            threads.append(thread)
        for i in range(self.numOfThreads):
            threads[i].join()
        print("createing %d tables takes %d seconds" % (self.numOfTables, (time.time() - startTime)))

        print("inserting data =======")
        threads = []
        startTime = time.time()
        for i in range(self.numOfThreads):
            thread = threading.Thread(target=self.insertData, args=(i,))
            thread.start()
            threads.append(thread)
        
        for i in range(self.numOfThreads):
            threads[i].join()
        print("inserting %d records takes %d seconds" % (self.numOfTables * self.recordsPerTable, (time.time() - startTime)))

ri = RestfulInsert()
ri.init()
ri.run()