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
import argparse

class RestfulInsert:
    def __init__(self, host, startTimestamp, dbname, threads, tables, records, batchSize, tbNamePerfix, outOfOrder,tablePerbatch):
        self.header = {'Authorization': 'Basic cm9vdDp0YW9zZGF0YQ=='}
        self.url = "http://%s:6041/rest/sql" % host
        self.ts = startTimestamp
        self.dbname = dbname
        self.numOfThreads = threads
        self.numOfTables = tables
        self.recordsPerTable = records
        self.batchSize = batchSize
        self.tableNamePerfix = tbNamePerfix
        self.outOfOrder = outOfOrder
        self.tablePerbatch = tablePerbatch
    
    def createTable(self, threadID):
        tablesPerThread = int (self.numOfTables / self.numOfThreads)
        loop = tablesPerThread if threadID != self.numOfThreads - 1 else self.numOfTables - tablesPerThread * threadID        
        print("create table %d to %d" % (tablesPerThread * threadID, tablesPerThread * threadID + loop - 1))
        for i in range(loop):
            tableID = threadID * tablesPerThread
            if tableID + i >= self.numOfTables : break
            name = 'beijing' if (tableID + i) % 2 == 0 else 'shanghai'
            data = "create table if not exists %s.%s%d using %s.meters tags(%d, '%s')" % (self.dbname, self.tableNamePerfix, tableID + i, self.dbname, tableID + i, name)
            try:
                response = requests.post(self.url, data, headers = self.header)
                if response.status_code != 200:
                    print(response.content)                
            except Exception as e:
                print(e)

    def insertData(self, threadID):        
        print("thread %d started" % threadID)
        tablesPerThread = int (self.numOfTables / self.numOfThreads)   
        loop = int(self.recordsPerTable / self.batchSize)   
        if self.tablePerbatch == 1 : 
            for i in range(tablesPerThread+1):            
                tableID = i + threadID * tablesPerThread
                if tableID >= self.numOfTables: return
                start = self.ts
                start1=time.time()
                for k in range(loop):
                    data = "insert into %s.%s%d values" % (self.dbname, self.tableNamePerfix, tableID)
                    values = []
                    bloop = self.batchSize if k != loop - 1 else self.recordsPerTable - self.batchSize * k
                    for l in range(bloop):
                        values.append("(%d, %d, %d, %d)" %  (start + k * self.batchSize + l, random.randint(1, 100), random.randint(1, 100), random.randint(1, 100)))                              
                    if len(data) > 1048576 : 
                        print ('batch size is larger than 1M')
                        exit(-1)
                    if self.outOfOrder :
                        random.shuffle(values)
                    data+=''.join(values)
                    response = requests.post(self.url, data, headers = self.header)
                    if response.status_code != 200:
                        print(response.content)
        else:
            for i in range(0,tablesPerThread+self.tablePerbatch,self.tablePerbatch): 
                for k in range(loop):
                    data = "insert into "
                    for j in range(self.tablePerbatch):
                        tableID = i + threadID * tablesPerThread+j
                        if tableID >= self.numOfTables: return
                        start = self.ts
                        data += "%s.%s%d values" % (self.dbname, self.tableNamePerfix, tableID)
                        values = []
                        bloop = self.batchSize if k != loop - 1 else self.recordsPerTable - self.batchSize * k
                        for l in range(bloop):
                            values.append("(%d, %d, %d, %d)" %  (start + k * self.batchSize + l, random.randint(1, 100), random.randint(1, 100), random.randint(1, 100)))    
                        if self.outOfOrder :
                            random.shuffle(values)
                        data+=''.join(values)                          
                    if len(data) > 1024*1024 : 
                        print ('batch size is larger than 1M')
                        exit(-1)
                    try:
                        startTime = time.time()
                        response = requests.post(self.url, data, headers = self.header)
                        endTime = time.time()
                        if response.status_code != 200:
                            print(response.content)
                        else:
                            print("inserted %d records, %d seconds" % (bloop, endTime - startTime))
                    except Exception as e:
                        print(e)

    def insertUnlimitedData(self, threadID):        
        print("thread %d started" % threadID)
        tablesPerThread = int (self.numOfTables / self.numOfThreads)
        
        count = 0
        while True:
            i = 0
            start = self.ts  + count * self.batchSize          
            count = count + 1            
            
            for i in range(tablesPerThread):
                tableID = i + threadID * tablesPerThread
                                
                data = "insert into %s.%s%d values" % (self.dbname, self.tableNamePerfix, tableID)
                values = []
                for k in range(self.batchSize):
                    values.append("(%d, %d, %d, %d)" %  (start + k, random.randint(1, 100), random.randint(1, 100), random.randint(1, 100)))
                
                if(self.outOfOrder == False):
                    for k in range(len(values)):            
                        data += values[k]
                else:
                    random.shuffle(values)
                    for k in range(len(values)):            
                        data += values[k]    
                try:     
                    startTime = time.time()
                    response = requests.post(self.url, data, headers = self.header)
                    endTime = time.time()
                    if response.status_code != 200:
                        print(response.content)
                    else:
                        print("inserted %d records, %d seconds" % (self.batchSize, endTime - startTime))
                except Exception as e:
                    print(e)                    

    def run(self):            
        data = "create database if not exists %s" % self.dbname
        requests.post(self.url, data, headers = self.header)
        data = "create table if not exists %s.meters(ts timestamp, f1 int, f2 int, f3 int) tags(id int, loc nchar(20))" % self.dbname
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
            if(self.recordsPerTable != -1):          
                thread = threading.Thread(target=self.insertData, args=(i,))
            else:
                thread = threading.Thread(target=self.insertUnlimitedData, args=(i,))
            thread.start()
            threads.append(thread)
        
        for i in range(self.numOfThreads):
            threads[i].join()
        print("inserting %s records takes %d seconds" % (self.numOfTables * self.recordsPerTable, (time.time() - startTime)))

parser = argparse.ArgumentParser()
parser.add_argument(
    '-H',
    '--host-name',
    action='store',
    default='127.0.0.1',
    type=str,
    help='host name to be connected (default: 127.0.0.1)')
parser.add_argument(
    '-S',
    '--start-timestamp',
    action='store',
    default=1500000000000,
    type=int,
    help='insert data from timestamp (default: 1500000000000)')
parser.add_argument(
    '-d',
    '--db-name',
    action='store',
    default='test',
    type=str,
    help='Database name to be created (default: test)')
parser.add_argument(
    '-t',
    '--number-of-threads',
    action='store',
    default=10,
    type=int,
    help='Number of threads to create tables and insert datas (default: 10)')
parser.add_argument(
    '-T',
    '--number-of-tables',
    action='store',
    default=10000,
    type=int,
    help='Number of tables to be created (default: 1000)')
parser.add_argument(
    '-r',
    '--number-of-records',
    action='store',
    default=10000,
    type=int,
    help='Number of record to be created for each table  (default: 1000, -1 for unlimited records)')
parser.add_argument(
    '-s',
    '--batch-size',
    action='store',
    default='1000',
    type=int,
    help='Number of tables to be created (default: 1000)')
parser.add_argument(
    '-p',
    '--table-name-prefix',
    action='store',
    default='t',
    type=str,
    help='Number of tables to be created (default: 1000)')
parser.add_argument(
    '-o',
    '--out-of-order',
    action='store_true', 
    help='The order of test data (default: False)')
parser.add_argument(
    '-b',
    '--table-per-batch',
    action='store', 
    default=1,
    type=int,
    help='the table per batch (default: 1)')



args = parser.parse_args()
ri = RestfulInsert(
        args.host_name, args.start_timestamp, args.db_name, args.number_of_threads, args.number_of_tables, 
        args.number_of_records, args.batch_size, args.table_name_prefix, args.out_of_order, args.table_per_batch)
ri.run()