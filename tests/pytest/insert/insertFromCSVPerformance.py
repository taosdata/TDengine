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
import taos
import time
import datetime
import csv
import random
import pandas as pd
import argparse
import os.path

class insertFromCSVPerformace:
    def __init__(self, commitID, dbName, tbName, branchName, buildType):
        self.commitID = commitID
        self.dbName = dbName
        self.tbName = tbName
        self.branchName = branchName
        self.type = buildType
        self.ts = 1500000000000
        self.host = "127.0.0.1"
        self.user = "root"
        self.password = "taosdata"
        self.config = "/etc/perf"
        self.conn = taos.connect(
            self.host,
            self.user,
            self.password,            
            self.config)
        self.host2 = "192.168.1.179"    
        self.conn2 = taos.connect(
            host = self.host2,
            user = self.user,
            password = self.password,
            config = self.config)

    def writeCSV(self):
        tsset = set()
        rows = 0
        with open('test4.csv','w', encoding='utf-8', newline='') as csvFile:
            writer = csv.writer(csvFile, dialect='excel')
            while True:
                newTimestamp = self.ts + random.randint(1, 10) * 10000000000 + random.randint(1, 10) * 1000000000 + random.randint(1, 10) * 100000000 + random.randint(1, 10) * 10000000 + random.randint(1, 10) * 1000000 + random.randint(1, 10) * 100000 + random.randint(1, 10) * 10000 + random.randint(1, 10) * 1000 + random.randint(1, 10) * 100 + random.randint(1, 10) * 10 + random.randint(1, 10)
                if newTimestamp not in tsset:
                    tsset.add(newTimestamp)
                    d = datetime.datetime.fromtimestamp(newTimestamp / 1000)
                    dt = str(d.strftime("%Y-%m-%d %H:%M:%S.%f"))
                    writer.writerow(["'%s'" % dt, random.randint(1, 100), random.uniform(1, 100), random.randint(1, 100), random.randint(1, 100)])
                    rows += 1
                    if rows == 2000000:
                        break
    
    def removCSVHeader(self):
        data = pd.read_csv("ordered.csv")
        data = data.drop([0])
        data.to_csv("ordered.csv", header = False, index = False)
    
    def run(self):
        cursor = self.conn.cursor()
        cursor.execute("create database if not exists %s" % self.dbName)
        cursor.execute("use %s" % self.dbName)
        print("==================== CSV insert performance ====================")
        
        totalTime = 0
        for i in range(10):
            cursor.execute("drop table if exists t1")  
            cursor.execute("create table if not exists t1(ts timestamp, c1 int, c2 float, c3 int, c4 int)")
            startTime = time.time()
            cursor.execute("insert into t1 file 'outoforder.csv'")
            totalTime += time.time() - startTime 
            time.sleep(1)
                                 
        out_of_order_time = (float) (totalTime / 10)
        print("Out of Order - Insert time: %f" % out_of_order_time)                      
        
        totalTime = 0
        for i in range(10):
            cursor.execute("drop table if exists t2")
            cursor.execute("create table if not exists t2(ts timestamp, c1 int, c2 float, c3 int, c4 int)")
            startTime = time.time()
            cursor.execute("insert into t2 file 'ordered.csv'")
            totalTime += time.time() - startTime
            time.sleep(1)          

        in_order_time = (float) (totalTime / 10)
        print("In order - Insert time: %f" % in_order_time)
        cursor.close()


        cursor2 = self.conn2.cursor()
        cursor2.execute("create database if not exists %s" % self.dbName)
        cursor2.execute("use %s" % self.dbName)
        cursor2.execute("create table if not exists %s(ts timestamp, in_order_time float, out_of_order_time float, commit_id binary(50), branch binary(50), type binary(20))" % self.tbName)     
        cursor2.execute("insert into %s values(now, %f, %f, '%s', '%s', '%s')" % (self.tbName, in_order_time, out_of_order_time, self.commitID, self.branchName, self.type))

        cursor2.close()
        
if __name__ == '__main__':
    parser = argparse.ArgumentParser()    
    parser.add_argument(
        '-c',
        '--commit-id',
        action='store',
        default='null',
        type=str,
        help='git commit id (default: null)')
    parser.add_argument(
        '-d',
        '--database-name',
        action='store',
        default='perf',
        type=str,
        help='Database name to be created (default: perf)')
    parser.add_argument(
        '-t',
        '--table-name',
        action='store',
        default='csv_insert',
        type=str,
        help='Database name to be created (default: csv_insert)')
    parser.add_argument(
        '-b',
        '--branch-name',
        action='store',
        default='develop',
        type=str,
        help='branch name (default: develop)')
    parser.add_argument(
        '-T',
        '--build-type',
        action='store',
        default='glibc',
        type=str,
        help='build type (default: glibc)')
    
    args = parser.parse_args()
    perftest = insertFromCSVPerformace(args.commit_id, args.database_name, args.table_name, args.branch_name, args.build_type)
    perftest.run()