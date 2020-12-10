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
    def __init__(self, commitID, dbName, stbName, branchName):
        self.commitID = commitID
        self.dbName = dbName
        self.stbName = stbName
        self.branchName = branchName
        self.ts = 1500074556514
        self.host = "127.0.0.1"
        self.user = "root"
        self.password = "taosdata"
        self.config = "/etc/taosperf"
        self.conn = taos.connect(
            self.host,
            self.user,
            self.password,
            self.config)
        
    def writeCSV(self):
        with open('test3.csv','w', encoding='utf-8', newline='') as csvFile:
            writer = csv.writer(csvFile, dialect='excel')
            for i in range(1000000):
                newTimestamp = self.ts + random.randint(10000000, 10000000000) + random.randint(1000, 10000000) + random.randint(1, 1000)
                d = datetime.datetime.fromtimestamp(newTimestamp / 1000)
                dt = str(d.strftime("%Y-%m-%d %H:%M:%S.%f"))
                writer.writerow(["'%s'" % dt, random.randint(1, 100), random.uniform(1, 100), random.randint(1, 100), random.randint(1, 100)])
    
    def removCSVHeader(self):
        data = pd.read_csv("ordered.csv")
        data = data.drop([0])
        data.to_csv("ordered.csv", header = False, index = False)
    
    def createTables(self):
        cursor = self.conn.cursor()
                    
        cursor.execute("create database if not exists %s" % self.dbName)
        cursor.execute("use %s" % self.dbName)
        cursor.execute("create table if not exists %s(ts timestamp, in_order_time float, out_of_order_time float, commit_id binary(50)) tags(branch binary(50))" % self.stbName)
        cursor.execute("create table if not exists %s using %s tags('%s')" % (self.branchName, self.stbName, self.branchName))

        cursor.execute("create table if not exists t1(ts timestamp, c1 int, c2 float, c3 int, c4 int)")
        cursor.execute("create table if not exists t2(ts timestamp, c1 int, c2 float, c3 int, c4 int)")

        cursor.close()
    
    def run(self):
        cursor = self.conn.cursor()
        cursor.execute("use %s" % self.dbName)
        print("==================== CSV insert performance ====================")
        
        totalTime = 0
        for i in range(10):
            cursor.execute("create table if not exists t1(ts timestamp, c1 int, c2 float, c3 int, c4 int)")
            startTime = time.time()
            cursor.execute("insert into t1 file 'outoforder.csv'")
            totalTime += time.time() - startTime
            cursor.execute("drop table if exists t1")            
        out_of_order_time = (float) (totalTime / 10)
        print("Out of Order - Insert time: %f" % out_of_order_time)                      
        
        totalTime = 0
        for i in range(10):
            cursor.execute("create table if not exists t2(ts timestamp, c1 int, c2 float, c3 int, c4 int)")
            startTime = time.time()
            cursor.execute("insert into t2 file 'ordered.csv'")
            totalTime += time.time() - startTime
            cursor.execute("drop table if exists t2")

        in_order_time = (float) (totalTime / 10)
        print("In order - Insert time: %f" % in_order_time)
        cursor.execute("insert into %s values(now, %f, %f, '%s')" % (self.branchName, in_order_time, out_of_order_time, self.commitID))
        

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
        '--stable-name',
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
    
    args = parser.parse_args()
    perftest = insertFromCSVPerformace(args.commit_id, args.database_name, args.stable_name, args.branch_name)

    perftest.createTables()
    perftest.run()