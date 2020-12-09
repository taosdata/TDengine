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

class taosdemoPerformace:
    def __init__(self, commitID, dbName, createTableTime, insertRecordsTime, recordsPerSecond):
        self.commitID = commitID
        self.dbName = dbName
        self.createTableTime = createTableTime
        self.insertRecordsTime = insertRecordsTime
        self.recordsPerSecond = recordsPerSecond        
        self.host = "127.0.0.1"
        self.user = "root"
        self.password = "taosdata"
        self.config = "/etc/taosperf"
        self.conn = taos.connect(
            self.host,
            self.user,
            self.password,
            self.config)    
    
    def createTablesAndStoreData(self):
        cursor = self.conn.cursor()
                    
        cursor.execute("create database if not exists %s" % self.dbName)
        cursor.execute("use %s" % self.dbName)
        cursor.execute("create table if not exists taosdemo_perf (ts timestamp, create_table_time float, insert_records_time float, records_per_second float, commit_id binary(50))")
        print("==================== taosdemo performance ====================")
        print("create tables time: %f" % self.createTableTime)
        print("insert records time: %f" % self.insertRecordsTime)
        print("records per second: %f" % self.recordsPerSecond)
        cursor.execute("insert into taosdemo_perf values(now, %f, %f, %f, '%s')" % (self.createTableTime, self.insertRecordsTime, self.recordsPerSecond, self.commitID))
        cursor.execute("drop database if exists taosdemo_insert_test")

        cursor.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-c',
        '--commit-id',
        action='store',        
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
        '--create-table',
        action='store',        
        type=float,
        help='create table time')
    parser.add_argument(
        '-i',
        '--insert-records',
        action='store',        
        type=float,
        help='insert records time')
    parser.add_argument(
        '-r',
        '---records-per-second',
        action='store',        
        type=float,
        help='records per request')
    
    args = parser.parse_args()

    perftest = taosdemoPerformace(args.commit_id, args.database_name, args.create_table, args.insert_records, args.records_per_second)    
    perftest.createTablesAndStoreData()