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

import taos
import pandas as pd
import argparse
import os.path
import json

class taosdemoPerformace:
    def __init__(self, commitID, dbName):
        self.commitID = commitID
        self.dbName = dbName        
        self.host = "127.0.0.1"
        self.user = "root"
        self.password = "taosdata"
        self.config = "/etc/taosperf"
        self.conn = taos.connect(
            self.host,
            self.user,
            self.password,
            self.config)
        self.insertDB = "insertDB";    
    
    def generateJson(self):
        db = {
            "name": "%s" % self.insertDB,
            "drop": "yes",
            "replica": 1
        }

        stb = {
            "name": "meters",
            "child_table_exists":"no",
            "childtable_count": 10000,
            "childtable_prefix": "stb_",
            "auto_create_table": "no",
            "data_source": "rand",
            "batch_create_tbl_num": 10,
            "insert_mode": "taosc",
            "insert_rows": 100000,
            "multi_thread_write_one_tbl": "no",
            "number_of_tbl_in_one_sql": 0,
            "rows_per_tbl": 100,
            "max_sql_len": 1024000,
            "disorder_ratio": 0,
            "disorder_range": 1000,
            "timestamp_step": 1,
            "start_timestamp": "2020-10-01 00:00:00.000",
            "sample_format": "csv",
            "sample_file": "./sample.csv",
            "tags_file": "",            
            "columns": [
                {"type": "INT", "count": 4}
            ],            
            "tags": [
                {"type": "INT", "count":1}, 
                {"type": "BINARY", "len": 16}
            ]
        }

        stables = []
        stables.append(stb)

        db = {
            "dbinfo": db,
            "super_tables": stables
        }

        insert_data = {
            "filetype": "insert",
            "cfgdir": "/etc/taosperf",
            "host": "127.0.0.1",
            "port": 6030,
            "user": "root",
            "password": "taosdata",
            "thread_count": 10,
            "thread_count_create_tbl": 10,
            "result_file": "./insert_res.txt",
            "confirm_parameter_prompt": "no",
            "insert_interval": 0,
            "num_of_records_per_req": 30000,
            "databases": [db]        
        }

        insert_json_file = f"/tmp/insert.json"

        with open(insert_json_file, 'w') as f:
            json.dump(insert_data, f)
        return insert_json_file

    def getCMDOutput(self, cmd):
        cmd = os.popen(cmd)
        output = cmd.read()
        cmd.close()
        return output

    def insertData(self):        
        os.system("taosdemo -f %s > taosdemoperf.txt" % self.generateJson())
        self.createTableTime = self.getCMDOutput("grep 'Spent' taosdemoperf.txt | awk 'NR==1{print $2}'")
        self.insertRecordsTime = self.getCMDOutput("grep 'Spent' taosdemoperf.txt | awk 'NR==2{print $2}'")
        self.recordsPerSecond = self.getCMDOutput("grep 'Spent' taosdemoperf.txt | awk 'NR==2{print $16}'")
        self.commitID = self.getCMDOutput("git rev-parse --short HEAD")
        delay = self.getCMDOutput("grep 'delay' taosdemoperf.txt | awk '{print $4}'")
        self.avgDelay = delay[:-4]
        delay = self.getCMDOutput("grep 'delay' taosdemoperf.txt | awk '{print $6}'")
        self.maxDelay = delay[:-4]
        delay = self.getCMDOutput("grep 'delay' taosdemoperf.txt | awk '{print $8}'") 
        self.minDelay = delay[:-3]

        os.system("[ -f taosdemoperf.txt ] && rm taosdemoperf.txt")

    def createTablesAndStoreData(self):
        cursor = self.conn.cursor()
                    
        cursor.execute("create database if not exists %s" % self.dbName)
        cursor.execute("use %s" % self.dbName)
        cursor.execute("create table if not exists taosdemo_perf (ts timestamp, create_table_time float, insert_records_time float, records_per_second float, commit_id binary(50), avg_delay float, max_delay float, min_delay float)")
        print("==================== taosdemo performance ====================")
        print("create tables time: %f" % float(self.createTableTime))
        print("insert records time: %f" % float(self.insertRecordsTime))
        print("records per second: %f" % float(self.recordsPerSecond))
        print("avg delay: %f" % float(self.avgDelay))
        print("max delay: %f" % float(self.maxDelay))
        print("min delay: %f" % float(self.minDelay)) 
        cursor.execute("insert into taosdemo_perf values(now, %f, %f, %f, '%s', %f, %f, %f)" % 
        (float(self.createTableTime), float(self.insertRecordsTime), float(self.recordsPerSecond), self.commitID, float(self.avgDelay), float(self.maxDelay), float(self.minDelay)))
        cursor.execute("drop database if exists %s" % self.insertDB)

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

    args = parser.parse_args()

    perftest = taosdemoPerformace(args.commit_id, args.database_name) 
    perftest.insertData()
    perftest.createTablesAndStoreData()