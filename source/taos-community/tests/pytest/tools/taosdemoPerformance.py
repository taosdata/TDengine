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
import sys

class taosdemoPerformace:
    def __init__(self, commitID, dbName, branch, type, numOfTables, numOfRows, numOfInt, numOfDouble, numOfBinary):
        self.commitID = commitID
        self.dbName = dbName
        self.branch = branch
        self.type = type
        self.numOfTables = numOfTables
        self.numOfRows = numOfRows
        self.numOfInt = numOfInt
        self.numOfDouble = numOfDouble
        self.numOfBinary = numOfBinary 
        self.host = "127.0.0.1"
        self.user = "root"
        self.password = "taosdata"
        self.config = "/etc/perf"
        self.conn = taos.connect(
            self.host,
            self.user,
            self.password,
            self.config)
        self.insertDB = "insertDB"
        self.host2 = "192.168.1.179"    
        self.conn2 = taos.connect(
            host = self.host2,
            user = self.user,
            password = self.password,
            config = self.config)

    def generateJson(self):
        db = {
            "name": "%s" % self.insertDB,
            "drop": "yes"
        }

        stb = {
            "name": "meters",
            "childtable_count": self.numOfTables,
            "childtable_prefix": "stb_",
            "batch_create_tbl_num": 10,
            "insert_mode": "rand",
            "insert_rows": self.numOfRows,
            "batch_rows": 1000000,
            "max_sql_len": 1048576,
            "timestamp_step": 1,
            "start_timestamp": "2020-10-01 00:00:00.000",
            "sample_format": "csv",
            "sample_file": "./sample.csv",
            "tags_file": "",
            "columns": [
                {"type": "INT", "count": self.numOfInt},
                {"type": "DOUBLE", "count": self.numOfDouble},
                {"type": "BINARY", "len": 128, "count": self.numOfBinary}
            ],
            "tags": [
                {"type": "INT", "count": 1},
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
            "cfgdir": "/etc/perf",
            "host": "127.0.0.1",
            "port": 6030,
            "user": "root",
            "password": "taosdata",
            "thread_count": 10,
            "create_table_thread_count": 4,
            "result_file": "./insert_res.txt",
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

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/build/bin")]
                    break
        return buildPath

    def insertData(self):
        buildPath = self.getBuildPath()
        if (buildPath == ""):
            print("taosdemo not found!")
            sys.exit(1)
            
        binPath = buildPath + "/build/bin/"

        os.system(
            "%sperfMonitor -f %s > /dev/null 2>&1" %
            (binPath, self.generateJson()))
        self.createTableTime = self.getCMDOutput(
            "grep 'Spent' insert_res.txt | awk 'NR==1{print $2}'")
        self.insertRecordsTime = self.getCMDOutput(
            "grep 'Spent' insert_res.txt | awk 'NR==2{print $2}'")
        self.recordsPerSecond = self.getCMDOutput(
            "grep 'Spent' insert_res.txt | awk 'NR==2{print $16}'")
        self.commitID = self.getCMDOutput("git rev-parse --short HEAD")
        delay = self.getCMDOutput(
            "grep 'delay' insert_res.txt | awk '{print $4}'")
        self.avgDelay = delay[:-4]
        delay = self.getCMDOutput(
            "grep 'delay' insert_res.txt | awk '{print $6}'")
        self.maxDelay = delay[:-4]
        delay = self.getCMDOutput(
            "grep 'delay' insert_res.txt | awk '{print $8}'")
        self.minDelay = delay[:-3]

        os.system("[ -f insert_res.txt ] && rm insert_res.txt")

    def createTablesAndStoreData(self):
        cursor = self.conn2.cursor()

        cursor.execute("create database if not exists %s" % self.dbName)
        cursor.execute("use %s" % self.dbName)
        cursor.execute("create table if not exists taosdemo_perf (ts timestamp, create_table_time float, insert_records_time float, records_per_second float, commit_id binary(50), avg_delay float, max_delay float, min_delay float, branch binary(50), type binary(20), numoftables int, numofrows int, numofint int, numofdouble int, numofbinary int)")
        print("create tables time: %f" % float(self.createTableTime))
        print("insert records time: %f" % float(self.insertRecordsTime))
        print("records per second: %f" % float(self.recordsPerSecond))
        print("avg delay: %f" % float(self.avgDelay))
        print("max delay: %f" % float(self.maxDelay))
        print("min delay: %f" % float(self.minDelay))
        cursor.execute("insert into taosdemo_perf values(now, %f, %f, %f, '%s', %f, %f, %f, '%s', '%s', %d, %d, %d, %d, %d)" %
            (float(self.createTableTime), float(self.insertRecordsTime), float(self.recordsPerSecond), 
            self.commitID, float(self.avgDelay), float(self.maxDelay), float(self.minDelay), self.branch, 
            self.type, self.numOfTables, self.numOfRows, self.numOfInt, self.numOfDouble, self.numOfBinary))
        cursor.close()

        cursor1 = self.conn.cursor()
        cursor1.execute("drop database if exists %s" % self.insertDB)
        cursor1.close()

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
        '-b',
        '--git-branch',
        action='store',
        default='master',
        type=str,
        help='git branch (default: master)')
    parser.add_argument(
        '-T',
        '--build-type',
        action='store',
        default='glibc',
        type=str,
        help='build type (default: glibc)')
    parser.add_argument(
        '-i',
        '--num-of-int',
        action='store',
        default=4,
        type=int,
        help='num of int columns (default: 4)')
    parser.add_argument(
        '-D',
        '--num-of-double',
        action='store',
        default=0,
        type=int,
        help='num of double columns (default: 4)')
    parser.add_argument(
        '-B',
        '--num-of-binary',
        action='store',
        default=0,
        type=int,
        help='num of binary columns (default: 4)')
    parser.add_argument(
        '-t',
        '--num-of-tables',
        action='store',
        default=10000,
        type=int,
        help='num of tables (default: 10000)')
    parser.add_argument(
        '-r',
        '--num-of-rows',
        action='store',
        default=100000,
        type=int,
        help='num of rows (default: 100000)')
    args = parser.parse_args()

    perftest = taosdemoPerformace(args.commit_id, args.database_name, args.git_branch, args.build_type, args.num_of_tables, args.num_of_rows, args.num_of_int, args.num_of_double, args.num_of_binary)
    perftest.insertData()
    perftest.createTablesAndStoreData()
