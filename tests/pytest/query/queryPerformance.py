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
import os
import taos
import time
import argparse


class taosdemoQueryPerformace:
    def __init__(self, clearCache, commitID, dbName, stbName, tbPerfix):
        self.clearCache = clearCache
        self.commitID = commitID
        self.dbName = dbName
        self.stbName = stbName
        self.tbPerfix = tbPerfix
        self.host = "127.0.0.1"
        self.user = "root"
        self.password = "taosdata"
        self.config = "/etc/taosperf"
        self.conn = taos.connect(
            self.host,
            self.user,
            self.password,
            self.config)

    def createPerfTables(self):
        cursor = self.conn.cursor()
        cursor.execute("create database if not exists %s" % self.dbName)
        cursor.execute("use %s" % self.dbName)
        cursor.execute("create table if not exists %s(ts timestamp, query_time float, commit_id binary(50)) tags(query_id int, query_sql binary(300))" % self.stbName)

        sql = "select count(*) from test.meters"
        tableid = 1
        cursor.execute("create table if not exists %s%d using %s tags(%d, '%s')" % (self.tbPerfix, tableid, self.stbName, tableid, sql))
        sql = "select avg(f1), max(f2), min(f3) from test.meters"
        tableid = 2
        cursor.execute("create table if not exists %s%d using %s tags(%d, '%s')" % (self.tbPerfix, tableid, self.stbName, tableid, sql))
        sql = "select count(*) from test.meters where loc='beijing'"
        tableid = 3
        cursor.execute("create table if not exists %s%d using %s tags(%d, \"%s\")" % (self.tbPerfix, tableid, self.stbName, tableid, sql))
        sql = "select avg(f1), max(f2), min(f3) from test.meters where areaid=10"
        tableid = 4
        cursor.execute("create table if not exists %s%d using %s tags(%d, '%s')" % (self.tbPerfix, tableid, self.stbName, tableid, sql))
        sql = "select avg(f1), max(f2), min(f3) from test.t10 interval(10s)"
        tableid = 5
        cursor.execute("create table if not exists %s%d using %s tags(%d, '%s')" % (self.tbPerfix, tableid, self.stbName, tableid, sql))
        sql = "select last_row(*) from meters"
        tableid = 6
        cursor.execute("create table if not exists %s%d using %s tags(%d, '%s')" % (self.tbPerfix, tableid, self.stbName, tableid, sql))
        sql = "select * from meters"
        tableid = 7
        cursor.execute("create table if not exists %s%d using %s tags(%d, '%s')" % (self.tbPerfix, tableid, self.stbName, tableid, sql))
        sql = "select avg(f1), max(f2), min(f3) from meters where ts <= '2017-07-15 10:40:01.000' and ts <= '2017-07-15 14:00:40.000'"
        tableid = 8
        cursor.execute("create table if not exists %s%d using %s tags(%d, \"%s\")" % (self.tbPerfix, tableid, self.stbName, tableid, sql))

        cursor.close()

    def query(self): 
        cursor = self.conn.cursor() 
        print("==================== query performance ====================")

        cursor.execute("use %s" % self.dbName)
        cursor.execute("select tbname, query_id, query_sql from %s" % self.stbName)      

        for data in cursor:
            table_name = data[0]
            query_id = data[1]
            sql = data[2]            
            
            totalTime = 0            
            cursor2 = self.conn.cursor()
            cursor2.execute("use test")
            for i in range(100):       
                if(self.clearCache == True):
                    # root permission is required
                    os.system("echo 3 > /proc/sys/vm/drop_caches")                
                
                startTime = time.time()         
                cursor2.execute(sql)
                totalTime += time.time() - startTime
            cursor2.close()    
            print("query time for: %s %f seconds" % (sql, totalTime / 100))
                        
            cursor3 = self.conn.cursor()
            cursor3.execute("insert into %s.%s values(now, %f, '%s')" % (self.dbName, table_name, totalTime / 100, self.commitID))

        cursor3.close()
        cursor.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()    
    parser.add_argument(
        '-r',
        '--remove-cache',                
        action='store_true',
        default=False,
        help='clear cache before query (default: False)')
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
        default='query_tb',
        type=str,
        help='table name to be created (default: query_tb)')
    parser.add_argument(
        '-p',
        '--table-perfix',
        action='store',
        default='q',
        type=str,
        help='table name perfix (default: q)')
    
    args = parser.parse_args()
    perftest = taosdemoQueryPerformace(args.remove_cache, args.commit_id, args.database_name, args.stable_name, args.table_perfix)
    perftest.createPerfTables()
    perftest.query()
