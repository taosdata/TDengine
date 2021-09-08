###################################################################
#           Copyright (c) 2021 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import random
import time
from copy import deepcopy
from util.log import *
from util.cases import *
from util.sql import *
from util.common import tdCom
import threading
import itertools
class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self._conn = conn 
        self.lock = threading.Lock()

    def genMultiColStr(self, int_count=4, double_count=0, binary_count=0):
        '''
            related to self.getPerfSql()
            :count = 4 ---> 4 int
            :count = 1000 ---> 400 int 400 double 200 binary(128)
            :count = 4000 ---> 1900 int 1900 double 200 binary(128)
        '''
        col_str = ""
        if double_count == 0 and binary_count == 0:
            for i in range(0, int_count):
                if i < (int_count-1):
                    col_str += f'c{i}={random.randint(0, 255)}i32,'
                else:
                    col_str += f'c{i}={random.randint(0, 255)}i32 '
        elif double_count > 0 and binary_count == 0:
            for i in range(0, int_count):
                col_str += f'c{i}={random.randint(0, 255)}i32,'
            for i in range(0, double_count):
                if i < (double_count-1):
                    col_str += f'c{i+int_count}={random.randint(1, 255)}.{i}f64,'
                else:
                    col_str += f'c{i+int_count}={random.randint(1, 255)}.{i}f64 '
        elif double_count == 0 and binary_count > 0:
            for i in range(0, int_count):
                col_str += f'c{i}={random.randint(0, 255)}i32,'
            for i in range(0, binary_count):
                if i < (binary_count-1):
                    col_str += f'c{i+int_count}=\"{tdCom.getLongName(5, "letters")}\",'
                else:
                    col_str += f'c{i+int_count}=\"{tdCom.getLongName(5, "letters")}\" '
        elif double_count > 0 and binary_count > 0:
            for i in range(0, int_count):
                col_str += f'c{i}={random.randint(0, 255)}i32,'
            for i in range(0, double_count):
                col_str += f'c{i+int_count}={random.randint(1, 255)}.{i}f64,'
            for i in range(0, binary_count):
                if i < (binary_count-1):
                    col_str += f'c{i+int_count+double_count}=\"{tdCom.getLongName(5, "letters")}\",'
                else:
                    col_str += f'c{i+int_count+double_count}=\"{tdCom.getLongName(5, "letters")}\" '
        return col_str

    def genLongSql(self, int_count=4, double_count=0, binary_count=0, init=False):
        '''
            :init ---> stb insert line
        '''
        if init:
            tag_str = f'id="init",t0={random.randint(0, 65535)}i32,t1=\"{tdCom.getLongName(10, "letters")}\"'
        else:
            tag_str = f'id="sub_{tdCom.getLongName(5, "letters")}_{tdCom.getLongName(5, "letters")}",t0={random.randint(0, 65535)}i32,t1=\"{tdCom.getLongName(10, "letters")}\"'
        col_str = self.genMultiColStr(int_count=int_count, double_count=double_count, binary_count=binary_count)
        long_sql = 'stb' + ',' + tag_str + ' ' + col_str + '0'
        return long_sql

    def getPerfSql(self, count=4, init=False):
        '''
            :count = 4 ---> 4 int
            :count = 1000 ---> 400 int 400 double 200 binary(128)
            :count = 4000 ---> 1900 int 1900 double 200 binary(128)
        '''
        if count == 4:
            input_sql = self.genLongSql(init=init)
        elif count == 1000:
            input_sql = self.genLongSql(400, 400, 200, init=init)
        elif count == 4000:
            input_sql = self.genLongSql(1900, 1900, 200, init=init)
        return input_sql

    def replaceLastStr(self, str, new):
        '''
            replace last element of str to new element 
        '''
        list_ori = list(str)
        list_ori[-1] = new
        return ''.join(list_ori)

    def createStb(self, count=4):
        '''
            create 1 stb
        '''
        input_sql = self.getPerfSql(count=count, init=True)
        print(threading.current_thread().name, "create stb line:", input_sql)
        self._conn.insert_lines([input_sql])
        print(threading.current_thread().name, "create stb end")

    def batchCreateTable(self, batch_list):
        '''
            schemaless insert api
        '''
        print(threading.current_thread().name, "length=", len(batch_list))
        print(threading.current_thread().name, 'firstline', batch_list[0][0:50], '...', batch_list[0][-50:-1])
        print(threading.current_thread().name, 'lastline:', batch_list[-1][0:50], '...', batch_list[-1][-50:-1])
        begin = time.time_ns();
        self._conn.insert_lines(batch_list)
        end = time.time_ns();
        print(threading.current_thread().name, 'end time:', (end-begin)/10**9)

    def splitGenerator(self, table_list, thread_count):
        '''
            split a list to n piece of sub_list
            [a, b, c, d] ---> [[a, b], [c, d]]
            yield type ---> generator
        '''
        sub_list_len = int(len(table_list)/thread_count)
        for i in range(0, len(table_list), sub_list_len):
            yield table_list[i:i + sub_list_len]

    def genTbListGenerator(self, table_list, thread_count):
        '''
            split table_list, after split
        '''
        table_list_generator = self.splitGenerator(table_list, thread_count)
        return table_list_generator

    def genTableList(self, count=4, table_count=10000):
        '''
            gen len(table_count) table_list
        '''
        table_list = list()
        for i in range(table_count):
            table_list.append(self.getPerfSql(count=count))
        return table_list

    def threadCreateTables(self, table_list_generator, thread_count=10):
        '''
            thread create tables
        '''
        threads = list()
        for i in range(thread_count):
            t = threading.Thread(target=self.batchCreateTable, args=(next(table_list_generator),))
            threads.append(t)
        return threads

    def batchInsertRows(self, table_list, rows_count):
        '''
            add rows in each table ---> count=rows_count
        '''
        for input_sql in table_list:
            ts = int(time.time())
            input_sql_list = list()
            for i in range(rows_count-1):
                ts -= 1
                elm_new = self.replaceLastStr(input_sql, str(ts)) + 's'
                input_sql_list.append(elm_new)
            self.batchCreateTable(input_sql_list)

    def threadsInsertRows(self, rows_generator, rows_count=1000, thread_count=10):
        '''
            multi insert rows in each table
        '''
        threads = list()
        for i in range(thread_count):
            self.lock.acquire()
            t = threading.Thread(target=self.batchInsertRows, args=(next(rows_generator), rows_count,))
            threads.append(t)
            self.lock.release()
        return threads

    def multiThreadRun(self, threads):
        '''
            multi run threads
        '''
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    def createTables(self, count, table_count=10000, thread_count=10):
        '''
            create stb and tb
        '''
        table_list = self.genTableList(count=count, table_count=table_count)
        create_tables_start_time = time.time()
        self.createStb(count=count)
        table_list_generator = self.genTbListGenerator(table_list, thread_count)
        create_tables_generator, insert_rows_generator = itertools.tee(table_list_generator, 2)
        self.multiThreadRun(self.threadCreateTables(table_list_generator=create_tables_generator, thread_count=thread_count))
        create_tables_end_time = time.time()
        create_tables_time = int(create_tables_end_time - create_tables_start_time)
        return_str = f'create tables\' time of {count} columns  ---> {create_tables_time}s'
        return insert_rows_generator, create_tables_time, return_str

    def insertRows(self, count, rows_generator, rows_count=1000, thread_count=10):
        '''
            insert rows 
        '''
        insert_rows_start_time = time.time()
        self.multiThreadRun(self.threadsInsertRows(rows_generator=rows_generator, rows_count=rows_count, thread_count=thread_count))
        insert_rows_end_time = time.time()
        insert_rows_time = int(insert_rows_end_time - insert_rows_start_time)
        return_str = f'insert rows\' time of {count} columns  ---> {insert_rows_time}s'
        return insert_rows_time, return_str

    def schemalessPerfTest(self, count, table_count=10000, thread_count=10, rows_count=1000):
        '''
            get performance
        '''
        insert_rows_generator = self.createTables(count=count, table_count=table_count, thread_count=thread_count)[0]
        return self.insertRows(count=count, rows_generator=insert_rows_generator, rows_count=rows_count, thread_count=thread_count)

    def getPerfResults(self, test_times=3, table_count=10000, thread_count=10):
        col4_time = 0
        col1000_time = 0
        col4000_time = 0

        for i in range(test_times):
            tdCom.cleanTb()
            time_used = self.schemalessPerfTest(count=4, table_count=table_count, thread_count=thread_count)[0]
            col4_time += time_used
        col4_time /= test_times
        print(col4_time)

        # for i in range(test_times):
        #     tdCom.cleanTb()
        #     time_used = self.schemalessPerfTest(count=1000, table_count=table_count, thread_count=thread_count)[0]
        #     col1000_time += time_used
        # col1000_time /= test_times    
        # print(col1000_time)
        
        # for i in range(test_times):
        #     tdCom.cleanTb()
        #     time_used = self.schemalessPerfTest(count=4000, table_count=table_count, thread_count=thread_count)[0]
        #     col4000_time += time_used
        # col4000_time /= test_times
        # print(col4000_time)

        return col4_time, col1000_time, col4000_time

    def run(self):
        print("running {}".format(__file__))
        tdSql.prepare()
        result = self.getPerfResults(test_times=1, table_count=1000, thread_count=10)
        print(result)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
