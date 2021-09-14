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

<<<<<<< HEAD
import traceback
import random
import string
from taos.error import LinesError
import datetime
import time
from copy import deepcopy
import numpy as np
=======
import random
import time
from copy import deepcopy
>>>>>>> origin/master
from util.log import *
from util.cases import *
from util.sql import *
from util.common import tdCom
import threading
<<<<<<< HEAD


=======
import itertools
>>>>>>> origin/master
class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self._conn = conn 
<<<<<<< HEAD

    def genRandomTs(self):
        year = random.randint(2000, 2021)
        month = random.randint(10, 12)
        day = random.randint(10, 29)
        hour = random.randint(10, 24)
        minute = random.randint(10, 59)
        second = random.randint(10, 59)
        m_second = random.randint(101, 199)
        date_time = f'{year}-{month}-{day} {hour}:{minute}:{second}'
        print(date_time)
        timeArray = time.strptime(date_time, "%Y-%m-%d %H:%M:%S")
        ts = int(time.mktime(timeArray))
        print("------", ts)
        # timestamp = time.mktime(datetime.datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S.%f").timetuple())
        return f'{ts}s'

    def genMultiColStr(self, int_count=4, double_count=0, binary_count=0):
        """
            genType must be tag/col
        """
=======
        self.lock = threading.Lock()

    def genMultiColStr(self, int_count=4, double_count=0, binary_count=0):
        '''
            related to self.getPerfSql()
            :count = 4 ---> 4 int
            :count = 1000 ---> 400 int 400 double 200 binary(128)
            :count = 4000 ---> 1900 int 1900 double 200 binary(128)
        '''
>>>>>>> origin/master
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
<<<<<<< HEAD
=======
        '''
            :init ---> stb insert line
        '''
>>>>>>> origin/master
        if init:
            tag_str = f'id="init",t0={random.randint(0, 65535)}i32,t1=\"{tdCom.getLongName(10, "letters")}\"'
        else:
            tag_str = f'id="sub_{tdCom.getLongName(5, "letters")}_{tdCom.getLongName(5, "letters")}",t0={random.randint(0, 65535)}i32,t1=\"{tdCom.getLongName(10, "letters")}\"'
<<<<<<< HEAD
        col_str = self.genMultiColStr(int_count, double_count, binary_count)
=======
        col_str = self.genMultiColStr(int_count=int_count, double_count=double_count, binary_count=binary_count)
>>>>>>> origin/master
        long_sql = 'stb' + ',' + tag_str + ' ' + col_str + '0'
        return long_sql

    def getPerfSql(self, count=4, init=False):
<<<<<<< HEAD
=======
        '''
            :count = 4 ---> 4 int
            :count = 1000 ---> 400 int 400 double 200 binary(128)
            :count = 4000 ---> 1900 int 1900 double 200 binary(128)
        '''
>>>>>>> origin/master
        if count == 4:
            input_sql = self.genLongSql(init=init)
        elif count == 1000:
            input_sql = self.genLongSql(400, 400, 200, init=init)
        elif count == 4000:
            input_sql = self.genLongSql(1900, 1900, 200, init=init)
        return input_sql

<<<<<<< HEAD
    def tableGenerator(self, count=4, table_count=1000):
        for i in range(table_count):
            yield self.getPerfSql(count)

    
            




    def genTableList(self, count=4, table_count=10000):
        table_list = list()
        for i in range(1, table_count+1):
            table_list.append(self.getPerfSql(count))
        return table_list
            
    def splitTableList(self, count=4, thread_count=10, table_count=1000):
        per_list_len = int(table_count/thread_count)
        table_list = self.genTableList(count=count)
        # ts = int(time.time())
        list_of_group = zip(*(iter(table_list),) *per_list_len) 
        end_list = [list(i) for i in list_of_group] # i is a tuple
        count = len(table_list) % per_list_len
        end_list.append(table_list[-count:]) if count !=0 else end_list
        return table_list, end_list

    def rowsGenerator(self, end_list):
        ts = int(time.time())
        input_sql_list = list()
        for elm_list in end_list:
            for elm in elm_list:
                for i in range(1, 10000):
                    ts -= 1
                    elm_new = self.replaceLastStr(elm, str(ts)) + 's'
                    input_sql_list.append(elm_new)
                yield input_sql_list

    # def insertRows(self, count=4, thread_count=10):
    #     table_list = self.splitTableList(count=count, thread_count=thread_count)[0]
    #     for 


    def replaceLastStr(self, str, new):
        list_ori = list(str)
        list_ori[-1] = new
        return ''.join(list_ori)
        
    def genDataList(self, table_list, row_count=10):
        data_list = list()
        ts = int(time.time())
        for table_str in table_list:
            for i in range(1, row_count+1):
                ts -= 1
                table_str_new = self.replaceLastStr(table_str, f'{str(ts)}s')
                data_list.append(table_str_new)
        print(data_list)
        return data_list


    def insertRows(self, count=4, table_count=1000):
        table_generator = self.tableGenerator(count=count, table_count=table_count)
        for table_name in table_generator:
            pass

    def perfTableInsert(self):
        table_generator = self.tableGenerator()
        for input_sql in table_generator:
            self._conn.insert_lines([input_sql])
            # for i in range(10):
            #     self._conn.insert_lines([input_sql])

    def perfDataInsert(self, count=4):
        table_generator = self.tableGenerator(count=count)
        ts = int(time.time())
        for input_sql in table_generator:
            print("input_sql-----------", input_sql)
            self._conn.insert_lines([input_sql])
            for i in range(100000):
                ts -= 1
                input_sql_new = self.replaceLastStr(input_sql, str(ts)) + 's'
                print("input_sql_new---------", input_sql_new)
                self._conn.insert_lines([input_sql_new])

    def batchInsertTable(self, batch_list):
        for insert_list in batch_list:
            print(threading.current_thread().name, "length=", len(insert_list))
            print(threading.current_thread().name, 'firstline', insert_list[0])
            print(threading.current_thread().name, 'lastline:', insert_list[-1])
            self._conn.insert_lines(insert_list)
            print(threading.current_thread().name, 'end')

    def genTableThread(self, thread_count=10):
        threads = list()
        for i in range(thread_count):
            t = threading.Thread(target=self.perfTableInsert)
            threads.append(t)
        return threads

    def genMultiThread(self, count, thread_count=10):
        threads = list()
        for i in range(thread_count):
            t = threading.Thread(target=self.perfDataInsert,args=(count,))
            threads.append(t)
        return threads

    def multiThreadRun(self, threads):
=======
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
>>>>>>> origin/master
        for t in threads:
            t.start()
        for t in threads:
            t.join()

<<<<<<< HEAD
    def createStb(self, count=4):
        input_sql = self.getPerfSql(count=count, init=True)
        self._conn.insert_lines([input_sql])

    def threadInsertTable(self, end_list, thread_count=10):
        threads = list()
        for i in range(thread_count):
            t = threading.Thread(target=self.batchInsertTable, args=(end_list,))
            threads.append(t)
        return threads


    def finalRun(self):
        self.createStb()
        table_list, end_list = self.splitTableList()
        batchInsertTableThread = self.threadInsertTable(end_list=end_list)
        self.multiThreadRun(batchInsertTableThread)
        # print(end_list)

    # def createTb(self, count=4):
    #     input_sql = self.getPerfSql(count=count)
    #     for i in range(10000):
    #         self._conn.insert_lines([input_sql])

    # def createTb1(self, count=4):
    #     start_time = time.time()
    #     self.multiThreadRun(self.genMultiThread(input_sql))
    #     end_time = time.time()
    #     return end_time - start_time
        
    # def calInsertTableTime(self):
    #     start_time = time.time()
    #     self.createStb()
    #     self.multiThreadRun(self.genMultiThread())
    #     end_time = time.time()
    #     return end_time - start_time

    def calRunTime(self, count=4):
        start_time = time.time()
        self.createStb()
        self.multiThreadRun(self.genMultiThread(count=count))
        end_time = time.time()
        return end_time - start_time

    def calRunTime1(self, count=4):
        start_time = time.time()
        self.createStb()
        self.multiThreadRun(self.perfTableInsert())
        # self.perfTableInsert()

    # def schemalessInsertPerfTest(self, count=4):
    #     input_sql = self.getPerfSql(count)
    #     self.calRunTime(input_sql)

    # def test(self):
    #     sql1 = 'stb,id="init",t0=14865i32,t1="tvnqbjuqck" c0=37i32,c1=217i32,c2=3i32,c3=88i32 1626006833640ms'
    #     sql2 = 'stb,id="init",t0=14865i32,t1="tvnqbjuqck" c0=38i32,c1=217i32,c2=3i32,c3=88i32 1626006833641ms'
    #     self._conn.insert_lines([sql1])
    #     self._conn.insert_lines([sql2])
=======
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
>>>>>>> origin/master

    def run(self):
        print("running {}".format(__file__))
        tdSql.prepare()
<<<<<<< HEAD
        self.finalRun()
        # print(self.calRunTime1(count=4))
        # print(self.calRunTime(count=4))
        # print(self.genRandomTs())
        # self.calInsertTableTime()
        # self.test()
        # table_list = self.splitTableList()[0]
        # data_list = self.genDataList(table_list)
        # print(len(data_list))
        # end_list = [['stb,id="sub_vzvfx_dbuxp",t0=9961i32,t1="zjjfayhfep" c0=83i32,c1=169i32,c2=177i32,c3=4i32 0','stb,id="sub_vzvfx_dbuxp",t0=9961i32,t1="zjjfayhfep" c0=83i32,c1=169i32,c2=177i32,c3=4i32 0'], ['stb,id="sub_vzvfx_dbuxp",t0=9961i32,t1="zjjfayhfep" c0=83i32,c1=169i32,c2=177i32,c3=4i32 0','stb,id="sub_vzvfx_dbuxp",t0=9961i32,t1="zjjfayhfep" c0=83i32,c1=169i32,c2=177i32,c3=4i32 0']]
        # rowsGenerator = self.rowsGenerator(end_list)
        # for i in rowsGenerator:
        #     print(i)
=======
        result = self.getPerfResults(test_times=1, table_count=1000, thread_count=10)
        print(result)
>>>>>>> origin/master

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
