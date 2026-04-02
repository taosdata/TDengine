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

import traceback
import random
import string
from taos.error import LinesError
import datetime
import time
from copy import deepcopy
import numpy as np
from util.log import *
from util.cases import *
from util.sql import *
from util.common import tdCom
import threading


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self._conn = conn 

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
        if init:
            tag_str = f'id="init",t0={random.randint(0, 65535)}i32,t1=\"{tdCom.getLongName(10, "letters")}\"'
        else:
            tag_str = f'id="sub_{tdCom.getLongName(5, "letters")}_{tdCom.getLongName(5, "letters")}",t0={random.randint(0, 65535)}i32,t1=\"{tdCom.getLongName(10, "letters")}\"'
        col_str = self.genMultiColStr(int_count, double_count, binary_count)
        long_sql = 'stb' + ',' + tag_str + ' ' + col_str + '0'
        return long_sql

    def getPerfSql(self, count=4, init=False):
        if count == 4:
            input_sql = self.genLongSql(init=init)
        elif count == 1000:
            input_sql = self.genLongSql(400, 400, 200, init=init)
        elif count == 4000:
            input_sql = self.genLongSql(1900, 1900, 200, init=init)
        return input_sql

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
        for t in threads:
            t.start()
        for t in threads:
            t.join()

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

    def run(self):
        print("running {}".format(__file__))
        tdSql.prepare()
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

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
