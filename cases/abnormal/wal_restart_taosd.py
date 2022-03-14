###################################################################
#           Copyright (c) 2020 by TAOS Technologies, Inc.
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
import threading
import time

import taos
from taostest import TDCase
from taostest.util.common import TDCom


class MyThread(threading.Thread):
    def __init__(self, func, args=()):
        super(MyThread, self).__init__()
        self.func = func
        self.args = args
        self.end_status = 1

    def run(self):
        self.result = self.func(*self.args)

    def get_result(self):
        try:
            if self.result:
                self.end_status = 0
            return self.result
        except Exception:
            self.end_status = 2
            return None


class TestWal(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.taosd_envs = self.get_component_by_name("taosd")

        self.primary_conf = self.taosd_envs[0]

        self.secondary_conf = self.taosd_envs[1]

        self.ts = 1643644800000  # 2010-02-01 00:00:00
        self.time_step = 10;
        self.count = 0
        self.sleep_time = 5  # every sleep_time ,restart taosd
        self.loops = 3  # loop restart taosd times
        self.nums = 100000  # rows of per_table ,if basic insert , only 1 table ,if multi insert ,table nums = self .thread_nums
        self.batch = 500
        self.thread_nums = 100  # thread nums to insert data ,every thread insert an sub_table

    def get_conn(self, conf):  # conf is an dict
        conn = self.tdSql.get_connection(conf)
        return conn

    def restart_primary_taosd(self, sleep_time, loops):

        # if match sleep time ,kill -9 primary taosd instance
        primary_dnode = self.primary_conf["spec"]["dnodes"][0]

        for loop in range(loops):
            time.sleep(sleep_time)
            print("this is the %d_th kill primary taosd instance" % loop)

            # do restart
            self.envMgr.taosd.restart(primary_dnode, sleep_time)

    def prepare_db_stable(self):
        conn_primary = self.get_conn(self.primary_conf)
        conn_secondary = self.get_conn(self.secondary_conf)

        conn_primary.execute("create database wal_test")
        conn_primary.execute("use wal_test")

        conn_primary.execute("create stable st (ts timestamp ,int_val int , double_val double ,\
         err_msg binary(80)) tags(name binary(20))")

        conn_secondary.execute("create database wal_success")
        conn_secondary.execute("create stable wal_success.st (ts timestamp ,int_val int , double_val double ,\
         err_msg binary(80)) tags(name binary(20))")

        conn_secondary.execute("create database wal_failed")
        conn_secondary.execute("create stable wal_failed.st (ts timestamp ,int_val int , double_val double , \
            err_msg binary(80)) tags(name binary(20))")

    def multi_insert_task(self, tbname):

        print(" ======= multi insert task is going now ======= ")

        tablename = tbname
        dbname_list = ['wal_test', 'wal_success', 'wal_failed', 'wal_error']
        conn_primary = self.get_conn(self.primary_conf)
        conn_secondary = self.get_conn(self.secondary_conf)

        row_length = 0
        dbname = dbname_list[0]

        multi_sqls = f'insert into {dbname}.{tablename} using {dbname}.st tags("test") values'

        for i in range(self.nums):  # 500 rows for a batch  default

            ts = self.ts + self.count * self.time_step
            int_val = self.count
            double_val = self.count + 0.01

            err_msg = "NULL"
            dbname = dbname_list[0]

            insert_sql = f'insert into {dbname}.{tablename} using {dbname}.st tags("test") values({ts} , {int_val}, \
            {double_val}, "{err_msg}")'
            if i % self.batch == 0:
                replace_body = f'insert into {dbname}.{tablename} using {dbname}.st tags("test") values'
                body = insert_sql.replace(replace_body, "")
                multi_sqls = multi_sqls + body
                row_length = len(insert_sql)
                if len(multi_sqls) >= row_length:  # avoid first multi_sqls only 1 rows
                    flag = 0
                    try:
                        conn_primary.execute(multi_sqls)
                    except taos.Error as err:
                        err_msg = str(err.msg)
                        multi_sqls = multi_sqls.replace("NULL", err_msg)
                        flag = 1
                    if flag == 0:  # means insert sucess
                        secondary_dbname = dbname_list[1]
                    elif flag == 1:
                        secondary_dbname = dbname_list[2]

                    multi_sqls = multi_sqls.replace(dbname, secondary_dbname)
                    conn_secondary.execute(multi_sqls)
                multi_sqls = f'insert into {dbname}.{tablename} using {dbname}.st tags("test") values'
            else:
                replace_body = f'insert into {dbname}.{tablename} using {dbname}.st tags("test") values'
                body = insert_sql.replace(replace_body, "")
                multi_sqls += body

            self.count += 1

        if len(multi_sqls) >= row_length:  # avoid first multi_sqls only 1 rows
            flag = 0
            try:
                conn_primary.execute(multi_sqls)
            except taos.Error as err:
                err_msg = str(err.msg)
                multi_sqls = multi_sqls.replace("NULL", err_msg)
                flag = 1
            if flag == 0:  # means insert sucess
                secondary_dbname = dbname_list[1]
            elif flag == 1:
                secondary_dbname = dbname_list[2]

            multi_sqls = multi_sqls.replace(dbname, secondary_dbname)
            conn_secondary.execute(multi_sqls)
            multi_sqls = f'insert into {dbname}.{tablename} using {dbname}.st tags("test") values'

    def basic_insert_task(self, tbname):
        print(" ======= insert task is going now ======= ")

        tablename = tbname
        dbname_list = ['wal_test', 'wal_success', 'wal_failed', 'wal_error']
        conn_primary = self.get_conn(self.primary_conf)
        conn_secondary = self.get_conn(self.secondary_conf)

        for i in range(self.nums):
            ts = self.ts + self.count * self.time_step
            int_val = self.count
            double_val = self.count + 0.01
            dbname = dbname_list[0]
            err_msg = "NULL"
            insert_sql = f'insert into {dbname}.{tablename} using {dbname}.st tags("test") values({ts} , {int_val}, \
            {double_val},{err_msg})'
            flag = 0
            try:
                conn_primary.execute(insert_sql)
            except taos.Error as err:
                err_msg = str(err.msg)
                insert_sql = f'insert into {dbname}.{tablename} using {dbname}.st tags("test") values({ts} , {int_val}, \
            {double_val},"{err_msg}")'
                flag = 1
            self.count += 1
            if flag == 0:  # means insert sucess
                secondary_dbname = dbname_list[1]
            elif flag == 1:
                secondary_dbname = dbname_list[2]
            insert_sql = insert_sql.replace(dbname, secondary_dbname)
            conn_secondary.execute(insert_sql)

    def compare_data(self):
        more = set()
        miss = set()
        conn_primary = self.get_conn(self.primary_conf)
        conn_secondary = self.get_conn(self.secondary_conf)
        result = conn_primary.query("select int_val from wal_test.st")
        primary_data = result.fetch_all()
        result = conn_secondary.query("select int_val from wal_success.st")
        secondary_success_data = result.fetch_all()

        more = set(primary_data) - set(secondary_success_data)
        miss = set(secondary_success_data) - set(primary_data)
        print("more rows numbers : ", len(more))
        print("miss rows numbers : ", len(miss))

        if len(miss) > 0:
            print(" there are some records was missed , case failed ")
            sys.exit(1)

    def basic_single_row(self):
        sleep_time = self.sleep_time
        loops = self.loops
        self.prepare_db_stable()
        thread_pool = []
        thread_insert = MyThread(func=self.basic_insert_task, args=('tb',))
        thread_kill_instance = MyThread(func=self.restart_primary_taosd, args=(sleep_time, loops))
        thread_pool.append(thread_insert)
        thread_pool.append(thread_kill_instance)

        # run task
        for task in thread_pool:
            task.start()
        thread_insert.join()
        thread_kill_instance.join()

    def basic_multi_insert_rows(self):

        sleep_time = self.sleep_time
        loops = self.loops
        self.prepare_db_stable()
        thread_pool = []
        thread_insert = MyThread(func=self.multi_insert_task, args=('tb',))
        thread_kill_instance = MyThread(func=self.restart_primary_taosd, args=(sleep_time, loops))
        thread_pool.append(thread_insert)
        thread_pool.append(thread_kill_instance)

        # run task
        for task in thread_pool:
            task.start()
        thread_insert.join()
        thread_kill_instance.join()

    def thread_pools_basic_insert(self):
        self.prepare_db_stable()
        thread_pool = []
        sleep_time = self.sleep_time
        loops = self.loops

        thread_kill_instance = MyThread(func=self.restart_primary_taosd, args=(sleep_time, loops))
        thread_kill_instance.start()
        for ids in range(self.thread_nums):
            tbname = "tb_%d" % ids
            thread_insert_ins = MyThread(func=self.basic_insert_task, args=(tbname,))
            thread_pool.append(thread_insert_ins)

        # run task
        index = 0
        for task in thread_pool:
            task.start()
            print("======== thread %d is start ======" % index)
            index += 1

        thread_kill_instance.join()
        for task in thread_pool:
            task.join()

    def thread_pools_multi_insert(self):
        self.prepare_db_stable()
        thread_pool = []
        sleep_time = self.sleep_time
        loops = self.loops
        thread_kill_instance = MyThread(func=self.restart_primary_taosd, args=(sleep_time, loops))
        thread_kill_instance.start()
        for ids in range(self.thread_nums):
            tbname = "tb_%d" % ids
            thread_insert_ins = MyThread(func=self.multi_insert_task, args=(tbname,))
            thread_pool.append(thread_insert_ins)
        # run task
        for task in thread_pool:
            task.start()

        thread_kill_instance.join()
        for task in thread_pool:
            task.join()

    def run(self) -> bool:

        start = time.time()

        self.thread_pools_multi_insert()
        time.sleep(3)
        self.compare_data()  # please use small data to compare
        end = time.time()

        print("total run time cost : %.3f  mins " % (float(end - start) / 60))

    def cleanup(self):
        pass

    def author(self):
        '''
        abstract about author
        '''
        return "wenzhouwww"

    def tags(self):
        '''
        set tags
        '''
        return "abnormal", "wal"

    def desc(self) -> str:
        case_description = '''
            [TD-13654]<wenzhouwww> test wal safety for taosd restart ;
        '''
        return case_description
