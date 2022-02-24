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


from glob import glob
import os 
import re
import time
import taos
import subprocess
from taostest import TDCase
from taostest.util.common import TDCom
import copy
import  threading

class MyThread(threading.Thread):
    def __init__(self, func, args=()):
        super(MyThread, self).__init__()
        self.func = func
        self.args = args
        self.end_status=1
         
 
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

        self.major_conf = {"host":"vm130", "port":6030,"user":"root", 
        "password":"taosdata", "config":"/data/run/dnode0/config"}

        self.sub_conf = {"host":"vm130", "port":6130,"user":"root", 
        "password":"taosdata", "config":"/data/run/dnode1/config"}

        self.ts = 1643644800000 # 2010-02-01 00:00:00
        self.time_step = 10;
        self.count = 0
        self.failed_conut = 0
        self.sleep_time = 10
        self.loops = 10
        self.nums =300000 
        self.batch = 500
        self.thread_nums = 100
        

    def get_conn(self,conf): # conf is an dict
        conn = taos.connect(host=conf["host"], port=conf["port"],user=conf["user"], password=conf["password"], config=conf["config"])
        return conn
    
    def kill_9_instance(self,pid_string):
        cmds = "nohup ps -ef | grep '%s'|awk '{print $2}'|xargs kill -9 > killout.log 2>&1 >>/dev/null"%pid_string
        status_code = os.system(cmds)
        print('show kill cmds :  ',cmds)  
        print("kill taosd_major instance ")

    def start_instance(self,cmds):
        status_code = os.system( "screen -d -m {}  ".format(cmds)) 
        if status_code!=0:
            print(" start failed ,shell run status code is : " , status_code)
        else:
            print(" ==== start success ====")

    def restart_major_taosd(self,major_pid_string,sleep_time,loops):

        # if match sleep time ,kill -9 major taosd instance
        
        for loop in range(loops):
        
            time.sleep(sleep_time)
            print("this is the %d_th kill major taosd instance"%loop)
            
            # do kill
            self.kill_9_instance(major_pid_string)
            time.sleep(3)
            # self.start_instance(major_pid_string)
            cmds = "taosd -c "+ major_pid_string
            os.system( "screen -d -m {}  ".format(cmds)) 
    
    def prepare_db_stable(self):
        conn_major = self.get_conn(self.major_conf)
        conn_sub = self.get_conn(self.sub_conf)

        conn_major.execute("create database wal_test")
        conn_major.execute("use wal_test")
        
        conn_major.execute("create stable st (ts timestamp ,int_val int , double_val double ,\
         err_no binary(30) , err_msg binary(100)) tags(name binary(20))")
        
        conn_sub.execute("create database wal_success")
        conn_sub.execute("create stable wal_success.st (ts timestamp ,int_val int , double_val double ,\
         err_no binary(30) , err_msg binary(100)) tags(name binary(20))")
        
        conn_sub.execute("create database wal_failed")
        conn_sub.execute("create stable wal_failed.st (ts timestamp ,int_val int , double_val double , \
            err_no binary(30) , err_msg binary(100)) tags(name binary(20))")
        
        conn_sub.execute("create database wal_error")
        conn_sub.execute("create stable wal_error.st (ts timestamp ,int_val int , double_val double ,\
             err_no binary(30) , err_msg binary(100)) tags(name binary(20))")
    
            
    def multi_insert_task(self,tbname):

        print(" ======= multi insert task is going now ======= ")
        
        tablename = tbname
        dbname_list = ['wal_test' ,'wal_success','wal_failed','wal_error']
        conn_major = self.get_conn(self.major_conf)
        conn_sub = self.get_conn(self.sub_conf)

        row_length = 0
        dbname = dbname_list[0]

        multi_sqls = f'insert into {dbname}.{tablename} using {dbname}.st tags("test") values'

        for i in range(self.nums):   # 500 rows for a batch  default

            ts = self.ts + self.count * self.time_step
            int_val = self.count
            double_val = self.count + 0.01
            dbname = dbname_list[0]

            err_no = "null"
            err_msg ="null"

            insert_sql = f'insert into {dbname}.{tablename} using {dbname}.st tags("test") values({ts} , {int_val}, \
            {double_val}, "{err_no}","{err_msg}")'

            # print(insert_sql)

            if i %self.batch == 0 :
                # print("insert is going ,insert rows :  ", i)
                replace_body = f'insert into {dbname}.{tablename} using {dbname}.st tags("test") values'
                body = insert_sql.replace(replace_body , "")
                multi_sqls = multi_sqls + body
                # print(multi_sqls) 
                

                row_length = len(insert_sql)
                if len(multi_sqls)>row_length: # avoid first multi_sqls only 1 rows 
                    flag = 0 
                    try:
                        conn_major.execute(multi_sqls)
                    except taos.Error as err: 
                        err_no = "\"" + str(err.errno) + "\""
                        err_msg = "\"" +  str(err.msg) + "\""
                        print(err_msg)
                        # multi_sqls.replace('"null"' ,err_no ,1 )
                        multi_sqls.replace('"null"' ,err_msg)
                        if err.msg=="Database not ready":
                            flag =2
                        else :
                            flag = 1
                        self.failed_conut+=1
                    if flag == 0: # means insert sucess
                        sub_dbname =dbname_list[1] 
                    elif flag ==1 :
                        sub_dbname =dbname_list[2] 
                    elif flag ==2 :
                        sub_dbname =dbname_list[3]

                    multi_sqls=multi_sqls.replace(dbname,sub_dbname)
                    conn_sub.execute(multi_sqls)

            else :
                replace_body =  f'insert into {dbname}.{tablename} using {dbname}.st tags("test") values'
                body = insert_sql.replace(replace_body , "")
                multi_sqls += body
            
            self.count+=1

        if len(multi_sqls)>row_length: # last rows to insert
            
            flag = 0 
            try:
                conn_major.execute(multi_sqls)
            except taos.Error as err: 
                err_no = "\"" + str(err.errno) + "\""
                err_msg = "\"" +  str(err.msg) + "\""
                print(err_msg)
                # multi_sqls.replace("null" ,err_no ,1 )
                multi_sqls.replace("null" ,err_msg )
                
                if err.msg=="Database not ready":
                    flag =2
                else :
                    flag = 1
                self.failed_conut+=1
            if flag == 0: # means insert sucess
                sub_dbname =dbname_list[1] 
            elif flag ==1 :
                sub_dbname =dbname_list[2] 
            elif flag ==2 :
                sub_dbname =dbname_list[3]

            multi_sqls=multi_sqls.replace(dbname,sub_dbname)
            conn_sub.execute(multi_sqls)

     
    def basic_insert_task(self,tbname):
        print(" ======= insert task is going now ======= ")
        
        tablename = tbname
        dbname_list = ['wal_test' ,'wal_success','wal_failed','wal_error']
        conn_major = self.get_conn(self.major_conf)
        conn_sub = self.get_conn(self.sub_conf)
      
        for i in range(self.nums):

            ts = self.ts + self.count * self.time_step
            int_val = self.count
            double_val = self.count + 0.01
            dbname = dbname_list[0]
            err_no = "null"
            err_msg ="null"
            insert_sql = f'insert into {dbname}.{tablename} using {dbname}.st tags("test") values({ts} , {int_val}, \
            {double_val}, {err_no},{err_msg})'
            
            flag = 0 
            try:
                conn_major.execute(insert_sql)
            except taos.Error as err: 
                err_no = "\"" + str(err.errno) + "\""
                err_msg = "\"" +  str(err.msg) + "\""
                insert_sql = f'insert into {dbname}.{tablename} using {dbname}.st tags("test") values({ts} , {int_val}, \
            {double_val}, {err_no},{err_msg})'
                if err.msg=="Database not ready":
                    flag =2
                else :
                    flag = 1
                self.failed_conut+=1

            self.count+=1 

            if flag == 0: # means insert sucess
                sub_dbname =dbname_list[1] 
            elif flag ==1 :
                sub_dbname =dbname_list[2] 
            elif flag ==2 :
                sub_dbname =dbname_list[3]

            insert_sql=insert_sql.replace(dbname,sub_dbname)
            conn_sub.execute(insert_sql)  
 
    def compare_data(self):

        more = set()
        miss = set()

        conn_major = self.get_conn(self.major_conf)
        conn_sub = self.get_conn(self.sub_conf)

        result = conn_major.query("select int_val from wal_test.st")
        major_data = result.fetch_all()

        result = conn_sub.query("select int_val from wal_success.st")
        sub_success_data = result.fetch_all()

        result = conn_sub.query("select int_val from wal_failed.st")
        sub_failed_data = result.fetch_all()

        result = conn_sub.query("select int_val from wal_error.st")
        sub_error_data = result.fetch_all()

        total_failed = sub_error_data+sub_failed_data

        more = set(major_data) - set(sub_success_data)
        miss = set(sub_success_data) - set(major_data)

        # print("more  rows ", more)
        print("more rows numbers : ", len(more))
        
        # print("miss  rows" , miss)
        print("miss rows numbers : ", len(miss))
        
    def basic_single_row(self , major_pid_string):
        sleep_time = self.sleep_time
        loops = self.loops
        self.prepare_db_stable()
        thread_pool = []
        thread_insert = MyThread(func=self.basic_insert_task,args=('tb',))
        thread_kill_instance = MyThread(func=self.restart_major_taosd, args=(major_pid_string,sleep_time,loops))
        thread_pool.append(thread_insert)
        thread_pool.append(thread_kill_instance)

        #run task
        for task in thread_pool:
            task.start() 
        thread_insert.join()
        thread_kill_instance.join()
    
    def basic_multi_insert_rows(self , major_pid_string):
        
        sleep_time = self.sleep_time
        loops = self.loops
        self.prepare_db_stable()
        thread_pool = []
        thread_insert = MyThread(func=self.multi_insert_task ,args=('tb',))
        thread_kill_instance = MyThread(func=self.restart_major_taosd, args=(major_pid_string,sleep_time,loops))
        thread_pool.append(thread_insert)
        thread_pool.append(thread_kill_instance)

        #run task
        for task in thread_pool:
            task.start() 
        thread_insert.join()
        thread_kill_instance.join()

    def thread_pools_basic_insert(self ,major_pid_string):
        self.prepare_db_stable()
        thread_pool = []

        sleep_time = self.sleep_time
        loops = self.loops
        
        thread_kill_instance = MyThread(func=self.restart_major_taosd, args=(major_pid_string,sleep_time,loops))
        thread_kill_instance.start()
        for ids in range(self.thread_nums):
            tbname = "tb_%d"%ids
            thread_insert_ins = MyThread(func=self.basic_insert_task,args=(tbname,))
            thread_pool.append(thread_insert_ins)

         #run task
        index = 0 
        for task in thread_pool:
            task.start()
            print("======== thread %d is start ======"%index)
            index +=1

     
        thread_kill_instance.join()
        for task in thread_pool:
            task.join()

    def thread_pools_multi_insert(self ,major_pid_string):
        self.prepare_db_stable()
        thread_pool = []

        sleep_time = self.sleep_time
        loops = self.loops
        
        thread_kill_instance = MyThread(func=self.restart_major_taosd, args=(major_pid_string,sleep_time,loops))
        thread_kill_instance.start()
        for ids in range(self.thread_nums):
            tbname = "tb_%d"%ids
            thread_insert_ins = MyThread(func=self.multi_insert_task,args=(tbname,))
            thread_pool.append(thread_insert_ins)

         #run task
        for task in thread_pool:
            task.start()
     
        thread_kill_instance.join()
        for task in thread_pool:
            task.join()

    def run(self) -> bool:
        
        start = time.time()

        # async threading run insert and kill instance major
        major_pid_string = self.major_conf["config"]
        
        
        # self.thread_pools_multi_insert(major_pid_string)
        self.thread_pools_basic_insert(major_pid_string)
        time.sleep(3)
        self.compare_data()  # please use small data to compare
        end = time.time()

        print("total run time cost : %.3f  mins " %(float(end - start)/60) )
        
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
        return "abnormal" ,"wal"

    def desc(self) -> str:
        case_description = '''
            [TD-13654]<wenzhouwww> test wal safety for taosd restart ;
        '''
        return case_description