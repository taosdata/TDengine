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


import os 
import re
import time
import taos
from taostest import TDCase
from taostest.util.common import TDCom
import copy
import  threading

class TestWal(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.major_taosd_config = "/data/run/dnode0/config"
        self.sub_taosd_config = "/data/run/dnode1/config"
        self.ts = 1643644800000 # 2010-02-01 00:00:00
        self.time_step = 10;
        self.count = 0
        self.failed_conut = 0
        self.sleep_time = 10
        self.loops = 10
        self.save_fail_file = './wal_failed.log'
        self.failed_types_counts = './failed_types.txt'
        self.nums =10000
    def dbname_check(self):
        '''
            wal_testdb is the major taosd instance for random kill it 
        '''
        dbname = 'wal_testdb'
        pass

        # self.tdSql.execute(f'create database if not exists {dbname}')
        
        # self.tdSql.query('show databases')
        # res = self.tdSql.getOneRow(0, dbname)
        # self.tdSql.checkEqual(res[0][0], dbname)
    
    def kill_9_instance(self,pid_string):
        cmds = "ps -ef | grep '%s'|awk '{print $2}'|xargs kill -9 "%pid_string
        status_code = os.system(cmds)
        print(cmds)  
        if status_code!=0:
            print(" kill failed ,shell run status code is : " , status_code)
        else:
            print(" ==== kill success ====")

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
            self.start_instance(major_pid_string)
            

    def basic_insert_task(self):
        print(" insert task is going now ")
        conn_major = taos.connect(host="vm130", port=6030,user="root", password="taosdata", config="/data/run/dnode0/config")
        conn_sub = taos.connect(host="vm130", port=6130,user="root", password="taosdata", config="/data/run/dnode1/config")

        conn_major.execute("create database wal_test")
        conn_major.execute("use wal_test")
        conn_major.execute("create stable st (ts timestamp ,int_val int , double_val double) tags(name binary(20))")
        conn_major.execute("create table tb using st tags('test')")

        conn_sub.execute("create database wal_success")
        conn_sub.execute("create stable wal_success.st (ts timestamp ,int_val int , double_val double) tags(name binary(20))")
        conn_sub.execute("create table wal_success.tb using wal_success.st tags('test')")

        conn_sub.execute("create database wal_failed")
        conn_sub.execute("create stable wal_failed.st (ts timestamp ,int_val int , double_val double) tags(name binary(20))")
        conn_sub.execute("create table wal_failed.tb using wal_failed.st tags('test')")

        conn_sub.execute("create database wal_error")
        conn_sub.execute("create stable wal_error.st (ts timestamp ,int_val int , double_val double) tags(name binary(20))")
        conn_sub.execute("create table wal_error.tb using wal_error.st tags('test')")

        insert_sql_failed = []
        insert_errno_failed = []
        tablename = "tb"
        if os.path.exists(self.save_fail_file):
            os.system("rm %s" %(self.save_fail_file))
        
        with open(self.save_fail_file, 'a+') as f:
            
            for i in range(self.nums):

                dbname_list = ['wal_test' ,'wal_success','wal_failed','wal_error']
                ts = self.ts + self.count * self.time_step
                int_val = self.count
                double_val = self.count + 0.01
                dbname = dbname_list[0]
                insert_sql = f'insert into {dbname}.{tablename} values({ts} , {int_val}, {double_val})'
                self.count+=1
                flag = 0 
                try:
                    conn_major.execute(insert_sql)
                except taos.Error as err:
                    insert_sql_failed.append(insert_sql)
                    f.write('SQL:'+insert_sql+'\n')
                    insert_errno_failed.append(err.errno)
                    f.write('errno:'+str(err.errno)+'\n')
                    f.write('error type:'+str(err.msg)+'\n')
                    f.write('======= count:%d =======\n'%self.failed_conut)
                    if err.msg=="Database not ready":
                        flag =2
                    else :
                        flag = 1
                    self.failed_conut+=1
                    f.write('\n')
                    f.flush()
                    

                if flag == 0: # means insert sucess
                    sub_dbname =dbname_list[1] 
                elif flag ==1 :
                    sub_dbname =dbname_list[2] 
                elif flag ==2 :
                    sub_dbname =dbname_list[3]

                insert_sql=insert_sql.replace(dbname,sub_dbname)
                conn_sub.execute(insert_sql)

        f.close()
 
    def compare_data(self):

        more = set()
        miss = set()
        conn_major = taos.connect(host="vm130", port=6030,user="root", password="taosdata", config="/data/run/dnode0/config")
        conn_sub = taos.connect(host="vm130", port=6130,user="root", password="taosdata", config="/data/run/dnode1/config")
        result = conn_major.query("select int_val from wal_test.st")
        major_data = result.fetch_all()
        print(type(major_data) , len(major_data))

        result = conn_sub.query("select int_val from wal_success.st")
        sub_success_data = result.fetch_all()

        result = conn_sub.query("select int_val from wal_failed.st")
        sub_failed_data = result.fetch_all()

        result = conn_sub.query("select int_val from wal_error.st")
        sub_error_data = result.fetch_all()

        total_failed = sub_error_data+sub_failed_data

        more = set(major_data) - set(sub_success_data)
        miss = set(sub_success_data) - set(major_data)

        print("more", more)
        print("miss" , miss)
        



    def run(self) -> bool:
        self.dbname_check()


        # async threading run insert and kill instance major
        major_pid_string = "taosd -c /data/run/dnode0/config"
        
        # thread_pool = []
        # thread_insert = threading.Thread(target=self.basic_insert_task)
        # thread_kill_instance = threading.Thread(target=self.restart_major_taosd, args=(major_pid_string,self.sleep_time,self.loops))
        # thread_pool.append(thread_insert)
        # thread_pool.append(thread_kill_instance)

        # #run task
        # for task in thread_pool:
        #     task.start() 
        self.compare_data()
        

        
        
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
        return "abnormal ,wal"

    def desc(self) -> str:
        case_description = '''
            [TD-13654]<wenzhouwww> test wal safety for taosd restart ;
        '''
        return case_description