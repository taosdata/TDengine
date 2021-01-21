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
import threading
import taos
import sys
import json
import time
import random
import requests
import argparse
import datetime
import string
from requests.auth import HTTPBasicAuth
func_list=['avg','count','twa','sum','stddev','leastsquares','min',
'max','first','last','top','bottom','percentile','apercentile',
'last_row','diff','spread']
condition_list=[
    "where _c0 > now -10d ",
    'interval(10s)',
    'limit 10',
    'group by',
    'order by',
    'fill(null)'
    
]
where_list = ['_c0>now-10d',' <50','like',' is null']
class ConcurrentInquiry:
    # def __init__(self,ts=1500000001000,host='127.0.0.1',user='root',password='taosdata',dbname='test',
    #             stb_prefix='st',subtb_prefix='t',n_Therads=10,r_Therads=10,probabilities=0.05,loop=5,
    #             stableNum = 2,subtableNum = 1000,insertRows = 100):  
    def __init__(self,ts,host,user,password,dbname,
                stb_prefix,subtb_prefix,n_Therads,r_Therads,probabilities,loop,
                stableNum ,subtableNum ,insertRows ,mix_table):  
        self.n_numOfTherads = n_Therads
        self.r_numOfTherads = r_Therads
        self.ts=ts
        self.host = host
        self.user = user
        self.password = password
        self.dbname=dbname
        self.stb_prefix = stb_prefix
        self.subtb_prefix = subtb_prefix
        self.stb_list=[]
        self.subtb_list=[]
        self.stb_stru_list=[]
        self.subtb_stru_list=[]
        self.stb_tag_list=[]
        self.subtb_tag_list=[]
        self.probabilities = [1-probabilities,probabilities]
        self.ifjoin = [1,0]
        self.loop = loop
        self.stableNum = stableNum
        self.subtableNum = subtableNum
        self.insertRows = insertRows
        self.mix_table = mix_table
        self.max_ts = datetime.datetime.now()
        self.min_ts = datetime.datetime.now() - datetime.timedelta(days=5)
    def SetThreadsNum(self,num):
        self.numOfTherads=num

    def ret_fcol(self,cl,sql):                     #返回结果的第一列
        cl.execute(sql)
        fcol_list=[]
        for data in cl:
            fcol_list.append(data[0])
        return fcol_list

    def r_stb_list(self,cl):                    #返回超级表列表
        sql='show '+self.dbname+'.stables'
        self.stb_list=self.ret_fcol(cl,sql)

    def r_subtb_list(self,cl,stablename):       #每个超级表返回2个子表
        sql='select tbname from '+self.dbname+'.'+stablename+' limit 2;'
        self.subtb_list+=self.ret_fcol(cl,sql)

    def cal_struct(self,cl,tbname):             #查看表结构
        tb=[]
        tag=[]
        sql='describe '+self.dbname+'.'+tbname+';'
        cl.execute(sql)
        for data in cl:
            if data[3]:
                tag.append(data[0])
            else:
                tb.append(data[0])
        return tb,tag

    def r_stb_stru(self,cl):                    #获取所有超级表的表结构
        for i in self.stb_list:
            tb,tag=self.cal_struct(cl,i)
            self.stb_stru_list.append(tb)
            self.stb_tag_list.append(tag)

    def r_subtb_stru(self,cl):                  #返回所有子表的表结构
        for i in self.subtb_list:
            tb,tag=self.cal_struct(cl,i)
            self.subtb_stru_list.append(tb)
            self.subtb_tag_list.append(tag)

    def get_timespan(self,cl):                  #获取时间跨度(仅第一个超级表)
        sql = 'select first(_c0),last(_c0) from ' + self.dbname + '.' + self.stb_list[0] + ';'
        print(sql)
        cl.execute(sql)
        for data in cl:
            self.max_ts = data[1]
            self.min_ts = data[0]

    def get_full(self):                         #获取所有的表、表结构
        host = self.host
        user = self.user
        password = self.password
        conn = taos.connect(
            host,
            user,
            password,
            )
        cl = conn.cursor()
        self.r_stb_list(cl)
        for i in self.stb_list:
            self.r_subtb_list(cl,i)
        self.r_stb_stru(cl)
        self.r_subtb_stru(cl)
        self.get_timespan(cl)
        cl.close()
        conn.close()  
        
    #query condition
    def con_where(self,tlist,col_list,tag_list):                               
        l=[]
        for i in range(random.randint(0,len(tlist))):
            c = random.choice(where_list)
            if c == '_c0>now-10d':
                rdate = self.min_ts + (self.max_ts - self.min_ts)/10 * random.randint(-11,11)
                conlist = ' _c0 ' + random.choice(['<','>','>=','<=','<>']) + "'" + str(rdate) + "'"
                if self.random_pick():
                    l.append(conlist)
                else: l.append(c)
            elif '<50' in c:
                conlist = ' ' + random.choice(tlist) + random.choice(['<','>','>=','<=','<>']) + str(random.randrange(-100,100))
                l.append(conlist) 
            elif 'is null' in c:
                conlist = ' ' + random.choice(tlist) + random.choice([' is null',' is not null'])
                l.append(conlist) 
            else:
                s_all = string.ascii_letters
                conlist = ' ' + random.choice(tlist) + " like \'%" + random.choice(s_all) + "%\' " 
                l.append(conlist)
        return 'where '+random.choice([' and ',' or ']).join(l)

    def con_interval(self,tlist,col_list,tag_list): 
        interval = 'interval(' + str(random.randint(0,20)) + random.choice(['a','s','d','w','n','y'])  + ')'          
        return interval

    def con_limit(self,tlist,col_list,tag_list):
        rand1 = str(random.randint(0,1000))
        rand2 = str(random.randint(0,1000))
        return random.choice(['limit ' + rand1,'limit ' + rand1 + ' offset '+rand2,
        ' slimit ' + rand1,' slimit ' + rand1 + ' offset ' + rand2,'limit '+rand1 + ' slimit '+ rand2,
        'limit '+ rand1 + ' offset' + rand2 + ' slimit '+ rand1 + ' soffset ' + rand2 ])
    
    def con_fill(self,tlist,col_list,tag_list):
        return random.choice(['fill(null)','fill(prev)','fill(none)','fill(LINEAR)'])
    
    def con_group(self,tlist,col_list,tag_list):
        rand_tag = random.randint(0,5)
        rand_col = random.randint(0,1)
        return 'group by '+','.join(random.sample(col_list,rand_col) + random.sample(tag_list,rand_tag))
    
    def con_order(self,tlist,col_list,tag_list):
        return 'order by '+random.choice(tlist)
    
    def gen_query_sql(self):                        #生成查询语句
        tbi=random.randint(0,len(self.subtb_list)+len(self.stb_list))  #随机决定查询哪张表
        tbname=''
        col_list=[]
        tag_list=[]
        is_stb=0
        if tbi>len(self.stb_list) :
            tbi=tbi-len(self.stb_list)
            tbname=self.subtb_list[tbi-1]
            col_list=self.subtb_stru_list[tbi-1]
            tag_list=self.subtb_tag_list[tbi-1]
        else:
            tbname=self.stb_list[tbi-1]
            col_list=self.stb_stru_list[tbi-1]
            tag_list=self.stb_tag_list[tbi-1]
            is_stb=1
        tlist=col_list+tag_list+['abc']            #增加不存在的域'abc'，是否会引起新bug
        con_rand=random.randint(0,len(condition_list))
        func_rand=random.randint(0,len(func_list))
        col_rand=random.randint(0,len(col_list))
        tag_rand=random.randint(0,len(tag_list))
        t_rand=random.randint(0,len(tlist))
        sql='select '                                           #select 
        random.shuffle(col_list)
        random.shuffle(func_list)
        sel_col_list=[]
        col_rand=random.randint(0,len(col_list))
        loop = 0
        for i,j in zip(col_list[0:col_rand],func_list):         #决定每个被查询col的函数
            alias = ' as '+ 'taos%d ' % loop
            loop += 1
            pick_func = ''
            if j == 'leastsquares':
                pick_func=j+'('+i+',1,1)'
            elif j == 'top' or j == 'bottom' or j == 'percentile' or j == 'apercentile':
                pick_func=j+'('+i+',1)'
            else:
                pick_func=j+'('+i+')'
            if bool(random.getrandbits(1)):
                pick_func+=alias
            sel_col_list.append(pick_func)
        if col_rand == 0:
            sql = sql + '*'   
        else: 
            sql=sql+','.join(sel_col_list)         #select col & func
        if self.mix_table == 0:
            sql = sql + ' from '+random.choice(self.stb_list+self.subtb_list)+' '         
        elif self.mix_table == 1:
            sql = sql + ' from '+random.choice(self.subtb_list)+' '
        else:
            sql = sql + ' from '+random.choice(self.stb_list)+' ' 
        con_func=[self.con_where,self.con_interval,self.con_limit,self.con_group,self.con_order,self.con_fill]
        sel_con=random.sample(con_func,random.randint(0,len(con_func)))
        sel_con_list=[]
        for i in sel_con:
            sel_con_list.append(i(tlist,col_list,tag_list))                                  #获取对应的条件函数
        sql+=' '.join(sel_con_list)                                       # condition
        #print(sql)
        return sql

    def gen_query_join(self):                        #生成join查询语句
        tbname   = []
        col_list = []
        tag_list = []
        col_intersection = []
        tag_intersection = []
        subtable = None
        if self.mix_table == 0:
            if bool(random.getrandbits(1)):
                subtable = True
                tbname = random.sample(self.subtb_list,2)
                for i in tbname:
                    col_list.append(self.subtb_stru_list[self.subtb_list.index(i)])
                    tag_list.append(self.subtb_stru_list[self.subtb_list.index(i)])
                col_intersection = list(set(col_list[0]).intersection(set(col_list[1])))
                tag_intersection = list(set(tag_list[0]).intersection(set(tag_list[1])))
            else:
                tbname = random.sample(self.stb_list,2)
                for i in tbname:
                    col_list.append(self.stb_stru_list[self.stb_list.index(i)])
                    tag_list.append(self.stb_stru_list[self.stb_list.index(i)])
                col_intersection = list(set(col_list[0]).intersection(set(col_list[1])))
                tag_intersection = list(set(tag_list[0]).intersection(set(tag_list[1])))
        elif self.mix_table == 1:
            subtable = True
            tbname = random.sample(self.subtb_list,2)
            for i in tbname:
                col_list.append(self.subtb_stru_list[self.subtb_list.index(i)])
                tag_list.append(self.subtb_stru_list[self.subtb_list.index(i)])
            col_intersection = list(set(col_list[0]).intersection(set(col_list[1])))
            tag_intersection = list(set(tag_list[0]).intersection(set(tag_list[1])))
        else:
            tbname = random.sample(self.stb_list,2)
            for i in tbname:
                col_list.append(self.stb_stru_list[self.stb_list.index(i)])
                tag_list.append(self.stb_stru_list[self.stb_list.index(i)])
            col_intersection = list(set(col_list[0]).intersection(set(col_list[1])))
            tag_intersection = list(set(tag_list[0]).intersection(set(tag_list[1])))
        con_rand=random.randint(0,len(condition_list))
        col_rand=random.randint(0,len(col_list))
        tag_rand=random.randint(0,len(tag_list))
        sql='select '                                           #select 
        
        sel_col_tag=[]
        col_rand=random.randint(0,len(col_list))
        if bool(random.getrandbits(1)):
            sql += '*'
        else:
            sel_col_tag.append('t1.' + str(random.choice(col_list[0] + tag_list[0])))
            sel_col_tag.append('t2.' + str(random.choice(col_list[1] + tag_list[1])))
            sel_col_list = []
            random.shuffle(func_list)
            if self.random_pick():
                loop = 0
                for i,j in zip(sel_col_tag,func_list):         #决定每个被查询col的函数
                    alias = ' as '+ 'taos%d ' % loop
                    loop += 1
                    pick_func = ''
                    if j == 'leastsquares':
                        pick_func=j+'('+i+',1,1)'
                    elif j == 'top' or j == 'bottom' or j == 'percentile' or j == 'apercentile':
                        pick_func=j+'('+i+',1)'
                    else:
                        pick_func=j+'('+i+')'
                    if bool(random.getrandbits(1)):
                        pick_func+=alias
                    sel_col_list.append(pick_func)
                sql += ','.join(sel_col_list)
            else:
                sql += ','.join(sel_col_tag)

        sql = sql + ' from '+ str(tbname[0]) +' t1,' + str(tbname[1]) + ' t2 '                        #select col & func
        join_section = None
        temp = None
        if subtable:
            temp = random.choices(col_intersection)
            join_section = temp.pop()
            sql += 'where t1._c0 = t2._c0 and ' + 't1.' + str(join_section) + '=t2.' + str(join_section)
        else:
            temp = random.choices(col_intersection+tag_intersection)
            join_section = temp.pop()
            sql += 'where t1._c0 = t2._c0 and ' + 't1.' + str(join_section) + '=t2.' + str(join_section)
        return sql

    def random_pick(self): 
        x = random.uniform(0,1) 
        cumulative_probability = 0.0 
        for item, item_probability in zip(self.ifjoin, self.probabilities): 
            cumulative_probability += item_probability 
            if x < cumulative_probability:break 
        return item
        
    def gen_data(self):
        stableNum = self.stableNum
        subtableNum = self.subtableNum
        insertRows = self.insertRows
        t0 = self.ts
        host = self.host
        user = self.user
        password = self.password
        conn = taos.connect(
            host,
            user,
            password,
            )
        cl = conn.cursor()
        cl.execute("drop database if  exists %s;" %self.dbname)
        cl.execute("create database if not exists %s;" %self.dbname)
        cl.execute("use %s" % self.dbname)
        for k in range(stableNum):
            sql="create table %s (ts timestamp, c1 int, c2 float, c3 bigint, c4 smallint, c5 tinyint, c6 double, c7 bool,c8 binary(20),c9 nchar(20)) \
            tags(t1 int, t2 float, t3 bigint, t4 smallint, t5 tinyint, t6 double, t7 bool,t8 binary(20),t9 nchar(20))" % (self.stb_prefix+str(k))
            cl.execute(sql)
            for j in range(subtableNum):
                sql = "create table %s using %s tags(%d,%d,%d,%d,%d,%d,%d,'%s','%s')" % \
                        (self.subtb_prefix+str(k)+'_'+str(j),self.stb_prefix+str(k),j,j/2.0,j%41,j%51,j%53,j*1.0,j%2,'taos'+str(j),'涛思'+str(j))
                print(sql)
                cl.execute(sql)
                for i in range(insertRows):
                    ret = cl.execute(
                        "insert into %s values (%d , %d,%d,%d,%d,%d,%d,%d,'%s','%s')" %
                        (self.subtb_prefix+str(k)+'_'+str(j),t0+i,i%100,i/2.0,i%41,i%51,i%53,i*1.0,i%2,'taos'+str(i),'涛思'+str(i)))
        cl.close()
        conn.close()
        
    def rest_query(self,sql):                                       #rest 接口
        host = self.host
        user = self.user
        password = self.password
        port =6041
        url = "http://{}:{}/rest/sql".format(host, port )
        try:
            r = requests.post(url, 
                data = 'use %s' % self.dbname,
                auth = HTTPBasicAuth('root', 'taosdata'))  
            r = requests.post(url, 
                data = sql,
                auth = HTTPBasicAuth('root', 'taosdata'))         
        except:
            print("REST API Failure (TODO: more info here)")
            raise
        rj = r.json()
        if ('status' not in rj):
            raise RuntimeError("No status in REST response")

        if rj['status'] == 'error':  # clearly reported error
            if ('code' not in rj):  # error without code
                raise RuntimeError("REST error return without code")
            errno = rj['code']  # May need to massage this in the future
            # print("Raising programming error with REST return: {}".format(rj))
            raise taos.error.ProgrammingError(
                rj['desc'], errno)  # todo: check existance of 'desc'

        if rj['status'] != 'succ':  # better be this
            raise RuntimeError(
                "Unexpected REST return status: {}".format(
                    rj['status']))

        nRows = rj['rows'] if ('rows' in rj) else 0
        return nRows

    
    def query_thread_n(self,threadID):                      #使用原生python接口查询
        host = self.host
        user = self.user
        password = self.password
        conn = taos.connect(
            host,
            user,
            password,
            )
        cl = conn.cursor()
        cl.execute("use %s;" % self.dbname)
        
        print("Thread %d: starting" % threadID)
        loop = self.loop
        while loop:
            
                try:
                    if self.random_pick():
                        sql=self.gen_query_sql()
                    else:
                        sql=self.gen_query_join()
                    print("sql is ",sql)
                    start = time.time()
                    cl.execute(sql)
                    cl.fetchall()
                    end = time.time()
                    print("time cost :",end-start)
                except Exception as e:
                    print('-'*40)
                    print(
                "Failure thread%d, sql: %s \nexception: %s" %
                (threadID, str(sql),str(e)))
                    err_uec='Unable to establish connection'
                    if err_uec in str(e) and loop >0:
                        exit(-1)
                loop -= 1
                if loop == 0: break
                    
        cl.close()
        conn.close()       
        print("Thread %d: finishing" % threadID)
          
    def query_thread_r(self,threadID):                      #使用rest接口查询
        print("Thread %d: starting" % threadID)
        loop = self.loop
        while loop:
            try:
                if self.random_pick():
                    sql=self.gen_query_sql()
                else:
                    sql=self.gen_query_join()
                print("sql is ",sql)
                start = time.time()
                self.rest_query(sql)
                end = time.time()
                print("time cost :",end-start)
            except Exception as e:
                print('-'*40)
                print(
            "Failure thread%d, sql: %s \nexception: %s" %
            (threadID, str(sql),str(e)))
                err_uec='Unable to establish connection'
                if err_uec in str(e) and loop >0:
                    exit(-1)
            loop -= 1    
            if loop == 0: break
                
        print("Thread %d: finishing" % threadID)    

    def run(self):
        print(self.n_numOfTherads,self.r_numOfTherads)  
        threads = []
        for i in range(self.n_numOfTherads):
            thread = threading.Thread(target=self.query_thread_n, args=(i,))
            threads.append(thread)
            thread.start()  
        for i in range(self.r_numOfTherads):
            thread = threading.Thread(target=self.query_thread_r, args=(i,))
            threads.append(thread)
            thread.start()
 
parser = argparse.ArgumentParser()
parser.add_argument(
    '-H',
    '--host-name',
    action='store',
    default='127.0.0.1',
    type=str,
    help='host name to be connected (default: 127.0.0.1)')
parser.add_argument(
    '-S',
    '--ts',
    action='store',
    default=1500000000000,
    type=int,
    help='insert data from timestamp (default: 1500000000000)')
parser.add_argument(
    '-d',
    '--db-name',
    action='store',
    default='test',
    type=str,
    help='Database name to be created (default: test)')
parser.add_argument(
    '-t',
    '--number-of-native-threads',
    action='store',
    default=10,
    type=int,
    help='Number of native threads (default: 10)')
parser.add_argument(
    '-T',
    '--number-of-rest-threads',
    action='store',
    default=10,
    type=int,
    help='Number of rest threads (default: 10)')
parser.add_argument(
    '-r',
    '--number-of-records',
    action='store',
    default=100,
    type=int,
    help='Number of record to be created for each table  (default: 100)')
parser.add_argument(
    '-c',
    '--create-table',
    action='store',
    default='0',
    type=int,
    help='whether gen data (default: 0)')
parser.add_argument(
    '-p',
    '--subtb-name-prefix',
    action='store',
    default='t',
    type=str,
    help='subtable-name-prefix (default: t)')
parser.add_argument(
    '-P',
    '--stb-name-prefix',
    action='store',
    default='st',
    type=str,
    help='stable-name-prefix (default: st)')
parser.add_argument(
    '-b',
    '--probabilities',
    action='store',
    default='0.05',
    type=float,
    help='probabilities of join (default: 0.05)')
parser.add_argument(
    '-l',
    '--loop-per-thread',
    action='store',
    default='100',
    type=int,
    help='loop per thread (default: 100)')
parser.add_argument(
    '-u',
    '--user',
    action='store', 
    default='root',
    type=str,
    help='user name')
parser.add_argument(
    '-w',
    '--password',
    action='store', 
    default='root',
    type=str,
    help='user name')
parser.add_argument(
    '-n',
    '--number-of-tables',
    action='store',
    default=1000,
    type=int,
    help='Number of subtales per stable (default: 1000)')
parser.add_argument(
    '-N',
    '--number-of-stables',
    action='store',
    default=2,
    type=int,
    help='Number of stables  (default: 2)')
parser.add_argument(
    '-m',
    '--mix-stable-subtable',
    action='store',
    default=0,
    type=int,
    help='0:stable & substable ,1:subtable ,2:stable (default: 0)')

args = parser.parse_args()
q = ConcurrentInquiry(
    args.ts,args.host_name,args.user,args.password,args.db_name,
                args.stb_name_prefix,args.subtb_name_prefix,args.number_of_native_threads,args.number_of_rest_threads,
                args.probabilities,args.loop_per_thread,args.number_of_stables,args.number_of_tables ,args.number_of_records,
                args.mix_stable_subtable )

if args.create_table: 
    q.gen_data()
q.get_full()

#q.gen_query_sql()
q.run()

