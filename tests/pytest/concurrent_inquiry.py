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
where_list = ['_c0>now-10d',' <50'," like \'%a%\'"]
class ConcurrentInquiry:
    def __init__(self,n_Therads=25,r_Therads=25):  
        self.n_numOfTherads = n_Therads
        self.r_numOfTherads = r_Therads
        self.ts=1500000001000
        self.dbname='test'
        self.stb_list=[]
        self.subtb_list=[]
        self.stb_stru_list=[]
        self.subtb_stru_list=[]
        self.stb_tag_list=[]
        self.subtb_tag_list=[]
        self.probabilities = [0.95,0.05]
        self.ifjoin = [0,1]
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

    def get_full(self):                         #获取所有的表、表结构
        host = "127.0.0.1"
        user = "root"
        password = "taosdata"
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
        cl.close()
        conn.close()  
        
    #query condition
    def con_where(self,tlist,col_list,tag_list):                               
        l=[]
        for i in range(random.randint(0,len(tlist))):
            c = random.choice(where_list)
            if c == '_c0>now-10d':
                l.append(c)
            else:
                l.append(random.choice(tlist)+c)
        return 'where '+random.choice([' and ',' or ']).join(l)

    def con_interval(self,tlist,col_list,tag_list): 
        interval = 'interval' + str(random.randint(0,100)) + random.choice(['a','s','d','w','n','y'])            
        return interval

    def con_limit(self,tlist,col_list,tag_list):
        rand1 = str(random.randint(0,1000))
        rand2 = str(random.randint(0,1000))
        return random.choice(['limit ' + rand1,'limit ' + rand1 + 'offset '+rand2,
        'slimit ' + rand1,'slimit ' + rand1 + 'offset ' + rand2,'limit '+rand1 + 'slimit '+ rand2,
        'limit '+ rand1 + 'offset' + rand2 + 'slimit '+ rand1 + 'soffset ' + rand2 ])
    
    def con_fill(self,tlist,col_list,tag_list):
        return random.choice(['fill(null)','fill(prev)','fill(none)','fill(LINEAR)'])
    
    def con_group(self,tlist,col_list,tag_list):
        rand_tag = random.randint(0,5)
        rand_col = random.randint(0,1)
        return 'group by '+','.join(random.sample(col_list,rand_col))+','.join(random.sample(tag_list,rand_tag))
    
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
        for i,j in zip(col_list[0:col_rand],func_list):         #决定每个被查询col的函数
            alias = 'as '+ str(i)
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
            
        sql=sql+','.join(sel_col_list)+' from '+random.choice(self.stb_list+self.subtb_list)+' '                        #select col & func
        con_func=[self.con_where,self.con_interval,self.con_limit,self.con_group,self.con_order,self.con_fill]
        sel_con=random.sample(con_func,random.randint(0,len(con_func)))
        sel_con_list=[]
        for i in sel_con:
            sel_con_list.append(i(tlist,col_list,tag_list))                                  #获取对应的条件函数
        sql+=' '.join(sel_con_list)                                       # condition
        print(sql)
        return sql

    def gen_query_join(self):                        #生成join查询语句
        tbname   = []
        col_list = []
        tag_list = []
        col_intersection = []
        tag_intersection = []
        

        if bool(random.getrandbits(1)):
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
            sql += ','.join(sel_col_tag)

        sql = sql + 'from '+ ','.join(tbname) + ' '                        #select col & func
        con_func=[self.con_where,self.con_interval,self.con_limit,self.con_group,self.con_order,self.con_fill]
        sel_con=random.sample(con_func,random.randint(0,len(con_func)))
        sel_con_list=[]
        
        # for i in sel_con:
        #     sel_con_list.append(i(tlist,col_list,tag_list))                                  #获取对应的条件函数
        sql+=' '.join(sel_con_list)                                       # condition
        print(sql)
        return sql

    def random_pick(self): 
        x = random.uniform(0,1) 
        cumulative_probability = 0.0 
        for item, item_probability in zip(self.ifjoin, self.probabilities): 
            cumulative_probability += item_probability 
            if x < cumulative_probability:break 
        return item
        

    def rest_query(self,sql):                                       #rest 接口
        host = "127.0.0.1"
        user = "root"
        password = "taosdata"
        port =6041
        url = "http://{}:{}/rest/sql".format(host, port )
        try:
            r = requests.post(url, 
                data = 'use test',
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
        host = "127.0.0.1"
        user = "root"
        password = "taosdata"
        conn = taos.connect(
            host,
            user,
            password,
            )
        cl = conn.cursor()
        cl.execute("use test;")
        
        print("Thread %d: starting" % threadID)
        
        while True:
            
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
                    print(
                "Failure thread%d, sql: %s,exception: %s" %
                (threadID, str(sql),str(e)))
                    #exit(-1)
                    
                
        print("Thread %d: finishing" % threadID)
          
    def query_thread_r(self,threadID):                      #使用rest接口查询
        print("Thread %d: starting" % threadID)
        while True:
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
                    print(
                "Failure thread%d, sql: %s,exception: %s" %
                (threadID, str(sql),str(e)))
                    #exit(-1)
                    
                
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
if len(sys.argv)>1:
    q = ConcurrentInquiry(n_Therads=sys.argv[1],r_Therads=sys.argv[2])
else:
    q = ConcurrentInquiry()
q.get_full()
#q.gen_query_sql()
q.run()
