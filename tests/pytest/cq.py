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
# query sql
query_sql = [
# first supertable
"select count(*) from test.meters ;",
"select count(*) from test.meters where t3 > 2;",
"select count(*) from test.meters where ts <> '2020-05-13 10:00:00.002';",
"select count(*) from test.meters where t7 like 'taos_1%';",
"select count(*) from test.meters where t7 like '_____2';",
"select count(*) from test.meters where t8 like '%思%';",
"select count(*) from test.meters interval(1n) order by ts desc;",
#"select max(c0) from test.meters group by tbname",
"select first(ts)  from test.meters where t5 >5000 and t5<5100;",
"select last(ts)  from test.meters where t5 >5000 and t5<5100;",
"select last_row(*)  from test.meters;",
"select twa(c1) from test.t1 where ts > 1500000001000  and ts < 1500000101000" ,
"select avg(c1) from test.meters where t5 >5000 and t5<5100;",
"select bottom(c1, 2) from test.t1;",
"select diff(c1) from test.t1;",
"select leastsquares(c1, 1, 1) from test.t1  ;",
"select max(c1) from test.meters where t5 >5000 and t5<5100;",
"select min(c1) from test.meters where t5 >5000 and t5<5100;",
"select c1 + c2 + c1 / c5 + c4 + c2 from test.t1;",
"select percentile(c1, 50) from test.t1;",
"select spread(c1) from test.t1  ;",
"select stddev(c1) from test.t1;",
"select sum(c1) from test.meters where t5 >5000 and t5<5100;",
"select top(c1, 2) from test.meters where t5 >5000 and t5<5100;"
"select twa(c4) from test.t1 where ts > 1500000001000  and ts < 1500000101000" ,
"select avg(c4) from test.meters where t5 >5000 and t5<5100;",
"select bottom(c4, 2) from test.t1 where t5 >5000 and t5<5100;",
"select diff(c4) from test.t1 where t5 >5000 and t5<5100;",
"select leastsquares(c4, 1, 1) from test.t1  ;",
"select max(c4) from test.meters where t5 >5000 and t5<5100;",
"select min(c4) from test.meters where t5 >5000 and t5<5100;",
"select c5 + c2  + c4 / c5 + c4 + c2 from test.t1 ;",
"select percentile(c5, 50) from test.t1;",
"select spread(c5) from test.t1  ;",
"select stddev(c5) from test.t1 where t5 >5000 and t5<5100;",
"select sum(c5) from test.meters where t5 >5000 and t5<5100;",
"select top(c5, 2) from test.meters where t5 >5000 and t5<5100;",
#all vnode
"select count(*) from test.meters where t5 >5000 and t5<5100",       
"select max(c0),avg(c1) from test.meters where t5 >5000 and t5<5100",
"select sum(c5),avg(c1) from test.meters where t5 >5000 and t5<5100",
"select max(c0),min(c5) from test.meters where t5 >5000 and t5<5100",
"select min(c0),avg(c5) from test.meters where t5 >5000 and t5<5100",
# second supertable
"select count(*) from test.meters1 where t3 > 2;",
"select count(*) from test.meters1 where ts <> '2020-05-13 10:00:00.002';",
"select count(*) from test.meters where t7 like 'taos_1%';",
"select count(*) from test.meters where t7 like '_____2';",
"select count(*) from test.meters where t8 like '%思%';",
"select count(*) from test.meters1 interval(1n) order by ts desc;",
#"select max(c0) from test.meters1 group by tbname",
"select first(ts)  from test.meters1 where t5 >5000 and t5<5100;",
"select last(ts)  from test.meters1 where t5 >5000 and t5<5100;",
"select last_row(*)  from test.meters1 ;",
"select twa(c1) from test.m1 where ts > 1500000001000  and ts < 1500000101000" ,
"select avg(c1) from test.meters1 where t5 >5000 and t5<5100;",
"select bottom(c1, 2) from test.m1 where t5 >5000 and t5<5100;",
"select diff(c1) from test.m1 ;",
"select leastsquares(c1, 1, 1) from test.m1  ;",
"select max(c1) from test.meters1 where t5 >5000 and t5<5100;",
"select min(c1) from test.meters1 where t5 >5000 and t5<5100;",
"select c1 + c2 + c1 / c0 + c2 from test.m1 ;",
"select percentile(c1, 50) from test.m1;",
"select spread(c1) from test.m1  ;",
"select stddev(c1) from test.m1;",
"select sum(c1) from test.meters1 where t5 >5000 and t5<5100;",
"select top(c1, 2) from test.meters1 where t5 >5000 and t5<5100;",
"select twa(c5) from test.m1 where ts > 1500000001000  and ts < 1500000101000" ,
"select avg(c5) from test.meters1 where t5 >5000 and t5<5100;",
"select bottom(c5, 2) from test.m1;",
"select diff(c5) from test.m1;",
"select leastsquares(c5, 1, 1) from test.m1  ;",
"select max(c5) from test.meters1 where t5 >5000 and t5<5100;",
"select min(c5) from test.meters1 where t5 >5000 and t5<5100;",
"select c5 + c2  + c4 / c5  + c0 from test.m1;",
"select percentile(c4, 50) from test.m1;",
"select spread(c4) from test.m1  ;",
"select stddev(c4) from test.m1;",
"select sum(c4) from test.meters1 where t5 >5100 and t5<5300;",
"select top(c4, 2) from test.meters1 where t5 >5100 and t5<5300;",
"select count(*) from test.meters1 where t5 >5100 and t5<5300",
#all vnode
"select count(*) from test.meters1 where t5 >5100 and t5<5300",       
"select max(c0),avg(c1) from test.meters1 where t5 >5000 and t5<5100",
"select sum(c5),avg(c1) from test.meters1 where t5 >5000 and t5<5100",
"select max(c0),min(c5) from test.meters1 where t5 >5000 and t5<5100",
"select min(c0),avg(c5) from test.meters1 where t5 >5000 and t5<5100",
#join 
# "select * from meters,meters1 where meters.ts = meters1.ts and meters.t5 = meters1.t5",
# "select * from meters,meters1 where meters.ts = meters1.ts and meters.t7 = meters1.t7",
# "select * from meters,meters1 where meters.ts = meters1.ts and meters.t8 = meters1.t8",
# "select meters.ts,meters1.c2 from meters,meters1 where meters.ts = meters1.ts and meters.t8 = meters1.t8"
]

class ConcurrentInquiry:
    def initConnection(self):  
        self.numOfTherads = 50
        self.ts=1500000001000
        
    def SetThreadsNum(self,num):
        self.numOfTherads=num
    def query_thread(self,threadID):
        host = "10.211.55.14"
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
            ran_query_sql=query_sql
            random.shuffle(ran_query_sql)
            for i in ran_query_sql:
                print("Thread %d : %s"% (threadID,i))
                try:
                    start = time.time()
                    cl.execute(i)
                    cl.fetchall()
                    end = time.time()
                    print("time cost :",end-start)
                except Exception as e:
                    print(
                "Failure thread%d, sql: %s,exception: %s" %
                (threadID, str(i),str(e)))
                    exit(-1)
                    
                
            print("Thread %d: finishing" % threadID)
          
        

    def run(self):
            
        threads = []
        for i in range(self.numOfTherads):
            thread = threading.Thread(target=self.query_thread, args=(i,))
            threads.append(thread)
            thread.start()  
        
q = ConcurrentInquiry()
q.initConnection()
q.run()
