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

import json
import time
import random
# query sql
query_sql = [
# first supertable
"select count(*) from test.meters where c1 > 50;",
"select count(*) from test.meters where c2 >= 50 and c2 < 100;",
"select count(*) from test.meters where c3 != 5;",
"select count(*) from test.meters where t3 > 2;",
"select count(*) from test.meters where ts <> '2020-05-13 10:00:00.002';",
"select count(*) from test.meters where t7 like 'fi%';",
"select count(*) from test.meters where t7 like '_econd';",
"select count(*) from test.meters interval(1n) order by ts desc;",
"select first(*)  from test.meters;",
"select last(*)  from test.meters;",
"select last_row(*)  from test.meters;",
"select twa(c1) from test.t1 where ts > 1500000001000  and ts < 1500000101000" ,
"select avg(c1) from test.meters;",
"select bottom(c1, 2) from test.t1;",
"select diff(c1) from test.t1;",
"select leastsquares(c1, 1, 1) from test.t1  ;",
"select max(c1) from test.meters;",
"select min(c1) from test.meters;",
"select c1 + c2 * c3 + c1 / c5 + c4 + c2 from test.t1;",
"select percentile(c1, 50) from test.t1;",
"select spread(c1) from test.t1  ;",
"select stddev(c1) from test.t1;",
"select sum(c1) from test.meters;",
"select top(c1, 2) from test.meters;"
"select twa(c6) from test.t1 where ts > 1500000001000  and ts < 1500000101000" ,
"select avg(c6) from test.meters;",
"select bottom(c6, 2) from test.t1;",
"select diff(c6) from test.t1;",
"select leastsquares(c6, 1, 1) from test.t1  ;",
"select max(c6) from test.meters;",
"select min(c6) from test.meters;",
"select c6 + c2 * c3 + c6 / c5 + c4 + c2 from test.t1;",
"select percentile(c6, 50) from test.t1;",
"select spread(c6) from test.t1  ;",
"select stddev(c6) from test.t1;",
"select sum(c6) from test.meters;",
"select top(c6, 2) from test.meters;",
# second supertable
"select count(*) from test.meters1 where c1 > 50;",
"select count(*) from test.meters1 where c2 >= 50 and c2 < 100;",
"select count(*) from test.meters1 where c3 != 5;",
"select count(*) from test.meters1 where t3 > 2;",
"select count(*) from test.meters1 where ts <> '2020-05-13 10:00:00.002';",
"select count(*) from test.meters1 where t7 like 'fi%';",
"select count(*) from test.meters1 where t7 like '_econd';",
"select count(*) from test.meters1 interval(1n) order by ts desc;",
"select first(*)  from test.meters1;",
"select last(*)  from test.meters1;",
"select last_row(*)  from test.meters1;",
"select twa(c1) from test.m1 where ts > 1500000001000  and ts < 1500000101000" ,
"select avg(c1) from test.meters1;",
"select bottom(c1, 2) from test.m1;",
"select diff(c1) from test.m1;",
"select leastsquares(c1, 1, 1) from test.m1  ;",
"select max(c1) from test.meters1;",
"select min(c1) from test.meters1;",
"select c1 + c2 * c3 + c1 / c5 + c3 + c2 from test.m1;",
"select percentile(c1, 50) from test.m1;",
"select spread(c1) from test.m1  ;",
"select stddev(c1) from test.m1;",
"select sum(c1) from test.meters1;",
"select top(c1, 2) from test.meters1;",
"select twa(c6) from test.m1 where ts > 1500000001000  and ts < 1500000101000" ,
"select avg(c6) from test.meters1;",
"select bottom(c6, 2) from test.m1;",
"select diff(c6) from test.m1;",
"select leastsquares(c6, 1, 1) from test.m1  ;",
"select max(c6) from test.meters1;",
"select min(c6) from test.meters1;",
"select c6 + c2 * c3 + c6 / c5 + c3 + c2 from test.m1;",
"select percentile(c6, 50) from test.m1;",
"select spread(c6) from test.m1  ;",
"select stddev(c6) from test.m1;",
"select sum(c6) from test.meters1;",
"select top(c6, 2) from test.meters1;"
]

class ConcurrentInquiry:
    def initConnection(self):  
        self.numOfTherads = 50
        self.ts=1500000001000
        

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
        
        print("Thread %d: starting" % threadID)
        
        while True:
            ran_query_sql=query_sql
            random.shuffle(ran_query_sql)
            for i in ran_query_sql:
                print("Thread %d : %s"% (threadID,i))
                try:
                    cl.execute(i)
                    cl.fetchall
                except Exception as e:
                    print(
                "Failure thread%d, sql: %s,exception: %s" %
                (threadID, str(i),str(e)))
                    
                
            print("Thread %d: finishing" % threadID)
          
        

    def run(self):
       
        threads = []
        for i in range(50):
            thread = threading.Thread(target=self.query_thread, args=(i,))
            threads.append(thread)
            thread.start()  
        
q = ConcurrentInquiry()
q.initConnection()
q.run()
