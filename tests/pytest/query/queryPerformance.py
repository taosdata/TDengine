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


import sys
import os
import taos
import time


class taosdemoQueryPerformace:
    def initConnection(self):
        self.host = "127.0.0.1"
        self.user = "root"
        self.password = "taosdata"
        self.config = "/etc/taosperf"
        self.conn = taos.connect(
            self.host,
            self.user,
            self.password,
            self.config)


    def query(self): 
        cursor = self.conn.cursor()                  
        cursor.execute("use test")

        totalTime = 0
        for i in range(100):       
            if(sys.argv[1] == '1'):
                # root permission is required
                os.system("echo 3 > /proc/sys/vm/drop_caches")  
            startTime = time.time()            
            cursor.execute("select count(*) from test.meters")
            totalTime += time.time() - startTime
        print("query time for: select count(*) from test.meters %f seconds" % (totalTime / 100))

        totalTime = 0
        for i in range(100):
            if(sys.argv[1] == '1'):  
                # root permission is required              
                os.system("echo 3 > /proc/sys/vm/drop_caches")
            startTime = time.time()
            cursor.execute("select avg(f1), max(f2), min(f3) from test.meters")
            totalTime += time.time() - startTime
        print("query time for: select avg(f1), max(f2), min(f3) from test.meters %f seconds" % (totalTime / 100))

        totalTime = 0
        for i in range(100):
            if(sys.argv[1] == '1'):
                # root permission is required
                os.system("echo 3 > /proc/sys/vm/drop_caches")
            startTime = time.time()
            cursor.execute("select count(*) from test.meters where loc='beijing'")
            totalTime += time.time() - startTime
        print("query time for: select count(*) from test.meters where loc='beijing' %f seconds" % (totalTime / 100))

        totalTime = 0
        for i in range(100):
            if(sys.argv[1] == '1'):
                # root permission is required              
                os.system("echo 3 > /proc/sys/vm/drop_caches")
            startTime = time.time()
            cursor.execute("select avg(f1), max(f2), min(f3) from test.meters where areaid=10")
            totalTime += time.time() - startTime
        print("query time for: select avg(f1), max(f2), min(f3) from test.meters where areaid=10 %f seconds" % (totalTime / 100))

        totalTime = 0
        for i in range(100):
            if(sys.argv[1] == '1'):                
                # root permission is required
                os.system("echo 3 > /proc/sys/vm/drop_caches")
            startTime = time.time()
            cursor.execute("select avg(f1), max(f2), min(f3) from test.t10 interval(10s)")
            totalTime += time.time() - startTime
        print("query time for: select avg(f1), max(f2), min(f3) from test.t10 interval(10s) %f seconds" % (totalTime / 100))

        totalTime = 0
        for i in range(100):
            if(sys.argv[1] == '1'):
                # root permission is required           
                os.system("echo 3 > /proc/sys/vm/drop_caches")
            startTime = time.time()
            cursor.execute("select last_row(*) from meters")
            totalTime += time.time() - startTime
        print("query time for: select last_row(*) from meters %f seconds" % (totalTime / 100))

        totalTime = 0
        for i in range(100):
            if(sys.argv[1] == '1'):
                # root permission is required           
                os.system("echo 3 > /proc/sys/vm/drop_caches")
            startTime = time.time()
            cursor.execute("select * from meters")
            totalTime += time.time() - startTime
        print("query time for: select * from meters %f seconds" % (totalTime / 100))

        totalTime = 0
        for i in range(100):
            if(sys.argv[1] == '1'):
                # root permission is required        
                os.system("echo 3 > /proc/sys/vm/drop_caches")
            startTime = time.time()
            cursor.execute("select avg(f1), max(f2), min(f3) from meters where ts <= '2017-07-15 10:40:01.000' and ts <= '2017-07-15 14:00:40.000'")
            totalTime += time.time() - startTime
        print("query time for: select avg(f1), max(f2), min(f3) from meters where ts <= '2017-07-15 10:40:01.000' and ts <= '2017-07-15 14:00:40.000' %f seconds" % (totalTime / 100))

if __name__ == '__main__':
    perftest = taosdemoQueryPerformace()
    perftest.initConnection()
    perftest.query()