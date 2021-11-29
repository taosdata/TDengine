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

class CompressTest:
    # def __init__(self,ts=1500000001000,host='127.0.0.1',user='root',password='taosdata',dbname='test',
    #             stb_prefix='st',subtb_prefix='t',n_Therads=10,r_Therads=10,probabilities=0.05,loop=5,
    #             stableNum = 2,subtableNum = 1000,insertRows = 100):  
    def __init__(self):
        self.host = 'vm95'   
        self.ts = 1569859200000
        self.dbname = 'db'
        self.user = 'root'
        self.password = 'taosdata'
        self.loop  = 6

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
        tstart = time.time()
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
        for loop in range(7):
                ts = 1569859200000+loop*86400000
                try:
                    # sql = 'select ts,c0 from stb_%d where ts >%d and ts <%d' % (threadID, ts, ts+86400000)
                    sql = 'select ts,c1 from stb_%d where ts >%d and ts <%d' % (threadID, ts, ts+86400000)
                    # print("sql is ",sql)
                    # start = time.time()
                    cl.execute(sql)
                    cl.fetchall()
                    # end = time.time()
                    # print("time cost :",end-start)
                except Exception as e:
                    print('-'*40)
                    print(
                "Failure thread%d, sql: %s \nexception: %s" %
                (threadID, str(sql),str(e)))
                    err_uec='Unable to establish connection'
                    if err_uec in str(e) and loop >0:
                        exit(-1)   
        cl.close()
        conn.close()       
        tstop = time.time()
        print("Thread %d: finishing, time collaps:%d " % (threadID, tstop - tstart))

    
    def run(self):
        threads = []
        for i in range(10):
            thread = threading.Thread(target=self.query_thread_n, args=(i,))
            threads.append(thread)
            thread.start()  
        # for i in range(1,11):
        #     thread = threading.Thread(target=self.query_thread_r, args=(i,))
        #     threads.append(thread)
        #     thread.start()
 
q = CompressTest()
q.run()
