# #################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.

#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao

# #################################################################

# -*- coding: utf-8 -*-

# TODO: after TD-4518 and TD-4510 is resolved, add the exception test case for these situations

from distutils.log import error
import sys

from requests.api import head
from requests.models import Response
from util.log import *
from util.cases import *
from util.sql import *
import time, datetime
import requests, json
import threading
import string
import random
import re
 

null = ''
true= 'true'
false = 'false'
def caseDescription(self):
    '''
    case1<jiacy-jcy>:dbname binding
    case2<jiacy-jcy>:dbname without binding

    '''
def check_unbind_db(url, data, header):
     resp = requests.post(url, data, headers = header )
     resp.encoding='utf-8'
     resp = eval(resp.text)
     status = resp['status']
     #cod = resp['code']
     sqls = data
     if status=="error" :#and cod == 401:
         print(" %s : check pass" %sqls)
     else:
         printf("%s error occured , " %sqls)
         sys.exit(1)

def check_bind_db(url, data, header):
    resp = requests.post(url, data, headers = header )
    resp.encoding='utf-8'
    resp_dict = eval(resp.text)
    status = resp_dict['status']
    if status =="succ":
        print("%s  run success!"%data)
        # print(resp.text)
    else :
        print("%s run failed !"%data)
        print(resp.text)
        sys.exit(1)
        
class TDTestCase():
     

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        
    def run(self):
        tdSql.prepare()
        tdSql.execute('reset query cache')
        tdSql.execute('drop database if exists test')
        tdSql.execute('drop database if exists db')
        tdSql.execute('drop database if exists test01')
        tdSql.execute('create database test')
        tdSql.execute('create database test01')

        header = {'Authorization': 'Basic cm9vdDp0YW9zZGF0YQ=='}
        url = "http://127.0.0.1:6041/rest/sql"

        
        # case 1: test with no bind dbname
        sqls1 = ["show databases;",
        "use test;", 
        "show dnodes;",
        "create database db;",
        "drop database db;",
        "select client_version();" ,
        "ALTER DATABASE test COMP 2;",
        "show test.tables",
        "create table test.tb (ts timestamp, id int , data double)",
        "insert into test.tb values (now , 2, 2.0) ",
        "select * from test.tb"
        ]
        sqls2 = ["show tables;",
        "show vgroups;",
        "create table tb (ts timestamp, id int , data double)",
        "insert into tb values (now , 1, 1.0) ",
        "select * from tb",
        "insert into tb values (now , 2, 2.0) ",
        "select * from tb"
        ]
        
        print("==================="*5)
        print("                        check unbind db about restful                  ")
        print("==================="*5)
        for sql in sqls1:
            print("===================")
            check_bind_db(url,sql,header)
        
        for sql in sqls2:
            print("===================")
            check_unbind_db(url,sql,header)

        tdSql.execute('drop database if exists test01')
        tdSql.execute('drop database if exists test')
        tdSql.execute('create database test')
        tdSql.execute('create database test01')

        #case 2: test with bind dbname
        sqls3 = ["show databases;",
        "use test;", 
        "show tables;",
        "show dnodes;",
        "show vgroups;",
        "create database db;",
        "drop database db;",
        "select client_version();" ,
        "use test",
        "ALTER DATABASE test COMP 2;",
        "create table tb (ts timestamp, id int , data double)",
        "insert into tb values (now , 1, 1.0) ",
        "select * from tb",
        "show test.tables",
        "show tables",
        "insert into tb values (now , 2, 2.0) ",
        "create table test.tb1 (ts timestamp, id int , data double)",
        "insert into test.tb1 values (now , 2, 2.0) ",
        "select * from tb",
        "select * from test.tb1"
        ]

        
        print("==================="*5)
        print("                        check bind db about restful                  ")
        print("==================="*5)
        url = "http://127.0.0.1:6041/rest/sql/test"
        for sql in sqls3:
            print("===================")
            
            check_bind_db(url,sql,header)
        # check data
        tdSql.query("select * from test.tb")
        tdSql.checkRows(2)

        os.system('sudo timedatectl set-ntp on')

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
