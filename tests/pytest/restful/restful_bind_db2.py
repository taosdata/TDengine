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

def check_res(url, data, header):
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
        sys.exit()
        
class TDTestCase():
    # updatecfgDict={'httpDbNameMandatory':0}  

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()
        tdSql.execute('reset query cache')
        tdSql.execute('drop database if exists test')
        tdSql.execute('drop database if exists db')
        tdSql.execute('drop database if exists des')
        tdSql.execute('create database test')
        tdSql.execute('create database des')
        header = {'Authorization': 'Basic cm9vdDp0YW9zZGF0YQ=='}
        url = "http://127.0.0.1:6041/rest/sql/"

        # test with no bind databases

        sqls = ["select * from information_schema.ins_databases;",
        "use test", 
        "show tables;",
        "select * from information_schema.ins_dnodes;",
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
        "create table test.tb (ts timestamp, id int , data double)",
        "insert into test.tb values (now , 3, 3.0) ",
        "select * from tb",
        "select * from test.tb",
        "create table des.tb (ts timestamp, id int , data double)",
        "insert into des.tb values (now , 3, 3.0)"]
        for sql in sqls:
            print("===================")
            if sql == "create table test.tb (ts timestamp, id int , data double)":
                resp = requests.post(url, sql, headers = header )
                print(resp.text)
                print ("%s run occur error as expect ,check pass!" %(sql))
            else:
                check_res(url,sql,header)

        tdSql.query("select * from test.tb")
        tdSql.checkRows(3)
        tdSql.query("select * from des.tb")
        tdSql.checkRows(1)

        print("==================="*5)
        print("                        check bind db about restful                  ")
        print("==================="*5)

        tdSql.execute('reset query cache')
        tdSql.execute('drop database if exists test')
        tdSql.execute('drop database if exists db')
        tdSql.execute('drop database if exists des')
        tdSql.execute('create database test')
        tdSql.execute('create database des')
        
        url = "http://127.0.0.1:6041/rest/sql/des"
        for sql in sqls:
            print("===================")
            if sql in ["create table des.tb (ts timestamp, id int , data double)"]:
                resp = requests.post(url, sql, headers = header )
                print(resp.text)
                print ("%s run occur error as expect ,check pass!" %(sql))
            else:
                check_res(url,sql,header)
        # check data
        tdSql.query("select * from test.tb")
        tdSql.checkRows(1)
        tdSql.query("select * from des.tb")
        tdSql.checkRows(3)

        os.system('sudo timedatectl set-ntp on')

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
