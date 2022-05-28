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
import threading as thd
import multiprocessing as mp
from numpy.lib.function_base import insert
import taos
from taos import *
from util.log import *
from util.cases import *
from util.sql import *
import numpy as np
import datetime as dt
from datetime import datetime
from ctypes import *
import time
# constant define
WAITS = 5 # wait seconds

class TDTestCase:
    #
    # --------------- main frame -------------------
    def caseDescription(self):
        '''
        limit and offset keyword function test cases;
        case1: limit offset base function test
        case2: offset return valid
        '''
        return 

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root)-len("/build/bin")]
                    break
        return buildPath

    # init
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        # tdSql.prepare()
        # self.create_tables();
        self.ts = 1500000000000

    # stop 
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


    # --------------- case  -------------------


    def newcon(self,host,cfg):
        user = "root"
        password = "taosdata"
        port =6030 
        con=taos.connect(host=host, user=user, password=password, config=cfg ,port=port)
        print(con)
        return con

    def test_stmt_insert_multi(self,conn):
        # type: (TaosConnection) -> None

        dbname = "pytest_taos_stmt_multi"
        try:
            conn.execute("drop database if exists %s" % dbname)
            conn.execute("create database if not exists %s" % dbname)
            conn.select_db(dbname)

            conn.execute(
                "create table if not exists log(ts timestamp, bo bool, nil tinyint, ti tinyint, si smallint, ii int,\
                bi bigint, tu tinyint unsigned, su smallint unsigned, iu int unsigned, bu bigint unsigned, \
                ff float, dd double, bb binary(100), nn nchar(100), tt timestamp)",
            )
            # conn.load_table_info("log")

            start = datetime.now()
            stmt = conn.statement("insert into log values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

            params = new_multi_binds(16)
            params[0].timestamp((1626861392589, 1626861392590, 1626861392591))
            params[1].bool((True, None, False))
            params[2].tinyint([-128, -128, None]) # -128 is tinyint null
            params[3].tinyint([0, 127, None])
            params[4].smallint([3, None, 2])
            params[5].int([3, 4, None])
            params[6].bigint([3, 4, None])
            params[7].tinyint_unsigned([3, 4, None])
            params[8].smallint_unsigned([3, 4, None])
            params[9].int_unsigned([3, 4, None])
            params[10].bigint_unsigned([3, 4, None])
            params[11].float([3, None, 1])
            params[12].double([3, None, 1.2])
            params[13].binary(["abc", "dddafadfadfadfadfa", None])
            params[14].nchar(["涛思数据", None, "a long string with 中文字符"])
            params[15].timestamp([None, None, 1626861392591])
            # print(type(stmt))
            stmt.bind_param_batch(params)
            stmt.execute()
            end = datetime.now()
            print("elapsed time: ", end - start)
            assert stmt.affected_rows == 3
            
            #query
            querystmt=conn.statement("select ?,bu from log")
            queryparam=new_bind_params(1)
            print(type(queryparam))
            queryparam[0].binary("ts")
            querystmt.bind_param(queryparam)
            querystmt.execute() 
            result=querystmt.use_result()
            rows=result.fetch_all()
            print( querystmt.use_result())

            # result = conn.query("select * from log")
            # rows=result.fetch_all()
            # rows=result.fetch_all()
            print(rows)
            assert rows[1][0] == "ts"
            assert rows[0][1] == 3

            #query
            querystmt1=conn.statement("select * from log where bu < ?")
            queryparam1=new_bind_params(1)
            print(type(queryparam1))
            queryparam1[0].int(4)
            querystmt1.bind_param(queryparam1)
            querystmt1.execute() 
            result1=querystmt1.use_result()
            rows1=result1.fetch_all()
            assert str(rows1[0][0]) == "2021-07-21 17:56:32.589000"
            assert rows1[0][10] == 3


            stmt.close()

            # conn.execute("drop database if exists %s" % dbname)
            conn.close()

        except Exception as err:
            # conn.execute("drop database if exists %s" % dbname)
            conn.close()
            raise err
        
    def test_stmt_set_tbname_tag(self,conn):
        dbname = "pytest_taos_stmt_set_tbname_tag"
        
        try:
            conn.execute("drop database if exists %s" % dbname)
            conn.execute("create database if not exists %s PRECISION 'us' "  % dbname)
            conn.select_db(dbname)
            conn.execute("create table if not exists log(ts timestamp, bo bool, nil tinyint, ti tinyint, si smallint, ii int,\
                bi bigint, tu tinyint unsigned, su smallint unsigned, iu int unsigned, bu bigint unsigned, \
                ff float, dd double, bb binary(100), nn nchar(100), tt timestamp) tags (t1 timestamp, t2 bool,\
                t3 tinyint, t4 tinyint, t5 smallint, t6 int, t7 bigint, t8 tinyint unsigned, t9 smallint unsigned, \
                t10 int unsigned, t11 bigint unsigned, t12 float, t13 double, t14 binary(100), t15 nchar(100), t16 timestamp)")
            
            stmt = conn.statement("insert into ? using log tags (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) \
                values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
            tags = new_bind_params(16)
            tags[0].timestamp(1626861392589123, PrecisionEnum.Microseconds)
            tags[1].bool(True)
            tags[2].null()
            tags[3].tinyint(2)
            tags[4].smallint(3)
            tags[5].int(4)
            tags[6].bigint(5)
            tags[7].tinyint_unsigned(6)
            tags[8].smallint_unsigned(7)
            tags[9].int_unsigned(8)
            tags[10].bigint_unsigned(9)
            tags[11].float(10.1)
            tags[12].double(10.11)
            tags[13].binary("hello")
            tags[14].nchar("stmt")
            tags[15].timestamp(1626861392589, PrecisionEnum.Milliseconds)
            stmt.set_tbname_tags("tb1", tags)
            params = new_multi_binds(16)
            params[0].timestamp((1626861392589111,  1626861392590111, 1626861392591111))
            params[1].bool((True, None, False))
            params[2].tinyint([-128, -128, None]) # -128 is tinyint null
            params[3].tinyint([0, 127, None])
            params[4].smallint([3, None, 2])
            params[5].int([3, 4, None])
            params[6].bigint([3, 4, None])
            params[7].tinyint_unsigned([3, 4, None])
            params[8].smallint_unsigned([3, 4, None])
            params[9].int_unsigned([3, 4, None])
            params[10].bigint_unsigned([3, 4, 5])
            params[11].float([3, None, 1])
            params[12].double([3, None, 1.2])
            params[13].binary(["abc", "dddafadfadfadfadfa", None])
            params[14].nchar(["涛思数据", None, "a long string with 中文字符"])
            params[15].timestamp([None, None, 1626861392591])
            
            stmt.bind_param_batch(params)
            stmt.execute()

            assert stmt.affected_rows == 3

            #query
            querystmt1=conn.statement("select * from log where bu < ?")
            queryparam1=new_bind_params(1)
            print(type(queryparam1))
            queryparam1[0].int(5)
            querystmt1.bind_param(queryparam1)
            querystmt1.execute() 
            result1=querystmt1.use_result()
            rows1=result1.fetch_all()
            assert str(rows1[0][0]) == "2021-07-21 17:56:32.589111"
            assert rows1[0][10] == 3
            assert rows1[1][10] == 4

            # conn.execute("drop database if exists %s" % dbname)
            conn.close()
        
        except Exception as err:
            # conn.execute("drop database if exists %s" % dbname)
            conn.close()
            raise err

    def run(self):
        buildPath = self.getBuildPath()
        config = buildPath+ "../sim/dnode1/cfg/"
        host="localhost"
        connectstmt=self.newcon(host,config)
        print(connectstmt)
        self.test_stmt_insert_multi(connectstmt)
        connectstmt=self.newcon(host,config)
        self.test_stmt_set_tbname_tag(connectstmt)

        return 


# add case with filename
#
tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())