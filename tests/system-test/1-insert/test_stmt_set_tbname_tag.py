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
            if ("taosd" in files or "taosd.exe" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root)-len("/build/bin")]
                    break
        return buildPath

    # init
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
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
        tdLog.debug(con)
        return con

    def stmtExe(self,conn,sql,bindStat):
            queryStat=conn.statement("%s"%sql)
            queryStat.bind_param(bindStat)
            queryStat.execute()
            result=queryStat.use_result()
            rows=result.fetch_all()
            return rows

    def test_stmt_set_tbname_tag(self,conn):
        dbname = "stmt_tag"
        stablename = 'log'
        try:
            conn.execute("drop database if exists %s" % dbname)
            conn.execute("create database if not exists %s PRECISION 'us' "  % dbname)
            conn.select_db(dbname)
            conn.execute("create table if not exists %s(ts timestamp, bo bool, nil tinyint, ti tinyint, si smallint, ii int,\
                bi bigint, tu tinyint unsigned, su smallint unsigned, iu int unsigned, bu bigint unsigned, \
                ff float, dd double, bb binary(100), nn nchar(100), tt timestamp , vc varchar(100)) tags (t1 timestamp, t2 bool,\
                t3 tinyint, t4 tinyint, t5 smallint, t6 int, t7 bigint, t8 tinyint unsigned, t9 smallint unsigned, \
                t10 int unsigned, t11 bigint unsigned, t12 float, t13 double, t14 binary(100), t15 nchar(100), t16 timestamp)"%stablename)

            stmt = conn.statement("insert into ? using log tags (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) \
                values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
            tags = new_bind_params(16)
            tags[0].timestamp(1626861392589123, PrecisionEnum.Microseconds)
            tags[1].bool(True)
            tags[2].bool(False)
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
            params = new_multi_binds(17)
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
            params[14].nchar(["涛思数据", None, "a long string with 中文?字符"])
            params[15].timestamp([None, None, 1626861392591])
            params[16].binary(["涛思数据16", None, None])

            stmt.bind_param_batch(params)
            stmt.execute()

            assert stmt.affected_rows == 3

            #query all
            queryparam=new_bind_params(1)
            queryparam[0].int(10)
            rows=self.stmtExe(conn,"select * from log where bu < ?",queryparam)
            tdLog.debug("assert 1st case %s"%rows)
            assert str(rows[0][0]) == "2021-07-21 17:56:32.589111"
            assert rows[0][10] == 3 , '1st case is failed'
            assert rows[1][10] == 4 , '1st case is failed'

            #query: Numeric Functions
            queryparam=new_bind_params(2)
            queryparam[0].int(5)
            queryparam[1].int(5)
            rows=self.stmtExe(conn,"select abs(?) from log where bu < ?",queryparam)
            tdLog.debug("assert 2nd case %s"%rows)
            assert rows[0][0] == 5 , '2nd case is failed'
            assert rows[1][0] == 5 , '2nd case is failed'


            #query: Numeric Functions and escapes
            queryparam=new_bind_params(1)
            queryparam[0].int(5)
            rows=self.stmtExe(conn,"select abs(?) from log where  nn= 'a? long string with 中文字符'",queryparam)
            tdLog.debug("assert 3rd case %s"%rows)
            assert rows == [] , '3rd case is failed'

            #query: string Functions
            queryparam=new_bind_params(1)
            queryparam[0].binary('中文字符')
            rows=self.stmtExe(conn,"select CHAR_LENGTH(?) from log ",queryparam)
            tdLog.debug("assert 4th case %s"%rows)
            assert rows[0][0] == 12, '4th case is failed'
            assert rows[1][0] == 12, '4th case is failed'

            queryparam=new_bind_params(1)
            queryparam[0].binary('123')
            rows=self.stmtExe(conn,"select CHAR_LENGTH(?) from log ",queryparam)
            tdLog.debug("assert 4th case %s"%rows)
            assert rows[0][0] == 3, '4th.1 case is failed'
            assert rows[1][0] == 3, '4th.1 case is failed'

            #query: conversion Functions
            queryparam=new_bind_params(1)
            queryparam[0].binary('1232a')
            rows=self.stmtExe(conn,"select cast( ? as bigint) from log",queryparam)
            tdLog.debug("assert 5th case %s"%rows)
            assert rows[0][0] == 1232, '5th.1 case is failed'
            assert rows[1][0] == 1232, '5th.1 case is failed'

            querystmt4=conn.statement("select cast( ? as binary(10)) from log  ")
            queryparam=new_bind_params(1)
            queryparam[0].int(123)
            rows=self.stmtExe(conn,"select cast( ? as bigint) from log",queryparam)
            tdLog.debug("assert 6th case %s"%rows)
            assert rows[0][0] == 123, '6th.1 case is failed'
            assert rows[1][0] == 123, '6th.1 case is failed'

            #query: datatime Functions
            queryparam=new_bind_params(1)
            queryparam[0].timestamp(1626861392591112)
            rows=self.stmtExe(conn,"select timediff('2021-07-21 17:56:32.590111',?,1a)  from log",queryparam)
            tdLog.debug("assert 7th case %s"%rows)
            assert rows[0][0] == 1, '7th case is failed'
            assert rows[1][0] == 1, '7th case is failed'

            #query: aggregate Functions
            queryparam=new_bind_params(1)
            queryparam[0].int(123)
            rows=self.stmtExe(conn,"select count(?)  from log ",queryparam)
            tdLog.debug("assert 8th case %s"%rows)
            assert rows[0][0] == 3, ' 8th case is failed'

            #query: selector Functions 9
            queryparam=new_bind_params(1)
            queryparam[0].int(2)
            rows=self.stmtExe(conn,"select bottom(bu,?)  from log group by bu  order by bu desc ; ",queryparam)
            tdLog.debug("assert 9th case %s"%rows)
            assert rows[1][0] == 4, ' 9 case is failed'
            assert rows[2][0] == 3, ' 9 case is failed'

            # #query: time-series specific Functions 10

            querystmt=conn.statement("  select twa(?)  from log;  ")
            queryparam=new_bind_params(1)
            queryparam[0].int(15)
            rows=self.stmtExe(conn," select twa(?)  from log; ",queryparam)
            tdLog.debug("assert 10th case %s"%rows)
            assert rows[0][0] == 15, ' 10th case is failed'


            # conn.execute("drop database if exists %s" % dbname)
            conn.close()
            tdLog.success("%s successfully executed" % __file__)

        except Exception as err:
            # conn.execute("drop database if exists %s" % dbname)
            conn.close()
            raise err

    def run(self):
        buildPath = self.getBuildPath()
        config = buildPath+ "../sim/dnode1/cfg/"
        host="localhost"
        connectstmt=self.newcon(host,config)
        self.test_stmt_set_tbname_tag(connectstmt)

        return


# add case with filename
#
tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
