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
import numpy as np
import datetime as dt
from datetime import datetime
from ctypes import *
from taos.constants import FieldType


from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *

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

    # compare
    def compare_result(self, conn, sql2, res2):
        lres1 = []
        lres2 = []
    
        # shor res2
        for row in res2:
            tdLog.debug(f" res2 rows = {row} \n")
            lres2.append(row)

        res1 = conn.query(sql2)
        for row in res1:
            tdLog.debug(f" res1 rows = {row} \n")
            lres1.append(row)

        row1 = len(lres1)
        row2 = len(lres2)
        col1 = len(lres1[0])
        col2 = len(lres2[0])

        # check number
        if row1 != row2:
            tdLog.exit(f"two results row count different. row1={row1} row2={row2}")
        if col1 != col2:
            tdLog.exit(f"two results column count different. col1={col1} col2={col2}")

        for i in range(row1):
            for j in range(col1):
                if lres1[i][j] != lres2[i][j]:
                    tdLog.exit(f" two results data different. i={i} j={j} data1={res1[i][j]} data2={res2[i][j]}\n")
            
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
        queryStat=conn.statement2("%s"%sql)
        queryStat.bind_param(bindStat)
        queryStat.execute()
        result=queryStat.result()
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

            stmt2 = conn.statement2("insert into ? using log tags (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) \
                values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

            # bind data
            tbanmes = ["t1"]
            tags    = [
                [1601481600000, True, False, 2, 3, 4, 5, 6, 7, 8, 9, 10.1, 10.11, "hello", "stmt", 1626861392589]
            ]

            # prepare data
            datas = [
                # table 1
                [
                    # student
                    [1626861392589111,1626861392590111,1626861392591111],
                    [True,         None,            False],
                    [-128,         -128,            None],
                    [0,            127,             None],
                    [3,            None,            2],
                    [3,            4,               None],
                    [3,            4,               None],
                    [3,            4,               None],
                    [3,            4,               None],
                    [3,            4,               None],
                    [3,            4,               5],
                    [3,            None,            1],
                    [3,            None,            1.2],
                    ["abc",       "dddafadfadfadf", None],
                    ["涛思数据", None, "a long string with 中文?字符"],
                    [None, None, 1626861392591],
                    ["涛思数据16", None, None]
                    
                ]
            ]

            table0 = BindTable(tbanmes[0], tags[0])

            for data in datas[0]:
                table0.add_col_data(data)

            # columns type for stable
            stmt2.bind_param_with_tables([table0])
            stmt2.execute()

            assert stmt2.affected_rows == 3
            tdLog.info("Insert data is done")

            # query1: all
            tbanmes = None
            tags    = None
            
            stmt2.prepare("select * from log where bu < ?")
            datas   = [
                [
                    [10]
                ]
            ]

            # set param
            types = [FieldType.C_BIGINT]
            stmt2.set_columns_type(types)

            # bind
            stmt2.bind_param(tbanmes, tags, datas)

            stmt2.execute()
            # get result
            rows = stmt2.result()

            self.compare_result(conn, "select * from log where bu < 10", rows)
            tdLog.info("[query1: all] PASS")
            
            # query2: Numeric Functions
            stmt2.prepare("select abs(?) from log where bu < ?")
            datas   = [
                [
                    [5],
                    [5]
                ]
            ]

            # set param
            types = [FieldType.C_BIGINT, FieldType.C_BIGINT]
            stmt2.set_columns_type(types)

            # bind
            stmt2.bind_param(tbanmes, tags, datas)

            stmt2.execute()
            # get result
            rows = stmt2.result()

            self.compare_result(conn, "select abs(5) from log where bu < 5", rows)
            tdLog.info("[query2: Numeric Functions] PASS")

            # query3: Numeric Functions and escapes
            stmt2.prepare("select abs(?) from log where  nn= 'a? long string with 中文字符'")
            datas   = [
                [
                    [5]
                ]
            ]

            # set param
            types = [FieldType.C_BIGINT]
            stmt2.set_columns_type(types)

            # bind
            stmt2.bind_param(tbanmes, tags, datas)

            stmt2.execute()
            # get result
            rows = stmt2.result()
            assert rows.row_count == 0 , '3rd case is failed'
            tdLog.info("[query3: Numeric Functions and escapes] PASS")

            # query4-1: string Functions
            stmt2.prepare("select CHAR_LENGTH(?) from log")
            datas   = [
                [
                    ['中文字符']
                ]
            ]

            # set param
            types = [FieldType.C_VARCHAR]
            stmt2.set_columns_type(types)

            # bind
            stmt2.bind_param(tbanmes, tags, datas)

            stmt2.execute()
            # get result
            rows = stmt2.result()

            self.compare_result(conn, "select CHAR_LENGTH('中文字符') from log", rows)
            tdLog.info("[query4-1: string Functions] PASS")

            # query4-2: string Functions
            stmt2.prepare("select CHAR_LENGTH(?) from log")
            datas   = [
                [
                    ['123']
                ]
            ]

            # set param
            types = [FieldType.C_VARCHAR]
            stmt2.set_columns_type(types)

            # bind
            stmt2.bind_param(tbanmes, tags, datas)

            stmt2.execute()
            # get result
            rows = stmt2.result()

            self.compare_result(conn, "select CHAR_LENGTH('123') from log", rows)
            tdLog.info("[query4-2: string Functions] PASS")

            # query5-1: conversion Functions
            stmt2.prepare("select cast( ? as bigint) from log")
            datas   = [
                [
                    ['1232a']
                ]
            ]

            # set param
            types = [FieldType.C_VARCHAR]
            stmt2.set_columns_type(types)

            # bind
            stmt2.bind_param(tbanmes, tags, datas)

            stmt2.execute()
            # get result
            rows = stmt2.result()

            self.compare_result(conn, "select cast( '1232a' as bigint) from log", rows)
            tdLog.info("[query5-1: conversion Functions] PASS")

            # query5-2: conversion Functions
            stmt2.prepare("select cast( ? as binary(10)) from log")
            datas   = [
                [
                    [123]
                ]
            ]

            # set param
            types = [FieldType.C_INT]
            stmt2.set_columns_type(types)

            # bind
            stmt2.bind_param(tbanmes, tags, datas)

            stmt2.execute()
            # get result
            rows = stmt2.result()

            self.compare_result(conn, "select cast( 123 as binary(10)) from log", rows)
            tdLog.info("[query5-2: conversion Functions] PASS")

            # query6: datatime Functions
            stmt2.prepare("select timediff('2021-07-21 17:56:32.590111', ?, 1a) from log")
            datas   = [
                [
                    [1626861392591112]
                ]
            ]

            # set param
            types = [FieldType.C_TIMESTAMP]
            stmt2.set_columns_type(types)

            # bind
            stmt2.bind_param(tbanmes, tags, datas)

            stmt2.execute()
            # get result
            rows = stmt2.result()

            self.compare_result(conn, "select timediff('2021-07-21 17:56:32.590111', 1626861392591112, 1a) from log", rows)
            tdLog.info("[query6: datatime Functions] PASS")

            # query7: aggregate Functions
            stmt2.prepare("select count(?) from log")
            datas   = [
                [
                    [123]
                ]
            ]

            # set param
            types = [FieldType.C_INT]
            stmt2.set_columns_type(types)

            # bind
            stmt2.bind_param(tbanmes, tags, datas)

            stmt2.execute()
            # get result
            rows = stmt2.result()

            self.compare_result(conn, "select count(123) from log", rows)
            tdLog.info("[query7: aggregate Functions] PASS")

            # query8: selector Functions
            stmt2.prepare("select bottom(bu,?) from log group by bu order by bu desc")
            datas   = [
                [
                    [2]
                ]
            ]

            # set param
            types = [FieldType.C_INT]
            stmt2.set_columns_type(types)

            # bind
            stmt2.bind_param(tbanmes, tags, datas)

            stmt2.execute()
            # get result
            rows = stmt2.result()

            self.compare_result(conn, "select bottom(bu,2) from log group by bu order by bu desc", rows)
            tdLog.info("[query8: selector Functions] PASS")

            
            # query9: time-series specific Functions
            stmt2.prepare("select twa(?) from log")
            datas   = [
                [
                    [15]
                ]
            ]

            # set param
            types = [FieldType.C_INT]
            stmt2.set_columns_type(types)

            # bind
            stmt2.bind_param(tbanmes, tags, datas)

            stmt2.execute()
            # get result
            rows = stmt2.result()

            self.compare_result(conn, "select twa(15) from log", rows)
            tdLog.info("[query9: time-series specific Functions] PASS")

            conn.close()
            tdLog.success("%s successfully executed" % __file__)

        except Exception as err:
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
