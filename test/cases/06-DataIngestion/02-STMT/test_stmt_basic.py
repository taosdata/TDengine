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

from new_test_framework.utils import tdLog, tdSql, tdCom
import sys
import os
import threading as thd
import itertools
import logging
import multiprocessing as mp
import itertools
import taos
from taos import *
import numpy as np
import datetime as dt
from datetime import datetime
from ctypes import *
import time

# constant define
WAITS = 5 # wait seconds



class TestStmtBasic:
    # init
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        cls.ts = 1500000000000

    #
    # --------------- case1 ----------------
    #

    def newcon(self,host,cfg):
        user = "root"
        password = "taosdata"
        port =6030
        con=taos.connect(host=host, user=user, password=password, config=cfg ,port=port)
        print(con)
        return con

    def clear_env(self,conn):
        conn.close()

    def check_stmt_insert_multi(self,conn, asc = True):
        # type: (TaosConnection) -> None

        dbname = "db_stmt"
        try:
            conn.execute("drop database if exists %s" % dbname)
            conn.execute("create database if not exists %s keep 36500d" % dbname)
            conn.select_db(dbname)

            conn.execute(
                "create table if not exists stb1(ts timestamp, bo bool, nil tinyint, ti tinyint, si smallint, ii int,\
                bi bigint, tu tinyint unsigned, su smallint unsigned, iu int unsigned, bu bigint unsigned, \
                ff float, dd double, bb binary(100), nn nchar(100), tt timestamp)",
            )
            # conn.load_table_info("log")
            tdLog.debug("statement start")
            start = datetime.now()
            stmt = conn.statement("insert into stb1 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

            rows = 512 # refact for TD-37374, don't change the value of rows
            params = new_multi_binds(16)
            if asc == True:
                params[0].timestamp(list(range(1626861392589, 1626861392589 + rows, 1)))
            else:
                params[0].timestamp(list(range(1626861392589 + rows - 1, 1626861392589 - 1, -1)))
            params[1].bool(list(itertools.islice(itertools.cycle((True, None, False, False)), rows)))
            params[2].tinyint(list(itertools.islice(itertools.cycle((-128, -128, None, None)), rows))) # -128 is tinyint null
            params[3].tinyint(list(itertools.islice(itertools.cycle((0, 127, None, None)), rows)))
            params[4].smallint(list(itertools.islice(itertools.cycle((3, None, 2, 2)), rows)))
            params[5].int(list(itertools.islice(itertools.cycle((3, 4, None, None)), rows)))
            params[6].bigint(list(itertools.islice(itertools.cycle((3, 4, None, None)), rows)))
            params[7].tinyint_unsigned(list(itertools.islice(itertools.cycle((3, 4, None, None)), rows)))
            params[8].smallint_unsigned(list(itertools.islice(itertools.cycle((3, 4, None, None)), rows)))
            params[9].int_unsigned(list(itertools.islice(itertools.cycle((3, 4, None, None)), rows)))
            params[10].bigint_unsigned(list(itertools.islice(itertools.cycle((3, 4, None, None)), rows)))
            params[11].float(list(itertools.islice(itertools.cycle((3, None, 1, 2)), rows)))
            params[12].double(list(itertools.islice(itertools.cycle((3, None, 1.2, 1.3)), rows)))
            params[13].binary(list(itertools.islice(itertools.cycle(("abc", "dddafadfadfadfadfa", None, None)), rows)))
            params[14].nchar(list(itertools.islice(itertools.cycle(("涛思数据", None, "a long string with 中文字符", None)), rows)))
            params[15].timestamp(list(itertools.islice(itertools.cycle((None, None, 1626861392591, None)), rows)))
            # print(type(stmt))
            tdLog.debug("bind_param_batch start")
            stmt.bind_param_batch(params)
            tdLog.debug("bind_param_batch end")
            stmt.execute()
            tdLog.debug("execute end")
            end = datetime.now()
            print("elapsed time: ", end - start)
            assert stmt.affected_rows == rows

            #query 1
            querystmt=conn.statement("select ?,bu from stb1")
            queryparam=new_bind_params(1)
            print(type(queryparam))
            queryparam[0].binary("ts")
            querystmt.bind_param(queryparam)
            querystmt.execute()
            result=querystmt.use_result()
            # rows=result.fetch_all()
            # print( querystmt.use_result())

            # result = conn.query("select * from stb1")
            rows=result.fetch_all()
            # rows=result.fetch_all()
            logging.info(rows)
            assert rows[1][0] == "ts"
            if asc == True:
                assert rows[0][1] == 3
                assert rows[3][1] == None
            else:
                assert rows[0][1] == None
                assert rows[3][1] == 3

            #query 2
            querystmt1=conn.statement("select * from stb1 where bu < ?")
            queryparam1=new_bind_params(1)
            print(type(queryparam1))
            queryparam1[0].int(4)
            querystmt1.bind_param(queryparam1)
            querystmt1.execute()
            result1=querystmt1.use_result()
            rows1=result1.fetch_all()
            print(rows1)
            if asc == True:
                assert str(rows1[0][0]) == "2021-07-21 17:56:32.589000"
            else:
                assert str(rows1[0][0]) == "2021-07-21 17:56:32.592000"
            assert rows1[0][10] == 3
            tdLog.debug("close start")

            stmt.close()

            # conn.execute("drop database if exists %s" % dbname)
            # conn.close()
            tdLog.success("%s successfully executed, asc:%s" % (__file__, asc))

        except Exception as err:
            # conn.execute("drop database if exists %s" % dbname)
            conn.close()
            raise err

    def do_stmt_muti_insert_query(self):

        buildPath = tdCom.getBuildPath()
        config = buildPath+ "../sim/dnode1/cfg/"
        host="localhost"
        connectstmt=self.newcon(host,config)
        self.check_stmt_insert_multi(connectstmt, True)
        self.check_stmt_insert_multi(connectstmt, False)
        self.clear_env(connectstmt)
        tdLog.success(f"{__file__} successfully executed")
        return

    #
    # --------------- case2 ----------------
    #

    def stmtExe(self,conn,sql,bindStat):
            queryStat=conn.statement("%s"%sql)
            queryStat.bind_param(bindStat)
            queryStat.execute()
            result=queryStat.use_result()
            rows=result.fetch_all()
            return rows

    def check_stmt_set_tbname_tag(self,conn):
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
            assert rows[0][0] == 4, '4th case is failed'
            assert rows[1][0] == 4, '4th case is failed'

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
            assert rows[0][0] == -1, '7th case is failed'
            assert rows[1][0] == -1, '7th case is failed'

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

    def do_stmt_set_tbname_tag(self):
        buildPath = tdCom.getBuildPath()
        config = buildPath+ "../sim/dnode1/cfg/"
        host="localhost"
        connectstmt=self.newcon(host,config)
        self.check_stmt_set_tbname_tag(connectstmt)

        tdLog.success(f"{__file__} successfully executed")

        return

    #
    # ----------------  main ---------------
    #
    def test_stmt_basic(self):
        """STMT basic

        1. stmt insert multi rows order by asc/desc
        2. stmt set tbname and tags, and query with different functions
        3. verify result is ok
        4. clean env

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-9-22 Alex Duan Migrated from uncatalog/system-test/1-insert/test_test_stmt_muti_insert_query.py
            - 2025-9-22 Alex Duan Migrated from uncatalog/system-test/1-insert/test_test_stmt_set_tbname_tag.py
        """

        self.do_stmt_muti_insert_query()
        self.do_stmt_set_tbname_tag()
        tdLog.success(f"{__file__} successfully executed")