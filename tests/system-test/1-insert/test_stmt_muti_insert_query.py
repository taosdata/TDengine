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
import itertools
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
        print(con)
        return con

    def clear_env(self,conn):
        conn.close()

    def test_stmt_insert_multi(self,conn,asc=True):
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
            print(rows)
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

    def run(self):
        buildPath = self.getBuildPath()
        config = buildPath+ "../sim/dnode1/cfg/"
        host="localhost"
        connectstmt=self.newcon(host,config)
        self.test_stmt_insert_multi(connectstmt, True)
        self.test_stmt_insert_multi(connectstmt, False)
        self.clear_env(connectstmt)
        return


# add case with filename
#
tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
