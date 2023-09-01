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
import subprocess
import random
import math
import numpy as np
import inspect

from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.init(conn.cursor(), logSql)

    def getBuildPath(self) -> str:
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/debug/build/bin")]
                    break
        return buildPath

    def getCfgDir(self) -> str:
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            cfgDir = self.getBuildPath() + "/community/sim/dnode1/cfg"
        else:
            cfgDir = self.getBuildPath() + "/sim/dnode1/cfg"
        return cfgDir

    def getCfgFile(self) -> str:
        return self.getCfgDir()+"/taos.cfg"


    def compactVnodes(self):
        cfg = {
            'minRowsPerFileBlock': '10',
            'maxRowsPerFileBlock': '200',
            'minRows': '10',
            'maxRows': '200',
            'maxVgroupsPerDb': '100',
            'maxTablesPerVnode': '1200',
        }

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database  if not exists db keep 3650 days 1 blocks 3 minrows 10 maxrows 200")

        tdSql.execute("use db")
        tdSql.execute("create stable db.stb1 (ts timestamp, c1 int) tags(t1 int)")

        nowtime = 1693464447
        for i in range(100):
            tdSql.execute(f"create table db.t1{i} using db.stb1 tags({i})")

            sql = f"insert into db.t1{i} values"
            for j in range(24):
                ts = (nowtime - 3600 * (i * 24 + j)) * 1000;
                sql += f"({ts}, {i * 24 +j})"
            tdSql.execute(sql)

            sql = f"insert into db.t1{i} values"
            for j in range(24):
                ts = (nowtime - 1800 * (i * 24 + j)) * 1000;
                sql += f"({ts}, {i * 24 +j})"
            tdSql.execute(sql)

        tdSql.query('select * from stb1');
        tdSql.checkRows(4788);

        tdSql.query("show vgroups")
        index = tdSql.getData(0,0)
        tdSql.checkData(0, 6, 0)
        tdSql.execute(f"compact vnodes in({index}) start with now-10d end with now+5d")
        start_time = time.time()
        while True:
            tdSql.query("show vgroups")
            if tdSql.getData(0, 6) == 0:
                break
            else:
                time.sleep(0.1)
        run_time = time.time()-start_time
        printf(f"it takes ${run_time} seconds")

        tdSql.query('select * from stb1');
        tdSql.checkRows(4788);

        tdSql.query("show vgroups")
        index = tdSql.getData(0,0)
        tdSql.checkData(0, 6, 0)
        tdSql.execute(f"compact vnodes in({index}) start with 1000 end with now+5b")
        start_time = time.time()
        while True:
            tdSql.query("show vgroups")
            if tdSql.getData(0, 6) == 0:
                break
            else:
                time.sleep(0.1)
        run_time = time.time()-start_time
        printf(f"it takes ${run_time} seconds")

        tdSql.query('select * from stb1');
        tdSql.checkRows(4788);

        tdSql.query("show vgroups")
        index = tdSql.getData(0,0)
        tdSql.checkData(0, 6, 0)
        tdSql.execute(f"compact vnodes in({index}) start with now-100s")
        start_time = time.time()
        while True:
            tdSql.query("show vgroups")
            if tdSql.getData(0, 6) == 0:
                break
            else:
                time.sleep(0.1)
        run_time = time.time()-start_time
        printf(f"it takes ${run_time} seconds")

        tdSql.query('select * from stb1');
        tdSql.checkRows(4788);

        tdSql.query("show vgroups")
        index = tdSql.getData(0,0)
        tdSql.checkData(0, 6, 0)
        tdSql.execute(f"compact vnodes in({index}) end with now-10s")
        start_time = time.time()
        while True:
            tdSql.query("show vgroups")
            if tdSql.getData(0, 6) == 0:
                break
            else:
                time.sleep(0.1)
        run_time = time.time()-start_time
        printf(f"it takes ${run_time} seconds")

        tdSql.query('select * from stb1');
        tdSql.checkRows(4788);

        tdSql.query("show vgroups")
        index = tdSql.getData(0,0)
        tdSql.checkData(0, 6, 0)
        tdSql.execute(f"compact vnodes in({index})")
        start_time = time.time()
        while True:
            tdSql.query("show vgroups")
            if tdSql.getData(0, 6) == 0:
                break
            else:
                time.sleep(0.1)
        run_time = time.time()-start_time
        printf(f"it takes ${run_time} seconds")

        tdSql.query('select * from stb1');
        tdSql.checkRows(4788);

    def run(self):

        self.compactVnodes();

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
