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
import time
import random

import taos
import frame

from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *


class TDTestCase(TBase):
    """
    verify the interval compute value
    """

    def init(self, conn, logSql,replicaVar=1):
        self.replicaVar= int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.dbname = 'db'
        self.stable = 'meters'
        self.table = 'd0'
        self.inser_value = [('2017-07-14T10:40:00.000+08:00',10.2,219,0.31),
                            ('2017-07-14T10:40:10.000+08:00',10.2,219,0.31),
                            ('2017-07-14T10:40:30.000+08:00',10.2,219,0.31),
                            ('2017-07-14T10:40:40.000+08:00',10.2,220,1.31),
                            ('2017-07-14T10:41:01.000+08:00',10.2,201,1.31),
                            ('2017-07-14T10:41:11.000+08:00',10.2,200,0.31),
                            ('2017-07-14T10:42:11.000+08:00',10.2,200,10.31),
                            ('2017-07-14T10:42:15.000+08:00',10.2,210,0.31),
                            ('2017-07-14T10:43:11.000+08:00',10.2,200,0.31),
                            ('2017-07-14T10:43:15.000+08:00',10.2,18,182.31),
                            ]
        self.start_ts = "2017-07-14 10:40:00.000"  # 2017-07-14 10:40:00.000
        self.end_ts = "2017-07-14 11:40:00.000"
        self.interval_ts = 30
        self.tag_value1 = 7 # groupid
        self.tag_value2 = "'bj'" #location


    def prepareData(self):
        # db
        tdSql.execute(f"create database {self.dbname};")
        tdSql.execute(f"use {self.dbname};")
        tdLog.debug(f"Create database {self.dbname}")

        # super table
        tdSql.execute(
            f"create stable {self.stable} (ts timestamp, current float, voltage int, phase float) tags (groupid int,location varchar(24));")
        tdLog.debug("Create super table %s" % self.stable)

        # subtable
        tdSql.execute(
            f"create table {self.table} using {self.stable}(groupid, location)tags({self.tag_value1},{self.tag_value2});")
        tdLog.debug("Create common table %s" % self.table)

        # insert data

        for d in self.inser_value:
            sql = "insert into {} values".format(self.table)+"{}".format(d) + ";"
            tdSql.execute(sql)
            tdLog.debug("Insert data into table %s" % self.table)
    def test_interval_query(self):
        # 5 kinds of type ,0,1,-1, -1*10,1*10^2, one column
        sql = f'''select _wstart as ts,last(voltage)-first(voltage)
                  from {self.stable} where ts between "{self.start_ts}" and "{self.end_ts}" partition by location interval({self.interval_ts}s);'''
        tdSql.query(sql)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 1, -1)
        tdSql.checkData(3, 1, 10) # display scientific notation,
        tdSql.checkData(4, 1, -182)

        sql2 = f'''select _wstart as ts,last(voltage)-first(voltage),last(phase)-first(phase)
                  from {self.stable} where ts between "{self.start_ts}" and "{self.end_ts}" partition by location interval({self.interval_ts}s);'''
        tdSql.query(sql2)
        tdSql.checkData(0, 2, 0.00)
        tdSql.checkData(1, 2, 1.00)
        tdSql.checkData(2, 2, -1.00)
        tdSql.checkData(3, 2, -10.000000417232513) # display scientific notation,
        tdSql.checkData(4, 2, 181.999997556209564)



    def run(self):
        self.prepareData()
        self.test_interval_query()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())