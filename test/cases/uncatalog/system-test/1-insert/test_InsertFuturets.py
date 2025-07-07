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


from new_test_framework.utils import tdLog, tdSql
import random
from random import randint
import os
import time

class TestInsertfuturets:
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        #tdSql.init(conn.cursor(), logSql))
        cls.timestamp_ms = int(round(time.time()*1000))
        cls.timestamp_us = int(round(time.time()*1000000))
        cls.timestamp_ns = int(time.time_ns())
        cls.ms_boundary = 31556995200000
        cls.us_boundary = 31556995200000000
        cls.ns_boundary = 9214646400000000000
        cls.ntbname = 'ntb'
        cls.stbname = 'stb'
        cls.ctbname = 'ctb'
    def insert_check(self,timestamp,tbname):
        tdSql.execute(f'insert into {tbname} values({timestamp},1)')
        tdSql.query(f'select * from {tbname} where ts = {timestamp}')
        tdSql.checkEqual(tdSql.queryResult[0][1],1)
        tdSql.execute('flush database db')
        tdSql.query(f'select * from {tbname} where ts = {timestamp}')
        tdSql.checkEqual(tdSql.queryResult[0][1],1)
        tdSql.execute(f'insert into {tbname} values({timestamp},2)')
        tdSql.query(f'select * from {tbname} where ts = {timestamp}')
        tdSql.checkEqual(tdSql.queryResult[0][1],2)
        tdSql.execute('flush database db')
        tdSql.query(f'select * from {tbname} where ts = {timestamp}')
        tdSql.checkEqual(tdSql.queryResult[0][1],2)
        tdSql.execute(f'delete from {tbname} where ts = {timestamp}')
        tdSql.query(f'select * from {tbname} where ts = {timestamp}')
        tdSql.checkRows(0)
        tdSql.execute('flush database db')
        tdSql.query(f'select * from {tbname} where ts = {timestamp}')
        tdSql.checkRows(0)
        
    def insert_ms(self):
        tdSql.prepare()
        tdSql.execute('use db')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp,c0 int)')
        timestamp = random.randint(self.timestamp_ms,self.ms_boundary-1)
        self.insert_check(timestamp,self.ntbname)
        self.insert_check(self.ms_boundary,self.ntbname)
        tdSql.error(f'insert into {self.ntbname} values({self.ms_boundary+1},1)')
        tdSql.execute(f'create table {self.stbname} (ts timestamp,c0 int) tags(t0 int)')
        tdSql.execute(f'create table {self.ctbname} using {self.stbname} tags (1)')
        self.insert_check(timestamp,self.ctbname)
        self.insert_check(self.ms_boundary,self.ctbname)
        tdSql.error(f'insert into {self.ctbname} values({self.ms_boundary+1},1)')
    def insert_us(self):
        tdSql.execute('create database db1 precision "us"')
        tdSql.execute('use db1')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp,c0 int)')
        timestamp = random.randint(self.timestamp_us,self.us_boundary-1)
        self.insert_check(timestamp,self.ntbname)
        self.insert_check(self.us_boundary,self.ntbname)
        tdSql.error(f'insert into {self.ntbname} values({self.us_boundary+1},1)')
        tdSql.execute(f'create table {self.stbname} (ts timestamp,c0 int) tags(t0 int)')
        tdSql.execute(f'create table {self.ctbname} using {self.stbname} tags (1)')
        self.insert_check(timestamp,self.ctbname)
        self.insert_check(self.us_boundary,self.ctbname)
        tdSql.error(f'insert into {self.ctbname} values({self.us_boundary+1},1)')
    def insert_ns(self):
        tdSql.execute('create database db2 precision "ns"')
        tdSql.execute('use db2')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp,c0 int)')
        timestamp = random.randint(self.timestamp_ns,self.ns_boundary-1)
        self.insert_check(timestamp,self.ntbname)
        self.insert_check(self.ns_boundary,self.ntbname)
        tdSql.error(f'insert into {self.ntbname} values({self.ns_boundary+1},1)')
        tdSql.execute(f'create table {self.stbname} (ts timestamp,c0 int) tags(t0 int)')
        tdSql.execute(f'create table {self.ctbname} using {self.stbname} tags (1)')
        self.insert_check(timestamp,self.ctbname)
        self.insert_check(self.ns_boundary,self.ctbname)
        tdSql.error(f'insert into {self.ctbname} values({self.ns_boundary+1},1)')
    def test_InsertFuturets(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
        - xxx:xxx

        History:
        - xxx
        - xxx

        """
        self.insert_ms()
        self.insert_us()
        self.insert_ns()
        #tdSql.close()
        tdLog.success(f"{__file__} successfully executed")
        