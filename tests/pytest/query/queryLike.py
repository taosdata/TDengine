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
import taos
from util.log import *
from util.cases import *
from util.sql import *

class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

    def run(self):
        tdSql.prepare()         
        tdSql.execute("drop database db ")
        tdSql.execute("create database if not exists db ")
        tdSql.execute("create table cars(ts timestamp, c nchar(2)) tags(t1 nchar(2))")
        tdSql.execute("insert into car0 using cars tags('aa') values(now, 'bb');")
        tdSql.query("select count(*) from cars where t1 like '%50 90 30 04 00 00%'")
        tdSql.checkRows(0)

        tdSql.execute("create table test_cars(ts timestamp, c nchar(2)) tags(t1 nchar(20))")
        tdSql.execute("insert into car1 using test_cars tags('150 90 30 04 00 002') values(now, 'bb');")
        tdSql.query("select * from test_cars where t1 like '%50 90 30 04 00 00%'")        
        tdSql.checkRows(1)
        
        tdSql.execute("create stable st (ts timestamp , id int , name  binary(20), data double ) tags (ind  int , tagg binary(20))  ")

        # check escape about tbname by show tables like

        tdSql.execute("create table tb_ using st tags (1 , 'tag_' ) ")
        tdSql.execute("insert into tb_ values( now , 1 , 'tbname_' , 1.0)")
        tdSql.execute("create table tba using st tags (1 , 'taga' ) ")
        tdSql.execute("insert into tba values( now , 1 , 'tbnamea' , 1.0)")
        tdSql.query("show tables like 'tb_'")
        tdSql.checkRows(2)
        tdSql.query("show tables like 'tb\_'")
        tdSql.checkRows(1)

        # check escape about tbname by show tables like
        tdSql.query("select * from st where tbname like 'tb\_'")
        tdSql.checkRows(1)
        
        # check escape about regular cols
        tdSql.query("select * from st where name like 'tbname\_';")
        tdSql.checkRows(1)

        # check escape about tags
        tdSql.query("select * from st where tagg like 'tag\_';")
        tdSql.checkRows(1)

        # ======================= check multi escape ===================
        
        tdSql.execute("create table tb_1 using st tags (1 , 'tag_1' ) ")
        tdSql.execute("insert into tb_1 values( now , 1 , 'tbname_1' , 1.0)")
        tdSql.execute("create table tb_2  using st tags (2 , 'tag_2' )")
        tdSql.execute("insert into tb_2 values( now , 2 , 'tbname_2' , 2.0)")
        tdSql.execute("create table tb__  using st tags (3 , 'tag__' )")
        tdSql.execute("insert into tb__ values( now , 3 , 'tbname__' , 2.0)")
        tdSql.execute("create table tb__1  using st tags (3 , 'tag__1' )")
        tdSql.execute("insert into tb__1 values( now , 1 , 'tbname__1' , 1.0)")
        tdSql.execute("create table tb__2  using st  tags (4 , 'tag__2' )")
        tdSql.execute("create table tb___  using st  tags (5 , 'tag___' )")
        tdSql.execute("insert into tb___ values( now , 1 , 'tbname___' , 1.0)")
        tdSql.execute("create table tb_d_  using st  tags (5 , 'tag_d_' )")
        tdSql.execute("insert into tb_d_ values( now , 1 , 'tbname_d_' , 1.0)")

        tdSql.execute("create table tb_d__  using st  tags (5 , 'tag_d__' )")
        tdSql.execute("insert into tb_d__ values( now , 1 , 'tbname_d__' , 1.0)")
        tdSql.execute("create table tb____  using st  tags (5 , 'tag____' )")
        tdSql.execute("insert into tb____ values( now , 1 , 'tbname____' , 1.0)")
        tdSql.execute("create table tb__a_  using st  tags (5 , 'tag__a_' )")
        tdSql.execute("insert into tb__a_ values( now , 1 , 'tbname__a_' , 1.0)")
        tdSql.execute("create table tb__ab__  using st  tags (5 , 'tag__ab__' )")
        tdSql.execute("insert into tb__ab__ values( now , 1 , 'tbname__ab__' , 1.0)")

        # check escape about tbname by show tables like
        tdSql.query("select * from st where tbname like 'tb__'")
        tdSql.checkRows(3)
        tdSql.query("select * from st where tbname like 'tb_\_'")
        tdSql.checkRows(1)
        tdSql.query("select * from st where tbname like 'tb___'")
        tdSql.checkRows(4)
        tdSql.query("select * from st where tbname like 'tb_\__'")
        tdSql.checkRows(3)
        tdSql.query("select * from st where tbname like 'tb_\_\_'")
        tdSql.checkRows(1)
        tdSql.query("select * from st where tbname like 'tb\__\_'")
        tdSql.checkRows(1)
        tdSql.query("select * from st where tbname like 'tb\__\__'")
        tdSql.checkRows(2)
        tdSql.query("select * from st where tbname like 'tb\__\_\_'")
        tdSql.checkRows(2)
        tdSql.query("select * from st where tbname like 'tb\____'")
        tdSql.checkRows(3)
        tdSql.query("select * from st where tbname like 'tb\_\__\_'")
        tdSql.checkRows(2)
        tdSql.query("select * from st where tbname like 'tb\_\_\_\_'")
        tdSql.checkRows(1)
        
        # check escape about regular cols
        tdSql.query("select * from st where name like 'tbname\_\_';")
        tdSql.checkRows(1)
        tdSql.query("select * from st where name like 'tbname\__';")
        tdSql.checkRows(3)
        tdSql.query("select * from st where name like 'tbname___';")
        tdSql.checkRows(4)
        tdSql.query("select * from st where name like 'tbname_\__';")
        tdSql.checkRows(3)
        tdSql.query("select * from st where name like 'tbname_\_\_';")
        tdSql.checkRows(1)
        tdSql.query("select * from st where name like 'tbname\_\__';")
        tdSql.checkRows(2)
        tdSql.query("select * from st where name like 'tbname____';")
        tdSql.checkRows(3)
        tdSql.query("select * from st where name like 'tbname\_\___';")
        tdSql.checkRows(2)
        tdSql.query("select * from st where name like 'tbname\_\_\__';")
        tdSql.checkRows(1)
        tdSql.query("select * from st where name like 'tbname\_\__\_';")
        tdSql.checkRows(2)
        tdSql.query("select name from st where name like 'tbname\_\_\__';")
        tdSql.checkData(0,0 "tbname____")

        # check escape about tags
        tdSql.query("select * from st where tagg like 'tag\_';")
        tdSql.checkRows(1)
        tdSql.query("select * from st where tagg like 'tag\_\_';")
        tdSql.checkRows(1)
        tdSql.query("select * from st where tagg like 'tag\__';")
        tdSql.checkRows(3)
        tdSql.query("select * from st where tagg like 'tag___';")
        tdSql.checkRows(4)
        tdSql.query("select * from st where tagg like 'tag_\__';")
        tdSql.checkRows(3)
        tdSql.query("select * from st where tagg like 'tag_\_\_';")
        tdSql.checkRows(1)
        tdSql.query("select * from st where tagg like 'tag\_\__';")
        tdSql.checkRows(2)
        tdSql.query("select * from st where tagg like 'tag____';")
        tdSql.checkRows(3)
        tdSql.query("select * from st where tagg like 'tag\_\___';")
        tdSql.checkRows(2)
        tdSql.query("select * from st where tagg like 'tag\_\_\__';")
        tdSql.checkRows(1)
        tdSql.query("select * from st where tagg like 'tag\_\__\_';")
        tdSql.checkRows(2)
        tdSql.query("select * from st where tagg like 'tag\_\__\_';")
        tdSql.checkData(0,0 "tag__a_")

        os.system("rm -rf ./*.py.sql")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
