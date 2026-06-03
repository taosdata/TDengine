
# -*- coding: utf-8 -*-

import random
import string
import subprocess
import sys
from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
    def run(self):
        tdLog.debug("check database")
        tdSql.prepare()

        # check default update value 
        sql = "create database if not exists db"
        tdSql.execute(sql)
        tdSql.query('select * from information_schema.ins_databases')
        tdSql.checkRows(1)
        tdSql.checkData(0,16,0)

        sql = "alter database db update 1"

        # check update value 
        tdSql.execute(sql)
        tdSql.query('select * from information_schema.ins_databases')
        tdSql.checkRows(1)
        tdSql.checkData(0,16,1)


        sql = "alter database db update 0"
        tdSql.execute(sql)
        tdSql.query('select * from information_schema.ins_databases')
        tdSql.checkRows(1)
        tdSql.checkData(0,16,0)

        sql = "alter database db update -1"
        tdSql.error(sql)

        sql = "alter database db update 100"
        tdSql.error(sql)

        tdSql.query('select * from information_schema.ins_databases')
        tdSql.checkRows(1)
        tdSql.checkData(0,16,0)

        tdSql.execute('drop database db') 
        tdSql.error('create database db update 100')
        tdSql.error('create database db update -1')

        tdSql.execute('create database db update 1')

        tdSql.query('select * from information_schema.ins_databases')
        tdSql.checkRows(1)
        tdSql.checkData(0,16,1)

        tdSql.execute('drop database db') 

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
