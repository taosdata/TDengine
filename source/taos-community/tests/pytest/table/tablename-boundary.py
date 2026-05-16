# -*- coding: utf-8 -*-

import sys
import string
import random
import subprocess
from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.ts = 1622100000000

    def get_random_string(self, length):
        letters = string.ascii_lowercase
        result_str = ''.join(random.choice(letters) for i in range(length))
        return result_str

    def run(self):
        tdSql.prepare()

        getTableNameLen = "grep -w '#define TSDB_TABLE_NAME_LEN' ../../src/inc/taosdef.h|awk '{print $3}'"
        tableNameMaxLen = int(
            subprocess.check_output(
                getTableNameLen,
                shell=True)) - 1
        tdLog.info("table name max length is %d" % tableNameMaxLen)
        chars = string.ascii_uppercase + string.ascii_lowercase
        tb_name = ''.join(random.choices(chars, k=tableNameMaxLen + 1))
        tdLog.info('tb_name length %d' % len(tb_name))
        tdLog.info('create table %s (ts timestamp, value int)' % tb_name)
        tdSql.error('create table %s (ts timestamp, speed binary(4089))' % tb_name)

        tb_name = ''.join(random.choices(chars, k=tableNameMaxLen))
        tdLog.info('tb_name length %d' % len(tb_name))
        tdLog.info('create table %s (ts timestamp, value int)' % tb_name)
        tdSql.execute(
            'create table %s (ts timestamp, speed binary(4089))' %
            tb_name)
        
        db_name = self.get_random_string(33)        
        tdSql.error("create database %s" % db_name)
        
        db_name = self.get_random_string(32)
        tdSql.execute("create database %s" % db_name)
        tdSql.execute("use %s" % db_name)

        tb_name = self.get_random_string(193)
        tdSql.error("create table %s(ts timestamp, val int)" % tb_name)

        tb_name = self.get_random_string(192)
        tdSql.execute("create table %s.%s(ts timestamp, val int)" % (db_name, tb_name))
        tdSql.query("show %s.tables" % db_name)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, tb_name)

        tdSql.execute("insert into %s.%s values(now, 1)" % (db_name, tb_name))
        tdSql.query("select * from %s.%s" %(db_name, tb_name))
        tdSql.checkRows(1)
        
        db_name = self.get_random_string(32)
        tdSql.execute("create database %s update 1" % db_name)

        stb_name = self.get_random_string(192)
        tdSql.execute("create table %s.%s(ts timestamp, val int) tags(id int)" % (db_name, stb_name))
        tb_name1 = self.get_random_string(192)        
        tdSql.execute("insert into %s.%s using %s.%s tags(1) values(%d, 1)(%d, 2)(%d, 3)" % (db_name, tb_name1, db_name, stb_name, self.ts, self.ts + 1, self.ts + 2))
        tb_name2 = self.get_random_string(192)
        tdSql.execute("insert into %s.%s using %s.%s tags(2) values(%d, 1)(%d, 2)(%d, 3)" % (db_name, tb_name2, db_name, stb_name, self.ts, self.ts + 1, self.ts + 2))
        
        tdSql.query("show %s.tables" % db_name)
        tdSql.checkRows(2)        

        tdSql.query("select * from %s.%s" % (db_name, stb_name))
        tdSql.checkRows(6)

        tdSql.execute("insert into %s.%s using %s.%s tags(1) values(%d, null)" % (db_name, tb_name1, db_name, stb_name, self.ts))
        
        tdSql.query("select * from %s.%s" % (db_name, stb_name))
        tdSql.checkRows(6)





    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
