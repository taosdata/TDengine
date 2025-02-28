import pytest
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
import time
#from utils.log import tdLog
#from utils.sql import tdSql


class TestStmtSetTbnameTag():

    # init
    def setup_class(cls):
        cls.tdLog.info("[TestStmtSetTbnameTag.setup_class] start to execute %s" % __file__)
        cls.db_name = cls.__name__.lower()
        cls.tdLog.debug(f"[TestStmtSetTbnameTag.setup_class] Current db_name: {cls.db_name}")
        cls.tdSql.create_database(cls.db_name, precision="'us'")

    # stop
    def teardown_class(cls):
        cls.tdLog.info("[TestStmtSetTbnameTag.teardown_class] %s successfully executed" % __file__)

    # --------------- case  -------------------


    def stmtExe(self,conn,sql,bindStat):
        queryStat=conn.statement("%s"%sql)
        queryStat.bind_param(bindStat)
        queryStat.execute()
        result=queryStat.use_result()
        rows=result.fetch_all()
        return rows

    def test_stmt_set_tbname_tag(self):
        """测试参数绑定tbname和tag

        创建参数绑定对象，绑定tbname和多种数据类型tag，插入数据成功

        Since: v3.3.0.0

        Labels: stmt,

        Jira: TD-12345,TS-1234

        History:
            - 2024-2-6 Feng Chao Created
            - 2025-2-26 Huo Hong Migrated to new test framework

        """
        #dbname = "stmt_tag"
        stablename = f"{self.__class__.__name__.lower()}_log"
        self.tdLog.debug(f"[TestStmtSetTbnameTag.test_stmt_set_tbname_tag] stablename: {stablename}")
        try:
            #self.conn.execute("drop database if exists %s" % dbname)
            #self.conn.execute("create database if not exists %s PRECISION 'us' "  % dbname)
            #self.conn.select_db(dbname)
            self.tdSql.prepare(self.db_name, precision="'us'")
            self.conn.execute("create table if not exists %s(ts timestamp, bo bool, nil tinyint, ti tinyint, si smallint, ii int,\
                bi bigint, tu tinyint unsigned, su smallint unsigned, iu int unsigned, bu bigint unsigned, \
                ff float, dd double, bb binary(100), nn nchar(100), tt timestamp , vc varchar(100)) tags (t1 timestamp, t2 bool,\
                t3 tinyint, t4 tinyint, t5 smallint, t6 int, t7 bigint, t8 tinyint unsigned, t9 smallint unsigned, \
                t10 int unsigned, t11 bigint unsigned, t12 float, t13 double, t14 binary(100), t15 nchar(100), t16 timestamp)"%stablename)

            stmt = self.conn.statement("insert into ? using log tags (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) \
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
            rows=self.stmtExe(self.conn,"select * from log where bu < ?",queryparam)
            tdLog.debug("assert 1st case %s"%rows)
            assert str(rows[0][0]) == "2021-07-21 17:56:32.589111"
            assert rows[0][10] == 3 , '1st case is failed'
            assert rows[1][10] == 4 , '1st case is failed'

            #query: Numeric Functions
            queryparam=new_bind_params(2)
            queryparam[0].int(5)
            queryparam[1].int(5)
            rows=self.stmtExe(self.conn,"select abs(?) from log where bu < ?",queryparam)
            self.tdLog.debug("assert 2nd case %s"%rows)
            assert rows[0][0] == 5 , '2nd case is failed'
            assert rows[1][0] == 5 , '2nd case is failed'


            #query: Numeric Functions and escapes
            queryparam=new_bind_params(1)
            queryparam[0].int(5)
            rows=self.stmtExe(self.conn,"select abs(?) from log where  nn= 'a? long string with 中文字符'",queryparam)
            tdLog.debug("assert 3rd case %s"%rows)
            assert rows == [] , '3rd case is failed'

            #query: string Functions
            queryparam=new_bind_params(1)
            queryparam[0].binary('中文字符')
            rows=self.stmtExe(self.conn,"select CHAR_LENGTH(?) from log ",queryparam)
            self.tdLog.info("assert 4th case %s"%rows)
            assert rows[0][0] == 4, '4th case is failed'
            assert rows[1][0] == 4, '4th case is failed'

            queryparam=new_bind_params(1)
            queryparam[0].binary('123')
            rows=self.stmtExe(self.conn,"select CHAR_LENGTH(?) from log ",queryparam)
            self.tdLog.info("assert 4th case %s"%rows)
            assert rows[0][0] == 3, '4th.1 case is failed'
            assert rows[1][0] == 3, '4th.1 case is failed'

            #query: conversion Functions
            queryparam=new_bind_params(1)
            queryparam[0].binary('1232a')
            rows=self.stmtExe(self.conn,"select cast( ? as bigint) from log",queryparam)
            self.tdLog.info("assert 5th case %s"%rows)
            assert rows[0][0] == 1232, '5th.1 case is failed'
            assert rows[1][0] == 1232, '5th.1 case is failed'

            querystmt4=self.conn.statement("select cast( ? as binary(10)) from log  ")
            queryparam=new_bind_params(1)
            queryparam[0].int(123)
            rows=self.stmtExe( self.conn,"select cast( ? as bigint) from log",queryparam)
            self.tdLog.info("assert 6th case %s"%rows)
            assert rows[0][0] == 123, '6th.1 case is failed'
            assert rows[1][0] == 123, '6th.1 case is failed'

            #query: datatime Functions
            queryparam=new_bind_params(1)
            queryparam[0].timestamp(1626861392591112)
            rows=self.stmtExe(self.conn,"select timediff('2021-07-21 17:56:32.590111',?,1a)  from log",queryparam)
            self.tdLog.info("assert 7th case %s"%rows)
            assert rows[0][0] == -1, '7th case is failed'
            assert rows[1][0] == -1, '7th case is failed'

            #query: aggregate Functions
            queryparam=new_bind_params(1)
            queryparam[0].int(123)
            rows=self.stmtExe(self.conn,"select count(?)  from log ",queryparam)
            self.tdLog.info("assert 8th case %s"%rows)
            assert rows[0][0] == 3, ' 8th case is failed'

            #query: selector Functions 9
            queryparam=new_bind_params(1)
            queryparam[0].int(2)
            rows=self.stmtExe(self.conn,"select bottom(bu,?)  from log group by bu  order by bu desc ; ",queryparam)
            self.tdLog.info("assert 9th case %s"%rows)
            assert rows[1][0] == 4, ' 9 case is failed'
            assert rows[2][0] == 3, ' 9 case is failed'

            # #query: time-series specific Functions 10

            querystmt=self.conn.statement("  select twa(?)  from log;  ")
            queryparam=new_bind_params(1)
            queryparam[0].int(15)
            rows=self.stmtExe(self.conn," select twa(?)  from log; ",queryparam)
            self.tdLog.info("assert 10th case %s"%rows)
            assert rows[0][0] == 15, ' 10th case is failed'


            # conn.execute("drop database if exists %s" % dbname)
            # self.conn.close()
            tdLog.success("%s successfully executed" % __file__)

        except Exception as err:
            # conn.execute("drop database if exists %s" % dbname)
            # self.conn.close()
            raise err

    #def run(self):
    #    self.init()
    #    self.test_stmt_set_tbname_tag(self.conn)

    #    return
