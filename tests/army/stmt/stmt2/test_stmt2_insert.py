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
import os
# from numpy.lib.function_base import insert
import taos
from taos import *
from stmt.common import StmtCommon
from ctypes import *
from taos.constants import FieldType
from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *


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

    # init
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.dbname = "stmt_insert_test_cases"
        self.stmt_common = StmtCommon()
        self.connectstmt = None

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

    def stmtExe(self, sql, bindStat):
        queryStat=self.connectstmt.statement2("%s"%sql)
        queryStat.bind_param(bindStat)
        queryStat.execute()
        result=queryStat.result()
        rows=result.fetch_all()
        return rows

    def test_stmt_insert_common_table_with_bind_data(self):
        ctablename = 'common_table'
        try:
            self.connectstmt.execute("drop database if exists %s" % self.dbname)
            self.connectstmt.execute("create database if not exists %s PRECISION 'us' "  % self.dbname)
            self.connectstmt.select_db(self.dbname)
            self.connectstmt.execute("create table if not exists %s (ts timestamp, bo bool, nil tinyint, ti tinyint, si smallint, ii int,\
                bi bigint, tu tinyint unsigned, su smallint unsigned, iu int unsigned, bu bigint unsigned, \
                ff float, dd double, bb binary(100), nn nchar(100), tt timestamp , vc varchar(100))" % ctablename)

            stmt2 = self.connectstmt.statement2(f"insert into {ctablename} values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

            tbanmes = [ctablename]
            tags    = None

            # bind_param_with_tables
            datas1 = [
                # table 1
                [
                    # student
                    [1626861392589111,1626861392590111],
                    [True,         None],
                    [-128,         -128],
                    [0,            127],
                    [3,            None],
                    [3,            4],
                    [3,            4],
                    [3,            4],
                    [3,            4],
                    [3,            4],
                    [3,            4],
                    [3,            None],
                    [3,            None],
                    ["abc",       "dddafadfadfadf"],
                    ["涛思数据", None],
                    [None, None],
                    ["涛思数据16", None]
                ]
            ]

            table0 = BindTable(tbanmes[0], tags)

            for data in datas1[0]:
                table0.add_col_data(data)

            # columns type for stable
            stmt2.bind_param_with_tables([table0])
            stmt2.execute()

            # bind_param
            datas2 = [
                # table 1
                [
                    # student
                    [1626861392591111],
                    [False],
                    [None],
                    [None],
                    [2],
                    [None],
                    [None],
                    [None],
                    [None],
                    [None],
                    [5],
                    [1],
                    [1.2],
                    [None],
                    ["a long string with 中文?字符"],
                    [1626861392591],
                    [None]
                    
                ]
            ]

            stmt2.bind_param(tbanmes, tags, datas2)
            stmt2.execute()

            assert stmt2.affected_rows == 1
            # check correct
            # self.checkResultCorrects(conn, self.dbname, None, tbanmes, tags, datas2)

            tdLog.info("Case [test_stmt_insert_common_table_with_bind_data] PASS")
        except Exception as err:
            raise err

    def test_stmt_insert_common_table_with_bind_tablename_data(self):
        ctablename1 = 'common_table1'
        ctablename2 = 'common_table2'
        ctablename3 = 'common_table3'
        try:
            self.connectstmt.execute("drop database if exists %s" % self.dbname)
            self.connectstmt.execute("create database if not exists %s PRECISION 'us' "  % self.dbname)
            self.connectstmt.select_db(self.dbname)
            self.connectstmt.execute("create table if not exists %s (ts timestamp, bo bool, nil tinyint, ti tinyint, si smallint, ii int,\
                bi bigint, tu tinyint unsigned, su smallint unsigned, iu int unsigned, bu bigint unsigned, \
                ff float, dd double, bb binary(100), nn nchar(100), tt timestamp , vc varchar(100))" % ctablename1)
            
            self.connectstmt.execute("create table if not exists %s (ts timestamp, bo bool, nil tinyint, ti tinyint, si smallint, ii int,\
                bi bigint, tu tinyint unsigned, su smallint unsigned, iu int unsigned, bu bigint unsigned, \
                ff float, dd double, bb binary(100), nn nchar(100), tt timestamp , vc varchar(100))" % ctablename2)

            self.connectstmt.execute("create table if not exists %s (ts timestamp, bo bool, nil tinyint, ti tinyint, si smallint, ii int,\
                bi bigint, tu tinyint unsigned, su smallint unsigned, iu int unsigned, bu bigint unsigned, \
                ff float, dd double, bb binary(100), nn nchar(100), tt timestamp , vc varchar(100))" % ctablename3)
            
            stmt2 = self.connectstmt.statement2(f"insert into ? values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

            tbanmes = [ctablename1, ctablename2]
            tags    = None

            datas = [
                [
                    [1626861392589111,1626861392590111],
                    [True,         None],
                    [-128,         -128],
                    [0,            127],
                    [3,            None],
                    [3,            4],
                    [3,            4],
                    [3,            4],
                    [3,            4],
                    [3,            4],
                    [3,            4],
                    [3,            None],
                    [3,            None],
                    ["abc",       "dddafadfadfadf"],
                    ["涛思数据", None],
                    [None, None],
                    ["涛思数据16", None]
                ],
                [
                    [1626861392589111,1626861392590111],
                    [True,         None],
                    [-128,         -128],
                    [0,            127],
                    [3,            None],
                    [3,            4],
                    [3,            4],
                    [3,            4],
                    [3,            4],
                    [3,            4],
                    [3,            4],
                    [3,            None],
                    [3,            None],
                    ["abc",       "dddafadfadfadf"],
                    ["涛思数据", None],
                    [None, None],
                    ["涛思数据16", None]
                ]
            ]  

            print("tags: %s" % tags)
            stmt2.bind_param(tbanmes, tags, datas)
            stmt2.execute()

            # check correct
            self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, None, tbanmes, tags, datas)


            # bind data
            tbanmes = [ctablename3]
            tags    = None

            # prepare data
            datas2 = [
                # table 1
                [
                    # student
                    [1626861392591111],
                    [False],
                    [None],
                    [None],
                    [2],
                    [None],
                    [None],
                    [None],
                    [None],
                    [None],
                    [5],
                    [1],
                    [1.2],
                    [None],
                    ["a long string with 中文?字符"],
                    [1626861392591],
                    [None]
                    
                ]
            ]

            stmt2.bind_param(tbanmes, tags, datas2)
            stmt2.execute()

            # check correct
            self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, None, tbanmes, tags, datas2)

            tdLog.info("Case [test_stmt_insert_common_table_with_bind_tablename_data] PASS")
        except Exception as err:
            raise err

    def test_stmt_insert_super_table_with_bind_ctablename_tags_data(self):
        stablename = 'super_table_with_bind_ctablename'
        try:
            self.connectstmt.execute("drop database if exists %s" % self.dbname)
            self.connectstmt.execute("create database if not exists %s PRECISION 'us' "  % self.dbname)
            self.connectstmt.select_db(self.dbname)
            self.connectstmt.execute("create table if not exists %s(ts timestamp, bo bool, nil tinyint, ti tinyint, si smallint, ii int,\
                bi bigint, tu tinyint unsigned, su smallint unsigned, iu int unsigned, bu bigint unsigned, \
                ff float, dd double, bb binary(100), nn nchar(100), tt timestamp , vc varchar(100)) tags (t1 timestamp, t2 bool,\
                t3 tinyint, t4 tinyint, t5 smallint, t6 int, t7 bigint, t8 tinyint unsigned, t9 smallint unsigned, \
                t10 int unsigned, t11 bigint unsigned, t12 float, t13 double, t14 binary(100), t15 nchar(100), t16 timestamp)"% stablename)

            stmt2 = self.connectstmt.statement2(f"insert into ? using {stablename} tags (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) \
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

            # check correct
            self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, stablename, tbanmes, tags, datas)

            tdLog.info("Case [test_stmt_insert_super_table_with_bind_ctablename_tags_data] PASS")
        except Exception as err:
            raise err

    def test_stmt_insert_super_table_with_bind_tags_data(self):
        stablename = 'super_table_without_bind_ctablename'
        try:
            self.connectstmt.execute("drop database if exists %s" % self.dbname)
            self.connectstmt.execute("create database if not exists %s PRECISION 'us' "  % self.dbname)
            self.connectstmt.select_db(self.dbname)
            self.connectstmt.execute("create table if not exists %s(ts timestamp, bo bool, nil tinyint, ti tinyint, si smallint, ii int,\
                bi bigint, tu tinyint unsigned, su smallint unsigned, iu int unsigned, bu bigint unsigned, \
                ff float, dd double, bb binary(100), nn nchar(100), tt timestamp , vc varchar(100)) tags (t1 timestamp, t2 bool,\
                t3 tinyint, t4 tinyint, t5 smallint, t6 int, t7 bigint, t8 tinyint unsigned, t9 smallint unsigned, \
                t10 int unsigned, t11 bigint unsigned, t12 float, t13 double, t14 binary(100), t15 nchar(100), t16 timestamp)"% stablename)

            stmt2 = self.connectstmt.statement2(f"insert into t100 using {stablename} tags (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) \
                values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

            # bind data
            tbanmes = None
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

            table0 = BindTable(tbanmes, tags[0])

            for data in datas[0]:
                table0.add_col_data(data)

            # columns type for stable
            stmt2.bind_param_with_tables([table0])
            stmt2.execute()

            # check correct
            self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, stablename, ['t100'], tags, datas)

            tdLog.info("Case [test_stmt_insert_super_table_without_bind_ctablename] PASS")
        except Exception as err:
            raise err
    
    def test_stmt_insert_super_table_with_bind_data(self):
        stablename = 'super_table'
        childname = 'child_table'
        try:
            self.connectstmt.execute("drop database if exists %s" % self.dbname)
            self.connectstmt.execute("create database if not exists %s PRECISION 'us' "  % self.dbname)
            self.connectstmt.select_db(self.dbname)
            self.connectstmt.execute("create table if not exists %s(ts timestamp, bo bool, nil tinyint, ti tinyint, si smallint, ii int,\
                bi bigint, tu tinyint unsigned, su smallint unsigned, iu int unsigned, bu bigint unsigned, \
                ff float, dd double, bb binary(100), nn nchar(100), tt timestamp , vc varchar(100)) tags (t1 timestamp, t2 nchar(100))" % stablename)

            stmt2 = self.connectstmt.statement2(f"insert into {childname} using {stablename} tags (?,?) \
                values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

            # bind data
            tbanmes = None
            tags    = [
                [1601481600000, "stmt"]
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

            table0 = BindTable(childname, tags[0])

            for data in datas[0]:
                table0.add_col_data(data)

            # columns type for stable
            stmt2.bind_param_with_tables([table0])
            stmt2.execute()

            # check correct
            self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, stablename, ['t100'], tags, datas)

            tdLog.info("Case [test_stmt_insert_super_table_with_bind_data] PASS")
        except Exception as err:
            raise err

    def test_stmt_insert_common_table_with_bind_ctablename_data(self):
        stablename = 'super_table_without_bind_tags'
        childname = 'child_table_without_bind_tags'
        try:
            self.connectstmt.execute("drop database if exists %s" % self.dbname)
            self.connectstmt.execute("create database if not exists %s PRECISION 'us' "  % self.dbname)
            self.connectstmt.select_db(self.dbname)
            self.connectstmt.execute("create table if not exists %s (ts timestamp, bo bool, nil tinyint, ti tinyint, si smallint, ii int,\
                bi bigint, tu tinyint unsigned, su smallint unsigned, iu int unsigned, bu bigint unsigned, \
                ff float, dd double, bb binary(100), nn nchar(100), tt timestamp , vc varchar(100)) tags (t1 timestamp, t2 bool,\
                t3 tinyint, t4 float, t5 double, t6 binary(100), t7 nchar(100))"% stablename)

            stmt2 = self.connectstmt.statement2(f"insert into ? using {stablename} tags ('1601481600000', True, 2, 10.1, 10.11, 'hello', 'stmt') \
                values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

            # bind data
            tbanmes = [childname]
            tags    = [
                [1601481600000, True, 2, 10.1, 10.11, "hello", "stmt"]
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

            table0 = BindTable(tbanmes[0], None)

            for data in datas[0]:
                table0.add_col_data(data)

            # columns type for stable
            stmt2.bind_param_with_tables([table0])
            stmt2.execute()

            # check correct
            self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, stablename, tbanmes, tags, datas)

            tdLog.info("Case [test_stmt_insert_common_table_without_bind_tags] PASS")
        except Exception as err:
            raise err
        
    def test_stmt_td31428(self):
        stablename = 'stmt_td31428'
        try:
            self.connectstmt.execute("drop database if exists %s" % self.dbname)
            self.connectstmt.execute("create database if not exists %s PRECISION 'us' "  % self.dbname)
            self.connectstmt.select_db(self.dbname)
            self.connectstmt.execute("create table if not exists %s(ts timestamp, bo bool, nil tinyint, ti tinyint, si smallint, ii int,\
                bi bigint, tu tinyint unsigned, su smallint unsigned, iu int unsigned, bu bigint unsigned, \
                ff float, dd double, bb binary(100), nn nchar(100), tt timestamp , vc varchar(100)) tags (t1 timestamp, t2 bool,\
                t3 tinyint, t4 tinyint, t5 smallint, t6 int, t7 bigint, t8 tinyint unsigned, t9 smallint unsigned, \
                t10 int unsigned, t11 bigint unsigned, t12 float, t13 double, t14 binary(100), t15 nchar(100), t16 timestamp)"% stablename)

            stmt2 = self.connectstmt.statement2(f"insert into ? using {stablename} tags (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) \
                values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

            # bind data
            tbanmes = ["t1"]
            tags    = [
                [1601481600000, True, False, 2, 3, 4, 5, 6, 7, 8, 9, 10.1, 10.11, "hello", "stmt", 1626861392589]
            ]

            # prepare data
            datas1 = [
                # table 1
                [
                    # student
                    [1626861392589111],
                    [True],
                    [-128],
                    [0],
                    [3],
                    [3],
                    [3],
                    [3],
                    [3],
                    [3],
                    [3],
                    [3],
                    [3],
                    ["abc"],
                    ["涛思数据"],
                    [1626861392591],
                    ["涛思数据16"]
                ]
            ]

            datas2 = [
                # table 1
                [
                    # student
                    [1626861392589111],
                    [IGNORE],
                    [None],
                    [None],
                    [IGNORE],
                    [IGNORE],
                    [IGNORE],
                    [IGNORE],
                    [IGNORE],
                    [IGNORE],
                    [IGNORE],
                    [IGNORE],
                    [IGNORE],
                    [IGNORE],
                    [IGNORE],
                    [IGNORE],
                    [IGNORE]
                ]
            ]

            datas2_expect = [
                # table 1
                [
                    # student
                    [1626861392589111],
                    [True],
                    [None],
                    [None],
                    [3],
                    [3],
                    [3],
                    [3],
                    [3],
                    [3],
                    [3],
                    [3],
                    [3],
                    ["abc"],
                    ["涛思数据"],
                    [1626861392591],
                    ["涛思数据16"]
                ]
            ]

            # prepare data
            datas3 = [
                # table 1
                [
                    [1626861392589111,1626861392591111],
                    [None,            False],
                    [-127,            None],
                    [127,             None],
                    [None,            2],
                    [IGNORE,               None],
                    [IGNORE,               None],
                    [4,               None],
                    [4,               None],
                    [4,               None],
                    [4,               5],
                    [None,            1],
                    [None,            1.2],
                    ["dddafadfadfadf", None],
                    [None, "a long string with 中文?字符"],
                    [None, 1626861392591],
                    [None, None]
                    
                ]
            ]

            datas3_expect = [
                # table 1
                [
                    [1626861392589111,1626861392591111],
                    [None,            False],
                    [-127,            None],
                    [127,             None],
                    [None,            2],
                    [3,               None],
                    [3,               None],
                    [4,               None],
                    [4,               None],
                    [4,               None],
                    [4,               5],
                    [None,            1],
                    [None,            1.2],
                    ["dddafadfadfadf", None],
                    [None, "a long string with 中文?字符"],
                    [None, 1626861392591],
                    [None, None]
                    
                ]
            ]

            # prepare data
            datas4 = [
                # table 1
                [
                    [1626861392589111,1626861392591111],
                    [None,            False],
                    [-127,            None],
                    [127,             None],
                    [None,            2],
                    [IGNORE,          None],
                    [IGNORE,          None],
                    [4,               None],
                    [4,               None],
                    [4,               None],
                    [4,               IGNORE],
                    [None,            None],
                    [None,            1.2],
                    ["dddafadfadfadf", None],
                    [None, "a long string with 中文?字符"],
                    [None, 1626861392591],
                    [None, None]
                    
                ]
            ]

            datas4_expect = [
                # table 1
                [
                    [1626861392589111,1626861392591111],
                    [None,            False],
                    [-127,            None],
                    [127,             None],
                    [None,            2],
                    [3,               None],
                    [3,               None],
                    [4,               None],
                    [4,               None],
                    [4,               None],
                    [4,               5],
                    [None,            None],
                    [None,            1.2],
                    ["dddafadfadfadf", None],
                    [None, "a long string with 中文?字符"],
                    [None, 1626861392591],
                    [None, None]
                    
                ]
            ]

            table1 = BindTable(tbanmes[0], tags[0])
            
            for data in datas1[0]:
                table1.add_col_data(data)

            # columns type for stable
            stmt2.bind_param_with_tables([table1])
            stmt2.execute()

            # check correct
            self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, stablename, tbanmes, tags, datas1)


            table2 = BindTable(tbanmes[0], tags[0])
            
            for data in datas2[0]:
                table2.add_col_data(data)

            # columns type for stable
            stmt2.bind_param_with_tables([table2])
            stmt2.execute()

            # check correct
            self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, stablename, tbanmes, tags, datas2_expect)

            table3 = BindTable(tbanmes[0], tags[0])
            
            for data in datas3[0]:
                table3.add_col_data(data)

            # columns type for stable
            stmt2.bind_param_with_tables([table3])
            stmt2.execute()

            # check correct
            self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, stablename, tbanmes, tags, datas3_expect)

            table4 = BindTable(tbanmes[0], tags[0])
            
            for data in datas4[0]:
                table4.add_col_data(data)

            # columns type for stable
            stmt2.bind_param_with_tables([table4])
            stmt2.execute()

            # check correct
            self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, stablename, tbanmes, tags, datas4_expect)

            tdLog.info("Case [test_stmt_td31428] PASS")
        except Exception as err:
            raise err


    def select(self, conn):
        # dbname = "stmt_insert_tc"
        ctablename = 'common_table'
        try:
            self.connectstmt.execute("drop database if exists %s" % self.dbname)
            self.connectstmt.execute("create database if not exists %s PRECISION 'us' "  % self.dbname)
            self.connectstmt.select_db(self.dbname)
            self.connectstmt.execute("create table if not exists %s (ts timestamp, bo bool, nil tinyint, ti tinyint, si smallint, ii int,\
                bi bigint, tu tinyint unsigned, su smallint unsigned, iu int unsigned, bu bigint unsigned, \
                ff float, dd double, bb binary(100), nn nchar(100), tt timestamp , vc varchar(100))" % ctablename)

            stmt2 = self.connectstmt.statement2(f"insert into {ctablename} values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

            # bind data
            tbanmes = None
            tags    = None

            # prepare data
            datas1 = [
                # table 1
                [
                    # student
                    [1626861392589111,1626861392590111],
                    [True,         None],
                    [-128,         -128],
                    [0,            127],
                    [3,            None],
                    [3,            4],
                    [3,            4],
                    [3,            4],
                    [3,            4],
                    [3,            4],
                    [3,            4],
                    [3,            None],
                    [3,            None],
                    ["abc",       "dddafadfadfadf"],
                    ["涛思数据", None],
                    [None, None],
                    ["涛思数据16", None]
                ]
            ]

            table0 = BindTable(None, None)

            for data in datas1[0]:
                table0.add_col_data(data)

            # columns type for stable
            # stmt2.bind_param_with_tables([table0])
            # stmt2.execute()

            # assert stmt2.affected_rows == 2

            # prepare data
            datas2 = [
                # table 1
                [
                    # student
                    [1626861392591111],
                    [False],
                    [None],
                    [None],
                    [2],
                    [None],
                    [None],
                    [None],
                    [None],
                    [None],
                    [5],
                    [1],
                    [1.2],
                    [None],
                    ["a long string with 中文?字符"],
                    [1626861392591],
                    [None]
                    
                ]
            ]

            stmt2.bind_param(tbanmes, tags, datas2)
            stmt2.execute()

            assert stmt2.affected_rows == 3

            tdLog.info("Insert data PASS")
        except Exception as err:
            raise err
        
        # dbname = "stmt_tag"
        stablename = 'log'
        try:
            self.connectstmt.execute("drop database if exists %s" % self.dbname)
            self.connectstmt.execute("create database if not exists %s PRECISION 'us' "  % self.dbname)
            self.connectstmt.select_db(self.dbname)
            self.connectstmt.execute("create table if not exists %s(ts timestamp, bo bool, nil tinyint, ti tinyint, si smallint, ii int,\
                bi bigint, tu tinyint unsigned, su smallint unsigned, iu int unsigned, bu bigint unsigned, \
                ff float, dd double, bb binary(100), nn nchar(100), tt timestamp , vc varchar(100)) tags (t1 timestamp, t2 bool,\
                t3 tinyint, t4 tinyint, t5 smallint, t6 int, t7 bigint, t8 tinyint unsigned, t9 smallint unsigned, \
                t10 int unsigned, t11 bigint unsigned, t12 float, t13 double, t14 binary(100), t15 nchar(100), t16 timestamp)"%stablename)

            stmt2 = self.connectstmt.statement2("insert into ? using log tags (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) \
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
            tdLog.info("Insert data PASS")

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

        except Exception as err:
            raise err

    

    def run(self):
        buildPath = self.stmt_common.getBuildPath()
        config = buildPath+ "../sim/dnode1/cfg/"
        host="localhost"
        self.connectstmt=self.newcon(host,config)

        # 1. tc for common table
        # self.test_stmt_set_tbname_tag()
        # self.test_stmt_insert_common_table_with_bind_tablename_data()   # pass
        # self.test_stmt_insert_common_table_with_bind_data()  # pass
        
        # 2. tc for super table
        # self.test_stmt_insert_super_table_with_bind_ctablename_tags_data()   # pass
        # self.test_stmt_insert_super_table_with_bind_tags_data()   # pass
        # self.test_stmt_insert_super_table_with_bind_data()   # pass
        # self.test_stmt_insert_common_table_with_bind_ctablename_data(connectstmt)  # pass
        
        # 3. bug fix
        self.test_stmt_td31428()  # pass

        self.connectstmt.close()

        return


# add case with filename
#
tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
