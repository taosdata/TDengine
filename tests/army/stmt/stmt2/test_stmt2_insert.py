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

    def test_stmt2_insert_common_table_with_bind_data(self):
        ctablename = 'common_table'
        try:
            self.connectstmt.execute("drop database if exists %s" % self.dbname)
            self.connectstmt.execute("create database if not exists %s PRECISION 'us' "  % self.dbname)
            self.connectstmt.select_db(self.dbname)
            self.connectstmt.execute("create table if not exists %s (ts timestamp, bo bool, nil tinyint, ti tinyint, si smallint, ii int,\
                bi bigint, tu tinyint unsigned, su smallint unsigned, iu int unsigned, bu bigint unsigned, \
                ff float, dd double, bb binary(100), nn nchar(100), tt timestamp , vc varchar(100))" % ctablename)

            stmt2 = self.connectstmt.statement2(f"insert into {ctablename} values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

            tbnames = [ctablename]
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

            table0 = BindTable(tbnames[0], tags)

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

            stmt2.bind_param(tbnames, tags, datas2)
            stmt2.execute()

            assert stmt2.affected_rows == 1
            # check correct
            # self.checkResultCorrects(conn, self.dbname, None, tbnames, tags, datas2)

            tdLog.info("Case [test_stmt2_insert_common_table_with_bind_data] PASS")
        except Exception as err:
            raise err

    def test_stmt2_insert_common_table_with_bind_tablename_data(self):
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

            tbnames = [ctablename1, ctablename2]
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

            stmt2.bind_param(tbnames, tags, datas)
            stmt2.execute()

            # check correct
            self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, None, tbnames, tags, datas)


            # bind data
            tbnames = [ctablename3]
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

            stmt2.bind_param(tbnames, tags, datas2)
            stmt2.execute()

            # check correct
            self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, None, tbnames, tags, datas2)

            tdLog.info("Case [test_stmt2_insert_common_table_with_bind_tablename_data] PASS")
        except Exception as err:
            raise err

    def test_stmt2_insert_super_table_with_bind_ctablename_tags_data(self):
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
            tbnames = ["t1"]
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

            table0 = BindTable(tbnames[0], tags[0])

            for data in datas[0]:
                table0.add_col_data(data)

            # columns type for stable
            stmt2.bind_param_with_tables([table0])
            stmt2.execute()

            # check correct
            self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, stablename, tbnames, tags, datas)

            tdLog.info("Case [test_stmt2_insert_super_table_with_bind_ctablename_tags_data] PASS")
        except Exception as err:
            raise err

    def test_stmt2_insert_super_table_with_bind_tags_data(self):
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
            tbnames = None
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

            table0 = BindTable(tbnames, tags[0])

            for data in datas[0]:
                table0.add_col_data(data)

            # columns type for stable
            stmt2.bind_param_with_tables([table0])
            stmt2.execute()

            # check correct
            self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, stablename, ['t100'], tags, datas)

            tdLog.info("Case [test_stmt2_insert_super_table_without_bind_ctablename] PASS")
        except Exception as err:
            raise err
    
    def test_stmt2_insert_super_table_with_bind_data(self):
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
            tbnames = None
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

            tdLog.info("Case [test_stmt2_insert_super_table_with_bind_data] PASS")
        except Exception as err:
            raise err
        
    def test_stmt2_td31428(self):
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
            tbnames = ["t1"]
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

            table1 = BindTable(tbnames[0], tags[0])
            
            for data in datas1[0]:
                table1.add_col_data(data)

            # columns type for stable
            stmt2.bind_param_with_tables([table1])
            stmt2.execute()

            # check correct
            self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, stablename, tbnames, tags, datas1)


            table2 = BindTable(tbnames[0], tags[0])
            
            for data in datas2[0]:
                table2.add_col_data(data)

            # columns type for stable
            stmt2.bind_param_with_tables([table2])
            stmt2.execute()

            # check correct
            self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, stablename, tbnames, tags, datas2_expect)

            table3 = BindTable(tbnames[0], tags[0])
            
            for data in datas3[0]:
                table3.add_col_data(data)

            # columns type for stable
            stmt2.bind_param_with_tables([table3])
            stmt2.execute()

            # check correct
            self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, stablename, tbnames, tags, datas3_expect)

            table4 = BindTable(tbnames[0], tags[0])
            
            for data in datas4[0]:
                table4.add_col_data(data)

            # columns type for stable
            stmt2.bind_param_with_tables([table4])
            stmt2.execute()

            # check correct
            self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, stablename, tbnames, tags, datas4_expect)

            tdLog.info("Case [test_stmt2_td31428] PASS")
        except Exception as err:
            raise err

    def test_stmt2_td31647(self):
        stablename = 'stmt_td31647'
        try:
            self.connectstmt.execute("drop database if exists %s" % self.dbname)
            self.connectstmt.execute("create database if not exists %s PRECISION 'us' "  % self.dbname)
            self.connectstmt.select_db(self.dbname)
            self.connectstmt.execute("create table if not exists %s (ts timestamp, bo bool, nil tinyint) tags (t1 timestamp, t2 bool, t3 tinyint)" % stablename)

            stmt2 = self.connectstmt.statement2(f"insert into ? using {stablename} tags (?,?,?) values (?,?,?)")

            ret = stmt2.get_fields(TAOS_FIELD_TBNAME)
            assert ret[0] == 1
            ret = None

            # bind data
            tbnames = ["t1"]
            tags    = [
                [1601481600000, True, 1]
            ]

            # prepare data
            datas1 = [
                # table 1
                [
                    # student
                    [1626861392589111],
                    [True],
                    [-128]
                ]
            ]

            table1 = BindTable(tbnames[0], tags[0])
            
            for data in datas1[0]:
                table1.add_col_data(data)

            # columns type for stable
            stmt2.bind_param_with_tables([table1])
            ret = stmt2.get_fields(TAOS_FIELD_TBNAME)
            assert ret[0] == 1

            stmt2 = self.connectstmt.statement2(f"insert into t2 using {stablename} tags (?,?,?) values (?,?,?)")

            ret = stmt2.get_fields(TAOS_FIELD_TBNAME)
            assert ret[0] == 0

            stmt2.bind_param(['t2'], tags, datas1)
            assert ret[0] == 0

            tdLog.info("Case [test_stmt2_td31647] PASS")
        except Exception as err:
            raise err
        
    def test_stmt2_insert_super_table_auto_create_table(self):
        stablename = 'stmt_insert_super_table_auto_creat_table_with_bind_tags'
        try:
            self.connectstmt.execute("drop database if exists %s" % self.dbname)
            self.connectstmt.execute("create database if not exists %s PRECISION 'us' "  % self.dbname)
            self.connectstmt.select_db(self.dbname)
            self.connectstmt.execute(f"CREATE STABLE {stablename} (`ts` TIMESTAMP , `current` FLOAT , `voltage` INT , `phase` FLOAT) TAGS (`location` VARCHAR(64), `groupid` INT)")

            stmt2 = self.connectstmt.statement2(f"insert into {stablename} (tbname, location, groupId, ts, current, voltage, phase) values(?,?,?,?,?,?,?”)")

            # bind data
            # tbnames = ['t1']
            # tags    = None

            # prepare data
            datas1 = [
                # table 1
                [
                    ['t1'],
                    ['Bj'],
                    [100],
                    [1626861392589111],
                    [3.1415926],
                    [110],
                    [3.7758521]
                ]
            ]

            table1 = BindTable('t1', None)
            
            for data in datas1[0]:
                table1.add_col_data(data)

            # columns type for stable
            stmt2.bind_param_with_tables([table1])

            tdLog.exit("Case [test_stmt2_insert_super_table_auto_creat_table_with_bind_tags] FAIL")
        except Exception as err:
            assert err.msg == 'insert into super table syntax is not supported for stmt'
            tdLog.info("Case [test_stmt2_insert_super_table_auto_creat_table_with_bind_tags] PASS")

    def test_stmt2_insert_super_table_muti_table_muti_rows_muti_cols(self):
        stablename = 'stmt_insert_super_table_auto_single_table_muti_rows_muti_cols'
        try:
            self.connectstmt.execute("drop database if exists %s" % self.dbname)
            self.connectstmt.execute("create database if not exists %s PRECISION 'us' "  % self.dbname)
            self.connectstmt.select_db(self.dbname)
            self.connectstmt.execute(f"CREATE STABLE {stablename} (`ts` TIMESTAMP , `current` FLOAT , `voltage` INT , `phase` FLOAT) TAGS (`location` VARCHAR(64), `groupid` INT)")

            stmt2 = self.connectstmt.statement2(f"insert into ? using {stablename} tags (?,?) values(?,?,?,?)")

            # bind data
            tbnames = ['t1', 't2']
            tags    = [
                ['Bj', 100],
                ['London', None],
            ]

            # prepare data
            datas = [
                # table 1
                [
                    [1626861392589111, 1626861392589222],
                    [3.1415926,        3.1415926],
                    [110,              110],
                    [3.7758521,        3.7758521]
                ],
                # table 2
                [
                    [1626861392589333, 1626861392589444],
                    [3.1415,           3.1415],
                    [2000,             2000],
                    [3.345,            3.345]
                ]
            ]

            table1 = BindTable(tbnames[0], tags[0])
            
            for data in datas[0]:
                table1.add_col_data(data)

            table2 = BindTable(tbnames[1], tags[1])
            
            for data in datas[1]:
                table2.add_col_data(data)

            # columns type for stable
            stmt2.bind_param_with_tables([table1, table2])
            stmt2.execute()

            # check correct
            self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, stablename, tbnames, tags, datas)

            self.connectstmt.execute(f"DROP STABLE if exists {stablename}")
            self.connectstmt.execute(f"CREATE STABLE {stablename} (`ts` TIMESTAMP , `current` FLOAT , `voltage` INT , `phase` FLOAT) TAGS (`location` VARCHAR(64), `groupid` INT)")

            stmt2.prepare(f"insert into ? using {stablename} tags (?,?) values(?,?,?,?)")
            # columns type for stable
            stmt2.bind_param(tbnames, tags, datas)
            stmt2.execute()

            # check correct
            self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, stablename, tbnames, tags, datas)

            tdLog.info("Case [stmt_insert_super_table_auto_single_table_muti_rows_muti_cols] PASS")
        except Exception as err:
            raise err
            
    def test_stmt2_insert_super_table_muti_table_single_rows_muti_cols(self):
        stablename = 'stmt_insert_super_table_auto_single_table_single_rows_muti_cols'
        try:
            self.connectstmt.execute("drop database if exists %s" % self.dbname)
            self.connectstmt.execute("create database if not exists %s PRECISION 'us' "  % self.dbname)
            self.connectstmt.select_db(self.dbname)
            self.connectstmt.execute(f"CREATE STABLE {stablename} (`ts` TIMESTAMP , `current` FLOAT , `voltage` INT , `phase` FLOAT) TAGS (`location` VARCHAR(64), `groupid` INT)")

            stmt2 = self.connectstmt.statement2(f"insert into ? using {stablename} tags (?,?) values(?,?,?,?)")

            # bind data
            tbnames = ['t1', 't2']
            tags    = [
                ['Bj', 100],
                ['London', None],
            ]

            # prepare data
            datas = [
                # table 1
                [
                    [1626861392589111],
                    [3.1415926],
                    [110],
                    [3.7758521]
                ],
                # table 2
                [
                    [1626861392589333, 1626861392589444],
                    [3.1415,           3.1415],
                    [2000,             2000],
                    [3.345,            3.345]
                ]
            ]

            table1 = BindTable(tbnames[0], tags[0])
            
            for data in datas[0]:
                table1.add_col_data(data)

            table2 = BindTable(tbnames[1], tags[1])
            
            for data in datas[1]:
                table2.add_col_data(data)

            # columns type for stable
            stmt2.bind_param_with_tables([table1, table2])
            stmt2.execute()

            # check correct
            self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, stablename, tbnames, tags, datas)

            self.connectstmt.execute(f"DROP STABLE if exists {stablename}")
            self.connectstmt.execute(f"CREATE STABLE {stablename} (`ts` TIMESTAMP , `current` FLOAT , `voltage` INT , `phase` FLOAT) TAGS (`location` VARCHAR(64), `groupid` INT)")

            stmt2.prepare(f"insert into ? using {stablename} tags (?,?) values(?,?,?,?)")
            # columns type for stable
            stmt2.bind_param(tbnames, tags, datas)
            stmt2.execute()

            # check correct
            self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, stablename, tbnames, tags, datas)

            tdLog.info("Case [stmt_insert_super_table_auto_single_table_single_rows_muti_cols] PASS")
        except Exception as err:
            raise err
        
    def test_stmt2_insert_super_table_muti_table_muti_rows_single_cols(self):
        stablename = 'stmt_insert_super_table_auto_single_table_single_rows_muti_cols'
        try:
            self.connectstmt.execute("drop database if exists %s" % self.dbname)
            self.connectstmt.execute("create database if not exists %s PRECISION 'us' "  % self.dbname)
            self.connectstmt.select_db(self.dbname)
            self.connectstmt.execute(f"CREATE STABLE {stablename} (`ts` TIMESTAMP , `current` FLOAT , `voltage` INT , `phase` FLOAT) TAGS (`location` VARCHAR(64), `groupid` INT)")

            stmt2 = self.connectstmt.statement2(f"insert into ? using {stablename} (location) tags (?) (ts,current) values(?, ?)")

            # bind data
            tbnames = ['t1', 't2']
            tags    = [
                ['Bj'],
                ['London'],
            ]

            tags_expect    = [
                ['Bj', None],
                ['London', None],
            ]

            # prepare data
            datas = [
                # table 1
                [
                    [1626861392589111],
                    [3.1415926]
                ],
                # table 2
                [
                    [1626861392589333, 1626861392589444],
                    [3.1415,           3.1415]
                ]
            ]

            datas_expect = [
                # table 1
                [
                    [1626861392589111],
                    [3.1415926],
                    [None],
                    [None]
                ],
                # table 2
                [
                    [1626861392589333, 1626861392589444],
                    [3.1415,           3.1415],
                    [None,             None],
                    [None,             None]
                ]
            ]

            table1 = BindTable(tbnames[0], tags[0])
            
            for data in datas[0]:
                table1.add_col_data(data)

            table2 = BindTable(tbnames[1], tags[1])
            
            for data in datas[1]:
                table2.add_col_data(data)

            # columns type for stable
            stmt2.bind_param_with_tables([table1, table2])
            stmt2.execute()

            # check correct
            self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, stablename, tbnames, tags_expect, datas_expect)

            self.connectstmt.execute(f"DROP STABLE if exists {stablename}")
            self.connectstmt.execute(f"CREATE STABLE {stablename} (`ts` TIMESTAMP , `current` FLOAT , `voltage` INT , `phase` FLOAT) TAGS (`location` VARCHAR(64), `groupid` INT)")

            stmt2.prepare(f"insert into ? using {stablename} (location) tags (?) (ts,current) values(?, ?)")
            # columns type for stable
            stmt2.bind_param(tbnames, tags, datas)
            stmt2.execute()

            # check correct
            self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, stablename, tbnames, tags_expect, datas_expect)

            tdLog.info("Case [stmt_insert_super_table_auto_single_table_single_rows_muti_cols] PASS")
        except Exception as err:
            raise err
        
    def test_stmt2_tag_number_not_match(self):
        stablename = 'test_stmt2_tag_number_not_match'

        # bind data
        tbnames = ['t1', 't2']
        tags    = [
            ['Bj'],
            ['London'],
        ]

        # prepare data
        datas = [
            # table 1
            [
                [1626861392589111],
                [3.1415926]
            ],
            # table 2
            [
                [1626861392589333, 1626861392589444],
                [3.1415,           3.1415]
            ]
        ]

        incorrect_datas = [
            # table 1
            [
                [1626861392589111],
                [3.1415926],
                [3.1415926]
            ],
            # table 2
            [
                [1626861392589333, 1626861392589444],
                [3.1415,           3.1415],
                [3.1415,           3.1415]
            ]
        ]

        table1 = BindTable(tbnames[0], tags[0])
        
        for data in datas[0]:
            table1.add_col_data(data)

        table2 = BindTable(tbnames[1], tags[1])
        
        for data in datas[1]:
            table2.add_col_data(data)

        self.connectstmt.execute("drop database if exists %s" % self.dbname)
        self.connectstmt.execute("create database if not exists %s PRECISION 'us' "  % self.dbname)
        self.connectstmt.select_db(self.dbname)
        
        # throw error using bind_param_with_tables
        try:
            self.connectstmt.execute(f"CREATE STABLE {stablename} (`ts` TIMESTAMP , `current` FLOAT , `voltage` INT , `phase` FLOAT) TAGS (`location` VARCHAR(64), `groupid` INT)")

            stmt2 = self.connectstmt.statement2(f"insert into ? using {stablename} (location) tags (?,?) (ts,current) values(?, ?)")            
              
            # columns type for stable
            stmt2.bind_param_with_tables([table1, table2])
            
            tdLog.exit("Case [test_stmt2_tag_number_not_match] Fail")
        except Exception as err:
            assert err.msg == 'Tags number not matched'

        # throw error using bind_param
        try:
            self.connectstmt.execute(f"DROP STABLE if exists {stablename}")
            self.connectstmt.execute(f"CREATE STABLE {stablename} (`ts` TIMESTAMP , `current` FLOAT , `voltage` INT , `phase` FLOAT) TAGS (`location` VARCHAR(64), `groupid` INT)")

            stmt2 = self.connectstmt.statement2(f"insert into ? using {stablename} (location) tags (?,?) (ts,current) values(?, ?)")            
              
            # columns type for stable
            stmt2.bind_param(tbnames, tags, datas)
            
            tdLog.exit("Case [test_stmt2_tag_number_not_match] Fail")
        except Exception as err:
            assert err.msg == 'Tags number not matched'

        # throw error using bind_param with incorrect_datas
        try:
            self.connectstmt.execute(f"DROP STABLE if exists {stablename}")
            self.connectstmt.execute(f"CREATE STABLE {stablename} (`ts` TIMESTAMP , `current` FLOAT , `voltage` INT , `phase` FLOAT) TAGS (`location` VARCHAR(64), `groupid` INT)")

            stmt2 = self.connectstmt.statement2(f"insert into ? using {stablename} tags (?,?) (ts,current) values(?, ?)")            
              
            # columns type for stable
            stmt2.bind_param(tbnames, tags, incorrect_datas)
            
            tdLog.exit("Case [test_stmt2_tag_number_not_match] Fail")
        except Exception as err:
            assert err.args[0] == 'list index out of range'

        tdLog.info("Case [test_stmt2_tag_number_not_match] PASS")

    def test_stmt2_col_number_not_match(self):
        stablename = 'test_stmt2_col_number_not_match'

        # bind data
        tbnames = ['t1', 't2']
        tags    = [
            ['Bj', 1],
            ['London', 2],
        ]

        # prepare data
        datas = [
            # table 1
            [
                [1626861392589111],
                [3.1415926],
                [3]
            ],
            # table 2
            [
                [1626861392589333, 1626861392589444],
                [3.1415,           3.1415],
                [4,           3]
            ]
        ]

        incorrect_datas = [
            # table 1
            [
                [1626861392589111],
                [3.1415926],
                [3.1415926]
            ],
            # table 2
            [
                [1626861392589333, 1626861392589444],
                [3.1415,           3.1415],
                [3.1415,           3.1415]
            ]
        ]

        table1 = BindTable(tbnames[0], tags[0])
        
        for data in datas[0]:
            table1.add_col_data(data)

        table2 = BindTable(tbnames[1], tags[1])
        
        for data in datas[1]:
            table2.add_col_data(data)

        self.connectstmt.execute("drop database if exists %s" % self.dbname)
        self.connectstmt.execute("create database if not exists %s PRECISION 'us' "  % self.dbname)
        self.connectstmt.select_db(self.dbname)

        # throw error using bind_param_with_tables
        try:
            self.connectstmt.execute(f"CREATE STABLE {stablename} (`ts` TIMESTAMP , `current` FLOAT , `voltage` INT , `phase` FLOAT) TAGS (`location` VARCHAR(64), `groupid` INT)")

            stmt2 = self.connectstmt.statement2(f"insert into ? using {stablename} tags (?,?) (ts,current) values(?, ?, ?)")            
              
            # columns type for stable
            stmt2.bind_param_with_tables([table1, table2])
            
            tdLog.exit("Case [test_stmt2_tag_number_not_match] Fail")
        except Exception as err:
            assert err.msg == 'Illegal number of columns'

        # throw error using bind_param
        try:
            self.connectstmt.execute(f"DROP STABLE if exists {stablename}")
            self.connectstmt.execute(f"CREATE STABLE {stablename} (`ts` TIMESTAMP , `current` FLOAT , `voltage` INT , `phase` FLOAT) TAGS (`location` VARCHAR(64), `groupid` INT)")

            stmt2 = self.connectstmt.statement2(f"insert into ? using {stablename} tags (?,?) (ts,current) values(?, ?, ?)")          
              
            # columns type for stable
            stmt2.bind_param(tbnames, tags, datas)
            
            tdLog.exit("Case [test_stmt2_tag_number_not_match] Fail")
        except Exception as err:
            assert err.msg == 'Illegal number of columns'

        # throw error using bind_param with incorrect_datas
        try:
            self.connectstmt.execute(f"DROP STABLE if exists {stablename}")
            self.connectstmt.execute(f"CREATE STABLE {stablename} (`ts` TIMESTAMP , `current` FLOAT , `voltage` INT , `phase` FLOAT) TAGS (`location` VARCHAR(64), `groupid` INT)")

            stmt2 = self.connectstmt.statement2(f"insert into ? using {stablename} tags (?,?) (ts,current) values(?, ?)")            
              
            # columns type for stable
            stmt2.bind_param(tbnames, tags, incorrect_datas)
            
            tdLog.exit("Case [test_stmt2_tag_number_not_match] Fail")
        except Exception as err:
            assert err.args[0] == 'list index out of range'

        tdLog.info("Case [test_stmt2_col_number_not_match] PASS")

    def test_stmt2_not_support_normal_value_in_sql(self):
        stablename = 'test_stmt2_not_support_normal_value_in_sql'

        # prepare data
        tbnames = ['t1', 't2']
        tags    = [
            ['Bj', 100],
            ['London', None],
        ]

        # prepare data
        datas = [
            # table 1
            [
                [1626861392589111],
                [3.1415926]
            ],
            # table 2
            [
                [1626861392589333, 1626861392589444],
                [3.1415,           3.1415]
            ]
        ]

        table1 = BindTable(tbnames[0], tags[0])
        
        for data in datas[0]:
            table1.add_col_data(data)

        table2 = BindTable(tbnames[1], tags[1])
        
        for data in datas[1]:
            table2.add_col_data(data)

        try:
            self.connectstmt.execute("drop database if exists %s" % self.dbname)
            self.connectstmt.execute("create database if not exists %s PRECISION 'us' "  % self.dbname)
            self.connectstmt.select_db(self.dbname)
            self.connectstmt.execute(f"CREATE STABLE {stablename} (`ts` TIMESTAMP , `current` FLOAT , `voltage` INT , `phase` FLOAT) TAGS (`location` VARCHAR(64), `groupid` INT)")

            stmt2 = self.connectstmt.statement2(f"insert into ? using {stablename} tags (?,?) values(?,?,1,2)")

            # columns type for stable
            stmt2.bind_param_with_tables([table1, table2])
            tdLog.exit("Case [test_stmt2_not_support_normal_value_in_sql] Fail")
        except Exception as err:
            assert err.msg == 'stmt bind param does not support normal value in sql'
        
        try:
            self.connectstmt.execute(f"DROP STABLE if exists {stablename}")
            self.connectstmt.execute(f"CREATE STABLE {stablename} (`ts` TIMESTAMP , `current` FLOAT , `voltage` INT , `phase` FLOAT) TAGS (`location` VARCHAR(64), `groupid` INT)")

            stmt2.prepare(f"insert into ? using {stablename} tags (?,?) values(?,4,3,?)")
            # columns type for stable
            stmt2.bind_param(tbnames, tags, datas)
            tdLog.exit("Case [test_stmt2_not_support_normal_value_in_sql] Fail")
        except Exception as err:
            assert err.msg == 'stmt bind param does not support normal value in sql'
        tdLog.info("Case [test_stmt2_not_support_normal_value_in_sql] PASS")

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
            tbnames = None
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

            stmt2.bind_param(tbnames, tags, datas2)
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
            tbnames = ["t1"]
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

            table0 = BindTable(tbnames[0], tags[0])

            for data in datas[0]:
                table0.add_col_data(data)

            # columns type for stable
            stmt2.bind_param_with_tables([table0])
            stmt2.execute()

            assert stmt2.affected_rows == 3
            tdLog.info("Insert data PASS")

            # query1: all
            tbnames = None
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
            stmt2.bind_param(tbnames, tags, datas)

            stmt2.execute()
            # get result
            rows = stmt2.result()

            self.stmt_common.compare_result(conn, "select * from log where bu < 10", rows)
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
            stmt2.bind_param(tbnames, tags, datas)

            stmt2.execute()
            # get result
            rows = stmt2.result()

            self.stmt_common.compare_result(conn, "select abs(5) from log where bu < 5", rows)
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
            stmt2.bind_param(tbnames, tags, datas)

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
            stmt2.bind_param(tbnames, tags, datas)

            stmt2.execute()
            # get result
            rows = stmt2.result()

            self.stmt_common.compare_result(conn, "select CHAR_LENGTH('中文字符') from log", rows)
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
            stmt2.bind_param(tbnames, tags, datas)

            stmt2.execute()
            # get result
            rows = stmt2.result()

            self.stmt_common.compare_result(conn, "select CHAR_LENGTH('123') from log", rows)
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
            stmt2.bind_param(tbnames, tags, datas)

            stmt2.execute()
            # get result
            rows = stmt2.result()

            self.stmt_common.compare_result(conn, "select cast( '1232a' as bigint) from log", rows)
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
            stmt2.bind_param(tbnames, tags, datas)

            stmt2.execute()
            # get result
            rows = stmt2.result()

            self.stmt_common.compare_result(conn, "select cast( 123 as binary(10)) from log", rows)
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
            stmt2.bind_param(tbnames, tags, datas)

            stmt2.execute()
            # get result
            rows = stmt2.result()

            self.stmt_common.compare_result(conn, "select timediff('2021-07-21 17:56:32.590111', 1626861392591112, 1a) from log", rows)
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
            stmt2.bind_param(tbnames, tags, datas)

            stmt2.execute()
            # get result
            rows = stmt2.result()

            self.stmt_common.compare_result(conn, "select count(123) from log", rows)
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
            stmt2.bind_param(tbnames, tags, datas)

            stmt2.execute()
            # get result
            rows = stmt2.result()

            self.stmt_common.compare_result(conn, "select bottom(bu,2) from log group by bu order by bu desc", rows)
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
            stmt2.bind_param(tbnames, tags, datas)

            stmt2.execute()
            # get result
            rows = stmt2.result()

            self.stmt_common.compare_result(conn, "select twa(15) from log", rows)
            tdLog.info("[query9: time-series specific Functions] PASS")

        except Exception as err:
            raise err

    def test_stmt2_insert_super_table_simple_table_muti_rows_muti_cols(self):
        stablename = 'test_stmt2_insert_super_table_simple_table_muti_rows_muti_cols'
        try:
            self.connectstmt.execute("drop database if exists %s" % self.dbname)
            self.connectstmt.execute("create database if not exists %s PRECISION 'us' "  % self.dbname)
            self.connectstmt.select_db(self.dbname)
            self.connectstmt.execute(f"CREATE STABLE {stablename} (`ts` TIMESTAMP , `current` FLOAT , `voltage` INT , `phase` FLOAT) TAGS (`location` VARCHAR(64), `groupid` INT)")

            stmt2 = self.connectstmt.statement2(f"insert into ? using {stablename} tags (?,?) values(?,?,?,?)")

            # bind data
            tbnames = ['t1']
            tags    = [
                ['Bj', 100],
            ]

            # prepare data
            datas = [
                # table 1
                [
                    [1626861392589111, 1626861392589222],
                    [3.1415926,        3.1415926],
                    [110,              110],
                    [3.7758521,        3.7758521]
                ],
            ]

            table1 = BindTable(tbnames[0], tags[0])
            
            for data in datas[0]:
                table1.add_col_data(data)

            # columns type for stable
            stmt2.bind_param_with_tables([table1])
            stmt2.execute()

            # check correct
            self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, stablename, tbnames, tags, datas)

            self.connectstmt.execute(f"DROP STABLE if exists {stablename}")
            self.connectstmt.execute(f"CREATE STABLE {stablename} (`ts` TIMESTAMP , `current` FLOAT , `voltage` INT , `phase` FLOAT) TAGS (`location` VARCHAR(64), `groupid` INT)")

            stmt2.prepare(f"insert into ? using {stablename} tags (?,?) values(?,?,?,?)")
            # columns type for stable
            stmt2.bind_param(tbnames, tags, datas)
            stmt2.execute()

            # check correct
            self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, stablename, tbnames, tags, datas)

            tdLog.info("Case [stmt_insert_super_table_auto_single_table_muti_rows_muti_cols] PASS")
        except Exception as err:
            raise err

    def test_stmt2_insert_super_table_simple_table_single_rows_muti_cols(self):
        stablename = 'stmt_insert_super_table_auto_single_table_single_rows_muti_cols'
        try:
            self.connectstmt.execute("drop database if exists %s" % self.dbname)
            self.connectstmt.execute("create database if not exists %s PRECISION 'us' "  % self.dbname)
            self.connectstmt.select_db(self.dbname)
            self.connectstmt.execute(f"CREATE STABLE {stablename} (`ts` TIMESTAMP , `current` FLOAT , `voltage` INT , `phase` FLOAT) TAGS (`location` VARCHAR(64), `groupid` INT)")

            stmt2 = self.connectstmt.statement2(f"insert into ? using {stablename} tags (?,?) values(?,?,?,?)")

            # bind data
            tbnames = ['t1']
            tags    = [
                ['Bj', 100],
            ]

            # prepare data
            datas = [
                # table 2
                [
                    [1626861392589333, 1626861392589444],
                    [3.1415,           3.1415],
                    [2000,             2000],
                    [3.345,            3.345]
                ]
            ]

            table1 = BindTable(tbnames[0], tags[0])
            
            for data in datas[0]:
                table1.add_col_data(data)

            # columns type for stable
            stmt2.bind_param_with_tables([table1])
            stmt2.execute()

            # check correct
            self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, stablename, tbnames, tags, datas)

            self.connectstmt.execute(f"DROP STABLE if exists {stablename}")
            self.connectstmt.execute(f"CREATE STABLE {stablename} (`ts` TIMESTAMP , `current` FLOAT , `voltage` INT , `phase` FLOAT) TAGS (`location` VARCHAR(64), `groupid` INT)")

            stmt2.prepare(f"insert into ? using {stablename} tags (?,?) values(?,?,?,?)")
            # columns type for stable
            stmt2.bind_param(tbnames, tags, datas)
            stmt2.execute()

            # check correct
            self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, stablename, tbnames, tags, datas)

            tdLog.info("Case [stmt_insert_super_table_auto_single_table_single_rows_muti_cols] PASS")
        except Exception as err:
            raise err
    
    def test_stmt2_insert_super_table_simple_table_muti_rows_single_cols(self):
        stablename = 'stmt_insert_super_table_auto_single_table_single_rows_muti_cols'
        try:
            self.connectstmt.execute("drop database if exists %s" % self.dbname)
            self.connectstmt.execute("create database if not exists %s PRECISION 'us' "  % self.dbname)
            self.connectstmt.select_db(self.dbname)
            self.connectstmt.execute(f"CREATE STABLE {stablename} (`ts` TIMESTAMP , `current` FLOAT , `voltage` INT , `phase` FLOAT) TAGS (`location` VARCHAR(64), `groupid` INT)")

            stmt2 = self.connectstmt.statement2(f"insert into ? using {stablename} (location) tags (?) (ts,current) values(?, ?)")

            # bind data
            tbnames = ['t1']
            tags    = [
                ['Bj'],
            ]

            tags_expect    = [
                ['Bj', None],
            ]

            # prepare data
            datas = [
                [
                    [1626861392589333, 1626861392589444],
                    [3.1415,           3.1415]
                ]
            ]

            datas_expect = [
                # table 2
                [
                    [1626861392589333, 1626861392589444],
                    [3.1415,           3.1415],
                    [None,             None],
                    [None,             None]
                ]
            ]

            table1 = BindTable(tbnames[0], tags[0])
            
            for data in datas[0]:
                table1.add_col_data(data)


            # columns type for stable
            stmt2.bind_param_with_tables([table1])
            stmt2.execute()

            # check correct
            self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, stablename, tbnames, tags_expect, datas_expect)

            self.connectstmt.execute(f"DROP STABLE if exists {stablename}")
            self.connectstmt.execute(f"CREATE STABLE {stablename} (`ts` TIMESTAMP , `current` FLOAT , `voltage` INT , `phase` FLOAT) TAGS (`location` VARCHAR(64), `groupid` INT)")

            stmt2.prepare(f"insert into ? using {stablename} (location) tags (?) (ts,current) values(?, ?)")
            # columns type for stable
            stmt2.bind_param(tbnames, tags, datas)
            stmt2.execute()

            # check correct
            self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, stablename, tbnames, tags_expect, datas_expect)

            tdLog.info("Case [stmt_insert_super_table_auto_single_table_single_rows_muti_cols] PASS")
        except Exception as err:
            raise err
    
    def test_stmt2_insert_common_table_simple_table_muti_rows_muti_cols(self):
        ctablename1 = 'common_table1'
        ctablename2 = 'common_table2'
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
            
            stmt2 = self.connectstmt.statement2(f"insert into ? values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

            tbnames = [ctablename1]
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
                ]
            ]  

            stmt2.bind_param(tbnames, tags, datas)
            stmt2.execute()

            # check correct
            self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, None, tbnames, tags, datas)

            tdLog.info("Case [test_stmt2_insert_common_table_simple_table_muti_rows_muti_cols] PASS")
        except Exception as err:
            raise err
    
    def test_stmt2_insert_common_table_simple_table_muti_rows_single_cols(self):
        ctablename1 = 'common_table1'
        ctablename2 = 'common_table2'

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
            
            stmt2 = self.connectstmt.statement2(f"insert into ? (ts, bo) values (?,?)")

            tbnames = [ctablename1]
            tags    = None

            datas = [
                [
                    [1626861392589111,1626861392590111],
                    [True,         None]
                ]
            ]  

            expect_datas = [
                [
                    [1626861392589111,1626861392590111],
                    [True,         None],
                    [None,         None],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None]
                ]
            ]

            stmt2.bind_param(tbnames, tags, datas)
            stmt2.execute()

            # check correct
            self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, None, tbnames, tags, expect_datas)

            tbnames2 = [ctablename2]
            table2 = BindTable(tbnames2[0], None)
            
            for data in datas[0]:
                table2.add_col_data(data)


            # columns type for stable
            stmt2.bind_param_with_tables([table2])

            # check correct
            self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, None, tbnames2, tags, expect_datas)

            tdLog.info("Case [test_stmt2_insert_common_table_simple_table_muti_rows_single_cols] PASS")
        except Exception as err:
            raise err
    
    def test_stmt2_insert_common_table_muti_table_muti_rows_single_cols(self):
        ctablename1 = 'common_table1'
        ctablename2 = 'common_table2'
        ctablename3 = 'common_table3'
        ctablename4 = 'common_table4'

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
            self.connectstmt.execute("create table if not exists %s (ts timestamp, bo bool, nil tinyint, ti tinyint, si smallint, ii int,\
                bi bigint, tu tinyint unsigned, su smallint unsigned, iu int unsigned, bu bigint unsigned, \
                ff float, dd double, bb binary(100), nn nchar(100), tt timestamp , vc varchar(100))" % ctablename4)
            
            stmt2 = self.connectstmt.statement2(f"insert into ? (ts, bo) values (?,?)")

            tbnames = [ctablename1, ctablename2]
            tags    = None

            datas = [
                [
                    [1626861392589111,1626861392590111],
                    [True,         None]
                ],
                [
                    [1626861392589111,1626861392590111],
                    [True,         False]
                ]
            ]  

            expect_datas = [
                [
                    [1626861392589111,1626861392590111],
                    [True,         None],
                    [None,         None],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None]
                ],
                [
                    [1626861392589111,1626861392590111],
                    [True,         False],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None]
                ]
            ]

            stmt2.bind_param(tbnames, tags, datas)
            stmt2.execute()

            # check correct
            self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, None, tbnames, tags, expect_datas)

            # tbnames2 = [ctablename3, ctablename4]
            # table3 = BindTable(tbnames2[0], None)
            
            # for data in datas[0]:
            #     table3.add_col_data(data)

            # table4 = BindTable(tbnames2[1], None)
            
            # for data in datas[1]:
            #     table4.add_col_data(data)


            # # columns type for stable
            # stmt2.bind_param_with_tables([table3, table4])

            # # check correct
            # self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, None, tbnames2, tags, expect_datas)

            tdLog.info("Case [test_stmt2_insert_common_table_muti_table_muti_rows_single_cols] PASS")
        except Exception as err:
            raise err
    
    def test_stmt2_not_support_insert_common_table_with_bind_table_name(self):
        try:
            self.connectstmt.statement2(f"insert into ? (ts, bo) values (?,?)")

            tdLog.info("Case [test_stmt2_not_support_insert_common_table_with_bind_table_name] FAIL")
        except Exception as err:
            assert 'obtain schema failed, maybe this sql is not supported' in err.msg
            tdLog.info("Case [test_stmt2_not_support_insert_common_table_with_bind_table_name] PASS")
        
    def run(self):
        buildPath = self.stmt_common.getBuildPath()
        config = buildPath+ "../sim/dnode1/cfg/"
        host="localhost"
        self.connectstmt=self.newcon(host,config)

        # 1. tc for common table
        # self.test_stmt2_set_tbname_tag()
        # self.test_stmt2_insert_common_table_with_bind_tablename_data()   # pass
        # self.test_stmt2_insert_common_table_with_bind_data()  # pass

        # self.test_stmt2_insert_common_table_simple_table_muti_rows_muti_cols()  # pass
        # self.test_stmt2_insert_common_table_simple_table_muti_rows_single_cols()  # pass
        # self.test_stmt2_insert_common_table_muti_table_muti_rows_single_cols()  #fail
        self.test_stmt2_not_support_insert_common_table_with_bind_table_name()  #fail TD-32478



        # 2. tc for super table
        # self.test_stmt2_insert_super_table_with_bind_ctablename_tags_data()   # pass
        # self.test_stmt2_insert_super_table_with_bind_tags_data()   # pass
        # self.test_stmt2_insert_super_table_with_bind_data()   # pass
        # self.test_stmt2_insert_super_table_muti_table_muti_rows_muti_cols()  # pass
        # self.test_stmt2_insert_super_table_muti_table_single_rows_muti_cols()  # pass
        # self.test_stmt2_insert_super_table_muti_table_muti_rows_single_cols()  # pass

        # self.test_stmt2_insert_super_table_simple_table_muti_rows_muti_cols()  # pass
        # self.test_stmt2_insert_super_table_simple_table_single_rows_muti_cols()  # pass
        # self.test_stmt2_insert_super_table_simple_table_muti_rows_single_cols()  # pass
        
        # self.test_stmt2_not_support_normal_value_in_sql()  # 

        # self.test_stmt2_tag_number_not_match()  # pass
        # self.test_stmt2_col_number_not_match()  # pass

        
        # 3. bug fix
        # self.test_stmt2_td31428()  # pass
        # self.test_stmt2_td31647()  # 等待python支持该功能验证

        if self.connectstmt:
            self.connectstmt.close()

        return


# add case with filename
#
tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
