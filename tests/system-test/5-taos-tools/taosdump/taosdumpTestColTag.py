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
import time
import os
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql


class TDTestCase:
    def caseDescription(self):
        '''
        case1<xyguo>:[TD-10540]The escape char "`" can be used for both tag name and column name
        case2<xyguo>:[TD-12435]create table as cause column error;
        '''
        return

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.tmpdir = "tmp"
        now = time.time()
        self.ts = int(round(now * 1000))

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        buildPath = ""
        for root, dirs, files in os.walk(projPath):
            if ("taosdump" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/build/bin")]
                    break
        return buildPath

    def table1_checkall(self, sql):
        tdLog.info(sql)
        tdSql.query(sql)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 3)
        tdSql.checkData(0, 4, 4)
        tdSql.checkData(0, 5, 'True')
        tdSql.checkData(0, 6, 6)
        tdSql.checkData(0, 7, 7)
        tdSql.checkData(0, 8, 8)
        tdSql.checkData(0, 9, 9)
        tdSql.checkData(0, 10, '1970-01-01 08:00:00.010')

    def table1_checkall_1(self, sql):
        tdSql.query(sql)
        tdSql.checkData(0, 1, 1)

    def table1_checkall_2(self, sql):
        self.table1_checkall_1(sql)
        tdSql.checkData(0, 2, 2)

    def table1_checkall_3(self, sql):
        self.table1_checkall_2(sql)
        tdSql.checkData(0, 3, 3)

    def table1_checkall_4(self, sql):
        self.table1_checkall_3(sql)
        tdSql.checkData(0, 4, 4)

    def table1_checkall_5(self, sql):
        self.table1_checkall_4(sql)
        tdSql.checkData(0, 5, 'True')

    def table1_checkall_6(self, sql):
        self.table1_checkall_5(sql)
        tdSql.checkData(0, 6, 6)

    def table1_checkall_7(self, sql):
        self.table1_checkall_6(sql)
        tdSql.checkData(0, 7, 7)

    def table1_checkall_8(self, sql):
        self.table1_checkall_7(sql)
        tdSql.checkData(0, 8, 8)

    def table1_checkall_9(self, sql):
        self.table1_checkall_8(sql)
        tdSql.checkData(0, 9, 9)

    def table1_checkall_10(self, sql):
        self.table1_checkall_9(sql)
        tdSql.checkData(0, 10, '1970-01-01 08:00:00.010')

    def run(self):

        testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf 5-taos-tools/%s.sql" % testcaseFilename)
        os.system("rm %s/db*" % self.tmpdir)
        os.system("rm dump_result.txt*")
        tdSql.prepare()

        print("==============step1")
        print("prepare data")

        tdSql.execute("create database db2")
        tdSql.execute("use db2")

        print(
            "==============new version [escape character] for stable==============")
        print("==============step1,#create db.stable,db.table; insert db.table; show db.table; select db.table; drop db.table;")
        print("prepare data")

        self.stb1 = "stable_1~!@#$%^&*()-_+=[]{}':,<.>/?stST13579"
        self.tb1 = "table_1~!@#$%^&*()-_+=[]{}':,<.>/?stST13579"

        self.col_base = "123~!@#$%^&*()-_+=[]{}':,<.>/?stST13579"

        self.col_int = "stable_col_int%s" % self.col_base
        print(self.col_int)
        self.col_bigint = "stable_col_bigint%s" % self.col_base
        self.col_smallint = "stable_col_smallint%s" % self.col_base
        self.col_tinyint = "stable_col_tinyint%s" % self.col_base
        self.col_bool = "stable_col_bool%s" % self.col_base
        self.col_binary = "stable_col_binary%s" % self.col_base
        self.col_nchar = "stable_col_nchar%s" % self.col_base
        self.col_float = "stable_col_float%s" % self.col_base
        self.col_double = "stable_col_double%s" % self.col_base
        self.col_ts = "stable_col_ts%s" % self.col_base

        self.tag_base = "abc~!@#$%^&*()-_+=[]{}':,<.>/?stST13579"
        self.tag_int = "stable_tag_int%s" % self.tag_base
        self.tag_bigint = "stable_tag_bigint%s" % self.tag_base
        self.tag_smallint = "stable_tag_smallint%s" % self.tag_base
        self.tag_tinyint = "stable_tag_tinyint%s" % self.tag_base
        self.tag_bool = "stable_tag_bool%s" % self.tag_base
        self.tag_binary = "stable_tag_binary%s" % self.tag_base
        self.tag_nchar = "stable_tag_nchar%s" % self.tag_base
        self.tag_float = "stable_tag_float%s" % self.tag_base
        self.tag_double = "stable_tag_double%s" % self.tag_base
        self.tag_ts = "stable_tag_ts%s" % self.tag_base

        tdSql.execute('''create stable db.`%s` (ts timestamp, `%s` int , `%s` bigint , `%s` smallint , `%s` tinyint, `%s` bool ,
                        `%s` binary(20) , `%s` nchar(20) ,`%s` float , `%s` double , `%s` timestamp)
                        tags(loc nchar(20),  `%s` int , `%s` bigint , `%s` smallint , `%s` tinyint, `%s` bool ,
                        `%s` binary(20) , `%s` nchar(20) ,`%s` float , `%s` double , `%s` timestamp);'''
                      % (self.stb1, self.col_int, self.col_bigint, self.col_smallint, self.col_tinyint, self.col_bool,
                         self.col_binary, self.col_nchar, self.col_float, self.col_double, self.col_ts,
                         self.tag_int, self.tag_bigint, self.tag_smallint, self.tag_tinyint, self.tag_bool,
                         self.tag_binary, self.tag_nchar, self.tag_float, self.tag_double, self.tag_ts))
        tdSql.query("describe db.`%s` ; " % self.stb1)
        tdSql.checkRows(22)

        tdSql.query("select count(*) from db.`%s` ; " % self.stb1)
        tdSql.checkRows(0)

        tdSql.query("show create stable db.`%s` ; " % self.stb1)
        tdSql.checkData(0, 0, self.stb1)
        tdSql.checkData(0, 1, "CREATE TABLE `%s` (`ts` TIMESTAMP,`%s` INT,`%s` BIGINT,`%s` SMALLINT,`%s` TINYINT,`%s` BOOL,`%s` BINARY(20),`%s` NCHAR(20),`%s` FLOAT,`%s` DOUBLE,`%s` TIMESTAMP)\
 TAGS (`loc` NCHAR(20),`%s` INT,`%s` BIGINT,`%s` SMALLINT,`%s` TINYINT,`%s` BOOL,`%s` BINARY(20),`%s` NCHAR(20),`%s` FLOAT,`%s` DOUBLE,`%s` TIMESTAMP)"
                        % (self.stb1, self.col_int, self.col_bigint, self.col_smallint, self.col_tinyint, self.col_bool,
                           self.col_binary, self.col_nchar, self.col_float, self.col_double, self.col_ts,
                           self.tag_int, self.tag_bigint, self.tag_smallint, self.tag_tinyint, self.tag_bool,
                           self.tag_binary, self.tag_nchar, self.tag_float, self.tag_double, self.tag_ts))

        tdSql.execute(
            "create table db.`table!1` using db.`%s` tags('table_1' , '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0')" %
            self.stb1)
        tdSql.query("describe db.`table!1` ; ")
        tdSql.checkRows(22)

        time.sleep(10)
        tdSql.query("show create table db.`table!1` ; ")
        tdSql.checkData(0, 0, "table!1")
        tdSql.checkData(
            0,
            1,
            "CREATE TABLE `table!1` USING `%s` TAGS (\"table_1\",0,0,0,0,false,\"0\",\"0\",0.000000,0.000000,\"0\")" %
            self.stb1)

        tdSql.execute(
            "insert into db.`table!1` values(now, 1 , 2, 3, 4, 5, 6 ,7 ,8 ,9 ,10)")
        sql = " select * from  db.`table!1`; "
        datacheck = self.table1_checkall(sql)
        tdSql.checkRows(1)
        sql = '''select ts ,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s` from  db.`table!1`; '''\
            % (self.col_int, self.col_bigint, self.col_smallint, self.col_tinyint, self.col_bool, self.col_binary, self.col_nchar, self.col_float, self.col_double, self.col_ts)
        datacheck = self.table1_checkall(sql)
        tdSql.checkRows(1)

        time.sleep(1)
        tdSql.execute('''insert into db.`table!1`(ts ,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`) values(now, 1 , 2, 3, 4, 5, 6 ,7 ,8 ,9 ,10)'''
                      % (self.col_int, self.col_bigint, self.col_smallint, self.col_tinyint, self.col_bool, self.col_binary, self.col_nchar, self.col_float, self.col_double, self.col_ts))
        sql = " select * from  db.`table!1`; "
        datacheck = self.table1_checkall(sql)
        tdSql.checkRows(2)

        tdSql.query("select count(*) from  db.`table!1`; ")
        tdSql.checkData(0, 0, 2)
        tdSql.query("select count(*) from db.`%s` ; " % self.stb1)
        tdSql.checkRows(1)

        tdSql.execute(
            "create table db.`%s` using db.`%s` TAGS (\"table_2\",2,2,2,2,true,\"2\",\"2\",2.000000,2.000000,\"2\")" %
            (self.tb1, self.stb1))
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(22)
        tdSql.query("show create table db.`%s` ; " % self.tb1)
        tdSql.checkData(0, 0, self.tb1)
        tdSql.checkData(
            0,
            1,
            "CREATE TABLE `%s` USING `%s` TAGS (\"table_2\",2,2,2,2,true,\"2\",\"2\",2.000000,2.000000,\"2\")" %
            (self.tb1,
             self.stb1))

        tdSql.execute(
            "insert into db.`%s`  values(now, 1 , 2, 3, 4, 5, 6 ,7 ,8 ,9 ,10)" %
            self.tb1)
        sql = "select * from  db.`%s` ; " % self.tb1
        datacheck = self.table1_checkall(sql)
        tdSql.checkRows(1)
        sql = '''select ts ,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s` from  db.`%s` ; '''\
            % (self.col_int, self.col_bigint, self.col_smallint, self.col_tinyint, self.col_bool, self.col_binary, self.col_nchar, self.col_float, self.col_double, self.col_ts,
               self.tag_int, self.tag_bigint, self.tag_smallint, self.tag_tinyint, self.tag_bool, self.tag_binary, self.tag_nchar, self.tag_float, self.tag_double, self.tag_ts, self.tb1)
        datacheck = self.table1_checkall(sql)
        tdSql.checkRows(1)

        time.sleep(1)
        tdSql.execute('''insert into db.`%s`(ts ,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`) values(now, 1 , 2, 3, 4, 5, 6 ,7 ,8 ,9 ,10)'''
                      % (self.tb1, self.col_int, self.col_bigint, self.col_smallint, self.col_tinyint, self.col_bool, self.col_binary, self.col_nchar, self.col_float, self.col_double, self.col_ts))
        sql = " select * from  db.`%s` ; " % self.tb1
        datacheck = self.table1_checkall(sql)
        tdSql.checkRows(2)

        sql = " select * from  db.`%s` where `%s`=1 and `%s`=2 and `%s`=3 and `%s`=4 and `%s`='True' and `%s`=6 and `%s`=7 and `%s`=8 and `%s`=9 and `%s`=10; " \
            % (self.tb1, self.col_int, self.col_bigint, self.col_smallint, self.col_tinyint, self.col_bool, self.col_binary, self.col_nchar, self.col_float, self.col_double, self.col_ts)
        datacheck = self.table1_checkall(sql)
        tdSql.checkRows(2)

        tdSql.query("select count(*) from  db.`%s`; " % self.tb1)
        tdSql.checkData(0, 0, 2)
        sql = "select * from  db.`%s` ; " % self.stb1
        datacheck = self.table1_checkall(sql)
        tdSql.checkRows(4)
        tdSql.query("select count(*) from  db.`%s`; " % self.stb1)
        tdSql.checkData(0, 0, 4)

        sql = "select * from (select * from db.`%s`) ; " % self.stb1
        datacheck = self.table1_checkall(sql)
        tdSql.checkRows(4)
        tdSql.query(
            "select count(*) from (select * from db.`%s`) ; " %
            self.stb1)
        tdSql.checkData(0, 0, 4)

        sql = "select * from (select ts ,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s` from db.`%s`) ; " \
            % (self.col_int, self.col_bigint, self.col_smallint, self.col_tinyint, self.col_bool, self.col_binary, self.col_nchar, self.col_float, self.col_double, self.col_ts, self.stb1)
        datacheck = self.table1_checkall(sql)
        tdSql.checkRows(4)

        sql = "select ts ,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s` from (select ts ,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s` from db.`%s`) ; " \
            % (self.col_int, self.col_bigint, self.col_smallint, self.col_tinyint, self.col_bool, self.col_binary, self.col_nchar, self.col_float, self.col_double, self.col_ts,
               self.col_int, self.col_bigint, self.col_smallint, self.col_tinyint, self.col_bool, self.col_binary, self.col_nchar, self.col_float, self.col_double, self.col_ts, self.stb1)
        datacheck = self.table1_checkall(sql)
        tdSql.checkRows(4)

        sql = "select ts ,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s` from (select ts ,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s` from db.`%s`\
                    where `%s`=1 and `%s`=2 and `%s`=3 and `%s`=4 and `%s`='True' and `%s`=6 and `%s`=7 and `%s`=8 and `%s`=9 and `%s`=10 ) ; " \
                    % (self.col_int, self.col_bigint, self.col_smallint, self.col_tinyint, self.col_bool, self.col_binary, self.col_nchar, self.col_float, self.col_double, self.col_ts,
                       self.col_int, self.col_bigint, self.col_smallint, self.col_tinyint, self.col_bool, self.col_binary, self.col_nchar, self.col_float, self.col_double, self.col_ts, self.stb1,
                       self.col_int, self.col_bigint, self.col_smallint, self.col_tinyint, self.col_bool, self.col_binary, self.col_nchar, self.col_float, self.col_double, self.col_ts)
        datacheck = self.table1_checkall(sql)
        tdSql.checkRows(4)

        tdSql.query("select * from information_schema.ins_stables where db_name = 'db' like 'stable_1%' ")
        tdSql.checkRows(1)
        tdSql.query("select * from information_schema.ins_tables where db_name = 'db' like 'table%' ")
        tdSql.checkRows(2)

        self.cr_tb1 = "create_table_1~!@#$%^&*()-_+=[]{}':,<.>/?stST13579"
        tdSql.execute(
            "create table db.`%s` as select avg(`%s`) from db.`%s` where ts > now interval(1m) sliding(30s);" %
            (self.cr_tb1, self.col_bigint, self.stb1))
        tdSql.query("select * from information_schema.ins_tables where db_name = 'db' like 'create_table_%' ")
        tdSql.checkRows(1)

        print(r"==============drop\ add\ change\ modify column or tag")
        print("==============drop==============")
        tdSql.execute(
            "ALTER TABLE db.`%s` DROP TAG `%s`; " %
            (self.stb1, self.tag_ts))
        sql = " select * from  db.`%s`; " % self.stb1
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(21)
        tdSql.execute(
            "ALTER TABLE db.`%s` DROP TAG `%s`; " %
            (self.stb1, self.tag_double))
        sql = " select * from  db.`%s`; " % self.stb1
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(20)
        tdSql.execute(
            "ALTER TABLE db.`%s` DROP TAG `%s`; " %
            (self.stb1, self.tag_float))
        sql = " select * from  db.`%s`; " % self.stb1
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(19)
        tdSql.execute(
            "ALTER TABLE db.`%s` DROP TAG `%s`; " %
            (self.stb1, self.tag_nchar))
        sql = " select * from  db.`%s`; " % self.stb1
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(18)
        tdSql.execute(
            "ALTER TABLE db.`%s` DROP TAG `%s`; " %
            (self.stb1, self.tag_binary))
        sql = " select * from  db.`%s`; " % self.stb1
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(17)
        tdSql.execute(
            "ALTER TABLE db.`%s` DROP TAG `%s`; " %
            (self.stb1, self.tag_bool))
        sql = " select * from  db.`%s`; " % self.stb1
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(16)
        tdSql.execute(
            "ALTER TABLE db.`%s` DROP TAG `%s`; " %
            (self.stb1, self.tag_tinyint))
        sql = " select * from  db.`%s`; " % self.stb1
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(15)
        tdSql.execute(
            "ALTER TABLE db.`%s` DROP TAG `%s`; " %
            (self.stb1, self.tag_smallint))
        sql = " select * from  db.`%s`; " % self.stb1
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(14)
        tdSql.execute(
            "ALTER TABLE db.`%s` DROP TAG `%s`; " %
            (self.stb1, self.tag_bigint))
        sql = " select * from  db.`%s`; " % self.stb1
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(13)
        tdSql.execute(
            "ALTER TABLE db.`%s` DROP TAG `%s`; " %
            (self.stb1, self.tag_int))
        sql = " select * from  db.`%s`; " % self.stb1
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(12)

        tdSql.execute(
            "ALTER TABLE db.`%s` DROP COLUMN `%s`; " %
            (self.stb1, self.col_ts))
        sql = " select * from  db.`%s`; " % self.stb1
        datacheck = self.table1_checkall_9(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(11)
        tdSql.execute(
            "ALTER TABLE db.`%s` DROP COLUMN `%s`; " %
            (self.stb1, self.col_double))
        sql = " select * from  db.`%s`; " % self.stb1
        datacheck = self.table1_checkall_8(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(10)
        tdSql.execute(
            "ALTER TABLE db.`%s` DROP COLUMN `%s`; " %
            (self.stb1, self.col_float))
        sql = " select * from  db.`%s`; " % self.stb1
        datacheck = self.table1_checkall_7(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(9)
        tdSql.execute(
            "ALTER TABLE db.`%s` DROP COLUMN `%s`; " %
            (self.stb1, self.col_nchar))
        sql = " select * from  db.`%s`; " % self.stb1
        datacheck = self.table1_checkall_6(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(8)
        tdSql.execute(
            "ALTER TABLE db.`%s` DROP COLUMN `%s`; " %
            (self.stb1, self.col_binary))
        sql = " select * from  db.`%s`; " % self.stb1
        datacheck = self.table1_checkall_5(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(7)
        tdSql.execute(
            "ALTER TABLE db.`%s` DROP COLUMN `%s`; " %
            (self.stb1, self.col_bool))
        sql = " select * from  db.`%s`; " % self.stb1
        datacheck = self.table1_checkall_4(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(6)
        tdSql.execute(
            "ALTER TABLE db.`%s` DROP COLUMN `%s`; " %
            (self.stb1, self.col_tinyint))
        sql = " select * from  db.`%s`; " % self.stb1
        datacheck = self.table1_checkall_3(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(5)
        tdSql.execute(
            "ALTER TABLE db.`%s` DROP COLUMN `%s`; " %
            (self.stb1, self.col_smallint))
        sql = " select * from  db.`%s`; " % self.stb1
        datacheck = self.table1_checkall_2(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(4)
        tdSql.execute(
            "ALTER TABLE db.`%s` DROP COLUMN `%s`; " %
            (self.stb1, self.col_bigint))
        sql = " select * from  db.`%s`; " % self.stb1
        datacheck = self.table1_checkall_1(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(3)
        tdSql.error(
            "ALTER TABLE db.`%s` DROP COLUMN `%s`; " %
            (self.stb1, self.col_int))
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(3)

        print("==============add==============")
        tdSql.execute(
            "ALTER TABLE db.`%s` ADD COLUMN `%s` bigint; " %
            (self.stb1, self.col_bigint))
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(4)
        tdSql.execute(
            "ALTER TABLE db.`%s` ADD COLUMN `%s` smallint; " %
            (self.stb1, self.col_smallint))
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(5)
        tdSql.execute(
            "ALTER TABLE db.`%s` ADD COLUMN `%s` tinyint; " %
            (self.stb1, self.col_tinyint))
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(6)
        tdSql.execute(
            "ALTER TABLE db.`%s` ADD COLUMN `%s` bool; " %
            (self.stb1, self.col_bool))
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(7)
        tdSql.execute(
            "ALTER TABLE db.`%s` ADD COLUMN `%s` binary(20); " %
            (self.stb1, self.col_binary))
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(8)

        tdSql.execute(
            "insert into db.`%s`  values(now, 1 , 2, 3, 4, 5, 6)" %
            self.tb1)
        sql = "select * from  db.`%s` order by ts desc; " % self.tb1
        datacheck = self.table1_checkall_5(sql)

        tdSql.execute(
            "ALTER TABLE db.`%s` ADD COLUMN `%s` nchar(20); " %
            (self.stb1, self.col_nchar))
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(9)
        tdSql.execute(
            "ALTER TABLE db.`%s` ADD COLUMN `%s` float; " %
            (self.stb1, self.col_float))
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(10)
        tdSql.execute(
            "ALTER TABLE db.`%s` ADD COLUMN `%s` double; " %
            (self.stb1, self.col_double))
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(11)
        tdSql.execute(
            "ALTER TABLE db.`%s` ADD COLUMN `%s` timestamp; " %
            (self.stb1, self.col_ts))
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(12)

        tdSql.execute(
            "insert into db.`%s`  values(now, 1 , 2, 3, 4, 5, 6 ,7 ,8 ,9 ,10)" %
            self.tb1)
        sql = "select * from  db.`%s` order by ts desc; " % self.tb1
        datacheck = self.table1_checkall(sql)

        tdSql.execute(
            "ALTER TABLE db.`%s` ADD TAG `%s` int; " %
            (self.stb1, self.tag_int))
        sql = " select * from  db.`%s` order by ts desc; " % self.stb1
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(13)
        tdSql.execute(
            "ALTER TABLE db.`%s` ADD TAG `%s` bigint; " %
            (self.stb1, self.tag_bigint))
        sql = " select * from  db.`%s` order by ts desc; " % self.stb1
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(14)
        tdSql.execute(
            "ALTER TABLE db.`%s` ADD TAG `%s` smallint; " %
            (self.stb1, self.tag_smallint))
        sql = " select * from  db.`%s` order by ts desc; " % self.stb1
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(15)
        tdSql.execute(
            "ALTER TABLE db.`%s` ADD TAG `%s` tinyint; " %
            (self.stb1, self.tag_tinyint))
        sql = " select * from  db.`%s` order by ts desc; " % self.stb1
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(16)
        tdSql.execute(
            "ALTER TABLE db.`%s` ADD TAG `%s` bool; " %
            (self.stb1, self.tag_bool))
        sql = " select * from  db.`%s` order by ts desc; " % self.stb1
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(17)
        tdSql.execute(
            "ALTER TABLE db.`%s` ADD TAG `%s` binary(20); " %
            (self.stb1, self.tag_binary))
        sql = " select * from  db.`%s` order by ts desc; " % self.stb1
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(18)
        tdSql.execute(
            "ALTER TABLE db.`%s` ADD TAG `%s` nchar(20); " %
            (self.stb1, self.tag_nchar))
        sql = " select * from  db.`%s` order by ts desc; " % self.stb1
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(19)
        tdSql.execute(
            "ALTER TABLE db.`%s` ADD TAG `%s` float; " %
            (self.stb1, self.tag_float))
        sql = " select * from  db.`%s` order by ts desc; " % self.stb1
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(20)
        tdSql.execute(
            "ALTER TABLE db.`%s` ADD TAG `%s` double; " %
            (self.stb1, self.tag_double))
        sql = " select * from  db.`%s` order by ts desc; " % self.stb1
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(21)
        tdSql.execute(
            "ALTER TABLE db.`%s` ADD TAG `%s` timestamp; " %
            (self.stb1, self.tag_ts))
        sql = " select * from  db.`%s` order by ts desc; " % self.stb1
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(22)

        print("==============change==============")
        self.tag_base_change = "abcdas"
        self.tag_int_change = "stable_tag_int%s" % self.tag_base_change
        self.tag_bigint_change = "stable_tag_bigint%s" % self.tag_base_change
        self.tag_smallint_change = "stable_tag_smallint%s" % self.tag_base_change
        self.tag_tinyint_change = "stable_tag_tinyint%s" % self.tag_base_change
        self.tag_bool_change = "stable_tag_bool%s" % self.tag_base_change
        self.tag_binary_change = "stable_tag_binary%s" % self.tag_base_change
        self.tag_nchar_change = "stable_tag_nchar%s" % self.tag_base_change
        self.tag_float_change = "stable_tag_float%s" % self.tag_base_change
        self.tag_double_change = "stable_tag_double%s" % self.tag_base_change
        self.tag_ts_change = "stable_tag_ts%s" % self.tag_base_change

        tdSql.execute(
            "ALTER TABLE db.`%s` CHANGE TAG `%s` `%s`; " %
            (self.stb1, self.tag_int, self.tag_int_change))
        sql = " select * from  db.`%s` order by ts desc; " % self.stb1
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(22)
        tdSql.execute(
            "ALTER TABLE db.`%s` CHANGE TAG `%s` `%s`; " %
            (self.stb1, self.tag_bigint, self.tag_bigint_change))
        sql = " select * from  db.`%s` order by ts desc; " % self.stb1
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(22)
        tdSql.execute(
            "ALTER TABLE db.`%s` CHANGE TAG `%s` `%s`; " %
            (self.stb1, self.tag_smallint, self.tag_smallint_change))
        sql = " select * from  db.`%s` order by ts desc; " % self.stb1
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(22)
        tdSql.execute(
            "ALTER TABLE db.`%s` CHANGE TAG `%s` `%s`; " %
            (self.stb1, self.tag_tinyint, self.tag_tinyint_change))
        sql = " select * from  db.`%s` order by ts desc; " % self.stb1
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(22)
        tdSql.execute(
            "ALTER TABLE db.`%s` CHANGE TAG `%s` `%s`; " %
            (self.stb1, self.tag_bool, self.tag_bool_change))
        sql = " select * from  db.`%s` order by ts desc; " % self.stb1
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(22)
        tdSql.execute(
            "ALTER TABLE db.`%s` CHANGE TAG `%s` `%s`; " %
            (self.stb1, self.tag_binary, self.tag_binary_change))
        sql = " select * from  db.`%s` order by ts desc; " % self.stb1
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(22)
        tdSql.execute(
            "ALTER TABLE db.`%s` CHANGE TAG `%s` `%s`; " %
            (self.stb1, self.tag_nchar, self.tag_nchar_change))
        sql = " select * from  db.`%s` order by ts desc; " % self.stb1
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(22)
        tdSql.execute(
            "ALTER TABLE db.`%s` CHANGE TAG `%s` `%s`; " %
            (self.stb1, self.tag_float, self.tag_float_change))
        sql = " select * from  db.`%s` order by ts desc; " % self.stb1
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(22)
        tdSql.execute(
            "ALTER TABLE db.`%s` CHANGE TAG `%s` `%s`; " %
            (self.stb1, self.tag_double, self.tag_double_change))
        sql = " select * from  db.`%s` order by ts desc; " % self.stb1
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(22)
        tdSql.execute(
            "ALTER TABLE db.`%s` CHANGE TAG `%s` `%s`; " %
            (self.stb1, self.tag_ts, self.tag_ts_change))
        sql = " select * from  db.`%s` order by ts desc; " % self.stb1
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(22)

        print("==============modify==============")
        # TD-10810
        tdSql.execute(
            "ALTER STABLE db.`%s` MODIFY TAG `%s` binary(30); ; " %
            (self.stb1, self.tag_binary_change))
        sql = " select * from  db.`%s` order by ts desc; " % self.stb1
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(22)
        tdSql.execute(
            "ALTER STABLE db.`%s` MODIFY TAG `%s` nchar(30); ; " %
            (self.stb1, self.tag_nchar_change))
        sql = " select * from  db.`%s` order by ts desc; " % self.stb1
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(22)

        tdSql.execute(
            "ALTER STABLE db.`%s` MODIFY COLUMN `%s` binary(30); ; " %
            (self.stb1, self.col_binary))
        sql = " select * from  db.`%s` order by ts desc; " % self.stb1
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(22)
        tdSql.execute(
            "ALTER STABLE db.`%s` MODIFY COLUMN `%s` nchar(30); ; " %
            (self.stb1, self.col_nchar))
        sql = " select * from  db.`%s` order by ts desc; " % self.stb1
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe db.`%s` ; " % self.tb1)
        tdSql.checkRows(22)

        print(r"==============drop table\stable")
        try:
            tdSql.execute("drop table db.`%s` " % self.tb1)
        except Exception as e:
            tdLog.exit(e)

        tdSql.error("select * from db.`%s`" % self.tb1)
        tdSql.query("select * from information_schema.ins_stables where db_name = 'db' like 'stable_1%' ")
        tdSql.checkRows(1)

        try:
            tdSql.execute("drop table db.`%s` " % self.stb1)
        except Exception as e:
            tdLog.exit(e)

        tdSql.error("select * from db.`%s`" % self.tb1)
        tdSql.error("select * from db.`%s`" % self.stb1)

        print("==============step2,#create stable,table; insert table; show table; select table; drop table")

        self.stb2 = "stable_2~!@#$%^&*()-_+=[]{}';:,<.>/?stST24680~!@#$%^&*()-_+=[]{}"
        self.tb2 = "table_2~!@#$%^&*()-_+=[]{}';:,<.>/?stST24680~!@#$%^&*()-_+=[]{}"

        tdSql.execute(
            "create stable `%s` (ts timestamp, i int) tags(j int);" %
            self.stb2)
        tdSql.query("describe `%s` ; " % self.stb2)
        tdSql.checkRows(3)

        tdSql.query("select count(*) from `%s` ; " % self.stb2)
        tdSql.checkRows(0)

        tdSql.query("show create stable `%s` ; " % self.stb2)
        tdSql.checkData(0, 0, self.stb2)
        tdSql.checkData(
            0,
            1,
            "CREATE TABLE `%s` (`ts` TIMESTAMP,`i` INT) TAGS (`j` INT)" %
            self.stb2)

        tdSql.execute("create table `table!2` using `%s` tags(1)" % self.stb2)
        tdSql.query("describe `table!2` ; ")
        tdSql.checkRows(3)

        time.sleep(10)

        tdSql.query("show create table `table!2` ; ")
        tdSql.checkData(0, 0, "table!2")
        tdSql.checkData(
            0,
            1,
            "CREATE TABLE `table!2` USING `%s` TAGS (1)" %
            self.stb2)
        tdSql.execute("insert into `table!2` values(now, 1)")
        tdSql.query("select * from  `table!2`; ")
        tdSql.checkRows(1)
        tdSql.query("select count(*) from  `table!2`; ")
        tdSql.checkData(0, 0, 1)
        tdSql.query("select count(*) from `%s` ; " % self.stb2)
        tdSql.checkRows(1)

        tdSql.execute(
            "create table `%s` using `%s` tags(1)" %
            (self.tb2, self.stb2))
        tdSql.query("describe `%s` ; " % self.tb2)
        tdSql.checkRows(3)
        tdSql.query("show create table `%s` ; " % self.tb2)
        tdSql.checkData(0, 0, self.tb2)
        tdSql.checkData(
            0, 1, "CREATE TABLE `%s` USING `%s` TAGS (1)" %
            (self.tb2, self.stb2))
        tdSql.execute("insert into `%s`  values(now, 1)" % self.tb2)
        tdSql.query("select * from  `%s` ; " % self.tb2)
        tdSql.checkRows(1)
        tdSql.query("select count(*) from  `%s`; " % self.tb2)
        tdSql.checkData(0, 0, 1)
        tdSql.query("select * from  `%s` ; " % self.stb2)
        tdSql.checkRows(2)
        tdSql.query("select count(*) from  `%s`; " % self.stb2)
        tdSql.checkData(0, 0, 2)

        tdSql.query("select * from (select * from `%s`) ; " % self.stb2)
        tdSql.checkRows(2)
        tdSql.query("select count(*) from (select * from `%s` ); " % self.stb2)
        tdSql.checkData(0, 0, 2)

        tdSql.query("show stables like 'stable_2%' ")
        tdSql.checkRows(1)
        tdSql.query("show tables like 'table%' ")
        tdSql.checkRows(2)

        # TD-10536
        self.cr_tb2 = "create_table_2~!@#$%^&*()-_+=[]{}';:,<.>/?stST24680~!@#$%^&*()-_+=[]{}"
        tdSql.execute(
            "create table `%s` as select * from `%s` ;" %
            (self.cr_tb2, self.stb2))
        tdSql.query("select * from information_schema.ins_tables where db_name = 'db' like 'create_table_%' ")
        tdSql.checkRows(1)

        print("==============step3,#create regular_table; insert regular_table; show regular_table; select regular_table; drop regular_table")
        self.regular_table = "regular_table~!@#$%^&*()-_+=[]{}';:,<.>/?stST24680~!@#$%^&*()-_+=[]{}"

        self.regular_col_base = "123@#$%^&*()-_+=[]{};:,<.>/?~!$%^"

        self.col_int = "regular_table_col_int%s" % self.regular_col_base
        print(self.col_int)
        self.col_bigint = "regular_table_col_bigint%s" % self.regular_col_base
        self.col_smallint = "regular_table_col_smallint%s" % self.regular_col_base
        self.col_tinyint = "regular_table_col_tinyint%s" % self.regular_col_base
        self.col_bool = "regular_table_col_bool%s" % self.regular_col_base
        self.col_binary = "regular_table_col_binary%s" % self.regular_col_base
        self.col_nchar = "regular_table_col_nchar%s" % self.regular_col_base
        self.col_float = "regular_table_col_float%s" % self.regular_col_base
        self.col_double = "regular_table_col_double%s" % self.regular_col_base
        self.col_ts = "regular_table_col_ts%s" % self.regular_col_base

        tdSql.execute("create table `%s` (ts timestamp,`%s` int , `%s` bigint , `%s` smallint , `%s` tinyint, `%s` bool , \
                    `%s` binary(20) , `%s` nchar(20) ,`%s` float , `%s` double , `%s` timestamp) ;"
                      % (self.regular_table, self.col_int, self.col_bigint, self.col_smallint, self.col_tinyint, self.col_bool,
                         self.col_binary, self.col_nchar, self.col_float, self.col_double, self.col_ts))
        tdSql.query("describe `%s` ; " % self.regular_table)
        tdSql.checkRows(11)

        tdSql.query("select count(*) from `%s` ; " % self.regular_table)
        tdSql.checkRows(1)

        tdSql.query("show create table `%s` ; " % self.regular_table)
        tdSql.checkData(0, 0, self.regular_table)
        tdSql.checkData(0, 1, "CREATE TABLE `%s` (`ts` TIMESTAMP,`%s` INT,`%s` BIGINT,`%s` SMALLINT,`%s` TINYINT,`%s` BOOL,`%s` BINARY(20),`%s` NCHAR(20),`%s` FLOAT,`%s` DOUBLE,`%s` TIMESTAMP)"
                        % (self.regular_table, self.col_int, self.col_bigint, self.col_smallint, self.col_tinyint, self.col_bool,
                           self.col_binary, self.col_nchar, self.col_float, self.col_double, self.col_ts))

        tdSql.execute(
            "insert into `%s`  values(now, 1 , 2, 3, 4, 5, 6 ,7 ,8 ,9 ,10)" %
            self.regular_table)
        sql = "select * from  `%s` ; " % self.regular_table
        datacheck = self.table1_checkall(sql)
        tdSql.checkRows(1)
        sql = '''select ts ,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s` from  db2.`%s`; '''\
            % (self.col_int, self.col_bigint, self.col_smallint, self.col_tinyint, self.col_bool, self.col_binary, self.col_nchar, self.col_float, self.col_double, self.col_ts, self.regular_table)
        datacheck = self.table1_checkall(sql)
        tdSql.checkRows(1)

        time.sleep(1)
        tdSql.execute('''insert into db2.`%s` (ts ,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`) values(now, 1 , 2, 3, 4, 5, 6 ,7 ,8 ,9 ,10)'''
                      % (self.regular_table, self.col_int, self.col_bigint, self.col_smallint, self.col_tinyint, self.col_bool, self.col_binary, self.col_nchar, self.col_float, self.col_double, self.col_ts))
        sql = " select * from  db2.`%s`; " % self.regular_table
        datacheck = self.table1_checkall(sql)
        tdSql.checkRows(2)

        sql = " select * from  db2.`%s` where `%s`=1 and `%s`=2 and `%s`=3 and `%s`=4 and `%s`='True' and `%s`=6 and `%s`=7 and `%s`=8 and `%s`=9 and `%s`=10; " \
            % (self.regular_table, self.col_int, self.col_bigint, self.col_smallint, self.col_tinyint, self.col_bool, self.col_binary, self.col_nchar, self.col_float, self.col_double, self.col_ts)
        datacheck = self.table1_checkall(sql)
        tdSql.checkRows(2)

        tdSql.query("select count(*) from  `%s`; " % self.regular_table)
        tdSql.checkData(0, 0, 2)
        tdSql.query("select count(*) from `%s` ; " % self.regular_table)
        tdSql.checkRows(1)

        sql = "select * from (select * from `%s`) ; " % self.regular_table
        datacheck = self.table1_checkall(sql)
        tdSql.checkRows(2)

        sql = "select ts ,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s` from (select ts ,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s` from db2.`%s`\
                    where `%s`=1 and `%s`=2 and `%s`=3 and `%s`=4 and `%s`='True' and `%s`=6 and `%s`=7 and `%s`=8 and `%s`=9 and `%s`=10 ) ; " \
                    % (self.col_int, self.col_bigint, self.col_smallint, self.col_tinyint, self.col_bool, self.col_binary, self.col_nchar, self.col_float, self.col_double, self.col_ts,
                       self.col_int, self.col_bigint, self.col_smallint, self.col_tinyint, self.col_bool, self.col_binary, self.col_nchar, self.col_float, self.col_double, self.col_ts, self.regular_table,
                       self.col_int, self.col_bigint, self.col_smallint, self.col_tinyint, self.col_bool, self.col_binary, self.col_nchar, self.col_float, self.col_double, self.col_ts)
        datacheck = self.table1_checkall(sql)
        tdSql.checkRows(2)

        tdSql.query(
            "select count(*) from (select * from `%s` ); " %
            self.regular_table)
        tdSql.checkData(0, 0, 2)

        tdSql.query("show tables like 'regular_table%' ")
        tdSql.checkRows(1)

        self.crr_tb = "create_r_table~!@#$%^&*()-_+=[]{}';:,<.>/?stST24680~!@#$%^&*()-_+=[]{}"
        tdSql.execute(
            "create table `%s` as select * from `%s` ;" %
            (self.crr_tb, self.regular_table))
        tdSql.query("show db2.tables like 'create_r_table%' ")
        tdSql.checkRows(1)

        print(r"==============drop\ add\ change\ modify column ")
        print("==============drop==============")
        tdSql.execute(
            "ALTER TABLE db2.`%s` DROP COLUMN `%s`; " %
            (self.regular_table, self.col_ts))
        sql = " select * from  db2.`%s`; " % self.regular_table
        datacheck = self.table1_checkall_9(sql)
        tdSql.query("describe db2.`%s` ; " % self.regular_table)
        tdSql.checkRows(10)
        tdSql.execute(
            "ALTER TABLE `%s` DROP COLUMN `%s`; " %
            (self.regular_table, self.col_double))
        sql = " select * from  `%s`; " % self.regular_table
        datacheck = self.table1_checkall_8(sql)
        tdSql.query("describe `%s` ; " % self.regular_table)
        tdSql.checkRows(9)
        tdSql.execute(
            "ALTER TABLE db2.`%s` DROP COLUMN `%s`; " %
            (self.regular_table, self.col_float))
        sql = " select * from  db2.`%s`; " % self.regular_table
        datacheck = self.table1_checkall_7(sql)
        tdSql.query("describe db2.`%s` ; " % self.regular_table)
        tdSql.checkRows(8)
        tdSql.execute(
            "ALTER TABLE `%s` DROP COLUMN `%s`; " %
            (self.regular_table, self.col_nchar))
        sql = " select * from  `%s`; " % self.regular_table
        datacheck = self.table1_checkall_6(sql)
        tdSql.query("describe `%s` ; " % self.regular_table)
        tdSql.checkRows(7)
        tdSql.execute(
            "ALTER TABLE db2.`%s` DROP COLUMN `%s`; " %
            (self.regular_table, self.col_binary))
        sql = " select * from  db2.`%s`; " % self.regular_table
        datacheck = self.table1_checkall_5(sql)
        tdSql.query("describe db2.`%s` ; " % self.regular_table)
        tdSql.checkRows(6)
        tdSql.execute(
            "ALTER TABLE `%s` DROP COLUMN `%s`; " %
            (self.regular_table, self.col_bool))
        sql = " select * from  `%s`; " % self.regular_table
        datacheck = self.table1_checkall_4(sql)
        tdSql.query("describe `%s` ; " % self.regular_table)
        tdSql.checkRows(5)
        tdSql.execute(
            "ALTER TABLE db2.`%s` DROP COLUMN `%s`; " %
            (self.regular_table, self.col_tinyint))
        sql = " select * from  db2.`%s`; " % self.regular_table
        datacheck = self.table1_checkall_3(sql)
        tdSql.query("describe db2.`%s` ; " % self.regular_table)
        tdSql.checkRows(4)
        tdSql.execute("ALTER TABLE `%s` DROP COLUMN `%s`; " %
                      (self.regular_table, self.col_smallint))
        sql = " select * from  `%s`; " % self.regular_table
        datacheck = self.table1_checkall_2(sql)
        tdSql.query("describe `%s` ; " % self.regular_table)
        tdSql.checkRows(3)
        tdSql.execute(
            "ALTER TABLE db2.`%s` DROP COLUMN `%s`; " %
            (self.regular_table, self.col_bigint))
        sql = " select * from  db2.`%s`; " % self.regular_table
        datacheck = self.table1_checkall_1(sql)
        tdSql.query("describe db2.`%s` ; " % self.regular_table)
        tdSql.checkRows(2)
        tdSql.error(
            "ALTER TABLE db2.`%s` DROP COLUMN `%s`; " %
            (self.regular_table, self.col_int))
        tdSql.query("describe `%s` ; " % self.regular_table)
        tdSql.checkRows(2)

        print("==============add==============")
        tdSql.execute(
            "ALTER TABLE db2.`%s` ADD COLUMN `%s` bigint; " %
            (self.regular_table, self.col_bigint))
        tdSql.query("describe db2.`%s` ; " % self.regular_table)
        tdSql.checkRows(3)
        tdSql.execute(
            "ALTER TABLE db2.`%s` ADD COLUMN `%s` smallint; " %
            (self.regular_table, self.col_smallint))
        tdSql.query("describe db2.`%s` ; " % self.regular_table)
        tdSql.checkRows(4)
        tdSql.execute(
            "ALTER TABLE db2.`%s` ADD COLUMN `%s` tinyint; " %
            (self.regular_table, self.col_tinyint))
        tdSql.query("describe db2.`%s` ; " % self.regular_table)
        tdSql.checkRows(5)
        tdSql.execute(
            "ALTER TABLE db2.`%s` ADD COLUMN `%s` bool; " %
            (self.regular_table, self.col_bool))
        tdSql.query("describe db2.`%s` ; " % self.regular_table)
        tdSql.checkRows(6)
        tdSql.execute(
            "ALTER TABLE db2.`%s` ADD COLUMN `%s` binary(20); " %
            (self.regular_table, self.col_binary))
        tdSql.query("describe db2.`%s` ; " % self.regular_table)
        tdSql.checkRows(7)

        tdSql.execute(
            "insert into db2.`%s`  values(now, 1 , 2, 3, 4, 5, 6)" %
            self.regular_table)
        sql = "select * from  db2.`%s` order by ts desc; " % self.regular_table
        datacheck = self.table1_checkall_5(sql)

        tdSql.execute(
            "ALTER TABLE db2.`%s` ADD COLUMN `%s` nchar(20); " %
            (self.regular_table, self.col_nchar))
        tdSql.query("describe db2.`%s` ; " % self.regular_table)
        tdSql.checkRows(8)
        tdSql.execute(
            "ALTER TABLE db2.`%s` ADD COLUMN `%s` float; " %
            (self.regular_table, self.col_float))
        tdSql.query("describe db2.`%s` ; " % self.regular_table)
        tdSql.checkRows(9)
        tdSql.execute(
            "ALTER TABLE db2.`%s` ADD COLUMN `%s` double; " %
            (self.regular_table, self.col_double))
        tdSql.query("describe db2.`%s` ; " % self.regular_table)
        tdSql.checkRows(10)
        tdSql.execute(
            "ALTER TABLE db2.`%s` ADD COLUMN `%s` timestamp; " %
            (self.regular_table, self.col_ts))
        tdSql.query("describe db2.`%s` ; " % self.regular_table)
        tdSql.checkRows(11)

        tdSql.execute(
            "insert into db2.`%s`  values(now, 1 , 2, 3, 4, 5, 6 ,7 ,8 ,9 ,10)" %
            self.regular_table)
        sql = "select * from  db2.`%s` order by ts desc; " % self.regular_table
        datacheck = self.table1_checkall(sql)

        print("==============change, regular not support==============")

        print("==============modify==============")
        # TD-10810
        tdSql.execute(
            "ALTER TABLE db2.`%s` MODIFY COLUMN `%s` binary(30); ; " %
            (self.regular_table, self.col_binary))
        sql = " select * from  db2.`%s` order by ts desc; " % self.regular_table
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe db2.`%s` ; " % self.regular_table)
        tdSql.checkRows(11)
        tdSql.execute(
            "ALTER TABLE `%s` MODIFY COLUMN `%s` nchar(30); ; " %
            (self.regular_table, self.col_nchar))
        sql = " select * from  `%s` order by ts desc; " % self.regular_table
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe `%s` ; " % self.regular_table)
        tdSql.checkRows(11)

        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosdump not found!")
        else:
            tdLog.info("taosdump found in %s" % buildPath)
        binPath = buildPath + "/build/bin/"

        if not os.path.exists(self.tmpdir):
            os.makedirs(self.tmpdir)
        else:
            print("directory exists")
            os.system("rm -rf %s" % self.tmpdir)
            os.makedirs(self.tmpdir)

        print("==============step4,#taosdump out ; drop db ; taosdumo in")
        assert os.system(
            "%staosdump -D db2 -o %s" %
            (binPath, self.tmpdir)) == 0

        tdSql.execute('''drop database if exists db2 ;''')

        assert os.system("%staosdump -i %s -g" % (binPath, self.tmpdir)) == 0

        print("==============step5,#create regular_table; insert regular_table; show regular_table; select regular_table; drop regular_table")
        self.regular_table = "regular_table~!@#$%^&*()-_+=[]{}';:,<.>/?stST24680~!@#$%^&*()-_+=[]{}"

        self.regular_col_base = "123@#$%^&*()-_+=[]{};:,<.>/?~!$%^"

        self.col_int = "regular_table_col_int%s" % self.regular_col_base
        print(self.col_int)
        self.col_bigint = "regular_table_col_bigint%s" % self.regular_col_base
        self.col_smallint = "regular_table_col_smallint%s" % self.regular_col_base
        self.col_tinyint = "regular_table_col_tinyint%s" % self.regular_col_base
        self.col_bool = "regular_table_col_bool%s" % self.regular_col_base
        self.col_binary = "regular_table_col_binary%s" % self.regular_col_base
        self.col_nchar = "regular_table_col_nchar%s" % self.regular_col_base
        self.col_float = "regular_table_col_float%s" % self.regular_col_base
        self.col_double = "regular_table_col_double%s" % self.regular_col_base
        self.col_ts = "regular_table_col_ts%s" % self.regular_col_base

        tdSql.query("describe `%s` ; " % self.regular_table)
        tdSql.checkRows(11)

        tdSql.query("select count(*) from `%s` ; " % self.regular_table)
        tdSql.checkRows(1)

        tdSql.query("show create table `%s` ; " % self.regular_table)
        tdSql.checkData(0, 0, self.regular_table)
        tdSql.checkData(0, 1, "CREATE TABLE `%s` (`ts` TIMESTAMP,`%s` INT,`%s` BIGINT,`%s` SMALLINT,`%s` TINYINT,`%s` BOOL,`%s` BINARY(30),`%s` NCHAR(30),`%s` FLOAT,`%s` DOUBLE,`%s` TIMESTAMP)"
                        % (self.regular_table, self.col_int, self.col_bigint, self.col_smallint, self.col_tinyint, self.col_bool,
                           self.col_binary, self.col_nchar, self.col_float, self.col_double, self.col_ts))

        tdSql.execute(
            "insert into `%s`  values(now, 1 , 2, 3, 4, 5, 6 ,7 ,8 ,9 ,10)" %
            self.regular_table)
        sql = "select * from  `%s`  order by ts desc; " % self.regular_table
        datacheck = self.table1_checkall(sql)
        tdSql.checkRows(5)
        sql = '''select ts ,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s` from  db2.`%s`  order by ts desc; '''\
            % (self.col_int, self.col_bigint, self.col_smallint, self.col_tinyint, self.col_bool, self.col_binary, self.col_nchar, self.col_float, self.col_double, self.col_ts, self.regular_table)
        datacheck = self.table1_checkall(sql)
        tdSql.checkRows(5)

        time.sleep(1)
        tdSql.execute('''insert into db2.`%s` (ts ,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`) values(now, 1 , 2, 3, 4, 5, 6 ,7 ,8 ,9 ,10)'''
                      % (self.regular_table, self.col_int, self.col_bigint, self.col_smallint, self.col_tinyint, self.col_bool, self.col_binary, self.col_nchar, self.col_float, self.col_double, self.col_ts))
        sql = " select * from  db2.`%s`  order by ts desc; " % self.regular_table
        datacheck = self.table1_checkall(sql)
        tdSql.checkRows(6)

        sql = " select * from  db2.`%s` where `%s`=1 and `%s`=2 and `%s`=3 and `%s`=4 and `%s`='True' and `%s`=6 and `%s`=7 and `%s`=8 and `%s`=9 and `%s`=10; " \
            % (self.regular_table, self.col_int, self.col_bigint, self.col_smallint, self.col_tinyint, self.col_bool, self.col_binary, self.col_nchar, self.col_float, self.col_double, self.col_ts)
        datacheck = self.table1_checkall(sql)
# CBD        tdSql.checkRows(3)

        tdSql.query(
            "select count(*) from  `%s` order by ts desc; " %
            self.regular_table)
        tdSql.checkData(0, 0, 6)
        tdSql.query("select count(*) from `%s` ; " % self.regular_table)
        tdSql.checkRows(1)

        sql = "select * from (select * from `%s` order by ts desc) ; " % self.regular_table
        datacheck = self.table1_checkall(sql)
        tdSql.checkRows(6)

        sql = "select ts ,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s` from (select ts ,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s` from db2.`%s`\
                    where `%s`=1 and `%s`=2 and `%s`=3 and `%s`=4 and `%s`='True' and `%s`=6 and `%s`=7 and `%s`=8 and `%s`=9 and `%s`=10 ) ; " \
                    % (self.col_int, self.col_bigint, self.col_smallint, self.col_tinyint, self.col_bool, self.col_binary, self.col_nchar, self.col_float, self.col_double, self.col_ts,
                       self.col_int, self.col_bigint, self.col_smallint, self.col_tinyint, self.col_bool, self.col_binary, self.col_nchar, self.col_float, self.col_double, self.col_ts, self.regular_table,
                       self.col_int, self.col_bigint, self.col_smallint, self.col_tinyint, self.col_bool, self.col_binary, self.col_nchar, self.col_float, self.col_double, self.col_ts)
        datacheck = self.table1_checkall(sql)
# CBD        tdSql.checkRows(3)

        tdSql.query(
            "select count(*) from (select * from `%s` ); " %
            self.regular_table)
        tdSql.checkData(0, 0, 6)

        tdSql.query("show tables like 'regular_table%' ")
        tdSql.checkRows(1)

        tdSql.query("show db2.tables like 'create_r_table%' ")
        tdSql.checkRows(1)

        print(r"==============drop\ add\ change\ modify column ")
        print("==============drop==============")
        tdSql.execute(
            "ALTER TABLE db2.`%s` DROP COLUMN `%s`; " %
            (self.regular_table, self.col_ts))
        sql = " select * from  db2.`%s`  order by ts desc; " % self.regular_table
        datacheck = self.table1_checkall_9(sql)
        tdSql.query("describe db2.`%s` ; " % self.regular_table)
        tdSql.checkRows(10)
        tdSql.execute(
            "ALTER TABLE `%s` DROP COLUMN `%s`; " %
            (self.regular_table, self.col_double))
        sql = " select * from  db2.`%s`  order by ts desc; " % self.regular_table
        datacheck = self.table1_checkall_8(sql)
        tdSql.query("describe `%s` ; " % self.regular_table)
        tdSql.checkRows(9)
        tdSql.execute(
            "ALTER TABLE db2.`%s` DROP COLUMN `%s`; " %
            (self.regular_table, self.col_float))
        sql = " select * from  db2.`%s`  order by ts desc; " % self.regular_table
        datacheck = self.table1_checkall_7(sql)
        tdSql.query("describe db2.`%s` ; " % self.regular_table)
        tdSql.checkRows(8)
        tdSql.execute(
            "ALTER TABLE `%s` DROP COLUMN `%s`; " %
            (self.regular_table, self.col_nchar))
        sql = " select * from  db2.`%s`  order by ts desc; " % self.regular_table
        datacheck = self.table1_checkall_6(sql)
        tdSql.query("describe `%s` ; " % self.regular_table)
        tdSql.checkRows(7)
        tdSql.execute(
            "ALTER TABLE db2.`%s` DROP COLUMN `%s`; " %
            (self.regular_table, self.col_binary))
        sql = " select * from  db2.`%s`  order by ts desc; " % self.regular_table
        datacheck = self.table1_checkall_5(sql)
        tdSql.query("describe db2.`%s` ; " % self.regular_table)
        tdSql.checkRows(6)
        tdSql.execute(
            "ALTER TABLE `%s` DROP COLUMN `%s`; " %
            (self.regular_table, self.col_bool))
        sql = " select * from  db2.`%s`  order by ts desc; " % self.regular_table
        datacheck = self.table1_checkall_4(sql)
        tdSql.query("describe `%s` ; " % self.regular_table)
        tdSql.checkRows(5)
        tdSql.execute(
            "ALTER TABLE db2.`%s` DROP COLUMN `%s`; " %
            (self.regular_table, self.col_tinyint))
        sql = " select * from  db2.`%s`  order by ts desc; " % self.regular_table
        datacheck = self.table1_checkall_3(sql)
        tdSql.query("describe db2.`%s` ; " % self.regular_table)
        tdSql.checkRows(4)
        tdSql.execute("ALTER TABLE `%s` DROP COLUMN `%s`; " %
                      (self.regular_table, self.col_smallint))
        sql = " select * from  db2.`%s`  order by ts desc; " % self.regular_table
        datacheck = self.table1_checkall_2(sql)
        tdSql.query("describe `%s` ; " % self.regular_table)
        tdSql.checkRows(3)
        tdSql.execute(
            "ALTER TABLE db2.`%s` DROP COLUMN `%s`; " %
            (self.regular_table, self.col_bigint))
        sql = " select * from  db2.`%s`  order by ts desc; " % self.regular_table
        datacheck = self.table1_checkall_1(sql)
        tdSql.query("describe db2.`%s` ; " % self.regular_table)
        tdSql.checkRows(2)
        tdSql.error(
            "ALTER TABLE db2.`%s` DROP COLUMN `%s`; " %
            (self.regular_table, self.col_int))
        tdSql.query("describe `%s` ; " % self.regular_table)
        tdSql.checkRows(2)

        print("==============add==============")
        tdSql.execute(
            "ALTER TABLE db2.`%s` ADD COLUMN `%s` bigint; " %
            (self.regular_table, self.col_bigint))
        tdSql.query("describe db2.`%s` ; " % self.regular_table)
        tdSql.checkRows(3)
        tdSql.execute(
            "ALTER TABLE db2.`%s` ADD COLUMN `%s` smallint; " %
            (self.regular_table, self.col_smallint))
        tdSql.query("describe db2.`%s` ; " % self.regular_table)
        tdSql.checkRows(4)
        tdSql.execute(
            "ALTER TABLE db2.`%s` ADD COLUMN `%s` tinyint; " %
            (self.regular_table, self.col_tinyint))
        tdSql.query("describe db2.`%s` ; " % self.regular_table)
        tdSql.checkRows(5)
        tdSql.execute(
            "ALTER TABLE db2.`%s` ADD COLUMN `%s` bool; " %
            (self.regular_table, self.col_bool))
        tdSql.query("describe db2.`%s` ; " % self.regular_table)
        tdSql.checkRows(6)
        tdSql.execute(
            "ALTER TABLE db2.`%s` ADD COLUMN `%s` binary(20); " %
            (self.regular_table, self.col_binary))
        tdSql.query("describe db2.`%s` ; " % self.regular_table)
        tdSql.checkRows(7)

        tdSql.execute(
            "insert into db2.`%s`  values(now, 1 , 2, 3, 4, 5, 6)" %
            self.regular_table)
        sql = "select * from  db2.`%s` order by ts desc; " % self.regular_table
        datacheck = self.table1_checkall_5(sql)

        tdSql.execute(
            "ALTER TABLE db2.`%s` ADD COLUMN `%s` nchar(20); " %
            (self.regular_table, self.col_nchar))
        tdSql.query("describe db2.`%s` ; " % self.regular_table)
        tdSql.checkRows(8)
        tdSql.execute(
            "ALTER TABLE db2.`%s` ADD COLUMN `%s` float; " %
            (self.regular_table, self.col_float))
        tdSql.query("describe db2.`%s` ; " % self.regular_table)
        tdSql.checkRows(9)
        tdSql.execute(
            "ALTER TABLE db2.`%s` ADD COLUMN `%s` double; " %
            (self.regular_table, self.col_double))
        tdSql.query("describe db2.`%s` ; " % self.regular_table)
        tdSql.checkRows(10)
        tdSql.execute(
            "ALTER TABLE db2.`%s` ADD COLUMN `%s` timestamp; " %
            (self.regular_table, self.col_ts))
        tdSql.query("describe db2.`%s` ; " % self.regular_table)
        tdSql.checkRows(11)

        tdSql.execute(
            "insert into db2.`%s`  values(now, 1 , 2, 3, 4, 5, 6 ,7 ,8 ,9 ,10)" %
            self.regular_table)
        sql = "select * from  db2.`%s` order by ts desc; " % self.regular_table
        datacheck = self.table1_checkall(sql)

        print("==============change, regular not support==============")

        print("==============modify==============")
        # TD-10810
        tdSql.execute(
            "ALTER TABLE db2.`%s` MODIFY COLUMN `%s` binary(40); ; " %
            (self.regular_table, self.col_binary))
        sql = " select * from  db2.`%s` order by ts desc; " % self.regular_table
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe db2.`%s` ; " % self.regular_table)
        tdSql.checkRows(11)
        tdSql.execute(
            "ALTER TABLE `%s` MODIFY COLUMN `%s` nchar(40); ; " %
            (self.regular_table, self.col_nchar))
        sql = " select * from  `%s` order by ts desc; " % self.regular_table
        datacheck = self.table1_checkall(sql)
        tdSql.query("describe `%s` ; " % self.regular_table)
        tdSql.checkRows(11)

        os.system("rm %s/db*" % self.tmpdir)
        os.system("rm dump_result.txt*")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
