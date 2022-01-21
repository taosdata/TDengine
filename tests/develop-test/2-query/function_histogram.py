###################################################################
#           Copyright (c) 2021 by TAOS Technologies, Inc.
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
from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def caseDescription(self):
        '''
        case1<Ganlin Zhao>: [TD-11222]<feature>: Histogram function
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/build/bin")]
                    break
        return buildPath

    def run(self):
        print("running {}".format(__file__))
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database if not exists db")
        tdSql.execute('use db')

        #Prepare data
        tdSql.execute("create stable stb (col_timestamp timestamp, col_tinyint tinyint, col_smallint smallint, col_int int, col_bigint bigint, col_float float, col_double double, col_bool bool, col_binary binary(10), col_nchar nchar(10)) \
                       tags(tag_timestamp timestamp, tag_tinyint tinyint, tag_smallint smallint, tag_int int, tag_bigint bigint, tag_float float, tag_double double, tag_bool bool, tag_binary binary(10), tag_nchar nchar(10));")
        tdSql.execute("create table ctb using stb tags (now, 1, 1, 1, 1, 1.0, 1.0, true, 'abc', 'abc');")
        tdSql.execute("create table tb (col_timestamp timestamp, col_tinyint tinyint, col_smallint smallint, col_int int, col_bigint bigint, col_float float, col_double double, col_bool bool, col_binary binary(10), col_nchar nchar(10));")

        tdSql.execute("insert into ctb values (now, -9, -9, -9, -9, -9.5, -9.5, true, 'abc', 'abc');")
        tdSql.execute("insert into ctb values (now + 1s, -1, -1, -1, -1, -1.5, -1.5, true, 'abc', 'abc');")
        tdSql.execute("insert into ctb values (now + 2s, 1, 1, 1, 1, 1.5, 1.5, true, 'abc', 'abc');")
        tdSql.execute("insert into ctb values (now + 3s, 2, 2, 2, 2, 2.5, 2.5, true, 'abc', 'abc');")
        tdSql.execute("insert into ctb values (now + 4s, 3, 3, 3, 3, 3.5, 3.5, true, 'abc', 'abc');")
        tdSql.execute("insert into ctb values (now + 5s, 4, 4, 4, 4, 4.5, 4.5, true, 'abc', 'abc');")
        tdSql.execute("insert into ctb values (now + 6s, 5, 5, 5, 5, 5.5, 5.5, true, 'abc', 'abc');")
        tdSql.execute("insert into ctb values (now + 7s, 6, 6, 6, 6, 6.5, 6.5, true, 'abc', 'abc');")
        tdSql.execute("insert into ctb values (now + 8s, 7, 7, 7, 7, 7.5, 7.5, false, 'abc', 'abc');")
        tdSql.execute("insert into ctb values (now + 9s, 8, 8, 8, 8, 8.5, 8.5, false, 'abc', 'abc');")
        tdSql.execute("insert into ctb values (now + 10s, 9, 9, 9, 9, 9.5, 9.5, false, 'abc', 'abc');")
        tdSql.execute("insert into ctb values (now + 11s, 10, 10, 10, 10, 10.5, 10.5, false, 'abc', 'abc');")
        tdSql.execute("insert into ctb values (now + 12s, 15, 15, 15, 15, 15.5, 15.5, false, 'abc', 'abc');")
        tdSql.execute("insert into ctb values (now + 13s, 20, 20, 20, 20, 20.5, 20.5, false, 'abc', 'abc');")
        tdSql.execute("insert into ctb values (now + 14s, 99, 99, 99, 99, 99.5, 99.5, false, 'abc', 'abc');")
        tdSql.execute("insert into ctb values (now + 15s, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);")

        tdSql.execute("insert into tb values (now, -9, -9, -9, -9, -9.5, -9.5, true, 'abc', 'abc');")
        tdSql.execute("insert into tb values (now + 1s, -1, -1, -1, -1, -1.5, -1.5, true, 'abc', 'abc');")
        tdSql.execute("insert into tb values (now + 2s, 1, 1, 1, 1, 1.5, 1.5, true, 'abc', 'abc');")
        tdSql.execute("insert into tb values (now + 3s, 2, 2, 2, 2, 2.5, 2.5, true, 'abc', 'abc');")
        tdSql.execute("insert into tb values (now + 4s, 3, 3, 3, 3, 3.5, 3.5, true, 'abc', 'abc');")
        tdSql.execute("insert into tb values (now + 5s, 4, 4, 4, 4, 4.5, 4.5, true, 'abc', 'abc');")
        tdSql.execute("insert into tb values (now + 6s, 5, 5, 5, 5, 5.5, 5.5, true, 'abc', 'abc');")
        tdSql.execute("insert into tb values (now + 7s, 6, 6, 6, 6, 6.5, 6.5, true, 'abc', 'abc');")
        tdSql.execute("insert into tb values (now + 8s, 7, 7, 7, 7, 7.5, 7.5, false, 'abc', 'abc');")
        tdSql.execute("insert into tb values (now + 9s, 8, 8, 8, 8, 8.5, 8.5, false, 'abc', 'abc');")
        tdSql.execute("insert into tb values (now + 10s, 9, 9, 9, 9, 9.5, 9.5, false, 'abc', 'abc');")
        tdSql.execute("insert into tb values (now + 11s, 10, 10, 10, 10, 10.5, 10.5, false, 'abc', 'abc');")
        tdSql.execute("insert into tb values (now + 12s, 15, 15, 15, 15, 15.5, 15.5, false, 'abc', 'abc');")
        tdSql.execute("insert into tb values (now + 13s, 20, 20, 20, 20, 20.5, 20.5, false, 'abc', 'abc');")
        tdSql.execute("insert into tb values (now + 14s, 99, 99, 99, 99, 99.5, 99.5, false, 'abc', 'abc');")
        tdSql.execute("insert into tb values (now + 15s, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);")

        #execute query
        print("============== STEP 1: column types  ================== ")
        #Supported column types
        tdSql.query('select histogram(col_tinyint, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.query('select histogram(col_tinyint, "user_input", "[1,3,5,7]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.query('select histogram(col_tinyint, "user_input", "[1,3,5,7]", 0) from tb;')
        tdSql.checkRows(3);

        tdSql.query('select histogram(col_smallint, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.query('select histogram(col_smallint, "user_input", "[1,3,5,7]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.query('select histogram(col_smallint, "user_input", "[1,3,5,7]", 0) from tb;')
        tdSql.checkRows(3);

        tdSql.query('select histogram(col_int, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.query('select histogram(col_int, "user_input", "[1,3,5,7]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.query('select histogram(col_int, "user_input", "[1,3,5,7]", 0) from tb;')
        tdSql.checkRows(3);

        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5,7]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5,7]", 0) from tb;')
        tdSql.checkRows(3);

        tdSql.query('select histogram(col_float, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.query('select histogram(col_float, "user_input", "[1,3,5,7]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.query('select histogram(col_float, "user_input", "[1,3,5,7]", 0) from tb;')
        tdSql.checkRows(3);

        tdSql.query('select histogram(col_double, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.query('select histogram(col_double, "user_input", "[1,3,5,7]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.query('select histogram(col_double, "user_input", "[1,3,5,7]", 0) from tb;')
        tdSql.checkRows(3);

        #Unsupported column types
        tdSql.error('select histogram(col_timestamp, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select histogram(col_timestamp, "user_input", "[1,3,5,7]", 0) from ctb;')
        tdSql.error('select histogram(col_timestamp, "user_input", "[1,3,5,7]", 0) from tb;')

        tdSql.error('select histogram(col_bool, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select histogram(col_bool, "user_input", "[1,3,5,7]", 0) from ctb;')
        tdSql.error('select histogram(col_bool, "user_input", "[1,3,5,7]", 0) from tb;')

        tdSql.error('select histogram(col_binary, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select histogram(col_binary, "user_input", "[1,3,5,7]", 0) from ctb;')
        tdSql.error('select histogram(col_binary, "user_input", "[1,3,5,7]", 0) from tb;')

        tdSql.error('select histogram(col_nchar, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select histogram(col_nchar, "user_input", "[1,3,5,7]", 0) from ctb;')
        tdSql.error('select histogram(col_nchar, "user_input", "[1,3,5,7]", 0) from tb;')

        tdSql.error('select histogram(col, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select histogram(col, "user_input", "[1,3,5,7]", 0) from ctb;')
        tdSql.error('select histogram(col, "user_input", "[1,3,5,7]", 0) from tb;')

        #Unsupported tags
        tdSql.error('select histogram(tag_timestamp, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select histogram(tag_timestamp, "user_input", "[1,3,5,7]", 0) from ctb;')
        tdSql.error('select histogram(tag_timestamp, "user_input", "[1,3,5,7]", 0) from tb;')

        tdSql.error('select histogram(tag_tinyint, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select histogram(tag_tinyint, "user_input", "[1,3,5,7]", 0) from ctb;')
        tdSql.error('select histogram(tag_tinyint, "user_input", "[1,3,5,7]", 0) from tb;')

        tdSql.error('select histogram(tag_smallint, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select histogram(tag_smallint, "user_input", "[1,3,5,7]", 0) from ctb;')
        tdSql.error('select histogram(tag_smallint, "user_input", "[1,3,5,7]", 0) from tb;')

        tdSql.error('select histogram(tag_int, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select histogram(tag_int, "user_input", "[1,3,5,7]", 0) from ctb;')
        tdSql.error('select histogram(tag_int, "user_input", "[1,3,5,7]", 0) from tb;')

        tdSql.error('select histogram(tag_bigint, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select histogram(tag_bigint, "user_input", "[1,3,5,7]", 0) from ctb;')
        tdSql.error('select histogram(tag_bigint, "user_input", "[1,3,5,7]", 0) from tb;')

        tdSql.error('select histogram(tag_float, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select histogram(tag_float, "user_input", "[1,3,5,7]", 0) from ctb;')
        tdSql.error('select histogram(tag_float, "user_input", "[1,3,5,7]", 0) from tb;')

        tdSql.error('select histogram(tag_double, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select histogram(tag_double, "user_input", "[1,3,5,7]", 0) from ctb;')
        tdSql.error('select histogram(tag_double, "user_input", "[1,3,5,7]", 0) from tb;')

        tdSql.error('select histogram(tag_bool, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select histogram(tag_bool, "user_input", "[1,3,5,7]", 0) from ctb;')
        tdSql.error('select histogram(tag_bool, "user_input", "[1,3,5,7]", 0) from tb;')

        tdSql.error('select histogram(tag_binary, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select histogram(tag_binary, "user_input", "[1,3,5,7]", 0) from ctb;')
        tdSql.error('select histogram(tag_binary, "user_input", "[1,3,5,7]", 0) from tb;')

        tdSql.error('select histogram(tag_nchar, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select histogram(tag_nchar, "user_input", "[1,3,5,7]", 0) from ctb;')
        tdSql.error('select histogram(tag_nchar, "user_input", "[1,3,5,7]", 0) from tb;')


        print("============== STEP 2: bin types  ================== ")
        ## user_input ##
        #TINYINT
        tdSql.query('select histogram(col_tinyint, "user_input", "[1,3,5]", 0) from stb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.query('select histogram(col_tinyint, "user_input", "[1,3,5]", 0) from ctb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.query('select histogram(col_tinyint, "user_input", "[1,3,5]", 0) from tb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");

        tdSql.query('select histogram(col_tinyint, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.query('select histogram(col_tinyint, "user_input", "[1,3,5,7]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.query('select histogram(col_tinyint, "user_input", "[1,3,5,7]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");

        tdSql.query('select histogram(col_tinyint, "user_input", "[0,10,20,100]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(0:10]:10");
        tdSql.checkData(1, 0, "(10:20]:2");
        tdSql.checkData(2, 0, "(20:100]:1");
        tdSql.query('select histogram(col_tinyint, "user_input", "[0,10,20,100]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(0:10]:10");
        tdSql.checkData(1, 0, "(10:20]:2");
        tdSql.checkData(2, 0, "(20:100]:1");
        tdSql.query('select histogram(col_tinyint, "user_input", "[0,10,20,100]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(0:10]:10");
        tdSql.checkData(1, 0, "(10:20]:2");
        tdSql.checkData(2, 0, "(20:100]:1");

        tdSql.query('select histogram(col_tinyint, "user_input", "[-10,10,20,100]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-10:10]:12");
        tdSql.checkData(1, 0, "(10:20]:2");
        tdSql.checkData(2, 0, "(20:100]:1");
        tdSql.query('select histogram(col_tinyint, "user_input", "[-10,10,20,100]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-10:10]:12");
        tdSql.checkData(1, 0, "(10:20]:2");
        tdSql.checkData(2, 0, "(20:100]:1");
        tdSql.query('select histogram(col_tinyint, "user_input", "[-10,10,20,100]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-10:10]:12");
        tdSql.checkData(1, 0, "(10:20]:2");
        tdSql.checkData(2, 0, "(20:100]:1");

        tdSql.query('select histogram(col_tinyint, "user_input", "[-8.9,9.9,19.9,99.9]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-8.9:9.9]:10");
        tdSql.checkData(1, 0, "(9.9:19.9]:2");
        tdSql.checkData(2, 0, "(19.9:99.9]:2");
        tdSql.query('select histogram(col_tinyint, "user_input", "[-8.9,9.9,19.9,99.9]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-8.9:9.9]:10");
        tdSql.checkData(1, 0, "(9.9:19.9]:2");
        tdSql.checkData(2, 0, "(19.9:99.9]:2");
        tdSql.query('select histogram(col_tinyint, "user_input", "[-8.9,9.9,19.9,99.9]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-8.9:9.9]:10");
        tdSql.checkData(1, 0, "(9.9:19.9]:2");
        tdSql.checkData(2, 0, "(19.9:99.9]:2");

        tdSql.query('select histogram(col_tinyint, "user_input", "[-99999999999999,9.9,19.9,99999999999999]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1e+14:9.9]:11");
        tdSql.checkData(1, 0, "(9.9:19.9]:2");
        tdSql.checkData(2, 0, "(19.9:1e+14]:2");
        tdSql.query('select histogram(col_tinyint, "user_input", "[-99999999999999,9.9,19.9,99999999999999]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1e+14:9.9]:11");
        tdSql.checkData(1, 0, "(9.9:19.9]:2");
        tdSql.checkData(2, 0, "(19.9:1e+14]:2");
        tdSql.query('select histogram(col_tinyint, "user_input", "[-99999999999999,9.9,19.9,99999999999999]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1e+14:9.9]:11");
        tdSql.checkData(1, 0, "(9.9:19.9]:2");
        tdSql.checkData(2, 0, "(19.9:1e+14]:2");

        #SMALLINT
        tdSql.query('select histogram(col_smallint, "user_input", "[1,3,5]", 0) from stb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.query('select histogram(col_smallint, "user_input", "[1,3,5]", 0) from ctb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.query('select histogram(col_smallint, "user_input", "[1,3,5]", 0) from tb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");

        tdSql.query('select histogram(col_smallint, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.query('select histogram(col_smallint, "user_input", "[1,3,5,7]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.query('select histogram(col_smallint, "user_input", "[1,3,5,7]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");

        tdSql.query('select histogram(col_smallint, "user_input", "[0,10,20,100]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(0:10]:10");
        tdSql.checkData(1, 0, "(10:20]:2");
        tdSql.checkData(2, 0, "(20:100]:1");
        tdSql.query('select histogram(col_smallint, "user_input", "[0,10,20,100]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(0:10]:10");
        tdSql.checkData(1, 0, "(10:20]:2");
        tdSql.checkData(2, 0, "(20:100]:1");
        tdSql.query('select histogram(col_smallint, "user_input", "[0,10,20,100]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(0:10]:10");
        tdSql.checkData(1, 0, "(10:20]:2");
        tdSql.checkData(2, 0, "(20:100]:1");

        tdSql.query('select histogram(col_smallint, "user_input", "[-10,10,20,100]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-10:10]:12");
        tdSql.checkData(1, 0, "(10:20]:2");
        tdSql.checkData(2, 0, "(20:100]:1");
        tdSql.query('select histogram(col_smallint, "user_input", "[-10,10,20,100]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-10:10]:12");
        tdSql.checkData(1, 0, "(10:20]:2");
        tdSql.checkData(2, 0, "(20:100]:1");
        tdSql.query('select histogram(col_smallint, "user_input", "[-10,10,20,100]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-10:10]:12");
        tdSql.checkData(1, 0, "(10:20]:2");
        tdSql.checkData(2, 0, "(20:100]:1");

        tdSql.query('select histogram(col_smallint, "user_input", "[-8.9,9.9,19.9,99.9]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-8.9:9.9]:10");
        tdSql.checkData(1, 0, "(9.9:19.9]:2");
        tdSql.checkData(2, 0, "(19.9:99.9]:2");
        tdSql.query('select histogram(col_smallint, "user_input", "[-8.9,9.9,19.9,99.9]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-8.9:9.9]:10");
        tdSql.checkData(1, 0, "(9.9:19.9]:2");
        tdSql.checkData(2, 0, "(19.9:99.9]:2");
        tdSql.query('select histogram(col_smallint, "user_input", "[-8.9,9.9,19.9,99.9]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-8.9:9.9]:10");
        tdSql.checkData(1, 0, "(9.9:19.9]:2");
        tdSql.checkData(2, 0, "(19.9:99.9]:2");

        tdSql.query('select histogram(col_smallint, "user_input", "[-99999999999999,9.9,19.9,99999999999999]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1e+14:9.9]:11");
        tdSql.checkData(1, 0, "(9.9:19.9]:2");
        tdSql.checkData(2, 0, "(19.9:1e+14]:2");
        tdSql.query('select histogram(col_smallint, "user_input", "[-99999999999999,9.9,19.9,99999999999999]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1e+14:9.9]:11");
        tdSql.checkData(1, 0, "(9.9:19.9]:2");
        tdSql.checkData(2, 0, "(19.9:1e+14]:2");
        tdSql.query('select histogram(col_smallint, "user_input", "[-99999999999999,9.9,19.9,99999999999999]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1e+14:9.9]:11");
        tdSql.checkData(1, 0, "(9.9:19.9]:2");
        tdSql.checkData(2, 0, "(19.9:1e+14]:2");

        #INT
        tdSql.query('select histogram(col_int, "user_input", "[1,3,5]", 0) from stb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.query('select histogram(col_int, "user_input", "[1,3,5]", 0) from ctb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.query('select histogram(col_int, "user_input", "[1,3,5]", 0) from tb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");

        tdSql.query('select histogram(col_int, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.query('select histogram(col_int, "user_input", "[1,3,5,7]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.query('select histogram(col_int, "user_input", "[1,3,5,7]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");

        tdSql.query('select histogram(col_int, "user_input", "[0,10,20,100]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(0:10]:10");
        tdSql.checkData(1, 0, "(10:20]:2");
        tdSql.checkData(2, 0, "(20:100]:1");
        tdSql.query('select histogram(col_int, "user_input", "[0,10,20,100]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(0:10]:10");
        tdSql.checkData(1, 0, "(10:20]:2");
        tdSql.checkData(2, 0, "(20:100]:1");
        tdSql.query('select histogram(col_int, "user_input", "[0,10,20,100]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(0:10]:10");
        tdSql.checkData(1, 0, "(10:20]:2");
        tdSql.checkData(2, 0, "(20:100]:1");

        tdSql.query('select histogram(col_int, "user_input", "[-10,10,20,100]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-10:10]:12");
        tdSql.checkData(1, 0, "(10:20]:2");
        tdSql.checkData(2, 0, "(20:100]:1");
        tdSql.query('select histogram(col_int, "user_input", "[-10,10,20,100]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-10:10]:12");
        tdSql.checkData(1, 0, "(10:20]:2");
        tdSql.checkData(2, 0, "(20:100]:1");
        tdSql.query('select histogram(col_int, "user_input", "[-10,10,20,100]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-10:10]:12");
        tdSql.checkData(1, 0, "(10:20]:2");
        tdSql.checkData(2, 0, "(20:100]:1");

        tdSql.query('select histogram(col_int, "user_input", "[-8.9,9.9,19.9,99.9]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-8.9:9.9]:10");
        tdSql.checkData(1, 0, "(9.9:19.9]:2");
        tdSql.checkData(2, 0, "(19.9:99.9]:2");
        tdSql.query('select histogram(col_int, "user_input", "[-8.9,9.9,19.9,99.9]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-8.9:9.9]:10");
        tdSql.checkData(1, 0, "(9.9:19.9]:2");
        tdSql.checkData(2, 0, "(19.9:99.9]:2");
        tdSql.query('select histogram(col_int, "user_input", "[-8.9,9.9,19.9,99.9]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-8.9:9.9]:10");
        tdSql.checkData(1, 0, "(9.9:19.9]:2");
        tdSql.checkData(2, 0, "(19.9:99.9]:2");

        tdSql.query('select histogram(col_int, "user_input", "[-99999999999999,9.9,19.9,99999999999999]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1e+14:9.9]:11");
        tdSql.checkData(1, 0, "(9.9:19.9]:2");
        tdSql.checkData(2, 0, "(19.9:1e+14]:2");
        tdSql.query('select histogram(col_int, "user_input", "[-99999999999999,9.9,19.9,99999999999999]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1e+14:9.9]:11");
        tdSql.checkData(1, 0, "(9.9:19.9]:2");
        tdSql.checkData(2, 0, "(19.9:1e+14]:2");
        tdSql.query('select histogram(col_int, "user_input", "[-99999999999999,9.9,19.9,99999999999999]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1e+14:9.9]:11");
        tdSql.checkData(1, 0, "(9.9:19.9]:2");
        tdSql.checkData(2, 0, "(19.9:1e+14]:2");

        #BIGINT
        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5]", 0) from stb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5]", 0) from ctb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5]", 0) from tb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");

        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5,7]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5,7]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");

        tdSql.query('select histogram(col_bigint, "user_input", "[0,10,20,100]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(0:10]:10");
        tdSql.checkData(1, 0, "(10:20]:2");
        tdSql.checkData(2, 0, "(20:100]:1");
        tdSql.query('select histogram(col_bigint, "user_input", "[0,10,20,100]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(0:10]:10");
        tdSql.checkData(1, 0, "(10:20]:2");
        tdSql.checkData(2, 0, "(20:100]:1");
        tdSql.query('select histogram(col_bigint, "user_input", "[0,10,20,100]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(0:10]:10");
        tdSql.checkData(1, 0, "(10:20]:2");
        tdSql.checkData(2, 0, "(20:100]:1");

        tdSql.query('select histogram(col_bigint, "user_input", "[-10,10,20,100]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-10:10]:12");
        tdSql.checkData(1, 0, "(10:20]:2");
        tdSql.checkData(2, 0, "(20:100]:1");
        tdSql.query('select histogram(col_bigint, "user_input", "[-10,10,20,100]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-10:10]:12");
        tdSql.checkData(1, 0, "(10:20]:2");
        tdSql.checkData(2, 0, "(20:100]:1");
        tdSql.query('select histogram(col_bigint, "user_input", "[-10,10,20,100]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-10:10]:12");
        tdSql.checkData(1, 0, "(10:20]:2");
        tdSql.checkData(2, 0, "(20:100]:1");

        tdSql.query('select histogram(col_bigint, "user_input", "[-8.9,9.9,19.9,99.9]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-8.9:9.9]:10");
        tdSql.checkData(1, 0, "(9.9:19.9]:2");
        tdSql.checkData(2, 0, "(19.9:99.9]:2");
        tdSql.query('select histogram(col_bigint, "user_input", "[-8.9,9.9,19.9,99.9]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-8.9:9.9]:10");
        tdSql.checkData(1, 0, "(9.9:19.9]:2");
        tdSql.checkData(2, 0, "(19.9:99.9]:2");
        tdSql.query('select histogram(col_bigint, "user_input", "[-8.9,9.9,19.9,99.9]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-8.9:9.9]:10");
        tdSql.checkData(1, 0, "(9.9:19.9]:2");
        tdSql.checkData(2, 0, "(19.9:99.9]:2");

        tdSql.query('select histogram(col_bigint, "user_input", "[-99999999999999,9.9,19.9,99999999999999]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1e+14:9.9]:11");
        tdSql.checkData(1, 0, "(9.9:19.9]:2");
        tdSql.checkData(2, 0, "(19.9:1e+14]:2");
        tdSql.query('select histogram(col_bigint, "user_input", "[-99999999999999,9.9,19.9,99999999999999]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1e+14:9.9]:11");
        tdSql.checkData(1, 0, "(9.9:19.9]:2");
        tdSql.checkData(2, 0, "(19.9:1e+14]:2");
        tdSql.query('select histogram(col_bigint, "user_input", "[-99999999999999,9.9,19.9,99999999999999]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1e+14:9.9]:11");
        tdSql.checkData(1, 0, "(9.9:19.9]:2");
        tdSql.checkData(2, 0, "(19.9:1e+14]:2");

        #FLOAT
        tdSql.query('select histogram(col_float, "user_input", "[1,3,5]", 0) from stb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.query('select histogram(col_float, "user_input", "[1,3,5]", 0) from ctb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.query('select histogram(col_float, "user_input", "[1,3,5]", 0) from tb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");

        tdSql.query('select histogram(col_float, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.query('select histogram(col_float, "user_input", "[1,3,5,7]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.query('select histogram(col_float, "user_input", "[1,3,5,7]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");

        tdSql.query('select histogram(col_float, "user_input", "[0,10,20,100]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(0:10]:9");
        tdSql.checkData(1, 0, "(10:20]:2");
        tdSql.checkData(2, 0, "(20:100]:2");
        tdSql.query('select histogram(col_float, "user_input", "[0,10,20,100]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(0:10]:9");
        tdSql.checkData(1, 0, "(10:20]:2");
        tdSql.checkData(2, 0, "(20:100]:2");
        tdSql.query('select histogram(col_float, "user_input", "[0,10,20,100]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(0:10]:9");
        tdSql.checkData(1, 0, "(10:20]:2");
        tdSql.checkData(2, 0, "(20:100]:2");

        tdSql.query('select histogram(col_float, "user_input", "[-10,10,20,100]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-10:10]:11");
        tdSql.checkData(1, 0, "(10:20]:2");
        tdSql.checkData(2, 0, "(20:100]:2");
        tdSql.query('select histogram(col_float, "user_input", "[-10,10,20,100]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-10:10]:11");
        tdSql.checkData(1, 0, "(10:20]:2");
        tdSql.checkData(2, 0, "(20:100]:2");
        tdSql.query('select histogram(col_float, "user_input", "[-10,10,20,100]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-10:10]:11");
        tdSql.checkData(1, 0, "(10:20]:2");
        tdSql.checkData(2, 0, "(20:100]:2");

        tdSql.query('select histogram(col_float, "user_input", "[-9.4,9.6,20.4,99.9]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-9.4:9.6]:10");
        tdSql.checkData(1, 0, "(9.6:20.4]:2");
        tdSql.checkData(2, 0, "(20.4:99.9]:2");
        tdSql.query('select histogram(col_float, "user_input", "[-9.4,9.6,20.4,99.9]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-9.4:9.6]:10");
        tdSql.checkData(1, 0, "(9.6:20.4]:2");
        tdSql.checkData(2, 0, "(20.4:99.9]:2");
        tdSql.query('select histogram(col_float, "user_input", "[-9.4,9.6,20.4,99.9]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-9.4:9.6]:10");
        tdSql.checkData(1, 0, "(9.6:20.4]:2");
        tdSql.checkData(2, 0, "(20.4:99.9]:2");


        tdSql.query('select histogram(col_float, "user_input", "[-99999999999999,9.9,19.9,99999999999999]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1e+14:9.9]:11");
        tdSql.checkData(1, 0, "(9.9:19.9]:2");
        tdSql.checkData(2, 0, "(19.9:1e+14]:2");
        tdSql.query('select histogram(col_float, "user_input", "[-99999999999999,9.9,19.9,99999999999999]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1e+14:9.9]:11");
        tdSql.checkData(1, 0, "(9.9:19.9]:2");
        tdSql.checkData(2, 0, "(19.9:1e+14]:2");
        tdSql.query('select histogram(col_float, "user_input", "[-99999999999999,9.9,19.9,99999999999999]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1e+14:9.9]:11");
        tdSql.checkData(1, 0, "(9.9:19.9]:2");
        tdSql.checkData(2, 0, "(19.9:1e+14]:2");

        #DOUBLE
        tdSql.query('select histogram(col_double, "user_input", "[1,3,5]", 0) from stb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.query('select histogram(col_double, "user_input", "[1,3,5]", 0) from ctb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.query('select histogram(col_double, "user_input", "[1,3,5]", 0) from tb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");

        tdSql.query('select histogram(col_double, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.query('select histogram(col_double, "user_input", "[1,3,5,7]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.query('select histogram(col_double, "user_input", "[1,3,5,7]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");

        tdSql.query('select histogram(col_double, "user_input", "[0,10,20,100]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(0:10]:9");
        tdSql.checkData(1, 0, "(10:20]:2");
        tdSql.checkData(2, 0, "(20:100]:2");
        tdSql.query('select histogram(col_double, "user_input", "[0,10,20,100]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(0:10]:9");
        tdSql.checkData(1, 0, "(10:20]:2");
        tdSql.checkData(2, 0, "(20:100]:2");
        tdSql.query('select histogram(col_double, "user_input", "[0,10,20,100]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(0:10]:9");
        tdSql.checkData(1, 0, "(10:20]:2");
        tdSql.checkData(2, 0, "(20:100]:2");

        tdSql.query('select histogram(col_double, "user_input", "[-10,10,20,100]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-10:10]:11");
        tdSql.checkData(1, 0, "(10:20]:2");
        tdSql.checkData(2, 0, "(20:100]:2");
        tdSql.query('select histogram(col_double, "user_input", "[-10,10,20,100]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-10:10]:11");
        tdSql.checkData(1, 0, "(10:20]:2");
        tdSql.checkData(2, 0, "(20:100]:2");
        tdSql.query('select histogram(col_double, "user_input", "[-10,10,20,100]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-10:10]:11");
        tdSql.checkData(1, 0, "(10:20]:2");
        tdSql.checkData(2, 0, "(20:100]:2");

        tdSql.query('select histogram(col_double, "user_input", "[-9.4,9.6,20.4,99.9]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-9.4:9.6]:10");
        tdSql.checkData(1, 0, "(9.6:20.4]:2");
        tdSql.checkData(2, 0, "(20.4:99.9]:2");
        tdSql.query('select histogram(col_double, "user_input", "[-9.4,9.6,20.4,99.9]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-9.4:9.6]:10");
        tdSql.checkData(1, 0, "(9.6:20.4]:2");
        tdSql.checkData(2, 0, "(20.4:99.9]:2");
        tdSql.query('select histogram(col_double, "user_input", "[-9.4,9.6,20.4,99.9]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-9.4:9.6]:10");
        tdSql.checkData(1, 0, "(9.6:20.4]:2");
        tdSql.checkData(2, 0, "(20.4:99.9]:2");


        tdSql.query('select histogram(col_double, "user_input", "[-99999999999999,9.9,19.9,99999999999999]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1e+14:9.9]:11");
        tdSql.checkData(1, 0, "(9.9:19.9]:2");
        tdSql.checkData(2, 0, "(19.9:1e+14]:2");
        tdSql.query('select histogram(col_double, "user_input", "[-99999999999999,9.9,19.9,99999999999999]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1e+14:9.9]:11");
        tdSql.checkData(1, 0, "(9.9:19.9]:2");
        tdSql.checkData(2, 0, "(19.9:1e+14]:2");
        tdSql.query('select histogram(col_double, "user_input", "[-99999999999999,9.9,19.9,99999999999999]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1e+14:9.9]:11");
        tdSql.checkData(1, 0, "(9.9:19.9]:2");
        tdSql.checkData(2, 0, "(19.9:1e+14]:2");

        #ERROR CASE
        tdSql.error('select histogram(col_double, 1, "[1,5,3,7]", 0) from stb;')
        tdSql.error('select histogram(col_double, 1, "[1,5,3,7]", 0) from ctb;')
        tdSql.error('select histogram(col_double, 1, "[1,5,3,7]", 0) from tb;')
        tdSql.error('select histogram(col_double, -1.0, "[1,5,3,7]", 0) from stb;')
        tdSql.error('select histogram(col_double, -1.0, "[1,5,3,7]", 0) from ctb;')
        tdSql.error('select histogram(col_double, -1.0, "[1,5,3,7]", 0) from tb;')
        tdSql.error('select histogram(col_double, true, "[1,5,3,7]", 0) from stb;')
        tdSql.error('select histogram(col_double, false, "[1,5,3,7]", 0) from ctb;')
        tdSql.error('select histogram(col_double, true, "[1,5,3,7]", 0) from tb;')
        tdSql.error('select histogram(col_double, "user", "[1,5,3,7]", 0) from stb;')
        tdSql.error('select histogram(col_double, "user", "[1,5,3,7]", 0) from ctb;')
        tdSql.error('select histogram(col_double, "user", "[1,5,3,7]", 0) from tb;')
        tdSql.error('select histogram(col_double, "user_input", "[1,5,3,7]", 0) from stb;')
        tdSql.error('select histogram(col_double, "user_input", "[1,5,3,7]", 0) from ctb;')
        tdSql.error('select histogram(col_double, "user_input", "[1,5,3,7]", 0) from tb;')
        tdSql.error('select histogram(col_double, "user_input", "[1,-1,3,-3]", 0) from stb;')
        tdSql.error('select histogram(col_double, "user_input", "[1,-1,3,-3]", 0) from ctb;')
        tdSql.error('select histogram(col_double, "user_input", "[1,-1,3,-3]", 0) from tb;')
        tdSql.error('select histogram(col_double, "user_input", "[1.0,5.5,3.3,7.7]", 0) from stb;')
        tdSql.error('select histogram(col_double, "user_input", "[1.0,5.5,3.3,7.7]", 0) from ctb;')
        tdSql.error('select histogram(col_double, "user_input", "[1.0,5.5,3.3,7.7]", 0) from tb;')
        tdSql.error('select histogram(col_double, "user_input", "[1,1,1]", 0) from stb;')
        tdSql.error('select histogram(col_double, "user_input", "[1,1,1]", 0) from ctb;')
        tdSql.error('select histogram(col_double, "user_input", "[1,1,1]", 0) from tb;')
        tdSql.error('select histogram(col_double, "user_input", "[-1,-1,1]", 0) from stb;')
        tdSql.error('select histogram(col_double, "user_input", "[-1,-1,1]", 0) from ctb;')
        tdSql.error('select histogram(col_double, "user_input", "[-1,-1,1]", 0) from tb;')
        tdSql.error('select histogram(col_double, "user_input", "[false,3,5]", 0) from stb;')
        tdSql.error('select histogram(col_double, "user_input", "[false,3,5]", 0) from ctb;')
        tdSql.error('select histogram(col_double, "user_input", "[false,3,5]", 0) from tb;')
        tdSql.error('select histogram(col_double, "user_input", "[1,true,5]", 0) from stb;')
        tdSql.error('select histogram(col_double, "user_input", "[1,true,5]", 0) from ctb;')
        tdSql.error('select histogram(col_double, "user_input", "[1,true,5]", 0) from tb;')
        tdSql.error('select histogram(col_double, "user_input", "[1.0,"abc",5]", 0) from stb;')
        tdSql.error('select histogram(col_double, "user_input", "[1.0,"abc",5]", 0) from ctb;')
        tdSql.error('select histogram(col_double, "user_input", "[1.0,"abc",5]", 0) from tb;')
        tdSql.error('select histogram(col_double, "user_input", "[1.0, 5, "中文"]", 0) from stb;')
        tdSql.error('select histogram(col_double, "user_input", "[1.0, 5, "中文"]", 0) from ctb;')
        tdSql.error('select histogram(col_double, "user_input", "[1.0, 5, "中文"]", 0) from tb;')
        tdSql.error('select histogram(col_double, "user_input", "{1.0, 3.0, 5.0}", 0) from stb;')
        tdSql.error('select histogram(col_double, "user_input", "{1.0, 3.0, 5.0}", 0) from ctb;')
        tdSql.error('select histogram(col_double, "user_input", "{1.0, 3.0, 5.0}", 0) from tb;')
        tdSql.error('select histogram(col_double, \'user_input\', \'{"start": 1.0, "width": 3.0, "count": 5, "infinity": true}\', 0) from stb;')
        tdSql.error('select histogram(col_double, \'user_input\', \'{"start": 1.0, "width": 3.0, "count": 5, "infinity": true}\', 0) from ctb;')
        tdSql.error('select histogram(col_double, \'user_input\', \'{"start": 1.0, "width": 3.0, "count": 5, "infinity": true}\', 0) from tb;')
        tdSql.error('select histogram(col_double, \'user_input\', \'{"start": 1.0, "factor": 3.0, "count": 5, "infinity": true}\', 0) from stb;')
        tdSql.error('select histogram(col_double, \'user_input\', \'{"start": 1.0, "factor": 3.0, "count": 5, "infinity": true}\', 0) from ctb;')
        tdSql.error('select histogram(col_double, \'user_input\', \'{"start": 1.0, "factor": 3.0, "count": 5, "infinity": true}\', 0) from tb;')


        ## linear_bins ##
        #INTEGER
        tdSql.query('select histogram(col_tinyint, \'linear_bin\', \'{"start": 1, "width": 3, "count": 8, "infinity": false}\', 0) from stb;')
        tdSql.checkRows(8);
        tdSql.checkData(0, 0, "(1:4]:3");
        tdSql.checkData(1, 0, "(4:7]:3");
        tdSql.checkData(2, 0, "(7:10]:3");
        tdSql.checkData(3, 0, "(10:13]:0");
        tdSql.checkData(4, 0, "(13:16]:1");
        tdSql.checkData(5, 0, "(16:19]:0");
        tdSql.checkData(6, 0, "(19:22]:1");
        tdSql.checkData(7, 0, "(22:25]:0");
        tdSql.query('select histogram(col_tinyint, \'linear_bin\', \'{"start": 1, "width": 3, "count": 8, "infinity": false}\', 0) from ctb;')
        tdSql.checkRows(8);
        tdSql.checkData(0, 0, "(1:4]:3");
        tdSql.checkData(1, 0, "(4:7]:3");
        tdSql.checkData(2, 0, "(7:10]:3");
        tdSql.checkData(3, 0, "(10:13]:0");
        tdSql.checkData(4, 0, "(13:16]:1");
        tdSql.checkData(5, 0, "(16:19]:0");
        tdSql.checkData(6, 0, "(19:22]:1");
        tdSql.checkData(7, 0, "(22:25]:0");
        tdSql.query('select histogram(col_tinyint, \'linear_bin\', \'{"start": 1, "width": 3, "count": 8, "infinity": false}\', 0) from tb;')
        tdSql.checkRows(8);
        tdSql.checkData(0, 0, "(1:4]:3");
        tdSql.checkData(1, 0, "(4:7]:3");
        tdSql.checkData(2, 0, "(7:10]:3");
        tdSql.checkData(3, 0, "(10:13]:0");
        tdSql.checkData(4, 0, "(13:16]:1");
        tdSql.checkData(5, 0, "(16:19]:0");
        tdSql.checkData(6, 0, "(19:22]:1");
        tdSql.checkData(7, 0, "(22:25]:0");

        tdSql.query('select histogram(col_tinyint, \'linear_bin\', \'{"start": -10.0, "width": 3.0, "count": 8, "infinity": false}\', 0) from stb;')
        tdSql.checkRows(8);
        tdSql.checkData(0, 0, "(-10:-7]:1");
        tdSql.checkData(1, 0, "(-7:-4]:0");
        tdSql.checkData(2, 0, "(-4:-1]:1");
        tdSql.checkData(3, 0, "(-1:2]:2");
        tdSql.checkData(4, 0, "(2:5]:3");
        tdSql.checkData(5, 0, "(5:8]:3");
        tdSql.checkData(6, 0, "(8:11]:2");
        tdSql.checkData(7, 0, "(11:14]:0");
        tdSql.query('select histogram(col_tinyint, \'linear_bin\', \'{"start": -10.0, "width": 3.0, "count": 8, "infinity": false}\', 0) from ctb;')
        tdSql.checkRows(8);
        tdSql.checkData(0, 0, "(-10:-7]:1");
        tdSql.checkData(1, 0, "(-7:-4]:0");
        tdSql.checkData(2, 0, "(-4:-1]:1");
        tdSql.checkData(3, 0, "(-1:2]:2");
        tdSql.checkData(4, 0, "(2:5]:3");
        tdSql.checkData(5, 0, "(5:8]:3");
        tdSql.checkData(6, 0, "(8:11]:2");
        tdSql.checkData(7, 0, "(11:14]:0");
        tdSql.query('select histogram(col_tinyint, \'linear_bin\', \'{"start": -10.0, "width": 3.0, "count": 8, "infinity": false}\', 0) from tb;')
        tdSql.checkRows(8);
        tdSql.checkData(0, 0, "(-10:-7]:1");
        tdSql.checkData(1, 0, "(-7:-4]:0");
        tdSql.checkData(2, 0, "(-4:-1]:1");
        tdSql.checkData(3, 0, "(-1:2]:2");
        tdSql.checkData(4, 0, "(2:5]:3");
        tdSql.checkData(5, 0, "(5:8]:3");
        tdSql.checkData(6, 0, "(8:11]:2");
        tdSql.checkData(7, 0, "(11:14]:0");

        tdSql.query('select histogram(col_tinyint, \'linear_bin\', \'{"start": -2.5, "width": 0.5, "count": 8, "infinity": false}\', 0) from stb;')
        tdSql.checkRows(8);
        tdSql.checkData(0, 0, "(-2.5:-2]:0");
        tdSql.checkData(1, 0, "(-2:-1.5]:0");
        tdSql.checkData(2, 0, "(-1.5:-1]:1");
        tdSql.checkData(3, 0, "(-1:-0.5]:0");
        tdSql.checkData(4, 0, "(-0.5:0]:0");
        tdSql.checkData(5, 0, "(0:0.5]:0");
        tdSql.checkData(6, 0, "(0.5:1]:1");
        tdSql.checkData(7, 0, "(1:1.5]:0");
        tdSql.query('select histogram(col_tinyint, \'linear_bin\', \'{"start": -2.5, "width": 0.5, "count": 8, "infinity": false}\', 0) from ctb;')
        tdSql.checkRows(8);
        tdSql.checkData(0, 0, "(-2.5:-2]:0");
        tdSql.checkData(1, 0, "(-2:-1.5]:0");
        tdSql.checkData(2, 0, "(-1.5:-1]:1");
        tdSql.checkData(3, 0, "(-1:-0.5]:0");
        tdSql.checkData(4, 0, "(-0.5:0]:0");
        tdSql.checkData(5, 0, "(0:0.5]:0");
        tdSql.checkData(6, 0, "(0.5:1]:1");
        tdSql.checkData(7, 0, "(1:1.5]:0");
        tdSql.query('select histogram(col_tinyint, \'linear_bin\', \'{"start": -2.5, "width": 0.5, "count": 8, "infinity": false}\', 0) from tb;')
        tdSql.checkRows(8);
        tdSql.checkData(0, 0, "(-2.5:-2]:0");
        tdSql.checkData(1, 0, "(-2:-1.5]:0");
        tdSql.checkData(2, 0, "(-1.5:-1]:1");
        tdSql.checkData(3, 0, "(-1:-0.5]:0");
        tdSql.checkData(4, 0, "(-0.5:0]:0");
        tdSql.checkData(5, 0, "(0:0.5]:0");
        tdSql.checkData(6, 0, "(0.5:1]:1");
        tdSql.checkData(7, 0, "(1:1.5]:0");

        tdSql.query('select histogram(col_tinyint, \'linear_bin\', \'{"start": 4, "width": -0.5, "count": 10, "infinity": false}\', 0) from stb;')
        tdSql.checkRows(10);
        tdSql.checkData(0, 0, "(3.5:4]:1");
        tdSql.checkData(1, 0, "(3:3.5]:0");
        tdSql.checkData(2, 0, "(2.5:3]:1");
        tdSql.checkData(3, 0, "(2:2.5]:0");
        tdSql.checkData(4, 0, "(1.5:2]:1");
        tdSql.checkData(5, 0, "(1:1.5]:0");
        tdSql.checkData(6, 0, "(0.5:1]:1");
        tdSql.checkData(7, 0, "(0:0.5]:0");
        tdSql.checkData(8, 0, "(-0.5:0]:0");
        tdSql.checkData(9, 0, "(-1:-0.5]:0");
        tdSql.query('select histogram(col_tinyint, \'linear_bin\', \'{"start": 4, "width": -0.5, "count": 10, "infinity": false}\', 0) from ctb;')
        tdSql.checkRows(10);
        tdSql.checkData(0, 0, "(3.5:4]:1");
        tdSql.checkData(1, 0, "(3:3.5]:0");
        tdSql.checkData(2, 0, "(2.5:3]:1");
        tdSql.checkData(3, 0, "(2:2.5]:0");
        tdSql.checkData(4, 0, "(1.5:2]:1");
        tdSql.checkData(5, 0, "(1:1.5]:0");
        tdSql.checkData(6, 0, "(0.5:1]:1");
        tdSql.checkData(7, 0, "(0:0.5]:0");
        tdSql.checkData(8, 0, "(-0.5:0]:0");
        tdSql.checkData(9, 0, "(-1:-0.5]:0");
        tdSql.query('select histogram(col_tinyint, \'linear_bin\', \'{"start": 4, "width": -0.5, "count": 10, "infinity": false}\', 0) from tb;')
        tdSql.checkRows(10);
        tdSql.checkData(0, 0, "(3.5:4]:1");
        tdSql.checkData(1, 0, "(3:3.5]:0");
        tdSql.checkData(2, 0, "(2.5:3]:1");
        tdSql.checkData(3, 0, "(2:2.5]:0");
        tdSql.checkData(4, 0, "(1.5:2]:1");
        tdSql.checkData(5, 0, "(1:1.5]:0");
        tdSql.checkData(6, 0, "(0.5:1]:1");
        tdSql.checkData(7, 0, "(0:0.5]:0");
        tdSql.checkData(8, 0, "(-0.5:0]:0");
        tdSql.checkData(9, 0, "(-1:-0.5]:0");

        tdSql.query('select histogram(col_tinyint, \'linear_bin\', \'{"start": 1, "width": 0.5, "count": 1.9999, "infinity": false}\', 0) from stb;')
        tdSql.checkRows(1);
        tdSql.checkData(0, 0, "(1:1.5]:0");
        tdSql.query('select histogram(col_tinyint, \'linear_bin\', \'{"start": 1, "width": 0.5, "count": 1.9999, "infinity": false}\', 0) from ctb;')
        tdSql.checkRows(1);
        tdSql.checkData(0, 0, "(1:1.5]:0");
        tdSql.query('select histogram(col_tinyint, \'linear_bin\', \'{"start": 1, "width": 0.5, "count": 1.9999, "infinity": false}\', 0) from tb;')
        tdSql.checkRows(1);
        tdSql.checkData(0, 0, "(1:1.5]:0");

        tdSql.query('select histogram(col_tinyint, \'linear_bin\', \'{"start": 1, "width": 0.5, "count": 1.99999999999999999, "infinity": false}\', 0) from stb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1:1.5]:0");
        tdSql.checkData(1, 0, "(1.5:2]:1");
        tdSql.query('select histogram(col_tinyint, \'linear_bin\', \'{"start": 1, "width": 0.5, "count": 1.99999999999999999, "infinity": false}\', 0) from ctb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1:1.5]:0");
        tdSql.checkData(1, 0, "(1.5:2]:1");
        tdSql.query('select histogram(col_tinyint, \'linear_bin\', \'{"start": 1, "width": 0.5, "count": 1.99999999999999999, "infinity": false}\', 0) from tb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1:1.5]:0");
        tdSql.checkData(1, 0, "(1.5:2]:1");

        tdSql.query('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 5, "count": 5, "infinity": true}\', 0) from stb;')
        tdSql.checkRows(7);
        tdSql.checkData(0, 0, "(-1.79769e+308:0]:2");
        tdSql.checkData(1, 0, "(0:5]:5");
        tdSql.checkData(2, 0, "(5:10]:5");
        tdSql.checkData(3, 0, "(10:15]:1");
        tdSql.checkData(4, 0, "(15:20]:1");
        tdSql.checkData(5, 0, "(20:25]:0");
        tdSql.checkData(6, 0, "(25:1.79769e+308]:1");
        tdSql.query('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 5, "count": 5, "infinity": true}\', 0) from ctb;')
        tdSql.checkRows(7);
        tdSql.checkData(0, 0, "(-1.79769e+308:0]:2");
        tdSql.checkData(1, 0, "(0:5]:5");
        tdSql.checkData(2, 0, "(5:10]:5");
        tdSql.checkData(3, 0, "(10:15]:1");
        tdSql.checkData(4, 0, "(15:20]:1");
        tdSql.checkData(5, 0, "(20:25]:0");
        tdSql.checkData(6, 0, "(25:1.79769e+308]:1");
        tdSql.query('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 5, "count": 5, "infinity": true}\', 0) from tb;')
        tdSql.checkRows(7);
        tdSql.checkData(0, 0, "(-1.79769e+308:0]:2");
        tdSql.checkData(1, 0, "(0:5]:5");
        tdSql.checkData(2, 0, "(5:10]:5");
        tdSql.checkData(3, 0, "(10:15]:1");
        tdSql.checkData(4, 0, "(15:20]:1");
        tdSql.checkData(5, 0, "(20:25]:0");
        tdSql.checkData(6, 0, "(25:1.79769e+308]:1");

        tdSql.query('select histogram(col_tinyint, \'linear_bin\', \'{"start": -1.76e+308, "width": 5, "count": 1, "infinity": true}\', 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1.79769e+308:-1.76e+308]:0");
        tdSql.checkData(1, 0, "(-1.76e+308:-1.76e+308]:0");
        tdSql.checkData(2, 0, "(-1.76e+308:1.79769e+308]:15");
        tdSql.query('select histogram(col_tinyint, \'linear_bin\', \'{"start": -1.76e+308, "width": 5, "count": 1, "infinity": true}\', 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1.79769e+308:-1.76e+308]:0");
        tdSql.checkData(1, 0, "(-1.76e+308:-1.76e+308]:0");
        tdSql.checkData(2, 0, "(-1.76e+308:1.79769e+308]:15");
        tdSql.query('select histogram(col_tinyint, \'linear_bin\', \'{"start": -1.76e+308, "width": 5, "count": 1, "infinity": true}\', 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1.79769e+308:-1.76e+308]:0");
        tdSql.checkData(1, 0, "(-1.76e+308:-1.76e+308]:0");
        tdSql.checkData(2, 0, "(-1.76e+308:1.79769e+308]:15");

        tdSql.query('select histogram(col_tinyint, \'linear_bin\', \'{"start": 10, "width": -5, "count": 3, "infinity": true}\', 0) from stb;')
        tdSql.checkRows(5);
        tdSql.checkData(0, 0, "(10:1.79769e+308]:3");
        tdSql.checkData(1, 0, "(5:10]:5");
        tdSql.checkData(2, 0, "(0:5]:5");
        tdSql.checkData(3, 0, "(-5:0]:1");
        tdSql.checkData(4, 0, "(-1.79769e+308:-5]:1");
        tdSql.query('select histogram(col_tinyint, \'linear_bin\', \'{"start": 10, "width": -5, "count": 3, "infinity": true}\', 0) from ctb;')
        tdSql.checkRows(5);
        tdSql.checkData(0, 0, "(10:1.79769e+308]:3");
        tdSql.checkData(1, 0, "(5:10]:5");
        tdSql.checkData(2, 0, "(0:5]:5");
        tdSql.checkData(3, 0, "(-5:0]:1");
        tdSql.checkData(4, 0, "(-1.79769e+308:-5]:1");
        tdSql.query('select histogram(col_tinyint, \'linear_bin\', \'{"start": 10, "width": -5, "count": 3, "infinity": true}\', 0) from tb;')
        tdSql.checkRows(5);
        tdSql.checkData(0, 0, "(10:1.79769e+308]:3");
        tdSql.checkData(1, 0, "(5:10]:5");
        tdSql.checkData(2, 0, "(0:5]:5");
        tdSql.checkData(3, 0, "(-5:0]:1");
        tdSql.checkData(4, 0, "(-1.79769e+308:-5]:1");

        tdSql.query('select histogram(col_tinyint, \'linear_bin\', \'{"start": -0.7e+308, "width": 0.7e+308, "count": 2, "infinity": false}\', 0) from stb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(-7e+307:0]:2");
        tdSql.checkData(1, 0, "(0:7e+307]:13");
        tdSql.query('select histogram(col_tinyint, \'linear_bin\', \'{"start": -0.7e+308, "width": 0.7e+308, "count": 2, "infinity": false}\', 0) from ctb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(-7e+307:0]:2");
        tdSql.checkData(1, 0, "(0:7e+307]:13");
        tdSql.query('select histogram(col_tinyint, \'linear_bin\', \'{"start": -0.7e+308, "width": 0.7e+308, "count": 2, "infinity": false}\', 0) from tb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(-7e+307:0]:2");
        tdSql.checkData(1, 0, "(0:7e+307]:13");

        tdSql.query('select histogram(col_tinyint, \'linear_bin\', \'{"start": -0.7e+308, "width": 0.7e+308, "count": 2, "infinity": true}\', 0) from stb;')
        tdSql.checkRows(4);
        tdSql.checkData(0, 0, "(-1.79769e+308:-7e+307]:0");
        tdSql.checkData(1, 0, "(-7e+307:0]:2");
        tdSql.checkData(2, 0, "(0:7e+307]:13");
        tdSql.checkData(3, 0, "(7e+307:1.79769e+308]:0");
        tdSql.query('select histogram(col_tinyint, \'linear_bin\', \'{"start": -0.7e+308, "width": 0.7e+308, "count": 2, "infinity": true}\', 0) from ctb;')
        tdSql.checkRows(4);
        tdSql.checkData(0, 0, "(-1.79769e+308:-7e+307]:0");
        tdSql.checkData(1, 0, "(-7e+307:0]:2");
        tdSql.checkData(2, 0, "(0:7e+307]:13");
        tdSql.checkData(3, 0, "(7e+307:1.79769e+308]:0");
        tdSql.query('select histogram(col_tinyint, \'linear_bin\', \'{"start": -0.7e+308, "width": 0.7e+308, "count": 2, "infinity": true}\', 0) from tb;')
        tdSql.checkRows(4);
        tdSql.checkData(0, 0, "(-1.79769e+308:-7e+307]:0");
        tdSql.checkData(1, 0, "(-7e+307:0]:2");
        tdSql.checkData(2, 0, "(0:7e+307]:13");
        tdSql.checkData(3, 0, "(7e+307:1.79769e+308]:0");

        #FLOATING NUMBER
        tdSql.query('select histogram(col_float, \'linear_bin\', \'{"start": 1, "width": 3, "count": 8, "infinity": false}\', 0) from stb;')
        tdSql.checkRows(8);
        tdSql.checkData(0, 0, "(1:4]:3");
        tdSql.checkData(1, 0, "(4:7]:3");
        tdSql.checkData(2, 0, "(7:10]:3");
        tdSql.checkData(3, 0, "(10:13]:1");
        tdSql.checkData(4, 0, "(13:16]:1");
        tdSql.checkData(5, 0, "(16:19]:0");
        tdSql.checkData(6, 0, "(19:22]:1");
        tdSql.checkData(7, 0, "(22:25]:0");
        tdSql.query('select histogram(col_float, \'linear_bin\', \'{"start": 1, "width": 3, "count": 8, "infinity": false}\', 0) from ctb;')
        tdSql.checkRows(8);
        tdSql.checkData(0, 0, "(1:4]:3");
        tdSql.checkData(1, 0, "(4:7]:3");
        tdSql.checkData(2, 0, "(7:10]:3");
        tdSql.checkData(3, 0, "(10:13]:1");
        tdSql.checkData(4, 0, "(13:16]:1");
        tdSql.checkData(5, 0, "(16:19]:0");
        tdSql.checkData(6, 0, "(19:22]:1");
        tdSql.checkData(7, 0, "(22:25]:0");
        tdSql.query('select histogram(col_float, \'linear_bin\', \'{"start": 1, "width": 3, "count": 8, "infinity": false}\', 0) from tb;')
        tdSql.checkRows(8);
        tdSql.checkData(0, 0, "(1:4]:3");
        tdSql.checkData(1, 0, "(4:7]:3");
        tdSql.checkData(2, 0, "(7:10]:3");
        tdSql.checkData(3, 0, "(10:13]:1");
        tdSql.checkData(4, 0, "(13:16]:1");
        tdSql.checkData(5, 0, "(16:19]:0");
        tdSql.checkData(6, 0, "(19:22]:1");
        tdSql.checkData(7, 0, "(22:25]:0");

        tdSql.query('select histogram(col_float, \'linear_bin\', \'{"start": -10.0, "width": 3.0, "count": 8, "infinity": false}\', 0) from stb;')
        tdSql.checkRows(8);
        tdSql.checkData(0, 0, "(-10:-7]:1");
        tdSql.checkData(1, 0, "(-7:-4]:0");
        tdSql.checkData(2, 0, "(-4:-1]:1");
        tdSql.checkData(3, 0, "(-1:2]:1");
        tdSql.checkData(4, 0, "(2:5]:3");
        tdSql.checkData(5, 0, "(5:8]:3");
        tdSql.checkData(6, 0, "(8:11]:3");
        tdSql.checkData(7, 0, "(11:14]:0");
        tdSql.query('select histogram(col_float, \'linear_bin\', \'{"start": -10.0, "width": 3.0, "count": 8, "infinity": false}\', 0) from ctb;')
        tdSql.checkRows(8);
        tdSql.checkData(0, 0, "(-10:-7]:1");
        tdSql.checkData(1, 0, "(-7:-4]:0");
        tdSql.checkData(2, 0, "(-4:-1]:1");
        tdSql.checkData(3, 0, "(-1:2]:1");
        tdSql.checkData(4, 0, "(2:5]:3");
        tdSql.checkData(5, 0, "(5:8]:3");
        tdSql.checkData(6, 0, "(8:11]:3");
        tdSql.checkData(7, 0, "(11:14]:0");
        tdSql.query('select histogram(col_float, \'linear_bin\', \'{"start": -10.0, "width": 3.0, "count": 8, "infinity": false}\', 0) from tb;')
        tdSql.checkRows(8);
        tdSql.checkData(0, 0, "(-10:-7]:1");
        tdSql.checkData(1, 0, "(-7:-4]:0");
        tdSql.checkData(2, 0, "(-4:-1]:1");
        tdSql.checkData(3, 0, "(-1:2]:1");
        tdSql.checkData(4, 0, "(2:5]:3");
        tdSql.checkData(5, 0, "(5:8]:3");
        tdSql.checkData(6, 0, "(8:11]:3");
        tdSql.checkData(7, 0, "(11:14]:0");

        tdSql.query('select histogram(col_float, \'linear_bin\', \'{"start": -2.5, "width": 0.5, "count": 8, "infinity": false}\', 0) from stb;')
        tdSql.checkRows(8);
        tdSql.checkData(0, 0, "(-2.5:-2]:0");
        tdSql.checkData(1, 0, "(-2:-1.5]:1");
        tdSql.checkData(2, 0, "(-1.5:-1]:0");
        tdSql.checkData(3, 0, "(-1:-0.5]:0");
        tdSql.checkData(4, 0, "(-0.5:0]:0");
        tdSql.checkData(5, 0, "(0:0.5]:0");
        tdSql.checkData(6, 0, "(0.5:1]:0");
        tdSql.checkData(7, 0, "(1:1.5]:1");
        tdSql.query('select histogram(col_float, \'linear_bin\', \'{"start": -2.5, "width": 0.5, "count": 8, "infinity": false}\', 0) from ctb;')
        tdSql.checkRows(8);
        tdSql.checkData(0, 0, "(-2.5:-2]:0");
        tdSql.checkData(1, 0, "(-2:-1.5]:1");
        tdSql.checkData(2, 0, "(-1.5:-1]:0");
        tdSql.checkData(3, 0, "(-1:-0.5]:0");
        tdSql.checkData(4, 0, "(-0.5:0]:0");
        tdSql.checkData(5, 0, "(0:0.5]:0");
        tdSql.checkData(6, 0, "(0.5:1]:0");
        tdSql.checkData(7, 0, "(1:1.5]:1");
        tdSql.query('select histogram(col_float, \'linear_bin\', \'{"start": -2.5, "width": 0.5, "count": 8, "infinity": false}\', 0) from tb;')
        tdSql.checkRows(8);
        tdSql.checkData(0, 0, "(-2.5:-2]:0");
        tdSql.checkData(1, 0, "(-2:-1.5]:1");
        tdSql.checkData(2, 0, "(-1.5:-1]:0");
        tdSql.checkData(3, 0, "(-1:-0.5]:0");
        tdSql.checkData(4, 0, "(-0.5:0]:0");
        tdSql.checkData(5, 0, "(0:0.5]:0");
        tdSql.checkData(6, 0, "(0.5:1]:0");
        tdSql.checkData(7, 0, "(1:1.5]:1");

        tdSql.query('select histogram(col_float, \'linear_bin\', \'{"start": 4, "width": -0.5, "count": 10, "infinity": false}\', 0) from stb;')
        tdSql.checkRows(10);
        tdSql.checkData(0, 0, "(3.5:4]:0");
        tdSql.checkData(1, 0, "(3:3.5]:1");
        tdSql.checkData(2, 0, "(2.5:3]:0");
        tdSql.checkData(3, 0, "(2:2.5]:1");
        tdSql.checkData(4, 0, "(1.5:2]:0");
        tdSql.checkData(5, 0, "(1:1.5]:1");
        tdSql.checkData(6, 0, "(0.5:1]:0");
        tdSql.checkData(7, 0, "(0:0.5]:0");
        tdSql.checkData(8, 0, "(-0.5:0]:0");
        tdSql.checkData(9, 0, "(-1:-0.5]:0");
        tdSql.query('select histogram(col_float, \'linear_bin\', \'{"start": 4, "width": -0.5, "count": 10, "infinity": false}\', 0) from ctb;')
        tdSql.checkRows(10);
        tdSql.checkData(0, 0, "(3.5:4]:0");
        tdSql.checkData(1, 0, "(3:3.5]:1");
        tdSql.checkData(2, 0, "(2.5:3]:0");
        tdSql.checkData(3, 0, "(2:2.5]:1");
        tdSql.checkData(4, 0, "(1.5:2]:0");
        tdSql.checkData(5, 0, "(1:1.5]:1");
        tdSql.checkData(6, 0, "(0.5:1]:0");
        tdSql.checkData(7, 0, "(0:0.5]:0");
        tdSql.checkData(8, 0, "(-0.5:0]:0");
        tdSql.checkData(9, 0, "(-1:-0.5]:0");
        tdSql.query('select histogram(col_float, \'linear_bin\', \'{"start": 4, "width": -0.5, "count": 10, "infinity": false}\', 0) from tb;')
        tdSql.checkRows(10);
        tdSql.checkData(0, 0, "(3.5:4]:0");
        tdSql.checkData(1, 0, "(3:3.5]:1");
        tdSql.checkData(2, 0, "(2.5:3]:0");
        tdSql.checkData(3, 0, "(2:2.5]:1");
        tdSql.checkData(4, 0, "(1.5:2]:0");
        tdSql.checkData(5, 0, "(1:1.5]:1");
        tdSql.checkData(6, 0, "(0.5:1]:0");
        tdSql.checkData(7, 0, "(0:0.5]:0");
        tdSql.checkData(8, 0, "(-0.5:0]:0");
        tdSql.checkData(9, 0, "(-1:-0.5]:0");

        tdSql.query('select histogram(col_float, \'linear_bin\', \'{"start": 1, "width": 0.5, "count": 1.9999, "infinity": false}\', 0) from stb;')
        tdSql.checkRows(1);
        tdSql.checkData(0, 0, "(1:1.5]:1");
        tdSql.query('select histogram(col_float, \'linear_bin\', \'{"start": 1, "width": 0.5, "count": 1.9999, "infinity": false}\', 0) from ctb;')
        tdSql.checkRows(1);
        tdSql.checkData(0, 0, "(1:1.5]:1");
        tdSql.query('select histogram(col_float, \'linear_bin\', \'{"start": 1, "width": 0.5, "count": 1.9999, "infinity": false}\', 0) from tb;')
        tdSql.checkRows(1);
        tdSql.checkData(0, 0, "(1:1.5]:1");

        tdSql.query('select histogram(col_float, \'linear_bin\', \'{"start": 1, "width": 0.5, "count": 1.99999999999999999, "infinity": false}\', 0) from stb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1:1.5]:1");
        tdSql.checkData(1, 0, "(1.5:2]:0");
        tdSql.query('select histogram(col_float, \'linear_bin\', \'{"start": 1, "width": 0.5, "count": 1.99999999999999999, "infinity": false}\', 0) from ctb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1:1.5]:1");
        tdSql.checkData(1, 0, "(1.5:2]:0");
        tdSql.query('select histogram(col_float, \'linear_bin\', \'{"start": 1, "width": 0.5, "count": 1.99999999999999999, "infinity": false}\', 0) from tb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1:1.5]:1");
        tdSql.checkData(1, 0, "(1.5:2]:0");

        tdSql.query('select histogram(col_float, \'linear_bin\', \'{"start": 0, "width": 5, "count": 5, "infinity": true}\', 0) from stb;')
        tdSql.checkRows(7);
        tdSql.checkData(0, 0, "(-1.79769e+308:0]:2");
        tdSql.checkData(1, 0, "(0:5]:4");
        tdSql.checkData(2, 0, "(5:10]:5");
        tdSql.checkData(3, 0, "(10:15]:1");
        tdSql.checkData(4, 0, "(15:20]:1");
        tdSql.checkData(5, 0, "(20:25]:1");
        tdSql.checkData(6, 0, "(25:1.79769e+308]:1");
        tdSql.query('select histogram(col_float, \'linear_bin\', \'{"start": 0, "width": 5, "count": 5, "infinity": true}\', 0) from ctb;')
        tdSql.checkRows(7);
        tdSql.checkData(0, 0, "(-1.79769e+308:0]:2");
        tdSql.checkData(1, 0, "(0:5]:4");
        tdSql.checkData(2, 0, "(5:10]:5");
        tdSql.checkData(3, 0, "(10:15]:1");
        tdSql.checkData(4, 0, "(15:20]:1");
        tdSql.checkData(5, 0, "(20:25]:1");
        tdSql.checkData(6, 0, "(25:1.79769e+308]:1");
        tdSql.query('select histogram(col_float, \'linear_bin\', \'{"start": 0, "width": 5, "count": 5, "infinity": true}\', 0) from tb;')
        tdSql.checkRows(7);
        tdSql.checkData(0, 0, "(-1.79769e+308:0]:2");
        tdSql.checkData(1, 0, "(0:5]:4");
        tdSql.checkData(2, 0, "(5:10]:5");
        tdSql.checkData(3, 0, "(10:15]:1");
        tdSql.checkData(4, 0, "(15:20]:1");
        tdSql.checkData(5, 0, "(20:25]:1");
        tdSql.checkData(6, 0, "(25:1.79769e+308]:1");

        tdSql.query('select histogram(col_float, \'linear_bin\', \'{"start": 10, "width": -5, "count": 3, "infinity": true}\', 0) from stb;')
        tdSql.checkRows(5);
        tdSql.checkData(0, 0, "(10:1.79769e+308]:4");
        tdSql.checkData(1, 0, "(5:10]:5");
        tdSql.checkData(2, 0, "(0:5]:4");
        tdSql.checkData(3, 0, "(-5:0]:1");
        tdSql.checkData(4, 0, "(-1.79769e+308:-5]:1");
        tdSql.query('select histogram(col_float, \'linear_bin\', \'{"start": 10, "width": -5, "count": 3, "infinity": true}\', 0) from ctb;')
        tdSql.checkRows(5);
        tdSql.checkData(0, 0, "(10:1.79769e+308]:4");
        tdSql.checkData(1, 0, "(5:10]:5");
        tdSql.checkData(2, 0, "(0:5]:4");
        tdSql.checkData(3, 0, "(-5:0]:1");
        tdSql.checkData(4, 0, "(-1.79769e+308:-5]:1");
        tdSql.query('select histogram(col_float, \'linear_bin\', \'{"start": 10, "width": -5, "count": 3, "infinity": true}\', 0) from tb;')
        tdSql.checkRows(5);
        tdSql.checkData(0, 0, "(10:1.79769e+308]:4");
        tdSql.checkData(1, 0, "(5:10]:5");
        tdSql.checkData(2, 0, "(0:5]:4");
        tdSql.checkData(3, 0, "(-5:0]:1");
        tdSql.checkData(4, 0, "(-1.79769e+308:-5]:1");

        tdSql.query('select histogram(col_float, \'linear_bin\', \'{"start": -1.76e+308, "width": 5, "count": 1, "infinity": true}\', 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1.79769e+308:-1.76e+308]:0");
        tdSql.checkData(1, 0, "(-1.76e+308:-1.76e+308]:0");
        tdSql.checkData(2, 0, "(-1.76e+308:1.79769e+308]:15");
        tdSql.query('select histogram(col_float, \'linear_bin\', \'{"start": -1.76e+308, "width": 5, "count": 1, "infinity": true}\', 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1.79769e+308:-1.76e+308]:0");
        tdSql.checkData(1, 0, "(-1.76e+308:-1.76e+308]:0");
        tdSql.checkData(2, 0, "(-1.76e+308:1.79769e+308]:15");
        tdSql.query('select histogram(col_float, \'linear_bin\', \'{"start": -1.76e+308, "width": 5, "count": 1, "infinity": true}\', 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1.79769e+308:-1.76e+308]:0");
        tdSql.checkData(1, 0, "(-1.76e+308:-1.76e+308]:0");
        tdSql.checkData(2, 0, "(-1.76e+308:1.79769e+308]:15");

        tdSql.query('select histogram(col_float, \'linear_bin\', \'{"start": -0.7e+308, "width": 0.7e+308, "count": 2, "infinity": false}\', 0) from stb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(-7e+307:0]:2");
        tdSql.checkData(1, 0, "(0:7e+307]:13");
        tdSql.query('select histogram(col_float, \'linear_bin\', \'{"start": -0.7e+308, "width": 0.7e+308, "count": 2, "infinity": false}\', 0) from ctb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(-7e+307:0]:2");
        tdSql.checkData(1, 0, "(0:7e+307]:13");
        tdSql.query('select histogram(col_float, \'linear_bin\', \'{"start": -0.7e+308, "width": 0.7e+308, "count": 2, "infinity": false}\', 0) from tb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(-7e+307:0]:2");
        tdSql.checkData(1, 0, "(0:7e+307]:13");

        tdSql.query('select histogram(col_float, \'linear_bin\', \'{"start": -0.7e+308, "width": 0.7e+308, "count": 2, "infinity": true}\', 0) from stb;')
        tdSql.checkRows(4);
        tdSql.checkData(0, 0, "(-1.79769e+308:-7e+307]:0");
        tdSql.checkData(1, 0, "(-7e+307:0]:2");
        tdSql.checkData(2, 0, "(0:7e+307]:13");
        tdSql.checkData(3, 0, "(7e+307:1.79769e+308]:0");
        tdSql.query('select histogram(col_float, \'linear_bin\', \'{"start": -0.7e+308, "width": 0.7e+308, "count": 2, "infinity": true}\', 0) from ctb;')
        tdSql.checkRows(4);
        tdSql.checkData(0, 0, "(-1.79769e+308:-7e+307]:0");
        tdSql.checkData(1, 0, "(-7e+307:0]:2");
        tdSql.checkData(2, 0, "(0:7e+307]:13");
        tdSql.checkData(3, 0, "(7e+307:1.79769e+308]:0");
        tdSql.query('select histogram(col_float, \'linear_bin\', \'{"start": -0.7e+308, "width": 0.7e+308, "count": 2, "infinity": true}\', 0) from tb;')
        tdSql.checkRows(4);
        tdSql.checkData(0, 0, "(-1.79769e+308:-7e+307]:0");
        tdSql.checkData(1, 0, "(-7e+307:0]:2");
        tdSql.checkData(2, 0, "(0:7e+307]:13");
        tdSql.checkData(3, 0, "(7e+307:1.79769e+308]:0");

        #ERROR CASE
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": true, "width": 5, "count": 5, "infinity": false}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": true, "width": 5, "count": 5, "infinity": false}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": true, "width": 5, "count": 5, "infinity": false}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": false, "width": 5, "count": 5, "infinity": false}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": false, "width": 5, "count": 5, "infinity": false}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": false, "width": 5, "count": 5, "infinity": false}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": "abc", "width": 5, "count": 5, "infinity": false}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": "abc", "width": 5, "count": 5, "infinity": false}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": "abc", "width": 5, "count": 5, "infinity": false}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": "中文", "width": 5, "count": 5, "infinity": false}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": "中文", "width": 5, "count": 5, "infinity": false}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": "中文", "width": 5, "count": 5, "infinity": false}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": abc, "width": 5, "count": 5, "infinity": false}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": abc, "width": 5, "count": 5, "infinity": false}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": abc, "width": 5, "count": 5, "infinity": false}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": -1.80e+308, "width": 5, "count": 5, "infinity": false}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": -1.80e+308, "width": 5, "count": 5, "infinity": false}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": -1.80e+308, "width": 5, "count": 5, "infinity": false}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 1.80e+308, "width": 5, "count": 5, "infinity": false}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 1.80e+308, "width": 5, "count": 5, "infinity": false}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 1.80e+308, "width": 5, "count": 5, "infinity": false}\', 0) from tb;')

        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": true, "count": 5, "infinity": false}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": true, "count": 5, "infinity": false}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": true, "count": 5, "infinity": false}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": false, "count": 5, "infinity": false}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": false, "count": 5, "infinity": false}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": false, "count": 5, "infinity": false}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": "abc", "count": 5, "infinity": false}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": "abc", "count": 5, "infinity": false}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": "abc", "count": 5, "infinity": false}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": "中文", "count": 5, "infinity": false}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": "中文", "count": 5, "infinity": false}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": "中文", "count": 5, "infinity": false}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": abc, "count": 5, "infinity": false}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": abc, "count": 5, "infinity": false}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": abc, "count": 5, "infinity": false}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 0, "count": 5, "infinity": false}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 0, "count": 5, "infinity": false}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 0, "count": 5, "infinity": false}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": -1.80e+308, "count": 5, "infinity": false}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": -1.80e+308, "count": 5, "infinity": false}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": -1.80e+308, "count": 5, "infinity": false}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1.80e+308, "count": 5, "infinity": false}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1.80e+308, "count": 5, "infinity": false}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1.80e+308, "count": 5, "infinity": false}\', 0) from tb;')

        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": -1.4e+308, "width": 1.4e+308, "count": 3, "infinity": false}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": -1.4e+308, "width": 1.4e+308, "count": 3, "infinity": false}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": -1.4e+308, "width": 1.4e+308, "count": 3, "infinity": false}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": -1.4e+308, "width": 1.4e+308, "count": 3, "infinity": true}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": -1.4e+308, "width": 1.4e+308, "count": 3, "infinity": true}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": -1.4e+308, "width": 1.4e+308, "count": 3, "infinity": true}\', 0) from tb;')

        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": -1, "infinity": false}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": -1, "infinity": false}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": -1, "infinity": false}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": 0, "infinity": true}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": 0, "infinity": true}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": 0, "infinity": true}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": true, "infinity": true}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": true, "infinity": true}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": true, "infinity": true}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": false, "infinity": true}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": false, "infinity": true}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": false, "infinity": true}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": "abc", "infinity": true}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": "abc", "infinity": true}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": "abc", "infinity": true}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": "中文", "infinity": true}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": "中文", "infinity": true}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": "中文", "infinity": true}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": abc, "infinity": true}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": abc, "infinity": true}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": abc, "infinity": true}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": 1.8e+308, "infinity": true}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": 1.8e+308, "infinity": true}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": 1.8e+308, "infinity": true}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": -1.8e+308, "infinity": true}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": -1.8e+308, "infinity": true}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": -1.8e+308, "infinity": true}\', 0) from tb;')

        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": 1, "infinity": 1}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": 1, "infinity": 1}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": 1, "infinity": 1}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": 1, "infinity": 0}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": 1, "infinity": 0}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": 1, "infinity": 0}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": 1, "infinity": -1.5}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": 1, "infinity": -1.5}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": 1, "infinity": -1.5}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": 1, "infinity": 1.8e+308}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": 1, "infinity": 1.8e+308}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": 1, "infinity": 1.8e+308}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": 1, "infinity": "abc"}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": 1, "infinity": "abc"}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": 1, "infinity": "abc"}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": 1, "infinity": "中文"}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": 1, "infinity": "中文"}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": 1, "infinity": "中文"}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": 1, "infinity": abc}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": 1, "infinity": abc}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": 1, "infinity": abc}\', 0) from tb;')

        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"begin": 0, "width": 1, "count": 1, "infinity": true}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"begin": 0, "width": 1, "count": 1, "infinity": true}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"begin": 0, "width": 1, "count": 1, "infinity": true}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "factor": 1, "count": 1, "infinity": true}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "factor": 1, "count": 1, "infinity": true}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "factor": 1, "count": 1, "infinity": true}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "factor": 1, "cnt": 1, "infinity": true}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "factor": 1, "cnt": 1, "infinity": true}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "factor": 1, "cnt": 1, "infinity": true}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": 1, "inf": true}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": 1, "inf": true}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{"start": 0, "width": 1, "count": 1, "inf": true}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{start: 0, width: 1, count: 1, infinity: true}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{start: 0, width: 1, count: 1, infinity: true}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'{start: 0, width: 1, count: 1, infinity: true}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'[ 0, 1, 1, true]\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'[ 0, 1, 1, true]\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'linear_bin\', \'[ 0, 1, 1, true]\', 0) from tb;')

        ## log_bin ##
        #INTEGER
        tdSql.query('select histogram(col_tinyint, \'log_bin\', \'{"start": 1, "factor": 3, "count": 6, "infinity": false}\', 0) from stb;')
        tdSql.checkRows(6);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:9]:6");
        tdSql.checkData(2, 0, "(9:27]:3");
        tdSql.checkData(3, 0, "(27:81]:0");
        tdSql.checkData(4, 0, "(81:243]:1");
        tdSql.checkData(5, 0, "(243:729]:0");
        tdSql.query('select histogram(col_tinyint, \'log_bin\', \'{"start": 1, "factor": 3, "count": 6, "infinity": false}\', 0) from ctb;')
        tdSql.checkRows(6);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:9]:6");
        tdSql.checkData(2, 0, "(9:27]:3");
        tdSql.checkData(3, 0, "(27:81]:0");
        tdSql.checkData(4, 0, "(81:243]:1");
        tdSql.checkData(5, 0, "(243:729]:0");
        tdSql.query('select histogram(col_tinyint, \'log_bin\', \'{"start": 1, "factor": 3, "count": 6, "infinity": false}\', 0) from tb;')
        tdSql.checkRows(6);
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:9]:6");
        tdSql.checkData(2, 0, "(9:27]:3");
        tdSql.checkData(3, 0, "(27:81]:0");
        tdSql.checkData(4, 0, "(81:243]:1");
        tdSql.checkData(5, 0, "(243:729]:0");

        tdSql.query('select histogram(col_tinyint, \'log_bin\', \'{"start": -1.0, "factor": 3.0, "count": 6, "infinity": false}\', 0) from stb;')
        tdSql.checkRows(6);
        tdSql.checkData(0, 0, "(-3:-1]:1");
        tdSql.checkData(1, 0, "(-9:-3]:0");
        tdSql.checkData(2, 0, "(-27:-9]:1");
        tdSql.checkData(3, 0, "(-81:-27]:0");
        tdSql.checkData(4, 0, "(-243:-81]:0");
        tdSql.checkData(5, 0, "(-729:-243]:0");
        tdSql.query('select histogram(col_tinyint, \'log_bin\', \'{"start": -1.0, "factor": 3.0, "count": 6, "infinity": false}\', 0) from ctb;')
        tdSql.checkRows(6);
        tdSql.checkData(0, 0, "(-3:-1]:1");
        tdSql.checkData(1, 0, "(-9:-3]:0");
        tdSql.checkData(2, 0, "(-27:-9]:1");
        tdSql.checkData(3, 0, "(-81:-27]:0");
        tdSql.checkData(4, 0, "(-243:-81]:0");
        tdSql.checkData(5, 0, "(-729:-243]:0");
        tdSql.query('select histogram(col_tinyint, \'log_bin\', \'{"start": -1.0, "factor": 3.0, "count": 6, "infinity": false}\', 0) from tb;')
        tdSql.checkRows(6);
        tdSql.checkData(0, 0, "(-3:-1]:1");
        tdSql.checkData(1, 0, "(-9:-3]:0");
        tdSql.checkData(2, 0, "(-27:-9]:1");
        tdSql.checkData(3, 0, "(-81:-27]:0");
        tdSql.checkData(4, 0, "(-243:-81]:0");
        tdSql.checkData(5, 0, "(-729:-243]:0");

        tdSql.query('select histogram(col_tinyint, \'log_bin\', \'{"start": 10, "factor": 0.5, "count": 6, "infinity": false}\', 0) from stb;')
        tdSql.checkRows(6);
        tdSql.checkData(0, 0, "(5:10]:5");
        tdSql.checkData(1, 0, "(2.5:5]:3");
        tdSql.checkData(2, 0, "(1.25:2.5]:1");
        tdSql.checkData(3, 0, "(0.625:1.25]:1");
        tdSql.checkData(4, 0, "(0.3125:0.625]:0");
        tdSql.checkData(5, 0, "(0.15625:0.3125]:0");
        tdSql.query('select histogram(col_tinyint, \'log_bin\', \'{"start": 10, "factor": 0.5, "count": 6, "infinity": false}\', 0) from ctb;')
        tdSql.checkRows(6);
        tdSql.checkData(0, 0, "(5:10]:5");
        tdSql.checkData(1, 0, "(2.5:5]:3");
        tdSql.checkData(2, 0, "(1.25:2.5]:1");
        tdSql.checkData(3, 0, "(0.625:1.25]:1");
        tdSql.checkData(4, 0, "(0.3125:0.625]:0");
        tdSql.checkData(5, 0, "(0.15625:0.3125]:0");
        tdSql.query('select histogram(col_tinyint, \'log_bin\', \'{"start": 10, "factor": 0.5, "count": 6, "infinity": false}\', 0) from tb;')
        tdSql.checkRows(6);
        tdSql.checkData(0, 0, "(5:10]:5");
        tdSql.checkData(1, 0, "(2.5:5]:3");
        tdSql.checkData(2, 0, "(1.25:2.5]:1");
        tdSql.checkData(3, 0, "(0.625:1.25]:1");
        tdSql.checkData(4, 0, "(0.3125:0.625]:0");
        tdSql.checkData(5, 0, "(0.15625:0.3125]:0");

        tdSql.query('select histogram(col_tinyint, \'log_bin\', \'{"start": -10, "factor": 0.5, "count": 6, "infinity": false}\', 0) from stb;')
        tdSql.checkRows(6);
        tdSql.checkData(0, 0, "(-10:-5]:1");
        tdSql.checkData(1, 0, "(-5:-2.5]:0");
        tdSql.checkData(2, 0, "(-2.5:-1.25]:0");
        tdSql.checkData(3, 0, "(-1.25:-0.625]:1");
        tdSql.checkData(4, 0, "(-0.625:-0.3125]:0");
        tdSql.checkData(5, 0, "(-0.3125:-0.15625]:0");
        tdSql.query('select histogram(col_tinyint, \'log_bin\', \'{"start": -10, "factor": 0.5, "count": 6, "infinity": false}\', 0) from ctb;')
        tdSql.checkRows(6);
        tdSql.checkData(0, 0, "(-10:-5]:1");
        tdSql.checkData(1, 0, "(-5:-2.5]:0");
        tdSql.checkData(2, 0, "(-2.5:-1.25]:0");
        tdSql.checkData(3, 0, "(-1.25:-0.625]:1");
        tdSql.checkData(4, 0, "(-0.625:-0.3125]:0");
        tdSql.checkData(5, 0, "(-0.3125:-0.15625]:0");
        tdSql.query('select histogram(col_tinyint, \'log_bin\', \'{"start": -10, "factor": 0.5, "count": 6, "infinity": false}\', 0) from tb;')
        tdSql.checkRows(6);
        tdSql.checkData(0, 0, "(-10:-5]:1");
        tdSql.checkData(1, 0, "(-5:-2.5]:0");
        tdSql.checkData(2, 0, "(-2.5:-1.25]:0");
        tdSql.checkData(3, 0, "(-1.25:-0.625]:1");
        tdSql.checkData(4, 0, "(-0.625:-0.3125]:0");
        tdSql.checkData(5, 0, "(-0.3125:-0.15625]:0");

        tdSql.query('select histogram(col_tinyint, \'log_bin\', \'{"start": 2, "factor": 1.5, "count": 6, "infinity": false}\', 0) from stb;')
        tdSql.checkRows(6);
        tdSql.checkData(0, 0, "(2:3]:1");
        tdSql.checkData(1, 0, "(3:4.5]:1");
        tdSql.checkData(2, 0, "(4.5:6.75]:2");
        tdSql.checkData(3, 0, "(6.75:10.125]:4");
        tdSql.checkData(4, 0, "(10.125:15.1875]:1");
        tdSql.checkData(5, 0, "(15.1875:22.7812]:1");
        tdSql.query('select histogram(col_tinyint, \'log_bin\', \'{"start": 2, "factor": 1.5, "count": 6, "infinity": false}\', 0) from ctb;')
        tdSql.checkRows(6);
        tdSql.checkData(0, 0, "(2:3]:1");
        tdSql.checkData(1, 0, "(3:4.5]:1");
        tdSql.checkData(2, 0, "(4.5:6.75]:2");
        tdSql.checkData(3, 0, "(6.75:10.125]:4");
        tdSql.checkData(4, 0, "(10.125:15.1875]:1");
        tdSql.checkData(5, 0, "(15.1875:22.7812]:1");
        tdSql.query('select histogram(col_tinyint, \'log_bin\', \'{"start": 2, "factor": 1.5, "count": 6, "infinity": false}\', 0) from tb;')
        tdSql.checkRows(6);
        tdSql.checkData(0, 0, "(2:3]:1");
        tdSql.checkData(1, 0, "(3:4.5]:1");
        tdSql.checkData(2, 0, "(4.5:6.75]:2");
        tdSql.checkData(3, 0, "(6.75:10.125]:4");
        tdSql.checkData(4, 0, "(10.125:15.1875]:1");
        tdSql.checkData(5, 0, "(15.1875:22.7812]:1");


        tdSql.query('select histogram(col_tinyint, \'log_bin\', \'{"start": 3.2, "factor": 0.5, "count": 1.9999, "infinity": false}\', 0) from stb;')
        tdSql.checkRows(1);
        tdSql.checkData(0, 0, "(1.6:3.2]:2");
        tdSql.query('select histogram(col_tinyint, \'log_bin\', \'{"start": 3.2, "factor": 0.5, "count": 1.9999, "infinity": false}\', 0) from ctb;')
        tdSql.checkRows(1);
        tdSql.checkData(0, 0, "(1.6:3.2]:2");
        tdSql.query('select histogram(col_tinyint, \'log_bin\', \'{"start": 3.2, "factor": 0.5, "count": 1.9999, "infinity": false}\', 0) from tb;')
        tdSql.checkRows(1);
        tdSql.checkData(0, 0, "(1.6:3.2]:2");

        tdSql.query('select histogram(col_tinyint, \'log_bin\', \'{"start": 3.2, "factor": 0.5, "count": 1.99999999999999999, "infinity": false}\', 0) from stb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1.6:3.2]:2");
        tdSql.checkData(1, 0, "(0.8:1.6]:1");
        tdSql.query('select histogram(col_tinyint, \'log_bin\', \'{"start": 3.2, "factor": 0.5, "count": 1.99999999999999999, "infinity": false}\', 0) from ctb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1.6:3.2]:2");
        tdSql.checkData(1, 0, "(0.8:1.6]:1");
        tdSql.query('select histogram(col_tinyint, \'log_bin\', \'{"start": 3.2, "factor": 0.5, "count": 1.99999999999999999, "infinity": false}\', 0) from tb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1.6:3.2]:2");
        tdSql.checkData(1, 0, "(0.8:1.6]:1");

        tdSql.query('select histogram(col_tinyint, \'log_bin\', \'{"start": 1, "factor": 5, "count": 3, "infinity": true}\', 0) from stb;')
        tdSql.checkRows(5);
        tdSql.checkData(0, 0, "(-1.79769e+308:1]:3");
        tdSql.checkData(1, 0, "(1:5]:4");
        tdSql.checkData(2, 0, "(5:25]:7");
        tdSql.checkData(3, 0, "(25:125]:1");
        tdSql.checkData(4, 0, "(125:1.79769e+308]:0");
        tdSql.query('select histogram(col_tinyint, \'log_bin\', \'{"start": 1, "factor": 5, "count": 3, "infinity": true}\', 0) from ctb;')
        tdSql.checkRows(5);
        tdSql.checkData(0, 0, "(-1.79769e+308:1]:3");
        tdSql.checkData(1, 0, "(1:5]:4");
        tdSql.checkData(2, 0, "(5:25]:7");
        tdSql.checkData(3, 0, "(25:125]:1");
        tdSql.checkData(4, 0, "(125:1.79769e+308]:0");
        tdSql.query('select histogram(col_tinyint, \'log_bin\', \'{"start": 1, "factor": 5, "count": 3, "infinity": true}\', 0) from tb;')
        tdSql.checkRows(5);
        tdSql.checkData(0, 0, "(-1.79769e+308:1]:3");
        tdSql.checkData(1, 0, "(1:5]:4");
        tdSql.checkData(2, 0, "(5:25]:7");
        tdSql.checkData(3, 0, "(25:125]:1");
        tdSql.checkData(4, 0, "(125:1.79769e+308]:0");

        tdSql.query('select histogram(col_tinyint, \'log_bin\', \'{"start": 0.2e+308, "factor": 3.14, "count": 1, "infinity": true}\', 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1.79769e+308:2e+307]:15");
        tdSql.checkData(1, 0, "(2e+307:6.28e+307]:0");
        tdSql.checkData(2, 0, "(6.28e+307:1.79769e+308]:0");
        tdSql.query('select histogram(col_tinyint, \'log_bin\', \'{"start": 0.2e+308, "factor": 3.14, "count": 1, "infinity": true}\', 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1.79769e+308:2e+307]:15");
        tdSql.checkData(1, 0, "(2e+307:6.28e+307]:0");
        tdSql.checkData(2, 0, "(6.28e+307:1.79769e+308]:0");
        tdSql.query('select histogram(col_tinyint, \'log_bin\', \'{"start": 0.2e+308, "factor": 3.14, "count": 1, "infinity": true}\', 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1.79769e+308:2e+307]:15");
        tdSql.checkData(1, 0, "(2e+307:6.28e+307]:0");
        tdSql.checkData(2, 0, "(6.28e+307:1.79769e+308]:0");

        tdSql.query('select histogram(col_tinyint, \'log_bin\', \'{"start": -2, "factor": 3, "count": 3, "infinity": true}\', 0) from stb;')
        tdSql.checkRows(5);
        tdSql.checkData(0, 0, "(-2:1.79769e+308]:14");
        tdSql.checkData(1, 0, "(-6:-2]:0");
        tdSql.checkData(2, 0, "(-18:-6]:1");
        tdSql.checkData(3, 0, "(-54:-18]:0");
        tdSql.checkData(4, 0, "(-1.79769e+308:-54]:0");
        tdSql.query('select histogram(col_tinyint, \'log_bin\', \'{"start": -2, "factor": 3, "count": 3, "infinity": true}\', 0) from ctb;')
        tdSql.checkRows(5);
        tdSql.checkData(0, 0, "(-2:1.79769e+308]:14");
        tdSql.checkData(1, 0, "(-6:-2]:0");
        tdSql.checkData(2, 0, "(-18:-6]:1");
        tdSql.checkData(3, 0, "(-54:-18]:0");
        tdSql.checkData(4, 0, "(-1.79769e+308:-54]:0");
        tdSql.query('select histogram(col_tinyint, \'log_bin\', \'{"start": -2, "factor": 3, "count": 3, "infinity": true}\', 0) from tb;')
        tdSql.checkRows(5);
        tdSql.checkData(0, 0, "(-2:1.79769e+308]:14");
        tdSql.checkData(1, 0, "(-6:-2]:0");
        tdSql.checkData(2, 0, "(-18:-6]:1");
        tdSql.checkData(3, 0, "(-54:-18]:0");
        tdSql.checkData(4, 0, "(-1.79769e+308:-54]:0");

        tdSql.query('select histogram(col_tinyint, \'log_bin\', \'{"start": 10, "factor": 0.5, "count": 3, "infinity": true}\', 0) from stb;')
        tdSql.checkRows(5);
        tdSql.checkData(0, 0, "(10:1.79769e+308]:3");
        tdSql.checkData(1, 0, "(5:10]:5");
        tdSql.checkData(2, 0, "(2.5:5]:3");
        tdSql.checkData(3, 0, "(1.25:2.5]:1");
        tdSql.checkData(4, 0, "(-1.79769e+308:1.25]:3");
        tdSql.query('select histogram(col_tinyint, \'log_bin\', \'{"start": 10, "factor": 0.5, "count": 3, "infinity": true}\', 0) from ctb;')
        tdSql.checkRows(5);
        tdSql.checkData(0, 0, "(10:1.79769e+308]:3");
        tdSql.checkData(1, 0, "(5:10]:5");
        tdSql.checkData(2, 0, "(2.5:5]:3");
        tdSql.checkData(3, 0, "(1.25:2.5]:1");
        tdSql.checkData(4, 0, "(-1.79769e+308:1.25]:3");
        tdSql.query('select histogram(col_tinyint, \'log_bin\', \'{"start": 10, "factor": 0.5, "count": 3, "infinity": true}\', 0) from tb;')
        tdSql.checkRows(5);
        tdSql.checkData(0, 0, "(10:1.79769e+308]:3");
        tdSql.checkData(1, 0, "(5:10]:5");
        tdSql.checkData(2, 0, "(2.5:5]:3");
        tdSql.checkData(3, 0, "(1.25:2.5]:1");
        tdSql.checkData(4, 0, "(-1.79769e+308:1.25]:3");

        #ERROR CASE
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": true, "factor": 5, "count": 5, "infinity": false}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": true, "factor": 5, "count": 5, "infinity": false}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": true, "factor": 5, "count": 5, "infinity": false}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": false, "factor": 5, "count": 5, "infinity": false}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": false, "factor": 5, "count": 5, "infinity": false}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": false, "factor": 5, "count": 5, "infinity": false}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": "abc", "factor": 5, "count": 5, "infinity": false}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": "abc", "factor": 5, "count": 5, "infinity": false}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": "abc", "factor": 5, "count": 5, "infinity": false}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": "中文", "factor": 5, "count": 5, "infinity": false}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": "中文", "factor": 5, "count": 5, "infinity": false}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": "中文", "factor": 5, "count": 5, "infinity": false}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": abc, "factor": 5, "count": 5, "infinity": false}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": abc, "factor": 5, "count": 5, "infinity": false}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": abc, "factor": 5, "count": 5, "infinity": false}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": -1.80e+308, "factor": 5, "count": 5, "infinity": false}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": -1.80e+308, "factor": 5, "count": 5, "infinity": false}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": -1.80e+308, "factor": 5, "count": 5, "infinity": false}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 1.80e+308, "factor": 5, "count": 5, "infinity": false}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 1.80e+308, "factor": 5, "count": 5, "infinity": false}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 1.80e+308, "factor": 5, "count": 5, "infinity": false}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 5, "count": 5, "infinity": false}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 5, "count": 5, "infinity": false}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 5, "count": 5, "infinity": false}\', 0) from tb;')

        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 1, "factor": true, "count": 5, "infinity": false}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 1, "factor": true, "count": 5, "infinity": false}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 1, "factor": true, "count": 5, "infinity": false}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 1, "factor": false, "count": 5, "infinity": false}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 1, "factor": false, "count": 5, "infinity": false}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 1, "factor": false, "count": 5, "infinity": false}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 1, "factor": "abc", "count": 5, "infinity": false}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 1, "factor": "abc", "count": 5, "infinity": false}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 1, "factor": "abc", "count": 5, "infinity": false}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 1, "factor": "中文", "count": 5, "infinity": false}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 1, "factor": "中文", "count": 5, "infinity": false}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 1, "factor": "中文", "count": 5, "infinity": false}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 1, "factor": abc, "count": 5, "infinity": false}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 1, "factor": abc, "count": 5, "infinity": false}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 1, "factor": abc, "count": 5, "infinity": false}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 1, "factor": 1.80e+308, "count": 5, "infinity": false}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 1, "factor": 1.80e+308, "count": 5, "infinity": false}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 1, "factor": 1.80e+308, "count": 5, "infinity": false}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 1, "factor": 0, "count": 5, "infinity": false}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 1, "factor": 0, "count": 5, "infinity": false}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 1, "factor": 0, "count": 5, "infinity": false}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 1, "factor": -5, "count": 5, "infinity": false}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 1, "factor": -5, "count": 5, "infinity": false}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 1, "factor": -5, "count": 5, "infinity": false}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 1, "factor": 1, "count": 5, "infinity": false}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 1, "factor": 1, "count": 5, "infinity": false}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 1, "factor": 1, "count": 5, "infinity": false}\', 0) from tb;')

        #out of range
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": -1.4e+308, "factor": 1.5, "count": 3, "infinity": false}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": -1.4e+308, "factor": 1.5, "count": 3, "infinity": false}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": -1.4e+308, "factor": 1.5, "count": 3, "infinity": false}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": -1.4e+308, "factor": 1.5, "count": 3, "infinity": true}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": -1.4e+308, "factor": 1.5, "count": 3, "infinity": true}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": -1.4e+308, "factor": 1.5, "count": 3, "infinity": true}\', 0) from tb;')

        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": -1, "infinity": false}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": -1, "infinity": false}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": -1, "infinity": false}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": 0, "infinity": true}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": 0, "infinity": true}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": 0, "infinity": true}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": true, "infinity": true}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": true, "infinity": true}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": true, "infinity": true}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": false, "infinity": true}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": false, "infinity": true}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": false, "infinity": true}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": "abc", "infinity": true}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": "abc", "infinity": true}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": "abc", "infinity": true}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": "中文", "infinity": true}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": "中文", "infinity": true}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": "中文", "infinity": true}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": abc, "infinity": true}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": abc, "infinity": true}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": abc, "infinity": true}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": 1.8e+308, "infinity": true}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": 1.8e+308, "infinity": true}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": 1.8e+308, "infinity": true}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": -1.8e+308, "infinity": true}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": -1.8e+308, "infinity": true}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": -1.8e+308, "infinity": true}\', 0) from tb;')

        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": 1, "infinity": 1}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": 1, "infinity": 1}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": 1, "infinity": 1}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": 1, "infinity": 0}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": 1, "infinity": 0}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": 1, "infinity": 0}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": 1, "infinity": -1.5}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": 1, "infinity": -1.5}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": 1, "infinity": -1.5}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": 1, "infinity": 1.8e+308}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": 1, "infinity": 1.8e+308}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": 1, "infinity": 1.8e+308}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": 1, "infinity": "abc"}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": 1, "infinity": "abc"}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": 1, "infinity": "abc"}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": 1, "infinity": "中文"}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": 1, "infinity": "中文"}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": 1, "infinity": "中文"}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": 1, "infinity": abc}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": 1, "infinity": abc}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": 1, "infinity": abc}\', 0) from tb;')

        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"begin": 0, "factor": 1, "count": 1, "infinity": true}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"begin": 0, "factor": 1, "count": 1, "infinity": true}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"begin": 0, "factor": 1, "count": 1, "infinity": true}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "width": 1, "count": 1, "infinity": true}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "width": 1, "count": 1, "infinity": true}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "width": 1, "count": 1, "infinity": true}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "cnt": 1, "infinity": true}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "cnt": 1, "infinity": true}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "cnt": 1, "infinity": true}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": 1, "inf": true}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": 1, "inf": true}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{"start": 0, "factor": 1, "count": 1, "inf": true}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{start: 0, factor: 1, count: 1, infinity: true}\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{start: 0, factor: 1, count: 1, infinity: true}\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'{start: 0, factor: 1, count: 1, infinity: true}\', 0) from tb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'[ 0, 1, 1, true]\', 0) from stb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'[ 0, 1, 1, true]\', 0) from ctb;')
        tdSql.error('select histogram(col_tinyint, \'log_bin\', \'[ 0, 1, 1, true]\', 0) from tb;')

        print("============== STEP 3: normalization  ================== ")
        ## Normalization ##
        tdSql.query('select histogram(col_smallint, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.query('select histogram(col_smallint, "user_input", "[1,3,5,7]", 1) from ctb;')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "(1:3]:0.333333");
        tdSql.checkData(1, 0, "(3:5]:0.333333");
        tdSql.checkData(2, 0, "(5:7]:0.333333");
        tdSql.query('select histogram(col_smallint, "user_input", "[1,3,5,7]", 1) from tb;')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "(1:3]:0.333333");
        tdSql.checkData(1, 0, "(3:5]:0.333333");
        tdSql.checkData(2, 0, "(5:7]:0.333333");

        tdSql.query('select histogram(col_int, "user_input", "[1,5,10]", 0) from stb;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "(1:5]:4");
        tdSql.checkData(1, 0, "(5:10]:5");
        tdSql.query('select histogram(col_int, "user_input", "[1,5,10]", 1) from ctb;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "(1:5]:0.444444");
        tdSql.checkData(1, 0, "(5:10]:0.555556");
        tdSql.query('select histogram(col_int, "user_input", "[1,5,10]", 1) from tb;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "(1:5]:0.444444");
        tdSql.checkData(1, 0, "(5:10]:0.555556");

        tdSql.query('select histogram(col_double, "user_input", "[0,5,11]", 0) from stb;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "(0:5]:4");
        tdSql.checkData(1, 0, "(5:11]:6");
        tdSql.query('select histogram(col_double, "user_input", "[0,5,11]", 1) from ctb;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "(0:5]:0.400000");
        tdSql.checkData(1, 0, "(5:11]:0.600000");
        tdSql.query('select histogram(col_double, "user_input", "[0,5,11]", 1) from tb;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "(0:5]:0.400000");
        tdSql.checkData(1, 0, "(5:11]:0.600000");

        tdSql.query('select histogram(col_bigint, \'linear_bin\', \'{"start": 1, "width": 5, "count": 2, "infinity": false}\', 0) from stb;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "(1:6]:5");
        tdSql.checkData(1, 0, "(6:11]:4");
        tdSql.query('select histogram(col_bigint, \'linear_bin\', \'{"start": 1, "width": 5, "count": 2, "infinity": false}\', 1) from ctb;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "(1:6]:0.555556");
        tdSql.checkData(1, 0, "(6:11]:0.444444");
        tdSql.query('select histogram(col_bigint, \'linear_bin\', \'{"start": 1, "width": 5, "count": 2, "infinity": false}\', 1) from tb;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "(1:6]:0.555556");
        tdSql.checkData(1, 0, "(6:11]:0.444444");

        tdSql.query('select histogram(col_int, \'linear_bin\', \'{"start": -10, "width": 5, "count": 3, "infinity": true}\', 0) from stb;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(-1.79769e+308:-10]:0");
        tdSql.checkData(1, 0, "(-10:-5]:1");
        tdSql.checkData(2, 0, "(-5:0]:1");
        tdSql.checkData(3, 0, "(0:5]:5");
        tdSql.checkData(4, 0, "(5:1.79769e+308]:8");
        tdSql.query('select histogram(col_int, \'linear_bin\', \'{"start": -10, "width": 5, "count": 3, "infinity": true}\', 1) from ctb;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(-1.79769e+308:-10]:0.000000");
        tdSql.checkData(1, 0, "(-10:-5]:0.066667");
        tdSql.checkData(2, 0, "(-5:0]:0.066667");
        tdSql.checkData(3, 0, "(0:5]:0.333333");
        tdSql.checkData(4, 0, "(5:1.79769e+308]:0.533333");
        tdSql.query('select histogram(col_int, \'linear_bin\', \'{"start": -10, "width": 5, "count": 3, "infinity": true}\', 1) from tb;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(-1.79769e+308:-10]:0.000000");
        tdSql.checkData(1, 0, "(-10:-5]:0.066667");
        tdSql.checkData(2, 0, "(-5:0]:0.066667");
        tdSql.checkData(3, 0, "(0:5]:0.333333");
        tdSql.checkData(4, 0, "(5:1.79769e+308]:0.533333");

        tdSql.query('select histogram(col_float, \'log_bin\', \'{"start": 1, "factor": 5, "count": 3, "infinity": false}\', 0) from stb;')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "(1:5]:4");
        tdSql.checkData(1, 0, "(5:25]:8");
        tdSql.checkData(2, 0, "(25:125]:1");
        tdSql.query('select histogram(col_float, \'log_bin\', \'{"start": 1, "factor": 5, "count": 3, "infinity": false}\', 1) from ctb;')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "(1:5]:0.307692");
        tdSql.checkData(1, 0, "(5:25]:0.615385");
        tdSql.checkData(2, 0, "(25:125]:0.076923");
        tdSql.query('select histogram(col_float, \'log_bin\', \'{"start": 1, "factor": 5, "count": 3, "infinity": false}\', 1) from tb;')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "(1:5]:0.307692");
        tdSql.checkData(1, 0, "(5:25]:0.615385");
        tdSql.checkData(2, 0, "(25:125]:0.076923");

        tdSql.query('select histogram(col_double, \'log_bin\', \'{"start": -0.5, "factor": 0.5, "count": 2, "infinity": false}\', 0) from stb;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "(-0.5:-0.25]:0");
        tdSql.checkData(1, 0, "(-0.25:-0.125]:0");
        tdSql.query('select histogram(col_double, \'log_bin\', \'{"start": -0.5, "factor": 0.5, "count": 2, "infinity": false}\', 1) from ctb;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "(-0.5:-0.25]:0.000000");
        tdSql.checkData(1, 0, "(-0.25:-0.125]:0.000000");
        tdSql.query('select histogram(col_double, \'log_bin\', \'{"start": -0.5, "factor": 0.5, "count": 2, "infinity": false}\', 1) from tb;')
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "(-0.5:-0.25]:0.000000");
        tdSql.checkData(1, 0, "(-0.25:-0.125]:0.000000");

        tdSql.query('select histogram(col_double, \'log_bin\', \'{"start": -0.5, "factor": 0.5, "count": 2, "infinity": true}\', 0) from stb;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "(-1.79769e+308:-0.5]:2");
        tdSql.checkData(1, 0, "(-0.5:-0.25]:0");
        tdSql.checkData(2, 0, "(-0.25:-0.125]:0");
        tdSql.checkData(3, 0, "(-0.125:1.79769e+308]:13");
        tdSql.query('select histogram(col_double, \'log_bin\', \'{"start": -0.5, "factor": 0.5, "count": 2, "infinity": true}\', 1) from ctb;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "(-1.79769e+308:-0.5]:0.133333");
        tdSql.checkData(1, 0, "(-0.5:-0.25]:0.000000");
        tdSql.checkData(2, 0, "(-0.25:-0.125]:0.000000");
        tdSql.checkData(3, 0, "(-0.125:1.79769e+308]:0.866667");
        tdSql.query('select histogram(col_double, \'log_bin\', \'{"start": -0.5, "factor": 0.5, "count": 2, "infinity": true}\', 1) from tb;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "(-1.79769e+308:-0.5]:0.133333");
        tdSql.checkData(1, 0, "(-0.5:-0.25]:0.000000");
        tdSql.checkData(2, 0, "(-0.25:-0.125]:0.000000");
        tdSql.checkData(3, 0, "(-0.125:1.79769e+308]:0.866667");

        #ERROR CASE
        tdSql.error('select histogram(col_smallint, "user_input", "[1,3,5,7]", -10) from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 2) from ctb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 3.14) from tb;')

        tdSql.error('select histogram(col_bigint, \'linear_bin\', \'{"start": 1, "width": 5, "count": 2, "infinity": false}\', true) from stb;')
        tdSql.error('select histogram(col_bigint, \'linear_bin\', \'{"start": 1, "width": 5, "count": 2, "infinity": false}\', false) from ctb;')

        tdSql.error('select histogram(col_double, \'log_bin\', \'{"start": -0.5, "factor": 0.5, "count": 2, "infinity": true}\', "abc") from tb;')
        tdSql.error('select histogram(col_double, \'log_bin\', \'{"start": -0.5, "factor": 0.5, "count": 2, "infinity": true}\', abc) from tb;')

        ## Combinations ##
        #select distinct func(col_name)
        tdSql.error('select distinct histogram(col_tinyint, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select distinct histogram(col_smallint, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select distinct histogram(col_int, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select distinct histogram(col_bigint, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select distinct histogram(col_float, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select distinct histogram(col_double, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select distinct histogram(col_bool, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select distinct histogram(col_timestamp, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select distinct histogram(col_nchar, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select distinct histogram(col_binary, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select distinct histogram(tag_tinyint, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select distinct histogram(tag_smallint, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select distinct histogram(tag_int, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select distinct histogram(tag_bigint, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select distinct histogram(tag_float, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select distinct histogram(tag_double, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select distinct histogram(tag_bool, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select distinct histogram(tag_timestamp, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select distinct histogram(tag_nchar, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select distinct histogram(tag_binary, "user_input", "[1,3,5,7]", 0) from stb;')

        tdSql.error('select histogram(*, "user_input", "[1,3,5,7]", 0) from stb;')

        #select func(col_name arith_oper xxx)
        tdSql.error('select histogram(col_int + 1, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select histogram(col_int - 1, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select histogram(col_int * 2.0, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select histogram(col_int / 2.0, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select histogram(col_int % 2.0, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select histogram(col_timestamp + now, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select histogram(col_int + col_bigint, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select histogram(col_int - col_bigint, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select histogram(col_int * col_bigint, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select histogram(col_int / col_bigint, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select histogram(col_int % col_bigint, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select histogram(col_int + pow(1,2), "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select histogram(col_int - abs(-100), "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select histogram(col_int * round(col_float), "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select histogram(col_int / ceil(1.5), "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select histogram(col_int % floor(col_double), "user_input", "[1,3,5,7]", 0) from stb;')

        #select func() arith_oper xxx
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0) + 1 from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0) - 1 from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0) * 1 from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0) / 1 from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0) % 1 from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0) + col_double from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0) - col_double from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0) * col_double from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0) / col_double from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0) % col_double from stb;')

        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0) + abs(col_double) from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0) - ceil(col_double) from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0) * floor(col_double) from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0) / round(col_double) from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0) % acos(col_double) from stb;')

        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0) + max(col_double) from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0) - min(col_double) from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0) * first(col_double) from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0) / last(col_double) from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0) % top(col_double, 1) from stb;')

        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0) + sum(col_double) from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0) - avg(col_double) from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0) * count(col_double) from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0) / stddev(col_double) from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0) % twa(col_double) from stb;')

        #select func(),xxx
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0),col_tinyint from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0),col_smallint from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0),col_int from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0),col_bigint from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0),col_timstamp from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0),col_bool from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0),col_float from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0),col_double from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0),col_binary from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0),col_nchar from stb;')

        tdSql.error('select histogram(tag_int, "user_input", "[1,3,5,7]", 0),tag_tinyint from stb;')
        tdSql.error('select histogram(tag_int, "user_input", "[1,3,5,7]", 0),tag_smallint from stb;')
        tdSql.error('select histogram(tag_int, "user_input", "[1,3,5,7]", 0),tag_int from stb;')
        tdSql.error('select histogram(tag_int, "user_input", "[1,3,5,7]", 0),tag_bigint from stb;')
        tdSql.error('select histogram(tag_int, "user_input", "[1,3,5,7]", 0),tag_timstamp from stb;')
        tdSql.error('select histogram(tag_int, "user_input", "[1,3,5,7]", 0),tag_bool from stb;')
        tdSql.error('select histogram(tag_int, "user_input", "[1,3,5,7]", 0),tag_float from stb;')
        tdSql.error('select histogram(tag_int, "user_input", "[1,3,5,7]", 0),tag_double from stb;')
        tdSql.error('select histogram(tag_int, "user_input", "[1,3,5,7]", 0),tag_binary from stb;')
        tdSql.error('select histogram(tag_int, "user_input", "[1,3,5,7]", 0),tag_nchar from stb;')

        tdSql.error('select histogram(tag_int, "user_input", "[1,3,5,7]", 0),tag_tinyint from stb;')
        tdSql.error('select histogram(tag_int, "user_input", "[1,3,5,7]", 0),tag_smallint from stb;')
        tdSql.error('select histogram(tag_int, "user_input", "[1,3,5,7]", 0),tag_int from stb;')
        tdSql.error('select histogram(tag_int, "user_input", "[1,3,5,7]", 0),tag_bigint from stb;')
        tdSql.error('select histogram(tag_int, "user_input", "[1,3,5,7]", 0),tag_timstamp from stb;')
        tdSql.error('select histogram(tag_int, "user_input", "[1,3,5,7]", 0),tag_bool from stb;')
        tdSql.error('select histogram(tag_int, "user_input", "[1,3,5,7]", 0),tag_float from stb;')
        tdSql.error('select histogram(tag_int, "user_input", "[1,3,5,7]", 0),tag_double from stb;')
        tdSql.error('select histogram(tag_int, "user_input", "[1,3,5,7]", 0),tag_binary from stb;')
        tdSql.error('select histogram(tag_int, "user_input", "[1,3,5,7]", 0),tag_nchar from stb;')

        tdSql.error('select histogram(tag_int, "user_input", "[1,3,5,7]", 0),ts from stb;')
        tdSql.error('select histogram(tag_int, "user_input", "[1,3,5,7]", 0),tbname from stb;')
        tdSql.error('select histogram(tag_int, "user_input", "[1,3,5,7]", 0),_c0 from stb;')
        tdSql.error('select histogram(tag_int, "user_input", "[1,3,5,7]", 0),_C0 from stb;')

        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0),abs(col_double) from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0),ceil(col_double) from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0),floor(col_double) from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0),round(col_double) from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0),acos(col_double) from stb;')

        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0),max(col_double) from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0),min(col_double) from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0),first(col_double) from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0),last(col_double) from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0),top(col_double, 1) from stb;')

        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0),sum(col_double) from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0),avg(col_double) from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0),count(col_double) from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0),stddev(col_double) from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0),twa(col_double) from stb;')

        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0),histogram(col_int, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0),histogram(col_int, "linear_bin", \'{"start": -1, "width":5, "count":5, "infinity":false}\', 0) from stb;')
        tdSql.error('select histogram(col_int, "user_input", "[1,3,5,7]", 0),histogram(col_int, "log_bin", \'{"start": 10, "factor":0.5, "count":5, "infinity":false}\', 0) from stb;')

        #select where condition
        tdSql.query('select histogram(col_int, "user_input", "[1,3,5,7,9,15]", 0) from tb;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.checkData(3, 0, "(7:9]:2");
        tdSql.checkData(4, 0, "(9:15]:2");
        tdSql.query('select histogram(col_int, "user_input", "[1,3,5,7,9,15]", 0) from tb where col_int > 3;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:0");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.checkData(3, 0, "(7:9]:2");
        tdSql.checkData(4, 0, "(9:15]:2");
        tdSql.query('select histogram(col_int, "user_input", "[1,3,5,7,9,15]", 0) from tb where col_int < 7;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:1");
        tdSql.checkData(3, 0, "(7:9]:0");
        tdSql.checkData(4, 0, "(9:15]:0");
        tdSql.query('select histogram(col_int, "user_input", "[1,3,5,7,9,15]", 0) from tb where col_int >= 3;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:1");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.checkData(3, 0, "(7:9]:2");
        tdSql.checkData(4, 0, "(9:15]:2");
        tdSql.query('select histogram(col_int, "user_input", "[1,3,5,7,9,15]", 0) from tb where col_int <= 7;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.checkData(3, 0, "(7:9]:0");
        tdSql.checkData(4, 0, "(9:15]:0");
        tdSql.query('select histogram(col_int, "user_input", "[1,3,5,7,9,15]", 0) from tb where col_int = 7;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:0");
        tdSql.checkData(1, 0, "(3:5]:0");
        tdSql.checkData(2, 0, "(5:7]:1");
        tdSql.checkData(3, 0, "(7:9]:0");
        tdSql.checkData(4, 0, "(9:15]:0");
        tdSql.query('select histogram(col_int, "user_input", "[1,3,5,7,9,15]", 0) from tb where col_int != 7;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:1");
        tdSql.checkData(3, 0, "(7:9]:2");
        tdSql.checkData(4, 0, "(9:15]:2");
        tdSql.query('select histogram(col_int, "user_input", "[1,3,5,7,9,15]", 0) from tb where col_int <> 7;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:1");
        tdSql.checkData(3, 0, "(7:9]:2");
        tdSql.checkData(4, 0, "(9:15]:2");
        tdSql.query('select histogram(col_int, "user_input", "[1,3,5,7,9,15]", 0) from tb where col_int > 5 and col_int <7;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:0");
        tdSql.checkData(1, 0, "(3:5]:0");
        tdSql.checkData(2, 0, "(5:7]:1");
        tdSql.checkData(3, 0, "(7:9]:0");
        tdSql.checkData(4, 0, "(9:15]:0");
        tdSql.query('select histogram(col_int, "user_input", "[1,3,5,7,9,15]", 0) from tb where col_int >= 5 and col_int <=7;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:0");
        tdSql.checkData(1, 0, "(3:5]:1");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.checkData(3, 0, "(7:9]:0");
        tdSql.checkData(4, 0, "(9:15]:0");
        tdSql.query('select histogram(col_int, "user_input", "[1,3,5,7,9,15]", 0) from tb where col_int between 5 and 7;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:0");
        tdSql.checkData(1, 0, "(3:5]:1");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.checkData(3, 0, "(7:9]:0");
        tdSql.checkData(4, 0, "(9:15]:0");

        tdSql.query('select histogram(col_tinyint, "user_input", "[1,3,5,7,9,15]", 0) from ctb;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.checkData(3, 0, "(7:9]:2");
        tdSql.checkData(4, 0, "(9:15]:2");
        tdSql.query('select histogram(col_tinyint, "user_input", "[1,3,5,7,9,15]", 0) from ctb where col_tinyint > 3;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:0");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.checkData(3, 0, "(7:9]:2");
        tdSql.checkData(4, 0, "(9:15]:2");
        tdSql.query('select histogram(col_tinyint, "user_input", "[1,3,5,7,9,15]", 0) from ctb where col_tinyint < 7;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:1");
        tdSql.checkData(3, 0, "(7:9]:0");
        tdSql.checkData(4, 0, "(9:15]:0");
        tdSql.query('select histogram(col_tinyint, "user_input", "[1,3,5,7,9,15]", 0) from ctb where col_tinyint >= 3;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:1");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.checkData(3, 0, "(7:9]:2");
        tdSql.checkData(4, 0, "(9:15]:2");
        tdSql.query('select histogram(col_tinyint, "user_input", "[1,3,5,7,9,15]", 0) from ctb where col_tinyint <= 7;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.checkData(3, 0, "(7:9]:0");
        tdSql.checkData(4, 0, "(9:15]:0");
        tdSql.query('select histogram(col_tinyint, "user_input", "[1,3,5,7,9,15]", 0) from ctb where col_tinyint = 7;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:0");
        tdSql.checkData(1, 0, "(3:5]:0");
        tdSql.checkData(2, 0, "(5:7]:1");
        tdSql.checkData(3, 0, "(7:9]:0");
        tdSql.checkData(4, 0, "(9:15]:0");
        tdSql.query('select histogram(col_tinyint, "user_input", "[1,3,5,7,9,15]", 0) from ctb where col_tinyint != 7;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:1");
        tdSql.checkData(3, 0, "(7:9]:2");
        tdSql.checkData(4, 0, "(9:15]:2");
        tdSql.query('select histogram(col_tinyint, "user_input", "[1,3,5,7,9,15]", 0) from ctb where col_tinyint <> 7;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:1");
        tdSql.checkData(3, 0, "(7:9]:2");
        tdSql.checkData(4, 0, "(9:15]:2");
        tdSql.query('select histogram(col_tinyint, "user_input", "[1,3,5,7,9,15]", 0) from ctb where col_tinyint > 5 and col_tinyint <7;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:0");
        tdSql.checkData(1, 0, "(3:5]:0");
        tdSql.checkData(2, 0, "(5:7]:1");
        tdSql.checkData(3, 0, "(7:9]:0");
        tdSql.checkData(4, 0, "(9:15]:0");
        tdSql.query('select histogram(col_tinyint, "user_input", "[1,3,5,7,9,15]", 0) from ctb where col_tinyint >= 5 and col_tinyint <=7;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:0");
        tdSql.checkData(1, 0, "(3:5]:1");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.checkData(3, 0, "(7:9]:0");
        tdSql.checkData(4, 0, "(9:15]:0");
        tdSql.query('select histogram(col_tinyint, "user_input", "[1,3,5,7,9,15]", 0) from ctb where col_tinyint between 5 and 7;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:0");
        tdSql.checkData(1, 0, "(3:5]:1");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.checkData(3, 0, "(7:9]:0");
        tdSql.checkData(4, 0, "(9:15]:0");

        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5,7,9,15]", 0) from stb;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.checkData(3, 0, "(7:9]:2");
        tdSql.checkData(4, 0, "(9:15]:2");
        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5,7,9,15]", 0) from stb where col_bigint > 3;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:0");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.checkData(3, 0, "(7:9]:2");
        tdSql.checkData(4, 0, "(9:15]:2");
        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5,7,9,15]", 0) from stb where col_bigint < 7;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:1");
        tdSql.checkData(3, 0, "(7:9]:0");
        tdSql.checkData(4, 0, "(9:15]:0");
        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5,7,9,15]", 0) from stb where col_bigint >= 3;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:1");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.checkData(3, 0, "(7:9]:2");
        tdSql.checkData(4, 0, "(9:15]:2");
        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5,7,9,15]", 0) from stb where col_bigint <= 7;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.checkData(3, 0, "(7:9]:0");
        tdSql.checkData(4, 0, "(9:15]:0");
        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5,7,9,15]", 0) from stb where col_bigint = 7;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:0");
        tdSql.checkData(1, 0, "(3:5]:0");
        tdSql.checkData(2, 0, "(5:7]:1");
        tdSql.checkData(3, 0, "(7:9]:0");
        tdSql.checkData(4, 0, "(9:15]:0");
        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5,7,9,15]", 0) from stb where col_bigint != 7;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:1");
        tdSql.checkData(3, 0, "(7:9]:2");
        tdSql.checkData(4, 0, "(9:15]:2");
        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5,7,9,15]", 0) from stb where col_bigint <> 7;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:1");
        tdSql.checkData(3, 0, "(7:9]:2");
        tdSql.checkData(4, 0, "(9:15]:2");
        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5,7,9,15]", 0) from stb where col_bigint > 5 and col_bigint <7;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:0");
        tdSql.checkData(1, 0, "(3:5]:0");
        tdSql.checkData(2, 0, "(5:7]:1");
        tdSql.checkData(3, 0, "(7:9]:0");
        tdSql.checkData(4, 0, "(9:15]:0");
        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5,7,9,15]", 0) from stb where col_bigint >= 5 and col_bigint <=7;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:0");
        tdSql.checkData(1, 0, "(3:5]:1");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.checkData(3, 0, "(7:9]:0");
        tdSql.checkData(4, 0, "(9:15]:0");
        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5,7,9,15]", 0) from stb where col_bigint between 5 and 7;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:0");
        tdSql.checkData(1, 0, "(3:5]:1");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.checkData(3, 0, "(7:9]:0");
        tdSql.checkData(4, 0, "(9:15]:0");

        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5,7,9,15]", 0) from stb;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.checkData(3, 0, "(7:9]:2");
        tdSql.checkData(4, 0, "(9:15]:2");
        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5,7,9,15]", 0) from stb where tag_bigint > 0;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.checkData(3, 0, "(7:9]:2");
        tdSql.checkData(4, 0, "(9:15]:2");
        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5,7,9,15]", 0) from stb where tag_bigint < 2;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.checkData(3, 0, "(7:9]:2");
        tdSql.checkData(4, 0, "(9:15]:2");
        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5,7,9,15]", 0) from stb where tag_bigint >= 1;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.checkData(3, 0, "(7:9]:2");
        tdSql.checkData(4, 0, "(9:15]:2");
        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5,7,9,15]", 0) from stb where tag_bigint <= 1;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.checkData(3, 0, "(7:9]:2");
        tdSql.checkData(4, 0, "(9:15]:2");
        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5,7,9,15]", 0) from stb where tag_bigint = 1;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.checkData(3, 0, "(7:9]:2");
        tdSql.checkData(4, 0, "(9:15]:2");
        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5,7,9,15]", 0) from stb where tag_bigint != 2;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.checkData(3, 0, "(7:9]:2");
        tdSql.checkData(4, 0, "(9:15]:2");
        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5,7,9,15]", 0) from stb where tag_bigint <> 2;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.checkData(3, 0, "(7:9]:2");
        tdSql.checkData(4, 0, "(9:15]:2");
        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5,7,9,15]", 0) from stb where tag_bigint > 0 and tag_bigint < 2;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.checkData(3, 0, "(7:9]:2");
        tdSql.checkData(4, 0, "(9:15]:2");
        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5,7,9,15]", 0) from stb where tag_bigint >= 1 and tag_bigint <= 1;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.checkData(3, 0, "(7:9]:2");
        tdSql.checkData(4, 0, "(9:15]:2");
        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5,7,9,15]", 0) from stb where tag_bigint between 0 and 2;')
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "(1:3]:2");
        tdSql.checkData(1, 0, "(3:5]:2");
        tdSql.checkData(2, 0, "(5:7]:2");
        tdSql.checkData(3, 0, "(7:9]:2");
        tdSql.checkData(4, 0, "(9:15]:2");

        #select session
        #select state_window
        #select interval/sliding/fill

        return
        tdSql.execute('drop database db')
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
