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
        tdSql.execute("insert into ctb values (now, -1, -1, -1, -1, -1.5, -1.5, true, 'abc', 'abc');")
        tdSql.execute("insert into ctb values (now, 1, 1, 1, 1, 1.5, 1.5, true, 'abc', 'abc');")
        tdSql.execute("insert into ctb values (now, 2, 2, 2, 2, 2.5, 2.5, true, 'abc', 'abc');")
        tdSql.execute("insert into ctb values (now, 3, 3, 3, 3, 3.5, 3.5, true, 'abc', 'abc');")
        tdSql.execute("insert into ctb values (now, 4, 4, 4, 4, 4.5, 4.5, true, 'abc', 'abc');")
        tdSql.execute("insert into ctb values (now, 5, 5, 5, 5, 5.5, 5.5, true, 'abc', 'abc');")
        tdSql.execute("insert into ctb values (now, 6, 6, 6, 6, 6.5, 6.5, true, 'abc', 'abc');")
        tdSql.execute("insert into ctb values (now, 7, 7, 7, 7, 7.5, 7.5, false, 'abc', 'abc');")
        tdSql.execute("insert into ctb values (now, 8, 8, 8, 8, 8.5, 8.5, false, 'abc', 'abc');")
        tdSql.execute("insert into ctb values (now, 9, 9, 9, 9, 9.5, 9.5, false, 'abc', 'abc');")
        tdSql.execute("insert into ctb values (now, 10, 10, 10, 10, 10.5, 10.5, false, 'abc', 'abc');")
        tdSql.execute("insert into ctb values (now, 15, 15, 15, 15, 15.5, 15.5, false, 'abc', 'abc');")
        tdSql.execute("insert into ctb values (now, 20, 20, 20, 20, 20.5, 20.5, false, 'abc', 'abc');")
        tdSql.execute("insert into ctb values (now, 99, 99, 99, 99, 99.5, 99.5, false, 'abc', 'abc');")

        tdSql.execute("insert into tb values (now, -9, -9, -9, -9, -9.5, -9.5, true, 'abc', 'abc');")
        tdSql.execute("insert into tb values (now, -1, -1, -1, -1, -1.5, -1.5, true, 'abc', 'abc');")
        tdSql.execute("insert into tb values (now, 1, 1, 1, 1, 1.5, 1.5, true, 'abc', 'abc');")
        tdSql.execute("insert into tb values (now, 2, 2, 2, 2, 2.5, 2.5, true, 'abc', 'abc');")
        tdSql.execute("insert into tb values (now, 3, 3, 3, 3, 3.5, 3.5, true, 'abc', 'abc');")
        tdSql.execute("insert into tb values (now, 4, 4, 4, 4, 4.5, 4.5, true, 'abc', 'abc');")
        tdSql.execute("insert into tb values (now, 5, 5, 5, 5, 5.5, 5.5, true, 'abc', 'abc');")
        tdSql.execute("insert into tb values (now, 6, 6, 6, 6, 6.5, 6.5, true, 'abc', 'abc');")
        tdSql.execute("insert into tb values (now, 7, 7, 7, 7, 7.5, 7.5, false, 'abc', 'abc');")
        tdSql.execute("insert into tb values (now, 8, 8, 8, 8, 8.5, 8.5, false, 'abc', 'abc');")
        tdSql.execute("insert into tb values (now, 9, 9, 9, 9, 9.5, 9.5, false, 'abc', 'abc');")
        tdSql.execute("insert into tb values (now, 10, 10, 10, 10, 10.5, 10.5, false, 'abc', 'abc');")
        tdSql.execute("insert into tb values (now, 15, 15, 15, 15, 15.5, 15.5, false, 'abc', 'abc');")
        tdSql.execute("insert into tb values (now, 20, 20, 20, 20, 20.5, 20.5, false, 'abc', 'abc');")
        tdSql.execute("insert into tb values (now, 99, 99, 99, 99, 99.5, 99.5, false, 'abc', 'abc');")

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
        #user_input
        #TINYINT
        tdSql.query('select histogram(col_tinyint, "user_input", "[1,3,5]", 0) from stb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1-3]:2");
        tdSql.checkData(1, 0, "(3-5]:2");
        tdSql.query('select histogram(col_tinyint, "user_input", "[1,3,5]", 0) from ctb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1-3]:2");
        tdSql.checkData(1, 0, "(3-5]:2");
        tdSql.query('select histogram(col_tinyint, "user_input", "[1,3,5]", 0) from tb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1-3]:2");
        tdSql.checkData(1, 0, "(3-5]:2");

        tdSql.query('select histogram(col_tinyint, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(1-3]:2");
        tdSql.checkData(1, 0, "(3-5]:2");
        tdSql.checkData(2, 0, "(5-7]:2");
        tdSql.query('select histogram(col_tinyint, "user_input", "[1,3,5,7]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(1-3]:2");
        tdSql.checkData(1, 0, "(3-5]:2");
        tdSql.checkData(2, 0, "(5-7]:2");
        tdSql.query('select histogram(col_tinyint, "user_input", "[1,3,5,7]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(1-3]:2");
        tdSql.checkData(1, 0, "(3-5]:2");
        tdSql.checkData(2, 0, "(5-7]:2");

        tdSql.query('select histogram(col_tinyint, "user_input", "[0,10,20,100]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(0-10]:10");
        tdSql.checkData(1, 0, "(10-20]:2");
        tdSql.checkData(2, 0, "(20-100]:1");
        tdSql.query('select histogram(col_tinyint, "user_input", "[0,10,20,100]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(0-10]:10");
        tdSql.checkData(1, 0, "(10-20]:2");
        tdSql.checkData(2, 0, "(20-100]:1");
        tdSql.query('select histogram(col_tinyint, "user_input", "[0,10,20,100]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(0-10]:10");
        tdSql.checkData(1, 0, "(10-20]:2");
        tdSql.checkData(2, 0, "(20-100]:1");

        tdSql.query('select histogram(col_tinyint, "user_input", "[-10,10,20,100]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-10-10]:12");
        tdSql.checkData(1, 0, "(10-20]:2");
        tdSql.checkData(2, 0, "(20-100]:1");
        tdSql.query('select histogram(col_tinyint, "user_input", "[-10,10,20,100]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-10-10]:12");
        tdSql.checkData(1, 0, "(10-20]:2");
        tdSql.checkData(2, 0, "(20-100]:1");
        tdSql.query('select histogram(col_tinyint, "user_input", "[-10,10,20,100]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-10-10]:12");
        tdSql.checkData(1, 0, "(10-20]:2");
        tdSql.checkData(2, 0, "(20-100]:1");

        tdSql.query('select histogram(col_tinyint, "user_input", "[-8.9,9.9,19.9,99.9]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-8.9-9.9]:10");
        tdSql.checkData(1, 0, "(9.9-19.9]:2");
        tdSql.checkData(2, 0, "(19.9-99.9]:2");
        tdSql.query('select histogram(col_tinyint, "user_input", "[-8.9,9.9,19.9,99.9]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-8.9-9.9]:10");
        tdSql.checkData(1, 0, "(9.9-19.9]:2");
        tdSql.checkData(2, 0, "(19.9-99.9]:2");
        tdSql.query('select histogram(col_tinyint, "user_input", "[-8.9,9.9,19.9,99.9]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-8.9-9.9]:10");
        tdSql.checkData(1, 0, "(9.9-19.9]:2");
        tdSql.checkData(2, 0, "(19.9-99.9]:2");

        tdSql.query('select histogram(col_tinyint, "user_input", "[-99999999999999,9.9,19.9,99999999999999]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1e+14-9.9]:11");
        tdSql.checkData(1, 0, "(9.9-19.9]:2");
        tdSql.checkData(2, 0, "(19.9-1e+14]:2");
        tdSql.query('select histogram(col_tinyint, "user_input", "[-99999999999999,9.9,19.9,99999999999999]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1e+14-9.9]:11");
        tdSql.checkData(1, 0, "(9.9-19.9]:2");
        tdSql.checkData(2, 0, "(19.9-1e+14]:2");
        tdSql.query('select histogram(col_tinyint, "user_input", "[-99999999999999,9.9,19.9,99999999999999]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1e+14-9.9]:11");
        tdSql.checkData(1, 0, "(9.9-19.9]:2");
        tdSql.checkData(2, 0, "(19.9-1e+14]:2");

        #SMALLINT
        tdSql.query('select histogram(col_smallint, "user_input", "[1,3,5]", 0) from stb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1-3]:2");
        tdSql.checkData(1, 0, "(3-5]:2");
        tdSql.query('select histogram(col_smallint, "user_input", "[1,3,5]", 0) from ctb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1-3]:2");
        tdSql.checkData(1, 0, "(3-5]:2");
        tdSql.query('select histogram(col_smallint, "user_input", "[1,3,5]", 0) from tb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1-3]:2");
        tdSql.checkData(1, 0, "(3-5]:2");

        tdSql.query('select histogram(col_smallint, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(1-3]:2");
        tdSql.checkData(1, 0, "(3-5]:2");
        tdSql.checkData(2, 0, "(5-7]:2");
        tdSql.query('select histogram(col_smallint, "user_input", "[1,3,5,7]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(1-3]:2");
        tdSql.checkData(1, 0, "(3-5]:2");
        tdSql.checkData(2, 0, "(5-7]:2");
        tdSql.query('select histogram(col_smallint, "user_input", "[1,3,5,7]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(1-3]:2");
        tdSql.checkData(1, 0, "(3-5]:2");
        tdSql.checkData(2, 0, "(5-7]:2");

        tdSql.query('select histogram(col_smallint, "user_input", "[0,10,20,100]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(0-10]:10");
        tdSql.checkData(1, 0, "(10-20]:2");
        tdSql.checkData(2, 0, "(20-100]:1");
        tdSql.query('select histogram(col_smallint, "user_input", "[0,10,20,100]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(0-10]:10");
        tdSql.checkData(1, 0, "(10-20]:2");
        tdSql.checkData(2, 0, "(20-100]:1");
        tdSql.query('select histogram(col_smallint, "user_input", "[0,10,20,100]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(0-10]:10");
        tdSql.checkData(1, 0, "(10-20]:2");
        tdSql.checkData(2, 0, "(20-100]:1");

        tdSql.query('select histogram(col_smallint, "user_input", "[-10,10,20,100]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-10-10]:12");
        tdSql.checkData(1, 0, "(10-20]:2");
        tdSql.checkData(2, 0, "(20-100]:1");
        tdSql.query('select histogram(col_smallint, "user_input", "[-10,10,20,100]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-10-10]:12");
        tdSql.checkData(1, 0, "(10-20]:2");
        tdSql.checkData(2, 0, "(20-100]:1");
        tdSql.query('select histogram(col_smallint, "user_input", "[-10,10,20,100]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-10-10]:12");
        tdSql.checkData(1, 0, "(10-20]:2");
        tdSql.checkData(2, 0, "(20-100]:1");

        tdSql.query('select histogram(col_smallint, "user_input", "[-8.9,9.9,19.9,99.9]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-8.9-9.9]:10");
        tdSql.checkData(1, 0, "(9.9-19.9]:2");
        tdSql.checkData(2, 0, "(19.9-99.9]:2");
        tdSql.query('select histogram(col_smallint, "user_input", "[-8.9,9.9,19.9,99.9]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-8.9-9.9]:10");
        tdSql.checkData(1, 0, "(9.9-19.9]:2");
        tdSql.checkData(2, 0, "(19.9-99.9]:2");
        tdSql.query('select histogram(col_smallint, "user_input", "[-8.9,9.9,19.9,99.9]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-8.9-9.9]:10");
        tdSql.checkData(1, 0, "(9.9-19.9]:2");
        tdSql.checkData(2, 0, "(19.9-99.9]:2");

        tdSql.query('select histogram(col_smallint, "user_input", "[-99999999999999,9.9,19.9,99999999999999]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1e+14-9.9]:11");
        tdSql.checkData(1, 0, "(9.9-19.9]:2");
        tdSql.checkData(2, 0, "(19.9-1e+14]:2");
        tdSql.query('select histogram(col_smallint, "user_input", "[-99999999999999,9.9,19.9,99999999999999]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1e+14-9.9]:11");
        tdSql.checkData(1, 0, "(9.9-19.9]:2");
        tdSql.checkData(2, 0, "(19.9-1e+14]:2");
        tdSql.query('select histogram(col_smallint, "user_input", "[-99999999999999,9.9,19.9,99999999999999]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1e+14-9.9]:11");
        tdSql.checkData(1, 0, "(9.9-19.9]:2");
        tdSql.checkData(2, 0, "(19.9-1e+14]:2");

        #INT
        tdSql.query('select histogram(col_int, "user_input", "[1,3,5]", 0) from stb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1-3]:2");
        tdSql.checkData(1, 0, "(3-5]:2");
        tdSql.query('select histogram(col_int, "user_input", "[1,3,5]", 0) from ctb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1-3]:2");
        tdSql.checkData(1, 0, "(3-5]:2");
        tdSql.query('select histogram(col_int, "user_input", "[1,3,5]", 0) from tb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1-3]:2");
        tdSql.checkData(1, 0, "(3-5]:2");

        tdSql.query('select histogram(col_int, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(1-3]:2");
        tdSql.checkData(1, 0, "(3-5]:2");
        tdSql.checkData(2, 0, "(5-7]:2");
        tdSql.query('select histogram(col_int, "user_input", "[1,3,5,7]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(1-3]:2");
        tdSql.checkData(1, 0, "(3-5]:2");
        tdSql.checkData(2, 0, "(5-7]:2");
        tdSql.query('select histogram(col_int, "user_input", "[1,3,5,7]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(1-3]:2");
        tdSql.checkData(1, 0, "(3-5]:2");
        tdSql.checkData(2, 0, "(5-7]:2");

        tdSql.query('select histogram(col_int, "user_input", "[0,10,20,100]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(0-10]:10");
        tdSql.checkData(1, 0, "(10-20]:2");
        tdSql.checkData(2, 0, "(20-100]:1");
        tdSql.query('select histogram(col_int, "user_input", "[0,10,20,100]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(0-10]:10");
        tdSql.checkData(1, 0, "(10-20]:2");
        tdSql.checkData(2, 0, "(20-100]:1");
        tdSql.query('select histogram(col_int, "user_input", "[0,10,20,100]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(0-10]:10");
        tdSql.checkData(1, 0, "(10-20]:2");
        tdSql.checkData(2, 0, "(20-100]:1");

        tdSql.query('select histogram(col_int, "user_input", "[-10,10,20,100]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-10-10]:12");
        tdSql.checkData(1, 0, "(10-20]:2");
        tdSql.checkData(2, 0, "(20-100]:1");
        tdSql.query('select histogram(col_int, "user_input", "[-10,10,20,100]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-10-10]:12");
        tdSql.checkData(1, 0, "(10-20]:2");
        tdSql.checkData(2, 0, "(20-100]:1");
        tdSql.query('select histogram(col_int, "user_input", "[-10,10,20,100]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-10-10]:12");
        tdSql.checkData(1, 0, "(10-20]:2");
        tdSql.checkData(2, 0, "(20-100]:1");

        tdSql.query('select histogram(col_int, "user_input", "[-8.9,9.9,19.9,99.9]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-8.9-9.9]:10");
        tdSql.checkData(1, 0, "(9.9-19.9]:2");
        tdSql.checkData(2, 0, "(19.9-99.9]:2");
        tdSql.query('select histogram(col_int, "user_input", "[-8.9,9.9,19.9,99.9]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-8.9-9.9]:10");
        tdSql.checkData(1, 0, "(9.9-19.9]:2");
        tdSql.checkData(2, 0, "(19.9-99.9]:2");
        tdSql.query('select histogram(col_int, "user_input", "[-8.9,9.9,19.9,99.9]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-8.9-9.9]:10");
        tdSql.checkData(1, 0, "(9.9-19.9]:2");
        tdSql.checkData(2, 0, "(19.9-99.9]:2");

        tdSql.query('select histogram(col_int, "user_input", "[-99999999999999,9.9,19.9,99999999999999]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1e+14-9.9]:11");
        tdSql.checkData(1, 0, "(9.9-19.9]:2");
        tdSql.checkData(2, 0, "(19.9-1e+14]:2");
        tdSql.query('select histogram(col_int, "user_input", "[-99999999999999,9.9,19.9,99999999999999]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1e+14-9.9]:11");
        tdSql.checkData(1, 0, "(9.9-19.9]:2");
        tdSql.checkData(2, 0, "(19.9-1e+14]:2");
        tdSql.query('select histogram(col_int, "user_input", "[-99999999999999,9.9,19.9,99999999999999]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1e+14-9.9]:11");
        tdSql.checkData(1, 0, "(9.9-19.9]:2");
        tdSql.checkData(2, 0, "(19.9-1e+14]:2");

        #BIGINT
        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5]", 0) from stb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1-3]:2");
        tdSql.checkData(1, 0, "(3-5]:2");
        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5]", 0) from ctb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1-3]:2");
        tdSql.checkData(1, 0, "(3-5]:2");
        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5]", 0) from tb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1-3]:2");
        tdSql.checkData(1, 0, "(3-5]:2");

        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(1-3]:2");
        tdSql.checkData(1, 0, "(3-5]:2");
        tdSql.checkData(2, 0, "(5-7]:2");
        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5,7]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(1-3]:2");
        tdSql.checkData(1, 0, "(3-5]:2");
        tdSql.checkData(2, 0, "(5-7]:2");
        tdSql.query('select histogram(col_bigint, "user_input", "[1,3,5,7]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(1-3]:2");
        tdSql.checkData(1, 0, "(3-5]:2");
        tdSql.checkData(2, 0, "(5-7]:2");

        tdSql.query('select histogram(col_bigint, "user_input", "[0,10,20,100]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(0-10]:10");
        tdSql.checkData(1, 0, "(10-20]:2");
        tdSql.checkData(2, 0, "(20-100]:1");
        tdSql.query('select histogram(col_bigint, "user_input", "[0,10,20,100]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(0-10]:10");
        tdSql.checkData(1, 0, "(10-20]:2");
        tdSql.checkData(2, 0, "(20-100]:1");
        tdSql.query('select histogram(col_bigint, "user_input", "[0,10,20,100]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(0-10]:10");
        tdSql.checkData(1, 0, "(10-20]:2");
        tdSql.checkData(2, 0, "(20-100]:1");

        tdSql.query('select histogram(col_bigint, "user_input", "[-10,10,20,100]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-10-10]:12");
        tdSql.checkData(1, 0, "(10-20]:2");
        tdSql.checkData(2, 0, "(20-100]:1");
        tdSql.query('select histogram(col_bigint, "user_input", "[-10,10,20,100]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-10-10]:12");
        tdSql.checkData(1, 0, "(10-20]:2");
        tdSql.checkData(2, 0, "(20-100]:1");
        tdSql.query('select histogram(col_bigint, "user_input", "[-10,10,20,100]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-10-10]:12");
        tdSql.checkData(1, 0, "(10-20]:2");
        tdSql.checkData(2, 0, "(20-100]:1");

        tdSql.query('select histogram(col_bigint, "user_input", "[-8.9,9.9,19.9,99.9]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-8.9-9.9]:10");
        tdSql.checkData(1, 0, "(9.9-19.9]:2");
        tdSql.checkData(2, 0, "(19.9-99.9]:2");
        tdSql.query('select histogram(col_bigint, "user_input", "[-8.9,9.9,19.9,99.9]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-8.9-9.9]:10");
        tdSql.checkData(1, 0, "(9.9-19.9]:2");
        tdSql.checkData(2, 0, "(19.9-99.9]:2");
        tdSql.query('select histogram(col_bigint, "user_input", "[-8.9,9.9,19.9,99.9]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-8.9-9.9]:10");
        tdSql.checkData(1, 0, "(9.9-19.9]:2");
        tdSql.checkData(2, 0, "(19.9-99.9]:2");

        tdSql.query('select histogram(col_bigint, "user_input", "[-99999999999999,9.9,19.9,99999999999999]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1e+14-9.9]:11");
        tdSql.checkData(1, 0, "(9.9-19.9]:2");
        tdSql.checkData(2, 0, "(19.9-1e+14]:2");
        tdSql.query('select histogram(col_bigint, "user_input", "[-99999999999999,9.9,19.9,99999999999999]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1e+14-9.9]:11");
        tdSql.checkData(1, 0, "(9.9-19.9]:2");
        tdSql.checkData(2, 0, "(19.9-1e+14]:2");
        tdSql.query('select histogram(col_bigint, "user_input", "[-99999999999999,9.9,19.9,99999999999999]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1e+14-9.9]:11");
        tdSql.checkData(1, 0, "(9.9-19.9]:2");
        tdSql.checkData(2, 0, "(19.9-1e+14]:2");

        #FLOAT
        tdSql.query('select histogram(col_float, "user_input", "[1,3,5]", 0) from stb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1-3]:2");
        tdSql.checkData(1, 0, "(3-5]:2");
        tdSql.query('select histogram(col_float, "user_input", "[1,3,5]", 0) from ctb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1-3]:2");
        tdSql.checkData(1, 0, "(3-5]:2");
        tdSql.query('select histogram(col_float, "user_input", "[1,3,5]", 0) from tb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1-3]:2");
        tdSql.checkData(1, 0, "(3-5]:2");

        tdSql.query('select histogram(col_float, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(1-3]:2");
        tdSql.checkData(1, 0, "(3-5]:2");
        tdSql.checkData(2, 0, "(5-7]:2");
        tdSql.query('select histogram(col_float, "user_input", "[1,3,5,7]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(1-3]:2");
        tdSql.checkData(1, 0, "(3-5]:2");
        tdSql.checkData(2, 0, "(5-7]:2");
        tdSql.query('select histogram(col_float, "user_input", "[1,3,5,7]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(1-3]:2");
        tdSql.checkData(1, 0, "(3-5]:2");
        tdSql.checkData(2, 0, "(5-7]:2");

        tdSql.query('select histogram(col_float, "user_input", "[0,10,20,100]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(0-10]:9");
        tdSql.checkData(1, 0, "(10-20]:2");
        tdSql.checkData(2, 0, "(20-100]:2");
        tdSql.query('select histogram(col_float, "user_input", "[0,10,20,100]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(0-10]:9");
        tdSql.checkData(1, 0, "(10-20]:2");
        tdSql.checkData(2, 0, "(20-100]:2");
        tdSql.query('select histogram(col_float, "user_input", "[0,10,20,100]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(0-10]:9");
        tdSql.checkData(1, 0, "(10-20]:2");
        tdSql.checkData(2, 0, "(20-100]:2");

        tdSql.query('select histogram(col_float, "user_input", "[-10,10,20,100]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-10-10]:11");
        tdSql.checkData(1, 0, "(10-20]:2");
        tdSql.checkData(2, 0, "(20-100]:2");
        tdSql.query('select histogram(col_float, "user_input", "[-10,10,20,100]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-10-10]:11");
        tdSql.checkData(1, 0, "(10-20]:2");
        tdSql.checkData(2, 0, "(20-100]:2");
        tdSql.query('select histogram(col_float, "user_input", "[-10,10,20,100]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-10-10]:11");
        tdSql.checkData(1, 0, "(10-20]:2");
        tdSql.checkData(2, 0, "(20-100]:2");

        tdSql.query('select histogram(col_float, "user_input", "[-9.4,9.6,20.4,99.9]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-9.4-9.6]:10");
        tdSql.checkData(1, 0, "(9.6-20.4]:2");
        tdSql.checkData(2, 0, "(20.4-99.9]:2");
        tdSql.query('select histogram(col_float, "user_input", "[-9.4,9.6,20.4,99.9]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-9.4-9.6]:10");
        tdSql.checkData(1, 0, "(9.6-20.4]:2");
        tdSql.checkData(2, 0, "(20.4-99.9]:2");
        tdSql.query('select histogram(col_float, "user_input", "[-9.4,9.6,20.4,99.9]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-9.4-9.6]:10");
        tdSql.checkData(1, 0, "(9.6-20.4]:2");
        tdSql.checkData(2, 0, "(20.4-99.9]:2");


        tdSql.query('select histogram(col_float, "user_input", "[-99999999999999,9.9,19.9,99999999999999]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1e+14-9.9]:11");
        tdSql.checkData(1, 0, "(9.9-19.9]:2");
        tdSql.checkData(2, 0, "(19.9-1e+14]:2");
        tdSql.query('select histogram(col_float, "user_input", "[-99999999999999,9.9,19.9,99999999999999]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1e+14-9.9]:11");
        tdSql.checkData(1, 0, "(9.9-19.9]:2");
        tdSql.checkData(2, 0, "(19.9-1e+14]:2");
        tdSql.query('select histogram(col_float, "user_input", "[-99999999999999,9.9,19.9,99999999999999]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1e+14-9.9]:11");
        tdSql.checkData(1, 0, "(9.9-19.9]:2");
        tdSql.checkData(2, 0, "(19.9-1e+14]:2");

        #DOUBLE
        tdSql.query('select histogram(col_double, "user_input", "[1,3,5]", 0) from stb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1-3]:2");
        tdSql.checkData(1, 0, "(3-5]:2");
        tdSql.query('select histogram(col_double, "user_input", "[1,3,5]", 0) from ctb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1-3]:2");
        tdSql.checkData(1, 0, "(3-5]:2");
        tdSql.query('select histogram(col_double, "user_input", "[1,3,5]", 0) from tb;')
        tdSql.checkRows(2);
        tdSql.checkData(0, 0, "(1-3]:2");
        tdSql.checkData(1, 0, "(3-5]:2");

        tdSql.query('select histogram(col_double, "user_input", "[1,3,5,7]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(1-3]:2");
        tdSql.checkData(1, 0, "(3-5]:2");
        tdSql.checkData(2, 0, "(5-7]:2");
        tdSql.query('select histogram(col_double, "user_input", "[1,3,5,7]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(1-3]:2");
        tdSql.checkData(1, 0, "(3-5]:2");
        tdSql.checkData(2, 0, "(5-7]:2");
        tdSql.query('select histogram(col_double, "user_input", "[1,3,5,7]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(1-3]:2");
        tdSql.checkData(1, 0, "(3-5]:2");
        tdSql.checkData(2, 0, "(5-7]:2");

        tdSql.query('select histogram(col_double, "user_input", "[0,10,20,100]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(0-10]:9");
        tdSql.checkData(1, 0, "(10-20]:2");
        tdSql.checkData(2, 0, "(20-100]:2");
        tdSql.query('select histogram(col_double, "user_input", "[0,10,20,100]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(0-10]:9");
        tdSql.checkData(1, 0, "(10-20]:2");
        tdSql.checkData(2, 0, "(20-100]:2");
        tdSql.query('select histogram(col_double, "user_input", "[0,10,20,100]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(0-10]:9");
        tdSql.checkData(1, 0, "(10-20]:2");
        tdSql.checkData(2, 0, "(20-100]:2");

        tdSql.query('select histogram(col_double, "user_input", "[-10,10,20,100]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-10-10]:11");
        tdSql.checkData(1, 0, "(10-20]:2");
        tdSql.checkData(2, 0, "(20-100]:2");
        tdSql.query('select histogram(col_double, "user_input", "[-10,10,20,100]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-10-10]:11");
        tdSql.checkData(1, 0, "(10-20]:2");
        tdSql.checkData(2, 0, "(20-100]:2");
        tdSql.query('select histogram(col_double, "user_input", "[-10,10,20,100]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-10-10]:11");
        tdSql.checkData(1, 0, "(10-20]:2");
        tdSql.checkData(2, 0, "(20-100]:2");

        tdSql.query('select histogram(col_double, "user_input", "[-9.4,9.6,20.4,99.9]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-9.4-9.6]:10");
        tdSql.checkData(1, 0, "(9.6-20.4]:2");
        tdSql.checkData(2, 0, "(20.4-99.9]:2");
        tdSql.query('select histogram(col_double, "user_input", "[-9.4,9.6,20.4,99.9]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-9.4-9.6]:10");
        tdSql.checkData(1, 0, "(9.6-20.4]:2");
        tdSql.checkData(2, 0, "(20.4-99.9]:2");
        tdSql.query('select histogram(col_double, "user_input", "[-9.4,9.6,20.4,99.9]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-9.4-9.6]:10");
        tdSql.checkData(1, 0, "(9.6-20.4]:2");
        tdSql.checkData(2, 0, "(20.4-99.9]:2");


        tdSql.query('select histogram(col_double, "user_input", "[-99999999999999,9.9,19.9,99999999999999]", 0) from stb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1e+14-9.9]:11");
        tdSql.checkData(1, 0, "(9.9-19.9]:2");
        tdSql.checkData(2, 0, "(19.9-1e+14]:2");
        tdSql.query('select histogram(col_double, "user_input", "[-99999999999999,9.9,19.9,99999999999999]", 0) from ctb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1e+14-9.9]:11");
        tdSql.checkData(1, 0, "(9.9-19.9]:2");
        tdSql.checkData(2, 0, "(19.9-1e+14]:2");
        tdSql.query('select histogram(col_double, "user_input", "[-99999999999999,9.9,19.9,99999999999999]", 0) from tb;')
        tdSql.checkRows(3);
        tdSql.checkData(0, 0, "(-1e+14-9.9]:11");
        tdSql.checkData(1, 0, "(9.9-19.9]:2");
        tdSql.checkData(2, 0, "(19.9-1e+14]:2");

        return
        tdSql.execute('drop database db')
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
