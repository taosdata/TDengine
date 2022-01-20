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
        case1<Ganlin Zhao>: [TD-12861] : taoshell crash coredump for such as "select first(c1)==max(c1) from st"
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
        tdSql.execute("create table tb (ts timestamp, value int);")
        tdSql.execute("insert into tb values (now, 1);")
        tdSql.execute("insert into tb values (now, 2);")
        tdSql.execute("insert into tb values (now, 3);")
        tdSql.execute("insert into tb values (now, 4);")

        ##operator: =
        tdSql.error('select count(*) = 4 from tb;')
        tdSql.error('select avg(value) = 2.5 from tb')
        tdSql.error('select twa(value) = 3.03 from tb')
        tdSql.error('select sum(value) = 10 from tb')
        tdSql.error('select stddev(value) = 2.0 from tb')
        tdSql.error('select min(value) = 1 from tb')
        tdSql.error('select max(value) = 4 from tb')
        tdSql.error('select first(*) = 3 from tb')
        tdSql.error('select last(*) = 3 from tb')
        tdSql.error('select top(value, 3) = 3 from tb')
        tdSql.error('select bottom(value, 3) = 3 from tb')
        tdSql.error('select percentile(value, 50) = 3 from tb')
        tdSql.error('select apercentile(value, 50) = 3 from tb')
        tdSql.error('select last_row(*) = 3 from tb')
        tdSql.error('select diff(value) = 3 from tb')
        tdSql.error('select ceil(value) = 12 from tb')
        tdSql.error('select floor(3.5) = 3 from tb')
        tdSql.error('select round(3.5) = 3 from tb')

        tdSql.error('select count(*) = max(value) from tb')
        tdSql.error('select avg(value) = min(value) from tb')
        tdSql.error('select first(value) = last(value) from tb')
        tdSql.error('select round(value) = round(value) from tb')

        ##operator: !=
        tdSql.error('select count(*) != 4 from tb;')
        tdSql.error('select avg(value) != 2.5 from tb')
        tdSql.error('select twa(value) != 3.03 from tb')
        tdSql.error('select sum(value) != 10 from tb')
        tdSql.error('select stddev(value) != 2.0 from tb')
        tdSql.error('select min(value) != 1 from tb')
        tdSql.error('select max(value) != 4 from tb')
        tdSql.error('select first(*) != 3 from tb')
        tdSql.error('select last(*) != 3 from tb')
        tdSql.error('select top(value, 3) != 3 from tb')
        tdSql.error('select bottom(value, 3) != 3 from tb')
        tdSql.error('select percentile(value, 50) != 3 from tb')
        tdSql.error('select apercentile(value, 50) != 3 from tb')
        tdSql.error('select last_row(*) != 3 from tb')
        tdSql.error('select diff(value) != 3 from tb')
        tdSql.error('select ceil(value) != 12 from tb')
        tdSql.error('select floor(3.5) != 3 from tb')
        tdSql.error('select round(3.5) != 3 from tb')

        tdSql.error('select count(*) != max(value) from tb')
        tdSql.error('select avg(value) != min(value) from tb')
        tdSql.error('select first(value) != last(value) from tb')
        tdSql.error('select round(value) != round(value) from tb')

        ##operator: <>
        tdSql.error('select count(*) <> 4 from tb;')
        tdSql.error('select avg(value) <> 2.5 from tb')
        tdSql.error('select twa(value) <> 3.03 from tb')
        tdSql.error('select sum(value) <> 10 from tb')
        tdSql.error('select stddev(value) <> 2.0 from tb')
        tdSql.error('select min(value) <> 1 from tb')
        tdSql.error('select max(value) <> 4 from tb')
        tdSql.error('select first(*) <> 3 from tb')
        tdSql.error('select last(*) <> 3 from tb')
        tdSql.error('select top(value, 3) <> 3 from tb')
        tdSql.error('select bottom(value, 3) <> 3 from tb')
        tdSql.error('select percentile(value, 50) <> 3 from tb')
        tdSql.error('select apercentile(value, 50) <> 3 from tb')
        tdSql.error('select last_row(*) <> 3 from tb')
        tdSql.error('select diff(value) <> 3 from tb')
        tdSql.error('select ceil(value) <> 12 from tb')
        tdSql.error('select floor(3.5) <> 3 from tb')
        tdSql.error('select round(3.5) <> 3 from tb')

        tdSql.error('select count(*) <> max(value) from tb')
        tdSql.error('select avg(value) <> min(value) from tb')
        tdSql.error('select first(value) <> last(value) from tb')
        tdSql.error('select round(value) <> round(value) from tb')

        ##operator: <
        tdSql.error('select count(*) < 4 from tb;')
        tdSql.error('select avg(value) < 2.5 from tb')
        tdSql.error('select twa(value) < 3.03 from tb')
        tdSql.error('select sum(value) < 10 from tb')
        tdSql.error('select stddev(value) < 2.0 from tb')
        tdSql.error('select min(value) < 1 from tb')
        tdSql.error('select max(value) < 4 from tb')
        tdSql.error('select first(*) < 3 from tb')
        tdSql.error('select last(*) < 3 from tb')
        tdSql.error('select top(value, 3) < 3 from tb')
        tdSql.error('select bottom(value, 3) < 3 from tb')
        tdSql.error('select percentile(value, 50) < 3 from tb')
        tdSql.error('select apercentile(value, 50) < 3 from tb')
        tdSql.error('select last_row(*) < 3 from tb')
        tdSql.error('select diff(value) < 3 from tb')
        tdSql.error('select ceil(value) < 12 from tb')
        tdSql.error('select floor(3.5) < 3 from tb')
        tdSql.error('select round(3.5) < 3 from tb')

        tdSql.error('select count(*) < max(value) from tb')
        tdSql.error('select avg(value) < min(value) from tb')
        tdSql.error('select first(value) < last(value) from tb')
        tdSql.error('select round(value) < round(value) from tb')

        ##operator: >
        tdSql.error('select count(*) > 4 from tb;')
        tdSql.error('select avg(value) > 2.5 from tb')
        tdSql.error('select twa(value) > 3.03 from tb')
        tdSql.error('select sum(value) > 10 from tb')
        tdSql.error('select stddev(value) > 2.0 from tb')
        tdSql.error('select min(value) > 1 from tb')
        tdSql.error('select max(value) > 4 from tb')
        tdSql.error('select first(*) > 3 from tb')
        tdSql.error('select last(*) > 3 from tb')
        tdSql.error('select top(value, 3) > 3 from tb')
        tdSql.error('select bottom(value, 3) > 3 from tb')
        tdSql.error('select percentile(value, 50) > 3 from tb')
        tdSql.error('select apercentile(value, 50) > 3 from tb')
        tdSql.error('select last_row(*) > 3 from tb')
        tdSql.error('select diff(value) > 3 from tb')
        tdSql.error('select ceil(value) > 12 from tb')
        tdSql.error('select floor(3.5) > 3 from tb')
        tdSql.error('select round(3.5) > 3 from tb')

        tdSql.error('select count(*) > max(value) from tb')
        tdSql.error('select avg(value) > min(value) from tb')
        tdSql.error('select first(value) > last(value) from tb')
        tdSql.error('select round(value) > round(value) from tb')

        ##operator: <=
        tdSql.error('select count(*) <= 4 from tb;')
        tdSql.error('select avg(value) <= 2.5 from tb')
        tdSql.error('select twa(value) <= 3.03 from tb')
        tdSql.error('select sum(value) <= 10 from tb')
        tdSql.error('select stddev(value) <= 2.0 from tb')
        tdSql.error('select min(value) <= 1 from tb')
        tdSql.error('select max(value) <= 4 from tb')
        tdSql.error('select first(*) <= 3 from tb')
        tdSql.error('select last(*) <= 3 from tb')
        tdSql.error('select top(value, 3) <= 3 from tb')
        tdSql.error('select bottom(value, 3) <= 3 from tb')
        tdSql.error('select percentile(value, 50) <= 3 from tb')
        tdSql.error('select apercentile(value, 50) <= 3 from tb')
        tdSql.error('select last_row(*) <= 3 from tb')
        tdSql.error('select diff(value) <= 3 from tb')
        tdSql.error('select ceil(value) <= 12 from tb')
        tdSql.error('select floor(3.5) <= 3 from tb')
        tdSql.error('select round(3.5) <= 3 from tb')

        tdSql.error('select count(*) <= max(value) from tb')
        tdSql.error('select avg(value) <= min(value) from tb')
        tdSql.error('select first(value) <= last(value) from tb')
        tdSql.error('select round(value) <= round(value) from tb')

        ##operator: >=
        tdSql.error('select count(*) >= 4 from tb;')
        tdSql.error('select avg(value) >= 2.5 from tb')
        tdSql.error('select twa(value) >= 3.03 from tb')
        tdSql.error('select sum(value) >= 10 from tb')
        tdSql.error('select stddev(value) >= 2.0 from tb')
        tdSql.error('select min(value) >= 1 from tb')
        tdSql.error('select max(value) >= 4 from tb')
        tdSql.error('select first(*) >= 3 from tb')
        tdSql.error('select last(*) >= 3 from tb')
        tdSql.error('select top(value, 3) >= 3 from tb')
        tdSql.error('select bottom(value, 3) >= 3 from tb')
        tdSql.error('select percentile(value, 50) >= 3 from tb')
        tdSql.error('select apercentile(value, 50) >= 3 from tb')
        tdSql.error('select last_row(*) >= 3 from tb')
        tdSql.error('select diff(value) >= 3 from tb')
        tdSql.error('select ceil(value) >= 12 from tb')
        tdSql.error('select floor(3.5) >= 3 from tb')
        tdSql.error('select round(3.5) >= 3 from tb')

        tdSql.error('select count(*) >= max(value) from tb')
        tdSql.error('select avg(value) >= min(value) from tb')
        tdSql.error('select first(value) >= last(value) from tb')
        tdSql.error('select round(value) >= round(value) from tb')

        ##operator: between and
        tdSql.error('select count(*) between 3 and 4 from tb;')
        tdSql.error('select avg(value) between 1.5 and 2.5 from tb')
        tdSql.error('select twa(value) between 3.0 and 3.03 from tb')
        tdSql.error('select sum(value) between 1 and 10 from tb')
        tdSql.error('select stddev(value) between 1 and 2.0 from tb')
        tdSql.error('select min(value) between 2 and 5 from tb')
        tdSql.error('select max(value) between 1 and 10 from tb')
        tdSql.error('select first(*) between 1 and 3 from tb')
        tdSql.error('select last(*) between 0 and 3 from tb')
        tdSql.error('select top(value, 3) between 0.0 and 3 from tb')
        tdSql.error('select bottom(value, 3) between 0.0 and 3 from tb')
        tdSql.error('select percentile(value, 50) between 1 and 3 from tb')
        tdSql.error('select apercentile(value, 50) between 2 and 3 from tb')
        tdSql.error('select last_row(*) between 2 and 3 from tb')
        tdSql.error('select diff(value) between 1 and 3 from tb')
        tdSql.error('select ceil(value) between 5 and 12 from tb')
        tdSql.error('select floor(3.5) between 12 and 3 from tb')
        tdSql.error('select round(3.5) between true and 3 from tb')

        tdSql.error('select count(*) between min(value) and max(value) from tb')
        tdSql.error('select avg(*) between min(value) and 3 from tb')
        tdSql.error('select avg(value) between 1 and max(value) from tb')
        tdSql.error('select first(value) between first(value) and last(value) from tb')
        tdSql.error('select round(value) between ceil(value) and floor(value) from tb')

        ##operator: and
        tdSql.error('select count(*) and 1 from tb;')
        tdSql.error('select avg(value) and 0.0 from tb')
        tdSql.error('select twa(value) and true from tb')
        tdSql.error('select sum(value) and false from tb')
        tdSql.error('select 1 and stddev(value) from tb')
        tdSql.error('select 0.0 and min(value) from tb')
        tdSql.error('select true and max(value) from tb')
        tdSql.error('select false and first(*) from tb')
        tdSql.error('select last(*) and first(value) from tb')
        tdSql.error('select top(value, 3) and bottom(value, 3) from tb')
        tdSql.error('select percentile(value, 50) and apercentile(value, 50) from tb')
        tdSql.error('select diff(value) and ceil(value) from tb')
        tdSql.error('select floor(3.5) and round(3.5) and ceil(3.5) from tb')
        tdSql.error('select true and round(3.5) and 3 from tb')

        ##operator: or
        tdSql.error('select count(*) or 1 from tb;')
        tdSql.error('select avg(value) or 0.0 from tb')
        tdSql.error('select twa(value) or true from tb')
        tdSql.error('select sum(value) or false from tb')
        tdSql.error('select 1 or stddev(value) from tb')
        tdSql.error('select 0.0 or min(value) from tb')
        tdSql.error('select true or max(value) from tb')
        tdSql.error('select false or first(*) from tb')
        tdSql.error('select last(*) or first(value) from tb')
        tdSql.error('select top(value, 3) or bottom(value, 3) from tb')
        tdSql.error('select percentile(value, 50) or apercentile(value, 50) from tb')
        tdSql.error('select diff(value) or ceil(value) from tb')
        tdSql.error('select floor(3.5) or round(3.5) or ceil(3.5) from tb')
        tdSql.error('select true or round(3.5) or 3 from tb')

        ##operator: multiple operations
        tdSql.error('select count(*) <> avg(value) or twa(value) and sum(value) or 1 from tb;')
        tdSql.error('select 1 and stddev(value) <= min(value) or max(value) and first(*) or 0.0 from tb')
        tdSql.error('select last(*) and first(value) or top(value, 3) and 3 between 4.0 and bottom(value, 3)from tb')
        tdSql.error('select percentile(value, 50) or diff(value) = ceil(value) and apercentile(value, 50) from tb')
        tdSql.error('select floor(3.5) or round(3.5) and ceil(3.5) > true and round(3.5) or 3 from tb')

        #operator: is NULL
        tdSql.error('select count(*) is NULL from tb;')
        tdSql.error('select avg(value) is NULL from tb;')
        tdSql.error('select twa(value) is NULL from tb;')
        tdSql.error('select sum(value) is NULL from tb;')
        tdSql.error('select stddev(value) is NULL from tb;')
        tdSql.error('select min(value) is NULL from tb;')
        tdSql.error('select max(value) is NULL from tb;')
        tdSql.error('select first(*) is NULL from tb;')
        tdSql.error('select last(*) is NULL from tb;')
        tdSql.error('select top(value, 3) is NULL or bottom(value,3) is NULL from tb;')
        tdSql.error('select percentile(value, 50) is NULL or apercentile(value, 50) is NULL from tb')
        tdSql.error('select diff(value) is NULL or ceil(value) is NULL from tb')
        tdSql.error('select floor(3.5) is NULL or round(3.5) is NULL or ceil(3.5) is NULL from tb')

        #operator: is not NULL
        tdSql.error('select count(*) is not NULL from tb;')
        tdSql.error('select avg(value) is not NULL from tb;')
        tdSql.error('select twa(value) is not NULL from tb;')
        tdSql.error('select sum(value) is not NULL from tb;')
        tdSql.error('select stddev(value) is not NULL from tb;')
        tdSql.error('select min(value) is not NULL from tb;')
        tdSql.error('select max(value) is not NULL from tb;')
        tdSql.error('select first(*) is not NULL from tb;')
        tdSql.error('select last(*) is not NULL from tb;')
        tdSql.error('select top(value, 3) is not NULL or bottom(value,3) is not NULL from tb;')
        tdSql.error('select percentile(value, 50) is not NULL or apercentile(value, 50) is not NULL from tb')
        tdSql.error('select diff(value) is not NULL or ceil(value) is not NULL from tb')
        tdSql.error('select floor(3.5) is not NULL or round(3.5) is not NULL or ceil(3.5) is not NULL from tb')

        #operator: like
        tdSql.error('select count(*) like "abc" from tb;')
        tdSql.error('select avg(value) like "abc" from tb;')
        tdSql.error('select twa(value) like "abc" from tb;')
        tdSql.error('select sum(value) like "abc" from tb;')
        tdSql.error('select stddev(value) like "abc" from tb;')
        tdSql.error('select min(value) like "abc" from tb;')
        tdSql.error('select max(value) like "abc" from tb;')
        tdSql.error('select first(*) like "abc" from tb;')
        tdSql.error('select last(*) like "abc" from tb;')
        tdSql.error('select top(value, 3) like "abc" or bottom(value,3) like "abc" from tb;')
        tdSql.error('select percentile(value, 50) like "abc" or apercentile(value, 50) like "abc" from tb')
        tdSql.error('select diff(value) like "abc" or ceil(value) like "abc" from tb')
        tdSql.error('select floor(3.5) like "abc" or round(3.5) like "abc" or ceil(3.5) like "abc" from tb')

        #operator: match
        tdSql.error('select count(*) match "abc" from tb;')
        tdSql.error('select avg(value) match "abc" from tb;')
        tdSql.error('select twa(value) match "abc" from tb;')
        tdSql.error('select sum(value) match "abc" from tb;')
        tdSql.error('select stddev(value) match "abc" from tb;')
        tdSql.error('select min(value) match "abc" from tb;')
        tdSql.error('select max(value) match "abc" from tb;')
        tdSql.error('select first(*) match "abc" from tb;')
        tdSql.error('select last(*) match "abc" from tb;')
        tdSql.error('select top(value, 3) match "abc" or bottom(value,3) match "abc" from tb;')
        tdSql.error('select percentile(value, 50) match "abc" or apercentile(value, 50) match "abc" from tb')
        tdSql.error('select diff(value) match "abc" or ceil(value) match "abc" from tb')
        tdSql.error('select floor(3.5) match "abc" or round(3.5) match "abc" or ceil(3.5) match "abc" from tb')

        #operator: nmatch
        tdSql.error('select count(*) nmatch "abc" from tb;')
        tdSql.error('select avg(value) nmatch "abc" from tb;')
        tdSql.error('select twa(value) nmatch "abc" from tb;')
        tdSql.error('select sum(value) nmatch "abc" from tb;')
        tdSql.error('select stddev(value) nmatch "abc" from tb;')
        tdSql.error('select min(value) nmatch "abc" from tb;')
        tdSql.error('select max(value) nmatch "abc" from tb;')
        tdSql.error('select first(*) nmatch "abc" from tb;')
        tdSql.error('select last(*) nmatch "abc" from tb;')
        tdSql.error('select top(value, 3) nmatch "abc" or bottom(value,3) nmatch "abc" from tb;')
        tdSql.error('select percentile(value, 50) nmatch "abc" or apercentile(value, 50) nmatch "abc" from tb')
        tdSql.error('select diff(value) nmatch "abc" or ceil(value) nmatch "abc" from tb')
        tdSql.error('select floor(3.5) nmatch "abc" or round(3.5) nmatch "abc" or ceil(3.5) nmatch "abc" from tb')

        #operator: in
        tdSql.error('select count(*) in 1 from tb;')
        tdSql.error('select avg(value) in (1, 2, 3) from tb;')
        tdSql.error('select twa(value) in 1.0 from tb;')
        tdSql.error('select sum(value) in (1.0, 2.0, 3.0) from tb;')
        tdSql.error('select min(value) in (true, false, true) from tb;')
        tdSql.error('select tbname in (\'acd\', \'bce\') from tb;')
        tdSql.error('select t in ("acd", "bce") from tb;')
        tdSql.error('select top(value, 3) in (1,2,3) and ceil(value) in (1.0,2.0,3.0) or last(*) in ("abc","cde") from tb;')

        tdSql.execute('drop database db')

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
