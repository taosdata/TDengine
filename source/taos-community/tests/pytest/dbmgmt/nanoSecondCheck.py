# #################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.

#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao

# #################################################################

# -*- coding: utf-8 -*-

# TODO: after TD-4518 and TD-4510 is resolved, add the exception test case for these situations

import sys
from util.log import *
from util.cases import *
from util.sql import *
import time
from datetime import datetime
import os


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()
        tdSql.execute('reset query cache')
        tdSql.execute('drop database if exists db')
        tdSql.execute('create database db precision "ns";')
        tdSql.query('select * from information_schema.ins_databases;')
        tdSql.checkData(0,16,'ns')
        tdSql.execute('use db')

        tdLog.debug('testing nanosecond support in 1st timestamp')
        tdSql.execute('create table tb (ts timestamp, speed int)')
        tdSql.execute('insert into tb values(\'2021-06-10 0:00:00.100000001\', 1);')
        tdSql.execute('insert into tb values(1623254400150000000, 2);')
        tdSql.execute('import into tb values(1623254400300000000, 3);')
        tdSql.execute('import into tb values(1623254400299999999, 4);')
        tdSql.execute('insert into tb values(1623254400300000001, 5);')
        tdSql.execute('insert into tb values(1623254400999999999, 7);')


        tdSql.query('select * from tb;')
        tdSql.checkData(0,0,'2021-06-10 0:00:00.100000001')
        tdSql.checkData(1,0,'2021-06-10 0:00:00.150000000')
        tdSql.checkData(2,0,'2021-06-10 0:00:00.299999999')
        tdSql.checkData(3,1,3)
        tdSql.checkData(4,1,5)
        tdSql.checkData(5,1,7)
        tdSql.checkRows(6)
        tdSql.query('select count(*) from tb where ts > 1623254400100000000 and ts < 1623254400100000002;')
        tdSql.checkData(0,0,1)
        tdSql.query('select count(*) from tb where ts > \'2021-06-10 0:00:00.100000001\' and ts < \'2021-06-10 0:00:00.160000000\';')
        tdSql.checkData(0,0,1)

        tdSql.query('select count(*) from tb where ts > 1623254400100000000 and ts < 1623254400150000000;')
        tdSql.checkData(0,0,1)
        tdSql.query('select count(*) from tb where ts > \'2021-06-10 0:00:00.100000000\' and ts < \'2021-06-10 0:00:00.150000000\';')
        tdSql.checkData(0,0,1)

        tdSql.query('select count(*) from tb where ts > 1623254400400000000;')
        tdSql.checkData(0,0,1)
        tdSql.query('select count(*) from tb where ts < \'2021-06-10 00:00:00.400000000\';')
        tdSql.checkData(0,0,5)

        tdSql.query('select count(*) from tb where ts > now + 400000000b;')
        tdSql.checkRows(0)

        tdSql.query('select count(*) from tb where ts >= \'2021-06-10 0:00:00.100000001\';')
        tdSql.checkData(0,0,6)

        tdSql.query('select count(*) from tb where ts <= 1623254400300000000;')
        tdSql.checkData(0,0,4)

        tdSql.query('select count(*) from tb where ts = \'2021-06-10 0:00:00.000000000\';')
        tdSql.checkRows(0)

        tdSql.query('select count(*) from tb where ts = 1623254400150000000;')
        tdSql.checkData(0,0,1)

        tdSql.query('select count(*) from tb where ts = \'2021-06-10 0:00:00.100000001\';')
        tdSql.checkData(0,0,1)

        tdSql.query('select count(*) from tb where ts between 1623254400000000000 and 1623254400400000000;')
        tdSql.checkData(0,0,5)

        tdSql.query('select count(*) from tb where ts between \'2021-06-10 0:00:00.299999999\' and \'2021-06-10 0:00:00.300000001\';')
        tdSql.checkData(0,0,3)

        tdSql.query('select avg(speed) from tb interval(5000000000b);')
        tdSql.checkRows(1)

        tdSql.query('select avg(speed) from tb interval(100000000b)')
        tdSql.checkRows(4)

        tdSql.error('select avg(speed) from tb interval(1b);')
        tdSql.error('select avg(speed) from tb interval(999b);')

        tdSql.query('select avg(speed) from tb interval(1000b);')
        tdSql.checkRows(5)

        tdSql.query('select avg(speed) from tb interval(1u);')
        tdSql.checkRows(5)

        tdSql.query('select avg(speed) from tb interval(100000000b) sliding (100000000b);')
        tdSql.checkRows(4)

        tdSql.query('select last(*) from tb')
        tdSql.checkData(0,0, '2021-06-10 0:00:00.999999999')
        tdSql.checkData(0,0, 1623254400999999999)

        tdSql.query('select first(*) from tb')
        tdSql.checkData(0,0, 1623254400100000001)
        tdSql.checkData(0,0, '2021-06-10 0:00:00.100000001')

        tdSql.execute('insert into tb values(now + 500000000b, 6);')
        tdSql.query('select * from tb;')
        tdSql.checkRows(7)

        tdLog.debug('testing nanosecond support in other timestamps')
        tdSql.execute('create table tb2 (ts timestamp, speed int, ts2 timestamp);')
        tdSql.execute('insert into tb2 values(\'2021-06-10 0:00:00.100000001\', 1, \'2021-06-11 0:00:00.100000001\');')
        tdSql.execute('insert into tb2 values(1623254400150000000, 2, 1623340800150000000);')
        tdSql.execute('import into tb2 values(1623254400300000000, 3, 1623340800300000000);')
        tdSql.execute('import into tb2 values(1623254400299999999, 4, 1623340800299999999);')
        tdSql.execute('insert into tb2 values(1623254400300000001, 5, 1623340800300000001);')
        tdSql.execute('insert into tb2 values(1623254400999999999, 7, 1623513600999999999);')

        tdSql.query('select * from tb2;')
        tdSql.checkData(0,0,'2021-06-10 0:00:00.100000001')
        tdSql.checkData(1,0,'2021-06-10 0:00:00.150000000')
        tdSql.checkData(2,1,4)
        tdSql.checkData(3,1,3)
        tdSql.checkData(4,2,'2021-06-11 00:00:00.300000001')
        tdSql.checkData(5,2,'2021-06-13 00:00:00.999999999')
        tdSql.checkRows(6)
        tdSql.query('select count(*) from tb2 where ts2 > 1623340800000000000 and ts2 < 1623340800150000000;')
        tdSql.checkData(0,0,1)
        tdSql.query('select count(*) from tb2 where ts2 > \'2021-06-11 0:00:00.100000000\' and ts2 < \'2021-06-11 0:00:00.100000002\';')
        tdSql.checkData(0,0,1)

        tdSql.query('select count(*) from tb2 where ts2 > 1623340800500000000;')
        tdSql.checkData(0,0,1)
        tdSql.query('select count(*) from tb2 where ts2 < \'2021-06-11 0:00:00.400000000\';')
        tdSql.checkData(0,0,5)

        tdSql.query('select count(*) from tb2 where ts2 > now + 400000000b;')
        tdSql.checkRows(0)

        tdSql.query('select count(*) from tb2 where ts2 >= \'2021-06-11 0:00:00.100000001\';')
        tdSql.checkData(0,0,6)

        tdSql.query('select count(*) from tb2 where ts2 <= 1623340800400000000;')
        tdSql.checkData(0,0,5)

        tdSql.query('select count(*) from tb2 where ts2 = \'2021-06-11 0:00:00.000000000\';')
        tdSql.checkRows(0)

        tdSql.query('select count(*) from tb2 where ts2 = \'2021-06-11 0:00:00.300000001\';')
        tdSql.checkData(0,0,1)

        tdSql.query('select count(*) from tb2 where ts2 = 1623340800300000001;')
        tdSql.checkData(0,0,1)

        tdSql.query('select count(*) from tb2 where ts2 between 1623340800000000000 and 1623340800450000000;')
        tdSql.checkData(0,0,5)

        tdSql.query('select count(*) from tb2 where ts2 between \'2021-06-11 0:00:00.299999999\' and \'2021-06-11 0:00:00.300000001\';')
        tdSql.checkData(0,0,3)

        tdSql.query('select count(*) from tb2 where ts2 <> 1623513600999999999;')
        tdSql.checkData(0,0,5)

        tdSql.query('select count(*) from tb2 where ts2 <> \'2021-06-11 0:00:00.100000001\';')
        tdSql.checkData(0,0,5)

        tdSql.query('select count(*) from tb2 where ts2 <> \'2021-06-11 0:00:00.100000000\';')
        tdSql.checkData(0,0,6)

        tdSql.query('select count(*) from tb2 where ts2 != 1623513600999999999;')
        tdSql.checkData(0,0,5)

        tdSql.query('select count(*) from tb2 where ts2 != \'2021-06-11 0:00:00.100000001\';')
        tdSql.checkData(0,0,5)

        tdSql.query('select count(*) from tb2 where ts2 != \'2021-06-11 0:00:00.100000000\';')
        tdSql.checkData(0,0,6)

        tdSql.execute('insert into tb2 values(now + 500000000b, 6, now +2d);')
        tdSql.query('select * from tb2;')
        tdSql.checkRows(7)

        tdLog.debug('testing ill nanosecond format handling')
        tdSql.execute('create table tb3 (ts timestamp, speed int);')

        tdSql.error('insert into tb3 values(16232544001500000, 2);')
        tdSql.execute('insert into tb3 values(\'2021-06-10 0:00:00.123456\', 2);')
        tdSql.query('select * from tb3 where ts = \'2021-06-10 0:00:00.123456000\';')
        tdSql.checkRows(1)

        tdSql.execute('insert into tb3 values(\'2021-06-10 0:00:00.123456789000\', 2);')
        tdSql.query('select * from tb3 where ts = \'2021-06-10 0:00:00.123456789\';')
        tdSql.checkRows(1)

        os.system('sudo timedatectl set-ntp on')

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())