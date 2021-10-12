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
import json
import taos
import time
import random

from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


'''

Before test start，Two TDengine services have been set up on different servers 

'''

host1 = '192.168.1.101'
host2 = '192.168.1.102'
user = 'root'
password = 'taosdata'
cfgdir = '/home/cp/taos/TDengine/sim/dnode1/cfg'

conn1 = taos.connect(host=host1, user=user, password=password, config=cfgdir)
conn2 = taos.connect(host=host2, user=user, password=password, config=cfgdir)
cursor1 = conn1.cursor()
cursor2 = conn2.cursor()
tdSql1 = TDSql()
tdSql2 = TDSql()
tdSql1.init(cursor1)
tdSql2.init(cursor2)

dbname11 = 'db11'
dbname12 = 'db12'
dbname21 = 'db21'
stbname11 = 'stb11'
stbname12 = 'stb12'
stbname21 = 'stb21'
tbnum = 100
data_row = 100
db1_stb1_column = 'ts timestamp, c1 int, c2 float, c3 timestamp, c4 binary(16), c5 double, c6 bool, c7 bigint, c8 smallint, c9 tinyint, c10 nchar(16)  '
db1_stb1_tag = 'st1 int, st2 float, st3 timestamp, st4 binary(16), st5 double, st6 bool, st7 bigint, st8 smallint, st9 tinyint, st10 nchar(16)'

def dbsql(dbname):
    return f"create database {dbname} keep 3650"

def stbsql(stbname, columntype, tagtype):
    return f'create stable {stbname} ({columntype}) tags ({tagtype}) '

def tbsql(tbname, stbname, tags):
    return f'create table {tbname} using {stbname} tags ({tags})'

def datasql(tbname, data):
    return f'insert into {tbname} values ({data})'

def testquery():
    ti = random.randint(0,tbnum-1)

    tdSql1.query(f"select c1, c2, c3, c4, c5, c6, c7, c8, c9, c10 from tm{ti}")
    tdSql1.checkData(0, 0, ti)
    tdSql1.checkData(0, 1, ti)
    tdSql1.checkData(0, 2, ti)
    tdSql1.checkData(0, 3, f'binary_{ti}')
    tdSql1.checkData(0, 4, ti)
    tdSql1.checkData(0, 5, ti%2)
    tdSql1.checkData(0, 6, ti)
    tdSql1.checkData(0, 7, ti%32768)
    tdSql1.checkData(0, 8, ti%128)
    tdSql1.checkData(0, 9, f'nchar_{ti}')
    tdSql2.query(f"select c1, c2, c3, c4, c5, c6, c7, c8, c9, c10 from tn{ti}")
    tdSql2.checkData(0, 0, ti+10000)
    tdSql2.checkData(0, 1, ti+10000)
    tdSql2.checkData(0, 2, ti+10000)
    tdSql2.checkData(0, 3, f'binary_{ti+10000}')
    tdSql2.checkData(0, 4, ti+10000)
    tdSql2.checkData(0, 5, (ti+10000)%2)
    tdSql2.checkData(0, 6, ti+10000)
    tdSql2.checkData(0, 7, (ti+10000)%32768)
    tdSql2.checkData(0, 8, (ti+10000)%128)
    tdSql2.checkData(0, 9, f'nchar_{ti+10000}')

    tdSql1.query(f"select last(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10) from {stbname11}")
    tdSql1.checkData(0, 0, data_row-1)
    tdSql1.checkData(0, 1, data_row-1)
    tdSql1.checkData(0, 2, data_row-1)
    tdSql1.checkData(0, 3, f'binary_{data_row-1}')
    tdSql1.checkData(0, 4, data_row-1)
    tdSql1.checkData(0, 5, (data_row-1)%2)
    tdSql1.checkData(0, 6, data_row-1)
    tdSql1.checkData(0, 7, (data_row-1)%32768)
    tdSql1.checkData(0, 8, (data_row-1)%128)
    tdSql1.checkData(0, 9, f'nchar_{data_row-1}')

    tdSql1.query(f"select first(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10) from {stbname11}")
    tdSql1.checkData(0, 0, 0)
    tdSql1.checkData(0, 1, 0)
    tdSql1.checkData(0, 2, 0)
    tdSql1.checkData(0, 3, f'binary_0')
    tdSql1.checkData(0, 4, 0)
    tdSql1.checkData(0, 5, 0)
    tdSql1.checkData(0, 6, 0)
    tdSql1.checkData(0, 7, 0)
    tdSql1.checkData(0, 8, 0)
    tdSql1.checkData(0, 9, f'nchar_0')

    tdSql1.error("select * from")

    tdSql1.query(f"select last(*) from tm1")
    tdSql1.checkData(0, 1, 1)
    tdSql1.checkData(0, 4, "binary_1")


    tdSql1.query(f"select min(c1),max(c2) from {stbname11}")
    tdSql1.checkData(0, 0, 0)
    tdSql1.checkData(0, 1, data_row-1)

    tdSql2.query(f"select count(*), count(c1) from {stbname21}")
    tdSql2.checkData(0, 0, data_row)
    tdSql2.checkData(0, 1, data_row)

    tdSql2.query(f"select first(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10) from {stbname21}")
    tdSql2.checkData(0, 0, 10000)
    tdSql2.checkData(0, 1, 10000)
    tdSql2.checkData(0, 2, 10000)
    tdSql2.checkData(0, 3, f'binary_10000')
    tdSql2.checkData(0, 4, 10000)
    tdSql2.checkData(0, 5, 10000%2)
    tdSql2.checkData(0, 6, 10000)
    tdSql2.checkData(0, 7, 10000%32768)
    tdSql2.checkData(0, 8, 10000%128)
    tdSql2.checkData(0, 9, f'nchar_10000')

    tdSql2.query(f"select last(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10) from {stbname21}")
    tdSql2.checkData(0, 0, data_row+9999)
    tdSql2.checkData(0, 1, data_row+9999)
    tdSql2.checkData(0, 2, data_row+9999)
    tdSql2.checkData(0, 3, f'binary_{data_row+9999}')
    tdSql2.checkData(0, 4, data_row+9999)
    tdSql2.checkData(0, 5, (data_row+9999)%2)
    tdSql2.checkData(0, 6, data_row+9999)
    tdSql2.checkData(0, 7, (data_row+9999)%32768)
    tdSql2.checkData(0, 8, (data_row+9999)%128)
    tdSql2.checkData(0, 9, f'nchar_{data_row+9999}')

    tdSql1.query(f"select max(c1) from (select top(c1,10) c1 from {stbname11})")
    tdSql1.checkData(0, 0, data_row-1)
    tdSql2.query(f"select max(c1) from (select top(c1,10) c1 from {stbname21})")
    tdSql2.checkData(0, 0, data_row+9999)

    tdSql1.query(f"select avg(c1) from {stbname11}")
    tdSql1.checkData(0, 0, sum(range(data_row))/data_row)
    tdSql2.query(f"select avg(c1) from {stbname21}")
    tdSql2.checkData(0, 0, sum(range(data_row))/data_row+10000)

    tdSql1.query(f"select spread(c1) from {stbname11}")
    tdSql1.checkData(0, 0, data_row-1)
    tdSql2.query(f"select spread(c1) from {stbname21}")
    tdSql2.checkData(0, 0, data_row-1)

    tdSql1.query(f"select max(c1)*2 from {stbname11}")
    tdSql1.checkData(0, 0, (data_row-1)*2)
    tdSql2.query(f"select max(c1)*2 from {stbname21}")
    tdSql2.checkData(0, 0, (data_row+9999)*2)

    tdSql1.query(f"select avg(c1) from {stbname11} where c1 <= 10")
    tdSql1.checkData(0, 0, 5)
    tdSql2.query(f"select avg(c1) from {stbname21} where c1 <= 10010")
    tdSql2.checkData(0, 0, 10005)

    tdSql1.query(f"select * from {stbname11} where tbname like 'tn%'")
    tdSql1.checkRows(0)
    tdSql2.query(f"select * from {stbname21} where tbname like 'tm%'")
    tdSql2.checkRows(0)

    tdSql1.query(f"select max(c1) from {stbname11} group by tbname")
    tdSql1.checkRows(tbnum)
    tdSql2.query(f"select max(c1) from {stbname21} group by tbname")
    tdSql2.checkRows(tbnum)

    tdSql1.error(f"select * from {stbname11}, {stbname21} where {stbname11}.ts = {stbname21}.ts and {stbname11}.st1 = {stbname21}.st1")
    tdSql2.error(f"select * from {stbname11}, {stbname21} where {stbname11}.ts = {stbname21}.ts and {stbname11}.st1 = {stbname21}.st1")

if __name__ == '__main__':

    tdSql1.execute('reset query cache')
    tdSql2.execute('reset query cache')
    tdSql1.execute(f'drop database if exists {dbname11}')
    tdSql1.execute(f'drop database if exists {dbname12}')
    tdSql1.execute(f'drop database if exists {dbname21}')
    tdSql2.execute(f'drop database if exists {dbname21}')
    tdSql2.execute(f'drop database if exists {dbname11}')
    tdSql2.execute(f'drop database if exists {dbname12}')

    tdSql1.execute(dbsql(dbname11))
    tdSql1.query('show databases')
    tdSql1.checkRows(1)
    tdSql2.query('show databases')
    tdSql2.checkRows(0)

    tdSql2.execute(dbsql(dbname21))

    tdSql1.query(f'show databases')
    tdSql1.checkData(0, 0, dbname11)
    tdSql2.query(f'show databases')
    tdSql2.checkData(0, 0, dbname21)

    tdSql1.execute(f'use {dbname11}')
    tdSql1.query("show stables")
    tdSql1.checkRows(0)
    tdSql2.error("show stables")


    ### conn1 create stable
    tdSql1.execute(stbsql(stbname11, db1_stb1_column, db1_stb1_tag))
    tdSql1.query(f"show stables like '{stbname11}' ")
    tdSql1.checkRows(1)
    tdSql2.error("show stables")

    # 'st1 int, st2 float, st3 timestamp, st4 binary(16), st5 double, st6 bool, st7 bigint, st8 smallint, st9 tinyint, st10 nchar(16)'
    for i in range(100):
        t1name = f"t{i}"
        stname = stbname11
        tags = f'{i}, {i}, {i}, "binary_{i}", {i}, {i%2}, {i}, {i%32768}, {i%128}, "nchar_{i}"'
        tdSql1.execute(tbsql(t1name, stname, tags))

    tdSql2.error(f'select * from t{random.randint(0, 99)}')

    tdSql1.query("show tables")
    tdSql1.checkRows(100)
    tdSql2.error("show tables")

    tdSql1.query(f'select * from {stbname11}')
    # tdSql1.query(f'select * from t1')
    tdSql1.checkRows(0)
    tdSql2.error(f'select * from {stname}')

    # conn1 insert data
    # 'ts timestamp, c1 int, c2 float, c3 timestamp, c4 binary(16), c5 double, c6 bool, c7 bigint, c8 smallint, c9 tinyint, c10 nchar(16)  '
    nowtime = int(round(time.time() * 1000))
    for i in range(100):
        data = f'{nowtime+i*10}, {i}, {i}, {i}, "binary_{i}", {i}, {i%2}, {i}, {i%32768}, {i%128}, "nchar_{i}"'
        tdSql1.execute(datasql(f"t{i}", data))
        # tdSql2.error(datasql(f't{i}', data))
    ti = random.randint(0,99)
    tdSql1.query(f"select c1, c2, c3, c4, c5, c6, c7, c8, c9, c10 from t{ti}")
    tdSql1.checkData(0, 0, ti)
    tdSql1.checkData(0, 1, ti)
    tdSql1.checkData(0, 2, ti)
    tdSql1.checkData(0, 3, f'binary_{ti}')
    tdSql1.checkData(0, 4, ti)
    tdSql1.checkData(0, 5, ti%2)
    tdSql1.checkData(0, 6, ti)
    tdSql1.checkData(0, 7, ti%32768)
    tdSql1.checkData(0, 8, ti%128)
    tdSql1.checkData(0, 9, f'nchar_{ti}')
    tdSql2.error(f"select c1, c2, c3, c4, c5, c6, c7, c8, c9, c10 from t{ti}")

    # delete conn1.database and reinsert the data to conn1.db and conn2.db
    tdSql1.execute(f"drop database if exists {dbname11}")
    tdSql1.query("show databases")
    tdSql1.checkRows(0)
    tdSql2.query(f"show databases")
    tdSql2.checkData(0, 0, dbname21)

    tdSql1.execute(dbsql(dbname11))
    tdSql1.query(f"show databases")
    tdSql1.checkData(0, 0, dbname11)
    tdSql2.query(f"show databases ")
    tdSql2.checkData(0, 0, dbname21)

    tdSql1.execute(dbsql(dbname12))
    tdSql1.query("show databases")
    tdSql1.checkData(0, 0, dbname11)
    tdSql1.checkData(1, 0, dbname12)
    tdSql2.query("show databases")
    tdSql2.checkData(0, 0, dbname21)

    tdSql1.execute(f"use {dbname11}")
    tdSql1.query("show stables")
    tdSql1.checkRows(0)
    tdSql2.error("show stables")

    tdSql2.execute(f"use {dbname21}")
    tdSql2.query("show stables")
    tdSql2.checkRows(0)
    tdSql2.error(f"use {dbname12}")

    tdSql1.execute(stbsql(stbname11, db1_stb1_column, db1_stb1_tag))
    tdSql1.query("show stables")
    tdSql1.checkRows(1)
    tdSql2.query("show stables")
    tdSql2.checkRows(0)

    tdSql2.execute(stbsql(stbname21, db1_stb1_column, db1_stb1_tag))
    tdSql1.query("show stables ")
    tdSql1.checkRows(1)
    tdSql1.query(f"show stables like '{stbname11}' ")
    tdSql1.checkRows(1)
    tdSql2.query("show stables ")
    tdSql1.checkRows(1)
    tdSql2.query(f"show stables like '{stbname21}' ")
    tdSql1.checkRows(1)

    for i in range(tbnum):
        t1name = f"tm{i}"
        t2name = f"tn{i}"
        s1tname = stbname11
        s2tname = stbname21
        tags = f'{i}, {i}, {i}, "binary_{i}", {i}, {i % 2}, {i}, {i % 32768}, {i % 128}, "nchar_{i}"'
        tdSql1.execute(tbsql(t1name, s1tname, tags))
        # tdSql2.error(f'select * from {t1name}')
        tdSql2.execute(tbsql(t2name, s2tname, tags))
        # tdSql2.query(f'select * from {t2name}')
        # tdSql1.error(f'select * from {t2name}')


    tdSql1.query("show tables like 'tm%' ")
    tdSql1.checkRows(tbnum)
    tdSql2.query("show tables like 'tn%' ")
    tdSql2.checkRows(tbnum)

    for i in range(data_row):
        data1 = f'{nowtime + i * 10}, {i}, {i}, {i}, "binary_{i}", {i}, {i % 2}, {i}, {i % 32768}, {i % 128}, "nchar_{i}"'
        data2 = f'{nowtime+i*10}, {i+10000}, {i+10000}, {i+10000}, "binary_{i+10000}", {i+10000}, {(i+10000)%2}, {i+10000}, {(i+10000)%32768}, {(i+10000)%128}, "nchar_{i+10000}" '
        tdSql1.execute(datasql(f"tm{i}", data1))
        tdSql2.execute(datasql(f'tn{i}', data2))

    testquery()