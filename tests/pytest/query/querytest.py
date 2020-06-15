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

import taos
import time

conn = taos.connect(host="127.0.0.1", user="root", password="taosdata", config="/etc/taos")
c1 = conn.cursor()

c1.execute('create database db')
c1.execute('use db')

c1.execute('create table if not exists st (ts timestamp, value nchar(50), speed int) tags(dev nchar(50))')

i = 1

while True:
    c1.execute("insert into st1 using st tags('dev_001') values(now, 'taosdata%d', %d)" % (i % 10000, i % 100000)) 
    i += 1
    i = i % 32000000    