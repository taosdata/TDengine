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
from datetime import datetime

class DBWriteNonStop:
    def __init(self)__:
        self.host = "127.0.0.1"
        self.user = "root"
        self.password = "taosdata"
        self.config = "/etc/taos"
    
    def init(self, host, user, password, config):
        self.host = host
        self.user = user
        self.password = password
        self.config = config
    
    def connectDB(self, conn, cursor):
        self.conn = taos.connect(self.host, self.user, self.password, self.config)
        self.cursor = conn.cursor()   
        return self.cursor            

    def createTable()


c1.execute('drop database if exists dbwrite')
c1.execute('create database dbwrite')
c1.execute('use dbwrite')
c1.execute('create table if not exists st (ts timestamp, value nchar(50), speed int) tags(dev nchar(50))')

i = 1

startTime = datetime.now()
while True:
    c1.execute("insert into st1 using st tags('dev_001') values(now, 'taosdata%d', %d)" % (i % 10000, i % 100000)) 
    i += 1
    i = i % 32000000
    endTime = datetime.now()
    if (endTime - startTime).seconds >= 5 * 2:
        startTime = endTime
        c1.execute("select count(*) from st")
        data = c1.fetchall()
        print(datetime.now())
        print("total records: %d" % data[0][0])
    time.sleep(.001)