###################################################################
#       Copyright (c) 2016 by TAOS Technologies, Inc.
#             All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import sys
import os
import csv 
import random
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self.csvfile = "/tmp/file.csv"
        self.rows = 10000
        self.ntables = 1
        self.startTime = 1520000010000
    def genRandomStr(self, maxLen):
        H = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
        salt = ''
        if maxLen <= 1:
            maxLen = 2
        l = random.randint(1,maxLen)
        for i in range(l):
            salt += random.choice(H)
        return salt
    def createCSVFile(self):
        f = open(self.csvfile,'w',encoding='utf-8') 
        csv_writer = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC) 
        for i in range(self.rows):   
            csv_writer.writerow([self.startTime + i, 
                                self.genRandomStr(5),
                                self.genRandomStr(6),
                                self.genRandomStr(7),
                                self.genRandomStr(8),
                                self.genRandomStr(9),
                                self.genRandomStr(10),
                                self.genRandomStr(11),
                                self.genRandomStr(12),
                                self.genRandomStr(13),
                                self.genRandomStr(14)])
        f.close()
    def destroyCSVFile(self):
        os.remove(self.csvfile) 
    def run(self):
        self.createCSVFile()

        tdDnodes.stop(1)
        tdDnodes.deploy(1)
        tdDnodes.start(1)

        tdSql.execute('reset query cache')
        tdSql.execute('drop database if exists db')
        tdSql.execute('create database db')
        tdSql.execute('use db')
        tdSql.execute('''create table tbx (ts TIMESTAMP, 
                         collect_area NCHAR(5), 
                         device_id BINARY(6), 
                         imsi BINARY(7),  
                         imei BINARY(8),  
                         mdn BINARY(9), 
                         net_type BINARY(10), 
                         mno NCHAR(11), 
                         province NCHAR(12), 
                         city NCHAR(13), 
                         alarm BINARY(14))''')
    
        tdSql.execute("import into tbx file \'%s\'"%(self.csvfile))
        tdSql.query('select * from tbx')
        tdSql.checkRows(self.rows)
        #TD-4447 import the same csv twice
        tdSql.execute("import into tbx file \'%s\'"%(self.csvfile))

    def stop(self):
        self.destroyCSVFile()
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)
        tdDnodes.stop(1)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
