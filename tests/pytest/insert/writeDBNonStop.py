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
from datetime import datetime
import csv


class DBWriteNonStop:
    def __init__(self):
        self.host = "127.0.0.1"
        self.user = "root"
        self.password = "taosdata"
        self.config = "/etc/taos"

    def connectDB(self):
        self.conn = taos.connect(
            self.host,
            self.user,
            self.password,
            self.config)
        self.cursor = self.conn.cursor()

    def createTable(self):
        self.cursor.execute("drop database if exists dbwrite")
        self.cursor.execute("create database dbwrite")
        self.cursor.execute("use dbwrite")
        self.cursor.execute(
            "create table if not exists st (ts timestamp, value nchar(50), speed int) tags(dev nchar(50))")

    def writeDataToCSVFile(self, data, duration):
        csvFile = open('/root/csvFile.csv', 'a', newline='')
        writer = csv.writer(csvFile)
        writer.writerow([data[0][0], data[0][1], data[0][2],
                         data[0][3], data[0][4], data[0][5], duration])
        csvFile.close()

    def insertData(self):
        print("===============inserting data===============")
        i = 1
        startTime = datetime.now()
        while True:
            self.cursor.execute(
                "insert into st1 using st tags('dev_001') values(now, 'taosdata%d', %d)" %
                (i %
                 10000,
                 i %
                 100000))
            i += 1
            i = i % 32000000
            endTime = datetime.now()
            if (endTime - startTime).seconds >= 3600 * 1:
                startTime = endTime
                start = datetime.now()
                self.cursor.execute(
                    "select first(ts), last(ts), min(speed), max(speed), avg(speed), count(*) from st")
                data = self.cursor.fetchall()
                end = datetime.now()
                self.writeDataToCSVFile(data, (end - start).seconds)
            time.sleep(.001)

    def closeConn(self):
        self.cursor.close()
        self.conn.close()


test = DBWriteNonStop()
test.connectDB()
test.createTable()
test.insertData()
test.closeConn()
