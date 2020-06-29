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


class QueryMetaData:
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

    def queryData(self):
        print("===============query tag data===============")
        self.cursor.execute("use test")
        startTime = datetime.now()
        self.cursor.execute("select areaid from meters")
        data = self.cursor.fetchall()
        endTime = datetime.now()
        
        print(endTime - startTime)
        
        start = datetime.now()
        self.cursor.execute("select areaid, loc from meters")
        data2 = self.cursor.fetchall()
        end = datetime.now()
        
        print(end - start)

    def closeConn(self):
        self.cursor.close()
        self.conn.close()


test = QueryMetaData()
test.connectDB()
test.queryData()
test.closeConn()
