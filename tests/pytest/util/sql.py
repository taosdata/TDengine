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
import os
import time
import datetime
from util.log import *


class TDSql:
    def __init__(self):
        self.queryRows = 0
        self.queryCols = 0
        self.affectedRows = 0

    def init(self, cursor):
        self.cursor = cursor

    def close(self):
        self.cursor.close()

    def prepare(self):
        tdLog.info("prepare database:db")
        self.cursor.execute('reset query cache')
        self.cursor.execute('drop database if exists db')
        self.cursor.execute('create database db')
        self.cursor.execute('use db')

    def error(self, sql):
        expectErrNotOccured = True
        try:
            self.cursor.execute(sql)
        except BaseException:
            expectErrNotOccured = False
        if expectErrNotOccured:
            tdLog.exit("failed: sql:%.40s, expect error not occured" % (sql))
        else:
            tdLog.info("sql:%.40s, expect error occured" % (sql))

    def query(self, sql):
        self.sql = sql
        self.cursor.execute(sql)
        self.queryResult = self.cursor.fetchall()
        self.queryRows = len(self.queryResult)
        self.queryCols = len(self.cursor.description)
        # if self.queryRows == 1 and self.queryCols == 1:
        #	tdLog.info("sql:%s, rows:%d cols:%d data:%s" % (self.sql, self.queryRows, self.queryCols, self.queryResult[0][0]))
        # else:
        #	tdLog.info("sql:%s, rows:%d cols:%d" % (self.sql, self.queryRows, self.queryCols))
        return self.queryRows

    def checkRows(self, expectRows):
        if self.queryRows != expectRows:
            tdLog.exit(
                "failed: sql:%.40s, queryRows:%d != expect:%d" %
                (self.sql, self.queryRows, expectRows))
        tdLog.info("sql:%.40s, queryRows:%d == expect:%d" %
                   (self.sql, self.queryRows, expectRows))

    def checkData(self, row, col, data):
        if row < 0:
            tdLog.exit(
                "failed: sql:%.40s, row:%d is smaller than zero" %
                (self.sql, row))
        if col < 0:
            tdLog.exit(
                "failed: sql:%.40s, col:%d is smaller than zero" %
                (self.sql, col))
        if row >= self.queryRows:
            tdLog.exit(
                "failed: sql:%.40s, row:%d is larger than queryRows:%d" %
                (self.sql, row, self.queryRows))
        if col >= self.queryCols:
            tdLog.exit(
                "failed: sql:%.40s, col:%d is larger than queryRows:%d" %
                (self.sql, col, self.queryCols))
        if self.queryResult[row][col] != data:
            tdLog.exit(
                "failed: sql:%.40s row:%d col:%d data:%s != expect:%s" %
                (self.sql, row, col, self.queryResult[row][col], data))

        if data is None:
            tdLog.info("sql:%.40s, row:%d col:%d data:%s == expect:%s" %
                       (self.sql, row, col, self.queryResult[row][col], data))
        elif isinstance(data, str):
            tdLog.info("sql:%.40s, row:%d col:%d data:%s == expect:%s" %
                       (self.sql, row, col, self.queryResult[row][col], data))
        elif isinstance(data, datetime.date):
            tdLog.info("sql:%.40s, row:%d col:%d data:%s == expect:%s" %
                       (self.sql, row, col, self.queryResult[row][col], data))
        else:
            tdLog.info("sql:%.40s, row:%d col:%d data:%s == expect:%d" %
                       (self.sql, row, col, self.queryResult[row][col], data))

    def getData(self, row, col):
        if row < 0:
            tdLog.exit(
                "failed: sql:%.40s, row:%d is smaller than zero" %
                (self.sql, row))
        if col < 0:
            tdLog.exit(
                "failed: sql:%.40s, col:%d is smaller than zero" %
                (self.sql, col))
        if row >= self.queryRows:
            tdLog.exit(
                "failed: sql:%.40s, row:%d is larger than queryRows:%d" %
                (self.sql, row, self.queryRows))
        if col >= self.queryCols:
            tdLog.exit(
                "failed: sql:%.40s, col:%d is larger than queryRows:%d" %
                (self.sql, col, self.queryCols))
        return self.queryResult[row][col]

    def executeTimes(self, sql, times):
        for i in range(times):
            try:
                return self.cursor.execute(sql)
            except BaseException:
                time.sleep(1)
                continue

    def execute(self, sql):
        self.sql = sql
        self.affectedRows = self.cursor.execute(sql)
        return self.affectedRows

    def checkAffectedRows(self, expectAffectedRows):
        if self.affectedRows != expectAffectedRows:
            tdLog.exit("failed: sql:%.40s, affectedRows:%d != expect:%d" %
                       (self.sql, self.affectedRows, expectAffectedRows))
        tdLog.info("sql:%.40s, affectedRows:%d == expect:%d" %
                   (self.sql, self.affectedRows, expectAffectedRows))


tdSql = TDSql()
