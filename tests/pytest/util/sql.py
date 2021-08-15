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
import inspect
import psutil
import shutil
import pandas as pd
from util.log import *



class TDSql:
    def __init__(self):
        self.queryRows = 0
        self.queryCols = 0
        self.affectedRows = 0

    def init(self, cursor, log=False):
        self.cursor = cursor

        if (log):
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            self.cursor.log(caller.filename + ".sql")

    def close(self):
        self.cursor.close()

    def prepare(self):
        tdLog.info("prepare database:db")
        s = 'reset query cache'
        self.cursor.execute(s)
        s = 'drop database if exists db'
        self.cursor.execute(s)
        s = 'create database db'
        self.cursor.execute(s)
        s = 'use db'
        self.cursor.execute(s)

    def error(self, sql):
        expectErrNotOccured = True
        try:
            self.cursor.execute(sql)
        except BaseException:
            expectErrNotOccured = False
        if expectErrNotOccured:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            tdLog.exit("%s(%d) failed: sql:%s, expect error not occured" % (caller.filename, caller.lineno, sql))
        else:
            self.queryRows = 0
            self.queryCols = 0
            self.queryResult = None
            tdLog.info("sql:%s, expect error occured" % (sql))

    def query(self, sql, row_tag=None):
        self.sql = sql
        try:
            self.cursor.execute(sql)
            self.queryResult = self.cursor.fetchall()
            self.queryRows = len(self.queryResult)
            self.queryCols = len(self.cursor.description)
        except Exception as e:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, sql, repr(e))
            tdLog.notice("%s(%d) failed: sql:%s, %s" % args)
            raise Exception(repr(e))
        if row_tag:
            return self.queryResult
        return self.queryRows

    def getVariable(self, search_attr):
        '''
            get variable of search_attr access "show variables"
        '''
        try:
            sql = 'show variables'
            param_list = self.query(sql, row_tag=True)
            for param in param_list:
                if param[0] == search_attr:
                    return param[1], param_list
        except Exception as e:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, sql, repr(e))
            tdLog.notice("%s(%d) failed: sql:%s, %s" % args)
            raise Exception(repr(e))

    def getColNameList(self, sql, col_tag=None):
        self.sql = sql
        try:
            col_name_list = []
            col_type_list = []
            self.cursor.execute(sql)
            self.queryCols = self.cursor.description
            for query_col in self.queryCols:
                col_name_list.append(query_col[0])
                col_type_list.append(query_col[1])
        except Exception as e:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, sql, repr(e))
            tdLog.notice("%s(%d) failed: sql:%s, %s" % args)
            raise Exception(repr(e))
        if col_tag:
            return col_name_list, col_type_list
        return col_name_list

    def waitedQuery(self, sql, expectRows, timeout):
        tdLog.info("sql: %s, try to retrieve %d rows in %d seconds" % (sql, expectRows, timeout))
        self.sql = sql
        try:
            for i in range(timeout):
                self.cursor.execute(sql)
                self.queryResult = self.cursor.fetchall()
                self.queryRows = len(self.queryResult)
                self.queryCols = len(self.cursor.description)
                tdLog.info("sql: %s, try to retrieve %d rows,get %d rows" % (sql, expectRows, self.queryRows))
                if self.queryRows >= expectRows:
                    return (self.queryRows, i)
                time.sleep(1)
        except Exception as e:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, sql, repr(e))
            tdLog.notice("%s(%d) failed: sql:%s, %s" % args)
            raise Exception(repr(e))
        return (self.queryRows, timeout)

    def checkRows(self, expectRows):
        if self.queryRows == expectRows:
            tdLog.info("sql:%s, queryRows:%d == expect:%d" % (self.sql, self.queryRows, expectRows))
        else:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, self.sql, self.queryRows, expectRows)
            tdLog.exit("%s(%d) failed: sql:%s, queryRows:%d != expect:%d" % args)

    def checkCols(self, expectCols):
        if self.queryCols == expectCols:
            tdLog.info("sql:%s, queryCols:%d == expect:%d" % (self.sql, self.queryCols, expectCols))
        else:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, self.sql, self.queryCols, expectCols)
            tdLog.exit("%s(%d) failed: sql:%s, queryCols:%d != expect:%d" % args)

    def checkRowCol(self, row, col):
        caller = inspect.getframeinfo(inspect.stack()[2][0])
        if row < 0:
            args = (caller.filename, caller.lineno, self.sql, row)
            tdLog.exit("%s(%d) failed: sql:%s, row:%d is smaller than zero" % args)
        if col < 0:
            args = (caller.filename, caller.lineno, self.sql, row)
            tdLog.exit("%s(%d) failed: sql:%s, col:%d is smaller than zero" % args)
        if row > self.queryRows:
            args = (caller.filename, caller.lineno, self.sql, row, self.queryRows)
            tdLog.exit("%s(%d) failed: sql:%s, row:%d is larger than queryRows:%d" % args)
        if col > self.queryCols:
            args = (caller.filename, caller.lineno, self.sql, col, self.queryCols)
            tdLog.exit("%s(%d) failed: sql:%s, col:%d is larger than queryCols:%d" % args)

    def checkDataType(self, row, col, dataType):
        self.checkRowCol(row, col)
        return self.cursor.istype(col, dataType)

    def checkData(self, row, col, data):
        self.checkRowCol(row, col)
        if self.queryResult[row][col] != data:
            if self.cursor.istype(col, "TIMESTAMP"):
                # suppose user want to check nanosecond timestamp if a longer data passed
                if (len(data) >= 28):
                    if pd.to_datetime(self.queryResult[row][col]) == pd.to_datetime(data):
                        tdLog.info("sql:%s, row:%d col:%d data:%d == expect:%s" %
                            (self.sql, row, col, self.queryResult[row][col], data))
                else:
                    if self.queryResult[row][col] == datetime.datetime.fromisoformat(data):
                        tdLog.info("sql:%s, row:%d col:%d data:%s == expect:%s" %
                            (self.sql, row, col, self.queryResult[row][col], data))
                return

            if str(self.queryResult[row][col]) == str(data):
                tdLog.info("sql:%s, row:%d col:%d data:%s == expect:%s" %
                            (self.sql, row, col, self.queryResult[row][col], data))
                return
            elif isinstance(data, float) and abs(self.queryResult[row][col] - data) <= 0.000001:
                tdLog.info("sql:%s, row:%d col:%d data:%f == expect:%f" %
                            (self.sql, row, col, self.queryResult[row][col], data))
                return
            else:
                caller = inspect.getframeinfo(inspect.stack()[1][0])
                args = (caller.filename, caller.lineno, self.sql, row, col, self.queryResult[row][col], data)
                tdLog.exit("%s(%d) failed: sql:%s row:%d col:%d data:%s != expect:%s" % args)

        if data is None:
            tdLog.info("sql:%s, row:%d col:%d data:%s == expect:%s" %
                       (self.sql, row, col, self.queryResult[row][col], data))
        elif isinstance(data, str):
            tdLog.info("sql:%s, row:%d col:%d data:%s == expect:%s" %
                       (self.sql, row, col, self.queryResult[row][col], data))
        elif isinstance(data, datetime.date):
            tdLog.info("sql:%s, row:%d col:%d data:%s == expect:%s" %
                       (self.sql, row, col, self.queryResult[row][col], data))
        elif isinstance(data, float):
            tdLog.info("sql:%s, row:%d col:%d data:%s == expect:%s" %
                       (self.sql, row, col, self.queryResult[row][col], data))
        else:
            tdLog.info("sql:%s, row:%d col:%d data:%s == expect:%d" %
                       (self.sql, row, col, self.queryResult[row][col], data))

    def getData(self, row, col):
        self.checkRowCol(row, col)
        return self.queryResult[row][col]

    def getResult(self, sql):
        self.sql = sql
        try:
            self.cursor.execute(sql)
            self.queryResult = self.cursor.fetchall()
        except Exception as e:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, sql, repr(e))
            tdLog.notice("%s(%d) failed: sql:%s, %s" % args)
            raise Exception(repr(e))
        return self.queryResult

        
    def executeTimes(self, sql, times):
        for i in range(times):
            try:
                return self.cursor.execute(sql)
            except BaseException:
                time.sleep(1)
                continue

    def execute(self, sql):
        self.sql = sql
        try:
            self.affectedRows = self.cursor.execute(sql)
        except Exception as e:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, sql, repr(e))
            tdLog.notice("%s(%d) failed: sql:%s, %s" % args)
            raise Exception(repr(e))
        return self.affectedRows

    def checkAffectedRows(self, expectAffectedRows):
        if self.affectedRows != expectAffectedRows:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, self.sql, self.affectedRows, expectAffectedRows)
            tdLog.exit("%s(%d) failed: sql:%s, affectedRows:%d != expect:%d" % args)

        tdLog.info("sql:%s, affectedRows:%d == expect:%d" % (self.sql, self.affectedRows, expectAffectedRows))

    def checkColNameList(self, col_name_list, expect_col_name_list):
        if col_name_list == expect_col_name_list:
            tdLog.info("sql:%s, col_name_list:%s == expect_col_name_list:%s" % (self.sql, col_name_list, expect_col_name_list))
        else:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, self.sql, col_name_list, expect_col_name_list)
            tdLog.exit("%s(%d) failed: sql:%s, col_name_list:%s != expect_col_name_list:%s" % args)

    def checkEqual(self, elm, expect_elm):
        if elm == expect_elm:
            tdLog.info("sql:%s, elm:%s == expect_elm:%s" % (self.sql, elm, expect_elm))
        else:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, self.sql, elm, expect_elm)
            tdLog.exit("%s(%d) failed: sql:%s, elm:%s != expect_elm:%s" % args)

    def checkNotEqual(self, elm, expect_elm):
        if elm != expect_elm:
            tdLog.info("sql:%s, elm:%s != expect_elm:%s" % (self.sql, elm, expect_elm))
        else:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, self.sql, elm, expect_elm)
            tdLog.exit("%s(%d) failed: sql:%s, elm:%s == expect_elm:%s" % args)

    def taosdStatus(self, state):
        tdLog.sleep(5)
        pstate = 0
        for i in range(30):
            pstate = 0
            pl = psutil.pids()
            for pid in pl:
                try:
                    if psutil.Process(pid).name() == 'taosd':
                        print('have already started')
                        pstate = 1
                        break
                except psutil.NoSuchProcess:
                    pass
            if pstate == state :break
            if state or pstate:
                tdLog.sleep(1)
                continue
            pstate = 0
            break

        args=(pstate,state)
        if pstate == state:
            tdLog.info("taosd state is %d == expect:%d" %args)
        else:
            tdLog.exit("taosd state is %d != expect:%d" %args)
        pass

    def haveFile(self, dir, state):
        if os.path.exists(dir) and os.path.isdir(dir):
            if not os.listdir(dir):
                if state :
                    tdLog.exit("dir: %s is empty, expect: not empty" %dir)
                else:
                    tdLog.info("dir: %s is empty, expect: empty" %dir)
            else:
                if state :
                    tdLog.info("dir: %s is not empty, expect: not empty" %dir)
                else:
                    tdLog.exit("dir: %s is not empty, expect: empty" %dir)
        else:
            tdLog.exit("dir: %s doesn't exist" %dir)
    def createDir(self, dir):
        if os.path.exists(dir):
            shutil.rmtree(dir)
            tdLog.info("dir: %s is removed" %dir)
        os.makedirs( dir, 755 )
        tdLog.info("dir: %s is created" %dir)
        pass

tdSql = TDSql()