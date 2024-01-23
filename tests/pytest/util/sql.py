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
import traceback
import psutil
import shutil
import pandas as pd
from util.log import *
from util.constant import *

# from datetime import timezone
import time

def _parse_ns_timestamp(timestr):
    dt_obj = datetime.datetime.strptime(timestr[:len(timestr)-3], "%Y-%m-%d %H:%M:%S.%f")
    tz = int(int((dt_obj-datetime.datetime.fromtimestamp(0,dt_obj.tzinfo)).total_seconds())*1e9) + int(dt_obj.microsecond * 1000) + int(timestr[-3:])
    return tz


def _parse_datetime(timestr):
    try:
        return datetime.datetime.strptime(timestr, '%Y-%m-%d %H:%M:%S.%f')
    except ValueError:
        pass
    try:
        return datetime.datetime.strptime(timestr, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        pass

class TDSql:
    def __init__(self):
        self.queryRows = 0
        self.queryCols = 0
        self.affectedRows = 0

    def init(self, cursor, log=True):
        self.cursor = cursor
        print(f"sqllog is :{log}")
        if (log):
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            self.cursor.log(caller.filename + ".sql")

    def close(self):
        self.cursor.close()

    def prepare(self, dbname="db", drop=True, **kwargs):
        tdLog.info(f"prepare database:{dbname}")
        s = 'reset query cache'
        try:
            self.cursor.execute(s)
        except:
            tdLog.notice("'reset query cache' is not supported")
        if drop:
            s = f'drop database if exists {dbname}'
            self.cursor.execute(s)
        s = f'create database {dbname}'
        for k, v in kwargs.items():
            s += f" {k} {v}"
        if "duration" not in kwargs:
            s += " duration 300"
        self.cursor.execute(s)
        s = f'use {dbname}'
        self.cursor.execute(s)
        time.sleep(2)

    def execute(self, sql, queryTimes=30, show=False):
        self.sql = sql
        if show:
            tdLog.info(sql)
        i=1
        while i <= queryTimes:
            try:
                self.affectedRows = self.cursor.execute(sql)
                return self.affectedRows
            except Exception as e:
                tdLog.notice("Try to execute sql again, execute times: %d "%i)
                if i == queryTimes:
                    caller = inspect.getframeinfo(inspect.stack()[1][0])
                    args = (caller.filename, caller.lineno, sql, repr(e))
                    tdLog.notice("%s(%d) failed: sql:%s, %s" % args)
                    raise Exception(repr(e))
                i+=1
                time.sleep(1)
                pass

    def no_error(self, sql):
        caller = inspect.getframeinfo(inspect.stack()[1][0])
        expectErrOccurred = False

        try:
            self.cursor.execute(sql)
        except BaseException as e:
            expectErrOccurred = True
            self.errno = e.errno
            error_info = repr(e)
            self.error_info = ','.join(error_info[error_info.index('(') + 1:-1].split(",")[:-1]).replace("'", "")

        if expectErrOccurred:
            tdLog.exit("%s(%d) failed: sql:%s, unexpect error '%s' occurred" % (caller.filename, caller.lineno, sql, self.error_info))
        else:
            tdLog.info("sql:%s, check passed, no ErrInfo occurred" % (sql))

    def error(self, sql, expectedErrno = None, expectErrInfo = None, fullMatched = True):
        caller = inspect.getframeinfo(inspect.stack()[1][0])
        expectErrNotOccured = True

        try:
            self.cursor.execute(sql)
        except BaseException as e:
            expectErrNotOccured = False
            self.errno = e.errno
            error_info = repr(e)
            self.error_info = ','.join(error_info[error_info.index('(')+1:-1].split(",")[:-1]).replace("'","")
            # self.error_info = (','.join(error_info.split(",")[:-1]).split("(",1)[1:][0]).replace("'","")
        if expectErrNotOccured:
            tdLog.exit("%s(%d) failed: sql:%s, expect error not occured" % (caller.filename, caller.lineno, sql))
        else:
            self.queryRows = 0
            self.queryCols = 0
            self.queryResult = None

            if fullMatched:
                if expectedErrno != None:
                    if expectedErrno == self.errno:
                        tdLog.info("sql:%s, expected errno %s occured" % (sql, expectedErrno))
                    else:
                        tdLog.exit("%s(%d) failed: sql:%s, errno '%s' occured, but not expected errno '%s'" % (caller.filename, caller.lineno, sql, self.errno, expectedErrno))

                if expectErrInfo != None:
                    if expectErrInfo == self.error_info:
                        tdLog.info("sql:%s, expected ErrInfo '%s' occured" % (sql, expectErrInfo))
                    else:
                        tdLog.exit("%s(%d) failed: sql:%s, ErrInfo '%s' occured, but not expected ErrInfo '%s'" % (caller.filename, caller.lineno, sql, self.error_info, expectErrInfo))
            else:
                if expectedErrno != None:
                    if expectedErrno in self.errno:
                        tdLog.info("sql:%s, expected errno %s occured" % (sql, expectedErrno))
                    else:
                        tdLog.exit("%s(%d) failed: sql:%s, errno '%s' occured, but not expected errno '%s'" % (caller.filename, caller.lineno, sql, self.errno, expectedErrno))

                if expectErrInfo != None:
                    if expectErrInfo in self.error_info:
                        tdLog.info("sql:%s, expected ErrInfo '%s' occured" % (sql, expectErrInfo))
                    else:
                        tdLog.exit("%s(%d) failed: sql:%s, ErrInfo %s occured, but not expected ErrInfo '%s'" % (caller.filename, caller.lineno, sql, self.error_info, expectErrInfo))

            return self.error_info

    def query(self, sql, row_tag=None, queryTimes=10, count_expected_res=None):
        self.sql = sql
        i=1
        while i <= queryTimes:
            try:
                self.cursor.execute(sql)
                self.queryResult = self.cursor.fetchall()
                self.queryRows = len(self.queryResult)
                self.queryCols = len(self.cursor.description)

                if count_expected_res is not None:
                    counter = 0
                    while count_expected_res != self.queryResult[0][0]:
                        self.cursor.execute(sql)
                        self.queryResult = self.cursor.fetchall()
                        if counter < queryTimes:
                            counter += 0.5
                            time.sleep(0.5)
                        else:
                            return False
                if row_tag:
                    return self.queryResult
                return self.queryRows
            except Exception as e:
                tdLog.notice("Try to query again, query times: %d "%i)
                if i == queryTimes:
                    caller = inspect.getframeinfo(inspect.stack()[1][0])
                    args = (caller.filename, caller.lineno, sql, repr(e))
                    tdLog.notice("%s(%d) failed: sql:%s, %s" % args)
                    raise Exception(repr(e))
                i+=1
                time.sleep(1)
                pass

    def query_success_failed(self, sql, row_tag=None, queryTimes=10, count_expected_res=None, expectErrInfo = None, fullMatched = True):
        self.sql = sql
        i=1
        while i <= queryTimes:
            try:
                self.cursor.execute(sql)
                self.queryResult = self.cursor.fetchall()
                self.queryRows = len(self.queryResult)
                self.queryCols = len(self.cursor.description)

                if count_expected_res is not None:
                    counter = 0
                    while count_expected_res != self.queryResult[0][0]:
                        self.cursor.execute(sql)
                        self.queryResult = self.cursor.fetchall()
                        if counter < queryTimes:
                            counter += 0.5
                            time.sleep(0.5)
                        else:
                            return False
                        
                tdLog.info("query is success")
                time.sleep(1)
                continue
            except Exception as e:
                tdLog.notice("Try to query again, query times: %d "%i)
                caller = inspect.getframeinfo(inspect.stack()[1][0])
                if i < queryTimes:
                    error_info = repr(e)
                    print(error_info)
                    self.error_info = ','.join(error_info[error_info.index('(')+1:-1].split(",")[:-1]).replace("'","")
                    self.queryRows = 0
                    self.queryCols = 0
                    self.queryResult = None

                    if fullMatched:
                        if expectErrInfo != None:
                            if expectErrInfo == self.error_info:
                                tdLog.info("sql:%s, expected expectErrInfo '%s' occured" % (sql, expectErrInfo))
                            else:
                                tdLog.exit("%s(%d) failed: sql:%s, expectErrInfo '%s' occured, but not expected expectErrInfo '%s'" % (caller.filename, caller.lineno, sql, self.error_info, expectErrInfo))
                    else:
                        if expectErrInfo != None:
                            if expectErrInfo in self.error_info:
                                tdLog.info("sql:%s, expected expectErrInfo '%s' occured" % (sql, expectErrInfo))
                            else:
                                tdLog.exit("%s(%d) failed: sql:%s, expectErrInfo %s occured, but not expected expectErrInfo '%s'" % (caller.filename, caller.lineno, sql, self.error_info, expectErrInfo))

                    return self.error_info                   
                elif i == queryTimes:
                    caller = inspect.getframeinfo(inspect.stack()[1][0])
                    args = (caller.filename, caller.lineno, sql, repr(e))
                    tdLog.notice("%s(%d) failed: sql:%s, %s" % args)
                    raise Exception(repr(e))
                i+=1
                time.sleep(1)
                pass

    def is_err_sql(self, sql):
        err_flag = True
        try:
            self.cursor.execute(sql)
        except BaseException:
            err_flag = False

        return False if err_flag else True

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
            for query_col in self.cursor.description:
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

    def getRows(self):
        return self.queryRows

    def checkRows(self, expectRows):
        if self.queryRows == expectRows:
            tdLog.info("sql:%s, queryRows:%d == expect:%d" % (self.sql, self.queryRows, expectRows))
            return True
        else:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, self.sql, self.queryRows, expectRows)
            tdLog.exit("%s(%d) failed: sql:%s, queryRows:%d != expect:%d" % args)

    def checkRows_range(self, excepte_row_list):
        if self.queryRows in excepte_row_list:
            tdLog.info(f"sql:{self.sql}, queryRows:{self.queryRows} in expect:{excepte_row_list}")
            return True
        else:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            tdLog.exit(f"{caller.filename}({caller.lineno}) failed: sql:{self.sql}, queryRows:{self.queryRows} not in expect:{excepte_row_list}")

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


    def checkData(self, row, col, data, show = False):
        if row >= self.queryRows:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, self.sql, row+1, self.queryRows)
            tdLog.exit("%s(%d) failed: sql:%s, row:%d is larger than queryRows:%d" % args)
        if col >= self.queryCols:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, self.sql, col+1, self.queryCols)
            tdLog.exit("%s(%d) failed: sql:%s, col:%d is larger than queryCols:%d" % args)   
      
        self.checkRowCol(row, col)

        if self.queryResult[row][col] != data:
            if self.cursor.istype(col, "TIMESTAMP"):
                # suppose user want to check nanosecond timestamp if a longer data passed``
                if isinstance(data,str) :
                    if (len(data) >= 28):
                        if self.queryResult[row][col] == _parse_ns_timestamp(data):
                            if(show):
                               tdLog.info("check successfully")
                        else:
                            caller = inspect.getframeinfo(inspect.stack()[1][0])
                            args = (caller.filename, caller.lineno, self.sql, row, col, self.queryResult[row][col], data)
                            tdLog.exit("%s(%d) failed: sql:%s row:%d col:%d data:%s != expect:%s" % args)
                    else:
                        if self.queryResult[row][col].astimezone(datetime.timezone.utc) == _parse_datetime(data).astimezone(datetime.timezone.utc):
                            # tdLog.info(f"sql:{self.sql}, row:{row} col:{col} data:{self.queryResult[row][col]} == expect:{data}")
                            if(show):
                               tdLog.info("check successfully")
                        else:
                            caller = inspect.getframeinfo(inspect.stack()[1][0])
                            args = (caller.filename, caller.lineno, self.sql, row, col, self.queryResult[row][col], data)
                            tdLog.exit("%s(%d) failed: sql:%s row:%d col:%d data:%s != expect:%s" % args)
                    return
                elif isinstance(data,int):
                    if len(str(data)) == 16:
                        precision = 'us'
                    elif len(str(data)) == 13:
                        precision = 'ms'
                    elif len(str(data)) == 19:
                        precision = 'ns'
                    else:
                        caller = inspect.getframeinfo(inspect.stack()[1][0])
                        args = (caller.filename, caller.lineno, self.sql, row, col, self.queryResult[row][col], data)
                        tdLog.exit("%s(%d) failed: sql:%s row:%d col:%d data:%s != expect:%s" % args)
                        return
                    success = False
                    if precision == 'ms':
                        dt_obj = self.queryResult[row][col]
                        ts = int(int((dt_obj-datetime.datetime.fromtimestamp(0,dt_obj.tzinfo)).total_seconds())*1000) + int(dt_obj.microsecond/1000)
                        if ts == data:
                            success = True
                    elif precision == 'us':
                        dt_obj = self.queryResult[row][col]
                        ts = int(int((dt_obj-datetime.datetime.fromtimestamp(0,dt_obj.tzinfo)).total_seconds())*1e6) + int(dt_obj.microsecond)
                        if ts == data:
                            success = True
                    elif precision == 'ns':
                        if data == self.queryResult[row][col]:
                            success = True
                    if success:
                        if(show):
                            tdLog.info("check successfully")
                    else:
                        caller = inspect.getframeinfo(inspect.stack()[1][0])
                        args = (caller.filename, caller.lineno, self.sql, row, col, self.queryResult[row][col], data)
                        tdLog.exit("%s(%d) failed: sql:%s row:%d col:%d data:%s != expect:%s" % args)
                    return
                elif isinstance(data,datetime.datetime):
                    dt_obj = self.queryResult[row][col]
                    delt_data = data-datetime.datetime.fromtimestamp(0,data.tzinfo)
                    delt_result = self.queryResult[row][col] - datetime.datetime.fromtimestamp(0,self.queryResult[row][col].tzinfo)
                    if delt_data == delt_result:
                        if(show):
                            tdLog.info("check successfully")
                    else:
                        caller = inspect.getframeinfo(inspect.stack()[1][0])
                        args = (caller.filename, caller.lineno, self.sql, row, col, self.queryResult[row][col], data)
                        tdLog.exit("%s(%d) failed: sql:%s row:%d col:%d data:%s != expect:%s" % args)
                    return
                else:
                    caller = inspect.getframeinfo(inspect.stack()[1][0])
                    args = (caller.filename, caller.lineno, self.sql, row, col, self.queryResult[row][col], data)
                    tdLog.exit("%s(%d) failed: sql:%s row:%d col:%d data:%s != expect:%s" % args)

            if str(self.queryResult[row][col]) == str(data):
                # tdLog.info(f"sql:{self.sql}, row:{row} col:{col} data:{self.queryResult[row][col]} == expect:{data}")
                if(show):
                    tdLog.info("check successfully")
                return

            elif isinstance(data, float):
                if abs(data) >= 1 and abs((self.queryResult[row][col] - data) / data) <= 0.000001:
                    # tdLog.info(f"sql:{self.sql}, row:{row} col:{col} data:{self.queryResult[row][col]} == expect:{data}")
                    if(show):
                        tdLog.info("check successfully")
                elif abs(data) < 1 and abs(self.queryResult[row][col] - data) <= 0.000001:
                    # tdLog.info(f"sql:{self.sql}, row:{row} col:{col} data:{self.queryResult[row][col]} == expect:{data}")
                    if(show):
                        tdLog.info("check successfully")

                else:
                    caller = inspect.getframeinfo(inspect.stack()[1][0])
                    args = (caller.filename, caller.lineno, self.sql, row, col, self.queryResult[row][col], data)
                    tdLog.exit("%s(%d) failed: sql:%s row:%d col:%d data:%s != expect:%s" % args)
                return
            else:
                caller = inspect.getframeinfo(inspect.stack()[1][0])
                args = (caller.filename, caller.lineno, self.sql, row, col, self.queryResult[row][col], data)
                tdLog.exit("%s(%d) failed: sql:%s row:%d col:%d data:%s != expect:%s" % args)
        if(show):         
            tdLog.info("check successfully")

    # return true or false replace exit, no print out
    def checkRowColNoExit(self, row, col):
        caller = inspect.getframeinfo(inspect.stack()[2][0])
        if row < 0:
            args = (caller.filename, caller.lineno, self.sql, row)
            return False
        if col < 0:
            args = (caller.filename, caller.lineno, self.sql, row)
            return False
        if row > self.queryRows:
            args = (caller.filename, caller.lineno, self.sql, row, self.queryRows)
            return False
        if col > self.queryCols:
            args = (caller.filename, caller.lineno, self.sql, col, self.queryCols)
            return False
            
        return True


    # return true or false replace exit, no print out
    def checkDataNoExit(self, row, col, data):
        if self.checkRowColNoExit(row, col) == False:
            return False
        if self.queryResult[row][col] != data:
            if self.cursor.istype(col, "TIMESTAMP"):
                # suppose user want to check nanosecond timestamp if a longer data passed
                if (len(data) >= 28):
                    if pd.to_datetime(self.queryResult[row][col]) == pd.to_datetime(data):
                        return True
                else:
                    if self.queryResult[row][col] == _parse_datetime(data):
                        return True
                return False

            if str(self.queryResult[row][col]) == str(data):
                return True
            elif isinstance(data, float):
                if abs(data) >= 1 and abs((self.queryResult[row][col] - data) / data) <= 0.000001:
                    return True
                elif abs(data) < 1 and abs(self.queryResult[row][col] - data) <= 0.000001:
                    return True
                else:
                    return False
            else:
                return False
                
        return True


    # loop execute sql then sleep(waitTime) , if checkData ok break loop
    def checkDataLoop(self, row, col, data, sql, loopCount, waitTime):
        # loop check util checkData return true
        for i in range(loopCount):
            self.query(sql)
            if self.checkDataNoExit(row, col, data) :
                self.checkData(row, col, data)
                return
            time.sleep(waitTime)

        # last check
        self.query(sql)
        self.checkData(row, col, data)


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

    def __check_equal(self, elm, expect_elm):
        if elm == expect_elm:
            return True
        if type(elm) in(list, tuple) and type(expect_elm) in(list, tuple):
            if len(elm) != len(expect_elm):
                return False
            if len(elm) == 0:
                return True
            for i in range(len(elm)):
                flag = self.__check_equal(elm[i], expect_elm[i])
                if not flag:
                    return False
            return True
        return False

    def checkEqual(self, elm, expect_elm):
        if elm == expect_elm:
            tdLog.info("sql:%s, elm:%s == expect_elm:%s" % (self.sql, elm, expect_elm))
            return
        if self.__check_equal(elm, expect_elm):
            tdLog.info("sql:%s, elm:%s == expect_elm:%s" % (self.sql, elm, expect_elm))
            return

        caller = inspect.getframeinfo(inspect.stack()[1][0])
        args = (caller.filename, caller.lineno, self.sql, elm, expect_elm)
        # tdLog.info("%s(%d) failed: sql:%s, elm:%s != expect_elm:%s" % args)
        raise Exception("%s(%d) failed: sql:%s, elm:%s != expect_elm:%s" % args)

    def checkNotEqual(self, elm, expect_elm):
        if elm != expect_elm:
            tdLog.info("sql:%s, elm:%s != expect_elm:%s" % (self.sql, elm, expect_elm))
        else:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, self.sql, elm, expect_elm)
            tdLog.info("%s(%d) failed: sql:%s, elm:%s == expect_elm:%s" % args)
            raise Exception

    def get_times(self, time_str, precision="ms"):
        caller = inspect.getframeinfo(inspect.stack()[1][0])
        if time_str[-1] not in TAOS_TIME_INIT:
            tdLog.exit(f"{caller.filename}({caller.lineno}) failed: {time_str} not a standard taos time init")
        if precision not in TAOS_PRECISION:
            tdLog.exit(f"{caller.filename}({caller.lineno}) failed: {precision} not a standard taos time precision")

        if time_str[-1] == TAOS_TIME_INIT[0]:
            times =  int(time_str[:-1]) * TIME_NS
        if time_str[-1] == TAOS_TIME_INIT[1]:
            times =  int(time_str[:-1]) * TIME_US
        if time_str[-1] == TAOS_TIME_INIT[2]:
            times =  int(time_str[:-1]) * TIME_MS
        if time_str[-1] == TAOS_TIME_INIT[3]:
            times =  int(time_str[:-1]) * TIME_S
        if time_str[-1] == TAOS_TIME_INIT[4]:
            times =  int(time_str[:-1]) * TIME_M
        if time_str[-1] == TAOS_TIME_INIT[5]:
            times =  int(time_str[:-1]) * TIME_H
        if time_str[-1] == TAOS_TIME_INIT[6]:
            times =  int(time_str[:-1]) * TIME_D
        if time_str[-1] == TAOS_TIME_INIT[7]:
            times =  int(time_str[:-1]) * TIME_W
        if time_str[-1] == TAOS_TIME_INIT[8]:
            times =  int(time_str[:-1]) * TIME_N
        if time_str[-1] == TAOS_TIME_INIT[9]:
            times =  int(time_str[:-1]) * TIME_Y

        if precision == "ms":
            return int(times)
        elif precision == "us":
            return int(times*1000)
        elif precision == "ns":
            return int(times*1000*1000)

    def get_type(self, col):
        if self.cursor.istype(col, "BOOL"):
            return "BOOL"
        if self.cursor.istype(col, "INT"):
            return "INT"
        if self.cursor.istype(col, "BIGINT"):
            return "BIGINT"
        if self.cursor.istype(col, "TINYINT"):
            return "TINYINT"
        if self.cursor.istype(col, "SMALLINT"):
            return "SMALLINT"
        if self.cursor.istype(col, "FLOAT"):
            return "FLOAT"
        if self.cursor.istype(col, "DOUBLE"):
            return "DOUBLE"
        if self.cursor.istype(col, "BINARY"):
            return "BINARY"
        if self.cursor.istype(col, "NCHAR"):
            return "NCHAR"
        if self.cursor.istype(col, "TIMESTAMP"):
            return "TIMESTAMP"
        if self.cursor.istype(col, "JSON"):
            return "JSON"
        if self.cursor.istype(col, "TINYINT UNSIGNED"):
            return "TINYINT UNSIGNED"
        if self.cursor.istype(col, "SMALLINT UNSIGNED"):
            return "SMALLINT UNSIGNED"
        if self.cursor.istype(col, "INT UNSIGNED"):
            return "INT UNSIGNED"
        if self.cursor.istype(col, "BIGINT UNSIGNED"):
            return "BIGINT UNSIGNED"

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
