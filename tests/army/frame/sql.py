﻿###################################################################
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
from frame.log import *
from frame.constant import *

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
        self.csvLine = 0

    def init(self, cursor, log=False):
        self.cursor = cursor

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
            s += " duration 100"
        self.cursor.execute(s)
        s = f'use {dbname}'
        self.cursor.execute(s)
        time.sleep(2)


    #
    #  do execute
    #

    def errors(self, sql_list, expected_error_id_list=None, expected_error_info_list=None):
        """Execute the sql query and check the error info, expected error id or info should keep the same order with sql list,
        expected_error_id_list or expected_error_info_list is None, then the error info will not be checked.
        :param sql_list: the sql list to be executed.
        :param expected_error_id: the expected error number.
        :param expected_error_info: the expected error info.
        :return: None
        """
        try:
            if len(sql_list) > 0:
                for i in range(len(sql_list)):
                    if expected_error_id_list and expected_error_info_list:
                        self.error(sql_list[i], expected_error_id_list[i], expected_error_info_list[i])
                    elif expected_error_id_list:
                        self.error(sql_list[i], expectedErrno=expected_error_id_list[i])
                    elif expected_error_info_list:
                        self.error(sql_list[i], expectErrInfo=expected_error_info_list[i])
                    else:
                        self.error(sql_list[i])
            else:
                tdLog.exit("sql list is empty")
        except Exception as ex:
            tdLog.exit("Failed to execute sql list: %s, error: %s" % (sql_list, ex))

    def queryAndCheckResult(self, sql_list, expect_result_list):
        """Execute the sql query and check the result.
        :param sql_list: the sql list to be executed.
        :param expect_result_list: the expected result list.
        :return: None
        """
        try:
            for index in range(len(sql_list)):
                self.query(sql_list[index])
                if len(expect_result_list[index]) == 0:
                    self.checkRows(0)
                else:
                    self.checkRows(len(expect_result_list[index]))
                    for row in range(len(expect_result_list[index])):
                        for col in range(len(expect_result_list[index][row])):
                            self.checkData(row, col, expect_result_list[index][row][col])
        except Exception as ex:
            raise(ex)

    def query(self, sql, row_tag=None, queryTimes=10, count_expected_res=None):
        self.sql = sql
        i=1
        while i <= queryTimes:
            try:
                self.cursor.execute(sql)
                self.res = self.cursor.fetchall()
                self.queryRows = len(self.res)
                self.queryCols = len(self.cursor.description)

                if count_expected_res is not None:
                    counter = 0
                    while count_expected_res != self.res[0][0]:
                        self.cursor.execute(sql)
                        self.res = self.cursor.fetchall()
                        if counter < queryTimes:
                            counter += 0.5
                            time.sleep(0.5)
                        else:
                            return False
                if row_tag:
                    return self.res
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

    def executeTimes(self, sql, times):
        for i in range(times):
            try:
                return self.cursor.execute(sql)
            except BaseException:
                time.sleep(1)
                continue

    def execute(self, sql, queryTimes=10, show=False):
        self.sql = sql
        if show:
            tdLog.info(sql)
        i=1
        while i <= queryTimes:
            try:
                self.affectedRows = self.cursor.execute(sql)
                return self.affectedRows
            except Exception as e:
                tdLog.notice("Try to execute sql again, query times: %d "%i)
                if i == queryTimes:
                    caller = inspect.getframeinfo(inspect.stack()[1][0])
                    args = (caller.filename, caller.lineno, sql, repr(e))
                    tdLog.notice("%s(%d) failed: sql:%s, %s" % args)
                    raise Exception(repr(e))
                i+=1
                time.sleep(1)
                pass

    # execute many sql
    def executes(self, sqls, queryTimes=30, show=False):
        for sql in sqls:
            self.execute(sql, queryTimes, show)

    def waitedQuery(self, sql, expectRows, timeout):
        tdLog.info("sql: %s, try to retrieve %d rows in %d seconds" % (sql, expectRows, timeout))
        self.sql = sql
        try:
            for i in range(timeout):
                self.cursor.execute(sql)
                self.res = self.cursor.fetchall()
                self.queryRows = len(self.res)
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

    def is_err_sql(self, sql):
        err_flag = True
        try:
            self.cursor.execute(sql)
        except BaseException:
            err_flag = False

        return False if err_flag else True

    def error(self, sql, expectedErrno = None, expectErrInfo = None):
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
            self.res = None

            if expectedErrno != None:
                if  expectedErrno == self.errno:
                    tdLog.info("sql:%s, expected errno %s occured" % (sql, expectedErrno))
                else:
                  tdLog.exit("%s(%d) failed: sql:%s, errno %s occured, but not expected errno %s" % (caller.filename, caller.lineno, sql, self.errno, expectedErrno))
            else:
              tdLog.info("sql:%s, expect error occured" % (sql))

            if expectErrInfo != None:
                if  expectErrInfo == self.error_info or expectErrInfo in self.error_info:
                    tdLog.info("sql:%s, expected expectErrInfo %s occured" % (sql, expectErrInfo))
                else:
                  tdLog.exit("%s(%d) failed: sql:%s, expectErrInfo %s occured, but not expected errno %s" % (caller.filename, caller.lineno, sql, self.error_info, expectErrInfo))

            return self.error_info

    #
    #  get session
    #

    def getData(self, row, col):
        self.checkRowCol(row, col)
        return self.res[row][col]
    
    def getColData(self, col):
        colDatas = []
        for i in range(self.queryRows):
            colDatas.append(self.res[i][col])
        return colDatas

    def getResult(self, sql):
        self.sql = sql
        try:
            self.cursor.execute(sql)
            self.res = self.cursor.fetchall()
        except Exception as e:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, sql, repr(e))
            tdLog.notice("%s(%d) failed: sql:%s, %s" % args)
            raise Exception(repr(e))
        return self.res

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

    def getRows(self):
        return self.queryRows

    # get first value
    def getFirstValue(self, sql) :
        self.query(sql)
        return self.getData(0, 0)

    
    #
    #  check session
    #

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

        if self.res[row][col] != data:
            if self.cursor.istype(col, "TIMESTAMP"):
                # suppose user want to check nanosecond timestamp if a longer data passed``
                if isinstance(data,str) :
                    if (len(data) >= 28):
                        if self.res[row][col] == _parse_ns_timestamp(data):
                            if(show):
                               tdLog.info("check successfully")
                        else:
                            caller = inspect.getframeinfo(inspect.stack()[1][0])
                            args = (caller.filename, caller.lineno, self.sql, row, col, self.res[row][col], data)
                            tdLog.exit("%s(%d) failed: sql:%s row:%d col:%d data:%s != expect:%s" % args)
                    else:
                        print(f"{self.res[row][col]}")
                        real = self.res[row][col]
                        if real is None:
                            # none
                            if str(real) == data:
                                if(show):
                                    tdLog.info("check successfully")
                        elif real.astimezone(datetime.timezone.utc) == _parse_datetime(data).astimezone(datetime.timezone.utc):
                            # tdLog.info(f"sql:{self.sql}, row:{row} col:{col} data:{self.res[row][col]} == expect:{data}")
                            if(show):
                               tdLog.info("check successfully")
                        else:
                            caller = inspect.getframeinfo(inspect.stack()[1][0])
                            args = (caller.filename, caller.lineno, self.sql, row, col, self.res[row][col], data)
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
                        args = (caller.filename, caller.lineno, self.sql, row, col, self.res[row][col], data)
                        tdLog.exit("%s(%d) failed: sql:%s row:%d col:%d data:%s != expect:%s" % args)
                        return
                    success = False
                    if precision == 'ms':
                        dt_obj = self.res[row][col]
                        ts = int(int((dt_obj-datetime.datetime.fromtimestamp(0,dt_obj.tzinfo)).total_seconds())*1000) + int(dt_obj.microsecond/1000)
                        if ts == data:
                            success = True
                    elif precision == 'us':
                        dt_obj = self.res[row][col]
                        ts = int(int((dt_obj-datetime.datetime.fromtimestamp(0,dt_obj.tzinfo)).total_seconds())*1e6) + int(dt_obj.microsecond)
                        if ts == data:
                            success = True
                    elif precision == 'ns':
                        if data == self.res[row][col]:
                            success = True
                    if success:
                        if(show):
                            tdLog.info("check successfully")
                    else:
                        caller = inspect.getframeinfo(inspect.stack()[1][0])
                        args = (caller.filename, caller.lineno, self.sql, row, col, self.res[row][col], data)
                        tdLog.exit("%s(%d) failed: sql:%s row:%d col:%d data:%s != expect:%s" % args)
                    return
                elif isinstance(data,datetime.datetime):
                    dt_obj = self.res[row][col]
                    delt_data = data-datetime.datetime.fromtimestamp(0,data.tzinfo)
                    delt_result = self.res[row][col] - datetime.datetime.fromtimestamp(0,self.res[row][col].tzinfo)
                    if delt_data == delt_result:
                        if(show):
                            tdLog.info("check successfully")
                    else:
                        caller = inspect.getframeinfo(inspect.stack()[1][0])
                        args = (caller.filename, caller.lineno, self.sql, row, col, self.res[row][col], data)
                        tdLog.exit("%s(%d) failed: sql:%s row:%d col:%d data:%s != expect:%s" % args)
                    return
                else:
                    caller = inspect.getframeinfo(inspect.stack()[1][0])
                    args = (caller.filename, caller.lineno, self.sql, row, col, self.res[row][col], data)
                    tdLog.exit("%s(%d) failed: sql:%s row:%d col:%d data:%s != expect:%s" % args)

            if str(self.res[row][col]) == str(data):
                # tdLog.info(f"sql:{self.sql}, row:{row} col:{col} data:{self.res[row][col]} == expect:{data}")
                if(show):
                    tdLog.info("check successfully")
                return

            elif isinstance(data, float):
                if abs(data) >= 1 and abs((self.res[row][col] - data) / data) <= 0.000001:
                    # tdLog.info(f"sql:{self.sql}, row:{row} col:{col} data:{self.res[row][col]} == expect:{data}")
                    if(show):
                        tdLog.info("check successfully")
                elif abs(data) < 1 and abs(self.res[row][col] - data) <= 0.000001:
                    # tdLog.info(f"sql:{self.sql}, row:{row} col:{col} data:{self.res[row][col]} == expect:{data}")
                    if(show):
                        tdLog.info("check successfully")

                else:
                    caller = inspect.getframeinfo(inspect.stack()[1][0])
                    args = (caller.filename, caller.lineno, self.sql, row, col, self.res[row][col], data)
                    tdLog.exit("%s(%d) failed: sql:%s row:%d col:%d data:%s != expect:%s" % args)
                return
            else:
                caller = inspect.getframeinfo(inspect.stack()[1][0])
                args = (caller.filename, caller.lineno, self.sql, row, col, self.res[row][col], data)
                tdLog.exit("%s(%d) failed: sql:%s row:%d col:%d data:%s != expect:%s" % args)
        if(show):         
            tdLog.info("check successfully")

    def checkDataMem(self, sql, mem):
        self.query(sql)
        if not isinstance(mem, list):
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, self.sql)
            tdLog.exit("%s(%d) failed: sql:%s, expect data is error, must is array[][]" % args)

        if len(mem) != self.queryRows:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, self.sql, len(mem), self.queryRows)
            tdLog.exit("%s(%d) failed: sql:%s, row:%d is larger than queryRows:%d" % args)
        # row, col, data
        for row, rowData in enumerate(mem):
            for col, colData in enumerate(rowData):
                self.checkData(row, col, colData)
        tdLog.info("check successfully")

    def checkDataCsv(self, sql, csvfilePath):
        if not isinstance(csvfilePath, str) or len(csvfilePath) == 0:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, self.sql, csvfilePath)
            tdLog.exit("%s(%d) failed: sql:%s, expect csvfile path error:%s" % args)

        tdLog.info("read csvfile read begin")
        data = []
        try:
            with open(csvfilePath) as csvfile:
                csv_reader = csv.reader(csvfile)  # csv.reader read csvfile\
                # header = next(csv_reader)        # Read the header of each column in the first row
                for row in csv_reader:  # csv file save to data
                    data.append(row)
        except FileNotFoundError:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, self.sql, csvfilePath)
            tdLog.exit("%s(%d) failed: sql:%s, expect csvfile not find error:%s" % args)
        except Exception as e:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, self.sql, csvfilePath, str(e))
            tdLog.exit("%s(%d) failed: sql:%s, expect csvfile path:%s, read error:%s" % args)

        tdLog.info("read csvfile read successfully")
        self.checkDataMem(sql, data)

    def checkDataMemByLine(self, sql, mem):
        if not isinstance(mem, list):
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, self.sql)
            tdLog.exit("%s(%d) failed: sql:%s, expect data is error, must is array[][]" % args)

        if len(mem) != self.queryRows:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, self.sql, len(mem), self.queryRows)
            tdLog.exit("%s(%d) failed: sql:%s, row:%d is larger than queryRows:%d" % args)
        # row, col, data
        for row, rowData in enumerate(mem):
            for col, colData in enumerate(rowData):
                self.checkData(row, col, colData)
        tdLog.info("check %s successfully" %sql)

    def checkDataCsvByLine(self, sql, csvfilePath):
        if not isinstance(csvfilePath, str) or len(csvfilePath) == 0:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, self.sql, csvfilePath)
            tdLog.exit("%s(%d) failed: sql:%s, expect csvfile path error:%s" % args)
        self.query(sql)
        data = []
        tdLog.info("check line %d start" %self.csvLine)
        try:
            with open(csvfilePath) as csvfile:
                skip_rows = self.csvLine
                # 计算需要读取的行数
                num_rows = self.queryRows
                # 读取指定范围的行
                df = pd.read_csv(csvfilePath, skiprows=skip_rows, nrows=num_rows, header=None)
                for index, row in df.iterrows():
                    data.append(row)
                self.csvLine += self.queryRows
        except FileNotFoundError:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, self.sql, csvfilePath)
            tdLog.exit("%s(%d) failed: sql:%s, expect csvfile not find error:%s" % args)
        except Exception as e:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, self.sql, csvfilePath, str(e))
            tdLog.exit("%s(%d) failed: sql:%s, expect csvfile path:%s, read error:%s" % args)
        self.checkDataMemByLine(sql, data)

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
        if self.res[row][col] != data:
            if self.cursor.istype(col, "TIMESTAMP"):
                # suppose user want to check nanosecond timestamp if a longer data passed
                if (len(data) >= 28):
                    if pd.to_datetime(self.res[row][col]) == pd.to_datetime(data):
                        return True
                else:
                    if self.res[row][col] == _parse_datetime(data):
                        return True
                return False

            if str(self.res[row][col]) == str(data):
                return True
            elif isinstance(data, float):
                if abs(data) >= 1 and abs((self.res[row][col] - data) / data) <= 0.000001:
                    return True
                elif abs(data) < 1 and abs(self.res[row][col] - data) <= 0.000001:
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

    # check like select count(*) ...  sql
    def checkAgg(self, sql, expectCnt):
        self.query(sql)
        self.checkData(0, 0, expectCnt)
        tdLog.info(f"{sql} expect {expectCnt} ok.")
    
    # expect first value
    def checkFirstValue(self, sql, expect):
        self.query(sql)
        self.checkData(0, 0, expect)

    # colIdx1 value same with colIdx2
    def checkSameColumn(self, c1, c2):
        for i in range(self.queryRows):
            if self.res[i][c1] != self.res[i][c2]:
                tdLog.exit(f"Not same. row={i} col1={c1} col2={c2}. {self.res[i][c1]}!={self.res[i][c2]}")
        tdLog.info(f"check {self.queryRows} rows two column value same. column index [{c1},{c2}]")

    #
    # others session
    #

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
        
    def check_query_col_data(self, sql1, sql2, colNum):
        """
        Execute sql1 and store the colNum-th column of each row in an array.
        Execute sql2 and compare the corresponding column of the result with the previously stored result.
        Return True if they are the same, otherwise return False.

        Args:
            sql1 (str): The first SQL query to execute.
            sql2 (str): The second SQL query to execute.
            colNum (int): The column number to compare (0-based index).

        Returns:
            bool: True if the colNum-th column of the results of sql1 and sql2 are the same, otherwise False.
        """

        # Execute sql1 and store the colNum-th column of each row in an array
        self.cursor.execute(sql1)
        result1 = self.cursor.fetchall()
        col1_data = [row[colNum] for row in result1]

        # Execute sql2 and compare the colNum-th column of the result with the previously stored result
        self.cursor.execute(sql2)
        result2 = self.cursor.fetchall()
        col2_data = [row[colNum] for row in result2]

        # Compare the two arrays
        if col1_data == col2_data:
            return
        else:
            tdLog.info(f"[sql1]:{sql1}, [sql2]:{sql2}, col:{colNum} {col1_data} != {col2_data}")
            raise Exception

    '''
    def taosdStatus(self, state):
        tdLog.sleep(5)
        pstate = 0
        for i in range(30):
            pstate = 0
            pl = pspids()
            for pid in pl:
                try:
                    if psProcess(pid).name() == 'taosd':
                        print('have already started')
                        pstate = 1
                        break
                except psNoSuchProcess:
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
            shrmtree(dir)
            tdLog.info("dir: %s is removed" %dir)
        os.makedirs( dir, 755 )
        tdLog.info("dir: %s is created" %dir)
        pass
'''        

tdSql = TDSql()
