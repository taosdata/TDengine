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
            s += " duration 300"
        self.cursor.execute(s)
        s = f'use {dbname}'
        self.cursor.execute(s)
        time.sleep(2)

    def error(self, sql):
        expectErrNotOccured = True
        try:
            self.cursor.execute(sql)
        except BaseException as e:
            expectErrNotOccured = False
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            self.error_info = repr(e)
            # print(error_info)
            # self.error_info = error_info[error_info.index('(')+1:-1].split(",")[0].replace("'","")
            # self.error_info = (','.join(error_info.split(",")[:-1]).split("(",1)[1:][0]).replace("'","")
            # print("!!!!!!!!!!!!!!",self.error_info)
            
        if expectErrNotOccured:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            tdLog.exit("%s(%d) failed: sql:%s, expect error not occured" % (caller.filename, caller.lineno, sql))
        else:
            self.queryRows = 0
            self.queryCols = 0
            self.queryResult = None
            tdLog.info("sql:%s, expect error occured" % (sql))
            return self.error_info
            

    def query(self, sql, row_tag=None,queryTimes=10):
        self.sql = sql
        i=1
        while i <= queryTimes:
            try:
                self.cursor.execute(sql)
                self.queryResult = self.cursor.fetchall()
                self.queryRows = len(self.queryResult)
                self.queryCols = len(self.cursor.description)
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

    def checkData(self, row, col, data):
        self.checkRowCol(row, col)
        if self.queryResult[row][col] != data:
            if self.cursor.istype(col, "TIMESTAMP"):
                # suppose user want to check nanosecond timestamp if a longer data passed
                if (len(data) >= 28):
                    if pd.to_datetime(self.queryResult[row][col]) == pd.to_datetime(data):
                        tdLog.info(f"sql:{self.sql}, row:{row} col:{col} data:{self.queryResult[row][col]} == expect:{data}")
                else:
                    if self.queryResult[row][col] == _parse_datetime(data):
                        tdLog.info(f"sql:{self.sql}, row:{row} col:{col} data:{self.queryResult[row][col]} == expect:{data}")
                return

            if str(self.queryResult[row][col]) == str(data):
                tdLog.info(f"sql:{self.sql}, row:{row} col:{col} data:{self.queryResult[row][col]} == expect:{data}")
                return

            elif isinstance(data, float):
                if abs(data) >= 1 and abs((self.queryResult[row][col] - data) / data) <= 0.000001:
                    tdLog.info(f"sql:{self.sql}, row:{row} col:{col} data:{self.queryResult[row][col]} == expect:{data}")
                elif abs(data) < 1 and abs(self.queryResult[row][col] - data) <= 0.000001:
                    tdLog.info(f"sql:{self.sql}, row:{row} col:{col} data:{self.queryResult[row][col]} == expect:{data}")
                else:
                    caller = inspect.getframeinfo(inspect.stack()[1][0])
                    args = (caller.filename, caller.lineno, self.sql, row, col, self.queryResult[row][col], data)
                    tdLog.exit("%s(%d) failed: sql:%s row:%d col:%d data:%s != expect:%s" % args)
                return
            else:
                caller = inspect.getframeinfo(inspect.stack()[1][0])
                args = (caller.filename, caller.lineno, self.sql, row, col, self.queryResult[row][col], data)
                tdLog.exit("%s(%d) failed: sql:%s row:%d col:%d data:%s != expect:%s" % args)

        tdLog.info(f"sql:{self.sql}, row:{row} col:{col} data:{self.queryResult[row][col]} == expect:{data}")

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

    def execute(self, sql,queryTimes=10):
        self.sql = sql
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
        tdLog.exit("%s(%d) failed: sql:%s, elm:%s != expect_elm:%s" % args)

    def checkNotEqual(self, elm, expect_elm):
        if elm != expect_elm:
            tdLog.info("sql:%s, elm:%s != expect_elm:%s" % (self.sql, elm, expect_elm))
        else:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, self.sql, elm, expect_elm)
            tdLog.exit("%s(%d) failed: sql:%s, elm:%s == expect_elm:%s" % args)

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
