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
import re
import pandas as pd
import csv
from .log import *
from .constant import *
import ctypes
import random
import datetime
import time
import taos
import platform
from tzlocal import get_localzone
from typing import Optional, Literal


def _parse_ns_timestamp(timestr):
    dt_obj = datetime.datetime.strptime(
        timestr[: len(timestr) - 3], "%Y-%m-%d %H:%M:%S.%f"
    )
    tz = (
        int(
            int(
                (
                    dt_obj - datetime.datetime.fromtimestamp(0, dt_obj.tzinfo)
                ).total_seconds()
            )
            * 1e9
        )
        + int(dt_obj.microsecond * 1000)
        + int(timestr[-3:])
    )
    return tz


def _parse_datetime(timestr):
    """
    Parse a string to a datetime object. The string can be in one of the following formats:
    The string can be in one of the following formats:
    - '%Y-%m-%d %H:%M:%S.%f%z': Contains microseconds and timezone offset.
    - '%Y-%m-%d %H:%M:%S%z': Contains no microseconds but contains timezone offset.
    - '%Y-%m-%d %H:%M:%S.%f': Contains microseconds but no timezone offset.
    - '%Y-%m-%d %H:%M:%S': Contains no microseconds and no timezone offset.

    Args:
        timestr (str): The string to be parsed.

    Returns:
        datetime.datetime: The datetime object parsed from the string.
    """
    formats = [
        "%Y-%m-%d %H:%M:%S.%f%z",  # 包含微秒和时区偏移
        "%Y-%m-%d %H:%M:%S%z",  # 不包含微秒但包含时区偏移
        "%Y-%m-%d %H:%M:%S.%f",  # 包含微秒
        "%Y-%m-%d %H:%M:%S",  # 不包含微秒
    ]

    for fmt in formats:
        try:
            # try to parse the string with the current format
            dt = datetime.datetime.strptime(timestr, fmt)
            # 如果字符串包含时区信息，则返回 aware 对象
            # if sting contains timezone info, return aware object
            if dt.tzinfo is not None:
                return dt

            else:
                # if sting does not contain timezone info, assume it is in local timezone
                # get local timezone
                local_timezone = get_localzone()
                # print("Timezone:", local_timezone)
                return dt.replace(tzinfo=local_timezone)
        except ValueError:
            continue  # if the current format does not match, try the next format

    # 如果所有格式都不匹配，返回 None
    # if none of the formats match, return
    raise ValueError(
        f"input format does not match. correct formats include: '{', '.join(formats)}'"
    )

def _fast_caller(depth: int = 1):
    try:
        f = sys._getframe(depth)
        return f.f_code.co_filename, f.f_lineno
    except Exception:
        fr = inspect.getframeinfo(inspect.stack()[depth][0])
        return fr.filename, fr.lineno

class TDSql:
    def __init__(self):
        self.queryRows = 0
        self.queryCols = 0
        self.affectedRows = 0
        self.csvLine = 0
        self.replica = 1

    def init(self, cursor, log=False):
        """
        Initializes the TDSql instance with a database cursor and optionally enables logging.

        Args:
            cursor: The database cursor to be used for executing SQL queries.
            log (bool, optional): If True, enables logging of SQL statements to a file. Defaults to False.

        Returns:
            None

        Raises:
            None
        """
        self.cursor = cursor
        self.sql = None

        if log:
            filename, _ = _fast_caller(1)
            self.cursor.log(filename + ".sql")

    def close(self):
        """
        Closes the cursor.

        Args:
            None

        Returns:
            None

        Raises:
            None
        """
        self.cursor.close()

    def connect(self, username="root", passwd="taosdata", **kwargs):
        """
        Reconnect

        Args:
            username (str):The username used to log in to the cluster.
            passwd (str, optional): The password used to log in to the cluster.

        Returns:
            None

        Raises:
            None

        """

        self.cursor.close()
        if not kwargs:
            kwargs = {
                'user': username,
                'password': passwd,
            }
        else:
            if 'user' not in kwargs:
                kwargs['user'] = username
            if 'password' not in kwargs:
                kwargs['password'] = passwd
        tdLog.debug(f"connect to {username}:{passwd} with kwargs: {kwargs}")

        testconn = taos.connect(**kwargs)
        self.cursor = testconn.cursor()

    def prepare(self, dbname="db", drop=True, **kwargs):
        """
        Prepares the database by optionally dropping it if it exists, creating it, and setting it as the active database.

        Args:
            dbname (str, optional): The name of the database to be prepared. Defaults to "db".
            drop (bool, optional): If True, drops the database if it exists before creating it. Defaults to True.
            **kwargs: Additional keyword arguments to be included in the database creation statement. If duration is not provided, it defaults to 100.

        Returns:
            None

        Raises:
            None
        """
        tdLog.debug(f"prepare database:{dbname}")
        s = "reset query cache"
        try:
            self.cursor.execute(s)
        except:
            tdLog.notice("'reset query cache' is not supported")
        if drop:
            s = f"drop database if exists {dbname}"
            self.cursor.execute(s)
        s = f"create database {dbname}"
        for k, v in kwargs.items():
            if isinstance(v, str):
                s += f" {k} '{v}'"
            else:
                s += f" {k} {v}"

        if "duration" not in kwargs:
            s += " duration 100"
        if "replica" not in kwargs:
            s += f" replica {self.replica}"
        tdLog.debug(f"create database cmd: {s}")
        self.cursor.execute(s)

        s = f"use {dbname}"
        self.cursor.execute(s)
        time.sleep(2)

    def queryAndCheckResult(self, sql_list, expect_result_list, dbPrecision=""):
        """
        Executes a list of SQL queries and checks the results against the expected results.

        Args:
            sql_list (list): The list of SQL queries to be executed.
            expect_result_list (list): The list of expected results corresponding to each SQL query.

        Returns:
            None

        Raises:
            Exception: If the execution of any SQL query fails or if the results do not match the expected results.
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
                            self.checkData(
                                row, col, expect_result_list[index][row][col], dbPrecision=dbPrecision
                            )
        except Exception as ex:
            raise (ex)

    def query(
        self,
        sql,
        row_tag=None,
        queryTimes=10,
        count_expected_res=None,
        show=False,
        exit=True,
    ):
        """
        Executes a SQL query and fetches the results.

        Args:
            sql (str): The SQL query to be executed.
            row_tag (optional): If provided, the method will return the fetched results. Defaults to None.
            queryTimes (int, optional): The number of times to attempt the query in case of failure. Defaults to 10.
            count_expected_res (optional): If provided, the method will repeatedly execute the query until the first result matches this value or the queryTimes limit is reached. Defaults to None.
            show (bool, optional): If True, the SQL statement will be logged before execution. Defaults to False.

        Returns:
            int: The number of rows fetched if row_tag is not provided.
            list: The fetched results if row_tag is provided.

        Raises:
            Exception: If the query fails after the specified number of attempts.
        """
        if show:
            tdLog.info(sql)
        self.sql = sql
        i = 1
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
                if i == queryTimes:
                    if exit:
                        filename, lineno = _fast_caller(1)
                        args = (filename, lineno, sql, repr(e))
                        tdLog.error("%s(%d) failed: sql:%s, %s" % args)
                        raise
                    else:
                        return False
                if exit:
                    tdLog.notice("Try to query again, query times: %d " % i)
                i += 1
                time.sleep(1)
                pass

    def querySuccessailed(
        self,
        sql,
        row_tag=None,
        queryTimes=10,
        count_expected_res=None,
        expectErrInfo=None,
        fullMatched=True,
    ):
        self.sql = sql
        i = 1
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
                tdLog.notice("Try to query again, query times: %d " % i)
                filename, lineno = _fast_caller(1)
                if i < queryTimes:
                    error_info = repr(e)
                    print(error_info)
                    self.error_info = ",".join(
                        error_info[error_info.index("(") + 1 : -1].split(",")[:-1]
                    ).replace("'", "")
                    self.queryRows = 0
                    self.queryCols = 0
                    self.queryResult = None

                    if fullMatched:
                        if expectErrInfo != None:
                            if expectErrInfo == self.error_info:
                                tdLog.info(
                                    "sql:%s, expected expectErrInfo '%s' occured"
                                    % (sql, expectErrInfo)
                                )
                            else:
                                tdLog.exit(
                                    "%s(%d) failed: sql:%s, expectErrInfo '%s' occured, but not expected expectErrInfo '%s'"
                                    % (
                                        filename,
                                        lineno,
                                        sql,
                                        self.error_info,
                                        expectErrInfo,
                                    )
                                )
                    else:
                        if expectErrInfo != None:
                            if expectErrInfo in self.error_info:
                                tdLog.info(
                                    "sql:%s, expected expectErrInfo '%s' occured"
                                    % (sql, expectErrInfo)
                                )
                            else:
                                tdLog.exit(
                                    "%s(%d) failed: sql:%s, expectErrInfo %s occured, but not expected expectErrInfo '%s'"
                                    % (
                                        filename,
                                        lineno,
                                        sql,
                                        self.error_info,
                                        expectErrInfo,
                                    )
                                )

                    return self.error_info
                elif i == queryTimes:
                    filename, lineno = _fast_caller(1)
                    args = (filename, lineno, sql, repr(e))
                    tdLog.notice("%s(%d) failed: sql:%s, %s" % args)
                    raise Exception(repr(e))
                i += 1
                time.sleep(1)
                pass

    def executeTimes(self, sql, times):
        """
        Executes a SQL statement a specified number of times.(Not used)

        Args:
            sql (str): The SQL statement to be executed.
            times (int): The number of times to execute the SQL statement.

        Returns:
            int: The number of affected rows from the last execution.

        Raises:
            None
        """
        for i in range(times):
            try:
                return self.cursor.execute(sql)
            except BaseException:
                time.sleep(1)
                continue

    def execute(self, sql, queryTimes=10, show=False):
        """
        Executes a SQL statement.

        Args:
            sql (str): The SQL statement to be executed.
            queryTimes (int, optional): The number of times to attempt the execution in case of failure. Defaults to 10.
            show (bool, optional): If True, the SQL statement will be logged before execution. Defaults to False.

        Returns:
            int: The number of affected rows.

        Raises:
            Exception: If the execution fails after the specified number of attempts.
        """
        self.sql = sql
        if show:
            tdLog.info(sql)
        i = 1
        while i <= queryTimes:
            try:
                self.affectedRows = self.cursor.execute(sql)
                return self.affectedRows
            except Exception as e:
                tdLog.notice("Try to execute sql again, execute times: %d " % i)
                if i == queryTimes:
                    filename, lineno = _fast_caller(1)
                    args = (filename, lineno, sql, repr(e))
                    tdLog.notice("%s(%d) failed: sql:%s, %s" % args)
                    raise Exception(repr(e))
                i += 1
                time.sleep(1)
                pass

    # execute many sql
    def executes(self, sqls, queryTimes=30, show=False):
        """
        Executes a list of SQL statements.

        Args:
            sqls (list): The list of SQL statements to be executed.
            queryTimes (int, optional): The number of times to attempt the execution in case of failure. Defaults to 30.
            show (bool, optional): If True, each SQL statement will be logged before execution. Defaults to False.

        Returns:
            None

        Raises:
            Exception: If the execution of any SQL statement fails after the specified number of attempts.
        """
        for sql in sqls:
            self.execute(sql, queryTimes, show)

    def waitedQuery(self, sql, expectedRows, timeout):
        """
        Executes a SQL query and waits until the expected number of rows is retrieved or the timeout is reached.

        Args:
            sql (str): The SQL query to be executed.
            expectedRows (int): The expected number of rows to be retrieved.
            timeout (int): The maximum time to wait (in seconds) for the expected number of rows to be retrieved.

        Returns:
            tuple: A tuple containing the number of rows retrieved and the time taken (in seconds).

        Raises:
            Exception: If the query execution fails.
        """
        tdLog.info(
            "sql: %s, try to retrieve %d rows in %d seconds"
            % (sql, expectedRows, timeout)
        )
        self.sql = sql
        try:
            for i in range(timeout):
                self.cursor.execute(sql)
                self.queryResult = self.cursor.fetchall()
                self.queryRows = len(self.queryResult)
                self.queryCols = len(self.cursor.description)
                tdLog.info(
                    "sql: %s, try to retrieve %d rows,get %d rows"
                    % (sql, expectedRows, self.queryRows)
                )
                if self.queryRows >= expectedRows:
                    return (self.queryRows, i)
                time.sleep(1)
        except Exception as e:
            filename, lineno = _fast_caller(1)
            args = (filename, lineno, sql, repr(e))
            tdLog.notice("%s(%d) failed: sql:%s, %s" % args)
            raise Exception(repr(e))
        return (self.queryRows, timeout)

    def query_success_failed(self, sql, row_tag=None, queryTimes=10, count_expected_res=None, expectErrInfo = None, fullMatched = True):
        """Executes a SQL query with retry mechanism and handles both successful and failed scenarios.

        This method attempts to execute a SQL query multiple times, handling both successful
        executions and expected error conditions. It's particularly useful for testing
        scenarios where queries might initially fail but eventually succeed, or for
        validating specific error conditions.

        Args:
            sql (str): The SQL query statement to be executed.
            row_tag (optional): If provided, the method will return the fetched results 
                            instead of just the row count. Defaults to None.
            queryTimes (int, optional): Maximum number of retry attempts if the query fails.
                                    Defaults to 10.
            count_expected_res (optional): If provided, the method will repeatedly execute 
                                        the query until the first result matches this value 
                                        or retry limit is reached. Defaults to None.
            expectErrInfo (str, optional): Expected error message to validate against when 
                                        query fails. If None, any error is acceptable. 
                                        Defaults to None.
            fullMatched (bool, optional): If True, performs exact string matching for error 
                                        messages. If False, performs partial string matching 
                                        (contains). Defaults to True.

        Returns:
            str: Error information string if an expected error occurs and query fails
                within retry attempts.
            None: If query succeeds or if unexpected error occurs and reaches retry limit.

        Raises:
            Exception: If query fails after all retry attempts and the error is not expected
                    or doesn't match the expected error pattern.
        """
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
                                filename, lineno = _fast_caller(1)
                                tdLog.exit("%s(%d) failed: sql:%s, expectErrInfo '%s' occured, but not expected expectErrInfo '%s'" % (filename, lineno, sql, self.error_info, expectErrInfo))
                    else:
                        if expectErrInfo != None:
                            if expectErrInfo in self.error_info:
                                tdLog.info("sql:%s, expected expectErrInfo '%s' occured" % (sql, expectErrInfo))
                            else:
                                filename, lineno = _fast_caller(1)
                                tdLog.exit("%s(%d) failed: sql:%s, expectErrInfo %s occured, but not expected expectErrInfo '%s'" % (filename, lineno, sql, self.error_info, expectErrInfo))

                    return self.error_info                   
                elif i == queryTimes:
                    filename, lineno = _fast_caller(1)
                    args = (filename, lineno, sql, repr(e))
                    tdLog.notice("%s(%d) failed: sql:%s, %s" % args)
                    raise Exception(repr(e))
                i+=1
                time.sleep(1)
                pass
            
    def isErrorSql(self, sql):
        """
        Executes a SQL statement and checks if it results in an error.(Not used)

        Args:
            sql (str): The SQL statement to be executed.

        Returns:
            bool: True if the SQL statement results in an error, False otherwise.

        Raises:
            None
        """
        err_flag = True
        try:
            self.cursor.execute(sql)
        except BaseException:
            err_flag = False

        return False if err_flag else True

    def errors(
        self, sql_list, expected_error_id_list=None, expected_error_info_list=None
    ):
        """
        Executes a list of SQL queries and checks for expected errors.

        Args:
            sql_list (list): The list of SQL queries to be executed.
            expected_error_id_list (list, optional): The list of expected error numbers corresponding to each SQL query. Defaults to None.
            expected_error_info_list (list, optional): The list of expected error information corresponding to each SQL query. Defaults to None.

        Returns:
            None

        Raises:
            SystemExit: If the SQL list is empty, if the execution of any SQL query fails, if the expected error does not occur, or if the error information does not match the expected information.
        """
        try:
            if len(sql_list) > 0:
                for i in range(len(sql_list)):
                    if expected_error_id_list and expected_error_info_list:
                        self.error(
                            sql_list[i],
                            expected_error_id_list[i],
                            expected_error_info_list[i],
                        )
                    elif expected_error_id_list:
                        self.error(sql_list[i], expectedErrno=expected_error_id_list[i])
                    elif expected_error_info_list:
                        self.error(
                            sql_list[i], expectErrInfo=expected_error_info_list[i]
                        )
                    else:
                        self.error(sql_list[i])
            else:
                tdLog.exit("sql list is empty")
        except Exception as ex:
            tdLog.exit("Failed to execute sql list: %s, error: %s" % (sql_list, ex))

    def is_err_sql(self, sql):
        """Checks if a SQL statement will result in an error when executed.

        This method executes the provided SQL statement and determines whether it 
        causes an exception. It's useful for testing error conditions and validating
        that certain SQL statements should fail.

        Args:
            sql (str): The SQL statement to be tested for errors.

        Returns:
            bool: False if the SQL statement executes successfully without errors,
                True if the SQL statement results in an error/exception.

        Raises:
            None: This method catches all exceptions internally and returns a boolean
                result instead of raising exceptions.
        """
        err_flag = True
        try:
            self.cursor.execute(sql)
        except BaseException:
            err_flag = False

        return False if err_flag else True

    def no_error(self, sql):
        """_summary_

        Args:
            sql (_type_): _description_
        """
        expectErrOccurred = False

        try:
            self.cursor.execute(sql)
        except BaseException as e:
            expectErrOccurred = True
            self.errno = e.errno
            error_info = repr(e)
            self.error_info = ','.join(error_info[error_info.index('(') + 1:-1].split(",")[:-1]).replace("'", "")

        if expectErrOccurred:
            filename, lineno = _fast_caller(1)
            tdLog.exit("%s(%d) failed: sql:%s, unexpect error '%s' occurred" % (filename, lineno, sql, self.error_info))
        else:
            tdLog.info("sql:%s, check passed, no ErrInfo occurred" % (sql))

    def error(
        self, sql, expectedErrno=None, expectErrInfo=None, fullMatched=True, show=False
    ):
        """
        Executes a SQL statement and checks for expected errors.

        Args:
            sql (str): The SQL statement to be executed.
            expectedErrno (int, optional): The expected error number. Defaults to None.
            expectErrInfo (str, optional): The expected error information. Defaults to None.
            fullMatched (bool, optional): If True, checks for exact matches of the expected error information. Defaults to True.
            show (bool, optional): If True, the SQL statement will be logged before execution. Defaults to False.

        Returns:
            str: The error information if an error occurs.

        Raises:
            SystemExit: If the expected error does not occur or if the error information does not match the expected information.
        """
        expectErrNotOccured = True
        if show:
            tdLog.info(sql)

        try:
            self.cursor.execute(sql)
            self.queryResult = self.cursor.fetchall()
            self.queryRows = len(self.queryResult)
            self.queryCols = len(self.cursor.description)
        except BaseException as e:
            expectErrNotOccured = False
            self.errno = e.errno
            error_info = repr(e)
            self.error_info = ",".join(
                error_info[error_info.index("(") + 1 : -1].split(",")[:-1]
            ).replace("'", "")
            # self.error_info = (','.join(error_info.split(",")[:-1]).split("(",1)[1:][0]).replace("'","")
        if expectErrNotOccured:
            filename, lineno = _fast_caller(1)
            tdLog.exit(
                "%s(%d) failed: sql:%s, expect error not occured"
                % (filename, lineno, sql)
            )
        else:
            self.queryRows = 0
            self.queryCols = 0
            self.queryResult = None
            if fullMatched:
                if expectedErrno != None:
                    expectedErrno_rest = expectedErrno & 0x0000FFFF
                    if expectedErrno == self.errno or expectedErrno_rest == self.errno:
                        tdLog.info(
                            "sql:%s, expected errno %s occured" % (sql, expectedErrno)
                        )
                    else:
                        tdLog.exit(
                            "%s(%d) failed: sql:%s, errno '%s' occured, but not expected errno '%s'"
                            % (
                                filename,
                                lineno,
                                sql,
                                self.errno,
                                expectedErrno,
                            )
                        )

                if expectErrInfo != None:
                    if expectErrInfo == self.error_info:
                        tdLog.info(
                            "sql:%s, expected ErrInfo '%s' occured"
                            % (sql, expectErrInfo)
                        )
                    else:
                        tdLog.exit(
                            "%s(%d) failed: sql:%s, ErrInfo '%s' occured, but not expected ErrInfo '%s'"
                            % (
                                filename,
                                lineno,
                                sql,
                                self.error_info,
                                expectErrInfo,
                            )
                        )
            else:
                if expectedErrno != None:
                    expectedErrno_rest = expectedErrno & 0x0000FFFF
                    if expectedErrno in self.errno or expectedErrno_rest in self.errno:
                        tdLog.info(
                            "sql:%s, expected errno %s occured" % (sql, expectedErrno)
                        )
                    else:
                        tdLog.exit(
                            "%s(%d) failed: sql:%s, errno '%s' occured, but not expected errno '%s'"
                            % (
                                filename,
                                lineno,
                                sql,
                                self.errno,
                                expectedErrno,
                            )
                        )

                if expectErrInfo != None:
                    if expectErrInfo in self.error_info:
                        tdLog.info(
                            "sql:%s, expected ErrInfo '%s' occured"
                            % (sql, expectErrInfo)
                        )
                    else:
                        tdLog.exit(
                            "%s(%d) failed: sql:%s, ErrInfo %s occured, but not expected ErrInfo '%s'"
                            % (
                                filename,
                                lineno,
                                sql,
                                self.error_info,
                                expectErrInfo,
                            )
                        )

            return self.error_info

    # loop call error
    def waitError(self, sql, loop=3, sleepMs=1000, expectedErrno=None, expectErrInfo=None, fullMatched=True, show=False):
        for i in range(loop):
            self.error(sql, expectedErrno, expectErrInfo, fullMatched, show)
            time.sleep(sleepMs / 1000)
    

    def noError(self, sql):
        unexpectErrOccurred = False

        try:
            self.cursor.execute(sql)
        except BaseException as e:
            unexpectErrOccurred = True
            self.errno = e.errno
            error_info = repr(e)
            self.error_info = ",".join(
                error_info[error_info.index("(") + 1 : -1].split(",")[:-1]
            ).replace("'", "")

        if unexpectErrOccurred:
            filename, lineno = _fast_caller(1)
            tdLog.exit(
                "%s(%d) failed: sql:%s, unexpect error '%s' occurred"
                % (filename, lineno, sql, self.error_info)
            )
        else:
            tdLog.info("sql:%s, check passed, no ErrInfo occurred" % (sql))


    def getData(self, row, col):
        """
        Retrieves the data at the specified row and column from the last query result.

        Args:
            row (int): The row index of the data to be retrieved.
            col (int): The column index of the data to be retrieved.

        Returns:
            The data at the specified row and column.

        Raises:
            SystemExit: If the specified row or column is out of range.
        """
        self.checkRowCol(row, col)
        return self.queryResult[row][col]
    
    def getDataWithOutCheck(self, row, col):
        """
        Retrieves the data at the specified row and column from the last query result.
        Args:
            row (int): The row index of the data to be retrieved.
            col (int): The column index of the data to be retrieved.
        Returns:
            The data at the specified row and column.
        Raises:
            IndexError: If the specified row or column is out of range.
        """
        return self.queryResult[row][col]

    def getColData(self, col):
        """
        Retrieves all data from the specified column in the last query result.

        Args:
            col (int): The column index of the data to be retrieved.

        Returns:
            list: A list containing all data from the specified column.

        Raises:
            None
        """
        colDatas = []
        for i in range(self.queryRows):
            colDatas.append(self.queryResult[i][col])
        return colDatas

    def getRowData(self, row):
        """
        Retrieves all data from the specified row in the last query result.

        Args:
            row (int): The row index of the data to be retrieved.

        Returns:
            list: A list containing all data from the specified row.

        Raises:
            SystemExit: If the specified row is out of range.
        """
        if row >= self.queryRows:
            return None
        
        return self.queryResult[row]

    def getResult(self, sql, exit=True):
        """
        Executes a SQL query and fetches the results.

        Args:
            sql (str): The SQL query to be executed.

        Returns:
            list: The fetched results.

        Raises:
            Exception: If the query execution fails.
        """
        self.sql = sql
        try:
            self.cursor.execute(sql)
            self.queryResult = self.cursor.fetchall()
        except Exception as e:
            if exit:
                filename, lineno = _fast_caller(1)
                args = (filename, lineno, sql, repr(e))
                tdLog.notice("%s(%d) failed: sql:%s, %s" % args)
                raise Exception(repr(e))
            else:
                return []
        return self.queryResult

    def getVariable(self, search_attr):
        """
        Retrieves the value of a specified variable from the database.

        Args:
            search_attr (str): The name of the variable to be retrieved.

        Returns:
            tuple: A tuple containing the value of the specified variable and the list of all variables.

        Raises:
            Exception: If the query execution fails.
        """
        try:
            sql = "show variables"
            param_list = self.query(sql, row_tag=True)
            for param in param_list:
                if param[0] == search_attr:
                    return param[1], param_list
        except Exception as e:
            filename, lineno = _fast_caller(1)
            args = (filename, lineno, sql, repr(e))
            tdLog.notice("%s(%d) failed: sql:%s, %s" % args)
            raise Exception(repr(e))

    def getColNameList(self, sql, col_tag=None):
        """
        Executes a SQL query and retrieves the column names and optionally the column types.

        Args:
            sql (str): The SQL query to be executed.
            col_tag (optional): If provided, the method will return both column names and column types. Defaults to None.

        Returns:
            list: A list containing the column names.
            tuple: A tuple containing two lists - the column names and the column types, if col_tag is provided.

        Raises:
            Exception: If the query execution fails.
        """
        self.sql = sql
        try:
            col_name_list = []
            col_type_list = []
            self.cursor.execute(sql)
            for query_col in self.cursor.description:
                col_name_list.append(query_col[0])
                col_type_list.append(query_col[1])
        except Exception as e:
            filename, lineno = _fast_caller(1)
            args = (filename, lineno, sql, repr(e))
            tdLog.notice("%s(%d) failed: sql:%s, %s" % args)
            raise Exception(repr(e))
        if col_tag:
            return col_name_list, col_type_list
        return col_name_list

    def getRows(self):
        """
        Retrieves the number of rows fetched by the last query.

        Args:
            None

        Returns:
            int: The number of rows fetched by the last query.

        Raises:
            None
        """
        return self.queryRows
    
    def getCols(self):
        """
        Retrieves the number of cols fetched by the last query.

        Args:
            None

        Returns:
            int: The number of cols fetched by the last query.

        Raises:
            None
        """
        return self.queryCols

    # get first value
    def getFirstValue(self, sql):
        """
        Executes a SQL query and retrieves the first value in the result.

        Args:
            sql (str): The SQL query to be executed.

        Returns:
            The first value in the result.

        Raises:
            Exception: If the query execution fails.
        """
        self.query(sql)
        return self.getData(0, 0)

    #
    #  check session
    #

    def checkRows(self, expectedRows, show=False):
        """
        Checks if the number of rows fetched by the last query matches the expected number of rows.

        Args:
            expectedRows (int): The expected number of rows.

        Returns:
            bool: True if the number of rows matches the expected number, otherwise it exits the program.

        Raises:
            SystemExit: If the number of rows does not match the expected number.
        """
        return self.checkEqual(self.queryRows, expectedRows, show=show)

    def checkRowsV2(
        self,
        expectedRows: int,
        operator: Literal["<", "<=", ">", ">=", "==", "!="] = "==",
        show: bool = True,
    ) -> bool:
        """
        Verify if the number of rows returned by SQL query meets the expected condition.

        Args:
            expectedRows : int
                The expected number of rows to compare against
            operator : str, optional
                Comparison operator ('<', '<=', '>', '>=', '==', '!='),
                defaults to '<'
            show : bool, optional
                Whether to print the verification result, defaults to True

        Returns:
            bool
                True if the actual row count meets the expected condition,
                False otherwise

        Raises:
            ValueError: If invalid operator is provided

        Usage:
            assert checker.checkRows(15, operator="<")  # Verify if less than 15 rows
        """
        actualRows = self.queryRows

        try:
            # Perform comparison based on operator
            if operator == "<":
                result = actualRows < expectedRows
            elif operator == "<=":
                result = actualRows <= expectedRows
            elif operator == ">":
                result = actualRows > expectedRows
            elif operator == ">=":
                result = actualRows >= expectedRows
            elif operator == "==":
                result = actualRows == expectedRows
            elif operator == "!=":
                result = actualRows != expectedRows
            else:
                raise ValueError(f"Unsupported comparison operator: {operator}")

            if show:
                tdLog.info(
                    f"Actual rows: {actualRows}, expected rows: {expectedRows}, comparison: {operator}, result: {'PASS' if result else 'FAIL'}"
                )
            return result

        except Exception as e:
            tdLog.error(f"checkRows failed: {str(e)}")
            return False

    def checkRowsNotExited(self, expectedRows):
        """
            Check if the query rows is equal to the expected rows

        Args:
            expectedRows: The expected number of rows.

        Returns:
            bool: Returns True if the actual number of rows matches the expected number, otherwise returns False.
        """
        if self.queryRows == expectedRows:
            return True
        else:
            return False

    def checkRowsRange(self, excepte_row_list):
        """
        Checks if the number of rows fetched by the last query is within the expected range.(Not used)

        Args:
            excepte_row_list (list): A list of expected row counts.

        Returns:
            bool: True if the number of rows is within the expected range, otherwise it exits the program.

        Raises:
            SystemExit: If the number of rows is not within the expected range.
        """
        if self.queryRows in excepte_row_list:
            tdLog.info(
                f"sql:{self.sql}, queryRows:{self.queryRows} in expect:{excepte_row_list}"
            )
            return True
        else:
            filename, lineno = _fast_caller(1)
            tdLog.exit(
                f"{filename}({lineno}) failed: sql:{self.sql}, queryRows:{self.queryRows} not in expect:{excepte_row_list}"
            )

    def checkCols(self, expectCols):
        """
        Checks if the number of columns fetched by the last query matches the expected number of columns.

        Args:
            expectCols (int): The expected number of columns.

        Returns:
            None

        Raises:
            SystemExit: If the number of columns does not match the expected number.
        """
        if self.queryCols == expectCols:
            tdLog.info(
                "sql:%s, queryCols:%d == expect:%d"
                % (self.sql, self.queryCols, expectCols)
            )
        else:
            filename, lineno = _fast_caller(1)
            args = (
                filename,
                lineno,
                self.sql,
                self.queryCols,
                expectCols,
            )
            tdLog.exit("%s(%d) failed: sql:%s, queryCols:%d != expect:%d" % args)

    def checkRowCol(self, row, col):
        """
        Checks if the specified row and column indices are within the range of the last query result.

        Args:
            row (int): The row index to be checked.
            col (int): The column index to be checked.

        Returns:
            None

        Raises:
            SystemExit: If the specified row or column index is out of range.
        """
        if row < 0:
            filename, lineno = _fast_caller(2)
            args = (filename, lineno, self.sql, row)
            tdLog.exit("%s(%d) failed: sql:%s, row:%d is smaller than zero" % args)
        if col < 0:
            filename, lineno = _fast_caller(2)
            args = (filename, lineno, self.sql, row)
            tdLog.exit("%s(%d) failed: sql:%s, col:%d is smaller than zero" % args)
        if row > self.queryRows:
            filename, lineno = _fast_caller(2)
            args = (filename, lineno, self.sql, row, self.queryRows)
            tdLog.exit(
                "%s(%d) failed: sql:%s, row:%d is larger than queryRows:%d" % args
            )
        if col > self.queryCols:
            filename, lineno = _fast_caller(2)
            args = (filename, lineno, self.sql, col, self.queryCols)
            tdLog.exit(
                "%s(%d) failed: sql:%s, col:%d is larger than queryCols:%d" % args
            )

    def checkDataType(self, row, col, dataType):
        """
        Checks if the data type at the specified row and column matches the expected data type.

        Args:
            row (int): The row index of the data to be checked.
            col (int): The column index of the data to be checked.
            dataType (str): The expected data type.

        Returns:
            bool: True if the data type matches the expected data type, otherwise False.

        Raises:
            SystemExit: If the specified row or column index is out of range.
        """
        self.checkRowCol(row, col)
        return self.cursor.istype(col, dataType)

    def checkFloatString(self, row, col, data, show=False):
        if row >= self.queryRows:
            filename, lineno = _fast_caller(1)
            args = (filename, lineno, self.sql, row + 1, self.queryRows)
            tdLog.exit(
                "%s(%d) failed: sql:%s, row:%d is larger than queryRows:%d" % args
            )
        if col >= self.queryCols:
            filename, lineno = _fast_caller(1)
            args = (filename, lineno, self.sql, col + 1, self.queryCols)
            tdLog.exit(
                "%s(%d) failed: sql:%s, col:%d is larger than queryCols:%d" % args
            )

        self.checkRowCol(row, col)

        val = float(self.queryResult[row][col])
        if abs(data) >= 1 and abs((val - data) / data) <= 0.000001:
            if show:
                tdLog.info("check successfully")
        elif abs(data) < 1 and abs(val - data) <= 0.000001:
            if show:
                tdLog.info("check successfully")
        else:
            filename, lineno = _fast_caller(1)
            args = (
                filename,
                lineno,
                self.sql,
                row,
                col,
                self.queryResult[row][col],
                data,
            )
            tdLog.exit(
                "%s(%d) failed: sql:%s row:%d col:%d data:%s != expect:%s" % args
            )

    def compareData(self, row, col, data, show=False):
        return self.checkData(row, col, data, show, False)

    def checkData(self, row, col, data, show=False, exit=True, dbPrecision=""):
        """
        Checks if the data at the specified row and column matches the expected data.

        Args:
            row (int): The row index of the data to be checked.
            col (int): The column index of the data to be checked.
            data: The expected data to be compared with.
            show (bool, optional): If True, logs a message when the check is successful. Defaults to False.

        Returns:
            None

        Raises:
            SystemExit: If the data at the specified row and column does not match the expected data.
        """
        if row >= self.queryRows:
            if exit:
                filename, lineno = _fast_caller(1)
                args = (
                    filename,
                    lineno,
                    self.sql,
                    row + 1,
                    self.queryRows,
                )
                tdLog.exit(
                    "%s(%d) failed: sql:%s, row:%d is larger than queryRows:%d" % args
                )
            else:
                return False
        if col >= self.queryCols:
            if exit:
                filename, lineno = _fast_caller(1)
                args = (
                    filename,
                    lineno,
                    self.sql,
                    col + 1,
                    self.queryCols,
                )
                tdLog.exit(
                    "%s(%d) failed: sql:%s, col:%d is larger than queryCols:%d" % args
                )
            else:
                return False

        self.checkRowCol(row, col)

        if self.queryResult[row][col] != data:
            if self.cursor.istype(col, "TIMESTAMP"):
                # suppose user want to check nanosecond timestamp if a longer data passed``
                if isinstance(data, str):
                    if len(data) >= 28:
                        if self.queryResult[row][col] == _parse_ns_timestamp(data):
                            if show:
                                tdLog.info("check successfully")
                        else:
                            if exit:
                                filename, lineno = _fast_caller(1)
                                args = (
                                    filename,
                                    lineno,
                                    self.sql,
                                    row,
                                    col,
                                    self.queryResult[row][col],
                                    data,
                                )
                                tdLog.exit(
                                    "%s(%d) failed: sql:%s row:%d col:%d data:%s != expect:%s"
                                    % args
                                )
                            else:
                                return False
                    else:
                        # print(f"{self.queryResult[row][col]}")
                        real = self.queryResult[row][col]
                        if real is None:
                            # none
                            if str(real) == data:
                                if show:
                                    tdLog.info("check successfully")
                        elif isinstance(real, datetime.datetime):
                            if platform.system().lower() == "windows":
                                dt_expected = _parse_datetime(data)
                                # 补齐 tzinfo，避免 Windows astimezone 报错
                                if real.tzinfo is None:
                                    real = real.replace(tzinfo=datetime.timezone.utc)
                                if dt_expected.tzinfo is None:
                                    dt_expected = dt_expected.replace(tzinfo=datetime.timezone.utc)
                                if real.astimezone(datetime.timezone.utc) == dt_expected.astimezone(datetime.timezone.utc):
                                    if show:
                                        tdLog.info("check successfully")
                            else:
                                if real.astimezone(datetime.timezone.utc) == _parse_datetime(
                                    data
                                ).astimezone(datetime.timezone.utc):
                                    if show:
                                        tdLog.info("check successfully")
                                else:
                                    if exit:
                                        caller = inspect.getframeinfo(
                                            inspect.stack()[1][0]
                                        )
                                        args = (
                                            caller.filename,
                                            caller.lineno,
                                            self.sql,
                                            row,
                                            col,
                                            self.queryResult[row][col],
                                            data,
                                        )
                                        tdLog.exit(
                                            "%s(%d) failed: sql:%s row:%d col:%d data:%s != expect:%s"
                                            % args
                                        )
                                    else:
                                        return False
                        else:
                            if exit:
                                filename, lineno = _fast_caller(1)
                                args = (
                                    filename,
                                    lineno,
                                    self.sql,
                                    row,
                                    col,
                                    self.queryResult[row][col],
                                    data,
                                )
                                tdLog.exit(
                                    "%s(%d) failed: sql:%s row:%d col:%d data:%s != expect:%s"
                                    % args
                                )
                            else:
                                return False
                    return True
                elif isinstance(data, int):
                    if(dbPrecision == ""):
                        if len(str(data)) == 16:
                            precision = "us"
                        elif len(str(data)) == 13:
                            precision = "ms"
                        elif len(str(data)) == 19:
                            precision = "ns"
                        else:
                            if exit:
                                filename, lineno = _fast_caller(1)
                                args = (
                                    filename,
                                    lineno,
                                    self.sql,
                                    row,
                                    col,
                                    self.queryResult[row][col],
                                    data,
                                )
                                tdLog.exit(
                                    f"%s(%d) failed: sql:%s row:%d col:%d len(str(data)):{len(str(data))} data:%s != expect:%s"
                                    % args
                                )
                            else:
                                return False
                    else:
                        precision = dbPrecision
                    success = False
                    if precision == "ms":
                        dt_obj = self.queryResult[row][col]
                        ts = int(
                            int(
                                (
                                    dt_obj
                                    - datetime.datetime.fromtimestamp(0, dt_obj.tzinfo)
                                ).total_seconds()
                            )
                            * 1000
                        ) + int(dt_obj.microsecond / 1000)
                        if ts == data:
                            success = True
                    elif precision == "us":
                        dt_obj = self.queryResult[row][col]
                        ts = int(
                            int(
                                (
                                    dt_obj
                                    - datetime.datetime.fromtimestamp(0, dt_obj.tzinfo)
                                ).total_seconds()
                            )
                            * 1e6
                        ) + int(dt_obj.microsecond)
                        if ts == data:
                            success = True
                    elif precision == "ns":
                        if data == self.queryResult[row][col]:
                            success = True
                    if success:
                        if show:
                            tdLog.info("check successfully")
                        return True
                    else:
                        if exit:
                            filename, lineno = _fast_caller(1)
                            args = (
                                filename,
                                lineno,
                                self.sql,
                                row,
                                col,
                                self.queryResult[row][col],
                                data,
                            )
                            tdLog.exit(
                                "%s(%d) failed: sql:%s row:%d col:%d data:%s != expect:%s"
                                % args
                            )
                        else:
                            return False
                elif isinstance(data, datetime.datetime):
                    dt_obj = self.queryResult[row][col]
                    delt_data = data - datetime.datetime.fromtimestamp(0, data.tzinfo)
                    delt_result = self.queryResult[row][
                        col
                    ] - datetime.datetime.fromtimestamp(
                        0, self.queryResult[row][col].tzinfo
                    )
                    if delt_data == delt_result:
                        if show:
                            tdLog.info("check successfully")
                        return True
                    else:
                        if exit:
                            filename, lineno = _fast_caller(1)
                            args = (
                                filename,
                                lineno,
                                self.sql,
                                row,
                                col,
                                self.queryResult[row][col],
                                data,
                            )
                            tdLog.exit(
                                "%s(%d) failed: sql:%s row:%d col:%d data:%s != expect:%s"
                                % args
                            )
                        return False
                else:
                    if exit:
                        filename, lineno = _fast_caller(1)
                        args = (
                            filename,
                            lineno,
                            self.sql,
                            row,
                            col,
                            self.queryResult[row][col],
                            data,
                        )
                        tdLog.exit(
                            "%s(%d) failed: sql:%s row:%d col:%d data:%s != expect:%s"
                            % args
                        )
                    else:
                        return False

            if str(self.queryResult[row][col]) == str(data):
                # tdLog.info(f"sql:{self.sql}, row:{row} col:{col} data:{self.queryResult[row][col]} == expect:{data}")
                if show:
                    tdLog.info("check successfully")
                return True

            elif isinstance(data, float):
                if (
                    abs(data) >= 1
                    and abs((self.queryResult[row][col] - data) / data) <= 0.000001
                ):
                    # tdLog.info(f"sql:{self.sql}, row:{row} col:{col} data:{self.queryResult[row][col]} == expect:{data}")
                    if show:
                        tdLog.info("check successfully")
                elif (
                    abs(data) < 1 and abs(self.queryResult[row][col] - data) <= 0.000001
                ):
                    # tdLog.info(f"sql:{self.sql}, row:{row} col:{col} data:{self.queryResult[row][col]} == expect:{data}")
                    if show:
                        tdLog.info("check successfully")

                else:
                    if exit:
                        filename, lineno = _fast_caller(1)
                        args = (
                            filename,
                            lineno,
                            self.sql,
                            row,
                            col,
                            self.queryResult[row][col],
                            data,
                        )
                        tdLog.exit(
                            "%s(%d) failed: sql:%s row:%d col:%d data:%s != expect:%s"
                            % args
                        )
                    else:
                        return False
                return True
            else:
                if exit:
                    filename, lineno = _fast_caller(1)
                    args = (
                        filename,
                        lineno,
                        self.sql,
                        row,
                        col,
                        self.queryResult[row][col],
                        data,
                    )
                    tdLog.exit(
                        "%s(%d) failed: sql:%s row:%d col:%d data:%s != expect:%s"
                        % args
                    )
                else:
                    return False
        if show:
            tdLog.info("check successfully")
        return True

    def checkDataV2(self, row, col, data, show=False, operator="==") -> bool:
        """
        Compare the data at the specified row and column with the expected data.

        Args:
            row (int): The row index of the data to be checked.
            col (int): The column index of the data to be checked.
            data: The expected data to compare against.
            show (bool, optional): If True, logs a message when the check is successful. Defaults to False.
            operator (str, optional): The operator to use for comparison. Defaults to "==".
        Returns:
            bool: True if the comparison is successful, False otherwise.
        Usage:
            assert self.checkDataV2(row, col, data, show=True, operator="==")
            assert self.checkDataV2(row, col, data, show=True, operator="<") # means actual value is less than data
        """
        if row >= self.queryRows:
            filename, lineno = _fast_caller(1)
            args = (filename, lineno, self.sql, row + 1, self.queryRows)
            tdLog.error(
                "%s(%d) failed: sql:%s, row:%d is larger than queryRows:%d" % args
            )
            return False
        if col >= self.queryCols:
            filename, lineno = _fast_caller(1)
            args = (filename, lineno, self.sql, col + 1, self.queryCols)
            tdLog.error(
                "%s(%d) failed: sql:%s, col:%d is larger than queryCols:%d" % args
            )
            return False
        self.checkRowCol(row, col)

        try:
            # 获取当前单元格的数据
            actual_value = self.queryResult[row][col]

            # 转换函数：将值统一转换为可比较的类型
            def to_timestamp(value) -> Optional[int]:
                """将任意时间格式转换为纳秒级时间戳"""
                if value is None:
                    return None

                # 处理数值时间戳
                if isinstance(value, (int, float)):
                    if value > 1e18:
                        return int(value)
                    elif value > 1e15:
                        tdLog.info(f"value={value}")
                        tdLog.info(f"int(value * 1000)={int(value * 1000)}")
                        return int(value * 1000)
                    elif value > 1e12:
                        return int(value * 1000000)
                    else:
                        return int(value * 1000000000)

                # 处理字符串时间
                elif isinstance(value, str):
                    if value.isdigit():
                        if len(value) == 19:
                            return int(value)
                        elif len(value) == 16:
                            return int(value) * 1000
                        elif len(value) == 13:
                            return int(value) * 1000000
                        elif len(value) == 10:
                            return int(value) * 1000000000
                    try:
                        # 解析常规SQL格式（支持纳秒）
                        pattern = r"(\d{4}-\d{2}-\d{2})\s(\d{2}:\d{2}:\d{2})(\.\d+)?"
                        time_match = re.match(pattern, value)
                        if time_match:
                            date_part, time_part, nano_part = time_match.groups()
                            nano = int(
                                nano_part[1:].ljust(9, "0")[:9] if nano_part else 0
                            )
                            dt_str = f"{date_part or '1970-01-01'} {time_part}"
                            dt = datetime.datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
                            return int(dt.timestamp()) * 1000000000 + nano
                    except Exception:
                        pass
                    raise ValueError(f"无法解析的时间字符串: {value}")

                # 处理datetime对象
                elif isinstance(value, datetime.datetime):
                    return (
                        int(value.timestamp()) * 1000000000 + value.microsecond * 1000
                    )

                raise TypeError(f"不支持的类型: {type(value)}")

            # 转换实际值和预期值
            if self.cursor.istype(col, "TIMESTAMP"):
                converted_actual = to_timestamp(actual_value)
                tdLog.debug(f"sql_result={converted_actual}")
                converted_expected = to_timestamp(data)
                tdLog.debug(f"sql_expect={converted_expected}")
            else:
                converted_actual = actual_value
                converted_expected = data

            # 处理None值比较
            if converted_actual is None or converted_expected is None:
                result = converted_actual == converted_expected
                if operator != "==" and operator != "!=":
                    raise ValueError("None values can only be compared with == or !=")
                result = result if operator == "==" else not result
            else:
                # 根据操作符进行比较
                if operator == "<":
                    result = converted_actual < converted_expected
                elif operator == "<=":
                    result = converted_actual <= converted_expected
                elif operator == ">":
                    result = converted_actual > converted_expected
                elif operator == ">=":
                    result = converted_actual >= converted_expected
                elif operator == "==":
                    result = converted_actual == converted_expected
                elif operator == "!=":
                    result = converted_actual != converted_expected
                else:
                    raise ValueError(f"Unsupported operator: {operator}")
            if show:
                tdLog.info(
                    f"Data at row {row}, col {col} actual value={actual_value}, operator={operator}, expected value={data}, result={result}"
                )
            return result
        except Exception as e:
            tdLog.error(f"Error comparing data at row {row}, col {col}: {e}")
            return False

    def checkKeyData(self, key, col, data, show=False):
        """
        Checks if the data at the specified key matches the expected data.

        Args:
            key: The first column to be compared with.
            col (int): The column index of the data to be checked.
            data: The expected data to be compared with.
            show (bool, optional): If True, logs a message when the check is successful. Defaults to False.

        Returns:
            None

        Raises:
            SystemExit: If the data of the specified key does not match the expected data.
        """

        if col >= self.queryCols:
            filename, lineno = _fast_caller(1)
            args = (filename, lineno, self.sql, col + 1, self.queryCols)
            tdLog.exit(
                "%s(%d) failed: sql:%s, col:%d is larger than queryCols:%d" % args
            )

        row = -1

        for i in range(self.queryRows):
            if self.queryResult[i][col] == data:
                row = i

        if show:
            tdLog.info(f"find key:{key}, row:{row} col:{col}, data:{data}")

        if row == -1:
            filename, lineno = _fast_caller(1)
            args = (filename, lineno, self.sql, key, col)
            tdLog.exit("%s(%d) failed: sql:%s key:%s col:%d not found" % args)

        if show:
            tdLog.info("check key successfully")

    def checkKeyExist(self, key, show=False):
        return self.checkKeyData(key, 0, key, show=show)

    def printResult(self, name="results", exit=False, input_result=None, input_sql=""):
        if input_result == None:
            rows = self.queryRows
            cols = self.queryCols
            results = self.queryResult
        else:
            rows = len(input_result)
            if rows == 0:
                cols = 0
            else:
                cols = len(input_result[0])
            results = input_result

        if input_sql == "":
            sql = self.sql
        else:
            sql = input_sql

        tdLog.info(f"==== {name}, rows:{rows}, cols:{cols}, sql:{sql}")
        for r in range(rows):
            data = "==== "
            for c in range(cols):
                data += f"d[{r}][{c}]={results[r][c]} "
            tdLog.info(data)

        if exit:
            filename, lineno = _fast_caller(1)
            tdLog.info(f"{name} {filename}({lineno})")
            filename, lineno = _fast_caller(2)
            tdLog.exit(f"{name} {filename}({lineno})")

    def expectKeyData(self, key, col, data, show=False):
        """
        Whether the data at the specified key matches the expected data.

        Args:
            key: The first column to be compared with.
            col (int): The column index of the data to be checked.
            data: The expected data to be compared with.
            show (bool, optional): If True, logs a message when the check is successful. Defaults to False.

        Returns:
            Bool

        Raises:
            None
        """

        if col >= self.queryCols:
            filename, lineno = _fast_caller(1)
            args = (filename, lineno, self.sql, col + 1, self.queryCols)
            tdLog.info(
                "%s(%d) failed: sql:%s, col:%d is larger than queryCols:%d" % args
            )
            return False

        row = -1

        for i in range(self.queryRows):
            if self.queryResult[i][col] == data:
                row = i

        tdLog.info(f"find key:{key}, row:{row} col:{col}, data:{data}")

        if row == -1:
            filename, lineno = _fast_caller(1)
            args = (filename, lineno, self.sql, key, col)
            return False

        if show:
            tdLog.info("check key successfully")

        return True

    def checkAssert(self, assertVal, show=False):
        """
        Checks if the assertVal is true.

        Args:
            assertVal: The value to be assert
            show (bool, optional): If True, logs a message when the check is successful. Defaults to False.

        Returns:
            None

        Raises:
            SystemExit: If the data of the specified key does not match the expected data.
        """

        if assertVal != True:
            filename, lineno = _fast_caller(1)
            args = (filename, lineno, self.sql)
            tdLog.exit("%s(%d) failed: sql:%s asserted" % args)

        if show:
            tdLog.info("check assert successfully")

    def checkDataMem(self, sql, mem):
        """
        Executes a SQL query and checks if the result matches the expected data.

        Args:
            sql (str): The SQL query to be executed.
            mem (list): The expected data, represented as a list of lists.

        Returns:
            None

        Raises:
            SystemExit: If the expected data is not a list of lists, or if the SQL result does not match the expected data.
        """
        self.query(sql)
        if not isinstance(mem, list):
            filename, lineno = _fast_caller(1)
            args = (filename, lineno, self.sql)
            tdLog.exit(
                "%s(%d) failed: sql:%s, expect data is error, must is array[][]" % args
            )

        if len(mem) != self.queryRows:
            filename, lineno = _fast_caller(1)
            args = (filename, lineno, self.sql, len(mem), self.queryRows)
            tdLog.exit(
                "%s(%d) failed: sql:%s, row:%d is larger than queryRows:%d" % args
            )
        # row, col, data
        for row, rowData in enumerate(mem):
            for col, colData in enumerate(rowData):
                self.checkData(row, col, colData)
        tdLog.info("check successfully")

    def checkDataCsv(self, sql, csvfilePath):
        """
        Executes a SQL query and checks if the result matches the expected data from a CSV file.(Not used)

        Args:
            sql (str): The SQL query to be executed.
            csvfilePath (str): The path to the CSV file containing the expected data.

        Returns:
            None

        Raises:
            SystemExit: If the CSV file path is invalid, the file is not found, there is an error reading the file,
                or if the sql result does not match the expected data from CSV file.
        """
        if not isinstance(csvfilePath, str) or len(csvfilePath) == 0:
            filename, lineno = _fast_caller(1)
            args = (filename, lineno, self.sql, csvfilePath)
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
            filename, lineno = _fast_caller(1)
            args = (filename, lineno, self.sql, csvfilePath)
            tdLog.exit("%s(%d) failed: sql:%s, expect csvfile not find error:%s" % args)
        except Exception as e:
            filename, lineno = _fast_caller(1)
            args = (filename, lineno, self.sql, csvfilePath, str(e))
            tdLog.exit(
                "%s(%d) failed: sql:%s, expect csvfile path:%s, read error:%s" % args
            )

        tdLog.info("read csvfile read successfully")
        self.checkDataMem(sql, data)

    def checkDataMemByLine(self, sql, mem):
        """
        Executes a SQL query and checks if the result matches the expected data (Same as checkDataMem).

        Args:
            sql (str): The SQL query to be executed.
            mem (list): The expected data, represented as a list of lists.

        Returns:
            None

        Raises:
            SystemExit: If the expected data is not a list of lists, or if the SQL result does not match the expected data.
        """
        if not isinstance(mem, list):
            filename, lineno = _fast_caller(1)
            args = (filename, lineno, self.sql)
            tdLog.exit(
                "%s(%d) failed: sql:%s, expect data is error, must is array[][]" % args
            )

        if len(mem) != self.queryRows:
            filename, lineno = _fast_caller(1)
            args = (filename, lineno, self.sql, len(mem), self.queryRows)
            tdLog.exit(
                "%s(%d) failed: sql:%s, row:%d is larger than queryRows:%d" % args
            )
        # row, col, data
        for row, rowData in enumerate(mem):
            for col, colData in enumerate(rowData):
                self.checkData(row, col, colData)
        tdLog.info("check %s successfully" % sql)

    def checkDataCsvByLine(self, sql, csvfilePath):
        """
        Executes a SQL query and checks if the result matches the expected data from a CSV file line by line.

        Args:
            sql (str): The SQL query to be executed.
            csvfilePath (str): The path to the CSV file containing the expected data.

        Returns:
            None

        Raises:
            SystemExit: If the CSV file path is invalid, the file is not found, there is an error reading the file,
                        or if the SQL result does not match the expected data from the CSV file.
        """
        if not isinstance(csvfilePath, str) or len(csvfilePath) == 0:
            filename, lineno = _fast_caller(1)
            args = (filename, lineno, self.sql, csvfilePath)
            tdLog.exit("%s(%d) failed: sql:%s, expect csvfile path error:%s" % args)
        self.query(sql)
        data = []
        tdLog.info("check line %d start" % self.csvLine)
        try:
            with open(csvfilePath) as csvfile:
                skip_rows = self.csvLine
                # 计算需要读取的行数
                num_rows = self.queryRows
                # 读取指定范围的行
                df = pd.read_csv(
                    csvfilePath, skiprows=skip_rows, nrows=num_rows, header=None
                )
                for index, row in df.iterrows():
                    data.append(row)
                self.csvLine += self.queryRows
        except FileNotFoundError:
            filename, lineno = _fast_caller(1)
            args = (filename, lineno, self.sql, csvfilePath)
            tdLog.exit("%s(%d) failed: sql:%s, expect csvfile not find error:%s" % args)
        except Exception as e:
            filename, lineno = _fast_caller(1)
            args = (filename, lineno, self.sql, csvfilePath, str(e))
            tdLog.exit(
                "%s(%d) failed: sql:%s, expect csvfile path:%s, read error:%s" % args
            )
        self.checkDataMemByLine(sql, data)

    # return true or false replace exit, no print out
    def checkRowColNoExist(self, row, col):
        """
        Checks if the specified row and column indices are within the range of the last query result without exiting the program.

        Args:
            row (int): The row index to be checked.
            col (int): The column index to be checked.

        Returns:
            bool: True if the specified row and column indices are within the range, otherwise False.

        Raises:
            None
        """
        if row < 0:
            return False
        if col < 0:
            return False
        if row > self.queryRows:
            return False
        if col > self.queryCols:
            return False

        return True

    # return true or false replace exit, no print out
    def checkDataNoExist(self, row, col, data):
        """
        Checks if the data at the specified row and column matches the expected data without exiting the program.

        Args:
            row (int): The row index of the data to be checked.
            col (int): The column index of the data to be checked.
            data: The expected data to be compared with.

        Returns:
            bool: True if the data matches the expected data, otherwise False.

        Raises:
            None
        """
        if self.checkRowColNoExist(row, col) == False:
            return False
        if self.queryResult[row][col] != data:
            if self.cursor.istype(col, "TIMESTAMP"):
                # suppose user want to check nanosecond timestamp if a longer data passed
                if len(data) >= 28:
                    if pd.to_datetime(self.queryResult[row][col]) == pd.to_datetime(
                        data
                    ):
                        return True
                else:
                    if self.queryResult[row][col] == _parse_datetime(data):
                        return True
                return False

            if str(self.queryResult[row][col]) == str(data):
                return True
            elif isinstance(data, float):
                if (
                    abs(data) >= 1
                    and abs((self.queryResult[row][col] - data) / data) <= 0.000001
                ):
                    return True
                elif (
                    abs(data) < 1 and abs(self.queryResult[row][col] - data) <= 0.000001
                ):
                    return True
                else:
                    return False
            else:
                return False

        return True

    # loop execute sql then sleep(waitTime) , if checkData ok break loop
    def checkDataLoop(self, row, col, data, sql, loopCount=10, waitTime=1):
        """
        Executes a SQL query in a loop and checks if the data at the specified row and column matches the expected data.

        Args:
            row (int): The row index of the data to be checked.
            col (int): The column index of the data to be checked.
            data: The expected data to be compared with.
            sql (str): The SQL query to be executed.
            loopCount (int): The number of times to execute the SQL query.
            waitTime (int): The time to wait (in seconds) between each execution.

        Returns:
            None

        Raises:
            Exception: If the query execution fails.
            SystemExit: If the data at the specified row and column does not match the expected data.
        """
        # loop check util checkData return true
        for i in range(loopCount):
            self.query(sql)
            if self.checkData(row, col, data, exit=False):
                return
            #print(f"checkData failed, retrying {i} [{row},{col}] {data} ...")
            time.sleep(waitTime)

        # last check
        self.query(sql)
        self.checkData(row, col, data)

    def checkRowsLoop(self, expectedRows, sql, loopCount, waitTime):
        # loop check util checkData return true
        for i in range(loopCount):
            self.query(sql)
            if self.checkRowsNotExited(expectedRows):
                return
            else:
                time.sleep(waitTime)
                continue
        # last check
        self.query(sql)
        self.checkRows(expectedRows)

    def checkAffectedRows(self, expectAffectedRows):
        """
        Checks if the number of affected rows from the last executed SQL statement matches the expected number of affected rows.

        Args:
            expectAffectedRows (int): The expected number of affected rows.

        Returns:
            None

        Raises:
            SystemExit: If the number of affected rows does not match the expected number.
        """
        if self.affectedRows != expectAffectedRows:
            filename, lineno = _fast_caller(1)
            args = (
                filename,
                lineno,
                self.sql,
                self.affectedRows,
                expectAffectedRows,
            )
            tdLog.exit("%s(%d) failed: sql:%s, affectedRows:%d != expect:%d" % args)

        tdLog.info(
            "sql:%s, affectedRows:%d == expect:%d"
            % (self.sql, self.affectedRows, expectAffectedRows)
        )

    def checkColNameList(self, col_name_list, expect_col_name_list):
        """
        Checks if the column names from the last query match the expected column names.

        Args:
            col_name_list (list): The list of column names from the last query.
            expect_col_name_list (list): The list of expected column names.

        Returns:
            None

        Raises:
            SystemExit: If the column names do not match the expected column names.
        """
        if col_name_list == expect_col_name_list:
            tdLog.info(
                "sql:%s, col_name_list:%s == expect_col_name_list:%s"
                % (self.sql, col_name_list, expect_col_name_list)
            )
        else:
            filename, lineno = _fast_caller(1)
            args = (
                filename,
                lineno,
                self.sql,
                col_name_list,
                expect_col_name_list,
            )
            tdLog.exit(
                "%s(%d) failed: sql:%s, col_name_list:%s != expect_col_name_list:%s"
                % args
            )

    def checkResColNameList(self, expect_col_name_list):
        col_name_list = []
        col_type_list = []
        for query_col in self.cursor.description:
            col_name_list.append(query_col[0])
            col_type_list.append(query_col[1])

        self.checkColNameList(col_name_list, expect_col_name_list)
        
    def __check_equal(self, elm, expect_elm):
        if elm == expect_elm:
            return True

        if isinstance(elm, datetime.datetime) and isinstance(expect_elm, str):
            try:
                parsed = datetime.datetime.fromisoformat(expect_elm)
                return elm == parsed
            except ValueError:
                return False
        if isinstance(expect_elm, datetime.datetime) and isinstance(elm, str):
            try:
                parsed = datetime.datetime.fromisoformat(elm)
                return expect_elm == parsed
            except ValueError:
                return False
        
        if type(elm) in (list, tuple) and type(expect_elm) in (list, tuple):
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

    def print_error_frame_info(self, elm, expect_elm, sql=None):
        filename, lineno = _fast_caller(1)
        print_sql = self.sql if sql is None else sql
        args = (filename, lineno, print_sql, elm, expect_elm)
        # tdLog.info("%s(%d) failed: sql:%s, elm:%s != expect_elm:%s" % args)
        raise Exception("%s(%d) failed: sql:%s, elm:%s != expect_elm:%s" % args)

    def checkEqual(self, elm, expect_elm, show=False):
        """
        Checks if the given element is equal to the expected element.

        Args:
            elm: The element to be checked.
            expect_elm: The expected element to be compared with.

        Returns:
            None

        Raises:
            Exception: If the element does not match the expected element.
        """
        if elm == expect_elm:
            if show:
                tdLog.info(
                    "sql:%s, elm:%s == expect_elm:%s" % (self.sql, elm, expect_elm)
                )
            return True
        if self.__check_equal(elm, expect_elm):
            if show:
                tdLog.info(
                    "sql:%s, elm:%s == expect_elm:%s" % (self.sql, elm, expect_elm)
                )
            return True
        self.print_error_frame_info(elm, expect_elm)

    def checkNotEqual(self, elm, expect_elm):
        """
        Checks if the given element is not equal to the expected element.

        Args:
            elm: The element to be checked.
            expect_elm: The expected element to be compared with.

        Returns:
            None

        Raises:
            Exception: If the element matches the expected element.
        """
        if elm != expect_elm:
            tdLog.info("sql:%s, elm:%s != expect_elm:%s" % (self.sql, elm, expect_elm))
        else:
            filename, lineno = _fast_caller(1)
            args = (filename, lineno, self.sql, elm, expect_elm)
            tdLog.info("%s(%d) failed: sql:%s, elm:%s == expect_elm:%s" % args)
            raise Exception

    def checkGreater(self, elm, expect_elm):
        """Verifies that the first element is greater than the second element.

        This method compares two values and ensures that the first value (elm) is 
        strictly greater than the second value (expect_elm). It's commonly used for
        validating query results, performance metrics, or any numeric comparisons
        in test cases.

        Args:
            elm: The actual value to be compared. Can be any comparable type 
                (int, float, string, etc.).
            expect_elm: The expected threshold value that elm should exceed.
                    Must be the same comparable type as elm.

        Returns:
            bool: True if elm > expect_elm, False otherwise.
        """
        if elm > expect_elm:
            tdLog.info("sql:%s, elm:%s > expect_elm:%s" % (self.sql, elm, expect_elm))
            return True
        else:
            filename, lineno = _fast_caller(1)
            args = (filename, lineno, self.sql, elm, expect_elm)
            tdLog.info("%s(%d) failed: sql:%s, elm:%s <= expect_elm:%s" % args)
            self.print_error_frame_info(elm, expect_elm)
            return False

    # check like select count(*) ...  sql
    def checkAgg(self, sql, expectCnt):
        """
        Executes an aggregate SQL query and checks if the result matches the expected count.

        Args:
            sql (str): The aggregate SQL query to be executed.
            expectCnt (int): The expected count from the aggregate query.

        Returns:
            None

        Raises:
            Exception: If the query execution fails.
            SystemExit: If the result of the aggregate query does not match the expected count.
        """
        self.query(sql)
        self.checkData(0, 0, expectCnt)
        tdLog.info(f"{sql} expect {expectCnt} ok.")

    # expect first value
    def checkFirstValue(self, sql, expect):
        """
        Executes a SQL query and checks if the first value in the result matches the expected value.

        Args:
            sql (str): The SQL query to be executed.
            expect: The expected value of the first result.

        Returns:
            None

        Raises:
            Exception: If the query execution fails.
            SystemExit: If the first value in the result does not match the expected value.
        """
        self.query(sql)
        self.checkData(0, 0, expect)

    # colIdx1 value same with colIdx2
    def checkSameColumn(self, c1, c2):
        """
        Checks if the values in two specified columns are the same for all rows in the last query result.

        Args:
            c1 (int): The index of the first column to be checked.
            c2 (int): The index of the second column to be checked.

        Returns:
            None

        Raises:
            SystemExit: If the values in the specified columns are not the same for any row.
        """
        for i in range(self.queryRows):
            if self.queryResult[i][c1] != self.queryResult[i][c2]:
                tdLog.exit(
                    f"Not same. row={i} col1={c1} col2={c2}. {self.queryResult[i][c1]}!={self.queryResult[i][c2]}"
                )
        tdLog.info(
            f"check {self.queryRows} rows two column value same. column index [{c1},{c2}]"
        )

    #
    # others session
    #

    def getTimes(self, time_str, precision="ms"):
        """
        Converts a time string to a timestamp based on the specified precision.(Not used)

        Args:
            time_str (str): The time string to be converted. The string should end with a character indicating the time unit (e.g., 's' for seconds, 'm' for minutes).
            precision (str, optional): The precision of the timestamp. Can be "ms" (milliseconds), "us" (microseconds), or "ns" (nanoseconds). Defaults to "ms".

        Returns:
            int: The timestamp in the specified precision.

        Raises:
            SystemExit: If the time string does not end with a valid time unit character or if the precision is not valid.
        """
        if time_str[-1] not in TAOS_TIME_INIT:
            filename, lineno = _fast_caller(1)
            tdLog.exit(
                f"{filename}({lineno}) failed: {time_str} not a standard taos time init"
            )
        if precision not in TAOS_PRECISION:
            filename, lineno = _fast_caller(1)
            tdLog.exit(
                f"{filename}({lineno}) failed: {precision} not a standard taos time precision"
            )

        if time_str[-1] == TAOS_TIME_INIT[0]:
            times = int(time_str[:-1]) * TIME_NS
        if time_str[-1] == TAOS_TIME_INIT[1]:
            times = int(time_str[:-1]) * TIME_US
        if time_str[-1] == TAOS_TIME_INIT[2]:
            times = int(time_str[:-1]) * TIME_MS
        if time_str[-1] == TAOS_TIME_INIT[3]:
            times = int(time_str[:-1]) * TIME_S
        if time_str[-1] == TAOS_TIME_INIT[4]:
            times = int(time_str[:-1]) * TIME_M
        if time_str[-1] == TAOS_TIME_INIT[5]:
            times = int(time_str[:-1]) * TIME_H
        if time_str[-1] == TAOS_TIME_INIT[6]:
            times = int(time_str[:-1]) * TIME_D
        if time_str[-1] == TAOS_TIME_INIT[7]:
            times = int(time_str[:-1]) * TIME_W
        if time_str[-1] == TAOS_TIME_INIT[8]:
            times = int(time_str[:-1]) * TIME_N
        if time_str[-1] == TAOS_TIME_INIT[9]:
            times = int(time_str[:-1]) * TIME_Y

        if precision == "ms":
            return int(times)
        elif precision == "us":
            return int(times * 1000)
        elif precision == "ns":
            return int(times * 1000 * 1000)

    def getType(self, col):
        """
        Retrieves the data type of the specified column in the last query result.(Not used)

        Args:
            col (int): The column index for which the data type is to be retrieved.

        Returns:
            str: The data type of the specified column.

        Raises:
            None
        """
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
                    if psutil.Process(pid).name() == "taosd":
                        print("have already started")
                        pstate = 1
                        break
                except psutil.NoSuchProcess:
                    pass
            if pstate == state:
                break
            if state or pstate:
                tdLog.sleep(1)
                continue
            pstate = 0
            break

        args = (pstate, state)
        if pstate == state:
            tdLog.info("taosd state is %d == expect:%d" % args)
        else:
            tdLog.exit("taosd state is %d != expect:%d" % args)
        pass

    def haveFile(self, dir, state):
        if os.path.exists(dir) and os.path.isdir(dir):
            if not os.listdir(dir):
                if state:
                    tdLog.exit("dir: %s is empty, expect: not empty" % dir)
                else:
                    tdLog.info("dir: %s is empty, expect: empty" % dir)
            else:
                if state:
                    tdLog.info("dir: %s is not empty, expect: not empty" % dir)
                else:
                    tdLog.exit("dir: %s is not empty, expect: empty" % dir)
        else:
            tdLog.exit("dir: %s doesn't exist" % dir)

    def createDir(self, dir):
        if os.path.exists(dir):
            shutil.rmtree(dir)
            tdLog.info("dir: %s is removed" % dir)
        os.makedirs(dir, 755)
        tdLog.info("dir: %s is created" % dir)
        pass

    def getDbVgroups(self, db_name: str = "test") -> list:
        db_vgroups_list = []
        self.query(f"show {db_name}.vgroups")
        for result in self.queryResult:
            db_vgroups_list.append(result[0])
        vgroup_nums = len(db_vgroups_list)
        tdLog.debug(f"{db_name} has {vgroup_nums} vgroups :{db_vgroups_list}")
        self.query("select * from information_schema.ins_vnodes")
        return db_vgroups_list

    def getCluseterDnodes(self) -> list:
        cluset_dnodes_list = []
        self.query("show dnodes")
        for result in self.queryResult:
            cluset_dnodes_list.append(result[0])
        self.clust_dnode_nums = len(cluset_dnodes_list)
        tdLog.debug(
            f"cluster has {len(cluset_dnodes_list)} dnodes :{cluset_dnodes_list}"
        )
        return cluset_dnodes_list

    def redistribute_one_vgroup(
        self,
        db_name: str = "test",
        replica: int = 1,
        vgroup_id: int = 1,
        useful_trans_dnodes_list: list = [],
    ):
        # redisutribute vgroup {vgroup_id} dnode {dnode_id}
        if replica == 1:
            dnode_id = random.choice(useful_trans_dnodes_list)
            redistribute_sql = f"redistribute vgroup {vgroup_id} dnode {dnode_id}"
        elif replica == 3:
            selected_dnodes = random.sample(useful_trans_dnodes_list, replica)
            redistribute_sql_parts = [f"dnode {dnode}" for dnode in selected_dnodes]
            redistribute_sql = f"redistribute vgroup {vgroup_id} " + " ".join(
                redistribute_sql_parts
            )
        else:
            raise ValueError(f"Replica count must be 1 or 3,but got {replica}")
        tdLog.debug(f"redistributeSql:{redistribute_sql}")
        self.query(redistribute_sql)
        tdLog.debug("redistributeSql ok")

    def redistributeDbAllVgroups(self, db_name: str = "test", replica: int = 1):
        db_vgroups_list = self.getDbVgroups(db_name)
        cluset_dnodes_list = self.getCluseterDnodes()
        useful_trans_dnodes_list = cluset_dnodes_list.copy()
        self.query("select * from information_schema.ins_vnodes")
        # result: dnode_id|vgroup_id|db_name|status|role_time|start_time|restored|

        results = list(self.queryResult)
        for vnode_group_id in db_vgroups_list:
            for result in results:
                print(
                    f"result[2] is {result[2]}, db_name is {db_name}, result[1] is {result[1]}, vnode_group_id is {vnode_group_id}"
                )
                if result[2] == db_name and result[1] == vnode_group_id:
                    tdLog.debug(
                        f"dbname: {db_name}, vgroup :{vnode_group_id}, dnode is {result[0]}"
                    )
                    print(useful_trans_dnodes_list)
                    useful_trans_dnodes_list.remove(result[0])
            tdLog.debug(
                f"vgroup :{vnode_group_id},redis_dnode list:{useful_trans_dnodes_list}"
            )
            self.redistribute_one_vgroup(
                db_name, replica, vnode_group_id, useful_trans_dnodes_list
            )
            useful_trans_dnodes_list = cluset_dnodes_list.copy()

    def pause(self):
        """
        Pause the execution of the program and wait for enter key. Used for debugging.
        Args:
            None
        Returns:
            None
        Raises:
            None
        """
        if os.name == "nt":  # Windows
            os.system("pause")  # 显示 "按任意键继续..."
        else:  # Linux/macOS
            input("press enter to continue...")

    def setConnMode(self, mode=0, value=1):
        """
        Set Conn Mode

        Args:
            mode (int, optional): connect mode options.

        Returns:
            None

        Raises:
            None

        """
        tdLog.info(f"set connection mode:{mode} value:{value}")
        self.cursor._connection.set_mode(mode, value)

    def checkResultsByFunc(self, sql, func, delay=0.0, retry=300, show=False):
        if delay != 0:
            time.sleep(delay)

        # show sql
        tdLog.info(sql)

        if retry <= 0:
            retry = 1

        for loop in range(retry):
            self.clearResult()
            if self.query(sql, queryTimes=1, exit=False):
                if func():
                    self.printResult(f"check succeed in {loop} seconds")
                    return

            if loop != retry - 1:
                if show:
                    self.printResult(f"check continue {loop} after sleep 1s ...")
                time.sleep(1)

        self.printResult(f"check failed for {retry} seconds, sql={sql}", exit=True)

    def checkResultsByArray(
        self, sql, exp_result, exp_sql="", delay=0.0, retry=60, show=False
    ):
        if delay != 0:
            time.sleep(delay)

        if retry <= 0:
            retry = 1

        for loop in range(retry):
            self.clearResult()
            res_result = self.getResult(sql, exit=False)
            if res_result != []:
                if self.compareResults(res_result, exp_result):
                    self.printResult(
                        f"check succeed in {loop} seconds", input_result=res_result
                    )
                    return

            if loop != retry - 1:
                if show:
                    self.printResult("check continue", input_result=res_result)
                time.sleep(1)

        self.printResult(f"expect results", input_result=exp_result, input_sql=exp_sql)
        self.printResult(
            f"check failed for {retry} seconds", input_result=res_result, input_sql=sql
        )
        self.compareResults(res_result, exp_result, show=True)

        filename, lineno = _fast_caller(1)
        tdLog.info(f"{filename}({lineno})  check result failed")
        filename, lineno = _fast_caller(2)
        tdLog.exit(f"{filename}({lineno})  check result failed")

    def checkResultsBySql(self, sql, exp_sql, delay=0.0, retry=300, show=False):
        # sleep
        if delay != 0:
            time.sleep(delay)

        # show sql
        tdLog.info(sql)

        if retry <= 0:
            retry = 1

        # loop retry
        for loop in range(retry):
            # clear
            self.clearResult()
            # query
            exp_result = self.getResult(exp_sql, exit=False)
            # check result
            if exp_result != [] and exp_result != None:
                # success
                break
            # sleep and retry
            if loop != retry - 1:
                if show:
                    self.printResult(f"check continue {loop} after sleep 1s ...")
                time.sleep(1)

        self.checkResultsByArray(sql, exp_result, exp_sql, delay, retry, show)

    def checkTableType(
        self,
        dbname,
        columns,
        tags=0,
        stbname=None,
        tbname=None,
        typename=None,
        delay=0.0,
        retry=60,
        show=False,
    ):
        if tags == 0:
            sql = f"select * from information_schema.ins_tables where db_name='{dbname}' and table_name='{tbname}'"
            self.checkResultsByFunc(
                sql=sql,
                func=lambda: self.getRows() == 1
                and self.compareData(0, 0, tbname)
                and self.compareData(0, 1, dbname)
                and self.compareData(0, 3, columns)
                and self.compareData(0, 4, stbname)
                and self.compareData(0, 9, typename),
                delay=delay,
                retry=retry,
                show=show,
            )
        else:
            sql = f"select * from information_schema.ins_stables where db_name='{dbname}' and stable_name='{stbname}'"
            self.checkResultsByFunc(
                sql=sql,
                func=lambda: self.getRows() == 1
                and self.compareData(0, 0, stbname)
                and self.compareData(0, 1, dbname)
                and self.compareData(0, 3, columns)
                and self.compareData(0, 4, tags),
                delay=delay,
                retry=retry,
                show=show,
            )

    def checkTableSchema(
        self,
        dbname,
        tbname,
        schema,
        delay=0.0,
        retry=60,
        show=False,
    ):
        sql = f"desc {dbname}.{tbname}"
        self.checkResultsByFunc(
            sql=sql,
            func=lambda: self.compareSchema(schema),
            delay=delay,
            retry=retry,
            show=show,
        )

    def compareSchema(self, schema):
        row = len(schema)
        for r in range(row):
            if (
                schema[r][0] != self.queryResult[r][0]  # field
                or schema[r][1] != self.queryResult[r][1]  # type
                or schema[r][2] != self.queryResult[r][2]  # length
                or schema[r][3] != self.queryResult[r][3]  # note
            ):
                tdLog.info(f"exp_schema[{r}]={schema[r]}, res_schema[{r}]={self.queryResult[r]}")
                return False

        return True

    def compareResults(self, res_result, exp_result, show=False):
        exp_rows = len(exp_result)
        res_rows = len(res_result)

        if exp_rows != res_rows:
            if show:
                tdLog.info(f"exp_rows={exp_rows}, res_rows={res_rows}")
            return False
        if exp_rows == 0:
            return True

        exp_cols = len(exp_result[0])
        res_cols = len(res_result[0])
        if exp_cols != res_cols:
            if show:
                tdLog.info(f"exp_cols={exp_cols}, res_cols={res_cols}")
            return False
        if exp_cols == 0:
            return True

        for r in range(res_rows):
            for c in range(res_cols):
                if res_result[r][c] != exp_result[r][c]:
                    if show:
                        tdLog.info(
                            f"res_result[{r}][{c}]={res_result[r][c]}, exp_result[{r}][{c}]={exp_result[r][c]}"
                        )
                    return False

        return True

    def clearResult(self):
        self.queryCols = 0
        self.queryRows = 0
        self.queryResult = []


    # insert table with fixed values, return next write ts
    def insertFixedVal(self, table, startTs, step, count, cols, fixedVals):
        # init
        ts = startTs
        # loop count
        for i in range(count):
            sql = f"INSERT INTO {table}({cols}) VALUES({ts},{fixedVals})"
            self.execute(sql, show=True)
            # next
            ts += step

        return ts

    
    # insert now
    def insertNow(self, table, sleepS, count, cols, fixedVals):
        # loop count
        for i in range(count):
            sql = f"INSERT INTO {table}({cols}) VALUES(now,{fixedVals})"
            self.execute(sql, show=True)
            # next
            time.sleep(sleepS)


    # insert table with order values, only support number cols, return next write ts
    def insertOrderVal(self, table, startTs, step, count, cols, orderVals, colStep = 1):
        # init
        ts      = startTs
        colsVal = orderVals

        # loop count
        for i in range(count):
            # insert sql
            sql = f"INSERT INTO {table}({cols}) VALUES({ts}, {','.join(map(str, colsVal))})"
            self.execute(sql, show=True)
            # next
            ts += step
            for j in range(len(colsVal)):
                colsVal[j] += colStep

        return ts

    # flush db
    def flushDb(self, dbName):
        self.execute(f"flush database {dbName}", show=True)

    # delete rows
    def deleteRows(self, table, where=None):
        """
        Deletes rows from the specified table based on the given condition.

        Args:
            table (str): The name of the table from which rows are to be deleted.
            where (str, optional): The condition for deleting rows. Defaults to None.

        Returns:
            None

        Raises:
            SystemExit: If the delete operation fails.
        """
        sql = f"DELETE FROM {table}"
        if where:
            sql += f" WHERE {where}"
        self.execute(sql, show=True) 


# global
tdSql = TDSql()
