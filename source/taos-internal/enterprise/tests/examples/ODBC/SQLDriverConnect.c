/*******************************************************************
 *           Copyright (c) 2017 by TAOS Technologies, Inc.
 *                     All rights reserved.
 *
 *  This file is proprietary and confidential to TAOS Technologies. 
 *  No part of this file may be reproduced, stored, transmitted, 
 *  disclosed or used in any form or by any means other than as 
 *  expressly provided by the written permission from Jianhui Tao
 *
 * ****************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <Windows.h>
#include <sql.h>
#include <sqltypes.h>
#include <sqlext.h>

int validOdbcDriverConnection()
{
    SQLCHAR pszSourceName[1024] = "demo";   //IP=172.16.170.137,USER=root,PASS=taosdata,db=odbc1,port=6101,log_level=FULL,LOG_DIR=C:\Users\slguan\Desktop\dll
    SQLCHAR pszUserId[20] = "root";
    SQLCHAR pszPassword[20] = "taosdata";
    SQLCHAR defaultDb[20] = "odbcdb";
    SQLCHAR defaultTable[20] = "t1";

    SQLHENV henv;
    SQLHDBC hdbc;
    SQLHSTMT hstmt;
    RETCODE retcode;
    SQLCHAR err[1024] = { 0 };
    SQLCHAR sql[1024] = { 0 };
    SQLCHAR buf[1024] = { 0 };
    SQLCHAR sqlState[20] = { 0 };
    SQLINTEGER nativeErr = 0;
    SQLSMALLINT errlen, ncols;
    SQLCHAR dsn[] = "DRIVER={TBase Odbc Driver};DB=odbc1;IP=172.16.170.137;PORT=;USER=root;PASSWORD=taosdata;LOG_LEVEL=NORMAL;LOG_DIR=./";
    time_t col1 = 0;
    int col2 = 0;

    if (SQLAllocEnv(&henv) == SQL_ERROR) {
        return -1;
    }
	
    if (SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, SQL_IS_INTEGER) == SQL_ERROR) {
        return -1;
    }
	
    if (SQLAllocConnect(henv, &hdbc) == SQL_ERROR) {
        return -1;
    }
    
    //retcode = SQLConnect(hdbc, pszSourceName, SQL_NTS, pszUserId, sizeof(pszUserId), pszPassword, sizeof(pszPassword));
    retcode = SQLDriverConnect(hdbc, 0, dsn, sizeof(dsn), buf, sizeof(buf), &errlen, SQL_DRIVER_COMPLETE);
    if (retcode == SQL_ERROR) {
        return -1;
    }

    retcode = SQLAllocStmt(hdbc, &hstmt);
    if (retcode == SQL_ERROR) {
        return -1;
    }

    sprintf(sql, "drop database %s", defaultDb);
    retcode = SQLExecDirect(hstmt, sql, SQL_NTS);

    sprintf(sql, "create database %s", defaultDb);
    retcode = SQLExecDirect(hstmt, sql, SQL_NTS);
    if (retcode == SQL_ERROR) {
        return -1;
    }

    sprintf(sql, "use %s", defaultDb);
    retcode = SQLExecDirect(hstmt, sql, SQL_NTS);
    if (retcode == SQL_ERROR) {
        return -1;
    }

    sprintf(sql, "create table t1(t timestamp, i int)");
    retcode = SQLExecDirect(hstmt, sql, SQL_NTS);
    if (retcode == SQL_ERROR) {
        return -1;
    }

    sprintf(sql, "insert into t1 values(now, %d)", 2);
    for (int i = 0; i < 3; ++i) {
        retcode = SQLExecDirect(hstmt, sql, SQL_NTS);
        if (retcode == SQL_ERROR) {
            SQLError(henv, hdbc, hstmt, sqlState, &nativeErr, err, sizeof(err), &errlen);
            //printf("======> error [%s][%d] is [%s] length [%d]\n", sqlState, nativeErr, err, errlen);
            if (strcmp(err, "redirect") == 0) {
                continue;
            }
            return -1;
        }
    }

    sprintf(sql, "select * from t1");
    retcode = SQLPrepare(hstmt, sql, SQL_NTS);
    if (retcode == SQL_ERROR) {
        return -1;
    }
    retcode = SQLExecute(hstmt);
    if (retcode == SQL_ERROR) {
        return -1;
    }

    retcode = SQLNumResultCols(hstmt, &ncols);
    if (retcode == SQL_ERROR) {
        return -1;
    }

    if (ncols != 2) {
        printf("======> query [%d] != 2", ncols);
        return -1;
    }

    for (int col = 0; col < ncols; col++) {
        SQLCHAR name[24] = { 0 };
        SQLSMALLINT len;
        SQLSMALLINT type;
        SQLULEN size;
        SQLSMALLINT digits;
        SQLSMALLINT nullable;
        SQLDescribeCol(hstmt, col+1, name, sizeof(name), &len, &type, &size, &digits, &nullable);
        printf("=====>%s, len=%d, type=%d, size=%d, digits=%d, null=%d\n", name, len, type, size, digits, nullable);
    }

    SQLLEN len1, len2;
    SQLBindCol(hstmt, 1, SQL_CHAR, &col1, sizeof(col1), &len1);
    SQLBindCol(hstmt, 2, SQL_CHAR, &col2, sizeof(col2), &len2);

    while (SQLFetch(hstmt) == SQL_SUCCESS) {
       printf("======> col1: %lld, col2: %d \n", col1, col2);
    }

    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);
    return 0;
}
