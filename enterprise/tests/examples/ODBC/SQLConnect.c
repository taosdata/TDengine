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

int validOdbcConnection() 
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
    SQLCHAR sqlState[20] = { 0 };
    SQLINTEGER nativeErr = 0;
    SQLSMALLINT errlen, ncols;
    SQLLEN lenp;
    time_t col1 = 0;
    int col2 = 0;

    if (SQLAllocHandle(SQL_HANDLE_ENV, NULL, &henv) == SQL_ERROR) {
        return -1;
    }
	
    if (SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, SQL_IS_INTEGER) == SQL_ERROR) {
        return -1;
    }
    
	if (SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc) == SQL_ERROR) {
        return -1;
    }

    retcode = SQLConnect(hdbc, pszSourceName, SQL_NTS,
        pszUserId, SQL_NTS,
        pszPassword, SQL_NTS);
    if (retcode == SQL_ERROR) {
        SQLError(henv, hdbc, NULL, sqlState, &nativeErr, err, sizeof(err), &errlen);
        printf("    ======> error: %s.\n", err);
       return -1;
    }
    
    if (SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt) == SQL_ERROR) {
        return -1;
    }

    sprintf(sql, "drop database %s", defaultDb);
    retcode = SQLExecDirect(hstmt, sql, SQL_NTS);

    sprintf(sql, "create database %s", defaultDb);
    retcode = SQLExecDirect(hstmt, sql, SQL_NTS);
    if (retcode == SQL_ERROR) {
        SQLError(henv, hdbc, hstmt, sqlState, &nativeErr, err, sizeof(err), &errlen);
        printf("    ======> error: %s.\n", err);
        return -1;
    }

    sprintf(sql, "use %s", defaultDb);
    retcode = SQLExecDirect(hstmt, sql, SQL_NTS);
    if (retcode == SQL_ERROR) {
        SQLError(henv, hdbc, hstmt, sqlState, &nativeErr, err, sizeof(err), &errlen);
        printf("    ======> error: %s.\n", err);
        return -1;
    }

    sprintf(sql, "create table %s(t timestamp, i int)", defaultTable);
    retcode = SQLExecDirect(hstmt, sql, SQL_NTS);
    if (retcode == SQL_ERROR) {
        SQLError(henv, hdbc, hstmt, sqlState, &nativeErr, err, sizeof(err), &errlen);
        printf("    ======> error: %s.\n", err);
        return -1;
    }

    sprintf(sql, "insert into %s values(now, %d)", defaultTable, 2);
    retcode = SQLExecDirect(hstmt, sql, SQL_NTS);
    if (retcode == SQL_ERROR) {
        SQLError(henv, hdbc, hstmt, sqlState, &nativeErr, err, sizeof(err), &errlen);
        printf("    ======> error: %s.\n", err);
        return -1;
    }

    sprintf(sql, "select * from %s", defaultTable);
    retcode = SQLExecDirect(hstmt, sql, SQL_NTS);
    if (retcode == SQL_ERROR) {
        //SQLGetDiagRec(SQL_HANDLE_DBC, hdbc, 1, sqlState, &nativeErr, err, sizeof(err), &length);
        SQLError(henv, hdbc, hstmt, sqlState, &nativeErr, err, sizeof(err), &errlen);
        printf("    ======> error [%s][%d] is [%s] length [%d]\n", sqlState, nativeErr, err, errlen);
        return -1;
    }

    retcode = SQLNumResultCols(hstmt, &ncols);
    if (retcode == SQL_ERROR) {
        SQLError(henv, hdbc, hstmt, sqlState, &nativeErr, err, sizeof(err), &errlen);
        printf("    ======> error: %s.\n", err);
        return -1;
    }
    if (ncols != 2) {
        return -1;
    }

     while (SQLFetch(hstmt) == SQL_SUCCESS) {
        SQLGetData(hstmt, 1, SQL_CHAR, &col1, sizeof(col1), &lenp);
        SQLGetData(hstmt, 2, SQL_CHAR, &col2, sizeof(col2), &lenp);
        if (col2 != 2) {
            return -1;
        }
    }

    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);
    return 0;
}

