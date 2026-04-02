#include <stdio.h>
#include <stdlib.h>
#include <sql.h>
#include <sqlext.h>

int main() {
    SQLHANDLE henv = SQL_NULL_HANDLE;
    SQLHANDLE hdbc = SQL_NULL_HANDLE;
    SQLHANDLE hstmt = SQL_NULL_HANDLE;

    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLSetEnvAttr(henv, SQL_ATTR_CONNECTION_POOLING, (SQLPOINTER)SQL_CP_ONE_PER_DRIVER, 0);

    SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);

    // create a connection to tdengine data source
    SQLCHAR OutConnectionString[1024] = { 0 };
    SQLSMALLINT StringLength2 = 0;

    char* conn_str = "DSN=TAOS_ODBC_DSN; server=127.0.0.1:6030; uid=root; pwd=taosdata; db=meter";
    SQLDriverConnect(hdbc,
        NULL,
        (SQLCHAR*)conn_str,
        (SQLSMALLINT)strlen(conn_str),
        OutConnectionString,
        sizeof(OutConnectionString),
        &StringLength2,
        SQL_DRIVER_NOPROMPT);

    SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);

    char* create_stable = "CREATE TABLE `meters` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) \
         TAGS (`groupid` INT, `location` BINARY(24))";
    SQLExecDirect(hstmt, (SQLCHAR*)create_stable, SQL_NTS);

    // create test table
    char* create_table_sql = "CREATE TABLE `d0` USING `meters` TAGS(0, 'California.LosAngles')";
    SQLExecDirect(hstmt, (SQLCHAR*)create_table_sql, SQL_NTS);

    // write data into test table
    char insert_sql[256];
    sprintf(insert_sql, "INSERT INTO `d0` values(now - 10s, 10, 116, 0.32)");
    SQLExecDirect(hstmt, (SQLCHAR*)insert_sql, SQL_NTS);
    SQLLEN numberOfrows;
    SQLRowCount(hstmt, &numberOfrows);
    printf("insert count: "SQLLEN_FORMAT"\n", numberOfrows);

    // reset cursor
    SQLCloseCursor(hstmt);

    // read data from table
    char select_sql[256];
    sprintf(select_sql, "select ts, current, voltage from d0");
    SQLExecDirect(hstmt, (SQLCHAR*)select_sql, SQL_NTS);

    int row = 0;
    SQLSMALLINT numberOfColumns;
    CALL_SQLNumResultCols(hstmt, &numberOfColumns);

    while (SQLFetch(hstmt) == SQL_SUCCESS) {
        row++;
        for (int i = 1; i <= numberOfColumns; i++) {
            SQLCHAR columnData[256];
            SQLLEN indicator;
            SQLGetData(hstmt, i, SQL_C_CHAR, columnData, sizeof(columnData), &indicator);
            if (indicator != SQL_NULL_DATA) {
                printf("Row:%d Column %d: %s \n", row, i, columnData);
            }
        }
    }

    // close handle
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    SQLDisconnect(hdbc);
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);

    return 0;
}