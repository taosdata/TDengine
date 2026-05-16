/*
 * MIT License
 *
 * Copyright (c) 2022-2023 freemine <freemine@yeah.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#ifndef _odbc_helper_h_
#define _odbc_helper_h_

#include "helpers.h"
#include "enums.h"

#include <sqlext.h>
#include <string.h>


EXTERN_C_BEGIN


#ifndef SUCCEEDED
  // #define SUCCEEDED(_sr) ({ SQLRETURN __sr = _sr; (__sr == SQL_SUCCESS) || (__sr == SQL_SUCCESS_WITH_INFO); })
  #define SUCCEEDED(_sr) ((_sr == SQL_SUCCESS) || (_sr == SQL_SUCCESS_WITH_INFO))
#endif
#ifndef FAILED
  #define FAILED(_sr) (!SUCCEEDED(_sr))
#endif

#define LOGD_ODBC(file, line, func, fmt, ...) do {                                                \
  tod_logger_write(tod_get_system_logger(), LOGGER_DEBUG, tod_get_system_logger_level(),          \
    file, line, func,                                                                             \
    fmt, ##__VA_ARGS__);                                                                          \
} while (0)

#define LOGE_ODBC(file, line, func, fmt, ...) do {                                                \
  tod_logger_write(tod_get_system_logger(), LOGGER_ERROR, tod_get_system_logger_level(),          \
    file, line, func,                                                                             \
    fmt, ##__VA_ARGS__);                                                                          \
} while (0)


static inline void diag_impl(const char *file, int line, const char *func, SQLRETURN sr, SQLSMALLINT HandleType, SQLHANDLE Handle)
{
  do {
    if (sr == SQL_SUCCESS) break;
    SQLCHAR sqlState[6]; sqlState[0] = '\0';
    SQLINTEGER nativeErrno = 0;
    SQLCHAR messageText[1024]; messageText[0] = '\0';
    SQLSMALLINT textLength = 0;
    SQLRETURN _sr = SQL_SUCCESS;
    const char *s1 = (sr == SQL_ERROR) ? color_red() : color_yellow();
    const char *s2 = color_reset();
    for (SQLSMALLINT i=1; i<10; ++i) {
      _sr = SQLGetDiagRec(HandleType, Handle, i,
          sqlState, &nativeErrno,
          messageText, sizeof(messageText), &textLength);
      if (_sr == SQL_NO_DATA) break;
      if (_sr == SQL_ERROR) break;
      if (textLength == SQL_NTS)
        LOGE_ODBC(file, line, func, "[%s][%d]: %s%s%s", (const char*)sqlState, nativeErrno, s1, messageText, s2);
      else
        LOGE_ODBC(file, line, func, "[%s][%d]: %s%.*s%s", (const char*)sqlState, nativeErrno, s1, textLength, messageText, s2);
    }
  } while (0);
}

#define diag(fmt, ...) diag_impl(file, line, func, fmt, ##__VA_ARGS__)

static inline SQLRETURN call_SQLAllocHandle(const char *file, int line, const char *func,
    SQLSMALLINT HandleType, SQLHANDLE InputHandle, SQLHANDLE *OutputHandle)
{
  LOGD_ODBC(file, line, func, "SQLAllocHandle(HandleType:%s,InputHandle:%p,OutputHandle:%p) ...",
      sql_handle_type(HandleType), InputHandle, OutputHandle);
  SQLRETURN sr = SQLAllocHandle(HandleType, InputHandle, OutputHandle);
  if (sr != SQL_INVALID_HANDLE) {
    switch (HandleType) {
      case SQL_HANDLE_DBC:
        diag(sr, SQL_HANDLE_ENV, InputHandle);
        break;
      case SQL_HANDLE_DESC:
        diag(sr, SQL_HANDLE_DBC, InputHandle);
        break;
      case SQL_HANDLE_STMT:
        diag(sr, SQL_HANDLE_DBC, InputHandle);
        break;
      default:
        break;
    }
  }
  SQLHANDLE p = OutputHandle ? *OutputHandle : NULL;
  LOGD_ODBC(file, line, func, "SQLAllocHandle(HandleType:%s,InputHandle:%p,OutputHandle:%p(%p)) => %s",
      sql_handle_type(HandleType), InputHandle, OutputHandle, p, sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLFreeHandle(const char *file, int line, const char *func,
    SQLSMALLINT HandleType, SQLHANDLE Handle)
{
  LOGD_ODBC(file, line, func, "SQLFreeHandle(HandleType:%s,Handle:%p) ...",
      sql_handle_type(HandleType), Handle);
  SQLRETURN sr = SQLFreeHandle(HandleType, Handle);
  LOGD_ODBC(file, line, func, "SQLFreeHandle(HandleType:%s,Handle:%p) => %s",
      sql_handle_type(HandleType), Handle, sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLSetEnvAttr(const char *file, int line, const char *func,
    SQLHENV EnvironmentHandle, SQLINTEGER Attribute, SQLPOINTER ValuePtr, SQLINTEGER StringLength)
{
  LOGD_ODBC(file, line, func, "SQLSetEnvAttr(EnvironmentHandle:%p,Attribute:%s,ValuePtr:%p,StringLength:%d) ...",
      EnvironmentHandle, sql_env_attr(Attribute), ValuePtr, StringLength);
  SQLRETURN sr = SQLSetEnvAttr(EnvironmentHandle, Attribute, ValuePtr, StringLength);
  diag(sr, SQL_HANDLE_ENV, EnvironmentHandle);
  LOGD_ODBC(file, line, func, "SQLSetEnvAttr(EnvironmentHandle:%p,Attribute:%s,ValuePtr:%p,StringLength:%d) => %s",
      EnvironmentHandle, sql_env_attr(Attribute), ValuePtr, StringLength, sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLConnect(const char *file, int line, const char *func,
  SQLHDBC ConnectionHandle, SQLCHAR *ServerName, SQLSMALLINT NameLength1, SQLCHAR *UserName, SQLSMALLINT NameLength2, SQLCHAR *Authentication, SQLSMALLINT NameLength3)
{
  int n1 = (NameLength1 == SQL_NTS) ? (ServerName ? (int)strlen((const char*)ServerName) : 0) : (int)NameLength1;
  int n2 = (NameLength2 == SQL_NTS) ? (UserName ? (int)strlen((const char*)UserName) : 0) : (int)NameLength2;
  int n3 = (NameLength3 == SQL_NTS) ? (Authentication ? (int)strlen((const char*)Authentication) : 0) : (int)NameLength3;

  LOGD_ODBC(file, line, func, "SQLConnect(ConnectionHandle:%p,ServerName:%.*s,NameLength1:%d,UserName:%.*s,NameLength2:%d,Authentication:%.*s,NameLength3:%d) ...",
      ConnectionHandle, n1, ServerName, NameLength1, n2, UserName, NameLength2, n3, Authentication, NameLength3);
  SQLRETURN sr = SQLConnect(ConnectionHandle, ServerName, NameLength1, UserName, NameLength2, Authentication, NameLength3);
  diag(sr, SQL_HANDLE_DBC, ConnectionHandle);
  LOGD_ODBC(file, line, func, "SQLConnect(ConnectionHandle:%p,ServerName:%.*s,NameLength1:%d,UserName:%.*s,NameLength2:%d,Authentication:%.*s,NameLength3:%d) => %s",
      ConnectionHandle, n1, ServerName, NameLength1, n2, UserName, NameLength2, n3, Authentication, NameLength3, sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLDriverConnect(const char *file, int line, const char *func,
    SQLHDBC ConnectionHandle, SQLHWND WindowHandle, SQLCHAR *InConnectionString, SQLSMALLINT StringLength1,
    SQLCHAR *OutConnectionString, SQLSMALLINT BufferLength, SQLSMALLINT *StringLength2Ptr, SQLUSMALLINT DriverCompletion)
{
  int n1 = (StringLength1 == SQL_NTS) ? (InConnectionString ? (int)strlen((const char*)InConnectionString) : 0) : (int)StringLength1;

  LOGD_ODBC(file, line, func,
      "SQLDriverConnect(ConnectionHandle:%p,WindowHandle:%p,InConnectionString:%.*s,StringLength1:%d,"
      "OutConnectionString:%p,BufferLength:%d,StringLength2Ptr:%p,DriverCompletion:%s) ...",
      ConnectionHandle, WindowHandle, n1, InConnectionString, StringLength1,
      OutConnectionString, BufferLength, StringLength2Ptr, sql_driver_completion(DriverCompletion));
  SQLRETURN sr = SQLDriverConnect(ConnectionHandle, WindowHandle, InConnectionString, StringLength1,
      OutConnectionString, BufferLength, StringLength2Ptr, DriverCompletion);
  diag(sr, SQL_HANDLE_DBC, ConnectionHandle);
  LOGD_ODBC(file, line, func,
      "SQLDriverConnect(ConnectionHandle:%p,WindowHandle:%p,InConnectionString:%.*s,StringLength1:%d,"
      "OutConnectionString:%p,BufferLength:%d,StringLength2Ptr:%p,DriverCompletion:%s) => %s",
      ConnectionHandle, WindowHandle, n1, InConnectionString, StringLength1,
      OutConnectionString, BufferLength, StringLength2Ptr, sql_driver_completion(DriverCompletion),
      sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLDisconnect(const char *file, int line, const char *func,
    SQLHDBC ConnectionHandle)
{
  LOGD_ODBC(file, line, func, "SQLDisconnect(ConnectionHandle:%p) ...", ConnectionHandle);
  SQLRETURN sr = SQLDisconnect(ConnectionHandle);
  LOGD_ODBC(file, line, func, "SQLDisconnect(ConnectionHandle:%p) => %s", ConnectionHandle, sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLDescribeCol(const char *file, int line, const char *func,
    SQLHSTMT StatementHandle, SQLUSMALLINT ColumnNumber, SQLCHAR *ColumnName, SQLSMALLINT BufferLength,
    SQLSMALLINT *NameLengthPtr, SQLSMALLINT *DataTypePtr, SQLULEN *ColumnSizePtr, SQLSMALLINT *DecimalDigitsPtr, SQLSMALLINT *NullablePtr)
{
  LOGD_ODBC(file, line, func, "SQLDescribeCol(StatementHandle:%p,ColumnNumber:%d,ColumnName:%p,BufferLength:%d,"
      "NameLengthPtr:%p,DataTypePtr:%p,ColumnSizePtr:%p,DecimalDigitsPtr:%p,NullablePtr:%p) ...",
      StatementHandle, ColumnNumber, ColumnName, BufferLength, NameLengthPtr, DataTypePtr, ColumnSizePtr, DecimalDigitsPtr, NullablePtr);
  SQLRETURN sr = SQLDescribeCol(StatementHandle, ColumnNumber, ColumnName, BufferLength, NameLengthPtr,
      DataTypePtr, ColumnSizePtr, DecimalDigitsPtr, NullablePtr);
  diag(sr, SQL_HANDLE_STMT, StatementHandle);
  if (sr == SQL_ERROR) {
    LOGD_ODBC(file, line, func, "SQLDescribeCol(StatementHandle:%p,ColumnNumber:%d,ColumnName:%p,BufferLength:%d,"
        "NameLengthPtr:%p,DataTypePtr:%p,ColumnSizePtr:%p,DecimalDigitsPtr:%p,NullablePtr:%p) => %s",
        StatementHandle, ColumnNumber, ColumnName, BufferLength,
        NameLengthPtr, DataTypePtr, ColumnSizePtr, DecimalDigitsPtr, NullablePtr,
        sql_return_type(sr));
  } else {
    LOGD_ODBC(file, line, func, "SQLDescribeCol(StatementHandle:%p,ColumnNumber:%d,ColumnName:%p(%s),BufferLength:%d,"
        "NameLengthPtr:%p(%d),DataTypePtr:%p(%s),ColumnSizePtr:%p(%zd),DecimalDigitsPtr:%p(%d),NullablePtr:%p(%d)) => %s",
        StatementHandle, ColumnNumber, ColumnName, (const char*)ColumnName, BufferLength,
        NameLengthPtr, NameLengthPtr ? *NameLengthPtr : 0,
        DataTypePtr, DataTypePtr ? sql_data_type(*DataTypePtr) : "",
        ColumnSizePtr, (size_t)(ColumnSizePtr ? *ColumnSizePtr : 0),
        DecimalDigitsPtr, DecimalDigitsPtr ? *DecimalDigitsPtr : 0,
        NullablePtr, NullablePtr ? *NullablePtr : 0,
        sql_return_type(sr));
  }
  return sr;
}

static inline SQLRETURN call_SQLFetch(const char *file, int line, const char *func,
    SQLHSTMT StatementHandle)
{
  LOGD_ODBC(file, line, func, "SQLFetch(StatementHandle:%p) ...", StatementHandle);
  SQLRETURN sr = SQLFetch(StatementHandle);
  diag(sr, SQL_HANDLE_STMT, StatementHandle);
  LOGD_ODBC(file, line, func, "SQLFetch(StatementHandle:%p) => %s", StatementHandle, sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLExecDirect(const char *file, int line, const char *func,
    SQLHSTMT StatementHandle, SQLCHAR *StatementText, SQLINTEGER TextLength)
{
  int n1 = (TextLength == SQL_NTS) ? (StatementText ? (int)strlen((const char*)StatementText) : 0) : (int)TextLength;
  LOGD_ODBC(file, line, func, "SQLExecDirect(StatementHandle:%p,StatementText:%.*s,TextLength:%d) ...",
      StatementHandle, n1, StatementText, TextLength);
  SQLRETURN sr = SQLExecDirect(StatementHandle, StatementText, TextLength);
  diag(sr, SQL_HANDLE_STMT, StatementHandle);
  LOGD_ODBC(file, line, func, "SQLExecDirect(StatementHandle:%p,StatementText:%.*s,TextLength:%d) => %s",
      StatementHandle, n1, StatementText, TextLength, sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLNumResultCols(const char *file, int line, const char *func,
    SQLHSTMT StatementHandle, SQLSMALLINT *ColumnCountPtr)
{
  LOGD_ODBC(file, line, func, "SQLNumResultCols(StatementHandle:%p,ColumnCountPtr:%p) ...", StatementHandle, ColumnCountPtr);
  SQLRETURN sr = SQLNumResultCols(StatementHandle, ColumnCountPtr);
  diag(sr, SQL_HANDLE_STMT, StatementHandle);
  SQLSMALLINT n = ColumnCountPtr ? *ColumnCountPtr : 0;
  LOGD_ODBC(file, line, func, "SQLNumResultCols(StatementHandle:%p,ColumnCountPtr:%p(%d)) => %s", StatementHandle, ColumnCountPtr, n, sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLGetData(const char *file, int line, const char *func,
    SQLHSTMT StatementHandle, SQLUSMALLINT Col_or_Param_Num, SQLSMALLINT TargetType, SQLPOINTER TargetValuePtr, SQLLEN BufferLength, SQLLEN *StrLen_or_IndPtr)
{
  LOGD_ODBC(file, line, func, "SQLGetData(StatementHandle:%p,Col_or_Param_Num:%d,"
      "TargetType:%s,TargetValuePtr:%p,BufferLength:%zd,StrLen_or_IndPtr:%p) ...",
      StatementHandle, Col_or_Param_Num, sqlc_data_type(TargetType), TargetValuePtr, BufferLength, StrLen_or_IndPtr);
  SQLRETURN sr = SQLGetData(StatementHandle, Col_or_Param_Num, TargetType, TargetValuePtr, (size_t)BufferLength, StrLen_or_IndPtr);
  diag(sr, SQL_HANDLE_STMT, StatementHandle);
  char buf[1024]; buf[0] = '\0';
  const char *s = NULL;
  if ((sr == SQL_SUCCESS || sr == SQL_SUCCESS_WITH_INFO) &&StrLen_or_IndPtr) {
    SQLLEN n = *StrLen_or_IndPtr;
    if (n == SQL_NULL_DATA) {
      s = "SQL_NULL_DATA";
    } else if (n == SQL_NO_TOTAL) {
      s = "SQL_NO_TOTAL";
    } else {
      snprintf(buf, sizeof(buf), "%zd", (size_t)n);
      s = buf;
    }
  } else {
    s = "";
  }
  LOGD_ODBC(file, line, func, "SQLGetData(StatementHandle:%p,Col_or_Param_Num:%d,"
      "TargetType:%s,TargetValuePtr:%p,BufferLength:%zd,StrLen_or_IndPtr:%p(%s)) => %s",
      StatementHandle, Col_or_Param_Num, sqlc_data_type(TargetType), TargetValuePtr, (size_t)BufferLength, StrLen_or_IndPtr, s, sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLPrepare(const char *file, int line, const char *func,
    SQLHSTMT StatementHandle, SQLCHAR *StatementText, SQLINTEGER TextLength)
{
  int n1 = (TextLength == SQL_NTS) ? (StatementText ? (int)strlen((const char*)StatementText) : 0) : (int)TextLength;
  LOGD_ODBC(file, line, func, "SQLPrepare(StatementHandle:%p,StatementText:%.*s,TextLength:%d) ...", StatementHandle, n1, StatementText, TextLength);
  SQLRETURN sr = SQLPrepare(StatementHandle, StatementText, TextLength);
  diag(sr, SQL_HANDLE_STMT, StatementHandle);
  LOGD_ODBC(file, line, func, "SQLPrepare(StatementHandle:%p,StatementText:%.*s,TextLength:%d) => %s", StatementHandle, n1, StatementText, TextLength, sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLNumParams(const char *file, int line, const char *func,
    SQLHSTMT StatementHandle, SQLSMALLINT *ParameterCountPtr)
{
  LOGD_ODBC(file, line, func, "SQLNumParams(StatementHandle:%p,ParameterCountPtr:%p) ...", StatementHandle, ParameterCountPtr);
  SQLRETURN sr = SQLNumParams(StatementHandle, ParameterCountPtr);
  diag(sr, SQL_HANDLE_STMT, StatementHandle);
  SQLSMALLINT n = ParameterCountPtr ? *ParameterCountPtr : 0;
  LOGD_ODBC(file, line, func, "SQLNumParams(StatementHandle:%p,ParameterCountPtr:%p(%d)) => %s", StatementHandle, ParameterCountPtr, n, sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLDescribeParam(const char *file, int line, const char *func,
    SQLHSTMT StatementHandle, SQLUSMALLINT ParameterNumber,
    SQLSMALLINT *DataTypePtr, SQLULEN *ParameterSizePtr, SQLSMALLINT *DecimalDigitsPtr, SQLSMALLINT *NullablePtr)
{
  LOGD_ODBC(file, line, func, "SQLDescribeParam(StatementHandle:%p,ParameterNumber:%d,"
      "DataTypePtr:%p,ParameterSizePtr:%p,DecimalDigitsPtr:%p,NullablePtr:%p) ...",
      StatementHandle, ParameterNumber, DataTypePtr, ParameterSizePtr, DecimalDigitsPtr, NullablePtr);
  SQLRETURN sr = SQLDescribeParam(StatementHandle, ParameterNumber,
      DataTypePtr, ParameterSizePtr, DecimalDigitsPtr, NullablePtr);
  diag(sr, SQL_HANDLE_STMT, StatementHandle);
  const char *a = DataTypePtr ? sql_data_type(*DataTypePtr) : NULL;
  SQLULEN b = ParameterSizePtr ? *ParameterSizePtr : 0;
  SQLSMALLINT c = DecimalDigitsPtr ? *DecimalDigitsPtr : 0;
  const char *d = NullablePtr ? (*NullablePtr ? "true" : "false") : NULL;
  LOGD_ODBC(file, line, func, "SQLDescribeParam(StatementHandle:%p,ParameterNumber:%d,"
      "DataTypePtr:%p(%s),ParameterSizePtr:%p(%zd),DecimalDigitsPtr:%p(%d),NullablePtr:%p(%s)) => %s",
      StatementHandle, ParameterNumber, DataTypePtr, a, ParameterSizePtr, (size_t)b, DecimalDigitsPtr, c, NullablePtr, d,
      sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLBindParameter(const char *file, int line, const char *func,
    SQLHSTMT StatementHandle, SQLUSMALLINT ParameterNumber, SQLSMALLINT InputOutputType, SQLSMALLINT ValueType,
    SQLSMALLINT ParameterType, SQLULEN ColumnSize, SQLSMALLINT DecimalDigits, SQLPOINTER ParameterValuePtr, SQLLEN BufferLength, SQLLEN *StrLen_or_IndPtr)
{
  LOGD_ODBC(file, line, func, "SQLBindParameter(StatementHandle:%p,ParameterNumber:%d,InputOutputType:%s,ValueType:%s,"
      "ParameterType:%s,ColumnSize:%zd,DecimalDigits:%d,ParameterValuePtr:%p,BufferLength:%zd,StrLen_or_IndPtr:%p) ...",
      StatementHandle, ParameterNumber, sql_input_output_type(InputOutputType), sqlc_data_type(ValueType),
      sql_data_type(ParameterType), (size_t)ColumnSize, DecimalDigits, ParameterValuePtr, (size_t)BufferLength, StrLen_or_IndPtr);
  SQLRETURN sr = SQLBindParameter(StatementHandle, ParameterNumber, InputOutputType, ValueType,
      ParameterType, ColumnSize, DecimalDigits, ParameterValuePtr, BufferLength, StrLen_or_IndPtr);
  diag(sr, SQL_HANDLE_STMT, StatementHandle);
  LOGD_ODBC(file, line, func, "SQLBindParameter(StatementHandle:%p,ParameterNumber:%d,InputOutputType:%s,ValueType:%s,"
      "ParameterType:%s,ColumnSize:%zd,DecimalDigits:%d,ParameterValuePtr:%p,BufferLength:%zd,StrLen_or_IndPtr:%p) => %s",
      StatementHandle, ParameterNumber, sql_input_output_type(InputOutputType), sqlc_data_type(ValueType),
      sql_data_type(ParameterType), (size_t)ColumnSize, DecimalDigits, ParameterValuePtr, (size_t)BufferLength, StrLen_or_IndPtr,
      sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLSetStmtAttr(const char *file, int line, const char *func,
    SQLHSTMT StatementHandle, SQLINTEGER Attribute, SQLPOINTER ValuePtr, SQLINTEGER StringLength)
{
  LOGD_ODBC(file, line, func, "SQLSetStmtAttr(StatementHandle:%p,Attribute:%s[%d/0x%x],ValuePtr:%p,StringLength:%d) ...",
      StatementHandle, sql_stmt_attr(Attribute), Attribute, Attribute, ValuePtr, StringLength);
  SQLRETURN sr = SQLSetStmtAttr(StatementHandle, Attribute, ValuePtr, StringLength);
  diag(sr, SQL_HANDLE_STMT, StatementHandle);
  LOGD_ODBC(file, line, func, "SQLSetStmtAttr(StatementHandle:%p,Attribute:%s[%d/0x%x],ValuePtr:%p,StringLength:%d) => %s",
      StatementHandle, sql_stmt_attr(Attribute), Attribute, Attribute, ValuePtr, StringLength, sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLGetStmtAttr(const char *file, int line, const char *func,
    SQLHSTMT StatementHandle, SQLINTEGER Attribute, SQLPOINTER ValuePtr, SQLINTEGER BufferLength, SQLINTEGER *StringLength)
{
  LOGD_ODBC(file, line, func, "SQLGetStmtAttr(StatementHandle:%p,Attribute:%s[%d/0x%x],ValuePtr:%p,BufferLength:%d,StringLength:%p) ...",
      StatementHandle, sql_stmt_attr(Attribute), Attribute, Attribute, ValuePtr, BufferLength, StringLength);
  SQLRETURN sr = SQLGetStmtAttr(StatementHandle, Attribute, ValuePtr, BufferLength, StringLength);
  diag(sr, SQL_HANDLE_STMT, StatementHandle);
  LOGD_ODBC(file, line, func, "SQLGetStmtAttr(StatementHandle:%p,Attribute:%s[%d/0x%x],ValuePtr:%p,BufferLength:%d,StringLength:%p(%d)) => %s",
      StatementHandle, sql_stmt_attr(Attribute), Attribute, Attribute, ValuePtr, BufferLength, StringLength, StringLength ? *StringLength : 0, sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLExecute(const char *file, int line, const char *func,
    SQLHSTMT StatementHandle)
{
  LOGD_ODBC(file, line, func, "SQLExecute(StatementHandle:%p) ...", StatementHandle);
  SQLRETURN sr = SQLExecute(StatementHandle);
  diag(sr, SQL_HANDLE_STMT, StatementHandle);
  LOGD_ODBC(file, line, func, "SQLExecute(StatementHandle:%p) => %s", StatementHandle, sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLEndTran(const char *file, int line, const char *func,
    SQLSMALLINT   HandleType,
    SQLHANDLE     Handle,
    SQLSMALLINT   CompletionType)
{
  LOGD_ODBC(file, line, func, "SQLEndTran(HandleType:%s,Handle:%p,CompletionType:%s) ...",
      sql_handle_type(HandleType), Handle, sql_completion_type(CompletionType));
  SQLRETURN sr = SQLEndTran(HandleType, Handle, CompletionType);
  diag(sr, SQL_HANDLE_DBC, Handle);
  diag(sr, SQL_HANDLE_STMT, Handle);
  LOGD_ODBC(file, line, func, "SQLEndTran(HandleType:%s,Handle:%p,CompletionType:%s) => %s",
      sql_handle_type(HandleType), Handle, sql_completion_type(CompletionType),
      sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLFreeStmt(const char *file, int line, const char *func,
    SQLHSTMT       StatementHandle,
    SQLUSMALLINT   Option)
{
  LOGD_ODBC(file, line, func, "SQLFreeStmt(StatementHandle:%p, Option:%s) ...",
      StatementHandle, sql_free_statement_option(Option));
  SQLRETURN sr = SQLFreeStmt(StatementHandle, Option);
  LOGD_ODBC(file, line, func, "SQLFreeStmt(StatementHandle:%p, Option:%s) => %s",
      StatementHandle, sql_free_statement_option(Option), sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLCloseCursor(const char *file, int line, const char *func,
    SQLHSTMT StatementHandle)
{
  LOGD_ODBC(file, line, func, "SQLCloseCursor(StatementHandle:%p) ...", StatementHandle);
  SQLRETURN sr = SQLCloseCursor(StatementHandle);
  diag(sr, SQL_HANDLE_STMT, StatementHandle);
  LOGD_ODBC(file, line, func, "SQLCloseCursor(StatementHandle:%p) => %s", StatementHandle, sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLSetConnectAttr(const char *file, int line, const char *func,
    SQLHDBC ConnectionHandle, SQLINTEGER Attribute, SQLPOINTER ValuePtr, SQLINTEGER StringLength)
{
  LOGD_ODBC(file, line, func, "SQLSetConnectAttr(ConnectionHandle:%p,Attribute:%s,ValuePtr:%p,StringLength:%d) ...",
      ConnectionHandle, sql_stmt_attr(Attribute), ValuePtr, StringLength);
  SQLRETURN sr = SQLSetConnectAttr(ConnectionHandle, Attribute, ValuePtr, StringLength);
  diag(sr, SQL_HANDLE_STMT, ConnectionHandle);
  LOGD_ODBC(file, line, func, "SQLSetConnectAttr(ConnectionHandle:%p,Attribute:%s,ValuePtr:%p,StringLength:%d) => %s",
      ConnectionHandle, sql_conn_attr(Attribute), ValuePtr, StringLength, sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLBindCol(const char *file, int line, const char *func,
    SQLHSTMT StatementHandle, SQLUSMALLINT ColumnNumber, SQLSMALLINT TargetType, SQLPOINTER TargetValuePtr,
    SQLLEN BufferLength, SQLLEN *StrLen_or_IndPtr)
{
  LOGD_ODBC(file, line, func, "SQLBindCol(StatementHandle:%p,ColumnNumber:%d,TargetType:%s,"
      "TargetValuePtr:%p,BufferLength:%zd,StrLen_or_IndPtr:%p) ...",
      StatementHandle, ColumnNumber, sqlc_data_type(TargetType),
      TargetValuePtr, (size_t)BufferLength, StrLen_or_IndPtr);
  SQLRETURN sr = SQLBindCol(StatementHandle, ColumnNumber, TargetType,
      TargetValuePtr, BufferLength, StrLen_or_IndPtr);
  diag(sr, SQL_HANDLE_STMT, StatementHandle);
  LOGD_ODBC(file, line, func, "SQLBindCol(StatementHandle:%p,ColumnNumber:%d,TargetType:%s,"
      "TargetValuePtr:%p,BufferLength:%zd,StrLen_or_IndPtr:%p) => %s",
      StatementHandle, ColumnNumber, sqlc_data_type(TargetType),
      TargetValuePtr, (size_t)BufferLength, StrLen_or_IndPtr,
      sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLDescribeColW(const char *file, int line, const char *func,
    SQLHSTMT StatementHandle, SQLUSMALLINT ColumnNumber, SQLWCHAR *ColumnName, SQLSMALLINT BufferLength,
    SQLSMALLINT *NameLengthPtr, SQLSMALLINT *DataTypePtr, SQLULEN *ColumnSizePtr, SQLSMALLINT *DecimalDigitsPtr, SQLSMALLINT *NullablePtr)
{
  LOGD_ODBC(file, line, func, "SQLDescribeColW(StatementHandle:%p,ColumnNumber:%d,ColumnName:%p,BufferLength:%d,"
      "NameLengthPtr:%p,DataTypePtr:%p,ColumnSizePtr:%p,DecimalDigitsPtr:%p,NullablePtr:%p) ...",
      StatementHandle, ColumnNumber, ColumnName, BufferLength, NameLengthPtr, DataTypePtr, ColumnSizePtr, DecimalDigitsPtr, NullablePtr);
  SQLRETURN sr = SQLDescribeColW(StatementHandle, ColumnNumber, ColumnName, BufferLength, NameLengthPtr,
      DataTypePtr, ColumnSizePtr, DecimalDigitsPtr, NullablePtr);
  diag(sr, SQL_HANDLE_STMT, StatementHandle);
  LOGD_ODBC(file, line, func, "SQLDescribeColW(StatementHandle:%p,ColumnNumber:%d,ColumnName:%p,BufferLength:%d,"
      "NameLengthPtr:%p,DataTypePtr:%p,ColumnSizePtr:%p,DecimalDigitsPtr:%p,NullablePtr:%p) => %s",
      StatementHandle, ColumnNumber, ColumnName, BufferLength, NameLengthPtr, DataTypePtr, ColumnSizePtr, DecimalDigitsPtr, NullablePtr,
      sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLTables(const char *file, int line, const char *func,
    SQLHSTMT       StatementHandle,
    SQLCHAR       *CatalogName,
    SQLSMALLINT    NameLength1,
    SQLCHAR       *SchemaName,
    SQLSMALLINT    NameLength2,
    SQLCHAR       *TableName,
    SQLSMALLINT    NameLength3,
    SQLCHAR       *TableType,
    SQLSMALLINT    NameLength4)
{
  int n1 = NameLength1;
  if (n1 == SQL_NTS) n1 = CatalogName ? (int)strlen((const char*)CatalogName) : 0;

  int n2 = NameLength2;
  if (n2 == SQL_NTS) n2 = SchemaName ? (int)strlen((const char*)SchemaName) : 0;

  int n3 = NameLength3;
  if (n3 == SQL_NTS) n3 = TableName ? (int)strlen((const char*)TableName) : 0;

  int n4 = NameLength4;
  if (n4 == SQL_NTS) n4 = TableType ? (int)strlen((const char*)TableType) : 0;

  LOGD_ODBC(file, line, func, "SQLTables(StatementHandle:%p,CatalogName:%p(%.*s),NameLength1:%d,SchemaName:%p(%.*s),NameLength2(%d),"
      "TableName:%p(%.*s),NameLength3:%d,TableType:%p(%.*s),NameLength4:%d) ...",
      StatementHandle, CatalogName, n1, CatalogName, n1, SchemaName, n2, SchemaName, n2,
      TableName, n3, TableName, n3, TableType, n4, TableType, n4);
  SQLRETURN sr = SQLTables(StatementHandle, CatalogName, NameLength1, SchemaName, NameLength2, TableName, NameLength3, TableType, NameLength4);
  diag(sr, SQL_HANDLE_STMT, StatementHandle);
  LOGD_ODBC(file, line, func, "SQLTables(StatementHandle:%p,CatalogName:%p(%.*s),NameLength1:%d,SchemaName:%p(%.*s),NameLength2(%d),"
      "TableName:%p(%.*s),NameLength3:%d,TableType:%p(%.*s),NameLength4:%d) => %s",
      StatementHandle, CatalogName, n1, CatalogName, n1, SchemaName, n2, SchemaName, n2,
      TableName, n3, TableName, n3, TableType, n4, TableType, n4,
      sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLGetInfo(const char *file, int line, const char *func,
    SQLHSTMT        ConnectionHandle,
    SQLUSMALLINT    InfoType,
    SQLPOINTER      InfoValuePtr,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLengthPtr)
{
  LOGD_ODBC(file, line, func, "SQLGetInfo(ConnectionHandle:%p,InfoType:%s,InfoValuePtr:%p,BufferLength:%d,StringLengthPtr:%p) ...",
      ConnectionHandle, sql_info_type(InfoType), InfoValuePtr, BufferLength, StringLengthPtr);
  SQLRETURN sr = SQLGetInfo(ConnectionHandle, InfoType, InfoValuePtr, BufferLength, StringLengthPtr);
  diag(sr, SQL_HANDLE_STMT, ConnectionHandle);
  LOGD_ODBC(file, line, func, "SQLGetInfo(ConnectionHandle:%p,InfoType:%s,InfoValuePtr:%p,BufferLength:%d,StringLengthPtr:%p) => %s",
      ConnectionHandle, sql_info_type(InfoType), InfoValuePtr, BufferLength, StringLengthPtr,
      sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLFetchScroll(const char *file, int line, const char *func,
    SQLHSTMT StatementHandle, SQLSMALLINT FetchOrientation, SQLLEN FetchOffset)
{
  LOGD_ODBC(file, line, func, "SQLFetchScroll(StatementHandle:%p,FetchOrientation:%s,FetchOffset:%zd) ...",
      StatementHandle, sql_fetch_orientation(FetchOrientation), (size_t)FetchOffset);
  SQLRETURN sr = SQLFetchScroll(StatementHandle, FetchOrientation, FetchOffset);
  diag(sr, SQL_HANDLE_STMT, StatementHandle);
  LOGD_ODBC(file, line, func, "SQLFetchScroll(StatementHandle:%p,FetchOrientation:%s,FetchOffset:%zd) => %s",
      StatementHandle, sql_fetch_orientation(FetchOrientation), (size_t)FetchOffset, sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLGetDiagField(const char *file, int line, const char *func,
    SQLSMALLINT HandleType, SQLHANDLE Handle,
    SQLSMALLINT RecNumber, SQLSMALLINT DiagIdentifier,
    SQLPOINTER DiagInfo, SQLSMALLINT BufferLength,
    SQLSMALLINT *StringLength)
{
  LOGD_ODBC(file, line, func, "SQLGetDiagField(HandleType:%s,Handle:%p,RecNumber:%d,DiagIdentifier:%s,DiagInfo:%p,BufferLength:%d,StringLength:%p) ...",
      sql_handle_type(HandleType), Handle, RecNumber, sql_diag_identifier(DiagIdentifier), DiagInfo, BufferLength, StringLength);
  SQLRETURN sr = SQLGetDiagField(HandleType, Handle, RecNumber, DiagIdentifier, DiagInfo, BufferLength, StringLength);
  diag(sr, HandleType, Handle);
  LOGD_ODBC(file, line, func, "SQLGetDiagField(HandleType:%s,Handle:%p,RecNumber:%d,DiagIdentifier:%s,DiagInfo:%p,BufferLength:%d,StringLength:%p(%d)) => %s",
            sql_handle_type(HandleType), Handle, RecNumber, sql_diag_identifier(DiagIdentifier), DiagInfo, BufferLength, StringLength, StringLength ? *StringLength : 0, sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLGetConnectAttr(const char *file, int line, const char *func,
    SQLHDBC        ConnectionHandle,
    SQLINTEGER     Attribute,
    SQLPOINTER     ValuePtr,
    SQLINTEGER     BufferLength,
    SQLINTEGER *   StringLengthPtr)
{
  LOGD_ODBC(file, line, func, "SQLGetConnectAttr(ConnectionHandle:%p,Attribute:%s[%d/0x%x],ValuePtr:%p,BufferLength:%d,StringLengthPtr:%p) ...",
      ConnectionHandle,
      sql_conn_attr(Attribute), Attribute, Attribute, ValuePtr, BufferLength, StringLengthPtr);
  SQLRETURN sr = SQLGetConnectAttr(ConnectionHandle, Attribute, ValuePtr, BufferLength, StringLengthPtr);
  diag(sr, SQL_HANDLE_DBC, ConnectionHandle);
  LOGD_ODBC(file, line, func, "SQLGetConnectAttr(ConnectionHandle:%p,Attribute:%s[%d/0x%x],ValuePtr:%p,BufferLength:%d,StringLengthPtr:%p(%d)) => %s",
      ConnectionHandle,
      sql_conn_attr(Attribute), Attribute, Attribute, ValuePtr, BufferLength, StringLengthPtr, StringLengthPtr ? *StringLengthPtr : 0, sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLColumns(const char *file, int line, const char *func,
    SQLHSTMT       StatementHandle,
    SQLCHAR       *CatalogName,
    SQLSMALLINT    NameLength1,
    SQLCHAR       *SchemaName,
    SQLSMALLINT    NameLength2,
    SQLCHAR       *TableName,
    SQLSMALLINT    NameLength3,
    SQLCHAR       *ColumnName,
    SQLSMALLINT    NameLength4)
{
  int n1 = NameLength1;
  if (n1 == SQL_NTS) n1 = CatalogName ? (int)strlen((const char*)CatalogName) : 0;

  int n2 = NameLength2;
  if (n2 == SQL_NTS) n2 = SchemaName ? (int)strlen((const char*)SchemaName) : 0;

  int n3 = NameLength3;
  if (n3 == SQL_NTS) n3 = TableName ? (int)strlen((const char*)TableName) : 0;

  int n4 = NameLength4;
  if (n4 == SQL_NTS) n4 = ColumnName ? (int)strlen((const char*)ColumnName) : 0;

  LOGD_ODBC(file, line, func, "SQLColumns(StatementHandle:%p,CatalogName:%p(%.*s),NameLength1:%d,SchemaName:%p(%.*s),NameLength2(%d),"
      "TableName:%p(%.*s),NameLength3:%d,ColumnName:%p(%.*s),NameLength4:%d) ...",
      StatementHandle, CatalogName, n1, CatalogName, n1, SchemaName, n2, SchemaName, n2,
      TableName, n3, TableName, n3, ColumnName, n4, ColumnName, n4);
  SQLRETURN sr = SQLColumns(StatementHandle, CatalogName, NameLength1, SchemaName, NameLength2, TableName, NameLength3, ColumnName, NameLength4);
  diag(sr, SQL_HANDLE_STMT, StatementHandle);
  LOGD_ODBC(file, line, func, "SQLColumns(StatementHandle:%p,CatalogName:%p(%.*s),NameLength1:%d,SchemaName:%p(%.*s),NameLength2(%d),"
      "TableName:%p(%.*s),NameLength3:%d,ColumnName:%p(%.*s),NameLength4:%d) => %s",
      StatementHandle, CatalogName, n1, CatalogName, n1, SchemaName, n2, SchemaName, n2,
      TableName, n3, TableName, n3, ColumnName, n4, ColumnName, n4,
      sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLColAttribute(const char *file, int line, const char *func,
    SQLHSTMT       StatementHandle,
    SQLUSMALLINT   ColumnNumber,
    SQLUSMALLINT   FieldIdentifier,
    SQLPOINTER     CharacterAttributePtr,
    SQLSMALLINT    BufferLength,
    SQLSMALLINT   *StringLengthPtr,
    SQLLEN        *NumericAttributePtr)
{
  LOGD_ODBC(file, line, func, "SQLColAttribute(StatementHandle:%p,ColumnNumber:%d,FieldIdentifier:%s,CharacterAttributePtr:%p,BufferLength(%d),"
      "StringLengthPtr:%p,NumericAttributePtr:%p) ...",
      StatementHandle, ColumnNumber,sql_col_attribute(FieldIdentifier),CharacterAttributePtr,BufferLength,StringLengthPtr,NumericAttributePtr);
  SQLRETURN sr = SQLColAttribute(StatementHandle, ColumnNumber,FieldIdentifier,CharacterAttributePtr,BufferLength,StringLengthPtr,NumericAttributePtr);
  diag(sr, SQL_HANDLE_STMT, StatementHandle);
  LOGD_ODBC(file, line, func, "SQLColAttribute(StatementHandle:%p,ColumnNumber:%d,FieldIdentifier:%s,CharacterAttributePtr:%p,BufferLength(%d),"
      "StringLengthPtr:%p[%d],NumericAttributePtr:%p) => %s",
      StatementHandle, ColumnNumber,sql_col_attribute(FieldIdentifier),CharacterAttributePtr,BufferLength,
      StringLengthPtr,StringLengthPtr?*StringLengthPtr:0,NumericAttributePtr,
      sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLGetTypeInfo(const char *file, int line, const char *func,
    SQLHSTMT       StatementHandle,
    SQLSMALLINT    DataType)
{
  LOGD_ODBC(file, line, func, "SQLGetTypeInfo(StatementHandle:%p,DataType:[%d]%s) ...",
      StatementHandle, DataType, sql_data_type(DataType));
  SQLRETURN sr = SQLGetTypeInfo(StatementHandle, DataType);
  diag(sr, SQL_HANDLE_STMT, StatementHandle);
  LOGD_ODBC(file, line, func, "SQLGetTypeInfo(StatementHandle:%p,DataType:[%d]%s) => %s",
      StatementHandle, DataType, sql_data_type(DataType),
      sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLMoreResults(const char *file, int line, const char *func,
    SQLHSTMT StatementHandle)
{
  LOGD_ODBC(file, line, func, "SQLMoreResults(StatementHandle:%p) ...", StatementHandle);
  SQLRETURN sr = SQLMoreResults(StatementHandle);
  diag(sr, SQL_HANDLE_STMT, StatementHandle);
  LOGD_ODBC(file, line, func, "SQLMoreResults(StatementHandle:%p) => %s", StatementHandle, sql_return_type(sr));
  return sr;
}

static inline SQLRETURN call_SQLRowCount(const char *file, int line, const char *func,
    SQLHSTMT StatementHandle, SQLLEN *RowCountPtr)
{
  LOGD_ODBC(file, line, func, "SQLRowCount(StatementHandle:%p,RowCountPtr:%p) ...", StatementHandle, RowCountPtr);
  SQLLEN nr = 0;
  SQLRETURN sr = SQLRowCount(StatementHandle, &nr);
  diag(sr, SQL_HANDLE_STMT, StatementHandle);
  LOGD_ODBC(file, line, func, "SQLRowCount(StatementHandle:%p,RowCountPtr:%p[%zd]) => %s",
      StatementHandle, RowCountPtr, nr, sql_return_type(sr));
  if (RowCountPtr) *RowCountPtr = nr;
  return sr;
}

#define CALL_SQLAllocHandle(...)                   call_SQLAllocHandle(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLFreeHandle(...)                    call_SQLFreeHandle(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLSetEnvAttr(...)                    call_SQLSetEnvAttr(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLConnect(...)                       call_SQLConnect(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLDriverConnect(...)                 call_SQLDriverConnect(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLDisconnect(...)                    call_SQLDisconnect(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLDescribeCol(...)                   call_SQLDescribeCol(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLFetch(...)                         call_SQLFetch(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLExecDirect(...)                    call_SQLExecDirect(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLNumResultCols(...)                 call_SQLNumResultCols(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLGetData(...)                       call_SQLGetData(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLPrepare(...)                       call_SQLPrepare(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLNumParams(...)                     call_SQLNumParams(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLDescribeParam(...)                 call_SQLDescribeParam(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLBindParameter(...)                 call_SQLBindParameter(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLSetStmtAttr(...)                   call_SQLSetStmtAttr(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLGetStmtAttr(...)                   call_SQLGetStmtAttr(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLExecute(...)                       call_SQLExecute(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLEndTran(...)                       call_SQLEndTran(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLFreeStmt(...)                      call_SQLFreeStmt(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLCloseCursor(...)                   call_SQLCloseCursor(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLSetConnectAttr(...)                call_SQLSetConnectAttr(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLBindCol(...)                       call_SQLBindCol(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLDescribeColW(...)                  call_SQLDescribeColW(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLTables(...)                        call_SQLTables(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLGetInfo(...)                       call_SQLGetInfo(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLFetchScroll(...)                   call_SQLFetchScroll(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLGetDiagField(...)                  call_SQLGetDiagField(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLGetConnectAttr(...)                call_SQLGetConnectAttr(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLColumns(...)                       call_SQLColumns(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLColAttribute(...)                  call_SQLColAttribute(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLGetTypeInfo(...)                   call_SQLGetTypeInfo(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLMoreResults(...)                   call_SQLMoreResults(__FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define CALL_SQLRowCount(...)                      call_SQLRowCount(__FILE__, __LINE__, __func__, ##__VA_ARGS__)



typedef enum odbc_stage_e {
  ODBC_INITED,
  ODBC_ENV,
  ODBC_DBC,
  ODBC_STMT,
} odbc_stage_t;

typedef struct odbc_handles_s       odbc_handles_t;
struct odbc_handles_s {
  SQLHANDLE                henv;
  SQLHANDLE                hconn;
  SQLHANDLE                hstmt;

  uint8_t                  taos_backend:1;
  uint8_t                  support_catalog:1;
};

typedef struct odbc_case_s          odbc_case_t;

typedef int (*odbc_f)(odbc_case_t *odbc_case, odbc_stage_t stage, odbc_handles_t *handles);

struct odbc_case_s {
  const char               *file;
  int                       line;

  const char               *name;
  odbc_f                    run;
};

#define ODBC_CASE(x) {__FILE__, __LINE__, #x, x}

typedef struct odbc_conn_arg_s            odbc_conn_arg_t;
struct odbc_conn_arg_s {
  const char      *dsn;
  const char      *uid;
  const char      *pwd;
  const char      *connstr;
  int              is_tdengine;  // NOTE: caller specifies
};

int run_odbc_cases(const char *name, const odbc_conn_arg_t *conn_arg, odbc_case_t *cases, size_t cases_nr) FA_HIDDEN;




EXTERN_C_END


#endif // _odbc_helper_h_


