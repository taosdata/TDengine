#include "base.h"

#include "base/env.h"
#include "base/null_conn.h"
#include "base/tsdb_impl.h"

#include "todbc_flex.h"
#include "todbc_tls.h"
#include "todbc_util.h"

#ifdef _MSC_VER
#include <odbcinst.h>
#endif // _MSC_VER

#define PROFILING 0
#define LOGGING   0

#define PROFILE(r_0911, statement)                    \
do {                                                  \
  if (!PROFILING) {                                   \
    if (LOGGING) D("");                               \
    statement;                                        \
    if (LOGGING) D("r=%zx", (size_t)r_0911);          \
    break;                                            \
  }                                                   \
  if (LOGGING) D("");                                 \
  struct timeval tv0, tv1;                            \
  gettimeofday(&tv0, NULL);                           \
  statement;                                          \
  gettimeofday(&tv1, NULL);                           \
  double delta = difftime(tv1.tv_sec, tv0.tv_sec);    \
  delta *= 1000000;                                   \
  delta += (double)(tv1.tv_usec-tv0.tv_usec);         \
  delta /= 1000000;                                   \
  D("%s: elapsed: [%.6f]s", #statement, delta);       \
  if (LOGGING) D("r=%zx", (size_t)r_0911);            \
} while (0)

#define P(fmt,...) do {                               \
  if (LOGGING) {                                      \
    D(fmt, ##__VA_ARGS__);                            \
  }                                                   \
} while (0)



static SQLRETURN doSQLAllocConnect(SQLHENV EnvironmentHandle, SQLHDBC *ConnectionHandle);

SQLRETURN SQL_API SQLAllocConnect(SQLHENV EnvironmentHandle, SQLHDBC *ConnectionHandle)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_ENV, EnvironmentHandle);
  PROFILE(r, r = doSQLAllocConnect(EnvironmentHandle, ConnectionHandle));
  todbc_tls_buf_reclaim();
  return r;
}


static SQLRETURN doSQLAllocEnv(SQLHENV *EnvironmentHandle);

SQLRETURN SQL_API SQLAllocEnv(SQLHENV *EnvironmentHandle)
{
  SQLRETURN r;
  PROFILE(r, r = doSQLAllocEnv(EnvironmentHandle));
  todbc_tls_buf_reclaim();
  return r;
}

static SQLRETURN doSQLAllocHandle(SQLSMALLINT HandleType, SQLHANDLE InputHandle, SQLHANDLE *OutputHandle);

SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT HandleType, SQLHANDLE InputHandle, SQLHANDLE *OutputHandle)
{
  SQLRETURN r;
  // HandleType is NOT the type of InputHandle
  PROFILE(r, r = doSQLAllocHandle(HandleType, InputHandle, OutputHandle));
  todbc_tls_buf_reclaim();
  return r;
}

static SQLRETURN doSQLAllocStmt(SQLHDBC ConnectionHandle, SQLHSTMT *StatementHandle);

SQLRETURN SQL_API SQLAllocStmt(SQLHDBC ConnectionHandle, SQLHSTMT *StatementHandle)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_DBC, ConnectionHandle);
  PROFILE(r, r = doSQLAllocStmt(ConnectionHandle, StatementHandle));
  todbc_tls_buf_reclaim();
  return r;
}

static SQLRETURN doSQLBindCol(SQLHSTMT StatementHandle,
    SQLUSMALLINT ColumnNumber, SQLSMALLINT TargetType,
    SQLPOINTER TargetValue,
    SQLLEN BufferLength, SQLLEN *StrLen_or_IndPtr);

SQLRETURN SQL_API SQLBindCol(SQLHSTMT StatementHandle,
    SQLUSMALLINT ColumnNumber, SQLSMALLINT TargetType,
    SQLPOINTER TargetValue,
    SQLLEN BufferLength, SQLLEN *StrLen_or_IndPtr)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_STMT, StatementHandle);
  PROFILE(r, r = doSQLBindCol(StatementHandle, ColumnNumber, TargetType,
        TargetValue, BufferLength, StrLen_or_IndPtr));
  todbc_tls_buf_reclaim();
  return r;
}

SQLRETURN SQL_API SQLBindParam(SQLHSTMT StatementHandle,
    SQLUSMALLINT ParameterNumber, SQLSMALLINT ValueType,
    SQLSMALLINT ParameterType, SQLULEN LengthPrecision,
    SQLSMALLINT ParameterScale, SQLPOINTER ParameterValue,
    SQLLEN *StrLen_or_IndPtr)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_STMT, StatementHandle);
  PROFILE(r, r = SQL_ERROR);
  todbc_tls_buf_reclaim();
  return r;
}

SQLRETURN SQL_API SQLCancel(SQLHSTMT StatementHandle)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_STMT, StatementHandle);
  PROFILE(r, r = SQL_ERROR);
  todbc_tls_buf_reclaim();
  return r;
}

SQLRETURN SQL_API SQLCancelHandle(SQLSMALLINT HandleType, SQLHANDLE InputHandle)
{
  SQLRETURN r;
  errs_clear(HandleType, InputHandle);
  PROFILE(r, r = SQL_ERROR);
  todbc_tls_buf_reclaim();
  return r;
}

static SQLRETURN doSQLCloseCursor(SQLHSTMT StatementHandle);

SQLRETURN SQL_API SQLCloseCursor(SQLHSTMT StatementHandle)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_STMT, StatementHandle);
  PROFILE(r, r = doSQLCloseCursor(StatementHandle));
  todbc_tls_buf_reclaim();
  return r;
}

SQLRETURN SQL_API SQLColAttribute (SQLHSTMT StatementHandle,
           SQLUSMALLINT ColumnNumber, SQLUSMALLINT FieldIdentifier,
           SQLPOINTER CharacterAttribute, SQLSMALLINT BufferLength,
           SQLSMALLINT *StringLength, SQLLEN *NumericAttribute)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_STMT, StatementHandle);
  PROFILE(r, r = SQL_ERROR);
  todbc_tls_buf_reclaim();
  return r;
}

SQLRETURN SQL_API SQLColumns(SQLHSTMT StatementHandle,
           SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
           SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
           SQLCHAR *TableName, SQLSMALLINT NameLength3,
           SQLCHAR *ColumnName, SQLSMALLINT NameLength4)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_STMT, StatementHandle);
  PROFILE(r, r = SQL_ERROR);
  todbc_tls_buf_reclaim();
  return r;
}

SQLRETURN SQL_API SQLCompleteAsync(SQLSMALLINT HandleType, SQLHANDLE Handle, RETCODE* AsyncRetCodePtr)
{
  SQLRETURN r;
  errs_clear(HandleType, Handle);
  PROFILE(r, r = SQL_ERROR);
  todbc_tls_buf_reclaim();
  return r;
}

static SQLRETURN doSQLConnect(SQLHDBC ConnectionHandle,
    SQLCHAR *ServerName, SQLSMALLINT NameLength1,
    SQLCHAR *UserName, SQLSMALLINT NameLength2,
    SQLCHAR *Authentication, SQLSMALLINT NameLength3);

SQLRETURN SQL_API SQLConnect(SQLHDBC ConnectionHandle,
    SQLCHAR *ServerName, SQLSMALLINT NameLength1,
    SQLCHAR *UserName, SQLSMALLINT NameLength2,
    SQLCHAR *Authentication, SQLSMALLINT NameLength3)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_DBC, ConnectionHandle);
  PROFILE(r, r = doSQLConnect(ConnectionHandle, ServerName, NameLength1, UserName, NameLength2, Authentication, NameLength3));
  todbc_tls_buf_reclaim();
  return r;
}

SQLRETURN SQL_API SQLCopyDesc(SQLHDESC SourceDescHandle, SQLHDESC TargetDescHandle)
{
  SQLRETURN r;
  PROFILE(r, r = SQL_ERROR);
  todbc_tls_buf_reclaim();
  return r;
}

SQLRETURN SQL_API SQLDataSources(SQLHENV EnvironmentHandle,
           SQLUSMALLINT Direction, SQLCHAR *ServerName, SQLSMALLINT BufferLength1, SQLSMALLINT *NameLength1Ptr,
           SQLCHAR *Description, SQLSMALLINT BufferLength2,
           SQLSMALLINT *NameLength2Ptr)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_ENV, EnvironmentHandle);
  PROFILE(r, r = SQL_ERROR);
  todbc_tls_buf_reclaim();
  return r;
}

static SQLRETURN doSQLDescribeCol(SQLHSTMT StatementHandle,
           SQLUSMALLINT ColumnNumber, SQLCHAR *ColumnName, SQLSMALLINT BufferLength,
           SQLSMALLINT *NameLength,
           SQLSMALLINT *DataType, SQLULEN *ColumnSize,
           SQLSMALLINT *DecimalDigits, SQLSMALLINT *Nullable);

SQLRETURN SQL_API SQLDescribeCol(SQLHSTMT StatementHandle,
           SQLUSMALLINT ColumnNumber, SQLCHAR *ColumnName, SQLSMALLINT BufferLength,
           SQLSMALLINT *NameLength,
           SQLSMALLINT *DataType, SQLULEN *ColumnSize,
           SQLSMALLINT *DecimalDigits, SQLSMALLINT *Nullable)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_STMT, StatementHandle);
  PROFILE(r, r = doSQLDescribeCol(StatementHandle, ColumnNumber, ColumnName, BufferLength,
           NameLength, DataType, ColumnSize, DecimalDigits, Nullable));
  todbc_tls_buf_reclaim();
  return r;
}

static SQLRETURN doSQLDisconnect(SQLHDBC ConnectionHandle);

SQLRETURN SQL_API SQLDisconnect(SQLHDBC ConnectionHandle)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_DBC, ConnectionHandle);
  PROFILE(r, r = doSQLDisconnect(ConnectionHandle));
  todbc_tls_buf_reclaim();
  return r;
}

SQLRETURN SQL_API SQLEndTran(SQLSMALLINT HandleType, SQLHANDLE Handle, SQLSMALLINT CompletionType)
{
  SQLRETURN r;
  errs_clear(HandleType, Handle);
  PROFILE(r, r = SQL_ERROR);
  todbc_tls_buf_reclaim();
  return r;
}

static SQLRETURN doSQLError(SQLHENV EnvironmentHandle,
    SQLHDBC ConnectionHandle, SQLHSTMT StatementHandle,
    SQLCHAR *Sqlstate, SQLINTEGER *NativeError,
    SQLCHAR *MessageText, SQLSMALLINT BufferLength,
    SQLSMALLINT *TextLength);

SQLRETURN SQL_API not_support_SQLError(SQLHENV EnvironmentHandle,
    SQLHDBC ConnectionHandle, SQLHSTMT StatementHandle,
    SQLCHAR *Sqlstate, SQLINTEGER *NativeError,
    SQLCHAR *MessageText, SQLSMALLINT BufferLength,
    SQLSMALLINT *TextLength)
{
  SQLRETURN r;
  PROFILE(r, r = doSQLError(EnvironmentHandle, ConnectionHandle, StatementHandle,
        Sqlstate, NativeError, MessageText, BufferLength, TextLength));
  todbc_tls_buf_reclaim();
  return r;
}

static SQLRETURN doSQLExecDirect(SQLHSTMT StatementHandle, SQLCHAR* StatementText, SQLINTEGER TextLength);

SQLRETURN SQL_API SQLExecDirect(SQLHSTMT StatementHandle, SQLCHAR* StatementText, SQLINTEGER TextLength)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_STMT, StatementHandle);
  PROFILE(r, r = doSQLExecDirect(StatementHandle, StatementText, TextLength));
  todbc_tls_buf_reclaim();
  return r;
}

static SQLRETURN doSQLExecute(SQLHSTMT StatementHandle);

SQLRETURN SQL_API SQLExecute(SQLHSTMT StatementHandle)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_STMT, StatementHandle);
  PROFILE(r, r = doSQLExecute(StatementHandle));
  todbc_tls_buf_reclaim();
  return r;
}

static SQLRETURN doSQLFetch(SQLHSTMT StatementHandle);

SQLRETURN SQL_API SQLFetch(SQLHSTMT StatementHandle)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_STMT, StatementHandle);
  PROFILE(r, r = doSQLFetch(StatementHandle));
  todbc_tls_buf_reclaim();
  return r;
}

SQLRETURN SQL_API SQLFetchScroll(SQLHSTMT StatementHandle, SQLSMALLINT FetchOrientation, SQLLEN FetchOffset)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_STMT, StatementHandle);
  PROFILE(r, r = SQL_ERROR);
  todbc_tls_buf_reclaim();
  return r;
}

static SQLRETURN doSQLFreeConnect(SQLHDBC ConnectionHandle);

SQLRETURN SQL_API SQLFreeConnect(SQLHDBC ConnectionHandle)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_DBC, ConnectionHandle);
  PROFILE(r, r = doSQLFreeConnect(ConnectionHandle));
  todbc_tls_buf_reclaim();
  return r;
}

static SQLRETURN doSQLFreeEnv(SQLHENV EnvironmentHandle);

SQLRETURN SQL_API SQLFreeEnv(SQLHENV EnvironmentHandle)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_ENV, EnvironmentHandle);
  PROFILE(r, r = doSQLFreeEnv(EnvironmentHandle));
  todbc_tls_buf_reclaim();
  return r;
}

static SQLRETURN doSQLFreeHandle(SQLSMALLINT HandleType, SQLHANDLE Handle);

SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT HandleType, SQLHANDLE Handle)
{
  SQLRETURN r;
  errs_clear(HandleType, Handle);
  PROFILE(r, r = doSQLFreeHandle(HandleType, Handle));
  todbc_tls_buf_reclaim();
  return r;
}

static SQLRETURN doSQLFreeStmt(SQLHSTMT StatementHandle, SQLUSMALLINT Option);

SQLRETURN SQL_API SQLFreeStmt(SQLHSTMT StatementHandle, SQLUSMALLINT Option)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_STMT, StatementHandle);
  PROFILE(r, r = doSQLFreeStmt(StatementHandle, Option));
  todbc_tls_buf_reclaim();
  return r;
}

SQLRETURN SQL_API SQLGetConnectAttr(SQLHDBC ConnectionHandle,
           SQLINTEGER Attribute, SQLPOINTER Value,
           SQLINTEGER BufferLength, SQLINTEGER *StringLengthPtr)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_DBC, ConnectionHandle);
  PROFILE(r, r = SQL_ERROR);
  todbc_tls_buf_reclaim();
  return r;
}

SQLRETURN SQL_API SQLGetConnectOption(SQLHDBC ConnectionHandle, SQLUSMALLINT Option, SQLPOINTER Value)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_DBC, ConnectionHandle);
  PROFILE(r, r = SQL_ERROR);
  todbc_tls_buf_reclaim();
  return r;
}

SQLRETURN SQL_API SQLGetCursorName(SQLHSTMT StatementHandle, SQLCHAR *CursorName, SQLSMALLINT BufferLength, SQLSMALLINT *NameLengthPtr)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_STMT, StatementHandle);
  PROFILE(r, r = SQL_ERROR);
  todbc_tls_buf_reclaim();
  return r;
}

static SQLRETURN doSQLGetData(SQLHSTMT StatementHandle,
           SQLUSMALLINT ColumnNumber, SQLSMALLINT TargetType,
           SQLPOINTER TargetValue, SQLLEN BufferLength,
           SQLLEN *StrLen_or_IndPtr);

SQLRETURN SQL_API SQLGetData(SQLHSTMT StatementHandle,
           SQLUSMALLINT ColumnNumber, SQLSMALLINT TargetType,
           SQLPOINTER TargetValue, SQLLEN BufferLength,
           SQLLEN *StrLen_or_IndPtr)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_STMT, StatementHandle);
  PROFILE(r, r = doSQLGetData(StatementHandle, ColumnNumber, TargetType, TargetValue, BufferLength, StrLen_or_IndPtr));
  todbc_tls_buf_reclaim();
  return r;
}

SQLRETURN SQL_API SQLGetDescField(SQLHDESC DescriptorHandle,
           SQLSMALLINT RecNumber, SQLSMALLINT FieldIdentifier,
           SQLPOINTER Value, SQLINTEGER BufferLength,
           SQLINTEGER *StringLength)
{
  SQLRETURN r;
  PROFILE(r, r = SQL_ERROR);
  todbc_tls_buf_reclaim();
  return r;
}

SQLRETURN SQL_API SQLGetDescRec(SQLHDESC DescriptorHandle,
           SQLSMALLINT RecNumber, SQLCHAR *Name,
           SQLSMALLINT BufferLength, SQLSMALLINT *StringLengthPtr,
           SQLSMALLINT *TypePtr, SQLSMALLINT *SubTypePtr,
           SQLLEN     *LengthPtr, SQLSMALLINT *PrecisionPtr,
           SQLSMALLINT *ScalePtr, SQLSMALLINT *NullablePtr)
{
  SQLRETURN r;
  PROFILE(r, r = SQL_ERROR);
  todbc_tls_buf_reclaim();
  return r;
}

static SQLRETURN doSQLGetDiagField(SQLSMALLINT HandleType, SQLHANDLE Handle, SQLSMALLINT RecNumber, SQLSMALLINT DiagIdentifier,
           SQLPOINTER DiagInfo, SQLSMALLINT BufferLength, SQLSMALLINT *StringLength);

SQLRETURN SQL_API SQLGetDiagField(SQLSMALLINT HandleType, SQLHANDLE Handle, SQLSMALLINT RecNumber, SQLSMALLINT DiagIdentifier,
           SQLPOINTER DiagInfo, SQLSMALLINT BufferLength, SQLSMALLINT *StringLength)
{
  SQLRETURN r;
  PROFILE(r, r = doSQLGetDiagField(HandleType, Handle, RecNumber, DiagIdentifier,
        DiagInfo, BufferLength, StringLength));
  todbc_tls_buf_reclaim();
  return r;
}

static SQLRETURN doSQLGetDiagRec(SQLSMALLINT HandleType, SQLHANDLE Handle, SQLSMALLINT RecNumber,
    SQLCHAR *Sqlstate, SQLINTEGER *NativeError, SQLCHAR* MessageText, SQLSMALLINT BufferLength, SQLSMALLINT *TextLength);

SQLRETURN SQL_API SQLGetDiagRec(SQLSMALLINT HandleType, SQLHANDLE Handle, SQLSMALLINT RecNumber,
    SQLCHAR *Sqlstate, SQLINTEGER *NativeError, SQLCHAR* MessageText, SQLSMALLINT BufferLength, SQLSMALLINT *TextLength)
{
  SQLRETURN r;
  PROFILE(r, r = doSQLGetDiagRec(HandleType, Handle, RecNumber, Sqlstate, NativeError, MessageText, BufferLength, TextLength));
  todbc_tls_buf_reclaim();
  return r;
}

static SQLRETURN doSQLGetEnvAttr(SQLHENV EnvironmentHandle, SQLINTEGER Attribute, SQLPOINTER Value,
           SQLINTEGER BufferLength, SQLINTEGER *StringLength);

SQLRETURN SQL_API SQLGetEnvAttr(SQLHENV EnvironmentHandle, SQLINTEGER Attribute, SQLPOINTER Value,
           SQLINTEGER BufferLength, SQLINTEGER *StringLength)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_ENV, EnvironmentHandle);
  PROFILE(r, r = doSQLGetEnvAttr(EnvironmentHandle, Attribute, Value, BufferLength, StringLength));
  todbc_tls_buf_reclaim();
  return r;
}

// SQLRETURN SQL_API SQLGetFunctions(SQLHDBC ConnectionHandle, SQLUSMALLINT FunctionId, SQLUSMALLINT *Supported)
// {
//   SQLRETURN r;
//   errs_clear(SQL_HANDLE_DBC, ConnectionHandle);
//   PROFILE(r, r = SQL_ERROR);
//   todbc_tls_buf_reclaim();
//   return r;
// }

static SQLRETURN doSQLGetInfo(SQLHDBC ConnectionHandle, SQLUSMALLINT InfoType, SQLPOINTER InfoValue,
           SQLSMALLINT BufferLength, SQLSMALLINT *StringLengthPtr);

SQLRETURN SQL_API SQLGetInfo(SQLHDBC ConnectionHandle, SQLUSMALLINT InfoType, SQLPOINTER InfoValue,
           SQLSMALLINT BufferLength, SQLSMALLINT *StringLengthPtr)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_DBC, ConnectionHandle);
  PROFILE(r, r = doSQLGetInfo(ConnectionHandle, InfoType, InfoValue, BufferLength, StringLengthPtr));
  todbc_tls_buf_reclaim();
  return r;
}

static SQLRETURN doSQLGetStmtAttr(SQLHSTMT StatementHandle, SQLINTEGER Attribute, SQLPOINTER Value,
           SQLINTEGER BufferLength, SQLINTEGER *StringLength);

SQLRETURN SQL_API SQLGetStmtAttr(SQLHSTMT StatementHandle, SQLINTEGER Attribute, SQLPOINTER Value,
           SQLINTEGER BufferLength, SQLINTEGER *StringLength)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_STMT, StatementHandle);
  PROFILE(r, r = doSQLGetStmtAttr(StatementHandle, Attribute, Value, BufferLength, StringLength));
  todbc_tls_buf_reclaim();
  return r;
}

SQLRETURN SQL_API SQLGetStmtOption(SQLHSTMT StatementHandle,
           SQLUSMALLINT Option, SQLPOINTER Value)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_STMT, StatementHandle);
  PROFILE(r, r = SQL_ERROR);
  todbc_tls_buf_reclaim();
  return r;
}

static SQLRETURN doSQLGetTypeInfo(SQLHSTMT StatementHandle, SQLSMALLINT DataType);

SQLRETURN SQL_API SQLGetTypeInfo(SQLHSTMT StatementHandle, SQLSMALLINT DataType)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_STMT, StatementHandle);
  PROFILE(r, r = doSQLGetTypeInfo(StatementHandle, DataType));
  todbc_tls_buf_reclaim();
  return r;
}

static SQLRETURN doSQLNumResultCols(SQLHSTMT StatementHandle, SQLSMALLINT *ColumnCount);

SQLRETURN SQL_API SQLNumResultCols(SQLHSTMT StatementHandle, SQLSMALLINT *ColumnCount)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_STMT, StatementHandle);
  PROFILE(r, r = doSQLNumResultCols(StatementHandle, ColumnCount));
  todbc_tls_buf_reclaim();
  return r;
}

SQLRETURN SQL_API SQLParamData(SQLHSTMT StatementHandle, SQLPOINTER *Value)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_STMT, StatementHandle);
  PROFILE(r, r = SQL_ERROR);
  todbc_tls_buf_reclaim();
  return r;
}

static SQLRETURN doSQLPrepare(SQLHSTMT StatementHandle, SQLCHAR* StatementText, SQLINTEGER TextLength);

SQLRETURN SQL_API SQLPrepare(SQLHSTMT StatementHandle, SQLCHAR* StatementText, SQLINTEGER TextLength)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_STMT, StatementHandle);
  PROFILE(r, r = doSQLPrepare(StatementHandle, StatementText, TextLength));
  todbc_tls_buf_reclaim();
  return r;
}

SQLRETURN SQL_API SQLPutData(SQLHSTMT StatementHandle, SQLPOINTER Data, SQLLEN StrLen_or_Ind)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_STMT, StatementHandle);
  PROFILE(r, r = SQL_ERROR);
  todbc_tls_buf_reclaim();
  return r;
}

static SQLRETURN doSQLRowCount(SQLHSTMT StatementHandle, SQLLEN* RowCount);

SQLRETURN SQL_API SQLRowCount(SQLHSTMT StatementHandle, SQLLEN* RowCount)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_STMT, StatementHandle);
  PROFILE(r, r = doSQLRowCount(StatementHandle, RowCount));
  todbc_tls_buf_reclaim();
  return r;
}

SQLRETURN SQL_API SQLSetConnectAttr(SQLHDBC ConnectionHandle, SQLINTEGER Attribute, SQLPOINTER Value, SQLINTEGER StringLength)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_DBC, ConnectionHandle);
  PROFILE(r, r = SQL_ERROR);
  todbc_tls_buf_reclaim();
  return r;
}

SQLRETURN SQL_API SQLSetConnectOption(SQLHDBC ConnectionHandle, SQLUSMALLINT Option, SQLULEN Value)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_DBC, ConnectionHandle);
  PROFILE(r, r = SQL_ERROR);
  todbc_tls_buf_reclaim();
  return r;
}

SQLRETURN SQL_API SQLSetCursorName(SQLHSTMT StatementHandle, SQLCHAR* CursorName, SQLSMALLINT NameLength)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_STMT, StatementHandle);
  PROFILE(r, r = SQL_ERROR);
  todbc_tls_buf_reclaim();
  return r;
}

SQLRETURN SQL_API SQLSetDescField(SQLHDESC DescriptorHandle, SQLSMALLINT RecNumber, SQLSMALLINT FieldIdentifier,
           SQLPOINTER Value, SQLINTEGER BufferLength)
{
  SQLRETURN r;
  PROFILE(r, r = SQL_ERROR);
  todbc_tls_buf_reclaim();
  return r;
}

SQLRETURN SQL_API SQLSetDescRec(SQLHDESC DescriptorHandle, SQLSMALLINT RecNumber, SQLSMALLINT Type,
           SQLSMALLINT SubType, SQLLEN Length, SQLSMALLINT Precision, SQLSMALLINT Scale,
           SQLPOINTER Data, SQLLEN *StringLength, SQLLEN *Indicator)
{
  SQLRETURN r;
  PROFILE(r, r = SQL_ERROR);
  todbc_tls_buf_reclaim();
  return r;
}

static SQLRETURN doSQLSetEnvAttr(SQLHENV EnvironmentHandle, SQLINTEGER Attribute, SQLPOINTER Value, SQLINTEGER StringLength);

SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV EnvironmentHandle, SQLINTEGER Attribute, SQLPOINTER Value, SQLINTEGER StringLength)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_ENV, EnvironmentHandle);
  PROFILE(r, r = doSQLSetEnvAttr(EnvironmentHandle, Attribute, Value, StringLength));
  todbc_tls_buf_reclaim();
  return r;
}

SQLRETURN SQL_API SQLSetParam(SQLHSTMT StatementHandle, SQLUSMALLINT ParameterNumber, SQLSMALLINT ValueType,
           SQLSMALLINT ParameterType, SQLULEN LengthPrecision, SQLSMALLINT ParameterScale, SQLPOINTER ParameterValue,
           SQLLEN *StrLen_or_IndPtr)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_STMT, StatementHandle);
  PROFILE(r, r = SQL_ERROR);
  todbc_tls_buf_reclaim();
  return r;
}

static SQLRETURN doSQLSetStmtAttr(SQLHSTMT StatementHandle, SQLINTEGER Attribute, SQLPOINTER Value, SQLINTEGER StringLength);

SQLRETURN SQL_API SQLSetStmtAttr(SQLHSTMT StatementHandle, SQLINTEGER Attribute, SQLPOINTER Value, SQLINTEGER StringLength)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_STMT, StatementHandle);
  PROFILE(r, r = doSQLSetStmtAttr(StatementHandle, Attribute, Value, StringLength));
  todbc_tls_buf_reclaim();
  return r;
}

SQLRETURN SQL_API SQLSetStmtOption(SQLHSTMT StatementHandle, SQLUSMALLINT Option, SQLULEN Value)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_STMT, StatementHandle);
  PROFILE(r, r = SQL_ERROR);
  todbc_tls_buf_reclaim();
  return r;
}

SQLRETURN SQL_API SQLSpecialColumns(SQLHSTMT StatementHandle, SQLUSMALLINT IdentifierType,
           SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
           SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
           SQLCHAR *TableName, SQLSMALLINT NameLength3,
           SQLUSMALLINT Scope, SQLUSMALLINT Nullable)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_STMT, StatementHandle);
  PROFILE(r, r = SQL_ERROR);
  todbc_tls_buf_reclaim();
  return r;
}

SQLRETURN SQL_API SQLStatistics(SQLHSTMT StatementHandle,
           SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
           SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
           SQLCHAR *TableName, SQLSMALLINT NameLength3,
           SQLUSMALLINT Unique, SQLUSMALLINT Reserved)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_STMT, StatementHandle);
  PROFILE(r, r = SQL_ERROR);
  todbc_tls_buf_reclaim();
  return r;
}

SQLRETURN SQL_API SQLTables(SQLHSTMT StatementHandle,
           SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
           SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
           SQLCHAR *TableName, SQLSMALLINT NameLength3,
           SQLCHAR *TableType, SQLSMALLINT NameLength4)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_STMT, StatementHandle);
  PROFILE(r, r = SQL_ERROR);
  todbc_tls_buf_reclaim();
  return r;
}

SQLRETURN SQL_API SQLTransact(SQLHENV EnvironmentHandle, SQLHDBC ConnectionHandle, SQLUSMALLINT CompletionType)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_ENV, EnvironmentHandle);
  errs_clear(SQL_HANDLE_DBC, ConnectionHandle);
  PROFILE(r, r = SQL_ERROR);
  todbc_tls_buf_reclaim();
  return r;
}




static SQLRETURN doSQLDriverConnect(
    SQLHDBC         ConnectionHandle,
    SQLHWND         WindowHandle,
    SQLCHAR        *InConnectionString,
    SQLSMALLINT     StringLength1,
    SQLCHAR        *OutConnectionString,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLength2Ptr,
    SQLUSMALLINT    DriverCompletion);

SQLRETURN SQL_API SQLDriverConnect(
    SQLHDBC         ConnectionHandle,
    SQLHWND         WindowHandle,
    SQLCHAR        *InConnectionString,
    SQLSMALLINT     StringLength1,
    SQLCHAR        *OutConnectionString,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLength2Ptr,
    SQLUSMALLINT    DriverCompletion)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_DBC, ConnectionHandle);
  PROFILE(r, r = doSQLDriverConnect(ConnectionHandle, WindowHandle, InConnectionString, StringLength1,
        OutConnectionString, BufferLength, StringLength2Ptr, DriverCompletion));
  todbc_tls_buf_reclaim();
  return r;
}

static SQLRETURN doSQLBindParameter(
    SQLHSTMT        StatementHandle,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT     InputOutputType,
    SQLSMALLINT     ValueType,
    SQLSMALLINT     ParameterType,
    SQLULEN         ColumnSize,
    SQLSMALLINT     DecimalDigits,
    SQLPOINTER      ParameterValuePtr,
    SQLLEN          BufferLength,
    SQLLEN *        StrLen_or_IndPtr);

SQLRETURN SQL_API SQLBindParameter(
    SQLHSTMT        StatementHandle,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT     InputOutputType,
    SQLSMALLINT     ValueType,
    SQLSMALLINT     ParameterType,
    SQLULEN         ColumnSize,
    SQLSMALLINT     DecimalDigits,
    SQLPOINTER      ParameterValuePtr,
    SQLLEN          BufferLength,
    SQLLEN *        StrLen_or_IndPtr)
{
  SQLRETURN r;
  P("ParameterNumber:[%d]; InputOutputType:[%d]%s; ValueType:[%d]%s; ParameterType:[%d]%s; "
    "ColumnSize:[%ld]; DecimalDigits:[%d]; ParameterValuePtr:[%p]; BufferLength:[%ld]; StrLen_or_IndPtr:[%p]",
    ParameterNumber, InputOutputType, sql_input_output_type(InputOutputType),
    ValueType, sql_c_type(ValueType),
    ParameterType, sql_sql_type(ParameterType),
    ColumnSize, DecimalDigits, ParameterValuePtr, BufferLength, StrLen_or_IndPtr);
  errs_clear(SQL_HANDLE_STMT, StatementHandle);
  PROFILE(r, r = doSQLBindParameter(StatementHandle, ParameterNumber, InputOutputType, ValueType, ParameterType,
      ColumnSize, DecimalDigits, ParameterValuePtr, BufferLength, StrLen_or_IndPtr));
  todbc_tls_buf_reclaim();
  return r;
}

static SQLRETURN doSQLNumParams(SQLHSTMT StatementHandle, SQLSMALLINT *ParameterCountPtr);

SQLRETURN SQL_API SQLNumParams(SQLHSTMT StatementHandle, SQLSMALLINT *ParameterCountPtr)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_STMT, StatementHandle);
  PROFILE(r, r = doSQLNumParams(StatementHandle, ParameterCountPtr));
  todbc_tls_buf_reclaim();
  return r;
}

static SQLRETURN doSQLDescribeParam(
    SQLHSTMT        StatementHandle,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT *   DataTypePtr,
    SQLULEN *       ParameterSizePtr,
    SQLSMALLINT *   DecimalDigitsPtr,
    SQLSMALLINT *   NullablePtr);

SQLRETURN SQL_API SQLDescribeParam(
    SQLHSTMT        StatementHandle,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT *   DataTypePtr,
    SQLULEN *       ParameterSizePtr,
    SQLSMALLINT *   DecimalDigitsPtr,
    SQLSMALLINT *   NullablePtr)
{
  SQLRETURN r;
  errs_clear(SQL_HANDLE_STMT, StatementHandle);
  PROFILE(r, r = doSQLDescribeParam(StatementHandle, ParameterNumber, DataTypePtr,
    ParameterSizePtr, DecimalDigitsPtr, NullablePtr));
  todbc_tls_buf_reclaim();
  return r;
}





SQLRETURN SQL_API SQLBrowseConnect(
    SQLHDBC            hdbc,
    SQLCHAR           *szConnStrIn,
    SQLSMALLINT        cchConnStrIn,
    SQLCHAR           *szConnStrOut,
    SQLSMALLINT        cchConnStrOutMax,
    SQLSMALLINT       *pcchConnStrOut);

SQLRETURN   SQL_API SQLBulkOperations(
    SQLHSTMT            StatementHandle,
    SQLSMALLINT         Operation);

SQLRETURN SQL_API SQLColAttributes(
    SQLHSTMT           hstmt,
    SQLUSMALLINT       icol,
    SQLUSMALLINT       fDescType,
    SQLPOINTER         rgbDesc,
    SQLSMALLINT        cbDescMax,
    SQLSMALLINT       *pcbDesc,
    SQLLEN            * pfDesc);

SQLRETURN SQL_API SQLColumnPrivileges(
    SQLHSTMT           hstmt,
    SQLCHAR           *szCatalogName,
    SQLSMALLINT        cchCatalogName,
    SQLCHAR           *szSchemaName,
    SQLSMALLINT        cchSchemaName,
    SQLCHAR           *szTableName,
    SQLSMALLINT        cchTableName,
    SQLCHAR           *szColumnName,
    SQLSMALLINT        cchColumnName);

SQLRETURN SQL_API SQLExtendedFetch(
    SQLHSTMT           hstmt,
    SQLUSMALLINT       fFetchType,
    SQLLEN             irow,
    SQLULEN           *pcrow,
    SQLUSMALLINT      *rgfRowStatus);

SQLRETURN SQL_API SQLForeignKeys(
    SQLHSTMT           hstmt,
    SQLCHAR           *szPkCatalogName,
    SQLSMALLINT        cchPkCatalogName,
    SQLCHAR           *szPkSchemaName,
    SQLSMALLINT        cchPkSchemaName,
    SQLCHAR           *szPkTableName,
    SQLSMALLINT        cchPkTableName,
    SQLCHAR           *szFkCatalogName,
    SQLSMALLINT        cchFkCatalogName,
    SQLCHAR           *szFkSchemaName,
    SQLSMALLINT        cchFkSchemaName,
    SQLCHAR           *szFkTableName,
    SQLSMALLINT        cchFkTableName);

SQLRETURN SQL_API SQLMoreResults(
    SQLHSTMT           hstmt);

SQLRETURN SQL_API SQLNativeSql
(
    SQLHDBC            hdbc,
    SQLCHAR* szSqlStrIn,
    SQLINTEGER  cchSqlStrIn,
    SQLCHAR* szSqlStr,
    SQLINTEGER         cchSqlStrMax,
    SQLINTEGER        *pcbSqlStr
);

SQLRETURN SQL_API SQLParamOptions(
    SQLHSTMT           hstmt,
    SQLULEN            crow,
    SQLULEN            *pirow);

SQLRETURN SQL_API SQLPrimaryKeys(
    SQLHSTMT           hstmt,
    SQLCHAR           *szCatalogName,
    SQLSMALLINT        cchCatalogName,
    SQLCHAR           *szSchemaName,
    SQLSMALLINT        cchSchemaName,
    SQLCHAR           *szTableName,
    SQLSMALLINT        cchTableName);

SQLRETURN SQL_API SQLProcedureColumns(
    SQLHSTMT           hstmt,
    SQLCHAR           *szCatalogName,
    SQLSMALLINT        cchCatalogName,
    SQLCHAR           *szSchemaName,
    SQLSMALLINT        cchSchemaName,
    SQLCHAR           *szProcName,
    SQLSMALLINT        cchProcName,
    SQLCHAR           *szColumnName,
    SQLSMALLINT        cchColumnName);

SQLRETURN SQL_API SQLProcedures(
    SQLHSTMT           hstmt,
    SQLCHAR           *szCatalogName,
    SQLSMALLINT        cchCatalogName,
    SQLCHAR           *szSchemaName,
    SQLSMALLINT        cchSchemaName,
    SQLCHAR           *szProcName,
    SQLSMALLINT        cchProcName);



SQLRETURN SQL_API SQLSetPos(
    SQLHSTMT           hstmt,
    SQLSETPOSIROW      irow,
    SQLUSMALLINT       fOption,
    SQLUSMALLINT       fLock);

SQLRETURN SQL_API SQLTablePrivileges(
    SQLHSTMT           hstmt,
    SQLCHAR           *szCatalogName,
    SQLSMALLINT        cchCatalogName,
    SQLCHAR           *szSchemaName,
    SQLSMALLINT        cchSchemaName,
    SQLCHAR           *szTableName,
    SQLSMALLINT        cchTableName);

SQLRETURN SQL_API SQLDrivers(
    SQLHENV            henv,
    SQLUSMALLINT       fDirection,
    SQLCHAR           *szDriverDesc,
    SQLSMALLINT        cchDriverDescMax,
    SQLSMALLINT       *pcchDriverDesc,
    SQLCHAR           *szDriverAttributes,
    SQLSMALLINT        cchDrvrAttrMax,
    SQLSMALLINT       *pcchDrvrAttr);

SQLRETURN SQL_API SQLAllocHandleStd(
    SQLSMALLINT     fHandleType,
    SQLHANDLE       hInput,
    SQLHANDLE      *phOutput);
SQLRETURN SQL_API SQLSetScrollOptions(
    SQLHSTMT           hstmt,
    SQLUSMALLINT       fConcurrency,
    SQLLEN             crowKeyset,
    SQLUSMALLINT       crowRowset);




static SQLRETURN doSQLAllocEnv(SQLHENV *EnvironmentHandle)
{
  env_t *env = (env_t*)calloc(1, sizeof(*env));
  OILE(env, "");

  int r = env_init(env);
  if (r) return SQL_ERROR;

  *EnvironmentHandle = env;

  return SQL_SUCCESS;
}

static SQLRETURN doSQLAllocHandle(SQLSMALLINT HandleType, SQLHANDLE InputHandle, SQLHANDLE *OutputHandle)
{
  P("HandleType:[%d]%s", HandleType, sql_handle_type(HandleType));
  switch (HandleType)
  {
    case SQL_HANDLE_ENV:
      {
        return doSQLAllocEnv(OutputHandle);
      } break;
    case SQL_HANDLE_DBC:
      {
        errs_clear(SQL_HANDLE_ENV, InputHandle);
        return doSQLAllocConnect(InputHandle, OutputHandle);
      } break;
    case SQL_HANDLE_STMT:
      {
        errs_clear(SQL_HANDLE_DBC, InputHandle);
        return doSQLAllocStmt(InputHandle, OutputHandle);
      } break;
    default:
      {
        ONIY(0, "");
      } break;
  }
}

static SQLRETURN doSQLSetEnvAttr(SQLHENV EnvironmentHandle, SQLINTEGER Attribute, SQLPOINTER Value, SQLINTEGER StringLength)
{
  P("Attribute:[%d]%s", Attribute, sql_env_attr_type(Attribute));
  env_t *env = (env_t*)EnvironmentHandle;
  OILE(env, "");

  switch (Attribute)
  {
    case SQL_ATTR_ODBC_VERSION:
      {
        int32_t ver = (int32_t)(size_t)Value;
        P("client odbc ver:[%d]", ver);
        if (ver < env->odbc_ver) {
          // fall back to lower version
          env->odbc_ver = ver;
        }
        return SQL_SUCCESS;
      } break;
    default:
      {
        ONIY(0, "");
      } break;
  }
}

static SQLRETURN doSQLGetEnvAttr(SQLHENV EnvironmentHandle, SQLINTEGER Attribute, SQLPOINTER Value,
           SQLINTEGER BufferLength, SQLINTEGER *StringLength)
{
  P("Attribute:[%d]%s; Value:[%p]; BufferLength:[%d]; StringLength:[%p][%d]",
     Attribute, sql_env_attr_type(Attribute), Value, BufferLength,
     StringLength, StringLength ? *StringLength : 0);
  env_t *env = (env_t*)EnvironmentHandle;
  OILE(env, "");

  switch (Attribute)
  {
    case SQL_ATTR_ODBC_VERSION:
      {
        *(int32_t*)Value = env->odbc_ver;
        P("odbc ver:[%d]", env->odbc_ver);
        return SQL_SUCCESS;
      } break;
    default:
      {
        ONIY(0, "");
      } break;
  }
}

static SQLRETURN doSQLAllocConnect(SQLHENV EnvironmentHandle, SQLHDBC *ConnectionHandle)
{
  env_t  *env  = (env_t*)EnvironmentHandle;
  OILE(env, "");
  errs_t *errs = env_get_errs(env);

  conn_t *conn = conn_new(env);
  if (!conn) {
    SET_OOM(errs, "alloc conn failed");
    return SQL_ERROR;
  }

  do {
    *ConnectionHandle = conn;

    return SQL_SUCCESS;
  } while (0);

  conn_free(conn);

  return SQL_ERROR;
}

static SQLRETURN doSQLFreeHandle(SQLSMALLINT HandleType, SQLHANDLE Handle)
{
  switch (HandleType)
  {
    case SQL_HANDLE_ENV:
      {
        return doSQLFreeEnv(Handle);
      } break;
    case SQL_HANDLE_DBC:
      {
        return doSQLFreeConnect(Handle);
      } break;
    case SQL_HANDLE_STMT:
      {
        stmt_t *stmt = (stmt_t*)Handle;
        stmt_free(stmt);
        return SQL_SUCCESS;
      } break;
    default:
      {
        ONIY(0, "HandleType:[%d]%s", HandleType, sql_handle_type(HandleType));
      } break;
  }
}

static SQLRETURN do_connect(conn_t *conn) {
  errs_t *errs = &conn->errs;

  OD("enc:src/char/wchar/db/locale:[%s/%s/%s/%s/%s]",
      conn->enc_src, conn->enc_char, conn->enc_wchar, conn->enc_db, conn->enc_locale);

  SQLRETURN r;

  r = conn_check_charset(conn, errs);
  if (r!=SQL_SUCCESS) return r;

  if (0) {
    // test with null_conn
    if (conn_init_null_conn(conn)) {
      SET_GENERAL(errs, "failed to init null conn for test");
      return SQL_ERROR;
    }
  } else {
    if (conn_init_tsdb_conn(conn)) {
      SET_GENERAL(errs, "failed to init taos conn for test");
      return SQL_ERROR;
    }
  }

  return conn_connect(conn);
}

static SQLRETURN doSQLConnect(SQLHDBC ConnectionHandle,
    SQLCHAR *ServerName, SQLSMALLINT NameLength1,
    SQLCHAR *UserName, SQLSMALLINT NameLength2,
    SQLCHAR *Authentication, SQLSMALLINT NameLength3)
{
  conn_t *conn = (conn_t*)ConnectionHandle;
  OILE(conn, "");
  ONIY(ServerName, "");

  errs_t *errs = &conn->errs;

  const char *enc_to   = conn->enc_locale;
  const char *enc_from = conn->enc_char;

  SQLRETURN ok = SQL_ERROR;

  conn_val_t *val = &conn->val;
  conn_val_reset(val);

  do {
    if (ServerName) {
      size_t slen = (size_t)NameLength1;
      todbc_string_t dsn = todbc_tls_conv(NULL, enc_to, enc_from, (const unsigned char*)ServerName, &slen);
      if (!dsn.buf) {
        SET_OOM(errs, "alloc buf failed");
        return SQL_ERROR;
      }
      val->dsn = strdup((const char*)dsn.buf);
      if (!val->dsn) {
        SET_OOM(errs, "strdup failed");
        return SQL_ERROR;
      }
    }
    if (UserName) {
      size_t slen = (size_t)NameLength2;
      todbc_string_t uid = todbc_tls_conv(NULL, enc_to, enc_from, (const unsigned char*)UserName, &slen);
      if (!uid.buf) {
        SET_OOM(errs, "alloc buf failed");
        return SQL_ERROR;
      }
      val->uid = strdup((const char*)uid.buf);
      if (!val->uid) {
        SET_OOM(errs, "strdup failed");
        return SQL_ERROR;
      }
    }
    if (Authentication) {
      size_t slen = (size_t)NameLength3;
      todbc_string_t pwd = todbc_tls_conv(NULL, enc_to, enc_from, (const unsigned char*)Authentication, &slen);
      if (!pwd.buf) {
        SET_OOM(errs, "alloc buf failed");
        return SQL_ERROR;
      }
      val->pwd = strdup((const char*)pwd.buf);
      if (!val->pwd) {
        SET_OOM(errs, "strdup failed");
        return SQL_ERROR;
      }
    }

  OD(".................");
    ok = do_connect(conn);
  OD(".................");
    if (ok!=SQL_SUCCESS) break;

    CONN_SET_CONNECTED(conn);
    return SQL_SUCCESS;
  } while (0);

  conn_val_reset(val);

  return ok;
}

static SQLRETURN doSQLDriverConnect(
    SQLHDBC         ConnectionHandle,
    SQLHWND         WindowHandle,
    SQLCHAR        *InConnectionString,
    SQLSMALLINT     StringLength1,
    SQLCHAR        *OutConnectionString,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT    *StringLength2Ptr,
    SQLUSMALLINT    DriverCompletion)
{
  conn_t *conn = (conn_t*)ConnectionHandle;
  OILE(conn, "");
  ONIY(InConnectionString, "");

  errs_t *errs = &conn->errs;

#ifndef _MSC_VER
  if (DriverCompletion!=SQL_DRIVER_NOPROMPT) {
    SET_NIY(errs, "option[%d] other than SQL_DRIVER_NOPROMPT not supported yet", DriverCompletion);
    return SQL_ERROR;
  }
#endif

  const char *enc_to   = conn->enc_locale;
  const char *enc_from = conn->enc_char;

  size_t slen = (size_t)StringLength1;
  todbc_string_t ts = todbc_tls_conv(NULL, enc_to, enc_from, (const unsigned char*)InConnectionString, &slen);
  const char *connStr = (const char*)ts.buf;
  if (!connStr) {
    SET_OOM(errs, "alloc buf failed");
    return SQL_ERROR;
  }

  SQLRETURN ok = SQL_ERROR;

  conn_val_t *val = &conn->val;
  conn_val_reset(val);

  do {
    // TO_DO: genralize
    int n = todbc_parse_conn_string(connStr, val);
    if (n) {
      SET_GENERAL(errs, "unrecognized connection string:[%s]", connStr);
      break;
    }

    todbc_enc_t enc;
    if (val->enc_char) {
      enc = todbc_tls_iconv_enc(val->enc_char);
      if (enc.enc[0]=='\0') {
        SET_GENERAL(errs, "unrecognized charset:[%s]", val->enc_char);
        break;
      }
      snprintf(conn->enc_char, sizeof(conn->enc_char), "%s", val->enc_char);
    }
    if (val->enc_wchar) {
      enc = todbc_tls_iconv_enc(val->enc_wchar);
      if (enc.enc[0]=='\0') {
        SET_GENERAL(errs, "unrecognized charset:[%s]", val->enc_wchar);
        break;
      }
      snprintf(conn->enc_wchar, sizeof(conn->enc_wchar), "%s", val->enc_wchar);
    }
    if (val->enc_db) {
      enc = todbc_tls_iconv_enc(val->enc_db);
      if (enc.enc[0]=='\0') {
        SET_GENERAL(errs, "unrecognized charset:[%s]", val->enc_db);
        break;
      }
      snprintf(conn->enc_db, sizeof(conn->enc_db), "%s", val->enc_db);
    }
    if (val->enc_local) {
      enc = todbc_tls_iconv_enc(val->enc_local);
      if (enc.enc[0]=='\0') {
        SET_GENERAL(errs, "unrecognized charset:[%s]", val->enc_local);
        break;
      }
      snprintf(conn->enc_locale, sizeof(conn->enc_locale), "%s", val->enc_local);
    }

    ok = do_connect(conn);
    if (ok!=SQL_SUCCESS) break;
    ok = SQL_ERROR;

    n = 0;
    if (OutConnectionString) {
      n = snprintf((char*)OutConnectionString, (size_t)BufferLength, "%s", connStr);
    }
    if (StringLength2Ptr) {
      *StringLength2Ptr = (SQLSMALLINT)n;
    }

    CONN_SET_CONNECTED(conn);
    return SQL_SUCCESS;
  } while (0);

  conn_val_reset(val);

  return ok;
}

static SQLRETURN doSQLGetInfo(SQLHDBC ConnectionHandle, SQLUSMALLINT InfoType, SQLPOINTER InfoValue,
           SQLSMALLINT BufferLength, SQLSMALLINT *StringLengthPtr)
{
  P("InfoType:[%d]%s; BufferLength:[%d]; StringLengthPtr:[%p]%d",
    InfoType, sql_info_type(InfoType), BufferLength,
    StringLengthPtr, StringLengthPtr ? *StringLengthPtr : 0);

  switch (InfoType)
  {
    case SQL_DRIVER_ODBC_VER:
      {
        // how to sync with env->odbc_ver ?
        const char *v = "03.00";
        int n = snprintf((char*)InfoValue, (size_t)BufferLength, "%s", v);
        OILE(n>0, "");
        *StringLengthPtr = (SQLSMALLINT)n;
      } break;
    case SQL_DESCRIBE_PARAMETER:
      {
        const char *v = "Y";
        int n = snprintf((char*)InfoValue, (size_t)BufferLength, "%s", v);
        OILE(n>0, "");
        *StringLengthPtr = (SQLSMALLINT)n;
      } break;
    case SQL_NEED_LONG_DATA_LEN:
      {
        const char *v = "Y";
        int n = snprintf((char*)InfoValue, (size_t)BufferLength, "%s", v);
        OILE(n>0, "");
        *StringLengthPtr = (SQLSMALLINT)n;
      } break;
    case SQL_MAX_COLUMN_NAME_LEN:
      {
        SQLUSMALLINT v = 64;
        OILE(BufferLength==sizeof(v), "");
        *(SQLUSMALLINT*)InfoValue = v;
        if (StringLengthPtr) *StringLengthPtr = sizeof(v);
      } break;
    case SQL_TXN_ISOLATION_OPTION:
      {
        SQLUINTEGER v = SQL_TXN_READ_UNCOMMITTED;
        OILE(BufferLength==sizeof(v), "");
        *(SQLUINTEGER*)InfoValue = v;
        if (StringLengthPtr) *StringLengthPtr = sizeof(v);
      } break;
    case SQL_CURSOR_COMMIT_BEHAVIOR:
      {
        SQLUSMALLINT v = SQL_CB_PRESERVE;
        OILE(BufferLength==sizeof(v), "");
        *(SQLUSMALLINT*)InfoValue = v;
        if (StringLengthPtr) *StringLengthPtr = sizeof(v);
      } break;
    case SQL_CURSOR_ROLLBACK_BEHAVIOR:
      {
        SQLUSMALLINT v = SQL_CB_PRESERVE;
        OILE(BufferLength==sizeof(v), "");
        *(SQLUSMALLINT*)InfoValue = v;
        if (StringLengthPtr) *StringLengthPtr = sizeof(v);
      } break;
    case SQL_GETDATA_EXTENSIONS:
      {
        SQLUINTEGER v = SQL_GD_ANY_COLUMN;
        OILE(BufferLength==sizeof(v), "");
        *(SQLUINTEGER*)InfoValue = v;
        if (StringLengthPtr) *StringLengthPtr = sizeof(v);
      } break;
    case SQL_DTC_TRANSITION_COST:
      {
        SQLUINTEGER v = SQL_DTC_ENLIST_EXPENSIVE | SQL_DTC_UNENLIST_EXPENSIVE;
        OILE(BufferLength==sizeof(v), "");
        *(SQLUINTEGER*)InfoValue = v;
        if (StringLengthPtr) *StringLengthPtr = sizeof(v);
      } break;
    case SQL_MAX_CONCURRENT_ACTIVITIES:
      {
        SQLUSMALLINT v = 10240;
        OILE(BufferLength==sizeof(v), "");
        *(SQLUSMALLINT*)InfoValue = v;
        if (StringLengthPtr) *StringLengthPtr = sizeof(v);
      } break;
    default:
      {
        ONIY(0, "");
        // return SQL_ERROR;
      } break;
  }

  return SQL_SUCCESS;
}

static SQLRETURN do_fill_error(errs_t *errs, const char *enc_to, const char *enc_from,
    SQLSMALLINT RecNumber, SQLCHAR *Sqlstate, SQLINTEGER *NativeError,
    SQLCHAR* MessageText, SQLSMALLINT BufferLength, SQLSMALLINT *TextLength)
{
  if (errs_count(errs)<=0) return SQL_NO_DATA;

  const char *sql_state = NULL;
  const char *err_str   = NULL;
  int r = errs_fetch(errs, RecNumber-1, &sql_state, &err_str);
  if (r) return SQL_NO_DATA;
  OILE(sql_state && err_str, "");
  const unsigned char *src  = (const unsigned char*)err_str;
  size_t               slen = strlen(err_str);
  unsigned char       *dst  = (unsigned char*)MessageText;
  size_t               dlen = (size_t)BufferLength;
  // OILE(dst, "");
  if (!MessageText) {
    OILE(TextLength, "");
    *TextLength = 4096;
    return SQL_SUCCESS;
  }
  todbc_string_t s = todbc_tls_write(enc_to, enc_from, src, &slen, dst, dlen);
  *NativeError = 0;
  *TextLength  = (SQLSMALLINT)s.bytes;
  snprintf((char*)Sqlstate, 6, "%s", sql_state);
  return SQL_SUCCESS;
}

static SQLRETURN doSQLGetDiagRec(SQLSMALLINT HandleType, SQLHANDLE Handle, SQLSMALLINT RecNumber,
    SQLCHAR *Sqlstate, SQLINTEGER *NativeError, SQLCHAR* MessageText, SQLSMALLINT BufferLength, SQLSMALLINT *TextLength)
{
  OILE(RecNumber>0, "");
  OILE(Sqlstate, "");
  // OILE(NativeError, "");
  OILE(TextLength, "");
  switch (HandleType)
  {
    // case SQL_HANDLE_ENV:
    //   {
    //     env_t *env = (env_t*)Handle;
    //     FILL_ERROR(env);
    //     return SQL_SUCCESS;
    //   } break;
    case SQL_HANDLE_DBC:
      {
        conn_t *conn = (conn_t*)Handle;
        OILE(conn, "");
        errs_t *errs = conn_get_errs(conn);
        OILE(errs, "");
        const char *enc_to   = conn->enc_char;
        const char *enc_from = conn->enc_src;
        return do_fill_error(errs, enc_to, enc_from, RecNumber, Sqlstate, NativeError, MessageText, BufferLength, TextLength);
      } break;
    case SQL_HANDLE_STMT:
      {
        stmt_t *stmt = (stmt_t*)Handle;
        OILE(stmt, "");
        if (!stmt->owner) return SQL_NO_DATA;
        conn_t *conn = stmt->owner->conn;
        OILE(conn, "");
        errs_t *errs = stmt_get_errs(stmt);
        OILE(errs, "");
        const char *enc_to   = conn->enc_char;
        const char *enc_from = conn->enc_src;
        return do_fill_error(errs, enc_to, enc_from, RecNumber, Sqlstate, NativeError, MessageText, BufferLength, TextLength);
      } break;
    default:
      {
        ONIY(0, "HandleType:[%d]%s", HandleType, sql_handle_type(HandleType));
        // return SQL_ERROR;
      } break;
  }
}

static SQLRETURN doSQLAllocStmt(SQLHDBC ConnectionHandle, SQLHSTMT *StatementHandle)
{
  conn_t *conn = (conn_t*)ConnectionHandle;
  OILE(conn, "");

  errs_t *errs = &conn->errs;

  stmt_t *stmt = stmt_new(conn);
  if (!stmt) {
    SET_OOM(errs, "alloc stmt failed");
    return SQL_ERROR;
  }

  do {
    if (!StatementHandle) break;

    *StatementHandle = stmt;
    return SQL_SUCCESS;
  } while (0);

  stmt_free(stmt);
  OILE(0, "");
}

static SQLRETURN doSQLFreeStmt(SQLHSTMT StatementHandle, SQLUSMALLINT Option)
{
  P("Option:[%d]%s", Option, sql_freestmt_option_type(Option));

  switch (Option)
  {
    case SQL_CLOSE:
      {
        stmt_t *stmt = (stmt_t*)StatementHandle;
        OILE(stmt, "");
        if (STMT_IS_NORM(stmt)) break;
        return doSQLCloseCursor(StatementHandle);
      } break;
    // case SQL_UNBIND:
    //   {
    //   } break;
    case SQL_RESET_PARAMS:
      {
        stmt_t *stmt = (stmt_t*)StatementHandle;
        OILE(stmt, "");
        stmt_reset_params(stmt);
      } break;
    default:
      {
        ONIY(0, "");
      } break;
  }

  return SQL_SUCCESS;
}

static SQLRETURN doSQLGetStmtAttr(SQLHSTMT StatementHandle, SQLINTEGER Attribute, SQLPOINTER Value,
           SQLINTEGER BufferLength, SQLINTEGER *StringLength)
{
  P("Attribute:[%d]%s; BufferLength:[%d]; StringLength:[%p][%d]",
    Attribute, sql_stmt_attr_type(Attribute), BufferLength,
    StringLength, StringLength ? *StringLength : 0);
  stmt_t *stmt = (stmt_t*)StatementHandle;
  descs_t *descs = &stmt->descs;

  switch (Attribute)
  {
    case SQL_ATTR_APP_ROW_DESC:
      {
        OILE(BufferLength==sizeof(descs->app_row) || BufferLength==SQL_IS_POINTER, "");
        *(void**)Value = descs->app_row;
      } break;
    case SQL_ATTR_APP_PARAM_DESC:
      {
        OILE(BufferLength==sizeof(descs->app_param) || BufferLength==SQL_IS_POINTER, "");
        *(void**)Value = descs->app_param;
      } break;
    case SQL_ATTR_IMP_ROW_DESC:
      {
        OILE(BufferLength==sizeof(descs->imp_row) || BufferLength==SQL_IS_POINTER, "");
        *(void**)Value = descs->imp_row;
      } break;
    case SQL_ATTR_IMP_PARAM_DESC:
      {
        OILE(BufferLength==sizeof(descs->imp_param) || BufferLength==SQL_IS_POINTER, "");
        *(void**)Value = descs->imp_param;
      } break;
    default:
      {
        ONIY(0, "");
      } break;
  }

  return SQL_SUCCESS;
}

static SQLRETURN doSQLExecDirect(SQLHSTMT StatementHandle, SQLCHAR* StatementText, SQLINTEGER TextLength)
{
  P("TextLength:[%d]%s", TextLength, sql_soi_type(TextLength));

  stmt_t *stmt = (stmt_t*)StatementHandle;
  OILE(stmt, "");
  OILE(!STMT_IS_EXECUTING(stmt), "");
  stmt_close_rs(stmt);
  OILE(STMT_IS_NORM(stmt), "");
  OILE(stmt->owner, "");
  conn_t *conn = stmt->owner->conn;
  OILE(conn, "");

  errs_t *errs = &stmt->errs;

  const char *enc = conn->enc_char;

  OILE(StatementText, "");
  OILE(TextLength>=0 || TextLength==SQL_NTS, "");
  size_t slen = (TextLength>=0) ? (size_t)TextLength : (size_t)-1;
  todbc_string_t sql = todbc_string_init(enc, StatementText, slen);
  if (!sql.buf) {
    SET_OOM(errs, "");
    return SQL_ERROR;
  }

  return stmt_exec_direct(stmt, &sql);


  // stmt_t *stmt = (stmt_t*)StatementHandle;
  // rs_t  *rs  = &stmt->rs;
  // rs_close(rs);
  // OILE(rs->stmt==NULL);

  // if (1) {
  //   // taos_stmt_prepare fails to prepare a non-param-stmt-statement
  //   // thus we fall-back to use taos_query
  //   OILE(stmt->owner);
  //   conn_t *conn = stmt->owner->conn;
  //   OILE(conn);

  //   OILE(rs->stmt==NULL);
  //   paramset_t *paramset = &stmt->paramset;
  //   paramset_close(paramset);

  //   SQLRETURN e = stmt_set_statement(stmt, StatementText, TextLength);
  //   if (e!=SQL_SUCCESS) return e;

  //   taos_rs_t *taos_rs = &rs->taos_rs;

  //   const char *buf = (const char*)stmt->statement_db.buf;
  //   taos_rs->rs = taos_query(conn->taos, buf);

  //   if (!taos_rs->rs) {
  //     int err = terrno;
  //     SET_ERROR(stmt, "HY000", "failed to execute statement:[%d]%s", err, tstrerror(err));
  //     // keep executing/executed state unchanged
  //     return SQL_ERROR;
  //   }

  //   taos_rs->owner = rs;
  //   taos_rs->rs_is_opened_by_taos_query = 1;

  //   SET_EXECUTED(stmt);
  //   return stmt_after_exec(stmt);
  // } else {
  //   SQLRETURN r = doSQLPrepare(StatementHandle, StatementText, TextLength);
  //   if (r!=SQL_SUCCESS) return r;

  //   return doSQLExecute(StatementHandle);
  // }
}

static SQLRETURN doSQLRowCount(SQLHSTMT StatementHandle, SQLLEN* RowCount)
{
  OILE(RowCount, "");

  stmt_t *stmt = (stmt_t*)StatementHandle;
  OILE(stmt, "");
  OILE(STMT_IS_EXECUTED(stmt), "");

  OILE(stmt->ext.get_affected_rows, "");
  SQLRETURN r = stmt->ext.get_affected_rows(stmt, RowCount);
  if (r!=SQL_SUCCESS) return r;

  P("RowCount:[%ld]", *RowCount);

  return SQL_SUCCESS;
}

static SQLRETURN doSQLNumResultCols(SQLHSTMT StatementHandle, SQLSMALLINT *ColumnCount)
{
  OILE(ColumnCount, "");

  stmt_t *stmt = (stmt_t*)StatementHandle;
  OILE(stmt, "");
  OILE(STMT_IS_EXECUTED(stmt), "");

  OILE(stmt->ext.get_fields_count, "");
  SQLRETURN r = stmt->ext.get_fields_count(stmt, ColumnCount);
  if (r!=SQL_SUCCESS) return r;

  P("ColumnCount:[%d]", *ColumnCount);

  return SQL_SUCCESS;
}

static SQLRETURN doSQLDescribeCol(SQLHSTMT StatementHandle,
    SQLUSMALLINT ColumnNumber, SQLCHAR *ColumnName, SQLSMALLINT BufferLength,
    SQLSMALLINT *NameLength,
    SQLSMALLINT *DataType, SQLULEN *ColumnSize,
    SQLSMALLINT *DecimalDigits, SQLSMALLINT *Nullable)
{
  P("ColumnNumber:[%d]; BufferLength:[%d]", ColumnNumber, BufferLength);

  stmt_t *stmt = (stmt_t*)StatementHandle;
  OILE(stmt, "");
  OILE(STMT_IS_EXECUTED(stmt), "");

  field_arg_t field_arg = {0};
  field_arg.ColumnNumber         = ColumnNumber;
  field_arg.ColumnName           = ColumnName;
  field_arg.BufferLength         = BufferLength;
  field_arg.NameLength           = NameLength;
  field_arg.DataType             = DataType;
  field_arg.ColumnSize           = ColumnSize;
  field_arg.DecimalDigits        = DecimalDigits;
  field_arg.Nullable             = Nullable;

  OILE(stmt->ext.get_field, "");
  return stmt->ext.get_field(stmt, &field_arg);

  // sql_t *sql = (sql_t*)StatementHandle;
  // rs_t  *rs  = &sql->rs;
  // OILE(rs->sql==sql);
  // OILE(BufferLength>=0);
  // OILE(NameLength);
  // OILE(DataType);
  // OILE(ColumnSize);
  // OILE(DecimalDigits);
  // OILE(Nullable);

  // OILE(ColumnNumber>0 && ColumnNumber<=rs->n_fields);
  // field_t *field = rs->fields + (ColumnNumber-1);

  // int n = snprintf((char*)ColumnName, (size_t)BufferLength, "%s", field->name);
  // *NameLength        = (SQLSMALLINT)n;
  // *DataType          = (SQLSMALLINT)field->data_type;
  // *ColumnSize        = (SQLUSMALLINT)field->col_size;
  // *DecimalDigits     = (SQLSMALLINT)field->decimal_digits;
  // *Nullable          = (SQLSMALLINT)field->nullable;


  // P("ColumnNumber:[%d]; DataType:[%d]%s; Name:[%s]; ColumnSize:[%ld]; DecimalDigits:[%d]; Nullable:[%d]%s",
  //    ColumnNumber, *DataType, sql_sql_type(*DataType), field->name,
  //    *ColumnSize, *DecimalDigits, *Nullable, sql_nullable_type(*Nullable));

  // return SQL_SUCCESS;
}

static SQLRETURN doSQLDisconnect(SQLHDBC ConnectionHandle)
{
  conn_t *conn = (conn_t*)ConnectionHandle;
  OILE(conn, "");
  OILE(CONN_IS_CONNECTED(conn), "");

  conn_disconnect(conn);

  CONN_SET_NORM(conn);
  return SQL_SUCCESS;


  // OILE(conn->taos);

  // // conn/stmts which ever to close first?
  // // as for ODBC, it's suggested to close connection first, then release all dangling statements
  // // but we are not sure if doing so will result in memory leakage for taos
  // // thus we choose to close dangling statements before shutdown connection
  // conn_release_sqls(conn);

  // taos_close(conn->taos);
  // conn->taos    = 0;

  // return SQL_SUCCESS;
}

static SQLRETURN doSQLPrepare(SQLHSTMT StatementHandle, SQLCHAR* StatementText, SQLINTEGER TextLength)
{
  stmt_t *stmt = (stmt_t*)StatementHandle;
  OILE(stmt, "");
  OILE(!STMT_IS_EXECUTING(stmt), "");
  stmt_close_rs(stmt);
  OILE(STMT_IS_NORM(stmt), "");
  OILE(stmt->owner, "");
  conn_t *conn = stmt->owner->conn;
  OILE(conn, "");

  errs_t *errs = &stmt->errs;

  const char *enc = conn->enc_char;

  OILE(StatementText, "");
  OILE(TextLength>=0 || TextLength==SQL_NTS, "");
  size_t slen = (TextLength>=0) ? (size_t)TextLength : (size_t)-1;
  todbc_string_t sql = todbc_string_init(enc, StatementText, slen);
  if (!sql.buf) {
    SET_OOM(errs, "");
    return SQL_ERROR;
  }

  if (1) {
    todbc_string_t l = todbc_string_conv_to(&sql, conn->enc_src, NULL);
    P("prepare:[%s]", (const char*)l.buf);
  }

  return stmt_prepare(stmt, &sql);

  // sql_t *sql = (sql_t*)StatementHandle;
  // rs_t  *rs  = &sql->rs;

  // OILE(rs->taos_rs.rs_is_opened_by_taos_query==0);

  // OILE(sql->owner);
  // conn_t *conn = sql->owner->conn;
  // OILE(conn);

  // OILE(rs->sql==NULL);
  // paramset_t *paramset = &sql->paramset;
  // paramset_close(paramset);

  // SQLRETURN r = sql_set_statement(sql, StatementText, TextLength);
  // if (r!=SQL_SUCCESS) return r;
  //
  // int n_params = 0;
  // r = sql_prepare(sql, &n_params);
  // if (r!=SQL_SUCCESS) return r;

  // paramset_init_params(paramset, n_params);
  // if (!paramset->params_cache || !paramset->params || !paramset->taos_binds) {
  //   sql_close_taos_stmt(sql);
  //   SET_ERROR(sql, "HY001", "");
  //   return SQL_ERROR;
  // }

  // // for (int i=0; i<n_params; ++i) {
  // //   param_t *param = paramset->params + i;

  // // }

  // paramset->sql             = sql;
  // paramset->n_params        = n_params;

  // return sql_after_prepare(sql);
}

static SQLRETURN doSQLNumParams(SQLHSTMT StatementHandle, SQLSMALLINT *ParameterCountPtr)
{
  OILE(ParameterCountPtr, "");

  stmt_t *stmt = (stmt_t*)StatementHandle;
  OILE(stmt, "");
  OILE(stmt->prepared, "");

  *ParameterCountPtr = (SQLSMALLINT)stmt->paramset.n_params;

  P("ParameterCount:[%d]", *ParameterCountPtr);

  return SQL_SUCCESS;
}

static SQLRETURN doSQLBindParameter(
    SQLHSTMT        StatementHandle,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT     InputOutputType,
    SQLSMALLINT     ValueType,
    SQLSMALLINT     ParameterType,
    SQLULEN         ColumnSize,
    SQLSMALLINT     DecimalDigits,
    SQLPOINTER      ParameterValuePtr,
    SQLLEN          BufferLength,
    SQLLEN *        StrLen_or_IndPtr)
{
  stmt_t *stmt = (stmt_t*)StatementHandle;
  OILE(stmt, "");
  OILE(!STMT_IS_EXECUTING(stmt), "");
  OILE(stmt->owner, "");
  conn_t *conn = stmt->owner->conn;
  OILE(conn, "");

  param_binding_t arg = {0};
  arg.ParameterNumber         = ParameterNumber;
  arg.InputOutputType         = InputOutputType;
  arg.ValueType               = ValueType;                  // sql c data type
  arg.ParameterType           = ParameterType;              // sql data type
  arg.ColumnSize              = ColumnSize;
  arg.DecimalDigits           = DecimalDigits;
  arg.ParameterValuePtr       = ParameterValuePtr;
  arg.BufferLength            = BufferLength;
  arg.StrLen_or_IndPtr        = StrLen_or_IndPtr;

  return stmt_bind_param(stmt, &arg);

  // sql_t *sql = (sql_t*)StatementHandle;
  // rs_t  *rs  = &sql->rs;
  // OILE(rs->sql==NULL);
  // OILE(IS_PREPARED(sql));
  // paramset_t *paramset = &sql->paramset;
  // OILE(ParameterNumber>0);

  // OILE(StrLen_or_IndPtr);

  // paramset_realloc_bps(paramset, ParameterNumber);
  // if (!paramset->bps_cache || !paramset->bps || paramset->n_bps<ParameterNumber) {
  //   SET_ERROR(sql, "HY001", "");
  //   return SQL_ERROR;
  // }

  // bp_t *bp = paramset->bps + (ParameterNumber-1);
  // bp->ParameterNumber            = ParameterNumber;
  // bp->InputOutputType            = InputOutputType;
  // bp->ValueType                  = ValueType;
  // bp->ParameterType              = ParameterType;
  // bp->ColumnSize                 = ColumnSize;
  // bp->DecimalDigits              = DecimalDigits;
  // bp->ParameterValuePtr          = ParameterValuePtr;
  // bp->BufferLength               = BufferLength;
  // bp->StrLen_or_IndPtr           = StrLen_or_IndPtr;

  // return SQL_SUCCESS;
}

static SQLRETURN doSQLSetStmtAttr(SQLHSTMT StatementHandle, SQLINTEGER Attribute, SQLPOINTER Value, SQLINTEGER StringLength)
{
  P("Attribute:[%d]%s; Value:[%p]; StringLength:[%d]%s",
     Attribute, sql_stmt_attr_type(Attribute), Value,
     StringLength, sql_soi_type(StringLength));

  stmt_t *stmt = (stmt_t*)StatementHandle;
  OILE(stmt, "");
  OILE(!STMT_IS_EXECUTING(stmt), "");
  OILE(stmt->owner, "");
  conn_t *conn = stmt->owner->conn;
  OILE(conn, "");

  stmt_attr_t *attr = &stmt->attr;

  errs_t *errs = &stmt->errs;

  switch (Attribute)
  {
    case SQL_ATTR_PARAM_BIND_TYPE:
      {
        SQLULEN v = (SQLULEN)Value;
        ONIY(v!=SQL_PARAM_BIND_BY_COLUMN, "");
        attr->bind_type = v;
        P("%s:[%ld]", sql_stmt_attr_type(Attribute), v);
      } break;
    case SQL_ATTR_PARAMSET_SIZE:
      {
        SQLULEN v = (SQLULEN)Value;
        ONIY(v!=0, "");
        attr->paramset_size = v;
        P("%s:[%ld]", sql_stmt_attr_type(Attribute), v);
      } break;
    case SQL_ATTR_PARAM_BIND_OFFSET_PTR:
      {
        SQLULEN *v = (SQLULEN*)Value;
        attr->bind_offset_ptr = v;
        P("%s:[%p]", sql_stmt_attr_type(Attribute), v);
      } break;
    default:
      {
        P("Attribute:[%d]%s not implemented yet", Attribute, sql_stmt_attr_type(Attribute));
        SET_NIY(errs, "Attribute:[%d]%s", Attribute, sql_stmt_attr_type(Attribute));
        // ONSP(0, "Attribute:[%d]%s", Attribute, sql_stmt_attr_type(Attribute));
        return SQL_ERROR;
      } break;
  }

  return SQL_SUCCESS;
}

static SQLRETURN doSQLExecute(SQLHSTMT StatementHandle)
{
  stmt_t *stmt = (stmt_t*)StatementHandle;
  OILE(stmt, "");
  OILE(!STMT_IS_EXECUTING(stmt), "");
  stmt_close_rs(stmt);
  OILE(STMT_IS_NORM(stmt), "");
  OILE(stmt->owner, "");
  conn_t *conn = stmt->owner->conn;
  OILE(conn, "");

  return stmt_execute(stmt);

  // sql_t *sql = (sql_t*)StatementHandle;
  // OILE(sql);
  // OILE(IS_PREPARED(sql));
  // OILE(!IS_EXECUTING(sql));

  // rs_t  *rs  = &sql->rs;
  // OILE(rs->taos_rs.rs_is_opened_by_taos_query==0);
  // rs_close(rs);
  // OILE(rs->sql==NULL);

  // SET_EXECUTING(sql);

  // paramset_t *paramset = &sql->paramset;
  // OILE(paramset);

  // // fetch parameters
  // OILE(paramset->n_bps==paramset->n_params);
  // if (paramset->paramset_size==0) {
  //   // nodejs workaround
  //   paramset->paramset_size = 1;
  // }
  // for (unsigned int i=0; i<paramset->paramset_size; ++i) {
  //   for (unsigned int j=0; j<paramset->n_bps && j<paramset->n_params; ++j) {
  //     SQLRETURN r = sql_process_param(sql, i, j);
  //     if (r!=SQL_SUCCESS) return r;
  //   }
  //   OILE(paramset->taos_binds);
  //   int tr = taos_stmt_bind_param(sql->stmt, paramset->taos_binds);
  //   if (tr) {
  //     SET_ERROR(sql, "HY000", "failed to bind parameters[%d in total][%d]%s", paramset->n_params, tr, tstrerror(tr));
  //     // keep executing/executed state unchanged
  //     return SQL_ERROR;
  //   }

  //   tr = taos_stmt_add_batch(sql->stmt);
  //   if (tr) {
  //     SET_ERROR(sql, "HY000", "failed to add batch:[%d]%s", tr, tstrerror(tr));
  //     // keep executing/executed state unchanged
  //     return SQL_ERROR;
  //   }
  // }

  // if (1) {
  //   int r = taos_stmt_execute(sql->stmt);
  //   if (r) {
  //     SET_ERROR(sql, "HY000", "failed to execute statement:[%d]%s", r, tstrerror(r));
  //     // keep executing/executed state unchanged
  //     return SQL_ERROR;
  //   }

  //   taos_rs_t *taos_rs = &rs->taos_rs;
  //   OILE(taos_rs->owner==NULL);
  //   taos_rs->owner = rs;
  //   taos_rs->rs    = taos_stmt_use_result(sql->stmt);
  // }

  // SET_EXECUTED(sql);

  // return sql_after_exec(sql);
}

static SQLRETURN doSQLFetch(SQLHSTMT StatementHandle)
{
  stmt_t *stmt = (stmt_t*)StatementHandle;
  OILE(stmt, "");
  OILE(STMT_IS_EXECUTED(stmt), "");

  if (STMT_TYPEINFO(stmt)) {
    return SQL_SUCCESS;
  }

  return stmt_fetch(stmt);
  // sql_t *sql = (sql_t*)StatementHandle;
  // OILE(sql);
  // rs_t  *rs  = &sql->rs;
  // OILE(rs->n_rows==-1 || rs->curr_row<rs->n_rows);
  // row_t *row = &rs->row;
  // taos_rs_t *taos_rs = &rs->taos_rs;
  // OA(rs->n_fields==taos_rs->n_fields, "%d/%d", rs->n_fields, taos_rs->n_fields);

  // if (rs->eof) return SQL_NO_DATA;

  // if (rs->n_rows!=-1 && rs->curr_row + 1 >= rs->n_rows) {
  //   row_release(row);
  //   taos_rs->row = NULL;
  //   rs->eof = 1;
  //   return SQL_NO_DATA;
  // }

  // row_release(row);
  // taos_rs->row = NULL;
  // ++rs->curr_row;
  // row->valid = 1;
  // taos_rs->row = taos_fetch_row(taos_rs->rs);
  // D("row:[%p]", taos_rs->row);
  // if (!taos_rs->row) {
  //   row->valid = 0;
  //   rs->eof = 1;
  //   D("...");
  //   return SQL_NO_DATA;
  // }

  // // column bound?
  // if (!rs->bcs) return SQL_SUCCESS;
  // for (int i=0; i<rs->n_fields; ++i) {
  //   SQLRETURN r = sql_get_data(sql, (unsigned int)i);
  //   if (r!=SQL_SUCCESS) return r;
  // }

  // return SQL_SUCCESS;
}

static SQLRETURN doSQLGetData(SQLHSTMT StatementHandle,
           SQLUSMALLINT ColumnNumber, SQLSMALLINT TargetType,
           SQLPOINTER TargetValue, SQLLEN BufferLength,
           SQLLEN *StrLen_or_IndPtr)
{
  P("ColumnNumber:[%d]; TargetType:[%d]%s; BufferLength:[%ld]; StrLen_or_IndPtr:[%p]",
     ColumnNumber,
     TargetType, sql_c_type(TargetType),
     BufferLength, StrLen_or_IndPtr);

  OILE(TargetValue, "");

  stmt_t *stmt = (stmt_t*)StatementHandle;
  OILE(stmt, "");
  OILE(STMT_IS_EXECUTED(stmt), "");

  if (STMT_TYPEINFO(stmt)) {
    ONIY(ColumnNumber==3, "");
    ONIY(TargetType==SQL_C_LONG, "");
    int32_t v = 255;
    switch (stmt->typeinfo) {
      case SQL_VARCHAR:
      case SQL_WVARCHAR:
      case SQL_VARBINARY:
        break;
      case SQL_TIMESTAMP:
      case SQL_TYPE_TIMESTAMP:
        v = 23;
        break;
      default:
        {
          OILE(0, "");
        } break;
    }
    *(int32_t*)TargetValue = v;
    return SQL_SUCCESS;
  }

  errs_t *errs = &stmt->errs;

  OILE(ColumnNumber>=0, "");
  if (ColumnNumber==0) {
    SET_ERR(errs, "07009",  "invalid ColumnNumber[%d]", ColumnNumber);
    return SQL_ERROR;
  }

  col_binding_t binding = {
    .ColumnNumber       = ColumnNumber,
    .TargetType         = TargetType,
    .TargetValue        = TargetValue,
    .BufferLength       = BufferLength,
    .StrLen_or_IndPtr   = StrLen_or_IndPtr
  };

  return stmt_get_data(stmt, &binding);
  
  // ONIY(TargetValue);

  // sql_t *sql = (sql_t*)StatementHandle;
  // OILE(sql);
  // rs_t  *rs  = &sql->rs;
  // OILE(rs->sql==sql);

  // if (rs->eof) return SQL_NO_DATA;
  // OILE(rs->curr_row>=0);
  // OILE(rs->n_rows==-1 || rs->curr_row<rs->n_rows);

  // field_t *fields = rs->fields;
  // if (!fields) return SQL_NO_DATA;

  // row_t *row = &rs->row;
  // OILE(row->valid);

  // col_t *cols = row->cols;

  // if (cols==cols_timestamp || cols==cols_varchar || cols==cols_wvarchar || cols==cols_varbinary) {
  //   ONIY(ColumnNumber==3);
  //   ONIY(ColumnNumber<=row->n_cols);
  //   col_t *col = cols + (ColumnNumber-1);
  //   OILE(col->valid);
  //   ONIY(col->c_type==TargetType);
  //   OILE(col->c_type==SQL_C_LONG);
  //   int32_t v = col->u.c_long;
  //   ONIY(BufferLength>=sizeof(v));
  //   ONIY(StrLen_or_IndPtr==0);
  //   *(int32_t*)TargetValue = v;
  //   return SQL_SUCCESS;
  // }

  // OILE(StrLen_or_IndPtr);
  // *StrLen_or_IndPtr = SQL_NULL_DATA;

  // return SQL_SUCCESS;
}

static SQLRETURN doSQLGetDiagField(SQLSMALLINT HandleType, SQLHANDLE Handle, SQLSMALLINT RecNumber, SQLSMALLINT DiagIdentifier,
           SQLPOINTER DiagInfo, SQLSMALLINT BufferLength, SQLSMALLINT *StringLength)
{
  P("HandleType:[%d]%s; RecNumber:[%d]; DiagIdentifier:[%d]%s; BufferLength:[%d]",
     HandleType, sql_handle_type(HandleType), RecNumber,
     DiagIdentifier, sql_diag_identifier(DiagIdentifier),
     BufferLength);
  OILE(0, "");
  // terror_t *err = NULL;
  // switch (HandleType)
  // {
  //   case SQL_HANDLE_ENV:
  //     {
  //       env_t *env = (env_t*)Handle;
  //       err = &env->err;
  //     } break;
  //   case SQL_HANDLE_DBC:
  //     {
  //       conn_t *conn = (conn_t*)Handle;
  //       err = &conn->err;
  //     } break;
  //   case SQL_HANDLE_STMT:
  //     {
  //       sql_t *sql = (sql_t*)Handle;
  //       err = &sql->err;
  //     } break;
  //   default:
  //     {
  //       OILE(0);
  //       return SQL_ERROR;
  //     } break;
  // }

  // OILE(err);
  // return SQL_NO_DATA;
}

static SQLRETURN doSQLGetTypeInfo(SQLHSTMT StatementHandle, SQLSMALLINT DataType)
{
  P("DataType:[%d]%s", DataType, sql_sql_type(DataType));

  stmt_t *stmt = (stmt_t*)StatementHandle;
  OILE(stmt, "");
  OILE(!STMT_IS_EXECUTING(stmt), "");
  stmt_close_rs(stmt);
  OILE(STMT_IS_NORM(stmt), "");
  OILE(stmt->owner, "");
  conn_t *conn = stmt->owner->conn;
  OILE(conn, "");

  switch (DataType)
  {
    case SQL_VARCHAR:
    case SQL_WVARCHAR:
    case SQL_VARBINARY:
    case SQL_TIMESTAMP:
    case SQL_TYPE_TIMESTAMP:
      {
        stmt->typeinfo = DataType;
      } break;
    default:
      {
        ONIY(0, "");
        return SQL_ERROR;
      } break;
  }

  STMT_SET_EXECUTED(stmt);

  return SQL_SUCCESS;
}

static SQLRETURN doSQLDescribeParam(
    SQLHSTMT        StatementHandle,
    SQLUSMALLINT    ParameterNumber,
    SQLSMALLINT *   DataTypePtr,
    SQLULEN *       ParameterSizePtr,
    SQLSMALLINT *   DecimalDigitsPtr,
    SQLSMALLINT *   NullablePtr)
{
  // SQLRETURN r;

  OILE(ParameterNumber>0, "");

  stmt_t *stmt = (stmt_t*)StatementHandle;
  OILE(stmt, "");
  OILE(stmt->prepared, "");
  OILE(stmt->owner, "");
  conn_t *conn = stmt->owner->conn;
  OILE(conn, "");

  errs_t *errs = &stmt->errs;

  // const char *enc = conn->enc_char;

  paramset_t *paramset = &stmt->paramset;
  param_t    *params   = paramset->params;
  int         n_params = paramset->n_params;

  if (ParameterNumber>n_params) {
    SET_ERR(errs, "07009", "invalid ParameterNumber/#params [%d/%d]", ParameterNumber, n_params);
    return SQL_ERROR;
  }

  param_t *param = params + (ParameterNumber-1);
  if (DataTypePtr)      *DataTypePtr          = param->DataType;
  if (ParameterSizePtr) *ParameterSizePtr     = param->ParameterSize;
  if (DecimalDigitsPtr) *DecimalDigitsPtr     = param->DecimalDigits;
  if (NullablePtr)      *NullablePtr          = param->Nullable;

  return SQL_SUCCESS;

  // sql_t *sql = (sql_t*)StatementHandle;
  // rs_t  *rs  = &sql->rs;
  // OILE(rs->sql==NULL);
  // OILE(IS_PREPARED(sql));
  // OILE(DataTypePtr);
  // OILE(ParameterSizePtr);
  // OILE(DecimalDigitsPtr);

  // paramset_t *paramset = &sql->paramset;
  // OILE(paramset->sql);
  // OILE(ParameterNumber>0 && ParameterNumber<=paramset->n_params);

  // *DataTypePtr            = SQL_C_CHAR;
  // *ParameterSizePtr       = 23;
  // *DecimalDigitsPtr       = 0;
  // if (NullablePtr) *NullablePtr            = SQL_NULLABLE;

  // param_t *param = paramset->params + (ParameterNumber-1);
  // int taos_type = param->taos_type;
  // switch (taos_type)
  // {
  //   case TSDB_DATA_TYPE_TIMESTAMP:
  //     {
  //       *DataTypePtr            = SQL_CHAR;
  //       *ParameterSizePtr       = 23;
  //       *DecimalDigitsPtr       = 0;
  //       if (NullablePtr) *NullablePtr            = SQL_NULLABLE;
  //     } break;
  //   case TSDB_DATA_TYPE_TINYINT:
  //     {
  //       *DataTypePtr            = SQL_TINYINT;
  //       *ParameterSizePtr       = 1;
  //       *DecimalDigitsPtr       = 0;
  //       if (NullablePtr) *NullablePtr            = SQL_NULLABLE;
  //     } break;
  //   default:
  //     {
  //       OA(0, "taos param:[%d][%d]%s", ParameterNumber, taos_type, taos_data_type(taos_type));
  //       return SQL_ERROR;
  //     } break;
  // }

  // P("ParameterNumber:[%d]; DataTypePtr:[%p]%s; ParameterSizePtr:[%p]%ld; DecimalDigitsPtr:[%p]%d; NullablePtr:[%p]%d",
  //    ParameterNumber,
  //    DataTypePtr, DataTypePtr ? sql_sql_type(*DataTypePtr) : "UNKNOWN",
  //    ParameterSizePtr, ParameterSizePtr ? *ParameterSizePtr : 0,
  //    DecimalDigitsPtr, DecimalDigitsPtr ? *DecimalDigitsPtr : 0,
  //    NullablePtr, NullablePtr ? *NullablePtr : 0);
  // return SQL_SUCCESS;
}

static SQLRETURN doSQLFreeConnect(SQLHDBC ConnectionHandle)
{
  conn_t *conn = (conn_t*)ConnectionHandle;
  OILE(conn->stmts.count==0, "");
  conn_free(conn);
  return SQL_SUCCESS;
}

static SQLRETURN doSQLFreeEnv(SQLHENV EnvironmentHandle)
{
  env_t *env = (env_t*)EnvironmentHandle;
  env_dec_ref(env);
  return SQL_SUCCESS;
}

static SQLRETURN doSQLBindCol(SQLHSTMT StatementHandle,
    SQLUSMALLINT ColumnNumber, SQLSMALLINT TargetType,
    SQLPOINTER TargetValue,
    SQLLEN BufferLength, SQLLEN *StrLen_or_IndPtr)
{
  P("ColumnNumber:[%d]; TargetType:[%d]%s; BufferLength:[%ld]",
     ColumnNumber, TargetType, sql_c_type(TargetType), BufferLength);

  stmt_t *stmt = (stmt_t*)StatementHandle;
  OILE(stmt, "");
  OILE(STMT_IS_EXECUTED(stmt), "");

  errs_t *errs = &stmt->errs;

  OILE(ColumnNumber>=0, "");
  if (ColumnNumber==0) {
    SET_ERR(errs, "07009",  "invalid ColumnNumber[%d]", ColumnNumber);
    return SQL_ERROR;
  }

  col_binding_t binding = {
    .ColumnNumber       = ColumnNumber,
    .TargetType         = TargetType,
    .TargetValue        = TargetValue,
    .BufferLength       = BufferLength,
    .StrLen_or_IndPtr   = StrLen_or_IndPtr
  };

  return stmt_bind_col(stmt, &binding);
  

  // OILE(ColumnNumber>0);
  // sql_t *sql = (sql_t*)StatementHandle;
  // OILE(!IS_EXECUTING(sql));

  // rs_t  *rs  = &sql->rs;
  // OILE(rs->fields);
  // OILE(rs->n_fields>=ColumnNumber);

  // if (!rs->bcs_cache) {
  //   rs->bcs_cache = todbc_buf_create();
  //   if (!rs->bcs_cache) {
  //     SET_ERROR(sql, "HY001", "");
  //     return SQL_ERROR;
  //   }
  //   OILE(rs->bcs==NULL);
  //   rs->bcs = (bc_t*)todbc_buf_calloc(rs->bcs_cache, (size_t)rs->n_fields, sizeof(*rs->bcs));
  //   if (!rs->bcs) {
  //     SET_ERROR(sql, "HY001", "");
  //     return SQL_ERROR;
  //   }
  // }
  // OILE(rs->bcs);

  // bc_t *bc = rs->bcs + (ColumnNumber-1);
  // bc->ColumnNumber      = ColumnNumber;
  // bc->TargetType        = TargetType;
  // bc->TargetValue       = TargetValue;
  // bc->BufferLength      = BufferLength;
  // bc->StrLen_or_IndPtr  = StrLen_or_IndPtr;

  // return SQL_SUCCESS;
}

static SQLRETURN doSQLCloseCursor(SQLHSTMT StatementHandle)
{
  stmt_t *stmt = (stmt_t*)StatementHandle;
  OILE(stmt, "");
  errs_t *errs = &stmt->errs;

  if (STMT_IS_NORM(stmt)) {
    OW("no cursor was opened previously");
    SET_ERR(errs, "24000", "");
    return SQL_ERROR;
  }

  OILE(STMT_IS_EXECUTED(stmt), "");
  stmt_close_rs(stmt);

  return SQL_SUCCESS;

  // sql_t *sql = (sql_t*)StatementHandle;
  // OILE(!IS_EXECUTING(sql));

  // rs_t  *rs  = &sql->rs;
  // if (!rs->sql) {
  //   SET_ERROR(sql, "24000", "");
  //   OW("no cursor was opened previously");
  //   return SQL_ERROR;
  // }

  // OILE(rs->sql==sql);
  // rs_close(rs);

  // return SQL_SUCCESS;
}

static SQLRETURN doSQLError(SQLHENV EnvironmentHandle,
    SQLHDBC ConnectionHandle, SQLHSTMT StatementHandle,
    SQLCHAR *Sqlstate, SQLINTEGER *NativeError,
    SQLCHAR *MessageText, SQLSMALLINT BufferLength,
    SQLSMALLINT *TextLength)
{
  env_t   *env   = (env_t*)EnvironmentHandle;
  conn_t  *conn  = (conn_t*)ConnectionHandle;
  stmt_t  *stmt  = (stmt_t*)StatementHandle;
  OD("env/conn/stmt:[%p/%p/%p]", env, conn, stmt);

  SQLSMALLINT HandleType;
  SQLHANDLE   Handle;
  if (stmt) {
    HandleType = SQL_HANDLE_STMT;
    Handle     = StatementHandle;
  } else if (conn) {
    HandleType = SQL_HANDLE_DBC;
    Handle     = ConnectionHandle;
  } else if (env) {
    HandleType = SQL_HANDLE_ENV;
    Handle     = EnvironmentHandle;
  } else {
    return SQL_NO_DATA;
  }

  return doSQLGetDiagRec(HandleType, Handle, 1, Sqlstate, NativeError, MessageText, BufferLength, TextLength);
}


#ifdef _MSC_VER

#define POST_INSTALLER_ERROR(hwndParent, code, fmt, ...)             \
do {                                                                 \
  char buf[4096];                                                    \
  snprintf(buf, sizeof(buf), "%s[%d]%s():" fmt "",                   \
           basename((char*)__FILE__), __LINE__, __func__,            \
           ##__VA_ARGS__);                                           \
  SQLPostInstallerError(code, buf);                                  \
  if (hwndParent) {                                                  \
    MessageBox(hwndParent, buf, "Error", MB_OK|MB_ICONEXCLAMATION);  \
  }                                                                  \
} while (0)

typedef struct kv_s           kv_t;
struct kv_s {
  char    *line;
  size_t   val;
};

static BOOL get_driver_dll_path(HWND hwndParent, char *buf, size_t len)
{
  HMODULE hm = NULL;

  if (GetModuleHandleEx(GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS | GET_MODULE_HANDLE_EX_FLAG_UNCHANGED_REFCOUNT,
          (LPCSTR) &ConfigDSN, &hm) == 0)
  {
      int ret = GetLastError();
      POST_INSTALLER_ERROR(hwndParent, ODBC_ERROR_REQUEST_FAILED, "GetModuleHandle failed, error = %d\n", ret);
      return FALSE;
  }
  if (GetModuleFileName(hm, buf, (DWORD)len) == 0)
  {
      int ret = GetLastError();
      POST_INSTALLER_ERROR(hwndParent, ODBC_ERROR_REQUEST_FAILED, "GetModuleFileName failed, error = %d\n", ret);
      return FALSE;
  }
  return TRUE;
}

static BOOL doDSNAdd(HWND	hwndParent, LPCSTR	lpszDriver, LPCSTR lpszAttributes)
{
  BOOL r = TRUE;

  kv_t *kvs = NULL;

  kv_t dsn = {0};
  char *line = NULL;

  do {
    char driver_dll[MAX_PATH + 1];
    r = get_driver_dll_path(hwndParent, driver_dll, sizeof(driver_dll));
    if (!r) break;

    dsn.line = strdup("DSN=TAOS_DEMO");
    if (!dsn.line) { r = FALSE; break; }

    const char *p = lpszAttributes;
    int ikvs = 0;
    while (p && *p) {
      line = strdup(p);
      if (!line) { r = FALSE; break; }
      char *v = strchr(line, '=');
      if (!v) { r = FALSE; break; }

      if (strstr(line, "DSN")==line) {
        if (dsn.line) {
          free(dsn.line);
          dsn.line = NULL;
          dsn.val  = 0;
        }
        dsn.line = line;
        line = NULL;
      } else {
        kv_t *t = (kv_t*)realloc(kvs, (ikvs+1)*sizeof(*t));
        if (!t) { r = FALSE; free(line); break; }
        t[ikvs].line = line;
        *v = '\0';
        if (v) t[ikvs].val = v - line + 1;
        line = NULL;

        kvs = t;
        ++ikvs;
      }

      p += strlen(p) + 1;
    }

    if (hwndParent) {
      MessageBox(hwndParent, "Please use odbcconf to add DSN for TAOS ODBC Driver", "Warning!", MB_OK|MB_ICONEXCLAMATION);
    }
    if (!r) break;

    char *v = NULL;
    v = strchr(dsn.line, '=');
    if (!v) { r = FALSE; break; }
    *v = '\0';
    dsn.val = v - dsn.line + 1;

    if ((!dsn.line)) {
      if (!r) POST_INSTALLER_ERROR(hwndParent, ODBC_ERROR_REQUEST_FAILED, "lack of either DSN or Driver");
    } else {
      if (r) r = SQLWritePrivateProfileString("ODBC Data Sources", dsn.line+dsn.val, lpszDriver, "Odbc.ini");
      if (r) r = SQLWritePrivateProfileString(dsn.line+dsn.val, "Driver", driver_dll, "Odbc.ini");
    }

    for (int i=0; r && i<ikvs; ++i) {
      const char *k = kvs[i].line;
      const char *v = NULL;
      if (kvs[i].val) v = kvs[i].line + kvs[i].val;
      r = SQLWritePrivateProfileString(dsn.line+dsn.val, k, v, "Odbc.ini");
    }
  } while (0);

  if (dsn.line) free(dsn.line);
  if (line) free(line);

  return r;
}

static BOOL doDSNConfig(HWND	hwndParent, LPCSTR	lpszDriver, LPCSTR lpszAttributes)
{
  const char *p = lpszAttributes;
  while (p && *p) {
    p += strlen(p) + 1;
  }
  return FALSE;
}

static BOOL doDSNRemove(HWND	hwndParent, LPCSTR	lpszDriver, LPCSTR lpszAttributes)
{
  BOOL r = TRUE;

  kv_t dsn = {0};
  char *line = NULL;

  do {
    const char *p = lpszAttributes;
    int ikvs = 0;
    while (p && *p) {
      line = strdup(p);
      if (!line) { r = FALSE; break; }
      char *v = strchr(line, '=');
      if (!v) { r = FALSE; break; }
      *v = '\0';

      if (strstr(line, "DSN")==line) {
        if (dsn.line) {
          free(dsn.line);
          dsn.line = NULL;
          dsn.val  = 0;
        }
        dsn.line = line;
        dsn.val = v - line + 1;
        line = NULL;
        break;
      } else {
        free(line);
        line = NULL;
      }

      p += strlen(p) + 1;
    }

    if (!r) break;

    if (!dsn.line) {
      POST_INSTALLER_ERROR(hwndParent, ODBC_ERROR_REQUEST_FAILED, "lack of DSN");
      r = FALSE;
      break;
    }

    r = SQLWritePrivateProfileString("ODBC Data Sources", dsn.line+dsn.val, NULL, "Odbc.ini");
    if (!r) break;

    char buf[8192];
    r = SQLGetPrivateProfileString(dsn.line+dsn.val, NULL, "null", buf, sizeof(buf), "Odbc.ini");
    if (!r) break;

    int n = 0;
    char *s = buf;
    while (s && *s && n++<10) {
      SQLWritePrivateProfileString(dsn.line+dsn.val, s, NULL, "Odbc.ini");
      s += strlen(s) + 1;
    }
  } while (0);

  if (dsn.line) free(dsn.line);
  if (line) free(line);
  return r;
}

static BOOL doConfigDSN(HWND	hwndParent, WORD fRequest, LPCSTR	lpszDriver, LPCSTR lpszAttributes)
{
  BOOL r = FALSE;
  const char *sReq = NULL;
  switch(fRequest) {
    case ODBC_ADD_DSN:    sReq = "ODBC_ADD_DSN";      break;
    case ODBC_CONFIG_DSN: sReq = "ODBC_CONFIG_DSN";   break;
    case ODBC_REMOVE_DSN: sReq = "ODBC_REMOVE_DSN";   break;
    default:              sReq = "UNKNOWN";           break;
  }
  switch(fRequest) {
    case ODBC_ADD_DSN: {
      r = doDSNAdd(hwndParent, lpszDriver, lpszAttributes);
    } break;
    case ODBC_CONFIG_DSN: {
      r = doDSNConfig(hwndParent, lpszDriver, lpszAttributes);
    } break;
    case ODBC_REMOVE_DSN: {
      r = doDSNRemove(hwndParent, lpszDriver, lpszAttributes);
    } break;
    default: {
      POST_INSTALLER_ERROR(hwndParent, ODBC_ERROR_GENERAL_ERR, "not implemented yet");
      r = FALSE;
    } break;
  }
  return r;
}

BOOL INSTAPI ConfigDSN(HWND	hwndParent, WORD fRequest, LPCSTR	lpszDriver, LPCSTR lpszAttributes)
{
  BOOL r;
  r = doConfigDSN(hwndParent, fRequest, lpszDriver, lpszAttributes);
  return r;
}

BOOL INSTAPI ConfigTranslator(HWND hwndParent, DWORD *pvOption)
{
  POST_INSTALLER_ERROR(hwndParent, ODBC_ERROR_GENERAL_ERR, "not implemented yet");
  return FALSE;
}

BOOL INSTAPI ConfigDriver(HWND hwndParent, WORD fRequest, LPCSTR lpszDriver, LPCSTR lpszArgs,
                          LPSTR lpszMsg, WORD cbMsgMax, WORD *pcbMsgOut)
{
  POST_INSTALLER_ERROR(hwndParent, ODBC_ERROR_GENERAL_ERR, "not implemented yet");
  return FALSE;
}

#endif // _MSC_VER
