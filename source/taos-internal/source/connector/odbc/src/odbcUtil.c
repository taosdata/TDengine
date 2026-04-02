/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "odbcUtil.h"
#include "pthread.h"
#include <stdarg.h>

//taos_init can't be used in setup dialog
static bool odbcLoaded = false;

void odbc_init_imp(void)
{
#if defined(_WIN32) || defined(_WIN64)
  taos_init();
  odbcDebug("taosodbc.dll load success");
  odbcLoaded = true;
#else
  taos_init();
  odbcDebug("libtaosodbc.so load success");
  odbcLoaded = true;
#endif
}

void odbc_init()
{
  static pthread_once_t odbcinit = PTHREAD_ONCE_INIT;
  pthread_once(&odbcinit, odbc_init_imp);
}

void odbc_setup_init_imp(void)
{
#if defined(_WIN32) || defined(_WIN64)
  if (!odbcLoaded) {
    struct stat dirstat;
    if (stat(tsLogDir, &dirstat) < 0) mkdir(tsLogDir, 0755); 
    
    char temp[128];
    sprintf(temp, "%s/setuplog", tsLogDir);
    if (taosInitLog(temp, 10000, 10) < 0) {
      printf("failed to open log file in directory:%s\n", tsLogDir);
    }

    odbcDebug("taosodbc.dll for setup load success");
    odbcLoaded = true;
  }
#else
  if (!odbcLoaded) {
    char temp[128];
    sprintf(temp, "%s/setuplog", tsLogDir);
    taosOpenLogFileWithMaxLines(temp, 10000, 10);
    odbcDebug("libtaosodbc.so for setup load success");
    odbcLoaded = true;
  }
#endif
}

void odbc_setup_init()
{
  static pthread_once_t odbcinit = PTHREAD_ONCE_INIT; 
  pthread_once(&odbcinit, odbc_setup_init_imp);
}

const char* odbcHandleTypeString(SQLSMALLINT type)
{
  switch (type) {
  case SQL_HANDLE_ENV:
    return "env";
  case SQL_HANDLE_DBC:
    return "dbc";
  case SQL_HANDLE_STMT:
    return "stmt";
  case SQL_HANDLE_DESC:
    return "desc";
  default:
    return "invalid handle type";
  }
}

const char* odbcInfoTypeString(SQLUSMALLINT type)
{
  switch (type) {
  case SQL_MAX_USER_NAME_LEN: return "SQL_MAX_USER_NAME_LEN";
  case SQL_USER_NAME: return "SQL_USER_NAME";
  case SQL_DRIVER_ODBC_VER: return "SQL_DRIVER_ODBC_VER checked";
  case SQL_ACTIVE_CONNECTIONS: return "SQL_ACTIVE_CONNECTIONS";
  case SQL_ACTIVE_STATEMENTS: return "SQL_ACTIVE_STATEMENTS";

#ifdef SQL_ASYNC_MODE
  case SQL_ASYNC_MODE: return "SQL_ASYNC_MODE";
#endif
#ifdef SQL_CREATE_TABLE
  case SQL_CREATE_TABLE: return "SQL_CREATE_TABLE";
#endif
#ifdef SQL_CREATE_VIEW
  case SQL_CREATE_VIEW: return "SQL_CREATE_VIEW";
#endif
#ifdef SQL_DDL_INDEX
  case SQL_DDL_INDEX: return "SQL_DDL_INDEX";
#endif
#ifdef SQL_DROP_TABLE
  case SQL_DROP_TABLE: return "SQL_DROP_TABLE";
#endif
#ifdef SQL_DROP_VIEW
  case SQL_DROP_VIEW: return "SQL_DROP_VIEW";
#endif
#ifdef SQL_INDEX_KEYWORDS
  case SQL_INDEX_KEYWORDS: return "SQL_INDEX_KEYWORDS";
#endif

  case SQL_DATA_SOURCE_NAME: return "SQL_DATA_SOURCE_NAME";
  case SQL_DRIVER_NAME: return "SQL_DRIVER_NAME checked";
  case SQL_DRIVER_VER: return "SQL_DRIVER_VER checked";
  case SQL_FETCH_DIRECTION: return "SQL_FETCH_DIRECTION";
  case SQL_ODBC_VER: return "SQL_ODBC_VER";
  case SQL_ODBC_SAG_CLI_CONFORMANCE: return "SQL_ODBC_SAG_CLI_CONFORMANCE";
  case SQL_STANDARD_CLI_CONFORMANCE: return "SQL_STANDARD_CLI_CONFORMANCE";
  case SQL_SQL_CONFORMANCE: return "SQL_SQL_CONFORMANCE checked";
  case SQL_SERVER_NAME: return "SQL_SERVER_NAME checked";
  case SQL_DATABASE_NAME: return "SQL_DATABASE_NAME";
  case SQL_SEARCH_PATTERN_ESCAPE: return "SQL_SEARCH_PATTERN_ESCAPE";
  case SQL_ODBC_SQL_CONFORMANCE: return "SQL_ODBC_SQL_CONFORMANCE";
  case SQL_ODBC_API_CONFORMANCE: return "SQL_ODBC_API_CONFORMANCE";
  case SQL_DBMS_NAME: return "SQL_DBMS_NAME checked";
  case SQL_DBMS_VER: return "SQL_DBMS_VER checked";
  case SQL_COLUMN_ALIAS: return "SQL_COLUMN_ALIAS checked";
  case SQL_NEED_LONG_DATA_LEN: return "SQL_NEED_LONG_DATA_LEN";
  case SQL_ROW_UPDATES: return "SQL_ROW_UPDATES";
  case SQL_ACCESSIBLE_PROCEDURES: return "SQL_ACCESSIBLE_PROCEDURES";
  case SQL_PROCEDURES: return "SQL_PROCEDURES";
  case SQL_EXPRESSIONS_IN_ORDERBY:return "SQL_EXPRESSIONS_IN_ORDERBY";
  case SQL_ODBC_SQL_OPT_IEF:return "SQL_ODBC_SQL_OPT_IEF";
  case SQL_LIKE_ESCAPE_CLAUSE:return "SQL_LIKE_ESCAPE_CLAUSE";
  case SQL_ORDER_BY_COLUMNS_IN_SELECT:return "SQL_ORDER_BY_COLUMNS_IN_SELECT";
  case SQL_OUTER_JOINS:return "SQL_OUTER_JOINS";
  case SQL_ACCESSIBLE_TABLES:return "SQL_ACCESSIBLE_TABLES";
  case SQL_MULT_RESULT_SETS:return "SQL_MULT_RESULT_SETS";
  case SQL_MULTIPLE_ACTIVE_TXN:return "SQL_MULTIPLE_ACTIVE_TXN";
  case SQL_MAX_ROW_SIZE_INCLUDES_LONG:return "SQL_MAX_ROW_SIZE_INCLUDES_LONG";

#ifdef SQL_CATALOG_NAME
  case SQL_CATALOG_NAME: return "SQL_CATALOG_NAME";
#endif

  case SQL_DATA_SOURCE_READ_ONLY:return "SQL_DATA_SOURCE_READ_ONLY";

#ifdef SQL_OJ_CAPABILITIES
  case SQL_OJ_CAPABILITIES: return "SQL_OJ_CAPABILITIES checked";
#endif
#ifdef SQL_MAX_IDENTIFIER_LEN
  case SQL_MAX_IDENTIFIER_LEN: return "SQL_MAX_IDENTIFIER_LEN checked";
#endif

  case SQL_CONCAT_NULL_BEHAVIOR: return "SQL_CONCAT_NULL_BEHAVIOR";
  case SQL_CURSOR_COMMIT_BEHAVIOR: return "SQL_CURSOR_COMMIT_BEHAVIOR checked";
  case SQL_CURSOR_ROLLBACK_BEHAVIOR: return "SQL_CURSOR_ROLLBACK_BEHAVIOR checked";

#ifdef SQL_CURSOR_SENSITIVITY
  case SQL_CURSOR_SENSITIVITY: return "SQL_CURSOR_SENSITIVITY";
#endif

  case SQL_DEFAULT_TXN_ISOLATION: return "SQL_DEFAULT_TXN_ISOLATION";

#ifdef SQL_DESCRIBE_PARAMETER
  case SQL_DESCRIBE_PARAMETER: return "SQL_DESCRIBE_PARAMETER";
#endif

  case SQL_TXN_ISOLATION_OPTION: return "SQL_TXN_ISOLATION_OPTION";
  case SQL_IDENTIFIER_CASE: return "SQL_IDENTIFIER_CASE";
  case SQL_IDENTIFIER_QUOTE_CHAR: return "SQL_IDENTIFIER_QUOTE_CHAR checked";
  case SQL_MAX_TABLE_NAME_LEN: return "SQL_MAX_TABLE_NAME_LEN";
  case SQL_MAX_COLUMN_NAME_LEN: return "SQL_MAX_COLUMN_NAME_LEN";
  case SQL_MAX_CURSOR_NAME_LEN: return "SQL_MAX_CURSOR_NAME_LEN";
  case SQL_MAX_PROCEDURE_NAME_LEN: return "SQL_MAX_PROCEDURE_NAME_LEN";
  case SQL_MAX_QUALIFIER_NAME_LEN: return "SQL_MAX_QUALIFIER_NAME_LEN";

  case SQL_MAX_OWNER_NAME_LEN: return "SQL_MAX_OWNER_NAME_LEN";
  case SQL_OWNER_TERM: return "SQL_OWNER_TERM checked";
  case SQL_PROCEDURE_TERM: return "SQL_PROCEDURE_TERM checked";
  case SQL_QUALIFIER_NAME_SEPARATOR: return "SQL_QUALIFIER_NAME_SEPARATOR checked";//SQL_CATALOG_NAME_SEPARATOR
  case SQL_QUALIFIER_TERM: return "SQL_QUALIFIER_TERM checked";

  case SQL_QUALIFIER_USAGE: return "SQL_QUALIFIER_USAGE checked";
  case SQL_SCROLL_CONCURRENCY: return "SQL_SCROLL_CONCURRENCY";
  case SQL_SCROLL_OPTIONS: return "SQL_SCROLL_OPTIONS";
  case SQL_TABLE_TERM: return "SQL_TABLE_TERM checked";
  case SQL_TXN_CAPABLE: return "SQL_TXN_CAPABLE checked";
  case SQL_CONVERT_FUNCTIONS: return "SQL_CONVERT_FUNCTIONS checked";

  case SQL_SYSTEM_FUNCTIONS: return "SQL_SYSTEM_FUNCTIONS checked";
  case SQL_NUMERIC_FUNCTIONS: return "SQL_NUMERIC_FUNCTIONS checked";
  case SQL_STRING_FUNCTIONS: return "SQL_STRING_FUNCTIONS checked";
  case SQL_TIMEDATE_FUNCTIONS: return "SQL_TIMEDATE_FUNCTIONS checked";

  case SQL_CONVERT_BIGINT: return "SQL_CONVERT_BIGINT";
  case SQL_CONVERT_BIT: return "SQL_CONVERT_BIT";
  case SQL_CONVERT_CHAR: return "SQL_CONVERT_CHAR";
  case SQL_CONVERT_DATE: return "SQL_CONVERT_DATE";
  case SQL_CONVERT_DECIMAL: return "SQL_CONVERT_DECIMAL";
  case SQL_CONVERT_DOUBLE: return "SQL_CONVERT_DOUBLE";
  case SQL_CONVERT_FLOAT: return "SQL_CONVERT_FLOAT";
  case SQL_CONVERT_INTEGER: return "SQL_CONVERT_INTEGER";
  case SQL_CONVERT_LONGVARCHAR: return "SQL_CONVERT_LONGVARCHAR";
  case SQL_CONVERT_NUMERIC: return "SQL_CONVERT_NUMERIC";
  case SQL_CONVERT_REAL: return "SQL_CONVERT_REAL";
  case SQL_CONVERT_SMALLINT: return "SQL_CONVERT_SMALLINT";
  case SQL_CONVERT_TIME: return "SQL_CONVERT_TIME";
  case SQL_CONVERT_TIMESTAMP: return "SQL_CONVERT_TIMESTAMP";
  case SQL_CONVERT_TINYINT: return "SQL_CONVERT_TINYINT";
  case SQL_CONVERT_VARCHAR: return "SQL_CONVERT_VARCHAR";

  case SQL_CONVERT_BINARY: return "SQL_CONVERT_BINARY";
  case SQL_CONVERT_VARBINARY: return "SQL_CONVERT_VARBINARY";
  case SQL_CONVERT_LONGVARBINARY: return "SQL_CONVERT_LONGVARBINARY";
  case SQL_POSITIONED_STATEMENTS: return "SQL_POSITIONED_STATEMENTS";
  case SQL_LOCK_TYPES: return "SQL_LOCK_TYPES";
  case SQL_BOOKMARK_PERSISTENCE: return "SQL_BOOKMARK_PERSISTENCE";
  case SQL_UNION: return "SQL_UNION";
  case SQL_SCHEMA_USAGE: return "SQL_SCHEMA_USAGE checked";  //SQL_OWNER_USAGE
  case SQL_SUBQUERIES: return "SQL_SUBQUERIES";
  case SQL_TIMEDATE_ADD_INTERVALS: return "SQL_TIMEDATE_ADD_INTERVALS checked";
  case SQL_TIMEDATE_DIFF_INTERVALS: return "SQL_TIMEDATE_DIFF_INTERVALS checked";
  case SQL_QUOTED_IDENTIFIER_CASE: return "SQL_QUOTED_IDENTIFIER_CASE checked";
  case SQL_POS_OPERATIONS: return "SQL_POS_OPERATIONS";
  case SQL_ALTER_TABLE: return "SQL_ALTER_TABLE";
  case SQL_CORRELATION_NAME: return "SQL_CORRELATION_NAME";
  case SQL_NON_NULLABLE_COLUMNS: return "SQL_NON_NULLABLE_COLUMNS";
  case SQL_NULL_COLLATION: return "SQL_NULL_COLLATION";
  case SQL_MAX_COLUMNS_IN_GROUP_BY: return "SQL_MAX_COLUMNS_IN_GROUP_BY";
  case SQL_MAX_COLUMNS_IN_ORDER_BY: return "SQL_MAX_COLUMNS_IN_ORDER_BY";
  case SQL_MAX_COLUMNS_IN_SELECT: return "SQL_MAX_COLUMNS_IN_SELECT";
  case SQL_MAX_COLUMNS_IN_TABLE: return "SQL_MAX_COLUMNS_IN_TABLE";
  case SQL_MAX_ROW_SIZE: return "SQL_MAX_ROW_SIZE";
  case SQL_MAX_TABLES_IN_SELECT: return "SQL_MAX_TABLES_IN_SELECT";
  case SQL_MAX_BINARY_LITERAL_LEN: return "SQL_MAX_BINARY_LITERAL_LEN";
  case SQL_MAX_CHAR_LITERAL_LEN: return "SQL_MAX_CHAR_LITERAL_LEN";
  case SQL_MAX_COLUMNS_IN_INDEX: return "SQL_MAX_COLUMNS_IN_INDEX";
  case SQL_MAX_INDEX_SIZE: return "SQL_MAX_INDEX_SIZE";

#ifdef SQL_MAX_IDENTIFIER_LENGTH
  case SQL_MAX_IDENTIFIER_LENGTH: return "SQL_MAX_IDENTIFIER_LENGTH";
#endif

  case SQL_MAX_STATEMENT_LEN: return "SQL_MAX_STATEMENT_LEN";
  case SQL_QUALIFIER_LOCATION: return "SQL_QUALIFIER_LOCATION";
  case SQL_GETDATA_EXTENSIONS: return "SQL_GETDATA_EXTENSIONS checked";
  case SQL_STATIC_SENSITIVITY: return "SQL_STATIC_SENSITIVITY";
  case SQL_FILE_USAGE: return "SQL_FILE_USAGE";
  case SQL_GROUP_BY: return "SQL_GROUP_BY";
  case SQL_KEYWORDS: return "SQL_KEYWORDS";
  case SQL_SPECIAL_CHARACTERS: return "SQL_SPECIAL_CHARACTERS checked";

#ifdef SQL_COLLATION_SEQ
  case SQL_COLLATION_SEQ: return "SQL_COLLATION_SEQ";
#endif

  case SQL_BATCH_SUPPORT: return "SQL_BATCH_SUPPORT";
  case SQL_BATCH_ROW_COUNT: return "SQL_BATCH_ROW_COUNT";
  case SQL_PARAM_ARRAY_ROW_COUNTS: return "SQL_PARAM_ARRAY_ROW_COUNTS";
  case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1: return "SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1";
  case SQL_STATIC_CURSOR_ATTRIBUTES1: return "SQL_STATIC_CURSOR_ATTRIBUTES1";
  case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2: return "SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2";
  case SQL_STATIC_CURSOR_ATTRIBUTES2: return "SQL_STATIC_CURSOR_ATTRIBUTES2";
  case SQL_KEYSET_CURSOR_ATTRIBUTES1: return "SQL_KEYSET_CURSOR_ATTRIBUTES1";
  case SQL_KEYSET_CURSOR_ATTRIBUTES2: return "SQL_KEYSET_CURSOR_ATTRIBUTES2";
  case SQL_DYNAMIC_CURSOR_ATTRIBUTES1: return "SQL_DYNAMIC_CURSOR_ATTRIBUTES1";

  case SQL_DYNAMIC_CURSOR_ATTRIBUTES2: return "SQL_DYNAMIC_CURSOR_ATTRIBUTES2";
  case SQL_ODBC_INTERFACE_CONFORMANCE: return "SQL_ODBC_INTERFACE_CONFORMANCE checked";
  case 65003: return "SQL_OJ_CAPABILITIES checked?";

  case SQL_AGGREGATE_FUNCTIONS: return "SQL_AGGREGATE_FUNCTIONS checked";
  case SQL_DATETIME_LITERALS: return "SQL_DATETIME_LITERALS checked";


  case SQL_SQL92_DATETIME_FUNCTIONS: return "SQL_SQL92_DATETIME_FUNCTIONS checked";
  case SQL_SQL92_FOREIGN_KEY_DELETE_RULE: return "SQL_SQL92_FOREIGN_KEY_DELETE_RULE";
  case SQL_SQL92_FOREIGN_KEY_UPDATE_RULE: return "SQL_SQL92_FOREIGN_KEY_UPDATE_RULE";
  case SQL_SQL92_GRANT: return "SQL_SQL92_GRANT";
  case SQL_SQL92_NUMERIC_VALUE_FUNCTIONS: return "SQL_SQL92_NUMERIC_VALUE_FUNCTIONS checked";
  case SQL_SQL92_PREDICATES: return "SQL_SQL92_PREDICATES checked";
  case SQL_SQL92_RELATIONAL_JOIN_OPERATORS: return "SQL_SQL92_RELATIONAL_JOIN_OPERATORS checked";
  case SQL_SQL92_REVOKE: return "SQL_SQL92_REVOKE";
  case SQL_SQL92_ROW_VALUE_CONSTRUCTOR: return "SQL_SQL92_ROW_VALUE_CONSTRUCTOR";
  case SQL_SQL92_STRING_FUNCTIONS: return "SQL_SQL92_STRING_FUNCTIONS checked";
  case SQL_SQL92_VALUE_EXPRESSIONS: return "SQL_SQL92_VALUE_EXPRESSIONS checked";
  }

  return "ERROR un-recongize info type";
}

const char *odbcStmtOptionString(SQLUSMALLINT opt) 
{
  switch (opt) {
  case SQL_ATTR_QUERY_TIMEOUT: return "SQL_ATTR_QUERY_TIMEOUT";
  case SQL_ATTR_CURSOR_TYPE: return "SQL_ATTR_CURSOR_TYPE checked";
  case SQL_ATTR_ROW_NUMBER: return "SQL_ATTR_ROW_NUMBER";
  case SQL_ATTR_ASYNC_ENABLE: return "SQL_ATTR_ASYNC_ENABLE";
  case SQL_ATTR_CONCURRENCY: return "SQL_ATTR_CONCURRENCY";
  case SQL_ATTR_RETRIEVE_DATA: return "SQL_ATTR_RETRIEVE_DATA";
  case SQL_ROWSET_SIZE: return "SQL_ROWSET_SIZE";
  case SQL_ATTR_ROW_ARRAY_SIZE: return "SQL_ATTR_ROW_ARRAY_SIZE";
  case SQL_ATTR_MAX_ROWS: return "SQL_ATTR_MAX_ROWS";
  case SQL_ATTR_MAX_LENGTH: return "SQL_ATTR_MAX_LENGTH";
  }

  return "ERROR un-recongize statement option";
}

const char *odbcFreeStmtOptionString(SQLUSMALLINT opt)
{
  switch (opt)
  {
  case SQL_RESET_PARAMS: return "SQL_RESET_PARAMS";
  case SQL_UNBIND: return "SQL_UNBIND";
  case SQL_CLOSE: return "SQL_CLOSE";
  case SQL_DROP: return "SQL_DROP";
  }

  return "ERROR un-recongize free statement option";
}

const char * odbcConnectOptionString(SQLUSMALLINT opt)
{
  switch (opt)
  {
  case SQL_ACCESS_MODE: return "SQL_ACCESS_MODE";
  case SQL_AUTOCOMMIT: return "SQL_AUTOCOMMIT";
  case SQL_LOGIN_TIMEOUT: return "SQL_LOGIN_TIMEOUT checked";
  case SQL_OPT_TRACE: return "SQL_OPT_TRACE";
  case SQL_OPT_TRACEFILE: return "SQL_OPT_TRACEFILE";
  case SQL_TRANSLATE_DLL: return "SQL_TRANSLATE_DLL";
  case SQL_TRANSLATE_OPTION: return "SQL_TRANSLATE_OPTION";
  case SQL_TXN_ISOLATION: return "SQL_TXN_ISOLATION checked";
  case SQL_CURRENT_QUALIFIER: return "SQL_CURRENT_QUALIFIER";
  case SQL_ODBC_CURSORS: return "SQL_ODBC_CURSORS";
  case SQL_QUIET_MODE: return "SQL_QUIET_MODE checked";
  case SQL_PACKET_SIZE: return "SQL_PACKET_SIZE";
  }

  return "ERROR un-recongize connect option";
}

const char * odbcConnectAttrString(SQLINTEGER attr)
{
  switch (attr)
  {
  case SQL_ACCESS_MODE: return "SQL_ACCESS_MODE";
  case SQL_AUTOCOMMIT: return "SQL_AUTOCOMMIT";
  case SQL_LOGIN_TIMEOUT: return "SQL_LOGIN_TIMEOUT checked";
  case SQL_OPT_TRACE: return "SQL_OPT_TRACE";
  case SQL_OPT_TRACEFILE: return "SQL_OPT_TRACEFILE";
  case SQL_TRANSLATE_DLL: return "SQL_TRANSLATE_DLL";
  case SQL_TRANSLATE_OPTION: return "SQL_TRANSLATE_OPTION";
  case SQL_TXN_ISOLATION: return "SQL_TXN_ISOLATION checked";
  case SQL_CURRENT_QUALIFIER: return "SQL_CURRENT_QUALIFIER";
  case SQL_ODBC_CURSORS: return "SQL_ODBC_CURSORS";
  case SQL_QUIET_MODE: return "SQL_QUIET_MODE checked";
  case SQL_PACKET_SIZE: return "SQL_PACKET_SIZE";
  case SQL_ATTR_CONNECTION_DEAD: return "SQL_ATTR_CONNECTION_DEAD";
  case SQL_ATTR_KEYSET_SIZE: return "SQL_ATTR_KEYSET_SIZE";
  case SQL_ATTR_PARAM_BIND_TYPE: return "SQL_ATTR_PARAM_BIND_TYPE";
  case SQL_ATTR_ROW_BIND_TYPE: return "SQL_ATTR_ROW_BIND_TYPE";
  case SQL_ATTR_ASYNC_ENABLE: return "SQL_ATTR_ASYNC_ENABLE";
  case SQL_ATTR_NOSCAN: return "SQL_ATTR_NOSCAN";
  case SQL_ATTR_CONCURRENCY: return "SQL_ATTR_CONCURRENCY";
  case SQL_ATTR_CURSOR_SENSITIVITY: return "SQL_ATTR_CURSOR_SENSITIVITY";
  case SQL_ATTR_SIMULATE_CURSOR: return "SQL_ATTR_SIMULATE_CURSOR";
  case SQL_ATTR_MAX_ROWS: return "SQL_ATTR_MAX_ROWS";
  case SQL_ATTR_MAX_LENGTH: return "SQL_ATTR_MAX_LENGTH";
  case SQL_ATTR_CURSOR_TYPE: return "SQL_ATTR_CURSOR_TYPE checked";
  case SQL_ATTR_RETRIEVE_DATA: return "SQL_ATTR_RETRIEVE_DATA";
  case SQL_ATTR_METADATA_ID: return "SQL_ATTR_METADATA_ID";
  }

  return "ERROR un-recongized connect attribute";
}

char * odbcDataTypeString(SQLSMALLINT type)
{
  switch (type){
  case SQL_BIT: return "SQL_BIT";
  case SQL_TINYINT: return "SQL_TINYINT";
  case SQL_SMALLINT: return "SQL_SMALLINT";
  case SQL_INTEGER: return "SQL_INTEGER";
  case SQL_BIGINT: return "SQL_BIGINT";
  case SQL_FLOAT: return "SQL_FLOAT";
  case SQL_DOUBLE: return "SQL_DOUBLE";
  case SQL_CHAR: return "SQL_CHAR";
  case SQL_TYPE_TIMESTAMP: return "SQL_TYPE_TIMESTAMP";
  }
  return "ERROR un-recongized odbc data type";
}

const char * odbcDataTypeTDengineString(int type)
{
  switch (type) {
  case TSDB_DATA_TYPE_BOOL: return "TSDB_DATA_TYPE_BOOL";
  case TSDB_DATA_TYPE_TINYINT: return "TSDB_DATA_TYPE_TINYINT";
  case TSDB_DATA_TYPE_SMALLINT: return "TSDB_DATA_TYPE_SMALLINT";
  case TSDB_DATA_TYPE_INT: return "TSDB_DATA_TYPE_INT";
  case TSDB_DATA_TYPE_BIGINT: return "TSDB_DATA_TYPE_BIGINT";
  case TSDB_DATA_TYPE_FLOAT: return "TSDB_DATA_TYPE_FLOAT";
  case TSDB_DATA_TYPE_DOUBLE: return "TSDB_DATA_TYPE_DOUBLE";
  case TSDB_DATA_TYPE_BINARY: return "TSDB_DATA_TYPE_BINARY";
  case TSDB_DATA_TYPE_TIMESTAMP: return "TSDB_DATA_TYPE_TIMESTAMP";
  case TSDB_DATA_TYPE_NCHAR: return "TSDB_DATA_TYPE_NCHAR";
  case TSDB_DATA_TYPE_NULL: return "TSDB_DATA_TYPE_NULL";
  }
  return "Unsupport TDengine Type";
}

const char * odbcReturnCodeString(SQLRETURN code)
{
  switch (code) {
  case SQL_SUCCESS: return "SQL_SUCCESS";
  case SQL_ERROR: return "SQL_ERROR";
  case SQL_SUCCESS_WITH_INFO: return "SQL_SUCCESS_WITH_INFO";
  case SQL_NO_DATA: return "SQL_NO_DATA";
  //case SQL_NO_DATA_FOUND: return "SQL_NO_DATA_FOUND";
  }
  return "ERROR un-recongized return code";
}

const char * odbcDiagFieldIdString(SQLSMALLINT id)
{
  switch (id) {
  case SQL_DIAG_RETURNCODE: return "SQL_DIAG_RETURNCODE";
  case SQL_DIAG_NUMBER: return "SQL_DIAG_NUMBER";
  case SQL_DIAG_ROW_COUNT: return "SQL_DIAG_ROW_COUNT";
  case SQL_DIAG_SQLSTATE: return "SQL_DIAG_SQLSTATE";
  case SQL_DIAG_NATIVE: return "SQL_DIAG_NATIVE";
  case SQL_DIAG_MESSAGE_TEXT: return "SQL_DIAG_MESSAGE_TEXT";
  case SQL_DIAG_DYNAMIC_FUNCTION: return "SQL_DIAG_DYNAMIC_FUNCTION";
  case SQL_DIAG_CLASS_ORIGIN: return "SQL_DIAG_CLASS_ORIGIN";
  case SQL_DIAG_SUBCLASS_ORIGIN: return "SQL_DIAG_SUBCLASS_ORIGIN";
  case SQL_DIAG_CONNECTION_NAME: return "SQL_DIAG_CONNECTION_NAME";
  case SQL_DIAG_SERVER_NAME: return "SQL_DIAG_SERVER_NAME";
  case SQL_DIAG_DYNAMIC_FUNCTION_CODE: return "SQL_DIAG_DYNAMIC_FUNCTION_CODE";
  }
  return "ERROR un-recongized return code";
}

const char * odbcCDataTypeString(SQLSMALLINT type)
{
  switch (type){
  case SQL_C_BIT: return "SQL_C_BIT";
  case SQL_C_STINYINT: return "SQL_C_STINYINT";
  case SQL_C_UTINYINT: return "SQL_C_UTINYINT";
  case SQL_C_SSHORT: return "SQL_C_SSHORT";
  case SQL_C_USHORT: return "SQL_C_USHORT";
  case SQL_C_SLONG: return "SQL_C_SLONG";
  case SQL_C_ULONG: return "SQL_C_ULONG";
  case SQL_C_SBIGINT: return "SQL_C_SBIGINT";
  case SQL_C_UBIGINT: return "SQL_C_UBIGINT";
  case SQL_C_FLOAT: return "SQL_C_FLOAT";
  case SQL_C_DOUBLE: return "SQL_C_DOUBLE";
  case SQL_C_BINARY: return "SQL_C_BINARY";
  case SQL_C_CHAR: return "SQL_C_CHAR";
  case SQL_C_WCHAR: return "SQL_C_WCHAR";
  case SQL_C_TYPE_TIMESTAMP: return "SQL_C_TYPE_TIMESTAMP";
  case SQL_C_DEFAULT: return "SQL_C_DEFAULT";
  }
 
  //  SQL_C_CHAR  SQLCHAR *  unsigned char *
  //  SQL_C_WCHAR  SQLWCHAR *  wchar_t *
  //  SQL_C_SSHORT[j]  SQLSMALLINT  short int
  //  SQL_C_USHORT[j]  SQLUSMALLINT  unsigned short int
  //  SQL_C_SLONG[j]  SQLINTEGER  long int
  //  SQL_C_ULONG[j]  SQLUINTEGER  unsigned long int
  //  SQL_C_FLOAT  SQLREAL  float
  //  SQL_C_DOUBLE  SQLDOUBLE, SQLFLOAT  double
  //  SQL_C_BIT  SQLCHAR  unsigned char
  //  SQL_C_STINYINT[j]  SQLSCHAR  signed char
  //  SQL_C_UTINYINT[j]  SQLCHAR  unsigned char
  //  SQL_C_SBIGINT  SQLBIGINT  _int64[h]
  //  SQL_C_UBIGINT  SQLUBIGINT  unsigned _int64[h]
  //  SQL_C_BINARY  SQLCHAR *  unsigned char *
  //  SQL_C_BOOKMARK[i]  BOOKMARK  unsigned long int[d]
  //  SQL_C_VARBOOKMARK  SQLCHAR *  unsigned char *
  return "ERROR un-recongized odbc c-data type";
}

int odbcTDengineType2SqlCType(int tdType)
{
  switch (tdType) {
  case TSDB_DATA_TYPE_BOOL: return SQL_C_BIT;
  case TSDB_DATA_TYPE_TINYINT: return SQL_C_STINYINT;
  case TSDB_DATA_TYPE_SMALLINT: return SQL_C_SSHORT;
  case TSDB_DATA_TYPE_INT: return SQL_C_SLONG;
  case TSDB_DATA_TYPE_BIGINT: return SQL_C_SBIGINT;
  case TSDB_DATA_TYPE_FLOAT: return SQL_C_FLOAT;
  case TSDB_DATA_TYPE_DOUBLE: return SQL_C_DOUBLE;
  case TSDB_DATA_TYPE_BINARY: return SQL_C_BINARY;
  case TSDB_DATA_TYPE_NCHAR: return SQL_C_BINARY;
  case TSDB_DATA_TYPE_TIMESTAMP: return SQL_C_TYPE_TIMESTAMP;
  case TSDB_DATA_TYPE_NULL: return SQL_C_STINYINT;
  }
  return SQL_UNKNOWN_TYPE;
}

int odbcTDengineType2SqlType(int tdType)
{
  switch (tdType) {
  case TSDB_DATA_TYPE_BOOL: return SQL_BIT;
  case TSDB_DATA_TYPE_TINYINT: return SQL_TINYINT;
  case TSDB_DATA_TYPE_SMALLINT: return SQL_SMALLINT;
  case TSDB_DATA_TYPE_INT: return SQL_INTEGER;
  case TSDB_DATA_TYPE_BIGINT: return SQL_BIGINT;
  case TSDB_DATA_TYPE_FLOAT: return SQL_FLOAT;
  case TSDB_DATA_TYPE_DOUBLE: return SQL_DOUBLE;
  case TSDB_DATA_TYPE_BINARY: return SQL_CHAR;
  case TSDB_DATA_TYPE_NCHAR: return SQL_CHAR;
  case TSDB_DATA_TYPE_TIMESTAMP: return SQL_TYPE_TIMESTAMP;
  }
  return SQL_UNKNOWN_TYPE;
}

int odbcTDengineTypeString2SqlType(const char* tdTypeString)
{
  if (strcasecmp(tdTypeString, "TIMESTAMP") == 0)
    return SQL_TYPE_TIMESTAMP;
  else if (strcasecmp(tdTypeString, "BOOL") == 0)
    return SQL_BIT;
  else if (strcasecmp(tdTypeString, "TINYINT") == 0)
    return SQL_TINYINT;
  else if (strcasecmp(tdTypeString, "SMALLINT") == 0)
    return SQL_SMALLINT;
  else if (strcasecmp(tdTypeString, "INTEGER") == 0)
    return SQL_INTEGER;
  else if (strcasecmp(tdTypeString, "BIGINT") == 0)
    return SQL_BIGINT;
  else if (strcasecmp(tdTypeString, "FLOAT") == 0)
    return SQL_FLOAT;
  else if (strcasecmp(tdTypeString, "DOUBLE") == 0)
    return SQL_DOUBLE;
  else if (strcasecmp(tdTypeString, "BINARY") == 0)
    return SQL_CHAR;
  else if (strcasecmp(tdTypeString, "NCHAR") == 0)
    return SQL_CHAR;

  return SQL_UNKNOWN_TYPE;
}

int odbcTDengineTypeString2TdengineType(const char* tdTypeString)
{
  if (strcasecmp(tdTypeString, "TIMESTAMP") == 0)
    return TSDB_DATA_TYPE_TIMESTAMP;
  else if (strcasecmp(tdTypeString, "BOOL") == 0)
    return TSDB_DATA_TYPE_BOOL;
  else if (strcasecmp(tdTypeString, "TINYINT") == 0)
    return TSDB_DATA_TYPE_TINYINT;
  else if (strcasecmp(tdTypeString, "SMALLINT") == 0)
    return TSDB_DATA_TYPE_SMALLINT;
  else if (strcasecmp(tdTypeString, "INTEGER") == 0)
    return TSDB_DATA_TYPE_INT;
  else if (strcasecmp(tdTypeString, "BIGINT") == 0)
    return TSDB_DATA_TYPE_BIGINT;
  else if (strcasecmp(tdTypeString, "FLOAT") == 0)
    return TSDB_DATA_TYPE_FLOAT;
  else if (strcasecmp(tdTypeString, "DOUBLE") == 0)
    return TSDB_DATA_TYPE_DOUBLE;
  else if (strcasecmp(tdTypeString, "BINARY") == 0)
    return TSDB_DATA_TYPE_BINARY;
  else if (strcasecmp(tdTypeString, "NCHAR") == 0)
    return TSDB_DATA_TYPE_NCHAR;

  return TSDB_DATA_TYPE_NULL;
}


//for SQLColumns
int odbcColumnSizeOfTDengineType(int tdType)
{
  switch (tdType) {
  case TSDB_DATA_TYPE_BOOL:
    return 1;
  case TSDB_DATA_TYPE_TINYINT:
    return 3;
  case TSDB_DATA_TYPE_SMALLINT:
    return 5;
  case TSDB_DATA_TYPE_INT:
    return 10;
  case TSDB_DATA_TYPE_BIGINT:
    return 19;
  case TSDB_DATA_TYPE_FLOAT:
    return 15;
  case TSDB_DATA_TYPE_DOUBLE:
    return 38;
  case TSDB_DATA_TYPE_BINARY:
  case TSDB_DATA_TYPE_NCHAR:
    return 0;
  case TSDB_DATA_TYPE_TIMESTAMP:
    return 19;
  }
  return 0;
}

//for SQLColumns
int odbcDecimalDigitsOfTDengineType(int tdType)
{
  switch (tdType) {
  case TSDB_DATA_TYPE_BOOL:
    return 0;
  case TSDB_DATA_TYPE_TINYINT:
    return 0;
  case TSDB_DATA_TYPE_SMALLINT:
    return 0;
  case TSDB_DATA_TYPE_INT:
    return 0;
  case TSDB_DATA_TYPE_BIGINT:
    return 0;
  case TSDB_DATA_TYPE_FLOAT:
    return 15;
  case TSDB_DATA_TYPE_DOUBLE:
    return 15;
  case TSDB_DATA_TYPE_BINARY:
  case TSDB_DATA_TYPE_NCHAR:
    return 0;
  case TSDB_DATA_TYPE_TIMESTAMP:
    return 0;
  }
  return 0;
}

//for SQLColumns
int odbcNumPrecRadixOfTDengineType(int tdType)
{
  switch (tdType) {
  case TSDB_DATA_TYPE_BOOL:
  case TSDB_DATA_TYPE_TINYINT:
  case TSDB_DATA_TYPE_SMALLINT:
  case TSDB_DATA_TYPE_INT:
  case TSDB_DATA_TYPE_BIGINT:
  case TSDB_DATA_TYPE_FLOAT:
  case TSDB_DATA_TYPE_DOUBLE:
    return 10;
  case TSDB_DATA_TYPE_BINARY:
  case TSDB_DATA_TYPE_NCHAR:
    return 0;
  case TSDB_DATA_TYPE_TIMESTAMP:
    return 10;
  }
  return 0;
}

const char * odbcDriverCompleteString(SQLUSMALLINT drvcompl)
{
  switch (drvcompl){
  case SQL_DRIVER_COMPLETE: return "SQL_DRIVER_COMPLETE";
  case SQL_DRIVER_COMPLETE_REQUIRED: return "SQL_DRIVER_COMPLETE_REQUIRED";
  case SQL_DRIVER_PROMPT: return "SQL_DRIVER_PROMPT";
  case SQL_DRIVER_NOPROMPT: return "SQL_DRIVER_NOPROMPT";
  }
  return "ERROR un-recongized driver complete type";
}

const char * odbcStmtAttrString(SQLINTEGER attr)
{
  switch (attr)
  {
  case SQL_ATTR_QUERY_TIMEOUT: return "SQL_ATTR_QUERY_TIMEOUT";
  case SQL_ATTR_CURSOR_TYPE: return "SQL_ATTR_CURSOR_TYPE";
  case SQL_ATTR_CURSOR_SCROLLABLE: return "SQL_ATTR_CURSOR_SCROLLABLE";
  case SQL_ATTR_CURSOR_SENSITIVITY: return "SQL_ATTR_CURSOR_SENSITIVITY";
  case SQL_ATTR_ROW_NUMBER: return "SQL_ATTR_ROW_NUMBER";
  case SQL_ATTR_ASYNC_ENABLE: return "SQL_ATTR_ASYNC_ENABLE";
  case SQL_ATTR_CONCURRENCY: return "SQL_ATTR_CONCURRENCY";
  case SQL_ATTR_RETRIEVE_DATA: return "SQL_ATTR_RETRIEVE_DATA";
  case SQL_ROWSET_SIZE: return "SQL_ROWSET_SIZE";
  case SQL_ATTR_ROW_ARRAY_SIZE: return "SQL_ATTR_ROW_ARRAY_SIZE";
  case SQL_ATTR_IMP_ROW_DESC: return "SQL_ATTR_IMP_ROW_DESC checked";
  case SQL_ATTR_APP_ROW_DESC: return "SQL_ATTR_APP_ROW_DESC checked";
  case SQL_ATTR_IMP_PARAM_DESC: return "SQL_ATTR_IMP_PARAM_DESC checked";
  case SQL_ATTR_APP_PARAM_DESC: return "SQL_ATTR_APP_PARAM_DESC checked";
  case SQL_ATTR_ROW_STATUS_PTR: return "SQL_ATTR_ROW_STATUS_PTR checked";
  case SQL_ATTR_ROWS_FETCHED_PTR: return "SQL_ATTR_ROWS_FETCHED_PTR checked";
  case SQL_ATTR_USE_BOOKMARKS: return "SQL_ATTR_USE_BOOKMARKS";
  case SQL_ATTR_FETCH_BOOKMARK_PTR: return "SQL_ATTR_FETCH_BOOKMARK_PTR";
  case SQL_ATTR_PARAM_BIND_OFFSET_PTR: return "SQL_ATTR_PARAM_BIND_OFFSET_PTR";
  case SQL_ATTR_PARAM_BIND_TYPE: return "SQL_ATTR_PARAM_BIND_TYPE";
  case SQL_ATTR_PARAM_OPERATION_PTR: return "SQL_ATTR_PARAM_OPERATION_PTR";
  case SQL_ATTR_PARAM_STATUS_PTR: return "SQL_ATTR_PARAM_STATUS_PTR";
  case SQL_ATTR_PARAMS_PROCESSED_PTR: return "SQL_ATTR_PARAMS_PROCESSED_PTR";
  case SQL_ATTR_PARAMSET_SIZE: return "SQL_ATTR_PARAMSET_SIZE";
  case SQL_ATTR_ROW_BIND_TYPE: return "SQL_ATTR_ROW_BIND_TYPE";
  case SQL_ATTR_ROW_BIND_OFFSET_PTR: return "SQL_ATTR_ROW_BIND_OFFSET_PTR";
  case SQL_ATTR_MAX_ROWS: return "SQL_ATTR_MAX_ROWS";
  case SQL_ATTR_MAX_LENGTH: return "SQL_ATTR_MAX_LENGTH";
  case SQL_ATTR_METADATA_ID: return "SQL_ATTR_METADATA_ID";
  }

  return "ERROR un-recongized statement attribute";
}

const char * odbcFunctionString(SQLUSMALLINT func)
{
  switch (func)
  {
  case SQL_API_ODBC3_ALL_FUNCTIONS: return "SQL_API_ODBC3_ALL_FUNCTIONS checked";
  case SQL_API_ALL_FUNCTIONS: return "SQL_API_ALL_FUNCTIONS checked";
  }

  return "ERROR un-recongized function type";
}

#if defined(_WIN32) || defined(_WIN64)
  const char * odbcAttachMsgName(DWORD reason)
  {
    switch (reason) {
    case DLL_PROCESS_ATTACH: return "process_attach";
    case DLL_THREAD_ATTACH: return "thread_attach";
    case DLL_PROCESS_DETACH: return "process_detach";
    case DLL_THREAD_DETACH: return "thread_detach";
    }
    return "ERROR un-recongized dll attach reason";
  }
#endif

const char * odbcConfigDsnType(WORD request)
{
  switch (request) {
  case ODBC_REMOVE_DSN: return "ODBC_REMOVE_DSN";
  case ODBC_ADD_DSN: return "ODBC_ADD_DSN";
  case ODBC_CONFIG_DSN: return "ODBC_CONFIG_DSN";
  }
  return "ERROR un-recongized config dsn request";
}

const char * odbcEnvAttrString(SQLINTEGER attr)
{
  switch (attr) {
  case SQL_ATTR_CONNECTION_POOLING: return "SQL_ATTR_CONNECTION_POOLING";
  case SQL_ATTR_CP_MATCH: return "SQL_ATTR_CP_MATCH";
  case SQL_ATTR_OUTPUT_NTS: return "SQL_ATTR_OUTPUT_NTS";
  case SQL_ATTR_ODBC_VERSION: return "SQL_ATTR_ODBC_VERSION checked";
  }
  return "ERROR un-recongized environment attribute";
}

const char * odbcStmtSqlType(int type)
{
  switch ((STMT_TYPE)type) {
  case STMT_NORMAL_SQL: return "STMT_NORMAL_SQL";
  case STMT_DESCRIBE_COLUMNS_SQL: return "STMT_DESCRIBE_COLUMNS_SQL";
  case STMT_SHOW_DATABASE_SQL: return "STMT_SHOW_DATABASE_SQL";
  case STMT_SHOW_SCHEMA_SQL: return "STMT_SHOW_SCHEMA_SQL";
  case STMT_SHOW_TABLES_TYPE_SQL: return "STMT_SHOW_TABLES_TYPE_SQL";
  case STMT_SHOW_TABLES_SQL: return "STMT_SHOW_TABLES_SQL";
  case STMT_SHOW_STABLES_SQL: return "STMT_SHOW_STABLES_SQL";
  case STMT_PRIMARY_KEY_SQL: return "STMT_PRIMARY_KEY_SQL";
  case STMT_FOERIGN_KEY_SQL: return "STMT_FOERIGN_KEY_SQL";
  }
  return "ERROR un-recongized statement type";
}

const char * odbcSqlTypeinfoString(SQLSMALLINT sqltype)
{
  return "ERROR un-recongized sql type info";
}

const char * odbcColAttrString(SQLUSMALLINT id)
{
  switch (id) {
  case SQL_COLUMN_NULLABLE: return "SQL_COLUMN_NULLABLE checked";
  case SQL_COLUMN_COUNT: return "SQL_COLUMN_COUNT checked";
  case SQL_COLUMN_NAME: return "SQL_COLUMN_NAME checked";
  case SQL_DESC_AUTO_UNIQUE_VALUE: return "SQL_DESC_AUTO_UNIQUE_VALUE checked"; //SQL_COLUMN_AUTO_INCREMENT
  case SQL_DESC_BASE_COLUMN_NAME: return "SQL_DESC_BASE_COLUMN_NAME checked";
  case SQL_DESC_BASE_TABLE_NAME: return "SQL_DESC_BASE_TABLE_NAME checked";
  case SQL_DESC_CASE_SENSITIVE: return "SQL_DESC_CASE_SENSITIVE checked";//SQL_COLUMN_CASE_SENSITIVE
  case SQL_DESC_CATALOG_NAME: return "SQL_DESC_CATALOG_NAME checked";    //SQL_COLUMN_QUALIFIER_NAME
  case SQL_DESC_CONCISE_TYPE: return "SQL_DESC_CONCISE_TYPE checked";    //SQL_COLUMN_TYPE
  case SQL_DESC_COUNT: return "SQL_DESC_COUNT checked";
  case SQL_DESC_DISPLAY_SIZE: return "SQL_DESC_DISPLAY_SIZE checked";    //SQL_COLUMN_DISPLAY_SIZE
  case SQL_DESC_FIXED_PREC_SCALE: return "SQL_DESC_FIXED_PREC_SCALE checked";   //SQL_COLUMN_MONEY
  case SQL_DESC_LABEL: return "SQL_DESC_LABEL checked";                  //SQL_COLUMN_LABEL
  case SQL_DESC_LENGTH: return "SQL_DESC_LENGTH checked";
  case SQL_DESC_LITERAL_PREFIX: return "SQL_DESC_LITERAL_PREFIX";
  case SQL_DESC_LITERAL_SUFFIX: return "SQL_DESC_LITERAL_SUFFIX";
  case SQL_DESC_LOCAL_TYPE_NAME: return "SQL_DESC_LOCAL_TYPE_NAME";
  case SQL_DESC_NAME: return "SQL_DESC_NAME checked";
  case SQL_DESC_NULLABLE: return "SQL_DESC_NULLABLE checked";
  case SQL_DESC_NUM_PREC_RADIX: return "SQL_DESC_NUM_PREC_RADIX checked";
  case SQL_DESC_OCTET_LENGTH: return "SQL_DESC_OCTET_LENGTH checked";
  case SQL_DESC_PRECISION: return "SQL_DESC_PRECISION checked";
  case SQL_DESC_SCALE: return "SQL_DESC_SCALE checked";
  case SQL_DESC_SCHEMA_NAME: return "SQL_DESC_SCHEMA_NAME checked";   //SQL_COLUMN_OWNER_NAME
  case SQL_DESC_SEARCHABLE: return "SQL_DESC_SEARCHABLE checked";    //SQL_COLUMN_SEARCHABLE
  case SQL_DESC_TABLE_NAME: return "SQL_DESC_TABLE_NAME checked";     //SQL_COLUMN_TABLE_NAME
  case SQL_DESC_TYPE: return "SQL_DESC_TYPE checked";
  case SQL_DESC_TYPE_NAME: return "SQL_DESC_TYPE_NAME checked";       //SQL_COLUMN_TYPE_NAME
  case SQL_DESC_UNNAMED: return "SQL_DESC_UNNAMED ";
  case SQL_DESC_UNSIGNED: return "SQL_DESC_UNSIGNED checked";         //SQL_COLUMN_UNSIGNED
  case SQL_DESC_UPDATABLE: return "SQL_DESC_UPDATABLE checked";       //SQL_COLUMN_UPDATABLE
  }
  return "ERROR un-recongized column attribute";
}

void * xmalloc(size_t size)
{
  void *alloc = taosMemoryMalloc(size);
  if (alloc == 0)
  {
    odbcError("out of memory (malloc)");
    exit(EXIT_FAILURE);
  }

  return alloc;
}

void xfree(void *p) {
  taosMemoryFree(p);
}

/**
* Set error message and SQL state on DBC
* @param d database connection pointer
* @param naterr native error code
* @param msg error message
* @param st SQL state
*/
#if defined(__GNUC__) && (__GNUC__ >= 2)
void setstatd(DBC *, int, char *, char *, ...)
__attribute__((format(printf, 3, 5)));
#endif

void setstatd(DBC *d, int naterr, char *msg, char *st, ...)
{
  va_list ap;

  if (!d) {
    return;
  }
  d->naterr = naterr;
  d->logmsg[0] = '\0';
  if (msg) {
    int count;

    va_start(ap, st);
    count = vsnprintf((char *)d->logmsg, sizeof(d->logmsg), msg, ap);
    va_end(ap);
    if (count < 0) {
      d->logmsg[sizeof(d->logmsg) - 1] = '\0';
    }
  }
  if (!st) {
    st = "?????";
  }
  strncpy(d->sqlstate, st, 5);
  d->sqlstate[5] = '\0';
}

/**
* Set error message and SQL state on statement
* @param s statement pointer
* @param naterr native error code
* @param msg error message
* @param st SQL state
*/

#if defined(__GNUC__) && (__GNUC__ >= 2)
void setstat(STMT *, int, char *, char *, ...)
__attribute__((format(printf, 3, 5)));
#endif

void setstat(STMT *s, int naterr, char *msg, char *st, ...)
{
  va_list ap;

  if (!s) {
    return;
  }
  s->dbc->naterr = naterr;
  s->dbc->logmsg[0] = '\0';
  if (msg) {
    int count;

    va_start(ap, st);
    count = vsnprintf((char *)s->dbc->logmsg, sizeof(s->dbc->logmsg), msg, ap);
    va_end(ap);
    if (count < 0) {
      s->dbc->logmsg[sizeof(s->dbc->logmsg) - 1] = '\0';
    }
  }
  if (!st) {
    st = "?????";
  }
  strncpy(s->dbc->sqlstate, st, 5);
  s->dbc->sqlstate[5] = '\0';
}


/**
* Return length of UNICODE string.
* @param str UNICODE string
* @result length of string in characters
*/

int uc_strlen(SQLWCHAR *str)
{
  int len = 0;

  if (str) {
    while (*str) {
      ++len;
      ++str;
    }
  }
  return len;
}

/**
* Copy UNICODE string like strncpy().
* @param dest destination area
* @param src source area
* @param len length of source area in characters
* @return pointer to destination area
*/

SQLWCHAR * uc_strncpy(SQLWCHAR *dest, SQLWCHAR *src, int len)
{
  int i = 0;

  while (i < len) {
    if (!src[i]) {
      break;
    }
    dest[i] = src[i];
    ++i;
  }
  if (i < len) {
    dest[i] = 0;
  }
  return dest;
}


/**
* Make UNICODE string from UTF8 string into buffer.
* @param str UTF8 string to be converted
* @param len length in characters of str or -1
* @param uc destination area to receive UNICODE string
* @param ucLen byte length of destination area
*/
void uc_from_utf_buf(unsigned char *str, int len, SQLWCHAR *uc, int ucLen)
{
  ucLen = ucLen / sizeof(SQLWCHAR);
  if (!uc || ucLen < 0) {
    return;
  }
  if (len < 0) {
    len = ucLen * 5;
  }
  uc[0] = 0;
  if (str) {
    int i = 0;

    while (i < len && *str && i < ucLen) {
      unsigned char c = str[0];

      if (c < 0x80) {
        uc[i++] = c;
        ++str;
      }
      else if (c <= 0xc1 || c >= 0xf5) {
        /* illegal, ignored */
        ++str;
      }
      else if (c < 0xe0) {
        if ((str[1] & 0xc0) == 0x80) {
          unsigned long t = ((c & 0x1f) << 6) | (str[1] & 0x3f);

          uc[i++] = (SQLWCHAR)t;
          str += 2;
        }
        else {
          uc[i++] = c;
          ++str;
        }
      }
      else if (c < 0xf0) {
        if ((str[1] & 0xc0) == 0x80 && (str[2] & 0xc0) == 0x80) {
          unsigned long t = ((c & 0x0f) << 12) |
            ((str[1] & 0x3f) << 6) | (str[2] & 0x3f);

          uc[i++] = (SQLWCHAR)t;
          str += 3;
        }
        else {
          uc[i++] = c;
          ++str;
        }
      }
      else if (c < 0xf8) {
        if ((str[1] & 0xc0) == 0x80 && (str[2] & 0xc0) == 0x80 &&
          (str[3] & 0xc0) == 0x80) {
          unsigned long t = ((c & 0x03) << 18) |
            ((str[1] & 0x3f) << 12) | ((str[2] & 0x3f) << 6) |
            (str[3] & 0x3f);

          if (sizeof(SQLWCHAR) == 2 * sizeof(char) &&
            t >= 0x10000) {
            t -= 0x10000;
            uc[i++] = 0xd800 | ((t >> 10) & 0x3ff);
            if (i >= ucLen) {
              break;
            }
            t = 0xdc00 | (t & 0x3ff);
          }
          uc[i++] = (SQLWCHAR)t;
          str += 4;
        }
        else {
          uc[i++] = c;
          ++str;
        }
      }
      else {
        /* ignore */
        ++str;
      }
    }
    if (i < ucLen) {
      uc[i] = 0;
    }
  }
}

/**
* Make UNICODE string from UTF8 string.
* @param str UTF8 string to be converted
* @param len length of UTF8 string
* @return alloc'ed UNICODE string to be free'd by uc_free()
*/
SQLWCHAR * uc_from_utf(unsigned char *str, int len)
{
  SQLWCHAR *uc = NULL;
  int ucLen;

  if (str) {
    if (len == SQL_NTS) {
      len = (int)strlen((char *)str);
    }
    ucLen = sizeof(SQLWCHAR) * (len + 1);
    uc = xmalloc(ucLen);
    if (uc) {
      uc_from_utf_buf(str, len, uc, ucLen);
    }
  }

  return uc;
}

/**
* Make UTF8 string from UNICODE string.
* @param str UNICODE string to be converted
* @param len length of UNICODE string in bytes
* @return alloc'ed UTF8 string to be free'd by uc_free()
*/

char * uc_to_utf(SQLWCHAR *str, int len)
{
  int i;
  char *cp, *ret = NULL;

  if (!str) {
    return ret;
  }
  if (len == SQL_NTS) {
    len = uc_strlen(str);
  }
  else {
    len = len / sizeof(SQLWCHAR);
  }
  cp = xmalloc(len * 6 + 1);
  if (!cp) {
    return ret;
  }
  ret = cp;
  for (i = 0; i < len; i++) {
    unsigned long c = str[i];

    if (sizeof(SQLWCHAR) == 2 * sizeof(char)) {
      c &= 0xffff;
    }
    if (c < 0x80) {
      *cp++ = (char)c;
    }
    else if (c < 0x800) {
      *cp++ = 0xc0 | ((c >> 6) & 0x1f);
      *cp++ = 0x80 | (c & 0x3f);
    }
    else if (c < 0x10000) {
      if (sizeof(SQLWCHAR) == 2 * sizeof(char) &&
        c >= 0xd800 && c <= 0xdbff && i + 1 < len) {
        unsigned long c2 = str[i + 1] & 0xffff;

        if (c2 >= 0xdc00 && c2 <= 0xdfff) {
          c = (((c & 0x3ff) << 10) | (c2 & 0x3ff)) + 0x10000;
          *cp++ = 0xf0 | ((c >> 18) & 0x07);
          *cp++ = 0x80 | ((c >> 12) & 0x3f);
          *cp++ = 0x80 | ((c >> 6) & 0x3f);
          *cp++ = 0x80 | (c & 0x3f);
          ++i;
          continue;
        }
      }
      *cp++ = 0xe0 | ((c >> 12) & 0x0f);
      *cp++ = 0x80 | ((c >> 6) & 0x3f);
      *cp++ = 0x80 | (c & 0x3f);
    }
    else if (c <= 0x10ffff) {
      *cp++ = 0xf0 | ((c >> 18) & 0x07);
      *cp++ = 0x80 | ((c >> 12) & 0x3f);
      *cp++ = 0x80 | ((c >> 6) & 0x3f);
      *cp++ = 0x80 | (c & 0x3f);
    }
  }
  *cp = '\0';
  return ret;
}

/**
* Make UTF8 string from UNICODE string.
* @param str UNICODE string to be converted
* @param len length of UNICODE string in characters
* @return alloc'ed UTF8 string to be free'd by uc_free()
*/
char * uc_to_utf_c(SQLWCHAR *str, int len)
{
  if (len != SQL_NTS) {
    len = (int)((size_t)len * sizeof(SQLWCHAR));
  }
  return uc_to_utf(str, len);
}

/**
* Free converted UTF8 or UNICODE string.
* @param str string to be free'd
*/
void uc_free(void *str)
{
  if (str) {
    xfree(str);
  }
}
