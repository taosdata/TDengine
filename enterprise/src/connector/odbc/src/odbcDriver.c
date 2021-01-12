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

#include "odbcDriver.h"
#include "odbcUtil.h"
#include "odbcSetup.h"

SQLRETURN
odbcExecuteSql(STMT *s);

/**
 * Report IM001 (not implemented) SQL error code for HDBC.
 * @param dbc database connection handle
 * @result ODBC error code
 */

static SQLRETURN
odbcUnImplDbc(HDBC dbc)
{
  DBC *d;

  if (dbc == SQL_NULL_HDBC) {
    return SQL_INVALID_HANDLE;
  }
  d = (DBC *)dbc;
  setstatd(d, -1, "not supported", "IM001");
  return SQL_ERROR;
}

/**
 * Report IM001 (not implemented) SQL error code for HSTMT.
 * @param stmt statement handle
 * @result ODBC error code
 */

static SQLRETURN
odbcUnImplStmt(HSTMT stmt)
{
  STMT *s;

  if (stmt == SQL_NULL_HSTMT) {
    return SQL_INVALID_HANDLE;
  }
  s = (STMT *)stmt;
  setstat(s, -1, "not supported", "IM001");
  return SQL_ERROR;
}

/**
 * Report IM001 (not implemented) SQL error code for HANDLE.
 * @param stmt statement handle
 * @result ODBC error code
 */

static SQLRETURN
odbcUnImplHandle(SQLSMALLINT type, SQLHANDLE handle)
{
  if (type == SQL_HANDLE_DBC) {
    return odbcUnImplDbc(handle);
  }
  else if (type == SQL_HANDLE_STMT) {
    return odbcUnImplStmt(handle);
  }
  else {
    return SQL_ERROR;
  }
}

/**
 * Free memory given pointer to memory pointer.
 * @param x pointer to pointer to memory to be free'd
 */

//static void
//freep(void *x)
//{
//  if (x && ((char **)x)[0]) {
//    xfree(((char **)x)[0]);
//    ((char **)x)[0] = NULL;
//  }
//}

/**
 * Report S1000 (out of memory) SQL error given STMT.
 * @param s statement pointer
 * @result ODBC error code
 */

static SQLRETURN
nomem(STMT *s)
{
  setstat(s, -1, "out of memory", "HY000");
  return SQL_ERROR;
}

/**
 * Report S1000 (not connected) SQL error given STMT.
 * @param s statement pointer
 * @result ODBC error code
 */

//static SQLRETURN
//noconn(STMT *s)
//{
//  setstat(s, -1, "not connected", "HY000");
//  return SQL_ERROR;
//}

/**
 * Function not implemented.
 */

SQLRETURN SQL_API
SQLDataSources(SQLHENV env, SQLUSMALLINT dir, SQLCHAR *srvname,
  SQLSMALLINT buflen1, SQLSMALLINT *lenp1,
  SQLCHAR *desc, SQLSMALLINT buflen2, SQLSMALLINT *lenp2)
{
  odbcError("SQLDataSources not implemented, env:%p", env);
  if (env == SQL_NULL_HENV) {
    return SQL_INVALID_HANDLE;
  }
  return SQL_ERROR;
}

/**
 * Function not implemented.
 */

SQLRETURN SQL_API
SQLDataSourcesW(SQLHENV env, SQLUSMALLINT dir, SQLWCHAR *srvname,
  SQLSMALLINT buflen1, SQLSMALLINT *lenp1,
  SQLWCHAR *desc, SQLSMALLINT buflen2, SQLSMALLINT *lenp2)
{
  odbcError("SQLDataSourcesW not implemented, env:%p", env);
  if (env == SQL_NULL_HENV) {
    return SQL_INVALID_HANDLE;
  }
  return SQL_ERROR;
}

/**
 * Function not implemented.
 */

SQLRETURN SQL_API
SQLDrivers(SQLHENV env, SQLUSMALLINT dir, SQLCHAR *drvdesc,
  SQLSMALLINT descmax, SQLSMALLINT *desclenp,
  SQLCHAR *drvattr, SQLSMALLINT attrmax, SQLSMALLINT *attrlenp)
{
  odbcError("SQLDrivers not implemented, env:%p", env);
  if (env == SQL_NULL_HENV) {
    return SQL_INVALID_HANDLE;
  }
  return SQL_ERROR;
}

/**
 * Function not implemented.
 */

SQLRETURN SQL_API
SQLDriversW(SQLHENV env, SQLUSMALLINT dir, SQLWCHAR *drvdesc,
  SQLSMALLINT descmax, SQLSMALLINT *desclenp,
  SQLWCHAR *drvattr, SQLSMALLINT attrmax, SQLSMALLINT *attrlenp)
{
  odbcError("SQLDriversW not implemented, env:%p", env);
  if (env == SQL_NULL_HENV) {
    return SQL_INVALID_HANDLE;
  }
  return SQL_ERROR;
}

/**
 * Function not implemented.
 */

SQLRETURN SQL_API
SQLBrowseConnect(SQLHDBC dbc, SQLCHAR *connin, SQLSMALLINT conninLen,
  SQLCHAR *connout, SQLSMALLINT connoutMax,
  SQLSMALLINT *connoutLen)
{
  odbcError("SQLBrowseConnect not implemented, dbc:%p", dbc);
  SQLRETURN ret = SQL_SUCCESS;

  HDBC_LOCK(dbc);
  ret = odbcUnImplDbc(dbc);
  HDBC_UNLOCK(dbc);
  return ret;
}

/**
 * Function not implemented.
 */

SQLRETURN SQL_API
SQLBrowseConnectW(SQLHDBC dbc, SQLWCHAR *connin, SQLSMALLINT conninLen,
  SQLWCHAR *connout, SQLSMALLINT connoutMax,
  SQLSMALLINT *connoutLen)
{
  odbcError("SQLBrowseConnectW not implemented, dbc:%p", dbc);
  SQLRETURN ret = SQL_SUCCESS;

  HDBC_LOCK(dbc);
  ret = odbcUnImplDbc(dbc);
  HDBC_UNLOCK(dbc);
  return ret;
}

/**
 * Put (partial) parameter data into executing statement.
 * @param stmt statement handle
 * @param data pointer to data
 * @param len length of data
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLPutData(SQLHSTMT stmt, SQLPOINTER data, SQLLEN len)
{
  odbcError("SQLPutData not implemented, stmt:%p", stmt);
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcUnImplStmt(stmt);
  HSTMT_UNLOCK(stmt);
  return ret;
}

/**
 * Bind parameter on HSTMT.
 * @param stmt statement handle
 * @param pnum parameter number, starting at 1
 * @param iotype input/output type of parameter
 * @param buftype type of host variable
 * @param ptype
 * @param coldef
 * @param scale
 * @param data pointer to host variable
 * @param buflen length of host variable
 * @param len output length pointer
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLBindParameter(SQLHSTMT stmt, SQLUSMALLINT pnum, SQLSMALLINT iotype,
  SQLSMALLINT buftype, SQLSMALLINT ptype, SQLULEN coldef,
  SQLSMALLINT scale,
  SQLPOINTER data, SQLLEN buflen, SQLLEN *len)
{
  odbcError("SQLBindParameter not implemented, stmt:%p", stmt);
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcUnImplStmt(stmt);
  HSTMT_UNLOCK(stmt);
  return ret;
}

/**
 * Return number of parameters.
 * @param stmt statement handle
 * @param nparam output parameter count
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLNumParams(SQLHSTMT stmt, SQLSMALLINT *nparam)
{
  odbcError("SQLNumParams not implemented, stmt:%p", stmt);
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcUnImplStmt(stmt);
  HSTMT_UNLOCK(stmt);
  return ret;
}

/**
 * Retrieve next parameter for sending data to executing query.
 * @param stmt statement handle
 * @param pind pointer to output parameter indicator
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLParamData(SQLHSTMT stmt, SQLPOINTER *pind)
{
  odbcError("SQLParamData not implemented, stmt:%p", stmt);
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcUnImplStmt(stmt);
  HSTMT_UNLOCK(stmt);
  return ret;
}

/**
 * Return information about parameter.
 * @param stmt statement handle
 * @param pnum parameter number, starting at 1
 * @param dtype output type indicator
 * @param size output size indicator
 * @param decdigits output number of digits
 * @param nullable output NULL allowed indicator
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLDescribeParam(SQLHSTMT stmt, SQLUSMALLINT pnum, SQLSMALLINT *dtype,
  SQLULEN *size, SQLSMALLINT *decdigits, SQLSMALLINT *nullable)
{
  odbcError("SQLDescribeParam not implemented, stmt:%p", stmt);
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcUnImplStmt(stmt);
  HSTMT_UNLOCK(stmt);
  return ret;
}

/**
 * Set information on parameter.
 * @param stmt statement handle
 * @param par parameter number, starting at 1
 * @param type type of host variable
 * @param sqltype
 * @param coldef
 * @param scale
 * @param val pointer to host variable
 * @param nval output length pointer
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLSetParam(SQLHSTMT stmt, SQLUSMALLINT par, SQLSMALLINT type,
  SQLSMALLINT sqltype, SQLULEN coldef,
  SQLSMALLINT scale, SQLPOINTER val, SQLLEN *nval)
{
  odbcError("SQLSetParam not implemented, stmt:%p", stmt);
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcUnImplStmt(stmt);
  HSTMT_UNLOCK(stmt);
  return ret;
}

/**
 * Function not implemented.
 */

SQLRETURN SQL_API
SQLParamOptions(SQLHSTMT stmt, SQLULEN rows, SQLULEN *rowp)
{
  odbcError("SQLParamOptions not implemented, stmt:%p", stmt);
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcUnImplStmt(stmt);
  HSTMT_UNLOCK(stmt);
  return ret;
}

/**
 * Function not implemented.
 */

SQLRETURN SQL_API
SQLGetDescField(SQLHDESC handle, SQLSMALLINT recno,
  SQLSMALLINT fieldid, SQLPOINTER value,
  SQLINTEGER buflen, SQLINTEGER *strlen)
{
  odbcError("SQLGetDescField not implemented, handle:%p", handle);
  return SQL_ERROR;
}

/**
 * Function not implemented.
 */

SQLRETURN SQL_API
SQLGetDescFieldW(SQLHDESC handle, SQLSMALLINT recno,
  SQLSMALLINT fieldid, SQLPOINTER value,
  SQLINTEGER buflen, SQLINTEGER *strlen)
{
  odbcError("SQLGetDescFieldW not implemented, handle:%p", handle);
  return SQL_ERROR;
}

/**
 * Function not implemented.
 */

SQLRETURN SQL_API
SQLSetDescField(SQLHDESC handle, SQLSMALLINT recno,
  SQLSMALLINT fieldid, SQLPOINTER value,
  SQLINTEGER buflen)
{
  odbcError("SQLSetDescField not implemented, handle:%p", handle);
  return SQL_ERROR;
}

/**
 * Function not implemented.
 */

SQLRETURN SQL_API
SQLSetDescFieldW(SQLHDESC handle, SQLSMALLINT recno,
  SQLSMALLINT fieldid, SQLPOINTER value,
  SQLINTEGER buflen)
{
  odbcError("SQLSetDescFieldW not implemented, handle:%p", handle);
  return SQL_ERROR;
}

/**
 * Function not implemented.
 */

SQLRETURN SQL_API
SQLGetDescRec(SQLHDESC handle, SQLSMALLINT recno,
  SQLCHAR *name, SQLSMALLINT buflen,
  SQLSMALLINT *strlen, SQLSMALLINT *type,
  SQLSMALLINT *subtype, SQLLEN *len,
  SQLSMALLINT *prec, SQLSMALLINT *scale,
  SQLSMALLINT *nullable)
{
  odbcError("SQLGetDescRec not implemented, handle:%p", handle);
  return SQL_ERROR;
}

/**
 * Function not implemented.
 */
SQLRETURN SQL_API
SQLGetDescRecW(SQLHDESC handle, SQLSMALLINT recno,
  SQLWCHAR *name, SQLSMALLINT buflen,
  SQLSMALLINT *strlen, SQLSMALLINT *type,
  SQLSMALLINT *subtype, SQLLEN *len,
  SQLSMALLINT *prec, SQLSMALLINT *scale,
  SQLSMALLINT *nullable)
{
  odbcError("SQLGetDescRecW not implemented, handle:%p", handle);
  return SQL_ERROR;
}

/**
 * Function not implemented.
 */

SQLRETURN SQL_API
SQLSetDescRec(SQLHDESC handle, SQLSMALLINT recno,
  SQLSMALLINT type, SQLSMALLINT subtype,
  SQLLEN len, SQLSMALLINT prec,
  SQLSMALLINT scale, SQLPOINTER data,
  SQLLEN *strlen, SQLLEN *indicator)
{
  odbcError("SQLSetDescRec not implemented, handle:%p", handle);
  return SQL_ERROR;
}

/**
 * Retrieve privileges on tables and/or views.
 * @param stmt statement handle
 * @param catalog catalog name/pattern or NULL
 * @param catalogLen length of catalog name/pattern or SQL_NTS
 * @param schema schema name/pattern or NULL
 * @param schemaLen length of schema name/pattern or SQL_NTS
 * @param table table name/pattern or NULL
 * @param tableLen length of table name/pattern or SQL_NTS
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLTablePrivileges(SQLHSTMT stmt,
  SQLCHAR *catalog, SQLSMALLINT catalogLen,
  SQLCHAR *schema, SQLSMALLINT schemaLen,
  SQLCHAR *table, SQLSMALLINT tableLen)
{
  odbcError("SQLTablePrivileges not implemented, stmt:%p", stmt);
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcUnImplStmt(stmt);
  HSTMT_UNLOCK(stmt);
  return ret;
}

/**
 * Retrieve privileges on tables and/or views (UNICODE version).
 * @param stmt statement handle
 * @param catalog catalog name/pattern or NULL
 * @param catalogLen length of catalog name/pattern or SQL_NTS
 * @param schema schema name/pattern or NULL
 * @param schemaLen length of schema name/pattern or SQL_NTS
 * @param table table name/pattern or NULL
 * @param tableLen length of table name/pattern or SQL_NTS
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLTablePrivilegesW(SQLHSTMT stmt,
  SQLWCHAR *catalog, SQLSMALLINT catalogLen,
  SQLWCHAR *schema, SQLSMALLINT schemaLen,
  SQLWCHAR *table, SQLSMALLINT tableLen)
{
  odbcError("SQLTablePrivilegesW not implemented, stmt:%p", stmt);
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcUnImplStmt(stmt);
  HSTMT_UNLOCK(stmt);
  return ret;
}

/**
 * Retrieve privileges on columns.
 * @param stmt statement handle
 * @param catalog catalog name/pattern or NULL
 * @param catalogLen length of catalog name/pattern or SQL_NTS
 * @param schema schema name/pattern or NULL
 * @param schemaLen length of schema name/pattern or SQL_NTS
 * @param table table name/pattern or NULL
 * @param tableLen length of table name/pattern or SQL_NTS
 * @param column column name or NULL
 * @param columnLen length of column name or SQL_NTS
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLColumnPrivileges(SQLHSTMT stmt,
  SQLCHAR *catalog, SQLSMALLINT catalogLen,
  SQLCHAR *schema, SQLSMALLINT schemaLen,
  SQLCHAR *table, SQLSMALLINT tableLen,
  SQLCHAR *column, SQLSMALLINT columnLen)
{
  odbcError("SQLColumnPrivileges not implemented, stmt:%p", stmt);
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcUnImplStmt(stmt);
  HSTMT_UNLOCK(stmt);
  return ret;
}

/**
 * Retrieve privileges on columns (UNICODE version).
 * @param stmt statement handle
 * @param catalog catalog name/pattern or NULL
 * @param catalogLen length of catalog name/pattern or SQL_NTS
 * @param schema schema name/pattern or NULL
 * @param schemaLen length of schema name/pattern or SQL_NTS
 * @param table table name/pattern or NULL
 * @param tableLen length of table name/pattern or SQL_NTS
 * @param column column name or NULL
 * @param columnLen length of column name or SQL_NTS
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLColumnPrivilegesW(SQLHSTMT stmt,
  SQLWCHAR *catalog, SQLSMALLINT catalogLen,
  SQLWCHAR *schema, SQLSMALLINT schemaLen,
  SQLWCHAR *table, SQLSMALLINT tableLen,
  SQLWCHAR *column, SQLSMALLINT columnLen)
{
  odbcError("SQLColumnPrivilegesW not implemented, stmt:%p", stmt);
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcUnImplStmt(stmt);
  HSTMT_UNLOCK(stmt);
  return ret;
}

SQLRETURN
odbcPrimaryKeys(SQLHSTMT stmt, char *cat, char *schema, char *table)
{
  STMT *taosStmt = (STMT*)stmt;

  taosStmt->type = STMT_PRIMARY_KEY_SQL;
  if (cat != NULL && strcmp(cat, SQL_ALL_CATALOGS) != 0) {
    strcpy(taosStmt->dbc->dbname, cat);
  }
  if (table != NULL) {
    tstrncpy(taosStmt->dbc->tbname, table, sizeof(taosStmt->dbc->tbname));
  }

  if (strlen(taosStmt->dbc->dbname) != 0) {
    sprintf(taosStmt->sql, "describe %s.%s", taosStmt->dbc->dbname, taosStmt->dbc->tbname);
  }
  else {
    sprintf(taosStmt->sql, "describe %s", taosStmt->dbc->tbname);
  }

  taosStmt->fixedResultSetIndex = -1;
  return odbcExecuteSql(stmt);
}

/**
 * Retrieve information about indexed columns.
 * @param stmt statement handle
 * @param cat catalog name/pattern or NULL
 * @param catLen length of catalog name/pattern or SQL_NTS
 * @param schema schema name/pattern or NULL
 * @param schemaLen length of schema name/pattern or SQL_NTS
 * @param table table name/pattern or NULL
 * @param tableLen length of table name/pattern or SQL_NTS
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLPrimaryKeys(SQLHSTMT stmt,
  SQLCHAR *cat, SQLSMALLINT catLen,
  SQLCHAR *schema, SQLSMALLINT schemaLen,
  SQLCHAR *table, SQLSMALLINT tableLen)
{
  odbcDebug("SQLPrimaryKeys, stmt:%p, cat:%s, schema:%s, table:%s", stmt, cat, schema, table);
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcPrimaryKeys(stmt, (char*)cat, (char*)schema, (char*)table);
  HSTMT_UNLOCK(stmt);

  return ret;
}

/**
 * Retrieve information about indexed columns (UNICODE version).
 * @param stmt statement handle
 * @param cat catalog name/pattern or NULL
 * @param catLen length of catalog name/pattern or SQL_NTS
 * @param schema schema name/pattern or NULL
 * @param schemaLen length of schema name/pattern or SQL_NTS
 * @param table table name/pattern or NULL
 * @param tableLen length of table name/pattern or SQL_NTS
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLPrimaryKeysW(SQLHSTMT stmt,
  SQLWCHAR *cat, SQLSMALLINT catLen,
  SQLWCHAR *schema, SQLSMALLINT schemaLen,
  SQLWCHAR *table, SQLSMALLINT tableLen)
{
  char *c = NULL, *s = NULL, *t = NULL;
  SQLRETURN ret = SQL_SUCCESS;

  if (cat) {
    c = uc_to_utf_c(cat, catLen);
    if (!c) {
      ret = nomem((STMT *)stmt);
      goto done;
    }
  }
  if (schema) {
    s = uc_to_utf_c(schema, schemaLen);
    if (!s) {
      ret = nomem((STMT *)stmt);
      goto done;
    }
  }
  if (table) {
    t = uc_to_utf_c(table, tableLen);
    if (!t) {
      ret = nomem((STMT *)stmt);
      goto done;
    }
  }
  
  odbcDebug("SQLPrimaryKeysW, stmt:%p, cat:%s, schema:%s, table:%s", stmt, c, s, t);
  
  HSTMT_LOCK(stmt);
  ret = odbcPrimaryKeys(stmt, c, s, t);
  HSTMT_UNLOCK(stmt);

  return ret;


done:
  uc_free(c);
  uc_free(t);
  uc_free(s);

  return ret;
}

/**
 * Retrieve information about indexed columns.
 * @param stmt statement handle
 * @param id type of information, e.g. best row id
 * @param cat catalog name/pattern or NULL
 * @param catLen length of catalog name/pattern or SQL_NTS
 * @param schema schema name/pattern or NULL
 * @param schemaLen length of schema name/pattern or SQL_NTS
 * @param table table name/pattern or NULL
 * @param tableLen length of table name/pattern or SQL_NTS
 * @param scope
 * @param nullable
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLSpecialColumns(SQLHSTMT stmt, SQLUSMALLINT id,
  SQLCHAR *cat, SQLSMALLINT catLen,
  SQLCHAR *schema, SQLSMALLINT schemaLen,
  SQLCHAR *table, SQLSMALLINT tableLen,
  SQLUSMALLINT scope, SQLUSMALLINT nullable)
{
  odbcError("SQLSpecialColumns not implemented, stmt:%p", stmt);
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcUnImplStmt(stmt);
  HSTMT_UNLOCK(stmt);
  return ret;
}

/**
 * Retrieve information about indexed columns (UNICODE version).
 * @param stmt statement handle
 * @param id type of information, e.g. best row id
 * @param cat catalog name/pattern or NULL
 * @param catLen length of catalog name/pattern or SQL_NTS
 * @param schema schema name/pattern or NULL
 * @param schemaLen length of schema name/pattern or SQL_NTS
 * @param table table name/pattern or NULL
 * @param tableLen length of table name/pattern or SQL_NTS
 * @param scope
 * @param nullable
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLSpecialColumnsW(SQLHSTMT stmt, SQLUSMALLINT id,
  SQLWCHAR *cat, SQLSMALLINT catLen,
  SQLWCHAR *schema, SQLSMALLINT schemaLen,
  SQLWCHAR *table, SQLSMALLINT tableLen,
  SQLUSMALLINT scope, SQLUSMALLINT nullable)
{
  odbcError("SQLSpecialColumnsW not implemented, stmt:%p", stmt);
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcUnImplStmt(stmt);
  HSTMT_UNLOCK(stmt);
  return ret;
}

/**
 * Retrieve information about primary/foreign keys.
 * @param stmt statement handle
 * @param PKcatalog primary key catalog name/pattern or NULL
 * @param PKcatalogLen length of PKcatalog or SQL_NTS
 * @param PKschema primary key schema name/pattern or NULL
 * @param PKschemaLen length of PKschema or SQL_NTS
 * @param PKtable primary key table name/pattern or NULL
 * @param PKtableLen length of PKtable or SQL_NTS
 * @param FKcatalog foreign key catalog name/pattern or NULL
 * @param FKcatalogLen length of FKcatalog or SQL_NTS
 * @param FKschema foreign key schema name/pattern or NULL
 * @param FKschemaLen length of FKschema or SQL_NTS
 * @param FKtable foreign key table name/pattern or NULL
 * @param FKtableLen length of FKtable or SQL_NTS
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLForeignKeys(SQLHSTMT stmt,
  SQLCHAR *PKcatalog, SQLSMALLINT PKcatalogLen,
  SQLCHAR *PKschema, SQLSMALLINT PKschemaLen,
  SQLCHAR *PKtable, SQLSMALLINT PKtableLen,
  SQLCHAR *FKcatalog, SQLSMALLINT FKcatalogLen,
  SQLCHAR *FKschema, SQLSMALLINT FKschemaLen,
  SQLCHAR *FKtable, SQLSMALLINT FKtableLen)
{
  odbcDebug("SQLForeignKeys, stmt:%p", stmt);
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  STMT *s = (STMT*)stmt;
  s->type = STMT_FOERIGN_KEY_SQL;
  HSTMT_UNLOCK(stmt);

  return ret;
}

/**
 * Retrieve information about primary/foreign keys (UNICODE version).
 * @param stmt statement handle
 * @param PKcatalog primary key catalog name/pattern or NULL
 * @param PKcatalogLen length of PKcatalog or SQL_NTS
 * @param PKschema primary key schema name/pattern or NULL
 * @param PKschemaLen length of PKschema or SQL_NTS
 * @param PKtable primary key table name/pattern or NULL
 * @param PKtableLen length of PKtable or SQL_NTS
 * @param FKcatalog foreign key catalog name/pattern or NULL
 * @param FKcatalogLen length of FKcatalog or SQL_NTS
 * @param FKschema foreign key schema name/pattern or NULL
 * @param FKschemaLen length of FKschema or SQL_NTS
 * @param FKtable foreign key table name/pattern or NULL
 * @param FKtableLen length of FKtable or SQL_NTS
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLForeignKeysW(SQLHSTMT stmt,
  SQLWCHAR *PKcatalog, SQLSMALLINT PKcatalogLen,
  SQLWCHAR *PKschema, SQLSMALLINT PKschemaLen,
  SQLWCHAR *PKtable, SQLSMALLINT PKtableLen,
  SQLWCHAR *FKcatalog, SQLSMALLINT FKcatalogLen,
  SQLWCHAR *FKschema, SQLSMALLINT FKschemaLen,
  SQLWCHAR *FKtable, SQLSMALLINT FKtableLen)
{
  odbcDebug("SQLForeignKeysW, stmt:%p", stmt);
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  STMT *s = (STMT*)stmt;
  s->type = STMT_FOERIGN_KEY_SQL;
  HSTMT_UNLOCK(stmt);

  return ret;
}

/**
 * Commit or rollback transaction.
 * @param type type of handle
 * @param handle HDBC, HENV, or HSTMT handle
 * @param comptype SQL_COMMIT or SQL_ROLLBACK
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLEndTran(SQLSMALLINT type, SQLHANDLE handle, SQLSMALLINT comptype)
{
  odbcError("SQLEndTran not implemented, handle:%p", handle);
  return odbcUnImplHandle(type, handle);
}

/**
 * Commit or rollback transaction.
 * @param env environment handle or NULL
 * @param dbc database connection handle or NULL
 * @param type SQL_COMMIT or SQL_ROLLBACK
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLTransact(SQLHENV env, SQLHDBC dbc, SQLUSMALLINT type)
{
  odbcError("SQLTransact not implemented, dbc:%p", dbc);
  SQLRETURN ret = SQL_SUCCESS;

  HDBC_LOCK(dbc);
  ret = odbcUnImplDbc(dbc);
  HDBC_UNLOCK(dbc);
  return ret;
}

/**
 * Function not implemented.
 */

SQLRETURN SQL_API
SQLCopyDesc(SQLHDESC source, SQLHDESC target)
{
  odbcError("SQLCopyDesc not implemented");
  return SQL_ERROR;
}

/**
 * Translate SQL string.
 * @param stmt statement handle
 * @param sqlin input string
 * @param sqlinLen length of input string
 * @param sql output string
 * @param sqlMax max space in output string
 * @param sqlLen value return for length of output string
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLNativeSql(SQLHSTMT stmt, SQLCHAR *sqlin, SQLINTEGER sqlinLen,
  SQLCHAR *sql, SQLINTEGER sqlMax, SQLINTEGER *sqlLen)
{
  odbcError("SQLNativeSql not implemented, stmt:%p", stmt);
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcUnImplStmt(stmt);
  HSTMT_UNLOCK(stmt);
  return ret;
}

/**
 * Translate SQL string (UNICODE version).
 * @param stmt statement handle
 * @param sqlin input string
 * @param sqlinLen length of input string
 * @param sql output string
 * @param sqlMax max space in output string
 * @param sqlLen value return for length of output string
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLNativeSqlW(SQLHSTMT stmt, SQLWCHAR *sqlin, SQLINTEGER sqlinLen,
  SQLWCHAR *sql, SQLINTEGER sqlMax, SQLINTEGER *sqlLen)
{
  odbcError("SQLNativeSqlW not implemented, stmt:%p", stmt);
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcUnImplStmt(stmt);
  HSTMT_UNLOCK(stmt);
  return ret;
}

/**
 * Retrieve information about stored procedures.
 * @param stmt statement handle
 * @param catalog catalog name/pattern or NULL
 * @param catalogLen length of catalog or SQL_NTS
 * @param schema schema name/pattern or NULL
 * @param schemaLen length of schema or SQL_NTS
 * @param proc procedure name/pattern or NULL
 * @param procLen length of proc or SQL_NTS
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLProcedures(SQLHSTMT stmt,
  SQLCHAR *catalog, SQLSMALLINT catalogLen,
  SQLCHAR *schema, SQLSMALLINT schemaLen,
  SQLCHAR *proc, SQLSMALLINT procLen)
{
  odbcError("SQLProcedures not implemented, stmt:%p", stmt);
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcUnImplStmt(stmt);
  HSTMT_UNLOCK(stmt);
  return ret;
}

/**
 * Retrieve information about stored procedures (UNICODE version).
 * @param stmt statement handle
 * @param catalog catalog name/pattern or NULL
 * @param catalogLen length of catalog or SQL_NTS
 * @param schema schema name/pattern or NULL
 * @param schemaLen length of schema or SQL_NTS
 * @param proc procedure name/pattern or NULL
 * @param procLen length of proc or SQL_NTS
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLProceduresW(SQLHSTMT stmt,
  SQLWCHAR *catalog, SQLSMALLINT catalogLen,
  SQLWCHAR *schema, SQLSMALLINT schemaLen,
  SQLWCHAR *proc, SQLSMALLINT procLen)
{
  odbcError("SQLProceduresW not implemented, stmt:%p", stmt);
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcUnImplStmt(stmt);
  HSTMT_UNLOCK(stmt);
  return ret;
}

/**
 * Retrieve information about columns in result set of stored procedures.
 * @param stmt statement handle
 * @param catalog catalog name/pattern or NULL
 * @param catalogLen length of catalog or SQL_NTS
 * @param schema schema name/pattern or NULL
 * @param schemaLen length of schema or SQL_NTS
 * @param proc procedure name/pattern or NULL
 * @param procLen length of proc or SQL_NTS
 * @param column column name/pattern or NULL
 * @param columnLen length of column or SQL_NTS
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLProcedureColumns(SQLHSTMT stmt,
  SQLCHAR *catalog, SQLSMALLINT catalogLen,
  SQLCHAR *schema, SQLSMALLINT schemaLen,
  SQLCHAR *proc, SQLSMALLINT procLen,
  SQLCHAR *column, SQLSMALLINT columnLen)
{
  odbcError("SQLProcedureColumns not implemented, stmt:%p", stmt);
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcUnImplStmt(stmt);
  HSTMT_UNLOCK(stmt);
  return ret;
}

/**
 * Retrieve information about columns in result
 * set of stored procedures (UNICODE version).
 * @param stmt statement handle
 * @param catalog catalog name/pattern or NULL
 * @param catalogLen length of catalog or SQL_NTS
 * @param schema schema name/pattern or NULL
 * @param schemaLen length of schema or SQL_NTS
 * @param proc procedure name/pattern or NULL
 * @param procLen length of proc or SQL_NTS
 * @param column column name/pattern or NULL
 * @param columnLen length of column or SQL_NTS
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLProcedureColumnsW(SQLHSTMT stmt,
  SQLWCHAR *catalog, SQLSMALLINT catalogLen,
  SQLWCHAR *schema, SQLSMALLINT schemaLen,
  SQLWCHAR *proc, SQLSMALLINT procLen,
  SQLWCHAR *column, SQLSMALLINT columnLen)
{
  odbcError("SQLProcedureColumnsW not implemented, stmt:%p", stmt);
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcUnImplStmt(stmt);
  HSTMT_UNLOCK(stmt);
  return ret;
}

/**
 * Get information of HENV.
 * @param env environment handle
 * @param attr attribute to be retrieved
 * @param val output buffer
 * @param len length of output buffer
 * @param lenp output length
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLGetEnvAttr(SQLHENV env, SQLINTEGER attr, SQLPOINTER val,
  SQLINTEGER len, SQLINTEGER *lenp)
{
  odbcDebug("SQLGetEnvAttr, env:%p attr:%d:%s val:%p len:%d", env, attr, odbcEnvAttrString(attr), val, len);

  ENV *e;
  SQLRETURN ret = SQL_ERROR;

  if (env == SQL_NULL_HENV) {
    return SQL_INVALID_HANDLE;
  }
  e = (ENV *)env;
  if (!e || e->signature != e) {
    return SQL_INVALID_HANDLE;
  }

  switch (attr) {
  case SQL_ATTR_CONNECTION_POOLING:
    if (val) {
      *((SQLINTEGER *)val) = SQL_CP_OFF;
    }
    if (lenp) {
      *lenp = sizeof(SQLINTEGER);
    }
    ret = SQL_SUCCESS;
    break;
  case SQL_ATTR_CP_MATCH:
    *((SQLINTEGER *)val) = SQL_CP_STRICT_MATCH;
    if (lenp) {
      *lenp = sizeof(SQLINTEGER);
    }
    ret = SQL_SUCCESS;
    break;
  case SQL_ATTR_OUTPUT_NTS:
    if (val) {
      *((SQLINTEGER *)val) = SQL_TRUE;
    }
    if (lenp) {
      *lenp = sizeof(SQLINTEGER);
    }
    ret = SQL_SUCCESS;
    break;
  case SQL_ATTR_ODBC_VERSION:
    if (val) {
      *((SQLINTEGER *)val) = SQL_OV_ODBC3;
    }
    if (lenp) {
      *lenp = sizeof(SQLINTEGER);
    }
    ret = SQL_SUCCESS;
    break;
  }

  return ret;
}

/**
 * Set information in HENV.
 * @param env environment handle
 * @param attr attribute to be retrieved
 * @param val parameter buffer
 * @param len length of parameter
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLSetEnvAttr(SQLHENV env, SQLINTEGER attr, SQLPOINTER val, SQLINTEGER len)
{
  odbcDebug("SQLSetEnvAttr, env:%p, attr:%d:%s, val:%p, len:%d", env, attr, odbcEnvAttrString(attr), val, len);

  SQLRETURN ret = SQL_ERROR;
  HENV_LOCK(env);

  switch (attr) {
  case SQL_ATTR_CONNECTION_POOLING:
    if (val == (SQLPOINTER)SQL_CP_ONE_PER_DRIVER) {
      odbcDebug("SQLSetEnvAttr, set SQL_ATTR_CONNECTION_POOLING to SQL_CP_ONE_PER_DRIVER");
      ret = SQL_SUCCESS;
    }
    else if (val == (SQLPOINTER)SQL_CP_OFF) {
      odbcDebug("SQLSetEnvAttr, set SQL_ATTR_CONNECTION_POOLING to SQL_CP_OFF");
      ret = SQL_SUCCESS;
    }
    break;
  case SQL_ATTR_CP_MATCH:
    odbcDebug("SQLSetEnvAttr, set SQL_ATTR_CP_MATCH");
    ret = SQL_SUCCESS;
    break;
  case SQL_ATTR_OUTPUT_NTS:
    if (val == (SQLPOINTER)SQL_TRUE) {
      odbcDebug("SQLSetEnvAttr, set SQL_ATTR_OUTPUT_NTS to SQL_TRUE");
      ret = SQL_SUCCESS;
    }
    break;
  case SQL_ATTR_ODBC_VERSION:     //֧��ODBC 3�汾����֧��ODBC 2�汾
    if (val == (SQLPOINTER)SQL_OV_ODBC3) {
      odbcDebug("SQLSetEnvAttr, set SQL_ATTR_ODBC_VERSION to SQL_OV_ODBC3");
      ret = SQL_SUCCESS;
    }
    else if (val == (SQLPOINTER)SQL_OV_ODBC2) {
      odbcError("SQLSetEnvAttr, set SQL_ATTR_ODBC_VERSION to SQL_OV_ODBC2, only support odbc3");
      ret = SQL_ERROR;
    }
    break;
  }

  HENV_UNLOCK(env);
  return ret;
}

SQLRETURN 
odbcGetDiagRec(SQLSMALLINT htype, SQLHANDLE handle, SQLSMALLINT recno,
  SQLCHAR *sqlstate, SQLINTEGER *nativeerr, SQLCHAR *msg,
  SQLSMALLINT buflen, SQLSMALLINT *msglen)
{
  DBC *d = NULL;
  STMT *s = NULL;
  int len, naterr;
  char *logmsg = NULL;
  char *sqlst;
  SQLRETURN ret = SQL_ERROR;

  if (handle == SQL_NULL_HANDLE) {
    return SQL_INVALID_HANDLE;
  }
  if (sqlstate) {
    sqlstate[0] = '\0';
  }
  if (msg && buflen > 0) {
    msg[0] = '\0';
  }
  if (msglen) {
    *msglen = 0;
  }
  if (nativeerr) {
    *nativeerr = 0;
  }
  switch (htype) {
  case SQL_HANDLE_ENV:
  case SQL_HANDLE_DESC:
    return SQL_NO_DATA;
  case SQL_HANDLE_DBC:
    HDBC_LOCK((SQLHDBC)handle);
    d = (DBC *)handle;
    logmsg = (char *)d->logmsg;
    sqlst = d->sqlstate;
    naterr = d->naterr;
    break;
  case SQL_HANDLE_STMT:
    HSTMT_LOCK((SQLHSTMT)handle);
    s = (STMT *)handle;
    logmsg = (char *)s->dbc->logmsg;
    sqlst = s->dbc->sqlstate;
    naterr = s->dbc->naterr;
    break;
  default:
    return SQL_INVALID_HANDLE;
  }
  if (buflen < 0) {
    goto done;
  }
  if (recno > 1) {
    ret = SQL_NO_DATA;
    goto done;
  }
  len = (int)strlen(logmsg);
  if (len == 0) {
    ret = SQL_NO_DATA;
    goto done;
  }
  if (nativeerr) {
    *nativeerr = naterr;
  }
  if (sqlstate) {
    strcpy((char *)sqlstate, sqlst);
  }
  if (msglen) {
    *msglen = (SQLSMALLINT)len;
  }
  if (len >= buflen) {
    if (msg && buflen > 0) {
      strncpy((char *)msg, logmsg, (size_t)buflen);
      msg[buflen - 1] = '\0';
      logmsg[0] = '\0';
    }
  }
  else if (msg) {
    strcpy((char *)msg, logmsg);
    logmsg[0] = '\0';
  }
  ret = SQL_SUCCESS;
done:
  switch (htype) {
  case SQL_HANDLE_DBC:
    HDBC_UNLOCK((SQLHDBC)handle);
    break;
  case SQL_HANDLE_STMT:
    HSTMT_UNLOCK((SQLHSTMT)handle);
    break;
  }
  odbcDebug("odbcGetDiagRec, htype:%d:%s, handle:%p return:%d:%s, msg:%s", htype, odbcHandleTypeString(htype), handle, ret, odbcReturnCodeString(ret), msg);
  return ret;
}

/**
 * Get error message given handle (HENV, HDBC, or HSTMT)
 * (UNICODE version).
 * @param htype handle type
 * @param handle HENV, HDBC, or HSTMT
 * @param recno
 * @param sqlstate output buffer for SQL state
 * @param nativeerr output buffer of native error code
 * @param msg output buffer for error message
 * @param buflen length of output buffer
 * @param msglen output length
 * @result ODBC error code
 */
SQLRETURN SQL_API 
SQLGetDiagRec(SQLSMALLINT htype, SQLHANDLE handle, SQLSMALLINT recno,
  SQLCHAR *sqlstate, SQLINTEGER *nativeerr, SQLCHAR *msg,
  SQLSMALLINT buflen, SQLSMALLINT *msglen)
{
  odbcDebug("SQLGetDiagRec, htype:%d:%s, handle:%p, recno:%d", htype, odbcHandleTypeString(htype), handle, recno);
  return odbcGetDiagRec(htype, handle, recno, sqlstate, nativeerr, msg, buflen, msglen);
}

/**
 * Get error message given handle (HENV, HDBC, or HSTMT)
 * (UNICODE version).
 * @param htype handle type
 * @param handle HENV, HDBC, or HSTMT
 * @param recno
 * @param sqlstate output buffer for SQL state
 * @param nativeerr output buffer of native error code
 * @param msg output buffer for error message
 * @param buflen length of output buffer
 * @param msglen output length
 * @result ODBC error code
 */
SQLRETURN SQL_API 
SQLGetDiagRecW(SQLSMALLINT htype, SQLHANDLE handle, SQLSMALLINT recno,
  SQLWCHAR *sqlstate, SQLINTEGER *nativeerr, SQLWCHAR *msg,
  SQLSMALLINT buflen, SQLSMALLINT *msglen)
{
  odbcDebug("SQLGetDiagRecW, htype:%d:%s, handle:%p, recno:%d", htype, odbcHandleTypeString(htype), handle, recno);
  
  char state[16];
  SQLSMALLINT len;
  SQLRETURN ret = SQL_SUCCESS;

  ret = odbcGetDiagRec(htype, handle, recno, (SQLCHAR *)state,
    nativeerr, (SQLCHAR *)msg, buflen, &len);
  if (ret == SQL_SUCCESS) {
    if (sqlstate) {
      uc_from_utf_buf((SQLCHAR *)state, -1, sqlstate,
        6 * sizeof(SQLWCHAR));
    }
    if (msg) {
      if (len > 0) {
        SQLWCHAR *m = NULL;

        m = uc_from_utf((unsigned char *)msg, len);
        if (m) {
          if (buflen) {
            buflen /= (SQLSMALLINT)sizeof(SQLWCHAR);
            uc_strncpy(msg, m, buflen);
            m[len] = 0;
            len = (SQLSMALLINT)(min((SQLSMALLINT)buflen, (SQLSMALLINT)uc_strlen(m)));
          }
          else {
            len = (SQLSMALLINT)uc_strlen(m);
          }
          uc_free(m);
        }
        else {
          len = 0;
        }
      }
      if (len <= 0) {
        len = 0;
        if (buflen > 0) {
          msg[0] = 0;
        }
      }
    }
    else {
      /* estimated length !!! */
      len = (SQLSMALLINT)((size_t)len * sizeof(SQLWCHAR));
    }
    if (msglen) {
      *msglen = len;
    }
  }
  else if (ret == SQL_NO_DATA) {
    if (sqlstate) {
      sqlstate[0] = 0;
    }
    if (msg) {
      if (buflen > 0) {
        msg[0] = 0;
      }
    }
    if (msglen) {
      *msglen = 0;
    }
  }
  return ret;
}

SQLRETURN
odbcGetDiagField(SQLSMALLINT htype, SQLHANDLE handle, SQLSMALLINT recno,
  SQLSMALLINT id, SQLPOINTER info,
  SQLSMALLINT buflen, SQLSMALLINT *stringlen)
{
  DBC *d = NULL;
  STMT *s = NULL;
  int len, naterr, strbuf = 1;
  char *logmsg, *sqlst, *clrmsg = NULL;
  SQLRETURN ret = SQL_ERROR;

  if (handle == SQL_NULL_HANDLE) {
    return SQL_INVALID_HANDLE;
  }
  if (stringlen) {
    *stringlen = 0;
  }
  switch (htype) {
  case SQL_HANDLE_ENV:
  case SQL_HANDLE_DESC:
    return SQL_NO_DATA;
  case SQL_HANDLE_DBC:
    HDBC_LOCK((SQLHDBC)handle);
    d = (DBC *)handle;
    logmsg = (char *)d->logmsg;
    sqlst = d->sqlstate;
    naterr = d->naterr;
    break;
  case SQL_HANDLE_STMT:
    HSTMT_LOCK((SQLHSTMT)handle);
    s = (STMT *)handle;
    d = (DBC *)s->dbc;
    logmsg = (char *)s->dbc->logmsg;
    sqlst = s->dbc->sqlstate;
    naterr = s->dbc->naterr;
    break;
  default:
    return SQL_INVALID_HANDLE;
  }
  if (buflen < 0) {
    switch (buflen) {
    case SQL_IS_POINTER:
    case SQL_IS_UINTEGER:
    case SQL_IS_INTEGER:
    case SQL_IS_USMALLINT:
    case SQL_IS_SMALLINT:
      strbuf = 0;
      break;
    default:
      ret = SQL_ERROR;
      goto done;
    }
  }
  if (recno > 1) {
    ret = SQL_NO_DATA;
    goto done;
  }
  switch (id) {
  case SQL_DIAG_CLASS_ORIGIN:
    logmsg = "ISO 9075";
    if (sqlst[0] == 'I' && sqlst[1] == 'M') {
      logmsg = "ODBC 3.0";
    }
    break;
  case SQL_DIAG_SUBCLASS_ORIGIN:
    logmsg = "ISO 9075";
    if (sqlst[0] == 'I' && sqlst[1] == 'M') {
      logmsg = "ODBC 3.0";
    }
    else if (sqlst[0] == 'H' && sqlst[1] == 'Y') {
      logmsg = "ODBC 3.0";
    }
    else if (sqlst[0] == '2' || sqlst[0] == '0' || sqlst[0] == '4') {
      logmsg = "ODBC 3.0";
    }
    break;
  case SQL_DIAG_CONNECTION_NAME:
  case SQL_DIAG_SERVER_NAME:
    logmsg = d->dsn[0] ? d->dsn : d->server;
    break;
  case SQL_DIAG_SQLSTATE:
    logmsg = sqlst;
    break;
  case SQL_DIAG_MESSAGE_TEXT:
    if (info) {
      clrmsg = logmsg;
    }
    break;
  case SQL_DIAG_NUMBER:
    naterr = d->naterr;
    /* fall through */
  case SQL_DIAG_NATIVE:
    len = (int)strlen(logmsg);
    if (len == 0) {
      ret = SQL_NO_DATA;
      goto done;
    }
    if (info) {
      *((SQLINTEGER *)info) = naterr;
    }
    ret = SQL_SUCCESS;
    goto done;
  case SQL_DIAG_DYNAMIC_FUNCTION:
    logmsg = "";
    break;
  case SQL_DIAG_CURSOR_ROW_COUNT:
    if (htype == SQL_HANDLE_STMT) {
      SQLULEN count;

      count = /*(s->isselect == 1 || s->isselect == -1) ? s->nrows : */0;
      *((SQLULEN *)info) = count;
      ret = SQL_SUCCESS;
    }
    goto done;
  case SQL_DIAG_ROW_COUNT:
    if (htype == SQL_HANDLE_STMT) {
      SQLULEN count;

      count = /*s->isselect ? 0 : s->nrows*/0;
      *((SQLULEN *)info) = count;
      ret = SQL_SUCCESS;
    }
    goto done;
  default:
    goto done;
  }
  if (info && buflen > 0) {
    ((char *)info)[0] = '\0';
  }
  len = (int)strlen(logmsg);
  if (len == 0) {
    ret = SQL_NO_DATA;
    goto done;
  }
  if (stringlen) {
    *stringlen = (SQLSMALLINT)len;
  }
  if (strbuf) {
    if (len >= buflen) {
      if (info && buflen > 0) {
        if (stringlen) {
          *stringlen = (SQLSMALLINT)(buflen - 1);
        }
        strncpy((char *)info, logmsg, (size_t)buflen);
        ((char *)info)[buflen - 1] = '\0';
      }
    }
    else if (info) {
      strcpy((char *)info, logmsg);
    }
  }
  if (clrmsg) {
    *clrmsg = '\0';
  }
  ret = SQL_SUCCESS;
done:
  switch (htype) {
  case SQL_HANDLE_DBC:
    HDBC_UNLOCK((SQLHDBC)handle);
    break;
  case SQL_HANDLE_STMT:
    HSTMT_UNLOCK((SQLHSTMT)handle);
    break;
  }

  odbcDebug("odbcGetDiagField, htype:%d:%s, handle:%p return:%d:%s:%s", htype, odbcHandleTypeString(htype), handle, ret, odbcReturnCodeString(ret), info);

  return ret;
}

/**
 * Get error record given handle (HDBC or HSTMT).
 * @param htype handle type
 * @param handle HDBC or HSTMT
 * @param recno diag record number for which info to be retrieved
 * @param id diag id for which info to be retrieved
 * @param info output buffer for error message
 * @param buflen length of output buffer
 * @param stringlen output length
 * @result ODBC error code
 */

SQLRETURN SQL_API 
SQLGetDiagField(SQLSMALLINT htype, SQLHANDLE handle, SQLSMALLINT recno,
  SQLSMALLINT id, SQLPOINTER info,
  SQLSMALLINT buflen, SQLSMALLINT *stringlen)
{
  odbcDebug("SQLGetDiagField, htype:%d:%s, handle:%p, recno:%d, id:%d:%s", htype, odbcHandleTypeString(htype), handle, recno, id, odbcDiagFieldIdString(id));
  return odbcGetDiagField(htype, handle, recno, id, info, buflen, stringlen);
}

/**
 * Get error record given handle (HDBC or HSTMT).
 * @param htype handle type
 * @param handle HDBC or HSTMT
 * @param recno diag record number for which info to be retrieved
 * @param id diag id for which info to be retrieved
 * @param info output buffer for error message
 * @param buflen length of output buffer
 * @param stringlen output length
 * @result ODBC error code
 */

SQLRETURN SQL_API 
SQLGetDiagFieldW(SQLSMALLINT htype, SQLHANDLE handle, SQLSMALLINT recno,
  SQLSMALLINT id, SQLPOINTER info,
  SQLSMALLINT buflen, SQLSMALLINT *stringlen)
{
  odbcDebug("SQLGetDiagFieldW, htype:%d:%s, handle:%p, recno:%d, id:%d:%s", htype, odbcHandleTypeString(htype), handle, recno, id, odbcDiagFieldIdString(id));
  SQLSMALLINT len;
  SQLRETURN ret = SQL_SUCCESS;

  ret = odbcGetDiagField(htype, handle, recno, id, info, buflen, &len);
  if (ret == SQL_SUCCESS) {
    if (info) {
      switch (id) {
      case SQL_DIAG_CLASS_ORIGIN:
      case SQL_DIAG_SUBCLASS_ORIGIN:
      case SQL_DIAG_CONNECTION_NAME:
      case SQL_DIAG_SERVER_NAME:
      case SQL_DIAG_SQLSTATE:
      case SQL_DIAG_MESSAGE_TEXT:
      case SQL_DIAG_DYNAMIC_FUNCTION:
        if (len > 0) {
          SQLWCHAR *m = NULL;

          m = uc_from_utf((unsigned char *)info, len);
          if (m) {
            if (buflen) {
              buflen /= (SQLSMALLINT)sizeof(SQLWCHAR);
              uc_strncpy(info, m, buflen);
              m[len] = 0;
              len = (SQLSMALLINT)(min(buflen, (SQLSMALLINT)uc_strlen(m)));
            }
            else {
              len = (SQLSMALLINT)uc_strlen(m);
            }
            uc_free(m);
            len = (SQLSMALLINT)((size_t)len * sizeof(SQLWCHAR));
          }
          else {
            len = 0;
          }
        }
        if (len <= 0) {
          len = 0;
          if (buflen > 0) {
            ((SQLWCHAR *)info)[0] = 0;
          }
        }
      }
    }
    else {
      switch (id) {
      case SQL_DIAG_CLASS_ORIGIN:
      case SQL_DIAG_SUBCLASS_ORIGIN:
      case SQL_DIAG_CONNECTION_NAME:
      case SQL_DIAG_SERVER_NAME:
      case SQL_DIAG_SQLSTATE:
      case SQL_DIAG_MESSAGE_TEXT:
      case SQL_DIAG_DYNAMIC_FUNCTION:
        len = (SQLSMALLINT)((size_t)len * sizeof(SQLWCHAR));
        break;
      }
    }
    if (stringlen) {
      *stringlen = len;
    }
  }
  return ret;
}

SQLRETURN 
odbcGetStmtAttr(SQLHSTMT stmt, SQLINTEGER attr, SQLPOINTER val, SQLINTEGER bufmax, SQLINTEGER *buflen)
{
  STMT *s = (STMT *)stmt;
  SQLULEN *uval = (SQLULEN *)val;
  SQLINTEGER dummy;
  char dummybuf[16];

  if (!buflen) {
    buflen = &dummy;
  }
  if (!uval) {
    uval = (SQLPOINTER)dummybuf;
  }
  switch (attr) {
  case SQL_ATTR_QUERY_TIMEOUT:      //checked
    *uval = 0;
    *buflen = sizeof(SQLULEN);
    return SQL_SUCCESS;
  case SQL_ATTR_CURSOR_TYPE:        //checked
    *uval = SQL_CURSOR_STATIC;
    *buflen = sizeof(SQLULEN);
    return SQL_SUCCESS;
  case SQL_ATTR_CURSOR_SCROLLABLE:  //checked
    *uval = SQL_NONSCROLLABLE;
    *buflen = sizeof(SQLULEN);
    return SQL_SUCCESS;
#ifdef SQL_ATTR_CURSOR_SENSITIVITY
  case SQL_ATTR_CURSOR_SENSITIVITY: //checked
    *uval = SQL_INSENSITIVE;
    *buflen = sizeof(SQLULEN);
    return SQL_SUCCESS;
#endif
  case SQL_ATTR_ROW_NUMBER:
    *uval = (SQLULEN)SQL_NO_ROW_NUMBER;      //checked
    *buflen = sizeof(SQLULEN);
    return SQL_SUCCESS;
  case SQL_ATTR_ASYNC_ENABLE:       //checked
    *uval = SQL_ASYNC_ENABLE_OFF;
    *buflen = sizeof(SQLULEN);
    return SQL_SUCCESS;
  case SQL_ATTR_CONCURRENCY:        //checked
    *uval = SQL_CONCUR_READ_ONLY;
    *buflen = sizeof(SQLULEN);
    return SQL_SUCCESS;
  case SQL_ATTR_RETRIEVE_DATA:      //checked but not understand
    *uval = SQL_RD_OFF;
    *buflen = sizeof(SQLULEN);
    return SQL_SUCCESS;
  case SQL_ROWSET_SIZE:
  case SQL_ATTR_ROW_ARRAY_SIZE:     //checked
    *uval = 1;
    *buflen = sizeof(SQLULEN);
    return SQL_SUCCESS;
    /* Needed for some driver managers, but dummies for now */
  case SQL_ATTR_IMP_ROW_DESC:       //checked but not understand, always called by tableau
  case SQL_ATTR_APP_ROW_DESC:       //checked but not understand, always called by tableau
  case SQL_ATTR_IMP_PARAM_DESC:     //checked but not understand, always called by tableau
  case SQL_ATTR_APP_PARAM_DESC:     //checked but not understand, always called by tableau
    *((SQLHDESC *)uval) = (SQLHDESC)DEAD_MAGIC;
    *buflen = sizeof(SQLHDESC);
    return SQL_SUCCESS;
  case SQL_ATTR_ROW_STATUS_PTR:     //checked but not understand
    *((SQLUSMALLINT **)uval) = NULL;
    *buflen = sizeof(SQLUSMALLINT *);
    return SQL_SUCCESS;
  case SQL_ATTR_ROWS_FETCHED_PTR:   //checked, may be have a bug
    *((SQLULEN **)uval) = &s->rowsFetched;
    *buflen = sizeof(SQLULEN *);
    return SQL_SUCCESS;
  case SQL_ATTR_USE_BOOKMARKS: {    //checked
    *(SQLUINTEGER *)uval = SQL_UB_OFF;
    *buflen = sizeof(SQLUINTEGER);
    return SQL_SUCCESS;
  }
  case SQL_ATTR_FETCH_BOOKMARK_PTR: //checked
    *(SQLPOINTER *)uval = NULL;
    *buflen = sizeof(SQLPOINTER);
    return SQL_SUCCESS;
  case SQL_ATTR_PARAM_BIND_OFFSET_PTR: //checked but not understand
    *((SQLULEN **)uval) = NULL;
    *buflen = sizeof(SQLULEN *);
    return SQL_SUCCESS;
  case SQL_ATTR_PARAM_BIND_TYPE:       //checked but not understand
    *((SQLULEN *)uval) = 0;
    *buflen = sizeof(SQLULEN);
    return SQL_SUCCESS;
  case SQL_ATTR_PARAM_OPERATION_PTR:   //checked but not understand
    *((SQLUSMALLINT **)uval) = NULL;
    *buflen = sizeof(SQLUSMALLINT *);
    return SQL_SUCCESS;
  case SQL_ATTR_PARAM_STATUS_PTR:      //checked but not understand
    *((SQLUSMALLINT **)uval) = NULL;
    *buflen = sizeof(SQLUSMALLINT *);
    return SQL_SUCCESS;
  case SQL_ATTR_PARAMS_PROCESSED_PTR:  //checked but not understand
    *((SQLULEN **)uval) = NULL;
    *buflen = sizeof(SQLULEN *);
    return SQL_SUCCESS;
  case SQL_ATTR_PARAMSET_SIZE:         //checked but not understand
    *((SQLULEN *)uval) = 0;
    *buflen = sizeof(SQLULEN);
    return SQL_SUCCESS;
  case SQL_ATTR_ROW_BIND_TYPE:         //checked but not understand
    *(SQLULEN *)uval = 0;
    *buflen = sizeof(SQLULEN);
    return SQL_SUCCESS;
  case SQL_ATTR_ROW_BIND_OFFSET_PTR:   //checked but not understand
    *((SQLULEN **)uval) = NULL;
    *buflen = sizeof(SQLULEN *);
    return SQL_SUCCESS;
  case SQL_ATTR_MAX_ROWS:              //checked but not understand
    *((SQLULEN *)uval) = 0;
    *buflen = sizeof(SQLULEN);
    return SQL_SUCCESS;
  case SQL_ATTR_MAX_LENGTH:            //checked but not understand
    *((SQLULEN *)uval) = 1000000000;
    *buflen = sizeof(SQLULEN);
    return SQL_SUCCESS;
#ifdef SQL_ATTR_METADATA_ID
  case SQL_ATTR_METADATA_ID:           //checked
    //In function:SQLTables 
    //If the SQL_ATTR_METADATA_ID statement attribute is set to SQL_TRUE, CatalogName is treated as an identifier and its case is not significant. 
    //If it is SQL_FALSE, CatalogName is a pattern value argument; it is treated literally, and its case is significant.
    *((SQLULEN *)uval) = SQL_TRUE;
    *buflen = sizeof(SQLULEN);
    return SQL_SUCCESS;
#endif
  }
  return odbcUnImplStmt(stmt);
}

/**
 * Get option of HSTMT.
 * @param stmt statement handle
 * @param attr attribute to be retrieved
 * @param val output buffer
 * @param bufmax length of output buffer
 * @param buflen output length
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLGetStmtAttr(SQLHSTMT stmt, SQLINTEGER attr, SQLPOINTER val,
  SQLINTEGER bufmax, SQLINTEGER *buflen)
{
  odbcDebug("SQLGetStmtAttr, stmt:%p, attr:%d:%s", stmt, attr, odbcStmtAttrString(attr));
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcGetStmtAttr(stmt, attr, val, bufmax, buflen);
  HSTMT_UNLOCK(stmt);
  return ret;
}

/**
 * Get option of HSTMT (UNICODE version).
 * @param stmt statement handle
 * @param attr attribute to be retrieved
 * @param val output buffer
 * @param bufmax length of output buffer
 * @param buflen output length
 * @result ODBC error code
 */

SQLRETURN SQL_API 
SQLGetStmtAttrW(SQLHSTMT stmt, SQLINTEGER attr, SQLPOINTER val,
  SQLINTEGER bufmax, SQLINTEGER *buflen)
{
  odbcDebug("SQLGetStmtAttrW, stmt:%p, attr:%d:%s", stmt, attr, odbcStmtAttrString(attr));

  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcGetStmtAttr(stmt, attr, val, bufmax, buflen);
  HSTMT_UNLOCK(stmt);
  return ret;
}

SQLRETURN
odbcSetStmtAttr(SQLHSTMT stmt, SQLINTEGER attr, SQLPOINTER val,
  SQLINTEGER buflen)
{
  STMT *s = (STMT *)stmt;
#if defined(SQL_BIGINT) && defined(__WORDSIZE) && (__WORDSIZE == 64)
  SQLBIGINT uval;

  uval = (SQLBIGINT)val;
#else
  SQLULEN uval;

  uval = (SQLULEN)val;
#endif
  switch (attr) {
  case SQL_ATTR_CURSOR_TYPE:
    if (val != (SQLPOINTER)SQL_CURSOR_FORWARD_ONLY &&
      val != (SQLPOINTER)SQL_CURSOR_STATIC) {
      goto e01s02;
    }
    return SQL_SUCCESS;
  case SQL_ATTR_CURSOR_SCROLLABLE:
    if (val != (SQLPOINTER)SQL_CURSOR_FORWARD_ONLY &&
      val != (SQLPOINTER)SQL_CURSOR_STATIC) {
      goto e01s02;
    }
    return SQL_SUCCESS;
  case SQL_ATTR_ASYNC_ENABLE:
    if (val != (SQLPOINTER)SQL_ASYNC_ENABLE_OFF) {
    e01s02:
      setstat(s, -1, "option value changed", "01S02");
      return SQL_SUCCESS_WITH_INFO;
    }
    return SQL_SUCCESS;
  case SQL_ATTR_CONCURRENCY:
    if (val != (SQLPOINTER)SQL_CONCUR_READ_ONLY) {
      goto e01s02;
    }
    return SQL_SUCCESS;
#ifdef SQL_ATTR_CURSOR_SENSITIVITY
  case SQL_ATTR_CURSOR_SENSITIVITY:
    if (val != (SQLPOINTER)SQL_INSENSITIVE) {
      goto e01s02;
    }
    return SQL_SUCCESS;
#endif
  case SQL_ATTR_QUERY_TIMEOUT:
    return SQL_SUCCESS;
  case SQL_ATTR_RETRIEVE_DATA:
    if (val != (SQLPOINTER)SQL_RD_OFF) {
      goto e01s02;
    }
    return SQL_SUCCESS;
  case SQL_ROWSET_SIZE:
  case SQL_ATTR_ROW_ARRAY_SIZE:
    if (uval != 1) {
      setstat(s, -1, "invalid attribute size, should be 1", "HY024");
      return SQL_ERROR;
    }
    return SQL_SUCCESS;
  case SQL_ATTR_ROW_STATUS_PTR:
    setstat(s, -1, "invalid attribute type of SQL_ATTR_ROW_STATUS_PTR", "HY092");
    return SQL_ERROR;
  case SQL_ATTR_ROWS_FETCHED_PTR:
    setstat(s, -1, "invalid attribute type of SQL_ATTR_ROWS_FETCHED_PTR", "HY092");
    return SQL_ERROR;
  case SQL_ATTR_PARAM_BIND_OFFSET_PTR:
    goto e01s02;
    return SQL_SUCCESS;
  case SQL_ATTR_PARAM_BIND_TYPE:
    goto e01s02;
    return SQL_SUCCESS;
  case SQL_ATTR_PARAM_OPERATION_PTR:
    goto e01s02;
    return SQL_SUCCESS;
  case SQL_ATTR_PARAM_STATUS_PTR:
    goto e01s02;
    return SQL_SUCCESS;
  case SQL_ATTR_PARAMS_PROCESSED_PTR:
    goto e01s02;
    return SQL_SUCCESS;
  case SQL_ATTR_PARAMSET_SIZE:
    goto e01s02;
    return SQL_SUCCESS;
  case SQL_ATTR_ROW_BIND_TYPE:
    goto e01s02;
    return SQL_SUCCESS;
  case SQL_ATTR_ROW_BIND_OFFSET_PTR:
    goto e01s02;
    return SQL_SUCCESS;
  case SQL_ATTR_USE_BOOKMARKS:
    goto e01s02;
    return SQL_SUCCESS;
  case SQL_ATTR_FETCH_BOOKMARK_PTR:
    goto e01s02;
    return SQL_SUCCESS;
  case SQL_ATTR_MAX_ROWS:
    goto e01s02;
    return SQL_SUCCESS;
  case SQL_ATTR_MAX_LENGTH:
    if (val != (SQLPOINTER)1000000000) {
      goto e01s02;
    }
    return SQL_SUCCESS;
#ifdef SQL_ATTR_METADATA_ID
  case SQL_ATTR_METADATA_ID:
    if (val != (SQLPOINTER)SQL_TRUE) {
      goto e01s02;
    }
    return SQL_SUCCESS;
#endif
  }
  return odbcUnImplStmt(stmt);
}

/**
 * Set option on HSTMT.
 * @param stmt statement handle
 * @param attr attribute to be set
 * @param val input buffer (attribute value)
 * @param buflen length of input buffer
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLSetStmtAttr(SQLHSTMT stmt, SQLINTEGER attr, SQLPOINTER val,
  SQLINTEGER buflen)
{
  odbcDebug("SQLSetStmtAttr, stmt:%p, attr:%d:%s, val:%p", stmt, attr, odbcStmtAttrString(attr), val);
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcSetStmtAttr(stmt, attr, val, buflen);
  HSTMT_UNLOCK(stmt);
  return ret;
}

/**
 * Set option on HSTMT (UNICODE version).
 * @param stmt statement handle
 * @param attr attribute to be set
 * @param val input buffer (attribute value)
 * @param buflen length of input buffer
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLSetStmtAttrW(SQLHSTMT stmt, SQLINTEGER attr, SQLPOINTER val,
  SQLINTEGER buflen)
{
  odbcDebug("SQLSetStmtAttrW, stmt:%p, attr:%d:%s, val:%p", stmt, attr, odbcStmtAttrString(attr), val);
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcSetStmtAttr(stmt, attr, val, buflen);
  HSTMT_UNLOCK(stmt);
  return ret;
}

SQLRETURN 
odbcGetStmtOption(SQLHSTMT stmt, SQLUSMALLINT opt, SQLPOINTER param)
{
  //STMT *s = (STMT *)stmt;
  SQLUINTEGER *ret = (SQLUINTEGER *)param;

  switch (opt) {
  case SQL_ATTR_QUERY_TIMEOUT:  //checked
    *ret = 0;
    return SQL_SUCCESS;
  case SQL_ATTR_CURSOR_TYPE:    //checked
    *ret = SQL_CURSOR_FORWARD_ONLY;
    return SQL_SUCCESS;
  case SQL_ATTR_ROW_NUMBER:     //checked
    *ret = (SQLUINTEGER)SQL_NO_ROW_NUMBER;
    return SQL_SUCCESS;
  case SQL_ATTR_ASYNC_ENABLE:   //checked
    *ret = SQL_ASYNC_ENABLE_OFF;
    return SQL_SUCCESS;
  case SQL_ATTR_CONCURRENCY:    //checked
    *ret = SQL_CONCUR_READ_ONLY;
    return SQL_SUCCESS;
  case SQL_ATTR_RETRIEVE_DATA:  //checked
    *ret = SQL_RD_OFF;
    return SQL_SUCCESS;
  case SQL_ROWSET_SIZE:
  case SQL_ATTR_ROW_ARRAY_SIZE: //checked
    *ret = 1;
    return SQL_SUCCESS;
  case SQL_ATTR_MAX_ROWS:       //checked 0 means un-limit
    *ret = 0;
    return SQL_SUCCESS;
  case SQL_ATTR_MAX_LENGTH:     //checked
    *ret = 1000000000;
    return SQL_SUCCESS;
  }
  return odbcUnImplStmt(stmt);
}

/**
 * Get option of HSTMT.
 * @param stmt statement handle
 * @param opt option to be retrieved
 * @param param output buffer
 * @result ODBC error code
 */

SQLRETURN SQL_API 
SQLGetStmtOption(SQLHSTMT stmt, SQLUSMALLINT opt, SQLPOINTER param)
{
  odbcDebug("SQLGetStmtOption, stmt:%p, opt:%d:%s", stmt, opt, odbcStmtOptionString(opt));
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcGetStmtOption(stmt, opt, param);
  HSTMT_UNLOCK(stmt);
  return ret;
}

SQLRETURN SQL_API 
SQLGetStmtOptionW(SQLHSTMT stmt, SQLUSMALLINT opt, SQLPOINTER param)
{
  odbcDebug("SQLGetStmtOptionW, stmt:%p, opt:%d:%s", stmt, opt, odbcStmtOptionString(opt));
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcGetStmtOption(stmt, opt, param);
  HSTMT_UNLOCK(stmt);
  return ret;
}

SQLRETURN
odbcSetStmtoption(SQLHSTMT stmt, SQLUSMALLINT opt, SQLUINTEGER param)
{
  STMT *s = (STMT *)stmt;

  switch (opt) {
  case SQL_CURSOR_TYPE:
    if (param != SQL_CURSOR_FORWARD_ONLY &&
      param != SQL_CURSOR_STATIC) {
      goto e01s02;
    }
    return SQL_SUCCESS;
  case SQL_ASYNC_ENABLE:
    if (param != SQL_ASYNC_ENABLE_OFF) {
      goto e01s02;
    }
    return SQL_SUCCESS;
  case SQL_ATTR_CONCURRENCY:
    if (param != SQL_CONCUR_READ_ONLY) {
      goto e01s02;
    }
    return SQL_SUCCESS;
  case SQL_QUERY_TIMEOUT:
    return SQL_SUCCESS;
  case SQL_ATTR_RETRIEVE_DATA:
    if (param != SQL_RD_OFF) {
    e01s02:
      setstat(s, -1, "option value changed", "01S02");
      return SQL_SUCCESS_WITH_INFO;
    }
    return SQL_SUCCESS;
  case SQL_ROWSET_SIZE:
  case SQL_ATTR_ROW_ARRAY_SIZE:
    if (param != 1) {
      setstat(s, -1, "invalid rowset size", "HY000");
      return SQL_ERROR;
    }
    return SQL_SUCCESS;
  case SQL_ATTR_MAX_ROWS:
    if (param != 1) {
      setstat(s, -1, "invalid max row size", "HY000");
      return SQL_SUCCESS_WITH_INFO;
    }
    return SQL_SUCCESS;
  case SQL_ATTR_MAX_LENGTH:
    if (param != 1000000000) {
      goto e01s02;
    }
    return SQL_SUCCESS;
  }
  return odbcUnImplStmt(stmt);
}

/**
 * Set option on HSTMT.
 * @param stmt statement handle
 * @param opt option to be set
 * @param param input buffer (option value)
 * @result ODBC error code
 */

//SQLRETURN SQL_API
//SQLSetStmtOption(SQLHSTMT stmt, SQLUSMALLINT opt,
//  /*SETSTMTOPTION_LAST_ARG_TYPE*/SQLUINTEGER param)
//{
//  odbcDebug("SQLSetStmtOption, stmt:%p, opt:%d:%s, param:%d", stmt, opt, odbcStmtOptionString(opt), param);
//  SQLRETURN ret = SQL_SUCCESS;
//
//  HSTMT_LOCK(stmt);
//  ret = odbcSetStmtoption(stmt, opt, (SQLUINTEGER)param);
//  HSTMT_UNLOCK(stmt);
//  return ret;
//}

/**
* Set option on HSTMT (UNICODE version).
* @param stmt statement handle
* @param opt option to be set
* @param param input buffer (option value)
* @result ODBC error code
*/

//SQLRETURN SQL_API
//SQLSetStmtOptionW(SQLHSTMT stmt, SQLUSMALLINT opt,
//  /*SETSTMTOPTION_LAST_ARG_TYPE*/SQLUINTEGER param)
//{
//  odbcDebug("SQLSetStmtOptionW, stmt:%p, opt:%d:%s, param:%d", stmt, opt, odbcStmtOptionString(opt), param);
//  SQLRETURN ret = SQL_SUCCESS;
//
//  HSTMT_LOCK(stmt);
//  ret = odbcSetStmtoption(stmt, opt, (SQLUINTEGER)param);
//  HSTMT_UNLOCK(stmt);
//  return ret;
//}

/**
 * Set position on result in HSTMT.
 * @param stmt statement handle
 * @param row row to be positioned
 * @param op operation code
 * @param lock locking type
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLSetPos(SQLHSTMT stmt, SQLSETPOSIROW row, SQLUSMALLINT op, SQLUSMALLINT lock)
{
  odbcError("SQLSetPos not implemented, stmt:%p", stmt);
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcUnImplStmt(stmt);
  HSTMT_UNLOCK(stmt);
  return ret;
}

/**
 * Perform bulk operation on HSTMT.
 * @param stmt statement handle
 * @param oper operation to be performed
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLBulkOperations(SQLHSTMT stmt, SQLSMALLINT oper)
{
  odbcError("SQLBulkOperations not implemented, stmt:%p", stmt);
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcUnImplStmt(stmt);
  HSTMT_UNLOCK(stmt);
  return ret;
}

/**
 * Function not implemented.
 */

SQLRETURN SQL_API
SQLSetScrollOptions(SQLHSTMT stmt, SQLUSMALLINT concur, SQLLEN rowkeyset,
  SQLUSMALLINT rowset)
{
  odbcError("SQLSetScrollOptions not implemented, stmt:%p", stmt);
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcUnImplStmt(stmt);
  HSTMT_UNLOCK(stmt);
  return ret;
}

SQLRETURN 
odbcGetInfo(SQLHDBC dbc, SQLUSMALLINT type, SQLPOINTER val, SQLSMALLINT valMax, SQLSMALLINT *valLen)
{
  DBC *d;
  char dummyc[16];
  SQLSMALLINT dummy;

  if (dbc == SQL_NULL_HDBC) {
    return SQL_INVALID_HANDLE;
  }
  d = (DBC *)dbc;
  if (valMax) {
    valMax--;
  }
  if (!valLen) {
    valLen = &dummy;
  }
  if (!val) {
    val = dummyc;
    valMax = sizeof(dummyc) - 1;
  }
  switch (type) {
  case SQL_MAX_USER_NAME_LEN:   //checked
    *((SQLSMALLINT *)val) = TSDB_USER_LEN;
    *valLen = sizeof(SQLSMALLINT);
    break;
  case SQL_USER_NAME:           //checked
    strmak(val, d->user, valMax, valLen);
    break;
  case SQL_DRIVER_ODBC_VER:     //odbc������汾�ţ�ֻ����3����2
    strmak(val, "03.00", valMax, valLen);
    break;
  case SQL_ACTIVE_CONNECTIONS:  //checked
  case SQL_ACTIVE_STATEMENTS:   //checked
    *((SQLSMALLINT *)val) = 0;
    *valLen = sizeof(SQLSMALLINT);
    break;
#ifdef SQL_ASYNC_MODE
  case SQL_ASYNC_MODE:          //checked
    *((SQLUINTEGER *)val) = SQL_AM_NONE;
    *valLen = sizeof(SQLUINTEGER);
    break;
#endif
#ifdef SQL_CREATE_TABLE
  case SQL_CREATE_TABLE:        //checked
    *((SQLUINTEGER *)val) = SQL_CT_CREATE_TABLE/* |
                                               SQL_CT_COLUMN_DEFAULT |
                                               SQL_CT_COLUMN_CONSTRAINT |
                                               SQL_CT_CONSTRAINT_NON_DEFERRABLE*/;
    *valLen = sizeof(SQLUINTEGER);
    break;
#endif
#ifdef SQL_CREATE_VIEW
  case SQL_CREATE_VIEW:         //checked
    *((SQLUINTEGER *)val) = 0;
    *valLen = sizeof(SQLUINTEGER);
    break;
#endif
#ifdef SQL_DDL_INDEX
  case SQL_DDL_INDEX:           //checked
    *((SQLUINTEGER *)val) = 0;
    *valLen = sizeof(SQLUINTEGER);
    break;
#endif
#ifdef SQL_DROP_TABLE
  case SQL_DROP_TABLE:          //checked
    *((SQLUINTEGER *)val) = SQL_DT_DROP_TABLE;
    *valLen = sizeof(SQLUINTEGER);
    break;
#endif
#ifdef SQL_DROP_VIEW
  case SQL_DROP_VIEW:           //checked
    *((SQLUINTEGER *)val) = 0;
    *valLen = sizeof(SQLUINTEGER);
    break;
#endif
#ifdef SQL_INDEX_KEYWORDS
  case SQL_INDEX_KEYWORDS:      //checked
    *((SQLUINTEGER *)val) = SQL_IK_NONE;
    *valLen = sizeof(SQLUINTEGER);
    break;
#endif
  case SQL_DATA_SOURCE_NAME:    //checked
    strmak(val, d->dsn ? d->dsn : "", valMax, valLen);
    break;
  case SQL_DRIVER_NAME:         //checked
#if defined(_WIN32) || defined(_WIN64)
    strmak(val, "taosodbc.dll", valMax, valLen);
#else
    strmak(val, "libtaosodbc.so", valMax, valLen);
#endif
    break;
  case SQL_DRIVER_VER:          //checked
    strmak(val, DRIVER_VER_INFO, valMax, valLen);
    break;
  case SQL_FETCH_DIRECTION:     //checked
    *((SQLUINTEGER *)val) = SQL_FD_FETCH_NEXT;
    *valLen = sizeof(SQLUINTEGER);
    break;
  case SQL_ODBC_VER:            //checked
    strmak(val, d->ov3 ? "03.00" : "02.50", valMax, valLen);
    break;
  case SQL_ODBC_SAG_CLI_CONFORMANCE:  //checked but not understand
    *((SQLSMALLINT *)val) = SQL_OSCC_NOT_COMPLIANT;
    *valLen = sizeof(SQLSMALLINT);
    break;
  case SQL_STANDARD_CLI_CONFORMANCE:  //checked but not understand
    *((SQLUINTEGER *)val) = SQL_SCC_XOPEN_CLI_VERSION1;
    *valLen = sizeof(SQLUINTEGER);
    break;
  case SQL_SQL_CONFORMANCE:           //checked but not understand
    *((SQLUINTEGER *)val) = SQL_SC_SQL92_ENTRY;
    *valLen = sizeof(SQLUINTEGER);
    break;
  case SQL_SERVER_NAME:               //checked
    strmak(val, d->server ? d->server : "", valMax, valLen);
    break;
  case SQL_DATABASE_NAME:             //checked
    strmak(val, d->dbname ? d->dbname : "", valMax, valLen);
    break;
  case SQL_SEARCH_PATTERN_ESCAPE:     //checked maybe wrong
    strmak(val, "_%", valMax, valLen);
    break;
  case SQL_ODBC_SQL_CONFORMANCE:      //checked but not understand
    *((SQLSMALLINT *)val) = SQL_OSC_MINIMUM;
    *valLen = sizeof(SQLSMALLINT);
    break;
  case SQL_ODBC_API_CONFORMANCE:      //checked but not understand
    *((SQLSMALLINT *)val) = SQL_OAC_LEVEL1;
    *valLen = sizeof(SQLSMALLINT);
    break;
  case SQL_DBMS_NAME:                 //checked
    strmak(val, "TDengine", valMax, valLen);
    break;
  case SQL_DBMS_VER:                  //checked
    strmak(val, version, valMax, valLen);
    break;
  case SQL_COLUMN_ALIAS:              //checked
    strmak(val, "Y", valMax, valLen);
    break;
  case SQL_NEED_LONG_DATA_LEN:        //checked
    strmak(val, "N", valMax, valLen);
    break;
  case SQL_ROW_UPDATES:               //checked
  case SQL_ACCESSIBLE_PROCEDURES:     //checked
  case SQL_PROCEDURES:                //checked
  case SQL_EXPRESSIONS_IN_ORDERBY:    //checked
  case SQL_ODBC_SQL_OPT_IEF:          //checked
    strmak(val, "N", valMax, valLen);
    break;
  case SQL_LIKE_ESCAPE_CLAUSE:        //checked
    strmak(val, "Y", valMax, valLen);
    break;
  case SQL_ORDER_BY_COLUMNS_IN_SELECT://checked
  case SQL_OUTER_JOINS:               //checked
  case SQL_ACCESSIBLE_TABLES:         //checked
  case SQL_MULT_RESULT_SETS:          //checked
  case SQL_MULTIPLE_ACTIVE_TXN:       //checked
  case SQL_MAX_ROW_SIZE_INCLUDES_LONG://checked
    strmak(val, "N", valMax, valLen);
    break;
#ifdef SQL_CATALOG_NAME
  case SQL_CATALOG_NAME:              //checked
    strmak(val, "Y", valMax, valLen);
    break;
#endif
  case SQL_DATA_SOURCE_READ_ONLY:     //checked
    strmak(val, "N", valMax, valLen);
    break;
#ifdef SQL_OJ_CAPABILITIES
  case SQL_OJ_CAPABILITIES:           //checked
    *((SQLUINTEGER *)val) = 0;
    *valLen = sizeof(SQLUINTEGER);
    break;
#endif
#ifdef SQL_MAX_IDENTIFIER_LEN  
  case SQL_MAX_IDENTIFIER_LEN:        //checked
    *((SQLUSMALLINT *)val) = TSDB_TABLE_NAME_LEN - 1;
    *valLen = sizeof(SQLUSMALLINT);
    break;
#endif
  case SQL_CONCAT_NULL_BEHAVIOR:      //checked
    *((SQLSMALLINT *)val) = SQL_CB_NULL;
    *valLen = sizeof(SQLSMALLINT);
    break;
  case SQL_CURSOR_COMMIT_BEHAVIOR:    //�α��ù�֮����ô����
  case SQL_CURSOR_ROLLBACK_BEHAVIOR:  //�α��ù�֮����ô����
    *((SQLSMALLINT *)val) = SQL_CB_DELETE;
    *valLen = sizeof(SQLSMALLINT);
    break;
#ifdef SQL_CURSOR_SENSITIVITY
  case SQL_CURSOR_SENSITIVITY:        //checked
    *((SQLUINTEGER *)val) = SQL_UNSPECIFIED;
    *valLen = sizeof(SQLUINTEGER);
    break;
#endif
  case SQL_DEFAULT_TXN_ISOLATION:     //checked
    *((SQLUINTEGER *)val) = 0;
    *valLen = sizeof(SQLUINTEGER);
    break;
#ifdef SQL_DESCRIBE_PARAMETER
  case SQL_DESCRIBE_PARAMETER:        //checked no cofirmed
    strmak(val, "Y", valMax, valLen);
    break;
#endif
  case SQL_TXN_ISOLATION_OPTION:      //checked
    *((SQLUINTEGER *)val) = 0;
    *valLen = sizeof(SQLUINTEGER);
    break;
  case SQL_IDENTIFIER_CASE:           //checked
    *((SQLSMALLINT *)val) = SQL_IC_MIXED;
    *valLen = sizeof(SQLSMALLINT);
    break;
  case SQL_IDENTIFIER_QUOTE_CHAR:     //checked
    strmak(val, "", valMax, valLen);
    break;
  case SQL_MAX_TABLE_NAME_LEN:        //checked
    *((SQLSMALLINT *)val) = TSDB_TABLE_NAME_LEN - 1;
    *valLen = sizeof(SQLSMALLINT);
  case SQL_MAX_COLUMN_NAME_LEN:       //checked
    *((SQLSMALLINT *)val) = TSDB_COL_NAME_LEN - 1;
    *valLen = sizeof(SQLSMALLINT);
    break;
  case SQL_MAX_CURSOR_NAME_LEN:       //checked
    *((SQLSMALLINT *)val) = 0;
    *valLen = sizeof(SQLSMALLINT);
    break;
  case SQL_MAX_PROCEDURE_NAME_LEN:    //checked
    *((SQLSMALLINT *)val) = 0;
    *valLen = sizeof(SQLSMALLINT);
    break;
  case SQL_MAX_QUALIFIER_NAME_LEN:    //checked
    //same as SQL_MAX_CATALOG_NAME_LEN
    //in function SQLTables, used as database name
    *((SQLSMALLINT *)val) = TSDB_DB_NAME_LEN - 1;
    *valLen = sizeof(SQLSMALLINT);
    break;
  case SQL_MAX_OWNER_NAME_LEN:        //checked
    //same as SQL_MAX_SCHEMA_NAME_LEN
    //in function SQLTables, used as schema name
    *((SQLSMALLINT *)val) = TSDB_DB_NAME_LEN - 1;
    *valLen = sizeof(SQLSMALLINT);
    break;
  case SQL_OWNER_TERM:                //checked
    strmak(val, "", valMax, valLen);
    break;
  case SQL_PROCEDURE_TERM:            //checked
    strmak(val, "PROCEDURE", valMax, valLen);
    break;
  case SQL_QUALIFIER_NAME_SEPARATOR:  //checked
    strmak(val, ".", valMax, valLen);
    break;
  case SQL_QUALIFIER_TERM:            //checked
    strmak(val, "", valMax, valLen);
    break;
  case SQL_QUALIFIER_USAGE:           //checked
    //same as SQL_CATALOG_USAGE
    *((SQLUINTEGER *)val) = SQL_CU_DML_STATEMENTS | SQL_CU_TABLE_DEFINITION;
    *valLen = sizeof(SQLUINTEGER);
    break;
  case SQL_SCROLL_CONCURRENCY:        //checked
    *((SQLUINTEGER *)val) = 0;
    *valLen = sizeof(SQLUINTEGER);
    break;
  case SQL_SCROLL_OPTIONS:            //checked not confirmed
    *((SQLUINTEGER *)val) = SQL_SO_STATIC | SQL_SO_FORWARD_ONLY;
    *valLen = sizeof(SQLUINTEGER);
    break;
  case SQL_TABLE_TERM:                //checked
    strmak(val, "table", valMax, valLen);
    break;
  case SQL_TXN_CAPABLE:               //checked
    *((SQLSMALLINT *)val) = SQL_TC_NONE;
    *valLen = sizeof(SQLSMALLINT);
    break;
  case SQL_CONVERT_FUNCTIONS:         //checked
    *((SQLUINTEGER *)val) = 0;
    *valLen = sizeof(SQLUINTEGER);
    break;
  case SQL_SYSTEM_FUNCTIONS:          //checked
  case SQL_NUMERIC_FUNCTIONS:         //checked
  case SQL_STRING_FUNCTIONS:          //checked
  case SQL_TIMEDATE_FUNCTIONS:        //checked
    *((SQLUINTEGER *)val) = 0;
    *valLen = sizeof(SQLUINTEGER);
    break;
  case SQL_CONVERT_BIGINT:            //checked
  case SQL_CONVERT_BIT:               //checked
  case SQL_CONVERT_CHAR:              //checked
  case SQL_CONVERT_DATE:              //checked
  case SQL_CONVERT_DECIMAL:           //checked
  case SQL_CONVERT_DOUBLE:            //checked
  case SQL_CONVERT_FLOAT:             //checked
  case SQL_CONVERT_INTEGER:           //checked
  case SQL_CONVERT_LONGVARCHAR:       //checked
  case SQL_CONVERT_NUMERIC:           //checked
  case SQL_CONVERT_REAL:              //checked
  case SQL_CONVERT_SMALLINT:          //checked
  case SQL_CONVERT_TIME:              //checked
  case SQL_CONVERT_TIMESTAMP:         //checked
  case SQL_CONVERT_TINYINT:           //checked
  case SQL_CONVERT_VARCHAR:           //checked
    *((SQLUINTEGER *)val) = 0;
    *valLen = sizeof(SQLUINTEGER);
    break;
  case SQL_CONVERT_BINARY:            //checked
  case SQL_CONVERT_VARBINARY:         //checked
  case SQL_CONVERT_LONGVARBINARY:     //checked
    *((SQLUINTEGER *)val) = 0;
    *valLen = sizeof(SQLUINTEGER);
    break;
  case SQL_POSITIONED_STATEMENTS:     //checked
    *((SQLUINTEGER *)val) = 0;
    *valLen = sizeof(SQLUINTEGER);
    break;
  case SQL_LOCK_TYPES:                //checked
    *((SQLUINTEGER *)val) = 0;
    *valLen = sizeof(SQLUINTEGER);
    break;
  case SQL_BOOKMARK_PERSISTENCE:      //checked
    *((SQLUINTEGER *)val) = 0;
    *valLen = sizeof(SQLUINTEGER);
    break;
  case SQL_UNION:                     //checked
    *((SQLUINTEGER *)val) = 0;
    *valLen = sizeof(SQLUINTEGER);
    break;
  case SQL_SCHEMA_USAGE:              //checked
  case SQL_SUBQUERIES:                //checked
  case SQL_TIMEDATE_ADD_INTERVALS:    //checked
  case SQL_TIMEDATE_DIFF_INTERVALS:   //checked
    *((SQLUINTEGER *)val) = 0;
    *valLen = sizeof(SQLUINTEGER);
    break;
  case SQL_QUOTED_IDENTIFIER_CASE:    //checked 
    *((SQLUSMALLINT *)val) = SQL_IC_MIXED;
    *valLen = sizeof(SQLUSMALLINT);
    break;
  case SQL_POS_OPERATIONS:            //checked
    *((SQLUINTEGER *)val) = 0;
    *valLen = sizeof(SQLUINTEGER);
    break;
  case SQL_ALTER_TABLE:               //checked
    *((SQLUINTEGER *)val) = 0;
    *valLen = sizeof(SQLUINTEGER);
    break;
  case SQL_CORRELATION_NAME:          //checked
    *((SQLSMALLINT *)val) = SQL_CN_NONE;
    *valLen = sizeof(SQLSMALLINT);
    break;
  case SQL_NON_NULLABLE_COLUMNS:      //checked
    *((SQLSMALLINT *)val) = SQL_NNC_NULL;
    *valLen = sizeof(SQLSMALLINT);
    break;
  case SQL_NULL_COLLATION:            //checked
    *((SQLSMALLINT *)val) = 0;
    *valLen = sizeof(SQLSMALLINT);
    break;
  case SQL_MAX_COLUMNS_IN_GROUP_BY:   //checked
  case SQL_MAX_COLUMNS_IN_ORDER_BY:   //checked
  case SQL_MAX_COLUMNS_IN_SELECT:     //checked
  case SQL_MAX_COLUMNS_IN_TABLE:      //checked
  case SQL_MAX_ROW_SIZE:              //checked
  case SQL_MAX_TABLES_IN_SELECT:      //checked
    *((SQLSMALLINT *)val) = 0;
    *valLen = sizeof(SQLSMALLINT);
    break;
  case SQL_MAX_BINARY_LITERAL_LEN:    //checked
  case SQL_MAX_CHAR_LITERAL_LEN:      //checked
    *((SQLUINTEGER *)val) = 0;
    *valLen = sizeof(SQLUINTEGER);
    break;
  case SQL_MAX_COLUMNS_IN_INDEX:      //checked
    *((SQLSMALLINT *)val) = 0;
    *valLen = sizeof(SQLSMALLINT);
    break;
  case SQL_MAX_INDEX_SIZE:            //checked
    *((SQLUINTEGER *)val) = 0;
    *valLen = sizeof(SQLUINTEGER);
    break;
#ifdef SQL_MAX_IDENTIFIER_LENGTH
  case SQL_MAX_IDENTIFIER_LENGTH:
    *((SQLUINTEGER *)val) = 255;
    *valLen = sizeof(SQLUINTEGER);
    break;
#endif
  case SQL_MAX_STATEMENT_LEN:         //checked
    *((SQLUINTEGER *)val) = 16384;
    *valLen = sizeof(SQLUINTEGER);
    break;
  case SQL_QUALIFIER_LOCATION:        //checked
    *((SQLSMALLINT *)val) = SQL_QL_START;
    *valLen = sizeof(SQLSMALLINT);
    break;
  case SQL_GETDATA_EXTENSIONS:        //SQLGetData����ѡ��
    *((SQLUINTEGER *)val) =
      SQL_GD_ANY_COLUMN | SQL_GD_BOUND;
    *valLen = sizeof(SQLUINTEGER);
    break;
  case SQL_STATIC_SENSITIVITY:        //checked
    *((SQLUINTEGER *)val) = 0;
    *valLen = sizeof(SQLUINTEGER);
    break;
  case SQL_FILE_USAGE:                //checked
    *((SQLSMALLINT *)val) = SQL_FILE_NOT_SUPPORTED;
    *valLen = sizeof(SQLSMALLINT);
    break;
  case SQL_GROUP_BY:                  //checked
    *((SQLSMALLINT *)val) = 0;
    *valLen = sizeof(SQLSMALLINT);
    break;
  case SQL_KEYWORDS:
    strmak(val, "CREATE,SELECT,DROP,DELETE,INSERT,"
      "INTO,VALUES,TABLE,FROM,WHERE,AND",
      valMax, valLen);
    break;
  case SQL_SPECIAL_CHARACTERS:        //checked
#ifdef SQL_COLLATION_SEQ
  case SQL_COLLATION_SEQ:
#endif
    strmak(val, "#$^", valMax, valLen);
    break;
  case SQL_BATCH_SUPPORT:             //checked
  case SQL_BATCH_ROW_COUNT:           //checked
  case SQL_PARAM_ARRAY_ROW_COUNTS:    //checked
    *((SQLUINTEGER *)val) = 0;
    *valLen = sizeof(SQLUINTEGER);
    break;
  case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1: //checked
    *((SQLUINTEGER *)val) = 0;
    *valLen = sizeof(SQLUINTEGER);
    break;
  case SQL_STATIC_CURSOR_ATTRIBUTES1:       //checked
    *((SQLUINTEGER *)val) = 0;
    *valLen = sizeof(SQLUINTEGER);
    break;
  case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2: //checked
  case SQL_STATIC_CURSOR_ATTRIBUTES2:       //checked
    *((SQLUINTEGER *)val) = 0;
    *valLen = sizeof(SQLUINTEGER);
    break;
  case SQL_KEYSET_CURSOR_ATTRIBUTES1:       //checked
  case SQL_KEYSET_CURSOR_ATTRIBUTES2:       //checked
  case SQL_DYNAMIC_CURSOR_ATTRIBUTES1:      //checked
  case SQL_DYNAMIC_CURSOR_ATTRIBUTES2:      //checked
    *((SQLUINTEGER *)val) = 0;
    *valLen = sizeof(SQLUINTEGER);
    break;
  case SQL_ODBC_INTERFACE_CONFORMANCE:      //checked
    *((SQLUINTEGER *)val) = SQL_OIC_CORE;
    *valLen = sizeof(SQLUINTEGER);
    break;
  case SQL_AGGREGATE_FUNCTIONS:             //checked
    *((SQLUINTEGER *)val) = SQL_AF_AVG | SQL_AF_COUNT | SQL_AF_MAX | SQL_AF_MIN | SQL_AF_SUM;
    *valLen = sizeof(SQLUINTEGER);
    break;
  case SQL_DATETIME_LITERALS:               //checked
    *((SQLUINTEGER *)val) = SQL_DL_SQL92_TIMESTAMP;
    *valLen = sizeof(SQLUINTEGER);
    break;
  case SQL_SQL92_NUMERIC_VALUE_FUNCTIONS:   //checked
    *((SQLUINTEGER *)val) = 0;
    *valLen = sizeof(SQLUINTEGER);
    break;
  case SQL_SQL92_VALUE_EXPRESSIONS:         //checked but not understand
    *((SQLUINTEGER *)val) = SQL_SVE_NULLIF; //old is 0
    *valLen = sizeof(SQLUINTEGER);
    break;
  case SQL_SQL92_STRING_FUNCTIONS:          //checked but not understand
    *((SQLUINTEGER *)val) = 0;
    *valLen = sizeof(SQLUINTEGER);
    break;
  case SQL_SQL92_DATETIME_FUNCTIONS:        //checked but not understand
    *((SQLUINTEGER *)val) = 0;
    *valLen = sizeof(SQLUINTEGER);
    break;
  case SQL_SQL92_RELATIONAL_JOIN_OPERATORS: //checked but not understand
    *((SQLUINTEGER *)val) = 0;
    *valLen = sizeof(SQLUINTEGER);
    break;
  case SQL_SQL92_PREDICATES:                //checked
    *((SQLUINTEGER *)val) = SQL_SP_COMPARISON | SQL_SP_ISNOTNULL | SQL_SP_ISNULL | SQL_SP_LIKE ;
    *valLen = sizeof(SQLUINTEGER);
    break;
  default:
    setstatd(d, -1, "unsupported info option %d",
      d->ov3 ? "HYC00" : "S1C00", type);
    return SQL_ERROR;
  }
  return SQL_SUCCESS;
}

/**
 * Return information about what this ODBC driver supports.
 * @param dbc database connection handle
 * @param type type of information to be retrieved
 * @param val output buffer
 * @param valMax length of output buffer
 * @param valLen output length
 * @result ODBC error code
 */
SQLRETURN SQL_API 
SQLGetInfo(SQLHDBC dbc, SQLUSMALLINT type, SQLPOINTER val, SQLSMALLINT valMax, SQLSMALLINT *valLen)
{
  SQLRETURN ret = SQL_SUCCESS;
  odbcDebug("SQLGetInfo, dbc:%p type:%d:%s", dbc, type, odbcInfoTypeString(type));

  HDBC_LOCK(dbc);
  ret = odbcGetInfo(dbc, type, val, valMax, valLen);
  HDBC_UNLOCK(dbc);
  return ret;
}

/**
 * Return information about what this ODBC driver supports.
 * @param dbc database connection handle
 * @param type type of information to be retrieved
 * @param val output buffer
 * @param valMax length of output buffer
 * @param valLen output length
 * @result ODBC error code
 */
SQLRETURN SQL_API 
SQLGetInfoW(SQLHDBC dbc, SQLUSMALLINT type, SQLPOINTER val, SQLSMALLINT valMax, SQLSMALLINT *valLen)
{
  SQLRETURN ret = SQL_SUCCESS;
  SQLSMALLINT len = 0;
  odbcDebug("SQLGetInfoW, dbc:%p, type:%d:%s", dbc, type, odbcInfoTypeString(type));

  HDBC_LOCK(dbc);
  ret = odbcGetInfo(dbc, type, val, valMax, &len);
  HDBC_UNLOCK(dbc);

  if (ret == SQL_SUCCESS) {
    SQLWCHAR *v = NULL;

    switch (type) {
    case SQL_USER_NAME:
    case SQL_DRIVER_ODBC_VER:
    case SQL_DATA_SOURCE_NAME:
    case SQL_DRIVER_NAME:
    case SQL_DRIVER_VER:
    case SQL_ODBC_VER:
    case SQL_SERVER_NAME:
    case SQL_DATABASE_NAME:
    case SQL_SEARCH_PATTERN_ESCAPE:
    case SQL_DBMS_NAME:
    case SQL_DBMS_VER:
    case SQL_NEED_LONG_DATA_LEN:
    case SQL_ROW_UPDATES:
    case SQL_ACCESSIBLE_PROCEDURES:
    case SQL_PROCEDURES:
    case SQL_EXPRESSIONS_IN_ORDERBY:
    case SQL_ODBC_SQL_OPT_IEF:
    case SQL_LIKE_ESCAPE_CLAUSE:
    case SQL_ORDER_BY_COLUMNS_IN_SELECT:
    case SQL_OUTER_JOINS:
    case SQL_COLUMN_ALIAS:
    case SQL_ACCESSIBLE_TABLES:
    case SQL_MULT_RESULT_SETS:
    case SQL_MULTIPLE_ACTIVE_TXN:
    case SQL_MAX_ROW_SIZE_INCLUDES_LONG:
    case SQL_DATA_SOURCE_READ_ONLY:
#ifdef SQL_DESCRIBE_PARAMETER
    case SQL_DESCRIBE_PARAMETER:
#endif
    case SQL_IDENTIFIER_QUOTE_CHAR:
    case SQL_OWNER_TERM:
    case SQL_PROCEDURE_TERM:
    case SQL_QUALIFIER_NAME_SEPARATOR:
    case SQL_QUALIFIER_TERM:
    case SQL_TABLE_TERM:
    case SQL_KEYWORDS:
    case SQL_SPECIAL_CHARACTERS:
#ifdef SQL_CATALOG_NAME
    case SQL_CATALOG_NAME:
#endif
#ifdef SQL_COLLATION_SEQ
    case SQL_COLLATION_SEQ:
#endif
      if (val) {
        if (len > 0) {
          v = uc_from_utf((SQLCHAR *)val, len);
          if (v) {
            int vmax = (int)((size_t)valMax / sizeof(SQLWCHAR));

            uc_strncpy(val, v, vmax);
            if (len < vmax) {
              len = (SQLSMALLINT)(min(vmax, uc_strlen(v)));
              v[len] = 0;
            }
            else {
              len = (SQLSMALLINT)vmax;
            }
            uc_free(v);
            len = (SQLSMALLINT)((size_t)len * sizeof(SQLWCHAR));
          }
          else {
            len = 0;
          }
        }
        if (len <= 0) {
          len = 0;
          if (valMax >= sizeof(SQLWCHAR)) {
            *((SQLWCHAR *)val) = 0;
          }
        }
      }
      else {
        len = (SQLSMALLINT)((size_t)len * sizeof(SQLWCHAR));
      }
      break;
    }
    if (valLen) {
      *valLen = len;
    }
  }

  return ret;
}

/**
 * Return information about supported ODBC API functions.
 * @param dbc database connection handle
 * @param func function code to be retrieved
 * @param flags output indicator
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLGetFunctions(SQLHDBC dbc, SQLUSMALLINT func,
  SQLUSMALLINT *flags)
{
  odbcDebug("SQLGetFunctions, dbc:%p, type:%d:%s", dbc, func, odbcFunctionString(func));

  int i;
  SQLUSMALLINT exists[100];

  if (dbc == SQL_NULL_HDBC) {
    return SQL_INVALID_HANDLE;
  }
  for (i = 0; i < array_size(exists); i++) {
    exists[i] = SQL_FALSE;
  }
  exists[SQL_API_SQLALLOCCONNECT] = SQL_TRUE;
  exists[SQL_API_SQLFETCH] = SQL_TRUE;
  exists[SQL_API_SQLALLOCENV] = SQL_TRUE;
  exists[SQL_API_SQLFREECONNECT] = SQL_TRUE;
  exists[SQL_API_SQLALLOCSTMT] = SQL_TRUE;
  exists[SQL_API_SQLFREEENV] = SQL_TRUE;
  exists[SQL_API_SQLBINDCOL] = SQL_TRUE;
  exists[SQL_API_SQLFREESTMT] = SQL_TRUE;
  exists[SQL_API_SQLCANCEL] = SQL_TRUE;
  exists[SQL_API_SQLGETCURSORNAME] = SQL_TRUE;
  exists[SQL_API_SQLCOLATTRIBUTES] = SQL_TRUE;
  exists[SQL_API_SQLNUMRESULTCOLS] = SQL_TRUE;
  exists[SQL_API_SQLCONNECT] = SQL_TRUE;
  exists[SQL_API_SQLPREPARE] = SQL_TRUE;
  exists[SQL_API_SQLDESCRIBECOL] = SQL_TRUE;
  exists[SQL_API_SQLROWCOUNT] = SQL_TRUE;
  exists[SQL_API_SQLDISCONNECT] = SQL_TRUE;
  exists[SQL_API_SQLSETCURSORNAME] = SQL_FALSE;
  exists[SQL_API_SQLERROR] = SQL_TRUE;
  exists[SQL_API_SQLSETPARAM] = SQL_TRUE;
  exists[SQL_API_SQLEXECDIRECT] = SQL_TRUE;
  exists[SQL_API_SQLTRANSACT] = SQL_TRUE;
  exists[SQL_API_SQLBULKOPERATIONS] = SQL_TRUE;
  exists[SQL_API_SQLEXECUTE] = SQL_TRUE;
  exists[SQL_API_SQLBINDPARAMETER] = SQL_TRUE;
  exists[SQL_API_SQLGETTYPEINFO] = SQL_TRUE;
  exists[SQL_API_SQLCOLUMNS] = SQL_TRUE;
  exists[SQL_API_SQLPARAMDATA] = SQL_TRUE;
  exists[SQL_API_SQLDRIVERCONNECT] = SQL_TRUE;
  exists[SQL_API_SQLPUTDATA] = SQL_TRUE;
  exists[SQL_API_SQLGETCONNECTOPTION] = SQL_TRUE;
  exists[SQL_API_SQLSETCONNECTOPTION] = SQL_TRUE;
  exists[SQL_API_SQLGETDATA] = SQL_TRUE;
  exists[SQL_API_SQLSETSTMTOPTION] = SQL_FALSE; //changed
  exists[SQL_API_SQLGETFUNCTIONS] = SQL_TRUE;
  exists[SQL_API_SQLSPECIALCOLUMNS] = SQL_TRUE;
  exists[SQL_API_SQLGETINFO] = SQL_TRUE;
  exists[SQL_API_SQLSTATISTICS] = SQL_TRUE;
  exists[SQL_API_SQLGETSTMTOPTION] = SQL_TRUE;
  exists[SQL_API_SQLTABLES] = SQL_TRUE;
  exists[SQL_API_SQLBROWSECONNECT] = SQL_FALSE;
  exists[SQL_API_SQLNUMPARAMS] = SQL_TRUE;
  exists[SQL_API_SQLCOLUMNPRIVILEGES] = SQL_FALSE;
  exists[SQL_API_SQLPARAMOPTIONS] = SQL_FALSE;
  exists[SQL_API_SQLDATASOURCES] = SQL_TRUE;
  exists[SQL_API_SQLPRIMARYKEYS] = SQL_TRUE;
  exists[SQL_API_SQLDESCRIBEPARAM] = SQL_TRUE;
  exists[SQL_API_SQLPROCEDURECOLUMNS] = SQL_TRUE;
  exists[SQL_API_SQLDRIVERS] = SQL_FALSE;
  exists[SQL_API_SQLPROCEDURES] = SQL_TRUE;
  exists[SQL_API_SQLEXTENDEDFETCH] = SQL_TRUE;
  exists[SQL_API_SQLSETPOS] = SQL_TRUE;
  exists[SQL_API_SQLFOREIGNKEYS] = SQL_TRUE;
  exists[SQL_API_SQLSETSCROLLOPTIONS] = SQL_TRUE;
  exists[SQL_API_SQLMORERESULTS] = SQL_TRUE;
  exists[SQL_API_SQLTABLEPRIVILEGES] = SQL_TRUE;
  exists[SQL_API_SQLNATIVESQL] = SQL_TRUE;
  if (func == SQL_API_ALL_FUNCTIONS) {
    memcpy(flags, exists, sizeof(exists));
  }
  else if (func == SQL_API_ODBC3_ALL_FUNCTIONS) {
    int i;
#define SET_EXISTS(x) \
  flags[(x) >> 4] |= (1 << ((x) & 0xF))
#define CLR_EXISTS(x) \
  flags[(x) >> 4] &= ~(1 << ((x) & 0xF))

    memset(flags, 0,
      sizeof(SQLUSMALLINT) * SQL_API_ODBC3_ALL_FUNCTIONS_SIZE);
    for (i = 0; i < array_size(exists); i++) {
      if (exists[i]) {
        flags[i >> 4] = (SQLUSMALLINT)(flags[i >> 4] | (1 << (i & 0xF)));
      }
    }
    SET_EXISTS(SQL_API_SQLALLOCHANDLE);
    SET_EXISTS(SQL_API_SQLFREEHANDLE);
    SET_EXISTS(SQL_API_SQLGETSTMTATTR);
    SET_EXISTS(SQL_API_SQLSETSTMTATTR);
    SET_EXISTS(SQL_API_SQLGETCONNECTATTR);
    SET_EXISTS(SQL_API_SQLSETCONNECTATTR);
    SET_EXISTS(SQL_API_SQLGETENVATTR);
    SET_EXISTS(SQL_API_SQLSETENVATTR);
    SET_EXISTS(SQL_API_SQLCLOSECURSOR);
    SET_EXISTS(SQL_API_SQLBINDPARAM);
#if !defined(HAVE_UNIXODBC) || !(HAVE_UNIXODBC)
    /*
    * Some unixODBC versions have problems with
    * SQLError() vs. SQLGetDiagRec() with loss
    * of error/warning messages.
    */
    SET_EXISTS(SQL_API_SQLGETDIAGREC);
#endif
    SET_EXISTS(SQL_API_SQLGETDIAGFIELD);
    SET_EXISTS(SQL_API_SQLFETCHSCROLL);
    SET_EXISTS(SQL_API_SQLENDTRAN);
  }
  else {
    if (func < array_size(exists)) {
      *flags = exists[func];
    }
    else {
      switch (func) {
      case SQL_API_SQLALLOCHANDLE:
      case SQL_API_SQLFREEHANDLE:
      case SQL_API_SQLGETSTMTATTR:
      case SQL_API_SQLSETSTMTATTR:
      case SQL_API_SQLGETCONNECTATTR:
      case SQL_API_SQLSETCONNECTATTR:
      case SQL_API_SQLGETENVATTR:
      case SQL_API_SQLSETENVATTR:
      case SQL_API_SQLCLOSECURSOR:
      case SQL_API_SQLBINDPARAM:
#if !defined(HAVE_UNIXODBC) || !(HAVE_UNIXODBC)
        /*
        * Some unixODBC versions have problems with
        * SQLError() vs. SQLGetDiagRec() with loss
        * of error/warning messages.
        */
      case SQL_API_SQLGETDIAGREC:
#endif
      case SQL_API_SQLGETDIAGFIELD:
      case SQL_API_SQLFETCHSCROLL:
      case SQL_API_SQLENDTRAN:
        *flags = SQL_TRUE;
        break;
      default:
        *flags = SQL_FALSE;
      }
    }
  }
  return SQL_SUCCESS;
}

SQLRETURN 
odbcAllocEnv(SQLHENV *env)
{
  ENV *e;
  e = (ENV*)malloc(sizeof(ENV));
  if (e == NULL) {
    *env = SQL_NULL_HENV;
    return SQL_ERROR;
  }

  memset(e, 0, sizeof(ENV));
  e->signature = e;
  pthread_mutex_init(&e->mutex, NULL);

  *env = (SQLHENV)e;
  
  return SQL_SUCCESS;
}

/**
 * Allocate HENV.
 * @param env pointer to environment handle
 * @result ODBC error code
 */

SQLRETURN SQL_API 
SQLAllocEnv(SQLHENV *env)
{
  odbc_init();
  SQLRETURN ret = odbcAllocEnv(env);
  odbcDebug("SQLAllocEnv, env:%p", *env);
  return ret;
}

SQLRETURN 
odbcFreeEnv(SQLHENV env)
{
  if (env == SQL_NULL_HENV) {
    return SQL_INVALID_HANDLE;
  }

  ENV *e = (ENV*)env;
  if (e->signature != e) {
    return SQL_SUCCESS;
  }

  if (env != NULL) {
    e->signature = 0;
    pthread_mutex_destroy(&e->mutex);
    free(e);
  }

  return SQL_SUCCESS;
}

/**
 * Free HENV.
 * @param env environment handle
 * @result ODBC error code
 */

SQLRETURN SQL_API 
SQLFreeEnv(SQLHENV env)
{
  odbcDebug("SQLFreeEnv, env:%p", env);
  return odbcFreeEnv(env);
}

SQLRETURN 
odbcAllocDbc(SQLHENV env, SQLHDBC *dbc)
{
  DBC *d;
  ENV *e = (ENV*)env;
  int maj = 0, min = 0, lev = 0;

  if (env == SQL_NULL_HENV) {
    return SQL_ERROR;
  }

  d = (DBC*)malloc(sizeof(DBC));
  if (d == NULL) {
    *dbc = SQL_NULL_HDBC;
    return SQL_ERROR;
  }

  memset(d, 0, sizeof(DBC));
  d->signature = d;
  
  sscanf(version, "%d.%d.%d", &maj, &min, &lev);
  d->version = VERINFO(maj & 0xFF, min & 0xFF, lev & 0xFF);
  d->version_maj = (uint8_t)maj;
  d->version_min = (uint8_t)min;
  d->version_lev = (uint8_t)lev;
  d->version_reserve = 0;

  d->env = e;
  d->ov3 = true;
  pthread_mutex_init(&d->mutex, NULL);

  *dbc = (SQLHDBC)d;
  
  return SQL_SUCCESS;
}

/**
 * Allocate HDBC.
 * @param env environment handle
 * @param dbc pointer to database connection handle
 * @result ODBC error code
 */

SQLRETURN SQL_API 
SQLAllocConnect(SQLHENV env, SQLHDBC *dbc)
{
  SQLRETURN ret = odbcAllocDbc(env, dbc);
  odbcDebug("SQLAllocConnect, env:%p, dbc:%p", env, *dbc);
  return ret;
}

SQLRETURN 
odbcFreeDbc(SQLHDBC dbc)
{
  DBC *d = (DBC*)dbc;
  if (d == SQL_NULL_HDBC || d->signature != d) {
    return SQL_INVALID_HANDLE;
  }

  pthread_mutex_lock(&d->mutex);
  if (d->con != NULL) {
    odbcDebug("free taos:%p", d->con);
    taos_close(d->con);
    d->con = NULL;
  }

  d->signature = 0;
  pthread_mutex_destroy(&d->mutex);
  free(d);

  return SQL_SUCCESS;
}

/**
 * Free connection (HDBC).
 * @param dbc database connection handle
 * @result ODBC error code
 */

SQLRETURN SQL_API 
SQLFreeConnect(SQLHDBC dbc)
{
  odbcDebug("SQLFreeConnect, dbc:%p", dbc);
  return odbcFreeDbc(dbc);
}

/**
 * Internal get connect attribute of HDBC.
 * @param dbc database connection handle
 * @param attr option to be retrieved
 * @param val output buffer
 * @param bufmax size of output buffer
 * @param buflen output length
 * @result ODBC error code
 */

static SQLRETURN
odbcGetConnectAttr(SQLHDBC dbc, SQLINTEGER attr, SQLPOINTER val,
  SQLINTEGER bufmax, SQLINTEGER *buflen)
{
  DBC *d;
  SQLINTEGER dummy;

  if (dbc == SQL_NULL_HDBC) {
    return SQL_INVALID_HANDLE;
  }
  d = (DBC *)dbc;
  if (!val) {
    val = (SQLPOINTER)&dummy;
  }
  if (!buflen) {
    buflen = &dummy;
  }
  switch (attr) {
  case SQL_ATTR_CONNECTION_DEAD:       //checked
    *((SQLINTEGER *)val) = d->con ? SQL_CD_FALSE : SQL_CD_TRUE;
    *buflen = sizeof(SQLINTEGER);
    break;
  case SQL_ATTR_ACCESS_MODE:           //checked
    *((SQLINTEGER *)val) = SQL_MODE_READ_WRITE;
    *buflen = sizeof(SQLINTEGER);
    break;
  case SQL_ATTR_AUTOCOMMIT:            //checked
    *((SQLINTEGER *)val) = SQL_AUTOCOMMIT_ON;
    *buflen = sizeof(SQLINTEGER);
    break;
  case SQL_ATTR_LOGIN_TIMEOUT:         //checked
    *((SQLINTEGER *)val) = 30;
    *buflen = sizeof(SQLINTEGER);
    break;
  case SQL_ATTR_ODBC_CURSORS:          //checked not confirmed
    *((SQLINTEGER *)val) = SQL_CUR_USE_DRIVER;
    *buflen = sizeof(SQLINTEGER);
    break;
  case SQL_ATTR_PACKET_SIZE:           //checked
    *((SQLINTEGER *)val) = 64*1024;
    *buflen = sizeof(SQLINTEGER);
    break;
  case SQL_ATTR_TXN_ISOLATION:         //checked
    *((SQLINTEGER *)val) = 0;
    *buflen = sizeof(SQLINTEGER);
    break;
  case SQL_ATTR_TRACEFILE:             //checked
  case SQL_ATTR_TRANSLATE_LIB:         //checked
    *((SQLCHAR *)val) = 0;
    *buflen = 0;
    break;
  case SQL_ATTR_CURRENT_CATALOG:       //checked
    if ((bufmax > 2) && (val != (SQLPOINTER)&dummy)) {
      strcpy((char *)val, d->dbname);
      *buflen = 4;
    }
    break;
  case SQL_ATTR_TRACE:                 //checked
    *((SQLINTEGER *)val) = SQL_OPT_TRACE_OFF;
    *buflen = sizeof(SQLINTEGER);
    break;
  case SQL_ATTR_QUIET_MODE:            //checked
  case SQL_ATTR_TRANSLATE_OPTION:      //checked
  case SQL_ATTR_KEYSET_SIZE:           //checked
  case SQL_ATTR_QUERY_TIMEOUT:         //checked
    *((SQLINTEGER *)val) = 0;
    *buflen = sizeof(SQLINTEGER);
    break;
  case SQL_ATTR_PARAM_BIND_TYPE:       //checked  but not understand
    *((SQLULEN *)val) = 0;
    *buflen = sizeof(SQLUINTEGER);
    break;
  case SQL_ATTR_ROW_BIND_TYPE:         //checked  but not understand
    *((SQLULEN *)val) = 0;
    *buflen = sizeof(SQLULEN);
    break;
  case SQL_ATTR_USE_BOOKMARKS:         //checked  but not understand
    *((SQLINTEGER *)val) = SQL_UB_OFF;
    *buflen = sizeof(SQLINTEGER);
    break;
  case SQL_ATTR_ASYNC_ENABLE:          //checked  but not understand
    *((SQLINTEGER *)val) = SQL_ASYNC_ENABLE_OFF;
    *buflen = sizeof(SQLINTEGER);
    break;
  case SQL_ATTR_NOSCAN:                //checked  but not understand
    *((SQLINTEGER *)val) = SQL_NOSCAN_ON;
    *buflen = sizeof(SQLINTEGER);
    break;
  case SQL_ATTR_CONCURRENCY:           //checked 
    *((SQLINTEGER *)val) = SQL_CONCUR_READ_ONLY;
    *buflen = sizeof(SQLINTEGER);
    break;
#ifdef SQL_ATTR_CURSOR_SENSITIVITY
  case SQL_ATTR_CURSOR_SENSITIVITY:    //checked 
    *((SQLINTEGER *)val) = SQL_INSENSITIVE;
    *buflen = sizeof(SQLINTEGER);
    break;
#endif
  case SQL_ATTR_SIMULATE_CURSOR:       //checked  but not understand
    *((SQLINTEGER *)val) = 0;
    *buflen = sizeof(SQLINTEGER);
    break;
  case SQL_ATTR_MAX_ROWS:              //checked
    *((SQLINTEGER *)val) = 0;
    *buflen = sizeof(SQLINTEGER);
	break;
  case SQL_ATTR_MAX_LENGTH:            //checked
    *((SQLINTEGER *)val) = 1000000000;
    *buflen = sizeof(SQLINTEGER);
    break;
  case SQL_ATTR_CURSOR_TYPE:           //checked
    *((SQLINTEGER *)val) = SQL_CURSOR_FORWARD_ONLY;
    *buflen = sizeof(SQLINTEGER);
    break;
  case SQL_ATTR_RETRIEVE_DATA:         //checked
    *((SQLINTEGER *)val) = SQL_RD_OFF;
    *buflen = sizeof(SQLINTEGER);
    break;
#ifdef SQL_ATTR_METADATA_ID
  case SQL_ATTR_METADATA_ID:           //checked
    *((SQLULEN *)val) = SQL_TRUE;
    return SQL_SUCCESS;
#endif
  default:
    *((SQLINTEGER *)val) = 0;
    *buflen = sizeof(SQLINTEGER);
    setstatd(d, -1, "unsupported connect attribute %d",
      (d->ov3) ? "HYC00" : "S1C00", (int)attr);
    return SQL_ERROR;
  }
  return SQL_SUCCESS;
}

/**
 * Get connect attribute of HDBC.
 * @param dbc database connection handle
 * @param attr option to be retrieved
 * @param val output buffer
 * @param bufmax size of output buffer
 * @param buflen output length
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLGetConnectAttr(SQLHDBC dbc, SQLINTEGER attr, SQLPOINTER val,
  SQLINTEGER bufmax, SQLINTEGER *buflen)
{
  odbcDebug("SQLGetConnectAttr, dbc:%p, attr:%d:%s, val:%p, bufmax:%d", dbc, attr, odbcConnectAttrString(attr), val, bufmax);
  SQLRETURN ret = SQL_SUCCESS;

  HDBC_LOCK(dbc);
  ret = odbcGetConnectAttr(dbc, attr, val, bufmax, buflen);
  HDBC_UNLOCK(dbc);
  return ret;
}

/**
 * Get connect attribute of HDBC (UNICODE version).
 * @param dbc database connection handle
 * @param attr option to be retrieved
 * @param val output buffer
 * @param bufmax size of output buffer
 * @param buflen output length
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLGetConnectAttrW(SQLHDBC dbc, SQLINTEGER attr, SQLPOINTER val,
  SQLINTEGER bufmax, SQLINTEGER *buflen)
{
  odbcDebug("SQLGetConnectAttrW, dbc:%p, attr:%d:%s, val:%p, bufmax:%d", dbc, attr, odbcConnectAttrString(attr), val, bufmax);
  SQLRETURN ret = SQL_SUCCESS;
  SQLINTEGER len = 0;

  HDBC_LOCK(dbc);
  ret = odbcGetConnectAttr(dbc, attr, val, bufmax, &len);
  if (ret == SQL_SUCCESS) {
    SQLWCHAR *v = NULL;

    switch (attr) {
    case SQL_ATTR_TRACEFILE:
    case SQL_ATTR_CURRENT_CATALOG:
    case SQL_ATTR_TRANSLATE_LIB:
      if (val) {
        if (len > 0) {
          v = uc_from_utf((SQLCHAR *)val, len);
          if (v) {
            int vmax = (int)((size_t)bufmax / sizeof(SQLWCHAR));

            uc_strncpy(val, v, vmax);
            if (len < vmax) {
              len = min(vmax, uc_strlen(v));
              v[len] = 0;
            }
            else {
              len = vmax;
            }
            uc_free(v);
            len = (SQLINTEGER)((size_t)len * sizeof(SQLWCHAR));
          }
          else {
            len = 0;
          }
        }
        if (len <= 0) {
          len = 0;
          if (bufmax >= sizeof(SQLWCHAR)) {
            *((SQLWCHAR *)val) = 0;
          }
        }
      }
      else {
        len = (SQLINTEGER)((size_t)len * sizeof(SQLWCHAR));
      }
      break;
    }
    if (buflen) {
      *buflen = len;
    }
  }
  HDBC_UNLOCK(dbc);
  return ret;
}

SQLRETURN odbcSetConnectAttr(SQLHDBC dbc, SQLINTEGER attr, SQLPOINTER val,
  SQLINTEGER len)
{
  //DBC *d;

  if (dbc == SQL_NULL_HDBC) {
    return SQL_INVALID_HANDLE;
  }
  //d = (DBC *)dbc;
  switch (attr) {
  case SQL_AUTOCOMMIT:
    return SQL_SUCCESS;
#ifdef SQL_ATTR_METADATA_ID
  case SQL_ATTR_METADATA_ID:
    if (val == (SQLPOINTER)SQL_TRUE) {
      break;
    }
    /* fall through */
#endif
  //default:
  //  setstatd(d, -1, "option value changed", "01S02");
  //  return SQL_SUCCESS_WITH_INFO;
  }
  return SQL_SUCCESS;
}

/**
 * Set connect attribute of HDBC.
 * @param dbc database connection handle
 * @param attr option to be set
 * @param val option value
 * @param len size of option
 * @result ODBC error code
 */
SQLRETURN SQL_API 
SQLSetConnectAttr(SQLHDBC dbc, SQLINTEGER attr, SQLPOINTER val,
  SQLINTEGER len)
{
  odbcDebug("SQLSetConnectAttr, dbc:%p, attr:%d:%s, val:%d, len:%d", dbc, attr, odbcConnectAttrString(attr), val, len);
  SQLRETURN ret = SQL_SUCCESS;

  HDBC_LOCK(dbc);
  ret = odbcSetConnectAttr(dbc, attr, val, len);
  HDBC_UNLOCK(dbc);
  return ret;
}

/**
 * Set connect attribute of HDBC (UNICODE version).
 * @param dbc database connection handle
 * @param attr option to be set
 * @param val option value
 * @param len size of option
 * @result ODBC error code
 */
SQLRETURN SQL_API 
SQLSetConnectAttrW(SQLHDBC dbc, SQLINTEGER attr, SQLPOINTER val,
  SQLINTEGER len)
{
  odbcDebug("SQLSetConnectAttrW, dbc:%p, attr:%d:%s, val:%d, len:%d", dbc, attr, odbcConnectAttrString(attr), val, len);
  SQLRETURN ret = SQL_SUCCESS;

  HDBC_LOCK(dbc);
  ret = odbcSetConnectAttr(dbc, attr, val, len);
  HDBC_UNLOCK(dbc);
  return ret;
}

SQLRETURN
odbcGetConnectOption(SQLHDBC dbc, SQLUSMALLINT opt, SQLPOINTER param)
{
  DBC *d;
  SQLINTEGER dummy;

  if (dbc == SQL_NULL_HDBC) {
    return SQL_INVALID_HANDLE;
  }
  d = (DBC *)dbc;
  if (!param) {
    param = (SQLPOINTER)&dummy;
  }
  switch (opt) {
  case SQL_ACCESS_MODE:    //checked
    *((SQLINTEGER *)param) = SQL_MODE_READ_WRITE;
    break;
  case SQL_AUTOCOMMIT:     //checked
    *((SQLINTEGER *)param) = SQL_AUTOCOMMIT_ON;
    break;
  case SQL_LOGIN_TIMEOUT:  //checked
    *((SQLINTEGER *)param) = 30;
    break;
  case SQL_ODBC_CURSORS:   //checked
    *((SQLINTEGER *)param) = SQL_CUR_USE_DRIVER;
    break;
  case SQL_PACKET_SIZE:    //checked
    *((SQLINTEGER *)param) = 64*1024;
    break;
  case SQL_TXN_ISOLATION:  //checked
    *((SQLINTEGER *)param) = 0;
    break;
  case SQL_OPT_TRACE:      //checked
  case SQL_OPT_TRACEFILE:  //checked
  case SQL_QUIET_MODE:     //checked
  case SQL_TRANSLATE_DLL:  //checked
  case SQL_TRANSLATE_OPTION:  //checked
  case SQL_KEYSET_SIZE:       //checked
  case SQL_QUERY_TIMEOUT:     //checked
  case SQL_BIND_TYPE:         //checked
  case SQL_CURRENT_QUALIFIER: //checked not confirmed
    *((SQLINTEGER *)param) = 0;
    break;
  case SQL_USE_BOOKMARKS:     //checked
    *((SQLINTEGER *)param) = SQL_UB_OFF;
    break;
  case SQL_ASYNC_ENABLE:      //checked
    *((SQLINTEGER *)param) = SQL_ASYNC_ENABLE_OFF;
    break;
  case SQL_NOSCAN:            //checked  but not understand
    *((SQLINTEGER *)param) = SQL_NOSCAN_ON;
    break;
  case SQL_CONCURRENCY:       //checked
    *((SQLINTEGER *)param) = SQL_CONCUR_LOCK;
    break;
  case SQL_SIMULATE_CURSOR:   //checked
    *((SQLINTEGER *)param) = SQL_SC_NON_UNIQUE;
    break;
  case SQL_MAX_ROWS:          //checked
    *((SQLINTEGER *)param) = 0;
    break;
  case SQL_ROWSET_SIZE:       //checked
  case SQL_MAX_LENGTH:        //checked
    *((SQLINTEGER *)param) = 1000000000;
    break;
  case SQL_CURSOR_TYPE:       //checked
    *((SQLINTEGER *)param) = SQL_CURSOR_STATIC;
    break;
  case SQL_RETRIEVE_DATA:     //checked
    *((SQLINTEGER *)param) = SQL_RD_OFF;
    break;
  default:
    *((SQLINTEGER *)param) = 0;
    setstatd(d, -1, "unsupported connect option %d",
      (d->ov3) ? "HYC00" : "S1C00", opt);
    return SQL_ERROR;
  }
  return SQL_SUCCESS;
}

/**
 * Get connect option of HDBC.
 * @param dbc database connection handle
 * @param opt option to be retrieved
 * @param param output buffer
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLGetConnectOption(SQLHDBC dbc, SQLUSMALLINT opt, SQLPOINTER param)
{
  odbcDebug("SQLGetConnectOption, dbc:%p, opt:%d:%s, param:%d", dbc, opt, odbcConnectOptionString(opt), param);
  SQLRETURN ret = SQL_SUCCESS;

  HDBC_LOCK(dbc);
  ret = odbcGetConnectOption(dbc, opt, param);
  HDBC_UNLOCK(dbc);
  return ret;
}

/**
 * Get connect option of HDBC (UNICODE version).
 * @param dbc database connection handle
 * @param opt option to be retrieved
 * @param param output buffer
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLGetConnectOptionW(SQLHDBC dbc, SQLUSMALLINT opt, SQLPOINTER param)
{
  odbcDebug("SQLGetConnectOptionW, dbc:%p, opt:%d:%s, param:%d", dbc, opt, odbcConnectOptionString(opt), param);
  SQLRETURN ret = SQL_SUCCESS;

  HDBC_LOCK(dbc);
  ret = odbcGetConnectOption(dbc, opt, param);
  if (SQL_SUCCEEDED(ret)) {
    switch (opt) {
    case SQL_OPT_TRACEFILE:
    case SQL_CURRENT_QUALIFIER:
    case SQL_TRANSLATE_DLL:
      if (param) {
        *(SQLWCHAR *)param = 0;
      }
      break;
    }
  }
  HDBC_UNLOCK(dbc);
  return ret;
}

SQLRETURN 
odbcSetConnectOption(SQLHDBC dbc, SQLUSMALLINT opt, SQLULEN param)
{
  DBC *d;

  if (dbc == SQL_NULL_HDBC) {
    return SQL_INVALID_HANDLE;
  }
  d = (DBC *)dbc;
  switch (opt) {
  case SQL_AUTOCOMMIT:
    break;
  default:
    setstatd(d, -1, "option value changed", "01S02");
    return SQL_SUCCESS_WITH_INFO;
  }
  return SQL_SUCCESS;
}

/**
 * Set option on HDBC.
 * @param dbc database connection handle
 * @param opt option to be set
 * @param param option value
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLSetConnectOption(SQLHDBC dbc, SQLUSMALLINT opt, SQLULEN param)
{
  odbcDebug("SQLSetConnectOption, dbc:%p, opt:%d:%s, param:%d", dbc, opt, odbcConnectOptionString(opt), param);

  SQLRETURN ret = SQL_SUCCESS;

  HDBC_LOCK(dbc);
  ret = odbcSetConnectOption(dbc, opt, param);
  HDBC_UNLOCK(dbc);

  return ret;
}

/**
 * Set option on HDBC (UNICODE version).
 * @param dbc database connection handle
 * @param opt option to be set
 * @param param option value
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLSetConnectOptionW(SQLHDBC dbc, SQLUSMALLINT opt, SQLULEN param)
{
  odbcDebug("SQLSetConnectOptionW, dbc:%p, opt:%d:%s, param:%d", dbc, opt, odbcConnectOptionString(opt), param);

  SQLRETURN ret = SQL_SUCCESS;

  HDBC_LOCK(dbc);
  ret = odbcSetConnectOption(dbc, opt, param);
  HDBC_UNLOCK(dbc);

  return ret;
}

/**
 * Handling of SQLDriverConnect() connection attributes
 * for standalone operation without driver manager.
 * @param dsn DSN/driver connection string
 * @param attr attribute string to be retrieved
 * @param out output buffer
 * @param outLen length of output buffer
 * @result true or false
 */

int odbcGetDsnAttr(char *dsn, char *attr, char *out, int outLen)
{
  char *str = dsn, *start;
  int len = (int)strlen(attr);

  while (*str) {
    while (*str && *str == ';') {
      ++str;
    }
    start = str;
    if ((str = strchr(str, '=')) == NULL) {
      return 0;
    }
    if (str - start == len && strncasecmp(start, attr, (size_t)len) == 0) {
      start = ++str;
      while (*str && *str != ';') {
        ++str;
      }
      len = (int)min(outLen - 1, str - start);
      strncpy(out, start, (size_t)len);
      out[len] = '\0';
      return 1;
    }
    while (*str && *str != ';') {
      ++str;
    }
  }
  return 0;
}

SQLRETURN 
odbcTaosConnect(DBC *d, char *dsn, char *serverInput, char *dbnameInput, char * userInput, char *passInput)
{
  if (d->con != NULL) {
    odbcError("dbc:%p, taos:%p, connection already established", d, d->con);
    setstatd(d, -1, "connection already established", "08002");
    return SQL_ERROR;
  }

  d->con = 0;
  d->sqlstate[0] = '\0';
  d->logmsg[0] = '\0';
  d->naterr = 0;

  char user[TSDB_USER_LEN] = { 0 };
  char pass[TSDB_KEY_LEN] = { 0 };
  char server[TSDB_IPv4ADDR_LEN] = { 0 };
  char dbname[TSDB_DB_NAME_LEN] = { 0 };

  if (serverInput != NULL) {
    tstrncpy(server, serverInput, sizeof(server));
  }
  if (dbnameInput != NULL) {
    tstrncpy(dbname, dbnameInput, sizeof(dbname));
  }
  if (userInput != NULL) {
    tstrncpy(user, userInput, sizeof(user));
  }
  if (passInput != NULL) {
    tstrncpy(pass, passInput, sizeof(pass));
  }

  if (strlen(server) == 0) {
    SQLGetPrivateProfileString(dsn, "SERVER", "", server, sizeof(server), ODBC_INI);
  }
  if (strlen(dbname) == 0) {
    SQLGetPrivateProfileString(dsn, "DATABASE", "", dbname, sizeof(dbname), ODBC_INI);
  }
  if (strlen(user) == 0) {
    SQLGetPrivateProfileString(dsn, "UID", "", user, sizeof(user), ODBC_INI);
  }
  if (strlen(pass) == 0) {
    SQLGetPrivateProfileString(dsn, "PWD", "", pass, sizeof(pass), ODBC_INI);
  }

  if (strlen(server) == 0) {
    strcpy(server, "127.0.0.1");
  }
  if (strlen(user) == 0) {
    strcpy(user, tsDefaultUser);
  }
  if (strlen(pass) == 0) {
    strcpy(pass, tsDefaultPass);
  }

  d->ov3 = true;
  strcpy(d->server, server);
  strcpy(d->dbname, dbname);
  strcpy(d->user, user);
  strcpy(d->pwd, pass);

  TAOS *taos = taos_connect(server, user, pass, dbname, 0);
  if (taos == NULL) {
    odbcError("dbc:%p, failed to connect to taos, reason:%s", d, taos_errstr(taos));
    //strncpy(d->logmsg, taos_errstr(taos), MAX_ERROR_LEN);
    setstatd(d, taos_errno(taos), taos_errstr(taos), (d->ov3) ? "HY000" : "S1000");
    return SQL_ERROR;
  }

  d->con = taos;

  odbcDebug("dbc:%p, connect to taos:%p success, server:%s, dbname:%s, user:%s", d, taos, server, dbname, user);
  return SQL_SUCCESS;
}

/**
 * Connect to SQLite database.
 * @param dbc database connection handle
 * @param dsn DSN string
 * @param dsnLen length of DSN string or SQL_NTS
 * @param uid user id string or NULL
 * @param uidLen length of user id string or SQL_NTS
 * @param pwd password string or NULL
 * @param pwdLen length of password string or SQL_NTS
 * @result ODBC error code
 */
SQLRETURN SQL_API 
SQLConnect(SQLHDBC dbc, SQLCHAR *dsn, SQLSMALLINT dsnLen,
  SQLCHAR *uid, SQLSMALLINT uidLen,
  SQLCHAR *pwd, SQLSMALLINT pwdLen)
{
  odbcDebug("SQLConnect, dbc:%p dsn:%s, uid:%s", dbc, dsn, uid);
  SQLRETURN ret = SQL_SUCCESS;

  HDBC_LOCK(dbc);
  ret = odbcTaosConnect((DBC*)dbc, (char*)dsn, (char*)"", (char*)"", (char*)uid, (char*)pwd);
  HDBC_UNLOCK(dbc);

  return ret;
}

SQLRETURN SQL_API SQLConnectW(SQLHDBC dbc, SQLWCHAR *dsn, SQLSMALLINT dsnLen,
  SQLWCHAR *uid, SQLSMALLINT uidLen,
  SQLWCHAR *pwd, SQLSMALLINT pwdLen)
{
  SQLRETURN ret = SQL_SUCCESS;
  SQLCHAR *dsnBuf = (SQLCHAR *)uc_to_utf_c(dsn, dsnLen);
  SQLCHAR *uidBuf = (SQLCHAR *)uc_to_utf_c(uid, uidLen);
  SQLCHAR *pwdBuf = (SQLCHAR *)uc_to_utf_c(pwd, pwdLen);
  odbcDebug("SQLConnectW, dbc:%p dsn:%s, uid:%s", dbc, dsnBuf, uidBuf);

  HDBC_LOCK(dbc);
  ret = odbcTaosConnect((DBC*)dbc, (char*)dsnBuf, "", "", (char*)uidBuf, (char*)pwdBuf);
  HDBC_UNLOCK(dbc);

  free(dsnBuf);
  free(uidBuf);
  free(pwdBuf);

  return ret;
}

/**
 * Disconnect given HDBC.
 * @param dbc database connection handle
 * @result ODBC error code
 */

SQLRETURN SQL_API SQLDisconnect(SQLHDBC dbc)
{
  if (dbc == SQL_NULL_HDBC) {
    odbcError("SQLDisconnect, dbc:%p", dbc);
    return SQL_INVALID_HANDLE;
  }

  DBC *d = (DBC*)dbc;
  odbcDebug("SQLDisconnect, dbc:%p, con:%p", dbc, d->con);
  HDBC_LOCK(d);
  if (d->con != NULL) {
    taos_close(d->con);
    d->con = NULL;
  }
  HDBC_UNLOCK(dbc);
  return SQL_SUCCESS;
}

SQLRETURN 
odbcAllocStmt(SQLHDBC hdbc, SQLHSTMT *stmt)
{
  SQLRETURN ret = SQL_SUCCESS;
  HDBC_LOCK(hdbc);

  STMT *s;
  DBC *d = (DBC*)hdbc;
  
  s = (STMT*)malloc(sizeof(STMT));
  if (s == NULL) {
    *stmt = SQL_NULL_HSTMT;
    HDBC_UNLOCK(hdbc);
    return SQL_ERROR;
  }

  memset(s, 0, sizeof(STMT));
  s->signature = s;
  s->dbc = d;
  *stmt = (SQLHSTMT)s;

  HDBC_UNLOCK(hdbc);
  return ret;
}

/**
 * Allocate HSTMT given HDBC.
 * @param dbc database connection handle
 * @param stmt pointer to statement handle
 * @result ODBC error code
 */

SQLRETURN SQL_API 
SQLAllocStmt(SQLHDBC dbc, SQLHSTMT *stmt)
{
   SQLRETURN ret = odbcAllocStmt(dbc, stmt);
   odbcDebug("SQLAllocStmt, dbc:%p, stmt:%p", dbc, *stmt);
   return ret;
}

SQLRETURN 
odbcClearStmt(SQLHSTMT stmt) 
{
  SQLRETURN ret = SQL_SUCCESS;
  HSTMT_LOCK(stmt);

  STMT *s = (STMT*)stmt;
  if (s->result) {
    taos_free_result(s->result);
    s->result = NULL;
  }
  HSTMT_UNLOCK(stmt);

  return ret;
}

SQLRETURN 
odbcFreeStmt(SQLHSTMT stmt, SQLUSMALLINT opt)
{
  SQLRETURN ret = SQL_SUCCESS;

  odbcClearStmt(stmt);

  STMT *s = (STMT*)stmt;
  s->signature = 0;
  free(s);

  return ret;
}

/**
 * Free HSTMT.
 * @param stmt statement handle
 * @param opt SQL_RESET_PARAMS, SQL_UNBIND, SQL_CLOSE, or SQL_DROP
 * @result ODBC error code
 */

SQLRETURN SQL_API 
SQLFreeStmt(SQLHSTMT stmt, SQLUSMALLINT opt)
{
  odbcDebug("SQLFreeStmt, stmt:%p, opt:%s", stmt, odbcFreeStmtOptionString(opt));
  SQLRETURN ret = SQL_ERROR;

  switch (opt) {
  case SQL_RESET_PARAMS:
  case SQL_UNBIND:
  case SQL_CLOSE:
    ret = odbcClearStmt(stmt);
    break;
  case SQL_DROP:
    ret = odbcFreeStmt(stmt, opt);
    break;
  default:
    setstat(stmt, -1, "unsupported free stmt option", "HYC00");
  }

  return ret;
}

/**
 * Cancel HSTMT closing cursor.
 * @param stmt statement handle
 * @result ODBC error code
 */

SQLRETURN SQL_API SQLCancel(SQLHSTMT stmt)
{
  odbcError("SQLCancel not implemented, stmt:%p", stmt);
  return SQL_ERROR;
}

/**
 * Get cursor name of STMT.
 * @param stmt statement handle
 * @param cursor output buffer
 * @param buflen length of output buffer
 * @param lenp output length
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLGetCursorName(SQLHSTMT stmt, SQLCHAR *cursor, SQLSMALLINT buflen,
  SQLSMALLINT *lenp)
{
  odbcError("SQLGetCursorName not implemented, stmt:%p", stmt);
  return SQL_ERROR;
}

/**
 * Get cursor name of STMT (UNICODE version).
 * @param stmt statement handle
 * @param cursor output buffer
 * @param buflen length of output buffer
 * @param lenp output length
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLGetCursorNameW(SQLHSTMT stmt, SQLWCHAR *cursor, SQLSMALLINT buflen,
  SQLSMALLINT *lenp)
{
  odbcError("SQLGetCursorNameW not implemented, stmt:%p", stmt);
  return SQL_ERROR;
}

/**
 * Set cursor name on STMT.
 * @param stmt statement handle
 * @param cursor new cursor name
 * @param len length of cursor name or SQL_NTS
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLSetCursorName(SQLHSTMT stmt, SQLCHAR *cursor, SQLSMALLINT len)
{
  odbcError("SQLSetCursorName not implemented, stmt:%p", stmt);
  return SQL_ERROR;
}

/**
 * Set cursor name on STMT (UNICODE version).
 * @param stmt statement handle
 * @param cursor new cursor name
 * @param len length of cursor name or SQL_NTS
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLSetCursorNameW(SQLHSTMT stmt, SQLWCHAR *cursor, SQLSMALLINT len)
{
  odbcError("SQLSetCursorNameW not implemented, stmt:%p", stmt);
  return SQL_ERROR;
}

/**
 * Close open cursor.
 * @param stmt statement handle
 * @return ODBC error code
 */

SQLRETURN SQL_API
SQLCloseCursor(SQLHSTMT stmt)
{
  odbcError("SQLCloseCursor not implemented, stmt:%p", stmt);
  return SQL_ERROR;
}

/**
 * Allocate a HENV, HDBC, or HSTMT handle.
 * @param type handle type
 * @param input input handle (HENV, HDBC)
 * @param output pointer to output handle (HENV, HDBC, HSTMT)
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLAllocHandle(SQLSMALLINT type, SQLHANDLE input, SQLHANDLE *output)
{
  odbc_init();
  
  SQLRETURN ret = SQL_SUCCESS;
  switch (type) {
  case SQL_HANDLE_ENV:
    ret = odbcAllocEnv((SQLHENV*)output);
    break;
  case SQL_HANDLE_DBC:
    ret = odbcAllocDbc((SQLHENV)input, (SQLHDBC *)output);
    break;
  case SQL_HANDLE_STMT:
    ret = odbcAllocStmt((SQLHDBC)input, (SQLHSTMT *)output);
    break;
  }

  odbcDebug("SQLAllocHandle, input:%p, type:%s:%p", input, odbcHandleTypeString(type), *output);
  return ret;
}

/**
 * Free a HENV, HDBC, or HSTMT handle.
 * @param type handle type
 * @param h handle (HENV, HDBC, or HSTMT)
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLFreeHandle(SQLSMALLINT type, SQLHANDLE h)
{
  odbcDebug("SQLFreeHandle, type:%s, handle:%p", odbcHandleTypeString(type), h);
  switch (type) {
  case SQL_HANDLE_ENV:
    return odbcFreeEnv((SQLHENV)h);
  case SQL_HANDLE_DBC:
    return odbcFreeDbc((SQLHDBC)h);
  case SQL_HANDLE_STMT:
    return odbcFreeStmt((SQLHSTMT)h, SQL_DROP);
  }
  return SQL_ERROR;
}

/**
 * Bind C variable to column of result set.
 * @param stmt statement handle
 * @param col column number, starting at 1
 * @param type output type
 * @param val output buffer
 * @param max length of output buffer
 * @param lenp output length pointer
 * @result ODBC error code
 */

SQLRETURN SQL_API 
SQLBindCol(SQLHSTMT stmt, SQLUSMALLINT col, SQLSMALLINT type,
  SQLPOINTER val, SQLLEN maxLen, SQLLEN *lenp)
{
  odbcDebug("SQLBindCol, stmt:%p, col:%d, type:%d:%s, val:%p, max:%d", stmt, col, type, odbcCDataTypeString(type), val, maxLen);
  
  SQLRETURN ret = SQL_SUCCESS;
  STMT *s = (STMT*)stmt;
  HSTMT_LOCK(stmt);

  if (s->dbc->con == NULL) {
    odbcError("stmt:%p, connection is lost", stmt);
    setstat(s, -1, "connection is lost", "HY000");
    ret = SQL_INVALID_HANDLE;
    goto done;
  }

  if (s->type == STMT_NORMAL_SQL && s->numFields == 0) {
    odbcError("stmt:%p, no result set was found", stmt);
    setstat(s, -1, "no result set was found", "HY000");
    ret = SQL_ERROR;
    goto done;
  }

  if (col <= 0) {
    odbcError("stmt:%p, column number was 0", stmt);
    setstat(s, -1, "column number was 0", "07006");
    ret = SQL_ERROR;
    goto done;
  }
  
  if (s->type == STMT_NORMAL_SQL) {
    if (col > s->numFields) {
      odbcError("stmt:%p, column number exceeded the maximum number of columns in the result set", stmt);
      setstat(s, -1, "column number exceeded the maximum number of columns in the result set", "07009");
      ret = SQL_ERROR;
      goto done;
    }
  }

  if (val == NULL) {
    odbcDebug("stmt:%p, column pointer was null, unbind this col:%d", stmt, col);
    //setstat(s, -1, "column pointer was null", "HY000");
    //ret = SQL_ERROR;
    //goto done;
  }

  if (maxLen < 0) {
    odbcError("stmt:%p, column max length was less than 0", stmt);
    setstat(s, -1, "column max length was less than 0", "HY090");
    ret = SQL_ERROR;
    goto done;
  }

  int c = col - 1;
  COL *bindCol = s->cols + c;
  bindCol->maxLen = (SQLLEN)(max(maxLen - 1, 0));
  bindCol->len = lenp;
  bindCol->val = val;
  bindCol->type = type;

done:
  HSTMT_UNLOCK(stmt);
  return ret;
}

void
odbcTrim(char *sql) {
  int i = 0; 
  while (sql[i] != '\0' && i < TSDB_MAX_SQL_LEN) {
    if (sql[i] == '\n' || sql[i] == '\r') {
      sql[i] = ' ';
    }
    ++i;
  }

  char *groupby = strstr(sql, "GROUP BY");
  if (groupby != NULL) {
    char *orderby = strstr(groupby, "ORDER BY");
    if (orderby != NULL) {
      for (char *p = groupby; p < orderby; ++p) {
        *p = ' ';
      }
    }

    char *having = strstr(sql, "HAVING ");
    if (having != NULL) {
      for (char *p = groupby; p < having; ++p) {
        *p = ' ';
      }
    }
  }

  char *orderby = strstr(sql, "ORDER BY 1 ASC");
  if (orderby != NULL) {
    for (int i = 0; i < 14; ++i) {
      orderby[i] = ' ';
    }
  } 

  char *having1 = strstr(sql, "HAVING COUNT(1)>0");
  if (having1 != NULL) {
    for (int i = 0; i < 17; ++i) {
      having1[i] = ' ';
    }
  }

  char *having2 = strstr(sql, "HAVING (COUNT(1) > 0)");
  if (having2 != NULL) {
    for (int i = 0; i < 21; ++i) {
      having2[i] = ' ';
    }
  }

  char *groupby1 = strstr(sql, "GROUP BY 1");
  if (groupby1 != NULL) {
    for (int i = 0; i < 10; ++i) {
      groupby1[i] = ' ';
    }
  }

  char *groupby2 = strstr(sql, "GROUP BY 2");
  if (groupby2 != NULL) {
    for (int i = 0; i < 10; ++i) {
      groupby2[i] = ' ';
    }
  }

  //char *top1 = strstr(sql, "TOP 1 *");
  //if (top1 != NULL) {
  //  top1[0] = 'f';
  //  top1[1] = 'i';
  //  top1[2] = 'r';
  //  top1[3] = 's';
  //  top1[4] = 't';
  //  top1[5] = '(';
  //  top1[6] = '*';
  //  top1[7] = ')';
  //}
}

SQLRETURN 
odbcExecuteSql(STMT *s)
{
  odbcTrim(s->sql);
  odbcDebug("taos:%p, type:%d:%s, sql:%s, last result set:%p", s->dbc->con, s->type, odbcStmtSqlType(s->type), s->sql, s->result);
  if (s->result != NULL) {
    taos_free_result(s->result);
  }
  s->result = NULL;
  s->fields = NULL;
  s->row = NULL;
  s->numFields = 0;
  s->rowsAffacted = 0;
  s->rowsFetched = 0;
  memset(s->cols, 0, sizeof(COL) * MAX_BIND_COL);
  
  int code = taos_query(s->dbc->con, s->sql);
  if (code != 0) {
    odbcError("failed to query from taos:%p, code:%d, reason:%s", s->dbc->con, code, taos_errstr(s->dbc->con));
    char *sqlState = NULL;
    switch (code) {
    case TSDB_CODE_TSC_INVALID_SQL:
      sqlState = "42000";
      break;
    case TSDB_CODE_MND_DB_NOT_SELECTED:
    case TSDB_CODE_MND_INVALID_DB:
      sqlState = "3D000";
      break;
    case TSDB_CODE_MND_TABLE_ALREADY_EXIST:
      sqlState = "42S01";
      break;
    case TSDB_CODE_MND_INVALID_TABLE_NAME:
      sqlState = "42S02";
      break;
    default:
      sqlState = "HY000";
      break;
    }
    setstatd(s->dbc, code, taos_errstr(s->dbc->con), sqlState);
    return SQL_ERROR;
  }

  int num_fields = taos_field_count(s->dbc->con);
  if (num_fields == 0) {
    s->result = NULL;
    s->numFields = 0;
    s->rowsAffacted = taos_affected_rows(s->dbc->con);
    odbcDebug("taos:%p, affect rows:%d", s->dbc->con, s->rowsAffacted);
    return SQL_SUCCESS;
  }
    
  s->result = taos_use_result(s->dbc->con);
  if (s->result == NULL) {
    odbcError("failed to use result from taos:%p, code:%d, reason:%s", s->dbc->con, taos_errno(s->dbc->con), taos_errstr(s->dbc->con));
    setstatd(s->dbc, -1, "failed to use result from taos", "HY000");
    return SQL_ERROR;
  }
   
  s->fields = taos_fetch_fields(s->result);
  s->numFields = taos_field_count(s->dbc->con);
  odbcDebug("taos:%p, result set:%p, fields:%p, numFields:%d", s->dbc->con, s->result, s->fields, s->numFields);

  if (s->fields == NULL) {
    odbcError("failed to fetch fields schema from taos:%p, code:%d, reason:%s", s->dbc->con, taos_errno(s->dbc->con), taos_errstr(s->dbc->con));
    setstatd(s->dbc, -1, "failed to fetch fields schema from taos", "HY000");
    return SQL_ERROR;
  }
      
  if (s->type == STMT_NORMAL_SQL) {
    for (int c = 0; c < num_fields; ++c) {
      COL *bindCol = s->cols + c;
      TAOS_FIELD *field = &(s->fields[c]);
      strcpy(bindCol->fieldName, field->name);
      bindCol->fieldSize = field->bytes;
      bindCol->fieldType = field->type;
      switch (field->type) {
      case TSDB_DATA_TYPE_NCHAR:
        bindCol->fieldDisplaySize = field->bytes;
        bindCol->fieldScale = 0;
        break;
      case TSDB_DATA_TYPE_BINARY:
        bindCol->fieldDisplaySize = field->bytes;
        bindCol->fieldScale = 0;
        break;
      case TSDB_DATA_TYPE_BOOL:
        bindCol->fieldDisplaySize = 6;
        bindCol->fieldScale = 0;
        break;
      case TSDB_DATA_TYPE_TINYINT:
        bindCol->fieldDisplaySize = 6;
        bindCol->fieldScale = 0;
        break;
      case TSDB_DATA_TYPE_SMALLINT:
        bindCol->fieldDisplaySize = 7;
        bindCol->fieldScale = 0;
        break;
      case TSDB_DATA_TYPE_INT:
        bindCol->fieldDisplaySize = 11;
        bindCol->fieldScale = 0;
        break;
      case TSDB_DATA_TYPE_BIGINT:
        bindCol->fieldDisplaySize = 21;
        bindCol->fieldScale = 0;
        break;
      case TSDB_DATA_TYPE_FLOAT:
        bindCol->fieldDisplaySize = 20;
        bindCol->fieldScale = 5;
        break;
      case TSDB_DATA_TYPE_DOUBLE:
        bindCol->fieldDisplaySize = 25;
        bindCol->fieldScale = 9;
        break;
      case TSDB_DATA_TYPE_TIMESTAMP:
        bindCol->fieldDisplaySize = 21;
        if (taos_result_precision(s->result) == TSDB_TIME_PRECISION_MILLI) {
          bindCol->fieldScale = 3;
        }
        else {
          bindCol->fieldScale = 6;
        }
        break;
      }
    }
  }

  else if (s->type == STMT_DESCRIBE_COLUMNS_SQL) {
    strcpy((s->cols + 0)->fieldName, "TABLE_CAT");
    strcpy((s->cols + 1)->fieldName, "TABLE_SCHEM");
    strcpy((s->cols + 2)->fieldName, "TABLE_NAME");
    strcpy((s->cols + 3)->fieldName, "COLUMN_NAME");
    strcpy((s->cols + 4)->fieldName, "DATA_TYPE");
    strcpy((s->cols + 5)->fieldName, "TYPE_NAME");
    strcpy((s->cols + 6)->fieldName, "COLUMN_SIZE");
    strcpy((s->cols + 7)->fieldName, "COLUMN_SIZE");
    strcpy((s->cols + 8)->fieldName, "BUFFER_LENGTH");
    strcpy((s->cols + 9)->fieldName, "DECIMAL_DIGITS");
    strcpy((s->cols + 10)->fieldName, "NUM_PREC_RADIX");
    strcpy((s->cols + 11)->fieldName, "NULLABLE");
    strcpy((s->cols + 12)->fieldName, "REMARKS");
    strcpy((s->cols + 13)->fieldName, "SQL_DATA_TYPE");
    strcpy((s->cols + 14)->fieldName, "SQL_DATETIME_SUB");
    strcpy((s->cols + 15)->fieldName, "CHAR_OCTET_LENGTH");
    strcpy((s->cols + 16)->fieldName, "ORDINAL_POSITION");
    strcpy((s->cols + 17)->fieldName, "IS_NULLABLE");

    (s->cols + 0)->fieldSize = TSDB_DB_NAME_LEN - 1;
    (s->cols + 1)->fieldSize = TSDB_DB_NAME_LEN - 1;
    (s->cols + 2)->fieldSize = TSDB_TABLE_NAME_LEN - 1;
    (s->cols + 3)->fieldSize = TSDB_COL_NAME_LEN - 1;
    (s->cols + 4)->fieldSize = s->fields[1].bytes;
    (s->cols + 5)->fieldSize = TSDB_TABLE_NAME_LEN - 1;
    (s->cols + 6)->fieldSize = s->fields[2].bytes;
    (s->cols + 7)->fieldSize = 4;
    (s->cols + 8)->fieldSize = 2;
    (s->cols + 9)->fieldSize = 2;
    (s->cols + 10)->fieldSize = 2;
    (s->cols + 11)->fieldSize = TSDB_COL_NAME_LEN - 1;
    (s->cols + 12)->fieldSize = TSDB_COL_NAME_LEN - 1;
    (s->cols + 13)->fieldSize = 2;
    (s->cols + 14)->fieldSize = 2;
    (s->cols + 15)->fieldSize = 4;
    (s->cols + 16)->fieldSize = 4;
    (s->cols + 17)->fieldSize = TSDB_COL_NAME_LEN - 1;

    (s->cols + 0)->fieldType = TSDB_DATA_TYPE_BINARY;
    (s->cols + 1)->fieldType = TSDB_DATA_TYPE_BINARY;
    (s->cols + 2)->fieldType = TSDB_DATA_TYPE_BINARY;
    (s->cols + 3)->fieldType = TSDB_DATA_TYPE_BINARY;
    (s->cols + 4)->fieldType = TSDB_DATA_TYPE_INT;
    (s->cols + 5)->fieldType = TSDB_DATA_TYPE_BINARY;
    (s->cols + 6)->fieldType = TSDB_DATA_TYPE_INT;
    (s->cols + 7)->fieldType = TSDB_DATA_TYPE_INT;
    (s->cols + 8)->fieldType = TSDB_DATA_TYPE_SMALLINT;
    (s->cols + 9)->fieldType = TSDB_DATA_TYPE_SMALLINT;
    (s->cols + 10)->fieldType = TSDB_DATA_TYPE_SMALLINT;
    (s->cols + 11)->fieldType = TSDB_DATA_TYPE_BINARY;
    (s->cols + 12)->fieldType = TSDB_DATA_TYPE_BINARY;
    (s->cols + 13)->fieldType = TSDB_DATA_TYPE_SMALLINT;
    (s->cols + 14)->fieldType = TSDB_DATA_TYPE_SMALLINT;
    (s->cols + 15)->fieldType = TSDB_DATA_TYPE_INT;
    (s->cols + 16)->fieldType = TSDB_DATA_TYPE_INT;
    (s->cols + 17)->fieldType = TSDB_DATA_TYPE_BINARY;
    
    (s->cols + 0)->fieldDisplaySize = TSDB_DB_NAME_LEN - 1;
    (s->cols + 1)->fieldDisplaySize = TSDB_DB_NAME_LEN - 1;
    (s->cols + 2)->fieldDisplaySize = TSDB_TABLE_NAME_LEN - 1;
    (s->cols + 3)->fieldDisplaySize = TSDB_COL_NAME_LEN - 1;
    (s->cols + 4)->fieldDisplaySize = 4;
    (s->cols + 5)->fieldDisplaySize = TSDB_TABLE_NAME_LEN - 1;
    (s->cols + 6)->fieldDisplaySize = 5;
    (s->cols + 7)->fieldDisplaySize = 5;
    (s->cols + 8)->fieldDisplaySize = 5;
    (s->cols + 9)->fieldDisplaySize = 5;
    (s->cols + 10)->fieldDisplaySize = 5;
    (s->cols + 11)->fieldDisplaySize = TSDB_COL_NAME_LEN - 1;
    (s->cols + 12)->fieldDisplaySize = TSDB_COL_NAME_LEN - 1;
    (s->cols + 13)->fieldDisplaySize = 5;
    (s->cols + 14)->fieldDisplaySize = 5;
    (s->cols + 15)->fieldDisplaySize = 5;
    (s->cols + 16)->fieldDisplaySize = 5;
    (s->cols + 17)->fieldDisplaySize = TSDB_COL_NAME_LEN - 1;

    (s->cols + 0)->fieldScale = 0;
    (s->cols + 1)->fieldScale = 0;
    (s->cols + 2)->fieldScale = 0;
    (s->cols + 3)->fieldScale = 0;
    (s->cols + 4)->fieldScale = 0;
    (s->cols + 5)->fieldScale = 0;
    (s->cols + 6)->fieldScale = 0;
    (s->cols + 7)->fieldScale = 0;
    (s->cols + 8)->fieldScale = 0;
    (s->cols + 9)->fieldScale = 0;
    (s->cols + 10)->fieldScale = 0;
    (s->cols + 11)->fieldScale = 0;
    (s->cols + 12)->fieldScale = 0;
    (s->cols + 13)->fieldScale = 0;
    (s->cols + 14)->fieldScale = 0;
    (s->cols + 15)->fieldScale = 0;
    (s->cols + 16)->fieldScale = 0;
    (s->cols + 17)->fieldScale = 0;

  }
  else if (s->type == STMT_SHOW_DATABASE_SQL
    || s->type == STMT_SHOW_SCHEMA_SQL
    || s->type == STMT_SHOW_TABLES_TYPE_SQL
    || s->type == STMT_SHOW_TABLES_SQL
    || s->type == STMT_SHOW_STABLES_SQL) {
    strcpy((s->cols + 0)->fieldName, "TABLE_CAT");
    strcpy((s->cols + 1)->fieldName, "TABLE_SCHEM");
    strcpy((s->cols + 2)->fieldName, "TABLE_NAME");
    strcpy((s->cols + 3)->fieldName, "TABLE_TYPE");
    strcpy((s->cols + 4)->fieldName, "REMARKS");
    (s->cols + 0)->fieldSize = TSDB_DB_NAME_LEN - 1;
    (s->cols + 1)->fieldSize = TSDB_DB_NAME_LEN - 1;
    (s->cols + 2)->fieldSize = TSDB_TABLE_NAME_LEN - 1;
    (s->cols + 3)->fieldSize = TSDB_TABLE_NAME_LEN - 1;
    (s->cols + 4)->fieldSize = TSDB_TABLE_NAME_LEN - 1;
    (s->cols + 0)->fieldType = TSDB_DATA_TYPE_BINARY;
    (s->cols + 1)->fieldType = TSDB_DATA_TYPE_BINARY;
    (s->cols + 2)->fieldType = TSDB_DATA_TYPE_BINARY;
    (s->cols + 3)->fieldType = TSDB_DATA_TYPE_BINARY;
    (s->cols + 4)->fieldType = TSDB_DATA_TYPE_BINARY;
    (s->cols + 0)->fieldDisplaySize = TSDB_DB_NAME_LEN - 1;
    (s->cols + 1)->fieldDisplaySize = TSDB_DB_NAME_LEN - 1;
    (s->cols + 2)->fieldDisplaySize = TSDB_TABLE_NAME_LEN - 1;
    (s->cols + 3)->fieldDisplaySize = TSDB_TABLE_NAME_LEN - 1;
    (s->cols + 4)->fieldDisplaySize = TSDB_TABLE_NAME_LEN - 1;
    (s->cols + 0)->fieldScale = 0;
    (s->cols + 1)->fieldScale = 0;
    (s->cols + 2)->fieldScale = 0;
    (s->cols + 3)->fieldScale = 0;
    (s->cols + 4)->fieldScale = 0;
  }
  else if (s->type == STMT_PRIMARY_KEY_SQL) {
    strcpy((s->cols + 0)->fieldName, "TABLE_CAT");
    strcpy((s->cols + 1)->fieldName, "TABLE_SCHEM");
    strcpy((s->cols + 2)->fieldName, "TABLE_NAME");
    strcpy((s->cols + 3)->fieldName, "COLUMN_NAME");
    strcpy((s->cols + 4)->fieldName, "KEY_SEQ");
    strcpy((s->cols + 5)->fieldName, "PK_NAME");
    (s->cols + 0)->fieldSize = TSDB_DB_NAME_LEN - 1;
    (s->cols + 1)->fieldSize = TSDB_DB_NAME_LEN - 1;
    (s->cols + 2)->fieldSize = TSDB_TABLE_NAME_LEN - 1;
    (s->cols + 3)->fieldSize = TSDB_TABLE_NAME_LEN - 1;
    (s->cols + 4)->fieldSize = 5;
    (s->cols + 5)->fieldSize = TSDB_TABLE_NAME_LEN - 1;
    (s->cols + 0)->fieldType = TSDB_DATA_TYPE_BINARY;
    (s->cols + 1)->fieldType = TSDB_DATA_TYPE_BINARY;
    (s->cols + 2)->fieldType = TSDB_DATA_TYPE_BINARY;
    (s->cols + 3)->fieldType = TSDB_DATA_TYPE_BINARY;
    (s->cols + 4)->fieldType = TSDB_DATA_TYPE_SMALLINT;
    (s->cols + 5)->fieldType = TSDB_DATA_TYPE_BINARY;
    (s->cols + 0)->fieldDisplaySize = TSDB_DB_NAME_LEN - 1;
    (s->cols + 1)->fieldDisplaySize = TSDB_DB_NAME_LEN - 1;
    (s->cols + 2)->fieldDisplaySize = TSDB_TABLE_NAME_LEN - 1;
    (s->cols + 3)->fieldDisplaySize = TSDB_TABLE_NAME_LEN - 1;
    (s->cols + 4)->fieldDisplaySize = 5;
    (s->cols + 5)->fieldDisplaySize = TSDB_TABLE_NAME_LEN - 1;
    (s->cols + 0)->fieldScale = 0;
    (s->cols + 1)->fieldScale = 0;
    (s->cols + 2)->fieldScale = 0;
    (s->cols + 3)->fieldScale = 0;
    (s->cols + 4)->fieldScale = 0;
    (s->cols + 5)->fieldScale = 0;
  }
  else if (s->type == STMT_FOERIGN_KEY_SQL) {
    //SQL_NO_DATA
  }
  return SQL_SUCCESS;
}

//cat:db
//schema:(null)
//table:t
//type:'TABLE', 'VIEW', 'SYSTEM TABLE', 'GLOBAL TEMPORARY'

//If CatalogName is SQL_ALL_CATALOGS and SchemaName and TableName are empty strings, the result set contains a list of valid catalogs for the data source
//If SchemaName is SQL_ALL_SCHEMAS and CatalogName and TableName are empty strings, the result set contains a list of valid schemas for the data source.
//If TableType is SQL_ALL_TABLE_TYPES and CatalogName, SchemaName, and TableName are empty strings, the result set contains a list of valid table types for the data source.

SQLRETURN
odbcTables(SQLHSTMT stmt, char *cat, char *schema, char *table, char *type)
{
  STMT *taosStmt = (STMT*)stmt;
  SQLRETURN ret = SQL_SUCCESS;

  if (cat != NULL && strcmp(cat, SQL_ALL_CATALOGS) == 0) {
    taosStmt->type = STMT_SHOW_DATABASE_SQL;
    strcpy(taosStmt->sql, "show databases");
    ret = odbcExecuteSql(stmt);
  }
  else if (schema != NULL && strcmp(schema, SQL_ALL_SCHEMAS) == 0) {
    taosStmt->type = STMT_SHOW_SCHEMA_SQL;
    strcpy(taosStmt->sql, "show databases");
    ret = odbcExecuteSql(stmt);
  }
  else if (type != NULL && strcmp(type, SQL_ALL_TABLE_TYPES) == 0) {
    taosStmt->type = STMT_SHOW_TABLES_TYPE_SQL;
    taosStmt->fixedResultSetIndex = -1;
    return SQL_SUCCESS;
  }
  else {
    taosStmt->type = STMT_SHOW_STABLES_SQL;
    if (cat != NULL && strcmp(cat, SQL_ALL_CATALOGS) != 0) {
      strcpy(taosStmt->dbc->dbname, cat);
    }

    if (strlen(taosStmt->dbc->dbname) != 0) {
      if (table == NULL || strlen(table) == 0) {
        sprintf(taosStmt->sql, "show %s.stables", taosStmt->dbc->dbname);
      }
      else {
        sprintf(taosStmt->sql, "show %s.stables like '%s'", taosStmt->dbc->dbname, table);
      }
    }
    else {
      if (table == NULL || strlen(table) == 0) {
        sprintf(taosStmt->sql, "show stables");
      }
      else {
        sprintf(taosStmt->sql, "show stables like '%s'", table);
      }
    }
   
    ret = odbcExecuteSql(stmt);
  }

  return ret;
}

/**
 * Retrieve information on tables and/or views.
 * @param stmt statement handle
 * @param cat catalog name/pattern or NULL
 * @param catLen length of catalog name/pattern or SQL_NTS
 * @param schema schema name/pattern or NULL
 * @param schemaLen length of schema name/pattern or SQL_NTS
 * @param table table name/pattern or NULL
 * @param tableLen length of table name/pattern or SQL_NTS
 * @param type types of tables string or NULL
 * @param typeLen length of types of tables string or SQL_NTS
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLTables(SQLHSTMT stmt,
  SQLCHAR *cat, SQLSMALLINT catLen,
  SQLCHAR *schema, SQLSMALLINT schemaLen,
  SQLCHAR *table, SQLSMALLINT tableLen,
  SQLCHAR *type, SQLSMALLINT typeLen)
{
  odbcDebug("SQLTables, stmt:%p, cat:%s, schema:%s, table:%s, type:%s", stmt, cat, schema, table, type);
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcTables(stmt, (char*)cat, (char*)schema, (char*)table, (char*)type);
  HSTMT_UNLOCK(stmt);

  return ret;
}

/**
 * Retrieve information on tables and/or views.
 * @param stmt statement handle
 * @param cat catalog name/pattern or NULL
 * @param catLen length of catalog name/pattern or SQL_NTS
 * @param schema schema name/pattern or NULL
 * @param schemaLen length of schema name/pattern or SQL_NTS
 * @param table table name/pattern or NULL
 * @param tableLen length of table name/pattern or SQL_NTS
 * @param type types of tables string or NULL
 * @param typeLen length of types of tables string or SQL_NTS
 * @result ODBC error code
 */

SQLRETURN SQL_API 
SQLTablesW(SQLHSTMT stmt,
  SQLWCHAR *cat, SQLSMALLINT catLen,
  SQLWCHAR *schema, SQLSMALLINT schemaLen,
  SQLWCHAR *table, SQLSMALLINT tableLen,
  SQLWCHAR *type, SQLSMALLINT typeLen)
{
  char *c = NULL, *s = NULL, *t = NULL, *y = NULL;
  SQLRETURN ret = SQL_ERROR;

  if (cat) {
    c = uc_to_utf_c(cat, catLen);
    if (!c) {
      ret = nomem((STMT *)stmt);
      goto done;
    }
  }
  if (schema) {
    s = uc_to_utf_c(schema, schemaLen);
    if (!s) {
      ret = nomem((STMT *)stmt);
      goto done;
    }
  }
  if (table) {
    t = uc_to_utf_c(table, tableLen);
    if (!t) {
      ret = nomem((STMT *)stmt);
      goto done;
    }
  }
  if (type) {
    y = uc_to_utf_c(type, typeLen);
    if (!y) {
      ret = nomem((STMT *)stmt);
      goto done;
    }
  }

  odbcDebug("SQLTables, stmt:%p, cat:%s, schema:%s, table:%s, type:%s", stmt, c, s, t, y);
  
  HSTMT_LOCK(stmt);
  ret = odbcTables(stmt, c, s, t, y);
  HSTMT_UNLOCK(stmt);

done:
  uc_free(y);
  uc_free(t);
  uc_free(s);
  uc_free(c);
  return ret;
}

SQLRETURN
odbcColumns(SQLHSTMT stmt, char *cat, char *schema, char *table, char *columns)
{
  STMT *taosStmt = (STMT*)stmt;
  
  taosStmt->type = STMT_DESCRIBE_COLUMNS_SQL;
  if (cat != NULL && strcmp(cat, SQL_ALL_CATALOGS) != 0) {
    strcpy(taosStmt->dbc->dbname, cat);
  }
  if (table != NULL) {
    tstrncpy(taosStmt->dbc->tbname, table, sizeof(taosStmt->dbc->tbname));
  }

  if (strlen(taosStmt->dbc->dbname) != 0) {
    sprintf(taosStmt->sql, "describe %s.%s", taosStmt->dbc->dbname, taosStmt->dbc->tbname);
  }
  else {
    sprintf(taosStmt->sql, "describe %s", taosStmt->dbc->tbname);
  }

  return odbcExecuteSql(stmt);
}

/**
 * Retrieve column information on table.
 * @param stmt statement handle
 * @param cat catalog name/pattern or NULL
 * @param catLen length of catalog name/pattern or SQL_NTS
 * @param schema schema name/pattern or NULL
 * @param schemaLen length of schema name/pattern or SQL_NTS
 * @param table table name/pattern or NULL
 * @param tableLen length of table name/pattern or SQL_NTS
 * @param col column name/pattern or NULL
 * @param colLen length of column name/pattern or SQL_NTS
 * @result ODBC error code
 */

SQLRETURN SQL_API 
SQLColumns(SQLHSTMT stmt,
  SQLCHAR *cat, SQLSMALLINT catLen,
  SQLCHAR *schema, SQLSMALLINT schemaLen,
  SQLCHAR *table, SQLSMALLINT tableLen,
  SQLCHAR *col, SQLSMALLINT colLen)
{
  odbcDebug("SQLColumns, stmt:%p, cat:%s, schema:%s, table:%s, col:%s", stmt, cat, schema, table, col);
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcColumns(stmt, (char*)cat, (char*)schema, (char*)table, (char*)col);
  HSTMT_UNLOCK(stmt);

  return ret;
}

/**
 * Retrieve column information on table (UNICODE version).
 * @param stmt statement handle
 * @param cat catalog name/pattern or NULL
 * @param catLen length of catalog name/pattern or SQL_NTS
 * @param schema schema name/pattern or NULL
 * @param schemaLen length of schema name/pattern or SQL_NTS
 * @param table table name/pattern or NULL
 * @param tableLen length of table name/pattern or SQL_NTS
 * @param col column name/pattern or NULL
 * @param colLen length of column name/pattern or SQL_NTS
 * @result ODBC error code
 */

SQLRETURN SQL_API 
SQLColumnsW(SQLHSTMT stmt,
  SQLWCHAR *cat, SQLSMALLINT catLen,
  SQLWCHAR *schema, SQLSMALLINT schemaLen,
  SQLWCHAR *table, SQLSMALLINT tableLen,
  SQLWCHAR *col, SQLSMALLINT colLen)
{
  char *c = NULL, *s = NULL, *t = NULL, *k = NULL;
  SQLRETURN ret = SQL_SUCCESS;

  if (cat) {
    c = uc_to_utf_c(cat, catLen);
    if (!c) {
      ret = nomem((STMT *)stmt);
      goto done;
    }
  }
  if (schema) {
    s = uc_to_utf_c(schema, schemaLen);
    if (!s) {
      ret = nomem((STMT *)stmt);
      goto done;
    }
  }
  if (table) {
    t = uc_to_utf_c(table, tableLen);
    if (!t) {
      ret = nomem((STMT *)stmt);
      goto done;
    }
  }
  if (col) {
    k = uc_to_utf_c(col, colLen);
    if (!k) {
      ret = nomem((STMT *)stmt);
      goto done;
    }
  }

  odbcDebug("SQLColumnsW, stmt:%p, cat:%s, schema:%s, table:%s, col:%s", stmt, c, s, t, k);

  HSTMT_LOCK(stmt);
  ret = odbcColumns(stmt, c, s, t, k);
  HSTMT_UNLOCK(stmt);

done:
  uc_free(k);
  uc_free(t);
  uc_free(s);
  uc_free(c);

  return ret;
}

SQLRETURN
odbcGetTypeInfo(SQLHSTMT stmt, SQLSMALLINT sqltype)
{
  return odbcUnImplStmt(stmt);
}

/**
 * Return data type information.
 * @param stmt statement handle
 * @param sqltype which type to retrieve
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLGetTypeInfo(SQLHSTMT stmt, SQLSMALLINT sqltype)
{
  odbcError("SQLGetTypeInfo not implemented, stmt:%p, sqltype:%d:%s", stmt, sqltype, odbcSqlTypeinfoString(sqltype));
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcGetTypeInfo(stmt, sqltype);
  HSTMT_UNLOCK(stmt);
  return ret;
}

/**
 * Return data type information (UNICODE version).
 * @param stmt statement handle
 * @param sqltype which type to retrieve
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLGetTypeInfoW(SQLHSTMT stmt, SQLSMALLINT sqltype)
{
  odbcError("SQLGetTypeInfoW not implemented, stmt:%p, sqltype:%d:%s", stmt, sqltype, odbcSqlTypeinfoString(sqltype));
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcGetTypeInfo(stmt, sqltype);
  HSTMT_UNLOCK(stmt);
  return ret;
}

SQLRETURN
odbcStatistics(SQLHSTMT stmt, SQLCHAR *cat,
  SQLCHAR *schema,
  SQLCHAR *table)
{
  return odbcUnImplStmt(stmt);
}

/**
 * Return statistic information on table indices.
 * @param stmt statement handle
 * @param cat catalog name/pattern or NULL
 * @param catLen length of catalog name/pattern or SQL_NTS
 * @param schema schema name/pattern or NULL
 * @param schemaLen length of schema name/pattern or SQL_NTS
 * @param table table name/pattern or NULL
 * @param tableLen length of table name/pattern or SQL_NTS
 * @param itype type of index information
 * @param resv reserved
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLStatistics(SQLHSTMT stmt, SQLCHAR *cat, SQLSMALLINT catLen,
  SQLCHAR *schema, SQLSMALLINT schemaLen,
  SQLCHAR *table, SQLSMALLINT tableLen,
  SQLUSMALLINT itype, SQLUSMALLINT resv)
{
  odbcError("SQLStatistics not implemented, stmt:%p", stmt);
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcStatistics(stmt, cat, schema, table);
  HSTMT_UNLOCK(stmt);
  return ret;
}

/**
 * Return statistic information on table indices (UNICODE version).
 * @param stmt statement handle
 * @param cat catalog name/pattern or NULL
 * @param catLen length of catalog name/pattern or SQL_NTS
 * @param schema schema name/pattern or NULL
 * @param schemaLen length of schema name/pattern or SQL_NTS
 * @param table table name/pattern or NULL
 * @param tableLen length of table name/pattern or SQL_NTS
 * @param itype type of index information
 * @param resv reserved
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLStatisticsW(SQLHSTMT stmt, SQLWCHAR *cat, SQLSMALLINT catLen,
  SQLWCHAR *schema, SQLSMALLINT schemaLen,
  SQLWCHAR *table, SQLSMALLINT tableLen,
  SQLUSMALLINT itype, SQLUSMALLINT resv)
{
  odbcError("SQLStatisticsW not implemented, stmt:%p", stmt);
  
  char *c = NULL, *s = NULL, *t = NULL;
  SQLRETURN ret = SQL_SUCCESS;

  if (cat) {
    c = uc_to_utf_c(cat, catLen);
    if (!c) {
      ret = nomem((STMT *)stmt);
      goto done;
    }
  }
  if (schema) {
    s = uc_to_utf_c(schema, schemaLen);
    if (!s) {
      ret = nomem((STMT *)stmt);
      goto done;
    }
  }
  if (table) {
    t = uc_to_utf_c(table, tableLen);
    if (!t) {
      ret = nomem((STMT *)stmt);
      goto done;
    }
  }

  HSTMT_LOCK(stmt);
  ret = odbcStatistics(stmt, (SQLCHAR*)c, (SQLCHAR*)s, (SQLCHAR*)t);
  HSTMT_UNLOCK(stmt);

done:
  uc_free(t);
  uc_free(s);
  uc_free(c);
  return ret;
}

bool
odbcSetColumnBoolValue(COL* col, int8_t rawData)
{
  switch (col->type) {
  case SQL_C_BIT:
    *(uint8_t*)(col->val) = (uint8_t)rawData;
    *col->len = sizeof(int8_t);
    return true;
  case SQL_C_STINYINT:
  case SQL_C_UTINYINT:
    *(int8_t*)(col->val) = rawData;
    *col->len = sizeof(int8_t);
    return true;
  case SQL_C_SSHORT:
  case SQL_C_USHORT:
    *(int16_t*)(col->val) = rawData;
    *col->len = sizeof(int16_t);
    return true;
  case SQL_C_SLONG:
  case SQL_C_ULONG:
    *(int32_t*)(col->val) = rawData;
    *col->len = sizeof(int32_t);
    return true;
  case SQL_C_SBIGINT:
  case SQL_C_UBIGINT:
    *(int64_t*)(col->val) = rawData;
    *col->len = sizeof(int64_t);
    return true;
  case SQL_C_FLOAT:
    *(float*)(col->val) = rawData;
    *col->len = sizeof(float);
    return true;
  case SQL_C_DOUBLE:
    *(double*)(col->val) = rawData;
    *col->len = sizeof(double);
    return true;
  case SQL_C_BINARY:
  case SQL_C_CHAR:
    *col->len = snprintf(col->val, col->maxLen, "%s", rawData == 1 ? "true" : "false");
    return true;
  default:
    odbcError("bind type:%d:%s not suitable for native type:%d:%s", col->type, odbcCDataTypeString((SQLSMALLINT)col->type), col->fieldType, odbcDataTypeTDengineString(col->fieldType));
    return false;
  }
}

bool 
odbcSetColumnInt8Value(COL* col, int8_t rawData)
{
  switch (col->type) {
  case SQL_C_BIT:
    *(uint8_t*)(col->val) = (uint8_t)rawData;
    *col->len = sizeof(int8_t);
    return true;
  case SQL_C_STINYINT:
  case SQL_C_UTINYINT:
    *(int8_t*)(col->val) = rawData;
    *col->len = sizeof(int8_t);
    return true;
  case SQL_C_SSHORT:
  case SQL_C_USHORT:
    *(int16_t*)(col->val) = rawData;
    *col->len = sizeof(int16_t);
    return true;
  case SQL_C_SLONG:
  case SQL_C_ULONG:
    *(int32_t*)(col->val) = rawData;
    *col->len = sizeof(int32_t);
    return true;
  case SQL_C_SBIGINT:
  case SQL_C_UBIGINT:
    *(int64_t*)(col->val) = rawData;
    *col->len = sizeof(int64_t);
    return true;
  case SQL_C_FLOAT:
    *(float*)(col->val) = rawData;
    *col->len = sizeof(float);
    return true;
  case SQL_C_DOUBLE:
    *(double*)(col->val) = rawData;
    *col->len = sizeof(double);
    return true;
  case SQL_C_BINARY:
  case SQL_C_CHAR:
    *col->len = snprintf(col->val, col->maxLen, "%d", rawData);
    return true;
  default:
    odbcError("bind type:%d:%s not suitable for native type:%d:%s", col->type, odbcCDataTypeString((SQLSMALLINT)col->type), col->fieldType, odbcDataTypeTDengineString(col->fieldType));
    return false;
  }
}

bool
odbcSetColumnInt16Value(COL* col, int16_t rawData)
{
  switch (col->type) {
  case SQL_C_BIT:
    *(uint8_t*)(col->val) = (rawData != 0);
    *col->len = sizeof(int8_t);
    return true;
  case SQL_C_STINYINT:
  case SQL_C_UTINYINT:
    *(int8_t*)(col->val) = (int8_t)rawData;
    *col->len = sizeof(int8_t);
    return true;
  case SQL_C_SSHORT:
  case SQL_C_USHORT:
    *(int16_t*)(col->val) = (int16_t)rawData;
    *col->len = sizeof(int16_t);
    return true;
  case SQL_C_SLONG:
  case SQL_C_ULONG:
    *(int32_t*)(col->val) = (int32_t)rawData;
    *col->len = sizeof(int32_t);
    return true;
  case SQL_C_SBIGINT:
  case SQL_C_UBIGINT:
    *(int64_t*)(col->val) = (int64_t)rawData;
    *col->len = sizeof(int64_t);
    return true;
  case SQL_C_FLOAT:
    *(float*)(col->val) = (float)rawData;
    *col->len = sizeof(float);
    return true;
  case SQL_C_DOUBLE:
    *(double*)(col->val) = (double)rawData;
    *col->len = sizeof(double);
    return true;
  case SQL_C_BINARY:
  case SQL_C_CHAR:
    *col->len = snprintf(col->val, col->maxLen, "%d", rawData);
    return true;
  default:
    odbcError("bind type:%d:%s not suitable for native type:%d:%s", col->type, odbcCDataTypeString((SQLSMALLINT)col->type), col->fieldType, odbcDataTypeTDengineString(col->fieldType));
    return false;
  }
}

bool
odbcSetColumnInt32Value(COL* col, int32_t rawData)
{
  switch (col->type) {
  case SQL_C_BIT:
    *(uint8_t*)(col->val) = (rawData != 0);
    *col->len = sizeof(int8_t);
    return true;
  case SQL_C_STINYINT:
  case SQL_C_UTINYINT:
    *(int8_t*)(col->val) = (int8_t)rawData;
    *col->len = sizeof(int8_t);
    return true;
  case SQL_C_SSHORT:
  case SQL_C_USHORT:
    *(int16_t*)(col->val) = (int16_t)rawData;
    *col->len = sizeof(int16_t);
    return true;
  case SQL_C_SLONG:
  case SQL_C_ULONG:
    *(int32_t*)(col->val) = (int32_t)rawData;
    *col->len = sizeof(int32_t);
    return true;
  case SQL_C_SBIGINT:
  case SQL_C_UBIGINT:
    *(int64_t*)(col->val) = (int64_t)rawData;
    *col->len = sizeof(int64_t);
    return true;
  case SQL_C_FLOAT:
    *(float*)(col->val) = (float)rawData;
    *col->len = sizeof(float);
    return true;
  case SQL_C_DOUBLE:
    *(double*)(col->val) = (double)rawData;
    *col->len = sizeof(double);
    return true;
  case SQL_C_BINARY:
  case SQL_C_CHAR:
    *col->len = snprintf(col->val, col->maxLen, "%d", rawData);
    return true;
  default:
    odbcError("bind type:%d:%s not suitable for native type:%d:%s", col->type, odbcCDataTypeString((SQLSMALLINT)col->type), col->fieldType, odbcDataTypeTDengineString(col->fieldType));
    return false;
  }
}

bool
odbcSetColumnInt64Value(COL* col, int64_t rawData)
{
  switch (col->type) {
  case SQL_C_BIT:
    *(uint8_t*)(col->val) = (rawData != 0);
    *col->len = sizeof(int8_t);
    return true;
  case SQL_C_STINYINT:
  case SQL_C_UTINYINT:
    *(int8_t*)(col->val) = (int8_t)rawData;
    *col->len = sizeof(int8_t);
    return true;
  case SQL_C_SSHORT:
  case SQL_C_USHORT:
    *(int16_t*)(col->val) = (int16_t)rawData;
    *col->len = sizeof(int16_t);
    return true;
  case SQL_C_SLONG:
  case SQL_C_ULONG:
    *(int32_t*)(col->val) = (int32_t)rawData;
    *col->len = sizeof(int32_t);
    return true;
  case SQL_C_SBIGINT:
  case SQL_C_UBIGINT:
    *(int64_t*)(col->val) = (int64_t)rawData;
    *col->len = sizeof(int64_t);
    return true;
  case SQL_C_FLOAT:
    *(float*)(col->val) = (float)rawData;
    *col->len = sizeof(float);
    return true;
  case SQL_C_DOUBLE:
    *(double*)(col->val) = (double)rawData;
    *col->len = sizeof(double);
    return true;
  case SQL_C_BINARY:
  case SQL_C_CHAR:
    *col->len = snprintf(col->val, col->maxLen, "%lld", rawData);
    return true;
  default:
    odbcError("bind type:%d:%s not suitable for native type:%d:%s", col->type, odbcCDataTypeString((SQLSMALLINT)col->type), col->fieldType, odbcDataTypeTDengineString(col->fieldType));
    return false;
  }
}

bool
odbcSetColumnTimestampValue(COL* col, int64_t rawData, void *result)
{
  char buf[25] = "\0";
  time_t tt;
  struct tm  *ptm;
  bool isMicro = (taos_result_precision(result) == TSDB_TIME_PRECISION_MICRO);

  switch (col->type) {
  case SQL_C_BIT:
    *(uint8_t*)(col->val) = (rawData != 0);
    *col->len = sizeof(int8_t);
    return true;
  case SQL_C_STINYINT:
  case SQL_C_UTINYINT:
    *(int8_t*)(col->val) = (int8_t)rawData;
    *col->len = sizeof(int8_t);
    return true;
  case SQL_C_SSHORT:
  case SQL_C_USHORT:
    *(int16_t*)(col->val) = (int16_t)rawData;
    *col->len = sizeof(int16_t);
    return true;
  case SQL_C_SLONG:
  case SQL_C_ULONG:
    *(int32_t*)(col->val) = (int32_t)rawData;
    *col->len = sizeof(int32_t);
    return true;
  case SQL_C_SBIGINT:
  case SQL_C_UBIGINT:
    *(int64_t*)(col->val) = (int64_t)rawData;
    *col->len = sizeof(int64_t);
    return true;
  case SQL_C_FLOAT:
    *(float*)(col->val) = (float)rawData;
    *col->len = sizeof(float);
    return true;
  case SQL_C_DOUBLE:
    *(double*)(col->val) = (double)rawData;
    *col->len = sizeof(double);
    return true;
  case SQL_C_TYPE_TIMESTAMP: {
      time_t tt;
      int remain;
      if (isMicro) {
        tt = (int)(rawData / 1000000);
        remain = (int)(rawData % 1000000); 
      }
      else {
        tt = (int)(rawData / 1000);
        remain = (int)(rawData % 1000);
      }

      struct tm * ptm = localtime(&tt);
      TIMESTAMP_STRUCT *ts = (TIMESTAMP_STRUCT*)(col->val);
      ts->year = (SQLSMALLINT)(ptm->tm_year + 1900);
      ts->month = (SQLUSMALLINT)(ptm->tm_mon + 1);
      ts->day = (SQLUSMALLINT)ptm->tm_mday;
      ts->hour = (SQLUSMALLINT)ptm->tm_hour;
      ts->minute = (SQLUSMALLINT)ptm->tm_min;
      ts->second = (SQLUSMALLINT)ptm->tm_sec;
      ts->fraction = (SQLUINTEGER)remain;

      *col->len = sizeof(TIMESTAMP_STRUCT);
    }
    return true;
  case SQL_C_BINARY:
  case SQL_C_CHAR:
    if (isMicro) {
      tt = rawData / 1000000;
    }
    else {
      tt = rawData / 1000;
    }

    ptm = localtime(&tt);
    strftime(buf, 24, "%y-%m-%d %H:%M:%S", ptm);

    if (isMicro) {
      *col->len = snprintf(col->val, col->maxLen, "%s.%06d", buf, rawData % 1000000);
    }
    else {
      *col->len = snprintf(col->val, col->maxLen, "%s.%03d", buf, rawData % 1000);
    }
    return true;
  default:
    odbcError("bind type:%d:%s not suitable for native type:%d:%s", col->type, odbcCDataTypeString((SQLSMALLINT)col->type), col->fieldType, odbcDataTypeTDengineString(col->fieldType));
    return false;
  }
}

bool
odbcSetColumnFloatValue(COL* col, float rawData)
{
  switch (col->type) {
  case SQL_C_BIT:
    *(uint8_t*)(col->val) = (rawData != 0);
    *col->len = sizeof(int8_t);
    return true;
  case SQL_C_STINYINT:
  case SQL_C_UTINYINT:
    *(int8_t*)(col->val) = (int8_t)rawData;
    *col->len = sizeof(int8_t);
    return true;
  case SQL_C_SSHORT:
  case SQL_C_USHORT:
    *(int16_t*)(col->val) = (int16_t)rawData;
    *col->len = sizeof(int16_t);
    return true;
  case SQL_C_SLONG:
  case SQL_C_ULONG:
    *(int32_t*)(col->val) = (int32_t)rawData;
    *col->len = sizeof(int32_t);
    return true;
  case SQL_C_SBIGINT:
  case SQL_C_UBIGINT:
    *(int64_t*)(col->val) = (int64_t)rawData;
    *col->len = sizeof(int64_t);
    return true;
  case SQL_C_FLOAT:
    *(float*)(col->val) = (float)rawData;
    *col->len = sizeof(float);
    return true;
  case SQL_C_DOUBLE:
    *(double*)(col->val) = (double)rawData;
    *col->len = sizeof(double);
    return true;
  case SQL_C_BINARY:
  case SQL_C_CHAR:
    *col->len = snprintf(col->val, col->maxLen, "%.5f", rawData);
    return true;
  default:
    odbcError("bind type:%d:%s not suitable for native type:%d:%s", col->type, odbcCDataTypeString((SQLSMALLINT)col->type), col->fieldType, odbcDataTypeTDengineString(col->fieldType));
    return false;
  }
}

bool
odbcSetColumnDoubleValue(COL* col, double rawData)
{
  switch (col->type) {
  case SQL_C_BIT:
    *(uint8_t*)(col->val) = (rawData != 0);
    *col->len = sizeof(int8_t);
    return true;
  case SQL_C_STINYINT:
  case SQL_C_UTINYINT:
    *(int8_t*)(col->val) = (int8_t)rawData;
    *col->len = sizeof(int8_t);
    return true;
  case SQL_C_SSHORT:
  case SQL_C_USHORT:
    *(int16_t*)(col->val) = (int16_t)rawData;
    *col->len = sizeof(int16_t);
    return true;
  case SQL_C_SLONG:
  case SQL_C_ULONG:
    *(int32_t*)(col->val) = (int32_t)rawData;
    *col->len = sizeof(int32_t);
    return true;
  case SQL_C_SBIGINT:
  case SQL_C_UBIGINT:
    *(int64_t*)(col->val) = (int64_t)rawData;
    *col->len = sizeof(int64_t);
    return true;
  case SQL_C_FLOAT:
    *(float*)(col->val) = (float)rawData;
    *col->len = sizeof(float);
    return true;
  case SQL_C_DOUBLE:
    *(double*)(col->val) = (double)rawData;
    *col->len = sizeof(double);
    return true;
  case SQL_C_BINARY:
  case SQL_C_CHAR:
    *col->len = snprintf(col->val, col->maxLen, "%.9f", rawData);
    return true;
  default:
    odbcError("bind type:%d:%s not suitable for native type:%d:%s", col->type, odbcCDataTypeString((SQLSMALLINT)col->type), col->fieldType, odbcDataTypeTDengineString(col->fieldType));
    return false;
  }
}

bool
odbcSetColumnBinaryValue(COL* col, char* rawData)
{
  int len;

  switch (col->type) {
  case SQL_C_BIT:
  case SQL_C_STINYINT:
  case SQL_C_UTINYINT:
  case SQL_C_SSHORT:
  case SQL_C_USHORT:
  case SQL_C_SLONG:
  case SQL_C_ULONG:
  case SQL_C_SBIGINT:
  case SQL_C_UBIGINT:
  case SQL_C_FLOAT:
  case SQL_C_DOUBLE:
    odbcError("bind type:%d:%s not suitable for native type:%d:%s", col->type, odbcCDataTypeString((SQLSMALLINT)col->type), col->fieldType, odbcDataTypeTDengineString(col->fieldType));
    return false;
  case SQL_C_BINARY:
  case SQL_C_CHAR:
    len = (int)min(col->maxLen, (SQLLEN)strlen(rawData));
    strncpy(col->val, rawData, (size_t)len);
    ((char*)col->val)[len] = 0;
    *(col->len) = len;
    return true;
  case SQL_C_WCHAR:
  {
    len = (int)min(col->maxLen, (SQLLEN)strlen(rawData));
    uc_from_utf_buf((unsigned char*)rawData, -1, (SQLWCHAR *)col->val, len*(int)(sizeof(SQLWCHAR)));
    ////strncpy(col->val, rawData, len);
    //wchar_t * uc = uc_to_utf_c(col->val, len);
    //wcsncpy(col->val, uc, len);
    *(col->len) = len;
    ((SQLWCHAR*)col->val)[len] = 0;
    return true;
  }
  default:
    odbcError("bind type:%d:%s not suitable for native type:%d:%s", col->type, odbcCDataTypeString((SQLSMALLINT)col->type), col->fieldType, odbcDataTypeTDengineString(col->fieldType));
    return false;
  }
}

bool
odbcSetColumnValue(COL* col, void *rawData, void *result)
{
  if (col == NULL || col->val == NULL) return false;

  switch (col->fieldType) {
  case TSDB_DATA_TYPE_BOOL:
    return odbcSetColumnBoolValue(col, *(int8_t *)rawData);
  case TSDB_DATA_TYPE_TINYINT:
    return odbcSetColumnInt8Value(col, *(int8_t *)rawData);
  case TSDB_DATA_TYPE_SMALLINT:
    return odbcSetColumnInt16Value(col, *(int16_t *)rawData);
  case TSDB_DATA_TYPE_INT:
    return odbcSetColumnInt32Value(col, *(int32_t *)rawData);
  case TSDB_DATA_TYPE_BIGINT:
    return odbcSetColumnInt64Value(col, *(int64_t *)rawData);
  case TSDB_DATA_TYPE_FLOAT:
    return odbcSetColumnFloatValue(col, *(float *)rawData);
  case TSDB_DATA_TYPE_DOUBLE:
    return odbcSetColumnDoubleValue(col, *(double *)rawData);
  case TSDB_DATA_TYPE_BINARY:
  case TSDB_DATA_TYPE_NCHAR:
    return odbcSetColumnBinaryValue(col, (char*)rawData);
  case TSDB_DATA_TYPE_TIMESTAMP:
    return odbcSetColumnTimestampValue(col, *(int64_t*)rawData, result);
  default:
    odbcError("bind type:%d:%s not suitable for native type:%d:%s", col->type, odbcCDataTypeString((SQLSMALLINT)col->type), col->fieldType, odbcDataTypeTDengineString(col->fieldType));
    return false;
  }
}

/**
 * Retrieve row data after fetch.
 * @param stmt statement handle
 * @param col column number, starting at 1
 * @param type output type
 * @param val output buffer
 * @param len length of output buffer
 * @param lenp output length
 * @result ODBC error code
 */

SQLRETURN SQL_API SQLGetData(SQLHSTMT stmt, SQLUSMALLINT col, SQLSMALLINT type,
  SQLPOINTER val, SQLLEN len, SQLLEN *lenp)
{
  odbcDebug("SQLGetData, stmt:%p, col:%d, type:%d:%s, val:%p, len:%d, lenp:%p", stmt, col, type, odbcCDataTypeString(type), val, len, lenp);

  SQLRETURN ret = SQL_SUCCESS;
  STMT *s = (STMT*)stmt;

  switch (s->type) {
  case STMT_SHOW_SCHEMA_SQL:
    odbcError("stmt:%p, fetch to end of the result set", stmt);
    setstat(s, -1, "fetch to end of the result set", "HY000");
    ret = SQL_ERROR;
    goto done;
  case STMT_SHOW_TABLES_TYPE_SQL:
    if (s->fixedResultSetIndex > 1 || s->fixedResultSetIndex < 0) {
      odbcError("stmt:%p, fetch to end of the result set", stmt);
      setstat(s, -1, "fetch to end of the result set", "HY000");
      ret = SQL_ERROR;
      goto done;
    }

    int c = col - 1;
    COL column;
    column.val = val;
    column.len = lenp;
    column.maxLen = len;
    column.type = type;
    column.fieldType = s->cols[c].fieldType;

    if (s->fixedResultSetIndex == 0) {
      if (c == 0) odbcSetColumnValue(&column, "", NULL);
      if (c == 1) odbcSetColumnValue(&column, "", NULL);
      if (c == 2) odbcSetColumnValue(&column, "", NULL);
      if (c == 3) odbcSetColumnValue(&column, "STABLE", NULL);
      if (c == 4) odbcSetColumnValue(&column, "", NULL);
    }
    else {
      if (c == 0) odbcSetColumnValue(&column, "", NULL);
      if (c == 1) odbcSetColumnValue(&column, "", NULL);
      if (c == 2) odbcSetColumnValue(&column, "", NULL);
      if (c == 3) odbcSetColumnValue(&column, "TABLE", NULL);
      if (c == 4) odbcSetColumnValue(&column, "", NULL);
    }
    ret = SQL_SUCCESS;
    goto done;
  case STMT_FOERIGN_KEY_SQL:
    odbcError("stmt:%p, fetch to end of the result set", stmt);
    setstat(s, -1, "fetch to end of the result set", "HY000");
    ret = SQL_ERROR;
    goto done;
  default:
    break;
  }

  //if (s->type == STMT_NORMAL_SQL) {
  //  odbcError("stmt:%p, only normal sql support this function", stmt);
  //  setstat(s, -1, "only normal sql support this function", "HY000");
  //  ret = SQL_ERROR;
  //  goto done;
  //}

  if (s->dbc->con == NULL) {
    odbcError("stmt:%p, connection is lost", stmt);
    setstat(s, -1, "connection is lost", "HY000");
    ret = SQL_INVALID_HANDLE;
    goto done;
  }

  if (val == NULL) {
    odbcError("stmt:%p, input column was a null pointer", stmt);
    setstat(s, -1, "input column was a null pointer", "HY009");
    ret = SQL_INVALID_HANDLE;
    goto done;
  }

  if (s->numFields == 0) {
    odbcError("stmt:%p, no result set was found", stmt);
    setstat(s, -1, "no result set was found", "HY000");
    ret = SQL_ERROR;
    goto done;
  }

  if (col <= 0) {
    odbcError("stmt:%p, column number was 0", stmt);
    setstat(s, -1, "column number was 0", "07009");
    ret = SQL_ERROR;
    goto done;
  }

  if (col > TSDB_MAX_COLUMNS) {
    odbcError("stmt:%p, column number exceeded the maximum number of columns in the result set", stmt);
    setstat(s, -1, "column number exceeded the maximum number of columns in the result set", "07009");
    ret = SQL_ERROR;
    goto done;
  }

  if (s->row == NULL) {
    odbcError("stmt:%p, fetch to end of the result set", stmt);
    setstat(s, -1, "fetch to end of the result set", "HY000");
    ret = SQL_ERROR;
    goto done;
  }

  int c = col - 1;
  COL column;
  column.val = val;
  column.len = lenp;
  column.maxLen = len;
  column.type = type;
  column.fieldType = s->cols[c].fieldType;

  HSTMT_LOCK(stmt);
  switch (s->type) {
  case STMT_NORMAL_SQL:    
    odbcSetColumnValue(&column, s->row[c], NULL);
    break;
  case STMT_SHOW_TABLES_SQL:
    if (c == 0) odbcSetColumnValue(&column, s->dbc->dbname, NULL); //assert all the columns type is binary
    else if (c == 1) odbcSetColumnValue(&column, "", NULL);
    else if (c == 2) odbcSetColumnValue(&column, s->row[0], NULL);
    else if (c == 3) odbcSetColumnValue(&column, "TABLE", NULL);
    else if (c == 4) odbcSetColumnValue(&column, "", NULL);
    break;
  case STMT_SHOW_STABLES_SQL:
    if (c == 0) odbcSetColumnValue(&column, s->dbc->dbname, NULL); //assert all the columns type is binary
    else if (c == 1) odbcSetColumnValue(&column, "", NULL);
    else if (c == 2) odbcSetColumnValue(&column, s->row[0], NULL);
    else if (c == 3) odbcSetColumnValue(&column, "STABLE", NULL);
    else if (c == 4) odbcSetColumnValue(&column, "", NULL);
    break;
  case STMT_SHOW_DATABASE_SQL:
    if (c == 0) odbcSetColumnValue(&column, s->row[0], NULL); //assert all the columns type is binary
    else if (c == 1) odbcSetColumnValue(&column, "", NULL);
    else if (c == 2) odbcSetColumnValue(&column, "", NULL);
    else if (c == 3) odbcSetColumnValue(&column, "DATABASE", NULL);
    else  if (c == 4) odbcSetColumnValue(&column, "", NULL);
    break;
  case STMT_PRIMARY_KEY_SQL:
    if (c == 0) odbcSetColumnValue(&column, s->dbc->dbname, NULL); //assert all the columns type is binary
    else if (c == 1) odbcSetColumnValue(&column, "", NULL);
    else if (c == 2) odbcSetColumnValue(&column, s->dbc->tbname, NULL);
    else if (c == 3) odbcSetColumnValue(&column, s->row[0], NULL);
    else if (c == 4) {
      int val = 1;
      odbcSetColumnValue(&column, &val, NULL);
    }
    else if (c == 5) odbcSetColumnValue(&column, s->row[0], NULL);
    break;
  case STMT_DESCRIBE_COLUMNS_SQL:{
    int tdType = odbcTDengineTypeString2TdengineType((char*)s->row[1]);
    int sqlType = odbcTDengineTypeString2SqlType((char*)s->row[1]);
    int canNull = SQL_NULLABLE;
    int canotNull = SQL_NO_NULLS;
    int zero = 0;

    int columnSize = odbcColumnSizeOfTDengineType(tdType);
    int decimalDigits = odbcDecimalDigitsOfTDengineType(tdType);
    int numpreRadix = odbcNumPrecRadixOfTDengineType(tdType);
    if (c == 0) odbcSetColumnValue(&column, s->dbc->dbname, NULL);  //���ݿ�����   SQL_C_CHAR
    else if (c == 1) odbcSetColumnValue(&column, "", NULL);         //schema����  SQL_C_CHAR
    else if (c == 2) odbcSetColumnValue(&column, s->dbc->tbname, NULL);         //������      SQL_C_CHAR
    else if (c == 3) odbcSetColumnValue(&column, s->row[0], NULL);  //������      SQL_C_CHAR
    else if (c == 4) odbcSetColumnValue(&column, &sqlType, NULL);   //�������ͣ�ODBC���ͣ�SQL_C_SSHORT 
    else if (c == 5) odbcSetColumnValue(&column, odbcDataTypeString((SQLSMALLINT)sqlType), NULL); //�����������ƣ�ODBC���ͣ�SQL_C_CHAR
    else if (c == 6) odbcSetColumnValue(&column, &columnSize, NULL);  //����������Ŀ SQL_C_SLONG
    else if (c == 7) odbcSetColumnValue(&column, s->row[2], NULL);  //ʵ���ֽ���   SQL_C_SLONG
    else if (c == 8) odbcSetColumnValue(&column, &decimalDigits, NULL); //С��������Ŀ SQL_C_SSHORT
    else if (c == 9) odbcSetColumnValue(&column, &numpreRadix, NULL);         //����6,8�����⣬��������ΪNULL��SQL_C_SSHORT
    else if (c == 10) odbcSetColumnValue(&column, s->rowsFetched == 1 ? &canotNull : &canNull, NULL); //�Ƿ�Ϊ�� SQL_C_SSHORT
    else if (c == 11) odbcSetColumnValue(&column, "", NULL); //���� SQL_C_CHAR
    else if (c == 12) odbcSetColumnValue(&column, "NULL", NULL);    //Ĭ��ֵ������Ϊ�� SQL_C_CHAR
    else if (c == 13) odbcSetColumnValue(&column, &tdType, NULL); //����Ϊtdengine����������  SQL_C_SSHORT
    else if (c == 14) odbcSetColumnValue(&column, &zero, NULL); //SQL_C_SSHORT The subtype code for datetime and interval data types. For other data types, this column returns a NULL
    else if (c == 15) odbcSetColumnValue(&column
      , tdType == TSDB_DATA_TYPE_BINARY || tdType == TSDB_DATA_TYPE_NCHAR ? s->row[2] : &zero, NULL); //SQL_C_SLONG The maximum length in bytes of a character or binary data type column. For all other data types, this column returns a NULL.
    else if (c == 16) odbcSetColumnValue(&column, &s->rowsFetched, NULL); //SQL_C_SLONG The ordinal position of the column in the table. The first column in the table is number 1.
    else if (c == 17) odbcSetColumnValue(&column
      , s->rowsFetched == 1 ? "NO" : "YES", NULL); //SQL_C_CHAR "NO" if the column does not include NULLs."YES" if the column could include NULLs.
    }break;
  default:
    break;
  }
  HSTMT_UNLOCK(stmt);

done:

  return ret;
}

/**
 * Fetch next result row.
 * @param stmt statement handle
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLFetch(SQLHSTMT stmt)
{
  //odbcDebug("SQLFetch, stmt:%p", stmt);
 
  STMT *s = (STMT*)stmt;
  SQLRETURN ret = SQL_SUCCESS;
  HSTMT_LOCK(stmt);

  switch (s->type) {
  case STMT_SHOW_SCHEMA_SQL:
    odbcDebug("stmt:%p, no schema was found", stmt);
    ret = SQL_NO_DATA;
    goto done;
  case STMT_FOERIGN_KEY_SQL:
    odbcDebug("stmt:%p, no foreign key was found", stmt);
    ret = SQL_NO_DATA;
    goto done;
  case STMT_SHOW_TABLES_TYPE_SQL:
    s->fixedResultSetIndex++;
    if (s->fixedResultSetIndex > 1 || s->fixedResultSetIndex < 0) {
      odbcDebug("stmt:%p, fetch to end of type result set, query table types", stmt);
      ret = SQL_NO_DATA;
      goto done;
    }
    if (s->fixedResultSetIndex == 0) {
      odbcSetColumnValue(s->cols + 0, "", NULL);
      odbcSetColumnValue(s->cols + 1, "", NULL);
      odbcSetColumnValue(s->cols + 2, "", NULL);
      odbcSetColumnValue(s->cols + 3, "STABLE", NULL);
      odbcSetColumnValue(s->cols + 4, "", NULL);
    }
    else {
      odbcSetColumnValue(s->cols + 0, "", NULL);
      odbcSetColumnValue(s->cols + 1, "", NULL);
      odbcSetColumnValue(s->cols + 2, "", NULL);
      odbcSetColumnValue(s->cols + 3, "TABLE", NULL);
      odbcSetColumnValue(s->cols + 4, "", NULL);
    }
    ret = SQL_SUCCESS;
    goto done;
  default:
    break;
  }

  if (s->dbc->con == NULL) {
    odbcError("stmt:%p, connection is lost", stmt);
    setstat(s, -1, "connection is lost", "HY000");
    ret = SQL_INVALID_HANDLE;
    goto done;
  }

  if (s->numFields == 0) {
    odbcError("stmt:%p, no result set was found", stmt);
    setstat(s, -1, "no result set was found", "HY000");
    ret = SQL_ERROR;
    goto done;
  }
 
  if (s->result == NULL) {
    odbcError("stmt:%p, result set is null", stmt);
    setstatd(s->dbc, -1, "result is null", "HY000");
    ret = SQL_ERROR;
    goto done;
  }
  
  s->row = taos_fetch_row(s->result);
  if (s->row == NULL) {
    if (s->type == STMT_SHOW_STABLES_SQL) {
      s->type = STMT_SHOW_TABLES_SQL;
      odbcDebug("stmt:%p, continue get tables", stmt);
      char *tablePos = strstr(s->sql, "stables");
      if (tablePos != NULL) {
        tablePos[0] = 't';
        tablePos[1] = 'a';
        tablePos[2] = 'b';
        tablePos[3] = 'l';
        tablePos[4] = 'e';
        tablePos[5] = 's';
        tablePos[6] = ' ';
        odbcDebug("stmt:%p, change sql to:%s", s, s->sql);
        if (s->result != NULL) {
          taos_free_result(s->result);
        }
        if (taos_query(s->dbc->con, s->sql) == 0) {
          s->result = taos_use_result(s->dbc->con);
          s->fields = taos_fetch_fields(s->result);
          s->numFields = taos_field_count(s->dbc->con);
          HSTMT_UNLOCK(stmt);
          return SQLFetch(stmt);
        }
      }
      ret = SQL_NO_DATA;
      goto done;
    }

    odbcDebug("stmt:%p, fetch to end of result set:%p", stmt, s->result);
    setstatd(s->dbc, -1, "fetch to end of result set", "HY000");
    ret = SQL_NO_DATA;
    goto done;
  }

  s->rowsFetched++;
  switch (s->type) {
  case STMT_NORMAL_SQL:
    for (int c = 0; c < s->numFields; c++) {
      odbcSetColumnValue(s->cols + c, s->row[c], NULL);
    }
    break;
  case STMT_SHOW_TABLES_SQL:
    //catlog, schema, tablename, tabletype, remark
    odbcSetColumnValue(s->cols + 0, s->dbc->dbname, NULL); //assert all the columns type is binary
    odbcSetColumnValue(s->cols + 1, "", NULL);
    odbcSetColumnValue(s->cols + 2, s->row[0], NULL);
    odbcSetColumnValue(s->cols + 3, "TABLE", NULL);
    odbcSetColumnValue(s->cols + 4, "", NULL);
    break;
  case STMT_SHOW_STABLES_SQL:
    odbcSetColumnValue(s->cols + 0, s->dbc->dbname, NULL); //assert all the columns type is binary
    odbcSetColumnValue(s->cols + 1, "", NULL);
    odbcSetColumnValue(s->cols + 2, s->row[0], NULL);
    odbcSetColumnValue(s->cols + 3, "STABLE", NULL);
    odbcSetColumnValue(s->cols + 4, "", NULL);
    break;
  case STMT_SHOW_DATABASE_SQL:
    odbcSetColumnValue(s->cols + 0, s->row[0], NULL); //assert all the columns type is binary
    odbcSetColumnValue(s->cols + 1, "", NULL);
    odbcSetColumnValue(s->cols + 2, "", NULL);
    odbcSetColumnValue(s->cols + 3, "DATABASE", NULL);
    odbcSetColumnValue(s->cols + 4, "", NULL);
    break;
  case STMT_PRIMARY_KEY_SQL:
    s->fixedResultSetIndex++;
    if (s->fixedResultSetIndex >= 1 || s->fixedResultSetIndex < 0) {
      odbcDebug("stmt:%p, fetch to end of primary key result set", stmt);
      ret = SQL_NO_DATA;
      goto done;
    }
    odbcSetColumnValue(s->cols + 0, s->dbc->dbname, NULL); //assert all the columns type is binary
    odbcSetColumnValue(s->cols + 1, "", NULL);
    odbcSetColumnValue(s->cols + 2, s->dbc->tbname, NULL);
    odbcSetColumnValue(s->cols + 3, s->row[0], NULL);
    {
      int val = 1;
      odbcSetColumnValue(s->cols + 4, &val, NULL);
    }
    odbcSetColumnValue(s->cols + 5, s->row[0], NULL);
    break;
  case STMT_DESCRIBE_COLUMNS_SQL:{
    int tdType = odbcTDengineTypeString2TdengineType((char*)s->row[1]);
    int sqlType = odbcTDengineTypeString2SqlType((char*)s->row[1]);
    int canNull = SQL_NULLABLE;
    int canotNull = SQL_NO_NULLS;
    int zero = 0;
    int columnSize = odbcColumnSizeOfTDengineType(tdType);
    int decimalDigits = odbcDecimalDigitsOfTDengineType(tdType);
    int numpreRadix = odbcNumPrecRadixOfTDengineType(tdType);

    odbcSetColumnValue(s->cols + 0, s->dbc->dbname, NULL);  //���ݿ�����   SQL_C_CHAR
    odbcSetColumnValue(s->cols + 1, "", NULL);         //schema����  SQL_C_CHAR
    odbcSetColumnValue(s->cols + 2, s->dbc->tbname, NULL);         //������      SQL_C_CHAR
    odbcSetColumnValue(s->cols + 3, s->row[0], NULL);  //������      SQL_C_CHAR
    odbcSetColumnValue(s->cols + 4, &sqlType, NULL);   //�������ͣ�ODBC���ͣ�SQL_C_SSHORT 
    odbcSetColumnValue(s->cols + 5, odbcDataTypeString((SQLSMALLINT)sqlType), NULL); //�����������ƣ�ODBC���ͣ�SQL_C_CHAR
    odbcSetColumnValue(s->cols + 6, &columnSize, NULL);  //����������Ŀ SQL_C_SLONG
    odbcSetColumnValue(s->cols + 7, s->row[2], NULL);  //ʵ���ֽ���   SQL_C_SLONG
    odbcSetColumnValue(s->cols + 8, &decimalDigits, NULL); //С��������Ŀ SQL_C_SSHORT
    odbcSetColumnValue(s->cols + 9, &numpreRadix, NULL);         //����6,8�����⣬��������ΪNULL��SQL_C_SSHORT
    odbcSetColumnValue(s->cols + 10, s->rowsFetched == 1 ? &canotNull : &canNull, NULL); //�Ƿ�Ϊ�� SQL_C_SSHORT
    odbcSetColumnValue(s->cols + 11, "", NULL); //���� SQL_C_CHAR
    odbcSetColumnValue(s->cols + 12, "NULL", NULL);    //Ĭ��ֵ������Ϊ�� SQL_C_CHAR
    odbcSetColumnValue(s->cols + 13, &tdType, NULL); //����Ϊtdengine����������  SQL_C_SSHORT
    odbcSetColumnValue(s->cols + 14, &zero, NULL); //SQL_C_SSHORT The subtype code for datetime and interval data types. For other data types, this column returns a NULL
    odbcSetColumnValue(s->cols + 15
      , tdType == TSDB_DATA_TYPE_BINARY || tdType == TSDB_DATA_TYPE_NCHAR ? s->row[2] : &zero, NULL); //SQL_C_SLONG The maximum length in bytes of a character or binary data type column. For all other data types, this column returns a NULL.
    odbcSetColumnValue(s->cols + 16, &s->rowsFetched, NULL); //SQL_C_SLONG The ordinal position of the column in the table. The first column in the table is number 1.
    odbcSetColumnValue(s->cols + 17
      , s->rowsFetched == 1 ? "NO" : "YES", NULL); //SQL_C_CHAR "NO" if the column does not include NULLs."YES" if the column could include NULLs.
  }break;
  default:
    odbcError("sql type:%d not implemented", s->type);
    setstatd(s->dbc, -1, "sql type not implemented", "HY000");
    ret = SQL_NO_DATA;
    goto done; 
  }

done:
  HSTMT_UNLOCK(stmt);
  return ret;
}

/**
 * Fetch result row with scrolling.
 * @param stmt statement handle
 * @param orient fetch direction
 * @param offset offset for fetch direction
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLFetchScroll(SQLHSTMT stmt, SQLSMALLINT orient, SQLLEN offset)
{
  odbcError("SQLFetchScroll not implemented, stmt:%p", stmt);
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcUnImplStmt(stmt);
  HSTMT_UNLOCK(stmt);
  return ret;
}

/**
 * Fetch result row with scrolling and row status.
 * @param stmt statement handle
 * @param orient fetch direction
 * @param offset offset for fetch direction
 * @param rowcount output number of fetched rows
 * @param rowstatus array for row stati
 * @result ODBC error code
 */

#if defined(_WIN32) || defined(_WIN64) 
SQLRETURN SQL_API 
SQLExtendedFetch(SQLHSTMT stmt, SQLUSMALLINT orient, SQLROWOFFSET offset,
  SQLROWSETSIZE *rowcount, SQLUSMALLINT *rowstatus)
{
  odbcError("SQLExtendedFetch not implemented, stmt:%p", stmt);
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcUnImplStmt(stmt);
  HSTMT_UNLOCK(stmt);
  return ret;
}
#endif
/**
 * Return number of affected rows of HSTMT.
 * @param stmt statement handle
 * @param nrows output number of rows
 * @result ODBC error code
 */

SQLRETURN SQL_API 
SQLRowCount(SQLHSTMT stmt, SQLLEN *nrows)
{
  odbcDebug("SQLRowCount, stmt:%p", stmt);
  SQLRETURN ret = SQL_SUCCESS;
  STMT *s = (STMT*)stmt;

  HSTMT_LOCK(stmt);
  if (nrows) {
    *nrows = s->rowsAffacted;
  }
  HSTMT_UNLOCK(stmt);
  return ret;
}

/**
 * Return number of columns of result set given HSTMT.
 * @param stmt statement handle
 * @param ncols output number of columns
 * @result ODBC error code
 */

SQLRETURN SQL_API 
SQLNumResultCols(SQLHSTMT stmt, SQLSMALLINT *ncols)
{
  SQLRETURN ret = SQL_SUCCESS;
  STMT *s = (STMT*)stmt;

  HSTMT_LOCK(stmt);
  
  switch (s->type) {
  case STMT_DESCRIBE_COLUMNS_SQL:
    *ncols = 18;
    break;
  case STMT_SHOW_DATABASE_SQL:
  case STMT_SHOW_SCHEMA_SQL:
  case STMT_SHOW_TABLES_TYPE_SQL:
  case STMT_SHOW_TABLES_SQL:
  case STMT_SHOW_STABLES_SQL:
    *ncols = 5;
    break;
  case STMT_PRIMARY_KEY_SQL:
    *ncols = 6;
    break;
  case STMT_FOERIGN_KEY_SQL:
    *ncols = 14;
    break;
  case STMT_NORMAL_SQL:
    if (ncols) {
      if (s->numFields == 0) {
        //not select or show commands
        *ncols = 0;
      }
      else  {
        *ncols = (SQLSMALLINT)s->numFields;
      }
    }
    break;
  default:
    *ncols = 0;
  }

  HSTMT_UNLOCK(stmt);

  odbcDebug("SQLNumResultCols, stmt:%p, ncols:%d", stmt, *ncols);
  return ret;
}

SQLRETURN
odbcDescribeCol(SQLHSTMT stmt, SQLUSMALLINT col, SQLCHAR *name,
  SQLSMALLINT nameMax, SQLSMALLINT *nameLen,
  SQLSMALLINT *type, SQLULEN *size,
  SQLSMALLINT *digits, SQLSMALLINT *nullable)
{
  STMT *s = (STMT*)stmt;
  SQLRETURN ret = SQL_SUCCESS;

  if (s->dbc->con == NULL) {
    odbcError("stmt:%p, connection is lost", stmt);
    setstat(s, -1, "connection is lost", "HY000");
    ret = SQL_INVALID_HANDLE;
    goto done;
  }
  
  if (size) {
    //*size = s->numFields;
    //may be wrong
    *size = 10;
  }
  if (digits) {
    *digits = 0;
  }
  if (nullable) {
    if (col != 1)
      *nullable = SQL_NULLABLE;
    else
      *nullable = SQL_NO_NULLS;
  }

  if (s->numFields == 0) {
    odbcError("stmt:%p, no result set was found", stmt);
    setstat(s, -1, "no result set was found", "HY000");
    ret = SQL_ERROR;
    goto done;
  }

  if (col <= 0) {
    odbcError("stmt:%p, column number was 0", stmt);
    setstat(s, -1, "column number was 0", "07006");
    ret = SQL_ERROR;
    goto done;
  }

  if (col > TSDB_MAX_COLUMNS) {
    odbcError("stmt:%p, column number exceeded the maximum number of columns in the result set", stmt);
    setstat(s, -1, "column number exceeded the maximum number of columns in the result set", "07009");
    ret = SQL_ERROR;
    goto done;
  }

  int c = col - 1;
  int len = (int)strlen(s->fields[c].name);
  len = min(nameMax, len);
  strncpy((char*)name, s->fields[c].name, (size_t)len);
  name[len] = 0;
  if (nameLen)
    *nameLen = (SQLSMALLINT)len;
  *type = (SQLSMALLINT)odbcTDengineType2SqlType(s->fields[c].type);
  
done:
  return ret;
}

/**
 * Describe column information.
 * @param stmt statement handle
 * @param col column number, starting at 1
 * @param name buffer for column name
 * @param nameMax length of name buffer
 * @param nameLen output length of column name
 * @param type output SQL type
 * @param size output column size
 * @param digits output number of digits
 * @param nullable output NULL allowed indicator
 * @result ODBC error code
 */

SQLRETURN SQL_API 
SQLDescribeCol(SQLHSTMT stmt, SQLUSMALLINT col, SQLCHAR *name,
  SQLSMALLINT nameMax, SQLSMALLINT *nameLen,
  SQLSMALLINT *type, SQLULEN *size,
  SQLSMALLINT *digits, SQLSMALLINT *nullable)
{
  odbcDebug("SQLDescribeCol, stmt:%p, col:%d", stmt, col);
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcDescribeCol(stmt, col, name, nameMax, nameLen, type, size, digits, nullable);
  HSTMT_UNLOCK(stmt);
  return ret;
}

/**
 * Describe column information (UNICODE version).
 * @param stmt statement handle
 * @param col column number, starting at 1
 * @param name buffer for column name
 * @param nameMax length of name buffer
 * @param nameLen output length of column name
 * @param type output SQL type
 * @param size output column size
 * @param digits output number of digits
 * @param nullable output NULL allowed indicator
 * @result ODBC error code
 */
 
SQLRETURN SQL_API 
SQLDescribeColW(SQLHSTMT stmt, SQLUSMALLINT col, SQLWCHAR *msg,
  SQLSMALLINT buflen, SQLSMALLINT *msglen,
  SQLSMALLINT *type, SQLULEN *size,
  SQLSMALLINT *digits, SQLSMALLINT *nullable)
{
  odbcDebug("SQLDescribeColW, stmt:%p, col:%d", stmt, col);
  
  HSTMT_LOCK(stmt);
  SQLSMALLINT len;
  SQLRETURN ret = odbcDescribeCol(stmt, col, (SQLCHAR *)msg, buflen, &len, type, size, digits, nullable);

  if (msg) {
    if (len > 0) {
      SQLWCHAR *m = NULL;

      m = uc_from_utf((unsigned char *)msg, len);
      if (m) {
        if (buflen) {
          buflen = (SQLSMALLINT)((size_t)buflen / sizeof(SQLWCHAR));
          uc_strncpy(msg, m, buflen);
          m[len] = 0;
          len = (SQLSMALLINT)(min(buflen, uc_strlen(m)));
        }
        else {
          len = (SQLSMALLINT)uc_strlen(m);
        }
        uc_free(m);
      }
      else {
        len = 0;
      }
    }
    if (len <= 0) {
      len = 0;
      if (buflen > 0) {
        msg[0] = 0;
      }
    }
  }
  else {
    /* estimated length !!! */
    len = (SQLSMALLINT)((size_t)len * sizeof(SQLWCHAR));
  }
  if (msglen) {
    *msglen = len;
  }

  HSTMT_UNLOCK(stmt);
  return ret;
}

SQLRETURN
odbcColAttributes(SQLHSTMT stmt, SQLUSMALLINT col, SQLUSMALLINT id,
  SQLPOINTER val, SQLSMALLINT valMax, SQLSMALLINT *valLen,
  SQLLEN *val2)
{
  STMT *s;
  COL *c;
  SQLSMALLINT dummy;
  char *valc = (char *)val;

  if (stmt == SQL_NULL_HSTMT) {
    return SQL_INVALID_HANDLE;
  }
  s = (STMT *)stmt;
  if (!s->cols) {
    return SQL_ERROR;
  }
  if (!valLen) {
    valLen = &dummy;
  }
  if (id == SQL_COLUMN_COUNT) {                  //checked
    if (val2) {
      *val2 = s->numFields;
    }
    *valLen = sizeof(int);
    return SQL_SUCCESS;
  }
  if (col < 1 || col > s->numFields) {
    setstat(s, -1, "invalid column", (s->dbc->ov3) ? "07009" : "S1002");
    return SQL_ERROR;
  }
  c = s->cols + col - 1;

  switch (id) {
  case SQL_DESC_OCTET_LENGTH:    //checked
  {
    if (val2) {
      *val2 = c->fieldSize;
    }
    *valLen = sizeof(int);
    return SQL_SUCCESS;
  }
  case SQL_COLUMN_LABEL:         //checked
    //SQL_DESC_LABEL
  case SQL_COLUMN_NAME:          //checked
  case SQL_DESC_NAME:            //checked
    if (valc && valMax > 0) {
      strncpy(valc, c->fieldName, (size_t)valMax);
      valc[valMax - 1] = '\0';
    }
    *valLen = (SQLSMALLINT)strlen(c->fieldName);
  checkLen:
    if (*valLen >= valMax) {
      setstat(s, -1, "data right truncated", "01004");
      return SQL_SUCCESS_WITH_INFO;
    }
    return SQL_SUCCESS;
#ifdef SQL_DESC_BASE_COLUMN_NAME
  case SQL_DESC_BASE_COLUMN_NAME://checked
    if (strchr(c->fieldName, '(') || strchr(c->fieldName, ')')) {
      if (valc && valMax > 0) {
        valc[0] = '\0';
      }
      *valLen = 0;
    }
    else if (valc && valMax > 0) {
      strncpy(valc, c->fieldName, (size_t)valMax);
      valc[valMax - 1] = '\0';
      *valLen = (SQLSMALLINT)strlen(c->fieldName);
    }
    goto checkLen;
#endif
  case SQL_COLUMN_TYPE:         //checked
    //same as SQL_DESC_CONCISE_TYPE
  case SQL_DESC_TYPE:           //checked
    if (val2) {
      *val2 = odbcTDengineType2SqlType(c->fieldType);
    }
    *valLen = sizeof(int);
    return SQL_SUCCESS;
  case SQL_COLUMN_DISPLAY_SIZE: //checked
    if (val2) {
      *val2 = c->fieldDisplaySize;
    }
    *valLen = sizeof(int);
    return SQL_SUCCESS;
  case SQL_COLUMN_UNSIGNED:     //checked
    //same as SQL_DESC_UNSIGNED
    if (val2) {
      if (c->fieldType == TSDB_DATA_TYPE_BINARY || c->fieldType == TSDB_DATA_TYPE_NCHAR || c->fieldType == TSDB_DATA_TYPE_TIMESTAMP)
        *val2 = SQL_TRUE;
      else
        *val2 = SQL_FALSE;
    }
    *valLen = sizeof(int);
    return SQL_SUCCESS;
  case SQL_COLUMN_SCALE:        //checked
  case SQL_DESC_SCALE:          //checked
    if (val2) {
      *val2 = c->fieldScale;
    }
    *valLen = sizeof(int);
    return SQL_SUCCESS;
  case SQL_COLUMN_PRECISION:    //checked
  case SQL_DESC_PRECISION:      //checked
    if (val2) {
      switch (odbcTDengineType2SqlType(c->fieldType)) {
      case SQL_BIT:
        *val2 = 1;
        break;
      case SQL_TINYINT:
        *val2 = 3;
        break;
      case SQL_SMALLINT:
        *val2 = 5;
        break;
      case SQL_INTEGER:
        *val2 = 10;
        break;
      case SQL_BIGINT:
        *val2 = 19;
        break;
      case SQL_FLOAT:
      case SQL_DOUBLE:
        *val2 = 15;
        break;
      case SQL_CHAR:
        *val2 = 0;
        break;
#ifdef SQL_TYPE_TIMESTAMP
      case SQL_TYPE_TIMESTAMP:
#endif
      case SQL_TIMESTAMP:
        *val2 = 3;
        break;
      default:
        *val2 = 0;
        break;
      }
    }
    *valLen = sizeof(int);
    return SQL_SUCCESS;   
  case SQL_DESC_FIXED_PREC_SCALE:  //checked
    if (val2) {
      *val2 = SQL_TRUE;
    }
    *valLen = sizeof(int);
    return SQL_SUCCESS;
  case SQL_COLUMN_AUTO_INCREMENT:   //checked
    *val2 = SQL_FALSE;
    *valLen = sizeof(int);
    return SQL_SUCCESS;
  case SQL_COLUMN_LENGTH:           //checked
  case SQL_DESC_LENGTH:             //checked
    if (val2) {
      switch (odbcTDengineType2SqlType(c->fieldType)) {
      case SQL_BIT:
        *val2 = 1;
        break;
      case SQL_TINYINT:
        *val2 = 127;
        break;
      case SQL_SMALLINT:
        *val2 = 32767;
        break;
      case SQL_INTEGER:
        *val2 = 2147483647;
        break;
      case SQL_BIGINT:
        *val2 = 9223372036854775806L;
        break;
      case SQL_FLOAT:
      case SQL_DOUBLE:
        *val2 = 9223372036854775806L;
        break;
      case SQL_CHAR:
        *val2 = c->fieldSize;
        break;
#ifdef SQL_TYPE_TIMESTAMP
      case SQL_TYPE_TIMESTAMP:
#endif
      case SQL_TIMESTAMP:
        *val2 = 621355968000000000L;
        break;
      default:
        *val2 = 0;
        break;
      }
    }
    *valLen = sizeof(int);
  case SQL_COLUMN_NULLABLE:         //checked
    if (val2) {
      *val2 = (col == 1) ? SQL_NO_NULLS : SQL_NULLABLE;
    }
    *valLen = sizeof(int);
    return SQL_SUCCESS;
  case SQL_DESC_NULLABLE:           //checked
    if (valc && valMax > 0) {
      const char * ret = (col == 1) ? "NO" : "YES";
      strncpy(valc, ret, (size_t)valMax);
      valc[valMax - 1] = '\0';
      *valLen = (SQLSMALLINT)strlen(ret);
    }
    goto checkLen;
  case SQL_COLUMN_SEARCHABLE:      //checked
    if (val2) {
      *val2 = SQL_PRED_SEARCHABLE; //SQL_PRED_BASIC
    }
    *valLen = sizeof(int);
    return SQL_SUCCESS;
  case SQL_COLUMN_CASE_SENSITIVE: //checked
    if (val2) {
      *val2 = SQL_TRUE;
    }
    *valLen = sizeof(int);
    return SQL_SUCCESS;
  case SQL_COLUMN_UPDATABLE:      //checked
    if (val2) {
      *val2 = SQL_FALSE;
    }
    *valLen = sizeof(int);
    return SQL_SUCCESS;
  case SQL_DESC_COUNT:            //checked
    if (val2) {
      *val2 = s->numFields;
    }
    *valLen = sizeof(int);
    return SQL_SUCCESS;
  case SQL_COLUMN_TYPE_NAME:     //checked
    //same as SQL_DESC_TYPE_NAME
    if (valc && valMax > 0) {
      strncpy(valc, odbcDataTypeTDengineString(c->fieldType), (size_t)valMax);
      valc[valMax - 1] = '\0';
      *valLen = (SQLSMALLINT)strlen(odbcDataTypeTDengineString(c->fieldType));
    }
    goto checkLen;
  case SQL_COLUMN_OWNER_NAME:    //checked
  case SQL_COLUMN_QUALIFIER_NAME: 
    if (valc && valMax > 0) {
      strncpy(valc, s->dbc->dbname, (size_t)valMax);
      valc[valMax - 1] = '\0';
      *valLen = (SQLSMALLINT)strlen(s->dbc->dbname);
    }
    goto checkLen;
  case SQL_COLUMN_TABLE_NAME:    //checked
#if (SQL_COLUMN_TABLE_NAME != SQL_DESC_TABLE_NAME)
  case SQL_DESC_TABLE_NAME:
#endif
#ifdef SQL_DESC_BASE_TABLE_NAME
  case SQL_DESC_BASE_TABLE_NAME: //checked
#endif
    if (valc && valMax > 0) {
      valc[0] = '\0';
    }
    *valLen = 0;
    goto checkLen;
#ifdef SQL_DESC_NUM_PREC_RADIX
  case SQL_DESC_NUM_PREC_RADIX: //checked
    if (val2) {
      switch (odbcTDengineType2SqlType(c->fieldType)) {
      case SQL_BIT:
      case SQL_TINYINT:
      case SQL_SMALLINT:
      case SQL_INTEGER:
      case SQL_BIGINT:
      case SQL_TIMESTAMP:
        *val2 = 10;
        break;
      case SQL_FLOAT:
      case SQL_DOUBLE:
        *val2 =2;
        break;
      default:
        *val2 = 0;
        break;
      }
    }
    *valLen = sizeof(int);
    return SQL_SUCCESS;
#endif
  }
  setstat(s, -1, "unsupported column attributes %d", "HY091", id);
  return SQL_ERROR;
}

/**
 * Retrieve column attributes.
 * @param stmt statement handle
 * @param col column number, starting at 1
 * @param id attribute id
 * @param val output buffer
 * @param valMax length of output buffer
 * @param valLen output length
 * @param val2 integer output buffer
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLColAttributes(SQLHSTMT stmt, SQLUSMALLINT col, SQLUSMALLINT id,
  SQLPOINTER val, SQLSMALLINT valMax, SQLSMALLINT *valLen,
  SQLLEN *val2)
{
  odbcDebug("SQLColAttributes, stmt:%p, col:%d, id:%d:%s", stmt, col, id, odbcColAttrString(id));
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcColAttributes(stmt, col, id, val, valMax, valLen, val2);
  HSTMT_UNLOCK(stmt);
  return ret;
}

/**
 * Retrieve column attributes (UNICODE version).
 * @param stmt statement handle
 * @param col column number, starting at 1
 * @param id attribute id
 * @param val output buffer
 * @param valMax length of output buffer
 * @param valLen output length
 * @param val2 integer output buffer
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLColAttributesW(SQLHSTMT stmt, SQLUSMALLINT col, SQLUSMALLINT id,
  SQLPOINTER val, SQLSMALLINT valMax, SQLSMALLINT *valLen,
  SQLLEN *val2)
{
  odbcDebug("SQLColAttributesW, stmt:%p, col:%d, id:%d:%s, ", stmt, col, id, odbcColAttrString(id));
  SQLRETURN ret = SQL_SUCCESS;
  SQLSMALLINT len = 0;

  HSTMT_LOCK(stmt);
  ret = odbcColAttributes(stmt, col, id, val, valMax, &len, val2);
  if (SQL_SUCCEEDED(ret)) {
    SQLWCHAR *v = NULL;

    switch (id) {
    case SQL_COLUMN_LABEL:
    case SQL_COLUMN_NAME:
    case SQL_DESC_NAME:
    case SQL_COLUMN_TYPE_NAME:
    case SQL_COLUMN_OWNER_NAME:
    case SQL_COLUMN_QUALIFIER_NAME:
    case SQL_COLUMN_TABLE_NAME:
#if (SQL_COLUMN_TABLE_NAME != SQL_DESC_TABLE_NAME)
    case SQL_DESC_TABLE_NAME:
#endif
#ifdef SQL_DESC_BASE_COLUMN_NAME
    case SQL_DESC_BASE_COLUMN_NAME:
#endif
#ifdef SQL_DESC_BASE_TABLE_NAME
    case SQL_DESC_BASE_TABLE_NAME:
#endif
      if (val && valMax > 0) {
        int vmax = (int)((size_t)valMax / sizeof(SQLWCHAR));

        v = uc_from_utf((SQLCHAR *)val, SQL_NTS);
        if (v) {
          uc_strncpy(val, v, vmax);
          len = (SQLSMALLINT)(min(vmax, uc_strlen(v)));
          uc_free(v);
          len = (SQLSMALLINT)((size_t)len * sizeof(SQLWCHAR));
        }
        if (vmax > 0) {
          v = (SQLWCHAR *)val;
          v[vmax - 1] = '\0';
        }
      }
      if (len <= 0) {
        len = 0;
      }
      break;
    }
    if (valLen) {
      *valLen = len;
    }
  }
  HSTMT_UNLOCK(stmt);
  return ret;
}

SQLRETURN
odbcGetError(SQLHENV env, SQLHDBC dbc, SQLHSTMT stmt,
  SQLCHAR *sqlState, SQLINTEGER *nativeErr,
  SQLCHAR *errmsg, SQLSMALLINT errmax, SQLSMALLINT *errlen)
{
  SQLCHAR dummy0[6];
  SQLINTEGER dummy1;
  SQLSMALLINT dummy2;

  if (env == SQL_NULL_HENV &&
    dbc == SQL_NULL_HDBC &&
    stmt == SQL_NULL_HSTMT) {
    return SQL_INVALID_HANDLE;
  }
  if (sqlState) {
    sqlState[0] = '\0';
  }
  else {
    sqlState = dummy0;
  }
  if (!nativeErr) {
    nativeErr = &dummy1;
  }
  *nativeErr = 0;
  if (!errlen) {
    errlen = &dummy2;
  }
  *errlen = 0;
  if (errmsg) {
    if (errmax > 0) {
      errmsg[0] = '\0';
    }
  }
  else {
    errmsg = dummy0;
    errmax = 0;
  }
  if (stmt) {
    STMT *s = (STMT *)stmt;

    HSTMT_LOCK(stmt);
    if (s->dbc->logmsg[0] == '\0') {
      HSTMT_UNLOCK(stmt);
      goto noerr;
    }
    *nativeErr = s->dbc->naterr;
    strcpy((char *)sqlState, s->dbc->sqlstate);
    if (errmax == SQL_NTS) {
      strcpy((char *)errmsg, "[TDengine]");
      strcat((char *)errmsg, (char *)s->dbc->logmsg);
      *errlen = (SQLSMALLINT)strlen((char *)errmsg);
    }
    else {
      strncpy((char *)errmsg, "[TDengine]", (size_t)errmax);
      if (errmax - 10 > 0) {
        strncpy((char *)errmsg + 10, (char *)s->dbc->logmsg, (size_t)(errmax - 10));
      }
      *errlen = (SQLSMALLINT)min(strlen((char *)s->dbc->logmsg) + 10, (size_t)errmax);
    }
    s->dbc->logmsg[0] = '\0';
    HSTMT_UNLOCK(stmt);
    return SQL_SUCCESS;
  }
  if (dbc) {
    DBC *d = (DBC *)dbc;

    HDBC_LOCK(dbc);
    if (d->signature != d || d->logmsg[0] == '\0') {
      HDBC_UNLOCK(dbc);
      goto noerr;
    }
    *nativeErr = d->naterr;
    strcpy((char *)sqlState, d->sqlstate);
    if (errmax == SQL_NTS) {
      strcpy((char *)errmsg, "[TDengine]");
      strcat((char *)errmsg, (char *)d->logmsg);
      *errlen = (SQLSMALLINT)strlen((char *)errmsg);
    }
    else {
      strncpy((char *)errmsg, "[TDengine]", (size_t)errmax);
      if (errmax - 10 > 0) {
        strncpy((char *)errmsg + 10, (char *)d->logmsg, (size_t)(errmax - 10));
      }
      *errlen = (SQLSMALLINT)min(strlen((char *)d->logmsg) + 10, (size_t)errmax);
    }
    d->logmsg[0] = '\0';
    HDBC_UNLOCK(dbc);
    return SQL_SUCCESS;
  }
noerr:
  sqlState[0] = '\0';
  errmsg[0] = '\0';
  *nativeErr = 0;
  *errlen = 0;
  odbcDebug("no error fetched, env:%p, dbc:%p, stmt:%p", env, dbc, stmt);
  return SQL_NO_DATA;
}

/**
 * Return last HDBC or HSTMT error message.
 * @param env environment handle or NULL
 * @param dbc database connection handle or NULL
 * @param stmt statement handle or NULL
 * @param sqlState output buffer for SQL state
 * @param nativeErr output buffer for native error code
 * @param errmsg output buffer for error message
 * @param errmax length of output buffer for error message
 * @param errlen output length of error message
 * @result ODBC error code
 */

SQLRETURN SQL_API SQLError(SQLHENV env, SQLHDBC dbc, SQLHSTMT stmt,
  SQLCHAR *sqlState, SQLINTEGER *nativeErr,
  SQLCHAR *errmsg, SQLSMALLINT errmax, SQLSMALLINT *errlen)
{
  odbcDebug("SQLError, env:%p, dbc:%p, stmt:%p, sqlState:%s", env, dbc, stmt, sqlState);
  return odbcGetError(env, dbc, stmt, sqlState, nativeErr, errmsg, errmax, errlen);
}

/**
 * Return last HDBC or HSTMT error message (UNICODE version).
 * @param env environment handle or NULL
 * @param dbc database connection handle or NULL
 * @param stmt statement handle or NULL
 * @param sqlState output buffer for SQL state
 * @param nativeErr output buffer for native error code
 * @param errmsg output buffer for error message
 * @param errmax length of output buffer for error message
 * @param errlen output length of error message
 * @result ODBC error code
 */

SQLRETURN SQL_API 
SQLErrorW(SQLHENV env, SQLHDBC dbc, SQLHSTMT stmt,
  SQLWCHAR *sqlState, SQLINTEGER *nativeErr,
  SQLWCHAR *errmsg, SQLSMALLINT errmax, SQLSMALLINT *errlen)
{
  odbcDebug("SQLErrorW, env:%p, dbc:%p, stmt:%p", env, dbc, stmt);

  char state[16];
  SQLSMALLINT len = 0;
  SQLRETURN ret = SQL_SUCCESS;

  ret = odbcGetError(env, dbc, stmt, (SQLCHAR *)state, nativeErr,
    (SQLCHAR *)errmsg, errmax, &len);
  if (ret == SQL_SUCCESS) {
    if (sqlState) {
      uc_from_utf_buf((SQLCHAR *)state, -1, sqlState,
        6 * sizeof(SQLWCHAR));
    }
    if (errmsg) {
      if (len > 0) {
        SQLWCHAR *e = NULL;

        e = uc_from_utf((SQLCHAR *)errmsg, len);
        if (e) {
          if (errmax > 0) {
            uc_strncpy(errmsg, e, errmax);
            e[len] = 0;
            len = (SQLSMALLINT)(min(errmax, uc_strlen(e)));
          }
          else {
            len = (SQLSMALLINT)uc_strlen(e);
          }
          uc_free(e);
        }
        else {
          len = 0;
        }
      }
      if (len <= 0) {
        len = 0;
        if (errmax > 0) {
          errmsg[0] = 0;
        }
      }
    }
    else {
      len = 0;
    }
    if (errlen) {
      *errlen = len;
    }
  }
  else if (ret == SQL_NO_DATA) {
    if (sqlState) {
      sqlState[0] = 0;
    }
    if (errmsg) {
      if (errmax > 0) {
        errmsg[0] = 0;
      }
    }
    if (errlen) {
      *errlen = 0;
    }
  }
  return ret;
}

/**
 * Return information for more result sets.
 * @param stmt statement handle
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLMoreResults(SQLHSTMT stmt)
{
  //odbcError("SQLMoreResults not implemented, stmt:%p", stmt);

  //SQLRETURN ret = SQL_SUCCESS;

  //HSTMT_LOCK(stmt);
  //ret = odbcUnImplStmt(stmt);
  //HSTMT_UNLOCK(stmt);
  //return ret;

  return SQL_NO_DATA;
}

SQLRETURN 
odbcPrepare(SQLHSTMT stmt, SQLCHAR *query)
{
  STMT *s = (STMT*)stmt;
  if (query == NULL) {
    odbcError("failed to prepare query from taos, reason: empty sql");
    setstatd(s->dbc, -1, "failed to prepare query from taos", "HY000");
    return SQL_ERROR;
  }

  int len = (int)min((size_t)strlen((char*)query), sizeof(s->sql));
  strncpy(s->sql, (char*)query, (size_t)len);
  s->sql[len] = 0;
  if (s->result != NULL) {
    taos_free_result(s->result);
  }
  s->result = NULL;
  s->fields = NULL;
  s->row = NULL;
  s->numFields = 0;
  s->rowsAffacted = 0;
  s->rowsFetched = 0;
  
  s->isPreparedStmt = true;
  s->type = STMT_NORMAL_SQL;
  return odbcExecuteSql(s);
}

/**
 * Prepare HSTMT.
 * @param stmt statement handle
 * @param query query string
 * @param queryLen length of query string or SQL_NTS
 * @result ODBC error code
 */

SQLRETURN SQL_API 
SQLPrepare(SQLHSTMT stmt, SQLCHAR *query, SQLINTEGER queryLen)
{
  odbcDebug("SQLPrepare, stmt:%p, query:%s", stmt, query);
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcPrepare(stmt, query);
  HSTMT_UNLOCK(stmt);
  return ret;
}

/**
 * Prepare HSTMT (UNICODE version).
 * @param stmt statement handle
 * @param query query string
 * @param queryLen length of query string or SQL_NTS
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLPrepareW(SQLHSTMT stmt, SQLWCHAR *query, SQLINTEGER queryLen)
{
  SQLCHAR *buf = (SQLCHAR*)uc_to_utf_c(query, queryLen);
  odbcDebug("SQLPrepareW, stmt:%p, query:%s", stmt, buf);
  SQLRETURN ret = SQL_SUCCESS;

  HSTMT_LOCK(stmt);
  ret = odbcPrepare(stmt, buf);
  HSTMT_UNLOCK(stmt);

  free(buf);
  return ret;
}

/**
 * Execute query.
 * @param stmt statement handle
 * @result ODBC error code
 */

SQLRETURN SQL_API 
SQLExecute(SQLHSTMT stmt)
{
  SQLRETURN ret = SQL_SUCCESS;
  STMT* s = (STMT*)stmt;
  odbcDebug("SQLExecute, stmt:%p, prepared:%d", stmt, s->isPreparedStmt);

  if (!s->isPreparedStmt) {
    HSTMT_LOCK(stmt);
    s->type = STMT_NORMAL_SQL;
    ret = odbcExecuteSql(s);
    HSTMT_UNLOCK(stmt);
  }
  else {
    s->isPreparedStmt = false;
  }
 

  return ret;
}

/**
 * Execute query directly.
 * @param stmt statement handle
 * @param query query string
 * @param queryLen length of query string or SQL_NTS
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLExecDirect(SQLHSTMT stmt, SQLCHAR *query, SQLINTEGER queryLen)
{
  odbcDebug("SQLExecDirect, stmt:%p, sql:%s", stmt, query);

  HSTMT_LOCK(stmt);
  STMT* s = (STMT*)stmt;
  s->type = STMT_NORMAL_SQL;
  strncpy(s->sql, (char*)query, (size_t)TSDB_MAX_SQL_LEN);
  s->sql[TSDB_MAX_SQL_LEN - 1] = 0;
  SQLRETURN ret = odbcExecuteSql(stmt);
  s->isPreparedStmt = false;
  HSTMT_UNLOCK(stmt);

  return ret;
}

/**
 * Execute query directly (UNICODE version).
 * @param stmt statement handle
 * @param query query string
 * @param queryLen length of query string or SQL_NTS
 * @result ODBC error code
 */

SQLRETURN SQL_API 
SQLExecDirectW(SQLHSTMT stmt, SQLWCHAR *query, SQLINTEGER queryLen)
{
  SQLCHAR *buf = (SQLCHAR*)uc_to_utf_c(query, queryLen);
  odbcDebug("SQLExecDirectW, stmt:%p, sql:%s", stmt, buf);

  HSTMT_LOCK(stmt);
  STMT* s = (STMT*)stmt;
  s->type = STMT_NORMAL_SQL;
  strncpy(s->sql, (char*)buf, (size_t)TSDB_MAX_SQL_LEN);
  s->sql[TSDB_MAX_SQL_LEN - 1] = 0;
  SQLRETURN ret = odbcExecuteSql(stmt);
  s->isPreparedStmt = false;
  HSTMT_UNLOCK(stmt);

  free(buf);
  return ret;
}

static SQLRETURN
odbcDriverConnect(SQLHDBC dbc, SQLHWND hwnd,
  SQLCHAR *connIn, SQLSMALLINT connInLen,
  SQLCHAR *connOut, SQLSMALLINT connOutMax,
  SQLSMALLINT *connOutLen, SQLUSMALLINT drvcompl)
{
  DBC *d;
  int len;
  SQLRETURN ret = SQL_SUCCESS;
  char buf[SQL_MAX_MESSAGE_LENGTH * 8], dbname[SQL_MAX_MESSAGE_LENGTH];
  char dsn[SQL_MAX_MESSAGE_LENGTH];
  char server[SQL_MAX_MESSAGE_LENGTH];
  char uid[SQL_MAX_MESSAGE_LENGTH];
  char pwd[SQL_MAX_MESSAGE_LENGTH];
  char driver[SQL_MAX_MESSAGE_LENGTH];
  
  if (dbc == SQL_NULL_HDBC) {
    return SQL_INVALID_HANDLE;
  }
  if (drvcompl != SQL_DRIVER_COMPLETE &&
    drvcompl != SQL_DRIVER_COMPLETE_REQUIRED &&
    drvcompl != SQL_DRIVER_PROMPT &&
    drvcompl != SQL_DRIVER_NOPROMPT) {
    return SQL_NO_DATA;
  }
  d = (DBC *)dbc;
  if (d->con != NULL) {
    odbcError("dbc:%p, taos:%p, connection already established", d, d->con);
    setstatd(d, -1, "connection already established", "08002");
    return SQL_ERROR;
  }

  buf[0] = '\0';
  if (connInLen == SQL_NTS) {
    len = sizeof(buf) - 1;
  }
  else {
    len = (int)(min((size_t)connInLen, sizeof(buf) - 1));
  }
  if (connIn != NULL) {
    strncpy(buf, (char *)connIn, (size_t)len);
    buf[len] = 0;
  }
  buf[len] = '\0';
  if (!buf[0]) {
    setstatd(d, -1, "invalid connect attributes",
      (d->ov3) ? "HY090" : "S1090");
    return SQL_ERROR;
  }
  dsn[0] = '\0';
  odbcGetDsnAttr(buf, "DSN", dsn, sizeof(dsn));

  /* special case: connIn is sole DSN value without keywords */
  if (!dsn[0] && !strchr(buf, ';') && !strchr(buf, '=')) {
    strncpy(dsn, buf, sizeof(dsn) - 1);
    dsn[sizeof(dsn) - 1] = '\0';
  }

  driver[0] = '\0';
  odbcGetDsnAttr(buf, "driver", uid, sizeof(uid));
  if (dsn[0] && !driver[0]) {
    SQLGetPrivateProfileString(dsn, "driver", "",
      driver, sizeof(driver), ODBC_INI);
  }

  dbname[0] = '\0';
  odbcGetDsnAttr(buf, "database", dbname, sizeof(dbname));
  if (dsn[0] && !dbname[0]) {
    SQLGetPrivateProfileString(dsn, "database", "",
      dbname, sizeof(dbname), ODBC_INI);
  }

  server[0] = '\0';
  odbcGetDsnAttr(buf, "server", server, sizeof(server));
  if (dsn[0] && !server[0]) {
    SQLGetPrivateProfileString(dsn, "server", "",
      server, sizeof(server), ODBC_INI);
  }

  uid[0] = '\0';
  odbcGetDsnAttr(buf, "uid", uid, sizeof(uid));
  if (dsn[0] && !uid[0]) {
    SQLGetPrivateProfileString(dsn, "uid", "",
      uid, sizeof(uid), ODBC_INI);
  }

  pwd[0] = '\0';
  odbcGetDsnAttr(buf, "pwd", pwd, sizeof(pwd));
  if (dsn[0] && !pwd[0]) {
    SQLGetPrivateProfileString(dsn, "pwd", "",
      pwd, sizeof(pwd), ODBC_INI);
  }

  odbcDebug("dbc:%p, dsn:%s, server:%s, database:%s, uid:%s", dbc, dsn, server, dbname, uid);

#if defined(_WIN32) || defined(_WIN64)
  if (!dbname[0] && !dsn[0]) {
    BOOL success;
    odbcSetupSilent = true;
    if (!dsn[0])
      success = ConfigDSN(hwnd, ODBC_ADD_DSN, driver, buf);
    else 
      success = ConfigDSN(hwnd, ODBC_CONFIG_DSN, driver, buf);
    if (!success) {
      odbcError("dbc:%p,  setup dialog return false", dbc);
      setstatd(d, -1, "dsn setup dialog return false",
             (d->ov3) ? "HY000" : "S1000");
      return SQL_ERROR;
    }
    else {
      odbcGetInfoFromSetupDlg(dsn, server, dbname, uid, pwd);
    }
  }
#endif

  if (connOut || connOutLen) {
    int count;

    buf[0] = '\0';
    count = snprintf(buf, sizeof(buf),
      "DSN=%s;Server=%s;Database=%s;"
      "UID=%s;PWD=%s",
      dsn, server, dbname, uid, pwd);
    if (count < 0) {
      buf[sizeof(buf) - 1] = '\0';
    }
    len = min(connOutMax - 1, (int)strlen(buf));
    if (connOut) {
      strncpy((char *)connOut, buf, (size_t)len);
      connOut[len] = '\0';
    }
    if (connOutLen) {
      *connOutLen = (SQLSMALLINT)len;
    }
  }
  
  odbcDebug("dbc:%p, dsn:%s, server:%s, database:%s, uid:%s, connOut", dbc, dsn, server, dbname, uid);
  if (dsn[0] || dbname[0]) {
    ret = odbcTaosConnect(d, dsn, server, dbname, uid, pwd);
  }

  return ret;
}

/**
 * Connect using a driver connection string.
 * @param dbc database connection handle
 * @param hwnd parent window handle
 * @param connIn driver connect input string
 * @param connInLen length of driver connect input string or SQL_NTS
 * @param connOut driver connect output string
 * @param connOutMax length of driver connect output string
 * @param connOutLen output length of driver connect output string
 * @param drvcompl completion type
 * @result ODBC error code
 */

SQLRETURN SQL_API SQLDriverConnect(SQLHDBC dbc, SQLHWND hwnd,
  SQLCHAR *connIn, SQLSMALLINT connInLen,
  SQLCHAR *connOut, SQLSMALLINT connOutMax,
  SQLSMALLINT *connOutLen, SQLUSMALLINT drvcompl)
{
  odbcDebug("SQLDriverConnect, dbc:%p, hwnd:%d, connIn:%s, connOut:%p, connOutMax:%d, drvcompl:%d:%s", dbc, hwnd, connIn, connOut, connOutMax, drvcompl, odbcDriverCompleteString(drvcompl));

  SQLRETURN ret = SQL_SUCCESS;
  
  HDBC_LOCK(dbc);
  ret = odbcDriverConnect(dbc, hwnd, connIn, connInLen,
    connOut, connOutMax, connOutLen, drvcompl);
  HDBC_UNLOCK(dbc);
  return ret;
}

/**
 * Connect using a driver connection string (UNICODE version).
 * @param dbc database connection handle
 * @param hwnd parent window handle
 * @param connIn driver connect input string
 * @param connInLen length of driver connect input string or SQL_NTS
 * @param connOut driver connect output string
 * @param connOutMax length of driver connect output string
 * @param connOutLen output length of driver connect output string
 * @param drvcompl completion type
 * @result ODBC error code
 */

SQLRETURN SQL_API SQLDriverConnectW(SQLHDBC dbc, SQLHWND hwnd,
  SQLWCHAR *connIn, SQLSMALLINT connInLen,
  SQLWCHAR *connOut, SQLSMALLINT connOutMax,
  SQLSMALLINT *connOutLen, SQLUSMALLINT drvcompl)
{
  SQLRETURN ret = SQL_SUCCESS;
  char *ci = NULL;
  SQLSMALLINT len = 0;

  HDBC_LOCK(dbc);
  if (connIn) {
    ci = uc_to_utf(connIn, connInLen);
    if (!ci) {
      DBC *d = (DBC *)dbc;

      setstatd(d, -1, "out of memory", (d->ov3) ? "HY000" : "S1000");
      HDBC_UNLOCK(dbc);
      odbcError("SQLDriverConnectW, dbc:%p, connIn:%s, drvcompl:%d:%s", dbc, ci, drvcompl,odbcDriverCompleteString(drvcompl));
      return SQL_ERROR;
    }
  }

  odbcDebug("SQLDriverConnectW, dbc:%p, hwnd:%d, connIn:%s, connOut:%p, connOutMax:%d, drvcompl:%d:%s", dbc, hwnd, ci, connOut, connOutMax, drvcompl, odbcDriverCompleteString(drvcompl));

  ret = odbcDriverConnect(dbc, hwnd, (SQLCHAR *)ci, SQL_NTS,
    (SQLCHAR *)connOut, connOutMax, &len, drvcompl);
  HDBC_UNLOCK(dbc);
  uc_free(ci);
  if (ret == SQL_SUCCESS) {
    SQLWCHAR *co = NULL;

    if (connOut) {
      if (len > 0) {
        co = uc_from_utf((SQLCHAR *)connOut, len);
        if (co) {
          uc_strncpy(connOut, co, (int)((size_t)connOutMax / sizeof(SQLWCHAR)));
          len = (SQLSMALLINT)(min((SQLSMALLINT)connOutMax / (SQLSMALLINT)sizeof(SQLWCHAR), (SQLSMALLINT)uc_strlen(co)));
          uc_free(co);
        }
        else {
          len = 0;
        }
      }
      if (len <= 0) {
        len = 0;
        connOut[0] = 0;
      }
    }
    else {
      len = 0;
    }
    if (connOutLen) {
      *connOutLen = len;
    }
  }
  return ret;
}
