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

#ifndef TDENGINE_TSQL_H
#define TDENGINE_TSQL_H

#ifdef __cplusplus
extern "C" {
#endif

#include "taos.h"
#include "tsqldef.h"
#include "ttypes.h"

#define TK_SPACE   200
#define TK_COMMENT 201
#define TK_ILLEGAL 202
#define TK_HEX     203
#define TK_OCT     204

#define TSQL_SO_ASC  1
#define TSQL_SO_DESC 0

#define MAX_TOKEN_LEN 30

#define TSQL_TBNAME   "TBNAME"
#define TSQL_TBNAME_L "tbname"

#define TSQL_STABLE_QTYPE_COND 1
#define TSQL_STABLE_QTYPE_SET  2

// token type
enum {
  TSQL_NODE_TYPE_EXPR = 0x1,
  TSQL_NODE_TYPE_ID = 0x2,
  TSQL_NODE_TYPE_VALUE = 0x4,
};

extern char tTokenTypeSwitcher[12];

#define toTSDBType(x)                          \
  do {                                         \
    if ((x) >= tListLen(tTokenTypeSwitcher)) { \
      (x) = TSDB_DATA_TYPE_BINARY;             \
    } else {                                   \
      (x) = tTokenTypeSwitcher[(x)];           \
    }                                          \
  } while (0)

typedef struct SLimitVal {
  int64_t limit;
  int64_t offset;
} SLimitVal;

typedef struct SOrderVal {
  int32_t order;
  int32_t orderColId;
} SOrderVal;

typedef struct tVariantListItem {
  tVariant pVar;
  uint8_t  sortOrder;
} tVariantListItem;

typedef struct tVariantList {
  int32_t           nExpr;  /* Number of expressions on the list */
  int32_t           nAlloc; /* Number of entries allocated below */
  tVariantListItem *a;      /* One entry for each expression */
} tVariantList;

typedef struct tFieldList {
  int32_t     nField;
  int32_t     nAlloc;
  TAOS_FIELD *p;
} tFieldList;

// sql operation type
enum TSQL_TYPE {
  TSQL_CREATE_NORMAL_METER = 0x01,
  TSQL_CREATE_NORMAL_METRIC = 0x02,
  TSQL_CREATE_METER_FROM_METRIC = 0x04,
  TSQL_CREATE_STREAM = 0x08,
  TSQL_QUERY_METER = 0x10,
  TSQL_INSERT = 0x20,

  DROP_DNODE = 0x40,
  DROP_DATABASE = 0x41,
  DROP_TABLE = 0x42,
  DROP_USER = 0x43,
  DROP_ACCOUNT = 0x44,

  USE_DATABASE = 0x50,

  // show operation
  SHOW_DATABASES = 0x60,
  SHOW_TABLES = 0x61,
  SHOW_STABLES = 0x62,
  SHOW_MNODES = 0x63,
  SHOW_DNODES = 0x64,
  SHOW_ACCOUNTS = 0x65,
  SHOW_USERS = 0x66,
  SHOW_VGROUPS = 0x67,
  SHOW_QUERIES = 0x68,
  SHOW_STREAMS = 0x69,
  SHOW_CONFIGS = 0x6a,
  SHOW_SCORES = 0x6b,
  SHOW_MODULES = 0x6c,
  SHOW_CONNECTIONS = 0x6d,
  SHOW_GRANTS = 0x6e,

  // create dnode
  CREATE_DNODE = 0x80,
  CREATE_DATABASE = 0x81,
  CREATE_USER = 0x82,
  CREATE_ACCOUNT = 0x83,

  DESCRIBE_TABLE = 0x90,

  ALTER_USER_PASSWD = 0xA0,
  ALTER_USER_PRIVILEGES = 0xA1,
  ALTER_DNODE = 0xA2,
  ALTER_LOCAL = 0xA3,
  ALTER_DATABASE = 0xA4,
  ALTER_ACCT = 0xA5,

  // reset operation
  RESET_QUERY_CACHE = 0xB0,

  // alter tags
  ALTER_TABLE_TAGS_ADD = 0xC0,
  ALTER_TABLE_TAGS_DROP = 0xC1,
  ALTER_TABLE_TAGS_CHG = 0xC2,
  ALTER_TABLE_TAGS_SET = 0xC4,

  // alter table column
  ALTER_TABLE_ADD_COLUMN = 0xD0,
  ALTER_TABLE_DROP_COLUMN = 0xD1,

  KILL_QUERY = 0xD2,
  KILL_STREAM = 0xD3,
  KILL_CONNECTION = 0xD4,
};

typedef struct SQuerySQL {
  struct tSQLExprList *pSelection;   // select clause
  struct SSQLToken     from;         // from clause
  struct tSQLExpr *    pWhere;       // where clause [optional]
  tVariantList *       pGroupby;     // groupby clause, only for tags[optional]
  tVariantList *       pSortOrder;   // orderby [optional]
  SSQLToken            interval;     // interval [optional]
  SSQLToken            sliding;      // sliding window [optional]
  SLimitVal            limit;        // limit offset [optional]
  SLimitVal            glimit;       // group limit offset [optional]
  tVariantList *       fillType;     // fill type[optional]
  SSQLToken            selectToken;  // sql string
} SQuerySQL;

typedef struct SCreateTableSQL {
  struct SSQLToken name;  // meter name, create table [meterName] xxx
  bool             existCheck;

  struct {
    tFieldList *pTagColumns;  // for normal table, pTagColumns = NULL;
    tFieldList *pColumns;
  } colInfo;

  struct {
    SSQLToken     metricName;  // metric name, for using clause
    tVariantList *pTagVals;    // create by using metric, tag value
  } usingInfo;

  SQuerySQL *pSelect;

} SCreateTableSQL;

typedef struct SAlterTableSQL {
  SSQLToken     name;
  tFieldList *  pAddColumns;
  SSQLToken     dropTagToken;
  tVariantList *varList;  // set t=val or: change src dst
} SAlterTableSQL;

typedef struct SInsertSQL {
  SSQLToken                name;
  struct tSQLExprListList *pValue;
} SInsertSQL;

typedef struct SCreateDBInfo {
  SSQLToken dbname;
  int32_t   replica;
  int32_t   cacheBlockSize;
  int32_t   tablesPerVnode;
  int32_t   daysPerFile;
  int32_t   rowPerFileBlock;

  float   numOfAvgCacheBlocks;
  int32_t numOfBlocksPerTable;

  int64_t   commitTime;
  int32_t   commitLog;
  int32_t   compressionLevel;
  SSQLToken precision;

  tVariantList *keep;
} SCreateDBInfo;

typedef struct SCreateAcctSQL {
  int32_t   users;
  int32_t   dbs;
  int32_t   tseries;
  int32_t   streams;
  int32_t   pps;
  int64_t   storage;
  int64_t   qtime;
  int32_t   conns;
  SSQLToken stat;
} SCreateAcctSQL;

typedef struct tDCLSQL {
  int32_t    nTokens; /* Number of expressions on the list */
  int32_t    nAlloc;  /* Number of entries allocated below */
  SSQLToken *a;       /* one entry for element */

  union {
    SCreateDBInfo   dbOpt;
    SCreateAcctSQL acctOpt;
  };
} tDCLSQL;

typedef struct SSqlInfo {
  int32_t sqlType;
  bool    validSql;

  union {
    SCreateTableSQL *pCreateTableInfo;
    SInsertSQL *     pInsertInfo;
    SAlterTableSQL * pAlterInfo;
    SQuerySQL *      pQueryInfo;
    tDCLSQL *        pDCLInfo;
  };

  char pzErrMsg[256];
} SSqlInfo;

typedef struct tSQLExpr {
  /*
   * for single operand:
   * TK_ALL
   * TK_ID
   * TK_SUM
   * TK_AVG
   * TK_MIN
   * TK_MAX
   * TK_FIRST
   * TK_LAST
   * TK_BOTTOM
   * TK_TOP
   * TK_STDDEV
   * TK_PERCENTILE
   *
   * for binary operand:
   * TK_LESS
   * TK_LARGE
   * TK_EQUAL etc...
   */
  uint32_t nSQLOptr;  // TK_FUNCTION: sql function, TK_LE: less than(binary expr)

  // the full sql string of function(col, param), which is actually the raw
  // field name,
  // since the function name is kept in nSQLOptr already
  SSQLToken            operand;
  struct tSQLExprList *pParam;  // function parameters

  SSQLToken colInfo;  // field id
  tVariant  val;      // value only for string, float, int

  struct tSQLExpr *pLeft;   // left child
  struct tSQLExpr *pRight;  // right child
} tSQLExpr;

// used in select clause. select <tSQLExprList> from xxx
typedef struct tSQLExprItem {
  tSQLExpr *pNode;      // The list of expressions
  char *    aliasName;  // alias name, null-terminated string
} tSQLExprItem;

typedef struct tSQLExprList {
  int32_t       nExpr;  /* Number of expressions on the list */
  int32_t       nAlloc; /* Number of entries allocated below */
  tSQLExprItem *a;      /* One entry for each expression */
} tSQLExprList;

typedef struct tSQLExprListList {
  int32_t        nList;  /* Number of expressions on the list */
  int32_t        nAlloc; /* Number of entries allocated below */
  tSQLExprList **a;      /* one entry for each row */
} tSQLExprListList;

#define ParseTOKENTYPE SSQLToken

void *ParseAlloc(void *(*mallocProc)(size_t));

/**
 *
 * @param yyp      The parser
 * @param yymajor  The major token code number
 * @param yyminor  The value for the token
 */
void Parse(void *yyp, int yymajor, ParseTOKENTYPE yyminor, SSqlInfo *);

/**
 *
 * @param p         The parser to be deleted
 * @param freeProc  Function used to reclaim memory
 */
void ParseFree(void *p, void (*freeProc)(void *));

tVariantList *tVariantListAppendToken(tVariantList *pList, SSQLToken *pAliasToken, uint8_t sortOrder);
tVariantList *tVariantListAppend(tVariantList *pList, tVariant *pVar, uint8_t sortOrder);

tVariantList *tVariantListInsert(tVariantList *pList, tVariant *pVar, uint8_t sortOrder, int32_t index);

void tVariantListDestroy(tVariantList *pList);

tFieldList *tFieldListAppend(tFieldList *pList, TAOS_FIELD *pField);

void tFieldListDestroy(tFieldList *pList);

tSQLExpr *tSQLExprCreate(tSQLExpr *pLeft, tSQLExpr *pRight, int32_t optType);

void tSQLExprDestroy(tSQLExpr *);

tSQLExprList *tSQLExprListAppend(tSQLExprList *pList, tSQLExpr *pNode, SSQLToken *pToken);

void tSQLExprListDestroy(tSQLExprList *pList);

int32_t tSQLSyntaxNodeToString(tSQLExpr *pNode, char *dst);

SQuerySQL *tSetQuerySQLElems(SSQLToken *pSelectToken, tSQLExprList *pSelection, SSQLToken *pFrom, tSQLExpr *pWhere,
                             tVariantList *pGroupby, tVariantList *pSortOrder, SSQLToken *pInterval,
                             SSQLToken *pSliding, tVariantList *pFill, SLimitVal *pLimit, SLimitVal *pGLimit);

SCreateTableSQL *tSetCreateSQLElems(tFieldList *pCols, tFieldList *pTags, SSQLToken *pMetricName,
                                    tVariantList *pTagVals, SQuerySQL *pSelect, int32_t type);

SAlterTableSQL *tAlterTableSQLElems(SSQLToken *pMeterName, tFieldList *pCols, tVariantList *pVals, int32_t type);

tSQLExprListList *tSQLListListAppend(tSQLExprListList *pList, tSQLExprList *pExprList);

void tSetInsertSQLElems(SSqlInfo *pInfo, SSQLToken *pName, tSQLExprListList *pList);

void destroyQuerySql(SQuerySQL *pSql);

void setSQLInfo(SSqlInfo *pInfo, void *pSqlExprInfo, SSQLToken *pMeterName, int32_t type);

void setCreatedMeterName(SSqlInfo *pInfo, SSQLToken *pMeterName, SSQLToken *pIfNotExists);

void SQLInfoDestroy(SSqlInfo *pInfo);

void setDCLSQLElems(SSqlInfo *pInfo, int32_t type, int32_t nParams, ...);

tDCLSQL *tTokenListAppend(tDCLSQL *pTokenList, SSQLToken *pToken);

void setCreateDBSQL(SSqlInfo *pInfo, int32_t type, SSQLToken *pToken, SCreateDBInfo *pDB, SSQLToken *pIgExists);

void setCreateAcctSQL(SSqlInfo *pInfo, int32_t type, SSQLToken *pName, SSQLToken *pPwd, SCreateAcctSQL *pAcctInfo);

// prefix show db.tables;
void setDBName(SSQLToken *pCpxName, SSQLToken *pDB);

tSQLExpr *tSQLExprIdValueCreate(SSQLToken *pToken, int32_t optType);

tSQLExpr *tSQLExprCreateFunction(tSQLExprList *pList, SSQLToken *pFuncToken, SSQLToken *endToken, int32_t optType);

void tSQLSetColumnInfo(TAOS_FIELD *pField, SSQLToken *pName, TAOS_FIELD *pType);

void tSQLSetColumnType(TAOS_FIELD *pField, SSQLToken *pToken);

int32_t tSQLParse(SSqlInfo *pSQLInfo, const char *pSql);

#ifdef __cplusplus
}
#endif

#endif
