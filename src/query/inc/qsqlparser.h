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

#ifndef TDENGINE_QSQLPARSER_H
#define TDENGINE_QSQLPARSER_H

#ifdef __cplusplus
extern "C" {
#endif

#include <tstrbuild.h>
#include "taos.h"
#include "taosmsg.h"
#include "tstoken.h"
#include "tvariant.h"

#define ParseTOKENTYPE SSQLToken
extern char tTokenTypeSwitcher[13];

#define toTSDBType(x)                          \
  do {                                         \
    if ((x) >= tListLen(tTokenTypeSwitcher)) { \
      (x) = TSDB_DATA_TYPE_BINARY;             \
    } else {                                   \
      (x) = tTokenTypeSwitcher[(x)];           \
    }                                          \
  } while (0)
  
typedef struct tFieldList {
  int32_t     nField;
  int32_t     nAlloc;
  TAOS_FIELD *p;
} tFieldList;

typedef struct SLimitVal {
  int64_t limit;
  int64_t offset;
} SLimitVal;

typedef struct SOrderVal {
  uint32_t order;
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

typedef struct SQuerySQL {
  struct tSQLExprList *pSelection;   // select clause
  tVariantList *       from;         // from clause
  struct tSQLExpr *    pWhere;       // where clause [optional]
  tVariantList *       pGroupby;     // groupby clause, only for tags[optional]
  tVariantList *       pSortOrder;   // orderby [optional]
  SSQLToken            interval;     // interval [optional]
  SSQLToken            sliding;      // sliding window [optional]
  SLimitVal            limit;        // limit offset [optional]
  SLimitVal            slimit;       // group limit offset [optional]
  tVariantList *       fillType;     // fill type[optional]
  SSQLToken            selectToken;  // sql string
} SQuerySQL;

typedef struct SCreateTableSQL {
  struct SSQLToken name;  // meter name, create table [meterName] xxx
  bool             existCheck;
  
  int8_t           type; // create normal table/from super table/ stream
  struct {
    tFieldList *pTagColumns;  // for normal table, pTagColumns = NULL;
    tFieldList *pColumns;
  } colInfo;
  
  struct {
    SSQLToken     stableName;  // super table name, for using clause
    tVariantList *pTagVals;    // create by using metric, tag value
    STagData      tagdata;
  } usingInfo;
  
  SQuerySQL *pSelect;
} SCreateTableSQL;

typedef struct SAlterTableSQL {
  SSQLToken     name;
  int16_t       type;
  STagData      tagData;
  
  tFieldList *  pAddColumns;
  tVariantList *varList;  // set t=val or: change src dst
} SAlterTableSQL;

typedef struct SCreateDBInfo {
  SSQLToken dbname;
  int32_t   replica;
  int32_t   cacheBlockSize;
  int32_t   tablesPerVnode;
  int32_t   daysPerFile;
  int32_t   rowPerFileBlock;
  float     numOfAvgCacheBlocks;
  int32_t   numOfBlocksPerTable;
  int64_t   commitTime;
  int32_t   commitLog;
  int32_t   compressionLevel;
  SSQLToken precision;
  bool      ignoreExists;
  
  tVariantList *keep;
} SCreateDBInfo;

typedef struct SCreateAcctSQL {
  int32_t   maxUsers;
  int32_t   maxDbs;
  int32_t   maxTimeSeries;
  int32_t   maxStreams;
  int32_t   maxPointsPerSecond;
  int64_t   maxStorage;
  int64_t   maxQueryTime;
  int32_t   maxConnections;
  SSQLToken stat;
} SCreateAcctSQL;

typedef struct SShowInfo {
  uint8_t showType;
  SSQLToken prefix;
  SSQLToken pattern;
} SShowInfo;

typedef struct SUserInfo {
  SSQLToken user;
  SSQLToken passwd;
  SSQLToken privilege;
  int16_t   type;
} SUserInfo;

typedef struct tDCLSQL {
  int32_t    nTokens; /* Number of expressions on the list */
  int32_t    nAlloc;  /* Number of entries allocated below */
  SSQLToken *a;       /* one entry for element */
  bool  existsCheck;
  
  union {
    SCreateDBInfo  dbOpt;
    SCreateAcctSQL acctOpt;
    SShowInfo      showOpt;
    SSQLToken ip;
  };
  
  SUserInfo user;
  
} tDCLSQL;

typedef struct SSubclauseInfo {  // "UNION" multiple select sub-clause
  SQuerySQL **pClause;
  int32_t     numOfClause;
} SSubclauseInfo;

typedef struct SSqlInfo {
  int32_t type;
  bool    valid;
  
  union {
    SCreateTableSQL *pCreateTableInfo;
    SAlterTableSQL * pAlterInfo;
    tDCLSQL *        pDCLInfo;
  };
  
  SSubclauseInfo subclauseInfo;
  char           pzErrMsg[256];
} SSqlInfo;

typedef struct tSQLExpr {
  // TK_FUNCTION: sql function, TK_LE: less than(binary expr)
  uint32_t nSQLOptr;
  
  // the full sql string of function(col, param), which is actually the raw
  // field name, since the function name is kept in nSQLOptr already
  SSQLToken operand;
  SSQLToken colInfo;            // field id
  tVariant  val;                // value only for string, float, int
  
  struct tSQLExpr *pLeft;       // left child
  struct tSQLExpr *pRight;      // right child
  
  struct tSQLExprList *pParam;  // function parameters
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

tVariantList *tVariantListAppend(tVariantList *pList, tVariant *pVar, uint8_t sortOrder);

tVariantList *tVariantListInsert(tVariantList *pList, tVariant *pVar, uint8_t sortOrder, int32_t index);

tVariantList *tVariantListAppendToken(tVariantList *pList, SSQLToken *pAliasToken, uint8_t sortOrder);
void          tVariantListDestroy(tVariantList *pList);

tFieldList *tFieldListAppend(tFieldList *pList, TAOS_FIELD *pField);

void tFieldListDestroy(tFieldList *pList);

tSQLExpr *tSQLExprCreate(tSQLExpr *pLeft, tSQLExpr *pRight, int32_t optType);

void tSQLExprDestroy(tSQLExpr *);

tSQLExprList *tSQLExprListAppend(tSQLExprList *pList, tSQLExpr *pNode, SSQLToken *pToken);

void tSQLExprListDestroy(tSQLExprList *pList);

SQuerySQL *tSetQuerySQLElems(SSQLToken *pSelectToken, tSQLExprList *pSelection, tVariantList *pFrom, tSQLExpr *pWhere,
                             tVariantList *pGroupby, tVariantList *pSortOrder, SSQLToken *pInterval,
                             SSQLToken *pSliding, tVariantList *pFill, SLimitVal *pLimit, SLimitVal *pGLimit);

SCreateTableSQL *tSetCreateSQLElems(tFieldList *pCols, tFieldList *pTags, SSQLToken *pMetricName,
                                    tVariantList *pTagVals, SQuerySQL *pSelect, int32_t type);

void      tSQLExprNodeDestroy(tSQLExpr *pExpr);
tSQLExpr *tSQLExprNodeClone(tSQLExpr *pExpr);

SAlterTableSQL *tAlterTableSQLElems(SSQLToken *pMeterName, tFieldList *pCols, tVariantList *pVals, int32_t type);

tSQLExprListList *tSQLListListAppend(tSQLExprListList *pList, tSQLExprList *pExprList);

void destroyAllSelectClause(SSubclauseInfo *pSql);
void doDestroyQuerySql(SQuerySQL *pSql);

SSqlInfo *      setSQLInfo(SSqlInfo *pInfo, void *pSqlExprInfo, SSQLToken *pMeterName, int32_t type);
SSubclauseInfo *setSubclause(SSubclauseInfo *pClause, void *pSqlExprInfo);

SSubclauseInfo *appendSelectClause(SSubclauseInfo *pInfo, void *pSubclause);

void setCreatedTableName(SSqlInfo *pInfo, SSQLToken *pMeterName, SSQLToken *pIfNotExists);

void SQLInfoDestroy(SSqlInfo *pInfo);

void setDCLSQLElems(SSqlInfo *pInfo, int32_t type, int32_t nParams, ...);
void setDropDBTableInfo(SSqlInfo *pInfo, int32_t type, SSQLToken* pToken, SSQLToken* existsCheck);
void setShowOptions(SSqlInfo *pInfo, int32_t type, SSQLToken* prefix, SSQLToken* pPatterns);

tDCLSQL *tTokenListAppend(tDCLSQL *pTokenList, SSQLToken *pToken);

void setCreateDBSQL(SSqlInfo *pInfo, int32_t type, SSQLToken *pToken, SCreateDBInfo *pDB, SSQLToken *pIgExists);

void setCreateAcctSQL(SSqlInfo *pInfo, int32_t type, SSQLToken *pName, SSQLToken *pPwd, SCreateAcctSQL *pAcctInfo);
void setCreateUserSQL(SSqlInfo *pInfo, SSQLToken *pName, SSQLToken *pPasswd);
void setKillSQL(SSqlInfo *pInfo, int32_t type, SSQLToken *ip);
void setAlterUserSQL(SSqlInfo *pInfo, int16_t type, SSQLToken *pName, SSQLToken* pPwd, SSQLToken *pPrivilege);

void setDefaultCreateDbOption(SCreateDBInfo *pDBInfo);

// prefix show db.tables;
void setDBName(SSQLToken *pCpxName, SSQLToken *pDB);

tSQLExpr *tSQLExprIdValueCreate(SSQLToken *pToken, int32_t optType);

tSQLExpr *tSQLExprCreateFunction(tSQLExprList *pList, SSQLToken *pFuncToken, SSQLToken *endToken, int32_t optType);

void tSQLSetColumnInfo(TAOS_FIELD *pField, SSQLToken *pName, TAOS_FIELD *pType);

void tSQLSetColumnType(TAOS_FIELD *pField, SSQLToken *pToken);

void *ParseAlloc(void *(*mallocProc)(size_t));

// convert the sql filter expression into binary data
int32_t tSQLExprToBinary(tSQLExpr* pExpr, SStringBuilder* sb);

enum {
  TSQL_NODE_TYPE_EXPR  = 0x1,
  TSQL_NODE_TYPE_ID    = 0x2,
  TSQL_NODE_TYPE_VALUE = 0x4,
};

#define NON_ARITHMEIC_EXPR 0
#define NORMAL_ARITHMETIC  1
#define AGG_ARIGHTMEIC     2

int32_t tSQLParse(SSqlInfo *pSQLInfo, const char *pSql);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_QSQLPARSER_H
