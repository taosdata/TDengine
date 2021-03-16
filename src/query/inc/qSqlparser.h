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

#include "taos.h"
#include "taosmsg.h"
#include "tstoken.h"
#include "tstrbuild.h"
#include "tvariant.h"

#define ParseTOKENTYPE SStrToken

#define NON_ARITHMEIC_EXPR 0
#define NORMAL_ARITHMETIC  1
#define AGG_ARIGHTMEIC     2

enum SQL_NODE_TYPE {
  SQL_NODE_TABLE_COLUMN= 1,
  SQL_NODE_SQLFUNCTION = 2,
  SQL_NODE_VALUE       = 3,
  SQL_NODE_EXPR        = 4,
};

extern char tTokenTypeSwitcher[13];

#define toTSDBType(x)                          \
  do {                                         \
    if ((x) >= tListLen(tTokenTypeSwitcher)) { \
      (x) = TSDB_DATA_TYPE_BINARY;             \
    } else {                                   \
      (x) = tTokenTypeSwitcher[(x)];           \
    }                                          \
  } while (0)

#define TPARSER_HAS_TOKEN(_t)      ((_t).n > 0)
#define TPARSER_SET_NONE_TOKEN(_t) ((_t).n = 0)

typedef struct SLimitVal {
  int64_t            limit;
  int64_t            offset;
} SLimitVal;

typedef struct SOrderVal {
  uint32_t           order;
  int32_t            orderColId;
} SOrderVal;

typedef struct tVariantListItem {
  tVariant           pVar;
  uint8_t            sortOrder;
} tVariantListItem;

typedef struct SIntervalVal {
  SStrToken          interval;
  SStrToken          offset;
} SIntervalVal;

typedef struct SSessionWindowVal {
  SStrToken          col;
  SStrToken          gap;
} SSessionWindowVal;

typedef struct SQuerySqlNode {
  struct SArray     *pSelectList;  // select clause
  SArray            *from;         // from clause  SArray<SQuerySqlNode>
  struct tSqlExpr   *pWhere;       // where clause [optional]
  SArray            *pGroupby;     // groupby clause, only for tags[optional], SArray<tVariantListItem>
  SArray            *pSortOrder;   // orderby [optional], SArray<tVariantListItem>
  SArray            *fillType;     // fill type[optional], SArray<tVariantListItem>
  SIntervalVal       interval;     // (interval, interval_offset) [optional]
  SSessionWindowVal  sessionVal;   // session window [optional]
  SStrToken          sliding;      // sliding window [optional]
  SLimitVal          limit;        // limit offset [optional]
  SLimitVal          slimit;       // group limit offset [optional]
  SStrToken          sqlstr;       // sql string in select clause
} SQuerySqlNode;

typedef struct SCreatedTableInfo {
  SStrToken          name;        // table name token
  SStrToken          stableName;  // super table name token , for using clause
  SArray            *pTagNames;   // create by using super table, tag name
  SArray            *pTagVals;    // create by using super table, tag value
  char              *fullname;    // table full name
  STagData           tagdata;     // true tag data, super table full name is in STagData
  int8_t             igExist;     // ignore if exists
} SCreatedTableInfo;

typedef struct SCreateTableSql {
  SStrToken          name;  // table name, create table [name] xxx
  int8_t             type;  // create normal table/from super table/ stream
  bool               existCheck;

  struct {
    SArray          *pTagColumns; // SArray<TAOS_FIELD>
    SArray          *pColumns;    // SArray<TAOS_FIELD>
  } colInfo;

  SArray            *childTableInfo;        // SArray<SCreatedTableInfo>
  SQuerySqlNode     *pSelect;
} SCreateTableSql;

typedef struct SAlterTableInfo {
  SStrToken          name;
  int16_t            tableType;
  int16_t            type;
  STagData           tagData;
  SArray            *pAddColumns; // SArray<TAOS_FIELD>
  SArray            *varList;     // set t=val or: change src dst, SArray<tVariantListItem>
} SAlterTableInfo;

typedef struct SCreateDbInfo {
  SStrToken          dbname;
  int32_t            replica;
  int32_t            cacheBlockSize;
  int32_t            maxTablesPerVnode;
  int32_t            numOfBlocks;
  int32_t            daysPerFile;
  int32_t            minRowsPerBlock;
  int32_t            maxRowsPerBlock;
  int32_t            fsyncPeriod;
  int64_t            commitTime;
  int32_t            walLevel;
  int32_t            quorum;
  int32_t            compressionLevel;
  SStrToken          precision;
  bool               ignoreExists;
  int8_t             update;
  int8_t             cachelast;
  SArray            *keep;
  int8_t             dbType;
  int16_t            partitions;
} SCreateDbInfo;

typedef struct SCreateAcctInfo {
  int32_t            maxUsers;
  int32_t            maxDbs;
  int32_t            maxTimeSeries;
  int32_t            maxStreams;
  int32_t            maxPointsPerSecond;
  int64_t            maxStorage;
  int64_t            maxQueryTime;
  int32_t            maxConnections;
  SStrToken          stat;
} SCreateAcctInfo;

typedef struct SShowInfo {
  uint8_t            showType;
  SStrToken          prefix;
  SStrToken          pattern;
} SShowInfo;

typedef struct SUserInfo {
  SStrToken          user;
  SStrToken          passwd;
  SStrToken          privilege;
  int16_t            type;
} SUserInfo;

typedef struct SMiscInfo {
  SArray            *a;         // SArray<SStrToken>
  bool               existsCheck;
  int16_t            dbType;
  int16_t            tableType;
  SUserInfo          user;
  union {
    SCreateDbInfo    dbOpt;
    SCreateAcctInfo  acctOpt;
    SShowInfo        showOpt;
    SStrToken        id;
  };
} SMiscInfo;

typedef struct SSubclauseInfo {  // "UNION" multiple select sub-clause
  SQuerySqlNode    **pClause;
  int32_t            numOfClause;
} SSubclauseInfo;

typedef struct SSqlInfo {
  int32_t            type;
  bool               valid;
  SSubclauseInfo     subclauseInfo;
  char               msg[256];
  union {
    SCreateTableSql *pCreateTableInfo;
    SAlterTableInfo *pAlterInfo;
    SMiscInfo       *pMiscInfo;
  };
} SSqlInfo;

typedef struct tSqlExpr {
  uint16_t           type;       // sql node type
  uint32_t           tokenId;    // TK_LE: less than(binary expr)

  // the whole string of the function(col, param), while the function name is kept in token
  SStrToken          operand;
  uint32_t           functionId;  // function id

  SStrToken          colInfo;     // table column info
  tVariant           value;       // the use input value
  SStrToken          token;       // original sql expr string

  struct tSqlExpr   *pLeft;       // left child
  struct tSqlExpr   *pRight;      // right child
  struct SArray     *pParam;      // function parameters list
} tSqlExpr;

// used in select clause. select <SArray> from xxx
typedef struct tSqlExprItem {
  tSqlExpr          *pNode;      // The list of expressions
  char              *aliasName;  // alias name, null-terminated string
  bool               distinct;
} tSqlExprItem;

SArray *tVariantListAppend(SArray *pList, tVariant *pVar, uint8_t sortOrder);
SArray *tVariantListInsert(SArray *pList, tVariant *pVar, uint8_t sortOrder, int32_t index);
SArray *tVariantListAppendToken(SArray *pList, SStrToken *pAliasToken, uint8_t sortOrder);

// sql expr leaf node
tSqlExpr *tSqlExprCreateIdValue(SStrToken *pToken, int32_t optrType);
tSqlExpr *tSqlExprCreateFunction(SArray *pParam, SStrToken *pFuncToken, SStrToken *endToken, int32_t optType);

tSqlExpr *tSqlExprCreate(tSqlExpr *pLeft, tSqlExpr *pRight, int32_t optrType);
tSqlExpr *tSqlExprClone(tSqlExpr *pSrc);
void      tSqlExprCompact(tSqlExpr** pExpr);
bool      tSqlExprIsLeaf(tSqlExpr* pExpr);
bool      tSqlExprIsParentOfLeaf(tSqlExpr* pExpr);
void      tSqlExprDestroy(tSqlExpr *pExpr);
SArray   *tSqlExprListAppend(SArray *pList, tSqlExpr *pNode, SStrToken *pDistinct, SStrToken *pToken);
void      tSqlExprListDestroy(SArray *pList);

SQuerySqlNode *tSetQuerySqlNode(SStrToken *pSelectToken, SArray *pSelectList, SArray *pFrom, tSqlExpr *pWhere,
                                SArray *pGroupby, SArray *pSortOrder, SIntervalVal *pInterval, SSessionWindowVal *ps,
                                SStrToken *pSliding, SArray *pFill, SLimitVal *pLimit, SLimitVal *pgLimit);

SCreateTableSql *tSetCreateTableInfo(SArray *pCols, SArray *pTags, SQuerySqlNode *pSelect, int32_t type);

SAlterTableInfo *tSetAlterTableInfo(SStrToken *pTableName, SArray *pCols, SArray *pVals, int32_t type, int16_t tableTable);
SCreatedTableInfo createNewChildTableInfo(SStrToken *pTableName, SArray *pTagNames, SArray *pTagVals, SStrToken *pToken, SStrToken* igExists);

void destroyAllSelectClause(SSubclauseInfo *pSql);
void destroyQuerySqlNode(SQuerySqlNode *pSql);
void freeCreateTableInfo(void* p);

SSqlInfo       *setSqlInfo(SSqlInfo *pInfo, void *pSqlExprInfo, SStrToken *pTableName, int32_t type);
SSubclauseInfo *setSubclause(SSubclauseInfo *pClause, void *pSqlExprInfo);

SSubclauseInfo *appendSelectClause(SSubclauseInfo *pInfo, void *pSubclause);

void setCreatedTableName(SSqlInfo *pInfo, SStrToken *pTableNameToken, SStrToken *pIfNotExists);

void SqlInfoDestroy(SSqlInfo *pInfo);

void setDCLSqlElems(SSqlInfo *pInfo, int32_t type, int32_t nParams, ...);
void setDropDbTableInfo(SSqlInfo *pInfo, int32_t type, SStrToken* pToken, SStrToken* existsCheck,int16_t dbType,int16_t tableType);
void setShowOptions(SSqlInfo *pInfo, int32_t type, SStrToken* prefix, SStrToken* pPatterns);

void setCreateDbInfo(SSqlInfo *pInfo, int32_t type, SStrToken *pToken, SCreateDbInfo *pDB, SStrToken *pIgExists);

void setCreateAcctSql(SSqlInfo *pInfo, int32_t type, SStrToken *pName, SStrToken *pPwd, SCreateAcctInfo *pAcctInfo);
void setCreateUserSql(SSqlInfo *pInfo, SStrToken *pName, SStrToken *pPasswd);
void setKillSql(SSqlInfo *pInfo, int32_t type, SStrToken *ip);
void setAlterUserSql(SSqlInfo *pInfo, int16_t type, SStrToken *pName, SStrToken* pPwd, SStrToken *pPrivilege);

void setDefaultCreateDbOption(SCreateDbInfo *pDBInfo);
void setDefaultCreateTopicOption(SCreateDbInfo *pDBInfo);

// prefix show db.tables;
void tSetDbName(SStrToken *pCpxName, SStrToken *pDb);

void tSetColumnInfo(TAOS_FIELD *pField, SStrToken *pName, TAOS_FIELD *pType);
void tSetColumnType(TAOS_FIELD *pField, SStrToken *type);

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

/**
 *
 * @param mallocProc  The parser allocator
 * @return
 */
void *ParseAlloc(void *(*mallocProc)(size_t));

/**
 *
 * @param str sql string
 * @return sql ast
 */
SSqlInfo qSqlParse(const char *str);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_QSQLPARSER_H
