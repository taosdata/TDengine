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

#ifndef TDENGINE_ASTGENERATOR_H
#define TDENGINE_ASTGENERATOR_H

#ifdef __cplusplus
extern "C" {
#endif

#include "ttoken.h"
#include "tvariant.h"
#include "parser.h"

#define ParseTOKENTYPE SToken

#define NON_ARITHMEIC_EXPR 0
#define NORMAL_ARITHMETIC  1
#define AGG_ARIGHTMEIC     2

enum SQL_NODE_TYPE {
  SQL_NODE_TABLE_COLUMN= 1,
  SQL_NODE_SQLFUNCTION = 2,
  SQL_NODE_VALUE       = 3,
  SQL_NODE_EXPR        = 4,
};

enum SQL_NODE_FROM_TYPE {
  SQL_NODE_FROM_SUBQUERY   = 1,
  SQL_NODE_FROM_TABLELIST  = 2,
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

typedef struct SListItem {
  SVariant           pVar;
  uint8_t            sortOrder;
} SListItem;

typedef struct SIntervalVal {
  int32_t            token;
  SToken             interval;
  SToken             offset;
} SIntervalVal;

typedef struct SSessionWindowVal {
  SToken          col;
  SToken          gap;
} SSessionWindowVal;

typedef struct SWindowStateVal {
  SToken          col;
} SWindowStateVal;

struct SRelationInfo;

typedef struct SSqlNode {
  struct SArray     *pSelNodeList; // select clause
  struct SRelationInfo  *from;     // from clause SArray<SSqlNode>
  struct tSqlExpr   *pWhere;       // where clause [optional]
  SArray            *pGroupby;     // groupby clause, only for tags[optional], SArray<SListItem>
  SArray            *pSortOrder;   // orderby [optional],  SArray<SListItem>
  SArray            *fillType;     // fill type[optional], SArray<SListItem>
  SIntervalVal       interval;     // (interval, interval_offset) [optional]
  SSessionWindowVal  sessionVal;   // session window [optional]
  SWindowStateVal    windowstateVal; // window_state(col) [optional]
  SToken             sliding;      // sliding window [optional]
  SLimit             limit;        // limit offset [optional]
  SLimit             slimit;       // group limit offset [optional]
  SToken             sqlstr;       // sql string in select clause
  struct tSqlExpr   *pHaving;      // having clause [optional]
} SSqlNode;

typedef struct SRelElementPair {
  union {
    SToken     tableName;
    SArray    *pSubquery;
  };

  SToken aliasName;
} SRelElementPair;

typedef struct SRelationInfo {
  int32_t           type;        // nested query|table name list
  SArray           *list;        // SArray<SRelElementPair>
} SRelationInfo;

typedef struct SCreatedTableInfo {
  SToken             name;        // table name token
  SToken             stableName;  // super table name token , for using clause
  SArray            *pTagNames;   // create by using super table, tag name
  SArray            *pTagVals;    // create by using super table, tag value
  char              *fullname;    // table full name
  STagData           tagdata;     // true tag data, super table full name is in STagData
  int8_t             igExist;     // ignore if exists
} SCreatedTableInfo;

typedef struct SCreateTableSql {
  SToken             name;  // table name, create table [name] xxx
  int8_t             type;  // create normal table/from super table/ stream
  bool               existCheck;

  struct {
    SArray          *pTagColumns; // SArray<SField>
    SArray          *pColumns;    // SArray<SField>
  } colInfo;

  SArray            *childTableInfo;        // SArray<SCreatedTableInfo>
  SSqlNode          *pSelect;
} SCreateTableSql;

typedef struct SAlterTableInfo {
  SToken             name;
  int16_t            tableType;
  int16_t            type;
  STagData           tagData;
  SArray            *pAddColumns; // SArray<SField>
  SArray            *varList;     // set t=val or: change src dst, SArray<SListItem>
} SAlterTableInfo;

typedef struct SCreateDbInfo {
  SToken             dbname;
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
  SToken             precision;
  bool               ignoreExists;
  int8_t             update;
  int8_t             cachelast;
  SArray            *keep;
  int8_t             dbType;
  int16_t            partitions;
} SCreateDbInfo;

typedef struct SCreateFuncInfo {
  SToken             name;
  SToken             path;
  int32_t            type;
  int32_t            bufSize;
  SField             output;
} SCreateFuncInfo;

typedef struct SCreateAcctInfo {
  int32_t            maxUsers;
  int32_t            maxDbs;
  int32_t            maxTimeSeries;
  int32_t            maxStreams;
  int32_t            maxPointsPerSecond;
  int64_t            maxStorage;
  int64_t            maxQueryTime;
  int32_t            maxConnections;
  SToken             stat;
} SCreateAcctInfo;

typedef struct SShowInfo {
  uint8_t            showType;
  SToken             prefix;
  SToken             pattern;
} SShowInfo;

typedef struct SUserInfo {
  SToken             user;
  SToken             passwd;
  SToken             privilege;
  int16_t            type;
} SUserInfo;

typedef struct SMiscInfo {
  SArray            *a;         // SArray<SToken>
  bool               existsCheck;
  int16_t            dbType;
  int16_t            tableType;
  SUserInfo          user;
  union {
    SCreateDbInfo    dbOpt;
    SCreateAcctInfo  acctOpt;
    SCreateFuncInfo  funcOpt;
    SShowInfo        showOpt;
    SToken           id;
  };
} SMiscInfo;

typedef struct SSqlInfo {
  int32_t            type;
  bool               valid;
  SArray            *list;    // todo refactor
  char               msg[256];
  SArray            *funcs;
  union {
    SCreateTableSql *pCreateTableInfo;
    SAlterTableInfo *pAlterInfo;
    SMiscInfo       *pMiscInfo;
  };
} SSqlInfo;

typedef struct tSqlExpr {
  uint16_t           type;       // sql node type
  uint32_t           tokenId;    // TK_LE: less than(binary expr)

  // The complete string of the function(col, param), and the function name is kept in exprToken
  struct {
    SToken           operand;
    struct SArray   *paramList;      // function parameters list
  } Expr;

  SToken             columnName;  // table column info
  SVariant           value;       // the use input value
  SToken             exprToken;   // original sql expr string  or function name of sql function
  struct tSqlExpr   *pLeft;       // the left child
  struct tSqlExpr   *pRight;      // the right child
} tSqlExpr;

// used in select clause. select <SArray> from xxx
typedef struct tSqlExprItem {
  tSqlExpr          *pNode;      // The list of expressions
  char              *aliasName;  // alias name, null-terminated string
  bool               distinct;
} tSqlExprItem;

SArray *tListItemAppend(SArray *pList, SVariant *pVar, uint8_t sortOrder);
SArray *tListItemInsert(SArray *pList, SVariant *pVar, uint8_t sortOrder, int32_t index);
SArray *tListItemAppendToken(SArray *pList, SToken *pAliasToken, uint8_t sortOrder);

SRelationInfo *setTableNameList(SRelationInfo *pRelationInfo, SToken *pName, SToken *pAlias);
void *         destroyRelationInfo(SRelationInfo *pFromInfo);
SRelationInfo *addSubquery(SRelationInfo *pRelationInfo, SArray *pSub, SToken *pAlias);

// sql expr leaf node
tSqlExpr *tSqlExprCreateIdValue(SToken *pToken, int32_t optrType);
tSqlExpr *tSqlExprCreateFunction(SArray *pParam, SToken *pFuncToken, SToken *endToken, int32_t optType);
SArray *  tRecordFuncName(SArray *pList, SToken *pToken);

tSqlExpr *tSqlExprCreate(tSqlExpr *pLeft, tSqlExpr *pRight, int32_t optrType);
tSqlExpr *tSqlExprClone(tSqlExpr *pSrc);
void      tSqlExprCompact(tSqlExpr **pExpr);
bool      tSqlExprIsLeaf(tSqlExpr *pExpr);
bool      tSqlExprIsParentOfLeaf(tSqlExpr *pExpr);
void      tSqlExprDestroy(tSqlExpr *pExpr);
SArray *  tSqlExprListAppend(SArray *pList, tSqlExpr *pNode, SToken *pDistinct, SToken *pToken);
void      tSqlExprListDestroy(SArray *pList);
void      tSqlExprEvaluate(tSqlExpr* pExpr);

SSqlNode *tSetQuerySqlNode(SToken *pSelectToken, SArray *pSelNodeList, SRelationInfo *pFrom, tSqlExpr *pWhere,
                           SArray *pGroupby, SArray *pSortOrder, SIntervalVal *pInterval, SSessionWindowVal *ps,
                           SWindowStateVal *pw, SToken *pSliding, SArray *pFill, SLimit *pLimit, SLimit *pgLimit, tSqlExpr *pHaving);
int32_t   tSqlExprCompare(tSqlExpr *left, tSqlExpr *right);

SCreateTableSql *tSetCreateTableInfo(SArray *pCols, SArray *pTags, SSqlNode *pSelect, int32_t type);

SAlterTableInfo * tSetAlterTableInfo(SToken *pTableName, SArray *pCols, SArray *pVals, int32_t type, int16_t tableType);
SCreatedTableInfo createNewChildTableInfo(SToken *pTableName, SArray *pTagNames, SArray *pTagVals, SToken *pToken,
                                          SToken *igExists);

void destroyAllSqlNode(SArray *pSqlNode);
void destroySqlNode(SSqlNode *pSql);
void freeCreateTableInfo(void* p);

SSqlInfo *setSqlInfo(SSqlInfo *pInfo, void *pSqlExprInfo, SToken *pTableName, int32_t type);
SArray   *setSubclause(SArray *pList, void *pSqlNode);
SArray   *appendSelectClause(SArray *pList, void *pSubclause);

void setCreatedTableName(SSqlInfo *pInfo, SToken *pTableNameToken, SToken *pIfNotExists);
void* destroyCreateTableSql(SCreateTableSql* pCreate);
void setDropFuncInfo(SSqlInfo *pInfo, int32_t type, SToken* pToken);
void setCreateFuncInfo(SSqlInfo *pInfo, int32_t type, SToken *pName, SToken *pPath, SField *output, SToken* bufSize, int32_t funcType);

void SqlInfoDestroy(SSqlInfo *pInfo);

void setDCLSqlElems(SSqlInfo *pInfo, int32_t type, int32_t nParams, ...);
void setDropDbTableInfo(SSqlInfo *pInfo, int32_t type, SToken* pToken, SToken* existsCheck,int16_t dbType,int16_t tableType);
void setShowOptions(SSqlInfo *pInfo, int32_t type, SToken* prefix, SToken* pPatterns);

void setCreateDbInfo(SSqlInfo *pInfo, int32_t type, SToken *pToken, SCreateDbInfo *pDB, SToken *pIgExists);

void setCreateAcctSql(SSqlInfo *pInfo, int32_t type, SToken *pName, SToken *pPwd, SCreateAcctInfo *pAcctInfo);
void setCreateUserSql(SSqlInfo *pInfo, SToken *pName, SToken *pPasswd);
void setKillSql(SSqlInfo *pInfo, int32_t type, SToken *ip);
void setAlterUserSql(SSqlInfo *pInfo, int16_t type, SToken *pName, SToken* pPwd, SToken *pPrivilege);

void setCompactVnodeSql(SSqlInfo *pInfo, int32_t type, SArray *pParam);

void setDefaultCreateDbOption(SCreateDbInfo *pDBInfo);
void setDefaultCreateTopicOption(SCreateDbInfo *pDBInfo);

// prefix show db.tables;
void tSetDbName(SToken *pCpxName, SToken *pDb);

void tSetColumnInfo(struct SField *pField, SToken *pName, struct SField *pType);
void tSetColumnType(struct SField *pField, SToken *type);

/**
 * The main parse function.
 * @param yyp      The parser
 * @param yymajor  The major token code number
 * @param yyminor  The value for the token
 */
void Parse(void *yyp, int yymajor, ParseTOKENTYPE yyminor, SSqlInfo *);

/**
 * Free the allocated resources in case of failure.
 * @param p         The parser to be deleted
 * @param freeProc  Function used to reclaim memory
 */
void ParseFree(void *p, void (*freeProc)(void *));

/**
 * Allocated callback function.
 * @param mallocProc  The parser allocator
 * @return
 */
void *ParseAlloc(void *(*mallocProc)(size_t));

/**
 *
 * @param str sql string
 * @return sql ast
 */
SSqlInfo doGenerateAST(const char *str);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_ASTGENERATOR_H
