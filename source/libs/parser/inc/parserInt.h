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

#ifndef _TD_PARSER_INT_H_
#define _TD_PARSER_INT_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "catalog.h"
#include "parser.h"
#include "tname.h"
#include "astGen.h"

typedef struct SField {
  char     name[TSDB_COL_NAME_LEN];
  uint8_t  type;
  int16_t  bytes;
} SField;

typedef struct SInterval {
  int32_t  tz;            // query client timezone
  char     intervalUnit;
  char     slidingUnit;
  char     offsetUnit;
  int64_t  interval;
  int64_t  sliding;
  int64_t  offset;
} SInterval;

typedef struct SSessionWindow {
  int64_t  gap;             // gap between two session window(in microseconds)
  int32_t  primaryColId;    // primary timestamp column
} SSessionWindow;

typedef struct SGroupbyExpr {
  int16_t  tableIndex;
  SArray*  columnInfo;  // SArray<SColIndex>, group by columns information
  int16_t  numOfGroupCols;  // todo remove it
  int16_t  orderIndex;  // order by column index
  int16_t  orderType;   // order by type: asc/desc
} SGroupbyExpr;

typedef struct SFieldInfo {
  int16_t     numOfOutput;   // number of column in result
  SField     *final;
  SArray     *internalField; // SArray<SInternalField>
} SFieldInfo;

typedef struct SLimitVal {
  int64_t   limit;
  int64_t   offset;
} SLimitVal;

typedef struct SOrderVal {
  uint32_t  order;
  int32_t   orderColId;
} SOrderVal;

typedef struct SCond {
  uint64_t uid;
  int32_t  len;  // length of tag query condition data
  char *   cond;
} SCond;

typedef struct SJoinNode {
  uint64_t uid;
  int16_t  tagColId;
  SArray*  tsJoin;
  SArray*  tagJoin;
} SJoinNode;

typedef struct SJoinInfo {
  bool       hasJoin;
  SJoinNode *joinTables[TSDB_MAX_JOIN_TABLE_NUM];
} SJoinInfo;

typedef struct STagCond {
  int16_t   relType;     // relation between tbname list and query condition, including : TK_AND or TK_OR
  SCond     tbnameCond;  // tbname query condition, only support tbname query condition on one table
  SJoinInfo joinInfo;    // join condition, only support two tables join currently
  SArray   *pCond;       // for different table, the query condition must be seperated
} STagCond;

typedef struct STableMetaInfo {
  STableMeta    *pTableMeta;      // table meta, cached in client side and acquired by name
  uint32_t       tableMetaSize;
  size_t         tableMetaCapacity;
  SVgroupsInfo  *vgroupList;
  SArray        *pVgroupTables;   // SArray<SVgroupTableInfo>

  /*
   * 1. keep the vgroup index during the multi-vnode super table projection query
   * 2. keep the vgroup index for multi-vnode insertion
   */
  int32_t       vgroupIndex;
  SName         name;
  char          aliasName[TSDB_TABLE_NAME_LEN];    // alias name of table specified in query sql
  SArray       *tagColList;                        // SArray<SColumn*>, involved tag columns
} STableMetaInfo;

typedef struct SInsertStmtInfo {
  SHashObj *pTableBlockHashList;     // data block for each table
  SArray   *pDataBlocks;             // SArray<STableDataBlocks*>. Merged submit block for each vgroup
  int8_t    schemaAttached;          // denote if submit block is built with table schema or not
  uint8_t   payloadType;             // EPayloadType. 0: K-V payload for non-prepare insert, 1: rawPayload for prepare insert
  uint32_t  insertType;              // insert data from [file|sql statement| bound statement]
  char     *sql;                     // current sql statement position
} SInsertStmtInfo;

typedef struct SQueryStmtInfo {
  int16_t          command;       // the command may be different for each subclause, so keep it seperately.
  uint32_t         type;          // query/insert type
  STimeWindow      window;        // the whole query time window

  SInterval        interval;      // tumble time window
  SSessionWindow   sessionWindow; // session time window

  SGroupbyExpr     groupbyExpr;   // groupby tags info
  SArray *         colList;       // SArray<SColumn*>
  SFieldInfo       fieldsInfo;
  SArray *         exprList;      // SArray<SExprInfo*>
  SArray *         exprList1;     // final exprlist in case of arithmetic expression exists
  SLimitVal        limit;
  SLimitVal        slimit;
  STagCond         tagCond;

  SArray *         colCond;

  SOrderVal        order;
  int16_t          numOfTables;
  int16_t          curTableIdx;
  STableMetaInfo **pTableMetaInfo;
  struct STSBuf   *tsBuf;

  int16_t          fillType;      // final result fill type
  int64_t *        fillVal;       // default value for fill
  int32_t          numOfFillVal;  // fill value size

  char *           msg;           // pointer to the pCmd->payload to keep error message temporarily
  int64_t          clauseLimit;   // limit for current sub clause

  int64_t          prjOffset;     // offset value in the original sql expression, only applied at client side
  int64_t          vgroupLimit;    // table limit in case of super table projection query + global order + limit

  int32_t          udColumnId;    // current user-defined constant output field column id, monotonically decreases from TSDB_UD_COLUMN_INDEX
  bool             distinct;   // distinct tag or not
  bool             onlyHasTagCond;
  int32_t          bufLen;
  char*            buf;
  SArray          *pUdfInfo;

  struct SQueryStmtInfo *sibling;     // sibling
  SArray            *pUpstream;   // SArray<struct SQueryStmtInfo>
  struct SQueryStmtInfo *pDownstream;
  int32_t            havingFieldNum;
  bool               stableQuery;
  bool               groupbyColumn;
  bool               simpleAgg;
  bool               arithmeticOnAgg;
  bool               projectionQuery;
  bool               hasFilter;
  bool               onlyTagQuery;
  bool               orderProjectQuery;
  bool               stateWindow;
  bool               globalMerge;
  bool               multigroupResult;
} SQueryStmtInfo;

/**
 * validate the AST by pNode
 * @param pNode
 * @return  SQueryNode a bounded AST with essential meta data from local buffer or mgmt node
 */
int32_t qParserValidateSqlNode(struct SCatalog* pCatalog, const SSqlNode* pNode, SQueryStmtInfo* pQueryInfo);

/**
 *
 * @param pSqlNode
 * @param pMetaInfo
 * @return
 */
int32_t qParserExtractRequestedMetaInfo(const SSqlNode* pSqlNode, SMetaReq* pMetaInfo);

#ifdef __cplusplus
}
#endif

#endif /*_TD_PARSER_INT_H_*/