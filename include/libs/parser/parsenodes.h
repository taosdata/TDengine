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

#ifndef _TD_PARSENODES_H_
#define _TD_PARSENODES_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "catalog.h"
#include "tcommon.h"
#include "function.h"
#include "tmsgtype.h"
#include "tname.h"
#include "tvariant.h"

/**
 * The first field of a node of any type is guaranteed to be the int16_t.
 * Hence the type of any node can be gotten by casting it to SQueryNode. 
 */
typedef struct SQueryNode {
  int16_t type;
} SQueryNode;

#define queryNodeType(nodeptr) (((const SQueryNode*)(nodeptr))->type)

typedef struct SFieldInfo {
  int16_t     numOfOutput;   // number of column in result
  SField     *final;
  SArray     *internalField; // SArray<SInternalField>
} SFieldInfo;

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
  SVgroupsInfo  *vgroupList;
  SName         name;
  char          aliasName[TSDB_TABLE_NAME_LEN];    // alias name of table specified in query sql
  SArray       *tagColList;                        // SArray<SColumn*>, involved tag columns
} STableMetaInfo;

typedef struct SColumnIndex {
  int16_t tableIndex;
  int16_t columnIndex;
  int16_t type;               // normal column/tag/ user input constant column
} SColumnIndex;

// select statement
typedef struct SQueryStmtInfo {
  int16_t          command;       // the command may be different for each subclause, so keep it seperately.
  uint32_t         type;          // query/insert type
  STimeWindow      window;        // the whole query time window
  SInterval        interval;      // tumble time window
  SSessionWindow   sessionWindow; // session time window
  SStateWindow     stateWindow;   // state window query
  SGroupbyExpr     groupbyExpr;   // groupby tags info
  SArray *         colList;       // SArray<SColumn*>
  SFieldInfo       fieldsInfo;
  SArray**         exprList;      // SArray<SExprInfo*>
  SLimit           limit;
  SLimit           slimit;
  STagCond         tagCond;
  SArray *         colCond;
  SArray *         order;
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
  int32_t          bufLen;
  char*            buf;
  SArray          *pUdfInfo;

  struct SQueryStmtInfo *sibling;     // sibling
  SMultiFunctionsDesc    info;
  SArray            *pDownstream;   // SArray<struct SQueryStmtInfo>
  int32_t            havingFieldNum;
  int32_t            exprListLevelIndex;
} SQueryStmtInfo;

typedef enum {
  PAYLOAD_TYPE_KV = 0,
  PAYLOAD_TYPE_RAW = 1,
} EPayloadType;

typedef struct SVgDataBlocks {
  SVgroupInfo vg;
  int32_t     numOfTables;  // number of tables in current submit block
  uint32_t    size;
  char       *pData;        // SMsgDesc + SSubmitReq + SSubmitBlk + ...
} SVgDataBlocks;

typedef struct SVnodeModifOpStmtInfo {
  int16_t     nodeType;
  SArray*     pDataBlocks;         // data block for each vgroup, SArray<SVgDataBlocks*>.
  int8_t      schemaAttache;       // denote if submit block is built with table schema or not
  uint8_t     payloadType;         // EPayloadType. 0: K-V payload for non-prepare insert, 1: rawPayload for prepare insert
  uint32_t    insertType;          // insert data from [file|sql statement| bound statement]
  const char* sql;                 // current sql statement position
} SVnodeModifOpStmtInfo;

typedef struct SDclStmtInfo {
  int16_t     nodeType;
  int16_t     msgType;
  SEpSet      epSet;
  char*       pMsg;
  int32_t     msgLen;
  void*       pExtension;  // todo remove it soon
} SDclStmtInfo;

#ifdef __cplusplus
}
#endif

#endif /*_TD_PARSENODES_H_*/
