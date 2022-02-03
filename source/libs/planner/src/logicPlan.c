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

#include <function.h>
#include "function.h"
#include "os.h"
#include "parser.h"
#include "plannerInt.h"

typedef struct SFillEssInfo {
  int32_t  fillType;  // fill type
  int64_t *val;       // fill value
} SFillEssInfo;

typedef struct SJoinCond {
  bool     tagExists; // denote if tag condition exists or not
  SColumn *tagCond[2];
  SColumn *colCond[2];
} SJoinCond;

static SArray* createQueryPlanImpl(const SQueryStmtInfo* pQueryInfo);
static void doDestroyQueryNode(SQueryPlanNode* pQueryNode);

int32_t printExprInfo(char* buf, const SQueryPlanNode* pQueryNode, int32_t len);
int32_t optimizeQueryPlan(struct SQueryPlanNode* pQueryNode) {
  return 0;
}

static int32_t createModificationOpPlan(const SQueryNode* pNode, SQueryPlanNode** pQueryPlan) {
  SVnodeModifOpStmtInfo* pModifStmtInfo = (SVnodeModifOpStmtInfo*)pNode;

  *pQueryPlan = calloc(1, sizeof(SQueryPlanNode));
  SArray* blocks = taosArrayInit(taosArrayGetSize(pModifStmtInfo->pDataBlocks), POINTER_BYTES);

  SDataPayloadInfo* pPayload = calloc(1, sizeof(SDataPayloadInfo));
  if (NULL == *pQueryPlan || NULL == blocks || NULL == pPayload) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  (*pQueryPlan)->info.type = QNODE_MODIFY;
  taosArrayAddAll(blocks, pModifStmtInfo->pDataBlocks);

  if (pNode->type == TSDB_SQL_INSERT) {
    pPayload->msgType = TDMT_VND_SUBMIT;
  } else if (pNode->type == TSDB_SQL_CREATE_TABLE) {
    pPayload->msgType = TDMT_VND_CREATE_TABLE;
  }

  pPayload->payload = blocks;
  (*pQueryPlan)->pExtInfo = pPayload;

  return TSDB_CODE_SUCCESS;
}

int32_t createSelectPlan(const SQueryStmtInfo* pSelect, SQueryPlanNode** pQueryPlan) {
  SArray* pDownstream = createQueryPlanImpl(pSelect);
  assert(taosArrayGetSize(pDownstream) == 1);

  *pQueryPlan = taosArrayGetP(pDownstream, 0);
  taosArrayDestroy(pDownstream);
  return TSDB_CODE_SUCCESS;
}

int32_t createQueryPlan(const SQueryNode* pNode, SQueryPlanNode** pQueryPlan) {
  switch (queryNodeType(pNode)) {
    case TSDB_SQL_SELECT: {
      return createSelectPlan((const SQueryStmtInfo*)pNode, pQueryPlan);
    }

    case TSDB_SQL_INSERT:
    case TSDB_SQL_CREATE_TABLE:
      return createModificationOpPlan(pNode, pQueryPlan);

    default:
      return TSDB_CODE_FAILED;
  }
}

int32_t queryPlanToSql(struct SQueryPlanNode* pQueryNode, char** sql) {
  return 0;
}

void destroyQueryPlan(SQueryPlanNode* pQueryNode) {
  if (pQueryNode == NULL) {
    return;
  }

  doDestroyQueryNode(pQueryNode);
}

//======================================================================================================================

static SQueryPlanNode* createQueryNode(int32_t type, const char* name, SQueryPlanNode** pChildrenNode, int32_t numOfChildren,
                                   SExprInfo** pExpr, int32_t numOfOutput, const void* pExtInfo) {
  SQueryPlanNode* pNode = calloc(1, sizeof(SQueryPlanNode));

  pNode->info.type = type;
  pNode->info.name = strdup(name);
  pNode->numOfExpr = numOfOutput;

  pNode->pExpr = taosArrayInit(numOfOutput, POINTER_BYTES);
  taosArrayAddBatch(pNode->pExpr, pExpr, numOfOutput);
  assert(pNode->numOfExpr == numOfOutput);

  pNode->pChildren = taosArrayInit(4, POINTER_BYTES);
  for(int32_t i = 0; i < numOfChildren; ++i) {
    taosArrayPush(pNode->pChildren, &pChildrenNode[i]);
  }

  switch(type) {
    case QNODE_TAGSCAN:
    case QNODE_STREAMSCAN:
    case QNODE_TABLESCAN: {
      SQueryTableInfo* info = calloc(1, sizeof(SQueryTableInfo));
      memcpy(info, pExtInfo, sizeof(SQueryTableInfo));
      info->tableName = strdup(((SQueryTableInfo*) pExtInfo)->tableName);
      pNode->pExtInfo = info;
      break;
    }

    case QNODE_TIMEWINDOW: {
      SInterval* pInterval = calloc(1, sizeof(SInterval));
      pNode->pExtInfo = pInterval;
      memcpy(pInterval, pExtInfo, sizeof(SInterval));
      break;
    }

    case QNODE_STATEWINDOW: {
      SColumn* psw = calloc(1, sizeof(SColumn));
      pNode->pExtInfo = psw;
      memcpy(psw, pExtInfo, sizeof(SColumn));
      break;
    }

    case QNODE_SESSIONWINDOW: {
      SSessionWindow *pSessionWindow = calloc(1, sizeof(SSessionWindow));
      pNode->pExtInfo = pSessionWindow;
      memcpy(pSessionWindow, pExtInfo, sizeof(struct SSessionWindow));
      break;
    }

    case QNODE_GROUPBY: {
      SGroupbyExpr* p = (SGroupbyExpr*) pExtInfo;

      SGroupbyExpr* pGroupbyExpr = calloc(1, sizeof(SGroupbyExpr));
      pGroupbyExpr->groupbyTag = p->groupbyTag;
      pGroupbyExpr->columnInfo = taosArrayDup(p->columnInfo);

      pNode->pExtInfo = pGroupbyExpr;
      break;
    }

    case QNODE_FILL: { // todo !!
      pNode->pExtInfo = (void*)pExtInfo;
      break;
    }

    case QNODE_LIMIT: {
      pNode->pExtInfo = calloc(1, sizeof(SLimit));
      memcpy(pNode->pExtInfo, pExtInfo, sizeof(SLimit));
      break;
    }

    case QNODE_SORT: {
      pNode->pExtInfo = taosArrayDup(pExtInfo);
      break;
    }

    default:
      break;
  }
  
  return pNode;
}

static SQueryPlanNode* doAddTableColumnNode(const SQueryStmtInfo* pQueryInfo, SQueryTableInfo* info, SArray* pExprs, SArray* tableCols) {
  if (pQueryInfo->info.onlyTagQuery) {
    int32_t num = (int32_t) taosArrayGetSize(pExprs);
    SQueryPlanNode* pNode = createQueryNode(QNODE_TAGSCAN, "TableTagScan", NULL, 0, pExprs->pData, num, info);

    if (pQueryInfo->info.distinct) {
      pNode = createQueryNode(QNODE_DISTINCT, "Distinct", &pNode, 1, pExprs->pData, num, NULL);
    }
    return pNode;
  }

  SQueryPlanNode* pNode = NULL;
  if (pQueryInfo->info.continueQuery) {
    pNode = createQueryNode(QNODE_STREAMSCAN, "StreamScan", NULL, 0, NULL, 0, info);
  } else {
    pNode = createQueryNode(QNODE_TABLESCAN, "TableScan", NULL, 0, NULL, 0, info);
  }

  if (!pQueryInfo->info.projectionQuery) {
    SArray* p = pQueryInfo->exprList[0];

    // table source column projection, generate the projection expr
    int32_t numOfCols = (int32_t) taosArrayGetSize(tableCols);

    pNode->numOfExpr = numOfCols;
    pNode->pExpr = taosArrayInit(numOfCols, POINTER_BYTES);
    for(int32_t i = 0; i < numOfCols; ++i) {
      SExprInfo* pExprInfo = taosArrayGetP(p, i);
      SColumn* pCol = pExprInfo->base.pColumns;

      SSchema schema = createSchema(pCol->info.type, pCol->info.bytes, pCol->info.colId, pCol->name);

      tExprNode* pExprNode = pExprInfo->pExpr->_function.pChild[0];
      SExprInfo* px = createBinaryExprInfo(pExprNode, &schema);
      taosArrayPush(pNode->pExpr, &px);
    }
  }

  return pNode;
}

static SQueryPlanNode* doCreateQueryPlanForSingleTableImpl(const SQueryStmtInfo* pQueryInfo, SQueryPlanNode* pNode, SQueryTableInfo* info) {
  // group by column not by tag
  size_t numOfGroupCols = taosArrayGetSize(pQueryInfo->groupbyExpr.columnInfo);

  // check for aggregation
  int32_t level = getExprFunctionLevel(pQueryInfo);

  for(int32_t i = level - 1; i >= 0; --i) {
    SArray* p = pQueryInfo->exprList[i];
    size_t  num = taosArrayGetSize(p);

    bool aggregateFunc = false;
    for(int32_t j = 0; j < num; ++j) {
      SExprInfo* pExpr = (SExprInfo*)taosArrayGetP(p, 0);
      if (pExpr->pExpr->nodeType != TEXPR_FUNCTION_NODE) {
        continue;
      }

      aggregateFunc = qIsAggregateFunction(pExpr->pExpr->_function.functionName);
      if (aggregateFunc) {
        break;
      }
    }

    if (aggregateFunc) {
      if (pQueryInfo->interval.interval > 0) {
        pNode = createQueryNode(QNODE_TIMEWINDOW, "TimeWindowAgg", &pNode, 1, p->pData, num, &pQueryInfo->interval);
      } else if (pQueryInfo->sessionWindow.gap > 0) {
        pNode = createQueryNode(QNODE_SESSIONWINDOW, "SessionWindowAgg", &pNode, 1, p->pData, num, &pQueryInfo->sessionWindow);
      } else if (pQueryInfo->stateWindow.col.info.colId > 0) {
        pNode = createQueryNode(QNODE_STATEWINDOW, "StateWindowAgg", &pNode, 1, p->pData, num, &pQueryInfo->stateWindow);
      } else if (numOfGroupCols != 0 && !pQueryInfo->groupbyExpr.groupbyTag) {
          pNode = createQueryNode(QNODE_GROUPBY, "Groupby", &pNode, 1, p->pData, num, &pQueryInfo->groupbyExpr);
      } else {
        pNode = createQueryNode(QNODE_AGGREGATE, "Aggregate", &pNode, 1, p->pData, num, NULL);
      }
    } else {
      // here we can push down the projection to tablescan operator.
      pNode->numOfExpr = num;
      taosArrayAddAll(pNode->pExpr, p);
    }
  }

  if (pQueryInfo->havingFieldNum > 0) {
//    int32_t numOfExpr = (int32_t)taosArrayGetSize(pQueryInfo->exprList1);
//    pNode = createQueryNode(QNODE_PROJECT, "Projection", &pNode, 1, pQueryInfo->exprList1->pData, numOfExpr, NULL);
  }

  if (pQueryInfo->fillType != TSDB_FILL_NONE) {
    SFillEssInfo* pInfo = calloc(1, sizeof(SFillEssInfo));
    pInfo->fillType = pQueryInfo->fillType;
    pInfo->val      = calloc(pNode->numOfExpr, sizeof(int64_t));
    memcpy(pInfo->val, pQueryInfo->fillVal, pNode->numOfExpr);

    SArray* p = pQueryInfo->exprList[0];  // top expression in select clause
    pNode = createQueryNode(QNODE_FILL, "Fill", &pNode, 1, p->pData, taosArrayGetSize(p), pInfo);
  }

  if (pQueryInfo->order != NULL) {
    SArray* pList = pQueryInfo->exprList[0];
    pNode = createQueryNode(QNODE_SORT, "Sort", &pNode, 1, pList->pData, taosArrayGetSize(pList), pQueryInfo->order);
  }

  if (pQueryInfo->limit.limit != -1 || pQueryInfo->limit.offset != 0) {
    pNode = createQueryNode(QNODE_LIMIT, "Limit", &pNode, 1, NULL, 0, &pQueryInfo->limit);
  }

  return pNode;
}

static SQueryPlanNode* doCreateQueryPlanForSingleTable(const SQueryStmtInfo* pQueryInfo, STableMetaInfo* pTableMetaInfo, SArray* pExprs,
                                                SArray* tableCols) {
  char name[TSDB_TABLE_FNAME_LEN] = {0};
  tstrncpy(name, pTableMetaInfo->name.tname, TSDB_TABLE_FNAME_LEN);

  SQueryTableInfo info = {.tableName = strdup(name), .uid = pTableMetaInfo->pTableMeta->uid,};
  info.window = pQueryInfo->window;
  info.pMeta  = pTableMetaInfo;

  // handle the only tag query
  SQueryPlanNode* pNode = doAddTableColumnNode(pQueryInfo, &info, pExprs, tableCols);
  if (pQueryInfo->info.onlyTagQuery) {
    tfree(info.tableName);
    return pNode;
  }

  SQueryPlanNode* pNode1 = doCreateQueryPlanForSingleTableImpl(pQueryInfo, pNode, &info);
  tfree(info.tableName);
  return pNode1;
}

static bool isAllAggExpr(SArray* pList) {
  assert(pList != NULL);

  for (int32_t k = 0; k < taosArrayGetSize(pList); ++k) {
    SExprInfo* p = taosArrayGetP(pList, k);
    if (p->pExpr->nodeType != TEXPR_FUNCTION_NODE || !qIsAggregateFunction(p->pExpr->_function.functionName)) {
      return false;
    }
  }

  return true;
}

SArray* createQueryPlanImpl(const SQueryStmtInfo* pQueryInfo) {
  SArray* pDownstream = NULL;

  if (pQueryInfo->pDownstream != NULL && taosArrayGetSize(pQueryInfo->pDownstream) > 0) {  // subquery in the from clause
    pDownstream = taosArrayInit(4, POINTER_BYTES);

    size_t size = taosArrayGetSize(pQueryInfo->pDownstream);
    for(int32_t i = 0; i < size; ++i) {
      SQueryStmtInfo* pq = taosArrayGet(pQueryInfo->pDownstream, i);
      SArray* p = createQueryPlanImpl(pq);
      taosArrayAddBatch(pDownstream, p->pData, (int32_t) taosArrayGetSize(p));
    }
  }

  if (pQueryInfo->numOfTables > 1) {  // it is a join query
    // 1. separate the select clause according to table
    taosArrayDestroy(pDownstream);
    pDownstream = taosArrayInit(5, POINTER_BYTES);

    for(int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
      STableMetaInfo* pTableMetaInfo = pQueryInfo->pTableMetaInfo[i];
      uint64_t        uid = pTableMetaInfo->pTableMeta->uid;

      SArray* exprList = taosArrayInit(4, POINTER_BYTES);
      if (copyExprInfoList(exprList, pQueryInfo->exprList[0], uid, true) != 0) {
        terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
        exit(-1);
      }

      // 2. create the query execution node
      char name[TSDB_TABLE_FNAME_LEN] = {0};
      tNameExtractFullName(&pTableMetaInfo->name, name);
      SQueryTableInfo info = {.tableName = strdup(name), .uid = pTableMetaInfo->pTableMeta->uid,};

      // 3. get the required table column list
      SArray* tableColumnList = taosArrayInit(4, sizeof(SColumn));
      columnListCopy(tableColumnList, pQueryInfo->colList, uid);

      // 4. add the projection query node
      SQueryPlanNode* pNode = doAddTableColumnNode(pQueryInfo, &info, exprList, tableColumnList);
      columnListDestroy(tableColumnList);
      taosArrayPush(pDownstream, &pNode);
    }

    // 3. add the join node here
    SQueryTableInfo info = {0};
    int32_t num = (int32_t) taosArrayGetSize(pQueryInfo->exprList[0]);
    SQueryPlanNode* pNode = createQueryNode(QNODE_JOIN, "Join", pDownstream->pData, pQueryInfo->numOfTables,
                                        pQueryInfo->exprList[0]->pData, num, NULL);

    // 4. add the aggregation or projection execution node
    pNode = doCreateQueryPlanForSingleTableImpl(pQueryInfo, pNode, &info);
    pDownstream = taosArrayInit(5, POINTER_BYTES);
    taosArrayPush(pDownstream, &pNode);
  } else { // only one table, normal query process
    STableMetaInfo* pTableMetaInfo = pQueryInfo->pTableMetaInfo[0];
    SQueryPlanNode* pNode = doCreateQueryPlanForSingleTable(pQueryInfo, pTableMetaInfo, pQueryInfo->exprList[0], pQueryInfo->colList);
    pDownstream = taosArrayInit(5, POINTER_BYTES);
    taosArrayPush(pDownstream, &pNode);
  }

  return pDownstream;
}

static void doDestroyQueryNode(SQueryPlanNode* pQueryNode) {
  int32_t type = queryNodeType(pQueryNode);
  if (type == QNODE_MODIFY) {
    SDataPayloadInfo* pInfo = pQueryNode->pExtInfo;

    size_t size = taosArrayGetSize(pInfo->payload);
    for (int32_t i = 0; i < size; ++i) {
      SVgDataBlocks* pBlock = taosArrayGetP(pInfo->payload, i);
      tfree(pBlock);
    }

    taosArrayDestroy(pInfo->payload);
  }

  if (type == QNODE_STREAMSCAN || type == QNODE_TABLESCAN) {
    SQueryTableInfo* pQueryTableInfo = pQueryNode->pExtInfo;
    tfree(pQueryTableInfo->tableName);
  }

  taosArrayDestroy(pQueryNode->pExpr);

  tfree(pQueryNode->pExtInfo);
  tfree(pQueryNode->pSchema);
  tfree(pQueryNode->info.name);

  if (pQueryNode->pChildren != NULL) {
    int32_t size = (int32_t) taosArrayGetSize(pQueryNode->pChildren);
    for(int32_t i = 0; i < size; ++i) {
      SQueryPlanNode* p = taosArrayGetP(pQueryNode->pChildren, i);
      doDestroyQueryNode(p);
    }

    taosArrayDestroy(pQueryNode->pChildren);
  }

  tfree(pQueryNode);
}

static int32_t doPrintPlan(char* buf, SQueryPlanNode* pQueryNode, int32_t level, int32_t totalLen) {
  if (level > 0) {
    sprintf(buf + totalLen, "%*c", level, ' ');
    totalLen += level;
  }

  int32_t len1 = sprintf(buf + totalLen, "%s(", pQueryNode->info.name);
  int32_t len = len1 + totalLen;

  switch(pQueryNode->info.type) {
    case QNODE_STREAMSCAN:
    case QNODE_TABLESCAN: {
      SQueryTableInfo* pInfo = (SQueryTableInfo*)pQueryNode->pExtInfo;
      len1 = sprintf(buf + len, "%s #%" PRIu64, pInfo->tableName, pInfo->uid);
      assert(len1 > 0);
      len += len1;

      len1 = sprintf(buf + len, " , cols:");
      assert(len1 > 0);
      len += len1;

      len = printExprInfo(buf, pQueryNode, len);
      len1 = sprintf(buf + len, ")");
      assert(len1 > 0);

      // todo print filter info
      len1 = sprintf(buf + len, ") filters:(nil)");
      len += len1;

      len1 = sprintf(buf + len, " time_range: %" PRId64 " - %" PRId64"\n", pInfo->window.skey, pInfo->window.ekey);
      len += len1;
      break;
    }

    case QNODE_PROJECT: {
      len1 = sprintf(buf + len, "cols:");
      assert(len1 > 0);
      len += len1;

      len = printExprInfo(buf, pQueryNode, len);
      len1 = sprintf(buf + len, ")");
      len += len1;

      // todo print filter info
      len1 = sprintf(buf + len, " filters:(nil)\n");
      len += len1;
      break;
    }

    case QNODE_AGGREGATE: {
      len = printExprInfo(buf, pQueryNode, len);
      len1 = sprintf(buf + len, ")\n");
      len += len1;

      break;
    }

    case QNODE_TIMEWINDOW: {
      len = printExprInfo(buf, pQueryNode, len);
      len1 = sprintf(buf + len, ") ");
      len += len1;

      SInterval* pInterval = pQueryNode->pExtInfo;

      // todo dynamic return the time precision
      len1 = sprintf(buf + len, "interval:%" PRId64 "(%s), sliding:%" PRId64 "(%s), offset:%" PRId64 "(%s)\n",
                     pInterval->interval, TSDB_TIME_PRECISION_MILLI_STR, pInterval->sliding,
                     TSDB_TIME_PRECISION_MILLI_STR, pInterval->offset, TSDB_TIME_PRECISION_MILLI_STR);
      len += len1;

      break;
    }

    case QNODE_STATEWINDOW: {
      len = printExprInfo(buf, pQueryNode, len);
      len1 = sprintf(buf + len, ") ");
      len += len1;

      SColumn* pCol = pQueryNode->pExtInfo;
      len1 = sprintf(buf + len, "col:%s #%d\n", pCol->name, pCol->info.colId);
      len += len1;
      break;
    }

    case QNODE_SESSIONWINDOW: {
      len = printExprInfo(buf, pQueryNode, len);

      len1 = sprintf(buf + len, ") ");
      len += len1;

      struct SSessionWindow* ps = pQueryNode->pExtInfo;
      len1 = sprintf(buf + len, "col:[%s #%d], gap:%" PRId64 " (ms) \n", ps->col.name, ps->col.info.colId, ps->gap);
      len += len1;
      break;
    }

    case QNODE_GROUPBY: {
      len = printExprInfo(buf, pQueryNode, len);

      SGroupbyExpr* pGroupbyExpr = pQueryNode->pExtInfo;
      len1 = sprintf(buf + len, ") groupby_col: ");
      len += len1;

      for (int32_t i = 0; i < taosArrayGetSize(pGroupbyExpr->columnInfo); ++i) {
        SColumn* pCol = taosArrayGet(pGroupbyExpr->columnInfo, i);
        len1 = sprintf(buf + len, "[%s #%d] ", pCol->name, pCol->info.colId);
        len += len1;
      }

      len += sprintf(buf + len, "\n");
      break;
    }

    case QNODE_FILL: {
      SFillEssInfo* pEssInfo = pQueryNode->pExtInfo;
      len1 = sprintf(buf + len, "%d", pEssInfo->fillType);
      len += len1;

      if (pEssInfo->fillType == TSDB_FILL_SET_VALUE) {
        len1 = sprintf(buf + len, ", val:");
        len += len1;

        // todo get the correct fill data type
        for (int32_t i = 0; i < pQueryNode->numOfExpr; ++i) {
          len1 = sprintf(buf + len, "%" PRId64, pEssInfo->val[i]);
          len += len1;

          if (i < pQueryNode->numOfExpr - 1) {
            len1 = sprintf(buf + len, ", ");
            len += len1;
          }
        }
      }

      len1 = sprintf(buf + len, ")\n");
      len += len1;
      break;
    }

    case QNODE_LIMIT: {
      SLimit* pVal = pQueryNode->pExtInfo;
      len1 = sprintf(buf + len, "limit: %" PRId64 ", offset: %" PRId64 ")\n", pVal->limit, pVal->offset);
      len += len1;
      break;
    }

    case QNODE_DISTINCT:
    case QNODE_TAGSCAN: {
      len1 = sprintf(buf + len, "cols: ");
      len += len1;

      len = printExprInfo(buf, pQueryNode, len);

      len1 = sprintf(buf + len, ")\n");
      len += len1;

      break;
    }

    case QNODE_SORT: {
      len1 = sprintf(buf + len, "cols:");
      len += len1;

      SArray* pSort = pQueryNode->pExtInfo;
      for (int32_t i = 0; i < taosArrayGetSize(pSort); ++i) {
        SOrder* p = taosArrayGet(pSort, i);
        len1 = sprintf(buf + len, " [%s #%d %s]", p->col.name, p->col.info.colId, p->order == TSDB_ORDER_ASC? "ASC":"DESC");

        len += len1;
      }

      len1 = sprintf(buf + len, ")\n");
      len += len1;
      break;
    }

    case QNODE_JOIN: {
      //  print join condition
      len1 = sprintf(buf + len, ")\n");
      len += len1;
      break;
    }
  }

  return len;
}

int32_t printExprInfo(char* buf, const SQueryPlanNode* pQueryNode, int32_t len) {
  int32_t len1 = 0;

  for (int32_t i = 0; i < pQueryNode->numOfExpr; ++i) {
    SExprInfo* pExprInfo = taosArrayGetP(pQueryNode->pExpr, i);

    SSqlExpr* pExpr = &pExprInfo->base;
    len1 = sprintf(buf + len, "%s [%s #%d]", pExpr->token, pExpr->resSchema.name, pExpr->resSchema.colId);
    assert(len1 > 0);

    len += len1;
    if (i < pQueryNode->numOfExpr - 1) {
      len1 = sprintf(buf + len, ", ");
      len += len1;
    }
  }

  return len;
}

int32_t queryPlanToStringImpl(char* buf, SQueryPlanNode* pQueryNode, int32_t level, int32_t totalLen) {
  int32_t len = doPrintPlan(buf, pQueryNode, level, totalLen);

  for(int32_t i = 0; i < taosArrayGetSize(pQueryNode->pChildren); ++i) {
    SQueryPlanNode* p1 = taosArrayGetP(pQueryNode->pChildren, i);
    int32_t len1 = queryPlanToStringImpl(buf, p1, level + 1, len);
    len = len1;
  }

  return len;
}

int32_t queryPlanToString(struct SQueryPlanNode* pQueryNode, char** str) {
  assert(pQueryNode);
  *str = calloc(1, 4096);

  int32_t len = sprintf(*str, "===== logic plan =====\n");
  queryPlanToStringImpl(*str, pQueryNode, 0, len);

  return TSDB_CODE_SUCCESS;
}

SQueryPlanNode* queryPlanFromString() {
  return NULL;
}
