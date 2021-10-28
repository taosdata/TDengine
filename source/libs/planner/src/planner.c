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

#include "os.h"
#include "plannerInt.h"
#include "parser.h"
#include "function.h"

#define QNODE_TAGSCAN       1
#define QNODE_TABLESCAN     2
#define QNODE_PROJECT       3
#define QNODE_AGGREGATE     4
#define QNODE_GROUPBY       5
#define QNODE_LIMIT         6
#define QNODE_JOIN          7
#define QNODE_DISTINCT      8
#define QNODE_SORT          9
#define QNODE_UNIONALL      10
#define QNODE_TIMEWINDOW    11
#define QNODE_SESSIONWINDOW 12
#define QNODE_FILL          13

typedef struct SFillEssInfo {
  int32_t  fillType;  // fill type
  int64_t *val;       // fill value
} SFillEssInfo;

typedef struct SJoinCond {
  bool     tagExists; // denote if tag condition exists or not
  SColumn *tagCond[2];
  SColumn *colCond[2];
} SJoinCond;

static SArray* createQueryPlanImpl(SQueryStmtInfo* pQueryInfo);
static void doDestroyQueryNode(SQueryPlanNode* pQueryNode);

  int32_t qOptimizeQueryPlan(struct SQueryPlanNode* pQueryNode) {
  return 0;
}

int32_t qCreateQueryPlan(const struct SQueryStmtInfo* pQueryInfo, struct SQueryPlanNode* pQueryNode) {
  SArray* upstream = createQueryPlanImpl((struct SQueryStmtInfo*) pQueryInfo);
  assert(taosArrayGetSize(upstream) == 1);

  /*SQueryPlanNode* p = */taosArrayGetP(upstream, 0);
  taosArrayDestroy(upstream);
  return TSDB_CODE_SUCCESS;
}

int32_t qQueryPlanToString(struct SQueryPlanNode* pQueryNode, char** str) {
  return 0;
}

int32_t qQueryPlanToSql(struct SQueryPlanNode* pQueryNode, char** sql) {
  return 0;
}

int32_t qCreatePhysicalPlan(struct SQueryPlanNode* pQueryNode, struct SEpSet* pQnode, struct SQueryDistPlanNode *pPhyNode) {
  return 0;
}

int32_t qPhyPlanToString(struct SQueryDistPlanNode *pPhyNode, char** str) {
  return 0;
}

void* qDestroyQueryPlan(SQueryPlanNode* pQueryNode) {
  if (pQueryNode == NULL) {
    return NULL;
  }

  doDestroyQueryNode(pQueryNode);
  return NULL;
}

void* qDestroyQueryPhyPlan(struct SQueryDistPlanNode* pQueryPhyNode) {
  return NULL;
}

int32_t qCreateQueryJob(const struct SQueryDistPlanNode* pPhyNode, struct SQueryJob** pJob) {
  return 0;
}

//======================================================================================================================

static SQueryPlanNode* createQueryNode(int32_t type, const char* name, SQueryPlanNode** prev, int32_t numOfPrev,
                                   SExprInfo** pExpr, int32_t numOfOutput, SQueryTableInfo* pTableInfo,
                                   void* pExtInfo) {
  SQueryPlanNode* pNode = calloc(1, sizeof(SQueryPlanNode));

  pNode->info.type = type;
  pNode->info.name = strdup(name);

  if (pTableInfo->uid != 0 && pTableInfo->tableName) { // it is a true table
    pNode->tableInfo.uid = pTableInfo->uid;
    pNode->tableInfo.tableName = strdup(pTableInfo->tableName);
  }

  pNode->numOfOutput = numOfOutput;
  pNode->pExpr = calloc(numOfOutput, sizeof(SExprInfo));
  for(int32_t i = 0; i < numOfOutput; ++i) {
    SExprInfo* pExprInfo = taosArrayGet(pNode->pExpr, i);
    assignExprInfo(pExprInfo, pExpr[i]);
  }

  pNode->pPrevNodes = taosArrayInit(4, POINTER_BYTES);
  for(int32_t i = 0; i < numOfPrev; ++i) {
    taosArrayPush(pNode->pPrevNodes, &prev[i]);
  }

  switch(type) {
    case QNODE_TABLESCAN: {
      STimeWindow* window = calloc(1, sizeof(STimeWindow));
      memcpy(window, pExtInfo, sizeof(STimeWindow));
      pNode->pExtInfo = window;
      break;
    }

    case QNODE_TIMEWINDOW: {
      SInterval* pInterval = calloc(1, sizeof(SInterval));
      pNode->pExtInfo = pInterval;
      memcpy(pInterval, pExtInfo, sizeof(SInterval));
      break;
    }

    case QNODE_GROUPBY: {
      SGroupbyExpr* p = (SGroupbyExpr*) pExtInfo;
      SGroupbyExpr* pGroupbyExpr = calloc(1, sizeof(SGroupbyExpr));

      pGroupbyExpr->tableIndex = p->tableIndex;
      pGroupbyExpr->orderType  = p->orderType;
      pGroupbyExpr->orderIndex = p->orderIndex;
      pGroupbyExpr->columnInfo = taosArrayDup(p->columnInfo);
      pNode->pExtInfo = pGroupbyExpr;
      break;
    }

    case QNODE_FILL: { // todo !!
      pNode->pExtInfo = pExtInfo;
      break;
    }

    case QNODE_LIMIT: {
      pNode->pExtInfo = calloc(1, sizeof(SLimit));
      memcpy(pNode->pExtInfo, pExtInfo, sizeof(SLimit));
      break;
    }
  }
  
  return pNode;
}

static SQueryPlanNode* doAddTableColumnNode(SQueryStmtInfo* pQueryInfo, STableMetaInfo* pTableMetaInfo, SQueryTableInfo* info,
                                        SArray* pExprs, SArray* tableCols) {
  if (pQueryInfo->info.onlyTagQuery) {
    int32_t     num = (int32_t) taosArrayGetSize(pExprs);
    SQueryPlanNode* pNode = createQueryNode(QNODE_TAGSCAN, "TableTagScan", NULL, 0, pExprs->pData, num, info, NULL);

    if (pQueryInfo->distinct) {
      pNode = createQueryNode(QNODE_DISTINCT, "Distinct", &pNode, 1, pExprs->pData, num, info, NULL);
    }

    return pNode;
  }

  STimeWindow* window = &pQueryInfo->window;
  SQueryPlanNode*  pNode = createQueryNode(QNODE_TABLESCAN, "TableScan", NULL, 0, NULL, 0, info, window);

  if (pQueryInfo->info.projectionQuery) {
    int32_t numOfOutput = (int32_t) taosArrayGetSize(pExprs);
    pNode = createQueryNode(QNODE_PROJECT, "Projection", &pNode, 1, pExprs->pData, numOfOutput, info, NULL);
  } else {
    // table source column projection, generate the projection expr
    int32_t     numOfCols = (int32_t) taosArrayGetSize(tableCols);
    SExprInfo** pExpr = calloc(numOfCols, POINTER_BYTES);
    SSchema*    pSchema = pTableMetaInfo->pTableMeta->schema;

    STableMetaInfo* pTableMetaInfo1 = getMetaInfo(pQueryInfo, 0);
    SSchema resultSchema = *pSchema;
    resultSchema.colId = getNewResColId();

    for (int32_t i = 0; i < numOfCols; ++i) {
      SColumn* pCol = taosArrayGetP(tableCols, i);
      SColumnIndex index = {.tableIndex = 0, .columnIndex = pCol->columnIndex};

      SExprInfo* p = createExprInfo(pTableMetaInfo1, FUNCTION_PRJ, &index, NULL, &resultSchema, 0);
      pExpr[i] = p;
    }

    pNode = createQueryNode(QNODE_PROJECT, "Projection", &pNode, 1, pExpr, numOfCols, info, NULL);
//    dropAllExprInfo(pExpr);
    tfree(pExpr);
  }

  return pNode;
}

static SQueryPlanNode* doCreateQueryPlanForOneTableImpl(SQueryStmtInfo* pQueryInfo, SQueryPlanNode* pNode, SQueryTableInfo* info,
                                                    SArray* pExprs) {
  // check for aggregation
  size_t numOfGroupCols = taosArrayGetSize(pQueryInfo->groupbyExpr.columnInfo);

  if (pQueryInfo->interval.interval > 0) {
    int32_t numOfOutput = (int32_t)taosArrayGetSize(pExprs);

    pNode = createQueryNode(QNODE_TIMEWINDOW, "TimeWindowAgg", &pNode, 1, pExprs->pData, numOfOutput, info, &pQueryInfo->interval);
    if (numOfGroupCols != 0) {
      pNode = createQueryNode(QNODE_GROUPBY, "Groupby", &pNode, 1, pExprs->pData, numOfOutput, info, &pQueryInfo->groupbyExpr);
    }
  } else if (numOfGroupCols > 0) {
    int32_t numOfOutput = (int32_t)taosArrayGetSize(pExprs);
    pNode = createQueryNode(QNODE_GROUPBY, "Groupby", &pNode, 1, pExprs->pData, numOfOutput, info,
                            &pQueryInfo->groupbyExpr);
  } else if (pQueryInfo->sessionWindow.gap > 0) {
    pNode = createQueryNode(QNODE_SESSIONWINDOW, "SessionWindowAgg", &pNode, 1, NULL, 0, info, NULL);
  } else if (pQueryInfo->info.simpleAgg) {
    int32_t numOfOutput = (int32_t)taosArrayGetSize(pExprs);
    pNode = createQueryNode(QNODE_AGGREGATE, "Aggregate", &pNode, 1, pExprs->pData, numOfOutput, info, NULL);
  }

  if (pQueryInfo->havingFieldNum > 0 || pQueryInfo->info.arithmeticOnAgg) {
    int32_t numOfExpr = (int32_t)taosArrayGetSize(pQueryInfo->exprList1);
    pNode =
        createQueryNode(QNODE_PROJECT, "Projection", &pNode, 1, pQueryInfo->exprList1->pData, numOfExpr, info, NULL);
  }

  if (pQueryInfo->fillType != TSDB_FILL_NONE) {
    SFillEssInfo* pInfo = calloc(1, sizeof(SFillEssInfo));
    pInfo->fillType = pQueryInfo->fillType;
    pInfo->val = calloc(pNode->numOfOutput, sizeof(int64_t));
    memcpy(pInfo->val, pQueryInfo->fillVal, pNode->numOfOutput);

    pNode = createQueryNode(QNODE_FILL, "Fill", &pNode, 1, NULL, 0, info, pInfo);
  }

  if (pQueryInfo->limit.limit != -1 || pQueryInfo->limit.offset != 0) {
    pNode = createQueryNode(QNODE_LIMIT, "Limit", &pNode, 1, NULL, 0, info, &pQueryInfo->limit);
  }

  return pNode;
}

static SQueryPlanNode* doCreateQueryPlanForOneTable(SQueryStmtInfo* pQueryInfo, STableMetaInfo* pTableMetaInfo, SArray* pExprs,
                                                SArray* tableCols) {
  char name[TSDB_TABLE_FNAME_LEN] = {0};
  tNameExtractFullName(&pTableMetaInfo->name, name);

  SQueryTableInfo info = {.tableName = strdup(name), .uid = pTableMetaInfo->pTableMeta->uid,};

  // handle the only tag query
  SQueryPlanNode* pNode = doAddTableColumnNode(pQueryInfo, pTableMetaInfo, &info, pExprs, tableCols);
  if (pQueryInfo->info.onlyTagQuery) {
    tfree(info.tableName);
    return pNode;
  }

  SQueryPlanNode* pNode1 = doCreateQueryPlanForOneTableImpl(pQueryInfo, pNode, &info, pExprs);
  tfree(info.tableName);
  return pNode1;
}

SArray* createQueryPlanImpl(SQueryStmtInfo* pQueryInfo) {
  SArray* upstream = NULL;

  if (pQueryInfo->pUpstream != NULL && taosArrayGetSize(pQueryInfo->pUpstream) > 0) {  // subquery in the from clause
    upstream = taosArrayInit(4, POINTER_BYTES);

    size_t size = taosArrayGetSize(pQueryInfo->pUpstream);
    for(int32_t i = 0; i < size; ++i) {
      SQueryStmtInfo* pq = taosArrayGet(pQueryInfo->pUpstream, i);
      SArray* p = createQueryPlanImpl(pq);
      taosArrayAddBatch(upstream, p->pData, (int32_t) taosArrayGetSize(p));
    }
  }

  if (pQueryInfo->numOfTables > 1) {  // it is a join query
    // 1. separate the select clause according to table
    taosArrayDestroy(upstream);
    upstream = taosArrayInit(5, POINTER_BYTES);

    for(int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
      STableMetaInfo* pTableMetaInfo = pQueryInfo->pTableMetaInfo[i];
      uint64_t        uid = pTableMetaInfo->pTableMeta->uid;

      SArray* exprList = taosArrayInit(4, POINTER_BYTES);
      if (copyExprInfoList(exprList, pQueryInfo->exprList, uid, true) != 0) {
        terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
        dropAllExprInfo(exprList);
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
      SQueryPlanNode* pNode = doAddTableColumnNode(pQueryInfo, pTableMetaInfo, &info, exprList, tableColumnList);
      columnListDestroy(tableColumnList);
      dropAllExprInfo(exprList);
      taosArrayPush(upstream, &pNode);
    }

    // 3. add the join node here
    SQueryTableInfo info = {0};
    int32_t num = (int32_t) taosArrayGetSize(pQueryInfo->exprList);
    SQueryPlanNode* pNode = createQueryNode(QNODE_JOIN, "Join", upstream->pData, pQueryInfo->numOfTables,
                                        pQueryInfo->exprList->pData, num, &info, NULL);

    // 4. add the aggregation or projection execution node
    pNode = doCreateQueryPlanForOneTableImpl(pQueryInfo, pNode, &info, pQueryInfo->exprList);
    upstream = taosArrayInit(5, POINTER_BYTES);
    taosArrayPush(upstream, &pNode);
  } else { // only one table, normal query process
    STableMetaInfo* pTableMetaInfo = pQueryInfo->pTableMetaInfo[0];
    SQueryPlanNode* pNode = doCreateQueryPlanForOneTable(pQueryInfo, pTableMetaInfo, pQueryInfo->exprList, pQueryInfo->colList);
    upstream = taosArrayInit(5, POINTER_BYTES);
    taosArrayPush(upstream, &pNode);
  }

  return upstream;
}

static void doDestroyQueryNode(SQueryPlanNode* pQueryNode) {
  tfree(pQueryNode->pExtInfo);
  tfree(pQueryNode->pSchema);
  tfree(pQueryNode->info.name);

  tfree(pQueryNode->tableInfo.tableName);
  dropAllExprInfo(pQueryNode->pExpr);

  if (pQueryNode->pPrevNodes != NULL) {
    int32_t size = (int32_t) taosArrayGetSize(pQueryNode->pPrevNodes);
    for(int32_t i = 0; i < size; ++i) {
      SQueryPlanNode* p = taosArrayGetP(pQueryNode->pPrevNodes, i);
      doDestroyQueryNode(p);
    }

    taosArrayDestroy(pQueryNode->pPrevNodes);
  }

  tfree(pQueryNode);
}

bool hasAliasName(SExprInfo* pExpr) {
  assert(pExpr != NULL);
  return true;
//  return strncmp(pExpr->base.token, pExpr->base., tListLen(pExpr->base.aliasName)) != 0;
}

static int32_t doPrintPlan(char* buf, SQueryPlanNode* pQueryNode, int32_t level, int32_t totalLen) {
  if (level > 0) {
    sprintf(buf + totalLen, "%*c", level, ' ');
    totalLen += level;
  }

  int32_t len1 = sprintf(buf + totalLen, "%s(", pQueryNode->info.name);
  int32_t len = len1 + totalLen;

  switch(pQueryNode->info.type) {
    case QNODE_TABLESCAN: {
      STimeWindow* win = (STimeWindow*)pQueryNode->pExtInfo;
      len1 = sprintf(buf + len, "%s #%" PRIu64 ") time_range: %" PRId64 " - %" PRId64 "\n",
                     pQueryNode->tableInfo.tableName, pQueryNode->tableInfo.uid, win->skey, win->ekey);
      len += len1;
      break;
    }

    case QNODE_PROJECT: {
      len1 = sprintf(buf + len, "cols: ");
      len += len1;

      for(int32_t i = 0; i < pQueryNode->numOfOutput; ++i) {
        SExprInfo* pExprInfo = taosArrayGetP(pQueryNode->pExpr, i);

        SSqlExpr* p = &pExprInfo->base;
        len1 = sprintf(buf + len, "[%s #%d]", p->resSchema.name, p->resSchema.colId);
        len += len1;

        if (i < pQueryNode->numOfOutput - 1) {
          len1 = sprintf(buf + len, ", ");
          len += len1;
        }
      }

      len1 = sprintf(buf + len, ")");
      len += len1;

      //todo print filter info
      len1 = sprintf(buf + len, " filters:(nil)\n");
      len += len1;
      break;
    }

    case QNODE_AGGREGATE: {
      for(int32_t i = 0; i < pQueryNode->numOfOutput; ++i) {
        SExprInfo* pExprInfo = taosArrayGetP(pQueryNode->pExpr, i);

        SSqlExpr* pExpr = &pExprInfo->base;
//        if (hasAliasName(&pQueryNode->pExpr[i])) {
          len1 = sprintf(buf + len,"[%s #%s]", pExpr->token, pExpr->resSchema.name);
//        } else {
//          len1 = sprintf(buf + len,"[%s]", pExpr->token);
//        }

        len += len1;
        if (i < pQueryNode->numOfOutput - 1) {
          len1 = sprintf(buf + len, ", ");
          len += len1;
        }
      }

      len1 = sprintf(buf + len, ")\n");
      len += len1;
      break;
    }

    case QNODE_TIMEWINDOW: {
      for(int32_t i = 0; i < pQueryNode->numOfOutput; ++i) {
        SExprInfo* pExprInfo = taosArrayGetP(pQueryNode->pExpr, i);

        SSqlExpr* pExpr = &pExprInfo->base;
        if (hasAliasName(pExprInfo)) {
          len1 = sprintf(buf + len,"[%s #%s]", pExpr->token, pExpr->resSchema.name);
        } else {
          len1 = sprintf(buf + len,"[%s]", pExpr->token);
        }

        len += len1;
        if (i < pQueryNode->numOfOutput - 1) {
          len1 = sprintf(buf + len,", ");
          len += len1;
        }
      }

      len1 = sprintf(buf + len,") ");
      len += len1;

      SInterval* pInterval = pQueryNode->pExtInfo;
      len1 = sprintf(buf + len, "interval:%" PRId64 "(%s), sliding:%" PRId64 "(%s), offset:%" PRId64 "\n",
                     pInterval->interval, TSDB_TIME_PRECISION_MILLI_STR, pInterval->sliding, TSDB_TIME_PRECISION_MILLI_STR,
                     pInterval->offset);
      len += len1;

      break;
    }

    case QNODE_GROUPBY: {  // todo hide the invisible column
      for(int32_t i = 0; i < pQueryNode->numOfOutput; ++i) {
        SExprInfo* pExprInfo = taosArrayGetP(pQueryNode->pExpr, i);

        SSqlExpr* pExpr = &pExprInfo->base;

        if (hasAliasName(pExprInfo)) {
          len1 = sprintf(buf + len,"[%s #%s]", pExpr->token, pExpr->resSchema.name);
        } else {
          len1 = sprintf(buf + len,"[%s]", pExpr->token);
        }

        len += len1;
        if (i < pQueryNode->numOfOutput - 1) {
          len1 = sprintf(buf + len,", ");
          len += len1;
        }
      }

      SGroupbyExpr* pGroupbyExpr = pQueryNode->pExtInfo;
      SColIndex* pIndex = taosArrayGet(pGroupbyExpr->columnInfo, 0);

      len1 = sprintf(buf + len,") groupby_col: [%s #%d]\n", pIndex->name, pIndex->colId);
      len += len1;

      break;
    }

    case QNODE_FILL: {
      SFillEssInfo* pEssInfo = pQueryNode->pExtInfo;
      len1 = sprintf(buf + len,"%d", pEssInfo->fillType);
      len += len1;

      if (pEssInfo->fillType == TSDB_FILL_SET_VALUE) {
        len1 = sprintf(buf + len,", val:");
        len += len1;

        // todo get the correct fill data type
        for(int32_t i = 0; i < pQueryNode->numOfOutput; ++i) {
          len1 = sprintf(buf + len,"%"PRId64, pEssInfo->val[i]);
          len += len1;

          if (i < pQueryNode->numOfOutput - 1) {
            len1 = sprintf(buf + len,", ");
            len += len1;
          }
        }
      }

      len1 = sprintf(buf + len,")\n");
      len += len1;
      break;
    }

    case QNODE_LIMIT: {
      SLimit* pVal = pQueryNode->pExtInfo;
      len1 = sprintf(buf + len,"limit: %"PRId64", offset: %"PRId64")\n", pVal->limit, pVal->offset);
      len += len1;
      break;
    }

    case QNODE_DISTINCT:
    case QNODE_TAGSCAN: {
      len1 = sprintf(buf + len,"cols: ");
      len += len1;

      for(int32_t i = 0; i < pQueryNode->numOfOutput; ++i) {
        SExprInfo* pExprInfo = taosArrayGetP(pQueryNode->pExpr, i);
        SSchema* resSchema = &pExprInfo->base.resSchema;

        len1 = sprintf(buf + len,"[%s #%d]", resSchema->name, resSchema->colId);
        len += len1;

        if (i < pQueryNode->numOfOutput - 1) {
          len1 = sprintf(buf + len,", ");
          len += len1;
        }
      }

      len1 = sprintf(buf + len,")\n");
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

int32_t queryPlanToStringImpl(char* buf, SQueryPlanNode* pQueryNode, int32_t level, int32_t totalLen) {
  int32_t len = doPrintPlan(buf, pQueryNode, level, totalLen);

  for(int32_t i = 0; i < taosArrayGetSize(pQueryNode->pPrevNodes); ++i) {
    SQueryPlanNode* p1 = taosArrayGetP(pQueryNode->pPrevNodes, i);
    int32_t len1 = queryPlanToStringImpl(buf, p1, level + 1, len);
    len = len1;
  }

  return len;
}

char* queryPlanToString(SQueryPlanNode* pQueryNode) {
  assert(pQueryNode);

  char* buf = calloc(1, 4096);

  int32_t len = sprintf(buf, "===== logic plan =====\n");
  queryPlanToStringImpl(buf, pQueryNode, 0, len);
  return buf;
}

SQueryPlanNode* queryPlanFromString() {
  return NULL;
}
