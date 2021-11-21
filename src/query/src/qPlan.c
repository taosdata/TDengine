#include "os.h"
#include "qTableMeta.h"
#include "qPlan.h"
#include "qExecutor.h"
#include "qUtil.h"
#include "texpr.h"
#include "tscUtil.h"
#include "tsclient.h"

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

static SQueryNode* createQueryNode(int32_t type, const char* name, SQueryNode** prev, int32_t numOfPrev,
                                   SExprInfo** pExpr, int32_t numOfOutput, SQueryTableInfo* pTableInfo,
                                   void* pExtInfo) {
  SQueryNode* pNode = calloc(1, sizeof(SQueryNode));

  pNode->info.type = type;
  pNode->info.name = strdup(name);

  if (pTableInfo->id.uid != 0 && pTableInfo->tableName) { // it is a true table
    pNode->tableInfo.id = pTableInfo->id;
    pNode->tableInfo.tableName = strdup(pTableInfo->tableName);
  }

  pNode->numOfOutput = numOfOutput;
  pNode->pExpr = calloc(numOfOutput, sizeof(SExprInfo));
  for(int32_t i = 0; i < numOfOutput; ++i) {
    tscExprAssign(&pNode->pExpr[i], pExpr[i]);
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
      pGroupbyExpr->numOfGroupCols = p->numOfGroupCols;
      pGroupbyExpr->columnInfo = taosArrayDup(p->columnInfo);
      pNode->pExtInfo = pGroupbyExpr;
      break;
    }

    case QNODE_FILL: { // todo !!
      pNode->pExtInfo = pExtInfo;
      break;
    }

    case QNODE_LIMIT: {
      pNode->pExtInfo = calloc(1, sizeof(SLimitVal));
      memcpy(pNode->pExtInfo, pExtInfo, sizeof(SLimitVal));
      break;
    }
  }
  return pNode;
}

static SQueryNode* doAddTableColumnNode(SQueryInfo* pQueryInfo, STableMetaInfo* pTableMetaInfo, SQueryTableInfo* info,
                                        SArray* pExprs, SArray* tableCols) {
  if (pQueryInfo->onlyTagQuery) {
    int32_t     num = (int32_t) taosArrayGetSize(pExprs);
    SQueryNode* pNode = createQueryNode(QNODE_TAGSCAN, "TableTagScan", NULL, 0, pExprs->pData, num, info, NULL);

    if (pQueryInfo->distinct) {
      pNode = createQueryNode(QNODE_DISTINCT, "Distinct", &pNode, 1, pExprs->pData, num, info, NULL);
    }

    return pNode;
  }

  STimeWindow* window = &pQueryInfo->window;
  SQueryNode*  pNode = createQueryNode(QNODE_TABLESCAN, "TableScan", NULL, 0, NULL, 0, info, window);

  if (pQueryInfo->projectionQuery) {
    int32_t numOfOutput = (int32_t) taosArrayGetSize(pExprs);
    pNode = createQueryNode(QNODE_PROJECT, "Projection", &pNode, 1, pExprs->pData, numOfOutput, info, NULL);
  } else {
    // table source column projection, generate the projection expr
    int32_t     numOfCols = (int32_t) taosArrayGetSize(tableCols);
    SExprInfo** pExpr = calloc(numOfCols, POINTER_BYTES);
    SSchema*    pSchema = pTableMetaInfo->pTableMeta->schema;

    for (int32_t i = 0; i < numOfCols; ++i) {
      SColumn* pCol = taosArrayGetP(tableCols, i);

      SColumnIndex qry_index = {.tableIndex = 0, .columnIndex = pCol->columnIndex};
      STableMetaInfo* pTableMetaInfo1 = tscGetMetaInfo(pQueryInfo, qry_index.tableIndex);
      SExprInfo*   p = tscExprCreate(pTableMetaInfo1, TSDB_FUNC_PRJ, &qry_index, pCol->info.type, pCol->info.bytes,
                                     pCol->info.colId, 0, TSDB_COL_NORMAL);
      strncpy(p->base.aliasName, pSchema[pCol->columnIndex].name, tListLen(p->base.aliasName));

      pExpr[i] = p;
    }

    pNode = createQueryNode(QNODE_PROJECT, "Projection", &pNode, 1, pExpr, numOfCols, info, NULL);
    for (int32_t i = 0; i < numOfCols; ++i) {
      destroyQueryFuncExpr(pExpr[i], 1);
    }
    tfree(pExpr);
  }

  return pNode;
}

static SQueryNode* doCreateQueryPlanForOneTableImpl(SQueryInfo* pQueryInfo, SQueryNode* pNode, SQueryTableInfo* info,
                                                    SArray* pExprs) {
  // check for aggregation
  if (pQueryInfo->interval.interval > 0) {
    int32_t numOfOutput = (int32_t)taosArrayGetSize(pExprs);

    pNode = createQueryNode(QNODE_TIMEWINDOW, "TimeWindowAgg", &pNode, 1, pExprs->pData, numOfOutput, info,
                            &pQueryInfo->interval);
    if (pQueryInfo->groupbyExpr.numOfGroupCols != 0) {
      pNode = createQueryNode(QNODE_GROUPBY, "Groupby", &pNode, 1, pExprs->pData, numOfOutput, info, &pQueryInfo->groupbyExpr);
    }
  } else if (pQueryInfo->groupbyColumn) {
    int32_t numOfOutput = (int32_t)taosArrayGetSize(pExprs);
    pNode = createQueryNode(QNODE_GROUPBY, "Groupby", &pNode, 1, pExprs->pData, numOfOutput, info,
                            &pQueryInfo->groupbyExpr);
  } else if (pQueryInfo->sessionWindow.gap > 0) {
    pNode = createQueryNode(QNODE_SESSIONWINDOW, "SessionWindowAgg", &pNode, 1, NULL, 0, info, NULL);
  } else if (pQueryInfo->simpleAgg) {
    int32_t numOfOutput = (int32_t)taosArrayGetSize(pExprs);
    pNode = createQueryNode(QNODE_AGGREGATE, "Aggregate", &pNode, 1, pExprs->pData, numOfOutput, info, NULL);
  }

  if (pQueryInfo->havingFieldNum > 0 || pQueryInfo->arithmeticOnAgg) {
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

static SQueryNode* doCreateQueryPlanForOneTable(SQueryInfo* pQueryInfo, STableMetaInfo* pTableMetaInfo, SArray* pExprs,
                                                SArray* tableCols) {
  char name[TSDB_TABLE_FNAME_LEN] = {0};
  tNameExtractFullName(&pTableMetaInfo->name, name);

  SQueryTableInfo info = {.tableName = strdup(name), .id = pTableMetaInfo->pTableMeta->id,};

  // handle the only tag query
  SQueryNode* pNode = doAddTableColumnNode(pQueryInfo, pTableMetaInfo, &info, pExprs, tableCols);
  if (pQueryInfo->onlyTagQuery) {
    tfree(info.tableName);
    return pNode;
  }

  SQueryNode* pNode1 = doCreateQueryPlanForOneTableImpl(pQueryInfo, pNode, &info, pExprs);
  tfree(info.tableName);
  return pNode1;
}

SArray* createQueryPlanImpl(SQueryInfo* pQueryInfo) {
  SArray* upstream = NULL;

  if (pQueryInfo->pUpstream != NULL && taosArrayGetSize(pQueryInfo->pUpstream) > 0) {  // subquery in the from clause
    upstream = taosArrayInit(4, POINTER_BYTES);

    size_t size = taosArrayGetSize(pQueryInfo->pUpstream);
    for(int32_t i = 0; i < size; ++i) {
      SQueryInfo* pq = taosArrayGet(pQueryInfo->pUpstream, i);
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
      uint64_t        uid = pTableMetaInfo->pTableMeta->id.uid;

      SArray* exprList = taosArrayInit(4, POINTER_BYTES);
      if (tscExprCopy(exprList, pQueryInfo->exprList, uid, true) != 0) {
        terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
        tscExprDestroy(exprList);
        exit(-1);
      }

      // 2. create the query execution node
      char name[TSDB_TABLE_FNAME_LEN] = {0};
      tNameExtractFullName(&pTableMetaInfo->name, name);
      SQueryTableInfo info = {.tableName = strdup(name), .id = pTableMetaInfo->pTableMeta->id,};

      // 3. get the required table column list
      SArray* tableColumnList = taosArrayInit(4, sizeof(SColumn));
      tscColumnListCopy(tableColumnList, pQueryInfo->colList, uid);

      // 4. add the projection query node
      SQueryNode* pNode = doAddTableColumnNode(pQueryInfo, pTableMetaInfo, &info, exprList, tableColumnList);
      tscColumnListDestroy(tableColumnList);
      tscExprDestroy(exprList);
      taosArrayPush(upstream, &pNode);
    }

    // 3. add the join node here
    SQueryTableInfo info = {0};
    int32_t num = (int32_t) taosArrayGetSize(pQueryInfo->exprList);
    SQueryNode* pNode = createQueryNode(QNODE_JOIN, "Join", upstream->pData, pQueryInfo->numOfTables,
        pQueryInfo->exprList->pData, num, &info, NULL);

    // 4. add the aggregation or projection execution node
    pNode = doCreateQueryPlanForOneTableImpl(pQueryInfo, pNode, &info, pQueryInfo->exprList);
    upstream = taosArrayInit(5, POINTER_BYTES);
    taosArrayPush(upstream, &pNode);
  } else { // only one table, normal query process
    STableMetaInfo* pTableMetaInfo = pQueryInfo->pTableMetaInfo[0];
    SQueryNode* pNode = doCreateQueryPlanForOneTable(pQueryInfo, pTableMetaInfo, pQueryInfo->exprList, pQueryInfo->colList);
    upstream = taosArrayInit(5, POINTER_BYTES);
    taosArrayPush(upstream, &pNode);
  }

  return upstream;
}

SQueryNode* qCreateQueryPlan(SQueryInfo* pQueryInfo) {
  SArray* upstream = createQueryPlanImpl(pQueryInfo);
  assert(taosArrayGetSize(upstream) == 1);

  SQueryNode* p = taosArrayGetP(upstream, 0);
  taosArrayDestroy(upstream);

  return p;
}

static void doDestroyQueryNode(SQueryNode* pQueryNode) {
  tfree(pQueryNode->pExtInfo);
  tfree(pQueryNode->pSchema);
  tfree(pQueryNode->info.name);

  tfree(pQueryNode->tableInfo.tableName);

  pQueryNode->pExpr = destroyQueryFuncExpr(pQueryNode->pExpr, pQueryNode->numOfOutput);

  if (pQueryNode->pPrevNodes != NULL) {
    int32_t size = (int32_t) taosArrayGetSize(pQueryNode->pPrevNodes);
    for(int32_t i = 0; i < size; ++i) {
      SQueryNode* p = taosArrayGetP(pQueryNode->pPrevNodes, i);
      doDestroyQueryNode(p);
    }

    taosArrayDestroy(pQueryNode->pPrevNodes);
  }

  tfree(pQueryNode);
}

void* qDestroyQueryPlan(SQueryNode* pQueryNode) {
  if (pQueryNode == NULL) {
    return NULL;
  }

  doDestroyQueryNode(pQueryNode);
  return NULL;
}

bool hasAliasName(SExprInfo* pExpr) {
  assert(pExpr != NULL);
  return strncmp(pExpr->base.token, pExpr->base.aliasName, tListLen(pExpr->base.aliasName)) != 0;
}

static int32_t doPrintPlan(char* buf, SQueryNode* pQueryNode, int32_t level, int32_t totalLen) {
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
                     pQueryNode->tableInfo.tableName, pQueryNode->tableInfo.id.uid, win->skey, win->ekey);
      len += len1;
      break;
    }

    case QNODE_PROJECT: {
      len1 = sprintf(buf + len, "cols: ");
      len += len1;

      for(int32_t i = 0; i < pQueryNode->numOfOutput; ++i) {
        SSqlExpr* p = &pQueryNode->pExpr[i].base;
        len1 = sprintf(buf + len, "[%s #%d]", p->aliasName, p->resColId);
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
        SSqlExpr* pExpr = &pQueryNode->pExpr[i].base;
        if (hasAliasName(&pQueryNode->pExpr[i])) {
          len1 = sprintf(buf + len,"[%s #%s]", pExpr->token, pExpr->aliasName);
        } else {
          len1 = sprintf(buf + len,"[%s]", pExpr->token);
        }

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
        SSqlExpr* pExpr = &pQueryNode->pExpr[i].base;
        if (hasAliasName(&pQueryNode->pExpr[i])) {
          len1 = sprintf(buf + len,"[%s #%s]", pExpr->token, pExpr->aliasName);
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
        SSqlExpr* pExpr = &pQueryNode->pExpr[i].base;

        if (hasAliasName(&pQueryNode->pExpr[i])) {
          len1 = sprintf(buf + len,"[%s #%s]", pExpr->token, pExpr->aliasName);
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
      SLimitVal* pVal = pQueryNode->pExtInfo;
      len1 = sprintf(buf + len,"limit: %"PRId64", offset: %"PRId64")\n", pVal->limit, pVal->offset);
      len += len1;
      break;
    }

    case QNODE_DISTINCT:
    case QNODE_TAGSCAN: {
      len1 = sprintf(buf + len,"cols: ");
      len += len1;

      for(int32_t i = 0; i < pQueryNode->numOfOutput; ++i) {
        SSqlExpr* p = &pQueryNode->pExpr[i].base;
        len1 = sprintf(buf + len,"[%s #%d]", p->aliasName, p->resColId);
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

int32_t queryPlanToStringImpl(char* buf, SQueryNode* pQueryNode, int32_t level, int32_t totalLen) {
  int32_t len = doPrintPlan(buf, pQueryNode, level, totalLen);

  for(int32_t i = 0; i < taosArrayGetSize(pQueryNode->pPrevNodes); ++i) {
    SQueryNode* p1 = taosArrayGetP(pQueryNode->pPrevNodes, i);
    int32_t len1 = queryPlanToStringImpl(buf, p1, level + 1, len);
    len = len1;
  }

  return len;
}

char* queryPlanToString(SQueryNode* pQueryNode) {
  assert(pQueryNode);

  char* buf = calloc(1, 4096);

  int32_t len = sprintf(buf, "===== logic plan =====\n");
  queryPlanToStringImpl(buf, pQueryNode, 0, len);
  return buf;
}

SQueryNode* queryPlanFromString() {
  return NULL;
}

SArray* createTableScanPlan(SQueryAttr* pQueryAttr) {
  SArray* plan = taosArrayInit(4, sizeof(int32_t));

  int32_t op = 0;
  if (onlyQueryTags(pQueryAttr)) {
//    op = OP_TagScan;
  } else {
    if (pQueryAttr->queryBlockDist) {
      op = OP_TableBlockInfoScan;
    } else if (pQueryAttr->tsCompQuery || pQueryAttr->pointInterpQuery || pQueryAttr->diffQuery) {
      op = OP_TableSeqScan;
    } else if (pQueryAttr->needReverseScan) {
      op = OP_DataBlocksOptScan;
    } else {
      op = OP_TableScan;
    }

    taosArrayPush(plan, &op);
  }

  return plan;
}

SArray* createExecOperatorPlan(SQueryAttr* pQueryAttr) {
  SArray* plan = taosArrayInit(4, sizeof(int32_t));
  int32_t op = 0;

  if (onlyQueryTags(pQueryAttr)) {  // do nothing for tags query
    op = OP_TagScan;
    taosArrayPush(plan, &op);

    if (pQueryAttr->distinct) {
      op = OP_Distinct;
      taosArrayPush(plan, &op);
    }
  } else if (pQueryAttr->interval.interval > 0) {
    if (pQueryAttr->stableQuery) {
      if (pQueryAttr->pointInterpQuery) {
        op = OP_AllMultiTableTimeInterval;
      } else {
        op = OP_MultiTableTimeInterval;
      }
      taosArrayPush(plan, &op);
    } else {      
      if (pQueryAttr->pointInterpQuery) {
        op = OP_AllTimeWindow;
      } else {
        op = OP_TimeWindow;
      }
      taosArrayPush(plan, &op);

      if (pQueryAttr->pExpr2 != NULL) {
        op = OP_Project;
        taosArrayPush(plan, &op);
      }

      if (pQueryAttr->fillType != TSDB_FILL_NONE) {
        op = OP_Fill;
        taosArrayPush(plan, &op);
      }
    }

  } else if (pQueryAttr->groupbyColumn) {
    op = OP_Groupby;
    taosArrayPush(plan, &op);

    if (!pQueryAttr->stableQuery && pQueryAttr->havingNum > 0) {
      op = OP_Filter;
      taosArrayPush(plan, &op);
    }

    if (pQueryAttr->pExpr2 != NULL) {
      op = OP_Project;
      taosArrayPush(plan, &op);
    }
  } else if (pQueryAttr->sw.gap > 0) {
    op = OP_SessionWindow;
    taosArrayPush(plan, &op);

    if (pQueryAttr->pExpr2 != NULL) {
      op = OP_Project;
      taosArrayPush(plan, &op);
    }
  } else if (pQueryAttr->stateWindow) {
    op =  OP_StateWindow;
    taosArrayPush(plan, &op);

    if (pQueryAttr->pExpr2 != NULL) {
      op = OP_Project;
      taosArrayPush(plan, &op);
    }
  } else if (pQueryAttr->simpleAgg) {
    if (pQueryAttr->stableQuery && !pQueryAttr->tsCompQuery && !pQueryAttr->diffQuery) {
      op = OP_MultiTableAggregate;
    } else {
      op = OP_Aggregate;
    }

    taosArrayPush(plan, &op);

    if (!pQueryAttr->stableQuery && pQueryAttr->havingNum > 0) {
      op = OP_Filter;
      taosArrayPush(plan, &op);
    }

    if (pQueryAttr->pExpr2 != NULL && !pQueryAttr->stableQuery) {
      op = OP_Project;
      taosArrayPush(plan, &op);
    }
  } else {  // diff/add/multiply/subtract/division
    if (pQueryAttr->numOfFilterCols > 0 && pQueryAttr->createFilterOperator && pQueryAttr->vgId == 0) { // todo refactor
      op = OP_Filter;
      taosArrayPush(plan, &op);
    } else {
      op = OP_Project;
      taosArrayPush(plan, &op);

      if (pQueryAttr->pExpr2 != NULL) {
        op = OP_Project;
        taosArrayPush(plan, &op);
      }

      if (pQueryAttr->distinct) {
        op = OP_Distinct;
        taosArrayPush(plan, &op);
      }
    }

    // outer query order by support
    int32_t orderColId = pQueryAttr->order.orderColId;
    if (pQueryAttr->vgId == 0 && orderColId != PRIMARYKEY_TIMESTAMP_COL_INDEX && orderColId != INT32_MIN) {
      op = OP_Order;
      taosArrayPush(plan, &op);
    }
  }

  if (pQueryAttr->limit.limit > 0 || pQueryAttr->limit.offset > 0) {
    op = OP_Limit;
    taosArrayPush(plan, &op);
  }

  return plan;
}

SArray* createGlobalMergePlan(SQueryAttr* pQueryAttr) {
  SArray* plan = taosArrayInit(4, sizeof(int32_t));

  if (!pQueryAttr->stableQuery) {
    return plan;
  }

  int32_t op = OP_MultiwayMergeSort;
  taosArrayPush(plan, &op);

  if (pQueryAttr->distinct) {
    op = OP_Distinct;
    taosArrayPush(plan, &op);
  }

  if (pQueryAttr->simpleAgg || (pQueryAttr->interval.interval > 0 || pQueryAttr->sw.gap > 0)) {
    op = OP_GlobalAggregate;
    taosArrayPush(plan, &op);

    if (pQueryAttr->havingNum > 0) {
      op = OP_Filter;
      taosArrayPush(plan, &op);
    }

    if (pQueryAttr->pExpr2 != NULL) {
      op = OP_Project;
      taosArrayPush(plan, &op);
    }
  }

  // fill operator
  if (pQueryAttr->fillType != TSDB_FILL_NONE && pQueryAttr->interval.interval > 0) {
    op = OP_Fill;
    taosArrayPush(plan, &op);
  }

  // limit/offset operator
  if (pQueryAttr->limit.limit > 0 || pQueryAttr->limit.offset > 0 ||
      pQueryAttr->slimit.limit > 0 || pQueryAttr->slimit.offset > 0) {
    op = OP_SLimit;
    taosArrayPush(plan, &op);
  }

  return plan;
}
