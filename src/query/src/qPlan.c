#include <tscUtil.h>
#include "os.h"
#include "qUtil.h"
#include "texpr.h"
#include "tsclient.h"

#define QNODE_PROJECT       1
#define QNODE_FILTER        2
#define QNODE_TABLESCAN     3
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

typedef struct SQueryNodeBasicInfo {
  int32_t type;
  char   *name;
} SQueryNodeBasicInfo;

typedef struct SQueryNode {
  SQueryNodeBasicInfo info;
//  char              *name;         // the name of logic node
//  int32_t            type;         // the type of logic node

  SSchema           *pSchema;      // the schema of the input SSDatablock
  int32_t            numOfCols;    // number of input columns
  SExprInfo         *pExpr;        // the query functions or sql aggregations
  int32_t            numOfOutput;  // number of result columns, which is also the number of pExprs

  // previous operator to generated result for current node to process
  // in case of join, multiple prev nodes exist.
  SArray            *pPrevNodes;// upstream nodes
  struct SQueryNode *nextNode;
} SQueryNode;

static SQueryNode* createQueryNode(int32_t type, const char* name, SQueryNode** prev, int32_t numOfPrev) {
  SQueryNode* pNode = calloc(1, sizeof(SQueryNode));
  pNode->info.type = type;
  pNode->info.name = strdup(name);
  pNode->pPrevNodes = taosArrayInit(4, POINTER_BYTES);
  for(int32_t i = 0; i < numOfPrev; ++i) {
    taosArrayPush(pNode->pPrevNodes, &prev[i]);
  }

  return pNode;
}

static SQueryNode* doCreateQueryPlanForOneTable(SQueryInfo* pQueryInfo) {
  SQueryNode* pNode = createQueryNode(QNODE_TABLESCAN, "", NULL, 0);

  // check for filter
  if (pQueryInfo->hasFilter) {
    pNode = createQueryNode(QNODE_FILTER, "", &pNode, 1);
  }

  if (pQueryInfo->distinctTag) {
    pNode = createQueryNode(QNODE_DISTINCT, "", &pNode, 0);

  } else if (pQueryInfo->projectionQuery) {
    pNode = createQueryNode(QNODE_PROJECT, "", &pNode, 1);
  } else {  // check for aggregation
    if (pQueryInfo->interval.interval > 0) {
      pNode = createQueryNode(QNODE_TIMEWINDOW, "", &pNode, 1);
    } else if (pQueryInfo->groupbyColumn) {
      pNode = createQueryNode(QNODE_GROUPBY, "", &pNode, 1);
    } else if (pQueryInfo->sessionWindow.gap > 0) {
      pNode = createQueryNode(QNODE_SESSIONWINDOW, "", &pNode, 1);
    } else if (pQueryInfo->simpleAgg) {
      pNode = createQueryNode(QNODE_AGGREGATE, "", &pNode, 1);
    }

    if (pQueryInfo->havingFieldNum > 0) {
      pNode = createQueryNode(QNODE_FILTER, "", &pNode, 1);
    }

    if (pQueryInfo->arithmeticOnAgg) {
      pNode = createQueryNode(QNODE_PROJECT, "", &pNode, 1);
    }

    if (pQueryInfo->fillType != TSDB_FILL_NONE) {
      pNode = createQueryNode(QNODE_FILL, "", &pNode, 1);
    }
  }

  if (pQueryInfo->limit.limit != -1 || pQueryInfo->limit.offset != 0) {
    pNode = createQueryNode(QNODE_LIMIT, "", &pNode, 1);
  }

  return pNode;
}

SArray* qCreateQueryPlan(SQueryInfo* pQueryInfo) {
  // join and subquery
  SArray* upstream = NULL;
  if (pQueryInfo->pUpstream != NULL) {  // subquery in the from clause
    upstream = taosArrayInit(4, POINTER_BYTES);

    size_t size = taosArrayGetSize(pQueryInfo->pUpstream);
    for(int32_t i = 0; i < size; ++i) {
      SQueryInfo* pq = taosArrayGet(pQueryInfo->pUpstream, i);
      SArray* p = qCreateQueryPlan(pq);
      taosArrayPushBatch(upstream, p->pData, (int32_t) taosArrayGetSize(p));
    }
  }

  if (pQueryInfo->numOfTables > 1) {  // it is a join query
    // 1. separate the select clause according to table
    int32_t tableIndex = 0;
    STableMetaInfo* pTableMetaInfo = pQueryInfo->pTableMetaInfo[tableIndex];
    uint64_t uid = pTableMetaInfo->pTableMeta->id.uid;

    SArray* exprList = taosArrayInit(4, POINTER_BYTES);
    if (tscSqlExprCopy(exprList, pQueryInfo->exprList, uid, true) != 0) {
      terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
      exit(-1);
    }

    SArray* tableColumnList = taosArrayInit(4, sizeof(SColumn));
    tscColumnListCopy(tableColumnList, pQueryInfo->colList, uid);


    // 2.
    SQueryNode* pNode = doCreateQueryPlanForOneTable(pQueryInfo);
    UNUSED(pNode);
  } else {  // only one table, normal query process
    SQueryNode* pNode = doCreateQueryPlanForOneTable(pQueryInfo);
    UNUSED(pNode);
  }

  return NULL;
}

char* queryPlanToString() {
  return NULL;
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
    } else if (pQueryAttr->tsCompQuery || pQueryAttr->pointInterpQuery) {
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
    if (pQueryAttr->distinctTag) {
      op = OP_Distinct;
      taosArrayPush(plan, &op);
    }
  } else if (pQueryAttr->interval.interval > 0) {
    if (pQueryAttr->stableQuery) {
      op = OP_MultiTableTimeInterval;
      taosArrayPush(plan, &op);
    } else {
      op = OP_TimeWindow;
      taosArrayPush(plan, &op);

      if (pQueryAttr->pExpr2 != NULL) {
        op = OP_Arithmetic;
        taosArrayPush(plan, &op);
      }

      if (pQueryAttr->fillType != TSDB_FILL_NONE && (!pQueryAttr->pointInterpQuery)) {
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
      op = OP_Arithmetic;
      taosArrayPush(plan, &op);
    }
  } else if (pQueryAttr->sw.gap > 0) {
    op = OP_SessionWindow;
    taosArrayPush(plan, &op);

    if (pQueryAttr->pExpr2 != NULL) {
      op = OP_Arithmetic;
      taosArrayPush(plan, &op);
    }
  } else if (pQueryAttr->simpleAgg) {
    if (pQueryAttr->stableQuery && !pQueryAttr->tsCompQuery) {
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
      op = OP_Arithmetic;
      taosArrayPush(plan, &op);
    }
  } else {  // diff/add/multiply/subtract/division
    op = OP_Arithmetic;
    taosArrayPush(plan, &op);
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

  if (pQueryAttr->distinctTag) {
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
      op = OP_Arithmetic;
      taosArrayPush(plan, &op);
    }
  }

  // fill operator
  if (pQueryAttr->fillType != TSDB_FILL_NONE && (!pQueryAttr->pointInterpQuery)) {
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
