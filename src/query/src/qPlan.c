#include "os.h"
#include "tsclient.h"
#include "qUtil.h"
#include "texpr.h"

#define QNODE_PROJECT    1
#define QNODE_FILTER     2
#define QNODE_RELATION   3
#define QNODE_AGGREGATE  4
#define QNODE_GROUPBY    5
#define QNODE_LIMIT      6
#define QNODE_JOIN       7
#define QNODE_DIST       8
#define QNODE_SORT       9
#define QNODE_UNIONALL   10
#define QNODE_TIMEWINDOW 11

typedef struct SQueryNode {
  int32_t    type;         // the type of logic node
  char      *name;         // the name of logic node

  SSchema   *pSchema;      // the schema of the input SSDatablock
  int32_t    numOfCols;    // number of input columns
  SExprInfo *pExpr;        // the query functions or sql aggregations
  int32_t    numOfOutput;  // number of result columns, which is also the number of pExprs

  // previous operator to generated result for current node to process
  // in case of join, multiple prev nodes exist.
  struct SQueryNode* prevNode;
  struct SQueryNode* nextNode;
} SQueryNode;

// TODO create the query plan
SQueryNode* qCreateQueryPlan(SQueryInfo* pQueryInfo) {
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
