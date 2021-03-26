#include "os.h"
#include "tsclient.h"
#include "qUtil.h"
#include "texpr.h"

UNUSED_FUNC SArray* createTableScanPlan(SQuery* pQuery) {
  SArray* plan = taosArrayInit(4, sizeof(int32_t));

  int32_t op = 0;
  if (onlyQueryTags(pQuery)) {
//    op = OP_TagScan;
  } else if (pQuery->queryBlockDist) {
    op = OP_TableBlockInfoScan;
  } else if (pQuery->tsCompQuery || pQuery->pointInterpQuery) {
    op = OP_TableSeqScan;
  } else if (pQuery->needReverseScan) {
    op = OP_DataBlocksOptScan;
  } else {
    op = OP_TableScan;
  }

  taosArrayPush(plan, &op);
  return plan;
}

UNUSED_FUNC SArray* createExecOperatorPlan(SQuery* pQuery) {
  SArray* plan = taosArrayInit(4, sizeof(int32_t));
  int32_t op = 0;

  if (onlyQueryTags(pQuery)) {  // do nothing for tags query
    op = OP_TagScan;
    taosArrayPush(plan, &op);
  } else if (pQuery->interval.interval > 0) {
    if (pQuery->stableQuery) {
      op = OP_MultiTableTimeInterval;
      taosArrayPush(plan, &op);
    } else {
      op = OP_TimeWindow;
      taosArrayPush(plan, &op);

      if (pQuery->pExpr2 != NULL) {
        op = OP_Arithmetic;
        taosArrayPush(plan, &op);
      }

      if (pQuery->fillType != TSDB_FILL_NONE && (!pQuery->pointInterpQuery)) {
        op = OP_Fill;
        taosArrayPush(plan, &op);
      }
    }

  } else if (pQuery->groupbyColumn) {
    op = OP_Groupby;
    taosArrayPush(plan, &op);

    if (pQuery->pExpr2 != NULL) {
      op = OP_Arithmetic;
      taosArrayPush(plan, &op);
    }
  } else if (pQuery->sw.gap > 0) {
    op = OP_SessionWindow;
    taosArrayPush(plan, &op);

    if (pQuery->pExpr2 != NULL) {
      op = OP_Arithmetic;
      taosArrayPush(plan, &op);
    }
  } else if (pQuery->simpleAgg) {
    if (pQuery->stableQuery && !pQuery->tsCompQuery) {
      op = OP_MultiTableAggregate;
    } else {
      op = OP_Aggregate;
    }

    taosArrayPush(plan, &op);

    if (pQuery->pExpr2 != NULL && !pQuery->stableQuery) {
      op = OP_Arithmetic;
      taosArrayPush(plan, &op);
    }
  } else {  // diff/add/multiply/subtract/division
    op = OP_Arithmetic;
    taosArrayPush(plan, &op);
  }

  if (pQuery->limit.offset > 0) {
    op = OP_Offset;
    taosArrayPush(plan, &op);
  }

  if (pQuery->limit.limit > 0) {
    op = OP_Limit;
    taosArrayPush(plan, &op);
  }

  return plan;
}


