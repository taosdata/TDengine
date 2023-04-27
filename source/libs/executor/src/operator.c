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

#include "filter.h"
#include "function.h"
#include "functionMgt.h"
#include "os.h"
#include "querynodes.h"
#include "tfill.h"
#include "tname.h"

#include "tdatablock.h"
#include "tglobal.h"
#include "tmsg.h"
#include "ttime.h"

#include "executorimpl.h"
#include "index.h"
#include "query.h"
#include "tcompare.h"
#include "thash.h"
#include "ttypes.h"
#include "vnode.h"

SOperatorFpSet createOperatorFpSet(__optr_open_fn_t openFn, __optr_fn_t nextFn, __optr_fn_t cleanup,
                                   __optr_close_fn_t closeFn, __optr_reqBuf_fn_t reqBufFn,
                                   __optr_explain_fn_t explain) {
  SOperatorFpSet fpSet = {
      ._openFn = openFn,
      .getNextFn = nextFn,
      .cleanupFn = cleanup,
      .closeFn = closeFn,
      .reqBufFn = reqBufFn,
      .getExplainFn = explain,
  };

  return fpSet;
}

int32_t optrDummyOpenFn(SOperatorInfo* pOperator) {
  OPTR_SET_OPENED(pOperator);
  pOperator->cost.openCost = 0;
  return TSDB_CODE_SUCCESS;
}

int32_t appendDownstream(SOperatorInfo* p, SOperatorInfo** pDownstream, int32_t num) {
  p->pDownstream = taosMemoryCalloc(1, num * POINTER_BYTES);
  if (p->pDownstream == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  memcpy(p->pDownstream, pDownstream, num * POINTER_BYTES);
  p->numOfDownstream = num;
  return TSDB_CODE_SUCCESS;
}

void setOperatorCompleted(SOperatorInfo* pOperator) {
  pOperator->status = OP_EXEC_DONE;
  pOperator->cost.totalCost = (taosGetTimestampUs() - pOperator->pTaskInfo->cost.start) / 1000.0;
  setTaskStatus(pOperator->pTaskInfo, TASK_COMPLETED);
}

void setOperatorInfo(SOperatorInfo* pOperator, const char* name, int32_t type, bool blocking, int32_t status,
                     void* pInfo, SExecTaskInfo* pTaskInfo) {
  pOperator->name = (char*)name;
  pOperator->operatorType = type;
  pOperator->blocking = blocking;
  pOperator->status = status;
  pOperator->info = pInfo;
  pOperator->pTaskInfo = pTaskInfo;
}

void destroyOperatorInfo(SOperatorInfo* pOperator) {
  if (pOperator == NULL) {
    return;
  }

  if (pOperator->fpSet.closeFn != NULL) {
    pOperator->fpSet.closeFn(pOperator->info);
  }

  if (pOperator->pDownstream != NULL) {
    for (int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
      destroyOperatorInfo(pOperator->pDownstream[i]);
    }

    taosMemoryFreeClear(pOperator->pDownstream);
    pOperator->numOfDownstream = 0;
  }

  cleanupExprSupp(&pOperator->exprSupp);
  taosMemoryFreeClear(pOperator);
}

// each operator should be set their own function to return total cost buffer
int32_t optrDefaultBufFn(SOperatorInfo* pOperator) {
  if (pOperator->blocking) {
    return -1;
  } else {
    return 0;
  }
}

//int32_t getTableScanInfo(SOperatorInfo* pOperator, int32_t* order, int32_t* scanFlag, bool inheritUsOrder) {
//  // todo add more information about exchange operation
//  int32_t type = pOperator->operatorType;
//  if (type == QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN || type == QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN ||
//      type == QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN || type == QUERY_NODE_PHYSICAL_PLAN_BLOCK_DIST_SCAN ||
//      type == QUERY_NODE_PHYSICAL_PLAN_LAST_ROW_SCAN || type == QUERY_NODE_PHYSICAL_PLAN_TABLE_COUNT_SCAN) {
//    *order = TSDB_ORDER_ASC;
//    *scanFlag = MAIN_SCAN;
//    return TSDB_CODE_SUCCESS;
//  } else if (type == QUERY_NODE_PHYSICAL_PLAN_EXCHANGE) {
//    if (!inheritUsOrder) {
//      *order = TSDB_ORDER_ASC;
//    }
//    *scanFlag = MAIN_SCAN;
//    return TSDB_CODE_SUCCESS;
//  } else if (type == QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN) {
//    STableScanInfo* pTableScanInfo = pOperator->info;
//    *order = pTableScanInfo->base.cond.order;
//    *scanFlag = pTableScanInfo->base.scanFlag;
//    return TSDB_CODE_SUCCESS;
//  } else if (type == QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN) {
//    STableMergeScanInfo* pTableScanInfo = pOperator->info;
//    *order = pTableScanInfo->base.cond.order;
//    *scanFlag = pTableScanInfo->base.scanFlag;
//    return TSDB_CODE_SUCCESS;
//  } else {
//    if (pOperator->pDownstream == NULL || pOperator->pDownstream[0] == NULL) {
//      return TSDB_CODE_INVALID_PARA;
//    } else {
//      return getTableScanInfo(pOperator->pDownstream[0], order, scanFlag, inheritUsOrder);
//    }
//  }
//}

static int64_t getQuerySupportBufSize(size_t numOfTables) {
  size_t s1 = sizeof(STableQueryInfo);
  //  size_t s3 = sizeof(STableCheckInfo);  buffer consumption in tsdb
  return (int64_t)(s1 * 1.5 * numOfTables);
}

int32_t checkForQueryBuf(size_t numOfTables) {
  int64_t t = getQuerySupportBufSize(numOfTables);
  if (tsQueryBufferSizeBytes < 0) {
    return TSDB_CODE_SUCCESS;
  } else if (tsQueryBufferSizeBytes > 0) {
    while (1) {
      int64_t s = tsQueryBufferSizeBytes;
      int64_t remain = s - t;
      if (remain >= 0) {
        if (atomic_val_compare_exchange_64(&tsQueryBufferSizeBytes, s, remain) == s) {
          return TSDB_CODE_SUCCESS;
        }
      } else {
        return TSDB_CODE_QRY_NOT_ENOUGH_BUFFER;
      }
    }
  }

  // disable query processing if the value of tsQueryBufferSize is zero.
  return TSDB_CODE_QRY_NOT_ENOUGH_BUFFER;
}

void releaseQueryBuf(size_t numOfTables) {
  if (tsQueryBufferSizeBytes < 0) {
    return;
  }

  int64_t t = getQuerySupportBufSize(numOfTables);

  // restore value is not enough buffer available
  atomic_add_fetch_64(&tsQueryBufferSizeBytes, t);
}

typedef enum {
  OPTR_FN_RET_CONTINUE = 0x1,
  OPTR_FN_RET_ABORT = 0x2,
} ERetType;

typedef struct STraverParam {
  void*   pRet;
  int32_t code;
  void*   pParam;
} STraverParam;

// iterate the operator tree helper
typedef ERetType (*optr_fn_t)(SOperatorInfo *pOperator, STraverParam *pParam, const char* pIdstr);

void traverseOperatorTree(SOperatorInfo* pOperator, optr_fn_t fn, STraverParam* pParam, const char* id) {
  if (pOperator == NULL) {
    return;
  }

  ERetType ret = fn(pOperator, pParam, id);
  if (ret == OPTR_FN_RET_ABORT || pParam->code != TSDB_CODE_SUCCESS) {
    return;
  }

  for (int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
    traverseOperatorTree(pOperator->pDownstream[i], fn, pParam, id);
    if (pParam->code != 0) {
      break;
    }
  }
}

ERetType extractOperatorInfo(SOperatorInfo* pOperator, STraverParam* pParam, const char* pIdStr) {
  STraverParam* p = pParam;
  if (pOperator->operatorType == *(int32_t*)p->pParam) {
    p->pRet = pOperator;
    return OPTR_FN_RET_ABORT;
  } else {
    return OPTR_FN_RET_CONTINUE;
  }
}

// QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN
SOperatorInfo* extractOperatorInTree(SOperatorInfo* pOperator, int32_t type, const char* id) {
  if (pOperator == NULL) {
    qError("invalid operator, failed to find tableScanOperator %s", id);
    terrno = TSDB_CODE_PAR_INTERNAL_ERROR;
    return NULL;
  }

  STraverParam p = {.pParam = &type, .pRet = NULL};
  traverseOperatorTree(pOperator, extractOperatorInfo, &p, id);
  if (p.code != 0) {
    terrno = p.code;
    return NULL;
  } else {
    return p.pRet;
  }
}

typedef struct SExtScanInfo {
  int32_t order;
  int32_t scanFlag;
  int32_t inheritUsOrder;
} SExtScanInfo;

static ERetType extractScanInfo(SOperatorInfo* pOperator, STraverParam* pParam, const char* pIdStr) {
  int32_t type = pOperator->operatorType;
  SExtScanInfo* pInfo = pParam->pParam;

  if (type == QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN || type == QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN ||
      type == QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN || type == QUERY_NODE_PHYSICAL_PLAN_BLOCK_DIST_SCAN ||
      type == QUERY_NODE_PHYSICAL_PLAN_LAST_ROW_SCAN || type == QUERY_NODE_PHYSICAL_PLAN_TABLE_COUNT_SCAN) {
    pInfo->order = TSDB_ORDER_ASC;
    pInfo->scanFlag= MAIN_SCAN;
    return OPTR_FN_RET_ABORT;
  } else if (type == QUERY_NODE_PHYSICAL_PLAN_EXCHANGE) {
    if (!pInfo->inheritUsOrder) {
      pInfo->order = TSDB_ORDER_ASC;
    }
    pInfo->scanFlag= MAIN_SCAN;
    return OPTR_FN_RET_ABORT;
  } else if (type == QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN) {
    STableScanInfo* pTableScanInfo = pOperator->info;
    pInfo->order = pTableScanInfo->base.cond.order;
    pInfo->scanFlag= pTableScanInfo->base.scanFlag;
    return OPTR_FN_RET_ABORT;
  } else if (type == QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN) {
    STableMergeScanInfo* pTableScanInfo = pOperator->info;
    pInfo->order = pTableScanInfo->base.cond.order;
    pInfo->scanFlag= pTableScanInfo->base.scanFlag;
    return OPTR_FN_RET_ABORT;
  } else {
    return OPTR_FN_RET_CONTINUE;
  }
}

int32_t getTableScanInfo(SOperatorInfo* pOperator, int32_t* order, int32_t* scanFlag, bool inheritUsOrder) {
  SExtScanInfo info = {.inheritUsOrder = inheritUsOrder};
  STraverParam p = {.pParam = &info};

  extractScanInfo(pOperator, &p, NULL);
  *order = info.order;
  *scanFlag = info.scanFlag;

  return p.code;
}

static ERetType doStopDataReader(SOperatorInfo* pOperator, STraverParam* pParam, const char* pIdStr) {
  if (pOperator->operatorType == QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN) {
    STableScanInfo* pInfo = pOperator->info;

    if (pInfo->base.dataReader != NULL) {
      tsdbReaderSetCloseFlag(pInfo->base.dataReader);
    }
    return OPTR_FN_RET_ABORT;
  } else if (pOperator->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN) {
    SStreamScanInfo* pInfo = pOperator->info;
    STableScanInfo* pTableScanInfo = pInfo->pTableScanOp->info;

    if (pTableScanInfo->base.dataReader != NULL) {
      tsdbReaderSetCloseFlag(pTableScanInfo->base.dataReader);
    }

    return OPTR_FN_RET_ABORT;
  }

  return OPTR_FN_RET_CONTINUE;
}

int32_t stopTableScanOperator(SOperatorInfo* pOperator, const char* pIdStr) {
  STraverParam p = {0};
  traverseOperatorTree(pOperator, doStopDataReader, &p, pIdStr);
  return p.code;
}
