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
#include "os.h"
#include "tname.h"

#include "tglobal.h"

#include "executorInt.h"
#include "index.h"
#include "operator.h"
#include "query.h"
#include "querytask.h"
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

// each operator should be set their own function to return total cost buffer
int32_t optrDefaultBufFn(SOperatorInfo* pOperator) {
  if (pOperator->blocking) {
    return -1;
  } else {
    return 0;
  }
}

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
  SExtScanInfo info = {.inheritUsOrder = inheritUsOrder, .order = *order};
  STraverParam p = {.pParam = &info};

  traverseOperatorTree(pOperator, extractScanInfo, &p, NULL);
  *order = info.order;
  *scanFlag = info.scanFlag;

  ASSERT(*order == TSDB_ORDER_ASC || *order == TSDB_ORDER_DESC);
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

    if (pInfo->pTableScanOp != NULL) {
      STableScanInfo* pTableScanInfo = pInfo->pTableScanOp->info;
      if (pTableScanInfo != NULL && pTableScanInfo->base.dataReader != NULL) {
        tsdbReaderSetCloseFlag(pTableScanInfo->base.dataReader);
      }
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

SOperatorInfo* createOperator(SPhysiNode* pPhyNode, SExecTaskInfo* pTaskInfo, SReadHandle* pHandle, SNode* pTagCond,
                              SNode* pTagIndexCond, const char* pUser, const char* dbname) {
  int32_t     type = nodeType(pPhyNode);
  const char* idstr = GET_TASKID(pTaskInfo);

  if (pPhyNode->pChildren == NULL || LIST_LENGTH(pPhyNode->pChildren) == 0) {
    SOperatorInfo* pOperator = NULL;
    if (QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN == type) {
      STableScanPhysiNode* pTableScanNode = (STableScanPhysiNode*)pPhyNode;

      // NOTE: this is an patch to fix the physical plan
      // TODO remove it later
      if (pTableScanNode->scan.node.pLimit != NULL) {
        pTableScanNode->groupSort = true;
      }

      STableListInfo* pTableListInfo = tableListCreate();
      int32_t         code =
          createScanTableListInfo(&pTableScanNode->scan, pTableScanNode->pGroupTags, pTableScanNode->groupSort, pHandle,
                                  pTableListInfo, pTagCond, pTagIndexCond, pTaskInfo);
      if (code) {
        pTaskInfo->code = code;
        tableListDestroy(pTableListInfo);
        qError("failed to createScanTableListInfo, code:%s, %s", tstrerror(code), idstr);
        return NULL;
      }

      code = initQueriedTableSchemaInfo(pHandle, &pTableScanNode->scan, dbname, pTaskInfo);
      if (code) {
        pTaskInfo->code = code;
        tableListDestroy(pTableListInfo);
        return NULL;
      }

      pOperator = createTableScanOperatorInfo(pTableScanNode, pHandle, pTableListInfo, pTaskInfo);
      if (NULL == pOperator) {
        pTaskInfo->code = terrno;
        tableListDestroy(pTableListInfo);
        return NULL;
      }

      STableScanInfo* pScanInfo = pOperator->info;
      pTaskInfo->cost.pRecoder = &pScanInfo->base.readRecorder;
    } else if (QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN == type) {
      STableMergeScanPhysiNode* pTableScanNode = (STableMergeScanPhysiNode*)pPhyNode;
      STableListInfo*           pTableListInfo = tableListCreate();

      int32_t code = createScanTableListInfo(&pTableScanNode->scan, pTableScanNode->pGroupTags, true, pHandle,
                                             pTableListInfo, pTagCond, pTagIndexCond, pTaskInfo);
      if (code) {
        pTaskInfo->code = code;
        tableListDestroy(pTableListInfo);
        qError("failed to createScanTableListInfo, code: %s", tstrerror(code));
        return NULL;
      }

      code = initQueriedTableSchemaInfo(pHandle, &pTableScanNode->scan, dbname, pTaskInfo);
      if (code) {
        pTaskInfo->code = terrno;
        tableListDestroy(pTableListInfo);
        return NULL;
      }

      pOperator = createTableMergeScanOperatorInfo(pTableScanNode, pHandle, pTableListInfo, pTaskInfo);
      if (NULL == pOperator) {
        pTaskInfo->code = terrno;
        tableListDestroy(pTableListInfo);
        return NULL;
      }

      STableScanInfo* pScanInfo = pOperator->info;
      pTaskInfo->cost.pRecoder = &pScanInfo->base.readRecorder;
    } else if (QUERY_NODE_PHYSICAL_PLAN_EXCHANGE == type) {
      pOperator = createExchangeOperatorInfo(pHandle ? pHandle->pMsgCb->clientRpc : NULL, (SExchangePhysiNode*)pPhyNode,
                                             pTaskInfo);
    } else if (QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN == type) {
      STableScanPhysiNode* pTableScanNode = (STableScanPhysiNode*)pPhyNode;
      STableListInfo*      pTableListInfo = tableListCreate();

      if (pHandle->vnode) {
        int32_t code =
            createScanTableListInfo(&pTableScanNode->scan, pTableScanNode->pGroupTags, pTableScanNode->groupSort,
                                    pHandle, pTableListInfo, pTagCond, pTagIndexCond, pTaskInfo);
        if (code) {
          pTaskInfo->code = code;
          tableListDestroy(pTableListInfo);
          qError("failed to createScanTableListInfo, code: %s", tstrerror(code));
          return NULL;
        }
      }

      pTaskInfo->schemaInfo.qsw = extractQueriedColumnSchema(&pTableScanNode->scan);
      pOperator = createStreamScanOperatorInfo(pHandle, pTableScanNode, pTagCond, pTableListInfo, pTaskInfo);
    } else if (QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN == type) {
      SSystemTableScanPhysiNode* pSysScanPhyNode = (SSystemTableScanPhysiNode*)pPhyNode;
      pOperator = createSysTableScanOperatorInfo(pHandle, pSysScanPhyNode, pUser, pTaskInfo);
    } else if (QUERY_NODE_PHYSICAL_PLAN_TABLE_COUNT_SCAN == type) {
      STableCountScanPhysiNode* pTblCountScanNode = (STableCountScanPhysiNode*)pPhyNode;
      pOperator = createTableCountScanOperatorInfo(pHandle, pTblCountScanNode, pTaskInfo);
    } else if (QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN == type) {
      STagScanPhysiNode* pScanPhyNode = (STagScanPhysiNode*)pPhyNode;
      STableListInfo*    pTableListInfo = tableListCreate();
      int32_t            code = createScanTableListInfo(pScanPhyNode, NULL, false, pHandle, pTableListInfo, pTagCond,
                                                        pTagIndexCond, pTaskInfo);
      if (code != TSDB_CODE_SUCCESS) {
        pTaskInfo->code = code;
        qError("failed to getTableList, code: %s", tstrerror(code));
        return NULL;
      }

      pOperator = createTagScanOperatorInfo(pHandle, pScanPhyNode, pTableListInfo, pTaskInfo);
    } else if (QUERY_NODE_PHYSICAL_PLAN_BLOCK_DIST_SCAN == type) {
      SBlockDistScanPhysiNode* pBlockNode = (SBlockDistScanPhysiNode*)pPhyNode;
      STableListInfo*          pTableListInfo = tableListCreate();

      if (pBlockNode->tableType == TSDB_SUPER_TABLE) {
        SArray* pList = taosArrayInit(4, sizeof(STableKeyInfo));
        int32_t code = vnodeGetAllTableList(pHandle->vnode, pBlockNode->uid, pList);
        if (code != TSDB_CODE_SUCCESS) {
          pTaskInfo->code = terrno;
          return NULL;
        }

        size_t num = taosArrayGetSize(pList);
        for (int32_t i = 0; i < num; ++i) {
          STableKeyInfo* p = taosArrayGet(pList, i);
          tableListAddTableInfo(pTableListInfo, p->uid, 0);
        }

        taosArrayDestroy(pList);
      } else {  // Create group with only one table
        tableListAddTableInfo(pTableListInfo, pBlockNode->uid, 0);
      }

      pOperator = createDataBlockInfoScanOperator(pHandle, pBlockNode, pTableListInfo, pTaskInfo);
    } else if (QUERY_NODE_PHYSICAL_PLAN_LAST_ROW_SCAN == type) {
      SLastRowScanPhysiNode* pScanNode = (SLastRowScanPhysiNode*)pPhyNode;
      STableListInfo*        pTableListInfo = tableListCreate();

      int32_t code = createScanTableListInfo(&pScanNode->scan, pScanNode->pGroupTags, true, pHandle, pTableListInfo,
                                             pTagCond, pTagIndexCond, pTaskInfo);
      if (code != TSDB_CODE_SUCCESS) {
        pTaskInfo->code = code;
        return NULL;
      }

      code = initQueriedTableSchemaInfo(pHandle, &pScanNode->scan, dbname, pTaskInfo);
      if (code != TSDB_CODE_SUCCESS) {
        pTaskInfo->code = code;
        return NULL;
      }

      pOperator = createCacherowsScanOperator(pScanNode, pHandle, pTableListInfo, pTaskInfo);
    } else if (QUERY_NODE_PHYSICAL_PLAN_PROJECT == type) {
      pOperator = createProjectOperatorInfo(NULL, (SProjectPhysiNode*)pPhyNode, pTaskInfo);
    } else {
      terrno = TSDB_CODE_INVALID_PARA;
      return NULL;
    }

    if (pOperator != NULL) {  // todo moved away
      pOperator->resultDataBlockId = pPhyNode->pOutputDataBlockDesc->dataBlockId;
    }

    return pOperator;
  }

  size_t          size = LIST_LENGTH(pPhyNode->pChildren);
  SOperatorInfo** ops = taosMemoryCalloc(size, POINTER_BYTES);
  if (ops == NULL) {
    return NULL;
  }

  for (int32_t i = 0; i < size; ++i) {
    SPhysiNode* pChildNode = (SPhysiNode*)nodesListGetNode(pPhyNode->pChildren, i);
    ops[i] = createOperator(pChildNode, pTaskInfo, pHandle, pTagCond, pTagIndexCond, pUser, dbname);
    if (ops[i] == NULL) {
      taosMemoryFree(ops);
      return NULL;
    }
  }

  SOperatorInfo* pOptr = NULL;
  if (QUERY_NODE_PHYSICAL_PLAN_PROJECT == type) {
    pOptr = createProjectOperatorInfo(ops[0], (SProjectPhysiNode*)pPhyNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_HASH_AGG == type) {
    SAggPhysiNode* pAggNode = (SAggPhysiNode*)pPhyNode;
    if (pAggNode->pGroupKeys != NULL) {
      pOptr = createGroupOperatorInfo(ops[0], pAggNode, pTaskInfo);
    } else {
      pOptr = createAggregateOperatorInfo(ops[0], pAggNode, pTaskInfo);
    }
  } else if (QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL == type) {
    SIntervalPhysiNode* pIntervalPhyNode = (SIntervalPhysiNode*)pPhyNode;
    pOptr = createIntervalOperatorInfo(ops[0], pIntervalPhyNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL == type) {
    pOptr = createStreamIntervalOperatorInfo(ops[0], pPhyNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_INTERVAL == type) {
    SMergeAlignedIntervalPhysiNode* pIntervalPhyNode = (SMergeAlignedIntervalPhysiNode*)pPhyNode;
    pOptr = createMergeAlignedIntervalOperatorInfo(ops[0], pIntervalPhyNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_MERGE_INTERVAL == type) {
    SMergeIntervalPhysiNode* pIntervalPhyNode = (SMergeIntervalPhysiNode*)pPhyNode;
    pOptr = createMergeIntervalOperatorInfo(ops[0], pIntervalPhyNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_INTERVAL == type) {
    int32_t children = 0;
    pOptr = createStreamFinalIntervalOperatorInfo(ops[0], pPhyNode, pTaskInfo, children);
  } else if (QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_INTERVAL == type) {
    int32_t children = pHandle->numOfVgroups;
    pOptr = createStreamFinalIntervalOperatorInfo(ops[0], pPhyNode, pTaskInfo, children);
  } else if (QUERY_NODE_PHYSICAL_PLAN_SORT == type) {
    pOptr = createSortOperatorInfo(ops[0], (SSortPhysiNode*)pPhyNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_GROUP_SORT == type) {
    pOptr = createGroupSortOperatorInfo(ops[0], (SGroupSortPhysiNode*)pPhyNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_MERGE == type) {
    SMergePhysiNode* pMergePhyNode = (SMergePhysiNode*)pPhyNode;
    pOptr = createMultiwayMergeOperatorInfo(ops, size, pMergePhyNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_MERGE_SESSION == type) {
    SSessionWinodwPhysiNode* pSessionNode = (SSessionWinodwPhysiNode*)pPhyNode;
    pOptr = createSessionAggOperatorInfo(ops[0], pSessionNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_STREAM_SESSION == type) {
    pOptr = createStreamSessionAggOperatorInfo(ops[0], pPhyNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_SESSION == type) {
    int32_t children = 0;
    pOptr = createStreamFinalSessionAggOperatorInfo(ops[0], pPhyNode, pTaskInfo, children);
  } else if (QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_SESSION == type) {
    int32_t children = pHandle->numOfVgroups;
    pOptr = createStreamFinalSessionAggOperatorInfo(ops[0], pPhyNode, pTaskInfo, children);
  } else if (QUERY_NODE_PHYSICAL_PLAN_PARTITION == type) {
    pOptr = createPartitionOperatorInfo(ops[0], (SPartitionPhysiNode*)pPhyNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_STREAM_PARTITION == type) {
    pOptr = createStreamPartitionOperatorInfo(ops[0], (SStreamPartitionPhysiNode*)pPhyNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_MERGE_STATE == type) {
    SStateWinodwPhysiNode* pStateNode = (SStateWinodwPhysiNode*)pPhyNode;
    pOptr = createStatewindowOperatorInfo(ops[0], pStateNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_STREAM_STATE == type) {
    pOptr = createStreamStateAggOperatorInfo(ops[0], pPhyNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN == type) {
    pOptr = createMergeJoinOperatorInfo(ops, size, (SSortMergeJoinPhysiNode*)pPhyNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_FILL == type) {
    pOptr = createFillOperatorInfo(ops[0], (SFillPhysiNode*)pPhyNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_STREAM_FILL == type) {
    pOptr = createStreamFillOperatorInfo(ops[0], (SStreamFillPhysiNode*)pPhyNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_INDEF_ROWS_FUNC == type) {
    pOptr = createIndefinitOutputOperatorInfo(ops[0], pPhyNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_INTERP_FUNC == type) {
    pOptr = createTimeSliceOperatorInfo(ops[0], pPhyNode, pTaskInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_MERGE_EVENT == type) {
    pOptr = createEventwindowOperatorInfo(ops[0], pPhyNode, pTaskInfo);
  } else {
    terrno = TSDB_CODE_INVALID_PARA;
    taosMemoryFree(ops);
    return NULL;
  }

  taosMemoryFree(ops);
  if (pOptr) {
    pOptr->resultDataBlockId = pPhyNode->pOutputDataBlockDesc->dataBlockId;
  }

  return pOptr;
}

void destroyOperator(SOperatorInfo* pOperator) {
  if (pOperator == NULL) {
    return;
  }

  if (pOperator->fpSet.closeFn != NULL) {
    pOperator->fpSet.closeFn(pOperator->info);
  }

  if (pOperator->pDownstream != NULL) {
    for (int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
      destroyOperator(pOperator->pDownstream[i]);
    }

    taosMemoryFreeClear(pOperator->pDownstream);
    pOperator->numOfDownstream = 0;
  }

  cleanupExprSupp(&pOperator->exprSupp);
  taosMemoryFreeClear(pOperator);
}

int32_t getOperatorExplainExecInfo(SOperatorInfo* operatorInfo, SArray* pExecInfoList) {
  SExplainExecInfo  execInfo = {0};
  SExplainExecInfo* pExplainInfo = taosArrayPush(pExecInfoList, &execInfo);

  pExplainInfo->numOfRows = operatorInfo->resultInfo.totalRows;
  pExplainInfo->startupCost = operatorInfo->cost.openCost;
  pExplainInfo->totalCost = operatorInfo->cost.totalCost;
  pExplainInfo->verboseLen = 0;
  pExplainInfo->verboseInfo = NULL;

  if (operatorInfo->fpSet.getExplainFn) {
    int32_t code =
        operatorInfo->fpSet.getExplainFn(operatorInfo, &pExplainInfo->verboseInfo, &pExplainInfo->verboseLen);
    if (code) {
      qError("%s operator getExplainFn failed, code:%s", GET_TASKID(operatorInfo->pTaskInfo), tstrerror(code));
      return code;
    }
  }

  int32_t code = 0;
  for (int32_t i = 0; i < operatorInfo->numOfDownstream; ++i) {
    code = getOperatorExplainExecInfo(operatorInfo->pDownstream[i], pExecInfoList);
    if (code != TSDB_CODE_SUCCESS) {
      //      taosMemoryFreeClear(*pRes);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  return TSDB_CODE_SUCCESS;
}
