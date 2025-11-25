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

#include "executor.h"
#include <stdint.h>
#include "cmdnodes.h"
#include "dataSinkInt.h"
#include "executil.h"
#include "executorInt.h"
#include "libs/new-stream/stream.h"
#include "operator.h"
#include "osMemPool.h"
#include "osMemory.h"
#include "planner.h"
#include "query.h"
#include "querytask.h"
#include "storageapi.h"
#include "streamexecutorInt.h"
#include "taosdef.h"
#include "tarray.h"
#include "tdatablock.h"
#include "tref.h"
#include "trpc.h"
#include "tudf.h"
#include "wal.h"

static TdThreadOnce initPoolOnce = PTHREAD_ONCE_INIT;
int32_t             exchangeObjRefPool = -1;
SGlobalExecInfo     gExecInfo = {0};

void gExecInfoInit(void* pDnode, getDnodeId_f getDnodeId, getMnodeEpset_f getMnode) {
  gExecInfo.dnode = pDnode;
  gExecInfo.getMnode = getMnode;
  gExecInfo.getDnodeId = getDnodeId;
  return;
}

int32_t getCurrentMnodeEpset(SEpSet* pEpSet) {
  if (gExecInfo.dnode == NULL || gExecInfo.getMnode == NULL) {
    qError("gExecInfo is not initialized");
    return TSDB_CODE_APP_ERROR;
  }
  gExecInfo.getMnode(gExecInfo.dnode, pEpSet);
  return TSDB_CODE_SUCCESS;
}

static void cleanupRefPool() {
  int32_t ref = atomic_val_compare_exchange_32(&exchangeObjRefPool, exchangeObjRefPool, 0);
  taosCloseRef(ref);
}

static void initRefPool() {
  exchangeObjRefPool = taosOpenRef(1024, doDestroyExchangeOperatorInfo);
  (void)atexit(cleanupRefPool);
}

static int32_t doSetSMABlock(SOperatorInfo* pOperator, void* input, size_t numOfBlocks, int32_t type, char* id) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (pOperator->operatorType != QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN) {
    if (pOperator->numOfDownstream == 0) {
      qError("failed to find stream scan operator to set the input data block, %s" PRIx64, id);
      return TSDB_CODE_APP_ERROR;
    }

    if (pOperator->numOfDownstream > 1) {  // not handle this in join query
      qError("join not supported for stream block scan, %s" PRIx64, id);
      return TSDB_CODE_APP_ERROR;
    }
    pOperator->status = OP_NOT_OPENED;
    return doSetSMABlock(pOperator->pDownstream[0], input, numOfBlocks, type, id);
  } else {
    pOperator->status = OP_NOT_OPENED;

    SStreamScanInfo* pInfo = pOperator->info;

    if (type == STREAM_INPUT__MERGED_SUBMIT) {
      for (int32_t i = 0; i < numOfBlocks; i++) {
        SPackedData* pReq = POINTER_SHIFT(input, i * sizeof(SPackedData));
        void*        tmp = taosArrayPush(pInfo->pBlockLists, pReq);
        QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
      }
      pInfo->blockType = STREAM_INPUT__DATA_SUBMIT;
    } else if (type == STREAM_INPUT__DATA_SUBMIT) {
      void* tmp = taosArrayPush(pInfo->pBlockLists, &input);
      QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
      pInfo->blockType = STREAM_INPUT__DATA_SUBMIT;
    } else if (type == STREAM_INPUT__DATA_BLOCK) {
      for (int32_t i = 0; i < numOfBlocks; ++i) {
        SSDataBlock* pDataBlock = &((SSDataBlock*)input)[i];
        SPackedData  tmp = {.pDataBlock = pDataBlock};
        void*        tmpItem = taosArrayPush(pInfo->pBlockLists, &tmp);
        QUERY_CHECK_NULL(tmpItem, code, lino, _end, terrno);
      }
      pInfo->blockType = STREAM_INPUT__DATA_BLOCK;
    } else if (type == STREAM_INPUT__CHECKPOINT) {
      SPackedData tmp = {.pDataBlock = input};
      void*       tmpItem = taosArrayPush(pInfo->pBlockLists, &tmp);
      QUERY_CHECK_NULL(tmpItem, code, lino, _end, terrno);
      pInfo->blockType = STREAM_INPUT__CHECKPOINT;
    } else if (type == STREAM_INPUT__REF_DATA_BLOCK) {
      for (int32_t i = 0; i < numOfBlocks; ++i) {
        SPackedData* pReq = POINTER_SHIFT(input, i * sizeof(SPackedData));
        void*        tmp = taosArrayPush(pInfo->pBlockLists, pReq);
        QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
      }
      pInfo->blockType = STREAM_INPUT__DATA_BLOCK;
    }

    return TSDB_CODE_SUCCESS;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doSetStreamOpOpen(SOperatorInfo* pOperator, char* id) {
  if (pOperator->operatorType != QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN) {
    if (pOperator->numOfDownstream == 0) {
      qError("failed to find stream scan operator to set the input data block, %s" PRIx64, id);
      return TSDB_CODE_APP_ERROR;
    }

    if (pOperator->numOfDownstream > 1) {  // not handle this in join query
      qError("join not supported for stream block scan, %s" PRIx64, id);
      return TSDB_CODE_APP_ERROR;
    }

    pOperator->status = OP_NOT_OPENED;
    return doSetStreamOpOpen(pOperator->pDownstream[0], id);
  }
  return 0;
}

int32_t doSetTaskId(SOperatorInfo* pOperator, SStorageAPI* pAPI) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  if (pOperator->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN) {
    SStreamScanInfo* pStreamScanInfo = pOperator->info;
    if (pStreamScanInfo->pTableScanOp != NULL) {
      STableScanInfo* pScanInfo = pStreamScanInfo->pTableScanOp->info;
      if (pScanInfo->base.dataReader != NULL) {
        int32_t code = pAPI->tsdReader.tsdSetReaderTaskId(pScanInfo->base.dataReader, pTaskInfo->id.str);
        if (code) {
          qError("failed to set reader id for executor, code:%s", tstrerror(code));
          return code;
        }
      }
    }
  } else {
    if (pOperator->pDownstream) return doSetTaskId(pOperator->pDownstream[0], pAPI);
  }

  return 0;
}

int32_t qSetTaskId(qTaskInfo_t tinfo, uint64_t taskId, uint64_t queryId) {
  SExecTaskInfo* pTaskInfo = tinfo;
  pTaskInfo->id.queryId = queryId;
  buildTaskId(taskId, queryId, pTaskInfo->id.str, 64);

  // set the idstr for tsdbReader
  return doSetTaskId(pTaskInfo->pRoot, &pTaskInfo->storageAPI);
}

bool qTaskIsDone(qTaskInfo_t tinfo) {
  SExecTaskInfo* pTaskInfo = tinfo;
  return pTaskInfo->status == OP_EXEC_DONE;
}

int32_t qSetSMAInput(qTaskInfo_t tinfo, const void* pBlocks, size_t numOfBlocks, int32_t type) {
  if (tinfo == NULL) {
    return TSDB_CODE_APP_ERROR;
  }

  if (pBlocks == NULL || numOfBlocks == 0) {
    return TSDB_CODE_SUCCESS;
  }

  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;

  int32_t code = doSetSMABlock(pTaskInfo->pRoot, (void*)pBlocks, numOfBlocks, type, GET_TASKID(pTaskInfo));
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed to set the sma block data", GET_TASKID(pTaskInfo));
  } else {
    qDebug("%s set the sma block successfully", GET_TASKID(pTaskInfo));
  }

  return code;
}

qTaskInfo_t qCreateQueueExecTaskInfo(void* msg, SReadHandle* pReaderHandle, int32_t vgId, int32_t* numOfCols,
                                     uint64_t id) {
  if (msg == NULL) {  // create raw scan
    SExecTaskInfo* pTaskInfo = NULL;

    int32_t code = doCreateTask(0, id, vgId, OPTR_EXEC_MODEL_QUEUE, &pReaderHandle->api, &pTaskInfo);
    if (NULL == pTaskInfo || code != 0) {
      return NULL;
    }

    code = createTmqRawScanOperatorInfo(pReaderHandle, pTaskInfo, &pTaskInfo->pRoot);
    if (NULL == pTaskInfo->pRoot || code != 0) {
      taosMemoryFree(pTaskInfo);
      return NULL;
    }

    pTaskInfo->storageAPI = pReaderHandle->api;
    qDebug("create raw scan task info completed, vgId:%d, %s", vgId, GET_TASKID(pTaskInfo));
    return pTaskInfo;
  }

  SSubplan* pPlan = NULL;
  int32_t   code = qStringToSubplan(msg, &pPlan);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return NULL;
  }

  qTaskInfo_t pTaskInfo = NULL;
  code = qCreateExecTask(pReaderHandle, vgId, 0, pPlan, &pTaskInfo, NULL, 0, NULL, OPTR_EXEC_MODEL_QUEUE);
  if (code != TSDB_CODE_SUCCESS) {
    qDestroyTask(pTaskInfo);
    terrno = code;
    return NULL;
  }

  // extract the number of output columns
  SDataBlockDescNode* pDescNode = pPlan->pNode->pOutputDataBlockDesc;
  *numOfCols = 0;

  SNode* pNode;
  FOREACH(pNode, pDescNode->pSlots) {
    SSlotDescNode* pSlotDesc = (SSlotDescNode*)pNode;
    if (pSlotDesc->output) {
      ++(*numOfCols);
    }
  }

  return pTaskInfo;
}

static int32_t checkInsertParam(SStreamInserterParam* streamInserterParam) {
  if (streamInserterParam == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  if (streamInserterParam->tbType == TSDB_SUPER_TABLE && streamInserterParam->suid <= 0) {
    stError("insertParam: invalid suid:%" PRIx64 " for child table", streamInserterParam->suid);
    return TSDB_CODE_INVALID_PARA;
  }

  if (streamInserterParam->dbFName == NULL || strlen(streamInserterParam->dbFName) == 0) {
    stError("insertParam: invalid db/table name");
    return TSDB_CODE_INVALID_PARA;
  }

  if (streamInserterParam->suid <= 0 &&
      (streamInserterParam->tbname == NULL || strlen(streamInserterParam->tbname) == 0)) {
    stError("insertParam: invalid table name, suid:%" PRIx64 "", streamInserterParam->suid);
    return TSDB_CODE_INVALID_PARA;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t qCreateStreamExecTask(SReadHandle* readHandle, int32_t vgId, uint64_t taskId, SSubplan* pSubplan,
                                     qTaskInfo_t* pTaskInfo, DataSinkHandle* handle, int8_t compressResult, char* sql,
                                     EOPTR_EXEC_MODEL model, SStreamInserterParam* streamInserterParam) {
  if (pSubplan == NULL || pTaskInfo == NULL) {
    qError("invalid parameter, pSubplan:%p, pTaskInfo:%p", pSubplan, pTaskInfo);
    nodesDestroyNode((SNode *)pSubplan);
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t lino = 0;
  int32_t code = checkInsertParam(streamInserterParam);
  if (code != TSDB_CODE_SUCCESS) {
    qError("invalid stream inserter param, code:%s", tstrerror(code));
    nodesDestroyNode((SNode *)pSubplan);
    return code;
  }
  SInserterParam* pInserterParam = NULL;
  SExecTaskInfo** pTask = (SExecTaskInfo**)pTaskInfo;
  (void)taosThreadOnce(&initPoolOnce, initRefPool);
  qDebug("start to create task, TID:0x%" PRIx64 " QID:0x%" PRIx64 ", vgId:%d", taskId, pSubplan->id.queryId, vgId);

  code = createExecTaskInfo(pSubplan, pTask, readHandle, taskId, vgId, sql, model);
  if (code != TSDB_CODE_SUCCESS || NULL == *pTask) {
    qError("failed to createExecTaskInfo, code:%s", tstrerror(code));
    goto _error;
  }

  if (streamInserterParam) {
    SDataSinkMgtCfg cfg = {.maxDataBlockNum = 500, .maxDataBlockNumPerQuery = 50, .compress = compressResult};
    void*           pSinkManager = NULL;
    code = dsDataSinkMgtInit(&cfg, &(*pTask)->storageAPI, &pSinkManager);
    if (code != TSDB_CODE_SUCCESS) {
      qError("failed to dsDataSinkMgtInit, code:%s, %s", tstrerror(code), (*pTask)->id.str);
      goto _error;
    }

    pInserterParam = taosMemoryCalloc(1, sizeof(SInserterParam));
    if (NULL == pInserterParam) {
      qError("failed to taosMemoryCalloc, code:%s, %s", tstrerror(terrno), (*pTask)->id.str);
      code = terrno;
      goto _error;
    }
    code = cloneStreamInserterParam(&pInserterParam->streamInserterParam, streamInserterParam);
    TSDB_CHECK_CODE(code, lino, _error);
    
    pInserterParam->readHandle = taosMemCalloc(1, sizeof(SReadHandle));
    pInserterParam->readHandle->pMsgCb = readHandle->pMsgCb;

    code = createStreamDataInserter(pSinkManager, handle, pInserterParam);
    if (code) {
      qError("failed to createStreamDataInserter, code:%s, %s", tstrerror(code), (*pTask)->id.str);
    }
  }
  qDebug("subplan task create completed, TID:0x%" PRIx64 " QID:0x%" PRIx64 " code:%s", taskId, pSubplan->id.queryId,
         tstrerror(code));

_error:

  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
    if (pInserterParam != NULL) {
      taosMemoryFree(pInserterParam);
    }
  }
  return code;
}

bool qNeedReset(qTaskInfo_t pInfo) {
  if (pInfo == NULL) {
    return false;
  }
  SExecTaskInfo*  pTaskInfo = (SExecTaskInfo*)pInfo;
  SOperatorInfo*  pOperator = pTaskInfo->pRoot;
  if (pOperator == NULL || pOperator->pPhyNode == NULL) {
    return false;
  }
  int32_t node = nodeType(pOperator->pPhyNode);
  return (QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN == node || 
          QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN == node ||
          QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN == node);
}

static void setReadHandle(SReadHandle* pHandle, STableScanBase* pScanBaseInfo) {
  if (pHandle == NULL || pScanBaseInfo == NULL) {
    return;
  }

  pScanBaseInfo->readHandle.uid = pHandle->uid;
  pScanBaseInfo->readHandle.winRangeValid = pHandle->winRangeValid;
  pScanBaseInfo->readHandle.winRange = pHandle->winRange;
  pScanBaseInfo->readHandle.extWinRangeValid = pHandle->extWinRangeValid;
  pScanBaseInfo->readHandle.extWinRange = pHandle->extWinRange;
  pScanBaseInfo->readHandle.cacheSttStatis = pHandle->cacheSttStatis;
}

int32_t qResetTableScan(qTaskInfo_t pInfo, SReadHandle* handle) {
  if (pInfo == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  SExecTaskInfo*  pTaskInfo = (SExecTaskInfo*)pInfo;
  SOperatorInfo*  pOperator = pTaskInfo->pRoot;

  void*           info = pOperator->info;
  STableScanBase* pScanBaseInfo = NULL;

  if (QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN == nodeType(pOperator->pPhyNode)) {
    pScanBaseInfo = &((STableScanInfo*)info)->base;
    setReadHandle(handle, pScanBaseInfo);
  } else if (QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN == nodeType(pOperator->pPhyNode)) {
    pScanBaseInfo = &((STableMergeScanInfo*)info)->base;
    setReadHandle(handle, pScanBaseInfo);
  }

  qDebug("reset table scan, name:%s, id:%s, time range: [%" PRId64 ", %" PRId64 "]", pOperator->name, GET_TASKID(pTaskInfo), handle->winRange.skey,
  handle->winRange.ekey);
  return pOperator->fpSet.resetStateFn(pOperator);
}

int32_t qCreateStreamExecTaskInfo(qTaskInfo_t* pTaskInfo, void* msg, SReadHandle* readers,
                                  SStreamInserterParam* pInserterParams, int32_t vgId, int32_t taskId) {
  if (msg == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  *pTaskInfo = NULL;

  SSubplan* pPlan = NULL;
  int32_t   code = qStringToSubplan(msg, &pPlan);
  if (code != TSDB_CODE_SUCCESS) {
    nodesDestroyNode((SNode *)pPlan);
    return code;
  }
  // todo: add stream inserter param
  code = qCreateStreamExecTask(readers, vgId, taskId, pPlan, pTaskInfo,
                               pInserterParams ? &pInserterParams->pSinkHandle : NULL, 0, NULL, OPTR_EXEC_MODEL_STREAM,
                               pInserterParams);
  if (code != TSDB_CODE_SUCCESS) {
    qDestroyTask(*pTaskInfo);
    return code;
  }

  return code;
}

typedef struct {
  tb_uid_t tableUid;
  tb_uid_t childUid;
  int8_t   check;
} STqPair;

static int32_t filterUnqualifiedTables(const SStreamScanInfo* pScanInfo, const SArray* tableIdList, const char* idstr,
                                       SStorageAPI* pAPI, SArray** ppArrayRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int8_t  locked = 0;
  SArray* qa = taosArrayInit(4, sizeof(tb_uid_t));

  QUERY_CHECK_NULL(qa, code, lino, _error, terrno);

  SArray* tUid = taosArrayInit(4, sizeof(STqPair));
  QUERY_CHECK_NULL(tUid, code, lino, _error, terrno);

  int32_t numOfUids = taosArrayGetSize(tableIdList);
  if (numOfUids == 0) {
    (*ppArrayRes) = qa;
    goto _error;
  }

  STableScanInfo* pTableScanInfo = pScanInfo->pTableScanOp->info;

  uint64_t suid = 0;
  uint64_t uid = 0;
  int32_t  type = 0;
  tableListGetSourceTableInfo(pTableScanInfo->base.pTableListInfo, &suid, &uid, &type);

  // let's discard the tables those are not created according to the queried super table.
  SMetaReader mr = {0};
  pAPI->metaReaderFn.initReader(&mr, pScanInfo->readHandle.vnode, META_READER_LOCK, &pAPI->metaFn);

  locked = 1;
  for (int32_t i = 0; i < numOfUids; ++i) {
    uint64_t* id = (uint64_t*)taosArrayGet(tableIdList, i);
    QUERY_CHECK_NULL(id, code, lino, _end, terrno);

    int32_t code = pAPI->metaReaderFn.getTableEntryByUid(&mr, *id);
    if (code != TSDB_CODE_SUCCESS) {
      qError("failed to get table meta, uid:%" PRIu64 " code:%s, %s", *id, tstrerror(terrno), idstr);
      continue;
    }

    tDecoderClear(&mr.coder);

    if (mr.me.type == TSDB_SUPER_TABLE) {
      continue;
    } else {
      if (type == TSDB_SUPER_TABLE) {
        // this new created child table does not belong to the scanned super table.
        if (mr.me.type != TSDB_CHILD_TABLE || mr.me.ctbEntry.suid != suid) {
          continue;
        }
      } else {  // ordinary table
        // In case that the scanned target table is an ordinary table. When replay the WAL during restore the vnode, we
        // should check all newly created ordinary table to make sure that this table isn't the destination table.
        if (mr.me.uid != uid) {
          continue;
        }
      }
    }

    STqPair item = {.tableUid = *id, .childUid = mr.me.uid, .check = 0};
    if (pScanInfo->pTagCond != NULL) {
      // tb_uid_t id = mr.me.uid;
      item.check = 1;
    }
    if (taosArrayPush(tUid, &item) == NULL) {
      QUERY_CHECK_NULL(NULL, code, lino, _end, terrno);
    }
  }

  pAPI->metaReaderFn.clearReader(&mr);
  locked = 0;

  for (int32_t j = 0; j < taosArrayGetSize(tUid); ++j) {
    bool     qualified = false;
    STqPair* t = (STqPair*)taosArrayGet(tUid, j);
    if (t == NULL) {
      continue;
    }

    if (t->check == 1) {
      STableKeyInfo info = {.groupId = 0, .uid = t->childUid};
      code = isQualifiedTable(&info, pScanInfo->pTagCond, pScanInfo->readHandle.vnode, &qualified, pAPI);
      if (code != TSDB_CODE_SUCCESS) {
        qError("failed to filter new table, uid:0x%" PRIx64 ", %s", info.uid, idstr);
        continue;
      }

      if (!qualified) {
        continue;
      }
    }

    void* tmp = taosArrayPush(qa, &t->tableUid);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
  }

  // handle multiple partition

_end:

  if (locked) {
    pAPI->metaReaderFn.clearReader(&mr);
  }
  (*ppArrayRes) = qa;
_error:

  taosArrayDestroy(tUid);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t qUpdateTableListForStreamScanner(qTaskInfo_t tinfo, const SArray* tableIdList, bool isAdd) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;
  const char*    id = GET_TASKID(pTaskInfo);
  int32_t        code = 0;

  if (isAdd) {
    qDebug("try to add %d tables id into query list, %s", (int32_t)taosArrayGetSize(tableIdList), id);
  }

  // traverse to the stream scanner node to add this table id
  SOperatorInfo* pInfo = NULL;
  code = extractOperatorInTree(pTaskInfo->pRoot, QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN, id, &pInfo);
  if (code != 0 || pInfo == NULL) {
    return code;
  }

  SStreamScanInfo* pScanInfo = pInfo->info;
  if (pInfo->pTaskInfo->execModel == OPTR_EXEC_MODEL_QUEUE) {  // clear meta cache for subscription if tag is changed
    for (int32_t i = 0; i < taosArrayGetSize(tableIdList); ++i) {
      int64_t*        uid = (int64_t*)taosArrayGet(tableIdList, i);
      STableScanInfo* pTableScanInfo = pScanInfo->pTableScanOp->info;
      taosLRUCacheErase(pTableScanInfo->base.metaCache.pTableMetaEntryCache, uid, LONG_BYTES);
    }
  }

  if (isAdd) {  // add new table id
    SArray* qa = NULL;
    code = filterUnqualifiedTables(pScanInfo, tableIdList, id, &pTaskInfo->storageAPI, &qa);
    if (code != TSDB_CODE_SUCCESS) {
      taosArrayDestroy(qa);
      return code;
    }
    int32_t numOfQualifiedTables = taosArrayGetSize(qa);
    qDebug("%d qualified child tables added into stream scanner, %s", numOfQualifiedTables, id);
    pTaskInfo->storageAPI.tqReaderFn.tqReaderAddTables(pScanInfo->tqReader, qa);

    bool   assignUid = false;
    size_t bufLen = (pScanInfo->pGroupTags != NULL) ? getTableTagsBufLen(pScanInfo->pGroupTags) : 0;
    char*  keyBuf = NULL;
    if (bufLen > 0) {
      assignUid = groupbyTbname(pScanInfo->pGroupTags);
      keyBuf = taosMemoryMalloc(bufLen);
      if (keyBuf == NULL) {
        taosArrayDestroy(qa);
        return terrno;
      }
    }

    STableListInfo* pTableListInfo = ((STableScanInfo*)pScanInfo->pTableScanOp->info)->base.pTableListInfo;
    taosWLockLatch(&pTaskInfo->lock);

    for (int32_t i = 0; i < numOfQualifiedTables; ++i) {
      uint64_t* uid = taosArrayGet(qa, i);
      if (!uid) {
        taosMemoryFree(keyBuf);
        taosArrayDestroy(qa);
        taosWUnLockLatch(&pTaskInfo->lock);
        return terrno;
      }
      STableKeyInfo keyInfo = {.uid = *uid, .groupId = 0};

      if (bufLen > 0) {
        if (assignUid) {
          keyInfo.groupId = keyInfo.uid;
        } else {
          code = getGroupIdFromTagsVal(pScanInfo->readHandle.vnode, keyInfo.uid, pScanInfo->pGroupTags, keyBuf,
                                       &keyInfo.groupId, &pTaskInfo->storageAPI);
          if (code != TSDB_CODE_SUCCESS) {
            taosMemoryFree(keyBuf);
            taosArrayDestroy(qa);
            taosWUnLockLatch(&pTaskInfo->lock);
            return code;
          }
        }
      }

      code = tableListAddTableInfo(pTableListInfo, keyInfo.uid, keyInfo.groupId);
      if (code != TSDB_CODE_SUCCESS) {
        taosMemoryFree(keyBuf);
        taosArrayDestroy(qa);
        taosWUnLockLatch(&pTaskInfo->lock);
        return code;
      }
    }

    taosWUnLockLatch(&pTaskInfo->lock);
    if (keyBuf != NULL) {
      taosMemoryFree(keyBuf);
    }

    taosArrayDestroy(qa);
  } else {  // remove the table id in current list
    qDebug("%d remove child tables from the stream scanner, %s", (int32_t)taosArrayGetSize(tableIdList), id);
    taosWLockLatch(&pTaskInfo->lock);
    pTaskInfo->storageAPI.tqReaderFn.tqReaderRemoveTables(pScanInfo->tqReader, tableIdList);
    taosWUnLockLatch(&pTaskInfo->lock);
  }

  return code;
}

int32_t qGetQueryTableSchemaVersion(qTaskInfo_t tinfo, char* dbName, int32_t dbNameBuffLen, char* tableName,
                                    int32_t tbaleNameBuffLen, int32_t* sversion, int32_t* tversion, int32_t* rversion,
                                    int32_t idx, bool* tbGet) {
  *tbGet = false;

  if (tinfo == NULL || dbName == NULL || tableName == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;

  if (taosArrayGetSize(pTaskInfo->schemaInfos) <= idx) {
    return TSDB_CODE_SUCCESS;
  }

  SSchemaInfo* pSchemaInfo = taosArrayGet(pTaskInfo->schemaInfos, idx);
  if (!pSchemaInfo) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
    return terrno;
  }

  *sversion = pSchemaInfo->sw->version;
  *tversion = pSchemaInfo->tversion;
  *rversion = pSchemaInfo->rversion;
  if (pSchemaInfo->dbname) {
    tstrncpy(dbName, pSchemaInfo->dbname, dbNameBuffLen);
  } else {
    dbName[0] = 0;
  }
  if (pSchemaInfo->tablename) {
    tstrncpy(tableName, pSchemaInfo->tablename, tbaleNameBuffLen);
  } else {
    tableName[0] = 0;
  }

  *tbGet = true;

  return TSDB_CODE_SUCCESS;
}

bool qIsDynamicExecTask(qTaskInfo_t tinfo) { return ((SExecTaskInfo*)tinfo)->dynamicTask; }

void qDestroyOperatorParam(SOperatorParam* pParam) {
  if (NULL == pParam) {
    return;
  }
  freeOperatorParam(pParam, OP_GET_PARAM);
}

void qUpdateOperatorParam(qTaskInfo_t tinfo, void* pParam) {
  TSWAP(pParam, ((SExecTaskInfo*)tinfo)->pOpParam);
  ((SExecTaskInfo*)tinfo)->paramSet = false;
}

int32_t qExecutorInit(void) {
  (void)taosThreadOnce(&initPoolOnce, initRefPool);
  return TSDB_CODE_SUCCESS;
}

int32_t qCreateExecTask(SReadHandle* readHandle, int32_t vgId, uint64_t taskId, SSubplan* pSubplan,
                        qTaskInfo_t* pTaskInfo, DataSinkHandle* handle, int8_t compressResult, char* sql,
                        EOPTR_EXEC_MODEL model) {
  SExecTaskInfo** pTask = (SExecTaskInfo**)pTaskInfo;
  (void)taosThreadOnce(&initPoolOnce, initRefPool);

  qDebug("start to create task, TID:0x%" PRIx64 " QID:0x%" PRIx64 ", vgId:%d", taskId, pSubplan->id.queryId, vgId);

  readHandle->uid = 0;
  int32_t code = createExecTaskInfo(pSubplan, pTask, readHandle, taskId, vgId, sql, model);
  if (code != TSDB_CODE_SUCCESS || NULL == *pTask) {
    qError("failed to createExecTaskInfo, code:%s", tstrerror(code));
    goto _error;
  }

  if (handle) {
    SDataSinkMgtCfg cfg = {.maxDataBlockNum = 500, .maxDataBlockNumPerQuery = 50, .compress = compressResult};
    void*           pSinkManager = NULL;
    code = dsDataSinkMgtInit(&cfg, &(*pTask)->storageAPI, &pSinkManager);
    if (code != TSDB_CODE_SUCCESS) {
      qError("failed to dsDataSinkMgtInit, code:%s, %s", tstrerror(code), (*pTask)->id.str);
      goto _error;
    }

    void* pSinkParam = NULL;
    code = createDataSinkParam(pSubplan->pDataSink, &pSinkParam, (*pTask), readHandle);
    if (code != TSDB_CODE_SUCCESS) {
      qError("failed to createDataSinkParam, vgId:%d, code:%s, %s", vgId, tstrerror(code), (*pTask)->id.str);
      taosMemoryFree(pSinkManager);
      goto _error;
    }

    SDataSinkNode* pSink = NULL;
    if (readHandle->localExec) {
      code = nodesCloneNode((SNode*)pSubplan->pDataSink, (SNode**)&pSink);
      if (code != TSDB_CODE_SUCCESS) {
        qError("failed to nodesCloneNode, srcType:%d, code:%s, %s", nodeType(pSubplan->pDataSink), tstrerror(code),
               (*pTask)->id.str);
        taosMemoryFree(pSinkManager);
        goto _error;
      }
    }

    // pSinkParam has been freed during create sinker.
    code = dsCreateDataSinker(pSinkManager, readHandle->localExec ? &pSink : &pSubplan->pDataSink, handle, pSinkParam,
                              (*pTask)->id.str, pSubplan->processOneBlock);
    if (code) {
      qError("s-task:%s failed to create data sinker, code:%s", (*pTask)->id.str, tstrerror(code));
    }
  }

  qDebug("subplan task create completed, TID:0x%" PRIx64 " QID:0x%" PRIx64 " code:%s", taskId, pSubplan->id.queryId,
         tstrerror(code));

_error:
  // if failed to add ref for all tables in this query, abort current query
  return code;
}

static void freeBlock(void* param) {
  SSDataBlock* pBlock = *(SSDataBlock**)param;
  blockDataDestroy(pBlock);
}

int32_t qExecTaskOpt(qTaskInfo_t tinfo, SArray* pResList, uint64_t* useconds, bool* hasMore, SLocalFetch* pLocal,
                     bool processOneBlock) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;
  int64_t        threadId = taosGetSelfPthreadId();

  if (pLocal) {
    memcpy(&pTaskInfo->localFetch, pLocal, sizeof(*pLocal));
  }

  taosArrayClear(pResList);

  int64_t curOwner = 0;
  if ((curOwner = atomic_val_compare_exchange_64(&pTaskInfo->owner, 0, threadId)) != 0) {
    qError("%s-%p execTask is now executed by thread:%p", GET_TASKID(pTaskInfo), pTaskInfo, (void*)curOwner);
    pTaskInfo->code = TSDB_CODE_QRY_IN_EXEC;
    return pTaskInfo->code;
  }

  if (pTaskInfo->cost.start == 0) {
    pTaskInfo->cost.start = taosGetTimestampUs();
  }

  if (isTaskKilled(pTaskInfo)) {
    atomic_store_64(&pTaskInfo->owner, 0);
    qDebug("%s already killed, abort", GET_TASKID(pTaskInfo));
    return pTaskInfo->code;
  }

  // error occurs, record the error code and return to client
  int32_t ret = setjmp(pTaskInfo->env);
  if (ret != TSDB_CODE_SUCCESS) {
    pTaskInfo->code = ret;
    (void)cleanUpUdfs();

    qDebug("%s task abort due to error/cancel occurs, code:%s", GET_TASKID(pTaskInfo), tstrerror(pTaskInfo->code));
    atomic_store_64(&pTaskInfo->owner, 0);

    return pTaskInfo->code;
  }

  qDebug("%s execTask is launched", GET_TASKID(pTaskInfo));

  int32_t      current = 0;
  SSDataBlock* pRes = NULL;
  int64_t      st = taosGetTimestampUs();

  if (pTaskInfo->pOpParam && !pTaskInfo->paramSet) {
    pTaskInfo->paramSet = true;
    code = pTaskInfo->pRoot->fpSet.getNextExtFn(pTaskInfo->pRoot, pTaskInfo->pOpParam, &pRes);
  } else {
    code = pTaskInfo->pRoot->fpSet.getNextFn(pTaskInfo->pRoot, &pRes);
  }

  QUERY_CHECK_CODE(code, lino, _end);
  code = blockDataCheck(pRes);
  QUERY_CHECK_CODE(code, lino, _end);

  if (pRes == NULL) {
    st = taosGetTimestampUs();
  }

  int32_t rowsThreshold = pTaskInfo->pSubplan->rowsThreshold;
  if (!pTaskInfo->pSubplan->dynamicRowThreshold || 4096 <= pTaskInfo->pSubplan->rowsThreshold) {
    rowsThreshold = 4096;
  }

  int32_t blockIndex = 0;
  while (pRes != NULL) {
    SSDataBlock* p = NULL;
    if (blockIndex >= taosArrayGetSize(pTaskInfo->pResultBlockList)) {
      SSDataBlock* p1 = NULL;
      code = createOneDataBlock(pRes, true, &p1);
      QUERY_CHECK_CODE(code, lino, _end);

      void* tmp = taosArrayPush(pTaskInfo->pResultBlockList, &p1);
      QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
      p = p1;
    } else {
      void* tmp = taosArrayGet(pTaskInfo->pResultBlockList, blockIndex);
      QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);

      p = *(SSDataBlock**)tmp;
      code = copyDataBlock(p, pRes);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    blockIndex += 1;

    current += p->info.rows;
    QUERY_CHECK_CONDITION((p->info.rows > 0 || p->info.type == STREAM_CHECKPOINT), code, lino, _end,
                          TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
    void* tmp = taosArrayPush(pResList, &p);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);

    if (current >= rowsThreshold || processOneBlock) {
      break;
    }

    code = pTaskInfo->pRoot->fpSet.getNextFn(pTaskInfo->pRoot, &pRes);
    QUERY_CHECK_CODE(code, lino, _end);
    code = blockDataCheck(pRes);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (pTaskInfo->pSubplan->dynamicRowThreshold) {
    pTaskInfo->pSubplan->rowsThreshold -= current;
  }

  *hasMore = (pRes != NULL);
  uint64_t el = (taosGetTimestampUs() - st);

  pTaskInfo->cost.elapsedTime += el;
  if (NULL == pRes) {
    *useconds = pTaskInfo->cost.elapsedTime;
  }

_end:
  (void)cleanUpUdfs();

  uint64_t total = pTaskInfo->pRoot->resultInfo.totalRows;
  qDebug("%s task suspended, %d rows in %d blocks returned, total:%" PRId64 " rows, in sinkNode:%d, elapsed:%.2f ms",
         GET_TASKID(pTaskInfo), current, (int32_t)taosArrayGetSize(pResList), total, 0, el / 1000.0);

  atomic_store_64(&pTaskInfo->owner, 0);
  if (code) {
    pTaskInfo->code = code;
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return pTaskInfo->code;
}

void qCleanExecTaskBlockBuf(qTaskInfo_t tinfo) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;
  SArray*        pList = pTaskInfo->pResultBlockList;
  size_t         num = taosArrayGetSize(pList);
  for (int32_t i = 0; i < num; ++i) {
    SSDataBlock** p = taosArrayGet(pTaskInfo->pResultBlockList, i);
    if (p) {
      blockDataDestroy(*p);
    }
  }

  taosArrayClear(pTaskInfo->pResultBlockList);
}

int32_t qExecTask(qTaskInfo_t tinfo, SSDataBlock** pRes, uint64_t* useconds) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;
  int64_t        threadId = taosGetSelfPthreadId();
  int64_t        curOwner = 0;

  *pRes = NULL;

  // todo extract method
  taosRLockLatch(&pTaskInfo->lock);
  bool isKilled = isTaskKilled(pTaskInfo);
  if (isKilled) {
    qDebug("%s already killed, abort", GET_TASKID(pTaskInfo));

    taosRUnLockLatch(&pTaskInfo->lock);
    return pTaskInfo->code;
  }

  if (pTaskInfo->owner != 0) {
    qError("%s-%p execTask is now executed by thread:%p", GET_TASKID(pTaskInfo), pTaskInfo, (void*)curOwner);
    pTaskInfo->code = TSDB_CODE_QRY_IN_EXEC;

    taosRUnLockLatch(&pTaskInfo->lock);
    return pTaskInfo->code;
  }

  pTaskInfo->owner = threadId;
  taosRUnLockLatch(&pTaskInfo->lock);

  if (pTaskInfo->cost.start == 0) {
    pTaskInfo->cost.start = taosGetTimestampUs();
  }

  // error occurs, record the error code and return to client
  int32_t ret = setjmp(pTaskInfo->env);
  if (ret != TSDB_CODE_SUCCESS) {
    pTaskInfo->code = ret;
    (void)cleanUpUdfs();
    qDebug("%s task abort due to error/cancel occurs, code:%s", GET_TASKID(pTaskInfo), tstrerror(pTaskInfo->code));
    atomic_store_64(&pTaskInfo->owner, 0);
    return pTaskInfo->code;
  }

  qDebug("%s execTask is launched", GET_TASKID(pTaskInfo));

  int64_t st = taosGetTimestampUs();
  int32_t code = TSDB_CODE_SUCCESS;
  if (pTaskInfo->pOpParam && !pTaskInfo->paramSet) {
    pTaskInfo->paramSet = true;
    code = pTaskInfo->pRoot->fpSet.getNextExtFn(pTaskInfo->pRoot, pTaskInfo->pOpParam, pRes);
  } else {
    code = pTaskInfo->pRoot->fpSet.getNextFn(pTaskInfo->pRoot, pRes);
  }
  if (code) {
    pTaskInfo->code = code;
    qError("%s failed at line %d, code:%s %s", __func__, __LINE__, tstrerror(code), GET_TASKID(pTaskInfo));
  }

  code = blockDataCheck(*pRes);
  if (code) {
    pTaskInfo->code = code;
    qError("%s failed at line %d, code:%s %s", __func__, __LINE__, tstrerror(code), GET_TASKID(pTaskInfo));
  }

  uint64_t el = (taosGetTimestampUs() - st);

  pTaskInfo->cost.elapsedTime += el;
  if (NULL == *pRes) {
    *useconds = pTaskInfo->cost.elapsedTime;
  }

  (void)cleanUpUdfs();

  int32_t  current = (*pRes != NULL) ? (*pRes)->info.rows : 0;
  uint64_t total = pTaskInfo->pRoot->resultInfo.totalRows;

  qDebug("%s task suspended, %d rows returned, total:%" PRId64 " rows, in sinkNode:%d, elapsed:%.2f ms",
         GET_TASKID(pTaskInfo), current, total, 0, el / 1000.0);

  atomic_store_64(&pTaskInfo->owner, 0);
  return pTaskInfo->code;
}

int32_t qAppendTaskStopInfo(SExecTaskInfo* pTaskInfo, SExchangeOpStopInfo* pInfo) {
  taosWLockLatch(&pTaskInfo->stopInfo.lock);
  void* tmp = taosArrayPush(pTaskInfo->stopInfo.pStopInfo, pInfo);
  taosWUnLockLatch(&pTaskInfo->stopInfo.lock);

  if (!tmp) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
    return terrno;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t stopInfoComp(void const* lp, void const* rp) {
  SExchangeOpStopInfo* key = (SExchangeOpStopInfo*)lp;
  SExchangeOpStopInfo* pInfo = (SExchangeOpStopInfo*)rp;

  if (key->refId < pInfo->refId) {
    return -1;
  } else if (key->refId > pInfo->refId) {
    return 1;
  }

  return 0;
}

void qRemoveTaskStopInfo(SExecTaskInfo* pTaskInfo, SExchangeOpStopInfo* pInfo) {
  taosWLockLatch(&pTaskInfo->stopInfo.lock);
  int32_t idx = taosArraySearchIdx(pTaskInfo->stopInfo.pStopInfo, pInfo, stopInfoComp, TD_EQ);
  if (idx >= 0) {
    taosArrayRemove(pTaskInfo->stopInfo.pStopInfo, idx);
  }
  taosWUnLockLatch(&pTaskInfo->stopInfo.lock);
}

void qStopTaskOperators(SExecTaskInfo* pTaskInfo) {
  taosWLockLatch(&pTaskInfo->stopInfo.lock);

  int32_t num = taosArrayGetSize(pTaskInfo->stopInfo.pStopInfo);
  for (int32_t i = 0; i < num; ++i) {
    SExchangeOpStopInfo* pStop = taosArrayGet(pTaskInfo->stopInfo.pStopInfo, i);
    if (!pStop) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
      continue;
    }
    SExchangeInfo* pExchangeInfo = taosAcquireRef(exchangeObjRefPool, pStop->refId);
    if (pExchangeInfo) {
      int32_t code = tsem_post(&pExchangeInfo->ready);
      if (code != TSDB_CODE_SUCCESS) {
        qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      } else {
        qDebug("post to exchange %" PRId64 " to stop", pStop->refId);
      }
      code = taosReleaseRef(exchangeObjRefPool, pStop->refId);
      if (code != TSDB_CODE_SUCCESS) {
        qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      }
    }
  }

  taosWUnLockLatch(&pTaskInfo->stopInfo.lock);
}

int32_t qAsyncKillTask(qTaskInfo_t qinfo, int32_t rspCode) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)qinfo;
  if (pTaskInfo == NULL) {
    return TSDB_CODE_QRY_INVALID_QHANDLE;
  }

  qDebug("%s execTask async killed", GET_TASKID(pTaskInfo));

  setTaskKilled(pTaskInfo, rspCode);
  qStopTaskOperators(pTaskInfo);

  return TSDB_CODE_SUCCESS;
}

int32_t qKillTask(qTaskInfo_t tinfo, int32_t rspCode, int64_t waitDuration) {
  int64_t        st = taosGetTimestampMs();
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;
  if (pTaskInfo == NULL) {
    return TSDB_CODE_QRY_INVALID_QHANDLE;
  }

  if (waitDuration > 0) {
    qDebug("%s sync killed execTask, and waiting for at most %.2fs", GET_TASKID(pTaskInfo), waitDuration / 1000.0);
  } else {
    qDebug("%s async killed execTask", GET_TASKID(pTaskInfo));
  }

  setTaskKilled(pTaskInfo, TSDB_CODE_TSC_QUERY_KILLED);

  if (waitDuration > 0) {
    while (1) {
      taosWLockLatch(&pTaskInfo->lock);
      if (qTaskIsExecuting(pTaskInfo)) {  // let's wait for 100 ms and try again
        taosWUnLockLatch(&pTaskInfo->lock);

        taosMsleep(200);

        int64_t d = taosGetTimestampMs() - st;
        if (d >= waitDuration && waitDuration >= 0) {
          qWarn("%s waiting more than %.2fs, not wait anymore", GET_TASKID(pTaskInfo), waitDuration / 1000.0);
          return TSDB_CODE_SUCCESS;
        }
      } else {  // not running now
        pTaskInfo->code = rspCode;
        taosWUnLockLatch(&pTaskInfo->lock);
        return TSDB_CODE_SUCCESS;
      }
    }
  }

  int64_t et = taosGetTimestampMs() - st;
  if (et < waitDuration) {
    qInfo("%s  waiting %.2fs for executor stopping", GET_TASKID(pTaskInfo), et / 1000.0);
    return TSDB_CODE_SUCCESS;
  }
  return TSDB_CODE_SUCCESS;
}

bool qTaskIsExecuting(qTaskInfo_t qinfo) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)qinfo;
  if (NULL == pTaskInfo) {
    return false;
  }

  return 0 != atomic_load_64(&pTaskInfo->owner);
}

static void printTaskExecCostInLog(SExecTaskInfo* pTaskInfo) {
  STaskCostInfo* pSummary = &pTaskInfo->cost;
  int64_t        idleTime = pSummary->start - pSummary->created;

  SFileBlockLoadRecorder* pRecorder = pSummary->pRecoder;
  if (pSummary->pRecoder != NULL) {
    qDebug(
        "%s :cost summary: idle:%.2f ms, elapsed time:%.2f ms, extract tableList:%.2f ms, "
        "createGroupIdMap:%.2f ms, total blocks:%d, "
        "load block SMA:%d, load data block:%d, total rows:%" PRId64 ", check rows:%" PRId64,
        GET_TASKID(pTaskInfo), idleTime / 1000.0, pSummary->elapsedTime / 1000.0, pSummary->extractListTime,
        pSummary->groupIdMapTime, pRecorder->totalBlocks, pRecorder->loadBlockStatis, pRecorder->loadBlocks,
        pRecorder->totalRows, pRecorder->totalCheckedRows);
  } else {
    qDebug("%s :cost summary: idle in queue:%.2f ms, elapsed time:%.2f ms", GET_TASKID(pTaskInfo), idleTime / 1000.0,
           pSummary->elapsedTime / 1000.0);
  }
}

void qDestroyTask(qTaskInfo_t qTaskHandle) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)qTaskHandle;
  if (pTaskInfo == NULL) {
    return;
  }

  if (pTaskInfo->pRoot != NULL) {
    qDebug("%s execTask completed, numOfRows:%" PRId64, GET_TASKID(pTaskInfo), pTaskInfo->pRoot->resultInfo.totalRows);
  } else {
    qDebug("%s execTask completed", GET_TASKID(pTaskInfo));
  }

  printTaskExecCostInLog(pTaskInfo);  // print the query cost summary
  doDestroyTask(pTaskInfo);
}

int32_t qGetExplainExecInfo(qTaskInfo_t tinfo, SArray* pExecInfoList) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;
  return getOperatorExplainExecInfo(pTaskInfo->pRoot, pExecInfoList);
}

void qExtractTmqScanner(qTaskInfo_t tinfo, void** scanner) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;
  SOperatorInfo* pOperator = pTaskInfo->pRoot;

  while (1) {
    uint16_t type = pOperator->operatorType;
    if (type == QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN) {
      *scanner = pOperator->info;
      break;
    } else {
      pOperator = pOperator->pDownstream[0];
    }
  }
}

static int32_t getOpratorIntervalInfo(SOperatorInfo* pOperator, int64_t* pWaterMark, SInterval* pInterval,
                                      STimeWindow* pLastWindow, TSKEY* pRecInteral) {
  if (pOperator->operatorType != QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN) {
    return getOpratorIntervalInfo(pOperator->pDownstream[0], pWaterMark, pInterval, pLastWindow, pRecInteral);
  }
  SStreamScanInfo* pScanOp = (SStreamScanInfo*)pOperator->info;
  *pWaterMark = pScanOp->twAggSup.waterMark;
  *pInterval = pScanOp->interval;
  *pLastWindow = pScanOp->lastScanRange;
  *pRecInteral = pScanOp->recalculateInterval;
  return TSDB_CODE_SUCCESS;
}

void* qExtractReaderFromTmqScanner(void* scanner) {
  SStreamScanInfo* pInfo = scanner;
  return (void*)pInfo->tqReader;
}

const SSchemaWrapper* qExtractSchemaFromTask(qTaskInfo_t tinfo) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;
  return pTaskInfo->streamInfo.schema;
}

const char* qExtractTbnameFromTask(qTaskInfo_t tinfo) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;
  return pTaskInfo->streamInfo.tbName;
}

SMqBatchMetaRsp* qStreamExtractMetaMsg(qTaskInfo_t tinfo) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;
  return &pTaskInfo->streamInfo.btMetaRsp;
}

int32_t qStreamExtractOffset(qTaskInfo_t tinfo, STqOffsetVal* pOffset) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;
  tOffsetCopy(pOffset, &pTaskInfo->streamInfo.currentOffset);
  return 0;
  /*if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }*/
}

int32_t initQueryTableDataCondForTmq(SQueryTableDataCond* pCond, SSnapContext* sContext, SMetaTableInfo* pMtInfo) {
  memset(pCond, 0, sizeof(SQueryTableDataCond));
  pCond->order = TSDB_ORDER_ASC;
  pCond->numOfCols = pMtInfo->schema->nCols;
  pCond->colList = taosMemoryCalloc(pCond->numOfCols, sizeof(SColumnInfo));
  pCond->pSlotList = taosMemoryMalloc(sizeof(int32_t) * pCond->numOfCols);
  if (pCond->colList == NULL || pCond->pSlotList == NULL) {
    taosMemoryFreeClear(pCond->colList);
    taosMemoryFreeClear(pCond->pSlotList);
    return terrno;
  }

  TAOS_SET_OBJ_ALIGNED(&pCond->twindows, TSWINDOW_INITIALIZER);
  pCond->suid = pMtInfo->suid;
  pCond->type = TIMEWINDOW_RANGE_CONTAINED;
  pCond->startVersion = -1;
  pCond->endVersion = sContext->snapVersion;

  for (int32_t i = 0; i < pCond->numOfCols; ++i) {
    SColumnInfo* pColInfo = &pCond->colList[i];
    pColInfo->type = pMtInfo->schema->pSchema[i].type;
    pColInfo->bytes = pMtInfo->schema->pSchema[i].bytes;
    if (pMtInfo->pExtSchemas != NULL) {
      decimalFromTypeMod(pMtInfo->pExtSchemas[i].typeMod, &pColInfo->precision, &pColInfo->scale);
    }
    pColInfo->colId = pMtInfo->schema->pSchema[i].colId;
    pColInfo->pk = pMtInfo->schema->pSchema[i].flags & COL_IS_KEY;

    pCond->pSlotList[i] = i;
  }

  return TSDB_CODE_SUCCESS;
}

void qStreamSetOpen(qTaskInfo_t tinfo) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;
  SOperatorInfo* pOperator = pTaskInfo->pRoot;
  pOperator->status = OP_NOT_OPENED;
}

void qStreamSetSourceExcluded(qTaskInfo_t tinfo, int8_t sourceExcluded) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;
  pTaskInfo->streamInfo.sourceExcluded = sourceExcluded;
}

int32_t qStreamPrepareScan(qTaskInfo_t tinfo, STqOffsetVal* pOffset, int8_t subType) {
  int32_t        code = TSDB_CODE_SUCCESS;
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;
  SStorageAPI*   pAPI = &pTaskInfo->storageAPI;

  SOperatorInfo* pOperator = pTaskInfo->pRoot;
  const char*    id = GET_TASKID(pTaskInfo);

  if (subType == TOPIC_SUB_TYPE__COLUMN && pOffset->type == TMQ_OFFSET__LOG) {
    code = extractOperatorInTree(pOperator, QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN, id, &pOperator);
    if (pOperator == NULL || code != 0) {
      return code;
    }

    SStreamScanInfo* pInfo = pOperator->info;
    SStoreTqReader*  pReaderAPI = &pTaskInfo->storageAPI.tqReaderFn;
    SWalReader*      pWalReader = pReaderAPI->tqReaderGetWalReader(pInfo->tqReader);
    walReaderVerifyOffset(pWalReader, pOffset);
  }
  // if pOffset equal to current offset, means continue consume
  if (tOffsetEqual(pOffset, &pTaskInfo->streamInfo.currentOffset)) {
    return 0;
  }

  if (subType == TOPIC_SUB_TYPE__COLUMN) {
    code = extractOperatorInTree(pOperator, QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN, id, &pOperator);
    if (pOperator == NULL || code != 0) {
      return code;
    }

    SStreamScanInfo* pInfo = pOperator->info;
    STableScanInfo*  pScanInfo = pInfo->pTableScanOp->info;
    STableScanBase*  pScanBaseInfo = &pScanInfo->base;
    STableListInfo*  pTableListInfo = pScanBaseInfo->pTableListInfo;

    if (pOffset->type == TMQ_OFFSET__LOG) {
      pTaskInfo->storageAPI.tsdReader.tsdReaderClose(pScanBaseInfo->dataReader);
      pScanBaseInfo->dataReader = NULL;

      SStoreTqReader* pReaderAPI = &pTaskInfo->storageAPI.tqReaderFn;
      SWalReader*     pWalReader = pReaderAPI->tqReaderGetWalReader(pInfo->tqReader);
      walReaderVerifyOffset(pWalReader, pOffset);
      code = pReaderAPI->tqReaderSeek(pInfo->tqReader, pOffset->version, id);
      if (code < 0) {
        qError("tqReaderSeek failed ver:%" PRId64 ", %s", pOffset->version, id);
        return code;
      }
    } else if (pOffset->type == TMQ_OFFSET__SNAPSHOT_DATA) {
      // iterate all tables from tableInfoList, and retrieve rows from each table one-by-one
      // those data are from the snapshot in tsdb, besides the data in the wal file.
      int64_t uid = pOffset->uid;
      int64_t ts = pOffset->ts;
      int32_t index = 0;

      // this value may be changed if new tables are created
      taosRLockLatch(&pTaskInfo->lock);
      int32_t numOfTables = 0;
      code = tableListGetSize(pTableListInfo, &numOfTables);
      if (code != TSDB_CODE_SUCCESS) {
        qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
        taosRUnLockLatch(&pTaskInfo->lock);
        return code;
      }

      if (uid == 0) {
        if (numOfTables != 0) {
          STableKeyInfo* tmp = tableListGetInfo(pTableListInfo, 0);
          if (!tmp) {
            qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
            taosRUnLockLatch(&pTaskInfo->lock);
            return terrno;
          }
          if (tmp) uid = tmp->uid;
          ts = INT64_MIN;
          pScanInfo->currentTable = 0;
        } else {
          taosRUnLockLatch(&pTaskInfo->lock);
          qError("no table in table list, %s", id);
          return TSDB_CODE_TMQ_NO_TABLE_QUALIFIED;
        }
      }
      pTaskInfo->storageAPI.tqReaderFn.tqSetTablePrimaryKey(pInfo->tqReader, uid);

      qDebug("switch to table uid:%" PRId64 " ts:%" PRId64 "% " PRId64 " rows returned", uid, ts,
             pInfo->pTableScanOp->resultInfo.totalRows);
      pInfo->pTableScanOp->resultInfo.totalRows = 0;

      // start from current accessed position
      // we cannot start from the pScanInfo->currentTable, since the commit offset may cause the rollback of the start
      // position, let's find it from the beginning.
      index = tableListFind(pTableListInfo, uid, 0);
      taosRUnLockLatch(&pTaskInfo->lock);

      if (index >= 0) {
        pScanInfo->currentTable = index;
      } else {
        qError("vgId:%d uid:%" PRIu64 " not found in table list, total:%d, index:%d %s", pTaskInfo->id.vgId, uid,
               numOfTables, pScanInfo->currentTable, id);
        return TSDB_CODE_TMQ_NO_TABLE_QUALIFIED;
      }

      STableKeyInfo keyInfo = {.uid = uid};
      int64_t       oldSkey = pScanBaseInfo->cond.twindows.skey;

      // let's start from the next ts that returned to consumer.
      if (pTaskInfo->storageAPI.tqReaderFn.tqGetTablePrimaryKey(pInfo->tqReader)) {
        pScanBaseInfo->cond.twindows.skey = ts;
      } else {
        pScanBaseInfo->cond.twindows.skey = ts + 1;
      }
      pScanInfo->scanTimes = 0;

      if (pScanBaseInfo->dataReader == NULL) {
        code = pTaskInfo->storageAPI.tsdReader.tsdReaderOpen(pScanBaseInfo->readHandle.vnode, &pScanBaseInfo->cond,
                                                             &keyInfo, 1, pScanInfo->pResBlock,
                                                             (void**)&pScanBaseInfo->dataReader, id, NULL);
        if (code != TSDB_CODE_SUCCESS) {
          qError("prepare read tsdb snapshot failed, uid:%" PRId64 ", code:%s %s", pOffset->uid, tstrerror(code), id);
          return code;
        }

        qDebug("tsdb reader created with offset(snapshot) uid:%" PRId64 " ts:%" PRId64 " table index:%d, total:%d, %s",
               uid, pScanBaseInfo->cond.twindows.skey, pScanInfo->currentTable, numOfTables, id);
      } else {
        code = pTaskInfo->storageAPI.tsdReader.tsdSetQueryTableList(pScanBaseInfo->dataReader, &keyInfo, 1);
        if (code != TSDB_CODE_SUCCESS) {
          qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
          return code;
        }

        code = pTaskInfo->storageAPI.tsdReader.tsdReaderResetStatus(pScanBaseInfo->dataReader, &pScanBaseInfo->cond);
        if (code != TSDB_CODE_SUCCESS) {
          qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
          return code;
        }
        qDebug("tsdb reader offset seek snapshot to uid:%" PRId64 " ts %" PRId64 "  table index:%d numOfTable:%d, %s",
               uid, pScanBaseInfo->cond.twindows.skey, pScanInfo->currentTable, numOfTables, id);
      }

      // restore the key value
      pScanBaseInfo->cond.twindows.skey = oldSkey;
    } else {
      qError("invalid pOffset->type:%d, %s", pOffset->type, id);
      return TSDB_CODE_PAR_INTERNAL_ERROR;
    }

  } else {  // subType == TOPIC_SUB_TYPE__TABLE/TOPIC_SUB_TYPE__DB
    if (pOffset->type == TMQ_OFFSET__SNAPSHOT_DATA) {
      SStreamRawScanInfo* pInfo = pOperator->info;
      SSnapContext*       sContext = pInfo->sContext;
      SOperatorInfo*      p = NULL;

      code = extractOperatorInTree(pOperator, QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, id, &p);
      if (code != 0) {
        return code;
      }

      STableListInfo* pTableListInfo = ((SStreamRawScanInfo*)(p->info))->pTableListInfo;

      if (pAPI->snapshotFn.setForSnapShot(sContext, pOffset->uid) != 0) {
        qError("setDataForSnapShot error. uid:%" PRId64 " , %s", pOffset->uid, id);
        return TSDB_CODE_PAR_INTERNAL_ERROR;
      }

      SMetaTableInfo mtInfo = {0};
      code = pTaskInfo->storageAPI.snapshotFn.getMetaTableInfoFromSnapshot(sContext, &mtInfo);
      if (code != 0) {
        destroyMetaTableInfo(&mtInfo);
        return code;
      }
      pTaskInfo->storageAPI.tsdReader.tsdReaderClose(pInfo->dataReader);
      pInfo->dataReader = NULL;

      cleanupQueryTableDataCond(&pTaskInfo->streamInfo.tableCond);
      tableListClear(pTableListInfo);

      if (mtInfo.uid == 0) {
        destroyMetaTableInfo(&mtInfo);
        goto end;  // no data
      }

      pAPI->snapshotFn.taosXSetTablePrimaryKey(sContext, mtInfo.uid);
      code = initQueryTableDataCondForTmq(&pTaskInfo->streamInfo.tableCond, sContext, &mtInfo);
      if (code != TSDB_CODE_SUCCESS) {
        qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
        destroyMetaTableInfo(&mtInfo);
        return code;
      }
      if (pAPI->snapshotFn.taosXGetTablePrimaryKey(sContext)) {
        pTaskInfo->streamInfo.tableCond.twindows.skey = pOffset->ts;
      } else {
        pTaskInfo->streamInfo.tableCond.twindows.skey = pOffset->ts + 1;
      }

      code = tableListAddTableInfo(pTableListInfo, mtInfo.uid, 0);
      if (code != TSDB_CODE_SUCCESS) {
        destroyMetaTableInfo(&mtInfo);
        qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
        return code;
      }

      STableKeyInfo* pList = tableListGetInfo(pTableListInfo, 0);
      if (!pList) {
        qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
        destroyMetaTableInfo(&mtInfo);
        return code;
      }
      int32_t size = 0;
      code = tableListGetSize(pTableListInfo, &size);
      if (code != TSDB_CODE_SUCCESS) {
        qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
        destroyMetaTableInfo(&mtInfo);
        return code;
      }

      code = pTaskInfo->storageAPI.tsdReader.tsdReaderOpen(pInfo->vnode, &pTaskInfo->streamInfo.tableCond, pList, size,
                                                           NULL, (void**)&pInfo->dataReader, NULL, NULL);
      if (code != TSDB_CODE_SUCCESS) {
        qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
        destroyMetaTableInfo(&mtInfo);
        return code;
      }

      cleanupQueryTableDataCond(&pTaskInfo->streamInfo.tableCond);
      tstrncpy(pTaskInfo->streamInfo.tbName, mtInfo.tbName, TSDB_TABLE_NAME_LEN);
      //      pTaskInfo->streamInfo.suid = mtInfo.suid == 0 ? mtInfo.uid : mtInfo.suid;
      tDeleteSchemaWrapper(pTaskInfo->streamInfo.schema);
      pTaskInfo->streamInfo.schema = mtInfo.schema;
      taosMemoryFreeClear(mtInfo.pExtSchemas);

      qDebug("tmqsnap qStreamPrepareScan snapshot data uid:%" PRId64 " ts %" PRId64 " %s", mtInfo.uid, pOffset->ts, id);
    } else if (pOffset->type == TMQ_OFFSET__SNAPSHOT_META) {
      SStreamRawScanInfo* pInfo = pOperator->info;
      SSnapContext*       sContext = pInfo->sContext;
      code = pTaskInfo->storageAPI.snapshotFn.setForSnapShot(sContext, pOffset->uid);
      if (code != 0) {
        qError("setForSnapShot error. uid:%" PRIu64 " ,version:%" PRId64, pOffset->uid, pOffset->version);
        return code;
      }
      qDebug("tmqsnap qStreamPrepareScan snapshot meta uid:%" PRId64 " ts %" PRId64 " %s", pOffset->uid, pOffset->ts,
             id);
    } else if (pOffset->type == TMQ_OFFSET__LOG) {
      SStreamRawScanInfo* pInfo = pOperator->info;
      pTaskInfo->storageAPI.tsdReader.tsdReaderClose(pInfo->dataReader);
      pInfo->dataReader = NULL;
      qDebug("tmqsnap qStreamPrepareScan snapshot log, %s", id);
    }
  }

end:
  tOffsetCopy(&pTaskInfo->streamInfo.currentOffset, pOffset);
  return 0;
}

void qProcessRspMsg(void* parent, SRpcMsg* pMsg, SEpSet* pEpSet) {
  SMsgSendInfo* pSendInfo = (SMsgSendInfo*)pMsg->info.ahandle;
  if (pMsg->info.ahandle == NULL) {
    qError("pMsg->info.ahandle is NULL");
    return;
  }

  qDebug("rsp msg got, code:%x, len:%d, 0x%" PRIx64 ":0x%" PRIx64, 
      pMsg->code, pMsg->contLen, TRACE_GET_ROOTID(&pMsg->info.traceId), TRACE_GET_MSGID(&pMsg->info.traceId));

  SDataBuf buf = {.len = pMsg->contLen, .pData = NULL};

  if (pMsg->contLen > 0) {
    buf.pData = taosMemoryCalloc(1, pMsg->contLen);
    if (buf.pData == NULL) {
      pMsg->code = terrno;
    } else {
      memcpy(buf.pData, pMsg->pCont, pMsg->contLen);
    }
  }

  (void)pSendInfo->fp(pSendInfo->param, &buf, pMsg->code);
  rpcFreeCont(pMsg->pCont);
  destroySendMsgInfo(pSendInfo);
}

SArray* qGetQueriedTableListInfo(qTaskInfo_t tinfo) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SExecTaskInfo* pTaskInfo = tinfo;
  SArray*        plist = NULL;

  code = getTableListInfo(pTaskInfo, &plist);
  if (code || plist == NULL) {
    return NULL;
  }

  // only extract table in the first elements
  STableListInfo* pTableListInfo = taosArrayGetP(plist, 0);

  SArray* pUidList = taosArrayInit(10, sizeof(uint64_t));
  QUERY_CHECK_NULL(pUidList, code, lino, _end, terrno);

  int32_t numOfTables = 0;
  code = tableListGetSize(pTableListInfo, &numOfTables);
  QUERY_CHECK_CODE(code, lino, _end);

  for (int32_t i = 0; i < numOfTables; ++i) {
    STableKeyInfo* pKeyInfo = tableListGetInfo(pTableListInfo, i);
    QUERY_CHECK_NULL(pKeyInfo, code, lino, _end, terrno);
    void* tmp = taosArrayPush(pUidList, &pKeyInfo->uid);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
  }

  taosArrayDestroy(plist);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return pUidList;
}

static int32_t extractTableList(SArray* pList, const SOperatorInfo* pOperator) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  if (pOperator->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN) {
    SStreamScanInfo* pScanInfo = pOperator->info;
    STableScanInfo*  pTableScanInfo = pScanInfo->pTableScanOp->info;

    void* tmp = taosArrayPush(pList, &pTableScanInfo->base.pTableListInfo);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
  } else if (pOperator->operatorType == QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN) {
    STableScanInfo* pScanInfo = pOperator->info;

    void* tmp = taosArrayPush(pList, &pScanInfo->base.pTableListInfo);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
  } else {
    if (pOperator->pDownstream != NULL && pOperator->pDownstream[0] != NULL) {
      code = extractTableList(pList, pOperator->pDownstream[0]);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s %s failed at line %d since %s", pTaskInfo->id.str, __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t getTableListInfo(const SExecTaskInfo* pTaskInfo, SArray** pList) {
  if (pList == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  *pList = NULL;
  SArray* pArray = taosArrayInit(0, POINTER_BYTES);
  if (pArray == NULL) {
    return terrno;
  }

  int32_t code = extractTableList(pArray, pTaskInfo->pRoot);
  if (code == 0) {
    *pList = pArray;
  } else {
    taosArrayDestroy(pArray);
  }
  return code;
}

int32_t qStreamOperatorReleaseState(qTaskInfo_t tInfo) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tInfo;
  if (pTaskInfo->pRoot->fpSet.releaseStreamStateFn != NULL) {
    pTaskInfo->pRoot->fpSet.releaseStreamStateFn(pTaskInfo->pRoot);
  }
  return 0;
}

int32_t qStreamOperatorReloadState(qTaskInfo_t tInfo) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tInfo;
  if (pTaskInfo->pRoot->fpSet.reloadStreamStateFn != NULL) {
    pTaskInfo->pRoot->fpSet.reloadStreamStateFn(pTaskInfo->pRoot);
  }
  return 0;
}

void qResetTaskCode(qTaskInfo_t tinfo) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;

  int32_t code = pTaskInfo->code;
  pTaskInfo->code = 0;
  qDebug("0x%" PRIx64 " reset task code to be success, prev:%s", pTaskInfo->id.taskId, tstrerror(code));
}

int32_t collectExprsToReplaceForStream(SOperatorInfo* pOper, SArray* pExprs) {
  int32_t code = 0;
  return code;
}

int32_t streamCollectExprsForReplace(qTaskInfo_t tInfo, SArray* pExprs) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tInfo;
  int32_t        code = collectExprsToReplaceForStream(pTaskInfo->pRoot, pExprs);
  return code;
}

int32_t clearStatesForOperator(SOperatorInfo* pOper) {
  int32_t code = 0;

  freeResetOperatorParams(pOper, OP_GET_PARAM, true);
  freeResetOperatorParams(pOper, OP_NOTIFY_PARAM, true);

  if (pOper->fpSet.resetStateFn) {
    code = pOper->fpSet.resetStateFn(pOper);
  }
  pOper->status = OP_NOT_OPENED;
  for (int32_t i = 0; i < pOper->numOfDownstream && code == 0; ++i) {
    code = clearStatesForOperator(pOper->pDownstream[i]);
  }
  return code;
}

int32_t streamClearStatesForOperators(qTaskInfo_t tInfo) {
  int32_t        code = 0;
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tInfo;
  SOperatorInfo* pOper = pTaskInfo->pRoot;
  pTaskInfo->code = TSDB_CODE_SUCCESS;
  code = clearStatesForOperator(pOper);
  return code;
}

int32_t streamExecuteTask(qTaskInfo_t tInfo, SSDataBlock** ppRes, uint64_t* useconds, bool* finished) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tInfo;
  int64_t        threadId = taosGetSelfPthreadId();
  int64_t        curOwner = 0;

  *ppRes = NULL;

  // todo extract method
  taosRLockLatch(&pTaskInfo->lock);
  bool isKilled = isTaskKilled(pTaskInfo);
  if (isKilled) {
    // clearStreamBlock(pTaskInfo->pRoot);
    qDebug("%s already killed, abort", GET_TASKID(pTaskInfo));

    taosRUnLockLatch(&pTaskInfo->lock);
    return pTaskInfo->code;
  }

  if (pTaskInfo->owner != 0) {
    qError("%s-%p execTask is now executed by thread:%p", GET_TASKID(pTaskInfo), pTaskInfo, (void*)curOwner);
    pTaskInfo->code = TSDB_CODE_QRY_IN_EXEC;

    taosRUnLockLatch(&pTaskInfo->lock);
    return pTaskInfo->code;
  }

  pTaskInfo->owner = threadId;
  taosRUnLockLatch(&pTaskInfo->lock);

  if (pTaskInfo->cost.start == 0) {
    pTaskInfo->cost.start = taosGetTimestampUs();
  }

  // error occurs, record the error code and return to client
  int32_t ret = setjmp(pTaskInfo->env);
  if (ret != TSDB_CODE_SUCCESS) {
    pTaskInfo->code = ret;
    (void)cleanUpUdfs();
    qDebug("%s task abort due to error/cancel occurs, code:%s", GET_TASKID(pTaskInfo), tstrerror(pTaskInfo->code));
    atomic_store_64(&pTaskInfo->owner, 0);
    return pTaskInfo->code;
  }

  qDebug("%s execTask is launched", GET_TASKID(pTaskInfo));

  int64_t st = taosGetTimestampUs();

  int32_t code = pTaskInfo->pRoot->fpSet.getNextFn(pTaskInfo->pRoot, ppRes);
  if (code) {
    pTaskInfo->code = code;
    qError("%s failed at line %d, code:%s %s", __func__, __LINE__, tstrerror(code), GET_TASKID(pTaskInfo));
  } else {
    *finished = *ppRes == NULL;
    code = blockDataCheck(*ppRes);
  }
  if (code) {
    pTaskInfo->code = code;
    qError("%s failed at line %d, code:%s %s", __func__, __LINE__, tstrerror(code), GET_TASKID(pTaskInfo));
  }

  uint64_t el = (taosGetTimestampUs() - st);

  pTaskInfo->cost.elapsedTime += el;
  if (NULL == *ppRes) {
    *useconds = pTaskInfo->cost.elapsedTime;
  }

  (void)cleanUpUdfs();

  int32_t  current = (*ppRes != NULL) ? (*ppRes)->info.rows : 0;
  uint64_t total = pTaskInfo->pRoot->resultInfo.totalRows;

  qDebug("%s task suspended, %d rows returned, total:%" PRId64 " rows, in sinkNode:%d, elapsed:%.2f ms",
         GET_TASKID(pTaskInfo), current, total, 0, el / 1000.0);

  atomic_store_64(&pTaskInfo->owner, 0);
  return pTaskInfo->code;
}

// void streamSetTaskRuntimeInfo(qTaskInfo_t tinfo, SStreamRuntimeInfo* pStreamRuntimeInfo) {
//   SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;
//   pTaskInfo->pStreamRuntimeInfo = pStreamRuntimeInfo;
// }

int32_t qStreamCreateTableListForReader(void* pVnode, uint64_t suid, uint64_t uid, int8_t tableType,
                                        SNodeList* pGroupTags, bool groupSort, SNode* pTagCond, SNode* pTagIndexCond,
                                        SStorageAPI* storageAPI, void** pTableListInfo, SHashObj* groupIdMap) {
  int32_t code = 0;                                        
  if (*pTableListInfo != NULL) {
    qDebug("table list already exists, no need to create again");
    goto end;
  }
  STableListInfo* pList = tableListCreate();
  if (pList == NULL) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
    code = terrno;
    goto end;
  }

  SScanPhysiNode pScanNode = {.suid = suid, .uid = uid, .tableType = tableType};
  SReadHandle    pHandle = {.vnode = pVnode};
  SExecTaskInfo  pTaskInfo = {.id.str = "", .storageAPI = *storageAPI};

  code = createScanTableListInfo(&pScanNode, pGroupTags, groupSort, &pHandle, pList, pTagCond, pTagIndexCond, &pTaskInfo, groupIdMap);
  if (code != 0) {
    tableListDestroy(pList);
    qError("failed to createScanTableListInfo, code:%s", tstrerror(code));
    goto end;
  }
  *pTableListInfo = pList;

end:
  return 0;
}

static int32_t doFilterTableByTagCond(void* pVnode, void* pListInfo, SArray* pUidList, SNode* pTagCond, SStorageAPI* pStorageAPI){
  bool   listAdded = false;
  int32_t code = doFilterByTagCond(pListInfo, pUidList, pTagCond, pVnode, SFLT_NOT_INDEX, pStorageAPI, true, &listAdded, NULL);
  if (code == 0 && !listAdded) {
    int32_t numOfTables = taosArrayGetSize(pUidList);
    for (int i = 0; i < numOfTables; i++) {
      void* tmp = taosArrayGet(pUidList, i);
      if (tmp == NULL) {
        return terrno;
      }
      STableKeyInfo info = {.uid = *(uint64_t*)tmp, .groupId = 0};

      void* p = taosArrayPush(((STableListInfo*)pListInfo)->pTableList, &info);
      if (p == NULL) {
        return terrno;
      }
    }
  }
  return code;
}

int32_t qStreamFilterTableListForReader(void* pVnode, SArray* uidList,
                                        SNodeList* pGroupTags, SNode* pTagCond, SNode* pTagIndexCond,
                                        SStorageAPI* storageAPI, SHashObj* groupIdMap, uint64_t suid, SArray** tableList) {
  int32_t code = TSDB_CODE_SUCCESS;
  STableListInfo* pList = tableListCreate();
  if (pList == NULL) {
    code = terrno;
    goto end;
  }
  SArray* uidListCopy = taosArrayDup(uidList, NULL);
  if (uidListCopy == NULL) {
    code = terrno;
    goto end;
  }
  SScanPhysiNode pScanNode = {.suid = suid, .tableType = TD_SUPER_TABLE};
  SReadHandle    pHandle = {.vnode = pVnode};

  pList->idInfo.suid = suid;
  pList->idInfo.tableType = TD_SUPER_TABLE;
  code = doFilterTableByTagCond(pVnode, pList, uidList, pTagCond, storageAPI);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }                                              
  code = buildGroupIdMapForAllTables(pList, &pHandle, &pScanNode, pGroupTags, false, NULL, storageAPI, groupIdMap);
  if (code != TSDB_CODE_SUCCESS) {
    goto end;
  }
  *tableList = pList->pTableList;
  pList->pTableList = NULL;

  taosArrayClear(uidList);
  for (int32_t i = 0; i < taosArrayGetSize(uidListCopy); i++){
    void* tmp = taosArrayGet(uidListCopy, i);
    if (tmp == NULL) {
      continue;
    }
    int32_t* slot = taosHashGet(pList->map, tmp, LONG_BYTES);
    if (slot == NULL) {
      if (taosArrayPush(uidList, tmp) == NULL) {
        code = terrno;
        goto end;
      }
    }
  }
end:
  taosArrayDestroy(uidListCopy);
  tableListDestroy(pList);
  return code;
}

// int32_t qStreamGetGroupIndex(void* pTableListInfo, int64_t gid, TdThreadRwlock* lock) {
//   int32_t index = -1;
//   (void)taosThreadRwlockRdlock(lock);
//   if (((STableListInfo*)pTableListInfo)->groupOffset == NULL){
//     index = 0;
//     goto end;
//   }
//   for (int32_t i = 0; i < ((STableListInfo*)pTableListInfo)->numOfOuputGroups; ++i) {
//     int32_t offset = ((STableListInfo*)pTableListInfo)->groupOffset[i];

//     STableKeyInfo* pKeyInfo = taosArrayGet(((STableListInfo*)pTableListInfo)->pTableList, offset);
//     if (pKeyInfo != NULL && pKeyInfo->groupId == gid) {
//       index = i;
//       goto end;
//     }
//   }
// end:
//   (void)taosThreadRwlockUnlock(lock);
//   return index;
// }

void qStreamDestroyTableList(void* pTableListInfo) { tableListDestroy(pTableListInfo); }
SArray*  qStreamGetTableListArray(void* pTableListInfo) {
  STableListInfo* pList = pTableListInfo;
  return pList->pTableList;
}

int32_t qStreamFilter(SSDataBlock* pBlock, void* pFilterInfo, SColumnInfoData** pRet) { return doFilter(pBlock, pFilterInfo, NULL, pRet); }

void streamDestroyExecTask(qTaskInfo_t tInfo) {
  qDebug("streamDestroyExecTask called, task:%p", tInfo);
  qDestroyTask(tInfo);
}

int32_t streamCalcOneScalarExpr(SNode* pExpr, SScalarParam* pDst, const SStreamRuntimeFuncInfo* pExtraParams) {
  return streamCalcOneScalarExprInRange(pExpr, pDst, -1, -1, pExtraParams);
}

int32_t streamCalcOneScalarExprInRange(SNode* pExpr, SScalarParam* pDst, int32_t rowStartIdx, int32_t rowEndIdx,
                                       const SStreamRuntimeFuncInfo* pExtraParams) {
  int32_t      code = 0;
  SNode*       pNode = 0;
  SNodeList*   pList = NULL;
  SExprInfo*   pExprInfo = NULL;
  int32_t      numOfExprs = 1;
  int32_t*     offset = 0;
  STargetNode* pTargetNode = NULL;
  code = nodesMakeNode(QUERY_NODE_TARGET, (SNode**)&pTargetNode);
  if (code == 0) code = nodesCloneNode(pExpr, &pNode);

  if (code == 0) {
    pTargetNode->dataBlockId = 0;
    pTargetNode->pExpr = pNode;
    pTargetNode->slotId = 0;
  }
  if (code == 0) {
    code = nodesMakeList(&pList);
  }
  if (code == 0) {
    code = nodesListAppend(pList, (SNode*)pTargetNode);
  }
  if (code == 0) {
    pNode = NULL;
    code = createExprInfo(pList, NULL, &pExprInfo, &numOfExprs);
  }

  if (code == 0) {
    const char* pVal = NULL;
    int32_t     len = 0;
    SNode*      pSclNode = NULL;
    switch (pExprInfo->pExpr->nodeType) {
      case QUERY_NODE_FUNCTION:
        pSclNode = (SNode*)pExprInfo->pExpr->_function.pFunctNode;
        break;
      case QUERY_NODE_OPERATOR:
        pSclNode = pExprInfo->pExpr->_optrRoot.pRootNode;
        break;
      default:
        code = TSDB_CODE_OPS_NOT_SUPPORT;
        break;
    }
    SArray*     pBlockList = taosArrayInit(2, POINTER_BYTES);
    SSDataBlock block = {0};
    block.info.rows = 1;
    SSDataBlock* pBlock = &block;
    void*        tmp = taosArrayPush(pBlockList, &pBlock);
    if (tmp == NULL) {
      code = terrno;
    }
    if (code == 0) {
      code = scalarCalculateInRange(pSclNode, pBlockList, pDst, rowStartIdx, rowEndIdx, pExtraParams, NULL);
    }
    taosArrayDestroy(pBlockList);
  }
  nodesDestroyList(pList);
  destroyExprInfo(pExprInfo, numOfExprs);
  taosMemoryFreeClear(pExprInfo);
  return code;
}

int32_t streamForceOutput(qTaskInfo_t tInfo, SSDataBlock** pRes, int32_t winIdx) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tInfo;
  const SArray*  pForceOutputCols = pTaskInfo->pStreamRuntimeInfo->pForceOutputCols;
  int32_t        code = 0;
  SNode*         pNode = NULL;
  if (!pForceOutputCols) return 0;
  if (!*pRes) {
    code = createDataBlock(pRes);
  }

  if (code == 0 && (!(*pRes)->pDataBlock || (*pRes)->pDataBlock->size == 0)) {
    int32_t idx = 0;
    for (int32_t i = 0; i < pForceOutputCols->size; ++i) {
      SStreamOutCol*  pCol = (SStreamOutCol*)taosArrayGet(pForceOutputCols, i);
      SColumnInfoData colInfo = createColumnInfoData(pCol->type.type, pCol->type.bytes, idx++);
      colInfo.info.precision = pCol->type.precision;
      colInfo.info.scale = pCol->type.scale;
      code = blockDataAppendColInfo(*pRes, &colInfo);
      if (code != 0) break;
    }
  }

  code = blockDataEnsureCapacity(*pRes, (*pRes)->info.rows + 1);
  if (code != TSDB_CODE_SUCCESS) {
    qError("failed to ensure capacity for force output, code:%s", tstrerror(code));
    return code;
  }

  // loop all exprs for force output, execute all exprs
  int32_t idx = 0;
  int32_t rowIdx = (*pRes)->info.rows;
  int32_t tmpWinIdx = pTaskInfo->pStreamRuntimeInfo->funcInfo.curIdx;
  pTaskInfo->pStreamRuntimeInfo->funcInfo.curIdx = winIdx;
  for (int32_t i = 0; i < pForceOutputCols->size; ++i) {
    SScalarParam   dst = {0};
    SStreamOutCol* pCol = (SStreamOutCol*)taosArrayGet(pForceOutputCols, i);
    code = nodesStringToNode(pCol->expr, &pNode);
    if (code != 0) break;
    SColumnInfoData* pInfo = taosArrayGet((*pRes)->pDataBlock, idx);
    if (nodeType(pNode) == QUERY_NODE_VALUE) {
      void* p = nodesGetValueFromNode((SValueNode*)pNode);
      code = colDataSetVal(pInfo, rowIdx, p, ((SValueNode*)pNode)->isNull);
    } else {
      dst.columnData = pInfo;
      dst.numOfRows = rowIdx;
      dst.colAlloced = false;
      code = streamCalcOneScalarExprInRange(pNode, &dst, rowIdx, rowIdx, &pTaskInfo->pStreamRuntimeInfo->funcInfo);
    }
    ++idx;
    // TODO sclFreeParam(&dst);
    nodesDestroyNode(pNode);
    if (code != 0) break;
  }
  if (code == TSDB_CODE_SUCCESS) {
    (*pRes)->info.rows++;
  }
  pTaskInfo->pStreamRuntimeInfo->funcInfo.curIdx = tmpWinIdx;
  return code;
}

int32_t streamCalcOutputTbName(SNode* pExpr, char* tbname, SStreamRuntimeFuncInfo* pStreamRuntimeInfo) {
  int32_t      code = 0;
  const char*  pVal = NULL;
  SScalarParam dst = {0};
  int32_t      len = 0;
  int32_t      nextIdx = pStreamRuntimeInfo->curIdx;
  pStreamRuntimeInfo->curIdx = 0;  // always use the first window to calc tbname
  // execute the expr
  switch (pExpr->type) {
    case QUERY_NODE_VALUE: {
      SValueNode* pValue = (SValueNode*)pExpr;
      int32_t     type = pValue->node.resType.type;
      if (!IS_STR_DATA_TYPE(type)) {
        qError("invalid sub tb expr with non-str type");
        code = TSDB_CODE_INVALID_PARA;
        break;
      }
      void* pTmp = nodesGetValueFromNode((SValueNode*)pExpr);
      if (pTmp == NULL) {
        qError("invalid sub tb expr with null value");
        code = TSDB_CODE_INVALID_PARA;
        break;
      }
      pVal = varDataVal(pTmp);
      len = varDataLen(pTmp);
    } break;
    case QUERY_NODE_FUNCTION: {
      SFunctionNode* pFunc = (SFunctionNode*)pExpr;
      if (!IS_STR_DATA_TYPE(pFunc->node.resType.type)) {
        qError("invalid sub tb expr with non-str type func");
        code = TSDB_CODE_INVALID_PARA;
        break;
      }
      SColumnInfoData* pCol = taosMemoryCalloc(1, sizeof(SColumnInfoData));
      if (!pCol) {
        code = terrno;
        qError("failed to allocate col info data at: %s, %d", __func__, __LINE__);
        break;
      }

      pCol->hasNull = true;
      pCol->info.type = ((SExprNode*)pExpr)->resType.type;
      pCol->info.colId = 0;
      pCol->info.bytes = ((SExprNode*)pExpr)->resType.bytes;
      pCol->info.precision = ((SExprNode*)pExpr)->resType.precision;
      pCol->info.scale = ((SExprNode*)pExpr)->resType.scale;
      code = colInfoDataEnsureCapacity(pCol, 1, true);
      if (code != 0) {
        qError("failed to ensure capacity for col info data at: %s, %d", __func__, __LINE__);
        taosMemoryFree(pCol);
        break;
      }
      dst.columnData = pCol;
      dst.numOfRows = 1;
      dst.colAlloced = true;
      code = streamCalcOneScalarExpr(pExpr, &dst, pStreamRuntimeInfo);
      if (colDataIsNull_var(dst.columnData, 0)) {
        qInfo("invalid sub tb expr with null value");
        code = TSDB_CODE_MND_STREAM_TBNAME_CALC_FAILED;
      }
      if (code == 0) {
        pVal = varDataVal(colDataGetVarData(dst.columnData, 0));
        len = varDataLen(colDataGetVarData(dst.columnData, 0));
      }
    } break;
    default:
      qError("wrong subtable expr with type: %d", pExpr->type);
      code = TSDB_CODE_OPS_NOT_SUPPORT;
      break;
  }
  if (code == 0) {
    if (!pVal || len == 0) {
      qError("tbname generated with no characters which is not allowed");
      code = TSDB_CODE_INVALID_PARA;
    }
    if(len > TSDB_TABLE_NAME_LEN - 1) {
      qError("tbname generated with too long characters, max allowed is %d, got %d, truncated.", TSDB_TABLE_NAME_LEN - 1, len);
      len = TSDB_TABLE_NAME_LEN - 1;
    }

    memcpy(tbname, pVal, len);
    tbname[len] = '\0';  // ensure null terminated
    if (NULL != strchr(tbname, '.')) {
      code = TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME;
      qError("tbname generated with invalid characters, '.' is not allowed");
    }
  }
  // TODO free dst
  sclFreeParam(&dst);
  pStreamRuntimeInfo->curIdx = nextIdx; // restore
  return code;
}

void destroyStreamInserterParam(SStreamInserterParam* pParam) {
  if (pParam) {
    if (pParam->tbname) {
      taosMemFree(pParam->tbname);
      pParam->tbname = NULL;
    }
    if (pParam->stbname) {
      taosMemFree(pParam->stbname);
      pParam->stbname = NULL;
    }
    if (pParam->dbFName) {
      taosMemFree(pParam->dbFName);
      pParam->dbFName = NULL;
    }
    if (pParam->pFields) {
      taosArrayDestroy(pParam->pFields);
      pParam->pFields = NULL;
    }
    if (pParam->pTagFields) {
      taosArrayDestroy(pParam->pTagFields);
      pParam->pTagFields = NULL;
    }
    taosMemFree(pParam);
  }
}

int32_t cloneStreamInserterParam(SStreamInserterParam** ppDst, SStreamInserterParam* pSrc) {
  int32_t code = 0, lino = 0;
  if (ppDst == NULL || pSrc == NULL) {
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_PARA);
  }
  *ppDst = (SStreamInserterParam*)taosMemoryCalloc(1, sizeof(SStreamInserterParam));
  TSDB_CHECK_NULL(*ppDst, code, lino, _exit, terrno);

  (*ppDst)->suid = pSrc->suid;
  (*ppDst)->sver = pSrc->sver;
  (*ppDst)->tbType = pSrc->tbType;
  (*ppDst)->tbname = taosStrdup(pSrc->tbname);
  TSDB_CHECK_NULL((*ppDst)->tbname, code, lino, _exit, terrno);

  if (pSrc->stbname) {
    (*ppDst)->stbname = taosStrdup(pSrc->stbname);
    TSDB_CHECK_NULL((*ppDst)->stbname, code, lino, _exit, terrno);
  }

  (*ppDst)->dbFName = taosStrdup(pSrc->dbFName);
  TSDB_CHECK_NULL((*ppDst)->dbFName, code, lino, _exit, terrno);

  (*ppDst)->pSinkHandle = pSrc->pSinkHandle;  // don't need clone and free

  if (pSrc->pFields && pSrc->pFields->size > 0) {
    (*ppDst)->pFields = taosArrayDup(pSrc->pFields, NULL);
    TSDB_CHECK_NULL((*ppDst)->pFields, code, lino, _exit, terrno);
  } else {
    (*ppDst)->pFields = NULL;
  }
  
  if (pSrc->pTagFields && pSrc->pTagFields->size > 0) {
    (*ppDst)->pTagFields = taosArrayDup(pSrc->pTagFields, NULL);
    TSDB_CHECK_NULL((*ppDst)->pTagFields, code, lino, _exit, terrno);
  } else {
    (*ppDst)->pTagFields = NULL;
  }

_exit:

  if (code != 0) {
    if (*ppDst) {
      destroyStreamInserterParam(*ppDst);
      *ppDst = NULL;
    }
    
    stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }
  return code;
}

int32_t dropStreamTable(SMsgCb* pMsgCb, void* pOutput, SSTriggerDropRequest* pReq) {
  return doDropStreamTable(pMsgCb, pOutput, pReq);
}

int32_t dropStreamTableByTbName(SMsgCb* pMsgCb, void* pOutput, SSTriggerDropRequest* pReq, char* tbName) {
  return doDropStreamTableByTbName(pMsgCb, pOutput, pReq, tbName);
}
