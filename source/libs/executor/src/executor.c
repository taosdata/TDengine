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
#include "executorInt.h"
#include "operator.h"
#include "planner.h"
#include "querytask.h"
#include "tdatablock.h"
#include "tref.h"
#include "tudf.h"
#include "vnode.h"

static TdThreadOnce initPoolOnce = PTHREAD_ONCE_INIT;
int32_t             exchangeObjRefPool = -1;

static void cleanupRefPool() {
  int32_t ref = atomic_val_compare_exchange_32(&exchangeObjRefPool, exchangeObjRefPool, 0);
  taosCloseRef(ref);
}

static void initRefPool() {
  exchangeObjRefPool = taosOpenRef(1024, doDestroyExchangeOperatorInfo);
  atexit(cleanupRefPool);
}

static int32_t doSetSMABlock(SOperatorInfo* pOperator, void* input, size_t numOfBlocks, int32_t type, char* id) {
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
        taosArrayPush(pInfo->pBlockLists, pReq);
      }
      pInfo->blockType = STREAM_INPUT__DATA_SUBMIT;
    } else if (type == STREAM_INPUT__DATA_SUBMIT) {
      taosArrayPush(pInfo->pBlockLists, &input);
      pInfo->blockType = STREAM_INPUT__DATA_SUBMIT;
    } else if (type == STREAM_INPUT__DATA_BLOCK) {
      for (int32_t i = 0; i < numOfBlocks; ++i) {
        SSDataBlock* pDataBlock = &((SSDataBlock*)input)[i];
        SPackedData  tmp = {
             .pDataBlock = pDataBlock,
        };
        taosArrayPush(pInfo->pBlockLists, &tmp);
      }
      pInfo->blockType = STREAM_INPUT__DATA_BLOCK;
    }

    return TSDB_CODE_SUCCESS;
  }
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

static void clearStreamBlock(SOperatorInfo* pOperator) {
  if (pOperator->operatorType != QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN) {
    if (pOperator->numOfDownstream == 1) {
      return clearStreamBlock(pOperator->pDownstream[0]);
    }
  } else {
    SStreamScanInfo* pInfo = pOperator->info;
    doClearBufferedBlocks(pInfo);
  }
}

void resetTaskInfo(qTaskInfo_t tinfo) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;
  pTaskInfo->code = 0;
  clearStreamBlock(pTaskInfo->pRoot);
}

static int32_t doSetStreamBlock(SOperatorInfo* pOperator, void* input, size_t numOfBlocks, int32_t type, const char* id) {
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
    return doSetStreamBlock(pOperator->pDownstream[0], input, numOfBlocks, type, id);
  } else {
    pOperator->status = OP_NOT_OPENED;

    SStreamScanInfo* pInfo = pOperator->info;
    qDebug("s-task:%s set source blocks:%d", id, (int32_t)numOfBlocks);
    ASSERT(pInfo->validBlockIndex == 0 && taosArrayGetSize(pInfo->pBlockLists) == 0);

    if (type == STREAM_INPUT__MERGED_SUBMIT) {
      for (int32_t i = 0; i < numOfBlocks; i++) {
        SPackedData* pReq = POINTER_SHIFT(input, i * sizeof(SPackedData));
        taosArrayPush(pInfo->pBlockLists, pReq);
      }
      pInfo->blockType = STREAM_INPUT__DATA_SUBMIT;
    } else if (type == STREAM_INPUT__DATA_SUBMIT) {
      taosArrayPush(pInfo->pBlockLists, input);
      pInfo->blockType = STREAM_INPUT__DATA_SUBMIT;
    } else if (type == STREAM_INPUT__DATA_BLOCK) {
      for (int32_t i = 0; i < numOfBlocks; ++i) {
        SSDataBlock* pDataBlock = &((SSDataBlock*)input)[i];
        SPackedData  tmp = { .pDataBlock = pDataBlock };
        taosArrayPush(pInfo->pBlockLists, &tmp);
      }
      pInfo->blockType = STREAM_INPUT__DATA_BLOCK;
    } else {
      ASSERT(0);
    }

    return TSDB_CODE_SUCCESS;
  }
}

void doSetTaskId(SOperatorInfo* pOperator) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  if (pOperator->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN) {
    SStreamScanInfo* pStreamScanInfo = pOperator->info;
    if (pStreamScanInfo->pTableScanOp != NULL) {
      STableScanInfo* pScanInfo = pStreamScanInfo->pTableScanOp->info;
      if (pScanInfo->base.dataReader != NULL) {
        tsdbReaderSetId(pScanInfo->base.dataReader, pTaskInfo->id.str);
      }
    }
  } else {
    doSetTaskId(pOperator->pDownstream[0]);
  }
}

void qSetTaskId(qTaskInfo_t tinfo, uint64_t taskId, uint64_t queryId) {
  SExecTaskInfo* pTaskInfo = tinfo;
  pTaskInfo->id.queryId = queryId;
  buildTaskId(taskId, queryId, pTaskInfo->id.str);

  // set the idstr for tsdbReader
  doSetTaskId(pTaskInfo->pRoot);
}

int32_t qSetStreamOpOpen(qTaskInfo_t tinfo) {
  if (tinfo == NULL) {
    return TSDB_CODE_APP_ERROR;
  }

  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;

  int32_t code = doSetStreamOpOpen(pTaskInfo->pRoot, GET_TASKID(pTaskInfo));
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed to set the stream block data", GET_TASKID(pTaskInfo));
  } else {
    qDebug("%s set the stream block successfully", GET_TASKID(pTaskInfo));
  }

  return code;
}

void qGetCheckpointVersion(qTaskInfo_t tinfo, int64_t* dataVer, int64_t* ckId) {
  SExecTaskInfo* pTaskInfo = tinfo;
  *dataVer = pTaskInfo->streamInfo.dataVersion;
  *ckId = pTaskInfo->streamInfo.checkPointId;
}


int32_t qSetMultiStreamInput(qTaskInfo_t tinfo, const void* pBlocks, size_t numOfBlocks, int32_t type) {
  if (tinfo == NULL) {
    return TSDB_CODE_APP_ERROR;
  }

  if (pBlocks == NULL || numOfBlocks == 0) {
    return TSDB_CODE_SUCCESS;
  }

  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;

  int32_t code = doSetStreamBlock(pTaskInfo->pRoot, (void*)pBlocks, numOfBlocks, type, GET_TASKID(pTaskInfo));
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed to set the stream block data", GET_TASKID(pTaskInfo));
  } else {
    qDebug("%s set the stream block successfully", GET_TASKID(pTaskInfo));
  }

  return code;
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
    SExecTaskInfo* pTaskInfo = doCreateTask(0, id, vgId, OPTR_EXEC_MODEL_QUEUE);
    if (NULL == pTaskInfo) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return NULL;
    }
    pTaskInfo->pRoot = createRawScanOperatorInfo(pReaderHandle, pTaskInfo);
    if (NULL == pTaskInfo->pRoot) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      taosMemoryFree(pTaskInfo);
      return NULL;
    }

    qDebug("create raw scan task info completed, vgId:%d, %s", vgId, GET_TASKID(pTaskInfo));
    return pTaskInfo;
  }

  struct SSubplan* pPlan = NULL;

  int32_t code = qStringToSubplan(msg, &pPlan);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return NULL;
  }

  qTaskInfo_t pTaskInfo = NULL;
  code = qCreateExecTask(pReaderHandle, vgId, 0, pPlan, &pTaskInfo, NULL, NULL, OPTR_EXEC_MODEL_QUEUE);
  if (code != TSDB_CODE_SUCCESS) {
    nodesDestroyNode((SNode*)pPlan);
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

qTaskInfo_t qCreateStreamExecTaskInfo(void* msg, SReadHandle* readers, int32_t vgId) {
  if (msg == NULL) {
    return NULL;
  }

  struct SSubplan* pPlan = NULL;
  int32_t          code = qStringToSubplan(msg, &pPlan);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return NULL;
  }

  qTaskInfo_t pTaskInfo = NULL;
  code = qCreateExecTask(readers, vgId, 0, pPlan, &pTaskInfo, NULL, NULL, OPTR_EXEC_MODEL_STREAM);
  if (code != TSDB_CODE_SUCCESS) {
    qDestroyTask(pTaskInfo);
    terrno = code;
    return NULL;
  }

  return pTaskInfo;
}

static SArray* filterUnqualifiedTables(const SStreamScanInfo* pScanInfo, const SArray* tableIdList, const char* idstr) {
  SArray* qa = taosArrayInit(4, sizeof(tb_uid_t));
  int32_t numOfUids = taosArrayGetSize(tableIdList);
  if (numOfUids == 0) {
    return qa;
  }

  STableScanInfo* pTableScanInfo = pScanInfo->pTableScanOp->info;

  uint64_t suid = 0;
  uint64_t uid = 0;
  int32_t type = 0;
  tableListGetSourceTableInfo(pTableScanInfo->base.pTableListInfo, &suid, &uid, &type);

  // let's discard the tables those are not created according to the queried super table.
  SMetaReader mr = {0};
  metaReaderInit(&mr, pScanInfo->readHandle.meta, 0);
  for (int32_t i = 0; i < numOfUids; ++i) {
    uint64_t* id = (uint64_t*)taosArrayGet(tableIdList, i);

    int32_t code = metaGetTableEntryByUid(&mr, *id);
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

    if (pScanInfo->pTagCond != NULL) {
      bool          qualified = false;
      STableKeyInfo info = {.groupId = 0, .uid = mr.me.uid};
      code = isQualifiedTable(&info, pScanInfo->pTagCond, pScanInfo->readHandle.meta, &qualified);
      if (code != TSDB_CODE_SUCCESS) {
        qError("failed to filter new table, uid:0x%" PRIx64 ", %s", info.uid, idstr);
        continue;
      }

      if (!qualified) {
        continue;
      }
    }

    // handle multiple partition
    taosArrayPush(qa, id);
  }

  metaReaderClear(&mr);
  return qa;
}

int32_t qUpdateTableListForStreamScanner(qTaskInfo_t tinfo, const SArray* tableIdList, bool isAdd) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;
  const char*    id = GET_TASKID(pTaskInfo);
  int32_t        code = 0;

  if (isAdd) {
    qDebug("add %d tables id into query list, %s", (int32_t)taosArrayGetSize(tableIdList), id);
  }

  // traverse to the stream scanner node to add this table id
  SOperatorInfo*   pInfo = extractOperatorInTree(pTaskInfo->pRoot, QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN, id);
  SStreamScanInfo* pScanInfo = pInfo->info;

  if (isAdd) {  // add new table id
    SArray* qa = filterUnqualifiedTables(pScanInfo, tableIdList, id);
    int32_t numOfQualifiedTables = taosArrayGetSize(qa);
    qDebug("%d qualified child tables added into stream scanner, %s", numOfQualifiedTables, id);
    code = tqReaderAddTbUidList(pScanInfo->tqReader, qa);
    if (code != TSDB_CODE_SUCCESS) {
      taosArrayDestroy(qa);
      return code;
    }

    bool   assignUid = false;
    size_t bufLen = (pScanInfo->pGroupTags != NULL) ? getTableTagsBufLen(pScanInfo->pGroupTags) : 0;
    char*  keyBuf = NULL;
    if (bufLen > 0) {
      assignUid = groupbyTbname(pScanInfo->pGroupTags);
      keyBuf = taosMemoryMalloc(bufLen);
      if (keyBuf == NULL) {
        taosArrayDestroy(qa);
        return TSDB_CODE_OUT_OF_MEMORY;
      }
    }

    STableListInfo* pTableListInfo = ((STableScanInfo*)pScanInfo->pTableScanOp->info)->base.pTableListInfo;
    taosWLockLatch(&pTaskInfo->lock);

    for (int32_t i = 0; i < numOfQualifiedTables; ++i) {
      uint64_t*     uid = taosArrayGet(qa, i);
      STableKeyInfo keyInfo = {.uid = *uid, .groupId = 0};

      if (bufLen > 0) {
        if (assignUid) {
          keyInfo.groupId = keyInfo.uid;
        } else {
          code = getGroupIdFromTagsVal(pScanInfo->readHandle.meta, keyInfo.uid, pScanInfo->pGroupTags, keyBuf,
                                       &keyInfo.groupId);
          if (code != TSDB_CODE_SUCCESS) {
            taosMemoryFree(keyBuf);
            taosArrayDestroy(qa);
            taosWUnLockLatch(&pTaskInfo->lock);
            return code;
          }
        }
      }

      tableListAddTableInfo(pTableListInfo, keyInfo.uid, keyInfo.groupId);
    }

    taosWUnLockLatch(&pTaskInfo->lock);
    if (keyBuf != NULL) {
      taosMemoryFree(keyBuf);
    }

    taosArrayDestroy(qa);
  } else {  // remove the table id in current list
    qDebug("%d remove child tables from the stream scanner, %s", (int32_t)taosArrayGetSize(tableIdList), id);
    taosWLockLatch(&pTaskInfo->lock);
    code = tqReaderRemoveTbUidList(pScanInfo->tqReader, tableIdList);
    taosWUnLockLatch(&pTaskInfo->lock);
  }

  return code;
}

int32_t qGetQueryTableSchemaVersion(qTaskInfo_t tinfo, char* dbName, char* tableName, int32_t* sversion,
                                    int32_t* tversion) {
  ASSERT(tinfo != NULL && dbName != NULL && tableName != NULL);
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;

  if (pTaskInfo->schemaInfo.sw == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  *sversion = pTaskInfo->schemaInfo.sw->version;
  *tversion = pTaskInfo->schemaInfo.tversion;
  if (pTaskInfo->schemaInfo.dbname) {
    strcpy(dbName, pTaskInfo->schemaInfo.dbname);
  } else {
    dbName[0] = 0;
  }
  if (pTaskInfo->schemaInfo.tablename) {
    strcpy(tableName, pTaskInfo->schemaInfo.tablename);
  } else {
    tableName[0] = 0;
  }

  return 0;
}

int32_t qCreateExecTask(SReadHandle* readHandle, int32_t vgId, uint64_t taskId, SSubplan* pSubplan,
                        qTaskInfo_t* pTaskInfo, DataSinkHandle* handle, char* sql, EOPTR_EXEC_MODEL model) {
  SExecTaskInfo** pTask = (SExecTaskInfo**)pTaskInfo;
  taosThreadOnce(&initPoolOnce, initRefPool);

  qDebug("start to create task, TID:0x%" PRIx64 " QID:0x%" PRIx64 ", vgId:%d", taskId, pSubplan->id.queryId, vgId);

  int32_t code = createExecTaskInfo(pSubplan, pTask, readHandle, taskId, vgId, sql, model);
  if (code != TSDB_CODE_SUCCESS) {
    qError("failed to createExecTaskInfo, code: %s", tstrerror(code));
    goto _error;
  }

  SDataSinkMgtCfg cfg = {.maxDataBlockNum = 500, .maxDataBlockNumPerQuery = 50};
  code = dsDataSinkMgtInit(&cfg);
  if (code != TSDB_CODE_SUCCESS) {
    qError("failed to dsDataSinkMgtInit, code:%s, %s", tstrerror(code), (*pTask)->id.str);
    goto _error;
  }

  if (handle) {
    void* pSinkParam = NULL;
    code = createDataSinkParam(pSubplan->pDataSink, &pSinkParam, (*pTask), readHandle);
    if (code != TSDB_CODE_SUCCESS) {
      qError("failed to createDataSinkParam, vgId:%d, code:%s, %s", vgId, tstrerror(code), (*pTask)->id.str);
      goto _error;
    }

    // pSinkParam has been freed during create sinker.
    code = dsCreateDataSinker(pSubplan->pDataSink, handle, pSinkParam, (*pTask)->id.str);
  }

  qDebug("subplan task create completed, TID:0x%" PRIx64 " QID:0x%" PRIx64, taskId, pSubplan->id.queryId);

_error:
  // if failed to add ref for all tables in this query, abort current query
  return code;
}

static void freeBlock(void* param) {
  SSDataBlock* pBlock = *(SSDataBlock**)param;
  blockDataDestroy(pBlock);
}

int32_t qExecTaskOpt(qTaskInfo_t tinfo, SArray* pResList, uint64_t* useconds, bool* hasMore, SLocalFetch* pLocal) {
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
    return TSDB_CODE_SUCCESS;
  }

  // error occurs, record the error code and return to client
  int32_t ret = setjmp(pTaskInfo->env);
  if (ret != TSDB_CODE_SUCCESS) {
    pTaskInfo->code = ret;
    cleanUpUdfs();

    qDebug("%s task abort due to error/cancel occurs, code:%s", GET_TASKID(pTaskInfo), tstrerror(pTaskInfo->code));
    atomic_store_64(&pTaskInfo->owner, 0);

    return pTaskInfo->code;
  }

  qDebug("%s execTask is launched", GET_TASKID(pTaskInfo));

  int32_t      current = 0;
  SSDataBlock* pRes = NULL;

  int64_t st = taosGetTimestampUs();

  int32_t blockIndex = 0;
  while ((pRes = pTaskInfo->pRoot->fpSet.getNextFn(pTaskInfo->pRoot)) != NULL) {
    SSDataBlock* p = NULL;
    if (blockIndex >= taosArrayGetSize(pTaskInfo->pResultBlockList)) {
      SSDataBlock* p1 = createOneDataBlock(pRes, true);
      taosArrayPush(pTaskInfo->pResultBlockList, &p1);
      p = p1;
    } else {
      p = *(SSDataBlock**)taosArrayGet(pTaskInfo->pResultBlockList, blockIndex);
      copyDataBlock(p, pRes);
    }

    blockIndex += 1;

    current += p->info.rows;
    ASSERT(p->info.rows > 0);
    taosArrayPush(pResList, &p);

    if (current >= 4096) {
      break;
    }
  }

  *hasMore = (pRes != NULL);
  uint64_t el = (taosGetTimestampUs() - st);

  pTaskInfo->cost.elapsedTime += el;
  if (NULL == pRes) {
    *useconds = pTaskInfo->cost.elapsedTime;
  }

  cleanUpUdfs();

  uint64_t total = pTaskInfo->pRoot->resultInfo.totalRows;
  qDebug("%s task suspended, %d rows in %d blocks returned, total:%" PRId64 " rows, in sinkNode:%d, elapsed:%.2f ms",
         GET_TASKID(pTaskInfo), current, (int32_t)taosArrayGetSize(pResList), total, 0, el / 1000.0);

  atomic_store_64(&pTaskInfo->owner, 0);
  return pTaskInfo->code;
}

void qCleanExecTaskBlockBuf(qTaskInfo_t tinfo) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;
  SArray*        pList = pTaskInfo->pResultBlockList;
  size_t         num = taosArrayGetSize(pList);
  for (int32_t i = 0; i < num; ++i) {
    SSDataBlock** p = taosArrayGet(pTaskInfo->pResultBlockList, i);
    blockDataDestroy(*p);
  }

  taosArrayClear(pTaskInfo->pResultBlockList);
}

int32_t qExecTask(qTaskInfo_t tinfo, SSDataBlock** pRes, uint64_t* useconds) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;
  int64_t        threadId = taosGetSelfPthreadId();

  *pRes = NULL;
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
    clearStreamBlock(pTaskInfo->pRoot);
    atomic_store_64(&pTaskInfo->owner, 0);
    qDebug("%s already killed, abort", GET_TASKID(pTaskInfo));
    return TSDB_CODE_SUCCESS;
  }

  // error occurs, record the error code and return to client
  int32_t ret = setjmp(pTaskInfo->env);
  if (ret != TSDB_CODE_SUCCESS) {
    pTaskInfo->code = ret;
    cleanUpUdfs();
    qDebug("%s task abort due to error/cancel occurs, code:%s", GET_TASKID(pTaskInfo), tstrerror(pTaskInfo->code));
    atomic_store_64(&pTaskInfo->owner, 0);
    return pTaskInfo->code;
  }

  qDebug("%s execTask is launched", GET_TASKID(pTaskInfo));

  int64_t st = taosGetTimestampUs();

  *pRes = pTaskInfo->pRoot->fpSet.getNextFn(pTaskInfo->pRoot);
  uint64_t el = (taosGetTimestampUs() - st);

  pTaskInfo->cost.elapsedTime += el;
  if (NULL == *pRes) {
    *useconds = pTaskInfo->cost.elapsedTime;
  }

  cleanUpUdfs();

  int32_t  current = (*pRes != NULL) ? (*pRes)->info.rows : 0;
  uint64_t total = pTaskInfo->pRoot->resultInfo.totalRows;

  qDebug("%s task suspended, %d rows returned, total:%" PRId64 " rows, in sinkNode:%d, elapsed:%.2f ms",
         GET_TASKID(pTaskInfo), current, total, 0, el / 1000.0);

  atomic_store_64(&pTaskInfo->owner, 0);
  return pTaskInfo->code;
}

int32_t qAppendTaskStopInfo(SExecTaskInfo* pTaskInfo, SExchangeOpStopInfo* pInfo) {
  taosWLockLatch(&pTaskInfo->stopInfo.lock);
  taosArrayPush(pTaskInfo->stopInfo.pStopInfo, pInfo);
  taosWUnLockLatch(&pTaskInfo->stopInfo.lock);

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
    SExchangeInfo*       pExchangeInfo = taosAcquireRef(exchangeObjRefPool, pStop->refId);
    if (pExchangeInfo) {
      tsem_post(&pExchangeInfo->ready);
      taosReleaseRef(exchangeObjRefPool, pStop->refId);
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

int32_t qKillTask(qTaskInfo_t tinfo, int32_t rspCode) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;
  if (pTaskInfo == NULL) {
    return TSDB_CODE_QRY_INVALID_QHANDLE;
  }

  qDebug("%s sync killed execTask", GET_TASKID(pTaskInfo));
  setTaskKilled(pTaskInfo, TSDB_CODE_TSC_QUERY_KILLED);

  while (qTaskIsExecuting(pTaskInfo)) {
    taosMsleep(10);
  }

  pTaskInfo->code = rspCode;
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

int32_t qSerializeTaskStatus(qTaskInfo_t tinfo, char** pOutput, int32_t* len) {
  SExecTaskInfo* pTaskInfo = (struct SExecTaskInfo*)tinfo;
  if (pTaskInfo->pRoot == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t nOptrWithVal = 0;
  //  int32_t code = encodeOperator(pTaskInfo->pRoot, pOutput, len, &nOptrWithVal);
  //  if ((code == TSDB_CODE_SUCCESS) && (nOptrWithVal == 0)) {
  //    taosMemoryFreeClear(*pOutput);
  //    *len = 0;
  //  }
  return 0;
}

int32_t qDeserializeTaskStatus(qTaskInfo_t tinfo, const char* pInput, int32_t len) {
  SExecTaskInfo* pTaskInfo = (struct SExecTaskInfo*)tinfo;

  if (pTaskInfo == NULL || pInput == NULL || len == 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  return 0;
  //  return decodeOperator(pTaskInfo->pRoot, pInput, len);
}

int32_t qExtractStreamScanner(qTaskInfo_t tinfo, void** scanner) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;
  SOperatorInfo* pOperator = pTaskInfo->pRoot;

  while (1) {
    uint16_t type = pOperator->operatorType;
    if (type == QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN) {
      *scanner = pOperator->info;
      return 0;
    } else {
      ASSERT(pOperator->numOfDownstream == 1);
      pOperator = pOperator->pDownstream[0];
    }
  }
}

int32_t qStreamSourceRecoverStep1(qTaskInfo_t tinfo, int64_t ver) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;
  ASSERT(pTaskInfo->execModel == OPTR_EXEC_MODEL_STREAM);
  pTaskInfo->streamInfo.fillHistoryVer1 = ver;
  pTaskInfo->streamInfo.recoverStep = STREAM_RECOVER_STEP__PREPARE1;
  return 0;
}

int32_t qStreamSourceRecoverStep2(qTaskInfo_t tinfo, int64_t ver) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;
  ASSERT(pTaskInfo->execModel == OPTR_EXEC_MODEL_STREAM);
  pTaskInfo->streamInfo.fillHistoryVer2 = ver;
  pTaskInfo->streamInfo.recoverStep = STREAM_RECOVER_STEP__PREPARE2;
  return 0;
}

int32_t qStreamRecoverFinish(qTaskInfo_t tinfo) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;
  ASSERT(pTaskInfo->execModel == OPTR_EXEC_MODEL_STREAM);
  pTaskInfo->streamInfo.recoverStep = STREAM_RECOVER_STEP__NONE;
  return 0;
}

int32_t qStreamSetParamForRecover(qTaskInfo_t tinfo) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;
  SOperatorInfo* pOperator = pTaskInfo->pRoot;

  while (1) {
    if (pOperator->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL ||
        pOperator->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_INTERVAL ||
        pOperator->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_INTERVAL) {
      SStreamIntervalOperatorInfo* pInfo = pOperator->info;
      ASSERT(pInfo->twAggSup.calTrigger == STREAM_TRIGGER_AT_ONCE ||
             pInfo->twAggSup.calTrigger == STREAM_TRIGGER_WINDOW_CLOSE);
      ASSERT(pInfo->twAggSup.calTriggerSaved == 0 && pInfo->twAggSup.deleteMarkSaved == 0);

      qInfo("save stream param for interval: %d,  %" PRId64, pInfo->twAggSup.calTrigger, pInfo->twAggSup.deleteMark);

      pInfo->twAggSup.calTriggerSaved = pInfo->twAggSup.calTrigger;
      pInfo->twAggSup.deleteMarkSaved = pInfo->twAggSup.deleteMark;
      pInfo->twAggSup.calTrigger = STREAM_TRIGGER_AT_ONCE;
      pInfo->twAggSup.deleteMark = INT64_MAX;
      pInfo->ignoreExpiredDataSaved = pInfo->ignoreExpiredData;
      pInfo->ignoreExpiredData = false;
    } else if (pOperator->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_SESSION ||
               pOperator->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_SESSION ||
               pOperator->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_SESSION) {
      SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
      ASSERT(pInfo->twAggSup.calTrigger == STREAM_TRIGGER_AT_ONCE ||
             pInfo->twAggSup.calTrigger == STREAM_TRIGGER_WINDOW_CLOSE);

      ASSERT(pInfo->twAggSup.calTriggerSaved == 0 && pInfo->twAggSup.deleteMarkSaved == 0);
      qInfo("save stream param for session: %d,  %" PRId64, pInfo->twAggSup.calTrigger, pInfo->twAggSup.deleteMark);

      pInfo->twAggSup.calTriggerSaved = pInfo->twAggSup.calTrigger;
      pInfo->twAggSup.deleteMarkSaved = pInfo->twAggSup.deleteMark;
      pInfo->twAggSup.calTrigger = STREAM_TRIGGER_AT_ONCE;
      pInfo->twAggSup.deleteMark = INT64_MAX;
      pInfo->ignoreExpiredDataSaved = pInfo->ignoreExpiredData;
      pInfo->ignoreExpiredData = false;
    } else if (pOperator->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_STATE) {
      SStreamStateAggOperatorInfo* pInfo = pOperator->info;
      ASSERT(pInfo->twAggSup.calTrigger == STREAM_TRIGGER_AT_ONCE ||
             pInfo->twAggSup.calTrigger == STREAM_TRIGGER_WINDOW_CLOSE);
      ASSERT(pInfo->twAggSup.calTriggerSaved == 0 && pInfo->twAggSup.deleteMarkSaved == 0);

      qInfo("save stream param for state: %d,  %" PRId64, pInfo->twAggSup.calTrigger, pInfo->twAggSup.deleteMark);

      pInfo->twAggSup.calTriggerSaved = pInfo->twAggSup.calTrigger;
      pInfo->twAggSup.deleteMarkSaved = pInfo->twAggSup.deleteMark;
      pInfo->twAggSup.calTrigger = STREAM_TRIGGER_AT_ONCE;
      pInfo->twAggSup.deleteMark = INT64_MAX;
      pInfo->ignoreExpiredDataSaved = pInfo->ignoreExpiredData;
      pInfo->ignoreExpiredData = false;
    }

    // iterate operator tree
    if (pOperator->numOfDownstream != 1 || pOperator->pDownstream[0] == NULL) {
      if (pOperator->numOfDownstream > 1) {
        qError("unexpected stream, multiple downstream");
        ASSERT(0);
        return -1;
      }
      return 0;
    } else {
      pOperator = pOperator->pDownstream[0];
    }
  }

  return 0;
}

int32_t qStreamRestoreParam(qTaskInfo_t tinfo) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;
  SOperatorInfo* pOperator = pTaskInfo->pRoot;

  while (1) {
    if (pOperator->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL ||
        pOperator->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_INTERVAL ||
        pOperator->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_INTERVAL) {
      SStreamIntervalOperatorInfo* pInfo = pOperator->info;
      pInfo->twAggSup.calTrigger = pInfo->twAggSup.calTriggerSaved;
      pInfo->twAggSup.deleteMark = pInfo->twAggSup.deleteMarkSaved;
      pInfo->ignoreExpiredData = pInfo->ignoreExpiredDataSaved;
      qInfo("restore stream param for interval: %d,  %" PRId64, pInfo->twAggSup.calTrigger, pInfo->twAggSup.deleteMark);
    } else if (pOperator->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_SESSION ||
               pOperator->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_SESSION ||
               pOperator->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_SESSION) {
      SStreamSessionAggOperatorInfo* pInfo = pOperator->info;
      pInfo->twAggSup.calTrigger = pInfo->twAggSup.calTriggerSaved;
      pInfo->twAggSup.deleteMark = pInfo->twAggSup.deleteMarkSaved;
      pInfo->ignoreExpiredData = pInfo->ignoreExpiredDataSaved;
      qInfo("restore stream param for session: %d,  %" PRId64, pInfo->twAggSup.calTrigger, pInfo->twAggSup.deleteMark);
    } else if (pOperator->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_STATE) {
      SStreamStateAggOperatorInfo* pInfo = pOperator->info;
      pInfo->twAggSup.calTrigger = pInfo->twAggSup.calTriggerSaved;
      pInfo->twAggSup.deleteMark = pInfo->twAggSup.deleteMarkSaved;
      pInfo->ignoreExpiredData = pInfo->ignoreExpiredDataSaved;
      qInfo("restore stream param for state: %d,  %" PRId64, pInfo->twAggSup.calTrigger, pInfo->twAggSup.deleteMark);
    }

    // iterate operator tree
    if (pOperator->numOfDownstream != 1 || pOperator->pDownstream[0] == NULL) {
      if (pOperator->numOfDownstream > 1) {
        qError("unexpected stream, multiple downstream");
        return -1;
      }
      return 0;
    } else {
      pOperator = pOperator->pDownstream[0];
    }
  }
  return 0;
}

bool qStreamRecoverScanFinished(qTaskInfo_t tinfo) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;
  return pTaskInfo->streamInfo.recoverScanFinished;
}

void* qExtractReaderFromStreamScanner(void* scanner) {
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

SMqMetaRsp* qStreamExtractMetaMsg(qTaskInfo_t tinfo) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;
  return &pTaskInfo->streamInfo.metaRsp;
}

void qStreamExtractOffset(qTaskInfo_t tinfo, STqOffsetVal* pOffset) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;
  memcpy(pOffset, &pTaskInfo->streamInfo.currentOffset, sizeof(STqOffsetVal));
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
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  pCond->twindows = TSWINDOW_INITIALIZER;
  pCond->suid = pMtInfo->suid;
  pCond->type = TIMEWINDOW_RANGE_CONTAINED;
  pCond->startVersion = -1;
  pCond->endVersion = sContext->snapVersion;

  for (int32_t i = 0; i < pCond->numOfCols; ++i) {
    SColumnInfo* pColInfo = &pCond->colList[i];
    pColInfo->type = pMtInfo->schema->pSchema[i].type;
    pColInfo->bytes = pMtInfo->schema->pSchema[i].bytes;
    pColInfo->colId = pMtInfo->schema->pSchema[i].colId;

    pCond->pSlotList[i] = i;
  }

  return TSDB_CODE_SUCCESS;
}

void qStreamSetOpen(qTaskInfo_t tinfo) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;
  SOperatorInfo* pOperator = pTaskInfo->pRoot;
  pOperator->status = OP_NOT_OPENED;
}

int32_t qStreamPrepareScan(qTaskInfo_t tinfo, STqOffsetVal* pOffset, int8_t subType) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;
  SOperatorInfo* pOperator = pTaskInfo->pRoot;
  const char*    id = GET_TASKID(pTaskInfo);

  // if pOffset equal to current offset, means continue consume
  if (tOffsetEqual(pOffset, &pTaskInfo->streamInfo.currentOffset)) {
    return 0;
  }

  if (subType == TOPIC_SUB_TYPE__COLUMN) {
    pOperator = extractOperatorInTree(pOperator, QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN, id);
    if (pOperator == NULL) {
      return -1;
    }

    SStreamScanInfo* pInfo = pOperator->info;
    STableScanInfo*  pScanInfo = pInfo->pTableScanOp->info;
    STableScanBase*  pScanBaseInfo = &pScanInfo->base;
    STableListInfo*  pTableListInfo = pScanBaseInfo->pTableListInfo;

    if (pOffset->type == TMQ_OFFSET__LOG) {
      // todo refactor: move away
      tsdbReaderClose(pScanBaseInfo->dataReader);
      pScanBaseInfo->dataReader = NULL;

      walReaderVerifyOffset(pInfo->tqReader->pWalReader, pOffset);
      if (tqSeekVer(pInfo->tqReader, pOffset->version + 1, id) < 0) {
        qError("tqSeekVer failed ver:%" PRId64 ", %s", pOffset->version + 1, id);
        return -1;
      }
    } else if (pOffset->type == TMQ_OFFSET__SNAPSHOT_DATA) {
      // iterate all tables from tableInfoList, and retrieve rows from each table one-by-one
      // those data are from the snapshot in tsdb, besides the data in the wal file.
      int64_t uid = pOffset->uid;
      int64_t ts = pOffset->ts;
      int32_t index = 0;

      // this value may be changed if new tables are created
      taosRLockLatch(&pTaskInfo->lock);
      int32_t numOfTables = tableListGetSize(pTableListInfo);

      if (uid == 0) {
        if (numOfTables != 0) {
          STableKeyInfo* pTableInfo = tableListGetInfo(pTableListInfo, 0);
          uid = pTableInfo->uid;
          ts = INT64_MIN;
          pScanInfo->currentTable = 0;
        } else {
          taosRUnLockLatch(&pTaskInfo->lock);
          qError("no table in table list, %s", id);
          terrno = TSDB_CODE_PAR_INTERNAL_ERROR;
          return -1;
        }
      }

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
        terrno = TSDB_CODE_PAR_INTERNAL_ERROR;
        return -1;
      }

      STableKeyInfo keyInfo = {.uid = uid};
      int64_t       oldSkey = pScanBaseInfo->cond.twindows.skey;

      // let's start from the next ts that returned to consumer.
      pScanBaseInfo->cond.twindows.skey = ts + 1;
      pScanInfo->scanTimes = 0;

      if (pScanBaseInfo->dataReader == NULL) {
        int32_t code = tsdbReaderOpen(pScanBaseInfo->readHandle.vnode, &pScanBaseInfo->cond, &keyInfo, 1,
                                      pScanInfo->pResBlock, &pScanBaseInfo->dataReader, id, false, NULL);
        if (code != TSDB_CODE_SUCCESS) {
          qError("prepare read tsdb snapshot failed, uid:%" PRId64 ", code:%s %s", pOffset->uid, tstrerror(code), id);
          terrno = code;
          return -1;
        }

        qDebug("tsdb reader created with offset(snapshot) uid:%" PRId64 " ts:%" PRId64 " table index:%d, total:%d, %s",
               uid, pScanBaseInfo->cond.twindows.skey, pScanInfo->currentTable, numOfTables, id);
      } else {
        tsdbSetTableList(pScanBaseInfo->dataReader, &keyInfo, 1);
        tsdbReaderReset(pScanBaseInfo->dataReader, &pScanBaseInfo->cond);
        qDebug("tsdb reader offset seek snapshot to uid:%" PRId64 " ts %" PRId64 "  table index:%d numOfTable:%d, %s",
               uid, pScanBaseInfo->cond.twindows.skey, pScanInfo->currentTable, numOfTables, id);
      }

      // restore the key value
      pScanBaseInfo->cond.twindows.skey = oldSkey;
    } else {
      qError("invalid pOffset->type:%d, %s", pOffset->type, id);
      terrno = TSDB_CODE_PAR_INTERNAL_ERROR;
      return -1;
    }

  } else {  // subType == TOPIC_SUB_TYPE__TABLE/TOPIC_SUB_TYPE__DB

    if (pOffset->type == TMQ_OFFSET__SNAPSHOT_DATA) {
      SStreamRawScanInfo* pInfo = pOperator->info;
      SSnapContext*       sContext = pInfo->sContext;

      SOperatorInfo*  p = extractOperatorInTree(pOperator, QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, id);
      STableListInfo* pTableListInfo = ((SStreamRawScanInfo*)(p->info))->pTableListInfo;

      if (setForSnapShot(sContext, pOffset->uid) != 0) {
        qError("setDataForSnapShot error. uid:%" PRId64 " , %s", pOffset->uid, id);
        terrno = TSDB_CODE_PAR_INTERNAL_ERROR;
        return -1;
      }

      SMetaTableInfo mtInfo = getUidfromSnapShot(sContext);
      tsdbReaderClose(pInfo->dataReader);
      pInfo->dataReader = NULL;

      cleanupQueryTableDataCond(&pTaskInfo->streamInfo.tableCond);
      tableListClear(pTableListInfo);

      if (mtInfo.uid == 0) {
        goto end;  // no data
      }

      initQueryTableDataCondForTmq(&pTaskInfo->streamInfo.tableCond, sContext, &mtInfo);
      pTaskInfo->streamInfo.tableCond.twindows.skey = pOffset->ts;

      tableListAddTableInfo(pTableListInfo, mtInfo.uid, 0);

      STableKeyInfo* pList = tableListGetInfo(pTableListInfo, 0);
      int32_t        size = tableListGetSize(pTableListInfo);

      tsdbReaderOpen(pInfo->vnode, &pTaskInfo->streamInfo.tableCond, pList, size, NULL, &pInfo->dataReader, NULL,
                     false, NULL);

      cleanupQueryTableDataCond(&pTaskInfo->streamInfo.tableCond);
      strcpy(pTaskInfo->streamInfo.tbName, mtInfo.tbName);
      tDeleteSchemaWrapper(pTaskInfo->streamInfo.schema);
      pTaskInfo->streamInfo.schema = mtInfo.schema;

      qDebug("tmqsnap qStreamPrepareScan snapshot data uid:%" PRId64 " ts %" PRId64 " %s", mtInfo.uid, pOffset->ts, id);
    } else if (pOffset->type == TMQ_OFFSET__SNAPSHOT_META) {
      SStreamRawScanInfo* pInfo = pOperator->info;
      SSnapContext*       sContext = pInfo->sContext;
      if (setForSnapShot(sContext, pOffset->uid) != 0) {
        qError("setForSnapShot error. uid:%" PRIu64 " ,version:%" PRId64, pOffset->uid, pOffset->version);
        terrno = TSDB_CODE_PAR_INTERNAL_ERROR;
        return -1;
      }
      qDebug("tmqsnap qStreamPrepareScan snapshot meta uid:%" PRId64 " ts %" PRId64 " %s", pOffset->uid, pOffset->ts,
             id);
    } else if (pOffset->type == TMQ_OFFSET__LOG) {
      SStreamRawScanInfo* pInfo = pOperator->info;
      tsdbReaderClose(pInfo->dataReader);
      pInfo->dataReader = NULL;
      qDebug("tmqsnap qStreamPrepareScan snapshot log, %s", id);
    }
  }

end:
  pTaskInfo->streamInfo.currentOffset = *pOffset;

  return 0;
}

void qProcessRspMsg(void* parent, SRpcMsg* pMsg, SEpSet* pEpSet) {
  SMsgSendInfo* pSendInfo = (SMsgSendInfo*)pMsg->info.ahandle;
  if (pMsg->info.ahandle == NULL) {
    qError("pMsg->info.ahandle is NULL");
    return;
  }

  SDataBuf buf = {.len = pMsg->contLen, .pData = NULL};

  if (pMsg->contLen > 0) {
    buf.pData = taosMemoryCalloc(1, pMsg->contLen);
    if (buf.pData == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      pMsg->code = TSDB_CODE_OUT_OF_MEMORY;
    } else {
      memcpy(buf.pData, pMsg->pCont, pMsg->contLen);
    }
  }

  pSendInfo->fp(pSendInfo->param, &buf, pMsg->code);
  rpcFreeCont(pMsg->pCont);
  destroySendMsgInfo(pSendInfo);
}

SArray* qGetQueriedTableListInfo(qTaskInfo_t tinfo) {
  SExecTaskInfo* pTaskInfo = tinfo;
  SArray* plist = getTableListInfo(pTaskInfo);

  // only extract table in the first elements
  STableListInfo* pTableListInfo = taosArrayGetP(plist, 0);

  SArray* pUidList = taosArrayInit(10, sizeof(uint64_t));

  int32_t numOfTables = tableListGetSize(pTableListInfo);
  for(int32_t i = 0; i < numOfTables; ++i) {
    STableKeyInfo* pKeyInfo = tableListGetInfo(pTableListInfo, i);
    taosArrayPush(pUidList, &pKeyInfo->uid);
  }

  taosArrayDestroy(plist);
  return pUidList;
}

static void extractTableList(SArray* pList, const SOperatorInfo* pOperator) {
  if (pOperator->operatorType == QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN) {
    SStreamScanInfo* pScanInfo = pOperator->info;
    STableScanInfo*  pTableScanInfo = pScanInfo->pTableScanOp->info;
    taosArrayPush(pList, &pTableScanInfo->base.pTableListInfo);
  } else if (pOperator->operatorType == QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN) {
    STableScanInfo* pScanInfo = pOperator->info;
    taosArrayPush(pList, &pScanInfo->base.pTableListInfo);
  } else {
    if (pOperator->pDownstream != NULL && pOperator->pDownstream[0] != NULL) {
      extractTableList(pList, pOperator->pDownstream[0]);
    }
  }
}

SArray* getTableListInfo(const SExecTaskInfo* pTaskInfo) {
  SArray*        pArray = taosArrayInit(0, POINTER_BYTES);
  SOperatorInfo* pOperator = pTaskInfo->pRoot;
  extractTableList(pArray, pOperator);
  return pArray;
}