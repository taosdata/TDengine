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
#include "tmsg.h"

#include "executorInt.h"
#include "index.h"
#include "operator.h"
#include "query.h"
#include "querytask.h"
#include "storageapi.h"
#include "thash.h"
#include "ttypes.h"

#define CLEAR_QUERY_STATUS(q, st) ((q)->status &= (~(st)))

SExecTaskInfo* doCreateTask(uint64_t queryId, uint64_t taskId, int32_t vgId, EOPTR_EXEC_MODEL model, SStorageAPI* pAPI) {
  SExecTaskInfo* pTaskInfo = taosMemoryCalloc(1, sizeof(SExecTaskInfo));
  if (pTaskInfo == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  setTaskStatus(pTaskInfo, TASK_NOT_COMPLETED);
  pTaskInfo->cost.created = taosGetTimestampUs();

  pTaskInfo->execModel = model;
  pTaskInfo->stopInfo.pStopInfo = taosArrayInit(4, sizeof(SExchangeOpStopInfo));
  pTaskInfo->pResultBlockList = taosArrayInit(128, POINTER_BYTES);
  pTaskInfo->storageAPI = *pAPI;

  taosInitRWLatch(&pTaskInfo->lock);

  pTaskInfo->id.vgId = vgId;
  pTaskInfo->id.queryId = queryId;
  pTaskInfo->id.taskId = taskId;
  pTaskInfo->id.str = taosMemoryMalloc(64);
  buildTaskId(taskId, queryId, pTaskInfo->id.str);
  pTaskInfo->schemaInfos = taosArrayInit(1, sizeof(SSchemaInfo));
  
  return pTaskInfo;
}

bool isTaskKilled(void* pTaskInfo) { return (0 != ((SExecTaskInfo*)pTaskInfo)->code); }

void setTaskKilled(SExecTaskInfo* pTaskInfo, int32_t rspCode) {
  pTaskInfo->code = rspCode;
  stopTableScanOperator(pTaskInfo->pRoot, pTaskInfo->id.str, &pTaskInfo->storageAPI);
}

void setTaskStatus(SExecTaskInfo* pTaskInfo, int8_t status) {
  if (status == TASK_NOT_COMPLETED) {
    pTaskInfo->status = status;
  } else {
    // QUERY_NOT_COMPLETED is not compatible with any other status, so clear its position first
    CLEAR_QUERY_STATUS(pTaskInfo, TASK_NOT_COMPLETED);
    pTaskInfo->status |= status;
  }
}

int32_t createExecTaskInfo(SSubplan* pPlan, SExecTaskInfo** pTaskInfo, SReadHandle* pHandle, uint64_t taskId,
                           int32_t vgId, char* sql, EOPTR_EXEC_MODEL model) {
  *pTaskInfo = doCreateTask(pPlan->id.queryId, taskId, vgId, model, &pHandle->api);
  if (*pTaskInfo == NULL) {
    taosMemoryFree(sql);
    return terrno;
  }

  if (pHandle) {
    if (pHandle->pStateBackend) {
      (*pTaskInfo)->streamInfo.pState = pHandle->pStateBackend;
    }
  }

  TSWAP((*pTaskInfo)->sql, sql);

  (*pTaskInfo)->pSubplan = pPlan;
  (*pTaskInfo)->pRoot = createOperator(pPlan->pNode, *pTaskInfo, pHandle, pPlan->pTagCond, pPlan->pTagIndexCond,
                                       pPlan->user, pPlan->dbFName);

  if (NULL == (*pTaskInfo)->pRoot) {
    int32_t code = (*pTaskInfo)->code;
    doDestroyTask(*pTaskInfo);
    (*pTaskInfo) = NULL;
    return code;
  } else {
    return TSDB_CODE_SUCCESS;
  }
}

void cleanupQueriedTableScanInfo(void* p) {
  SSchemaInfo* pSchemaInfo = p;
  
  taosMemoryFreeClear(pSchemaInfo->dbname);
  taosMemoryFreeClear(pSchemaInfo->tablename);
  tDeleteSchemaWrapper(pSchemaInfo->sw);
  tDeleteSchemaWrapper(pSchemaInfo->qsw);
}

int32_t initQueriedTableSchemaInfo(SReadHandle* pHandle, SScanPhysiNode* pScanNode, const char* dbName, SExecTaskInfo* pTaskInfo) {
  SMetaReader mr = {0};
  if (pHandle == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  SStorageAPI* pAPI = &pTaskInfo->storageAPI;

  pAPI->metaReaderFn.initReader(&mr, pHandle->vnode, 0, &pAPI->metaFn);
  int32_t code = pAPI->metaReaderFn.getEntryGetUidCache(&mr, pScanNode->uid);
  if (code != TSDB_CODE_SUCCESS) {
    qError("failed to get the table meta, uid:0x%" PRIx64 ", suid:0x%" PRIx64 ", %s", pScanNode->uid, pScanNode->suid,
           GET_TASKID(pTaskInfo));

    pAPI->metaReaderFn.clearReader(&mr);
    return terrno;
  }

  SSchemaInfo schemaInfo = {0};

  schemaInfo.tablename = taosStrdup(mr.me.name);
  schemaInfo.dbname = taosStrdup(dbName);

  if (mr.me.type == TSDB_SUPER_TABLE) {
    schemaInfo.sw = tCloneSSchemaWrapper(&mr.me.stbEntry.schemaRow);
    schemaInfo.tversion = mr.me.stbEntry.schemaTag.version;
  } else if (mr.me.type == TSDB_CHILD_TABLE) {
    tDecoderClear(&mr.coder);

    tb_uid_t suid = mr.me.ctbEntry.suid;
    code = pAPI->metaReaderFn.getEntryGetUidCache(&mr, suid);
    if (code != TSDB_CODE_SUCCESS) {
      pAPI->metaReaderFn.clearReader(&mr);
      taosMemoryFree(schemaInfo.tablename);
      taosMemoryFree(schemaInfo.dbname);
      return terrno;
    }

    schemaInfo.sw = tCloneSSchemaWrapper(&mr.me.stbEntry.schemaRow);
    schemaInfo.tversion = mr.me.stbEntry.schemaTag.version;
  } else {
    schemaInfo.sw = tCloneSSchemaWrapper(&mr.me.ntbEntry.schemaRow);
  }

  pAPI->metaReaderFn.clearReader(&mr);

  schemaInfo.qsw = extractQueriedColumnSchema(pScanNode);
  
  taosArrayPush(pTaskInfo->schemaInfos, &schemaInfo);
  
  return TSDB_CODE_SUCCESS;
}

SSchemaWrapper* extractQueriedColumnSchema(SScanPhysiNode* pScanNode) {
  int32_t numOfCols = LIST_LENGTH(pScanNode->pScanCols);
  int32_t numOfTags = LIST_LENGTH(pScanNode->pScanPseudoCols);

  SSchemaWrapper* pqSw = taosMemoryCalloc(1, sizeof(SSchemaWrapper));
  pqSw->pSchema = taosMemoryCalloc(numOfCols + numOfTags, sizeof(SSchema));

  for (int32_t i = 0; i < numOfCols; ++i) {
    STargetNode* pNode = (STargetNode*)nodesListGetNode(pScanNode->pScanCols, i);
    SColumnNode* pColNode = (SColumnNode*)pNode->pExpr;

    SSchema* pSchema = &pqSw->pSchema[pqSw->nCols++];
    pSchema->colId = pColNode->colId;
    pSchema->type = pColNode->node.resType.type;
    pSchema->bytes = pColNode->node.resType.bytes;
    tstrncpy(pSchema->name, pColNode->colName, tListLen(pSchema->name));
  }

  // this the tags and pseudo function columns, we only keep the tag columns
  for (int32_t i = 0; i < numOfTags; ++i) {
    STargetNode* pNode = (STargetNode*)nodesListGetNode(pScanNode->pScanPseudoCols, i);

    int32_t type = nodeType(pNode->pExpr);
    if (type == QUERY_NODE_COLUMN) {
      SColumnNode* pColNode = (SColumnNode*)pNode->pExpr;

      SSchema* pSchema = &pqSw->pSchema[pqSw->nCols++];
      pSchema->colId = pColNode->colId;
      pSchema->type = pColNode->node.resType.type;
      pSchema->bytes = pColNode->node.resType.bytes;
      tstrncpy(pSchema->name, pColNode->colName, tListLen(pSchema->name));
    }
  }

  return pqSw;
}

static void cleanupStreamInfo(SStreamTaskInfo* pStreamInfo) { tDeleteSchemaWrapper(pStreamInfo->schema); }

static void freeBlock(void* pParam) {
  SSDataBlock* pBlock = *(SSDataBlock**)pParam;
  blockDataDestroy(pBlock);
}

void doDestroyTask(SExecTaskInfo* pTaskInfo) {
  qDebug("%s execTask is freed", GET_TASKID(pTaskInfo));
  destroyOperator(pTaskInfo->pRoot);
  pTaskInfo->pRoot = NULL;

  taosArrayDestroyEx(pTaskInfo->schemaInfos, cleanupQueriedTableScanInfo);
  cleanupStreamInfo(&pTaskInfo->streamInfo);

  if (!pTaskInfo->localFetch.localExec) {
    nodesDestroyNode((SNode*)pTaskInfo->pSubplan);
    pTaskInfo->pSubplan = NULL;
  }

  taosArrayDestroyEx(pTaskInfo->pResultBlockList, freeBlock);
  taosArrayDestroy(pTaskInfo->stopInfo.pStopInfo);
  taosMemoryFreeClear(pTaskInfo->sql);
  taosMemoryFreeClear(pTaskInfo->id.str);
  taosMemoryFreeClear(pTaskInfo);
}

void buildTaskId(uint64_t taskId, uint64_t queryId, char* dst) {
  char* p = dst;

  int32_t offset = 6;
  memcpy(p, "TID:0x", offset);
  offset += tintToHex(taskId, &p[offset]);

  memcpy(&p[offset], " QID:0x", 7);
  offset += 7;
  offset += tintToHex(queryId, &p[offset]);

  p[offset] = 0;
}
