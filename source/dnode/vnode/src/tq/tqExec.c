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

#include "tq.h"

static int32_t tqAddBlockDataToRsp(const SSDataBlock* pBlock, SMqDataRsp* pRsp) {
  int32_t dataStrLen = sizeof(SRetrieveTableRsp) + blockGetEncodeSize(pBlock);
  void*   buf = taosMemoryCalloc(1, dataStrLen);
  if (buf == NULL) return -1;

  SRetrieveTableRsp* pRetrieve = (SRetrieveTableRsp*)buf;
  pRetrieve->useconds = 0;
  pRetrieve->precision = TSDB_DEFAULT_PRECISION;
  pRetrieve->compressed = 0;
  pRetrieve->completed = 1;
  pRetrieve->numOfRows = htonl(pBlock->info.rows);

  // TODO enable compress
  int32_t actualLen = 0;
  blockEncode(pBlock, pRetrieve->data, &actualLen, taosArrayGetSize(pBlock->pDataBlock), false);
  actualLen += sizeof(SRetrieveTableRsp);
  ASSERT(actualLen <= dataStrLen);
  taosArrayPush(pRsp->blockDataLen, &actualLen);
  taosArrayPush(pRsp->blockData, &buf);
  return 0;
}

static int32_t tqAddBlockSchemaToRsp(const STqExecHandle* pExec, int32_t workerId, SMqDataRsp* pRsp) {
  SSchemaWrapper* pSW = tCloneSSchemaWrapper(pExec->pExecReader[workerId]->pSchemaWrapper);
  if (pSW == NULL) {
    return -1;
  }
  taosArrayPush(pRsp->blockSchema, &pSW);
  return 0;
}

static int32_t tqAddTbNameToRsp(const STQ* pTq, int64_t uid, SMqDataRsp* pRsp, int32_t workerId) {
  SMetaReader mr = {0};
  metaReaderInit(&mr, pTq->pVnode->pMeta, 0);
  if (metaGetTableEntryByUid(&mr, uid) < 0) {
    ASSERT(0);
    return -1;
  }
  char* tbName = strdup(mr.me.name);
  taosArrayPush(pRsp->blockTbName, &tbName);
  metaReaderClear(&mr);
  return 0;
}

int32_t tqScanSnapshot(STQ* pTq, const STqExecHandle* pExec, SMqDataRsp* pRsp, STqOffsetVal offset, int32_t workerId) {
  ASSERT(pExec->subType == TOPIC_SUB_TYPE__COLUMN);
  qTaskInfo_t task = pExec->execCol.task[workerId];

  /*if (qStreamScanSnapshot(task) < 0) {*/
  /*ASSERT(0);*/
  /*}*/

  if (qStreamPrepareScan(task, offset.uid, offset.ts) < 0) {
    ASSERT(0);
  }

  int32_t rowCnt = 0;
  while (1) {
    SSDataBlock* pDataBlock = NULL;
    uint64_t     ts = 0;
    if (qExecTask(task, &pDataBlock, &ts) < 0) {
      ASSERT(0);
    }
    if (pDataBlock == NULL) break;

    ASSERT(pDataBlock->info.rows != 0);
    ASSERT(taosArrayGetSize(pDataBlock->pDataBlock) != 0);

    tqAddBlockDataToRsp(pDataBlock, pRsp);

    if (pRsp->withTbName) {
      pRsp->withTbName = 0;
#if 0
      int64_t uid;
      int64_t ts;
      if (qGetStreamScanStatus(task, &uid, &ts) < 0) {
        ASSERT(0);
      }
      tqAddTbNameToRsp(pTq, uid, pRsp, workerId);
#endif
    }
    pRsp->blockNum++;

    rowCnt += pDataBlock->info.rows;
    if (rowCnt >= 4096) break;
  }
  int64_t uid;
  int64_t ts;
  if (qGetStreamScanStatus(task, &uid, &ts) < 0) {
    ASSERT(0);
  }
  tqOffsetResetToData(&pRsp->rspOffset, uid, ts);

  return 0;
}

int32_t tqLogScanExec(STQ* pTq, STqExecHandle* pExec, SSubmitReq* pReq, SMqDataRsp* pRsp, int32_t workerId) {
  if (pExec->subType == TOPIC_SUB_TYPE__COLUMN) {
    qTaskInfo_t task = pExec->execCol.task[workerId];
    ASSERT(task);
    qSetStreamInput(task, pReq, STREAM_INPUT__DATA_SUBMIT, false);
    while (1) {
      SSDataBlock* pDataBlock = NULL;
      uint64_t     ts = 0;
      if (qExecTask(task, &pDataBlock, &ts) < 0) {
        ASSERT(0);
      }
      if (pDataBlock == NULL) break;

      ASSERT(pDataBlock->info.rows != 0);

      tqAddBlockDataToRsp(pDataBlock, pRsp);
      if (pRsp->withTbName) {
        int64_t uid = pExec->pExecReader[workerId]->msgIter.uid;
        tqAddTbNameToRsp(pTq, uid, pRsp, workerId);
      }
      pRsp->blockNum++;
    }
  } else if (pExec->subType == TOPIC_SUB_TYPE__TABLE) {
    pRsp->withSchema = 1;
    SStreamReader* pReader = pExec->pExecReader[workerId];
    tqReadHandleSetMsg(pReader, pReq, 0);
    while (tqNextDataBlock(pReader)) {
      SSDataBlock block = {0};
      if (tqRetrieveDataBlock(&block, pReader) < 0) {
        if (terrno == TSDB_CODE_TQ_TABLE_SCHEMA_NOT_FOUND) continue;
        ASSERT(0);
      }
      tqAddBlockDataToRsp(&block, pRsp);
      if (pRsp->withTbName) {
        int64_t uid = pExec->pExecReader[workerId]->msgIter.uid;
        tqAddTbNameToRsp(pTq, uid, pRsp, workerId);
      }
      tqAddBlockSchemaToRsp(pExec, workerId, pRsp);
      pRsp->blockNum++;
    }
  } else if (pExec->subType == TOPIC_SUB_TYPE__DB) {
    pRsp->withSchema = 1;
    SStreamReader* pReader = pExec->pExecReader[workerId];
    tqReadHandleSetMsg(pReader, pReq, 0);
    while (tqNextDataBlockFilterOut(pReader, pExec->execDb.pFilterOutTbUid)) {
      SSDataBlock block = {0};
      if (tqRetrieveDataBlock(&block, pReader) < 0) {
        if (terrno == TSDB_CODE_TQ_TABLE_SCHEMA_NOT_FOUND) continue;
        ASSERT(0);
      }
      tqAddBlockDataToRsp(&block, pRsp);
      if (pRsp->withTbName) {
        int64_t uid = pExec->pExecReader[workerId]->msgIter.uid;
        tqAddTbNameToRsp(pTq, uid, pRsp, workerId);
      }
      tqAddBlockSchemaToRsp(pExec, workerId, pRsp);
      pRsp->blockNum++;
    }
  }
  if (pRsp->blockNum == 0) {
    pRsp->skipLogNum++;
    return -1;
  }
  return 0;
}
