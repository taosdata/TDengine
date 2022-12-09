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

int32_t tqAddBlockDataToRsp(const SSDataBlock* pBlock, SMqDataRsp* pRsp, int32_t numOfCols, int8_t precision) {
  int32_t dataStrLen = sizeof(SRetrieveTableRsp) + blockGetEncodeSize(pBlock);
  void*   buf = taosMemoryCalloc(1, dataStrLen);
  if (buf == NULL) return -1;

  SRetrieveTableRsp* pRetrieve = (SRetrieveTableRsp*)buf;
  pRetrieve->useconds = 0;
  pRetrieve->precision = precision;
  pRetrieve->compressed = 0;
  pRetrieve->completed = 1;
  pRetrieve->numOfRows = htobe64((int64_t)pBlock->info.rows);

  int32_t actualLen = blockEncode(pBlock, pRetrieve->data, numOfCols);
  actualLen += sizeof(SRetrieveTableRsp);
  ASSERT(actualLen <= dataStrLen);
  taosArrayPush(pRsp->blockDataLen, &actualLen);
  taosArrayPush(pRsp->blockData, &buf);
  return 0;
}

static int32_t tqAddBlockSchemaToRsp(const STqExecHandle* pExec, SMqDataRsp* pRsp) {
  SSchemaWrapper* pSW = tCloneSSchemaWrapper(pExec->pExecReader->pSchemaWrapper);
  if (pSW == NULL) {
    return -1;
  }
  taosArrayPush(pRsp->blockSchema, &pSW);
  return 0;
}

static int32_t tqAddTbNameToRsp(const STQ* pTq, int64_t uid, SMqDataRsp* pRsp, int32_t n) {
  SMetaReader mr = {0};
  metaReaderInit(&mr, pTq->pVnode->pMeta, 0);
  // TODO add reference to gurantee success
  if (metaGetTableEntryByUidCache(&mr, uid) < 0) {
    metaReaderClear(&mr);
    return -1;
  }
  for (int32_t i = 0; i < n; i++) {
    char* tbName = strdup(mr.me.name);
    taosArrayPush(pRsp->blockTbName, &tbName);
  }
  metaReaderClear(&mr);
  return 0;
}

int32_t tqScanData(STQ* pTq, const STqHandle* pHandle, SMqDataRsp* pRsp, STqOffsetVal* pOffset) {
  const STqExecHandle* pExec = &pHandle->execHandle;
  ASSERT(pExec->subType == TOPIC_SUB_TYPE__COLUMN);

  qTaskInfo_t task = pExec->task;

  if (qStreamPrepareScan(task, pOffset, pHandle->execHandle.subType) < 0) {
    tqDebug("prepare scan failed, return");
    if (pOffset->type == TMQ_OFFSET__LOG) {
      pRsp->rspOffset = *pOffset;
      return 0;
    } else {
      tqOffsetResetToLog(pOffset, pHandle->snapshotVer);
      if (qStreamPrepareScan(task, pOffset, pHandle->execHandle.subType) < 0) {
        tqDebug("prepare scan failed, return");
        pRsp->rspOffset = *pOffset;
        return 0;
      }
    }
  }

  int32_t rowCnt = 0;
  while (1) {
    SSDataBlock* pDataBlock = NULL;
    uint64_t     ts = 0;
    tqDebug("vgId:%d, tmq task start to execute", pTq->pVnode->config.vgId);
    if (qExecTask(task, &pDataBlock, &ts) < 0) {
      ASSERT(0);
    }
    tqDebug("vgId:%d, tmq task executed, get %p", pTq->pVnode->config.vgId, pDataBlock);

    if (pDataBlock == NULL) {
      break;
    }

    tqAddBlockDataToRsp(pDataBlock, pRsp, pExec->numOfCols, pTq->pVnode->config.tsdbCfg.precision);
    pRsp->blockNum++;

    if (pOffset->type == TMQ_OFFSET__SNAPSHOT_DATA) {
      rowCnt += pDataBlock->info.rows;
      if (rowCnt >= 4096) break;
    }
  }

  if (qStreamExtractOffset(task, &pRsp->rspOffset) < 0) {
    ASSERT(0);
    return -1;
  }
  ASSERT(pRsp->rspOffset.type != 0);

  if (pRsp->withTbName) {
    if (pRsp->rspOffset.type == TMQ_OFFSET__LOG) {
      int64_t uid = pExec->pExecReader->msgIter.uid;
      tqAddTbNameToRsp(pTq, uid, pRsp, 1);
    } else {
      pRsp->withTbName = false;
    }
  }
  ASSERT(pRsp->withSchema == false);

  return 0;
}

int32_t tqScanTaosx(STQ* pTq, const STqHandle* pHandle, STaosxRsp* pRsp, SMqMetaRsp* pMetaRsp, STqOffsetVal* pOffset) {
  const STqExecHandle* pExec = &pHandle->execHandle;
  qTaskInfo_t          task = pExec->task;

  if (qStreamPrepareScan(task, pOffset, pHandle->execHandle.subType) < 0) {
    tqDebug("prepare scan failed, return");
    if (pOffset->type == TMQ_OFFSET__LOG) {
      pRsp->rspOffset = *pOffset;
      return 0;
    } else {
      tqOffsetResetToLog(pOffset, pHandle->snapshotVer);
      if (qStreamPrepareScan(task, pOffset, pHandle->execHandle.subType) < 0) {
        tqDebug("prepare scan failed, return");
        pRsp->rspOffset = *pOffset;
        return 0;
      }
    }
  }

  int32_t rowCnt = 0;
  while (1) {
    SSDataBlock* pDataBlock = NULL;
    uint64_t     ts = 0;
    tqDebug("tmqsnap task start to execute");
    if (qExecTask(task, &pDataBlock, &ts) < 0) {
      ASSERT(0);
    }
    tqDebug("tmqsnap task execute end, get %p", pDataBlock);

    if (pDataBlock != NULL) {
      if (pRsp->withTbName) {
        int64_t uid = 0;
        if (pOffset->type == TMQ_OFFSET__LOG) {
          uid = pExec->pExecReader->msgIter.uid;
          if (tqAddTbNameToRsp(pTq, uid, (SMqDataRsp*)pRsp, 1) < 0) {
            continue;
          }
        } else {
          char* tbName = strdup(qExtractTbnameFromTask(task));
          taosArrayPush(pRsp->blockTbName, &tbName);
        }
      }
      if (pRsp->withSchema) {
        if (pOffset->type == TMQ_OFFSET__LOG) {
          tqAddBlockSchemaToRsp(pExec, (SMqDataRsp*)pRsp);
        } else {
          SSchemaWrapper* pSW = tCloneSSchemaWrapper(qExtractSchemaFromTask(task));
          taosArrayPush(pRsp->blockSchema, &pSW);
        }
      }

      tqAddBlockDataToRsp(pDataBlock, (SMqDataRsp*)pRsp, taosArrayGetSize(pDataBlock->pDataBlock),
                          pTq->pVnode->config.tsdbCfg.precision);
      pRsp->blockNum++;
      if (pOffset->type == TMQ_OFFSET__LOG) {
        continue;
      } else {
        rowCnt += pDataBlock->info.rows;
        if (rowCnt <= 4096) continue;
      }
    }

    if (pDataBlock == NULL && pOffset->type == TMQ_OFFSET__SNAPSHOT_DATA) {
      if (qStreamExtractPrepareUid(task) != 0) {
        continue;
      }
      tqDebug("tmqsnap vgId: %d, tsdb consume over, switch to wal, ver %" PRId64, TD_VID(pTq->pVnode),
              pHandle->snapshotVer + 1);
      break;
    }

    if (pRsp->blockNum > 0) {
      tqDebug("tmqsnap task exec exited, get data");
      break;
    }

    SMqMetaRsp* tmp = qStreamExtractMetaMsg(task);
    if (tmp->rspOffset.type == TMQ_OFFSET__SNAPSHOT_DATA) {
      tqOffsetResetToData(pOffset, tmp->rspOffset.uid, tmp->rspOffset.ts);
      qStreamPrepareScan(task, pOffset, pHandle->execHandle.subType);
      tmp->rspOffset.type = TMQ_OFFSET__SNAPSHOT_META;
      tqDebug("tmqsnap task exec change to get data");
      continue;
    }

    *pMetaRsp = *tmp;
    tqDebug("tmqsnap task exec exited, get meta");

    tqDebug("task exec exited");
    break;
  }

  if (qStreamExtractOffset(task, &pRsp->rspOffset) < 0) {
    ASSERT(0);
  }

  ASSERT(pRsp->rspOffset.type != 0);
  return 0;
}

int32_t tqTaosxScanLog(STQ* pTq, STqHandle* pHandle, SSubmitReq* pReq, STaosxRsp* pRsp) {
  STqExecHandle* pExec = &pHandle->execHandle;
  ASSERT(pExec->subType != TOPIC_SUB_TYPE__COLUMN);

  SArray* pBlocks = taosArrayInit(0, sizeof(SSDataBlock));
  SArray* pSchemas = taosArrayInit(0, sizeof(void*));

  if (pExec->subType == TOPIC_SUB_TYPE__TABLE) {
    STqReader* pReader = pExec->pExecReader;
    tqReaderSetDataMsg(pReader, pReq, 0);
    while (tqNextDataBlock(pReader)) {
      /*SSDataBlock block = {0};*/
      /*if (tqRetrieveDataBlock(&block, pReader) < 0) {*/
      /*if (terrno == TSDB_CODE_TQ_TABLE_SCHEMA_NOT_FOUND) continue;*/
      /*}*/

      taosArrayClear(pBlocks);
      taosArrayClear(pSchemas);
      if (tqRetrieveTaosxBlock(pReader, pBlocks, pSchemas) < 0) {
        if (terrno == TSDB_CODE_TQ_TABLE_SCHEMA_NOT_FOUND) continue;
      }
      if (pRsp->withTbName) {
        int64_t uid = pExec->pExecReader->msgIter.uid;
        if (tqAddTbNameToRsp(pTq, uid, (SMqDataRsp*)pRsp, taosArrayGetSize(pBlocks)) < 0) {
          taosArrayDestroyEx(pBlocks, (FDelete)blockDataFreeRes);
          taosArrayDestroyP(pSchemas, (FDelete)tDeleteSSchemaWrapper);
          pBlocks = taosArrayInit(0, sizeof(SSDataBlock));
          pSchemas = taosArrayInit(0, sizeof(void*));
          continue;
        }
      }
      if (pHandle->fetchMeta) {
        SSubmitBlk* pBlk = pReader->pBlock;
        int32_t     schemaLen = htonl(pBlk->schemaLen);
        if (schemaLen > 0) {
          if (pRsp->createTableNum == 0) {
            pRsp->createTableLen = taosArrayInit(0, sizeof(int32_t));
            pRsp->createTableReq = taosArrayInit(0, sizeof(void*));
          }
          void* createReq = taosMemoryCalloc(1, schemaLen);
          memcpy(createReq, pBlk->data, schemaLen);
          taosArrayPush(pRsp->createTableLen, &schemaLen);
          taosArrayPush(pRsp->createTableReq, &createReq);
          pRsp->createTableNum++;
        }
      }
      for (int32_t i = 0; i < taosArrayGetSize(pBlocks); i++) {
        SSDataBlock* pBlock = taosArrayGet(pBlocks, i);
        tqAddBlockDataToRsp(pBlock, (SMqDataRsp*)pRsp, taosArrayGetSize(pBlock->pDataBlock),
                            pTq->pVnode->config.tsdbCfg.precision);
        blockDataFreeRes(pBlock);
        SSchemaWrapper* pSW = taosArrayGetP(pSchemas, i);
        taosArrayPush(pRsp->blockSchema, &pSW);
        pRsp->blockNum++;
      }
    }
  } else if (pExec->subType == TOPIC_SUB_TYPE__DB) {
    STqReader* pReader = pExec->pExecReader;
    tqReaderSetDataMsg(pReader, pReq, 0);
    while (tqNextDataBlockFilterOut(pReader, pExec->execDb.pFilterOutTbUid)) {
      /*SSDataBlock block = {0};*/
      /*if (tqRetrieveDataBlock(&block, pReader) < 0) {*/
      /*if (terrno == TSDB_CODE_TQ_TABLE_SCHEMA_NOT_FOUND) continue;*/
      /*}*/
      taosArrayClear(pBlocks);
      taosArrayClear(pSchemas);
      if (tqRetrieveTaosxBlock(pReader, pBlocks, pSchemas) < 0) {
        if (terrno == TSDB_CODE_TQ_TABLE_SCHEMA_NOT_FOUND) continue;
      }
      if (pRsp->withTbName) {
        int64_t uid = pExec->pExecReader->msgIter.uid;
        if (tqAddTbNameToRsp(pTq, uid, (SMqDataRsp*)pRsp, taosArrayGetSize(pBlocks)) < 0) {
          taosArrayDestroyEx(pBlocks, (FDelete)blockDataFreeRes);
          taosArrayDestroyP(pSchemas, (FDelete)tDeleteSSchemaWrapper);
          pBlocks = taosArrayInit(0, sizeof(SSDataBlock));
          pSchemas = taosArrayInit(0, sizeof(void*));
          continue;
        }
      }
      if (pHandle->fetchMeta) {
        SSubmitBlk* pBlk = pReader->pBlock;
        int32_t     schemaLen = htonl(pBlk->schemaLen);
        if (schemaLen > 0) {
          if (pRsp->createTableNum == 0) {
            pRsp->createTableLen = taosArrayInit(0, sizeof(int32_t));
            pRsp->createTableReq = taosArrayInit(0, sizeof(void*));
          }
          void* createReq = taosMemoryCalloc(1, schemaLen);
          memcpy(createReq, pBlk->data, schemaLen);
          taosArrayPush(pRsp->createTableLen, &schemaLen);
          taosArrayPush(pRsp->createTableReq, &createReq);
          pRsp->createTableNum++;
        }
      }
      /*tqAddBlockDataToRsp(&block, (SMqDataRsp*)pRsp, taosArrayGetSize(block.pDataBlock),*/
      /*pTq->pVnode->config.tsdbCfg.precision);*/
      /*blockDataFreeRes(&block);*/
      /*tqAddBlockSchemaToRsp(pExec, (SMqDataRsp*)pRsp);*/
      /*pRsp->blockNum++;*/
      for (int32_t i = 0; i < taosArrayGetSize(pBlocks); i++) {
        SSDataBlock* pBlock = taosArrayGet(pBlocks, i);
        tqAddBlockDataToRsp(pBlock, (SMqDataRsp*)pRsp, taosArrayGetSize(pBlock->pDataBlock),
                            pTq->pVnode->config.tsdbCfg.precision);
        blockDataFreeRes(pBlock);
        SSchemaWrapper* pSW = taosArrayGetP(pSchemas, i);
        taosArrayPush(pRsp->blockSchema, &pSW);
        pRsp->blockNum++;
      }
    }
  }

  taosArrayDestroy(pBlocks);
  taosArrayDestroy(pSchemas);

  if (pRsp->blockNum == 0) {
    return -1;
  }

  return 0;
}
