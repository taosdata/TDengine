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

int32_t tqAddBlockDataToRsp(const SSDataBlock* pBlock, void* pRsp, int32_t numOfCols, int8_t precision) {
  int32_t dataStrLen = sizeof(SRetrieveTableRspForTmq) + blockGetEncodeSize(pBlock);
  void*   buf = taosMemoryCalloc(1, dataStrLen);
  if (buf == NULL) {
    return terrno;
  }

  SRetrieveTableRspForTmq* pRetrieve = (SRetrieveTableRspForTmq*)buf;
  pRetrieve->version = 1;
  pRetrieve->precision = precision;
  pRetrieve->compressed = 0;
  pRetrieve->numOfRows = htobe64((int64_t)pBlock->info.rows);

  int32_t actualLen = blockEncode(pBlock, pRetrieve->data, numOfCols);
  if(actualLen < 0){
    taosMemoryFree(buf);
    return terrno;
  }
  actualLen += sizeof(SRetrieveTableRspForTmq);
  if (taosArrayPush(((SMqDataRspCommon*)pRsp)->blockDataLen, &actualLen) == NULL){
    taosMemoryFree(buf);
    return terrno;
  }
  if (taosArrayPush(((SMqDataRspCommon*)pRsp)->blockData, &buf) == NULL) {
    taosMemoryFree(buf);
    return terrno;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t tqAddBlockSchemaToRsp(const STqExecHandle* pExec, void* pRsp) {
  SSchemaWrapper* pSW = tCloneSSchemaWrapper(pExec->pTqReader->pSchemaWrapper);
  if (pSW == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  if (taosArrayPush(((SMqDataRspCommon*)pRsp)->blockSchema, &pSW) == NULL) {
    return terrno;
  }
  return 0;
}

static int32_t tqAddTbNameToRsp(const STQ* pTq, int64_t uid, void* pRsp, int32_t n) {
  SMetaReader mr = {0};
  metaReaderDoInit(&mr, pTq->pVnode->pMeta, META_READER_LOCK);

  int32_t code = metaReaderGetTableEntryByUidCache(&mr, uid);
  if (code < 0) {
    metaReaderClear(&mr);
    return code;
  }

  for (int32_t i = 0; i < n; i++) {
    char* tbName = taosStrdup(mr.me.name);
    if (tbName == NULL) {
      metaReaderClear(&mr);
      return terrno;
    }
    if(taosArrayPush(((SMqDataRspCommon*)pRsp)->blockTbName, &tbName) == NULL){
      continue;
    }
  }
  metaReaderClear(&mr);
  return 0;
}

int32_t getDataBlock(qTaskInfo_t task, const STqHandle* pHandle, int32_t vgId, SSDataBlock** res) {
  uint64_t ts = 0;
  qStreamSetOpen(task);

  tqDebug("consumer:0x%" PRIx64 " vgId:%d, tmq one task start execute", pHandle->consumerId, vgId);
  int32_t code = qExecTask(task, res, &ts);
  if (code != TSDB_CODE_SUCCESS) {
    tqError("consumer:0x%" PRIx64 " vgId:%d, task exec error since %s", pHandle->consumerId, vgId, tstrerror(code));
    return code;
  }

  tqDebug("consumer:0x%" PRIx64 " vgId:%d tmq one task end executed, pDataBlock:%p", pHandle->consumerId, vgId, *res);
  return 0;
}

int32_t tqScanData(STQ* pTq, STqHandle* pHandle, SMqDataRsp* pRsp, STqOffsetVal* pOffset, const SMqPollReq* pRequest) {
  int32_t vgId = TD_VID(pTq->pVnode);
  int32_t code = 0;
  int32_t line = 0;
  int32_t totalRows = 0;

  const STqExecHandle* pExec = &pHandle->execHandle;
  qTaskInfo_t          task = pExec->task;

  code = qStreamPrepareScan(task, pOffset, pHandle->execHandle.subType);
  TSDB_CHECK_CODE(code, line, END);

  qStreamSetSourceExcluded(task, pRequest->sourceExcluded);
  uint64_t st = taosGetTimestampMs();
  while (1) {
    SSDataBlock* pDataBlock = NULL;
    code = getDataBlock(task, pHandle, vgId, &pDataBlock);
    TSDB_CHECK_CODE(code, line, END);

    if (pRequest->enableReplay) {
      if (IS_OFFSET_RESET_TYPE(pRequest->reqOffset.type) && pHandle->block != NULL) {
        blockDataDestroy(pHandle->block);
        pHandle->block = NULL;
      }
      if (pHandle->block == NULL) {
        if (pDataBlock == NULL) {
          break;
        }

        STqOffsetVal offset = {0};
        code = qStreamExtractOffset(task, &offset);
        TSDB_CHECK_CODE(code, line, END);

        pHandle->block = NULL;

        code = createOneDataBlock(pDataBlock, true, &pHandle->block);
        TSDB_CHECK_CODE(code, line, END);

        pHandle->blockTime = offset.ts;
        tOffsetDestroy(&offset);
        code = getDataBlock(task, pHandle, vgId, &pDataBlock);
        TSDB_CHECK_CODE(code, line, END);
      }

      code = tqAddBlockDataToRsp(pHandle->block, pRsp, pExec->numOfCols, pTq->pVnode->config.tsdbCfg.precision);
      TSDB_CHECK_CODE(code, line, END);

      pRsp->common.blockNum++;
      if (pDataBlock == NULL) {
        blockDataDestroy(pHandle->block);
        pHandle->block = NULL;
      } else {
        code = copyDataBlock(pHandle->block, pDataBlock);
        TSDB_CHECK_CODE(code, line, END);

        STqOffsetVal offset = {0};
        code = qStreamExtractOffset(task, &offset);
        TSDB_CHECK_CODE(code, line, END);

        pRsp->sleepTime = offset.ts - pHandle->blockTime;
        pHandle->blockTime = offset.ts;
        tOffsetDestroy(&offset);
      }
      break;
    } else {
      if (pDataBlock == NULL) {
        break;
      }
      code = tqAddBlockDataToRsp(pDataBlock, pRsp, pExec->numOfCols, pTq->pVnode->config.tsdbCfg.precision);
      TSDB_CHECK_CODE(code, line, END);

      pRsp->common.blockNum++;
      totalRows += pDataBlock->info.rows;
      if (totalRows >= tmqRowSize || (taosGetTimestampMs() - st > 1000)) {
        break;
      }
    }
  }

  tqDebug("consumer:0x%" PRIx64 " vgId:%d tmq task executed finished, total blocks:%d, totalRows:%d",
          pHandle->consumerId, vgId, pRsp->common.blockNum, totalRows);
  code = qStreamExtractOffset(task, &pRsp->common.rspOffset);
END:
  if (code != 0) {
    tqError("consumer:0x%" PRIx64 " vgId:%d tmq task executed error, line:%d code:%d", pHandle->consumerId, vgId, line,
            code);
  }
  return code;
}

int32_t tqScanTaosx(STQ* pTq, const STqHandle* pHandle, STaosxRsp* pRsp, SMqBatchMetaRsp* pBatchMetaRsp, STqOffsetVal* pOffset) {
  const STqExecHandle* pExec = &pHandle->execHandle;
  qTaskInfo_t          task = pExec->task;
  int code = qStreamPrepareScan(task, pOffset, pHandle->execHandle.subType);
  if (code != 0) {
    return code;
  }

  int32_t rowCnt = 0;
  while (1) {
    SSDataBlock* pDataBlock = NULL;
    uint64_t     ts = 0;
    tqDebug("tmqsnap task start to execute");
    code = qExecTask(task, &pDataBlock, &ts);
    if (code != 0) {
      tqError("vgId:%d, task exec error since %s", pTq->pVnode->config.vgId, tstrerror(code));
      return code;
    }

    tqDebug("tmqsnap task execute end, get %p", pDataBlock);

    if (pDataBlock != NULL && pDataBlock->info.rows > 0) {
      if (pRsp->common.withTbName) {
        if (pOffset->type == TMQ_OFFSET__LOG) {
          int64_t uid = pExec->pTqReader->lastBlkUid;
          if (tqAddTbNameToRsp(pTq, uid, pRsp, 1) < 0) {
            tqError("vgId:%d, failed to add tbname to rsp msg", pTq->pVnode->config.vgId);
            continue;
          }
        } else {
          char* tbName = taosStrdup(qExtractTbnameFromTask(task));
          if (tbName == NULL) {
            tqError("vgId:%d, failed to add tbname to rsp msg, null", pTq->pVnode->config.vgId);
            return terrno;
          }
          if (taosArrayPush(pRsp->common.blockTbName, &tbName) == NULL){
            tqError("vgId:%d, failed to add tbname to rsp msg", pTq->pVnode->config.vgId);
            continue;
          }
        }
      }
      if (pRsp->common.withSchema) {
        if (pOffset->type == TMQ_OFFSET__LOG) {
          if (tqAddBlockSchemaToRsp(pExec, pRsp) != 0){
            tqError("vgId:%d, failed to add schema to rsp msg", pTq->pVnode->config.vgId);
            continue;
          }
        } else {
          SSchemaWrapper* pSW = tCloneSSchemaWrapper(qExtractSchemaFromTask(task));
          if(taosArrayPush(pRsp->common.blockSchema, &pSW) == NULL){
            tqError("vgId:%d, failed to add schema to rsp msg", pTq->pVnode->config.vgId);
            continue;
          }
        }
      }

      if (tqAddBlockDataToRsp(pDataBlock, (SMqDataRsp*)pRsp, taosArrayGetSize(pDataBlock->pDataBlock),
                          pTq->pVnode->config.tsdbCfg.precision) != 0) {
        tqError("vgId:%d, failed to add block to rsp msg", pTq->pVnode->config.vgId);
        continue;
      }
      pRsp->common.blockNum++;
      if (pOffset->type == TMQ_OFFSET__LOG) {
        continue;
      } else {
        rowCnt += pDataBlock->info.rows;
        if (rowCnt <= tmqRowSize) continue;
      }
    }

    // get meta
    SMqBatchMetaRsp* tmp = qStreamExtractMetaMsg(task);
    if (taosArrayGetSize(tmp->batchMetaReq) > 0) {
      code = qStreamExtractOffset(task, &tmp->rspOffset);
      if (code) {
        return code;
      }

      *pBatchMetaRsp = *tmp;
      tqDebug("tmqsnap task get meta");
      break;
    }

    if (pDataBlock == NULL) {
      code = qStreamExtractOffset(task, pOffset);
      if (code) {
        break;
      }

      if (pOffset->type == TMQ_OFFSET__SNAPSHOT_DATA) {
        continue;
      }

      tqDebug("tmqsnap vgId: %d, tsdb consume over, switch to wal, ver %" PRId64, TD_VID(pTq->pVnode),
              pHandle->snapshotVer + 1);
      code = qStreamExtractOffset(task, &pRsp->common.rspOffset);
      break;
    }

    if (pRsp->common.blockNum > 0) {
      tqDebug("tmqsnap task exec exited, get data");
      code = qStreamExtractOffset(task, &pRsp->common.rspOffset);
      break;
    }
  }

  return code;
}


static void tqProcessSubData(STQ* pTq, STqHandle* pHandle, STaosxRsp* pRsp, int32_t* totalRows, int8_t sourceExcluded){
  int32_t code = 0;
  STqExecHandle* pExec = &pHandle->execHandle;
  STqReader* pReader = pExec->pTqReader;
  SArray* pBlocks = NULL;
  SArray* pSchemas = NULL;
  pBlocks = taosArrayInit(0, sizeof(SSDataBlock));
  if (pBlocks == NULL) {
    code = terrno;
    goto END;
  }
  pSchemas = taosArrayInit(0, sizeof(void*));
  if(pSchemas == NULL){
    code = terrno;
    goto END;
  }

  SSubmitTbData* pSubmitTbDataRet = NULL;
  code = tqRetrieveTaosxBlock(pReader, pBlocks, pSchemas, &pSubmitTbDataRet);
  if (code != 0) {
    tqError("vgId:%d, failed to retrieve block", pTq->pVnode->config.vgId);
    goto END;
  }

  if ((pSubmitTbDataRet->flags & sourceExcluded) != 0) {
    goto END;
  }
  if (pRsp->common.withTbName) {
    int64_t uid = pExec->pTqReader->lastBlkUid;
    code = tqAddTbNameToRsp(pTq, uid, pRsp, taosArrayGetSize(pBlocks));
    if (code != 0) {
      tqError("vgId:%d, failed to add tbname to rsp msg", pTq->pVnode->config.vgId);
      goto END;
    }
  }
  if (pHandle->fetchMeta != WITH_DATA && pSubmitTbDataRet->pCreateTbReq != NULL) {
    if (pRsp->createTableNum == 0) {
      pRsp->createTableLen = taosArrayInit(0, sizeof(int32_t));
      if (pRsp->createTableLen == NULL) {
        code = terrno;
        goto END;
      }
      pRsp->createTableReq = taosArrayInit(0, sizeof(void*));
      if (pRsp->createTableReq == NULL) {
        code = terrno;
        goto END;
      }
    }

    uint32_t len = 0;
    tEncodeSize(tEncodeSVCreateTbReq, pSubmitTbDataRet->pCreateTbReq, len, code);
    if (TSDB_CODE_SUCCESS != code) {
      goto END;
    }
    void*    createReq = taosMemoryCalloc(1, len);
    if (createReq == NULL){
      code = terrno;
      goto END;
    }
    SEncoder encoder = {0};
    tEncoderInit(&encoder, createReq, len);
    code = tEncodeSVCreateTbReq(&encoder, pSubmitTbDataRet->pCreateTbReq);
    tEncoderClear(&encoder);
    if (code < 0) {
      taosMemoryFree(createReq);
      goto END;
    }
    if (taosArrayPush(pRsp->createTableLen, &len) == NULL){
      taosMemoryFree(createReq);
      goto END;
    }
    if (taosArrayPush(pRsp->createTableReq, &createReq) == NULL){
      taosMemoryFree(createReq);
      goto END;
    }
    pRsp->createTableNum++;
  }
  if (pHandle->fetchMeta == ONLY_META && pSubmitTbDataRet->pCreateTbReq == NULL) {
    goto END;
  }
  for (int32_t i = 0; i < taosArrayGetSize(pBlocks); i++) {
    SSDataBlock* pBlock = taosArrayGet(pBlocks, i);
    if (pBlock == NULL) {
      continue;
    }
    if (tqAddBlockDataToRsp(pBlock, (SMqDataRsp*)pRsp, taosArrayGetSize(pBlock->pDataBlock),
                            pTq->pVnode->config.tsdbCfg.precision) != 0){
      tqError("vgId:%d, failed to add block to rsp msg", pTq->pVnode->config.vgId);
      continue;
    }
    *totalRows += pBlock->info.rows;
    blockDataFreeRes(pBlock);
    SSchemaWrapper* pSW = taosArrayGetP(pSchemas, i);
    if (taosArrayPush(pRsp->common.blockSchema, &pSW) == NULL){
      tqError("vgId:%d, failed to add schema to rsp msg", pTq->pVnode->config.vgId);
      continue;
    }
    pRsp->common.blockNum++;
  }

  taosArrayDestroy(pBlocks);
  taosArrayDestroy(pSchemas);
  return;

END:
  taosArrayDestroyEx(pBlocks, (FDelete)blockDataFreeRes);
  taosArrayDestroyP(pSchemas, (FDelete)tDeleteSchemaWrapper);
}

int32_t tqTaosxScanLog(STQ* pTq, STqHandle* pHandle, SPackedData submit, STaosxRsp* pRsp, int32_t* totalRows,
                       int8_t sourceExcluded) {
  STqExecHandle* pExec = &pHandle->execHandle;
  int32_t        code = 0;
  STqReader* pReader = pExec->pTqReader;
  code = tqReaderSetSubmitMsg(pReader, submit.msgStr, submit.msgLen, submit.ver);
  if (code != 0) {
    return code;
  }

  if (pExec->subType == TOPIC_SUB_TYPE__TABLE) {
    while (tqNextBlockImpl(pReader, NULL)) {
      tqProcessSubData(pTq, pHandle, pRsp, totalRows, sourceExcluded);
    }
  } else if (pExec->subType == TOPIC_SUB_TYPE__DB) {
    while (tqNextDataBlockFilterOut(pReader, pExec->execDb.pFilterOutTbUid)) {
      tqProcessSubData(pTq, pHandle, pRsp, totalRows, sourceExcluded);
    }
  }

  return code;
}
