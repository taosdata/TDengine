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
  int32_t dataStrLen = sizeof(SRetrieveTableRspForTmq) + blockGetEncodeSize(pBlock);
  void*   buf = taosMemoryCalloc(1, dataStrLen);
  if (buf == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SRetrieveTableRspForTmq* pRetrieve = (SRetrieveTableRspForTmq*)buf;
  pRetrieve->version = 1;
  pRetrieve->precision = precision;
  pRetrieve->compressed = 0;
  pRetrieve->numOfRows = htobe64((int64_t)pBlock->info.rows);

  int32_t actualLen = blockEncode(pBlock, pRetrieve->data, numOfCols);
  actualLen += sizeof(SRetrieveTableRspForTmq);
  taosArrayPush(pRsp->blockDataLen, &actualLen);
  taosArrayPush(pRsp->blockData, &buf);

  return TSDB_CODE_SUCCESS;
}

static int32_t tqAddBlockSchemaToRsp(const STqExecHandle* pExec, STaosxRsp* pRsp) {
  SSchemaWrapper* pSW = tCloneSSchemaWrapper(pExec->pTqReader->pSchemaWrapper);
  if (pSW == NULL) {
    return -1;
  }
  taosArrayPush(pRsp->blockSchema, &pSW);
  return 0;
}

static int32_t tqAddTbNameToRsp(const STQ* pTq, int64_t uid, STaosxRsp* pRsp, int32_t n) {
  SMetaReader mr = {0};
  metaReaderDoInit(&mr, pTq->pVnode->pMeta, 0);

  // TODO add reference to gurantee success
  if (metaReaderGetTableEntryByUidCache(&mr, uid) < 0) {
    metaReaderClear(&mr);
    return -1;
  }

  for (int32_t i = 0; i < n; i++) {
    char* tbName = taosStrdup(mr.me.name);
    taosArrayPush(pRsp->blockTbName, &tbName);
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
    terrno = code;
    return -1;
  }

  tqDebug("consumer:0x%" PRIx64 " vgId:%d tmq one task end executed, pDataBlock:%p", pHandle->consumerId, vgId, *res);
  return 0;
}

int32_t tqScanData(STQ* pTq, STqHandle* pHandle, SMqDataRsp* pRsp, STqOffsetVal* pOffset, const SMqPollReq* pRequest) {
  const int32_t MAX_ROWS_TO_RETURN = 4096;

  int32_t vgId = TD_VID(pTq->pVnode);
  int32_t code = 0;
  int32_t totalRows = 0;

  const STqExecHandle* pExec = &pHandle->execHandle;
  qTaskInfo_t          task = pExec->task;

  if (qStreamPrepareScan(task, pOffset, pHandle->execHandle.subType) < 0) {
    return -1;
  }

  qStreamSetSourceExcluded(task, pRequest->sourceExcluded);
  while (1) {
    SSDataBlock* pDataBlock = NULL;
    code = getDataBlock(task, pHandle, vgId, &pDataBlock);
    if (code != 0) {
      return code;
    }

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
        qStreamExtractOffset(task, &offset);
        pHandle->block = createOneDataBlock(pDataBlock, true);
        //        pHandle->block = createDataBlock();
        //        copyDataBlock(pHandle->block, pDataBlock);
        pHandle->blockTime = offset.ts;
        code = getDataBlock(task, pHandle, vgId, &pDataBlock);
        if (code != 0) {
          return code;
        }
      }

      code = tqAddBlockDataToRsp(pHandle->block, pRsp, pExec->numOfCols, pTq->pVnode->config.tsdbCfg.precision);
      if (code != TSDB_CODE_SUCCESS) {
        tqError("vgId:%d, failed to add block to rsp msg", vgId);
        return code;
      }

      pRsp->blockNum++;
      if (pDataBlock == NULL) {
        blockDataDestroy(pHandle->block);
        pHandle->block = NULL;
      } else {
        copyDataBlock(pHandle->block, pDataBlock);

        STqOffsetVal offset = {0};
        qStreamExtractOffset(task, &offset);
        pRsp->sleepTime = offset.ts - pHandle->blockTime;
        pHandle->blockTime = offset.ts;
      }
      break;
    } else {
      if (pDataBlock == NULL) {
        break;
      }
      code = tqAddBlockDataToRsp(pDataBlock, pRsp, pExec->numOfCols, pTq->pVnode->config.tsdbCfg.precision);
      if (code != TSDB_CODE_SUCCESS) {
        tqError("vgId:%d, failed to add block to rsp msg", vgId);
        return code;
      }

      pRsp->blockNum++;
      totalRows += pDataBlock->info.rows;
      if (totalRows >= MAX_ROWS_TO_RETURN) {
        break;
      }
    }
  }

  tqDebug("consumer:0x%" PRIx64 " vgId:%d tmq task executed finished, total blocks:%d, totalRows:%d",
          pHandle->consumerId, vgId, pRsp->blockNum, totalRows);
  qStreamExtractOffset(task, &pRsp->rspOffset);
  return 0;
}

int32_t tqScanTaosx(STQ* pTq, const STqHandle* pHandle, STaosxRsp* pRsp, SMqMetaRsp* pMetaRsp, STqOffsetVal* pOffset) {
  const STqExecHandle* pExec = &pHandle->execHandle;
  qTaskInfo_t          task = pExec->task;

  if (qStreamPrepareScan(task, pOffset, pHandle->execHandle.subType) < 0) {
    return -1;
  }

  int32_t rowCnt = 0;
  while (1) {
    SSDataBlock* pDataBlock = NULL;
    uint64_t     ts = 0;
    tqDebug("tmqsnap task start to execute");
    int code = qExecTask(task, &pDataBlock, &ts);
    if (code != 0) {
      tqError("vgId:%d, task exec error since %s", pTq->pVnode->config.vgId, tstrerror(code));
      terrno = code;
      return -1;
    }

    tqDebug("tmqsnap task execute end, get %p", pDataBlock);

    if (pDataBlock != NULL && pDataBlock->info.rows > 0) {
      if (pRsp->withTbName) {
        if (pOffset->type == TMQ_OFFSET__LOG) {
          int64_t uid = pExec->pTqReader->lastBlkUid;
          if (tqAddTbNameToRsp(pTq, uid, pRsp, 1) < 0) {
            continue;
          }
        } else {
          char* tbName = taosStrdup(qExtractTbnameFromTask(task));
          taosArrayPush(pRsp->blockTbName, &tbName);
        }
      }
      if (pRsp->withSchema) {
        if (pOffset->type == TMQ_OFFSET__LOG) {
          tqAddBlockSchemaToRsp(pExec, pRsp);
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

    // get meta
    SMqMetaRsp* tmp = qStreamExtractMetaMsg(task);
    if (tmp->metaRspLen > 0) {
      qStreamExtractOffset(task, &tmp->rspOffset);
      *pMetaRsp = *tmp;

      tqDebug("tmqsnap task get meta");
      break;
    }

    if (pDataBlock == NULL) {
      qStreamExtractOffset(task, pOffset);
      if (pOffset->type == TMQ_OFFSET__SNAPSHOT_DATA) {
        continue;
      }
      tqDebug("tmqsnap vgId: %d, tsdb consume over, switch to wal, ver %" PRId64, TD_VID(pTq->pVnode),
              pHandle->snapshotVer + 1);
      qStreamExtractOffset(task, &pRsp->rspOffset);
      break;
    }

    if (pRsp->blockNum > 0) {
      tqDebug("tmqsnap task exec exited, get data");
      qStreamExtractOffset(task, &pRsp->rspOffset);
      break;
    }
  }

  return 0;
}

int32_t tqTaosxScanLog(STQ* pTq, STqHandle* pHandle, SPackedData submit, STaosxRsp* pRsp, int32_t* totalRows,
                       int8_t sourceExcluded) {
  STqExecHandle* pExec = &pHandle->execHandle;
  SArray*        pBlocks = taosArrayInit(0, sizeof(SSDataBlock));
  SArray*        pSchemas = taosArrayInit(0, sizeof(void*));

  if (pExec->subType == TOPIC_SUB_TYPE__TABLE) {
    STqReader* pReader = pExec->pTqReader;
    tqReaderSetSubmitMsg(pReader, submit.msgStr, submit.msgLen, submit.ver);
    while (tqNextBlockImpl(pReader, NULL)) {
      taosArrayClear(pBlocks);
      taosArrayClear(pSchemas);
      SSubmitTbData* pSubmitTbDataRet = NULL;
      if (tqRetrieveTaosxBlock(pReader, pBlocks, pSchemas, &pSubmitTbDataRet) < 0) {
        if (terrno == TSDB_CODE_TQ_TABLE_SCHEMA_NOT_FOUND) goto loop_table;
      }

      if ((pSubmitTbDataRet->flags & sourceExcluded) != 0) {
        goto loop_table;
      }
      if (pRsp->withTbName) {
        int64_t uid = pExec->pTqReader->lastBlkUid;
        if (tqAddTbNameToRsp(pTq, uid, pRsp, taosArrayGetSize(pBlocks)) < 0) {
          goto loop_table;
        }
      }
      if (pHandle->fetchMeta != WITH_DATA && pSubmitTbDataRet->pCreateTbReq != NULL) {
        if (pRsp->createTableNum == 0) {
          pRsp->createTableLen = taosArrayInit(0, sizeof(int32_t));
          pRsp->createTableReq = taosArrayInit(0, sizeof(void*));
        }

        int32_t  code = TSDB_CODE_SUCCESS;
        uint32_t len = 0;
        tEncodeSize(tEncodeSVCreateTbReq, pSubmitTbDataRet->pCreateTbReq, len, code);
        if (TSDB_CODE_SUCCESS != code) {
          goto loop_table;
        }
        void*    createReq = taosMemoryCalloc(1, len);
        SEncoder encoder = {0};
        tEncoderInit(&encoder, createReq, len);
        code = tEncodeSVCreateTbReq(&encoder, pSubmitTbDataRet->pCreateTbReq);
        if (code < 0) {
          tEncoderClear(&encoder);
          taosMemoryFree(createReq);
          goto loop_table;
        }

        taosArrayPush(pRsp->createTableLen, &len);
        taosArrayPush(pRsp->createTableReq, &createReq);
        pRsp->createTableNum++;

        tEncoderClear(&encoder);
      }
      if (pHandle->fetchMeta == ONLY_META && pSubmitTbDataRet->pCreateTbReq == NULL) {
        goto loop_table;
      }
      for (int32_t i = 0; i < taosArrayGetSize(pBlocks); i++) {
        SSDataBlock* pBlock = taosArrayGet(pBlocks, i);
        tqAddBlockDataToRsp(pBlock, (SMqDataRsp*)pRsp, taosArrayGetSize(pBlock->pDataBlock),
                            pTq->pVnode->config.tsdbCfg.precision);
        totalRows += pBlock->info.rows;
        blockDataFreeRes(pBlock);
        SSchemaWrapper* pSW = taosArrayGetP(pSchemas, i);
        taosArrayPush(pRsp->blockSchema, &pSW);
        pRsp->blockNum++;
      }
      continue;
    loop_table:
      taosArrayDestroyEx(pBlocks, (FDelete)blockDataFreeRes);
      taosArrayDestroyP(pSchemas, (FDelete)tDeleteSchemaWrapper);
      pBlocks = taosArrayInit(0, sizeof(SSDataBlock));
      pSchemas = taosArrayInit(0, sizeof(void*));
    }
  } else if (pExec->subType == TOPIC_SUB_TYPE__DB) {
    STqReader* pReader = pExec->pTqReader;
    tqReaderSetSubmitMsg(pReader, submit.msgStr, submit.msgLen, submit.ver);
    while (tqNextDataBlockFilterOut(pReader, pExec->execDb.pFilterOutTbUid)) {
      taosArrayClear(pBlocks);
      taosArrayClear(pSchemas);
      SSubmitTbData* pSubmitTbDataRet = NULL;
      if (tqRetrieveTaosxBlock(pReader, pBlocks, pSchemas, &pSubmitTbDataRet) < 0) {
        if (terrno == TSDB_CODE_TQ_TABLE_SCHEMA_NOT_FOUND) goto loop_db;
      }

      if ((pSubmitTbDataRet->flags & sourceExcluded) != 0) {
        goto loop_db;
      }
      if (pRsp->withTbName) {
        int64_t uid = pExec->pTqReader->lastBlkUid;
        if (tqAddTbNameToRsp(pTq, uid, pRsp, taosArrayGetSize(pBlocks)) < 0) {
          goto loop_db;
        }
      }
      if (pHandle->fetchMeta != WITH_DATA && pSubmitTbDataRet->pCreateTbReq != NULL) {
        if (pRsp->createTableNum == 0) {
          pRsp->createTableLen = taosArrayInit(0, sizeof(int32_t));
          pRsp->createTableReq = taosArrayInit(0, sizeof(void*));
        }

        int32_t  code = TSDB_CODE_SUCCESS;
        uint32_t len = 0;
        tEncodeSize(tEncodeSVCreateTbReq, pSubmitTbDataRet->pCreateTbReq, len, code);
        if (TSDB_CODE_SUCCESS != code) {
          goto loop_db;
        }
        void*    createReq = taosMemoryCalloc(1, len);
        SEncoder encoder = {0};
        tEncoderInit(&encoder, createReq, len);
        code = tEncodeSVCreateTbReq(&encoder, pSubmitTbDataRet->pCreateTbReq);
        if (code < 0) {
          tEncoderClear(&encoder);
          taosMemoryFree(createReq);
          goto loop_db;
        }

        taosArrayPush(pRsp->createTableLen, &len);
        taosArrayPush(pRsp->createTableReq, &createReq);
        pRsp->createTableNum++;

        tEncoderClear(&encoder);
      }
      if (pHandle->fetchMeta == ONLY_META && pSubmitTbDataRet->pCreateTbReq == NULL) {
        goto loop_db;
      }
      for (int32_t i = 0; i < taosArrayGetSize(pBlocks); i++) {
        SSDataBlock* pBlock = taosArrayGet(pBlocks, i);
        tqAddBlockDataToRsp(pBlock, (SMqDataRsp*)pRsp, taosArrayGetSize(pBlock->pDataBlock),
                            pTq->pVnode->config.tsdbCfg.precision);
        *totalRows += pBlock->info.rows;
        blockDataFreeRes(pBlock);
        SSchemaWrapper* pSW = taosArrayGetP(pSchemas, i);
        taosArrayPush(pRsp->blockSchema, &pSW);
        pRsp->blockNum++;
      }
      continue;
    loop_db:
      taosArrayDestroyEx(pBlocks, (FDelete)blockDataFreeRes);
      taosArrayDestroyP(pSchemas, (FDelete)tDeleteSchemaWrapper);
      pBlocks = taosArrayInit(0, sizeof(SSDataBlock));
      pSchemas = taosArrayInit(0, sizeof(void*));
    }
  }
  taosArrayDestroy(pBlocks);
  taosArrayDestroy(pSchemas);
  return 0;
}
