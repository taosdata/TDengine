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

static int32_t tqAddBlockDataToRsp(const SSDataBlock* pBlock, SMqDataRsp* pRsp, int32_t numOfCols, int8_t precision) {
  int32_t code = 0;
  int32_t lino = 0;

  size_t dataEncodeBufSize = blockGetEncodeSize(pBlock);
  int32_t dataStrLen = sizeof(SRetrieveTableRspForTmq) + dataEncodeBufSize;
  void*   buf = taosMemoryCalloc(1, dataStrLen);
  TSDB_CHECK_NULL(buf, code, lino, END, terrno);

  SRetrieveTableRspForTmq* pRetrieve = (SRetrieveTableRspForTmq*)buf;
  pRetrieve->version = 1;
  pRetrieve->precision = precision;
  pRetrieve->compressed = 0;
  pRetrieve->numOfRows = htobe64((int64_t)pBlock->info.rows);

  int32_t actualLen = blockEncode(pBlock, pRetrieve->data, dataEncodeBufSize, numOfCols);
  TSDB_CHECK_CONDITION(actualLen >= 0, code, lino, END, terrno);

  actualLen += sizeof(SRetrieveTableRspForTmq);
  TSDB_CHECK_NULL(taosArrayPush(pRsp->blockDataLen, &actualLen), code, lino, END, terrno);
  TSDB_CHECK_NULL(taosArrayPush(pRsp->blockData, &buf), code, lino, END, terrno);

  buf = NULL;
END:
  if (code != 0){
    tqError("%s failed at line %d with msg:%s", __func__, lino, tstrerror(code));
  }
  taosMemoryFree(buf);
  return code;
}

static int32_t tqAddTbNameToRsp(const STQ* pTq, int64_t uid, SMqDataRsp* pRsp, int32_t n) {
  int32_t    code = TDB_CODE_SUCCESS;
  int32_t    lino = 0;
  SMetaReader mr = {0};

  TSDB_CHECK_NULL(pTq, code, lino, END, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pRsp, code, lino, END, TSDB_CODE_INVALID_PARA);

  metaReaderDoInit(&mr, pTq->pVnode->pMeta, META_READER_LOCK);

  code = metaReaderGetTableEntryByUidCache(&mr, uid);
  TSDB_CHECK_CODE(code, lino, END);

  for (int32_t i = 0; i < n; i++) {
    char* tbName = taosStrdup(mr.me.name);
    TSDB_CHECK_NULL(tbName, code, lino, END, terrno);
    if(taosArrayPush(pRsp->blockTbName, &tbName) == NULL){
      tqError("failed to push tbName to blockTbName:%s, uid:%"PRId64, tbName, uid);
      continue;
    }
    tqDebug("add tbName to response success tbname:%s, uid:%"PRId64, tbName, uid);
  }

END:
  if (code != TSDB_CODE_SUCCESS) {
    tqError("%s failed at %d, failed to add tbName to response:%s, uid:%"PRId64, __FUNCTION__, lino, tstrerror(code), uid);
  }
  metaReaderClear(&mr);
  return code;
}

int32_t getDataBlock(qTaskInfo_t task, const STqHandle* pHandle, int32_t vgId, SSDataBlock** res) {
  if (task == NULL || pHandle == NULL || res == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
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

static int32_t tqProcessReplayRsp(STQ* pTq, STqHandle* pHandle, SMqDataRsp* pRsp, const SMqPollReq* pRequest, SSDataBlock* pDataBlock, qTaskInfo_t task){
  int32_t code = 0;
  int32_t lino = 0;

  if (IS_OFFSET_RESET_TYPE(pRequest->reqOffset.type) && pHandle->block != NULL) {
    blockDataDestroy(pHandle->block);
    pHandle->block = NULL;
  }
  if (pHandle->block == NULL) {
    if (pDataBlock == NULL) {
      goto END;
    }

    STqOffsetVal offset = {0};
    code = qStreamExtractOffset(task, &offset);
    TSDB_CHECK_CODE(code, lino, END);

    pHandle->block = NULL;

    code = createOneDataBlock(pDataBlock, true, &pHandle->block);
    TSDB_CHECK_CODE(code, lino, END);

    pHandle->blockTime = offset.ts;
    tOffsetDestroy(&offset);
    int32_t vgId = TD_VID(pTq->pVnode);
    code = getDataBlock(task, pHandle, vgId, &pDataBlock);
    TSDB_CHECK_CODE(code, lino, END);
  }

  const STqExecHandle* pExec = &pHandle->execHandle;
  code = tqAddBlockDataToRsp(pHandle->block, pRsp, pExec->numOfCols, pTq->pVnode->config.tsdbCfg.precision);
  TSDB_CHECK_CODE(code, lino, END);

  pRsp->blockNum++;
  if (pDataBlock == NULL) {
    blockDataDestroy(pHandle->block);
    pHandle->block = NULL;
  } else {
    code = copyDataBlock(pHandle->block, pDataBlock);
    TSDB_CHECK_CODE(code, lino, END);

    STqOffsetVal offset = {0};
    code = qStreamExtractOffset(task, &offset);
    TSDB_CHECK_CODE(code, lino, END);

    pRsp->sleepTime = offset.ts - pHandle->blockTime;
    pHandle->blockTime = offset.ts;
    tOffsetDestroy(&offset);
  }

END:
  if (code != TSDB_CODE_SUCCESS) {
    tqError("%s failed at %d, failed to process replay response:%s", __FUNCTION__, lino, tstrerror(code));
  }
  return code;
}

int32_t tqScanData(STQ* pTq, STqHandle* pHandle, SMqDataRsp* pRsp, STqOffsetVal* pOffset, const SMqPollReq* pRequest) {
  int32_t code = 0;
  int32_t lino = 0;
  TSDB_CHECK_NULL(pRsp, code, lino, END, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pTq, code, lino, END, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pHandle, code, lino, END, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pOffset, code, lino, END, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pRequest, code, lino, END, TSDB_CODE_INVALID_PARA);

  int32_t vgId = TD_VID(pTq->pVnode);
  int32_t totalRows = 0;

  const STqExecHandle* pExec = &pHandle->execHandle;
  qTaskInfo_t          task = pExec->task;

  code = qStreamPrepareScan(task, pOffset, pHandle->execHandle.subType);
  TSDB_CHECK_CODE(code, lino, END);

  qStreamSetSourceExcluded(task, pRequest->sourceExcluded);
  int64_t st = taosGetTimestampMs();
  while (1) {
    SSDataBlock* pDataBlock = NULL;
    code = getDataBlock(task, pHandle, vgId, &pDataBlock);
    TSDB_CHECK_CODE(code, lino, END);

    if (pRequest->enableReplay) {
      code = tqProcessReplayRsp(pTq, pHandle, pRsp, pRequest, pDataBlock, task);
      TSDB_CHECK_CODE(code, lino, END);
      break;
    }
    if (pDataBlock == NULL) {
      break;
    }
    code = tqAddBlockDataToRsp(pDataBlock, pRsp, pExec->numOfCols, pTq->pVnode->config.tsdbCfg.precision);
    TSDB_CHECK_CODE(code, lino, END);

    pRsp->blockNum++;
    totalRows += pDataBlock->info.rows;
    if (totalRows >= tmqRowSize || (taosGetTimestampMs() - st > TMIN(TQ_POLL_MAX_TIME, pRequest->timeout))) {
      break;
    }
  }

  tqDebug("consumer:0x%" PRIx64 " vgId:%d tmq task executed finished, total blocks:%d, totalRows:%d", pHandle->consumerId, vgId, pRsp->blockNum, totalRows);
  code = qStreamExtractOffset(task, &pRsp->rspOffset);

END:
  if (code != 0) {
    tqError("%s failed at %d, tmq task executed error msg:%s", __FUNCTION__, lino, tstrerror(code));
  }
  return code;
}

int32_t tqScanTaosx(STQ* pTq, const STqHandle* pHandle, SMqDataRsp* pRsp, SMqBatchMetaRsp* pBatchMetaRsp, STqOffsetVal* pOffset, int64_t timeout) {
  int32_t code = 0;
  int32_t lino = 0;
  char* tbName = NULL;
  SSchemaWrapper* pSW = NULL;
  const STqExecHandle* pExec = &pHandle->execHandle;
  qTaskInfo_t          task = pExec->task;
  code = qStreamPrepareScan(task, pOffset, pHandle->execHandle.subType);
  TSDB_CHECK_CODE(code, lino, END);

  int32_t rowCnt = 0;
  int64_t st = taosGetTimestampMs();
  while (1) {
    SSDataBlock* pDataBlock = NULL;
    uint64_t     ts = 0;
    tqDebug("tmqsnap task start to execute");
    code = qExecTask(task, &pDataBlock, &ts);
    TSDB_CHECK_CODE(code, lino, END);
    tqDebug("tmqsnap task execute end, get %p", pDataBlock);

    if (pDataBlock != NULL && pDataBlock->info.rows > 0) {
      if (pRsp->withTbName) {
        tbName = taosStrdup(qExtractTbnameFromTask(task));
        TSDB_CHECK_NULL(tbName, code, lino, END, terrno);
        TSDB_CHECK_NULL(taosArrayPush(pRsp->blockTbName, &tbName), code, lino, END, terrno);
        tqDebug("vgId:%d, add tbname:%s to rsp msg", pTq->pVnode->config.vgId, tbName);
        tbName = NULL;
      }
      if (pRsp->withSchema) {
        SSchemaWrapper* pSW = tCloneSSchemaWrapper(qExtractSchemaFromTask(task));
        TSDB_CHECK_NULL(pSW, code, lino, END, terrno);
        TSDB_CHECK_NULL(taosArrayPush(pRsp->blockSchema, &pSW), code, lino, END, terrno);
        pSW = NULL;
      }

      code = tqAddBlockDataToRsp(pDataBlock, pRsp, taosArrayGetSize(pDataBlock->pDataBlock),
                                 pTq->pVnode->config.tsdbCfg.precision);
      TSDB_CHECK_CODE(code, lino, END);

      pRsp->blockNum++;
      rowCnt += pDataBlock->info.rows;
      if (rowCnt <= tmqRowSize && (taosGetTimestampMs() - st <= TMIN(TQ_POLL_MAX_TIME, timeout))) {
        continue;
      }
    }

    // get meta
    SMqBatchMetaRsp* tmp = qStreamExtractMetaMsg(task);
    if (taosArrayGetSize(tmp->batchMetaReq) > 0) {
      code = qStreamExtractOffset(task, &tmp->rspOffset);
      TSDB_CHECK_CODE(code, lino, END);
      *pBatchMetaRsp = *tmp;
      tqDebug("tmqsnap task get meta");
      break;
    }

    if (pDataBlock == NULL) {
      code = qStreamExtractOffset(task, pOffset);
      TSDB_CHECK_CODE(code, lino, END);

      if (pOffset->type == TMQ_OFFSET__SNAPSHOT_DATA) {
        continue;
      }

      tqDebug("tmqsnap vgId: %d, tsdb consume over, switch to wal, ver %" PRId64, TD_VID(pTq->pVnode), pHandle->snapshotVer + 1);
      code = qStreamExtractOffset(task, &pRsp->rspOffset);
      break;
    }

    if (pRsp->blockNum > 0) {
      tqDebug("tmqsnap task exec exited, get data");
      code = qStreamExtractOffset(task, &pRsp->rspOffset);
      break;
    }
  }
  tqDebug("%s:%d success", __FUNCTION__, lino);
END:
  if (code != 0){
    tqError("%s failed at %d, vgId:%d, task exec error since %s", __FUNCTION__ , lino, pTq->pVnode->config.vgId, tstrerror(code));
  }
  taosMemoryFree(pSW);
  taosMemoryFree(tbName);
  return code;
}

static int32_t buildCreateTbInfo(SMqDataRsp* pRsp, SVCreateTbReq* pCreateTbReq){
  int32_t code = 0;
  int32_t lino = 0;
  void*   createReq = NULL;
  TSDB_CHECK_NULL(pRsp, code, lino, END, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pCreateTbReq, code, lino, END, TSDB_CODE_INVALID_PARA);

  if (pRsp->createTableNum == 0) {
    pRsp->createTableLen = taosArrayInit(0, sizeof(int32_t));
    TSDB_CHECK_NULL(pRsp->createTableLen, code, lino, END, terrno);
    pRsp->createTableReq = taosArrayInit(0, sizeof(void*));
    TSDB_CHECK_NULL(pRsp->createTableReq, code, lino, END, terrno);
  }

  uint32_t len = 0;
  tEncodeSize(tEncodeSVCreateTbReq, pCreateTbReq, len, code);
  TSDB_CHECK_CODE(code, lino, END);
  createReq = taosMemoryCalloc(1, len);
  TSDB_CHECK_NULL(createReq, code, lino, END, terrno);

  SEncoder encoder = {0};
  tEncoderInit(&encoder, createReq, len);
  code = tEncodeSVCreateTbReq(&encoder, pCreateTbReq);
  tEncoderClear(&encoder);
  TSDB_CHECK_CODE(code, lino, END);
  TSDB_CHECK_NULL(taosArrayPush(pRsp->createTableLen, &len), code, lino, END, terrno);
  TSDB_CHECK_NULL(taosArrayPush(pRsp->createTableReq, &createReq), code, lino, END, terrno);
  pRsp->createTableNum++;
  tqDebug("build create table info msg success");

END:
  if (code != 0){
    tqError("%s failed at %d, failed to build create table info msg:%s", __FUNCTION__, lino, tstrerror(code));
    taosMemoryFree(createReq);
  }
  return code;
}

static void tqProcessSubData(STQ* pTq, STqHandle* pHandle, SMqDataRsp* pRsp, int32_t* totalRows, int8_t sourceExcluded){
  int32_t code = 0;
  int32_t lino = 0;
  SArray* pBlocks = NULL;
  SArray* pSchemas = NULL;

  STqExecHandle* pExec = &pHandle->execHandle;
  STqReader* pReader = pExec->pTqReader;

  pBlocks = taosArrayInit(0, sizeof(SSDataBlock));
  TSDB_CHECK_NULL(pBlocks, code, lino, END, terrno);
  pSchemas = taosArrayInit(0, sizeof(void*));
  TSDB_CHECK_NULL(pSchemas, code, lino, END, terrno);

  SSubmitTbData* pSubmitTbDataRet = NULL;
  int64_t createTime = INT64_MAX;
  code = tqRetrieveTaosxBlock(pReader, pBlocks, pSchemas, &pSubmitTbDataRet, &createTime);
  TSDB_CHECK_CODE(code, lino, END);
  bool tmp = (pSubmitTbDataRet->flags & sourceExcluded) != 0;
  TSDB_CHECK_CONDITION(!tmp, code, lino, END, TSDB_CODE_SUCCESS);
  if (pRsp->withTbName) {
    int64_t uid = pExec->pTqReader->lastBlkUid;
    code = tqAddTbNameToRsp(pTq, uid, pRsp, taosArrayGetSize(pBlocks));
    TSDB_CHECK_CODE(code, lino, END);
  }
  if (pHandle->fetchMeta != WITH_DATA && pSubmitTbDataRet->pCreateTbReq != NULL) {
    if (pSubmitTbDataRet->ctimeMs - createTime <= 1000) {  // judge if table is already created to avoid sending crateTbReq
      code = buildCreateTbInfo(pRsp, pSubmitTbDataRet->pCreateTbReq);
      TSDB_CHECK_CODE(code, lino, END);
    }
  }
  tmp = (pHandle->fetchMeta == ONLY_META && pSubmitTbDataRet->pCreateTbReq == NULL);
  TSDB_CHECK_CONDITION(!tmp, code, lino, END, TSDB_CODE_SUCCESS);
  for (int32_t i = 0; i < taosArrayGetSize(pBlocks); i++) {
    SSDataBlock* pBlock = taosArrayGet(pBlocks, i);
    if (pBlock == NULL) {
      continue;
    }
    if (tqAddBlockDataToRsp(pBlock, pRsp, taosArrayGetSize(pBlock->pDataBlock), pTq->pVnode->config.tsdbCfg.precision) != 0){
      tqError("vgId:%d, failed to add block to rsp msg", pTq->pVnode->config.vgId);
      continue;
    }
    *totalRows += pBlock->info.rows;
    blockDataFreeRes(pBlock);
    SSchemaWrapper* pSW = taosArrayGetP(pSchemas, i);
    if (taosArrayPush(pRsp->blockSchema, &pSW) == NULL){
      tqError("vgId:%d, failed to add schema to rsp msg", pTq->pVnode->config.vgId);
      continue;
    }
    pRsp->blockNum++;
  }
  tqDebug("vgId:%d, process sub data success, response blocknum:%d, rows:%d", pTq->pVnode->config.vgId, pRsp->blockNum, *totalRows);
END:
  if (code != 0){
    tqError("%s failed at %d, failed to process sub data:%s", __FUNCTION__, lino, tstrerror(code));
    taosArrayDestroyEx(pBlocks, (FDelete)blockDataFreeRes);
    taosArrayDestroyP(pSchemas, (FDelete)tDeleteSchemaWrapper);
  } else {
    taosArrayDestroy(pBlocks);
    taosArrayDestroy(pSchemas);
  }
}

int32_t tqTaosxScanLog(STQ* pTq, STqHandle* pHandle, SPackedData submit, SMqDataRsp* pRsp, int32_t* totalRows, int8_t sourceExcluded) {
  int32_t code = 0;
  int32_t lino = 0;
  TSDB_CHECK_NULL(pRsp, code, lino, END, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pTq, code, lino, END, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pHandle, code, lino, END, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(totalRows, code, lino, END, TSDB_CODE_INVALID_PARA);
  STqExecHandle* pExec = &pHandle->execHandle;
  STqReader* pReader = pExec->pTqReader;
  code = tqReaderSetSubmitMsg(pReader, submit.msgStr, submit.msgLen, submit.ver);
  TSDB_CHECK_CODE(code, lino, END);

  if (pExec->subType == TOPIC_SUB_TYPE__TABLE) {
    while (tqNextBlockImpl(pReader, NULL)) {
      tqProcessSubData(pTq, pHandle, pRsp, totalRows, sourceExcluded);
    }
  } else if (pExec->subType == TOPIC_SUB_TYPE__DB) {
    while (tqNextDataBlockFilterOut(pReader, pExec->execDb.pFilterOutTbUid)) {
      tqProcessSubData(pTq, pHandle, pRsp, totalRows, sourceExcluded);
    }
  }

END:
  if (code != 0){
    tqError("%s failed at %d, failed to scan log:%s", __FUNCTION__, lino, tstrerror(code));
  }
  return code;
}