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

#include "dataSinkInt.h"
#include "dataSinkMgt.h"
#include "executorInt.h"
#include "planner.h"
#include "storageapi.h"
#include "tcompression.h"
#include "tdatablock.h"
#include "tglobal.h"
#include "tqueue.h"

extern SDataSinkStat gDataSinkStat;

typedef struct SSubmitRes {
  int64_t      affectedRows;
  int32_t      code;
  SSubmitRsp2* pRsp;
} SSubmitRes;

typedef struct SDataInserterHandle {
  SDataSinkHandle     sink;
  SDataSinkManager*   pManager;
  STSchema*           pSchema;
  SQueryInserterNode* pNode;
  SSubmitRes          submitRes;
  SInserterParam*     pParam;
  SArray*             pDataBlocks;
  SHashObj*           pCols;
  int32_t             status;
  bool                queryEnd;
  bool                fullOrderColList;
  uint64_t            useconds;
  uint64_t            cachedSize;
  TdThreadMutex       mutex;
  tsem_t              ready;
  bool                explain;
} SDataInserterHandle;

typedef struct SSubmitRspParam {
  SDataInserterHandle* pInserter;
} SSubmitRspParam;

int32_t inserterCallback(void* param, SDataBuf* pMsg, int32_t code) {
  SSubmitRspParam*     pParam = (SSubmitRspParam*)param;
  SDataInserterHandle* pInserter = pParam->pInserter;
  int32_t code2 = 0;

  if (code) {
    pInserter->submitRes.code = code;
  }

  if (code == TSDB_CODE_SUCCESS) {
    pInserter->submitRes.pRsp = taosMemoryCalloc(1, sizeof(SSubmitRsp2));
    if (NULL == pInserter->submitRes.pRsp) {
      pInserter->submitRes.code = terrno;
      goto _return;
    }

    SDecoder coder = {0};
    tDecoderInit(&coder, pMsg->pData, pMsg->len);
    code = tDecodeSSubmitRsp2(&coder, pInserter->submitRes.pRsp);
    if (code) {
      taosMemoryFree(pInserter->submitRes.pRsp);
      pInserter->submitRes.code = code;
      goto _return;
    }

    if (pInserter->submitRes.pRsp->affectedRows > 0) {
      SArray* pCreateTbList = pInserter->submitRes.pRsp->aCreateTbRsp;
      int32_t numOfTables = taosArrayGetSize(pCreateTbList);

      for (int32_t i = 0; i < numOfTables; ++i) {
        SVCreateTbRsp* pRsp = taosArrayGet(pCreateTbList, i);
        if (NULL == pRsp) {
          pInserter->submitRes.code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
          goto _return;
        }
        if (TSDB_CODE_SUCCESS != pRsp->code) {
          code = pRsp->code;
          taosMemoryFree(pInserter->submitRes.pRsp);
          pInserter->submitRes.code = code;
          goto _return;
        }
      }
    }

    pInserter->submitRes.affectedRows += pInserter->submitRes.pRsp->affectedRows;
    qDebug("submit rsp received, affectedRows:%d, total:%" PRId64, pInserter->submitRes.pRsp->affectedRows,
           pInserter->submitRes.affectedRows);
    tDecoderClear(&coder);
    taosMemoryFree(pInserter->submitRes.pRsp);
  }

_return:

  code2 = tsem_post(&pInserter->ready);
  if (code2 < 0) {
    qError("tsem_post inserter ready failed, error:%s", tstrerror(code2));
    if (TSDB_CODE_SUCCESS == code) {
      pInserter->submitRes.code = code2;
    }
  }
  
  taosMemoryFree(pMsg->pData);

  return TSDB_CODE_SUCCESS;
}

static int32_t sendSubmitRequest(SDataInserterHandle* pInserter, void* pMsg, int32_t msgLen, void* pTransporter,
                                 SEpSet* pEpset) {
  // send the fetch remote task result reques
  SMsgSendInfo* pMsgSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (NULL == pMsgSendInfo) {
    taosMemoryFreeClear(pMsg);
    return terrno;
  }

  SSubmitRspParam* pParam = taosMemoryCalloc(1, sizeof(SSubmitRspParam));
  if (NULL == pParam) {
    taosMemoryFreeClear(pMsg);
    taosMemoryFreeClear(pMsgSendInfo);
    return terrno;
  }
  pParam->pInserter = pInserter;

  pMsgSendInfo->param = pParam;
  pMsgSendInfo->paramFreeFp = taosMemoryFree;
  pMsgSendInfo->msgInfo.pData = pMsg;
  pMsgSendInfo->msgInfo.len = msgLen;
  pMsgSendInfo->msgType = TDMT_VND_SUBMIT;
  pMsgSendInfo->fp = inserterCallback;

  return asyncSendMsgToServer(pTransporter, pEpset, NULL, pMsgSendInfo);
}

static int32_t submitReqToMsg(int32_t vgId, SSubmitReq2* pReq, void** pData, int32_t* pLen) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t len = 0;
  void*   pBuf = NULL;
  tEncodeSize(tEncodeSubmitReq, pReq, len, code);
  if (TSDB_CODE_SUCCESS == code) {
    SEncoder encoder;
    len += sizeof(SSubmitReq2Msg);
    pBuf = taosMemoryMalloc(len);
    if (NULL == pBuf) {
      return terrno;
    }
    ((SSubmitReq2Msg*)pBuf)->header.vgId = htonl(vgId);
    ((SSubmitReq2Msg*)pBuf)->header.contLen = htonl(len);
    ((SSubmitReq2Msg*)pBuf)->version = htobe64(1);
    tEncoderInit(&encoder, POINTER_SHIFT(pBuf, sizeof(SSubmitReq2Msg)), len - sizeof(SSubmitReq2Msg));
    code = tEncodeSubmitReq(&encoder, pReq);
    tEncoderClear(&encoder);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pData = pBuf;
    *pLen = len;
  } else {
    taosMemoryFree(pBuf);
  }

  return code;
}

int32_t buildSubmitReqFromBlock(SDataInserterHandle* pInserter, SSubmitReq2** ppReq, const SSDataBlock* pDataBlock,
                                const STSchema* pTSchema, int64_t uid, int32_t vgId, tb_uid_t suid) {
  SSubmitReq2* pReq = *ppReq;
  SArray*      pVals = NULL;
  int32_t      numOfBlks = 0;

  terrno = TSDB_CODE_SUCCESS;

  if (NULL == pReq) {
    if (!(pReq = taosMemoryMalloc(sizeof(SSubmitReq2)))) {
      goto _end;
    }

    if (!(pReq->aSubmitTbData = taosArrayInit(1, sizeof(SSubmitTbData)))) {
      goto _end;
    }
  }

  int32_t colNum = taosArrayGetSize(pDataBlock->pDataBlock);
  int32_t rows = pDataBlock->info.rows;

  SSubmitTbData tbData = {0};
  if (!(tbData.aRowP = taosArrayInit(rows, sizeof(SRow*)))) {
    goto _end;
  }
  tbData.suid = suid;
  tbData.uid = uid;
  tbData.sver = pTSchema->version;

  if (!pVals && !(pVals = taosArrayInit(colNum, sizeof(SColVal)))) {
    taosArrayDestroy(tbData.aRowP);
    goto _end;
  }

  int64_t lastTs = TSKEY_MIN;
  bool    needSortMerge = false;

  for (int32_t j = 0; j < rows; ++j) {  // iterate by row
    taosArrayClear(pVals);

    int32_t offset = 0;
    for (int32_t k = 0; k < pTSchema->numOfCols; ++k) {  // iterate by column
      int16_t         colIdx = k;
      const STColumn* pCol = &pTSchema->columns[k];
      if (!pInserter->fullOrderColList) {
        int16_t* slotId = taosHashGet(pInserter->pCols, &pCol->colId, sizeof(pCol->colId));
        if (NULL == slotId) {
          continue;
        }

        colIdx = *slotId;
      }

      SColumnInfoData* pColInfoData = taosArrayGet(pDataBlock->pDataBlock, colIdx);
      if (NULL == pColInfoData) {
        terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        goto _end;
      }
      void* var = POINTER_SHIFT(pColInfoData->pData, j * pColInfoData->info.bytes);

      switch (pColInfoData->info.type) {
        case TSDB_DATA_TYPE_NCHAR:
        case TSDB_DATA_TYPE_VARBINARY:
        case TSDB_DATA_TYPE_VARCHAR: {  // TSDB_DATA_TYPE_BINARY
          if (pColInfoData->info.type != pCol->type) {
            qError("column:%d type:%d in block dismatch with schema col:%d type:%d", colIdx, pColInfoData->info.type, k,
                   pCol->type);
            terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
            goto _end;
          }
          if (colDataIsNull_s(pColInfoData, j)) {
            SColVal cv = COL_VAL_NULL(pCol->colId, pCol->type);
            if (NULL == taosArrayPush(pVals, &cv)) {
              goto _end;
            }
          } else {
            void*  data = colDataGetVarData(pColInfoData, j);
            SValue sv = (SValue){
                .type = pCol->type, .nData = varDataLen(data), .pData = varDataVal(data)};  // address copy, no value
            SColVal cv = COL_VAL_VALUE(pCol->colId, sv);
            if (NULL == taosArrayPush(pVals, &cv)) {
              goto _end;
            }
          }
          break;
        }
        case TSDB_DATA_TYPE_DECIMAL:
        case TSDB_DATA_TYPE_BLOB:
        case TSDB_DATA_TYPE_JSON:
        case TSDB_DATA_TYPE_MEDIUMBLOB:
          qError("the column type %" PRIi16 " is defined but not implemented yet", pColInfoData->info.type);
          terrno = TSDB_CODE_APP_ERROR;
          goto _end;
          break;
        default:
          if (pColInfoData->info.type < TSDB_DATA_TYPE_MAX && pColInfoData->info.type > TSDB_DATA_TYPE_NULL) {
            if (colDataIsNull_s(pColInfoData, j)) {
              if (PRIMARYKEY_TIMESTAMP_COL_ID == pCol->colId) {
                qError("Primary timestamp column should not be null");
                terrno = TSDB_CODE_PAR_INCORRECT_TIMESTAMP_VAL;
                goto _end;
              }

              SColVal cv = COL_VAL_NULL(pCol->colId, pCol->type);  // should use pCol->type
              if (NULL == taosArrayPush(pVals, &cv)) {
                goto _end;
              }
            } else {
              if (PRIMARYKEY_TIMESTAMP_COL_ID == pCol->colId && !needSortMerge) {
                if (*(int64_t*)var <= lastTs) {
                  needSortMerge = true;
                } else {
                  lastTs = *(int64_t*)var;
                }
              }

              SValue sv = {.type = pCol->type};
              TAOS_MEMCPY(&sv.val, var, tDataTypes[pCol->type].bytes);
              SColVal cv = COL_VAL_VALUE(pCol->colId, sv);
              if (NULL == taosArrayPush(pVals, &cv)) {
                goto _end;
              }
            }
          } else {
            uError("the column type %" PRIi16 " is undefined\n", pColInfoData->info.type);
            terrno = TSDB_CODE_APP_ERROR;
            goto _end;
          }
          break;
      }
    }

    SRow* pRow = NULL;
    if ((terrno = tRowBuild(pVals, pTSchema, &pRow)) < 0) {
      tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
      goto _end;
    }
    if (NULL == taosArrayPush(tbData.aRowP, &pRow)) {
      goto _end;
    }
  }

  if (needSortMerge) {
    if ((tRowSort(tbData.aRowP) != TSDB_CODE_SUCCESS) ||
        (terrno = tRowMerge(tbData.aRowP, (STSchema*)pTSchema, 0)) != 0) {
      goto _end;
    }
  }

  if (NULL == taosArrayPush(pReq->aSubmitTbData, &tbData)) {
    goto _end;
  }

_end:

  taosArrayDestroy(pVals);
  if (terrno != 0) {
    *ppReq = NULL;
    if (pReq) {
      tDestroySubmitReq(pReq, TSDB_MSG_FLG_ENCODE);
      taosMemoryFree(pReq);
    }

    return terrno;
  }
  *ppReq = pReq;

  return TSDB_CODE_SUCCESS;
}

int32_t dataBlocksToSubmitReq(SDataInserterHandle* pInserter, void** pMsg, int32_t* msgLen) {
  const SArray*   pBlocks = pInserter->pDataBlocks;
  const STSchema* pTSchema = pInserter->pSchema;
  int64_t         uid = pInserter->pNode->tableId;
  int64_t         suid = pInserter->pNode->stableId;
  int32_t         vgId = pInserter->pNode->vgId;
  int32_t         sz = taosArrayGetSize(pBlocks);
  int32_t         code = 0;
  SSubmitReq2*    pReq = NULL;

  for (int32_t i = 0; i < sz; i++) {
    SSDataBlock* pDataBlock = taosArrayGetP(pBlocks, i);
    if (NULL == pDataBlock) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    code = buildSubmitReqFromBlock(pInserter, &pReq, pDataBlock, pTSchema, uid, vgId, suid);
    if (code) {
      if (pReq) {
        tDestroySubmitReq(pReq, TSDB_MSG_FLG_ENCODE);
        taosMemoryFree(pReq);
      }

      return code;
    }
  }

  code = submitReqToMsg(vgId, pReq, pMsg, msgLen);
  tDestroySubmitReq(pReq, TSDB_MSG_FLG_ENCODE);
  taosMemoryFree(pReq);

  return code;
}

static int32_t putDataBlock(SDataSinkHandle* pHandle, const SInputData* pInput, bool* pContinue) {
  SDataInserterHandle* pInserter = (SDataInserterHandle*)pHandle;
  if (!pInserter->explain) {
    if (NULL == taosArrayPush(pInserter->pDataBlocks, &pInput->pData)) {
      return terrno;
    }
    void*   pMsg = NULL;
    int32_t msgLen = 0;
    int32_t code = dataBlocksToSubmitReq(pInserter, &pMsg, &msgLen);
    if (code) {
      return code;
    }

    taosArrayClear(pInserter->pDataBlocks);

    code = sendSubmitRequest(pInserter, pMsg, msgLen, pInserter->pParam->readHandle->pMsgCb->clientRpc,
                             &pInserter->pNode->epSet);
    if (code) {
      return code;
    }

    QRY_ERR_RET(tsem_wait(&pInserter->ready));

    if (pInserter->submitRes.code) {
      return pInserter->submitRes.code;
    }
  }

  *pContinue = true;

  return TSDB_CODE_SUCCESS;
}

static void endPut(struct SDataSinkHandle* pHandle, uint64_t useconds) {
  SDataInserterHandle* pInserter = (SDataInserterHandle*)pHandle;
  (void)taosThreadMutexLock(&pInserter->mutex);
  pInserter->queryEnd = true;
  pInserter->useconds = useconds;
  (void)taosThreadMutexUnlock(&pInserter->mutex);
}

static void getDataLength(SDataSinkHandle* pHandle, int64_t* pLen, int64_t* pRawLen, bool* pQueryEnd) {
  SDataInserterHandle* pDispatcher = (SDataInserterHandle*)pHandle;
  *pLen = pDispatcher->submitRes.affectedRows;
  qDebug("got total affectedRows %" PRId64, *pLen);
}

static int32_t destroyDataSinker(SDataSinkHandle* pHandle) {
  SDataInserterHandle* pInserter = (SDataInserterHandle*)pHandle;
  (void)atomic_sub_fetch_64(&gDataSinkStat.cachedSize, pInserter->cachedSize);
  taosArrayDestroy(pInserter->pDataBlocks);
  taosMemoryFree(pInserter->pSchema);
  taosMemoryFree(pInserter->pParam);
  taosHashCleanup(pInserter->pCols);
  (void)taosThreadMutexDestroy(&pInserter->mutex);

  taosMemoryFree(pInserter->pManager);
  return TSDB_CODE_SUCCESS;
}

static int32_t getCacheSize(struct SDataSinkHandle* pHandle, uint64_t* size) {
  SDataInserterHandle* pDispatcher = (SDataInserterHandle*)pHandle;

  *size = atomic_load_64(&pDispatcher->cachedSize);
  return TSDB_CODE_SUCCESS;
}

int32_t createDataInserter(SDataSinkManager* pManager, const SDataSinkNode* pDataSink, DataSinkHandle* pHandle,
                           void* pParam) {
  SDataInserterHandle* inserter = taosMemoryCalloc(1, sizeof(SDataInserterHandle));
  if (NULL == inserter) {
    taosMemoryFree(pParam);
    goto _return;
  }

  SQueryInserterNode* pInserterNode = (SQueryInserterNode*)pDataSink;
  inserter->sink.fPut = putDataBlock;
  inserter->sink.fEndPut = endPut;
  inserter->sink.fGetLen = getDataLength;
  inserter->sink.fGetData = NULL;
  inserter->sink.fDestroy = destroyDataSinker;
  inserter->sink.fGetCacheSize = getCacheSize;
  inserter->pManager = pManager;
  inserter->pNode = pInserterNode;
  inserter->pParam = pParam;
  inserter->status = DS_BUF_EMPTY;
  inserter->queryEnd = false;
  inserter->explain = pInserterNode->explain;

  int64_t suid = 0;
  int32_t code = pManager->pAPI->metaFn.getTableSchema(inserter->pParam->readHandle->vnode, pInserterNode->tableId,
                                                       &inserter->pSchema, &suid);
  if (code) {
    terrno = code;
    goto _return;
  }

  if (pInserterNode->stableId != suid) {
    terrno = TSDB_CODE_TDB_INVALID_TABLE_ID;
    goto _return;
  }

  inserter->pDataBlocks = taosArrayInit(1, POINTER_BYTES);
  if (NULL == inserter->pDataBlocks) {
    goto _return;
  }
  QRY_ERR_JRET(taosThreadMutexInit(&inserter->mutex, NULL));

  inserter->fullOrderColList = pInserterNode->pCols->length == inserter->pSchema->numOfCols;

  inserter->pCols = taosHashInit(pInserterNode->pCols->length, taosGetDefaultHashFunction(TSDB_DATA_TYPE_SMALLINT),
                                 false, HASH_NO_LOCK);
  if (NULL == inserter->pCols) {
    goto _return;
  }

  SNode*  pNode = NULL;
  int32_t i = 0;
  FOREACH(pNode, pInserterNode->pCols) {
    SColumnNode* pCol = (SColumnNode*)pNode;
    QRY_ERR_JRET(taosHashPut(inserter->pCols, &pCol->colId, sizeof(pCol->colId), &pCol->slotId, sizeof(pCol->slotId)));
    if (inserter->fullOrderColList && pCol->colId != inserter->pSchema->columns[i].colId) {
      inserter->fullOrderColList = false;
    }
    ++i;
  }

  QRY_ERR_JRET(tsem_init(&inserter->ready, 0, 0));

  *pHandle = inserter;
  return TSDB_CODE_SUCCESS;

_return:

  if (inserter) {
    (void)destroyDataSinker((SDataSinkHandle*)inserter);
    taosMemoryFree(inserter);
  } else {
    taosMemoryFree(pManager);
  }

  return terrno;
}
