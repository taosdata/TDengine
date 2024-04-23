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

  pInserter->submitRes.code = code;

  if (code == TSDB_CODE_SUCCESS) {
    pInserter->submitRes.pRsp = taosMemoryCalloc(1, sizeof(SSubmitRsp2));
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
  tsem_post(&pInserter->ready);
  taosMemoryFree(pMsg->pData);
  return TSDB_CODE_SUCCESS;
}

static int32_t sendSubmitRequest(SDataInserterHandle* pInserter, void* pMsg, int32_t msgLen, void* pTransporter,
                                 SEpSet* pEpset) {
  // send the fetch remote task result reques
  SMsgSendInfo* pMsgSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (NULL == pMsgSendInfo) {
    taosMemoryFreeClear(pMsg);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  SSubmitRspParam* pParam = taosMemoryCalloc(1, sizeof(SSubmitRspParam));
  pParam->pInserter = pInserter;

  pMsgSendInfo->param = pParam;
  pMsgSendInfo->paramFreeFp = taosMemoryFree;
  pMsgSendInfo->msgInfo.pData = pMsg;
  pMsgSendInfo->msgInfo.len = msgLen;
  pMsgSendInfo->msgType = TDMT_VND_SUBMIT;
  pMsgSendInfo->fp = inserterCallback;

  int64_t transporterId = 0;
  return asyncSendMsgToServer(pTransporter, pEpset, &transporterId, pMsgSendInfo);
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
      return TSDB_CODE_OUT_OF_MEMORY;
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
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      goto _end;
    }

    if (!(pReq->aSubmitTbData = taosArrayInit(1, sizeof(SSubmitTbData)))) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
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
  bool    updateLastRow = false;
  bool    disorderTs = false;

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
      void*            var = POINTER_SHIFT(pColInfoData->pData, j * pColInfoData->info.bytes);

      switch (pColInfoData->info.type) {
        case TSDB_DATA_TYPE_NCHAR:
        case TSDB_DATA_TYPE_VARBINARY:
        case TSDB_DATA_TYPE_VARCHAR: {  // TSDB_DATA_TYPE_BINARY
          ASSERT(pColInfoData->info.type == pCol->type);
          if (colDataIsNull_s(pColInfoData, j)) {
            SColVal cv = COL_VAL_NULL(pCol->colId, pCol->type);
            taosArrayPush(pVals, &cv);
          } else {
            void*   data = colDataGetVarData(pColInfoData, j);
            SValue  sv = (SValue){.nData = varDataLen(data), .pData = varDataVal(data)};  // address copy, no value
            SColVal cv = COL_VAL_VALUE(pCol->colId, pCol->type, sv);
            taosArrayPush(pVals, &cv);
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
                qError("NULL value for primary key");
                terrno = TSDB_CODE_PAR_INCORRECT_TIMESTAMP_VAL;
                goto _end;
              }
              
              SColVal cv = COL_VAL_NULL(pCol->colId, pCol->type);  // should use pCol->type
              taosArrayPush(pVals, &cv);
            } else {
              if (PRIMARYKEY_TIMESTAMP_COL_ID == pCol->colId) {
                if (*(int64_t*)var == lastTs) {
                  updateLastRow = true;
                } else if (*(int64_t*)var < lastTs) {
                  disorderTs = true;
                } else {
                  lastTs = *(int64_t*)var;
                }
              }

              SValue sv;
              memcpy(&sv.val, var, tDataTypes[pCol->type].bytes);
              SColVal cv = COL_VAL_VALUE(pCol->colId, pCol->type, sv);
              taosArrayPush(pVals, &cv);
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
    if (updateLastRow) {
      updateLastRow = false;
      SRow** lastRow = taosArrayPop(tbData.aRowP);
      tRowDestroy(*lastRow);
      taosArrayPush(tbData.aRowP, &pRow);
    } else {
      taosArrayPush(tbData.aRowP, &pRow);
    }
  }

  if (disorderTs) {
    if ((tRowSort(tbData.aRowP) != TSDB_CODE_SUCCESS) ||
      (terrno = tRowMerge(tbData.aRowP, (STSchema*)pTSchema, 0)) != 0) {
      goto _end;
    }
  }

  taosArrayPush(pReq->aSubmitTbData, &tbData);

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
    taosArrayPush(pInserter->pDataBlocks, &pInput->pData);
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

    tsem_wait(&pInserter->ready);

    if (pInserter->submitRes.code) {
      return pInserter->submitRes.code;
    }
  }

  *pContinue = true;

  return TSDB_CODE_SUCCESS;
}

static void endPut(struct SDataSinkHandle* pHandle, uint64_t useconds) {
  SDataInserterHandle* pInserter = (SDataInserterHandle*)pHandle;
  taosThreadMutexLock(&pInserter->mutex);
  pInserter->queryEnd = true;
  pInserter->useconds = useconds;
  taosThreadMutexUnlock(&pInserter->mutex);
}

static void getDataLength(SDataSinkHandle* pHandle, int64_t* pLen, bool* pQueryEnd) {
  SDataInserterHandle* pDispatcher = (SDataInserterHandle*)pHandle;
  *pLen = pDispatcher->submitRes.affectedRows;
  qDebug("got total affectedRows %" PRId64, *pLen);
}

static int32_t destroyDataSinker(SDataSinkHandle* pHandle) {
  SDataInserterHandle* pInserter = (SDataInserterHandle*)pHandle;
  atomic_sub_fetch_64(&gDataSinkStat.cachedSize, pInserter->cachedSize);
  taosArrayDestroy(pInserter->pDataBlocks);
  taosMemoryFree(pInserter->pSchema);
  taosMemoryFree(pInserter->pParam);
  taosHashCleanup(pInserter->pCols);
  taosThreadMutexDestroy(&pInserter->mutex);
  
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
    terrno = TSDB_CODE_OUT_OF_MEMORY;
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
  int32_t code = pManager->pAPI->metaFn.getTableSchema(inserter->pParam->readHandle->vnode, pInserterNode->tableId, &inserter->pSchema, &suid);
  if (code) {
    terrno = code;
    goto _return;
  }

  if (pInserterNode->stableId != suid) {
    terrno = TSDB_CODE_TDB_INVALID_TABLE_ID;
    goto _return;
  }

  inserter->pDataBlocks = taosArrayInit(1, POINTER_BYTES);
  taosThreadMutexInit(&inserter->mutex, NULL);
  if (NULL == inserter->pDataBlocks) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _return;
  }

  inserter->fullOrderColList = pInserterNode->pCols->length == inserter->pSchema->numOfCols;

  inserter->pCols = taosHashInit(pInserterNode->pCols->length, taosGetDefaultHashFunction(TSDB_DATA_TYPE_SMALLINT),
                                 false, HASH_NO_LOCK);
  SNode*  pNode = NULL;
  int32_t i = 0;
  FOREACH(pNode, pInserterNode->pCols) {
    SColumnNode* pCol = (SColumnNode*)pNode;
    taosHashPut(inserter->pCols, &pCol->colId, sizeof(pCol->colId), &pCol->slotId, sizeof(pCol->slotId));
    if (inserter->fullOrderColList && pCol->colId != inserter->pSchema->columns[i].colId) {
      inserter->fullOrderColList = false;
    }
    ++i;
  }

  tsem_init(&inserter->ready, 0, 0);

  *pHandle = inserter;
  return TSDB_CODE_SUCCESS;

_return:

  if (inserter) {
    destroyDataSinker((SDataSinkHandle*)inserter);
    taosMemoryFree(inserter);
  } else {
    taosMemoryFree(pManager);
  }
  
  return terrno;  
}
