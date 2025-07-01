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
#include "functionMgt.h"
#include "planner.h"
#include "query.h"
#include "querytask.h"
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

typedef struct SSubmitTbDataMsg {
  int32_t vgId;
  int32_t len;
  void*   pData;
} SSubmitTbDataMsg;

static void destroySSubmitTbDataMsg(void* p) {
  if (p == NULL) return;
  SSubmitTbDataMsg* pVg = p;
  taosMemoryFree(pVg->pData);
  taosMemoryFree(pVg);
}

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
  uint64_t            flags;
  TdThreadMutex       mutex;
  tsem_t              ready;
  bool                explain;
  bool                isStbInserter;
  SSchemaWrapper*     pTagSchema;
  const char*         dbFName;
  SHashObj*           dbVgInfoMap;
  SUseDbRsp*          pRsp;
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

void freeUseDbOutput_tmp(void* pOutput) {
  SUseDbOutput* pOut = *(SUseDbOutput**)pOutput;
  if (NULL == pOutput) {
    return;
  }

  if (pOut->dbVgroup) {
    freeVgInfo(pOut->dbVgroup);
  }
  taosMemFree(pOut);
}

static int32_t processUseDbRspForInserter(void* param, SDataBuf* pMsg, int32_t code) {
  int32_t              lino = 0;
  SDataInserterHandle* pInserter = (SDataInserterHandle*)param;

  if (TSDB_CODE_SUCCESS != code) {
    // pInserter->pTaskInfo->code = rpcCvtErrCode(code);
    // if (pInserter->pTaskInfo->code != code) {
    //   qError("load db info rsp received, error:%s, cvted error:%s", tstrerror(code),
    //          tstrerror(pInserter->pTaskInfo->code));
    // } else {
    //   qError("load db info rsp received, error:%s", tstrerror(code));
    // }
    goto _return;
  }

  pInserter->pRsp = taosMemoryMalloc(sizeof(SUseDbRsp));
  QUERY_CHECK_NULL(pInserter->pRsp, code, lino, _return, terrno);

  code = tDeserializeSUseDbRsp(pMsg->pData, (int32_t)pMsg->len, pInserter->pRsp);
  QUERY_CHECK_CODE(code, lino, _return);

  taosMemoryFreeClear(pMsg->pData);

  code = tsem_post(&pInserter->ready);
  QUERY_CHECK_CODE(code, lino, _return);

  return code;
_return:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

static int32_t buildDbVgInfoMapForInserter(SDataInserterHandle* pInserter, SReadHandle* pHandle, const char* dbFName,
                                           SUseDbOutput* output) {
  int32_t    code = TSDB_CODE_SUCCESS;
  int32_t    lino = 0;
  char*      buf1 = NULL;
  SUseDbReq* pReq = NULL;

  pReq = taosMemoryMalloc(sizeof(SUseDbReq));
  QUERY_CHECK_NULL(pReq, code, lino, _return, terrno);

  tstrncpy(pReq->db, dbFName, TSDB_DB_FNAME_LEN);
  QUERY_CHECK_CODE(code, lino, _return);

  int32_t contLen = tSerializeSUseDbReq(NULL, 0, pReq);
  buf1 = taosMemoryCalloc(1, contLen);
  QUERY_CHECK_NULL(buf1, code, lino, _return, terrno);

  int32_t tempRes = tSerializeSUseDbReq(buf1, contLen, pReq);
  if (tempRes < 0) {
    QUERY_CHECK_CODE(terrno, lino, _return);
  }

  SMsgSendInfo* pMsgSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  QUERY_CHECK_NULL(pMsgSendInfo, code, lino, _return, terrno);

  pMsgSendInfo->param = pInserter;
  pMsgSendInfo->msgInfo.pData = buf1;
  pMsgSendInfo->msgInfo.len = contLen;
  pMsgSendInfo->msgType = TDMT_MND_GET_DB_INFO;
  pMsgSendInfo->fp = processUseDbRspForInserter;
  // pMsgSendInfo->requestId = pTaskInfo->id.queryId;

  tsem_init(&pInserter->ready, 0, 0);

  code = asyncSendMsgToServer(pHandle->pMsgCb->clientRpc, &pInserter->pNode->epSet, NULL, pMsgSendInfo);
  QUERY_CHECK_CODE(code, lino, _return);

  code = tsem_wait(&pInserter->ready);
  QUERY_CHECK_CODE(code, lino, _return);

  code = queryBuildUseDbOutput(output, pInserter->pRsp);
  QUERY_CHECK_CODE(code, lino, _return);

_return:
  taosMemoryFree(pReq);
  if (pInserter->pRsp) {
    tFreeSUsedbRsp(pInserter->pRsp);
    taosMemoryFreeClear(pInserter->pRsp);
  }
  return code;
}

int32_t inserterBuildCreateTbReq(SVCreateTbReq* pTbReq, const char* tname, STag* pTag, int64_t suid, const char* sname,
                                 SArray* tagName, uint8_t tagNum, int32_t ttl) {
  pTbReq->type = TD_CHILD_TABLE;
  pTbReq->ctb.pTag = (uint8_t*)pTag;
  pTbReq->name = taosStrdup(tname);
  if (!pTbReq->name) return terrno;
  pTbReq->ctb.suid = suid;
  pTbReq->ctb.tagNum = tagNum;
  if (sname) {
    pTbReq->ctb.stbName = taosStrdup(sname);
    if (!pTbReq->ctb.stbName) return terrno;
  }
  pTbReq->ctb.tagName = taosArrayDup(tagName, NULL);
  if (!pTbReq->ctb.tagName) return terrno;
  pTbReq->ttl = ttl;
  pTbReq->commentLen = -1;

  return TSDB_CODE_SUCCESS;
}

int32_t inserterHashValueComp(void const* lp, void const* rp) {
  uint32_t*    key = (uint32_t*)lp;
  SVgroupInfo* pVg = (SVgroupInfo*)rp;

  if (*key < pVg->hashBegin) {
    return -1;
  } else if (*key > pVg->hashEnd) {
    return 1;
  }

  return 0;
}

int inserterVgInfoComp(const void* lp, const void* rp) {
  SVgroupInfo* pLeft = (SVgroupInfo*)lp;
  SVgroupInfo* pRight = (SVgroupInfo*)rp;
  if (pLeft->hashBegin < pRight->hashBegin) {
    return -1;
  } else if (pLeft->hashBegin > pRight->hashBegin) {
    return 1;
  }

  return 0;
}

int32_t inserterGetVgId(SDBVgInfo* dbInfo, char* tbName, int32_t* vgId) {
  if (NULL == dbInfo) {
    return TSDB_CODE_CTG_INTERNAL_ERROR;
  }

  if (dbInfo->vgHash && NULL == dbInfo->vgArray) {
    int32_t vgSize = taosHashGetSize(dbInfo->vgHash);
    dbInfo->vgArray = taosArrayInit(vgSize, sizeof(SVgroupInfo));
    if (NULL == dbInfo->vgArray) {
      return terrno;
    }

    void* pIter = taosHashIterate(dbInfo->vgHash, NULL);
    while (pIter) {
      if (NULL == taosArrayPush(dbInfo->vgArray, pIter)) {
        taosHashCancelIterate(dbInfo->vgHash, pIter);
        return terrno;
      }

      pIter = taosHashIterate(dbInfo->vgHash, pIter);
    }

    taosArraySort(dbInfo->vgArray, inserterVgInfoComp);
  }

  uint32_t hashValue =
      taosGetTbHashVal(tbName, (int32_t)strlen(tbName), dbInfo->hashMethod, dbInfo->hashPrefix, dbInfo->hashSuffix);
  SVgroupInfo* vgInfo = taosArraySearch(dbInfo->vgArray, &hashValue, inserterHashValueComp, TD_EQ);
  if (NULL == vgInfo) {
    qError("no hash range found for hash value [%u], table:%s, numOfVgId:%d", hashValue, tbName,
           (int32_t)taosArrayGetSize(dbInfo->vgArray));
    return TSDB_CODE_CTG_INTERNAL_ERROR;
  }
  *vgId = vgInfo->vgId;
  return TSDB_CODE_SUCCESS;
}

int32_t inserterGetDbVgInfo(SDataInserterHandle* pInserter, const char* dbFName, SDBVgInfo** dbVgInfo) {
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       line = 0;
  SUseDbOutput* output = NULL;

  // QRY_PARAM_CHECK(dbVgInfo);
  // QRY_PARAM_CHECK(pInserter);
  // QRY_PARAM_CHECK(name);

  if (pInserter->dbVgInfoMap == NULL) {
    pInserter->dbVgInfoMap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
    if (pInserter->dbVgInfoMap == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  SUseDbOutput** find = (SUseDbOutput**)taosHashGet(pInserter->dbVgInfoMap, dbFName, strlen(dbFName));

  if (find == NULL) {
    output = taosMemoryMalloc(sizeof(SUseDbOutput));
    if (output == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    code = buildDbVgInfoMapForInserter(pInserter, pInserter->pParam->readHandle, dbFName, output);
    QUERY_CHECK_CODE(code, line, _return);

    code = taosHashPut(pInserter->dbVgInfoMap, dbFName, strlen(dbFName), &output, POINTER_BYTES);
    QUERY_CHECK_CODE(code, line, _return);
  } else {
    output = *find;
  }

  *dbVgInfo = output->dbVgroup;
  return code;

_return:
  qError("%s failed at line %d since %s", __func__, line, tstrerror(code));
  freeUseDbOutput_tmp(output);
  return code;
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
  pMsgSendInfo->paramFreeFp = taosAutoMemoryFree;
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

int32_t buildSubmitReqFromStbBlock(SDataInserterHandle* pInserter, SHashObj* pHash, const SSDataBlock* pDataBlock,
                                   const STSchema* pTSchema, int64_t uid, int32_t vgId, tb_uid_t suid) {
  SArray* pVals = NULL;
  SArray* pTagVals = NULL;
  int32_t numOfBlks = 0;

  terrno = TSDB_CODE_SUCCESS;

  int32_t colNum = taosArrayGetSize(pDataBlock->pDataBlock);
  int32_t rows = pDataBlock->info.rows;

  if (!pTagVals && !(pTagVals = taosArrayInit(colNum, sizeof(STagVal)))) {
    goto _end;
  }

  if (!pVals && !(pVals = taosArrayInit(colNum, sizeof(SColVal)))) {
    goto _end;
  }

  SDBVgInfo* dbInfo = NULL;
  int32_t    code = inserterGetDbVgInfo(pInserter, pInserter->dbFName, &dbInfo);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    goto _end;
  }

  for (int32_t j = 0; j < rows; ++j) {
    SSubmitTbData tbData = {0};
    if (!(tbData.aRowP = taosArrayInit(rows, sizeof(SRow*)))) {
      goto _end;
    }
    tbData.suid = suid;
    tbData.uid = uid;
    tbData.sver = pTSchema->version;

    int64_t lastTs = TSKEY_MIN;

    taosArrayClear(pVals);

    int32_t offset = 0;
    taosArrayClear(pTagVals);
    tbData.uid = 0;
    tbData.pCreateTbReq = taosMemoryCalloc(1, sizeof(SVCreateTbReq));
    if (NULL == tbData.pCreateTbReq) {
      tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
      goto _end;
    }
    tbData.flags |= SUBMIT_REQ_AUTO_CREATE_TABLE;

    SColumnInfoData* tbname = taosArrayGet(pDataBlock->pDataBlock, 0);
    if (NULL == tbname) {
      terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      qError("Insert into stable must have tbname column");
      tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
      goto _end;
    }
    if (tbname->info.type != TSDB_DATA_TYPE_BINARY) {
      terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      qError("tbname column must be binary");
      tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
      goto _end;
    }

    if (colDataIsNull_s(tbname, j)) {
      qError("insert into stable tbname column is null");
      tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
      goto _end;
    }
    void*   data = colDataGetVarData(tbname, j);
    SValue  sv = (SValue){TSDB_DATA_TYPE_VARCHAR, .nData = varDataLen(data), .pData = varDataVal(data)};
    SColVal cv = COL_VAL_VALUE(0, sv);

    char tbFullName[TSDB_TABLE_FNAME_LEN];
    char tableName[TSDB_TABLE_FNAME_LEN];
    memcpy(tableName, sv.pData, sv.nData);
    tableName[sv.nData] = '\0';

    int32_t len = snprintf(tbFullName, TSDB_TABLE_FNAME_LEN, "%s.%s", pInserter->dbFName, tableName);
    if (len >= TSDB_TABLE_FNAME_LEN) {
      terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      qError("table name too long after format, len:%d, maxLen:%d", len, TSDB_TABLE_FNAME_LEN);
      tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
      goto _end;
    }
    int32_t vgIdForTbName = 0;
    code = inserterGetVgId(dbInfo, tbFullName, &vgIdForTbName);
    if (code != TSDB_CODE_SUCCESS) {
      terrno = code;
      tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
      goto _end;
    }

    SSubmitReq2* pReq = taosHashGet(pHash, &vgIdForTbName, sizeof(int32_t));
    if (pReq == NULL) {
      pReq = taosMemoryCalloc(1, sizeof(SSubmitReq2));
      if (NULL == pReq) {
        tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
        goto _end;
      }

      if (!(pReq->aSubmitTbData = taosArrayInit(1, sizeof(SSubmitTbData)))) {
        tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
        goto _end;
      }
      taosHashPut(pHash, &vgIdForTbName, sizeof(int32_t), pReq, sizeof(SSubmitReq2));
    }

    if (code != TSDB_CODE_SUCCESS) {
      terrno = code;
      tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
      goto _end;
    }
    SArray* TagNames = taosArrayInit(8, TSDB_COL_NAME_LEN);
    if (!TagNames) {
      terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
      goto _end;
    }
    for (int32_t i = 0; i < pInserter->pTagSchema->nCols; ++i) {
      SSchema* tSchema = &pInserter->pTagSchema->pSchema[i];
      int16_t  colIdx = tSchema->colId;
      int16_t* slotId = taosHashGet(pInserter->pCols, &colIdx, sizeof(colIdx));
      if (NULL == slotId) {
        continue;
      }
      if (NULL == taosArrayPush(TagNames, tSchema->name)) {
        tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
        goto _end;
      }

      colIdx = *slotId;
      SColumnInfoData* pColInfoData = taosArrayGet(pDataBlock->pDataBlock, colIdx);
      if (NULL == pColInfoData) {
        terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
        goto _end;
      }
      // void* var = POINTER_SHIFT(pColInfoData->pData, j * pColInfoData->info.bytes);
      switch (pColInfoData->info.type) {
        case TSDB_DATA_TYPE_NCHAR:
        case TSDB_DATA_TYPE_VARBINARY:
        case TSDB_DATA_TYPE_VARCHAR: {
          if (pColInfoData->info.type != tSchema->type) {
            qError("tag:%d type:%d in block dismatch with schema tag:%d type:%d", colIdx, pColInfoData->info.type, i,
                   tSchema->type);
            terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
            tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
            goto _end;
          }
          if (colDataIsNull_s(pColInfoData, j)) {
            continue;
          } else {
            void*   data = colDataGetVarData(pColInfoData, j);
            STagVal tv = (STagVal){
                .cid = tSchema->colId, .type = tSchema->type, .nData = varDataLen(data), .pData = varDataVal(data)};
            if (NULL == taosArrayPush(pTagVals, &tv)) {
              tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
              goto _end;
            }
          }
          break;
        }
        case TSDB_DATA_TYPE_BLOB:
        case TSDB_DATA_TYPE_JSON:
        case TSDB_DATA_TYPE_MEDIUMBLOB:
          qError("the tag type %" PRIi16 " is defined but not implemented yet", pColInfoData->info.type);
          terrno = TSDB_CODE_APP_ERROR;
          tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
          goto _end;
          break;
        default:
          if (pColInfoData->info.type < TSDB_DATA_TYPE_MAX && pColInfoData->info.type > TSDB_DATA_TYPE_NULL) {
            if (colDataIsNull_s(pColInfoData, j)) {
              continue;
            } else {
              void*   data = colDataGetData(pColInfoData, j);
              STagVal tv = {.cid = tSchema->colId, .type = tSchema->type};
              memcpy(&tv.i64, data, tSchema->bytes);
              if (NULL == taosArrayPush(pTagVals, &tv)) {
                tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
                goto _end;
              }
            }
          } else {
            uError("the column type %" PRIi16 " is undefined\n", pColInfoData->info.type);
            terrno = TSDB_CODE_APP_ERROR;
            tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
            goto _end;
          }
          break;
      }
    }
    STag* pTag = NULL;
    code = tTagNew(pTagVals, 1, false, &pTag);
    if (code != TSDB_CODE_SUCCESS) {
      terrno = code;
      qError("failed to create tag, error:%s", tstrerror(code));
      tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
      goto _end;
    }

    code = inserterBuildCreateTbReq(tbData.pCreateTbReq, tableName, pTag, suid, pInserter->pNode->tableName, TagNames,
                                    pInserter->pTagSchema->nCols, TSDB_DEFAULT_TABLE_TTL);
    if (code != TSDB_CODE_SUCCESS) {
      terrno = code;
      qError("failed to build create table request, error:%s", tstrerror(code));
      tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
      goto _end;
    }

    for (int32_t k = 0; k < pTSchema->numOfCols; ++k) {
      int16_t         colIdx = k;
      const STColumn* pCol = &pTSchema->columns[k];
      int16_t*        slotId = taosHashGet(pInserter->pCols, &pCol->colId, sizeof(pCol->colId));
      if (NULL == slotId) {
        continue;
      }
      colIdx = *slotId;

      SColumnInfoData* pColInfoData = taosArrayGet(pDataBlock->pDataBlock, colIdx);
      if (NULL == pColInfoData) {
        terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
        goto _end;
      }
      void* var = POINTER_SHIFT(pColInfoData->pData, j * pColInfoData->info.bytes);

      switch (pColInfoData->info.type) {
        case TSDB_DATA_TYPE_NCHAR:
        case TSDB_DATA_TYPE_VARBINARY:
        case TSDB_DATA_TYPE_VARCHAR: {
          if (pColInfoData->info.type != pCol->type) {
            qError("column:%d type:%d in block dismatch with schema col:%d type:%d", colIdx, pColInfoData->info.type, k,
                   pCol->type);
            terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
            tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
            goto _end;
          }
          if (colDataIsNull_s(pColInfoData, j)) {
            SColVal cv = COL_VAL_NULL(pCol->colId, pCol->type);
            if (NULL == taosArrayPush(pVals, &cv)) {
              tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
              goto _end;
            }
          } else {
            void*  data = colDataGetVarData(pColInfoData, j);
            SValue  sv = (SValue){.type = pCol->type, .nData = varDataLen(data), .pData = varDataVal(data)};
            SColVal cv = COL_VAL_VALUE(pCol->colId, sv);
            if (NULL == taosArrayPush(pVals, &cv)) {
              tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
              goto _end;
            }
          }
          break;
        }
        case TSDB_DATA_TYPE_BLOB:
        case TSDB_DATA_TYPE_JSON:
        case TSDB_DATA_TYPE_MEDIUMBLOB:
          qError("the column type %" PRIi16 " is defined but not implemented yet", pColInfoData->info.type);
          terrno = TSDB_CODE_APP_ERROR;
          tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
          goto _end;
          break;
        default:
          if (pColInfoData->info.type < TSDB_DATA_TYPE_MAX && pColInfoData->info.type > TSDB_DATA_TYPE_NULL) {
            if (colDataIsNull_s(pColInfoData, j)) {
              if (PRIMARYKEY_TIMESTAMP_COL_ID == pCol->colId) {
                qError("Primary timestamp column should not be null");
                terrno = TSDB_CODE_PAR_INCORRECT_TIMESTAMP_VAL;
                tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
                goto _end;
              }

              SColVal cv = COL_VAL_NULL(pCol->colId, pCol->type);
              if (NULL == taosArrayPush(pVals, &cv)) {
                tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
                goto _end;
              }
            } else {
              // if (PRIMARYKEY_TIMESTAMP_COL_ID == pCol->colId && !needSortMerge) {
              //   if (*(int64_t*)var <= lastTs) {
              //     needSortMerge = true;
              //   } else {
              //     lastTs = *(int64_t*)var;
              //   }
              // }

              SValue sv = {.type = pCol->type};
              valueSetDatum(&sv, sv.type, var, tDataTypes[pCol->type].bytes);
              SColVal cv = COL_VAL_VALUE(pCol->colId, sv);
              if (NULL == taosArrayPush(pVals, &cv)) {
                tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
                goto _end;
              }
            }
          } else {
            uError("the column type %" PRIi16 " is undefined\n", pColInfoData->info.type);
            terrno = TSDB_CODE_APP_ERROR;
            tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
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
      tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
      goto _end;
    }

    if (NULL == taosArrayPush(pReq->aSubmitTbData, &tbData)) {
      goto _end;
    }
  }

_end:
  taosArrayDestroy(pTagVals);
  taosArrayDestroy(pVals);

  return terrno;
}

int32_t buildSubmitReqFromBlock(SDataInserterHandle* pInserter, SSubmitReq2** ppReq, const SSDataBlock* pDataBlock,
                                const STSchema* pTSchema, int64_t uid, int32_t vgId, tb_uid_t suid) {
  SSubmitReq2* pReq = *ppReq;
  SArray*      pVals = NULL;
  int32_t      numOfBlks = 0;

  terrno = TSDB_CODE_SUCCESS;

  if (NULL == pReq) {
    if (!(pReq = taosMemoryCalloc(1, sizeof(SSubmitReq2)))) {
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
              valueSetDatum(&sv, sv.type, var, tDataTypes[pCol->type].bytes);
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

int32_t dataBlocksToSubmitReqArray(SDataInserterHandle* pInserter, SArray* pMsgs) {
  const SArray*   pBlocks = pInserter->pDataBlocks;
  const STSchema* pTSchema = pInserter->pSchema;
  int64_t         uid = pInserter->pNode->tableId;
  int64_t         suid = pInserter->pNode->stableId;
  int32_t         vgId = pInserter->pNode->vgId;
  int32_t         sz = taosArrayGetSize(pBlocks);
  int32_t         code = 0;

  SHashObj* pHash = NULL;
  void*     iterator = NULL;

  for (int32_t i = 0; i < sz; i++) {
    SSDataBlock* pDataBlock = taosArrayGetP(pBlocks, i);  // pDataBlock select查询到的结果
    if (NULL == pDataBlock) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    if (pHash == NULL) {
      pHash = taosHashInit(sz * pDataBlock->info.rows, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false,
                           HASH_ENTRY_LOCK);
      if (NULL == pHash) {
        return terrno;
      }
    }
    code = buildSubmitReqFromStbBlock(pInserter, pHash, pDataBlock, pTSchema, uid, vgId, suid);
    if (code != TSDB_CODE_SUCCESS) {
      goto _end;
    }
  }

  size_t keyLen = 0;
  while ((iterator = taosHashIterate(pHash, iterator))) {
    SSubmitReq2* pReq = (SSubmitReq2*)iterator;
    int32_t*     ctbVgId = taosHashGetKey(iterator, &keyLen);

    SSubmitTbDataMsg* pMsg = taosMemoryCalloc(1, sizeof(SSubmitTbDataMsg));
    if (NULL == pMsg) {
      code = terrno;
      goto _end;
    }
    code = submitReqToMsg(*ctbVgId, pReq, &pMsg->pData, &pMsg->len);
    if (code != TSDB_CODE_SUCCESS) {
      goto _end;
    }
    if (NULL == taosArrayPush(pMsgs, &pMsg)) {
      code = terrno;
      goto _end;
    }
  }

_end:
  if (pHash != NULL) {
    while ((iterator = taosHashIterate(pHash, iterator))) {
      SSubmitReq2* pReq = (SSubmitReq2*)iterator;
      if (pReq) {
        tDestroySubmitReq(pReq, TSDB_MSG_FLG_ENCODE);
        // taosMemoryFree(pReq);
      }
    }
    taosHashCleanup(pHash);
  }

  return code;
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
    SSDataBlock* pDataBlock = taosArrayGetP(pBlocks, i);  // pDataBlock select查询到的结果
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
    if (pInserter->isStbInserter) {
      SArray* pMsgs = taosArrayInit(4, sizeof(POINTER_BYTES));
      if (NULL == pMsgs) {
        return terrno;
      }
      int32_t code = dataBlocksToSubmitReqArray(pInserter, pMsgs);
      if (code) {
        taosArrayDestroyP(pMsgs, destroySSubmitTbDataMsg);
        return code;
      }
      taosArrayClear(pInserter->pDataBlocks);
      for (int32_t i = 0; i < taosArrayGetSize(pMsgs); ++i) {
        SSubmitTbDataMsg* pMsg = taosArrayGetP(pMsgs, i);
        code = sendSubmitRequest(pInserter, pMsg->pData, pMsg->len, pInserter->pParam->readHandle->pMsgCb->clientRpc,
                                 &pInserter->pNode->epSet);
        if (code) {
          for (int j = i + 1; j < taosArrayGetSize(pMsgs); ++j) {
            SSubmitTbDataMsg* pMsg2 = taosArrayGetP(pMsgs, j);
            destroySSubmitTbDataMsg(pMsg2);
          }
          taosArrayDestroy(pMsgs);
          return code;
        }
        QRY_ERR_RET(tsem_wait(&pInserter->ready));

        if (pInserter->submitRes.code) {
          for (int j = i + 1; j < taosArrayGetSize(pMsgs); ++j) {
            SSubmitTbDataMsg* pMsg2 = taosArrayGetP(pMsgs, j);
            destroySSubmitTbDataMsg(pMsg2);
          }
          taosArrayDestroy(pMsgs);
          return pInserter->submitRes.code;
        }
      }

      taosArrayDestroy(pMsgs);

    } else {
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
  nodesDestroyNode((SNode *)pInserter->pNode);
  pInserter->pNode = NULL;
  
  (void)taosThreadMutexDestroy(&pInserter->mutex);

  taosMemoryFree(pInserter->pManager);

  if (pInserter->dbVgInfoMap) {
    taosHashCleanup(pInserter->dbVgInfoMap);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t getCacheSize(struct SDataSinkHandle* pHandle, uint64_t* size) {
  SDataInserterHandle* pDispatcher = (SDataInserterHandle*)pHandle;

  *size = atomic_load_64(&pDispatcher->cachedSize);
  return TSDB_CODE_SUCCESS;
}

static int32_t getSinkFlags(struct SDataSinkHandle* pHandle, uint64_t* pFlags) {
  SDataInserterHandle* pDispatcher = (SDataInserterHandle*)pHandle;

  *pFlags = atomic_load_64(&pDispatcher->flags);
  return TSDB_CODE_SUCCESS;
}

int32_t createDataInserter(SDataSinkManager* pManager, SDataSinkNode** ppDataSink, DataSinkHandle* pHandle,
                           void* pParam) {
  SDataSinkNode* pDataSink = *ppDataSink;
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
  inserter->sink.fGetFlags = getSinkFlags;
  inserter->pManager = pManager;
  inserter->pNode = pInserterNode;
  inserter->pParam = pParam;
  inserter->status = DS_BUF_EMPTY;
  inserter->queryEnd = false;
  inserter->explain = pInserterNode->explain;
  *ppDataSink = NULL;

  int64_t suid = 0;
  int32_t code = pManager->pAPI->metaFn.getTableSchema(inserter->pParam->readHandle->vnode, pInserterNode->tableId,
                                                       &inserter->pSchema, &suid, &inserter->pTagSchema);
  if (code) {
    terrno = code;
    goto _return;
  }

  pManager->pAPI->metaFn.getBasicInfo(inserter->pParam->readHandle->vnode, &inserter->dbFName, NULL, NULL, NULL);

  if (pInserterNode->tableType == TSDB_SUPER_TABLE) {
    inserter->isStbInserter = true;
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
  bool    foundTbname = false;
  FOREACH(pNode, pInserterNode->pCols) {
    if (pNode->type == QUERY_NODE_FUNCTION && ((SFunctionNode*)pNode)->funcType == FUNCTION_TYPE_TBNAME) {
      int16_t colId = 0;
      int16_t slotId = 0;
      QRY_ERR_JRET(taosHashPut(inserter->pCols, &colId, sizeof(colId), &slotId, sizeof(slotId)));
      foundTbname = true;
      continue;
    }
    SColumnNode* pCol = (SColumnNode*)pNode;
    QRY_ERR_JRET(taosHashPut(inserter->pCols, &pCol->colId, sizeof(pCol->colId), &pCol->slotId, sizeof(pCol->slotId)));
    if (inserter->fullOrderColList && pCol->colId != inserter->pSchema->columns[i].colId) {
      inserter->fullOrderColList = false;
    }
    ++i;
  }

  if (inserter->isStbInserter && !foundTbname) {
    QRY_ERR_JRET(TSDB_CODE_PAR_TBNAME_ERROR);
  }

  QRY_ERR_JRET(tsem_init(&inserter->ready, 0, 0));

  inserter->dbVgInfoMap = NULL;

  *pHandle = inserter;
  return TSDB_CODE_SUCCESS;

_return:

  if (inserter) {
    (void)destroyDataSinker((SDataSinkHandle*)inserter);
    taosMemoryFree(inserter);
  } else {
    taosMemoryFree(pManager);
  }

  nodesDestroyNode((SNode *)*ppDataSink);
  *ppDataSink = NULL;

  return terrno;
}
