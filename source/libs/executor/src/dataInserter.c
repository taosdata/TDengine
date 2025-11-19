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

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "dataSinkInt.h"
#include "dataSinkMgt.h"
#include "executor.h"
#include "executorInt.h"
#include "functionMgt.h"
#include "libs/new-stream/stream.h"
#include "osAtomic.h"
#include "osMemPool.h"
#include "osMemory.h"
#include "osSemaphore.h"
#include "planner.h"
#include "query.h"
#include "querytask.h"
#include "storageapi.h"
#include "taoserror.h"
#include "tarray.h"
#include "tcompression.h"
#include "tdatablock.h"
#include "tdataformat.h"
#include "tglobal.h"
#include "thash.h"
#include "tmsg.h"
#include "tqueue.h"

extern SDataSinkStat gDataSinkStat;
SHashObj*            gStreamGrpTableHash = NULL;
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
  void*                putParam;
} SSubmitRspParam;

typedef struct SBuildInsertDataInfo {
  SSubmitTbData  pTbData;
  bool           isFirstBlock;
  bool           isLastBlock;
  int64_t        lastTs;
  bool           needSortMerge;
} SBuildInsertDataInfo;

typedef struct SDropTbCtx {
  SSTriggerDropRequest* req;
  tsem_t                ready;
  int32_t               code;
} SDropTbCtx;
typedef struct SDropTbDataMsg {
  SMsgHead header;
  void*    pData;
} SDropTbDataMsg;

typedef struct SRunnerDropTableInfo {
  SSTriggerDropRequest* pReq;
  int32_t               code;
} SRunnerDropTableInfo;

static int32_t initInsertProcessInfo(SBuildInsertDataInfo* pBuildInsertDataInfo, int32_t rows) {
  pBuildInsertDataInfo->isLastBlock = false;
  pBuildInsertDataInfo->lastTs = TSKEY_MIN;
  pBuildInsertDataInfo->isFirstBlock = true;
  pBuildInsertDataInfo->needSortMerge = false;

  if (!(pBuildInsertDataInfo->pTbData.aRowP = taosArrayInit(rows, sizeof(SRow*)))) {
    return terrno;
  }

  return TSDB_CODE_SUCCESS;
}

static void freeCacheTbInfo(void* pp) {
  if (pp == NULL || *(SInsertTableInfo**)pp == NULL) {
    return;
  }
  SInsertTableInfo* pTbInfo = *(SInsertTableInfo**)pp;
  if (pTbInfo->tbname) {
    taosMemFree(pTbInfo->tbname);
    pTbInfo->tbname = NULL;
  }
  if (pTbInfo->pSchema) {
    tDestroyTSchema(pTbInfo->pSchema);
    pTbInfo->pSchema = NULL;
  }
  taosMemoryFree(pTbInfo);
}

int32_t initInserterGrpInfo() {
  gStreamGrpTableHash = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  if (NULL == gStreamGrpTableHash) {
    qError("failed to create stream group table hash");
    return terrno;
  }
  taosHashSetFreeFp(gStreamGrpTableHash, freeCacheTbInfo);
  return TSDB_CODE_SUCCESS;
}

void destroyInserterGrpInfo() {
  static int8_t destoryGrpInfo = 0;
  int8_t        flag = atomic_val_compare_exchange_8(&destoryGrpInfo, 0, 1);
  if (flag != 0) {
    return;
  }
  if (NULL != gStreamGrpTableHash) {
    taosHashCleanup(gStreamGrpTableHash);
    gStreamGrpTableHash = NULL;
  }
}

static int32_t checkResAndResetTableInfo(const SSubmitRes* pSubmitRes, SInsertTableInfo* res,
                                         bool* pSchemaChaned) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (!pSubmitRes->pRsp) {
    stError("create table response is NULL");
    return TSDB_CODE_MND_STREAM_INTERNAL_ERROR;
  }
  if (pSubmitRes->pRsp->aCreateTbRsp->size < 1) {
    stError("create table response size is less than 1");
    return TSDB_CODE_MND_STREAM_INTERNAL_ERROR;
  }
  SVCreateTbRsp* pCreateTbRsp = taosArrayGet(pSubmitRes->pRsp->aCreateTbRsp, 0);
  if (pCreateTbRsp->code != 0 && pCreateTbRsp->code != TSDB_CODE_TDB_TABLE_ALREADY_EXIST) {
    stError("create table failed, code:%d", pCreateTbRsp->code);
    return pCreateTbRsp->code;
  }
  if (!pCreateTbRsp->pMeta || pCreateTbRsp->pMeta->tuid == 0) {
    stError("create table can not get tuid");
    return TSDB_CODE_MND_STREAM_INTERNAL_ERROR;
  }

  *pSchemaChaned = false;
  res->vgid = pCreateTbRsp->pMeta->vgId;
  res->uid = pCreateTbRsp->pMeta->tuid;

  if (pCreateTbRsp->pMeta->sversion != 0 && res->version != pCreateTbRsp->pMeta->sversion) {
    *pSchemaChaned = true;
  }

  stDebug("inserter callback, uid:%" PRId64 "  vgid: %" PRId64 ", version: %d", res->uid, res->vgid, res->version);

  return TSDB_CODE_SUCCESS;
}

static int32_t createNewInsertTbInfo(const SSubmitRes* pSubmitRes, SInsertTableInfo* pOldInsertTbInfo,
                                     SInsertTableInfo** ppNewInsertTbInfo) {
  SVCreateTbRsp* pCreateTbRsp = taosArrayGet(pSubmitRes->pRsp->aCreateTbRsp, 0);
  if (pCreateTbRsp->code != 0 && pCreateTbRsp->code != TSDB_CODE_TDB_TABLE_ALREADY_EXIST) {
    stError("create table failed, code:%d", pCreateTbRsp->code);
    return pCreateTbRsp->code;
  }

  SInsertTableInfo* res = taosMemoryCalloc(1, sizeof(SInsertTableInfo));
  if (res == NULL) {
    return terrno;
  }
  res->tbname = taosStrdup(pOldInsertTbInfo->tbname);
  if (res->tbname == NULL) {
    taosMemoryFree(res);
    stError("failed to allocate memory for table name");
    return terrno;
  }

  res->vgid = pCreateTbRsp->pMeta->vgId;

  res->uid = pCreateTbRsp->pMeta->tuid;
  res->vgid = pCreateTbRsp->pMeta->vgId;

  res->version = pCreateTbRsp->pMeta->sversion;
  res->pSchema = tBuildTSchema(pCreateTbRsp->pMeta->pSchemas, pCreateTbRsp->pMeta->numOfColumns, res->version);
  if (res->pSchema == NULL) {
    stError("failed to build schema for table:%s, uid:%" PRId64 ", vgid:%" PRId64 ", version:%d", res->tbname, res->uid,
            res->vgid, res->version);
    return terrno;
  }
  *ppNewInsertTbInfo = res;
  return TSDB_CODE_SUCCESS;
}

static int32_t updateInsertGrpTableInfo(SStreamDataInserterInfo* pInserterInfo, const SSubmitRes* pSubmitRes) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  int64_t            key[2] = {pInserterInfo->streamId, pInserterInfo->groupId};
  SInsertTableInfo** ppTbRes = taosHashAcquire(gStreamGrpTableHash, key, sizeof(key));
  if (NULL == ppTbRes || *ppTbRes == NULL) {
    return TSDB_CODE_MND_STREAM_INTERNAL_ERROR;
  }

  bool schemaChanged = false;
  code = checkResAndResetTableInfo(pSubmitRes, *ppTbRes, &schemaChanged);
  QUERY_CHECK_CODE(code, lino, _exit);

  if (schemaChanged) {
    SInsertTableInfo* pNewInfo = NULL;
    code = createNewInsertTbInfo(pSubmitRes, *ppTbRes, &pNewInfo);
    QUERY_CHECK_CODE(code, lino, _exit);

    TAOS_UNUSED(taosHashRemove(gStreamGrpTableHash, key, sizeof(key)));

    code = taosHashPut(gStreamGrpTableHash, key, sizeof(key), &pNewInfo, sizeof(SInsertTableInfo*));

    if (code == TSDB_CODE_DUP_KEY) {
      freeCacheTbInfo(&pNewInfo);
      code = TSDB_CODE_SUCCESS;
      goto _exit;
    } else if (code != TSDB_CODE_SUCCESS) {
      freeCacheTbInfo(&pNewInfo);
      stError("failed to put new insert tbInfo for streamId:%" PRIx64 ", groupId:%" PRIx64 ", code:%d",
              pInserterInfo->streamId, pInserterInfo->groupId, code);
      QUERY_CHECK_CODE(code, lino, _exit);
    }

    stInfo("update table info for streamId:%" PRIx64 ", groupId:%" PRIx64 ", uid:%" PRId64 ", vgid:%" PRId64
           ", version:%d",
           pInserterInfo->streamId, pInserterInfo->groupId, pNewInfo->uid, pNewInfo->vgid, pNewInfo->version);
  }
  return TSDB_CODE_SUCCESS;

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    stError("failed to check and reset table info for streamId:%" PRIx64 ", groupId:%" PRIx64 ", code:%d",
            pInserterInfo->streamId, pInserterInfo->groupId, code);
  }
  taosHashRelease(gStreamGrpTableHash, ppTbRes);
  return code;
}

static int32_t buildTSchmaFromInserter(SStreamInserterParam* pInsertParam, STSchema** ppTSchema);
static int32_t initTableInfo(SDataInserterHandle* pInserter, SStreamDataInserterInfo* pInserterInfo) {
  int32_t           code = TSDB_CODE_SUCCESS;
  int32_t           lino = 0;
  SInsertTableInfo* res = taosMemoryCalloc(1, sizeof(SInsertTableInfo));
  if (res == NULL) {
    return terrno;
  }

  SStreamInserterParam* pInsertParam = pInserter->pParam->streamInserterParam;
  res->uid = 0;
  if (pInsertParam->tbType == TSDB_NORMAL_TABLE) {
    res->version = 1;
  } else {
    res->version = pInsertParam->sver;
  }

  res->tbname = taosStrdup(pInserterInfo->tbName);
  if (res->tbname == NULL) {
    taosMemoryFree(res);
    stError("failed to allocate memory for table name");
    return terrno;
  }

  code = buildTSchmaFromInserter(pInserter->pParam->streamInserterParam, &res->pSchema);
  QUERY_CHECK_CODE(code, lino, _return);

  int64_t key[2] = {pInserterInfo->streamId, pInserterInfo->groupId};
  code = taosHashPut(gStreamGrpTableHash, key, sizeof(key), &res, sizeof(SInsertTableInfo*));
  if (code == TSDB_CODE_DUP_KEY) {
    freeCacheTbInfo(&res);
    return TSDB_CODE_SUCCESS;
  }

_return:
  if (code != TSDB_CODE_SUCCESS) {
    stError("failed to build table info for streamId:%" PRIx64 ", groupId:%" PRIx64 ", code:%d",
            pInserterInfo->streamId, pInserterInfo->groupId, code);
    freeCacheTbInfo(&res);
  }
  return code;
}

static bool colsIsSupported(const STableMetaRsp* pTableMetaRsp, const SStreamInserterParam* pInserterParam) {
  SArray* pCreatingFields = pInserterParam->pFields;

  for (int32_t i = 0; i < pCreatingFields->size; ++i) {
    SFieldWithOptions* pField = taosArrayGet(pCreatingFields, i);
    if (NULL == pField) {
      stError("isSupportedSTableSchema: failed to get field from array");
      return false;
    }

    for (int j = 0; j < pTableMetaRsp->numOfColumns; ++j) {
      if (strncmp(pTableMetaRsp->pSchemas[j].name, pField->name, TSDB_COL_NAME_LEN) == 0) {
        if (pTableMetaRsp->pSchemas[j].type == pField->type && pTableMetaRsp->pSchemas[j].bytes == pField->bytes) {
          break;
        } else {
          return false;
        }
      }
    }
  }
  return true;
}

static bool TagsIsSupported(const STableMetaRsp* pTableMetaRsp, const SStreamInserterParam* pInserterParam) {
  SArray* pCreatingTags = pInserterParam->pTagFields;

  int32_t            tagIndexOffset = -1;
  SFieldWithOptions* pField = taosArrayGet(pCreatingTags, 0);
  if (NULL == pField) {
    stError("isSupportedSTableSchema: failed to get field from array");
    return false;
  }
  for (int32_t i = 0; i < pTableMetaRsp->numOfColumns + pTableMetaRsp->numOfTags; ++i) {
    if (strncmp(pTableMetaRsp->pSchemas[i].name, pField->name, TSDB_COL_NAME_LEN) != 0) {
      tagIndexOffset = i;
      break;
    }
  }
  if (tagIndexOffset == -1) {
    stError("isSupportedSTableSchema: failed to get tag index");
    return false;
  }

  for (int32_t i = 0; i < pTableMetaRsp->numOfTags; ++i) {
    int32_t            index = i + tagIndexOffset;
    SFieldWithOptions* pField = taosArrayGet(pCreatingTags, i);
    if (NULL == pField) {
      stError("isSupportedSTableSchema: failed to get field from array");
      return false;
    }

    for(int32_t j = 0; j < pTableMetaRsp->numOfTags; ++j) {
      if (strncmp(pTableMetaRsp->pSchemas[index].name, pField->name, TSDB_COL_NAME_LEN) == 0) {
        if (pTableMetaRsp->pSchemas[index].type == pField->type &&
            pTableMetaRsp->pSchemas[index].bytes == pField->bytes) {
          break;
        } else {
          return false;
        }
      }
    }
  }
  return true;
}

static bool isSupportedSTableSchema(const STableMetaRsp* pTableMetaRsp, const SStreamInserterParam* pInserterParam) {
  if (!colsIsSupported(pTableMetaRsp, pInserterParam)) {
    return false;
  }
  if (!TagsIsSupported(pTableMetaRsp, pInserterParam)) {
    return false;
  }
  return true;
}

static bool isSupportedNTableSchema(const STableMetaRsp* pTableMetaRsp, const SStreamInserterParam* pInserterParam) {
  return colsIsSupported(pTableMetaRsp, pInserterParam);
}

static int32_t checkAndSaveCreateGrpTableInfo(SDataInserterHandle*     pInserthandle,
                                              SStreamDataInserterInfo* pInserterInfo) {
  int32_t     code = TSDB_CODE_SUCCESS;
  SSubmitRes* pSubmitRes = &pInserthandle->submitRes;
  int8_t      tbType = pInserthandle->pParam->streamInserterParam->tbType;

  SVCreateTbRsp*        pCreateTbRsp = taosArrayGet(pSubmitRes->pRsp->aCreateTbRsp, 0);
  SSchema*              pExistRow = pCreateTbRsp->pMeta->pSchemas;
  SStreamInserterParam* pInserterParam = pInserthandle->pParam->streamInserterParam;

  if (tbType == TSDB_CHILD_TABLE || tbType == TSDB_SUPER_TABLE) {
    if (!isSupportedSTableSchema(pCreateTbRsp->pMeta, pInserterParam)) {
      stError("create table failed, schema is not supported");
      return TSDB_CODE_STREAM_INSERT_SCHEMA_NOT_MATCH;
    }
  } else if (tbType == TSDB_NORMAL_TABLE) {
    if (!isSupportedNTableSchema(pCreateTbRsp->pMeta, pInserterParam)) {
      stError("create table failed, schema is not supported");
      return TSDB_CODE_STREAM_INSERT_SCHEMA_NOT_MATCH;
    }
  } else {
    stError("checkAndSaveCreateGrpTableInfo failed, tbType:%d is not supported", tbType);
    return TSDB_CODE_MND_STREAM_INTERNAL_ERROR;
  }

  return updateInsertGrpTableInfo(pInserterInfo, pSubmitRes);
}

int32_t inserterCallback(void* param, SDataBuf* pMsg, int32_t code) {
  SSubmitRspParam*     pParam = (SSubmitRspParam*)param;
  SDataInserterHandle* pInserter = pParam->pInserter;
  int32_t              code2 = 0;

  if (code) {
    pInserter->submitRes.code = code;
  } else {
    pInserter->submitRes.code = TSDB_CODE_SUCCESS;
  }
  SDecoder coder = {0};

  if (code == TSDB_CODE_SUCCESS) {
    pInserter->submitRes.pRsp = taosMemoryCalloc(1, sizeof(SSubmitRsp2));
    if (NULL == pInserter->submitRes.pRsp) {
      pInserter->submitRes.code = terrno;
      goto _return;
    }

    tDecoderInit(&coder, pMsg->pData, pMsg->len);
    code = tDecodeSSubmitRsp2(&coder, pInserter->submitRes.pRsp);
    if (code) {
      tDestroySSubmitRsp2(pInserter->submitRes.pRsp, TSDB_MSG_FLG_DECODE);
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
          tDestroySSubmitRsp2(pInserter->submitRes.pRsp, TSDB_MSG_FLG_DECODE);
          taosMemoryFree(pInserter->submitRes.pRsp);
          pInserter->submitRes.code = code;
          goto _return;
        }
      }
    }

    if (pParam->putParam != NULL && ((SStreamDataInserterInfo*)pParam->putParam)->isAutoCreateTable) {
      code2 = updateInsertGrpTableInfo((SStreamDataInserterInfo*)pParam->putParam, &pInserter->submitRes);
    }

    pInserter->submitRes.affectedRows += pInserter->submitRes.pRsp->affectedRows;
    qDebug("submit rsp received, affectedRows:%d, total:%" PRId64, pInserter->submitRes.pRsp->affectedRows,
           pInserter->submitRes.affectedRows);
    tDestroySSubmitRsp2(pInserter->submitRes.pRsp, TSDB_MSG_FLG_DECODE);
    taosMemoryFree(pInserter->submitRes.pRsp);
  } else if ((TSDB_CODE_TDB_TABLE_ALREADY_EXIST == code && pParam->putParam != NULL &&
              ((SStreamDataInserterInfo*)pParam->putParam)->isAutoCreateTable) ||
             TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER == code) {
    pInserter->submitRes.code = TSDB_CODE_TDB_TABLE_ALREADY_EXIST;
    pInserter->submitRes.pRsp = taosMemoryCalloc(1, sizeof(SSubmitRsp2));
    if (NULL == pInserter->submitRes.pRsp) {
      code2 = terrno;
      goto _return;
    }

    tDecoderInit(&coder, pMsg->pData, pMsg->len);
    code2 = tDecodeSSubmitRsp2(&coder, pInserter->submitRes.pRsp);
    if (code2 == TSDB_CODE_SUCCESS) {
      code2 = checkAndSaveCreateGrpTableInfo(pInserter, (SStreamDataInserterInfo*)pParam->putParam);
    }
    tDestroySSubmitRsp2(pInserter->submitRes.pRsp, TSDB_MSG_FLG_DECODE);
    taosMemoryFree(pInserter->submitRes.pRsp);
  }

_return:

  if (code2) {
    qError("update inserter table info failed, error:%s", tstrerror(code2));
  }
  tDecoderClear(&coder);
  TAOS_UNUSED(tsem_post(&pInserter->ready));

  taosMemoryFree(pMsg->pData);

  return TSDB_CODE_SUCCESS;
}

void freeUseDbOutput_tmp(void* ppOutput) {
  SUseDbOutput* pOut = *(SUseDbOutput**)ppOutput;
  if (NULL == ppOutput) {
    return;
  }

  if (pOut->dbVgroup) {
    freeVgInfo(pOut->dbVgroup);
  }
  taosMemFree(pOut);
  *(SUseDbOutput**)ppOutput = NULL;
}

static int32_t processUseDbRspForInserter(void* param, SDataBuf* pMsg, int32_t code) {
  int32_t       lino = 0;
  SDBVgInfoReq* pVgInfoReq = (SDBVgInfoReq*)param;

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

  pVgInfoReq->pRsp = taosMemoryMalloc(sizeof(SUseDbRsp));
  QUERY_CHECK_NULL(pVgInfoReq->pRsp, code, lino, _return, terrno);

  code = tDeserializeSUseDbRsp(pMsg->pData, (int32_t)pMsg->len, pVgInfoReq->pRsp);
  QUERY_CHECK_CODE(code, lino, _return);

_return:
  taosMemoryFreeClear(pMsg->pData);
  taosMemoryFreeClear(pMsg->pEpSet);
  if (code != 0){
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  int ret = tsem_post(&pVgInfoReq->ready);
  if (ret != 0) {
    qError("%s failed code: %d", __func__, ret);
  }
  return code;
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

static int32_t buildDbVgInfoMap(void* clientRpc, const char* dbFName, SUseDbOutput* output) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  char*        buf1 = NULL;
  SUseDbReq*   pReq = NULL;
  SDBVgInfoReq dbVgInfoReq = {0};
  code = tsem_init(&dbVgInfoReq.ready, 0, 0);
  if (code != TSDB_CODE_SUCCESS) {
    qError("tsem_init failed, error:%s", tstrerror(code));
    return code;
  }

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

  SEpSet pEpSet = {0};
  QUERY_CHECK_CODE(getCurrentMnodeEpset(&pEpSet), lino, _return);

  pMsgSendInfo->param = &dbVgInfoReq;
  pMsgSendInfo->msgInfo.pData = buf1;
  buf1 = NULL;
  pMsgSendInfo->msgInfo.len = contLen;
  pMsgSendInfo->msgType = TDMT_MND_GET_DB_INFO;
  pMsgSendInfo->fp = processUseDbRspForInserter;
  // pMsgSendInfo->requestId = pTaskInfo->id.queryId;

  code = asyncSendMsgToServer(clientRpc, &pEpSet, NULL, pMsgSendInfo);
  QUERY_CHECK_CODE(code, lino, _return);

  code = tsem_wait(&dbVgInfoReq.ready);
  QUERY_CHECK_CODE(code, lino, _return);

  code = queryBuildUseDbOutput(output, dbVgInfoReq.pRsp);
  QUERY_CHECK_CODE(code, lino, _return);

  output->dbVgroup->vgArray = taosArrayInit(dbVgInfoReq.pRsp->vgNum, sizeof(SVgroupInfo));
  if (NULL == output->dbVgroup->vgArray) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _return);
  }

  void* pIter = taosHashIterate(output->dbVgroup->vgHash, NULL);
  while (pIter) {
    if (NULL == taosArrayPush(output->dbVgroup->vgArray, pIter)) {
      taosHashCancelIterate(output->dbVgroup->vgHash, pIter);
      return terrno;
    }

    pIter = taosHashIterate(output->dbVgroup->vgHash, pIter);
  }

  taosArraySort(output->dbVgroup->vgArray, inserterVgInfoComp);

_return:

  if (code) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    taosMemoryFree(buf1);
  }
  taosMemoryFree(pReq);
  TAOS_UNUSED(tsem_destroy(&dbVgInfoReq.ready));
  if (dbVgInfoReq.pRsp) {
    tFreeSUsedbRsp(dbVgInfoReq.pRsp);
    taosMemoryFreeClear(dbVgInfoReq.pRsp);
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
    if (!pTbReq->ctb.stbName) {
      taosMemoryFree(pTbReq->name);
      return terrno;
    }
  }
  pTbReq->ctb.tagName = tagName;
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


int32_t inserterGetVgInfo(SDBVgInfo* dbInfo, char* tbName, SVgroupInfo* pVgInfo) {
  if (NULL == dbInfo) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  if (NULL == dbInfo->vgArray) {
    qError("empty db vgArray, hashSize:%d", taosHashGetSize(dbInfo->vgHash));
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  uint32_t hashValue =
      taosGetTbHashVal(tbName, (int32_t)strlen(tbName), dbInfo->hashMethod, dbInfo->hashPrefix, dbInfo->hashSuffix);
  SVgroupInfo* vgInfo = taosArraySearch(dbInfo->vgArray, &hashValue, inserterHashValueComp, TD_EQ);
  if (NULL == vgInfo) {
    qError("no hash range found for hash value [%u], table:%s, numOfVgId:%d", hashValue, tbName,
           (int32_t)taosArrayGetSize(dbInfo->vgArray));
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  
  *pVgInfo = *vgInfo;
  qDebug("insert get vgInfo, tbName:%s vgId:%d epset(%s:%d)", tbName, pVgInfo->vgId, pVgInfo->epSet.eps[0].fqdn,
        pVgInfo->epSet.eps[0].port);
        
  return TSDB_CODE_SUCCESS;
}

int32_t inserterGetVgId(SDBVgInfo* dbInfo, char* tbName, int32_t* vgId) {
  SVgroupInfo vgInfo = {0};
  int32_t     code = inserterGetVgInfo(dbInfo, tbName, &vgInfo);
  if (code != TSDB_CODE_SUCCESS) {
    qError("inserterGetVgId failed, code:%d", code);
    return code;
  }
  *vgId = vgInfo.vgId;

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

    code = buildDbVgInfoMap(pInserter->pParam->readHandle->pMsgCb->clientRpc, dbFName, output);
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
  freeUseDbOutput_tmp(&output);
  return code;
}

int32_t getTableVgInfo(SDataInserterHandle* pInserter, const char* dbFName,
                       const char* tbName, SVgroupInfo* pVgInfo) {
  return getDbVgInfoForExec(pInserter->pParam->readHandle->pMsgCb->clientRpc, dbFName,
                              tbName, pVgInfo);
}

static int32_t sendSubmitRequest(SDataInserterHandle* pInserter, void* putParam, void* pMsg, int32_t msgLen,
                                 void* pTransporter, SEpSet* pEpset) {
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
  pParam->putParam = putParam;

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
  SSubmitReq2** ppReq = NULL;
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
    SSubmitReq2* pReq = NULL;
    ppReq = taosHashGet(pHash, &vgIdForTbName, sizeof(int32_t));
    if (ppReq == NULL) {
      pReq = taosMemoryCalloc(1, sizeof(SSubmitReq2));
      if (NULL == pReq) {
        tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
        goto _end;
      }

      if (!(pReq->aSubmitTbData = taosArrayInit(1, sizeof(SSubmitTbData)))) {
        tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
        goto _end;
      }
      code = taosHashPut(pHash, &vgIdForTbName, sizeof(int32_t), &pReq, POINTER_BYTES);
    } else {
      pReq = *ppReq;
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
        taosArrayDestroy(TagNames);
        tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
        goto _end;
      }

      colIdx = *slotId;
      SColumnInfoData* pColInfoData = taosArrayGet(pDataBlock->pDataBlock, colIdx);
      if (NULL == pColInfoData) {
        terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        taosArrayDestroy(TagNames);
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
            taosArrayDestroy(TagNames);
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
              taosArrayDestroy(TagNames);
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
          taosArrayDestroy(TagNames);
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
                taosArrayDestroy(TagNames);
                tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
                goto _end;
              }
            }
          } else {
            uError("the column type %" PRIi16 " is undefined\n", pColInfoData->info.type);
            terrno = TSDB_CODE_APP_ERROR;
            taosArrayDestroy(TagNames);
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
      taosArrayDestroy(TagNames);
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
            void*   data = colDataGetVarData(pColInfoData, j);
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
    SRowBuildScanInfo sinfo = {0};
    if ((terrno = tRowBuild(pVals, pTSchema, &pRow, &sinfo)) < 0) {
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
                                const STSchema* pTSchema, int64_t* uid, int32_t* vgId, tb_uid_t* suid) {
  SSubmitReq2* pReq = *ppReq;
  SArray*      pVals = NULL;
  SArray*      pTagVals = NULL;
  int32_t      numOfBlks = 0;
  char*        tableName = NULL;
  int32_t      code = 0, lino = 0;

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
  tbData.suid = *suid;
  tbData.uid = *uid;
  tbData.sver = pTSchema->version;

  if (!pVals && !(pVals = taosArrayInit(colNum, sizeof(SColVal)))) {
    taosArrayDestroy(tbData.aRowP);
    goto _end;
  }

  if (pInserter->isStbInserter) {
    if (!pTagVals && !(pTagVals = taosArrayInit(colNum, sizeof(STagVal)))) {
      taosArrayDestroy(tbData.aRowP);
      goto _end;
    }
  }

  int64_t lastTs = TSKEY_MIN;
  bool    needSortMerge = false;

  for (int32_t j = 0; j < rows; ++j) {  // iterate by row
    taosArrayClear(pVals);

    int32_t offset = 0;
    // 处理超级表的tbname和tags
    if (pInserter->isStbInserter) {
      taosArrayClear(pTagVals);
      tbData.uid = 0;
      *uid = 0;
      tbData.pCreateTbReq = taosMemoryCalloc(1, sizeof(SVCreateTbReq));
      tbData.flags |= SUBMIT_REQ_AUTO_CREATE_TABLE;

      SColumnInfoData* tbname = taosArrayGet(pDataBlock->pDataBlock, 0);
      if (NULL == tbname) {
        terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        qError("Insert into stable must have tbname column");
        goto _end;
      }
      if (tbname->info.type != TSDB_DATA_TYPE_BINARY) {
        terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        qError("tbname column must be binary");
        goto _end;
      }

      if (colDataIsNull_s(tbname, j)) {
        qError("insert into stable tbname column is null");
        goto _end;
      }
      void*   data = colDataGetVarData(tbname, j);
      SValue  sv = (SValue){TSDB_DATA_TYPE_VARCHAR, .nData = varDataLen(data),
                            .pData = varDataVal(data)};  // address copy, no value
      SColVal cv = COL_VAL_VALUE(0, sv);

      // 获取子表vgId
      SDBVgInfo* dbInfo = NULL;
      code = inserterGetDbVgInfo(pInserter, pInserter->dbFName, &dbInfo);
      if (code != TSDB_CODE_SUCCESS) {
        goto _end;
      }

      char tbFullName[TSDB_TABLE_FNAME_LEN];
      taosMemoryFreeClear(tableName);
      tableName = taosMemoryCalloc(1, sv.nData + 1);
      TSDB_CHECK_NULL(tableName, code, lino, _end, terrno);
      tstrncpy(tableName, sv.pData, sv.nData);
      tableName[sv.nData] = '\0';

      int32_t len = snprintf(tbFullName, TSDB_TABLE_FNAME_LEN, "%s.%s", pInserter->dbFName, tableName);
      if (len >= TSDB_TABLE_FNAME_LEN) {
        terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        qError("table name too long after format, len:%d, maxLen:%d", len, TSDB_TABLE_FNAME_LEN);
        goto _end;
      }
      code = inserterGetVgId(dbInfo, tbFullName, vgId);
      if (code != TSDB_CODE_SUCCESS) {
        terrno = code;
        goto _end;
      }
      // 解析tag
      SArray* TagNames = taosArrayInit(8, TSDB_COL_NAME_LEN);
      if (!TagNames) {
        terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        goto _end;
      }
      for (int32_t i = 0; i < pInserter->pTagSchema->nCols; ++i) {
        SSchema* tSchema = &pInserter->pTagSchema->pSchema[i];
        int16_t  colIdx = tSchema->colId;
        if (NULL == taosArrayPush(TagNames, tSchema->name)) {
          goto _end;
        }
        int16_t* slotId = taosHashGet(pInserter->pCols, &colIdx, sizeof(colIdx));
        if (NULL == slotId) {
          continue;
        }

        colIdx = *slotId;
        SColumnInfoData* pColInfoData = taosArrayGet(pDataBlock->pDataBlock, colIdx);
        if (NULL == pColInfoData) {
          terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
          goto _end;
        }
        // void* var = POINTER_SHIFT(pColInfoData->pData, j * pColInfoData->info.bytes);
        switch (pColInfoData->info.type) {
          case TSDB_DATA_TYPE_NCHAR:
          case TSDB_DATA_TYPE_VARBINARY:
          case TSDB_DATA_TYPE_VARCHAR: {  // TSDB_DATA_TYPE_BINARY
            if (pColInfoData->info.type != tSchema->type) {
              qError("tag:%d type:%d in block dismatch with schema tag:%d type:%d", colIdx, pColInfoData->info.type, i,
                     tSchema->type);
              terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
              goto _end;
            }
            if (colDataIsNull_s(pColInfoData, j)) {
              continue;
            } else {
              void*   data = colDataGetVarData(pColInfoData, j);
              STagVal tv = (STagVal){.cid = tSchema->colId,
                                     .type = tSchema->type,
                                     .nData = varDataLen(data),
                                     .pData = varDataVal(data)};  // address copy, no value
              if (NULL == taosArrayPush(pTagVals, &tv)) {
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
      STag* pTag = NULL;
      code = tTagNew(pTagVals, 1, false, &pTag);
      if (code != TSDB_CODE_SUCCESS) {
        terrno = code;
        qError("failed to create tag, error:%s", tstrerror(code));
        goto _end;
      }

      code = inserterBuildCreateTbReq(tbData.pCreateTbReq, tableName, pTag, *suid, pInserter->pNode->tableName, TagNames,
                               pInserter->pTagSchema->nCols, TSDB_DEFAULT_TABLE_TTL);
    }

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
        case TSDB_DATA_TYPE_MEDIUMBLOB:
        case TSDB_DATA_TYPE_JSON:
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

    SRow*             pRow = NULL;
    SRowBuildScanInfo sinfo = {0};
    if ((terrno = tRowBuild(pVals, pTSchema, &pRow, &sinfo)) < 0) {
      tDestroySubmitTbData(&tbData, TSDB_MSG_FLG_ENCODE);
      goto _end;
    }
    if (NULL == taosArrayPush(tbData.aRowP, &pRow)) {
      goto _end;
    }
  }

  if (needSortMerge) {
    if ((tRowSort(tbData.aRowP) != TSDB_CODE_SUCCESS) ||
        (terrno = tRowMerge(tbData.aRowP, (STSchema*)pTSchema, KEEP_CONSISTENCY)) != 0) {
      goto _end;
    }
  }

  if (NULL == taosArrayPush(pReq->aSubmitTbData, &tbData)) {
    goto _end;
  }

_end:

  taosMemoryFreeClear(tableName);

  taosArrayDestroy(pTagVals);
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

static void destroySubmitReqWrapper(void* p) {
  SSubmitReq2* pReq = *(SSubmitReq2**)p;
  if (pReq != NULL) {
    tDestroySubmitReq(pReq, TSDB_MSG_FLG_ENCODE);
    taosMemoryFree(pReq);
  }
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
      taosHashSetFreeFp(pHash, destroySubmitReqWrapper);
    }
    code = buildSubmitReqFromStbBlock(pInserter, pHash, pDataBlock, pTSchema, uid, vgId, suid);
    if (code != TSDB_CODE_SUCCESS) {
      goto _end;
    }
  }

  size_t keyLen = 0;
  while ((iterator = taosHashIterate(pHash, iterator))) {
    SSubmitReq2* pReq = *(SSubmitReq2**)iterator;
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
    code = buildSubmitReqFromBlock(pInserter, &pReq, pDataBlock, pTSchema, &uid, &vgId, &suid);
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

int32_t getStreamInsertTableInfo(int64_t streamId, int64_t groupId, SInsertTableInfo*** ppTbInfo) {
  int64_t            key[2] = {streamId, groupId};
  SInsertTableInfo** pTmp = taosHashAcquire(gStreamGrpTableHash, key, sizeof(key));
  if (NULL == pTmp || *pTmp == NULL) {
    return TSDB_CODE_STREAM_INSERT_TBINFO_NOT_FOUND;
  }

  *ppTbInfo = pTmp;
  return TSDB_CODE_SUCCESS;
}

static int32_t releaseStreamInsertTableInfo(SInsertTableInfo** ppTbInfo) {
  taosHashRelease(gStreamGrpTableHash, ppTbInfo);
  return TSDB_CODE_SUCCESS;
}

int32_t buildNormalTableCreateReq(SDataInserterHandle* pInserter, SStreamInserterParam* pInsertParam,
                                  SSubmitTbData* tbData) {
  int32_t code = TSDB_CODE_SUCCESS;

  tbData->suid = 0;

  tbData->pCreateTbReq = taosMemoryCalloc(1, sizeof(SVCreateTbReq));
  if (NULL == tbData->pCreateTbReq) {
    goto _end;
  }
  tbData->flags |= (SUBMIT_REQ_AUTO_CREATE_TABLE | SUBMIT_REQ_SCHEMA_RES);
  tbData->pCreateTbReq->type = TSDB_NORMAL_TABLE;
  tbData->pCreateTbReq->flags |= (TD_CREATE_NORMAL_TB_IN_STREAM | TD_CREATE_IF_NOT_EXISTS);
  tbData->pCreateTbReq->uid = 0;
  tbData->sver = pInsertParam->sver;

  tbData->pCreateTbReq->name = taosStrdup(pInsertParam->tbname);
  if (!tbData->pCreateTbReq->name) return terrno;

  int32_t numOfCols = pInsertParam->pFields->size;
  tbData->pCreateTbReq->ntb.schemaRow.nCols = numOfCols;
  tbData->pCreateTbReq->ntb.schemaRow.version = 1;

  tbData->pCreateTbReq->ntb.schemaRow.pSchema = taosMemoryCalloc(numOfCols, sizeof(SSchema));
  if (NULL == tbData->pCreateTbReq->ntb.schemaRow.pSchema) {
    goto _end;
  }
  for (int32_t i = 0; i < numOfCols; ++i) {
    SFieldWithOptions* pField = taosArrayGet(pInsertParam->pFields, i);
    if (NULL == pField) {
      terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      goto _end;
    }
    tbData->pCreateTbReq->ntb.schemaRow.pSchema[i].colId = i + 1;
    tbData->pCreateTbReq->ntb.schemaRow.pSchema[i].type = pField->type;
    tbData->pCreateTbReq->ntb.schemaRow.pSchema[i].bytes = pField->bytes;
    tbData->pCreateTbReq->ntb.schemaRow.pSchema[i].flags = pField->flags;
    if (i == 0 && pField->type != TSDB_DATA_TYPE_TIMESTAMP) {
      terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      qError("buildNormalTableCreateReq, the first column must be timestamp.");
      goto _end;
    }
    if (i == 0) {
      tbData->pCreateTbReq->ntb.schemaRow.pSchema[i].flags |= COL_IS_KEY;
    }
    snprintf(tbData->pCreateTbReq->ntb.schemaRow.pSchema[i].name, TSDB_COL_NAME_LEN, "%s", pField->name);
    if (IS_DECIMAL_TYPE(pField->type)) {
      if (!tbData->pCreateTbReq->pExtSchemas) {
        tbData->pCreateTbReq->pExtSchemas = taosMemoryCalloc(numOfCols, sizeof(SExtSchema));
        if (NULL == tbData->pCreateTbReq->pExtSchemas) {
          tdDestroySVCreateTbReq(tbData->pCreateTbReq);
          tbData->pCreateTbReq = NULL;
          return terrno;
        }
      }
      tbData->pCreateTbReq->pExtSchemas[i].typeMod = pField->typeMod;
    }
  }
  return TSDB_CODE_SUCCESS;
_end:
  return code;
}

// reference tBuildTSchema funciton
static int32_t buildTSchmaFromInserter(SStreamInserterParam* pInsertParam, STSchema** ppTSchema) {
  int32_t code = TSDB_CODE_SUCCESS;

  int32_t   numOfCols = pInsertParam->pFields->size;
  STSchema* pTSchema = taosMemoryCalloc(1, sizeof(STSchema) + sizeof(STColumn) * numOfCols);
  if (NULL == pTSchema) {
    return terrno;
  }
  if (pInsertParam->tbType == TSDB_NORMAL_TABLE) {
    pTSchema->version =
        1;  // normal table version start from 1, if has exist table, it will be reset by resetInserterTbVersion
  } else {
    pTSchema->version = pInsertParam->sver;
  }
  pTSchema->numOfCols = numOfCols;

  SFieldWithOptions* pField = taosArrayGet(pInsertParam->pFields, 0);
  if (NULL == pField) {
    code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    goto _end;
  }
  pTSchema->columns[0].colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  pTSchema->columns[0].type = pField->type;
  pTSchema->columns[0].flags = pField->flags | COL_IS_KEY;
  pTSchema->columns[0].bytes = TYPE_BYTES[pField->type];
  pTSchema->columns[0].offset = -1;

  pTSchema->tlen = 0;
  pTSchema->flen = 0;
  for (int32_t i = 1; i < numOfCols; ++i) {
    SFieldWithOptions* pField = taosArrayGet(pInsertParam->pFields, i);
    if (NULL == pField) {
      code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      goto _end;
    }
    pTSchema->columns[i].colId = i + 1;
    pTSchema->columns[i].type = pField->type;
    pTSchema->columns[i].flags = pField->flags;
    pTSchema->columns[i].bytes = pField->bytes;
    pTSchema->columns[i].offset = pTSchema->flen;

    if (IS_VAR_DATA_TYPE(pField->type)) {
      pTSchema->columns[i].bytes = pField->bytes;
      pTSchema->tlen += (TYPE_BYTES[pField->type] + pField->bytes);
    } else {
      pTSchema->columns[i].bytes = TYPE_BYTES[pField->type];
      pTSchema->tlen += TYPE_BYTES[pField->type];
    }

    pTSchema->flen += TYPE_BYTES[pField->type];
  }

#if 1
  pTSchema->tlen += (int32_t)TD_BITMAP_BYTES(numOfCols);
#endif

_end:
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pTSchema);
    *ppTSchema = NULL;
  } else {
    *ppTSchema = pTSchema;
  }
  return code;
}

static int32_t getTagValsFromStreamInserterInfo(SStreamDataInserterInfo* pInserterInfo, int32_t preCols,
                                                SArray** ppTagVals) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t nTags = pInserterInfo->pTagVals->size;
  *ppTagVals = taosArrayInit(nTags, sizeof(STagVal));
  if (!ppTagVals) {
    return terrno;
  }
  for (int32_t i = 0; i < pInserterInfo->pTagVals->size; ++i) {
    SStreamTagInfo* pTagInfo = taosArrayGet(pInserterInfo->pTagVals, i);
    STagVal         tagVal = {
                .cid = preCols + i + 1,
                .type = pTagInfo->val.data.type,
    };
    if (!pTagInfo->val.isNull) {
      if (IS_VAR_DATA_TYPE(pTagInfo->val.data.type)) {
        tagVal.nData = pTagInfo->val.data.nData;
        tagVal.pData = pTagInfo->val.data.pData;
      } else {
        tagVal.i64 = pTagInfo->val.data.val;
      }

      if (NULL == taosArrayPush(*ppTagVals, &tagVal)) {
        code = terrno;
        goto _end;
      }
    }
  }
_end:
  if (code != TSDB_CODE_SUCCESS) {
    taosArrayDestroy(*ppTagVals);
    *ppTagVals = NULL;
  }
  return code;
}

static int32_t buildStreamSubTableCreateReq(SStreamRunnerTask* pTask, SDataInserterHandle* pInserter,
                                            SStreamInserterParam* pInsertParam, SStreamDataInserterInfo* pInserterInfo,
                                            SSubmitTbData* tbData) {
  int32_t code = TSDB_CODE_SUCCESS;
  STag*   pTag = NULL;
  SArray* pTagVals = NULL;
  SArray* TagNames = NULL;

  if (pInsertParam->pTagFields == NULL) {
    ST_TASK_ELOG("buildStreamSubTableCreateReq, pTagFields is NULL, suid:%" PRId64 ", sver:%d", pInsertParam->suid,
                 pInsertParam->sver);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  if (pInserterInfo->pTagVals == NULL || pInserterInfo->pTagVals->size == 0) {
    ST_TASK_ELOG("buildStreamSubTableCreateReq, pTagVals is NULL, suid:%" PRId64 ", sver:%d", pInsertParam->suid,
                 pInsertParam->sver);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  if (pInsertParam->suid <= 0 || pInsertParam->sver <= 0) {
    ST_TASK_ELOG("buildStreamSubTableCreateReq, suid:%" PRId64
                 ", sver:%d"
                 " must be greater than 0",
                 pInsertParam->suid, pInsertParam->sver);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  int32_t nTags = pInserterInfo->pTagVals->size;

  TagNames = taosArrayInit(nTags, TSDB_COL_NAME_LEN);
  if (!TagNames) {
    code = terrno;
    goto _end;
  }
  for (int32_t i = 0; i < nTags; ++i) {
    SFieldWithOptions* pField = taosArrayGet(pInsertParam->pTagFields, i);
    if (NULL == taosArrayPush(TagNames, pField->name)) {
      code = terrno;
      goto _end;
    }
  }

  tbData->flags |= (SUBMIT_REQ_AUTO_CREATE_TABLE | SUBMIT_REQ_SCHEMA_RES);
  tbData->uid = 0;
  tbData->suid = pInsertParam->suid;
  tbData->sver = pInsertParam->sver;

  tbData->pCreateTbReq = taosMemoryCalloc(1, sizeof(SVCreateTbReq));
  if (NULL == tbData->pCreateTbReq) {
    code = terrno;
    goto _end;
  }
  tbData->pCreateTbReq->type = TSDB_CHILD_TABLE;
  tbData->pCreateTbReq->flags |= (TD_CREATE_SUB_TB_IN_STREAM | TD_CREATE_IF_NOT_EXISTS);

  code = getTagValsFromStreamInserterInfo(pInserterInfo, pInsertParam->pFields->size, &pTagVals);
  if (code != TSDB_CODE_SUCCESS) {
    goto _end;
  }

  code = tTagNew(pTagVals, pInsertParam->sver, false, &pTag);
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("failed to create tag, error:%s", tstrerror(code));
    goto _end;
  }
  code = inserterBuildCreateTbReq(tbData->pCreateTbReq, pInserterInfo->tbName, pTag, tbData->suid,
                                  pInsertParam->stbname, TagNames, nTags, TSDB_DEFAULT_TABLE_TTL);
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("failed to build create table request, error:%s", tstrerror(code));
    goto _end;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("buildStreamSubTableCreateReq failed, error:%s", tstrerror(code));
    if (tbData->pCreateTbReq) {
      taosMemoryFreeClear(tbData->pCreateTbReq->name);
      taosMemoryFreeClear(tbData->pCreateTbReq);
    }
    if (TagNames) {
      taosArrayDestroy(TagNames);
    }
  }

  if (pTagVals) {
    taosArrayDestroy(pTagVals);
  }
  return code;
}

static int32_t appendInsertData(SStreamInserterParam* pInsertParam, const SSDataBlock* pDataBlock,
                                SSubmitTbData* tbData, STSchema* pTSchema, SBuildInsertDataInfo* dataInsertInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  int32_t rows = pDataBlock ? pDataBlock->info.rows : 0;
  int32_t numOfCols = pInsertParam->pFields->size;
  int32_t colNum = pDataBlock ? taosArrayGetSize(pDataBlock->pDataBlock) : 0;

  SArray* pVals = NULL;
  if (!(pVals = taosArrayInit(colNum, sizeof(SColVal)))) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _end);
  }

  for (int32_t j = 0; j < rows; ++j) {  // iterate by row
    taosArrayClear(pVals);

    bool tsOrPrimaryKeyIsNull = false;
    for (int32_t k = 0; k < numOfCols; ++k) {  // iterate by column
      int16_t colIdx = k + 1;

      SFieldWithOptions* pCol = taosArrayGet(pInsertParam->pFields, k);
      if (PRIMARYKEY_TIMESTAMP_COL_ID != colIdx && TSDB_DATA_TYPE_NULL == pCol->type) {
        SColVal cv = COL_VAL_NULL(colIdx, pCol->type);
        if (NULL == taosArrayPush(pVals, &cv)) {
          code = terrno;
          QUERY_CHECK_CODE(code, lino, _end);
        }
        continue;
      }

      SColumnInfoData* pColInfoData = taosArrayGet(pDataBlock->pDataBlock, k);
      if (NULL == pColInfoData) {
        code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        QUERY_CHECK_CODE(code, lino, _end);
      }
      void* var = POINTER_SHIFT(pColInfoData->pData, j * pColInfoData->info.bytes);

      if (colDataIsNull_s(pColInfoData, j) && (pCol->flags & COL_IS_KEY)) {
        tsOrPrimaryKeyIsNull = true;
        qDebug("Primary key column should not be null, skip this row");
        break;
      }
      switch (pColInfoData->info.type) {
        case TSDB_DATA_TYPE_NCHAR:
        case TSDB_DATA_TYPE_VARBINARY:
        case TSDB_DATA_TYPE_VARCHAR: {  // TSDB_DATA_TYPE_BINARY
          if (pColInfoData->info.type != pCol->type) {
            qError("tb:%s column:%d type:%d in block dismatch with schema col:%d type:%d", pInsertParam->tbname, k,
                   pColInfoData->info.type, k, pCol->type);
            code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
            QUERY_CHECK_CODE(code, lino, _end);
          }
          if (colDataIsNull_s(pColInfoData, j)) {
            SColVal cv = COL_VAL_NULL(colIdx, pCol->type);
            if (NULL == taosArrayPush(pVals, &cv)) {
              code = terrno;
              QUERY_CHECK_CODE(code, lino, _end);
            }
          } else {
            if (pColInfoData->pData == NULL) {
              qError("build insert tb:%s, column:%d data is NULL in block", pInsertParam->tbname, k);
              code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
              QUERY_CHECK_CODE(code, lino, _end);
            }
            void*  data = colDataGetVarData(pColInfoData, j);
            SValue sv = (SValue){
                .type = pCol->type, .nData = varDataLen(data), .pData = varDataVal(data)};  // address copy, no value
            SColVal cv = COL_VAL_VALUE(colIdx, sv);
            if (NULL == taosArrayPush(pVals, &cv)) {
              code = terrno;
              QUERY_CHECK_CODE(code, lino, _end);
            }
          }
          break;
        }
        case TSDB_DATA_TYPE_BLOB:
        case TSDB_DATA_TYPE_JSON:
        case TSDB_DATA_TYPE_MEDIUMBLOB:
          qError("the column type %" PRIi16 " is defined but not implemented yet", pColInfoData->info.type);
          code = TSDB_CODE_APP_ERROR;
          QUERY_CHECK_CODE(code, lino, _end);
          break;
        default:
          if (pColInfoData->info.type < TSDB_DATA_TYPE_MAX && pColInfoData->info.type > TSDB_DATA_TYPE_NULL) {
            if (colDataIsNull_s(pColInfoData, j)) {
              if (PRIMARYKEY_TIMESTAMP_COL_ID == colIdx) {
                tsOrPrimaryKeyIsNull = true;
                qDebug("Primary timestamp column should not be null, skip this row");
                break;
              }

              SColVal cv = COL_VAL_NULL(colIdx, pCol->type);  // should use pCol->type
              if (NULL == taosArrayPush(pVals, &cv)) {
                code = terrno;
                QUERY_CHECK_CODE(code, lino, _end);
              }
            } else {
              if (PRIMARYKEY_TIMESTAMP_COL_ID == colIdx && !dataInsertInfo->needSortMerge) {
                if (*(int64_t*)var <= dataInsertInfo->lastTs) {
                  dataInsertInfo->needSortMerge = true;
                } else {
                  dataInsertInfo->lastTs = *(int64_t*)var;
                }
              }

              SValue sv = {.type = pCol->type};
              valueSetDatum(&sv, sv.type, var, tDataTypes[pCol->type].bytes);
              SColVal cv = COL_VAL_VALUE(colIdx, sv);
              if (NULL == taosArrayPush(pVals, &cv)) {
                code = terrno;
                QUERY_CHECK_CODE(code, lino, _end);
              }
            }
          } else {
            uError("the column type %" PRIi16 " is undefined\n", pColInfoData->info.type);
            code = TSDB_CODE_APP_ERROR;
            QUERY_CHECK_CODE(code, lino, _end);
          }
          break;
      }
      if (tsOrPrimaryKeyIsNull) break;  // skip remaining columns because the primary key is null
    }
    if (tsOrPrimaryKeyIsNull) continue;  // skip this row if primary key is null
    SRow*             pRow = NULL;
    SRowBuildScanInfo sinfo = {0};
    if ((code = tRowBuild(pVals, pTSchema, &pRow, &sinfo)) != TSDB_CODE_SUCCESS) {
      QUERY_CHECK_CODE(code, lino, _end);
    }
    if (NULL == taosArrayPush(tbData->aRowP, &pRow)) {
      taosMemFree(pRow);
      code = terrno;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }
  if (dataInsertInfo->isLastBlock) {
    int32_t nRows = taosArrayGetSize(tbData->aRowP);
    if (taosArrayGetSize(tbData->aRowP) == 0) {
      tbData->flags |= SUBMIT_REQ_ONLY_CREATE_TABLE;
      stDebug("no valid data to insert, try to only create tabale:%s", pInsertParam->tbname);
    }
    stDebug("appendInsertData, isLastBlock:%d, needSortMerge:%d, totalRows:%d", dataInsertInfo->isLastBlock,
            dataInsertInfo->needSortMerge, nRows);
    if (dataInsertInfo->needSortMerge) {
      if ((tRowSort(tbData->aRowP) != TSDB_CODE_SUCCESS) ||
          (code = tRowMerge(tbData->aRowP, (STSchema*)pTSchema, KEEP_CONSISTENCY)) != 0) {
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
    nRows = taosArrayGetSize(tbData->aRowP);
    stDebug("appendInsertData, after merge, totalRows:%d", nRows);
  }

_end:
  taosArrayDestroy(pVals);
  return code;
}

int32_t buildStreamSubmitReqFromBlock(SStreamRunnerTask* pTask, SDataInserterHandle* pInserter,
                                      SStreamDataInserterInfo* pInserterInfo, SSubmitReq2** ppReq,
                                      const SSDataBlock* pDataBlock, SVgroupInfo* vgInfo,
                                      SBuildInsertDataInfo* tbDataInfo) {
  SSubmitReq2* pReq = *ppReq;
  int32_t      numOfBlks = 0;

  int32_t               code = TSDB_CODE_SUCCESS;
  int32_t               lino = 0;
  SStreamInserterParam* pInsertParam = pInserter->pParam->streamInserterParam;
  SInsertTableInfo**    ppTbInfo = NULL;
  SInsertTableInfo*     pTbInfo = NULL;
  STSchema*             pTSchema = NULL;
  SSubmitTbData*        tbData = &tbDataInfo->pTbData;
  int32_t               colNum = 0;
  int32_t               rows = 0;

  if (NULL == pReq) {
    if (!(pReq = taosMemoryCalloc(1, sizeof(SSubmitReq2)))) {
      code = terrno;
      QUERY_CHECK_CODE(code, lino, _end);
    }
    *ppReq = pReq;

    if (!(pReq->aSubmitTbData = taosArrayInit(1, sizeof(SSubmitTbData)))) {
      code = terrno;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  if (pDataBlock) {
    colNum = taosArrayGetSize(pDataBlock->pDataBlock);
    rows = pDataBlock->info.rows;
  }

  tbData->flags |= SUBMIT_REQ_SCHEMA_RES;

  if (tbDataInfo->isFirstBlock) {
    if (pInserterInfo->isAutoCreateTable) {
      code = initTableInfo(pInserter, pInserterInfo);
      QUERY_CHECK_CODE(code, lino, _end);
      if (pInsertParam->tbType == TSDB_NORMAL_TABLE) {
        code = buildNormalTableCreateReq(pInserter, pInsertParam, tbData);
      } else if (pInsertParam->tbType == TSDB_SUPER_TABLE) {
        code = buildStreamSubTableCreateReq(pTask, pInserter, pInsertParam, pInserterInfo, tbData);
      } else {
        code = TSDB_CODE_MND_STREAM_INTERNAL_ERROR;
        ST_TASK_ELOG("buildStreamSubmitReqFromBlock, unknown table type %d", pInsertParam->tbType);
      }
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  code = getStreamInsertTableInfo(pInserterInfo->streamId, pInserterInfo->groupId, &ppTbInfo);
  pTbInfo = *ppTbInfo;
  if (tbDataInfo->isFirstBlock) {
    if (!pInserterInfo->isAutoCreateTable) {
      tstrncpy(pInserterInfo->tbName, pTbInfo->tbname, TSDB_TABLE_NAME_LEN);
    }

    tbData->uid = pTbInfo->uid;
    tbData->sver = pTbInfo->version;

    if (pInsertParam->tbType == TSDB_SUPER_TABLE) {
      tbData->suid = pInsertParam->suid;
    }

    pTSchema = pTbInfo->pSchema;
  } else {
    pTSchema = pTbInfo->pSchema;
  }

  code = getTableVgInfo(pInserter, pInsertParam->dbFName, pTbInfo->tbname, vgInfo);
  QUERY_CHECK_CODE(code, lino, _end);

  ST_TASK_DLOG("[data inserter], Handle:%p, GROUP:%" PRId64 " tbname:%s autoCreate:%d uid:%" PRId64 " suid:%" PRId64
               " sver:%d vgid:%d isLastBlock:%d",
               pInserter, pInserterInfo->groupId, pInserterInfo->tbName, pInserterInfo->isAutoCreateTable, tbData->uid,
               tbData->suid, tbData->sver, vgInfo->vgId, tbDataInfo->isFirstBlock);

  code = appendInsertData(pInsertParam, pDataBlock, tbData, pTSchema, tbDataInfo);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  releaseStreamInsertTableInfo(ppTbInfo);
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("buildStreamSubmitReqFromBlock, code:0x%0x, groupId:%" PRId64 " tbname:%s autoCreate:%d", code,
                 pInserterInfo->groupId, pInserterInfo->tbName, pInserterInfo->isAutoCreateTable);
  }
  return code;
}

int32_t streamDataBlocksToSubmitReq(SStreamRunnerTask* pTask, SDataInserterHandle* pInserter,
                                    SStreamDataInserterInfo* pInserterInfo, void** pMsg, int32_t* msgLen,
                                    SVgroupInfo* vgInfo) {
  int32_t code = 0;
  int32_t lino = 0;

  const SArray*        pBlocks = pInserter->pDataBlocks;
  int32_t              sz = taosArrayGetSize(pBlocks);
  SSubmitReq2*         pReq = NULL;
  SBuildInsertDataInfo tbDataInfo = {0};

  int32_t rows = 0;
  for (int32_t i = 0; i < sz; i++) {
    SSDataBlock* pDataBlock = taosArrayGetP(pBlocks, i);
    if (NULL == pDataBlock) {
      stDebug("data block is NULL, just create empty table");
      continue;
    }
    rows += pDataBlock->info.rows;
  }
  code = initInsertProcessInfo(&tbDataInfo, rows);
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("streamDataBlocksToSubmitReq, initInsertDataInfo failed, code:%d", code);
    return code;
  }

  for (int32_t i = 0; i < sz; i++) {
    tbDataInfo.isFirstBlock = (i == 0);
    tbDataInfo.isLastBlock = (i == sz - 1);
    SSDataBlock* pDataBlock = taosArrayGetP(pBlocks, i);  // pDataBlock select查询到的结果
    ST_TASK_DLOG("[data inserter], Handle:%p, GROUP:%" PRId64
            " tbname:%s autoCreate:%d block: %d/%d rows:%" PRId64,
            pInserter, pInserterInfo->groupId, pInserterInfo->tbName,
            pInserterInfo->isAutoCreateTable, i + 1, sz, (pDataBlock != NULL ? pDataBlock->info.rows : 0));
    code = buildStreamSubmitReqFromBlock(pTask, pInserter, pInserterInfo, &pReq, pDataBlock, vgInfo, &tbDataInfo);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (NULL == taosArrayPush(pReq->aSubmitTbData, &tbDataInfo.pTbData)) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _end);
  }

  code = submitReqToMsg(vgInfo->vgId, pReq, pMsg, msgLen);
  tDestroySubmitReq(pReq, TSDB_MSG_FLG_ENCODE);
  taosMemoryFree(pReq);
  ST_TASK_DLOG("[data inserter], submit req, vgid:%d, GROUP:%" PRId64 " tbname:%s autoCreate:%d code:%d ", vgInfo->vgId,
               pInserterInfo->groupId, pInserterInfo->tbName, pInserterInfo->isAutoCreateTable, code);

_end:
  if (code != 0) {
    tDestroySubmitTbData(&tbDataInfo.pTbData, TSDB_MSG_FLG_ENCODE);
    tDestroySubmitReq(pReq, TSDB_MSG_FLG_ENCODE);
    taosMemoryFree(pReq);
  }

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
        code = sendSubmitRequest(pInserter, NULL, pMsg->pData, pMsg->len,
                                 pInserter->pParam->readHandle->pMsgCb->clientRpc, &pInserter->pNode->epSet);
        taosMemoryFree(pMsg);
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

      code = sendSubmitRequest(pInserter, NULL, pMsg, msgLen, pInserter->pParam->readHandle->pMsgCb->clientRpc,
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

static int32_t resetInserterTbVersion(SDataInserterHandle* pInserter, const SInputData* pInput) {
  SInsertTableInfo** ppTbInfo = NULL;
  int32_t           code = getStreamInsertTableInfo(pInput->pStreamDataInserterInfo->streamId, pInput->pStreamDataInserterInfo->groupId, &ppTbInfo);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  SInsertTableInfo*  pTbInfo  = *ppTbInfo;
  stDebug("resetInserterTbVersion, streamId:0x%" PRIx64 " groupId:%" PRId64 " tbName:%s, uid:%" PRId64 ", version:%d",
          pInput->pStreamDataInserterInfo->streamId, pInput->pStreamDataInserterInfo->groupId,
          pInput->pStreamDataInserterInfo->tbName, pTbInfo->uid, pTbInfo->version);
  if (pInserter->pParam->streamInserterParam->tbType != TSDB_NORMAL_TABLE) {
    pInserter->pParam->streamInserterParam->sver = pTbInfo->version;
  }
  code = releaseStreamInsertTableInfo(ppTbInfo);
  return code;
}

static int32_t putStreamDataBlock(SDataSinkHandle* pHandle, const SInputData* pInput, bool* pContinue) {
  int32_t              code = 0;
  int32_t              lino = 0;
  SDataInserterHandle* pInserter = (SDataInserterHandle*)pHandle;
  SStreamRunnerTask*   pTask = pInput->pTask;
  if (!pInserter || !pInserter->pParam || !pInserter->pParam->streamInserterParam) {
    ST_TASK_ELOG("putStreamDataBlock invalid param, pInserter: %p, pParam:%p", pInserter,
                 pInserter ? pInserter->pParam : NULL);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  if (!pInserter->explain) {
    code = TSDB_CODE_SUCCESS;
    if (NULL == taosArrayPush(pInserter->pDataBlocks, &pInput->pData)) {
      return terrno;
    }
    void*       pMsg = NULL;
    int32_t     msgLen = 0;
    SVgroupInfo vgInfo = {0};

    code = streamDataBlocksToSubmitReq(pTask, pInserter, pInput->pStreamDataInserterInfo, &pMsg, &msgLen, &vgInfo);
    QUERY_CHECK_CODE(code, lino, _return);

    code = sendSubmitRequest(pInserter, pInput->pStreamDataInserterInfo, pMsg, msgLen,
                             pInserter->pParam->readHandle->pMsgCb->clientRpc, &vgInfo.epSet);
    QUERY_CHECK_CODE(code, lino, _return);

    code = tsem_wait(&pInserter->ready);
    QUERY_CHECK_CODE(code, lino, _return);

    if (pInserter->submitRes.code == TSDB_CODE_TDB_TABLE_ALREADY_EXIST) {
      pInput->pStreamDataInserterInfo->isAutoCreateTable = false;
      code = resetInserterTbVersion(pInserter, pInput);
      QUERY_CHECK_CODE(code, lino, _return);

      code = streamDataBlocksToSubmitReq(pTask, pInserter, pInput->pStreamDataInserterInfo, &pMsg, &msgLen, &vgInfo);
      QUERY_CHECK_CODE(code, lino, _return);

      code = sendSubmitRequest(pInserter, pInput->pStreamDataInserterInfo, pMsg, msgLen,
                               pInserter->pParam->readHandle->pMsgCb->clientRpc, &vgInfo.epSet);
      QUERY_CHECK_CODE(code, lino, _return);

      code = tsem_wait(&pInserter->ready);
      QUERY_CHECK_CODE(code, lino, _return);
    }

    if (pInput->pStreamDataInserterInfo->isAutoCreateTable &&
        pInserter->submitRes.code == TSDB_CODE_VND_INVALID_VGROUP_ID) {
      rmDbVgInfoFromCache(pInserter->pParam->streamInserterParam->dbFName);
      ST_TASK_ILOG("putStreamDataBlock, stream inserter table info not found, groupId:%" PRId64
                   ", tbName:%s. so reset dbVgInfo and try again",
                   pInput->pStreamDataInserterInfo->groupId, pInput->pStreamDataInserterInfo->tbName);
      return putStreamDataBlock(pHandle, pInput, pContinue);
    }

    if ((pInserter->submitRes.code == TSDB_CODE_TDB_TABLE_NOT_EXIST &&
         !pInput->pStreamDataInserterInfo->isAutoCreateTable) ||
        pInserter->submitRes.code == TSDB_CODE_VND_INVALID_VGROUP_ID) {
      rmDbVgInfoFromCache(pInserter->pParam->streamInserterParam->dbFName);
      ST_TASK_ILOG("putStreamDataBlock, stream inserter table info not found, groupId:%" PRId64
                   ", tbName:%s. so reset dbVgInfo",
                   pInput->pStreamDataInserterInfo->groupId, pInput->pStreamDataInserterInfo->tbName);
      code = TSDB_CODE_STREAM_INSERT_TBINFO_NOT_FOUND;
      QUERY_CHECK_CODE(code, lino, _return);
    }

    if (pInserter->submitRes.code) {
      code = pInserter->submitRes.code;
      ST_TASK_ELOG("submitRes err:%s, code:%0x", tstrerror(pInserter->submitRes.code), pInserter->submitRes.code);
      QUERY_CHECK_CODE(code, lino, _return);
    }

    *pContinue = true;

  _return:
    taosArrayClear(pInserter->pDataBlocks);
    if (code == TSDB_CODE_STREAM_NO_DATA) {
      ST_TASK_DLOG("putStreamDataBlock, no valid data to insert, skip this block, groupID:%" PRId64,
                   pInput->pStreamDataInserterInfo->groupId);
      code = TSDB_CODE_SUCCESS;
    } else if (code) {
      ST_TASK_ELOG("submitRes err:%s, code:%0x lino:%d", tstrerror(code), code, lino);
      return code;
    }
    return code;
  }
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
  if (pInserter->pParam->streamInserterParam) {
    destroyStreamInserterParam(pInserter->pParam->streamInserterParam);
    taosMemoryFree(pInserter->pParam->readHandle); // only for stream
  }
  taosMemoryFree(pInserter->pParam);
  taosHashCleanup(pInserter->pCols);
  nodesDestroyNode((SNode*)pInserter->pNode);
  pInserter->pNode = NULL;

  (void)taosThreadMutexDestroy(&pInserter->mutex);

  taosMemoryFree(pInserter->pManager);

  if (pInserter->dbVgInfoMap) {
    taosHashSetFreeFp(pInserter->dbVgInfoMap, freeUseDbOutput_tmp);
    taosHashCleanup(pInserter->dbVgInfoMap);
  }

  if (pInserter->pTagSchema) {
    taosMemoryFreeClear(pInserter->pTagSchema->pSchema);
    taosMemoryFree(pInserter->pTagSchema);
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
  SDataSinkNode*       pDataSink = *ppDataSink;
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

  nodesDestroyNode((SNode*)*ppDataSink);
  *ppDataSink = NULL;

  return terrno;
}

                           
static TdThreadOnce g_dbVgInfoMgrInit = PTHREAD_ONCE_INIT;

SDBVgInfoMgr g_dbVgInfoMgr = {0};
                           
void dbVgInfoMgrInitOnce() {
  g_dbVgInfoMgr.dbVgInfoMap = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  if (g_dbVgInfoMgr.dbVgInfoMap == NULL) {
    stError("%s failed at line %d, error:%s", __FUNCTION__, __LINE__, tstrerror(terrno));
    return;
  }

  taosHashSetFreeFp(g_dbVgInfoMgr.dbVgInfoMap, freeUseDbOutput_tmp);
}



int32_t createStreamDataInserter(SDataSinkManager* pManager, DataSinkHandle* pHandle, void* pParam) {
  int32_t code = TSDB_CODE_SUCCESS, lino = 0;

  TAOS_UNUSED(taosThreadOnce(&g_dbVgInfoMgrInit, dbVgInfoMgrInitOnce));
  TSDB_CHECK_NULL(g_dbVgInfoMgr.dbVgInfoMap, code, lino, _exit, terrno);

  SDataInserterHandle* inserter = taosMemoryCalloc(1, sizeof(SDataInserterHandle));
  TSDB_CHECK_NULL(inserter, code, lino, _exit, terrno);

  inserter->sink.fPut = putStreamDataBlock;
  inserter->sink.fEndPut = endPut;
  inserter->sink.fGetLen = getDataLength;
  inserter->sink.fGetData = NULL;
  inserter->sink.fDestroy = destroyDataSinker;
  inserter->sink.fGetCacheSize = getCacheSize;
  inserter->sink.fGetFlags = getSinkFlags;
  inserter->pManager = pManager;
  inserter->pNode = NULL;
  inserter->pParam = pParam;
  inserter->status = DS_BUF_EMPTY;
  inserter->queryEnd = false;
  inserter->explain = false;

  inserter->pDataBlocks = taosArrayInit(1, POINTER_BYTES);
  TSDB_CHECK_NULL(inserter->pDataBlocks, code, lino, _exit, terrno);
  
  TAOS_CHECK_EXIT(taosThreadMutexInit(&inserter->mutex, NULL));
  TAOS_CHECK_EXIT(tsem_init(&inserter->ready, 0, 0));

  inserter->dbVgInfoMap = NULL;

  *pHandle = inserter;
  return TSDB_CODE_SUCCESS;

_exit:

  if (inserter) {
    (void)destroyDataSinker((SDataSinkHandle*)inserter);
    taosMemoryFree(inserter);
  } else {
    taosMemoryFree(pManager);
  }

  if (code) {
    stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

int32_t getDbVgInfoByTbName(void* clientRpc, const char* dbFName, SDBVgInfo** dbVgInfo) {
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       line = 0;
  SUseDbOutput* output = NULL;

  SUseDbOutput** find = (SUseDbOutput**)taosHashGet(g_dbVgInfoMgr.dbVgInfoMap, dbFName, strlen(dbFName));

  if (find == NULL) {
    output = taosMemoryCalloc(1, sizeof(SUseDbOutput));
    if (output == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    code = buildDbVgInfoMap(clientRpc, dbFName, output);
    QUERY_CHECK_CODE(code, line, _return);

    code = taosHashPut(g_dbVgInfoMgr.dbVgInfoMap, dbFName, strlen(dbFName), &output, POINTER_BYTES);
    if (code == TSDB_CODE_DUP_KEY) {
      code = TSDB_CODE_SUCCESS;
      // another thread has put the same dbFName, so we need to free the output
      freeUseDbOutput_tmp(&output);
      find = (SUseDbOutput**)taosHashGet(g_dbVgInfoMgr.dbVgInfoMap, dbFName, strlen(dbFName));
      if (find == NULL) {
        QUERY_CHECK_CODE(code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR, line, _return);
      }
      output = *find;
    }
    QUERY_CHECK_CODE(code, line, _return);
  } else {
    output = *find;
  }

  *dbVgInfo = output->dbVgroup;
  return code;

_return:
  qError("%s failed at line %d since %s", __func__, line, tstrerror(code));
  freeUseDbOutput_tmp(&output);
  return code;
}

int32_t getDbVgInfoForExec(void* clientRpc, const char* dbFName, const char* tbName, SVgroupInfo* pVgInfo) {
  SDBVgInfo* dbInfo = NULL;
  int32_t code = 0, lino = 0;
  char tbFullName[TSDB_TABLE_FNAME_LEN];
  snprintf(tbFullName, TSDB_TABLE_FNAME_LEN, "%s.%s", dbFName, tbName);
  
  taosRLockLatch(&g_dbVgInfoMgr.lock);
  
  TAOS_CHECK_EXIT(getDbVgInfoByTbName(clientRpc, dbFName, &dbInfo));

  TAOS_CHECK_EXIT(inserterGetVgInfo(dbInfo, tbFullName, pVgInfo));

_exit:

  taosRUnLockLatch(&g_dbVgInfoMgr.lock);

  if (code) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return code;
}

void rmDbVgInfoFromCache(const char* dbFName) {
  taosWLockLatch(&g_dbVgInfoMgr.lock);

  TAOS_UNUSED(taosHashRemove(g_dbVgInfoMgr.dbVgInfoMap, dbFName, strlen(dbFName)));

  taosWUnLockLatch(&g_dbVgInfoMgr.lock);
}

static int32_t dropTableReqToMsg(int32_t vgId, SVDropTbBatchReq* pReq, void** pData, int32_t* pLen) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t len = 0;
  void*   pBuf = NULL;
  tEncodeSize(tEncodeSVDropTbBatchReq, pReq, len, code);
  if (TSDB_CODE_SUCCESS == code) {
    SEncoder encoder;
    len += sizeof(SMsgHead);
    pBuf = taosMemoryMalloc(len);
    if (NULL == pBuf) {
      return terrno;
    }
    ((SDropTbDataMsg*)pBuf)->header.vgId = htonl(vgId);
    ((SDropTbDataMsg*)pBuf)->header.contLen = htonl(len);
    //((SDropTbDataMsg*)pBuf)->pData = POINTER_SHIFT(pBuf, sizeof(SMsgHead));
    tEncoderInit(&encoder, POINTER_SHIFT(pBuf, sizeof(SMsgHead)), len - sizeof(SMsgHead));
    code = tEncodeSVDropTbBatchReq(&encoder, pReq);
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

int32_t dropTbCallback(void* param, SDataBuf* pMsg, int32_t code) {
  SDropTbCtx* pCtx = (SDropTbCtx*)param;
  if (code) {
    stError("dropTbCallback, code:%d, stream:%" PRId64 " gid:%" PRId64, code, pCtx->req->streamId, pCtx->req->gid);
  }
  pCtx->code = code;
  code = tsem_post(&pCtx->ready);
  taosMemoryFree(pMsg->pData);

  return TSDB_CODE_SUCCESS;
}

static int32_t sendDropTbRequest(SDropTbCtx* ctx, void* pMsg, int32_t msgLen, void* pTransporter, SEpSet* pEpset) {
  // send the fetch remote task result reques
  SMsgSendInfo* pMsgSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (NULL == pMsgSendInfo) {
    return terrno;
  }

  pMsgSendInfo->param = ctx;
  pMsgSendInfo->paramFreeFp = NULL;
  pMsgSendInfo->msgInfo.pData = pMsg;
  pMsgSendInfo->msgInfo.len = msgLen;
  pMsgSendInfo->msgType = TDMT_VND_SNODE_DROP_TABLE;
  pMsgSendInfo->fp = dropTbCallback;

  return asyncSendMsgToServer(pTransporter, pEpset, NULL, pMsgSendInfo);
}

int32_t doDropStreamTable(SMsgCb* pMsgCb, void* pTaskOutput, SSTriggerDropRequest* pReq) {
  SStreamRunnerTaskOutput* pOutput = pTaskOutput;
  int32_t                  code = 0;
  int32_t                  lino = 0;
  SVDropTbBatchReq         req = {.nReqs = 1};
  SVDropTbReq*             pDropReq = NULL;
  int32_t                  msgLen = 0;
  tsem_t*                  pSem = NULL;
  SDropTbDataMsg*          pMsg = NULL;

  SInsertTableInfo** ppTbInfo = NULL;
  int32_t            vgId = 0;

  req.pArray = taosArrayInit_s(sizeof(SVDropTbReq), 1);
  if (!req.pArray) return terrno;

  pDropReq = taosArrayGet(req.pArray, 0);

  code = getStreamInsertTableInfo(pReq->streamId, pReq->gid, &ppTbInfo);
  if (TSDB_CODE_SUCCESS == code) {
    pDropReq->name = taosStrdup((*ppTbInfo)->tbname);
    pDropReq->suid = (*ppTbInfo)->uid;
    pDropReq->uid = (*ppTbInfo)->uid;
    pDropReq->igNotExists = true;
    vgId = (*ppTbInfo)->vgid;

    int64_t key[2] = {pReq->streamId, pReq->gid};
    TAOS_UNUSED(taosHashRemove(gStreamGrpTableHash, key, sizeof(key)));
  } else {
    code = TSDB_CODE_STREAM_INSERT_TBINFO_NOT_FOUND;
  }
  QUERY_CHECK_CODE(code, lino, _end);

  code = dropTableReqToMsg(vgId, &req, (void**)&pMsg, &msgLen);
  QUERY_CHECK_CODE(code, lino, _end);

  SVgroupInfo vgInfo = {0};
  code = getDbVgInfoForExec(pMsgCb->clientRpc, pOutput->outDbFName, pDropReq->name, &vgInfo);
  QUERY_CHECK_CODE(code, lino, _end);

  SDropTbCtx ctx = {.req = pReq};
  code = tsem_init(&ctx.ready, 0, 0);
  QUERY_CHECK_CODE(code, lino, _end);
  pSem = &ctx.ready;

  code = sendDropTbRequest(&ctx, pMsg, msgLen, pMsgCb->clientRpc, &vgInfo.epSet);
  QUERY_CHECK_CODE(code, lino, _end);
  pMsg = NULL;  // now owned by sendDropTbRequest

  code = tsem_wait(&ctx.ready);
  code = ctx.code;
  stDebug("doDropStreamTable,  code:0x%" PRIx32 " req:%p, streamId:0x%" PRIx64 " groupId:%" PRId64 " tbname:%s", code, pReq,
          pReq->streamId, pReq->gid, pDropReq ? pDropReq->name : "unknown");

_end:
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_STREAM_INSERT_TBINFO_NOT_FOUND) {
    stError("doDropStreamTable, code:0x%" PRIx32 ", streamId:0x%" PRIx64 " groupId:%" PRId64 " tbname:%s", code, pReq->streamId,
            pReq->gid, pDropReq ? pDropReq->name : "unknown");
    if (pMsg) {
      taosMemoryFreeClear(pMsg);
    }
  }
  if (pSem) tsem_destroy(pSem);
  if (pDropReq && pDropReq->name) taosMemoryFreeClear(pDropReq->name);
  if (ppTbInfo) releaseStreamInsertTableInfo(ppTbInfo);
  taosArrayDestroy(req.pArray);

  return code;
}

int32_t doDropStreamTableByTbName(SMsgCb* pMsgCb, void* pTaskOutput, SSTriggerDropRequest* pReq, char* tbName) {
  SStreamRunnerTaskOutput* pOutput = pTaskOutput;
  int32_t                  code = 0;
  int32_t                  lino = 0;
  SVDropTbBatchReq         req = {.nReqs = 1};
  SVDropTbReq*             pDropReq = NULL;
  int32_t                  msgLen = 0;
  tsem_t*                  pSem = NULL;
  SDropTbDataMsg*          pMsg = NULL;

  TAOS_UNUSED(taosThreadOnce(&g_dbVgInfoMgrInit, dbVgInfoMgrInitOnce));

  req.pArray = taosArrayInit_s(sizeof(SVDropTbReq), 1);
  if (!req.pArray) return terrno;

  pDropReq = taosArrayGet(req.pArray, 0);

  pDropReq->name = tbName;
  pDropReq->igNotExists = true;

  int64_t key[2] = {pReq->streamId, pReq->gid};
  TAOS_UNUSED(taosHashRemove(gStreamGrpTableHash, key, sizeof(key)));

  SVgroupInfo vgInfo = {0};
  code = getDbVgInfoForExec(pMsgCb->clientRpc, pOutput->outDbFName, pDropReq->name, &vgInfo);
  QUERY_CHECK_CODE(code, lino, _end);

  code = dropTableReqToMsg(vgInfo.vgId, &req, (void**)&pMsg, &msgLen);
  QUERY_CHECK_CODE(code, lino, _end);

  SDropTbCtx ctx = {.req = pReq};
  code = tsem_init(&ctx.ready, 0, 0);
  QUERY_CHECK_CODE(code, lino, _end);
  pSem = &ctx.ready;

  code = sendDropTbRequest(&ctx, pMsg, msgLen, pMsgCb->clientRpc, &vgInfo.epSet);
  QUERY_CHECK_CODE(code, lino, _end);
  pMsg = NULL;  // now owned by sendDropTbRequest

  code = tsem_wait(&ctx.ready);
  code = ctx.code;
  stDebug("doDropStreamTableByTbName,  code:%d req:%p, streamId:0x%" PRIx64 " groupId:%" PRId64 " tbname:%s", code, pReq,
          pReq->streamId, pReq->gid, pDropReq ? pDropReq->name : "unknown");

_end:
  if (code != TSDB_CODE_SUCCESS) {
    stError("doDropStreamTableByTbName, code:%d, streamId:0x%" PRIx64 " groupId:%" PRId64 " tbname:%s", code, pReq->streamId,
            pReq->gid, pDropReq ? pDropReq->name : "unknown");
    if (pMsg) {
      taosMemoryFreeClear(pMsg);
    }
  }
  if (pSem) tsem_destroy(pSem);
  taosArrayDestroy(req.pArray);

  return code;
}
