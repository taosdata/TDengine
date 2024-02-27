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

#include "catalog.h"
#include "clientInt.h"
#include "clientLog.h"
#include "cmdnodes.h"
#include "os.h"
#include "query.h"
#include "systable.h"
#include "tdatablock.h"
#include "tdef.h"
#include "tglobal.h"
#include "tname.h"
#include "tversion.h"

extern SClientHbMgr clientHbMgr;

static void setErrno(SRequestObj* pRequest, int32_t code) {
  pRequest->code = code;
  terrno = code;
}

int32_t genericRspCallback(void* param, SDataBuf* pMsg, int32_t code) {
  SRequestObj* pRequest = param;
  setErrno(pRequest, code);

  if (NEED_CLIENT_RM_TBLMETA_REQ(pRequest->type)) {
    removeMeta(pRequest->pTscObj, pRequest->targetTableList, IS_VIEW_REQUEST(pRequest->type));
  }

  taosMemoryFree(pMsg->pEpSet);
  taosMemoryFree(pMsg->pData);
  if (pRequest->body.queryFp != NULL) {
    doRequestCallback(pRequest, code);
  } else {
    tsem_post(&pRequest->body.rspSem);
  }
  return code;
}

int32_t processConnectRsp(void* param, SDataBuf* pMsg, int32_t code) {
  SRequestObj* pRequest = acquireRequest(*(int64_t*)param);
  if (NULL == pRequest) {
    goto End;
  }

  if (code != TSDB_CODE_SUCCESS) {
    setErrno(pRequest, code);
    tsem_post(&pRequest->body.rspSem);
    goto End;
  }

  STscObj* pTscObj = pRequest->pTscObj;

  if (NULL == pTscObj->pAppInfo) {
    code = TSDB_CODE_TSC_DISCONNECTED;
    setErrno(pRequest, code);
    tsem_post(&pRequest->body.rspSem);
    goto End;
  }

  SConnectRsp connectRsp = {0};
  if (tDeserializeSConnectRsp(pMsg->pData, pMsg->len, &connectRsp) != 0) {
    code = TSDB_CODE_TSC_INVALID_VERSION;
    setErrno(pRequest, code);
    tsem_post(&pRequest->body.rspSem);
    goto End;
  }

  if ((code = taosCheckVersionCompatibleFromStr(version, connectRsp.sVer, 3)) != 0) {
    tscError("version not compatible. client version: %s, server version: %s", version, connectRsp.sVer);
    setErrno(pRequest, code);
    tsem_post(&pRequest->body.rspSem);
    goto End;
  }

  int32_t now = taosGetTimestampSec();
  int32_t delta = abs(now - connectRsp.svrTimestamp);
  if (delta > timestampDeltaLimit) {
    code = TSDB_CODE_TIME_UNSYNCED;
    tscError("time diff:%ds is too big", delta);
    setErrno(pRequest, code);
    tsem_post(&pRequest->body.rspSem);
    goto End;
  }

  if (connectRsp.epSet.numOfEps == 0) {
    code = TSDB_CODE_APP_ERROR;
    setErrno(pRequest, code);
    tsem_post(&pRequest->body.rspSem);
    goto End;
  }

  int updateEpSet = 1;
  if (connectRsp.dnodeNum == 1) {
    SEpSet srcEpSet = getEpSet_s(&pTscObj->pAppInfo->mgmtEp);
    SEpSet dstEpSet = connectRsp.epSet;
    if (srcEpSet.numOfEps == 1) {
      rpcSetDefaultAddr(pTscObj->pAppInfo->pTransporter, srcEpSet.eps[srcEpSet.inUse].fqdn,
                        dstEpSet.eps[dstEpSet.inUse].fqdn);
      updateEpSet = 0;
    }
  }
  if (updateEpSet == 1 && !isEpsetEqual(&pTscObj->pAppInfo->mgmtEp.epSet, &connectRsp.epSet)) {
    SEpSet corEpSet = getEpSet_s(&pTscObj->pAppInfo->mgmtEp);

    SEpSet* pOrig = &corEpSet;
    SEp*    pOrigEp = &pOrig->eps[pOrig->inUse];
    SEp*    pNewEp = &connectRsp.epSet.eps[connectRsp.epSet.inUse];
    tscDebug("mnode epset updated from %d/%d=>%s:%d to %d/%d=>%s:%d in connRsp", pOrig->inUse, pOrig->numOfEps,
             pOrigEp->fqdn, pOrigEp->port, connectRsp.epSet.inUse, connectRsp.epSet.numOfEps, pNewEp->fqdn,
             pNewEp->port);
    updateEpSet_s(&pTscObj->pAppInfo->mgmtEp, &connectRsp.epSet);
  }

  for (int32_t i = 0; i < connectRsp.epSet.numOfEps; ++i) {
    tscDebug("0x%" PRIx64 " epSet.fqdn[%d]:%s port:%d, connObj:0x%" PRIx64, pRequest->requestId, i,
             connectRsp.epSet.eps[i].fqdn, connectRsp.epSet.eps[i].port, pTscObj->id);
  }

  pTscObj->sysInfo = connectRsp.sysInfo;
  pTscObj->connId = connectRsp.connId;
  pTscObj->acctId = connectRsp.acctId;
  tstrncpy(pTscObj->sVer, connectRsp.sVer, tListLen(pTscObj->sVer));
  tstrncpy(pTscObj->sDetailVer, connectRsp.sDetailVer, tListLen(pTscObj->sDetailVer));

  // update the appInstInfo
  pTscObj->pAppInfo->clusterId = connectRsp.clusterId;
  lastClusterId = connectRsp.clusterId;

  pTscObj->connType = connectRsp.connType;
  pTscObj->passInfo.ver = connectRsp.passVer;
  pTscObj->authVer = connectRsp.authVer;
  pTscObj->whiteListInfo.ver = connectRsp.whiteListVer;

  taosThreadMutexLock(&clientHbMgr.lock);
  SAppHbMgr* pAppHbMgr = taosArrayGetP(clientHbMgr.appHbMgrs, pTscObj->appHbMgrIdx);
  if (pAppHbMgr) {
    hbRegisterConn(pAppHbMgr, pTscObj->id, connectRsp.clusterId, connectRsp.connType);
  } else {
    taosThreadMutexUnlock(&clientHbMgr.lock);
    code = TSDB_CODE_TSC_DISCONNECTED;
    setErrno(pRequest, code);
    tsem_post(&pRequest->body.rspSem);
    goto End;
  }
  taosThreadMutexUnlock(&clientHbMgr.lock);

  tscDebug("0x%" PRIx64 " clusterId:%" PRId64 ", totalConn:%" PRId64, pRequest->requestId, connectRsp.clusterId,
           pTscObj->pAppInfo->numOfConns);

  tsem_post(&pRequest->body.rspSem);
End:

  if (pRequest) {
    releaseRequest(pRequest->self);
  }

  taosMemoryFree(param);
  taosMemoryFree(pMsg->pEpSet);
  taosMemoryFree(pMsg->pData);
  return code;
}

SMsgSendInfo* buildMsgInfoImpl(SRequestObj* pRequest) {
  SMsgSendInfo* pMsgSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));

  pMsgSendInfo->requestObjRefId = pRequest->self;
  pMsgSendInfo->requestId = pRequest->requestId;
  pMsgSendInfo->param = pRequest;
  pMsgSendInfo->msgType = pRequest->type;
  pMsgSendInfo->target.type = TARGET_TYPE_MNODE;

  pMsgSendInfo->msgInfo = pRequest->body.requestMsg;
  pMsgSendInfo->fp = getMsgRspHandle(pRequest->type);
  return pMsgSendInfo;
}

int32_t processCreateDbRsp(void* param, SDataBuf* pMsg, int32_t code) {
  // todo rsp with the vnode id list
  SRequestObj* pRequest = param;
  taosMemoryFree(pMsg->pData);
  taosMemoryFree(pMsg->pEpSet);
  if (code != TSDB_CODE_SUCCESS) {
    setErrno(pRequest, code);
  } else {
    struct SCatalog* pCatalog = NULL;
    int32_t          code = catalogGetHandle(pRequest->pTscObj->pAppInfo->clusterId, &pCatalog);
    if (TSDB_CODE_SUCCESS == code) {
      STscObj* pTscObj = pRequest->pTscObj;

      SRequestConnInfo conn = {.pTrans = pTscObj->pAppInfo->pTransporter,
                               .requestId = pRequest->requestId,
                               .requestObjRefId = pRequest->self,
                               .mgmtEps = getEpSet_s(&pTscObj->pAppInfo->mgmtEp)};
      char             dbFName[TSDB_DB_FNAME_LEN];
      snprintf(dbFName, sizeof(dbFName) - 1, "%d.%s", pTscObj->acctId, TSDB_INFORMATION_SCHEMA_DB);
      catalogRefreshDBVgInfo(pCatalog, &conn, dbFName);
      snprintf(dbFName, sizeof(dbFName) - 1, "%d.%s", pTscObj->acctId, TSDB_PERFORMANCE_SCHEMA_DB);
      catalogRefreshDBVgInfo(pCatalog, &conn, dbFName);
    }
  }

  if (pRequest->body.queryFp) {
    doRequestCallback(pRequest, code);
  } else {
    tsem_post(&pRequest->body.rspSem);
  }
  return code;
}

int32_t processUseDbRsp(void* param, SDataBuf* pMsg, int32_t code) {
  SRequestObj* pRequest = param;

  if (TSDB_CODE_MND_DB_NOT_EXIST == code || TSDB_CODE_MND_DB_IN_CREATING == code ||
      TSDB_CODE_MND_DB_IN_DROPPING == code) {
    SUseDbRsp usedbRsp = {0};
    tDeserializeSUseDbRsp(pMsg->pData, pMsg->len, &usedbRsp);
    struct SCatalog* pCatalog = NULL;

    if (usedbRsp.vgVersion >= 0) {  // cached in local
      uint64_t clusterId = pRequest->pTscObj->pAppInfo->clusterId;
      int32_t  code1 = catalogGetHandle(clusterId, &pCatalog);
      if (code1 != TSDB_CODE_SUCCESS) {
        tscWarn("0x%" PRIx64 "catalogGetHandle failed, clusterId:%" PRIx64 ", error:%s", pRequest->requestId, clusterId,
                tstrerror(code1));
      } else {
        catalogRemoveDB(pCatalog, usedbRsp.db, usedbRsp.uid);
      }
    }

    tFreeSUsedbRsp(&usedbRsp);
  }

  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pMsg->pData);
    taosMemoryFree(pMsg->pEpSet);
    setErrno(pRequest, code);

    if (pRequest->body.queryFp != NULL) {
      doRequestCallback(pRequest, pRequest->code);

    } else {
      tsem_post(&pRequest->body.rspSem);
    }

    return code;
  }

  SUseDbRsp usedbRsp = {0};
  tDeserializeSUseDbRsp(pMsg->pData, pMsg->len, &usedbRsp);

  if (strlen(usedbRsp.db) == 0) {
    if (usedbRsp.errCode != 0) {
      return usedbRsp.errCode;
    } else {
      return TSDB_CODE_APP_ERROR;
    }
  }

  tscTrace("db:%s, usedbRsp received, numOfVgroups:%d", usedbRsp.db, usedbRsp.vgNum);
  for (int32_t i = 0; i < usedbRsp.vgNum; ++i) {
    SVgroupInfo* pInfo = taosArrayGet(usedbRsp.pVgroupInfos, i);
    tscTrace("vgId:%d, numOfEps:%d inUse:%d ", pInfo->vgId, pInfo->epSet.numOfEps, pInfo->epSet.inUse);
    for (int32_t j = 0; j < pInfo->epSet.numOfEps; ++j) {
      tscTrace("vgId:%d, index:%d epset:%s:%u", pInfo->vgId, j, pInfo->epSet.eps[j].fqdn, pInfo->epSet.eps[j].port);
    }
  }

  SName name = {0};
  tNameFromString(&name, usedbRsp.db, T_NAME_ACCT | T_NAME_DB);

  SUseDbOutput output = {0};
  code = queryBuildUseDbOutput(&output, &usedbRsp);

  if (code != 0) {
    terrno = code;
    if (output.dbVgroup) taosHashCleanup(output.dbVgroup->vgHash);

    tscError("0x%" PRIx64 " failed to build use db output since %s", pRequest->requestId, terrstr());
  } else if (output.dbVgroup && output.dbVgroup->vgHash) {
    struct SCatalog* pCatalog = NULL;

    int32_t code1 = catalogGetHandle(pRequest->pTscObj->pAppInfo->clusterId, &pCatalog);
    if (code1 != TSDB_CODE_SUCCESS) {
      tscWarn("catalogGetHandle failed, clusterId:%" PRIx64 ", error:%s", pRequest->pTscObj->pAppInfo->clusterId,
              tstrerror(code1));
    } else {
      catalogUpdateDBVgInfo(pCatalog, output.db, output.dbId, output.dbVgroup);
      output.dbVgroup = NULL;
    }
  }

  taosMemoryFreeClear(output.dbVgroup);

  tFreeSUsedbRsp(&usedbRsp);

  char db[TSDB_DB_NAME_LEN] = {0};
  tNameGetDbName(&name, db);

  setConnectionDB(pRequest->pTscObj, db);
  taosMemoryFree(pMsg->pData);
  taosMemoryFree(pMsg->pEpSet);

  if (pRequest->body.queryFp != NULL) {
    doRequestCallback(pRequest, pRequest->code);
  } else {
    tsem_post(&pRequest->body.rspSem);
  }
  return 0;
}

int32_t processCreateSTableRsp(void* param, SDataBuf* pMsg, int32_t code) {
  if (pMsg == NULL || param == NULL) {
    return TSDB_CODE_TSC_INVALID_INPUT;
  }
  SRequestObj* pRequest = param;

  if (code != TSDB_CODE_SUCCESS) {
    setErrno(pRequest, code);
  } else {
    SMCreateStbRsp createRsp = {0};
    SDecoder       coder = {0};
    tDecoderInit(&coder, pMsg->pData, pMsg->len);
    tDecodeSMCreateStbRsp(&coder, &createRsp);
    tDecoderClear(&coder);

    pRequest->body.resInfo.execRes.msgType = TDMT_MND_CREATE_STB;
    pRequest->body.resInfo.execRes.res = createRsp.pMeta;
  }

  taosMemoryFree(pMsg->pEpSet);
  taosMemoryFree(pMsg->pData);

  if (pRequest->body.queryFp != NULL) {
    SExecResult* pRes = &pRequest->body.resInfo.execRes;

    if (code == TSDB_CODE_SUCCESS) {
      SCatalog* pCatalog = NULL;
      int32_t   ret = catalogGetHandle(pRequest->pTscObj->pAppInfo->clusterId, &pCatalog);
      if (pRes->res != NULL) {
        ret = handleCreateTbExecRes(pRes->res, pCatalog);
      }

      if (ret != TSDB_CODE_SUCCESS) {
        code = ret;
      }
    }

    doRequestCallback(pRequest, code);
  } else {
    tsem_post(&pRequest->body.rspSem);
  }
  return code;
}

int32_t processDropDbRsp(void* param, SDataBuf* pMsg, int32_t code) {
  SRequestObj* pRequest = param;
  if (code != TSDB_CODE_SUCCESS) {
    setErrno(pRequest, code);
  } else {
    SDropDbRsp dropdbRsp = {0};
    tDeserializeSDropDbRsp(pMsg->pData, pMsg->len, &dropdbRsp);

    struct SCatalog* pCatalog = NULL;
    int32_t          code = catalogGetHandle(pRequest->pTscObj->pAppInfo->clusterId, &pCatalog);
    if (TSDB_CODE_SUCCESS == code) {
      catalogRemoveDB(pCatalog, dropdbRsp.db, dropdbRsp.uid);
      STscObj* pTscObj = pRequest->pTscObj;

      SRequestConnInfo conn = {.pTrans = pTscObj->pAppInfo->pTransporter,
                               .requestId = pRequest->requestId,
                               .requestObjRefId = pRequest->self,
                               .mgmtEps = getEpSet_s(&pTscObj->pAppInfo->mgmtEp)};
      char             dbFName[TSDB_DB_FNAME_LEN];
      snprintf(dbFName, sizeof(dbFName) - 1, "%d.%s", pTscObj->acctId, TSDB_INFORMATION_SCHEMA_DB);
      catalogRefreshDBVgInfo(pCatalog, &conn, dbFName);
      snprintf(dbFName, sizeof(dbFName) - 1, "%d.%s", pTscObj->acctId, TSDB_PERFORMANCE_SCHEMA_DB);
      catalogRefreshDBVgInfo(pCatalog, &conn, dbFName);
    }
  }

  taosMemoryFree(pMsg->pData);
  taosMemoryFree(pMsg->pEpSet);

  if (pRequest->body.queryFp != NULL) {
    doRequestCallback(pRequest, code);
  } else {
    tsem_post(&pRequest->body.rspSem);
  }
  return code;
}

int32_t processAlterStbRsp(void* param, SDataBuf* pMsg, int32_t code) {
  SRequestObj* pRequest = param;
  if (code != TSDB_CODE_SUCCESS) {
    setErrno(pRequest, code);
  } else {
    SMAlterStbRsp alterRsp = {0};
    SDecoder      coder = {0};
    tDecoderInit(&coder, pMsg->pData, pMsg->len);
    tDecodeSMAlterStbRsp(&coder, &alterRsp);
    tDecoderClear(&coder);

    pRequest->body.resInfo.execRes.msgType = TDMT_MND_ALTER_STB;
    pRequest->body.resInfo.execRes.res = alterRsp.pMeta;
  }

  taosMemoryFree(pMsg->pData);
  taosMemoryFree(pMsg->pEpSet);

  if (pRequest->body.queryFp != NULL) {
    SExecResult* pRes = &pRequest->body.resInfo.execRes;

    if (code == TSDB_CODE_SUCCESS) {
      SCatalog* pCatalog = NULL;
      int32_t   ret = catalogGetHandle(pRequest->pTscObj->pAppInfo->clusterId, &pCatalog);
      if (pRes->res != NULL) {
        ret = handleAlterTbExecRes(pRes->res, pCatalog);
      }

      if (ret != TSDB_CODE_SUCCESS) {
        code = ret;
      }
    }

    doRequestCallback(pRequest, code);
  } else {
    tsem_post(&pRequest->body.rspSem);
  }
  return code;
}

static int32_t buildShowVariablesBlock(SArray* pVars, SSDataBlock** block) {
  SSDataBlock* pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  pBlock->info.hasVarCol = true;

  pBlock->pDataBlock = taosArrayInit(SHOW_VARIABLES_RESULT_COLS, sizeof(SColumnInfoData));

  SColumnInfoData infoData = {0};
  infoData.info.type = TSDB_DATA_TYPE_VARCHAR;
  infoData.info.bytes = SHOW_VARIABLES_RESULT_FIELD1_LEN;
  taosArrayPush(pBlock->pDataBlock, &infoData);

  infoData.info.type = TSDB_DATA_TYPE_VARCHAR;
  infoData.info.bytes = SHOW_VARIABLES_RESULT_FIELD2_LEN;
  taosArrayPush(pBlock->pDataBlock, &infoData);

  infoData.info.type = TSDB_DATA_TYPE_VARCHAR;
  infoData.info.bytes = SHOW_VARIABLES_RESULT_FIELD3_LEN;
  taosArrayPush(pBlock->pDataBlock, &infoData);

  int32_t numOfCfg = taosArrayGetSize(pVars);
  blockDataEnsureCapacity(pBlock, numOfCfg);

  for (int32_t i = 0, c = 0; i < numOfCfg; ++i, c = 0) {
    SVariablesInfo* pInfo = taosArrayGet(pVars, i);

    char name[TSDB_CONFIG_OPTION_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(name, pInfo->name, TSDB_CONFIG_OPTION_LEN + VARSTR_HEADER_SIZE);
    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, c++);
    colDataSetVal(pColInfo, i, name, false);

    char value[TSDB_CONFIG_VALUE_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(value, pInfo->value, TSDB_CONFIG_VALUE_LEN + VARSTR_HEADER_SIZE);
    pColInfo = taosArrayGet(pBlock->pDataBlock, c++);
    colDataSetVal(pColInfo, i, value, false);

    char scope[TSDB_CONFIG_SCOPE_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(scope, pInfo->scope, TSDB_CONFIG_SCOPE_LEN + VARSTR_HEADER_SIZE);
    pColInfo = taosArrayGet(pBlock->pDataBlock, c++);
    colDataSetVal(pColInfo, i, scope, false);
  }

  pBlock->info.rows = numOfCfg;

  *block = pBlock;

  return TSDB_CODE_SUCCESS;
}

static int32_t buildShowVariablesRsp(SArray* pVars, SRetrieveTableRsp** pRsp) {
  SSDataBlock* pBlock = NULL;
  int32_t      code = buildShowVariablesBlock(pVars, &pBlock);
  if (code) {
    return code;
  }

  size_t rspSize = sizeof(SRetrieveTableRsp) + blockGetEncodeSize(pBlock);
  *pRsp = taosMemoryCalloc(1, rspSize);
  if (NULL == *pRsp) {
    blockDataDestroy(pBlock);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  (*pRsp)->useconds = 0;
  (*pRsp)->completed = 1;
  (*pRsp)->precision = 0;
  (*pRsp)->compressed = 0;
  (*pRsp)->compLen = 0;
  (*pRsp)->numOfRows = htobe64((int64_t)pBlock->info.rows);
  (*pRsp)->numOfCols = htonl(SHOW_VARIABLES_RESULT_COLS);

  int32_t len = blockEncode(pBlock, (*pRsp)->data, SHOW_VARIABLES_RESULT_COLS);
  blockDataDestroy(pBlock);

  if (len != rspSize - sizeof(SRetrieveTableRsp)) {
    uError("buildShowVariablesRsp error, len:%d != rspSize - sizeof(SRetrieveTableRsp):%" PRIu64, len,
           (uint64_t)(rspSize - sizeof(SRetrieveTableRsp)));
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t processShowVariablesRsp(void* param, SDataBuf* pMsg, int32_t code) {
  SRequestObj* pRequest = param;
  if (code != TSDB_CODE_SUCCESS) {
    setErrno(pRequest, code);
  } else {
    SShowVariablesRsp  rsp = {0};
    SRetrieveTableRsp* pRes = NULL;
    code = tDeserializeSShowVariablesRsp(pMsg->pData, pMsg->len, &rsp);
    if (TSDB_CODE_SUCCESS == code) {
      code = buildShowVariablesRsp(rsp.variables, &pRes);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = setQueryResultFromRsp(&pRequest->body.resInfo, pRes, false, true);
    }

    if (code != 0) {
      taosMemoryFree(pRes);
    }
    tFreeSShowVariablesRsp(&rsp);
  }

  taosMemoryFree(pMsg->pData);
  taosMemoryFree(pMsg->pEpSet);

  if (pRequest->body.queryFp != NULL) {
    doRequestCallback(pRequest, code);
  } else {
    tsem_post(&pRequest->body.rspSem);
  }
  return code;
}

static int32_t buildCompactDbBlock(SCompactDbRsp* pRsp, SSDataBlock** block) {
  SSDataBlock* pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  pBlock->info.hasVarCol = true;

  pBlock->pDataBlock = taosArrayInit(COMPACT_DB_RESULT_COLS, sizeof(SColumnInfoData));

  SColumnInfoData infoData = {0};
  infoData.info.type = TSDB_DATA_TYPE_VARCHAR;
  infoData.info.bytes = COMPACT_DB_RESULT_FIELD1_LEN;
  taosArrayPush(pBlock->pDataBlock, &infoData);

  infoData.info.type = TSDB_DATA_TYPE_INT;
  infoData.info.bytes = tDataTypes[TSDB_DATA_TYPE_INT].bytes;
  taosArrayPush(pBlock->pDataBlock, &infoData);

  infoData.info.type = TSDB_DATA_TYPE_VARCHAR;
  infoData.info.bytes = COMPACT_DB_RESULT_FIELD3_LEN;
  taosArrayPush(pBlock->pDataBlock, &infoData);

  blockDataEnsureCapacity(pBlock, 1);

  SColumnInfoData* pResultCol = taosArrayGet(pBlock->pDataBlock, 0);
  SColumnInfoData* pIdCol = taosArrayGet(pBlock->pDataBlock, 1);
  SColumnInfoData* pReasonCol = taosArrayGet(pBlock->pDataBlock, 2);
  char result[COMPACT_DB_RESULT_FIELD1_LEN] = {0};
  char reason[COMPACT_DB_RESULT_FIELD3_LEN] = {0};
  if (pRsp->bAccepted) {
    STR_TO_VARSTR(result, "accepted");
    colDataSetVal(pResultCol, 0, result, false);
    colDataSetVal(pIdCol, 0, (void*)&pRsp->compactId, false);
    STR_TO_VARSTR(reason, "success");
    colDataSetVal(pReasonCol, 0, reason, false);
  } else {
    STR_TO_VARSTR(result, "rejected");
    colDataSetVal(pResultCol, 0, result, false);
    colDataSetNULL(pIdCol, 0);
    STR_TO_VARSTR(reason, "compaction is ongoing");
    colDataSetVal(pReasonCol, 0, reason, false);
  }
  pBlock->info.rows = 1;

  *block = pBlock;

  return TSDB_CODE_SUCCESS;
}

static int32_t buildRetriveTableRspForCompactDb(SCompactDbRsp* pCompactDb, SRetrieveTableRsp** pRsp) {
  SSDataBlock* pBlock = NULL;
  int32_t      code = buildCompactDbBlock(pCompactDb, &pBlock);
  if (code) {
    return code;
  }

  size_t rspSize = sizeof(SRetrieveTableRsp) + blockGetEncodeSize(pBlock);
  *pRsp = taosMemoryCalloc(1, rspSize);
  if (NULL == *pRsp) {
    blockDataDestroy(pBlock);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  (*pRsp)->useconds = 0;
  (*pRsp)->completed = 1;
  (*pRsp)->precision = 0;
  (*pRsp)->compressed = 0;
  (*pRsp)->compLen = 0;
  (*pRsp)->numOfRows = htobe64((int64_t)pBlock->info.rows);
  (*pRsp)->numOfCols = htonl(COMPACT_DB_RESULT_COLS);

  int32_t len = blockEncode(pBlock, (*pRsp)->data, COMPACT_DB_RESULT_COLS);
  blockDataDestroy(pBlock);

  if (len != rspSize - sizeof(SRetrieveTableRsp)) {
    uError("buildRetriveTableRspForCompactDb error, len:%d != rspSize - sizeof(SRetrieveTableRsp):%" PRIu64, len,
           (uint64_t)(rspSize - sizeof(SRetrieveTableRsp)));
    return TSDB_CODE_TSC_INVALID_INPUT;
  }

  return TSDB_CODE_SUCCESS;
}


int32_t processCompactDbRsp(void* param, SDataBuf* pMsg, int32_t code) {
  SRequestObj* pRequest = param;
  if (code != TSDB_CODE_SUCCESS) {
    setErrno(pRequest, code);
  } else {
    SCompactDbRsp  rsp = {0};
    SRetrieveTableRsp* pRes = NULL;
    code = tDeserializeSCompactDbRsp(pMsg->pData, pMsg->len, &rsp);
    if (TSDB_CODE_SUCCESS == code) {
      code = buildRetriveTableRspForCompactDb(&rsp, &pRes);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = setQueryResultFromRsp(&pRequest->body.resInfo, pRes, false, true);
    }

    if (code != 0) {
      taosMemoryFree(pRes);
    }
  }

  taosMemoryFree(pMsg->pData);
  taosMemoryFree(pMsg->pEpSet);

  if (pRequest->body.queryFp != NULL) {
    pRequest->body.queryFp(((SSyncQueryParam *)pRequest->body.interParam)->userParam, pRequest, code);
  } else {
    tsem_post(&pRequest->body.rspSem);
  }
  return code;  
}

__async_send_cb_fn_t getMsgRspHandle(int32_t msgType) {
  switch (msgType) {
    case TDMT_MND_CONNECT:
      return processConnectRsp;
    case TDMT_MND_CREATE_DB:
      return processCreateDbRsp;
    case TDMT_MND_USE_DB:
      return processUseDbRsp;
    case TDMT_MND_CREATE_STB:
      return processCreateSTableRsp;
    case TDMT_MND_DROP_DB:
      return processDropDbRsp;
    case TDMT_MND_ALTER_STB:
      return processAlterStbRsp;
    case TDMT_MND_SHOW_VARIABLES:
      return processShowVariablesRsp;
    case TDMT_MND_COMPACT_DB:
      return processCompactDbRsp;  
    default:
      return genericRspCallback;
  }
}
