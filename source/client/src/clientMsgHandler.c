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
#include "os.h"
#include "query.h"
#include "tdef.h"
#include "tname.h"

static void setErrno(SRequestObj* pRequest, int32_t code) {
  pRequest->code = code;
  terrno = code;
}

int32_t genericRspCallback(void* param, SDataBuf* pMsg, int32_t code) {
  SRequestObj* pRequest = param;
  setErrno(pRequest, code);

  if (NEED_CLIENT_RM_TBLMETA_REQ(pRequest->type)) {
    removeMeta(pRequest->pTscObj, pRequest->targetTableList);
  }

  taosMemoryFree(pMsg->pData);
  if (pRequest->body.queryFp != NULL) {
    pRequest->body.queryFp(pRequest->body.param, pRequest, code);
  } else {
    tsem_post(&pRequest->body.rspSem);
  }
  return code;
}

int32_t processConnectRsp(void* param, SDataBuf* pMsg, int32_t code) {
  SRequestObj* pRequest = param;
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pMsg->pData);
    setErrno(pRequest, code);
    tsem_post(&pRequest->body.rspSem);
    return code;
  }

  STscObj* pTscObj = pRequest->pTscObj;

  SConnectRsp connectRsp = {0};
  tDeserializeSConnectRsp(pMsg->pData, pMsg->len, &connectRsp);

  int32_t now = taosGetTimestampSec();
  int32_t delta = abs(now - connectRsp.svrTimestamp);
  if (delta > timestampDeltaLimit) {
    code = TSDB_CODE_TIME_UNSYNCED;
    tscError("time diff:%ds is too big", delta);
    taosMemoryFree(pMsg->pData);
    setErrno(pRequest, code);
    tsem_post(&pRequest->body.rspSem);
    return code;
  }

  /*assert(connectRsp.epSet.numOfEps > 0);*/
  if (connectRsp.epSet.numOfEps == 0) {
    taosMemoryFree(pMsg->pData);
    setErrno(pRequest, TSDB_CODE_MND_APP_ERROR);
    tsem_post(&pRequest->body.rspSem);
    return code;
  }

  if (connectRsp.dnodeNum == 1) {
    SEpSet srcEpSet = getEpSet_s(&pTscObj->pAppInfo->mgmtEp);
    SEpSet dstEpSet = connectRsp.epSet;
    rpcSetDefaultAddr(pTscObj->pAppInfo->pTransporter, srcEpSet.eps[srcEpSet.inUse].fqdn,
                      dstEpSet.eps[dstEpSet.inUse].fqdn);
  } else if (connectRsp.dnodeNum > 1 && !isEpsetEqual(&pTscObj->pAppInfo->mgmtEp.epSet, &connectRsp.epSet)) {
    SEpSet* pOrig = &pTscObj->pAppInfo->mgmtEp.epSet;
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

  pTscObj->connType = connectRsp.connType;

  hbRegisterConn(pTscObj->pAppInfo->pAppHbMgr, pTscObj->id, connectRsp.clusterId, connectRsp.connType);

  //  pRequest->body.resInfo.pRspMsg = pMsg->pData;
  tscDebug("0x%" PRIx64 " clusterId:%" PRId64 ", totalConn:%" PRId64, pRequest->requestId, connectRsp.clusterId,
           pTscObj->pAppInfo->numOfConns);

  taosMemoryFree(pMsg->pData);
  tsem_post(&pRequest->body.rspSem);
  return 0;
}

SMsgSendInfo* buildMsgInfoImpl(SRequestObj* pRequest) {
  SMsgSendInfo* pMsgSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));

  pMsgSendInfo->requestObjRefId = pRequest->self;
  pMsgSendInfo->requestId = pRequest->requestId;
  pMsgSendInfo->param = pRequest;
  pMsgSendInfo->msgType = pRequest->type;
  pMsgSendInfo->target.type = TARGET_TYPE_MNODE;

  assert(pRequest != NULL);
  pMsgSendInfo->msgInfo = pRequest->body.requestMsg;
  pMsgSendInfo->fp = getMsgRspHandle(pRequest->type);
  return pMsgSendInfo;
}

int32_t processCreateDbRsp(void* param, SDataBuf* pMsg, int32_t code) {
  // todo rsp with the vnode id list
  SRequestObj* pRequest = param;
  taosMemoryFree(pMsg->pData);
  if (code != TSDB_CODE_SUCCESS) {
    setErrno(pRequest, code);
  }

  if (pRequest->body.queryFp) {
    pRequest->body.queryFp(pRequest->body.param, pRequest, code);
  } else {
    tsem_post(&pRequest->body.rspSem);
  }
  return code;
}

int32_t processUseDbRsp(void* param, SDataBuf* pMsg, int32_t code) {
  SRequestObj* pRequest = param;

  if (TSDB_CODE_MND_DB_NOT_EXIST == code) {
    SUseDbRsp usedbRsp = {0};
    tDeserializeSUseDbRsp(pMsg->pData, pMsg->len, &usedbRsp);
    struct SCatalog* pCatalog = NULL;

    if (usedbRsp.vgVersion >= 0) {
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
    setErrno(pRequest, code);

    if (pRequest->body.queryFp != NULL) {
      pRequest->body.queryFp(pRequest->body.param, pRequest, pRequest->code);
    } else {
      tsem_post(&pRequest->body.rspSem);
    }

    return code;
  }

  SUseDbRsp usedbRsp = {0};
  tDeserializeSUseDbRsp(pMsg->pData, pMsg->len, &usedbRsp);

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

  if (pRequest->body.queryFp != NULL) {
    pRequest->body.queryFp(pRequest->body.param, pRequest, pRequest->code);
  } else {
    tsem_post(&pRequest->body.rspSem);
  }
  return 0;
}

int32_t processCreateSTableRsp(void* param, SDataBuf* pMsg, int32_t code) {
  assert(pMsg != NULL && param != NULL);
  SRequestObj* pRequest = param;

  if (code != TSDB_CODE_SUCCESS) {
    setErrno(pRequest, code);
  } else {
    SMCreateStbRsp createRsp = {0};
    SDecoder      coder = {0};
    tDecoderInit(&coder, pMsg->pData, pMsg->len);
    tDecodeSMCreateStbRsp(&coder, &createRsp);
    tDecoderClear(&coder);

    pRequest->body.resInfo.execRes.msgType = TDMT_MND_CREATE_STB;
    pRequest->body.resInfo.execRes.res = createRsp.pMeta;
  }

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
    
    pRequest->body.queryFp(pRequest->body.param, pRequest, code);
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
    catalogGetHandle(pRequest->pTscObj->pAppInfo->clusterId, &pCatalog);
    catalogRemoveDB(pCatalog, dropdbRsp.db, dropdbRsp.uid);
  }

  taosMemoryFree(pMsg->pData);

  if (pRequest->body.queryFp != NULL) {
    pRequest->body.queryFp(pRequest->body.param, pRequest, code);
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

    pRequest->body.queryFp(pRequest->body.param, pRequest, code);
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

  int32_t numOfCfg = taosArrayGetSize(pVars);
  blockDataEnsureCapacity(pBlock, numOfCfg);

  for (int32_t i = 0, c = 0; i < numOfCfg; ++i, c = 0) {
    SVariablesInfo* pInfo = taosArrayGet(pVars, i);

    char name[TSDB_CONFIG_OPTION_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(name, pInfo->name, TSDB_CONFIG_OPTION_LEN + VARSTR_HEADER_SIZE);
    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, c++);
    colDataAppend(pColInfo, i, name, false);

    char value[TSDB_CONFIG_VALUE_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(value, pInfo->value, TSDB_CONFIG_VALUE_LEN + VARSTR_HEADER_SIZE);
    pColInfo = taosArrayGet(pBlock->pDataBlock, c++);
    colDataAppend(pColInfo, i, value, false);
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
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  (*pRsp)->useconds = 0;
  (*pRsp)->completed = 1;
  (*pRsp)->precision = 0;
  (*pRsp)->compressed = 0;
  (*pRsp)->compLen = 0;
  (*pRsp)->numOfRows = htonl(pBlock->info.rows);
  (*pRsp)->numOfCols = htonl(SHOW_VARIABLES_RESULT_COLS);

  int32_t len = 0;
  blockEncode(pBlock, (*pRsp)->data, &len, SHOW_VARIABLES_RESULT_COLS, false);
  ASSERT(len == rspSize - sizeof(SRetrieveTableRsp));

  blockDataDestroy(pBlock);
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

    tFreeSShowVariablesRsp(&rsp);
  }

  taosMemoryFree(pMsg->pData);

  if (pRequest->body.queryFp != NULL) {
    pRequest->body.queryFp(pRequest->body.param, pRequest, code);
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
    default:
      return genericRspCallback;
  }
}
