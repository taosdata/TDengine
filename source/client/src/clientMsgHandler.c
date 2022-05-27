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

int32_t (*handleRequestRspFp[TDMT_MAX])(void*, const SDataBuf* pMsg, int32_t code);

static void setErrno(SRequestObj* pRequest, int32_t code) {
  pRequest->code = code;
  terrno = code;
}

int32_t genericRspCallback(void* param, const SDataBuf* pMsg, int32_t code) {
  SRequestObj* pRequest = param;
  setErrno(pRequest, code);

  taosMemoryFree(pMsg->pData);
  tsem_post(&pRequest->body.rspSem);
  return code;
}

int32_t processConnectRsp(void* param, const SDataBuf* pMsg, int32_t code) {
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
    updateEpSet_s(&pTscObj->pAppInfo->mgmtEp, &connectRsp.epSet);
  }

  for (int32_t i = 0; i < connectRsp.epSet.numOfEps; ++i) {
    tscDebug("0x%" PRIx64 " epSet.fqdn[%d]:%s port:%d, connObj:0x%" PRIx64, pRequest->requestId, i,
             connectRsp.epSet.eps[i].fqdn, connectRsp.epSet.eps[i].port, pTscObj->id);
  }

  pTscObj->connId = connectRsp.connId;
  pTscObj->acctId = connectRsp.acctId;
  tstrncpy(pTscObj->ver, connectRsp.sVersion, tListLen(pTscObj->ver));

  // update the appInstInfo
  pTscObj->pAppInfo->clusterId = connectRsp.clusterId;
  atomic_add_fetch_64(&pTscObj->pAppInfo->numOfConns, 1);

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

  assert(pRequest != NULL);
  pMsgSendInfo->msgInfo = pRequest->body.requestMsg;

  pMsgSendInfo->fp = (handleRequestRspFp[TMSG_INDEX(pRequest->type)] == NULL)
                         ? genericRspCallback
                         : handleRequestRspFp[TMSG_INDEX(pRequest->type)];
  return pMsgSendInfo;
}

int32_t processCreateDbRsp(void* param, const SDataBuf* pMsg, int32_t code) {
  // todo rsp with the vnode id list
  SRequestObj* pRequest = param;
  taosMemoryFree(pMsg->pData);
  if (code != TSDB_CODE_SUCCESS) {
    setErrno(pRequest, code);
  }
  tsem_post(&pRequest->body.rspSem);
  return code;
}

int32_t processUseDbRsp(void* param, const SDataBuf* pMsg, int32_t code) {
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
    tsem_post(&pRequest->body.rspSem);
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
    taosMemoryFreeClear(output.dbVgroup);

    tscError("0x%" PRIx64 " failed to build use db output since %s", pRequest->requestId, terrstr());
  } else if (output.dbVgroup) {
    struct SCatalog* pCatalog = NULL;

    int32_t code1 = catalogGetHandle(pRequest->pTscObj->pAppInfo->clusterId, &pCatalog);
    if (code1 != TSDB_CODE_SUCCESS) {
      tscWarn("catalogGetHandle failed, clusterId:%" PRIx64 ", error:%s", pRequest->pTscObj->pAppInfo->clusterId,
              tstrerror(code1));
      taosMemoryFreeClear(output.dbVgroup);
    } else {
      catalogUpdateDBVgInfo(pCatalog, output.db, output.dbId, output.dbVgroup);
    }
  }

  tFreeSUsedbRsp(&usedbRsp);

  char db[TSDB_DB_NAME_LEN] = {0};
  tNameGetDbName(&name, db);

  setConnectionDB(pRequest->pTscObj, db);
  taosMemoryFree(pMsg->pData);
  tsem_post(&pRequest->body.rspSem);
  return 0;
}

int32_t processCreateTableRsp(void* param, const SDataBuf* pMsg, int32_t code) {
  assert(pMsg != NULL && param != NULL);
  SRequestObj* pRequest = param;

  taosMemoryFree(pMsg->pData);
  if (code != TSDB_CODE_SUCCESS) {
    setErrno(pRequest, code);
    tsem_post(&pRequest->body.rspSem);
    return code;
  }

  tsem_post(&pRequest->body.rspSem);
  return code;
}

int32_t processDropDbRsp(void* param, const SDataBuf* pMsg, int32_t code) {
  SRequestObj* pRequest = param;
  if (code != TSDB_CODE_SUCCESS) {
    setErrno(pRequest, code);
    tsem_post(&pRequest->body.rspSem);
    return code;
  }

  SDropDbRsp dropdbRsp = {0};
  tDeserializeSDropDbRsp(pMsg->pData, pMsg->len, &dropdbRsp);

  struct SCatalog* pCatalog = NULL;
  catalogGetHandle(pRequest->pTscObj->pAppInfo->clusterId, &pCatalog);
  catalogRemoveDB(pCatalog, dropdbRsp.db, dropdbRsp.uid);

  tsem_post(&pRequest->body.rspSem);
  return code;
}

void initMsgHandleFp() {
  handleRequestRspFp[TMSG_INDEX(TDMT_MND_CONNECT)] = processConnectRsp;
  handleRequestRspFp[TMSG_INDEX(TDMT_MND_CREATE_DB)] = processCreateDbRsp;
  handleRequestRspFp[TMSG_INDEX(TDMT_MND_USE_DB)] = processUseDbRsp;
  handleRequestRspFp[TMSG_INDEX(TDMT_MND_CREATE_STB)] = processCreateTableRsp;
  handleRequestRspFp[TMSG_INDEX(TDMT_MND_DROP_DB)] = processDropDbRsp;
}
