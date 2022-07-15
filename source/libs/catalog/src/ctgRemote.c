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

#include "trpc.h"
#include "query.h"
#include "tname.h"
#include "catalogInt.h"
#include "systable.h"
#include "ctgRemote.h"
#include "tref.h"

int32_t ctgProcessRspMsg(void* out, int32_t reqType, char* msg, int32_t msgSize, int32_t rspCode, char* target) {
  int32_t code = 0;
  
  switch (reqType) {
    case TDMT_MND_QNODE_LIST: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        qError("error rsp for qnode list, error:%s", tstrerror(rspCode));
        CTG_ERR_RET(rspCode);
      }
      
      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("Process qnode list rsp failed, error:%s", tstrerror(rspCode));
        CTG_ERR_RET(code);
      }
      
      qDebug("Got qnode list from mnode, listNum:%d", (int32_t)taosArrayGetSize(out));
      break;
    }
    case TDMT_MND_DNODE_LIST: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        qError("error rsp for dnode list, error:%s", tstrerror(rspCode));
        CTG_ERR_RET(rspCode);
      }
      
      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("Process dnode list rsp failed, error:%s", tstrerror(rspCode));
        CTG_ERR_RET(code);
      }
      
      qDebug("Got dnode list from mnode, listNum:%d", (int32_t)taosArrayGetSize(*(SArray**)out));
      break;
    }
    case TDMT_MND_USE_DB: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        qError("error rsp for use db, error:%s, dbFName:%s", tstrerror(rspCode), target);
        CTG_ERR_RET(rspCode);
      }
      
      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("Process use db rsp failed, error:%s, dbFName:%s", tstrerror(code), target);
        CTG_ERR_RET(code);
      }
      
      qDebug("Got db vgInfo from mnode, dbFName:%s", target);
      break;
    }
    case TDMT_MND_GET_DB_CFG: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        qError("error rsp for get db cfg, error:%s, db:%s", tstrerror(rspCode), target);
        CTG_ERR_RET(rspCode);
      }
      
      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("Process get db cfg rsp failed, error:%s, db:%s", tstrerror(code), target);
        CTG_ERR_RET(code);
      }
      
      qDebug("Got db cfg from mnode, dbFName:%s", target);
      break;
    }
    case TDMT_MND_GET_INDEX: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        qError("error rsp for get index, error:%s, indexName:%s", tstrerror(rspCode), target);
        CTG_ERR_RET(rspCode);
      }
      
      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("Process get index rsp failed, error:%s, indexName:%s", tstrerror(code), target);
        CTG_ERR_RET(code);
      }
      
      qDebug("Got index from mnode, indexName:%s", target);
      break;
    }
    case TDMT_MND_GET_TABLE_INDEX: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        qError("error rsp for get table index, error:%s, tbFName:%s", tstrerror(rspCode), target);
        CTG_ERR_RET(rspCode);
      }
      
      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("Process get table index rsp failed, error:%s, tbFName:%s", tstrerror(code), target);
        CTG_ERR_RET(code);
      }
      
      qDebug("Got table index from mnode, tbFName:%s", target);
      break;
    }
    case TDMT_MND_RETRIEVE_FUNC: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        qError("error rsp for get udf, error:%s, funcName:%s", tstrerror(rspCode), target);
        CTG_ERR_RET(rspCode);
      }
      
      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("Process get udf rsp failed, error:%s, funcName:%s", tstrerror(code), target);
        CTG_ERR_RET(code);
      }
      
      qDebug("Got udf from mnode, funcName:%s", target);
      break;
    }
    case TDMT_MND_GET_USER_AUTH: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        qError("error rsp for get user auth, error:%s, user:%s", tstrerror(rspCode), target);
        CTG_ERR_RET(rspCode);
      }
      
      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("Process get user auth rsp failed, error:%s, user:%s", tstrerror(code), target);
        CTG_ERR_RET(code);
      }
      
      qDebug("Got user auth from mnode, user:%s", target);
      break;
    }
    case TDMT_MND_TABLE_META: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        if (CTG_TABLE_NOT_EXIST(rspCode)) {
          SET_META_TYPE_NULL(((STableMetaOutput*)out)->metaType);
          qDebug("stablemeta not exist in mnode, tbFName:%s", target);
          return TSDB_CODE_SUCCESS;
        }
        
        qError("error rsp for stablemeta from mnode, error:%s, tbFName:%s", tstrerror(rspCode), target);
        CTG_ERR_RET(rspCode);
      }
      
      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("Process mnode stablemeta rsp failed, error:%s, tbFName:%s", tstrerror(code), target);
        CTG_ERR_RET(code);
      }
      
      qDebug("Got table meta from mnode, tbFName:%s", target);
      break;
    }
    case TDMT_VND_TABLE_META: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        if (CTG_TABLE_NOT_EXIST(rspCode)) {
          SET_META_TYPE_NULL(((STableMetaOutput*)out)->metaType);
          qDebug("tablemeta not exist in vnode, tbFName:%s", target);
          return TSDB_CODE_SUCCESS;
        }
      
        qError("error rsp for table meta from vnode, code:%s, tbFName:%s", tstrerror(rspCode), target);
        CTG_ERR_RET(rspCode);
      }
      
      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("Process vnode tablemeta rsp failed, code:%s, tbFName:%s", tstrerror(code), target);
        CTG_ERR_RET(code);
      }
      
      qDebug("Got table meta from vnode, tbFName:%s", target);
      break;
    }
    case TDMT_VND_TABLE_CFG: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        qError("error rsp for table cfg from vnode, code:%s, tbFName:%s", tstrerror(rspCode), target);
        CTG_ERR_RET(rspCode);
      }
      
      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("Process vnode tb cfg rsp failed, code:%s, tbFName:%s", tstrerror(code), target);
        CTG_ERR_RET(code);
      }
      
      qDebug("Got table cfg from vnode, tbFName:%s", target);
      break;
    }
    case TDMT_MND_TABLE_CFG: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        qError("error rsp for stb cfg from mnode, error:%s, tbFName:%s", tstrerror(rspCode), target);
        CTG_ERR_RET(rspCode);
      }
      
      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("Process mnode stb cfg rsp failed, error:%s, tbFName:%s", tstrerror(code), target);
        CTG_ERR_RET(code);
      }
      
      qDebug("Got stb cfg from mnode, tbFName:%s", target);
      break;
    }    
    case TDMT_MND_SERVER_VERSION: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        qError("error rsp for svr ver from mnode, error:%s", tstrerror(rspCode));
        CTG_ERR_RET(rspCode);
      }
      
      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("Process svr ver rsp failed, error:%s", tstrerror(code));
        CTG_ERR_RET(code);
      }
      
      qDebug("Got svr ver from mnode");
      break;
    }
    default:
      qError("invalid req type %s", TMSG_INFO(reqType));
      return TSDB_CODE_APP_ERROR;
  }

  return TSDB_CODE_SUCCESS;
}


int32_t ctgHandleMsgCallback(void *param, SDataBuf *pMsg, int32_t rspCode) {
  SCtgTaskCallbackParam* cbParam = (SCtgTaskCallbackParam*)param;
  int32_t code = 0;
  
  CTG_API_ENTER();

  SCtgJob* pJob = taosAcquireRef(gCtgMgmt.jobPool, cbParam->refId);
  if (NULL == pJob) {
    qDebug("ctg job refId 0x%" PRIx64 " already dropped", cbParam->refId);
    goto _return;
  }

  SCtgTask *pTask = taosArrayGet(pJob->pTasks, cbParam->taskId);

  qDebug("QID:0x%" PRIx64 " ctg task %d start to handle rsp %s", pJob->queryId, pTask->taskId, TMSG_INFO(cbParam->reqType + 1));
  
  CTG_ERR_JRET((*gCtgAsyncFps[pTask->type].handleRspFp)(pTask, cbParam->reqType, pMsg, rspCode));
 
_return:

  taosMemoryFree(pMsg->pData);

  if (pJob) {
    taosReleaseRef(gCtgMgmt.jobPool, cbParam->refId);
  }
  
  taosMemoryFree(param);

  CTG_API_LEAVE(code);
}


int32_t ctgMakeMsgSendInfo(SCtgTask* pTask, int32_t msgType, SMsgSendInfo **pMsgSendInfo) {
  int32_t       code = 0;
  SMsgSendInfo *msgSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (NULL == msgSendInfo) {
    qError("calloc %d failed", (int32_t)sizeof(SMsgSendInfo));
    CTG_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  SCtgTaskCallbackParam *param = taosMemoryCalloc(1, sizeof(SCtgTaskCallbackParam));
  if (NULL == param) {
    qError("calloc %d failed", (int32_t)sizeof(SCtgTaskCallbackParam));
    CTG_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  param->reqType = msgType;
  param->queryId = pTask->pJob->queryId;
  param->refId = pTask->pJob->refId;
  param->taskId = pTask->taskId;

  msgSendInfo->param = param;
  msgSendInfo->fp = ctgHandleMsgCallback;

  *pMsgSendInfo = msgSendInfo;

  return TSDB_CODE_SUCCESS;

_return:

  taosMemoryFree(param);
  taosMemoryFree(msgSendInfo);

  CTG_RET(code);
}

int32_t ctgAsyncSendMsg(SCatalog* pCtg, SRequestConnInfo *pConn, SCtgTask* pTask, int32_t msgType, void *msg, uint32_t msgSize) {
  int32_t code = 0;
  SMsgSendInfo *pMsgSendInfo = NULL;
  CTG_ERR_JRET(ctgMakeMsgSendInfo(pTask, msgType, &pMsgSendInfo));

  ctgUpdateSendTargetInfo(pMsgSendInfo, msgType, pTask);

  pMsgSendInfo->requestId = pConn->requestId;
  pMsgSendInfo->requestObjRefId = pConn->requestObjRefId;
  pMsgSendInfo->msgInfo.pData = msg;
  pMsgSendInfo->msgInfo.len = msgSize;
  pMsgSendInfo->msgInfo.handle = NULL;
  pMsgSendInfo->msgType = msgType;

  int64_t transporterId = 0;
  code = asyncSendMsgToServer(pConn->pTrans, &pConn->mgmtEps, &transporterId, pMsgSendInfo);
  if (code) {
    ctgError("asyncSendMsgToSever failed, error: %s", tstrerror(code));
    CTG_ERR_JRET(code);
  }

  ctgDebug("ctg req msg sent, reqId:0x%" PRIx64 ", msg type:%d, %s", pTask->pJob->queryId, msgType, TMSG_INFO(msgType));
  return TSDB_CODE_SUCCESS;

_return:

  if (pMsgSendInfo) {
    taosMemoryFreeClear(pMsgSendInfo->param);
    taosMemoryFreeClear(pMsgSendInfo);
  }

  CTG_RET(code);
}

int32_t ctgGetQnodeListFromMnode(SCatalog* pCtg, SRequestConnInfo *pConn, SArray *out, SCtgTask* pTask) {
  char *msg = NULL;
  int32_t msgLen = 0;
  int32_t reqType = TDMT_MND_QNODE_LIST;
  void*(*mallocFp)(int32_t) = pTask ? taosMemoryMalloc : rpcMallocCont;

  ctgDebug("try to get qnode list from mnode, mgmtEpInUse:%d", pConn->mgmtEps.inUse);

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)](NULL, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("Build qnode list msg failed, error:%s", tstrerror(code));
    CTG_ERR_RET(code);
  }

  if (pTask) {
    void* pOut = taosArrayInit(4, sizeof(SQueryNodeLoad));
    if (NULL == pOut) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    CTG_ERR_RET(ctgUpdateMsgCtx(&pTask->msgCtx, reqType, pOut, NULL));
    CTG_RET(ctgAsyncSendMsg(pCtg, pConn, pTask, reqType, msg, msgLen));
  }
  
  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont   = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp);

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, NULL));

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetDnodeListFromMnode(SCatalog* pCtg, SRequestConnInfo *pConn, SArray **out, SCtgTask* pTask) {
  char *msg = NULL;
  int32_t msgLen = 0;
  int32_t reqType = TDMT_MND_DNODE_LIST;
  void*(*mallocFp)(int32_t) = pTask ? taosMemoryMalloc : rpcMallocCont;

  ctgDebug("try to get dnode list from mnode, mgmtEpInUse:%d", pConn->mgmtEps.inUse);

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)](NULL, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("Build dnode list msg failed, error:%s", tstrerror(code));
    CTG_ERR_RET(code);
  }

  if (pTask) {
    CTG_ERR_RET(ctgUpdateMsgCtx(&pTask->msgCtx, reqType, NULL, NULL));
    CTG_RET(ctgAsyncSendMsg(pCtg, pConn, pTask, reqType, msg, msgLen));
  }
  
  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont   = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp);

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, NULL));

  return TSDB_CODE_SUCCESS;
}


int32_t ctgGetDBVgInfoFromMnode(SCatalog* pCtg, SRequestConnInfo *pConn, SBuildUseDBInput *input, SUseDbOutput *out, SCtgTask* pTask) {
  char *msg = NULL;
  int32_t msgLen = 0;
  int32_t reqType = TDMT_MND_USE_DB;
  void*(*mallocFp)(int32_t) = pTask ? taosMemoryMalloc : rpcMallocCont;

  ctgDebug("try to get db vgInfo from mnode, dbFName:%s", input->db);

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)](input, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("Build use db msg failed, code:%x, db:%s", code, input->db);
    CTG_ERR_RET(code);
  }

  if (pTask) {
    void* pOut = taosMemoryCalloc(1, sizeof(SUseDbOutput));
    if (NULL == pOut) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    CTG_ERR_RET(ctgUpdateMsgCtx(&pTask->msgCtx, reqType, pOut, input->db));
    
    CTG_RET(ctgAsyncSendMsg(pCtg, pConn, pTask, reqType, msg, msgLen));
  }
  
  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont   = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp);

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, input->db));
  
  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetDBCfgFromMnode(SCatalog* pCtg, SRequestConnInfo *pConn, const char *dbFName, SDbCfgInfo *out, SCtgTask* pTask) {
  char *msg = NULL;
  int32_t msgLen = 0;
  int32_t reqType = TDMT_MND_GET_DB_CFG;
  void*(*mallocFp)(int32_t) = pTask ? taosMemoryMalloc : rpcMallocCont;

  ctgDebug("try to get db cfg from mnode, dbFName:%s", dbFName);

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)]((void *)dbFName, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("Build get db cfg msg failed, code:%x, db:%s", code, dbFName);
    CTG_ERR_RET(code);
  }

  if (pTask) {
    void* pOut = taosMemoryCalloc(1, sizeof(SDbCfgInfo));
    if (NULL == pOut) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    CTG_ERR_RET(ctgUpdateMsgCtx(&pTask->msgCtx, reqType, pOut, (char*)dbFName));
    
    CTG_RET(ctgAsyncSendMsg(pCtg, pConn, pTask, reqType, msg, msgLen));
  }
  
  SRpcMsg rpcMsg = {
      .msgType = TDMT_MND_GET_DB_CFG,
      .pCont   = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp);

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, (char*)dbFName));

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetIndexInfoFromMnode(SCatalog* pCtg, SRequestConnInfo *pConn, const char *indexName, SIndexInfo *out, SCtgTask* pTask) {
  char *msg = NULL;
  int32_t msgLen = 0;
  int32_t reqType = TDMT_MND_GET_INDEX;
  void*(*mallocFp)(int32_t) = pTask ? taosMemoryMalloc : rpcMallocCont;

  ctgDebug("try to get index from mnode, indexName:%s", indexName);

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)]((void *)indexName, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("Build get index msg failed, code:%x, db:%s", code, indexName);
    CTG_ERR_RET(code);
  }

  if (pTask) {
    void* pOut = taosMemoryCalloc(1, sizeof(SIndexInfo));
    if (NULL == pOut) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    CTG_ERR_RET(ctgUpdateMsgCtx(&pTask->msgCtx, reqType, pOut, (char*)indexName));
    
    CTG_RET(ctgAsyncSendMsg(pCtg, pConn, pTask, reqType, msg, msgLen));
  }
  
  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont   = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp);

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, (char*)indexName));
  
  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetTbIndexFromMnode(SCatalog* pCtg, SRequestConnInfo *pConn, SName *name, STableIndex* out, SCtgTask* pTask) {
  char *msg = NULL;
  int32_t msgLen = 0;
  int32_t reqType = TDMT_MND_GET_TABLE_INDEX;
  void*(*mallocFp)(int32_t) = pTask ? taosMemoryMalloc : rpcMallocCont;
  char tbFName[TSDB_TABLE_FNAME_LEN];
  tNameExtractFullName(name, tbFName);

  ctgDebug("try to get tb index from mnode, tbFName:%s", tbFName);

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)]((void *)tbFName, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("Build get index msg failed, code:%s, tbFName:%s", tstrerror(code), tbFName);
    CTG_ERR_RET(code);
  }

  if (pTask) {
    void* pOut = taosMemoryCalloc(1, sizeof(STableIndex));
    if (NULL == pOut) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    
    CTG_ERR_RET(ctgUpdateMsgCtx(&pTask->msgCtx, reqType, pOut, (char*)tbFName));
    
    CTG_RET(ctgAsyncSendMsg(pCtg, pConn, pTask, reqType, msg, msgLen));
  }
  
  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont   = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp);

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, (char*)tbFName));
  
  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetUdfInfoFromMnode(SCatalog* pCtg, SRequestConnInfo *pConn, const char *funcName, SFuncInfo *out, SCtgTask* pTask) {
  char *msg = NULL;
  int32_t msgLen = 0;
  int32_t reqType = TDMT_MND_RETRIEVE_FUNC;
  void*(*mallocFp)(int32_t) = pTask ? taosMemoryMalloc : rpcMallocCont;

  ctgDebug("try to get udf info from mnode, funcName:%s", funcName);

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)]((void *)funcName, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("Build get udf msg failed, code:%x, db:%s", code, funcName);
    CTG_ERR_RET(code);
  }

  if (pTask) {
    void* pOut = taosMemoryCalloc(1, sizeof(SFuncInfo));
    if (NULL == pOut) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    CTG_ERR_RET(ctgUpdateMsgCtx(&pTask->msgCtx, reqType, pOut, (char*)funcName));
    
    CTG_RET(ctgAsyncSendMsg(pCtg, pConn, pTask, reqType, msg, msgLen));
  }
  
  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont   = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp);

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, (char*)funcName));

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetUserDbAuthFromMnode(SCatalog* pCtg, SRequestConnInfo *pConn, const char *user, SGetUserAuthRsp *out, SCtgTask* pTask) {
  char *msg = NULL;
  int32_t msgLen = 0;
  int32_t reqType = TDMT_MND_GET_USER_AUTH;
  void*(*mallocFp)(int32_t) = pTask ? taosMemoryMalloc : rpcMallocCont;

  ctgDebug("try to get user auth from mnode, user:%s", user);

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)]((void *)user, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("Build get user auth msg failed, code:%x, db:%s", code, user);
    CTG_ERR_RET(code);
  }

  if (pTask) {
    void* pOut = taosMemoryCalloc(1, sizeof(SGetUserAuthRsp));
    if (NULL == pOut) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    CTG_ERR_RET(ctgUpdateMsgCtx(&pTask->msgCtx, reqType, pOut, (char*)user));
    
    CTG_RET(ctgAsyncSendMsg(pCtg, pConn, pTask, reqType, msg, msgLen));
  }
  
  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont   = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp);

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, (char*)user));
  
  return TSDB_CODE_SUCCESS;
}


int32_t ctgGetTbMetaFromMnodeImpl(SCatalog* pCtg, SRequestConnInfo *pConn, char *dbFName, char* tbName, STableMetaOutput* out, SCtgTask* pTask) {
  SBuildTableInput bInput = {.vgId = 0, .dbFName = dbFName, .tbName = tbName};
  char *msg = NULL;
  SEpSet *pVnodeEpSet = NULL;
  int32_t msgLen = 0;
  int32_t reqType = TDMT_MND_TABLE_META;
  char tbFName[TSDB_TABLE_FNAME_LEN];
  sprintf(tbFName, "%s.%s", dbFName, tbName);
  void*(*mallocFp)(int32_t) = pTask ? taosMemoryMalloc : rpcMallocCont;

  ctgDebug("try to get table meta from mnode, tbFName:%s", tbFName);

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)](&bInput, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("Build mnode stablemeta msg failed, code:%x", code);
    CTG_ERR_RET(code);
  }

  if (pTask) {
    void* pOut = taosMemoryCalloc(1, sizeof(STableMetaOutput));
    if (NULL == pOut) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    CTG_ERR_RET(ctgUpdateMsgCtx(&pTask->msgCtx, reqType, pOut, tbFName));
    
    CTG_RET(ctgAsyncSendMsg(pCtg, pConn, pTask, reqType, msg, msgLen));
  }

  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont   = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp);
  
  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, tbFName));

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetTbMetaFromMnode(SCatalog* pCtg, SRequestConnInfo *pConn, const SName* pTableName, STableMetaOutput* out, SCtgTask* pTask) {
  char dbFName[TSDB_DB_FNAME_LEN];
  tNameGetFullDbName(pTableName, dbFName);

  return ctgGetTbMetaFromMnodeImpl(pCtg, pConn, dbFName, (char *)pTableName->tname, out, pTask);
}

int32_t ctgGetTbMetaFromVnode(SCatalog* pCtg, SRequestConnInfo *pConn, const SName* pTableName, SVgroupInfo *vgroupInfo, STableMetaOutput* out, SCtgTask* pTask) {
  char dbFName[TSDB_DB_FNAME_LEN];
  tNameGetFullDbName(pTableName, dbFName);
  int32_t reqType = TDMT_VND_TABLE_META;
  char tbFName[TSDB_TABLE_FNAME_LEN];
  sprintf(tbFName, "%s.%s", dbFName, pTableName->tname);
  void*(*mallocFp)(int32_t) = pTask ? taosMemoryMalloc : rpcMallocCont;

  SEp* pEp = &vgroupInfo->epSet.eps[vgroupInfo->epSet.inUse];
  ctgDebug("try to get table meta from vnode, vgId:%d, ep num:%d, ep %s:%d, tbFName:%s", 
           vgroupInfo->vgId, vgroupInfo->epSet.numOfEps, pEp->fqdn, pEp->port, tbFName);

  SBuildTableInput bInput = {.vgId = vgroupInfo->vgId, .dbFName = dbFName, .tbName = (char *)tNameGetTableName(pTableName)};
  char *msg = NULL;
  int32_t msgLen = 0;

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)](&bInput, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("Build vnode tablemeta msg failed, code:%x, tbFName:%s", code, tbFName);
    CTG_ERR_RET(code);
  }

  if (pTask) {
    void* pOut = taosMemoryCalloc(1, sizeof(STableMetaOutput));
    if (NULL == pOut) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    CTG_ERR_RET(ctgUpdateMsgCtx(&pTask->msgCtx, reqType, pOut, tbFName));

    SRequestConnInfo vConn = {.pTrans = pConn->pTrans, 
                             .requestId = pConn->requestId,
                             .requestObjRefId = pConn->requestObjRefId,
                             .mgmtEps = vgroupInfo->epSet};
    CTG_RET(ctgAsyncSendMsg(pCtg, &vConn, pTask, reqType, msg, msgLen));
  }

  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont   = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  rpcSendRecv(pConn->pTrans, &vgroupInfo->epSet, &rpcMsg, &rpcRsp);

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, tbFName));  

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetTableCfgFromVnode(SCatalog* pCtg, SRequestConnInfo *pConn, const SName* pTableName, SVgroupInfo *vgroupInfo, STableCfg **out, SCtgTask* pTask) {
  char *msg = NULL;
  int32_t msgLen = 0;
  int32_t reqType = TDMT_VND_TABLE_CFG;
  char tbFName[TSDB_TABLE_FNAME_LEN];
  tNameExtractFullName(pTableName, tbFName);
  void*(*mallocFp)(int32_t) = pTask ? taosMemoryMalloc : rpcMallocCont;
  char dbFName[TSDB_DB_FNAME_LEN];
  tNameGetFullDbName(pTableName, dbFName);
  SBuildTableInput bInput = {.vgId = vgroupInfo->vgId, .dbFName = dbFName, .tbName = (char*)pTableName->tname};

  SEp* pEp = &vgroupInfo->epSet.eps[vgroupInfo->epSet.inUse];
  ctgDebug("try to get table cfg from vnode, vgId:%d, ep num:%d, ep %s:%d, tbFName:%s", 
           vgroupInfo->vgId, vgroupInfo->epSet.numOfEps, pEp->fqdn, pEp->port, tbFName);

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)](&bInput, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("Build get tb cfg msg failed, code:%s, tbFName:%s", tstrerror(code), tbFName);
    CTG_ERR_RET(code);
  }

  if (pTask) {
    CTG_ERR_RET(ctgUpdateMsgCtx(&pTask->msgCtx, reqType, NULL, (char*)tbFName));

    SRequestConnInfo vConn = {.pTrans = pConn->pTrans, 
                             .requestId = pConn->requestId,
                             .requestObjRefId = pConn->requestObjRefId,
                             .mgmtEps = vgroupInfo->epSet};    
    CTG_RET(ctgAsyncSendMsg(pCtg, &vConn, pTask, reqType, msg, msgLen));
  }
  
  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont   = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  rpcSendRecv(pConn->pTrans, &vgroupInfo->epSet, &rpcMsg, &rpcRsp);

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, (char*)tbFName));
  
  return TSDB_CODE_SUCCESS;
}


int32_t ctgGetTableCfgFromMnode(SCatalog* pCtg, SRequestConnInfo *pConn, const SName* pTableName, STableCfg **out, SCtgTask* pTask) {
  char *msg = NULL;
  int32_t msgLen = 0;
  int32_t reqType = TDMT_MND_TABLE_CFG;
  char tbFName[TSDB_TABLE_FNAME_LEN];
  tNameExtractFullName(pTableName, tbFName);
  void*(*mallocFp)(int32_t) = pTask ? taosMemoryMalloc : rpcMallocCont;
  char dbFName[TSDB_DB_FNAME_LEN];
  tNameGetFullDbName(pTableName, dbFName);
  SBuildTableInput bInput = {.vgId = 0, .dbFName = dbFName, .tbName = (char*)pTableName->tname};

  ctgDebug("try to get table cfg from mnode, tbFName:%s", tbFName);

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)](&bInput, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("Build get tb cfg msg failed, code:%s, tbFName:%s", tstrerror(code), tbFName);
    CTG_ERR_RET(code);
  }

  if (pTask) {
    CTG_ERR_RET(ctgUpdateMsgCtx(&pTask->msgCtx, reqType, NULL, (char*)tbFName));
    
    CTG_RET(ctgAsyncSendMsg(pCtg, pConn, pTask, reqType, msg, msgLen));
  }
  
  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont   = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp);

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, (char*)tbFName));
  
  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetSvrVerFromMnode(SCatalog* pCtg, SRequestConnInfo *pConn, char **out, SCtgTask* pTask) {
  char *msg = NULL;
  int32_t msgLen = 0;
  int32_t reqType = TDMT_MND_SERVER_VERSION;
  void*(*mallocFp)(int32_t) = pTask ? taosMemoryMalloc : rpcMallocCont;

  qDebug("try to get svr ver from mnode");

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)](NULL, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("Build get svr ver msg failed, code:%s", tstrerror(code));
    CTG_ERR_RET(code);
  }

  if (pTask) {
    CTG_ERR_RET(ctgUpdateMsgCtx(&pTask->msgCtx, reqType, NULL, NULL));
    
    CTG_RET(ctgAsyncSendMsg(pCtg, pConn, pTask, reqType, msg, msgLen));
  }
  
  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont   = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp);

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, NULL));
  
  return TSDB_CODE_SUCCESS;
}


