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

#define _DEFAULT_SOURCE
#include "dmMgmt.h"
#include "qworker.h"
#include "tanalytics.h"
#include "tversion.h"

#define IS_STREAM_TRIGGER_RSP_MSG(_msg) (TDMT_STREAM_TRIGGER_CALC_RSP == (_msg) || TDMT_STREAM_TRIGGER_PULL_RSP == (_msg))

static inline void dmSendRsp(SRpcMsg *pMsg) {
  if (rpcSendResponse(pMsg) != 0) {
    dError("failed to send response, msg:%p", pMsg);
  }
}

static inline void dmBuildMnodeRedirectRsp(SDnode *pDnode, SRpcMsg *pMsg) {
  SEpSet epSet = {0};
  dmGetMnodeEpSetForRedirect(&pDnode->data, pMsg, &epSet);

  if (epSet.numOfEps <= 1) {
    if (epSet.numOfEps == 0) {
      pMsg->pCont = NULL;
      pMsg->code = TSDB_CODE_MNODE_NOT_FOUND;
      return;
    }
    // dnode is not the mnode or mnode leader  and This ensures that the function correctly handles cases where the
    // dnode cannot obtain a valid epSet and avoids returning an incorrect or misleading epSet.
    if (strcmp(epSet.eps[0].fqdn, tsLocalFqdn) == 0 && epSet.eps[0].port == tsServerPort) {
      pMsg->pCont = NULL;
      pMsg->code = TSDB_CODE_MNODE_NOT_FOUND;
      return;
    }
  }

  int32_t contLen = tSerializeSEpSet(NULL, 0, &epSet);
  pMsg->pCont = rpcMallocCont(contLen);
  if (pMsg->pCont == NULL) {
    pMsg->code = TSDB_CODE_OUT_OF_MEMORY;
  } else {
    contLen = tSerializeSEpSet(pMsg->pCont, contLen, &epSet);
    if (contLen < 0) {
      pMsg->code = contLen;
      return;
    }
    pMsg->contLen = contLen;
  }
}

int32_t dmProcessNodeMsg(SMgmtWrapper *pWrapper, SRpcMsg *pMsg) {
  const STraceId *trace = &pMsg->info.traceId;

  NodeMsgFp msgFp = pWrapper->msgFps[TMSG_INDEX(pMsg->msgType)];
  if (msgFp == NULL) {
    // terrno = TSDB_CODE_MSG_NOT_PROCESSED;
    dGError("msg:%p, not processed since no handler, type:%s", pMsg, TMSG_INFO(pMsg->msgType));
    return TSDB_CODE_MSG_NOT_PROCESSED;
  }

  dGTrace("msg:%p, will be processed by %s", pMsg, pWrapper->name);
  pMsg->info.wrapper = pWrapper;
  return (*msgFp)(pWrapper->pMgmt, pMsg);
}

static bool dmFailFastFp(tmsg_t msgType) {
  // add more msg type later
  return msgType == TDMT_SYNC_HEARTBEAT || msgType == TDMT_SYNC_APPEND_ENTRIES;
}

static int32_t dmConvertErrCode(tmsg_t msgType, int32_t code) {
  if (code != TSDB_CODE_APP_IS_STOPPING) {
    return code;
  }
  if ((msgType > TDMT_VND_MSG_MIN && msgType < TDMT_VND_MSG_MAX) ||
      (msgType > TDMT_SCH_MSG_MIN && msgType < TDMT_SCH_MSG_MAX)) {
    code = TSDB_CODE_VND_STOPPED;
  }
  return code;
}
static void dmUpdateRpcIpWhite(SDnodeData *pData, void *pTrans, SRpcMsg *pRpc) {
  int32_t        code = 0;
  SUpdateIpWhite ipWhite = {0};  // aosMemoryCalloc(1, sizeof(SUpdateIpWhite));
  code = tDeserializeSUpdateIpWhiteDual(pRpc->pCont, pRpc->contLen, &ipWhite);
  if (code < 0) {
    dError("failed to update rpc ip-white since: %s", tstrerror(code));
    return;
  }
  code = rpcSetIpWhite(pTrans, &ipWhite);
  pData->ipWhiteVer = ipWhite.ver;

  (void)tFreeSUpdateIpWhiteDualReq(&ipWhite);

  rpcFreeCont(pRpc->pCont);
}

static void dmUpdateRpcIpWhiteUnused(SDnodeData *pDnode, void *pTrans, SRpcMsg *pRpc) {
  int32_t code = TSDB_CODE_INVALID_MSG;
  dError("failed to update rpc ip-white since: %s", tstrerror(code));
  rpcFreeCont(pRpc->pCont);
  pRpc->pCont = NULL;
  return;
}
static bool dmIsForbiddenIp(int8_t forbidden, char *user, SIpAddr *clientIp) {
  if (forbidden) {
    dError("User:%s host:%s not in ip white list", user, IP_ADDR_STR(clientIp));
    return true;
  } else {
    return false;
  }
}

static void dmUpdateAnalyticFunc(SDnodeData *pData, void *pTrans, SRpcMsg *pRpc) {
  SRetrieveAnalyticAlgoRsp rsp = {0};
  if (tDeserializeRetrieveAnalyticAlgoRsp(pRpc->pCont, pRpc->contLen, &rsp) == 0) {
    taosAnalyUpdate(rsp.ver, rsp.hash);
    rsp.hash = NULL;
  }
  tFreeRetrieveAnalyticAlgoRsp(&rsp);
  rpcFreeCont(pRpc->pCont);
}

static void dmProcessRpcMsg(SDnode *pDnode, SRpcMsg *pRpc, SEpSet *pEpSet) {
  SDnodeTrans  *pTrans = &pDnode->trans;
  int32_t       code = -1;
  SRpcMsg      *pMsg = NULL;
  SMgmtWrapper *pWrapper = NULL;
  SDnodeHandle *pHandle = &pTrans->msgHandles[TMSG_INDEX(pRpc->msgType)];

  const STraceId *trace = &pRpc->info.traceId;
  dGDebug("msg:%s is received, handle:%p len:%d code:0x%x app:%p refId:%" PRId64 " %" PRIx64 ":%" PRIx64, TMSG_INFO(pRpc->msgType),
          pRpc->info.handle, pRpc->contLen, pRpc->code, pRpc->info.ahandle, pRpc->info.refId, TRACE_GET_ROOTID(trace), TRACE_GET_MSGID(trace));

  int32_t svrVer = 0;
  code = taosVersionStrToInt(td_version, &svrVer);
  if (code != 0) {
    dError("failed to convert version string:%s to int, code:%d", td_version, code);
    goto _OVER;
  }
  if ((code = taosCheckVersionCompatible(pRpc->info.cliVer, svrVer, 3)) != 0) {
    dError("Version not compatible, cli ver: %d, svr ver: %d, ip:%s", pRpc->info.cliVer, svrVer,
           IP_ADDR_STR(&pRpc->info.conn.cliAddr));
    goto _OVER;
  }

  bool isForbidden = dmIsForbiddenIp(pRpc->info.forbiddenIp, pRpc->info.conn.user, &pRpc->info.conn.cliAddr);
  if (isForbidden) {
    code = TSDB_CODE_IP_NOT_IN_WHITE_LIST;
    goto _OVER;
  }

  switch (pRpc->msgType) {
    case TDMT_DND_NET_TEST:
      dmProcessNetTestReq(pDnode, pRpc);
      return;
    case TDMT_MND_SYSTABLE_RETRIEVE_RSP:
    case TDMT_DND_SYSTABLE_RETRIEVE_RSP:
    case TDMT_SCH_FETCH_RSP:
    case TDMT_SCH_MERGE_FETCH_RSP:
    case TDMT_VND_SUBMIT_RSP:
    case TDMT_MND_GET_DB_INFO_RSP:
    case TDMT_STREAM_FETCH_RSP:
    case TDMT_STREAM_FETCH_FROM_RUNNER_RSP:
    case TDMT_STREAM_FETCH_FROM_CACHE_RSP:
      code = qWorkerProcessRspMsg(NULL, NULL, pRpc, 0);
      return;
    case TDMT_MND_STATUS_RSP:
      if (pEpSet != NULL) {
        dmSetMnodeEpSet(&pDnode->data, pEpSet);
      }
      break;
    case TDMT_MND_RETRIEVE_IP_WHITE_RSP:
      dmUpdateRpcIpWhiteUnused(&pDnode->data, pTrans->serverRpc, pRpc);
      return;
    case TDMT_MND_RETRIEVE_IP_WHITE_DUAL_RSP:
      dmUpdateRpcIpWhite(&pDnode->data, pTrans->serverRpc, pRpc);
      return;
    case TDMT_MND_RETRIEVE_ANAL_ALGO_RSP:
      dmUpdateAnalyticFunc(&pDnode->data, pTrans->serverRpc, pRpc);
      return;
    default:
      break;
  }

  /*
  pDnode is null, TD-22618
  at trans.c line 91
  before this line, dmProcessRpcMsg callback is set
  after this line, parent is set
  so when dmProcessRpcMsg is called, pDonde is still null.
  */
  if (pDnode != NULL) {
    if (pDnode->status != DND_STAT_RUNNING) {
      if (pRpc->msgType == TDMT_DND_SERVER_STATUS) {
        dmProcessServerStartupStatus(pDnode, pRpc);
        return;
      } else {
        if (pDnode->status == DND_STAT_INIT) {
          code = TSDB_CODE_APP_IS_STARTING;
        } else {
          code = TSDB_CODE_APP_IS_STOPPING;
        }
        goto _OVER;
      }
    }
  } else {
    code = TSDB_CODE_APP_IS_STARTING;
    goto _OVER;
  }

  if (pRpc->pCont == NULL && (IsReq(pRpc) || pRpc->contLen != 0)) {
    dGError("msg:%p, type:%s pCont is NULL", pRpc, TMSG_INFO(pRpc->msgType));
    code = TSDB_CODE_INVALID_MSG_LEN;
    goto _OVER;
  } else if ((pRpc->code == TSDB_CODE_RPC_NETWORK_UNAVAIL || pRpc->code == TSDB_CODE_RPC_BROKEN_LINK) &&
             (!IsReq(pRpc)) && (pRpc->pCont == NULL)) {
    dGError("msg:%p, type:%s pCont is NULL, err: %s", pRpc, TMSG_INFO(pRpc->msgType), tstrerror(pRpc->code));
  }

  if (pHandle->defaultNtype == NODE_END) {
    dGError("msg:%p, type:%s not processed since no handle", pRpc, TMSG_INFO(pRpc->msgType));
    code = TSDB_CODE_MSG_NOT_PROCESSED;
    goto _OVER;
  }

  pWrapper = &pDnode->wrappers[pHandle->defaultNtype];
  if (pHandle->needCheckVgId) {
    if (pRpc->contLen > 0) {
      const SMsgHead *pHead = pRpc->pCont;
      const int32_t   vgId = ntohl(pHead->vgId);
      switch (vgId) {
        case QNODE_HANDLE:
          pWrapper = &pDnode->wrappers[QNODE];
          break;
        case SNODE_HANDLE:
          pWrapper = &pDnode->wrappers[SNODE];
          break;
        case MNODE_HANDLE:
          pWrapper = &pDnode->wrappers[MNODE];
          break;
        default:
          break;
      }
    } else {
      dGError("msg:%p, type:%s contLen is 0", pRpc, TMSG_INFO(pRpc->msgType));
      code = TSDB_CODE_INVALID_MSG_LEN;
      goto _OVER;
    }
  }

  if ((code = dmMarkWrapper(pWrapper)) != 0) {
    pWrapper = NULL;
    goto _OVER;
  }

  pRpc->info.wrapper = pWrapper;

  EQItype itype = RPC_QITEM;  // rsp msg is not restricted by tsQueueMemoryUsed
  if (IsReq(pRpc)) {
    if (pRpc->msgType == TDMT_SYNC_HEARTBEAT || pRpc->msgType == TDMT_SYNC_HEARTBEAT_REPLY)
      itype = DEF_QITEM;
    else
      itype = RPC_QITEM;
  } else {
    itype = DEF_QITEM;
  }
  code = taosAllocateQitem(sizeof(SRpcMsg), itype, pRpc->contLen, (void **)&pMsg);
  if (code) goto _OVER;

  memcpy(pMsg, pRpc, sizeof(SRpcMsg));
  dGDebug("msg:%p, is created, type:%s handle:%p len:%d %" PRIx64 ":%" PRIx64, pMsg, TMSG_INFO(pRpc->msgType), pMsg->info.handle,
          pRpc->contLen, TRACE_GET_ROOTID(&pMsg->info.traceId), TRACE_GET_MSGID(&pMsg->info.traceId));

  code = dmProcessNodeMsg(pWrapper, pMsg);

_OVER:
  if (code != 0) {
    code = dmConvertErrCode(pRpc->msgType, code);
    if (pMsg) {
      dGTrace("msg:%p, failed to process %s since %s", pMsg, TMSG_INFO(pMsg->msgType), tstrerror(code));
    } else {
      dGTrace("msg:%p, failed to process empty msg since %s", pMsg, tstrerror(code));
    }

    if (IsReq(pRpc)) {
      SRpcMsg rsp = {.code = code, .info = pRpc->info, .msgType = pRpc->msgType + 1};
      if (code == TSDB_CODE_MNODE_NOT_FOUND) {
        dmBuildMnodeRedirectRsp(pDnode, &rsp);
      }

      if (pWrapper != NULL) {
        dmSendRsp(&rsp);
      } else {
        if (rpcSendResponse(&rsp) != 0) {
          dError("failed to send response, msg:%p", &rsp);
        }
      }
    } else if (NULL == pMsg && IS_STREAM_TRIGGER_RSP_MSG(pRpc->msgType)) {
      destroyAhandle(pRpc->info.ahandle);
      dDebug("msg:%s ahandle freed", TMSG_INFO(pRpc->msgType));
    }

    if (pMsg != NULL) {
      dGTrace("msg:%p, is freed", pMsg);
      taosFreeQitem(pMsg);
    }
    rpcFreeCont(pRpc->pCont);
    pRpc->pCont = NULL;
  }

  dmReleaseWrapper(pWrapper);
}

int32_t dmInitMsgHandle(SDnode *pDnode) {
  SDnodeTrans *pTrans = &pDnode->trans;

  for (EDndNodeType ntype = DNODE; ntype < NODE_END; ++ntype) {
    SMgmtWrapper *pWrapper = &pDnode->wrappers[ntype];
    SArray       *pArray = (*pWrapper->func.getHandlesFp)();
    if (pArray == NULL) return -1;

    for (int32_t i = 0; i < taosArrayGetSize(pArray); ++i) {
      SMgmtHandle  *pMgmt = taosArrayGet(pArray, i);
      SDnodeHandle *pHandle = &pTrans->msgHandles[TMSG_INDEX(pMgmt->msgType)];
      if (pMgmt->needCheckVgId) {
        pHandle->needCheckVgId = pMgmt->needCheckVgId;
      }
      if (!pMgmt->needCheckVgId) {
        pHandle->defaultNtype = ntype;
      }
      pWrapper->msgFps[TMSG_INDEX(pMgmt->msgType)] = pMgmt->msgFp;
    }

    taosArrayDestroy(pArray);
  }

  return 0;
}

static inline int32_t dmSendReq(const SEpSet *pEpSet, SRpcMsg *pMsg) {
  int32_t code = 0;
  SDnode *pDnode = dmInstance();
  if (pDnode->status != DND_STAT_RUNNING && pMsg->msgType < TDMT_SYNC_MSG_MIN) {
    rpcFreeCont(pMsg->pCont);
    pMsg->pCont = NULL;
    if (pDnode->status == DND_STAT_INIT) {
      code = TSDB_CODE_APP_IS_STARTING;
    } else {
      code = TSDB_CODE_APP_IS_STOPPING;
    }
    dError("failed to send rpc msg:%s since %s, handle:%p", TMSG_INFO(pMsg->msgType), tstrerror(code),
           pMsg->info.handle);
    return code;
  } else {
    pMsg->info.handle = 0;
    code = rpcSendRequest(pDnode->trans.clientRpc, pEpSet, pMsg, NULL);
    if (code != 0) {
      dError("failed to send rpc msg");
      return code;
    }
    return 0;
  }
}
static inline int32_t dmSendSyncReq(const SEpSet *pEpSet, SRpcMsg *pMsg) {
  int32_t code = 0;
  SDnode *pDnode = dmInstance();
  if (pDnode->status != DND_STAT_RUNNING && pMsg->msgType < TDMT_SYNC_MSG_MIN) {
    rpcFreeCont(pMsg->pCont);
    pMsg->pCont = NULL;
    if (pDnode->status == DND_STAT_INIT) {
      code = TSDB_CODE_APP_IS_STARTING;
    } else {
      code = TSDB_CODE_APP_IS_STOPPING;
    }
    dError("failed to send rpc msg:%s since %s, handle:%p", TMSG_INFO(pMsg->msgType), tstrerror(code),
           pMsg->info.handle);
    return code;
  } else {
    return rpcSendRequest(pDnode->trans.syncRpc, pEpSet, pMsg, NULL);
  }
}

static inline void dmRegisterBrokenLinkArg(SRpcMsg *pMsg) { (void)rpcRegisterBrokenLinkArg(pMsg); }

static inline void dmReleaseHandle(SRpcHandleInfo *pHandle, int8_t type, int32_t status) {
  (void)rpcReleaseHandle(pHandle, type, status);
}

static bool rpcRfp(int32_t code, tmsg_t msgType) {
  if (code == TSDB_CODE_RPC_NETWORK_UNAVAIL || code == TSDB_CODE_RPC_BROKEN_LINK || code == TSDB_CODE_MNODE_NOT_FOUND ||
      code == TSDB_CODE_RPC_SOMENODE_NOT_CONNECTED || code == TSDB_CODE_SYN_NOT_LEADER ||
      code == TSDB_CODE_SYN_RESTORING || code == TSDB_CODE_VND_STOPPED || code == TSDB_CODE_APP_IS_STARTING ||
      code == TSDB_CODE_APP_IS_STOPPING) {
    if (msgType == TDMT_SCH_QUERY || msgType == TDMT_SCH_MERGE_QUERY || msgType == TDMT_SCH_FETCH ||
        msgType == TDMT_SCH_MERGE_FETCH || msgType == TDMT_SCH_TASK_NOTIFY || msgType == TDMT_VND_DROP_TTL_TABLE) {
      return false;
    }
    return true;
  } else {
    return false;
  }
}
static bool rpcNoDelayMsg(tmsg_t msgType) {
  if (msgType == TDMT_VND_FETCH_TTL_EXPIRED_TBS || msgType == TDMT_VND_SSMIGRATE ||
      msgType == TDMT_VND_QUERY_COMPACT_PROGRESS || msgType == TDMT_VND_DROP_TTL_TABLE ||
      msgType == TDMT_VND_FOLLOWER_SSMIGRATE) {
    return true;
  }
  return false;
}
int32_t dmInitClient(SDnode *pDnode) {
  SDnodeTrans *pTrans = &pDnode->trans;

  SRpcInit rpcInit = {0};
  rpcInit.label = "DNODE-CLI";
  rpcInit.numOfThreads = tsNumOfRpcThreads / 2;
  rpcInit.cfp = (RpcCfp)dmProcessRpcMsg;
  rpcInit.sessions = 1024;
  rpcInit.connType = TAOS_CONN_CLIENT;
  rpcInit.user = TSDB_DEFAULT_USER;
  rpcInit.idleTime = tsShellActivityTimer * 1000;
  rpcInit.parent = pDnode;
  rpcInit.rfp = rpcRfp;
  rpcInit.compressSize = tsCompressMsgSize;
  rpcInit.dfp = destroyAhandle;

  rpcInit.retryMinInterval = tsRedirectPeriod;
  rpcInit.retryStepFactor = tsRedirectFactor;
  rpcInit.retryMaxInterval = tsRedirectMaxPeriod;
  rpcInit.retryMaxTimeout = tsMaxRetryWaitTime;

  rpcInit.failFastInterval = 5000;  // interval threshold(ms)
  rpcInit.failFastThreshold = 3;    // failed threshold
  rpcInit.ffp = dmFailFastFp;

  rpcInit.noDelayFp = rpcNoDelayMsg;

  int32_t connLimitNum = tsNumOfRpcSessions / (tsNumOfRpcThreads * 3);
  connLimitNum = TMAX(connLimitNum, 10);
  connLimitNum = TMIN(connLimitNum, 500);

  rpcInit.connLimitNum = connLimitNum;
  rpcInit.connLimitLock = 1;
  rpcInit.supportBatch = 1;
  rpcInit.shareConnLimit = tsShareConnLimit * 2;
  rpcInit.shareConn = 1;
  rpcInit.timeToGetConn = tsTimeToGetAvailableConn;
  rpcInit.notWaitAvaliableConn = 0;
  rpcInit.startReadTimer = 1;
  rpcInit.readTimeout = tsReadTimeout;
  rpcInit.ipv6 = tsEnableIpv6;

  if (taosVersionStrToInt(td_version, &rpcInit.compatibilityVer) != 0) {
    dError("failed to convert version string:%s to int", td_version);
  }

  pTrans->clientRpc = rpcOpen(&rpcInit);
  if (pTrans->clientRpc == NULL) {
    dError("failed to init dnode rpc client since:%s", tstrerror(terrno));
    return terrno;
  }

  dDebug("dnode rpc client is initialized");
  return 0;
}
int32_t dmInitStatusClient(SDnode *pDnode) {
  SDnodeTrans *pTrans = &pDnode->trans;

  SRpcInit rpcInit = {0};
  rpcInit.label = "DNODE-STA-CLI";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp = (RpcCfp)dmProcessRpcMsg;
  rpcInit.sessions = 1024;
  rpcInit.connType = TAOS_CONN_CLIENT;
  rpcInit.user = TSDB_DEFAULT_USER;
  rpcInit.idleTime = tsShellActivityTimer * 1000;
  rpcInit.parent = pDnode;
  rpcInit.rfp = rpcRfp;
  rpcInit.compressSize = tsCompressMsgSize;

  rpcInit.retryMinInterval = tsRedirectPeriod;
  rpcInit.retryStepFactor = tsRedirectFactor;
  rpcInit.retryMaxInterval = tsRedirectMaxPeriod;
  rpcInit.retryMaxTimeout = tsMaxRetryWaitTime;

  rpcInit.failFastInterval = 5000;  // interval threshold(ms)
  rpcInit.failFastThreshold = 3;    // failed threshold
  rpcInit.ffp = dmFailFastFp;

  int32_t connLimitNum = 100;
  connLimitNum = TMAX(connLimitNum, 10);
  connLimitNum = TMIN(connLimitNum, 500);

  rpcInit.connLimitNum = connLimitNum;
  rpcInit.connLimitLock = 1;
  rpcInit.supportBatch = 1;
  rpcInit.shareConnLimit = tsShareConnLimit * 2;
  rpcInit.timeToGetConn = tsTimeToGetAvailableConn;
  rpcInit.startReadTimer = 0;
  rpcInit.readTimeout = 0;
  rpcInit.ipv6 = tsEnableIpv6;

  if (taosVersionStrToInt(td_version, &rpcInit.compatibilityVer) != 0) {
    dError("failed to convert version string:%s to int", td_version);
  }

  pTrans->statusRpc = rpcOpen(&rpcInit);
  if (pTrans->statusRpc == NULL) {
    dError("failed to init dnode rpc status client since %s", tstrerror(terrno));
    return terrno;
  }

  dDebug("dnode rpc status client is initialized");
  return 0;
}

int32_t dmInitSyncClient(SDnode *pDnode) {
  SDnodeTrans *pTrans = &pDnode->trans;

  SRpcInit rpcInit = {0};
  rpcInit.label = "DNODE-SYNC-CLI";
  rpcInit.numOfThreads = tsNumOfRpcThreads / 2;
  rpcInit.cfp = (RpcCfp)dmProcessRpcMsg;
  rpcInit.sessions = 1024;
  rpcInit.connType = TAOS_CONN_CLIENT;
  rpcInit.user = TSDB_DEFAULT_USER;
  rpcInit.idleTime = tsShellActivityTimer * 1000;
  rpcInit.parent = pDnode;
  rpcInit.rfp = rpcRfp;
  rpcInit.compressSize = tsCompressMsgSize;

  rpcInit.retryMinInterval = tsRedirectPeriod;
  rpcInit.retryStepFactor = tsRedirectFactor;
  rpcInit.retryMaxInterval = tsRedirectMaxPeriod;
  rpcInit.retryMaxTimeout = tsMaxRetryWaitTime;

  rpcInit.failFastInterval = 5000;  // interval threshold(ms)
  rpcInit.failFastThreshold = 3;    // failed threshold
  rpcInit.ffp = dmFailFastFp;

  int32_t connLimitNum = tsNumOfRpcSessions / (tsNumOfRpcThreads * 3) / 2;
  connLimitNum = TMAX(connLimitNum, 10);
  connLimitNum = TMIN(connLimitNum, 500);

  rpcInit.connLimitNum = connLimitNum;
  rpcInit.connLimitLock = 1;
  rpcInit.supportBatch = 1;
  rpcInit.shareConnLimit = tsShareConnLimit * 8;
  rpcInit.timeToGetConn = tsTimeToGetAvailableConn;
  rpcInit.startReadTimer = 1;
  rpcInit.readTimeout = tsReadTimeout;
  rpcInit.ipv6 = tsEnableIpv6;

  if (taosVersionStrToInt(td_version, &rpcInit.compatibilityVer) != 0) {
    dError("failed to convert version string:%s to int", td_version);
  }

  pTrans->syncRpc = rpcOpen(&rpcInit);
  if (pTrans->syncRpc == NULL) {
    dError("failed to init dnode rpc sync client since %s", tstrerror(terrno));
    return terrno;
  }

  dDebug("dnode rpc sync client is initialized");
  return 0;
}

void dmCleanupClient(SDnode *pDnode) {
  SDnodeTrans *pTrans = &pDnode->trans;
  if (pTrans->clientRpc) {
    rpcClose(pTrans->clientRpc);
    pTrans->clientRpc = NULL;
    dDebug("dnode rpc client is closed");
  }
}
void dmCleanupStatusClient(SDnode *pDnode) {
  SDnodeTrans *pTrans = &pDnode->trans;
  if (pTrans->statusRpc) {
    rpcClose(pTrans->statusRpc);
    pTrans->statusRpc = NULL;
    dDebug("dnode rpc status client is closed");
  }
}
void dmCleanupSyncClient(SDnode *pDnode) {
  SDnodeTrans *pTrans = &pDnode->trans;
  if (pTrans->syncRpc) {
    rpcClose(pTrans->syncRpc);
    pTrans->syncRpc = NULL;
    dDebug("dnode rpc sync client is closed");
  }
}

int32_t dmInitServer(SDnode *pDnode) {
  int32_t      code = 0;
  SDnodeTrans *pTrans = &pDnode->trans;

  SRpcInit rpcInit = {0};
  tstrncpy(rpcInit.localFqdn, tsLocalFqdn, TSDB_FQDN_LEN);

  rpcInit.localPort = tsServerPort;
  rpcInit.label = "DND-S";
  rpcInit.numOfThreads = tsNumOfRpcThreads;
  rpcInit.cfp = (RpcCfp)dmProcessRpcMsg;
  rpcInit.sessions = tsMaxShellConns;
  rpcInit.connType = TAOS_CONN_SERVER;
  rpcInit.idleTime = tsShellActivityTimer * 1000;
  rpcInit.parent = pDnode;
  rpcInit.compressSize = tsCompressMsgSize;
  rpcInit.shareConnLimit = tsShareConnLimit * 16;
  rpcInit.ipv6 = tsEnableIpv6;

  if (taosVersionStrToInt(td_version, &rpcInit.compatibilityVer) != 0) {
    dError("failed to convert version string:%s to int", td_version);
  }

  pTrans->serverRpc = rpcOpen(&rpcInit);
  if (pTrans->serverRpc == NULL) {
    dError("failed to init dnode rpc server since:%s", tstrerror(terrno));
    return terrno;
  }

  dDebug("dnode rpc server is initialized");
  return 0;
}

void dmCleanupServer(SDnode *pDnode) {
  SDnodeTrans *pTrans = &pDnode->trans;
  if (pTrans->serverRpc) {
    rpcClose(pTrans->serverRpc);
    pTrans->serverRpc = NULL;
    dDebug("dnode rpc server is closed");
  }
}

SMsgCb dmGetMsgcb(SDnode *pDnode) {
  SMsgCb msgCb = {
      .clientRpc = pDnode->trans.clientRpc,
      .serverRpc = pDnode->trans.serverRpc,
      .statusRpc = pDnode->trans.statusRpc,
      .syncRpc = pDnode->trans.syncRpc,
      .sendReqFp = dmSendReq,
      .sendSyncReqFp = dmSendSyncReq,
      .sendRspFp = dmSendRsp,
      .registerBrokenLinkArgFp = dmRegisterBrokenLinkArg,
      .releaseHandleFp = dmReleaseHandle,
      .reportStartupFp = dmReportStartup,
      .updateDnodeInfoFp = dmUpdateDnodeInfo,
      .getDnodeEpFp = dmGetDnodeEp,
      .data = &pDnode->data,
  };
  return msgCb;
}
