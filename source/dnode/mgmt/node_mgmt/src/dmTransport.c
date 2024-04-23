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
#include "tversion.h"

static inline void dmSendRsp(SRpcMsg *pMsg) { rpcSendResponse(pMsg); }

static inline void dmBuildMnodeRedirectRsp(SDnode *pDnode, SRpcMsg *pMsg) {
  SEpSet epSet = {0};
  dmGetMnodeEpSetForRedirect(&pDnode->data, pMsg, &epSet);

  const int32_t contLen = tSerializeSEpSet(NULL, 0, &epSet);
  pMsg->pCont = rpcMallocCont(contLen);
  if (pMsg->pCont == NULL) {
    pMsg->code = TSDB_CODE_OUT_OF_MEMORY;
  } else {
    tSerializeSEpSet(pMsg->pCont, contLen, &epSet);
    pMsg->contLen = contLen;
  }
}

int32_t dmProcessNodeMsg(SMgmtWrapper *pWrapper, SRpcMsg *pMsg) {
  const STraceId *trace = &pMsg->info.traceId;

  NodeMsgFp msgFp = pWrapper->msgFps[TMSG_INDEX(pMsg->msgType)];
  if (msgFp == NULL) {
    terrno = TSDB_CODE_MSG_NOT_PROCESSED;
    dGError("msg:%p, not processed since no handler, type:%s", pMsg, TMSG_INFO(pMsg->msgType));
    return -1;
  }

  dGTrace("msg:%p, will be processed by %s", pMsg, pWrapper->name);
  pMsg->info.wrapper = pWrapper;
  return (*msgFp)(pWrapper->pMgmt, pMsg);
}

static bool dmFailFastFp(tmsg_t msgType) {
  // add more msg type later
  return msgType == TDMT_SYNC_HEARTBEAT || msgType == TDMT_SYNC_APPEND_ENTRIES;
}

static void dmConvertErrCode(tmsg_t msgType) {
  if (terrno != TSDB_CODE_APP_IS_STOPPING) {
    return;
  }
  if ((msgType > TDMT_VND_MSG && msgType < TDMT_VND_MAX_MSG) ||
      (msgType > TDMT_SCH_MSG && msgType < TDMT_SCH_MAX_MSG)) {
    terrno = TSDB_CODE_VND_STOPPED;
  }
}
static void dmUpdateRpcIpWhite(SDnodeData *pData, void *pTrans, SRpcMsg *pRpc) {
  SUpdateIpWhite ipWhite = {0};  // aosMemoryCalloc(1, sizeof(SUpdateIpWhite));
  tDeserializeSUpdateIpWhite(pRpc->pCont, pRpc->contLen, &ipWhite);

  rpcSetIpWhite(pTrans, &ipWhite);
  pData->ipWhiteVer = ipWhite.ver;

  tFreeSUpdateIpWhiteReq(&ipWhite);

  rpcFreeCont(pRpc->pCont);
}
static bool dmIsForbiddenIp(int8_t forbidden, char *user, uint32_t clientIp) {
  if (forbidden) {
    SIpV4Range range = {.ip = clientIp, .mask = 32};
    char       buf[36] = {0};

    rpcUtilSIpRangeToStr(&range, buf);
    dError("User:%s host:%s not in ip white list", user, buf);
    return true;
  } else {
    return false;
  }
}
static void dmProcessRpcMsg(SDnode *pDnode, SRpcMsg *pRpc, SEpSet *pEpSet) {
  SDnodeTrans  *pTrans = &pDnode->trans;
  int32_t       code = -1;
  SRpcMsg      *pMsg = NULL;
  SMgmtWrapper *pWrapper = NULL;
  SDnodeHandle *pHandle = &pTrans->msgHandles[TMSG_INDEX(pRpc->msgType)];

  const STraceId *trace = &pRpc->info.traceId;
  dGTrace("msg:%s is received, handle:%p len:%d code:0x%x app:%p refId:%" PRId64, TMSG_INFO(pRpc->msgType),
          pRpc->info.handle, pRpc->contLen, pRpc->code, pRpc->info.ahandle, pRpc->info.refId);

  int32_t svrVer = 0;
  taosVersionStrToInt(version, &svrVer);
  if (0 != taosCheckVersionCompatible(pRpc->info.cliVer, svrVer, 3)) {
    dError("Version not compatible, cli ver: %d, svr ver: %d", pRpc->info.cliVer, svrVer);
    goto _OVER;
  }

  bool isForbidden = dmIsForbiddenIp(pRpc->info.forbiddenIp, pRpc->info.conn.user, pRpc->info.conn.clientIp);
  if (isForbidden) {
    terrno = TSDB_CODE_IP_NOT_IN_WHITE_LIST;
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
      qWorkerProcessRspMsg(NULL, NULL, pRpc, 0);
      return;
    case TDMT_MND_STATUS_RSP:
      if (pEpSet != NULL) {
        dmSetMnodeEpSet(&pDnode->data, pEpSet);
      }
      break;
    case TDMT_MND_RETRIEVE_IP_WHITE_RSP: {
      dmUpdateRpcIpWhite(&pDnode->data, pTrans->serverRpc, pRpc);
      return;
    } break;
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
          terrno = TSDB_CODE_APP_IS_STARTING;
        } else {
          terrno = TSDB_CODE_APP_IS_STOPPING;
        }
        goto _OVER;
      }
    }
  } else {
    terrno = TSDB_CODE_APP_IS_STARTING;
    goto _OVER;
  }

  if (pRpc->pCont == NULL && (IsReq(pRpc) || pRpc->contLen != 0)) {
    dGError("msg:%p, type:%s pCont is NULL", pRpc, TMSG_INFO(pRpc->msgType));
    terrno = TSDB_CODE_INVALID_MSG_LEN;
    goto _OVER;
  } else if ((pRpc->code == TSDB_CODE_RPC_NETWORK_UNAVAIL || pRpc->code == TSDB_CODE_RPC_BROKEN_LINK) &&
             (!IsReq(pRpc)) && (pRpc->pCont == NULL)) {
    dGError("msg:%p, type:%s pCont is NULL, err: %s", pRpc, TMSG_INFO(pRpc->msgType), tstrerror(pRpc->code));
    terrno = pRpc->code;
    goto _OVER;
  }

  if (pHandle->defaultNtype == NODE_END) {
    dGError("msg:%p, type:%s not processed since no handle", pRpc, TMSG_INFO(pRpc->msgType));
    terrno = TSDB_CODE_MSG_NOT_PROCESSED;
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
      terrno = TSDB_CODE_INVALID_MSG_LEN;
      goto _OVER;
    }
  }

  if (dmMarkWrapper(pWrapper) != 0) {
    pWrapper = NULL;
    goto _OVER;
  }

  pRpc->info.wrapper = pWrapper;
  pMsg = taosAllocateQitem(sizeof(SRpcMsg), RPC_QITEM, pRpc->contLen);
  if (pMsg == NULL) goto _OVER;

  memcpy(pMsg, pRpc, sizeof(SRpcMsg));
  dGTrace("msg:%p, is created, type:%s handle:%p len:%d", pMsg, TMSG_INFO(pRpc->msgType), pMsg->info.handle,
          pRpc->contLen);

  code = dmProcessNodeMsg(pWrapper, pMsg);

_OVER:
  if (code != 0) {
    dmConvertErrCode(pRpc->msgType);
    if (terrno != 0) code = terrno;
    if (pMsg) {
      dGTrace("msg:%p, failed to process %s since %s", pMsg, TMSG_INFO(pMsg->msgType), terrstr());
    } else {
      dGTrace("msg:%p, failed to process empty msg since %s", pMsg, terrstr());
    }

    if (IsReq(pRpc)) {
      SRpcMsg rsp = {.code = code, .info = pRpc->info};
      if (code == TSDB_CODE_MNODE_NOT_FOUND) {
        dmBuildMnodeRedirectRsp(pDnode, &rsp);
      }

      if (pWrapper != NULL) {
        dmSendRsp(&rsp);
      } else {
        rpcSendResponse(&rsp);
      }
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
  SDnode *pDnode = dmInstance();
  if (pDnode->status != DND_STAT_RUNNING && pMsg->msgType < TDMT_SYNC_MSG) {
    rpcFreeCont(pMsg->pCont);
    pMsg->pCont = NULL;
    if (pDnode->status == DND_STAT_INIT) {
      terrno = TSDB_CODE_APP_IS_STARTING;
    } else {
      terrno = TSDB_CODE_APP_IS_STOPPING;
    }
    dError("failed to send rpc msg:%s since %s, handle:%p", TMSG_INFO(pMsg->msgType), terrstr(), pMsg->info.handle);
    return -1;
  } else {
    pMsg->info.handle = 0;
    rpcSendRequest(pDnode->trans.clientRpc, pEpSet, pMsg, NULL);
    return 0;
  }
}
static inline int32_t dmSendSyncReq(const SEpSet *pEpSet, SRpcMsg *pMsg) {
  SDnode *pDnode = dmInstance();
  if (pDnode->status != DND_STAT_RUNNING && pMsg->msgType < TDMT_SYNC_MSG) {
    rpcFreeCont(pMsg->pCont);
    pMsg->pCont = NULL;
    if (pDnode->status == DND_STAT_INIT) {
      terrno = TSDB_CODE_APP_IS_STARTING;
    } else {
      terrno = TSDB_CODE_APP_IS_STOPPING;
    }
    dError("failed to send rpc msg:%s since %s, handle:%p", TMSG_INFO(pMsg->msgType), terrstr(), pMsg->info.handle);
    return -1;
  } else {
    rpcSendRequest(pDnode->trans.syncRpc, pEpSet, pMsg, NULL);
    return 0;
  }
}

static inline void dmRegisterBrokenLinkArg(SRpcMsg *pMsg) { rpcRegisterBrokenLinkArg(pMsg); }

static inline void dmReleaseHandle(SRpcHandleInfo *pHandle, int8_t type) { rpcReleaseHandle(pHandle, type); }

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
  rpcInit.batchSize = 8 * 1024;
  rpcInit.timeToGetConn = tsTimeToGetAvailableConn;
  taosVersionStrToInt(version, &(rpcInit.compatibilityVer));

  pTrans->clientRpc = rpcOpen(&rpcInit);
  if (pTrans->clientRpc == NULL) {
    dError("failed to init dnode rpc client");
    return -1;
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
  rpcInit.batchSize = 8 * 1024;
  rpcInit.timeToGetConn = tsTimeToGetAvailableConn;
  taosVersionStrToInt(version, &(rpcInit.compatibilityVer));

  pTrans->statusRpc = rpcOpen(&rpcInit);
  if (pTrans->statusRpc == NULL) {
    dError("failed to init dnode rpc status client");
    return -1;
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
  rpcInit.batchSize = 8 * 1024;
  rpcInit.timeToGetConn = tsTimeToGetAvailableConn;
  taosVersionStrToInt(version, &(rpcInit.compatibilityVer));

  pTrans->syncRpc = rpcOpen(&rpcInit);
  if (pTrans->syncRpc == NULL) {
    dError("failed to init dnode rpc sync client");
    return -1;
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
  taosVersionStrToInt(version, &(rpcInit.compatibilityVer));
  pTrans->serverRpc = rpcOpen(&rpcInit);
  if (pTrans->serverRpc == NULL) {
    dError("failed to init dnode rpc server");
    return -1;
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
      .data = &pDnode->data,
  };
  return msgCb;
}
