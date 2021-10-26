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
#include "os.h"
#include "tworker.h"
#include "tglobal.h"
#include "mnodeMnode.h"
#include "mnodeSdb.h"
#include "mnodeShow.h"
#include "mnodeSync.h"
#include "mnodeWorker.h"

static struct {
  SWorkerPool read;
  SWorkerPool write;
  SWorkerPool peerReq;
  SWorkerPool peerRsp;
  taos_queue  readQ;
  taos_queue  writeQ;
  taos_queue  peerReqQ;
  taos_queue  peerRspQ;
  int32_t (*writeMsgFp[TSDB_MSG_TYPE_MAX])(SMnMsg *);
  int32_t (*readMsgFp[TSDB_MSG_TYPE_MAX])(SMnMsg *);
  int32_t (*peerReqFp[TSDB_MSG_TYPE_MAX])(SMnMsg *);
  void (*peerRspFp[TSDB_MSG_TYPE_MAX])(SRpcMsg *);
  void (*msgFp[TSDB_MSG_TYPE_MAX])(SRpcMsg *pMsg);
} tsMworker = {0};

static SMnMsg *mnodeInitMsg(SRpcMsg *pRpcMsg) {
  int32_t    size = sizeof(SMnMsg) + pRpcMsg->contLen;
  SMnMsg *pMsg = taosAllocateQitem(size);

  pMsg->rpcMsg = *pRpcMsg;
  pMsg->rpcMsg.pCont = pMsg->pCont;
  pMsg->createdTime = taosGetTimestampSec();
  memcpy(pMsg->pCont, pRpcMsg->pCont, pRpcMsg->contLen);

  SRpcConnInfo connInfo = {0};
  if (rpcGetConnInfo(pMsg->rpcMsg.handle, &connInfo) == 0) {
    pMsg->pUser = sdbGetRow(MN_SDB_USER, connInfo.user);
  }

  if (pMsg->pUser == NULL) {
    mError("can not get user from conn:%p", pMsg->rpcMsg.handle);
    taosFreeQitem(pMsg);
    return NULL;
  }

  return pMsg;
}

static void mnodeCleanupMsg(SMnMsg *pMsg) {
  if (pMsg == NULL) return;
  if (pMsg->rpcMsg.pCont != pMsg->pCont) {
    tfree(pMsg->rpcMsg.pCont);
  }

  taosFreeQitem(pMsg);
}

static void mnodeDispatchToWriteQueue(SRpcMsg *pRpcMsg) {
  if (mnodeGetStatus() != MN_STATUS_READY || tsMworker.writeQ == NULL) {
    mnodeSendRedirectMsg(pRpcMsg, true);
  } else {
    SMnMsg *pMsg = mnodeInitMsg(pRpcMsg);
    if (pMsg == NULL) {
      SRpcMsg rpcRsp = {.handle = pRpcMsg->handle, .code = TSDB_CODE_MND_INVALID_USER};
      rpcSendResponse(&rpcRsp);
    } else {
      mTrace("msg:%p, app:%p type:%s is put into wqueue", pMsg, pMsg->rpcMsg.ahandle, taosMsg[pMsg->rpcMsg.msgType]);
      taosWriteQitem(tsMworker.writeQ, TAOS_QTYPE_RPC, pMsg);
    }
  }

  rpcFreeCont(pRpcMsg->pCont);
}

void mnodeReDispatchToWriteQueue(SMnMsg *pMsg) {
  if (mnodeGetStatus() != MN_STATUS_READY || tsMworker.writeQ == NULL) {
    mnodeSendRedirectMsg(&pMsg->rpcMsg, true);
    mnodeCleanupMsg(pMsg);
  } else {
    taosWriteQitem(tsMworker.writeQ, TAOS_QTYPE_RPC, pMsg);
  }
}

static void mnodeDispatchToReadQueue(SRpcMsg *pRpcMsg) {
  if (mnodeGetStatus() != MN_STATUS_READY || tsMworker.readQ == NULL) {
    mnodeSendRedirectMsg(pRpcMsg, true);
  } else {
    SMnMsg *pMsg = mnodeInitMsg(pRpcMsg);
    if (pMsg == NULL) {
      SRpcMsg rpcRsp = {.handle = pRpcMsg->handle, .code = TSDB_CODE_MND_INVALID_USER};
      rpcSendResponse(&rpcRsp);
    } else {
      mTrace("msg:%p, app:%p type:%s is put into rqueue", pMsg, pMsg->rpcMsg.ahandle, taosMsg[pMsg->rpcMsg.msgType]);
      taosWriteQitem(tsMworker.readQ, TAOS_QTYPE_RPC, pMsg);
    }
  }

  rpcFreeCont(pRpcMsg->pCont);
}

static void mnodeDispatchToPeerQueue(SRpcMsg *pRpcMsg) {
  if (mnodeGetStatus() != MN_STATUS_READY || tsMworker.peerReqQ == NULL) {
    mnodeSendRedirectMsg(pRpcMsg, false);
  } else {
    SMnMsg *pMsg = mnodeInitMsg(pRpcMsg);
    if (pMsg == NULL) {
      SRpcMsg rpcRsp = {.handle = pRpcMsg->handle, .code = TSDB_CODE_MND_INVALID_USER};
      rpcSendResponse(&rpcRsp);
    } else {
      mTrace("msg:%p, app:%p type:%s is put into peer req queue", pMsg, pMsg->rpcMsg.ahandle,
             taosMsg[pMsg->rpcMsg.msgType]);
      taosWriteQitem(tsMworker.peerReqQ, TAOS_QTYPE_RPC, pMsg);
    }
  }

  rpcFreeCont(pRpcMsg->pCont);
}

void mnodeDispatchToPeerRspQueue(SRpcMsg *pRpcMsg) {
  SMnMsg *pMsg = mnodeInitMsg(pRpcMsg);
  if (pMsg == NULL) {
    SRpcMsg rpcRsp = {.handle = pRpcMsg->handle, .code = TSDB_CODE_MND_INVALID_USER};
    rpcSendResponse(&rpcRsp);
  } else {
    mTrace("msg:%p, app:%p type:%s is put into peer rsp queue", pMsg, pMsg->rpcMsg.ahandle,
           taosMsg[pMsg->rpcMsg.msgType]);
    taosWriteQitem(tsMworker.peerRspQ, TAOS_QTYPE_RPC, pMsg);
  }

  // rpcFreeCont(pRpcMsg->pCont);
}

static void mnodeSendRpcRsp(void *ahandle, SMnMsg *pMsg, int32_t qtype, int32_t code) {
  if (pMsg == NULL) return;
  if (code == TSDB_CODE_MND_ACTION_IN_PROGRESS) return;
  if (code == TSDB_CODE_MND_ACTION_NEED_REPROCESSED) {
    mnodeReDispatchToWriteQueue(pMsg);
    return;
  }

  SRpcMsg rpcRsp = {
    .handle  = pMsg->rpcMsg.handle,
    .pCont   = pMsg->rpcRsp.rsp,
    .contLen = pMsg->rpcRsp.len,
    .code    = code,
  };

  rpcSendResponse(&rpcRsp);
  mnodeCleanupMsg(pMsg);
}

void mnodeSendRsp(SMnMsg *pMsg, int32_t code) { mnodeSendRpcRsp(NULL, pMsg, 0, code); }

static void mnodeProcessPeerRspEnd(void *ahandle, SMnMsg *pMsg, int32_t qtype, int32_t code) {
  mnodeCleanupMsg(pMsg);
}

static void mnodeInitMsgFp() {
//   // peer req
//   tsMworker.msgFp[TSDB_MSG_TYPE_DM_CONFIG_TABLE] = mnodeDispatchToPeerQueue;
//   tsMworker.peerReqFp[TSDB_MSG_TYPE_DM_CONFIG_TABLE] = mnodeProcessTableCfgMsg;
//   tsMworker.msgFp[TSDB_MSG_TYPE_DM_CONFIG_VNODE] = mnodeDispatchToPeerQueue;
//   tsMworker.peerReqFp[TSDB_MSG_TYPE_DM_CONFIG_VNODE] = mnodeProcessVnodeCfgMsg;
//   tsMworker.msgFp[TSDB_MSG_TYPE_DM_AUTH] = mnodeDispatchToPeerQueue;
//   tsMworker.peerReqFp[TSDB_MSG_TYPE_DM_AUTH] = mnodeProcessAuthMsg;
//   // tsMworker.msgFp[TSDB_MSG_TYPE_DM_GRANT] = mnodeDispatchToPeerQueue;
//   // tsMworker.peerReqFp[TSDB_MSG_TYPE_DM_GRANT] = grantProcessMsgInMgmt;
//   tsMworker.msgFp[TSDB_MSG_TYPE_DM_STATUS] = mnodeDispatchToPeerQueue;
//   tsMworker.peerReqFp[TSDB_MSG_TYPE_DM_STATUS] = mnodeProcessDnodeStatusMsg;

//   // peer rsp
//   tsMworker.msgFp[TSDB_MSG_TYPE_MD_CONFIG_DNODE_RSP] = mnodeDispatchToPeerRspQueue;
//   tsMworker.peerRspFp[TSDB_MSG_TYPE_MD_CONFIG_DNODE_RSP] = mnodeProcessCfgDnodeMsgRsp;

//   tsMworker.msgFp[TSDB_MSG_TYPE_MD_DROP_STABLE_RSP] = mnodeDispatchToPeerRspQueue;
//   tsMworker.peerRspFp[TSDB_MSG_TYPE_MD_DROP_STABLE_RSP] = mnodeProcessDropSuperTableRsp;
//   tsMworker.msgFp[TSDB_MSG_TYPE_MD_CREATE_TABLE_RSP] = mnodeDispatchToPeerRspQueue;
//   tsMworker.peerRspFp[TSDB_MSG_TYPE_MD_CREATE_TABLE_RSP] = mnodeProcessCreateChildTableRsp;
//   tsMworker.msgFp[TSDB_MSG_TYPE_MD_DROP_TABLE_RSP] = mnodeDispatchToPeerRspQueue;
//   tsMworker.peerRspFp[TSDB_MSG_TYPE_MD_DROP_TABLE_RSP] = mnodeProcessDropChildTableRsp;
//   tsMworker.msgFp[TSDB_MSG_TYPE_MD_ALTER_TABLE_RSP] = mnodeDispatchToPeerRspQueue;
//   tsMworker.peerRspFp[TSDB_MSG_TYPE_MD_ALTER_TABLE_RSP] = mnodeProcessAlterTableRsp;

//   tsMworker.msgFp[TSDB_MSG_TYPE_MD_CREATE_VNODE_RSP] = mnodeDispatchToPeerRspQueue;
//   tsMworker.peerRspFp[TSDB_MSG_TYPE_MD_CREATE_VNODE_RSP] = mnodeProcessCreateVnodeRsp;
//   tsMworker.msgFp[TSDB_MSG_TYPE_MD_ALTER_VNODE_RSP] = mnodeDispatchToPeerRspQueue;
//   tsMworker.peerRspFp[TSDB_MSG_TYPE_MD_ALTER_VNODE_RSP] = mnodeProcessAlterVnodeRsp;
//   tsMworker.msgFp[TSDB_MSG_TYPE_MD_COMPACT_VNODE_RSP] = mnodeDispatchToPeerRspQueue;
//   tsMworker.peerRspFp[TSDB_MSG_TYPE_MD_COMPACT_VNODE_RSP] = mnodeProcessCompactVnodeRsp;
//   tsMworker.msgFp[TSDB_MSG_TYPE_MD_DROP_VNODE_RSP] = mnodeDispatchToPeerRspQueue;
//   tsMworker.peerRspFp[TSDB_MSG_TYPE_MD_DROP_VNODE_RSP] = mnodeProcessDropVnodeRsp;

//   // read msg
//   tsMworker.msgFp[TSDB_MSG_TYPE_CM_HEARTBEAT] = mnodeDispatchToReadQueue;
//   tsMworker.readMsgFp[TSDB_MSG_TYPE_CM_HEARTBEAT] = mnodeProcessHeartBeatMsg;
//   tsMworker.msgFp[TSDB_MSG_TYPE_CM_CONNECT] = mnodeDispatchToReadQueue;
//   tsMworker.readMsgFp[TSDB_MSG_TYPE_CM_CONNECT] = mnodeProcessConnectMsg;
//   tsMworker.msgFp[TSDB_MSG_TYPE_CM_USE_DB] = mnodeDispatchToReadQueue;
//   tsMworker.readMsgFp[TSDB_MSG_TYPE_CM_USE_DB] = mnodeProcessUseMsg;

//   tsMworker.msgFp[TSDB_MSG_TYPE_CM_TABLE_META] = mnodeDispatchToReadQueue;
//   tsMworker.readMsgFp[TSDB_MSG_TYPE_CM_TABLE_META] = mnodeProcessTableMetaMsg;
//   tsMworker.msgFp[TSDB_MSG_TYPE_CM_TABLES_META] = mnodeDispatchToReadQueue;
//   tsMworker.readMsgFp[TSDB_MSG_TYPE_CM_TABLES_META] = mnodeProcessMultiTableMetaMsg;
//   tsMworker.msgFp[TSDB_MSG_TYPE_CM_STABLE_VGROUP] = mnodeDispatchToReadQueue;
//   tsMworker.readMsgFp[TSDB_MSG_TYPE_CM_STABLE_VGROUP] = mnodeProcessSuperTableVgroupMsg;

//   tsMworker.msgFp[TSDB_MSG_TYPE_CM_SHOW] = mnodeDispatchToReadQueue;
//   tsMworker.readMsgFp[TSDB_MSG_TYPE_CM_SHOW] = mnodeProcessShowMsg;
//   tsMworker.msgFp[TSDB_MSG_TYPE_CM_RETRIEVE] = mnodeDispatchToReadQueue;
//   tsMworker.readMsgFp[TSDB_MSG_TYPE_CM_RETRIEVE] = mnodeProcessRetrieveMsg;
//   tsMworker.msgFp[TSDB_MSG_TYPE_CM_RETRIEVE_FUNC] = mnodeDispatchToReadQueue;
//   tsMworker.readMsgFp[TSDB_MSG_TYPE_CM_RETRIEVE_FUNC] = mnodeProcessRetrieveFuncReq;

//   // tsMworker.msgFp[TSDB_MSG_TYPE_CM_CREATE_ACCT] = mnodeDispatchToWriteQueue;
//   // tsMworker.readMsgFp[TSDB_MSG_TYPE_CM_CREATE_ACCT] = acctProcessCreateAcctMsg;
//   // tsMworker.msgFp[TSDB_MSG_TYPE_CM_ALTER_ACCT] = mnodeDispatchToWriteQueue;
//   // tsMworker.readMsgFp[TSDB_MSG_TYPE_CM_ALTER_ACCT] = acctProcessDropAcctMsg;
//   // tsMworker.msgFp[TSDB_MSG_TYPE_CM_DROP_ACCT] = mnodeDispatchToWriteQueue;
//   // tsMworker.readMsgFp[TSDB_MSG_TYPE_CM_DROP_ACCT] = acctProcessAlterAcctMsg;

//   // write msg
//   tsMworker.msgFp[TSDB_MSG_TYPE_CM_CREATE_USER] = mnodeDispatchToWriteQueue;
//   tsMworker.writeMsgFp[TSDB_MSG_TYPE_CM_CREATE_USER] = mnodeProcessCreateUserMsg;
//   tsMworker.msgFp[TSDB_MSG_TYPE_CM_ALTER_USER] = mnodeDispatchToWriteQueue;
//   tsMworker.writeMsgFp[TSDB_MSG_TYPE_CM_ALTER_USER] = mnodeProcessAlterUserMsg;
//   tsMworker.msgFp[TSDB_MSG_TYPE_CM_DROP_USER] = mnodeDispatchToWriteQueue;
//   tsMworker.writeMsgFp[TSDB_MSG_TYPE_CM_DROP_USER] = mnodeProcessDropUserMsg;

//   tsMworker.msgFp[TSDB_MSG_TYPE_CM_CREATE_DNODE] = mnodeDispatchToWriteQueue;
//   tsMworker.writeMsgFp[TSDB_MSG_TYPE_CM_CREATE_DNODE] = mnodeProcessCreateDnodeMsg;
//   tsMworker.msgFp[TSDB_MSG_TYPE_CM_DROP_DNODE] = mnodeDispatchToWriteQueue;
//   tsMworker.writeMsgFp[TSDB_MSG_TYPE_CM_DROP_DNODE] = mnodeProcessDropDnodeMsg;
//   tsMworker.msgFp[TSDB_MSG_TYPE_CM_CONFIG_DNODE] = mnodeDispatchToWriteQueue;
//   tsMworker.writeMsgFp[TSDB_MSG_TYPE_CM_CONFIG_DNODE] = mnodeProcessCfgDnodeMsg;

//   tsMworker.msgFp[TSDB_MSG_TYPE_CM_CREATE_DB] = mnodeDispatchToWriteQueue;
//   tsMworker.writeMsgFp[TSDB_MSG_TYPE_CM_CREATE_DB] = mnodeProcessCreateDbMsg;
//   tsMworker.msgFp[TSDB_MSG_TYPE_CM_ALTER_DB] = mnodeDispatchToWriteQueue;
//   tsMworker.writeMsgFp[TSDB_MSG_TYPE_CM_ALTER_DB] = mnodeProcessAlterDbMsg;
//   tsMworker.msgFp[TSDB_MSG_TYPE_CM_DROP_DB] = mnodeDispatchToWriteQueue;
//   tsMworker.writeMsgFp[TSDB_MSG_TYPE_CM_DROP_DB] = mnodeProcessDropDbMsg;
//   tsMworker.msgFp[TSDB_MSG_TYPE_CM_SYNC_DB] = mnodeDispatchToWriteQueue;
//   tsMworker.writeMsgFp[TSDB_MSG_TYPE_CM_SYNC_DB] = mnodeProcessSyncDbMsg;
//   tsMworker.msgFp[TSDB_MSG_TYPE_CM_COMPACT_VNODE] = mnodeDispatchToWriteQueue;
//   tsMworker.writeMsgFp[TSDB_MSG_TYPE_CM_COMPACT_VNODE] = mnodeProcessCompactMsg;

//   tsMworker.msgFp[TSDB_MSG_TYPE_CM_CREATE_FUNCTION] = mnodeDispatchToWriteQueue;
//   tsMworker.writeMsgFp[TSDB_MSG_TYPE_CM_CREATE_FUNCTION] = mnodeProcessCreateFuncMsg;
//   tsMworker.msgFp[TSDB_MSG_TYPE_CM_DROP_FUNCTION] = mnodeDispatchToWriteQueue;
//   tsMworker.writeMsgFp[TSDB_MSG_TYPE_CM_DROP_FUNCTION] = mnodeProcessDropFuncMsg;

//   // tsMworker.msgFp[TSDB_MSG_TYPE_CM_CREATE_TP] = mnodeDispatchToWriteQueue;
//   // tsMworker.readMsgFp[TSDB_MSG_TYPE_CM_CREATE_TP] = tpProcessCreateTpMsg;
//   // tsMworker.msgFp[TSDB_MSG_TYPE_CM_DROP_TP] = mnodeDispatchToWriteQueue;
//   // tsMworker.readMsgFp[TSDB_MSG_TYPE_CM_DROP_TP] = tpProcessAlterTpMsg;
//   // tsMworker.msgFp[TSDB_MSG_TYPE_CM_ALTER_TP] = mnodeDispatchToWriteQueue;
//   // tsMworker.readMsgFp[TSDB_MSG_TYPE_CM_ALTER_TP] = tpProcessDropTpMsg;

//   tsMworker.msgFp[TSDB_MSG_TYPE_CM_CREATE_TABLE] = mnodeDispatchToWriteQueue;
//   tsMworker.writeMsgFp[TSDB_MSG_TYPE_CM_CREATE_TABLE] = mnodeProcessCreateTableMsg;
//   tsMworker.msgFp[TSDB_MSG_TYPE_CM_DROP_TABLE] = mnodeDispatchToWriteQueue;
//   tsMworker.writeMsgFp[TSDB_MSG_TYPE_CM_DROP_TABLE] = mnodeProcessDropTableMsg;
//   tsMworker.msgFp[TSDB_MSG_TYPE_CM_ALTER_TABLE] = mnodeDispatchToWriteQueue;
//   tsMworker.writeMsgFp[TSDB_MSG_TYPE_CM_ALTER_TABLE] = mnodeProcessAlterTableMsg;

//   tsMworker.msgFp[TSDB_MSG_TYPE_CM_ALTER_STREAM] = mnodeDispatchToWriteQueue;
//   tsMworker.writeMsgFp[TSDB_MSG_TYPE_CM_ALTER_STREAM] = NULL;
//   tsMworker.msgFp[TSDB_MSG_TYPE_CM_KILL_QUERY] = mnodeDispatchToWriteQueue;
//   tsMworker.writeMsgFp[TSDB_MSG_TYPE_CM_KILL_QUERY] = mnodeProcessKillQueryMsg;
//   tsMworker.msgFp[TSDB_MSG_TYPE_CM_KILL_STREAM] = mnodeDispatchToWriteQueue;
//   tsMworker.writeMsgFp[TSDB_MSG_TYPE_CM_KILL_STREAM] = mnodeProcessKillStreamMsg;
//   tsMworker.msgFp[TSDB_MSG_TYPE_CM_KILL_CONN] = mnodeDispatchToWriteQueue;
//   tsMworker.writeMsgFp[TSDB_MSG_TYPE_CM_KILL_CONN] = mnodeProcessKillConnectionMsg;
}

static int32_t mnodeProcessWriteReq(void *unused, SMnMsg *pMsg, int32_t qtype) {
  int32_t msgType = pMsg->rpcMsg.msgType;
  void *ahandle = pMsg->rpcMsg.ahandle;

  if (pMsg->rpcMsg.pCont == NULL) {
    mError("msg:%p, app:%p type:%s content is null", pMsg, ahandle, taosMsg[msgType]);
    return TSDB_CODE_MND_INVALID_MSG_LEN;
  }

  if (!mnodeIsMaster()) {
    SMnRsp *rpcRsp = &pMsg->rpcRsp;
    SRpcEpSet *epSet = rpcMallocCont(sizeof(SRpcEpSet));
    mnodeGetMnodeEpSetForShell(epSet, true);
    rpcRsp->rsp = epSet;
    rpcRsp->len = sizeof(SRpcEpSet);

    mDebug("msg:%p, app:%p type:%s in write queue, is redirected, numOfEps:%d inUse:%d", pMsg, ahandle,
           taosMsg[msgType], epSet->numOfEps, epSet->inUse);

    return TSDB_CODE_RPC_REDIRECT;
  }

  if (tsMworker.writeMsgFp[msgType] == NULL) {
    mError("msg:%p, app:%p type:%s not processed", pMsg, ahandle, taosMsg[msgType]);
    return TSDB_CODE_MND_MSG_NOT_PROCESSED;
  }

  return (*tsMworker.writeMsgFp[msgType])(pMsg);
}

static int32_t mnodeProcessReadReq(void* unused, SMnMsg *pMsg, int32_t qtype) {
  int32_t msgType = pMsg->rpcMsg.msgType;
  void *ahandle = pMsg->rpcMsg.ahandle;

  if (pMsg->rpcMsg.pCont == NULL) {
    mError("msg:%p, app:%p type:%s in mread queue, content is null", pMsg, ahandle, taosMsg[msgType]);
    return TSDB_CODE_MND_INVALID_MSG_LEN;
  }

  if (!mnodeIsMaster()) {
    SMnRsp *rpcRsp = &pMsg->rpcRsp;
    SRpcEpSet *epSet = rpcMallocCont(sizeof(SRpcEpSet));
    if (!epSet) {
      return TSDB_CODE_MND_OUT_OF_MEMORY;
    }
    mnodeGetMnodeEpSetForShell(epSet, true);
    rpcRsp->rsp = epSet;
    rpcRsp->len = sizeof(SRpcEpSet);

    mDebug("msg:%p, app:%p type:%s in mread queue is redirected, numOfEps:%d inUse:%d", pMsg, ahandle, taosMsg[msgType],
           epSet->numOfEps, epSet->inUse);
    return TSDB_CODE_RPC_REDIRECT;
  }

  if (tsMworker.readMsgFp[msgType] == NULL) {
    mError("msg:%p, app:%p type:%s in mread queue, not processed", pMsg, ahandle, taosMsg[msgType]);
    return TSDB_CODE_MND_MSG_NOT_PROCESSED;
  }

  mTrace("msg:%p, app:%p type:%s will be processed in mread queue", pMsg, ahandle, taosMsg[msgType]);
  return (*tsMworker.readMsgFp[msgType])(pMsg);
}

static int32_t mnodeProcessPeerReq(void *unused, SMnMsg *pMsg, int32_t qtype) {
  int32_t msgType = pMsg->rpcMsg.msgType;
  void *  ahandle = pMsg->rpcMsg.ahandle;

  if (pMsg->rpcMsg.pCont == NULL) {
    mError("msg:%p, ahandle:%p type:%s in mpeer queue, content is null", pMsg, ahandle, taosMsg[msgType]);
    return TSDB_CODE_MND_INVALID_MSG_LEN;
  }

  if (!mnodeIsMaster()) {
    SMnRsp *rpcRsp = &pMsg->rpcRsp;
    SRpcEpSet *epSet = rpcMallocCont(sizeof(SRpcEpSet));
    mnodeGetMnodeEpSetForPeer(epSet, true);
    rpcRsp->rsp = epSet;
    rpcRsp->len = sizeof(SRpcEpSet);

    mDebug("msg:%p, ahandle:%p type:%s in mpeer queue is redirected, numOfEps:%d inUse:%d", pMsg, ahandle,
           taosMsg[msgType], epSet->numOfEps, epSet->inUse);

    return TSDB_CODE_RPC_REDIRECT;
  }

  if (tsMworker.peerReqFp[msgType] == NULL) {
    mError("msg:%p, ahandle:%p type:%s in mpeer queue, not processed", pMsg, ahandle, taosMsg[msgType]);
    return TSDB_CODE_MND_MSG_NOT_PROCESSED;
  }

  return (*tsMworker.peerReqFp[msgType])(pMsg);
}

static int32_t mnodeProcessPeerRsp(void *ahandle, SMnMsg *pMsg, int32_t qtype) {
  int32_t  msgType = pMsg->rpcMsg.msgType;
  SRpcMsg *pRpcMsg = &pMsg->rpcMsg;

  if (!mnodeIsMaster()) {
    mError("msg:%p, ahandle:%p type:%s not processed for not master", pRpcMsg, pRpcMsg->ahandle, taosMsg[msgType]);
    return 0;
  }

  if (tsMworker.peerRspFp[msgType]) {
    (*tsMworker.peerRspFp[msgType])(pRpcMsg);
  } else {
    mError("msg:%p, ahandle:%p type:%s is not processed", pRpcMsg, pRpcMsg->ahandle, taosMsg[msgType]);
  }

  return 0;
}

int32_t mnodeInitWorker() {
  mnodeInitMsgFp();

  SWorkerPool *pPool = &tsMworker.write;
  pPool->name = "mnode-write";
  pPool->startFp = (ProcessStartFp)mnodeProcessWriteReq;
  pPool->endFp = (ProcessEndFp)mnodeSendRpcRsp;
  pPool->min = 1;
  pPool->max = 1;
  if (tWorkerInit(pPool) != 0) {
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  } else {
    tsMworker.writeQ = tWorkerAllocQueue(pPool, NULL);
  }

  pPool = &tsMworker.read;
  pPool->name = "mnode-read";
  pPool->startFp = (ProcessStartFp)mnodeProcessReadReq;
  pPool->endFp = (ProcessEndFp)mnodeSendRpcRsp;
  pPool->min = 2;
  pPool->max = (int32_t)(tsNumOfCores * tsNumOfThreadsPerCore / 2);
  pPool->max = MAX(2, pPool->max);
  pPool->max = MIN(4, pPool->max);
  if (tWorkerInit(pPool) != 0) {
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  } else {
    tsMworker.readQ = tWorkerAllocQueue(pPool, NULL);
  }

  pPool = &tsMworker.peerReq;
  pPool->name = "mnode-peer-req";
  pPool->startFp = (ProcessStartFp)mnodeProcessPeerReq;
  pPool->endFp = (ProcessEndFp)mnodeSendRpcRsp;
  pPool->min = 1;
  pPool->max = 1;
  if (tWorkerInit(pPool) != 0) {
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  } else {
    tsMworker.peerReqQ = tWorkerAllocQueue(pPool, NULL);
  }

  pPool = &tsMworker.peerRsp;
  pPool->name = "mnode-peer-rsp";
  pPool->startFp = (ProcessStartFp)mnodeProcessPeerRsp;
  pPool->endFp = (ProcessEndFp)mnodeProcessPeerRspEnd;
  pPool->min = 1;
  pPool->max = 1;
  if (tWorkerInit(pPool) != 0) {
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  } else {
    tsMworker.peerRspQ = tWorkerAllocQueue(pPool, NULL);
  }

  mInfo("mnode worker is initialized");
  return 0;
}

void mnodeCleanupWorker() {
  tWorkerFreeQueue(&tsMworker.write, tsMworker.writeQ);
  tWorkerCleanup(&tsMworker.write);
  tsMworker.writeQ = NULL;

  tWorkerFreeQueue(&tsMworker.read, tsMworker.readQ);
  tWorkerCleanup(&tsMworker.read);
  tsMworker.readQ = NULL;

  tWorkerFreeQueue(&tsMworker.peerReq, tsMworker.peerReqQ);
  tWorkerCleanup(&tsMworker.peerReq);
  tsMworker.peerReqQ = NULL;

  tWorkerFreeQueue(&tsMworker.peerRsp, tsMworker.peerRspQ);
  tWorkerCleanup(&tsMworker.peerRsp);
  tsMworker.peerRspQ = NULL;

  mInfo("mnode worker is closed");
}

void mnodeProcessMsg(SRpcMsg *pMsg) {
  if (tsMworker.msgFp[pMsg->msgType]) {
    (*tsMworker.msgFp[pMsg->msgType])(pMsg);
  } else {
    assert(0);
  }
}
