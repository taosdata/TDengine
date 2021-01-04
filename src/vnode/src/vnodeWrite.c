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
#include "taosmsg.h"
#include "taoserror.h"
#include "tglobal.h"
#include "tqueue.h"
#include "ttimer.h"
#include "dnode.h"
#include "vnodeStatus.h"

#define MAX_QUEUED_MSG_NUM 10000

extern void *  tsDnodeTmr;
static int32_t (*vnodeProcessWriteMsgFp[TSDB_MSG_TYPE_MAX])(SVnodeObj *, void *pCont, SRspRet *);
static int32_t vnodeProcessSubmitMsg(SVnodeObj *pVnode, void *pCont, SRspRet *);
static int32_t vnodeProcessCreateTableMsg(SVnodeObj *pVnode, void *pCont, SRspRet *);
static int32_t vnodeProcessDropTableMsg(SVnodeObj *pVnode, void *pCont, SRspRet *);
static int32_t vnodeProcessAlterTableMsg(SVnodeObj *pVnode, void *pCont, SRspRet *);
static int32_t vnodeProcessDropStableMsg(SVnodeObj *pVnode, void *pCont, SRspRet *);
static int32_t vnodeProcessUpdateTagValMsg(SVnodeObj *pVnode, void *pCont, SRspRet *);
static int32_t vnodePerformFlowCtrl(SVWriteMsg *pWrite);

int32_t vnodeInitWrite(void) {
  vnodeProcessWriteMsgFp[TSDB_MSG_TYPE_SUBMIT]          = vnodeProcessSubmitMsg;
  vnodeProcessWriteMsgFp[TSDB_MSG_TYPE_MD_CREATE_TABLE] = vnodeProcessCreateTableMsg;
  vnodeProcessWriteMsgFp[TSDB_MSG_TYPE_MD_DROP_TABLE]   = vnodeProcessDropTableMsg;
  vnodeProcessWriteMsgFp[TSDB_MSG_TYPE_MD_ALTER_TABLE]  = vnodeProcessAlterTableMsg;
  vnodeProcessWriteMsgFp[TSDB_MSG_TYPE_MD_DROP_STABLE]  = vnodeProcessDropStableMsg;
  vnodeProcessWriteMsgFp[TSDB_MSG_TYPE_UPDATE_TAG_VAL]  = vnodeProcessUpdateTagValMsg;

  return 0;
}

void vnodeCleanupWrite() {}

int32_t vnodeProcessWrite(void *vparam, void *wparam, int32_t qtype, void *rparam) {
  int32_t    code = 0;
  SVnodeObj *pVnode = vparam;
  SWalHead * pHead = wparam;
  SRspRet *  pRspRet = rparam;

  if (vnodeProcessWriteMsgFp[pHead->msgType] == NULL) {
    vError("vgId:%d, msg:%s not processed since no handle, qtype:%s hver:%" PRIu64, pVnode->vgId,
           taosMsg[pHead->msgType], qtypeStr[qtype], pHead->version);
    return TSDB_CODE_VND_MSG_NOT_PROCESSED;
  }

  vTrace("vgId:%d, msg:%s will be processed in vnode, qtype:%s hver:%" PRIu64 " vver:%" PRIu64, pVnode->vgId,
         taosMsg[pHead->msgType], qtypeStr[qtype], pHead->version, pVnode->version);

  if (pHead->version == 0) {  // from client or CQ
    if (!vnodeInReadyStatus(pVnode)) {
      vDebug("vgId:%d, msg:%s not processed since vstatus:%d, qtype:%s hver:%" PRIu64, pVnode->vgId,
             taosMsg[pHead->msgType], pVnode->status, qtypeStr[qtype], pHead->version);
      return TSDB_CODE_APP_NOT_READY;  // it may be in deleting or closing state
    }

    if (pVnode->role != TAOS_SYNC_ROLE_MASTER) {
      vDebug("vgId:%d, msg:%s not processed since replica:%d role:%s, qtype:%s hver:%" PRIu64, pVnode->vgId,
             taosMsg[pHead->msgType], pVnode->syncCfg.replica, syncRole[pVnode->role], qtypeStr[qtype], pHead->version);
      return TSDB_CODE_APP_NOT_READY;
    }

    // assign version
    pHead->version = pVnode->version + 1;
  } else {  // from wal or forward
    // for data from WAL or forward, version may be smaller
    if (pHead->version <= pVnode->version) return 0;
  }

  // forward to peers, even it is WAL/FWD, it shall be called to update version in sync
  int32_t syncCode = 0;
  syncCode = syncForwardToPeer(pVnode->sync, pHead, pRspRet, qtype);
  if (syncCode < 0) return syncCode;

  // write into WAL
  code = walWrite(pVnode->wal, pHead);
  if (code < 0) return code;

  pVnode->version = pHead->version;

  // write data locally
  code = (*vnodeProcessWriteMsgFp[pHead->msgType])(pVnode, pHead->cont, pRspRet);
  if (code < 0) return code;

  return syncCode;
}

static int32_t vnodeCheckWrite(SVnodeObj *pVnode) {
  if (!(pVnode->accessState & TSDB_VN_WRITE_ACCCESS)) {
    vDebug("vgId:%d, no write auth, refCount:%d pVnode:%p", pVnode->vgId, pVnode->refCount, pVnode);
    return TSDB_CODE_VND_NO_WRITE_AUTH;
  }

  if (pVnode->dbReplica != pVnode->syncCfg.replica &&
      pVnode->syncCfg.nodeInfo[pVnode->syncCfg.replica - 1].nodeId == dnodeGetDnodeId()) {
    vDebug("vgId:%d, vnode is balancing and will be dropped, dbReplica:%d vgReplica:%d, refCount:%d pVnode:%p",
           pVnode->vgId, pVnode->dbReplica, pVnode->syncCfg.replica, pVnode->refCount, pVnode);
    return TSDB_CODE_VND_IS_BALANCING;
  }

  // tsdb may be in reset state
  if (pVnode->tsdb == NULL) {
    vDebug("vgId:%d, tsdb is null, refCount:%d pVnode:%p", pVnode->vgId, pVnode->refCount, pVnode);
    return TSDB_CODE_APP_NOT_READY;
  }

  if (pVnode->isFull) {
    vDebug("vgId:%d, vnode is full, refCount:%d", pVnode->vgId, pVnode->refCount);
    return TSDB_CODE_VND_IS_FULL;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t vnodeProcessSubmitMsg(SVnodeObj *pVnode, void *pCont, SRspRet *pRet) {
  int32_t code = TSDB_CODE_SUCCESS;

  vTrace("vgId:%d, submit msg is processed", pVnode->vgId);

  // save insert result into item
  SShellSubmitRspMsg *pRsp = NULL;
  if (pRet) {
    pRet->len = sizeof(SShellSubmitRspMsg);
    pRet->rsp = rpcMallocCont(pRet->len);
    pRsp = pRet->rsp;
  }

  if (tsdbInsertData(pVnode->tsdb, pCont, pRsp) < 0) code = terrno;

  return code;
}

static int32_t vnodeProcessCreateTableMsg(SVnodeObj *pVnode, void *pCont, SRspRet *pRet) {
  int code = TSDB_CODE_SUCCESS;

  STableCfg *pCfg = tsdbCreateTableCfgFromMsg((SMDCreateTableMsg *)pCont);
  if (pCfg == NULL) {
    ASSERT(terrno != 0);
    return terrno;
  }

  if (tsdbCreateTable(pVnode->tsdb, pCfg) < 0) {
    code = terrno;
    ASSERT(code != 0);
  }

  tsdbClearTableCfg(pCfg);
  return code;
}

static int32_t vnodeProcessDropTableMsg(SVnodeObj *pVnode, void *pCont, SRspRet *pRet) {
  SMDDropTableMsg *pTable = pCont;
  int32_t          code = TSDB_CODE_SUCCESS;

  vDebug("vgId:%d, table:%s, start to drop", pVnode->vgId, pTable->tableId);
  STableId tableId = {.uid = htobe64(pTable->uid), .tid = htonl(pTable->tid)};

  if (tsdbDropTable(pVnode->tsdb, tableId) < 0) code = terrno;

  return code;
}

static int32_t vnodeProcessAlterTableMsg(SVnodeObj *pVnode, void *pCont, SRspRet *pRet) {
  // TODO: disposed in tsdb
  // STableCfg *pCfg = tsdbCreateTableCfgFromMsg((SMDCreateTableMsg *)pCont);
  // if (pCfg == NULL) return terrno;
  // if (tsdbCreateTable(pVnode->tsdb, pCfg) < 0) code = terrno;

  // tsdbClearTableCfg(pCfg);
  vDebug("vgId:%d, alter table msg is received", pVnode->vgId);
  return TSDB_CODE_SUCCESS;
}

static int32_t vnodeProcessDropStableMsg(SVnodeObj *pVnode, void *pCont, SRspRet *pRet) {
  SDropSTableMsg *pTable = pCont;
  int32_t         code = TSDB_CODE_SUCCESS;

  vDebug("vgId:%d, stable:%s, start to drop", pVnode->vgId, pTable->tableId);

  STableId stableId = {.uid = htobe64(pTable->uid), .tid = -1};

  if (tsdbDropTable(pVnode->tsdb, stableId) < 0) code = terrno;

  vDebug("vgId:%d, stable:%s, drop stable result:%s", pVnode->vgId, pTable->tableId, tstrerror(code));

  return code;
}

static int32_t vnodeProcessUpdateTagValMsg(SVnodeObj *pVnode, void *pCont, SRspRet *pRet) {
  if (tsdbUpdateTableTagValue(pVnode->tsdb, (SUpdateTableTagValMsg *)pCont) < 0) {
    return terrno;
  }
  return TSDB_CODE_SUCCESS;
}

static SVWriteMsg *vnodeBuildVWriteMsg(SVnodeObj *pVnode, SWalHead *pHead, int32_t qtype, SRpcMsg *pRpcMsg) {
  if (pHead->len > TSDB_MAX_WAL_SIZE) {
    vError("vgId:%d, wal len:%d exceeds limit, hver:%" PRIu64, pVnode->vgId, pHead->len, pHead->version);
    terrno = TSDB_CODE_WAL_SIZE_LIMIT;
    return NULL;
  }

  int32_t size = sizeof(SVWriteMsg) + sizeof(SWalHead) + pHead->len;
  SVWriteMsg *pWrite = taosAllocateQitem(size);
  if (pWrite == NULL) {
    terrno = TSDB_CODE_VND_OUT_OF_MEMORY;
    return NULL;
  }

  if (pRpcMsg != NULL) {
    pWrite->rpcMsg = *pRpcMsg;
  }

  memcpy(pWrite->pHead, pHead, sizeof(SWalHead) + pHead->len);
  pWrite->pVnode = pVnode;
  pWrite->qtype = qtype;

  atomic_add_fetch_32(&pVnode->refCount, 1);

  return pWrite;
}

static int32_t vnodeWriteToWQueueImp(SVWriteMsg *pWrite) {
  SVnodeObj *pVnode = pWrite->pVnode;

  if (pWrite->qtype == TAOS_QTYPE_RPC) {
    int32_t code = vnodeCheckWrite(pVnode);
    if (code != TSDB_CODE_SUCCESS) {
      taosFreeQitem(pWrite);
      vnodeRelease(pVnode);
      return code;
    }
  }

  if (!vnodeInReadyStatus(pVnode)) {
    vDebug("vgId:%d, vnode status is %s, refCount:%d pVnode:%p", pVnode->vgId, vnodeStatus[pVnode->status],
           pVnode->refCount, pVnode);
    taosFreeQitem(pWrite);
    vnodeRelease(pVnode);       
    return TSDB_CODE_APP_NOT_READY;
  }

  int32_t queued = atomic_add_fetch_32(&pVnode->queuedWMsg, 1);
  if (queued > MAX_QUEUED_MSG_NUM) {
    int32_t ms = (queued / MAX_QUEUED_MSG_NUM) * 10 + 3;
    if (ms > 100) ms = 100;
    vDebug("vgId:%d, too many msg:%d in vwqueue, flow control %dms", pVnode->vgId, queued, ms);
    taosMsleep(ms);
  }

  vTrace("vgId:%d, write into vwqueue, refCount:%d queued:%d", pVnode->vgId, pVnode->refCount, pVnode->queuedWMsg);

  taosWriteQitem(pVnode->wqueue, pWrite->qtype, pWrite);
  return TSDB_CODE_SUCCESS;
}

int32_t vnodeWriteToWQueue(void *vparam, void *wparam, int32_t qtype, void *rparam) {
  SVWriteMsg *pWrite = vnodeBuildVWriteMsg(vparam, wparam, qtype, rparam);
  if (pWrite == NULL) {
    assert(terrno != 0);
    return terrno;
  }

  int32_t code = vnodePerformFlowCtrl(pWrite);
  if (code != 0) return 0;

  return vnodeWriteToWQueueImp(pWrite);
}

void vnodeFreeFromWQueue(void *vparam, SVWriteMsg *pWrite) {
  SVnodeObj *pVnode = vparam;

  int32_t queued = atomic_sub_fetch_32(&pVnode->queuedWMsg, 1);
  vTrace("vgId:%d, msg:%p, app:%p, free from vwqueue, queued:%d", pVnode->vgId, pWrite, pWrite->rpcMsg.ahandle, queued);

  taosFreeQitem(pWrite);
  vnodeRelease(pVnode);
}

static void vnodeFlowCtrlMsgToWQueue(void *param, void *tmrId) {
  SVWriteMsg *pWrite = param;
  SVnodeObj * pVnode = pWrite->pVnode;
  int32_t     code = TSDB_CODE_VND_IS_SYNCING;

  if (pVnode->flowctrlLevel <= 0) code = TSDB_CODE_VND_IS_FLOWCTRL;

  pWrite->processedCount++;
  if (pWrite->processedCount > 100) {
    vError("vgId:%d, msg:%p, failed to process since %s, retry:%d", pVnode->vgId, pWrite, tstrerror(code),
           pWrite->processedCount);
    pWrite->processedCount = 1;
    dnodeSendRpcVWriteRsp(pWrite->pVnode, pWrite, code);
  } else {
    code = vnodePerformFlowCtrl(pWrite);
    if (code == 0) {
      vDebug("vgId:%d, msg:%p, write into vwqueue after flowctrl, retry:%d", pVnode->vgId, pWrite,
             pWrite->processedCount);
      pWrite->processedCount = 0;
      code = vnodeWriteToWQueueImp(pWrite);
      if (code != 0) {
        dnodeSendRpcVWriteRsp(pWrite->pVnode, pWrite, code);
      }
    }
  }
}

static int32_t vnodePerformFlowCtrl(SVWriteMsg *pWrite) {
  SVnodeObj *pVnode = pWrite->pVnode;
  if (pWrite->qtype != TAOS_QTYPE_RPC) return 0;
  if (pVnode->queuedWMsg < MAX_QUEUED_MSG_NUM && pVnode->flowctrlLevel <= 0) return 0;

  if (tsEnableFlowCtrl == 0) {
    int32_t ms = pow(2, pVnode->flowctrlLevel + 2);
    if (ms > 100) ms = 100;
    vTrace("vgId:%d, msg:%p, app:%p, perform flowctrl for %d ms", pVnode->vgId, pWrite, pWrite->rpcMsg.ahandle, ms);
    taosMsleep(ms);
    return 0;
  } else {
    void *unUsed = NULL;
    taosTmrReset(vnodeFlowCtrlMsgToWQueue, 100, pWrite, tsDnodeTmr, &unUsed);

    vTrace("vgId:%d, msg:%p, app:%p, perform flowctrl, retry:%d", pVnode->vgId, pWrite, pWrite->rpcMsg.ahandle,
           pWrite->processedCount);
    return TSDB_CODE_VND_ACTION_IN_PROGRESS;
  }
}

void vnodeWaitWriteCompleted(void *pVnode) {}