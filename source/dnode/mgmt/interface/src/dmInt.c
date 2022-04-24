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
#include "dmInt.h"

const char *dmStatName(EDndRunStatus status) {
  switch (status) {
    case DND_STAT_INIT:
      return "init";
    case DND_STAT_RUNNING:
      return "running";
    case DND_STAT_STOPPED:
      return "stopped";
    default:
      return "UNKNOWN";
  }
}

const char *dmLogName(EDndNodeType ntype) {
  switch (ntype) {
    case VNODE:
      return "vnode";
    case QNODE:
      return "qnode";
    case SNODE:
      return "snode";
    case MNODE:
      return "mnode";
    case BNODE:
      return "bnode";
    default:
      return "taosd";
  }
}

const char *dmProcName(EDndNodeType ntype) {
  switch (ntype) {
    case VNODE:
      return "taosv";
    case QNODE:
      return "taosq";
    case SNODE:
      return "taoss";
    case MNODE:
      return "taosm";
    case BNODE:
      return "taosb";
    default:
      return "taosd";
  }
}

const char *dmEventName(EDndEvent ev) {
  switch (ev) {
    case DND_EVENT_START:
      return "start";
    case DND_EVENT_STOP:
      return "stop";
    case DND_EVENT_CHILD:
      return "child";
    default:
      return "UNKNOWN";
  }
}

void dmSetStatus(SDnode *pDnode, EDndRunStatus status) {
  if (pDnode->status != status) {
    dDebug("dnode status set from %s to %s", dmStatName(pDnode->status), dmStatName(status));
    pDnode->status = status;
  }
}

void dmSetEvent(SDnode *pDnode, EDndEvent event) {
  if (event == DND_EVENT_STOP) {
    pDnode->event = event;
  }
}

void dmSetMsgHandle(SMgmtWrapper *pWrapper, tmsg_t msgType, NodeMsgFp nodeMsgFp, int8_t vgId) {
  pWrapper->msgFps[TMSG_INDEX(msgType)] = nodeMsgFp;
  pWrapper->msgVgIds[TMSG_INDEX(msgType)] = vgId;
}

SMgmtWrapper *dmAcquireWrapper(SDnode *pDnode, EDndNodeType ntype) {
  SMgmtWrapper *pWrapper = &pDnode->wrappers[ntype];
  SMgmtWrapper *pRetWrapper = pWrapper;

  taosRLockLatch(&pWrapper->latch);
  if (pWrapper->deployed) {
    int32_t refCount = atomic_add_fetch_32(&pWrapper->refCount, 1);
    dTrace("node:%s, is acquired, refCount:%d", pWrapper->name, refCount);
  } else {
    terrno = TSDB_CODE_NODE_NOT_DEPLOYED;
    pRetWrapper = NULL;
  }
  taosRUnLockLatch(&pWrapper->latch);

  return pRetWrapper;
}

int32_t dmMarkWrapper(SMgmtWrapper *pWrapper) {
  int32_t code = 0;

  taosRLockLatch(&pWrapper->latch);
  if (pWrapper->deployed || (pWrapper->procType == DND_PROC_PARENT && pWrapper->required)) {
    int32_t refCount = atomic_add_fetch_32(&pWrapper->refCount, 1);
    dTrace("node:%s, is marked, refCount:%d", pWrapper->name, refCount);
  } else {
    terrno = TSDB_CODE_NODE_NOT_DEPLOYED;
    code = -1;
  }
  taosRUnLockLatch(&pWrapper->latch);

  return code;
}

void dmReleaseWrapper(SMgmtWrapper *pWrapper) {
  if (pWrapper == NULL) return;

  taosRLockLatch(&pWrapper->latch);
  int32_t refCount = atomic_sub_fetch_32(&pWrapper->refCount, 1);
  taosRUnLockLatch(&pWrapper->latch);
  dTrace("node:%s, is released, refCount:%d", pWrapper->name, refCount);
}

void dmReportStartup(SDnode *pDnode, const char *pName, const char *pDesc) {
  SStartupInfo *pStartup = &pDnode->startup;
  tstrncpy(pStartup->name, pName, TSDB_STEP_NAME_LEN);
  tstrncpy(pStartup->desc, pDesc, TSDB_STEP_DESC_LEN);
  dInfo("step:%s, %s", pStartup->name, pStartup->desc);
}

void dmReportStartupByWrapper(SMgmtWrapper *pWrapper, const char *pName, const char *pDesc) {
  dmReportStartup(pWrapper->pDnode, pName, pDesc);
}

static void dmGetServerStatus(SDnode *pDnode, SServerStatusRsp *pStatus) {
  pStatus->details[0] = 0;

  if (pDnode->status == DND_STAT_INIT) {
    pStatus->statusCode = TSDB_SRV_STATUS_NETWORK_OK;
    snprintf(pStatus->details, sizeof(pStatus->details), "%s: %s", pDnode->startup.name, pDnode->startup.desc);
  } else if (pDnode->status == DND_STAT_STOPPED) {
    pStatus->statusCode = TSDB_SRV_STATUS_EXTING;
  } else {
    SDnodeData *pData = &pDnode->data;
    if (pData->isMnode && pData->mndState != TAOS_SYNC_STATE_LEADER && pData->mndState == TAOS_SYNC_STATE_FOLLOWER) {
      pStatus->statusCode = TSDB_SRV_STATUS_SERVICE_DEGRADED;
      snprintf(pStatus->details, sizeof(pStatus->details), "mnode sync state is %s", syncStr(pData->mndState));
    } else if (pData->unsyncedVgId != 0 && pData->vndState != TAOS_SYNC_STATE_LEADER &&
               pData->vndState != TAOS_SYNC_STATE_FOLLOWER) {
      pStatus->statusCode = TSDB_SRV_STATUS_SERVICE_DEGRADED;
      snprintf(pStatus->details, sizeof(pStatus->details), "vnode:%d sync state is %s", pData->unsyncedVgId,
               syncStr(pData->vndState));
    } else {
      pStatus->statusCode = TSDB_SRV_STATUS_SERVICE_OK;
    }
  }
}

void dmProcessNettestReq(SDnode *pDnode, SRpcMsg *pRpc) {
  dDebug("net test req is received");
  SRpcMsg rsp = {.handle = pRpc->handle, .ahandle = pRpc->ahandle, .code = 0};
  rsp.pCont = rpcMallocCont(pRpc->contLen);
  rsp.contLen = pRpc->contLen;
  rpcSendResponse(&rsp);
}

void dmProcessServerStatusReq(SDnode *pDnode, SRpcMsg *pReq) {
  dDebug("server status req is received");

  SServerStatusRsp statusRsp = {0};
  dmGetServerStatus(pDnode, &statusRsp);

  SRpcMsg rspMsg = {.handle = pReq->handle, .ahandle = pReq->ahandle};
  int32_t rspLen = tSerializeSServerStatusRsp(NULL, 0, &statusRsp);
  if (rspLen < 0) {
    rspMsg.code = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  void *pRsp = rpcMallocCont(rspLen);
  if (pRsp == NULL) {
    rspMsg.code = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  tSerializeSServerStatusRsp(pRsp, rspLen, &statusRsp);
  rspMsg.pCont = pRsp;
  rspMsg.contLen = rspLen;

_OVER:
  rpcSendResponse(&rspMsg);
}

void dmGetMonitorSysInfo(SMonSysInfo *pInfo) {
  taosGetCpuUsage(&pInfo->cpu_engine, &pInfo->cpu_system);
  taosGetCpuCores(&pInfo->cpu_cores);
  taosGetProcMemory(&pInfo->mem_engine);
  taosGetSysMemory(&pInfo->mem_system);
  pInfo->mem_total = tsTotalMemoryKB;
  pInfo->disk_engine = 0;
  pInfo->disk_used = tsDataSpace.size.used;
  pInfo->disk_total = tsDataSpace.size.total;
  taosGetCardInfoDelta(&pInfo->net_in, &pInfo->net_out);
  taosGetProcIODelta(&pInfo->io_read, &pInfo->io_write, &pInfo->io_read_disk, &pInfo->io_write_disk);
}
