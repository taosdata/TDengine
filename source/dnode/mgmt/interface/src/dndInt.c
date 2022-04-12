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
#include "dndInt.h"

const char *dndStatName(EDndRunStatus status) {
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

const char *dndLogName(EDndNodeType ntype) {
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

const char *dndProcName(EDndNodeType ntype) {
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

const char *dndEventName(EDndEvent ev) {
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

EDndRunStatus dndGetStatus(SDnode *pDnode) { return pDnode->status; }

void dndSetStatus(SDnode *pDnode, EDndRunStatus status) {
  if (pDnode->status != status) {
    dDebug("dnode status set from %s to %s", dndStatName(pDnode->status), dndStatName(status));
    pDnode->status = status;
  }
}

void dndSetEvent(SDnode *pDnode, EDndEvent event) {
  if (event == DND_EVENT_STOP) {
    pDnode->event = event;
  }
}

void dndSetMsgHandle(SMgmtWrapper *pWrapper, tmsg_t msgType, NodeMsgFp nodeMsgFp, int8_t vgId) {
  pWrapper->msgFps[TMSG_INDEX(msgType)] = nodeMsgFp;
  pWrapper->msgVgIds[TMSG_INDEX(msgType)] = vgId;
}

SMgmtWrapper *dndAcquireWrapper(SDnode *pDnode, EDndNodeType ntype) {
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

int32_t dndMarkWrapper(SMgmtWrapper *pWrapper) {
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

void dndReleaseWrapper(SMgmtWrapper *pWrapper) {
  if (pWrapper == NULL) return;

  taosRLockLatch(&pWrapper->latch);
  int32_t refCount = atomic_sub_fetch_32(&pWrapper->refCount, 1);
  taosRUnLockLatch(&pWrapper->latch);
  dTrace("node:%s, is released, refCount:%d", pWrapper->name, refCount);
}

void dndReportStartup(SDnode *pDnode, const char *pName, const char *pDesc) {
  SStartupReq *pStartup = &pDnode->startup;
  tstrncpy(pStartup->name, pName, TSDB_STEP_NAME_LEN);
  tstrncpy(pStartup->desc, pDesc, TSDB_STEP_DESC_LEN);
  pStartup->finished = 0;
}

static void dndGetStartup(SDnode *pDnode, SStartupReq *pStartup) {
  memcpy(pStartup, &pDnode->startup, sizeof(SStartupReq));
  pStartup->finished = (dndGetStatus(pDnode) == DND_STAT_RUNNING);
}

void dndProcessStartupReq(SDnode *pDnode, SRpcMsg *pReq) {
  dDebug("startup req is received");
  SStartupReq *pStartup = rpcMallocCont(sizeof(SStartupReq));
  dndGetStartup(pDnode, pStartup);

  dDebug("startup req is sent, step:%s desc:%s finished:%d", pStartup->name, pStartup->desc, pStartup->finished);
  SRpcMsg rpcRsp = {
      .handle = pReq->handle, .pCont = pStartup, .contLen = sizeof(SStartupReq), .ahandle = pReq->ahandle};
  rpcSendResponse(&rpcRsp);
}

void dndGetMonitorSysInfo(SMonSysInfo *pInfo) {
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

SMsgCb dndCreateMsgcb(SMgmtWrapper *pWrapper) {
  SMsgCb msgCb = pWrapper->pDnode->data.msgCb;
  msgCb.pWrapper = pWrapper;
  return msgCb;
}