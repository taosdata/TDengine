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

static void dmGetMonitorBasicInfo(SDnode *pDnode, SMonBasicInfo *pInfo) {
  pInfo->protocol = 1;
  pInfo->dnode_id = pDnode->dnodeId;
  pInfo->cluster_id = pDnode->clusterId;
  tstrncpy(pInfo->dnode_ep, tsLocalEp, TSDB_EP_LEN);
}

static void dmGetMonitorDnodeInfo(SDnode *pDnode, SMonDnodeInfo *pInfo) {
  pInfo->uptime = (taosGetTimestampMs() - pDnode->rebootTime) / (86400000.0f);
  SMgmtWrapper *pWrapper = dndAcquireWrapper(pDnode, MNODE);
  if (pWrapper != NULL) {
    pInfo->has_mnode = pWrapper->required;
    dndReleaseWrapper(pWrapper);
  }
  tstrncpy(pInfo->logdir.name, tsLogDir, sizeof(pInfo->logdir.name));
  pInfo->logdir.size = tsLogSpace.size;
  tstrncpy(pInfo->tempdir.name, tsTempDir, sizeof(pInfo->tempdir.name));
  pInfo->tempdir.size = tsTempSpace.size;
}

static void dmGetMonitorInfo(SDnode *pDnode, SMonDmInfo *pInfo) {
  dmGetMonitorBasicInfo(pDnode, &pInfo->basic);
  dmGetMonitorSysInfo(&pInfo->sys);
  dmGetMonitorDnodeInfo(pDnode, &pInfo->dnode);
}

void dmSendMonitorReport(SDnode *pDnode) {
  if (!tsEnableMonitor || tsMonitorFqdn[0] == 0 || tsMonitorPort == 0) return;
  dTrace("send monitor report to %s:%u", tsMonitorFqdn, tsMonitorPort);

  SMonDmInfo dmInfo = {0};
  SMonMmInfo mmInfo = {0};
  SMonVmInfo vmInfo = {0};
  SMonQmInfo qmInfo = {0};
  SMonSmInfo smInfo = {0};
  SMonBmInfo bmInfo = {0};

  SRpcMsg req = {0};
  SRpcMsg rsp;
  SEpSet  epset = {.inUse = 0, .numOfEps = 1};
  tstrncpy(epset.eps[0].fqdn, tsLocalFqdn, TSDB_FQDN_LEN);
  epset.eps[0].port = tsServerPort;

  dmGetMonitorInfo(pDnode, &dmInfo);

  SMgmtWrapper *pWrapper = dndAcquireWrapper(pDnode, MNODE);
  if (pWrapper != NULL) {
    if (!tsMultiProcess) {
      mmGetMonitorInfo(pWrapper, &mmInfo);
    } else {
      req.msgType = TDMT_MON_MM_INFO;
      dndSendRecv(pDnode, &epset, &req, &rsp);
      tDeserializeSMonMmInfo(rsp.pCont, rsp.contLen, &mmInfo);
      rpcFreeCont(rsp.pCont);
    }
    dndReleaseWrapper(pWrapper);
  }

  pWrapper = dndAcquireWrapper(pDnode, VNODES);
  if (pWrapper != NULL) {
    if (!tsMultiProcess) {
      vmGetMonitorInfo(pWrapper, &vmInfo);
    } else {
      req.msgType = TDMT_MON_VM_INFO;
      dndSendRecv(pDnode, &epset, &req, &rsp);
      dndReleaseWrapper(pWrapper);
      tDeserializeSMonVmInfo(rsp.pCont, rsp.contLen, &vmInfo);
      rpcFreeCont(rsp.pCont);
    }
  }

  pWrapper = dndAcquireWrapper(pDnode, QNODE);
  if (pWrapper != NULL) {
    if (!tsMultiProcess) {
      qmGetMonitorInfo(pWrapper, &qmInfo);
    } else {
      req.msgType = TDMT_MON_QM_INFO;
      dndSendRecv(pDnode, &epset, &req, &rsp);
      dndReleaseWrapper(pWrapper);
      tDeserializeSMonQmInfo(rsp.pCont, rsp.contLen, &qmInfo);
      rpcFreeCont(rsp.pCont);
    }
    dndReleaseWrapper(pWrapper);
  }

  pWrapper = dndAcquireWrapper(pDnode, SNODE);
  if (pWrapper != NULL) {
    if (!tsMultiProcess) {
      smGetMonitorInfo(pWrapper, &smInfo);
    } else {
      req.msgType = TDMT_MON_SM_INFO;
      dndSendRecv(pDnode, &epset, &req, &rsp);
      dndReleaseWrapper(pWrapper);
      tDeserializeSMonSmInfo(rsp.pCont, rsp.contLen, &smInfo);
      rpcFreeCont(rsp.pCont);
    }
    dndReleaseWrapper(pWrapper);
  }

  pWrapper = dndAcquireWrapper(pDnode, BNODE);
  if (pWrapper != NULL) {
    if (!tsMultiProcess) {
      bmGetMonitorInfo(pWrapper, &bmInfo);
    } else {
      req.msgType = TDMT_MON_BM_INFO;
      dndSendRecv(pDnode, &epset, &req, &rsp);
      dndReleaseWrapper(pWrapper);
      tDeserializeSMonBmInfo(rsp.pCont, rsp.contLen, &bmInfo);
      rpcFreeCont(rsp.pCont);
    }
    dndReleaseWrapper(pWrapper);
  }

  monSetDmInfo(&dmInfo);
  monSetMmInfo(&mmInfo);
  monSetVmInfo(&vmInfo);
  monSetQmInfo(&qmInfo);
  monSetSmInfo(&smInfo);
  monSetBmInfo(&bmInfo);
  tFreeSMonMmInfo(&mmInfo);
  tFreeSMonVmInfo(&vmInfo);
  tFreeSMonQmInfo(&qmInfo);
  tFreeSMonSmInfo(&smInfo);
  tFreeSMonBmInfo(&bmInfo);
  monSendReport();
}

int32_t dmProcessGetMonMmInfoReq(SDnodeMgmt *pMgmt, SNodeMsg *pReq) {
  SMgmtWrapper *pWrapper = dndAcquireWrapper(pMgmt->pDnode, MNODE);
  if (pWrapper == NULL) return -1;

  SMonMmInfo mmInfo = {0};
  mmGetMonitorInfo(pWrapper, &mmInfo);
  dndReleaseWrapper(pWrapper);

  int32_t rspLen = tSerializeSMonMmInfo(NULL, 0, &mmInfo);
  if (rspLen < 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  void *pRsp = rpcMallocCont(rspLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  tSerializeSMonMmInfo(pRsp, rspLen, &mmInfo);
  pReq->pRsp = pRsp;
  pReq->rspLen = rspLen;
  tFreeSMonMmInfo(&mmInfo);
  return 0;
}

int32_t dmProcessGetMonVmInfoReq(SDnodeMgmt *pMgmt, SNodeMsg *pReq) {
  SMgmtWrapper *pWrapper = dndAcquireWrapper(pMgmt->pDnode, VNODES);
  if (pWrapper == NULL) return -1;

  SMonVmInfo vmInfo = {0};
  vmGetMonitorInfo(pWrapper, &vmInfo);
  dndReleaseWrapper(pWrapper);

  int32_t rspLen = tSerializeSMonVmInfo(NULL, 0, &vmInfo);
  if (rspLen < 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  void *pRsp = rpcMallocCont(rspLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  tSerializeSMonVmInfo(pRsp, rspLen, &vmInfo);
  pReq->pRsp = pRsp;
  pReq->rspLen = rspLen;
  tFreeSMonVmInfo(&vmInfo);
  return 0;
}

int32_t dmProcessGetMonQmInfoReq(SDnodeMgmt *pMgmt, SNodeMsg *pReq) {
  SMgmtWrapper *pWrapper = dndAcquireWrapper(pMgmt->pDnode, QNODE);
  if (pWrapper == NULL) return -1;

  SMonQmInfo qmInfo = {0};
  qmGetMonitorInfo(pWrapper, &qmInfo);
  dndReleaseWrapper(pWrapper);

  int32_t rspLen = tSerializeSMonQmInfo(NULL, 0, &qmInfo);
  if (rspLen < 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  void *pRsp = rpcMallocCont(rspLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  tSerializeSMonQmInfo(pRsp, rspLen, &qmInfo);
  pReq->pRsp = pRsp;
  pReq->rspLen = rspLen;
  tFreeSMonQmInfo(&qmInfo);
  return 0;
}

int32_t dmProcessGetMonSmInfoReq(SDnodeMgmt *pMgmt, SNodeMsg *pReq) {
  SMgmtWrapper *pWrapper = dndAcquireWrapper(pMgmt->pDnode, SNODE);
  if (pWrapper == NULL) return -1;

  SMonSmInfo smInfo = {0};
  smGetMonitorInfo(pWrapper, &smInfo);
  dndReleaseWrapper(pWrapper);

  int32_t rspLen = tSerializeSMonSmInfo(NULL, 0, &smInfo);
  if (rspLen < 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  void *pRsp = rpcMallocCont(rspLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  tSerializeSMonSmInfo(pRsp, rspLen, &smInfo);
  pReq->pRsp = pRsp;
  pReq->rspLen = rspLen;
  tFreeSMonSmInfo(&smInfo);
  return 0;
}

int32_t dmProcessGetMonBmInfoReq(SDnodeMgmt *pMgmt, SNodeMsg *pReq) {
  SMgmtWrapper *pWrapper = dndAcquireWrapper(pMgmt->pDnode, BNODE);
  if (pWrapper == NULL) return -1;

  SMonBmInfo bmInfo = {0};
  bmGetMonitorInfo(pWrapper, &bmInfo);
  dndReleaseWrapper(pWrapper);

  int32_t rspLen = tSerializeSMonBmInfo(NULL, 0, &bmInfo);
  if (rspLen < 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  void *pRsp = rpcMallocCont(rspLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  tSerializeSMonBmInfo(pRsp, rspLen, &bmInfo);
  pReq->pRsp = pRsp;
  pReq->rspLen = rspLen;
  tFreeSMonBmInfo(&bmInfo);
  return 0;
}

int32_t dmProcessGetVnodeLoadsReq(SDnodeMgmt *pMgmt, SNodeMsg *pReq) {
  SMgmtWrapper *pWrapper = dndAcquireWrapper(pMgmt->pDnode, VNODES);
  if (pWrapper == NULL) return -1;

  SMonVloadInfo vloads = {0};
  vmGetVnodeLoads(pWrapper, &vloads);
  dndReleaseWrapper(pWrapper);

  int32_t rspLen = tSerializeSMonVloadInfo(NULL, 0, &vloads);
  if (rspLen < 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  void *pRsp = rpcMallocCont(rspLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  tSerializeSMonVloadInfo(pRsp, rspLen, &vloads);
  pReq->pRsp = pRsp;
  pReq->rspLen = rspLen;
  tFreeSMonVloadInfo(&vloads);
  return 0;
}

void dmGetVnodeLoads(SMgmtWrapper *pWrapper, SMonVloadInfo *pInfo) {
  if (!tsMultiProcess) {
    vmGetVnodeLoads(pWrapper, pInfo);
  } else {
    SRpcMsg req = {.msgType = TDMT_MON_VM_LOAD};
    SRpcMsg rsp = {0};
    SEpSet  epset = {.inUse = 0, .numOfEps = 1};
    tstrncpy(epset.eps[0].fqdn, tsLocalFqdn, TSDB_FQDN_LEN);
    epset.eps[0].port = tsServerPort;

    dndSendRecv(pWrapper->pDnode, &epset, &req, &rsp);
    if (rsp.code == 0) {
      tDeserializeSMonVloadInfo(rsp.pCont, rsp.contLen, pInfo);
    }
    rpcFreeCont(rsp.pCont);
  }
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
