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

#define dmSendLocalRecv(pMgmt, mtype, func, pInfo)           \
  if (!tsMultiProcess) {                                     \
    SRpcMsg rsp = {0};                                       \
    SRpcMsg req = {.msgType = mtype};                        \
    SEpSet  epset = {.inUse = 0, .numOfEps = 1};             \
    tstrncpy(epset.eps[0].fqdn, tsLocalFqdn, TSDB_FQDN_LEN); \
    epset.eps[0].port = tsServerPort;                        \
    rpcSendRecv(pMgmt->msgCb.clientRpc, &epset, &req, &rsp); \
    if (rsp.code == 0 && rsp.contLen > 0) {                  \
      func(rsp.pCont, rsp.contLen, pInfo);                   \
    }                                                        \
    rpcFreeCont(rsp.pCont);                                  \
  }

static void dmGetMonitorBasicInfo(SDnodeMgmt *pMgmt, SMonBasicInfo *pInfo) {
  pInfo->protocol = 1;
  pInfo->dnode_id = pMgmt->pData->dnodeId;
  pInfo->cluster_id = pMgmt->pData->clusterId;
  tstrncpy(pInfo->dnode_ep, tsLocalEp, TSDB_EP_LEN);
}

static void dmGetMonitorDnodeInfo(SDnodeMgmt *pMgmt, SMonDnodeInfo *pInfo) {
  pInfo->uptime = (taosGetTimestampMs() - pMgmt->pData->rebootTime) / (86400000.0f);
  pInfo->has_mnode = (*pMgmt->isNodeRequiredFp)(MNODE);
  pInfo->has_qnode = (*pMgmt->isNodeRequiredFp)(QNODE);
  pInfo->has_snode = (*pMgmt->isNodeRequiredFp)(SNODE);
  pInfo->has_bnode = (*pMgmt->isNodeRequiredFp)(BNODE);
  tstrncpy(pInfo->logdir.name, tsLogDir, sizeof(pInfo->logdir.name));
  pInfo->logdir.size = tsLogSpace.size;
  tstrncpy(pInfo->tempdir.name, tsTempDir, sizeof(pInfo->tempdir.name));
  pInfo->tempdir.size = tsTempSpace.size;
}

static void dmGetMonitorInfo(SDnodeMgmt *pMgmt, SMonDmInfo *pInfo) {
  dmGetMonitorBasicInfo(pMgmt, &pInfo->basic);
  dmGetMonitorDnodeInfo(pMgmt, &pInfo->dnode);
  dmGetMonitorSystemInfo(&pInfo->sys);
}

void dmSendMonitorReport(SDnodeMgmt *pMgmt) {
  if (!tsEnableMonitor || tsMonitorFqdn[0] == 0 || tsMonitorPort == 0) return;
  dTrace("send monitor report to %s:%u", tsMonitorFqdn, tsMonitorPort);

  SMonDmInfo dmInfo = {0};
  SMonMmInfo mmInfo = {0};
  SMonVmInfo vmInfo = {0};
  SMonQmInfo qmInfo = {0};
  SMonSmInfo smInfo = {0};
  SMonBmInfo bmInfo = {0};

  dmGetMonitorInfo(pMgmt, &dmInfo);
  dmSendLocalRecv(pMgmt, TDMT_MON_VM_INFO, tDeserializeSMonVmInfo, &vmInfo);
  if (dmInfo.dnode.has_mnode) {
    dmSendLocalRecv(pMgmt, TDMT_MON_MM_INFO, tDeserializeSMonMmInfo, &mmInfo);
  }
  if (dmInfo.dnode.has_qnode) {
    dmSendLocalRecv(pMgmt, TDMT_MON_QM_INFO, tDeserializeSMonQmInfo, &qmInfo);
  }
  if (dmInfo.dnode.has_snode) {
    dmSendLocalRecv(pMgmt, TDMT_MON_SM_INFO, tDeserializeSMonSmInfo, &smInfo);
  }
  if (dmInfo.dnode.has_bnode) {
    dmSendLocalRecv(pMgmt, TDMT_MON_BM_INFO, tDeserializeSMonBmInfo, &bmInfo);
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

void dmGetVnodeLoads(SDnodeMgmt *pMgmt, SMonVloadInfo *pInfo) {
  dmSendLocalRecv(pMgmt, TDMT_MON_VM_LOAD, tDeserializeSMonVloadInfo, pInfo);
}

void dmGetMnodeLoads(SDnodeMgmt *pMgmt, SMonMloadInfo *pInfo) {
  dmSendLocalRecv(pMgmt, TDMT_MON_MM_LOAD, tDeserializeSMonMloadInfo, pInfo);
}
