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
#include "dmImp.h"

static void dmGetMonitorBasicInfo(SDnode *pDnode, SMonBasicInfo *pInfo) {
  pInfo->protocol = 1;
  pInfo->dnode_id = pDnode->data.dnodeId;
  pInfo->cluster_id = pDnode->data.clusterId;
  tstrncpy(pInfo->dnode_ep, tsLocalEp, TSDB_EP_LEN);
}

static void dmGetMonitorDnodeInfo(SDnode *pDnode, SMonDnodeInfo *pInfo) {
  pInfo->uptime = (taosGetTimestampMs() - pDnode->data.rebootTime) / (86400000.0f);
  pInfo->has_mnode = pDnode->wrappers[MNODE].required;
  pInfo->has_qnode = pDnode->wrappers[QNODE].required;
  pInfo->has_snode = pDnode->wrappers[SNODE].required;
  pInfo->has_bnode = pDnode->wrappers[BNODE].required;
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
  tstrncpy(epset.eps[0].fqdn, pDnode->data.localFqdn, TSDB_FQDN_LEN);
  epset.eps[0].port = tsServerPort;

  SMgmtWrapper *pWrapper = NULL;
  dmGetMonitorInfo(pDnode, &dmInfo);

  bool getFromAPI = !tsMultiProcess;
  pWrapper = &pDnode->wrappers[MNODE];
  if (getFromAPI) {
    if (dmMarkWrapper(pWrapper) == 0) {
      mmGetMonitorInfo(pWrapper, &mmInfo);
      dmReleaseWrapper(pWrapper);
    }
  } else {
    if (pWrapper->required) {
      req.msgType = TDMT_MON_MM_INFO;
      dmSendRecv(pDnode, &epset, &req, &rsp);
      if (rsp.code == 0 && rsp.contLen > 0) {
        tDeserializeSMonMmInfo(rsp.pCont, rsp.contLen, &mmInfo);
      }
      rpcFreeCont(rsp.pCont);
    }
  }

  pWrapper = &pDnode->wrappers[VNODE];
  if (getFromAPI) {
    if (dmMarkWrapper(pWrapper) == 0) {
      vmGetMonitorInfo(pWrapper, &vmInfo);
      dmReleaseWrapper(pWrapper);
    }
  } else {
    if (pWrapper->required) {
      req.msgType = TDMT_MON_VM_INFO;
      dmSendRecv(pDnode, &epset, &req, &rsp);
      if (rsp.code == 0 && rsp.contLen > 0) {
        tDeserializeSMonVmInfo(rsp.pCont, rsp.contLen, &vmInfo);
      }
      rpcFreeCont(rsp.pCont);
    }
  }

  pWrapper = &pDnode->wrappers[QNODE];
  if (getFromAPI) {
    if (dmMarkWrapper(pWrapper) == 0) {
      qmGetMonitorInfo(pWrapper, &qmInfo);
      dmReleaseWrapper(pWrapper);
    }
  } else {
    if (pWrapper->required) {
      req.msgType = TDMT_MON_QM_INFO;
      dmSendRecv(pDnode, &epset, &req, &rsp);
      if (rsp.code == 0 && rsp.contLen > 0) {
        tDeserializeSMonQmInfo(rsp.pCont, rsp.contLen, &qmInfo);
      }
      rpcFreeCont(rsp.pCont);
    }
  }

  pWrapper = &pDnode->wrappers[SNODE];
  if (getFromAPI) {
    if (dmMarkWrapper(pWrapper) == 0) {
      smGetMonitorInfo(pWrapper, &smInfo);
      dmReleaseWrapper(pWrapper);
    }
  } else {
    if (pWrapper->required) {
      req.msgType = TDMT_MON_SM_INFO;
      dmSendRecv(pDnode, &epset, &req, &rsp);
      if (rsp.code == 0 && rsp.contLen > 0) {
        tDeserializeSMonSmInfo(rsp.pCont, rsp.contLen, &smInfo);
      }
      rpcFreeCont(rsp.pCont);
    }
  }

  pWrapper = &pDnode->wrappers[BNODE];
  if (getFromAPI) {
    if (dmMarkWrapper(pWrapper) == 0) {
      bmGetMonitorInfo(pWrapper, &bmInfo);
      dmReleaseWrapper(pWrapper);
    }
  } else {
    if (pWrapper->required) {
      req.msgType = TDMT_MON_BM_INFO;
      dmSendRecv(pDnode, &epset, &req, &rsp);
      if (rsp.code == 0 && rsp.contLen > 0) {
        tDeserializeSMonBmInfo(rsp.pCont, rsp.contLen, &bmInfo);
      }
      rpcFreeCont(rsp.pCont);
    }
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

void dmGetVnodeLoads(SDnode *pDnode, SMonVloadInfo *pInfo) {
  SMgmtWrapper *pWrapper = dmAcquireWrapper(pDnode, VNODE);
  if (pWrapper == NULL) return;

  bool getFromAPI = !tsMultiProcess;
  if (getFromAPI) {
    vmGetVnodeLoads(pWrapper, pInfo);
  } else {
    SRpcMsg req = {.msgType = TDMT_MON_VM_LOAD};
    SRpcMsg rsp = {0};
    SEpSet  epset = {.inUse = 0, .numOfEps = 1};
    tstrncpy(epset.eps[0].fqdn, pDnode->data.localFqdn, TSDB_FQDN_LEN);
    epset.eps[0].port = tsServerPort;

    dmSendRecv(pDnode, &epset, &req, &rsp);
    if (rsp.code == 0 && rsp.contLen > 0) {
      tDeserializeSMonVloadInfo(rsp.pCont, rsp.contLen, pInfo);
    }
    rpcFreeCont(rsp.pCont);
  }
  dmReleaseWrapper(pWrapper);
}

void dmGetMnodeLoads(SDnode *pDnode, SMonMloadInfo *pInfo) {
  SMgmtWrapper *pWrapper = dmAcquireWrapper(pDnode, MNODE);
  if (pWrapper == NULL) {
    pInfo->isMnode = 0;
    return;
  }

  bool getFromAPI = !tsMultiProcess;
  if (getFromAPI) {
    mmGetMnodeLoads(pWrapper, pInfo);
  } else {
    SRpcMsg req = {.msgType = TDMT_MON_MM_LOAD};
    SRpcMsg rsp = {0};
    SEpSet  epset = {.inUse = 0, .numOfEps = 1};
    tstrncpy(epset.eps[0].fqdn, pDnode->data.localFqdn, TSDB_FQDN_LEN);
    epset.eps[0].port = tsServerPort;

    dmSendRecv(pDnode, &epset, &req, &rsp);
    if (rsp.code == 0 && rsp.contLen > 0) {
      tDeserializeSMonMloadInfo(rsp.pCont, rsp.contLen, pInfo);
    }
    rpcFreeCont(rsp.pCont);
  }
  dmReleaseWrapper(pWrapper);
}
