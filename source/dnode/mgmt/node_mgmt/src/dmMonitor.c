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
#include "dmNodes.h"
#include "audit.h"

static void dmGetMonitorBasicInfo(SDnode *pDnode, SMonBasicInfo *pInfo) {
  pInfo->protocol = 1;
  pInfo->dnode_id = pDnode->data.dnodeId;
  pInfo->cluster_id = pDnode->data.clusterId;
  tstrncpy(pInfo->dnode_ep, tsLocalEp, TSDB_EP_LEN);
}

static void dmGetMonitorBasicInfoBasic(SDnode *pDnode, SMonBasicInfo *pInfo) {
  pInfo->protocol = 2;
  pInfo->dnode_id = pDnode->data.dnodeId;
  pInfo->cluster_id = pDnode->data.clusterId;
  tstrncpy(pInfo->dnode_ep, tsLocalEp, TSDB_EP_LEN);
}

static void dmGetMonitorDnodeInfo(SDnode *pDnode, SMonDnodeInfo *pInfo) {
  //pInfo->uptime = (taosGetTimestampMs() - pDnode->data.rebootTime) / (86400000.0f);
  pInfo->uptime = (taosGetTimestampMs() - pDnode->data.rebootTime) /1000.0f;
  pInfo->has_mnode = pDnode->wrappers[MNODE].required;
  pInfo->has_qnode = pDnode->wrappers[QNODE].required;
  pInfo->has_snode = pDnode->wrappers[SNODE].required;
  tstrncpy(pInfo->logdir.name, tsLogDir, sizeof(pInfo->logdir.name));
  pInfo->logdir.size = tsLogSpace.size;
  tstrncpy(pInfo->tempdir.name, tsTempDir, sizeof(pInfo->tempdir.name));
  pInfo->tempdir.size = tsTempSpace.size;
}

static void dmGetDmMonitorInfo(SDnode *pDnode) {
  SMonDmInfo dmInfo = {0};
  dmGetMonitorBasicInfo(pDnode, &dmInfo.basic);
  dmGetMonitorDnodeInfo(pDnode, &dmInfo.dnode);
  dmGetMonitorSystemInfo(&dmInfo.sys);
  monSetDmInfo(&dmInfo);
}

static void dmGetDmMonitorInfoBasic(SDnode *pDnode) {
  SMonDmInfo dmInfo = {0};
  dmGetMonitorBasicInfoBasic(pDnode, &dmInfo.basic);
  monSetDmInfo(&dmInfo);
}

static void dmGetMmMonitorInfo(SDnode *pDnode) {
  SMgmtWrapper *pWrapper = &pDnode->wrappers[MNODE];
  if (dmMarkWrapper(pWrapper) == 0) {
    SMonMmInfo mmInfo = {0};
    if (pWrapper->pMgmt != NULL) {
      mmGetMonitorInfo(pWrapper->pMgmt, &mmInfo);
    }
    dmReleaseWrapper(pWrapper);
    monSetMmInfo(&mmInfo);
    tFreeSMonMmInfo(&mmInfo);
  }
}

static void dmGetVmMonitorInfo(SDnode *pDnode) {
  SMgmtWrapper *pWrapper = &pDnode->wrappers[VNODE];
  if (dmMarkWrapper(pWrapper) == 0) {
    SMonVmInfo vmInfo = {0};
    if (pWrapper->pMgmt != NULL) {
      vmGetMonitorInfo(pWrapper->pMgmt, &vmInfo);
    }
    dmReleaseWrapper(pWrapper);
    monSetVmInfo(&vmInfo);
    tFreeSMonVmInfo(&vmInfo);
  }
}

static void dmGetQmMonitorInfo(SDnode *pDnode) {
  SMgmtWrapper *pWrapper = &pDnode->wrappers[QNODE];
  if (dmMarkWrapper(pWrapper) == 0) {
    SMonQmInfo qmInfo = {0};
    if (pWrapper->pMgmt != NULL) {
      qmGetMonitorInfo(pWrapper->pMgmt, &qmInfo);
    }
    dmReleaseWrapper(pWrapper);
    monSetQmInfo(&qmInfo);
    tFreeSMonQmInfo(&qmInfo);
  }
}

static void dmGetSmMonitorInfo(SDnode *pDnode) {
  SMgmtWrapper *pWrapper = &pDnode->wrappers[SNODE];
  if (dmMarkWrapper(pWrapper) == 0) {
    SMonSmInfo smInfo = {0};
    if (pWrapper->pMgmt != NULL) {
      smGetMonitorInfo(pWrapper->pMgmt, &smInfo);
    }
    dmReleaseWrapper(pWrapper);
    monSetSmInfo(&smInfo);
    tFreeSMonSmInfo(&smInfo);
  }
}

void dmSendMonitorReport() {
  if (!tsEnableMonitor || tsMonitorFqdn[0] == 0 || tsMonitorPort == 0) return;
  dTrace("send monitor report to %s:%u", tsMonitorFqdn, tsMonitorPort);

  SDnode *pDnode = dmInstance();
  dmGetDmMonitorInfo(pDnode);
  dmGetMmMonitorInfo(pDnode);
  dmGetVmMonitorInfo(pDnode);
  dmGetQmMonitorInfo(pDnode);
  dmGetSmMonitorInfo(pDnode);
  monGenAndSendReport();
}

void dmSendMonitorReportBasic() {
  if (!tsEnableMonitor || tsMonitorFqdn[0] == 0 || tsMonitorPort == 0) return;
  dTrace("send monitor report to %s:%u", tsMonitorFqdn, tsMonitorPort);

  SDnode *pDnode = dmInstance();
  dmGetDmMonitorInfoBasic(pDnode);
  dmGetMmMonitorInfo(pDnode);
  monGenAndSendReportBasic();
}

//Todo: put this in seperate file in the future
void dmSendAuditRecords() {
  auditSendRecordsInBatch();
}

void dmGetVnodeLoads(SMonVloadInfo *pInfo) {
  SDnode       *pDnode = dmInstance();
  SMgmtWrapper *pWrapper = &pDnode->wrappers[VNODE];
  if (dmMarkWrapper(pWrapper) == 0) {
    if (pWrapper->pMgmt != NULL) {
      vmGetVnodeLoads(pWrapper->pMgmt, pInfo, false);
    }
    dmReleaseWrapper(pWrapper);
  }
}

void dmGetVnodeLoadsLite(SMonVloadInfo *pInfo) {
  SDnode       *pDnode = dmInstance();
  SMgmtWrapper *pWrapper = &pDnode->wrappers[VNODE];
  if (dmMarkWrapper(pWrapper) == 0) {
    if (pWrapper->pMgmt != NULL) {
      vmGetVnodeLoadsLite(pWrapper->pMgmt, pInfo);
    }
    dmReleaseWrapper(pWrapper);
  }
}

void dmGetMnodeLoads(SMonMloadInfo *pInfo) {
  SDnode       *pDnode = dmInstance();
  SMgmtWrapper *pWrapper = &pDnode->wrappers[MNODE];
  if (dmMarkWrapper(pWrapper) == 0) {
    if (pWrapper->pMgmt != NULL) {
      mmGetMnodeLoads(pWrapper->pMgmt, pInfo);
    }
    dmReleaseWrapper(pWrapper);
  }
}

void dmGetQnodeLoads(SQnodeLoad *pInfo) {
  SDnode       *pDnode = dmInstance();
  SMgmtWrapper *pWrapper = &pDnode->wrappers[QNODE];
  if (dmMarkWrapper(pWrapper) == 0) {
    if (pWrapper->pMgmt != NULL) {
      qmGetQnodeLoads(pWrapper->pMgmt, pInfo);
    }
    dmReleaseWrapper(pWrapper);
  }
}
