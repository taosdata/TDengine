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
#include "metrics.h"

static void collectDnodeMetricsInfo(SDnode *pDnode) {
  SRawDnodeMetrics rawMetrics = {0};

  rawMetrics.rpcQueueMemoryAllowed = tsQueueMemoryAllowed;
  rawMetrics.rpcQueueMemoryUsed = atomic_load_64(&tsQueueMemoryUsed);
  rawMetrics.applyMemoryAllowed = tsApplyMemoryAllowed;
  rawMetrics.applyMemoryUsed = atomic_load_64(&tsApplyMemoryUsed);

  int32_t code = addDnodeMetrics(&rawMetrics, dmGetClusterId(), pDnode->data.dnodeId, tsLocalEp);
  if (code != TSDB_CODE_SUCCESS) {
    dError("Failed to add dnode metrics, code: %d", code);
  }
}

static void collectWriteMetricsInfo(SDnode *pDnode) {
  SMgmtWrapper *pWrapper = &pDnode->wrappers[VNODE];
  if (dmMarkWrapper(pWrapper) == 0) {
    if (pWrapper->pMgmt != NULL) {
      vmUpdateMetricsInfo(pWrapper->pMgmt, dmGetClusterId());
    }
    dmReleaseWrapper(pWrapper);
  }
}

void dmSendMetricsReport() {
  if (!tsEnableMonitor || tsMonitorFqdn[0] == 0 || tsMonitorPort == 0 || !tsEnableMetrics) {
    return;
  }
  SDnode *pDnode = dmInstance();
  if (pDnode == NULL) {
    return;
  }

  collectDnodeMetricsInfo(pDnode);
  collectWriteMetricsInfo(pDnode);

  // monitorfw automatically handles metrics reporting
}

void dmCleanExpiredMetrics(SDnode *pDnode) {
  SMgmtWrapper *pWrapper = &pDnode->wrappers[VNODE];
  if (dmMarkWrapper(pWrapper) == 0) {
    if (pWrapper->pMgmt != NULL) {
      vmCleanExpiredMetrics(pWrapper->pMgmt);
    }
  }
  dmReleaseWrapper(pWrapper);
}

void dmMetricsCleanExpiredSamples() {
  if (!tsEnableMonitor || tsMonitorFqdn[0] == 0 || tsMonitorPort == 0 || !tsEnableMetrics) {
    return;
  }
  dTrace("clean metrics expired samples");

  SDnode *pDnode = dmInstance();
  dmCleanExpiredMetrics(pDnode);
}
