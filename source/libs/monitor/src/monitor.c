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
#include "monInt.h"
#include "taoserror.h"
#include "thttp.h"
#include "tlog.h"
#include "ttime.h"

static SMonitor tsMonitor = {0};

int32_t monInit(const SMonCfg *pCfg) {
  tsMonitor.logs = taosArrayInit(16, sizeof(SMonInfo));
  if (tsMonitor.logs == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  tsMonitor.maxLogs = pCfg->maxLogs;
  tsMonitor.server = pCfg->server;
  tsMonitor.port = pCfg->port;
  taosInitRWLatch(&tsMonitor.lock);
  return 0;
}

void monCleanup() {
  taosArrayDestroy(tsMonitor.logs);
  tsMonitor.logs = NULL;
}

void monAddLogItem(SMonLogItem *pItem) {
  taosWLockLatch(&tsMonitor.lock);
  int32_t size = taosArrayGetSize(tsMonitor.logs);
  if (size > tsMonitor.maxLogs) {
    uInfo("too many logs for monitor");
  } else {
    taosArrayPush(tsMonitor.logs, pItem);
  }
  taosWUnLockLatch(&tsMonitor.lock);
}

SMonInfo *monCreateMonitorInfo() {
  SMonInfo *pMonitor = calloc(1, sizeof(SMonInfo));
  if (pMonitor == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  taosWLockLatch(&tsMonitor.lock);
  pMonitor->logs = taosArrayDup(tsMonitor.logs);
  taosArrayClear(tsMonitor.logs);
  taosWUnLockLatch(&tsMonitor.lock);

  pMonitor->pJson = tjsonCreateObject();
  if (pMonitor->pJson == NULL || pMonitor->logs == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    monCleanupMonitorInfo(pMonitor);
    return NULL;
  }

  return pMonitor;
}

void monCleanupMonitorInfo(SMonInfo *pMonitor) {
  taosArrayDestroy(pMonitor->logs);
  tjsonDelete(pMonitor->pJson);
  free(pMonitor);
}

void monSetBasicInfo(SMonInfo *pMonitor, SMonBasicInfo *pInfo) {
  SJson  *pJson = pMonitor->pJson;
  int64_t ms = taosGetTimestampMs();
  char    buf[40] = {0};
  taosFormatUtcTime(buf, sizeof(buf), ms, TSDB_TIME_PRECISION_MILLI);

  tjsonAddStringToObject(pJson, "ts", buf);
  tjsonAddDoubleToObject(pJson, "dnode_id", pInfo->dnode_id);
  tjsonAddStringToObject(pJson, "dnode_ep", pInfo->dnode_ep);
}

void monSetClusterInfo(SMonInfo *pMonitor, SMonClusterInfo *pInfo) {
  SJson *pJson = tjsonCreateObject();
  if (pJson == NULL) return;
  if (tjsonAddItemToObject(pMonitor->pJson, "cluster_info", pJson) != 0) {
    tjsonDelete(pJson);
    return;
  }

  tjsonAddStringToObject(pJson, "first_ep", pInfo->first_ep);
  tjsonAddDoubleToObject(pJson, "first_ep_dnode_id", pInfo->first_ep_dnode_id);
  tjsonAddStringToObject(pJson, "version", pInfo->version);
  tjsonAddDoubleToObject(pJson, "master_uptime", pInfo->master_uptime);
  tjsonAddDoubleToObject(pJson, "monitor_interval", pInfo->monitor_interval);
  tjsonAddDoubleToObject(pJson, "vgroups_total", pInfo->vgroups_total);
  tjsonAddDoubleToObject(pJson, "vgroups_alive", pInfo->vgroups_alive);
  tjsonAddDoubleToObject(pJson, "vnodes_total", pInfo->vnodes_total);
  tjsonAddDoubleToObject(pJson, "vnodes_alive", pInfo->vnodes_alive);
  tjsonAddDoubleToObject(pJson, "connections_total", pInfo->connections_total);

  SJson *pDnodesJson = tjsonAddArrayToObject(pJson, "dnodes");
  if (pDnodesJson == NULL) return;

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->dnodes); ++i) {
    SJson *pDnodeJson = tjsonCreateObject();
    if (pDnodeJson == NULL) continue;

    SMonDnodeDesc *pDnodeDesc = taosArrayGet(pInfo->dnodes, i);
    if (tjsonAddDoubleToObject(pDnodesJson, "dnode_id", pDnodeDesc->dnode_id) != 0) tjsonDelete(pDnodeJson);
    if (tjsonAddStringToObject(pDnodesJson, "dnode_ep", pDnodeDesc->dnode_ep) != 0) tjsonDelete(pDnodeJson);
    if (tjsonAddStringToObject(pDnodesJson, "status", pDnodeDesc->status) != 0) tjsonDelete(pDnodeJson);

    if (tjsonAddItemToArray(pDnodesJson, pDnodeJson) != 0) tjsonDelete(pDnodeJson);
  }

  SJson *pMnodesJson = tjsonAddArrayToObject(pJson, "mnodes");
  if (pMnodesJson == NULL) return;

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->dnodes); ++i) {
    SJson *pMnodeJson = tjsonCreateObject();
    if (pMnodeJson == NULL) continue;

    SMonMnodeDesc *pMnodeDesc = taosArrayGet(pInfo->dnodes, i);
    if (tjsonAddDoubleToObject(pMnodesJson, "mnode_id", pMnodeDesc->mnode_id) != 0) tjsonDelete(pMnodeJson);
    if (tjsonAddStringToObject(pMnodesJson, "mnode_ep", pMnodeDesc->mnode_ep) != 0) tjsonDelete(pMnodeJson);
    if (tjsonAddStringToObject(pMnodesJson, "role", pMnodeDesc->role) != 0) tjsonDelete(pMnodeJson);

    if (tjsonAddItemToArray(pMnodesJson, pMnodeJson) != 0) tjsonDelete(pMnodeJson);
  }
}

void monSetVgroupInfo(SMonInfo *pMonitor, SMonVgroupInfo *pInfo) {
  SJson *pJson = tjsonCreateObject();
  if (pJson == NULL) return;
  if (tjsonAddItemToObject(pMonitor->pJson, "vgroup_infos", pJson) != 0) {
    tjsonDelete(pJson);
    return;
  }

  tjsonAddStringToObject(pJson, "database_name", pInfo->database_name);
  tjsonAddDoubleToObject(pJson, "tables_num", pInfo->tables_num);
  tjsonAddStringToObject(pJson, "status", pInfo->status);

  SJson *pVgroupsJson = tjsonAddArrayToObject(pJson, "vgroups");
  if (pVgroupsJson == NULL) return;

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->vgroups); ++i) {
    SJson *pVgroupJson = tjsonCreateObject();
    if (pVgroupJson == NULL) continue;

    SMonVgroupDesc *pVgroupDesc = taosArrayGet(pInfo->vgroups, i);
    if (tjsonAddDoubleToObject(pVgroupJson, "vgroup_id", pVgroupDesc->vgroup_id) != 0) tjsonDelete(pVgroupJson);

    SJson *pVnodesJson = tjsonAddArrayToObject(pVgroupJson, "vnodes");
    if (pVnodesJson == NULL) tjsonDelete(pVgroupJson);

    for (int32_t j = 0; j < TSDB_MAX_REPLICA; ++j) {
      SMonVnodeDesc *pVnodeDesc = &pVgroupDesc->vnodes[j];
      if (pVnodeDesc[j].dnode_id <= 0) continue;

      SJson *pVnodeJson = tjsonCreateObject();
      if (pVnodeJson == NULL) continue;

      if (tjsonAddDoubleToObject(pVnodeJson, "dnode_id", pVnodeDesc->dnode_id) != 0) tjsonDelete(pVnodeJson);
      if (tjsonAddStringToObject(pVnodeJson, "vnode_role", pVnodeDesc->vnode_role) != 0) tjsonDelete(pVnodeJson);

      if (tjsonAddItemToArray(pVnodesJson, pVnodeJson) != 0) tjsonDelete(pVnodeJson);
    }
  }
}

void monSetGrantInfo(SMonInfo *pMonitor, SMonGrantInfo *pInfo) {
  SJson *pJson = tjsonCreateObject();
  if (pJson == NULL) return;
  if (tjsonAddItemToObject(pMonitor->pJson, "grant_info", pJson) != 0) {
    tjsonDelete(pJson);
    return;
  }

  tjsonAddDoubleToObject(pJson, "expire_time", pInfo->expire_time);
  tjsonAddDoubleToObject(pJson, "timeseries_used", pInfo->timeseries_used);
  tjsonAddDoubleToObject(pJson, "timeseries_total", pInfo->timeseries_total);
}

void monSetDnodeInfo(SMonInfo *pMonitor, SMonDnodeInfo *pInfo) {
  SJson *pJson = tjsonCreateObject();
  if (pJson == NULL) return;
  if (tjsonAddItemToObject(pMonitor->pJson, "dnode_info", pJson) != 0) {
    tjsonDelete(pJson);
    return;
  }

  tjsonAddDoubleToObject(pJson, "uptime", pInfo->uptime);
  tjsonAddDoubleToObject(pJson, "cpu_engine", pInfo->cpu_engine);
  tjsonAddDoubleToObject(pJson, "cpu_system", pInfo->cpu_system);
  tjsonAddDoubleToObject(pJson, "cpu_cores", pInfo->cpu_cores);
  tjsonAddDoubleToObject(pJson, "mem_engine", pInfo->mem_engine);
  tjsonAddDoubleToObject(pJson, "mem_system", pInfo->mem_system);
  tjsonAddDoubleToObject(pJson, "mem_total", pInfo->mem_total);
  tjsonAddDoubleToObject(pJson, "disk_engine", pInfo->disk_engine);
  tjsonAddDoubleToObject(pJson, "disk_used", pInfo->disk_used);
  tjsonAddDoubleToObject(pJson, "disk_total", pInfo->disk_total);
  tjsonAddDoubleToObject(pJson, "net_in", pInfo->net_in);
  tjsonAddDoubleToObject(pJson, "net_out", pInfo->net_out);
  tjsonAddDoubleToObject(pJson, "io_read", pInfo->io_read);
  tjsonAddDoubleToObject(pJson, "io_write", pInfo->io_write);
  tjsonAddDoubleToObject(pJson, "io_read_disk", pInfo->io_read_disk);
  tjsonAddDoubleToObject(pJson, "io_write_disk", pInfo->io_write_disk);
  tjsonAddDoubleToObject(pJson, "req_select", pInfo->req_select);
  tjsonAddDoubleToObject(pJson, "req_select_rate", pInfo->req_select_rate);
  tjsonAddDoubleToObject(pJson, "req_insert", pInfo->req_insert);
  tjsonAddDoubleToObject(pJson, "req_insert_success", pInfo->req_insert_success);
  tjsonAddDoubleToObject(pJson, "req_insert_rate", pInfo->req_insert_rate);
  tjsonAddDoubleToObject(pJson, "req_insert_batch", pInfo->req_insert_batch);
  tjsonAddDoubleToObject(pJson, "req_insert_batch_success", pInfo->req_insert_batch_success);
  tjsonAddDoubleToObject(pJson, "req_insert_batch_rate", pInfo->req_insert_batch_rate);
  tjsonAddDoubleToObject(pJson, "errors", pInfo->errors);
  tjsonAddDoubleToObject(pJson, "vnodes_num", pInfo->vnodes_num);
  tjsonAddDoubleToObject(pJson, "masters", pInfo->masters);
  tjsonAddDoubleToObject(pJson, "has_mnode", pInfo->has_mnode);
}

void monSetDiskInfo(SMonInfo *pMonitor, SMonDiskInfo *pInfo) {
  SJson *pJson = tjsonCreateObject();
  if (pJson == NULL) return;
  if (tjsonAddItemToObject(pMonitor->pJson, "disks_infos", pJson) != 0) {
    tjsonDelete(pJson);
    return;
  }

  SJson *pDatadirsJson = tjsonAddArrayToObject(pJson, "datadir");
  if (pDatadirsJson == NULL) return;

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->datadirs); ++i) {
    SJson *pDatadirJson = tjsonCreateObject();
    if (pDatadirJson == NULL) continue;

    SMonDiskDesc *pDatadirDesc = taosArrayGet(pInfo->datadirs, i);
    if (tjsonAddStringToObject(pDatadirJson, "name", pDatadirDesc->name) != 0) tjsonDelete(pDatadirJson);
    if (tjsonAddDoubleToObject(pDatadirJson, "level", pDatadirDesc->level) != 0) tjsonDelete(pDatadirJson);
    if (tjsonAddDoubleToObject(pDatadirJson, "avail", pDatadirDesc->size.avail) != 0) tjsonDelete(pDatadirJson);
    if (tjsonAddDoubleToObject(pDatadirJson, "used", pDatadirDesc->size.used) != 0) tjsonDelete(pDatadirJson);
    if (tjsonAddDoubleToObject(pDatadirJson, "total", pDatadirDesc->size.total) != 0) tjsonDelete(pDatadirJson);

    if (tjsonAddItemToArray(pDatadirsJson, pDatadirJson) != 0) tjsonDelete(pDatadirJson);
  }

  SJson *pLogdirJson = tjsonCreateObject();
  if (pLogdirJson == NULL) return;
  if (tjsonAddItemToObject(pJson, "logdir", pLogdirJson) != 0) return;
  tjsonAddStringToObject(pLogdirJson, "name", pInfo->logdir.name);
  tjsonAddDoubleToObject(pLogdirJson, "level", pInfo->logdir.level);
  tjsonAddDoubleToObject(pLogdirJson, "avail", pInfo->logdir.size.avail);
  tjsonAddDoubleToObject(pLogdirJson, "used", pInfo->logdir.size.used);
  tjsonAddDoubleToObject(pLogdirJson, "total", pInfo->logdir.size.total);

  SJson *pTempdirJson = tjsonCreateObject();
  if (pTempdirJson == NULL) return;
  if (tjsonAddItemToObject(pJson, "tempdir", pTempdirJson) != 0) return;
  tjsonAddStringToObject(pTempdirJson, "name", pInfo->tempdir.name);
  tjsonAddDoubleToObject(pTempdirJson, "level", pInfo->tempdir.level);
  tjsonAddDoubleToObject(pTempdirJson, "avail", pInfo->tempdir.size.avail);
  tjsonAddDoubleToObject(pTempdirJson, "used", pInfo->tempdir.size.used);
  tjsonAddDoubleToObject(pTempdirJson, "total", pInfo->tempdir.size.total);
}

static void monSetLogInfo(SMonInfo *pMonitor) {
  SJson *pJson = tjsonCreateObject();
  if (pJson == NULL) return;
  if (tjsonAddItemToObject(pMonitor->pJson, "log_infos", pJson) != 0) {
    tjsonDelete(pJson);
    return;
  }

  SJson *pLogsJson = tjsonAddArrayToObject(pJson, "logs");
  if (pLogsJson == NULL) return;

  for (int32_t i = 0; i < taosArrayGetSize(pMonitor->logs); ++i) {
    SJson *pLogJson = tjsonCreateObject();
    if (pLogJson == NULL) continue;

    SMonLogItem *pLogItem = taosArrayGet(pMonitor->logs, i);

    char buf[40] = {0};
    taosFormatUtcTime(buf, sizeof(buf), pLogItem->ts, TSDB_TIME_PRECISION_MILLI);

    if (tjsonAddStringToObject(pLogItem, "ts", buf) != 0) tjsonDelete(pLogJson);
    if (tjsonAddDoubleToObject(pLogItem, "level", pLogItem->level) != 0) tjsonDelete(pLogJson);
    if (tjsonAddStringToObject(pLogItem, "content", pLogItem->content) != 0) tjsonDelete(pLogJson);

    if (tjsonAddItemToArray(pLogsJson, pLogJson) != 0) tjsonDelete(pLogJson);
  }

  SJson *pSummaryJson = tjsonAddArrayToObject(pJson, "summary");
  if (pSummaryJson == NULL) return;

  SJson *pLogError = tjsonCreateObject();
  if (pLogError == NULL) return;
  tjsonAddStringToObject(pLogError, "level", "error");
  tjsonAddDoubleToObject(pLogError, "total", 1);
  if (tjsonAddItemToArray(pSummaryJson, pLogError) != 0) tjsonDelete(pLogError);

  SJson *pLogInfo = tjsonCreateObject();
  if (pLogInfo == NULL) return;
  tjsonAddStringToObject(pLogInfo, "level", "info");
  tjsonAddDoubleToObject(pLogInfo, "total", 1);
  if (tjsonAddItemToArray(pSummaryJson, pLogInfo) != 0) tjsonDelete(pLogInfo);

  SJson *pLogDebug = tjsonCreateObject();
  if (pLogDebug == NULL) return;
  tjsonAddStringToObject(pLogDebug, "level", "debug");
  tjsonAddDoubleToObject(pLogDebug, "total", 1);
  if (tjsonAddItemToArray(pSummaryJson, pLogDebug) != 0) tjsonDelete(pLogDebug);

  SJson *pLogTrace = tjsonCreateObject();
  if (pLogTrace == NULL) return;
  tjsonAddStringToObject(pLogTrace, "level", "trace");
  tjsonAddDoubleToObject(pLogTrace, "total", 1);
  if (tjsonAddItemToArray(pSummaryJson, pLogTrace) != 0) tjsonDelete(pLogTrace);
}

void monSendReport(SMonInfo *pMonitor) {
  monSetLogInfo(pMonitor);

  char *pCont = tjsonToString(pMonitor->pJson);
  if (pCont != NULL) {
    taosSendHttpReport(tsMonitor.server, tsMonitor.port, pCont, strlen(pCont));
    free(pCont);
  }
}
