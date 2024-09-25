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
#include "taos_monitor.h"
#include "taoserror.h"
#include "tglobal.h"
#include "thttp.h"
#include "ttime.h"

#define VNODE_METRIC_SQL_COUNT "taosd_sql_req:count"

#define VNODE_METRIC_TAG_NAME_SQL_TYPE   "sql_type"
#define VNODE_METRIC_TAG_NAME_CLUSTER_ID "cluster_id"
#define VNODE_METRIC_TAG_NAME_DNODE_ID   "dnode_id"
#define VNODE_METRIC_TAG_NAME_DNODE_EP   "dnode_ep"
#define VNODE_METRIC_TAG_NAME_VGROUP_ID  "vgroup_id"
#define VNODE_METRIC_TAG_NAME_USERNAME   "username"
#define VNODE_METRIC_TAG_NAME_RESULT     "result"

// #define VNODE_METRIC_TAG_VALUE_INSERT "insert"
// #define VNODE_METRIC_TAG_VALUE_DELETE "delete"

SMonitor tsMonitor = {0};
char    *tsMonUri = "/report";
char    *tsMonFwUri = "/general-metric";
char    *tsMonSlowLogUri = "/slow-sql-detail-batch";
char    *tsMonFwBasicUri = "/taosd-cluster-basic";
taos_counter_t *tsInsertCounter = NULL;

void monRecordLog(int64_t ts, ELogLevel level, const char *content) {
  (void)taosThreadMutexLock(&tsMonitor.lock);
  int32_t size = taosArrayGetSize(tsMonitor.logs);
  if (size < tsMonitor.cfg.maxLogs) {
    SMonLogItem  item = {.ts = ts, .level = level};
    SMonLogItem *pItem = taosArrayPush(tsMonitor.logs, &item);
    if (pItem != NULL) {
      tstrncpy(pItem->content, content, MON_LOG_LEN);
    }
  }
  (void)taosThreadMutexUnlock(&tsMonitor.lock);
}

int32_t monGetLogs(SMonLogs *logs) {
  (void)taosThreadMutexLock(&tsMonitor.lock);
  logs->logs = taosArrayDup(tsMonitor.logs, NULL);
  logs->numOfInfoLogs = tsNumOfInfoLogs;
  logs->numOfErrorLogs = tsNumOfErrorLogs;
  logs->numOfDebugLogs = tsNumOfDebugLogs;
  logs->numOfTraceLogs = tsNumOfTraceLogs;
  tsNumOfInfoLogs = 0;
  tsNumOfErrorLogs = 0;
  tsNumOfDebugLogs = 0;
  tsNumOfTraceLogs = 0;
  taosArrayClear(tsMonitor.logs);
  (void)taosThreadMutexUnlock(&tsMonitor.lock);
  if (logs->logs == NULL) {
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }
  return 0;
}

void monSetDmInfo(SMonDmInfo *pInfo) {
  (void)taosThreadMutexLock(&tsMonitor.lock);
  memcpy(&tsMonitor.dmInfo, pInfo, sizeof(SMonDmInfo));
  (void)taosThreadMutexUnlock(&tsMonitor.lock);
  memset(pInfo, 0, sizeof(SMonDmInfo));
}

void monSetMmInfo(SMonMmInfo *pInfo) {
  (void)taosThreadMutexLock(&tsMonitor.lock);
  memcpy(&tsMonitor.mmInfo, pInfo, sizeof(SMonMmInfo));
  (void)taosThreadMutexUnlock(&tsMonitor.lock);
  memset(pInfo, 0, sizeof(SMonMmInfo));
}

void monSetVmInfo(SMonVmInfo *pInfo) {
  (void)taosThreadMutexLock(&tsMonitor.lock);
  memcpy(&tsMonitor.vmInfo, pInfo, sizeof(SMonVmInfo));
  (void)taosThreadMutexUnlock(&tsMonitor.lock);
  memset(pInfo, 0, sizeof(SMonVmInfo));
}

void monSetQmInfo(SMonQmInfo *pInfo) {
  (void)taosThreadMutexLock(&tsMonitor.lock);
  memcpy(&tsMonitor.qmInfo, pInfo, sizeof(SMonQmInfo));
  (void)taosThreadMutexUnlock(&tsMonitor.lock);
  memset(pInfo, 0, sizeof(SMonQmInfo));
}

void monSetSmInfo(SMonSmInfo *pInfo) {
  (void)taosThreadMutexLock(&tsMonitor.lock);
  memcpy(&tsMonitor.smInfo, pInfo, sizeof(SMonSmInfo));
  (void)taosThreadMutexUnlock(&tsMonitor.lock);
  memset(pInfo, 0, sizeof(SMonSmInfo));
}

void monSetBmInfo(SMonBmInfo *pInfo) {
  (void)taosThreadMutexLock(&tsMonitor.lock);
  memcpy(&tsMonitor.bmInfo, pInfo, sizeof(SMonBmInfo));
  (void)taosThreadMutexUnlock(&tsMonitor.lock);
  memset(pInfo, 0, sizeof(SMonBmInfo));
}

int32_t monInit(const SMonCfg *pCfg) {
  tsMonitor.logs = taosArrayInit(16, sizeof(SMonLogItem));
  if (tsMonitor.logs == NULL) {
    TAOS_RETURN(terrno);
  }

  tsMonitor.cfg = *pCfg;
  tsLogFp = monRecordLog;
  tsMonitor.lastTime = taosGetTimestampMs();
  (void)taosThreadMutexInit(&tsMonitor.lock, NULL);

  monInitMonitorFW();

  return 0;
}

void monSetDnodeId(int32_t dnodeId) { tsMonitor.dnodeId = dnodeId; }

void monInitVnode() {
  if (!tsEnableMonitor || tsMonitorFqdn[0] == 0 || tsMonitorPort == 0) return;
  if (tsInsertCounter == NULL) {
    taos_counter_t *counter = NULL;
    int32_t         label_count = 7;
    const char     *sample_labels[] = {VNODE_METRIC_TAG_NAME_SQL_TYPE,  VNODE_METRIC_TAG_NAME_CLUSTER_ID,
                                       VNODE_METRIC_TAG_NAME_DNODE_ID,  VNODE_METRIC_TAG_NAME_DNODE_EP,
                                       VNODE_METRIC_TAG_NAME_VGROUP_ID, VNODE_METRIC_TAG_NAME_USERNAME,
                                       VNODE_METRIC_TAG_NAME_RESULT};
    counter = taos_counter_new(VNODE_METRIC_SQL_COUNT, "counter for insert sql", label_count, sample_labels);
    uDebug("new metric:%p", counter);
    if (taos_collector_registry_register_metric(counter) == 1) {
      if (taos_counter_destroy(counter) != 0) {
        uError("failed to destroy metric:%p", counter);
      }
      uError("failed to register metric:%p", counter);
    } else {
      tsInsertCounter = counter;
      uInfo("succeed to set inserted row metric:%p", tsInsertCounter);
    }
  } else {
    uError("failed to set insert counter, already set");
  }
}

void monCleanup() {
  tsLogFp = NULL;
  taosArrayDestroy(tsMonitor.logs);
  tsMonitor.logs = NULL;
  tFreeSMonMmInfo(&tsMonitor.mmInfo);
  tFreeSMonVmInfo(&tsMonitor.vmInfo);
  tFreeSMonSmInfo(&tsMonitor.smInfo);
  tFreeSMonQmInfo(&tsMonitor.qmInfo);
  tFreeSMonBmInfo(&tsMonitor.bmInfo);
  (void)taosThreadMutexDestroy(&tsMonitor.lock);

  monCleanupMonitorFW();
}

static void monCleanupMonitorInfo(SMonInfo *pMonitor) {
  tsMonitor.lastTime = pMonitor->curTime;
  taosArrayDestroy(pMonitor->log.logs);
  tFreeSMonMmInfo(&pMonitor->mmInfo);
  tFreeSMonVmInfo(&pMonitor->vmInfo);
  tFreeSMonSmInfo(&pMonitor->smInfo);
  tFreeSMonQmInfo(&pMonitor->qmInfo);
  tFreeSMonBmInfo(&pMonitor->bmInfo);
  tjsonDelete(pMonitor->pJson);
  taosMemoryFree(pMonitor);
}

static SMonInfo *monCreateMonitorInfo() {
  terrno = 0;
  SMonInfo *pMonitor = taosMemoryCalloc(1, sizeof(SMonInfo));
  if (pMonitor == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  if ((terrno = monGetLogs(&pMonitor->log)) != 0) {
    return NULL;
  }

  (void)taosThreadMutexLock(&tsMonitor.lock);
  memcpy(&pMonitor->dmInfo, &tsMonitor.dmInfo, sizeof(SMonDmInfo));
  memcpy(&pMonitor->mmInfo, &tsMonitor.mmInfo, sizeof(SMonMmInfo));
  memcpy(&pMonitor->vmInfo, &tsMonitor.vmInfo, sizeof(SMonVmInfo));
  memcpy(&pMonitor->smInfo, &tsMonitor.smInfo, sizeof(SMonSmInfo));
  memcpy(&pMonitor->qmInfo, &tsMonitor.qmInfo, sizeof(SMonQmInfo));
  memcpy(&pMonitor->bmInfo, &tsMonitor.bmInfo, sizeof(SMonBmInfo));
  memset(&tsMonitor.dmInfo, 0, sizeof(SMonDmInfo));
  memset(&tsMonitor.mmInfo, 0, sizeof(SMonMmInfo));
  memset(&tsMonitor.vmInfo, 0, sizeof(SMonVmInfo));
  memset(&tsMonitor.smInfo, 0, sizeof(SMonSmInfo));
  memset(&tsMonitor.qmInfo, 0, sizeof(SMonQmInfo));
  memset(&tsMonitor.bmInfo, 0, sizeof(SMonBmInfo));
  (void)taosThreadMutexUnlock(&tsMonitor.lock);

  pMonitor->pJson = tjsonCreateObject();
  if (pMonitor->pJson == NULL || pMonitor->log.logs == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    monCleanupMonitorInfo(pMonitor);
    return NULL;
  }

  pMonitor->curTime = taosGetTimestampMs();
  pMonitor->lastTime = tsMonitor.lastTime;
  return pMonitor;
}

static void monGenBasicJson(SMonInfo *pMonitor) {
  SMonBasicInfo *pInfo = &pMonitor->dmInfo.basic;

  SJson *pJson = pMonitor->pJson;
  char   buf[40] = {0};
  if (taosFormatUtcTime(buf, sizeof(buf), pMonitor->curTime, TSDB_TIME_PRECISION_MILLI) != 0) {
    uError("failed to format time");
    return;
  }

  if (tjsonAddStringToObject(pJson, "ts", buf) != 0) uError("failed to add ts");
  if (tjsonAddDoubleToObject(pJson, "dnode_id", pInfo->dnode_id) != 0) uError("failed to add dnode_id");
  if (tjsonAddStringToObject(pJson, "dnode_ep", pInfo->dnode_ep) != 0) uError("failed to add dnode_ep");
  snprintf(buf, sizeof(buf), "%" PRId64, pInfo->cluster_id);
  if (tjsonAddStringToObject(pJson, "cluster_id", buf) != 0) uError("failed to add cluster_id");
  if (tjsonAddDoubleToObject(pJson, "protocol", pInfo->protocol) != 0) uError("failed to add protocol");
}

static void monGenBasicJsonBasic(SMonInfo *pMonitor) {
  SMonBasicInfo *pInfo = &pMonitor->dmInfo.basic;
  if (pMonitor->mmInfo.cluster.first_ep_dnode_id == 0) return;

  SJson *pJson = pMonitor->pJson;
  char   buf[40] = {0};

  sprintf(buf, "%" PRId64, taosGetTimestamp(TSDB_TIME_PRECISION_MILLI));
  if (tjsonAddStringToObject(pJson, "ts", buf) != 0) uError("failed to add ts");
  if (tjsonAddDoubleToObject(pJson, "dnode_id", pInfo->dnode_id) != 0) uError("failed to add dnode_id");
  if (tjsonAddStringToObject(pJson, "dnode_ep", pInfo->dnode_ep) != 0) uError("failed to add dnode_ep");
  snprintf(buf, sizeof(buf), "%" PRId64, pInfo->cluster_id);
  if (tjsonAddStringToObject(pJson, "cluster_id", buf) != 0) uError("failed to add cluster_id");
  if (tjsonAddDoubleToObject(pJson, "protocol", pInfo->protocol) != 0) uError("failed to add protocol");
}

static void monGenClusterJson(SMonInfo *pMonitor) {
  SMonClusterInfo *pInfo = &pMonitor->mmInfo.cluster;
  if (pMonitor->mmInfo.cluster.first_ep_dnode_id == 0) return;

  SJson *pJson = tjsonCreateObject();
  if (pJson == NULL) return;
  if (tjsonAddItemToObject(pMonitor->pJson, "cluster_info", pJson) != 0) {
    tjsonDelete(pJson);
    return;
  }

  if (tjsonAddStringToObject(pJson, "first_ep", pInfo->first_ep) != 0) uError("failed to add first_ep");
  if (tjsonAddDoubleToObject(pJson, "first_ep_dnode_id", pInfo->first_ep_dnode_id) != 0)
    uError("failed to add first_ep_dnode_id");
  if (tjsonAddStringToObject(pJson, "version", pInfo->version) != 0) uError("failed to add version");
  if (tjsonAddDoubleToObject(pJson, "master_uptime", pInfo->master_uptime) != 0) uError("failed to add master_uptime");
  if (tjsonAddDoubleToObject(pJson, "monitor_interval", pInfo->monitor_interval) != 0)
    uError("failed to add monitor_interval");
  if (tjsonAddDoubleToObject(pJson, "dbs_total", pInfo->dbs_total) != 0) uError("failed to add dbs_total");
  if (tjsonAddDoubleToObject(pJson, "tbs_total", pInfo->tbs_total) != 0) uError("failed to add tbs_total");
  if (tjsonAddDoubleToObject(pJson, "stbs_total", pInfo->stbs_total) != 0) uError("failed to add stbs_total");
  if (tjsonAddDoubleToObject(pJson, "vgroups_total", pInfo->vgroups_total) != 0) uError("failed to add vgroups_total");
  if (tjsonAddDoubleToObject(pJson, "vgroups_alive", pInfo->vgroups_alive) != 0) uError("failed to add vgroups_alive");
  if (tjsonAddDoubleToObject(pJson, "vnodes_total", pInfo->vnodes_total) != 0) uError("failed to add vnodes_total");
  if (tjsonAddDoubleToObject(pJson, "vnodes_alive", pInfo->vnodes_alive) != 0) uError("failed to add vnodes_alive");
  if (tjsonAddDoubleToObject(pJson, "connections_total", pInfo->connections_total) != 0)
    uError("failed to add connections_total");
  if (tjsonAddDoubleToObject(pJson, "topics_total", pInfo->topics_toal) != 0) uError("failed to add topics_total");
  if (tjsonAddDoubleToObject(pJson, "streams_total", pInfo->streams_total) != 0) uError("failed to add streams_total");

  SJson *pDnodesJson = tjsonAddArrayToObject(pJson, "dnodes");
  if (pDnodesJson == NULL) return;

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->dnodes); ++i) {
    SJson *pDnodeJson = tjsonCreateObject();
    if (pDnodeJson == NULL) continue;

    SMonDnodeDesc *pDnodeDesc = taosArrayGet(pInfo->dnodes, i);
    if (tjsonAddDoubleToObject(pDnodeJson, "dnode_id", pDnodeDesc->dnode_id) != 0) uError("failed to add dnode_id");
    if (tjsonAddStringToObject(pDnodeJson, "dnode_ep", pDnodeDesc->dnode_ep) != 0) uError("failed to add dnode_ep");
    if (tjsonAddStringToObject(pDnodeJson, "status", pDnodeDesc->status) != 0) uError("failed to add status");

    if (tjsonAddItemToArray(pDnodesJson, pDnodeJson) != 0) tjsonDelete(pDnodeJson);
  }

  SJson *pMnodesJson = tjsonAddArrayToObject(pJson, "mnodes");
  if (pMnodesJson == NULL) return;

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->mnodes); ++i) {
    SJson *pMnodeJson = tjsonCreateObject();
    if (pMnodeJson == NULL) continue;

    SMonMnodeDesc *pMnodeDesc = taosArrayGet(pInfo->mnodes, i);
    if (tjsonAddDoubleToObject(pMnodeJson, "mnode_id", pMnodeDesc->mnode_id) != 0) uError("failed to add mnode_id");
    if (tjsonAddStringToObject(pMnodeJson, "mnode_ep", pMnodeDesc->mnode_ep) != 0) uError("failed to add mnode_ep");
    if (tjsonAddStringToObject(pMnodeJson, "role", pMnodeDesc->role) != 0) uError("failed to add role");

    if (tjsonAddItemToArray(pMnodesJson, pMnodeJson) != 0) tjsonDelete(pMnodeJson);
  }
}

static void monGenClusterJsonBasic(SMonInfo *pMonitor) {
  SMonClusterInfo *pInfo = &pMonitor->mmInfo.cluster;
  if (pMonitor->mmInfo.cluster.first_ep_dnode_id == 0) return;

  if (tjsonAddStringToObject(pMonitor->pJson, "first_ep", tsFirst) != 0) uError("failed to add first_ep");
  if (tjsonAddDoubleToObject(pMonitor->pJson, "first_ep_dnode_id", pInfo->first_ep_dnode_id) != 0)
    uError("failed to add first_ep_dnode_id");
  if (tjsonAddStringToObject(pMonitor->pJson, "cluster_version", pInfo->version) != 0)
    uError("failed to add cluster_version");
}

static void monGenVgroupJson(SMonInfo *pMonitor) {
  SMonVgroupInfo *pInfo = &pMonitor->mmInfo.vgroup;
  if (pMonitor->mmInfo.cluster.first_ep_dnode_id == 0) return;

  SJson *pJson = tjsonAddArrayToObject(pMonitor->pJson, "vgroup_infos");
  if (pJson == NULL) return;

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->vgroups); ++i) {
    SJson *pVgroupJson = tjsonCreateObject();
    if (pVgroupJson == NULL) continue;
    if (tjsonAddItemToArray(pJson, pVgroupJson) != 0) {
      tjsonDelete(pVgroupJson);
      continue;
    }

    SMonVgroupDesc *pVgroupDesc = taosArrayGet(pInfo->vgroups, i);
    if (tjsonAddDoubleToObject(pVgroupJson, "vgroup_id", pVgroupDesc->vgroup_id) != 0)
      uError("failed to add vgroup_id");
    if (tjsonAddStringToObject(pVgroupJson, "database_name", pVgroupDesc->database_name) != 0)
      uError("failed to add database_name");
    if (tjsonAddDoubleToObject(pVgroupJson, "tables_num", pVgroupDesc->tables_num) != 0)
      uError("failed to add tables_num");
    if (tjsonAddStringToObject(pVgroupJson, "status", pVgroupDesc->status) != 0) uError("failed to add status");

    SJson *pVnodesJson = tjsonAddArrayToObject(pVgroupJson, "vnodes");
    if (pVnodesJson == NULL) continue;

    for (int32_t j = 0; j < TSDB_MAX_REPLICA; ++j) {
      SMonVnodeDesc *pVnodeDesc = &pVgroupDesc->vnodes[j];
      if (pVnodeDesc->dnode_id <= 0) continue;

      SJson *pVnodeJson = tjsonCreateObject();
      if (pVnodeJson == NULL) continue;

      if (tjsonAddDoubleToObject(pVnodeJson, "dnode_id", pVnodeDesc->dnode_id) != 0) uError("failed to add dnode_id");
      if (tjsonAddStringToObject(pVnodeJson, "vnode_role", pVnodeDesc->vnode_role) != 0)
        uError("failed to add vnode_role");

      if (tjsonAddItemToArray(pVnodesJson, pVnodeJson) != 0) tjsonDelete(pVnodeJson);
    }
  }
}

static void monGenStbJson(SMonInfo *pMonitor) {
  SMonStbInfo *pInfo = &pMonitor->mmInfo.stb;
  if (pMonitor->mmInfo.cluster.first_ep_dnode_id == 0) return;

  SJson *pJson = tjsonAddArrayToObject(pMonitor->pJson, "stb_infos");
  if (pJson == NULL) return;

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->stbs); ++i) {
    SJson *pStbJson = tjsonCreateObject();
    if (pStbJson == NULL) continue;
    if (tjsonAddItemToArray(pJson, pStbJson) != 0) {
      tjsonDelete(pStbJson);
      continue;
    }

    SMonStbDesc *pStbDesc = taosArrayGet(pInfo->stbs, i);
    if (tjsonAddStringToObject(pStbJson, "stb_name", pStbDesc->stb_name) != 0) uError("failed to add stb_name");
    if (tjsonAddStringToObject(pStbJson, "database_name", pStbDesc->database_name) != 0)
      uError("failed to add database_name");
  }
}

static void monGenGrantJson(SMonInfo *pMonitor) {
  SMonGrantInfo *pInfo = &pMonitor->mmInfo.grant;
  if (pMonitor->mmInfo.cluster.first_ep_dnode_id == 0) return;

  SJson *pJson = tjsonCreateObject();
  if (pJson == NULL) return;
  if (tjsonAddItemToObject(pMonitor->pJson, "grant_info", pJson) != 0) {
    tjsonDelete(pJson);
    return;
  }

  if (tjsonAddDoubleToObject(pJson, "expire_time", pInfo->expire_time) != 0) uError("failed to add expire_time");
  if (tjsonAddDoubleToObject(pJson, "timeseries_used", pInfo->timeseries_used) != 0)
    uError("failed to add timeseries_used");
  if (tjsonAddDoubleToObject(pJson, "timeseries_total", pInfo->timeseries_total) != 0)
    uError("failed to add timeseries_total");
}

static void monGenDnodeJson(SMonInfo *pMonitor) {
  SMonDnodeInfo *pInfo = &pMonitor->dmInfo.dnode;
  SMonSysInfo   *pSys = &pMonitor->dmInfo.sys;
  SVnodesStat   *pStat = &pMonitor->vmInfo.vstat;

  SJson *pJson = tjsonCreateObject();
  if (pJson == NULL) return;
  if (tjsonAddItemToObject(pMonitor->pJson, "dnode_info", pJson) != 0) {
    tjsonDelete(pJson);
    return;
  }

  double interval = (pMonitor->curTime - pMonitor->lastTime) / 1000.0;
  if (pMonitor->curTime - pMonitor->lastTime == 0) {
    interval = 1;
  }

  double cpu_engine = 0;
  double mem_engine = 0;
  double net_in = 0;
  double net_out = 0;
  double io_read = 0;
  double io_write = 0;
  double io_read_disk = 0;
  double io_write_disk = 0;

  SMonSysInfo *sysArrays[6];
  sysArrays[0] = &pMonitor->dmInfo.sys;
  sysArrays[1] = &pMonitor->mmInfo.sys;
  sysArrays[2] = &pMonitor->vmInfo.sys;
  sysArrays[3] = &pMonitor->qmInfo.sys;
  sysArrays[4] = &pMonitor->smInfo.sys;
  sysArrays[5] = &pMonitor->bmInfo.sys;
  for (int32_t i = 0; i < 6; ++i) {
    cpu_engine += sysArrays[i]->cpu_engine;
    mem_engine += sysArrays[i]->mem_engine;
    net_in += sysArrays[i]->net_in;
    net_out += sysArrays[i]->net_out;
    io_read += sysArrays[i]->io_read;
    io_write += sysArrays[i]->io_write;
    io_read_disk += sysArrays[i]->io_read_disk;
    io_write_disk += sysArrays[i]->io_write_disk;
  }

  double req_select_rate = pStat->numOfSelectReqs / interval;
  double req_insert_rate = pStat->numOfInsertReqs / interval;
  double req_insert_batch_rate = pStat->numOfBatchInsertReqs / interval;
  double net_in_rate = net_in / interval;
  double net_out_rate = net_out / interval;
  double io_read_rate = io_read / interval;
  double io_write_rate = io_write / interval;
  double io_read_disk_rate = io_read_disk / interval;
  double io_write_disk_rate = io_write_disk / interval;

  if (tjsonAddDoubleToObject(pJson, "uptime", pInfo->uptime) != 0) uError("failed to add uptime");
  if (tjsonAddDoubleToObject(pJson, "cpu_engine", cpu_engine) != 0) uError("failed to add cpu_engine");
  if (tjsonAddDoubleToObject(pJson, "cpu_system", pSys->cpu_system) != 0) uError("failed to add cpu_system");
  if (tjsonAddDoubleToObject(pJson, "cpu_cores", pSys->cpu_cores) != 0) uError("failed to add cpu_cores");
  if (tjsonAddDoubleToObject(pJson, "mem_engine", mem_engine) != 0) uError("failed to add mem_engine");
  if (tjsonAddDoubleToObject(pJson, "mem_system", pSys->mem_system) != 0) uError("failed to add mem_system");
  if (tjsonAddDoubleToObject(pJson, "mem_total", pSys->mem_total) != 0) uError("failed to add mem_total");
  if (tjsonAddDoubleToObject(pJson, "disk_engine", pSys->disk_engine) != 0) uError("failed to add disk_engine");
  if (tjsonAddDoubleToObject(pJson, "disk_used", pSys->disk_used) != 0) uError("failed to add disk_used");
  if (tjsonAddDoubleToObject(pJson, "disk_total", pSys->disk_total) != 0) uError("failed to add disk_total");
  if (tjsonAddDoubleToObject(pJson, "net_in", net_in_rate) != 0) uError("failed to add net_in");
  if (tjsonAddDoubleToObject(pJson, "net_out", net_out_rate) != 0) uError("failed to add net_out");
  if (tjsonAddDoubleToObject(pJson, "io_read", io_read_rate) != 0) uError("failed to add io_read");
  if (tjsonAddDoubleToObject(pJson, "io_write", io_write_rate) != 0) uError("failed to add io_write");
  if (tjsonAddDoubleToObject(pJson, "io_read_disk", io_read_disk_rate) != 0) uError("failed to add io_read_disk");
  if (tjsonAddDoubleToObject(pJson, "io_write_disk", io_write_disk_rate) != 0) uError("failed to add io_write_disk");
  if (tjsonAddDoubleToObject(pJson, "req_select", pStat->numOfSelectReqs) != 0) uError("failed to add req_select");
  if (tjsonAddDoubleToObject(pJson, "req_select_rate", req_select_rate) != 0) uError("failed to add req_select_rate");
  if (tjsonAddDoubleToObject(pJson, "req_insert", pStat->numOfInsertReqs) != 0) uError("failed to add req_insert");
  if (tjsonAddDoubleToObject(pJson, "req_insert_success", pStat->numOfInsertSuccessReqs) != 0)
    uError("failed to add req_insert_success");
  if (tjsonAddDoubleToObject(pJson, "req_insert_rate", req_insert_rate) != 0) uError("failed to add req_insert_rate");
  if (tjsonAddDoubleToObject(pJson, "req_insert_batch", pStat->numOfBatchInsertReqs) != 0)
    uError("failed to add req_insert_batch");
  if (tjsonAddDoubleToObject(pJson, "req_insert_batch_success", pStat->numOfBatchInsertSuccessReqs) != 0)
    uError("failed to add req_insert_batch_success");
  if (tjsonAddDoubleToObject(pJson, "req_insert_batch_rate", req_insert_batch_rate) != 0)
    uError("failed to add req_insert_batch_rate");
  if (tjsonAddDoubleToObject(pJson, "errors", pStat->errors) != 0) uError("failed to add errors");
  if (tjsonAddDoubleToObject(pJson, "vnodes_num", pStat->totalVnodes) != 0) uError("failed to add vnodes_num");
  if (tjsonAddDoubleToObject(pJson, "masters", pStat->masterNum) != 0) uError("failed to add masters");
  if (tjsonAddDoubleToObject(pJson, "has_mnode", pInfo->has_mnode) != 0) uError("failed to add has_mnode");
  if (tjsonAddDoubleToObject(pJson, "has_qnode", pInfo->has_qnode) != 0) uError("failed to add has_qnode");
  if (tjsonAddDoubleToObject(pJson, "has_snode", pInfo->has_snode) != 0) uError("failed to add has_snode");
}

static void monGenDiskJson(SMonInfo *pMonitor) {
  SMonDiskInfo *pInfo = &pMonitor->vmInfo.tfs;
  SMonDiskDesc *pLogDesc = &pMonitor->dmInfo.dnode.logdir;
  SMonDiskDesc *pTempDesc = &pMonitor->dmInfo.dnode.tempdir;

  SJson *pJson = tjsonCreateObject();
  if (pJson == NULL) return;
  if (tjsonAddItemToObject(pMonitor->pJson, "disk_infos", pJson) != 0) {
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
  if (tjsonAddStringToObject(pLogdirJson, "name", pLogDesc->name) != 0) uError("failed to add string to json");
  if (tjsonAddDoubleToObject(pLogdirJson, "avail", pLogDesc->size.avail) != 0) uError("failed to add double to json");
  if (tjsonAddDoubleToObject(pLogdirJson, "used", pLogDesc->size.used) != 0) uError("failed to add double to json");
  if (tjsonAddDoubleToObject(pLogdirJson, "total", pLogDesc->size.total) != 0) uError("failed to add double to json");

  SJson *pTempdirJson = tjsonCreateObject();
  if (pTempdirJson == NULL) return;
  if (tjsonAddItemToObject(pJson, "tempdir", pTempdirJson) != 0) return;
  if (tjsonAddStringToObject(pTempdirJson, "name", pTempDesc->name) != 0) uError("failed to add string to json");
  if (tjsonAddDoubleToObject(pTempdirJson, "avail", pTempDesc->size.avail) != 0) uError("failed to add double to json");
  if (tjsonAddDoubleToObject(pTempdirJson, "used", pTempDesc->size.used) != 0) uError("failed to add double to json");
  if (tjsonAddDoubleToObject(pTempdirJson, "total", pTempDesc->size.total) != 0) uError("failed to add double to json");
}

static const char *monLogLevelStr(ELogLevel level) {
  if (level == DEBUG_ERROR) {
    return "error";
  } else {
    return "info";
  }
}

static void monGenLogJson(SMonInfo *pMonitor) {
  SJson *pJson = tjsonCreateObject();
  if (pJson == NULL) return;
  if (tjsonAddItemToObject(pMonitor->pJson, "log_infos", pJson) != 0) {
    tjsonDelete(pJson);
    return;
  }

  SMonLogs *logs[6];
  logs[0] = &pMonitor->log;
  logs[1] = &pMonitor->mmInfo.log;
  logs[2] = &pMonitor->vmInfo.log;
  logs[3] = &pMonitor->smInfo.log;
  logs[4] = &pMonitor->qmInfo.log;
  logs[5] = &pMonitor->bmInfo.log;

  int32_t numOfErrorLogs = 0;
  int32_t numOfInfoLogs = 0;
  int32_t numOfDebugLogs = 0;
  int32_t numOfTraceLogs = 0;

  for (int32_t j = 0; j < 6; j++) {
    SMonLogs *pLog = logs[j];
    numOfErrorLogs += pLog->numOfErrorLogs;
    numOfInfoLogs += pLog->numOfInfoLogs;
    numOfDebugLogs += pLog->numOfDebugLogs;
    numOfTraceLogs += pLog->numOfTraceLogs;
  }

  SJson *pSummaryJson = tjsonAddArrayToObject(pJson, "summary");
  if (pSummaryJson == NULL) return;

  SJson *pLogError = tjsonCreateObject();
  if (pLogError == NULL) return;
  if (tjsonAddStringToObject(pLogError, "level", "error") != 0) uError("failed to add string to json");
  if (tjsonAddDoubleToObject(pLogError, "total", numOfErrorLogs) != 0) uError("failed to add double to json");
  if (tjsonAddItemToArray(pSummaryJson, pLogError) != 0) tjsonDelete(pLogError);

  SJson *pLogInfo = tjsonCreateObject();
  if (pLogInfo == NULL) return;
  if (tjsonAddStringToObject(pLogInfo, "level", "info") != 0) uError("failed to add string to json");
  if (tjsonAddDoubleToObject(pLogInfo, "total", numOfInfoLogs) != 0) uError("failed to add double to json");
  if (tjsonAddItemToArray(pSummaryJson, pLogInfo) != 0) tjsonDelete(pLogInfo);

  SJson *pLogDebug = tjsonCreateObject();
  if (pLogDebug == NULL) return;
  if (tjsonAddStringToObject(pLogDebug, "level", "debug") != 0) uError("failed to add string to json");
  if (tjsonAddDoubleToObject(pLogDebug, "total", numOfDebugLogs) != 0) uError("failed to add double to json");
  if (tjsonAddItemToArray(pSummaryJson, pLogDebug) != 0) tjsonDelete(pLogDebug);

  SJson *pLogTrace = tjsonCreateObject();
  if (pLogTrace == NULL) return;
  if (tjsonAddStringToObject(pLogTrace, "level", "trace") != 0) uError("failed to add string to json");
  if (tjsonAddDoubleToObject(pLogTrace, "total", numOfTraceLogs) != 0) uError("failed to add double to json");
  if (tjsonAddItemToArray(pSummaryJson, pLogTrace) != 0) tjsonDelete(pLogTrace);
}

void monSendReport(SMonInfo *pMonitor) {
  char *pCont = tjsonToString(pMonitor->pJson);
  if (tsMonitorLogProtocol) {
    uInfoL("report cont:\n%s", pCont);
  }
  if (pCont != NULL) {
    EHttpCompFlag flag = tsMonitor.cfg.comp ? HTTP_GZIP : HTTP_FLAT;
    char          tmp[100] = {0};
    (void)snprintf(tmp, 100, "0x%" PRIxLEAST64, tGenQid64(tsMonitor.dnodeId));
    uDebug("report cont with QID:%s", tmp);
    if (taosSendHttpReportWithQID(tsMonitor.cfg.server, tsMonUri, tsMonitor.cfg.port, pCont, strlen(pCont), flag,
                                  tmp) != 0) {
      uError("failed to send monitor msg");
    }
    taosMemoryFree(pCont);
  }
}

void monSendReportBasic(SMonInfo *pMonitor) {
  char *pCont = tjsonToString(pMonitor->pJson);
  if (tsMonitorLogProtocol) {
    if (pCont != NULL) {
      uInfoL("report cont basic:\n%s", pCont);
    } else {
      uInfo("report cont basic is null");
    }
  }
  if (pCont != NULL) {
    EHttpCompFlag flag = tsMonitor.cfg.comp ? HTTP_GZIP : HTTP_FLAT;
    char          tmp[100] = {0};
    (void)sprintf(tmp, "0x%" PRIxLEAST64, tGenQid64(tsMonitor.dnodeId));
    uDebug("report cont basic with QID:%s", tmp);
    if (taosSendHttpReportWithQID(tsMonitor.cfg.server, tsMonFwBasicUri, tsMonitor.cfg.port, pCont, strlen(pCont), flag,
                                  tmp) != 0) {
      uError("failed to send monitor msg");
    }
    taosMemoryFree(pCont);
  }
}

void monGenAndSendReport() {
  SMonInfo *pMonitor = monCreateMonitorInfo();
  if (pMonitor == NULL) return;

  if (!tsMonitorForceV2) {
    monGenBasicJson(pMonitor);
    monGenClusterJson(pMonitor);
    monGenVgroupJson(pMonitor);
    monGenStbJson(pMonitor);
    monGenGrantJson(pMonitor);
    monGenDnodeJson(pMonitor);
    monGenDiskJson(pMonitor);
    monGenLogJson(pMonitor);

    monSendReport(pMonitor);
  } else {
    monGenClusterInfoTable(pMonitor);
    monGenVgroupInfoTable(pMonitor);
    monGenDnodeInfoTable(pMonitor);
    monGenDnodeStatusInfoTable(pMonitor);
    monGenDataDiskTable(pMonitor);
    monGenLogDiskTable(pMonitor);
    monGenMnodeRoleTable(pMonitor);
    monGenVnodeRoleTable(pMonitor);

    monSendPromReport();
    if (pMonitor->mmInfo.cluster.first_ep_dnode_id != 0) {
      monGenBasicJsonBasic(pMonitor);
      monGenClusterJsonBasic(pMonitor);
      monSendReportBasic(pMonitor);
    }
  }

  monCleanupMonitorInfo(pMonitor);
}

void monSendContent(char *pCont, const char *uri) {
  if (!tsEnableMonitor || tsMonitorFqdn[0] == 0 || tsMonitorPort == 0) return;
  if (tsMonitorLogProtocol) {
    if (pCont != NULL) {
      uInfoL("report client cont:\n%s\n", pCont);
    }
  }
  if (pCont != NULL) {
    char tmp[100] = {0};
    (void)sprintf(tmp, "0x%" PRIxLEAST64, tGenQid64(tsMonitor.dnodeId));
    uInfoL("report client cont with QID:%s", tmp);
    EHttpCompFlag flag = tsMonitor.cfg.comp ? HTTP_GZIP : HTTP_FLAT;
    if (taosSendHttpReportWithQID(tsMonitor.cfg.server, uri, tsMonitor.cfg.port, pCont, strlen(pCont), flag, tmp) !=
        0) {
      uError("failed to send monitor msg");
    }
  }
}