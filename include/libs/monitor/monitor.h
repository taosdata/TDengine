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

#ifndef _TD_MONITOR_H_
#define _TD_MONITOR_H_

#include "tarray.h"
#include "tdef.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  int32_t dnode_id;
  char    dnode_ep[TSDB_EP_LEN];
} SMonBasicInfo;

typedef struct {
  int32_t dnode_id;
  char    dnode_ep[TSDB_EP_LEN];
  char    status[8];
} SMonDnodeDesc;

typedef struct {
  int32_t mnode_id;
  char    mnode_ep[TSDB_EP_LEN];
  char    role[8];
} SMonMnodeDesc;

typedef struct {
  char    first_ep[TSDB_EP_LEN];
  int32_t first_ep_dnode_id;
  char    version[12];
  float   master_uptime;     // day
  int32_t monitor_interval;  // sec
  int32_t vgroups_total;
  int32_t vgroups_alive;
  int32_t vnodes_total;
  int32_t vnodes_alive;
  int32_t connections_total;
  SArray *dnodes;  // array of SMonDnodeDesc
  SArray *mnodes;  // array of SMonMnodeDesc
} SMonClusterInfo;

typedef struct {
  int32_t dnode_id;
  int8_t  vnode_online;
  char    vnode_role[8];
} SMonVnodeDesc;

typedef struct {
  int32_t       vgroup_id;
  SMonVnodeDesc vnodes[TSDB_MAX_REPLICA];
} SMonVgroupDesc;

typedef struct {
  char    database_name[TSDB_DB_NAME_LEN];
  int32_t tables_num;
  int8_t  status;
  SArray *vgroups;  // array of SMonVgroupDesc
} SMonVgroupInfo;

typedef struct {
  int32_t expire_time;
  int32_t timeseries_used;
  int32_t timeseries_total;
} SMonGrantInfo;

typedef struct {
  float   uptime;  // day
  float   cpu_engine;
  float   cpu_system;
  float   cpu_cores;
  float   mem_engine;     // MB
  float   mem_system;     // MB
  float   mem_total;      // MB
  float   disk_engine;    // GB
  float   disk_used;      // GB
  float   disk_total;     // GB
  float   net_in;         // Kb/s
  float   net_out;        // Kb/s
  float   io_read;        // Mb/s
  float   io_write;       // Mb/s
  float   io_read_disk;   // Mb/s
  float   io_write_disk;  // Mb/s
  int32_t req_select;
  float   req_select_rate;
  int32_t req_insert;
  int32_t req_insert_success;
  float   req_insert_rate;
  int32_t req_insert_batch;
  int32_t req_insert_batch_success;
  float   req_insert_batch_rate;
  int32_t errors;
  int32_t vnodes_num;
  int32_t masters;
  int32_t has_mnode;
} SMonDnodeInfo;

typedef struct {
  char      name[TSDB_FILENAME_LEN];
  int32_t   level;
  SDiskSize size;
} SMonDiskDesc;

typedef struct {
  SArray *disks;  // array of SMonDiskDesc
} SMonDiskInfo;

typedef struct {
  int64_t ts;
  int8_t  level;
  char    content[1024];
} SMonLogItem;

typedef struct SMonInfo SMonInfo;

typedef struct {
  const char *server;
  uint16_t    port;
  int32_t     maxLogs;
} SMonCfg;

int32_t monInit(const SMonCfg *pCfg);
void    monCleanup();
void    monAddLogItem(SMonLogItem *pItem);

SMonInfo *monCreateMonitorInfo();
void      monSetBasicInfo(SMonInfo *pMonitor, SMonBasicInfo *pInfo);
void      monSetClusterInfo(SMonInfo *pMonitor, SMonClusterInfo *pInfo);
void      monSetVgroupInfo(SMonInfo *pMonitor, SMonVgroupInfo *pInfo);
void      monSetGrantInfo(SMonInfo *pMonitor, SMonGrantInfo *pInfo);
void      monSetDnodeInfo(SMonInfo *pMonitor, SMonDnodeInfo *pInfo);
void      monSetDiskInfo(SMonInfo *pMonitor, SMonDiskInfo *pInfo);
void      monSendReport(SMonInfo *pMonitor);
void      monCleanupMonitorInfo(SMonInfo *pMonitor);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MONITOR_H_*/
