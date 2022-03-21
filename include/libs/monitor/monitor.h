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
#include "tlog.h"

#ifdef __cplusplus
extern "C" {
#endif

#define MON_STATUS_LEN 8
#define MON_ROLE_LEN   9
#define MON_VER_LEN    12
#define MON_LOG_LEN    1024

typedef struct {
  int32_t dnode_id;
  char    dnode_ep[TSDB_EP_LEN];
  int64_t cluster_id;
  int32_t protocol;
} SMonBasicInfo;

typedef struct {
  int32_t dnode_id;
  char    dnode_ep[TSDB_EP_LEN];
  char    status[MON_STATUS_LEN];
} SMonDnodeDesc;

typedef struct {
  int32_t mnode_id;
  char    mnode_ep[TSDB_EP_LEN];
  char    role[MON_ROLE_LEN];
} SMonMnodeDesc;

typedef struct {
  char    first_ep[TSDB_EP_LEN];
  int32_t first_ep_dnode_id;
  char    version[MON_VER_LEN];
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
  char    vnode_role[MON_ROLE_LEN];
} SMonVnodeDesc;

typedef struct {
  int32_t       vgroup_id;
  char          database_name[TSDB_DB_NAME_LEN];
  int32_t       tables_num;
  char          status[MON_STATUS_LEN];
  SMonVnodeDesc vnodes[TSDB_MAX_REPLICA];
} SMonVgroupDesc;

typedef struct {
  SArray *vgroups;  // array of SMonVgroupDesc
} SMonVgroupInfo;

typedef struct {
  int32_t expire_time;
  int32_t timeseries_used;
  int32_t timeseries_total;
} SMonGrantInfo;

typedef struct {
  float   uptime;  // day
  double  cpu_engine;
  double  cpu_system;
  float   cpu_cores;
  int64_t mem_engine;   // KB
  int64_t mem_system;   // KB
  int64_t mem_total;    // KB
  int64_t disk_engine;  // Byte
  int64_t disk_used;    // Byte
  int64_t disk_total;   // Byte
  int64_t net_in;       // bytes
  int64_t net_out;      // bytes
  int64_t io_read;      // bytes
  int64_t io_write;     // bytes
  int64_t io_read_disk;   // bytes
  int64_t io_write_disk;  // bytes
  int64_t req_select;
  int64_t req_insert;
  int64_t req_insert_success;
  int64_t req_insert_batch;
  int64_t req_insert_batch_success;
  int32_t errors;
  int32_t vnodes_num;
  int32_t masters;
  int8_t  has_mnode;
} SMonDnodeInfo;

typedef struct {
  char      name[TSDB_FILENAME_LEN];
  int8_t    level;
  SDiskSize size;
} SMonDiskDesc;

typedef struct {
  SArray      *datadirs;  // array of SMonDiskDesc
  SMonDiskDesc logdir;
  SMonDiskDesc tempdir;
} SMonDiskInfo;

typedef struct SMonInfo SMonInfo;

typedef struct {
  const char *server;
  uint16_t    port;
  int32_t     maxLogs;
  bool        comp;
} SMonCfg;

int32_t monInit(const SMonCfg *pCfg);
void    monCleanup();
void    monRecordLog(int64_t ts, ELogLevel level, const char *content);

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
