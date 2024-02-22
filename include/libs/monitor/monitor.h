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
#include "tmsg.h"

#ifdef __cplusplus
extern "C" {
#endif

#define MON_STATUS_LEN 8
#define MON_ROLE_LEN   9
#define MON_VER_LEN    12
#define MON_LOG_LEN    1024

typedef struct {
  int64_t   ts;
  ELogLevel level;
  char      content[MON_LOG_LEN];
} SMonLogItem;

typedef struct {
  SArray *logs;  // array of SMonLogItem
  int32_t numOfErrorLogs;
  int32_t numOfInfoLogs;
  int32_t numOfDebugLogs;
  int32_t numOfTraceLogs;
} SMonLogs;

typedef struct {
  char      name[TSDB_FILENAME_LEN];
  int8_t    level;
  SDiskSize size;
} SMonDiskDesc;

typedef struct {
  double  cpu_engine;
  double  cpu_system;
  float   cpu_cores;
  int64_t mem_engine;     // KB
  int64_t mem_system;     // KB
  int64_t mem_total;      // KB
  int64_t disk_engine;    // Byte
  int64_t disk_used;      // Byte
  int64_t disk_total;     // Byte
  int64_t net_in;         // bytes
  int64_t net_out;        // bytes
  int64_t io_read;        // bytes
  int64_t io_write;       // bytes
  int64_t io_read_disk;   // bytes
  int64_t io_write_disk;  // bytes
} SMonSysInfo;

typedef struct {
  int32_t dnode_id;
  char    dnode_ep[TSDB_EP_LEN];
  int64_t cluster_id;
  int32_t protocol;
} SMonBasicInfo;

typedef struct {
  //float        uptime;  // day
  int64_t      uptime;  // second
  int8_t       has_mnode;
  int8_t       has_qnode;
  int8_t       has_snode;
  SMonDiskDesc logdir;
  SMonDiskDesc tempdir;
} SMonDnodeInfo;

typedef struct {
  SMonBasicInfo basic;
  SMonDnodeInfo dnode;
  SMonSysInfo   sys;
} SMonDmInfo;

typedef struct {
  int32_t dnode_id;
  char    dnode_ep[TSDB_EP_LEN];
  char    status[MON_STATUS_LEN];
} SMonDnodeDesc;

typedef struct {
  int32_t mnode_id;
  char    mnode_ep[TSDB_EP_LEN];
  char    role[MON_ROLE_LEN];
  int32_t syncState;
} SMonMnodeDesc;

typedef struct {
  char    first_ep[TSDB_EP_LEN];
  int32_t first_ep_dnode_id;
  char    version[MON_VER_LEN];
  //float   master_uptime;     // day
  int64_t master_uptime;        //second
  int32_t monitor_interval;  // sec
  int32_t dbs_total;
  int32_t stbs_total;
  int64_t tbs_total;
  int32_t vgroups_total;
  int32_t vgroups_alive;
  int32_t vnodes_total;
  int32_t vnodes_alive;
  int32_t connections_total;
  int32_t topics_toal;
  int32_t streams_total;
  SArray *dnodes;  // array of SMonDnodeDesc
  SArray *mnodes;  // array of SMonMnodeDesc
} SMonClusterInfo;

typedef struct {
  int32_t dnode_id;
  char    vnode_role[MON_ROLE_LEN];
  int32_t syncState;
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
  char stb_name[TSDB_TABLE_NAME_LEN];
  char database_name[TSDB_DB_NAME_LEN];
} SMonStbDesc;

typedef struct {
  SArray *stbs;  // array of SMonStbDesc
} SMonStbInfo;

typedef struct {
  int64_t  expire_time;
  int64_t  timeseries_used;
  int64_t  timeseries_total;
} SMonGrantInfo;

typedef struct {
  SMonClusterInfo cluster;
  SMonVgroupInfo  vgroup;
  SMonStbInfo     stb;
  SMonGrantInfo   grant;
  SMonSysInfo     sys;
  SMonLogs        log;
} SMonMmInfo;

typedef struct {
  SArray *datadirs;  // array of SMonDiskDesc
} SMonDiskInfo;

typedef struct {
  SMonDiskInfo tfs;
  SVnodesStat  vstat;
  SMonSysInfo  sys;
  SMonLogs     log;
} SMonVmInfo;

typedef struct {
  SMonSysInfo sys;
  SMonLogs    log;
  SQnodeLoad  load;
} SMonQmInfo;

typedef struct {
  SMonSysInfo sys;
  SMonLogs    log;
} SMonSmInfo;

typedef struct {
  SMonSysInfo sys;
  SMonLogs    log;
} SMonBmInfo;

typedef struct {
  SArray *pVloads;  // SVnodeLoad/SVnodeLoadLite
} SMonVloadInfo;

typedef struct {
  int8_t     isMnode;
  SMnodeLoad load;
} SMonMloadInfo;

typedef struct {
  const char *server;
  uint16_t    port;
  int32_t     maxLogs;
  bool        comp;
} SMonCfg;

typedef struct {
  int8_t state;
  tsem_t sem;
} SDmNotifyHandle;

int32_t monInit(const SMonCfg *pCfg);
void    monCleanup();
void    monRecordLog(int64_t ts, ELogLevel level, const char *content);
int32_t monGetLogs(SMonLogs *logs);
void    monSetDmInfo(SMonDmInfo *pInfo);
void    monSetMmInfo(SMonMmInfo *pInfo);
void    monSetVmInfo(SMonVmInfo *pInfo);
void    monSetQmInfo(SMonQmInfo *pInfo);
void    monSetSmInfo(SMonSmInfo *pInfo);
void    monSetBmInfo(SMonBmInfo *pInfo);
void    monGenAndSendReport();
void    monGenAndSendReportBasic();
void    monSendContent(char *pCont);

void tFreeSMonMmInfo(SMonMmInfo *pInfo);
void tFreeSMonVmInfo(SMonVmInfo *pInfo);
void tFreeSMonQmInfo(SMonQmInfo *pInfo);
void tFreeSMonSmInfo(SMonSmInfo *pInfo);
void tFreeSMonBmInfo(SMonBmInfo *pInfo);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MONITOR_H_*/
