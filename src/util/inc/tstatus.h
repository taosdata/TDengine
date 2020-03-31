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

#ifndef TDENGINE_TSTATUS_H
#define TDENGINE_TSTATUS_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>
#include "taoserror.h"

enum _TSDB_VG_STATUS {
  TSDB_VG_STATUS_READY               = TSDB_CODE_SUCCESS,
  TSDB_VG_STATUS_IN_PROGRESS         = 1, //TSDB_CODE_ACTION_IN_PROGRESS,
  TSDB_VG_STATUS_NO_DISK_PERMISSIONS = 73,//TSDB_CODE_NO_DISK_PERMISSIONS,
  TSDB_VG_STATUS_SERVER_NO_PACE      = 110, //TSDB_CODE_SERV_NO_DISKSPACE,
  TSDB_VG_STATUS_SERV_OUT_OF_MEMORY  = 69, //TSDB_CODE_SERV_OUT_OF_MEMORY,
  TSDB_VG_STATUS_INIT_FAILED         = 74, //TSDB_CODE_VG_INIT_FAILED,
  TSDB_VG_STATUS_FULL                = 48, //TSDB_CODE_NO_ENOUGH_DNODES,
};

enum _TSDB_DB_STATUS {
  TSDB_DB_STATUS_READY,
  TSDB_DB_STATUS_DROPPING,
  TSDB_DB_STATUS_DROP_FROM_SDB
};

typedef enum _TSDB_VN_STATUS {
  TSDB_VN_STATUS_NOT_READY,
  TSDB_VN_STATUS_UNSYNCED,
  TSDB_VN_STATUS_SLAVE,
  TSDB_VN_STATUS_MASTER,
  TSDB_VN_STATUS_CREATING,
  TSDB_VN_STATUS_CLOSING,
  TSDB_VN_STATUS_DELETING,
} EVnodeStatus;

enum _TSDB_VN_SYNC_STATUS {
  TSDB_VN_SYNC_STATUS_INIT,
  TSDB_VN_SYNC_STATUS_SYNCING,
  TSDB_VN_SYNC_STATUS_SYNC_CACHE,
  TSDB_VN_SYNC_STATUS_SYNC_FILE
};

enum _TSDB_VN_DROP_STATUS {
  TSDB_VN_DROP_STATUS_READY,
  TSDB_VN_DROP_STATUS_DROPPING
};

enum _TSDB_DN_STATUS {
  TSDB_DN_STATUS_OFFLINE,
  TSDB_DN_STATUS_READY
};

enum _TSDB_DN_LB_STATUS {
  TSDB_DN_LB_STATUS_BALANCED,
  TSDB_DN_LB_STATUS_BALANCING,
  TSDB_DN_LB_STATUS_OFFLINE_REMOVING,
  TSDB_DN_LB_STATE_SHELL_REMOVING
};

enum _TSDB_VG_LB_STATUS {
  TSDB_VG_LB_STATUS_READY,
  TSDB_VG_LB_STATUS_UPDATE
};

enum _TSDB_VN_STREAM_STATUS {
  TSDB_VN_STREAM_STATUS_STOP,
  TSDB_VN_STREAM_STATUS_START
};

enum TSDB_TABLE_STATUS {
  TSDB_METER_STATE_READY       = 0x00,
  TSDB_METER_STATE_INSERTING   = 0x01,
  TSDB_METER_STATE_IMPORTING   = 0x02,
  TSDB_METER_STATE_UPDATING    = 0x04,
  TSDB_METER_STATE_DROPPING    = 0x10,
  TSDB_METER_STATE_DROPPED     = 0x18,
};

char* taosGetVgroupStatusStr(int32_t vgroupStatus);
char* taosGetDbStatusStr(int32_t dbStatus);
char* taosGetVnodeStatusStr(int32_t vnodeStatus);
char* taosGetVnodeSyncStatusStr(int32_t vnodeSyncStatus);
char* taosGetVnodeDropStatusStr(int32_t dropping);
char* taosGetDnodeStatusStr(int32_t dnodeStatus);
char* taosGetDnodeLbStatusStr(int32_t dnodeBalanceStatus);
char* taosGetVgroupLbStatusStr(int32_t vglbStatus);
char* taosGetVnodeStreamStatusStr(int32_t vnodeStreamStatus);
char* taosGetTableStatusStr(int32_t tableStatus);
char *taosGetShowTypeStr(int32_t showType);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TSTATUS_H
